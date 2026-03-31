from __future__ import annotations

import datetime as dt
import math
from dataclasses import asdict, dataclass
from statistics import mean, pstdev
from typing import Any

import pandas as pd

from .hurdle_rate import get_hurdle_settings
from .market_data_client import download_daily_bars
from .order_routing import compute_adv, get_routing_settings
from .persistence import (
    get_execution_quality_history,
    get_execution_quality_snapshot,
    get_theme,
    get_trade_plans,
    update_trade_plan_benchmarks,
)


@dataclass
class SlippageMetric:
    plan_id: int
    ticker: str
    action: str
    arrival_price: float | None
    fill_price: float | None
    vwap_benchmark: float | None
    close_price: float | None
    proposed_price: float | None
    impl_shortfall_bps: float | None
    vs_vwap_bps: float | None
    vs_close_bps: float | None
    vs_proposed_bps: float | None


@dataclass
class SlippageAggregation:
    dimension: str
    bucket: str
    sample_count: int
    avg_impl_shortfall_bps: float
    std_impl_shortfall_bps: float
    avg_vs_vwap_bps: float
    avg_vs_close_bps: float
    min_impl_shortfall_bps: float
    max_impl_shortfall_bps: float
    p25_impl_shortfall_bps: float
    p75_impl_shortfall_bps: float


@dataclass
class SlippagePattern:
    pattern_type: str
    description: str
    dimension: str
    bucket: str
    avg_slippage_bps: float
    baseline_bps: float
    z_score: float
    sample_count: int
    severity: str


@dataclass
class ExecutionQualityReport:
    portfolio_id: int
    analysis_date: str
    total_trades: int
    overall_avg_impl_shortfall_bps: float
    overall_avg_vs_vwap_bps: float
    by_strategy: list[SlippageAggregation]
    by_algo: list[SlippageAggregation]
    by_time_of_day: list[SlippageAggregation]
    by_theme: list[SlippageAggregation]
    by_adv_bucket: list[SlippageAggregation]
    patterns: list[SlippagePattern]
    best_strategy: str | None
    worst_strategy: str | None


def _signed_bps(fill_price: float | None, benchmark_price: float | None, action: str) -> float | None:
    if fill_price is None or benchmark_price is None:
        return None
    if float(benchmark_price) <= 0:
        return None
    fill_value = float(fill_price)
    benchmark_value = float(benchmark_price)
    if str(action or "").lower() == "sell":
        return ((benchmark_value - fill_value) / benchmark_value) * 10000.0
    return ((fill_value - benchmark_value) / benchmark_value) * 10000.0


def _float_or_none(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except Exception:
        return None


def _quantile(values: list[float], percentile: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return values[0]
    sorted_values = sorted(values)
    index = (len(sorted_values) - 1) * percentile
    lower = math.floor(index)
    upper = math.ceil(index)
    if lower == upper:
        return sorted_values[lower]
    fraction = index - lower
    return sorted_values[lower] + ((sorted_values[upper] - sorted_values[lower]) * fraction)


def compute_slippage_metric(plan: dict[str, Any]) -> SlippageMetric:
    action = str(plan.get("action") or "")
    arrival_price = _float_or_none(plan.get("arrival_price"))
    fill_price = _float_or_none(plan.get("execution_price"))
    vwap_benchmark = _float_or_none(plan.get("vwap_benchmark"))
    close_price = _float_or_none(plan.get("close_price"))
    proposed_price = _float_or_none(plan.get("proposed_price"))
    return SlippageMetric(
        plan_id=int(plan.get("id") or 0),
        ticker=str(plan.get("ticker") or "").upper(),
        action=action,
        arrival_price=arrival_price,
        fill_price=fill_price,
        vwap_benchmark=vwap_benchmark,
        close_price=close_price,
        proposed_price=proposed_price,
        impl_shortfall_bps=_signed_bps(fill_price, arrival_price, action),
        vs_vwap_bps=_signed_bps(fill_price, vwap_benchmark, action),
        vs_close_bps=_signed_bps(fill_price, close_price, action),
        vs_proposed_bps=_signed_bps(fill_price, proposed_price, action),
    )


def _time_of_day_bucket(plan: dict[str, Any]) -> str:
    raw = str(plan.get("executed_at") or plan.get("created_at") or "").strip()
    if not raw:
        return "other"
    try:
        timestamp = dt.datetime.fromisoformat(raw)
    except ValueError:
        return "other"
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=dt.timezone.utc)
    eastern = timestamp.astimezone(dt.timezone(dt.timedelta(hours=-4 if timestamp.month in {4, 5, 6, 7, 8, 9, 10} else -5)))
    clock = eastern.hour + (eastern.minute / 60.0)
    if 9.5 <= clock < 12.0:
        return "morning"
    if 12.0 <= clock < 14.0:
        return "midday"
    if 14.0 <= clock <= 16.0:
        return "closing"
    return "other"


def _adv_bucket_for_ticker(ticker: str) -> str:
    settings = get_routing_settings()
    adv = compute_adv(ticker, int(settings.get("adv_lookback_days", 20)))
    if adv is None:
        return "unknown"
    if adv > float(settings.get("adv_high_threshold", 1_000_000.0)):
        return "high_liquidity"
    if adv > float(settings.get("adv_low_threshold", 500_000.0)):
        return "medium_liquidity"
    return "low_liquidity"


def _aggregate_bucket(dimension: str, bucket: str, metrics: list[SlippageMetric]) -> SlippageAggregation:
    shortfalls = [float(metric.impl_shortfall_bps) for metric in metrics if metric.impl_shortfall_bps is not None]
    vs_vwap = [float(metric.vs_vwap_bps) for metric in metrics if metric.vs_vwap_bps is not None]
    vs_close = [float(metric.vs_close_bps) for metric in metrics if metric.vs_close_bps is not None]
    return SlippageAggregation(
        dimension=dimension,
        bucket=bucket,
        sample_count=len(metrics),
        avg_impl_shortfall_bps=mean(shortfalls) if shortfalls else 0.0,
        std_impl_shortfall_bps=pstdev(shortfalls) if len(shortfalls) > 1 else 0.0,
        avg_vs_vwap_bps=mean(vs_vwap) if vs_vwap else 0.0,
        avg_vs_close_bps=mean(vs_close) if vs_close else 0.0,
        min_impl_shortfall_bps=min(shortfalls) if shortfalls else 0.0,
        max_impl_shortfall_bps=max(shortfalls) if shortfalls else 0.0,
        p25_impl_shortfall_bps=_quantile(shortfalls, 0.25),
        p75_impl_shortfall_bps=_quantile(shortfalls, 0.75),
    )


def detect_slippage_patterns(
    aggregations: dict[str, list[SlippageAggregation]],
    overall_avg_bps: float,
    overall_std_bps: float,
    *,
    z_threshold: float = 2.0,
    min_samples: int = 5,
) -> list[SlippagePattern]:
    patterns: list[SlippagePattern] = []
    for dimension, rows in aggregations.items():
        for row in rows:
            if row.sample_count < int(min_samples):
                continue
            denominator = overall_std_bps or max(abs(overall_avg_bps), 1.0)
            z_score = (float(row.avg_impl_shortfall_bps) - float(overall_avg_bps)) / denominator
            if abs(z_score) < float(z_threshold):
                continue
            if z_score > 3.0:
                severity = "critical"
            elif z_score > 2.0:
                severity = "warning"
            else:
                severity = "info"
            pattern_type = "strategy_bias"
            if dimension == "time_of_day" and row.bucket == "morning":
                pattern_type = "morning_bias"
            elif dimension == "time_of_day" and row.bucket == "closing":
                pattern_type = "closing_bias"
            elif dimension == "algo":
                pattern_type = "algo_efficacy"
            elif dimension == "theme":
                pattern_type = "theme_bias"
            elif dimension == "adv_bucket":
                pattern_type = "liquidity_bias"
            patterns.append(
                SlippagePattern(
                    pattern_type=pattern_type,
                    description=f"{row.bucket} averages {row.avg_impl_shortfall_bps:.1f} bps versus baseline {overall_avg_bps:.1f} bps",
                    dimension=dimension,
                    bucket=row.bucket,
                    avg_slippage_bps=row.avg_impl_shortfall_bps,
                    baseline_bps=overall_avg_bps,
                    z_score=z_score,
                    sample_count=row.sample_count,
                    severity=severity,
                )
            )
    return patterns


def _execution_date(plan: dict[str, Any]) -> str | None:
    raw = str(plan.get("executed_at") or "").strip()
    if not raw:
        return None
    try:
        return dt.datetime.fromisoformat(raw).date().isoformat()
    except ValueError:
        return None


def _daily_bar_for_date(ticker: str, target_date: str) -> pd.Series | None:
    frame = download_daily_bars(str(ticker or "").upper(), period="3mo", auto_adjust=False)
    if frame is None or frame.empty:
        return None
    normalized = frame.copy()
    normalized.index = pd.to_datetime(normalized.index).tz_localize(None)
    match = normalized.loc[normalized.index.date == dt.date.fromisoformat(target_date)]
    if match.empty:
        return None
    return match.iloc[-1]


def backfill_execution_benchmarks(portfolio_id: int, date: str | None = None) -> dict[str, Any]:
    target_date = str(date or dt.datetime.now(dt.timezone.utc).date().isoformat())
    updated = 0
    skipped = 0
    errors: list[str] = []
    for plan in get_trade_plans(portfolio_id, status="all"):
        if str(plan.get("status") or "") != "Executed":
            skipped += 1
            continue
        if _execution_date(plan) != target_date:
            skipped += 1
            continue
        if plan.get("vwap_benchmark") not in (None, "") and plan.get("close_price") not in (None, ""):
            skipped += 1
            continue
        try:
            bar = _daily_bar_for_date(str(plan.get("ticker") or ""), target_date)
            if bar is None:
                skipped += 1
                continue
            high = _float_or_none(bar.get("High"))
            low = _float_or_none(bar.get("Low"))
            close = _float_or_none(bar.get("Close"))
            if high is None or low is None or close is None:
                skipped += 1
                continue
            vwap = (high + low + close) / 3.0
            if update_trade_plan_benchmarks(int(plan.get("id") or 0), vwap_benchmark=vwap, close_price=close):
                updated += 1
        except Exception as exc:
            errors.append(f"{plan.get('ticker')}: {exc}")
    return {"updated": updated, "skipped": skipped, "errors": errors}


def compute_execution_quality(
    portfolio_id: int,
    *,
    lookback_days: int = 90,
) -> ExecutionQualityReport:
    cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=max(1, int(lookback_days)))
    plans = [
        plan
        for plan in get_trade_plans(portfolio_id, status="all")
        if str(plan.get("status") or "") == "Executed"
        and str(plan.get("executed_at") or "")
        and dt.datetime.fromisoformat(str(plan.get("executed_at"))).astimezone(dt.timezone.utc) >= cutoff
    ]
    metrics = [compute_slippage_metric(plan) for plan in plans]
    overall_values = [float(metric.impl_shortfall_bps) for metric in metrics if metric.impl_shortfall_bps is not None]
    overall_vwap = [float(metric.vs_vwap_bps) for metric in metrics if metric.vs_vwap_bps is not None]
    by_dimension: dict[str, dict[str, list[SlippageMetric]]] = {
        "strategy": {},
        "algo": {},
        "time_of_day": {},
        "theme": {},
        "adv_bucket": {},
    }
    for plan, metric in zip(plans, metrics, strict=False):
        by_dimension["strategy"].setdefault(str(plan.get("routing_strategy") or "unspecified"), []).append(metric)
        by_dimension["algo"].setdefault(str(plan.get("algo_strategy") or "none"), []).append(metric)
        by_dimension["time_of_day"].setdefault(_time_of_day_bucket(plan), []).append(metric)
        theme_name = "Unassigned"
        if plan.get("theme_id") is not None:
            theme = get_theme(int(plan["theme_id"]))
            if theme is not None:
                theme_name = str(theme.get("name") or theme_name)
        by_dimension["theme"].setdefault(theme_name, []).append(metric)
        by_dimension["adv_bucket"].setdefault(_adv_bucket_for_ticker(metric.ticker), []).append(metric)
    aggregations = {
        name: [
            _aggregate_bucket(name, bucket, bucket_metrics)
            for bucket, bucket_metrics in sorted(grouped.items(), key=lambda item: item[0])
        ]
        for name, grouped in by_dimension.items()
    }
    patterns = detect_slippage_patterns(
        aggregations,
        mean(overall_values) if overall_values else 0.0,
        pstdev(overall_values) if len(overall_values) > 1 else 0.0,
    )
    best_strategy = None
    worst_strategy = None
    if aggregations["strategy"]:
        best_strategy = min(aggregations["strategy"], key=lambda row: row.avg_impl_shortfall_bps).bucket
        worst_strategy = max(aggregations["strategy"], key=lambda row: row.avg_impl_shortfall_bps).bucket
    return ExecutionQualityReport(
        portfolio_id=int(portfolio_id),
        analysis_date=dt.datetime.now(dt.timezone.utc).date().isoformat(),
        total_trades=len(metrics),
        overall_avg_impl_shortfall_bps=mean(overall_values) if overall_values else 0.0,
        overall_avg_vs_vwap_bps=mean(overall_vwap) if overall_vwap else 0.0,
        by_strategy=aggregations["strategy"],
        by_algo=aggregations["algo"],
        by_time_of_day=aggregations["time_of_day"],
        by_theme=aggregations["theme"],
        by_adv_bucket=aggregations["adv_bucket"],
        patterns=patterns,
        best_strategy=best_strategy,
        worst_strategy=worst_strategy,
    )


def estimate_execution_cost(
    ticker: str,
    routing_strategy: str,
    algo_strategy: str,
    portfolio_id: int,
    *,
    min_sample_size: int = 10,
    lookback_days: int = 90,
) -> float:
    settings = get_hurdle_settings()
    if not bool(settings.get("slippage_feedback_enabled", True)):
        return 0.0
    min_trades = int(settings.get("slippage_min_sample_size", min_sample_size) or min_sample_size)
    days = int(settings.get("slippage_lookback_days", lookback_days) or lookback_days)
    cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=max(1, days))
    plans = [
        plan
        for plan in get_trade_plans(portfolio_id, status="all")
        if str(plan.get("status") or "") == "Executed"
        and str(plan.get("executed_at") or "")
        and dt.datetime.fromisoformat(str(plan.get("executed_at"))).astimezone(dt.timezone.utc) >= cutoff
    ]
    metrics = [compute_slippage_metric(plan) for plan in plans]
    target_buy = [
        metric.impl_shortfall_bps
        for plan, metric in zip(plans, metrics, strict=False)
        if metric.impl_shortfall_bps is not None
        and str(plan.get("action") or "").lower() == "buy"
        and str(plan.get("routing_strategy") or "") == str(routing_strategy or "")
        and str(plan.get("algo_strategy") or "") == str(algo_strategy or "")
    ]
    target_sell = [
        metric.impl_shortfall_bps
        for plan, metric in zip(plans, metrics, strict=False)
        if metric.impl_shortfall_bps is not None
        and str(plan.get("action") or "").lower() == "sell"
        and str(plan.get("routing_strategy") or "") == str(routing_strategy or "")
        and str(plan.get("algo_strategy") or "") == str(algo_strategy or "")
    ]
    if len(target_buy) + len(target_sell) >= min_trades:
        buy_leg = mean(target_buy) if target_buy else (mean(target_sell) if target_sell else 0.0)
        sell_leg = mean(target_sell) if target_sell else (mean(target_buy) if target_buy else 0.0)
        return max(0.0, (buy_leg + sell_leg) / 100.0)
    overall_buy = [
        metric.impl_shortfall_bps
        for plan, metric in zip(plans, metrics, strict=False)
        if metric.impl_shortfall_bps is not None and str(plan.get("action") or "").lower() == "buy"
    ]
    overall_sell = [
        metric.impl_shortfall_bps
        for plan, metric in zip(plans, metrics, strict=False)
        if metric.impl_shortfall_bps is not None and str(plan.get("action") or "").lower() == "sell"
    ]
    if len(overall_buy) + len(overall_sell) < min_trades:
        return 0.0
    buy_leg = mean(overall_buy) if overall_buy else (mean(overall_sell) if overall_sell else 0.0)
    sell_leg = mean(overall_sell) if overall_sell else (mean(overall_buy) if overall_buy else 0.0)
    return max(0.0, (buy_leg + sell_leg) / 100.0)


def get_execution_quality_trades(
    portfolio_id: int,
    *,
    limit: int = 50,
    offset: int = 0,
    ticker: str | None = None,
    strategy: str | None = None,
) -> list[dict[str, Any]]:
    plans = [plan for plan in get_trade_plans(portfolio_id, status="all") if str(plan.get("status") or "") == "Executed"]
    if ticker:
        plans = [plan for plan in plans if str(plan.get("ticker") or "").upper() == str(ticker or "").upper()]
    if strategy:
        plans = [plan for plan in plans if str(plan.get("routing_strategy") or "") == str(strategy or "")]
    sliced = plans[int(offset): int(offset) + max(1, int(limit))]
    return [{**plan, **asdict(compute_slippage_metric(plan))} for plan in sliced]


def get_execution_quality_ticker_diagnostic(ticker: str, *, portfolio_id: int, lookback_days: int = 90) -> dict[str, Any]:
    symbol = str(ticker or "").upper()
    trades = get_execution_quality_trades(portfolio_id, limit=500, offset=0, ticker=symbol)
    metrics: list[float] = [float(row["impl_shortfall_bps"]) for row in trades if row.get("impl_shortfall_bps") is not None]
    report = get_execution_quality_snapshot(portfolio_id)
    grouped_trend: dict[str, list[float]] = {}
    for row in trades:
        raw = str(row.get("executed_at") or row.get("created_at") or "")
        if not raw or row.get("impl_shortfall_bps") is None:
            continue
        try:
            trade_date = dt.datetime.fromisoformat(raw).date().isoformat()
        except ValueError:
            continue
        grouped_trend.setdefault(trade_date, []).append(float(row["impl_shortfall_bps"]))
    trend = [
        {
            "date": trade_date,
            "avg_impl_shortfall_bps": mean(values),
            "trade_count": len(values),
        }
        for trade_date, values in sorted(grouped_trend.items())
    ]
    estimated_cost = estimate_execution_cost(
        symbol,
        routing_strategy=str(trades[0].get("routing_strategy") or "") if trades else "",
        algo_strategy=str(trades[0].get("algo_strategy") or "") if trades else "",
        portfolio_id=portfolio_id,
    )
    return {
        "ticker": symbol,
        "sample_count": len(metrics),
        "avg_impl_shortfall_bps": mean(metrics) if metrics else 0.0,
        "estimated_execution_cost_pct": estimated_cost,
        "trades": trades,
        "trend": trend,
        "latest_snapshot_date": report.get("analysis_date") if isinstance(report, dict) else None,
    }
