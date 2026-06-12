# mypy: ignore-errors
from __future__ import annotations

import datetime as dt
import json
import logging
import math
import uuid
from dataclasses import asdict
from typing import Any

import pandas as pd

from ..agent_competition import active_ticker_owners, configured_beta_portfolio_ids, diversification_settings
from ..agent_policy import (
    agent_candidate_policy,
    buy_pause_status,
    earnings_blackout_status,
    near_close_cancel_active,
    setting_bool as policy_setting_bool,
    setting_float as policy_setting_float,
    setting_int as policy_setting_int,
)
from ..broker_adapter import BrokerAdapter, OrderRequest, PaperBrokerAdapter, submit_guarded_order, validate_guardrails
from ..config import (
    DEFAULT_PAPER_TRADING_CONFIG,
    DEFAULT_RISK_GUARDRAILS,
    PaperTradingConfig,
    RiskGuardrails,
)
from ..decision_constants import (
    DEFAULT_EXIT_TIME_STOP_DAYS,
    DEFAULT_NEUTRAL_REDUCE_FRACTION,
    TRAILING_STOP_ACTIVATION_ATR,
)
from ..discovery import _quick_regime_screen
from ..fundamental_data import fetch_financial_statements
from ..hurdle_rate import check_duration_gate, check_hurdle_rate, get_hurdle_settings
from ..ltcg_override import get_ltcg_override_settings
from ..market_data_client import download_daily_bars, get_ticker_info
from ..order_routing import decide_routing
from ..persistence import (
    close_paper_position,
    count_todays_trades,
    create_trade_plan,
    get_auto_approve_threshold,
    get_daily_capital_ceiling_pct,
    get_daily_capital_deployed,
    get_daily_snapshots,
    get_audit_trail,
    get_operating_mode,
    get_performance_timeseries,
    get_paper_portfolio,
    get_paper_portfolio_summary,
    get_paper_positions,
    get_latest_signal_snapshot,
    get_setting,
    get_trade_plans,
    get_watchlist,
    get_watchlist_by_ticker,
    log_barrier_override,
    log_audit_event,
    list_paper_portfolios,
    list_themes,
    open_paper_position,
    save_alert,
    save_daily_snapshot,
    set_auto_approve_threshold,
    set_daily_capital_ceiling_pct,
    set_operating_mode,
    update_paper_portfolio,
    update_paper_position_quantity,
    update_paper_position_risk,
    update_trade_plan_status,
)
from ..ib_types import ET
from ..signal_quality import ACTIONABLE_SIGNAL_SCORE, SignalQuality, evaluate_signal_quality
from . import core

logger = core.logger
CachedRegimeValue = core.CachedRegimeValue
CachedRegimeMap = core.CachedRegimeMap
DEFAULT_SIZING_METHOD = core.DEFAULT_SIZING_METHOD
DEFAULT_SIZING_BASE_RISK_FRACTION = core.DEFAULT_SIZING_BASE_RISK_FRACTION
DEFAULT_SIZING_ATR_MULTIPLIER = core.DEFAULT_SIZING_ATR_MULTIPLIER
DEFAULT_BETA_TARGET_MONTHLY_RETURN = core.DEFAULT_BETA_TARGET_MONTHLY_RETURN
DEFAULT_BETA_TARGET_ROLLING_MONTHS = core.DEFAULT_BETA_TARGET_ROLLING_MONTHS
DEFAULT_BETA_TARGET_BENCHMARKS = core.DEFAULT_BETA_TARGET_BENCHMARKS
LONG_TERM_HOLDING_DAYS = core.LONG_TERM_HOLDING_DAYS

_routing_time_in_force_from_plan = core._routing_time_in_force_from_plan
_guardrail_block_details = core._guardrail_block_details
_normalize_close_series = core._normalize_close_series
_last_price_from_series = core._last_price_from_series
_now = core._now
_theme_map = core._theme_map
_parse_timestamp = core._parse_timestamp
_aware_utc = core._aware_utc
_holding_days = core._holding_days
_cached_regime_map = core._cached_regime_map
_batch_current_prices = core._batch_current_prices
_pending_plan_index = core._pending_plan_index
_open_position_index = core._open_position_index
get_sizing_settings = core.get_sizing_settings
get_beta_target_settings = core.get_beta_target_settings
is_portfolio_autonomy_enabled = core.is_portfolio_autonomy_enabled
_positive_float = core._positive_float
_entry_signal_max_age_days = core._entry_signal_max_age_days
_lookup_atr = core._lookup_atr
_lookup_beta = core._lookup_beta
_truthy = core._truthy
_serialize_rows = core._serialize_rows

from .sizing import allocate_budget

def _agent_tax_rates() -> dict[str, float]:
    hurdle = get_hurdle_settings()
    ltcg = get_ltcg_override_settings()
    stcg_rate = policy_setting_float(
        "agent_estimated_stcg_rate",
        float(hurdle.get("estimated_stcg_rate") or 0.32),
        minimum=0.0,
        maximum=0.99,
    )
    ltcg_rate = policy_setting_float(
        "agent_estimated_ltcg_rate",
        float(ltcg.get("ltcg_rate") or 0.15),
        minimum=0.0,
        maximum=0.99,
    )
    return {"stcg_rate": stcg_rate, "ltcg_rate": ltcg_rate}


def _open_position_unrealized_pnl(row: dict[str, Any]) -> float:
    if row.get("unrealized_pnl") not in (None, ""):
        return float(row.get("unrealized_pnl") or 0.0)
    quantity = float(row.get("quantity") or 0.0)
    entry_price = float(row.get("entry_price") or row.get("cost_basis_per_share") or 0.0)
    current_price = float(row.get("current_price") or row.get("market_price") or entry_price or 0.0)
    side = str(row.get("side") or "long").lower()
    return (entry_price - current_price) * quantity if side == "short" else (current_price - entry_price) * quantity


def _tax_term(row: dict[str, Any], *, now: dt.datetime | None = None) -> str:
    explicit = str(row.get("gain_loss_term") or "").upper()
    if explicit.startswith("LT"):
        return "LT"
    if explicit.startswith("ST"):
        return "ST"
    return "LT" if _holding_days(row, now=now) >= LONG_TERM_HOLDING_DAYS else "ST"


def estimate_after_tax_performance(
    portfolio_id: int,
    *,
    summary: dict[str, Any] | None = None,
    open_positions: list[dict[str, Any]] | None = None,
    closed_positions: list[dict[str, Any]] | None = None,
    now: dt.datetime | None = None,
) -> dict[str, Any]:
    """Estimate after-tax equity using a conservative reserve on taxable gains."""

    portfolio = get_paper_portfolio(portfolio_id)
    if portfolio is None:
        return {}
    summary_payload = summary if summary is not None else get_paper_portfolio_summary(portfolio_id)
    open_rows = list(open_positions if open_positions is not None else summary_payload.get("positions") or get_paper_positions(portfolio_id, status="Open"))
    closed_rows = list(closed_positions if closed_positions is not None else get_paper_positions(portfolio_id, status="Closed"))
    rates = _agent_tax_rates()
    stcg_rate = rates["stcg_rate"]
    ltcg_rate = rates["ltcg_rate"]
    current = now or _now()

    realized_st = 0.0
    realized_lt = 0.0
    unrealized_st = 0.0
    unrealized_lt = 0.0

    for row in closed_rows:
        pnl = float(row.get("realized_pnl") or 0.0)
        if _tax_term(row, now=current) == "LT":
            realized_lt += pnl
        else:
            realized_st += pnl
    for row in open_rows:
        pnl = _open_position_unrealized_pnl(row)
        if _tax_term(row, now=current) == "LT":
            unrealized_lt += pnl
        else:
            unrealized_st += pnl

    estimated_realized_tax = max(0.0, realized_st) * stcg_rate + max(0.0, realized_lt) * ltcg_rate
    estimated_unrealized_tax = max(0.0, unrealized_st) * stcg_rate + max(0.0, unrealized_lt) * ltcg_rate
    estimated_tax_drag = estimated_realized_tax + estimated_unrealized_tax
    estimated_realized_loss_tax_value = max(0.0, -realized_st) * stcg_rate + max(0.0, -realized_lt) * ltcg_rate
    estimated_unrealized_loss_tax_value = max(0.0, -unrealized_st) * stcg_rate + max(0.0, -unrealized_lt) * ltcg_rate

    current_cash = float(summary_payload.get("current_cash") or portfolio.get("current_cash") or 0.0)
    total_market_value = float(summary_payload.get("total_market_value") or 0.0)
    pretax_equity = current_cash + total_market_value
    starting_budget = float(portfolio.get("starting_budget") or summary_payload.get("starting_budget") or 0.0)
    pretax_profit = pretax_equity - starting_budget
    after_tax_equity = pretax_equity - estimated_tax_drag
    after_tax_profit = after_tax_equity - starting_budget
    return {
        "tax_model": "estimated_gain_reserve",
        "estimated_stcg_rate": stcg_rate,
        "estimated_ltcg_rate": ltcg_rate,
        "realized_st_pnl": realized_st,
        "realized_lt_pnl": realized_lt,
        "unrealized_st_pnl": unrealized_st,
        "unrealized_lt_pnl": unrealized_lt,
        "estimated_realized_tax": estimated_realized_tax,
        "estimated_unrealized_tax": estimated_unrealized_tax,
        "estimated_tax_drag": estimated_tax_drag,
        "estimated_realized_loss_tax_value": estimated_realized_loss_tax_value,
        "estimated_unrealized_loss_tax_value": estimated_unrealized_loss_tax_value,
        "pretax_equity": pretax_equity,
        "pretax_profit": pretax_profit,
        "after_tax_equity": after_tax_equity,
        "after_tax_profit": after_tax_profit,
        "after_tax_return_pct": (after_tax_profit / starting_budget * 100.0) if starting_budget > 0 else 0.0,
        "tax_efficiency_pct": (after_tax_profit / pretax_profit * 100.0) if pretax_profit > 0 else None,
    }


def compute_benchmark_comparison(
    portfolio_id: int,
    benchmark_ticker: str = "SPY",
    *,
    benchmark_data: pd.DataFrame | None = None,
) -> dict[str, Any]:
    portfolio = get_paper_portfolio(portfolio_id)
    if portfolio is None:
        return {}
    started_at = _parse_timestamp(portfolio.get("created_at")) or _now()
    days = max(30, (_now() - started_at).days + 5)
    try:
        frame = benchmark_data
        if frame is None:
            frame = download_daily_bars(benchmark_ticker, period=f"{days}d", auto_adjust=False)
        close = _normalize_close_series(frame)
        _close_first = float(close.iloc[0]) if len(close) else 0.0
        _close_last = float(close.iloc[-1]) if len(close) else 0.0
        benchmark_return = float((_close_last - _close_first) / _close_first) if len(close) >= 2 and _close_first else None
    except Exception as exc:
        logger.warning("Unable to compute paper-trading benchmark comparison.", exc_info=exc)
        benchmark_return = None
    summary = get_paper_portfolio_summary(portfolio_id)
    portfolio_return = float(summary.get("total_return_pct") or 0.0) / 100.0
    alpha = (portfolio_return - benchmark_return) if benchmark_return is not None else None
    return {
        "benchmark_ticker": benchmark_ticker,
        "benchmark": benchmark_ticker,
        "benchmark_return": benchmark_return,
        "benchmark_return_pct": (benchmark_return * 100.0) if benchmark_return is not None else None,
        "portfolio_return": portfolio_return,
        "paper_return_pct": portfolio_return * 100.0,
        "alpha": alpha,
        "alpha_pct": (alpha * 100.0) if alpha is not None else None,
    }


def compute_benchmark_set(
    portfolio_id: int,
    benchmarks: tuple[str, ...] | list[str] | None = None,
    *,
    preloaded: dict[str, pd.DataFrame | None] | None = None,
) -> dict[str, dict[str, Any]]:
    symbols = tuple(benchmarks or get_beta_target_settings()["benchmarks"])
    rows: dict[str, dict[str, Any]] = {}
    for symbol in symbols:
        ticker = str(symbol or "").strip().upper()
        if not ticker:
            continue
        try:
            rows[ticker] = compute_benchmark_comparison(
                portfolio_id,
                benchmark_ticker=ticker,
                benchmark_data=(preloaded or {}).get(ticker),
            )
        except Exception as exc:
            logger.warning("Unable to compute %s benchmark comparison.", ticker, exc_info=exc)
            rows[ticker] = {
                "benchmark_ticker": ticker,
                "benchmark": ticker,
                "benchmark_return": None,
                "benchmark_return_pct": None,
                "portfolio_return": None,
                "paper_return_pct": None,
                "alpha": None,
                "alpha_pct": None,
                "error": str(exc),
            }
    return rows


def compute_beta_target_progress(portfolio_id: int, *, summary: dict[str, Any] | None = None) -> dict[str, Any]:
    portfolio = get_paper_portfolio(portfolio_id)
    if portfolio is None:
        return {}
    target_settings = get_beta_target_settings()
    starting_budget = float(portfolio.get("starting_budget") or 0.0)
    summary_payload = summary if summary is not None else get_paper_portfolio_summary(portfolio_id)
    equity = float(summary_payload.get("current_cash") or 0.0) + float(summary_payload.get("total_market_value") or 0.0)
    after_tax = estimate_after_tax_performance(portfolio_id, summary=summary_payload)
    after_tax_equity = float(after_tax.get("after_tax_equity") or equity)
    target_basis = "after_tax" if policy_setting_bool("agent_after_tax_performance_enabled", True) else "pretax"
    basis_equity = after_tax_equity if target_basis == "after_tax" else equity
    created_at = _parse_timestamp(portfolio.get("created_at")) or _now()
    elapsed_days = max(0.0, (_now() - created_at).total_seconds() / 86400.0)
    elapsed_months = elapsed_days / 30.4375
    monthly_target = float(target_settings["monthly_return"])
    if starting_budget > 0:
        total_return = (basis_equity - starting_budget) / starting_budget
    else:
        total_return = 0.0
    target_return = ((1.0 + monthly_target) ** elapsed_months) - 1.0 if elapsed_months > 0 else 0.0
    target_equity = starting_budget * (1.0 + target_return)
    current_monthly_run_rate = None
    if starting_budget > 0 and basis_equity > 0 and elapsed_months >= (2.0 / 30.4375):
        current_monthly_run_rate = (basis_equity / starting_budget) ** (1.0 / elapsed_months) - 1.0
    gap_to_target = basis_equity - target_equity
    gap_to_target_return = total_return - target_return
    if elapsed_days < 2:
        status = "warming_up"
        status_label = "Warming up"
    elif current_monthly_run_rate is not None and current_monthly_run_rate >= monthly_target:
        status = "on_track"
        status_label = "On target"
    else:
        status = "behind"
        status_label = "Behind target"
    return {
        "target_monthly_return": monthly_target,
        "target_monthly_return_pct": monthly_target * 100.0,
        "rolling_months": int(target_settings["rolling_months"]),
        "elapsed_days": elapsed_days,
        "elapsed_months": elapsed_months,
        "starting_budget": starting_budget,
        "basis": target_basis,
        "basis_label": "Estimated after-tax equity" if target_basis == "after_tax" else "Pretax equity",
        "current_equity": basis_equity,
        "pretax_equity": equity,
        "after_tax_equity": after_tax_equity,
        "estimated_tax_drag": float(after_tax.get("estimated_tax_drag") or 0.0),
        "current_total_return": total_return,
        "current_total_return_pct": total_return * 100.0,
        "current_monthly_run_rate": current_monthly_run_rate,
        "current_monthly_run_rate_pct": (current_monthly_run_rate * 100.0) if current_monthly_run_rate is not None else None,
        "target_return": target_return,
        "target_return_pct": target_return * 100.0,
        "target_equity": target_equity,
        "gap_to_target": gap_to_target,
        "gap_to_target_return": gap_to_target_return,
        "gap_to_target_return_pct": gap_to_target_return * 100.0,
        "status": status,
        "status_label": status_label,
        "benchmarks": list(target_settings["benchmarks"]),
    }


def compute_paper_performance(portfolio_id: int) -> dict[str, Any]:
    portfolio = get_paper_portfolio(portfolio_id)
    if portfolio is None:
        return {}
    summary = get_paper_portfolio_summary(portfolio_id)
    open_positions = get_paper_positions(portfolio_id, status="Open")
    closed_positions = get_paper_positions(portfolio_id, status="Closed")
    prices = _batch_current_prices([str(row.get("ticker") or "") for row in open_positions])
    market_value = 0.0
    unrealized_pnl = 0.0
    marked_positions: list[dict[str, Any]] = []
    for row in open_positions:
        ticker = str(row.get("ticker") or "").upper()
        quantity = float(row.get("quantity") or 0.0)
        entry_price = float(row.get("entry_price") or 0.0)
        current_price = prices.get(ticker, entry_price)
        value = quantity * current_price
        pnl = (current_price - entry_price) * quantity
        market_value += value
        unrealized_pnl += pnl
        marked_positions.append({**row, "current_price": current_price, "market_value": value, "unrealized_pnl": pnl})
    realized_pnl = sum(float(row.get("realized_pnl") or 0.0) for row in closed_positions)
    total_equity = float(portfolio.get("current_cash") or 0.0) + market_value
    starting_budget = float(portfolio.get("starting_budget") or 0.0)
    total_return_pct = ((total_equity - starting_budget) / starting_budget * 100.0) if starting_budget > 0 else 0.0
    after_tax = estimate_after_tax_performance(
        portfolio_id,
        summary={**summary, "current_cash": float(portfolio.get("current_cash") or 0.0), "total_market_value": market_value},
        open_positions=marked_positions,
        closed_positions=closed_positions,
    )
    wins = [row for row in closed_positions if float(row.get("realized_pnl") or 0.0) > 0]
    started_at = _parse_timestamp(portfolio.get("created_at")) or _now()
    days = max(30, (_now() - started_at).days + 5)
    benchmark_data = None
    preloaded_benchmarks: dict[str, pd.DataFrame | None] = {}
    for benchmark_symbol in get_beta_target_settings()["benchmarks"]:
        try:
            preloaded_benchmarks[str(benchmark_symbol)] = download_daily_bars(str(benchmark_symbol), period=f"{days}d", auto_adjust=False)
        except Exception as exc:
            logger.warning("Unable to prefetch %s benchmark data for paper trading.", benchmark_symbol, exc_info=exc)
    try:
        benchmark = compute_benchmark_comparison(portfolio_id, benchmark_ticker="SPY", benchmark_data=preloaded_benchmarks.get("SPY"))
    except TypeError:
        benchmark = compute_benchmark_comparison(portfolio_id)
    benchmarks = compute_benchmark_set(portfolio_id, preloaded=preloaded_benchmarks)
    snapshots = get_performance_timeseries(portfolio_id)
    drawdowns = [abs(min(0.0, float(row.get("drawdown_pct") or 0.0))) for row in snapshots]
    max_drawdown_pct = max(drawdowns) if drawdowns else 0.0
    target = compute_beta_target_progress(portfolio_id, summary={**summary, **after_tax, "total_market_value": market_value})
    return {
        **summary,
        **after_tax,
        "portfolio_id": portfolio_id,
        "positions": _serialize_rows(marked_positions),
        "current_cash": float(portfolio.get("current_cash") or 0.0),
        "total_market_value": market_value,
        "unrealized_pnl": unrealized_pnl,
        "realized_pnl": realized_pnl,
        "total_equity": total_equity,
        "total_return_pct": total_return_pct,
        "win_rate": (len(wins) / len(closed_positions)) if closed_positions else None,
        "closed_trade_count": len(closed_positions),
        "benchmark": benchmark,
        "benchmarks": benchmarks,
        "target": target,
        "max_drawdown_pct": max_drawdown_pct,
        "snapshots": snapshots,
    }


def get_paper_dashboard(portfolio_id: int, *, cached_regime: dict[str, Any] | None = None) -> dict[str, Any]:
    portfolio = get_paper_portfolio(portfolio_id)
    if portfolio is None:
        return {}
    return {
        "portfolio": portfolio,
        "allocation": allocate_budget(portfolio_id),
        "summary": get_paper_portfolio_summary(portfolio_id),
        "positions": get_paper_positions(portfolio_id, status="all"),
        "plans": get_trade_plans(portfolio_id, status="all"),
        "performance": compute_paper_performance(portfolio_id),
        "cached_regime_available": bool(_cached_regime_map(cached_regime)),
    }


def compute_daily_snapshot(portfolio_id: int) -> dict[str, Any]:
    summary = get_paper_portfolio_summary(portfolio_id)
    portfolio = get_paper_portfolio(portfolio_id)
    if portfolio is None or not summary:
        return {}
    positions = get_paper_positions(portfolio_id, status="Open")
    snapshots = get_daily_snapshots(portfolio_id)
    equity = float(summary.get("current_cash") or 0.0) + float(summary.get("total_market_value") or 0.0)
    max_equity = max([float(row.get("equity") or 0.0) for row in snapshots] + [equity])
    drawdown_pct = ((equity - max_equity) / max_equity * 100.0) if max_equity > 0 else 0.0
    regime_exposure = {"Bull": 0.0, "Neutral": 0.0, "Bear": 0.0}
    if positions:
        try:
            from src.app.routes.regime_cache import load_payload

            cached = load_payload() or {}
            rows = cached.get("rows") if isinstance(cached, dict) else []
            regime_by_ticker = {
                str(row.get("ticker") or "").upper(): str(row.get("regime") or "")
                for row in (rows or [])
                if isinstance(row, dict)
            }
        except Exception:
            regime_by_ticker = {}
        total_market_value = float(summary.get("total_market_value") or 0.0)
        if total_market_value > 0 and summary.get("positions"):
            for row in summary.get("positions") or []:
                ticker = str(row.get("ticker") or "").upper()
                label = regime_by_ticker.get(ticker)
                if label in regime_exposure:
                    regime_exposure[label] += float(row.get("market_value") or 0.0) / total_market_value
    return {
        "snapshot_date": dt.datetime.now(ET).date().isoformat(),
        "portfolio_id": int(portfolio_id),
        "equity": equity,
        "cash": float(summary.get("current_cash") or 0.0),
        "market_value": float(summary.get("total_market_value") or 0.0),
        "realized_pnl": float(summary.get("realized_pnl") or 0.0),
        "unrealized_pnl": float(summary.get("unrealized_pnl") or 0.0),
        "position_count": len(positions),
        "trades_today": count_todays_trades(portfolio_id),
        "drawdown_pct": drawdown_pct,
        "regime_exposure_json": json.dumps(regime_exposure),
    }


def _plan_has_llm_attribution(plan: dict[str, Any]) -> bool:
    return _truthy(plan.get("llm_used")) or any(
        str(plan.get(key) or "").strip()
        for key in ("llm_verdict", "llm_provider", "llm_model", "llm_model_display", "llm_influence")
    )


def _entry_plan_for_position(portfolio_id: int, position: dict[str, Any]) -> dict[str, Any] | None:
    ticker = str(position.get("ticker") or "").upper()
    if not ticker:
        return None
    entry_date = _parse_timestamp(position.get("entry_date"))
    entry_price = _positive_float(position.get("entry_price"))
    quantity = _positive_float(position.get("quantity"))
    candidates: list[tuple[float, dict[str, Any]]] = []
    for plan in get_trade_plans(portfolio_id, status="all"):
        if str(plan.get("ticker") or "").upper() != ticker:
            continue
        if str(plan.get("action") or "") != "Buy":
            continue
        if str(plan.get("status") or "") != "Executed":
            continue
        if not _plan_has_llm_attribution(plan):
            continue
        executed_at = _parse_timestamp(plan.get("executed_at"))
        if entry_date is not None and executed_at is not None and executed_at > entry_date + dt.timedelta(days=1):
            continue
        plan_price = _positive_float(plan.get("execution_price")) or _positive_float(plan.get("proposed_price"))
        plan_quantity = _positive_float(plan.get("filled_quantity")) or _positive_float(plan.get("quantity"))
        price_diff = abs(float(plan_price or 0.0) - float(entry_price or 0.0))
        quantity_diff = abs(float(plan_quantity or 0.0) - float(quantity or 0.0))
        time_diff = 0.0
        if entry_date is not None and executed_at is not None:
            time_diff = abs((entry_date - executed_at).total_seconds()) / 86400.0
        candidates.append((price_diff + quantity_diff + time_diff, plan))
    if not candidates:
        return None
    candidates.sort(key=lambda item: item[0])
    return candidates[0][1]


def _record_llm_outcome_attribution(
    portfolio_id: int,
    position: dict[str, Any],
    close_price: float,
    *,
    return_pct: float,
    holding_days: int,
) -> None:
    plan = _entry_plan_for_position(portfolio_id, position)
    if plan is None:
        return
    ticker = str(position.get("ticker") or "").upper()
    quantity = float(position.get("quantity") or 0.0)
    entry_price = float(position.get("entry_price") or 0.0)
    side = str(position.get("side") or "long")
    realized_pnl = position.get("realized_pnl")
    if realized_pnl in (None, ""):
        realized_pnl = (float(close_price) - entry_price) * quantity if side == "long" else (entry_price - float(close_price)) * quantity
    position_id = str(position.get("id") or f"{ticker}-{position.get('entry_date') or ''}")
    order_id = f"llm-attribution-position-{position_id}"
    if get_audit_trail(portfolio_id=portfolio_id, order_id=order_id, event_type="llm_attribution", days=3650, limit=1):
        return
    payload = {
        "position_id": position.get("id"),
        "plan_id": plan.get("id"),
        "ticker": ticker,
        "verdict": str(plan.get("llm_verdict") or "unknown"),
        "confidence": float(plan.get("llm_confidence")) if plan.get("llm_confidence") not in (None, "") else None,
        "influence": str(plan.get("llm_influence") or ""),
        "provider": str(plan.get("llm_provider") or ""),
        "model": str(plan.get("llm_model_display") or plan.get("llm_model") or ""),
        "realized_net_pnl": float(realized_pnl or 0.0),
        "return_pct": float(return_pct),
        "holding_days": int(holding_days),
    }
    log_audit_event(
        order_id=order_id,
        portfolio_id=portfolio_id,
        event_type="llm_attribution",
        ticker=ticker,
        action="outcome",
        quantity=quantity,
        price=float(close_price),
        actor="system",
        details=json.dumps(payload, sort_keys=True),
    )


def record_trade_outcome(portfolio_id: int, position: dict[str, Any], close_price: float) -> dict[str, Any]:
    entry_price = float(position.get("entry_price") or 0.0)
    exit_date = _parse_timestamp(position.get("exit_date")) or _now()
    entry_date = _parse_timestamp(position.get("entry_date")) or exit_date
    return_pct = ((float(close_price) - entry_price) / entry_price) if entry_price > 0 else 0.0
    holding_days = max(0, (exit_date - entry_date).days)
    _record_llm_outcome_attribution(
        portfolio_id,
        position,
        close_price,
        return_pct=return_pct,
        holding_days=holding_days,
    )
    return {
        "ticker": str(position.get("ticker") or "").upper(),
        "return_pct": return_pct,
        "holding_days": holding_days,
        "outcome": "win" if return_pct > 0 else "loss",
    }
