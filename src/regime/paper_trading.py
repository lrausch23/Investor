from __future__ import annotations

import datetime as dt
import json
import logging
import math
import uuid
from dataclasses import asdict
from typing import Any

import pandas as pd

from .broker_adapter import BrokerAdapter, OrderRequest, PaperBrokerAdapter, submit_guarded_order, validate_guardrails
from .config import (
    DEFAULT_PAPER_TRADING_CONFIG,
    DEFAULT_RISK_GUARDRAILS,
    PaperTradingConfig,
    RiskGuardrails,
)
from .discovery import _quick_regime_screen
from .fundamental_data import fetch_financial_statements
from .hurdle_rate import check_duration_gate, check_hurdle_rate, get_hurdle_settings
from .market_data_client import download_daily_bars, get_ticker_info
from .persistence import (
    close_paper_position,
    count_todays_trades,
    create_trade_plan,
    get_auto_approve_threshold,
    get_daily_capital_ceiling_pct,
    get_daily_capital_deployed,
    get_daily_snapshots,
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
    update_trade_plan_status,
)
from .ib_types import ET

logger = logging.getLogger(__name__)

CachedRegimeValue = tuple[str, float] | dict[str, Any]
CachedRegimeMap = dict[str, CachedRegimeValue]
DEFAULT_SIZING_METHOD = "risk_budget"
DEFAULT_SIZING_BASE_RISK_FRACTION = 0.02
DEFAULT_SIZING_ATR_MULTIPLIER = 2.0


def _normalize_close_series(frame: pd.DataFrame | None) -> pd.Series:
    if frame is None or frame.empty or "Close" not in frame.columns:
        return pd.Series(dtype=float)
    close = frame["Close"]
    if getattr(close, "ndim", 1) > 1:
        close = close.iloc[:, 0]
    return pd.to_numeric(close, errors="coerce").dropna()


def compute_theme_budget(
    total_budget: float,
    conviction: int,
    config: PaperTradingConfig = DEFAULT_PAPER_TRADING_CONFIG,
) -> float:
    conviction_index = max(0, min(int(conviction), len(config.conviction_allocation) - 1))
    return float(total_budget) * float(config.conviction_allocation[conviction_index])


def compute_position_budget(
    theme_budget: float,
    role: str,
    total_budget: float,
    config: PaperTradingConfig = DEFAULT_PAPER_TRADING_CONFIG,
) -> float:
    normalized_role = str(role or "Critical-Path")
    if normalized_role == "Core":
        budget = float(theme_budget) * float(config.core_max_pct)
    elif normalized_role == "Speculative":
        budget = float(theme_budget) * float(config.speculative_max_pct)
        budget = min(budget, float(total_budget) * float(config.speculative_absolute_cap_pct))
    else:
        budget = float(theme_budget) * float(config.critical_path_max_pct)
    return max(0.0, budget)


def allocate_budget(
    portfolio_id: int,
    themes: list[dict[str, Any]] | None = None,
    config: PaperTradingConfig = DEFAULT_PAPER_TRADING_CONFIG,
) -> dict[str, Any]:
    portfolio = get_paper_portfolio(portfolio_id)
    if portfolio is None:
        return {}
    total_budget = float(portfolio.get("starting_budget") or config.default_budget)
    cash_reserve = total_budget * float(config.min_cash_reserve_pct)
    allocatable = max(0.0, total_budget - cash_reserve)
    active_themes = themes if themes is not None else [
        theme for theme in list_themes(include_closed=False) if str(theme.get("status") or "") == "Active"
    ]

    theme_rows: list[dict[str, Any]] = []
    total_requested = 0.0
    for theme in active_themes:
        conviction = int(theme.get("conviction") or 0)
        allocated = compute_theme_budget(total_budget, conviction, config=config)
        total_requested += allocated
        theme_rows.append(
            {
                "theme_id": int(theme.get("id") or 0),
                "theme_name": str(theme.get("name") or ""),
                "conviction": conviction,
                "allocated": allocated,
                "by_role": {
                    "Core": compute_position_budget(allocated, "Core", total_budget, config=config),
                    "Critical-Path": compute_position_budget(allocated, "Critical-Path", total_budget, config=config),
                    "Speculative": compute_position_budget(allocated, "Speculative", total_budget, config=config),
                },
            }
        )

    scale = (allocatable / total_requested) if total_requested > allocatable and total_requested > 0 else 1.0
    if scale != 1.0:
        for theme_row in theme_rows:
            theme_row["allocated"] = float(theme_row["allocated"]) * scale
            theme_row["by_role"] = {
                key: float(value) * scale
                for key, value in theme_row["by_role"].items()
            }

    allocated_total = sum(float(theme_row["allocated"]) for theme_row in theme_rows)
    return {
        "total_budget": total_budget,
        "cash_reserve": cash_reserve,
        "allocatable": allocatable,
        "themes": theme_rows,
        "unallocated": max(0.0, allocatable - allocated_total),
    }


def _now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _theme_map() -> dict[int, dict[str, Any]]:
    return {int(theme.get("id") or 0): theme for theme in list_themes(include_closed=False)}


def _parse_timestamp(raw: Any) -> dt.datetime | None:
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        return dt.datetime.fromisoformat(text)
    except ValueError:
        return None


def _cached_regime_map(cached_regime: CachedRegimeMap | dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    if not isinstance(cached_regime, dict):
        return {}
    if "rows" in cached_regime:
        rows = cached_regime.get("rows") or []
        return {
            str(row.get("ticker") or "").upper(): row
            for row in rows
            if isinstance(row, dict) and str(row.get("ticker") or "").strip()
        }
    mapped: dict[str, dict[str, Any]] = {}
    for ticker, value in cached_regime.items():
        symbol = str(ticker or "").strip().upper()
        if not symbol:
            continue
        if isinstance(value, dict):
            mapped[symbol] = value
        elif isinstance(value, (tuple, list)) and len(value) >= 2:
            mapped[symbol] = {"regime": value[0], "probability": value[1]}
    return mapped


def _batch_current_prices(tickers: list[str]) -> dict[str, float]:
    normalized = []
    seen: set[str] = set()
    for ticker in tickers:
        symbol = str(ticker or "").strip().upper()
        if symbol and symbol not in seen:
            seen.add(symbol)
            normalized.append(symbol)
    if not normalized:
        return {}
    try:
        frame = download_daily_bars(
            normalized if len(normalized) > 1 else normalized[0],
            period="5d",
            auto_adjust=False,
            group_by="column",
        )
    except Exception as exc:
        logger.warning("Batch price download failed for paper trading.", exc_info=exc)
        frame = None
    prices: dict[str, float] = {}
    if frame is not None and not getattr(frame, "empty", True):
        try:
            if isinstance(frame.columns, pd.MultiIndex):
                close_frame = frame["Close"] if "Close" in frame.columns.get_level_values(0) else None
                if close_frame is not None:
                    for ticker in normalized:
                        series = close_frame.get(ticker)
                        if series is None:
                            continue
                        cleaned = series.dropna()
                        if not cleaned.empty:
                            prices[ticker] = float(cleaned.iloc[-1])
            else:
                close_series = frame["Close"] if "Close" in frame.columns else None
                if close_series is not None:
                    cleaned = close_series.dropna()
                    if not cleaned.empty:
                        prices[normalized[0]] = float(cleaned.iloc[-1])
        except Exception as exc:
            logger.warning("Unable to parse batch prices for paper trading.", exc_info=exc)
    missing = [ticker for ticker in normalized if ticker not in prices]
    for ticker in missing:
        try:
            history = download_daily_bars(ticker, period="5d", auto_adjust=False)
            if history is not None and not history.empty and "Close" in history.columns:
                cleaned = history["Close"].dropna()
                if not cleaned.empty:
                    prices[ticker] = float(cleaned.iloc[-1])
                    continue
        except Exception as exc:
            logger.debug("Ticker history fallback failed for %s.", ticker, exc_info=exc)
        try:
            info_price = get_ticker_info(ticker).get("currentPrice")
            if info_price is not None:
                prices[ticker] = float(info_price)
        except Exception as exc:
            logger.debug("Ticker info fallback failed for %s.", ticker, exc_info=exc)
    return prices


def _pending_plan_index(portfolio_id: int, action: str) -> set[str]:
    return {
        str(plan.get("ticker") or "").upper()
        for plan in get_trade_plans(portfolio_id, status="Pending")
        if str(plan.get("action") or "") == action
    }


def _open_position_index(portfolio_id: int) -> dict[str, list[dict[str, Any]]]:
    by_ticker: dict[str, list[dict[str, Any]]] = {}
    for row in get_paper_positions(portfolio_id, status="Open"):
        by_ticker.setdefault(str(row.get("ticker") or "").upper(), []).append(row)
    return by_ticker


def get_sizing_settings() -> dict[str, Any]:
    raw_method = str(get_setting("sizing_method") or DEFAULT_SIZING_METHOD).strip().lower()
    method = raw_method if raw_method in {"risk_budget", "equal_dollar"} else DEFAULT_SIZING_METHOD
    try:
        base_risk_fraction = float(get_setting("sizing_base_risk_fraction") or DEFAULT_SIZING_BASE_RISK_FRACTION)
    except Exception:
        base_risk_fraction = DEFAULT_SIZING_BASE_RISK_FRACTION
    try:
        atr_multiplier = float(get_setting("sizing_atr_multiplier") or DEFAULT_SIZING_ATR_MULTIPLIER)
    except Exception:
        atr_multiplier = DEFAULT_SIZING_ATR_MULTIPLIER
    return {
        "sizing_method": method,
        "sizing_base_risk_fraction": max(0.001, min(base_risk_fraction, 0.25)),
        "sizing_atr_multiplier": max(0.5, min(atr_multiplier, 5.0)),
    }


def _lookup_atr(ticker: str) -> float | None:
    snapshot = get_latest_signal_snapshot(str(ticker or "").upper(), max_age_days=7)
    if not snapshot:
        return None
    atr = snapshot.get("atr_14")
    try:
        return float(atr) if atr is not None else None
    except Exception:
        return None


def _lookup_beta(ticker: str) -> float | None:
    try:
        info = fetch_financial_statements(str(ticker or "").upper()).info or {}
    except Exception:
        info = {}
    beta = info.get("beta")
    try:
        return float(beta) if beta is not None else None
    except Exception:
        return None


def _risk_adjusted_quantity(
    role_budget: float,
    proposed_price: float,
    atr_14: float | None,
    beta: float | None,
    *,
    risk_per_share_multiplier: float = DEFAULT_SIZING_ATR_MULTIPLIER,
    base_risk_fraction: float = DEFAULT_SIZING_BASE_RISK_FRACTION,
) -> int:
    max_shares_by_capital = math.floor(float(role_budget) / float(proposed_price)) if proposed_price > 0 else 0
    if max_shares_by_capital <= 0:
        return 0
    if atr_14 is None or atr_14 <= 0:
        return max_shares_by_capital
    effective_beta = max(float(beta or 1.0), 0.3)
    effective_risk_fraction = float(base_risk_fraction) / effective_beta
    risk_per_share = float(atr_14) * float(risk_per_share_multiplier)
    if risk_per_share <= 0:
        return max_shares_by_capital
    max_shares_by_risk = math.floor((float(role_budget) * effective_risk_fraction) / risk_per_share)
    return max(0, min(max_shares_by_capital, max_shares_by_risk))


def generate_buy_plans(
    portfolio_id: int,
    *,
    config: PaperTradingConfig = DEFAULT_PAPER_TRADING_CONFIG,
) -> list[dict[str, Any]]:
    from .vix_freeze import is_vix_frozen

    portfolio = get_paper_portfolio(portfolio_id)
    if portfolio is None:
        return []
    if is_vix_frozen():
        logger.warning("VIX freeze active — skipping buy plan generation")
        save_alert(
            "vix_freeze",
            "Buy plan generation skipped by VIX freeze",
            severity="critical",
            portfolio_id=portfolio_id,
            message="VIX freeze is active. New Buy plans were not generated.",
        )
        return []
    allocation = allocate_budget(portfolio_id, config=config)
    sizing_settings = get_sizing_settings()
    hurdle_settings = get_hurdle_settings()
    sizing_method = str(sizing_settings.get("sizing_method") or DEFAULT_SIZING_METHOD)
    base_risk_fraction = float(sizing_settings.get("sizing_base_risk_fraction") or DEFAULT_SIZING_BASE_RISK_FRACTION)
    atr_multiplier = float(sizing_settings.get("sizing_atr_multiplier") or DEFAULT_SIZING_ATR_MULTIPLIER)
    theme_budgets = {int(item["theme_id"]): item for item in allocation.get("themes", [])}
    pending_buys = _pending_plan_index(portfolio_id, "Buy")
    open_positions = _open_position_index(portfolio_id)
    planned_keys: set[tuple[str, int]] = set()
    created: list[dict[str, Any]] = []
    for item in get_watchlist(status=["Entry Signal", "Added"]):
        ticker = str(item.get("ticker") or "").upper()
        theme_id = int(item.get("theme_id") or 0)
        key = (ticker, theme_id)
        if not ticker or ticker in pending_buys or ticker in open_positions or key in planned_keys:
            continue
        theme_budget = theme_budgets.get(theme_id)
        if not theme_budget:
            continue
        role = str(item.get("suggested_role") or "Critical-Path")
        role_budget = float((theme_budget.get("by_role") or {}).get(role) or 0.0)
        proposed_price = float(item.get("suggested_entry_price") or 0.0)
        if role_budget <= 0 or proposed_price <= 0:
            continue
        snapshot = get_latest_signal_snapshot(ticker, max_age_days=7) or {}
        exit_price = item.get("suggested_exit_price")
        if exit_price in (None, ""):
            exit_price = snapshot.get("exit_price")
        hurdle_result = None
        duration_result = None
        if bool(hurdle_settings.get("hurdle_enabled", True)):
            hurdle_result = check_hurdle_rate(ticker, proposed_price, float(exit_price) if exit_price not in (None, "") else None)
            if not hurdle_result.passed:
                logger.info("Hurdle gate blocked %s: %s", ticker, hurdle_result.reason)
                continue
        if bool(hurdle_settings.get("duration_gate_enabled", True)):
            expected_duration = snapshot.get("expected_regime_duration")
            if expected_duration in (None, "") and snapshot.get("timeframe_days") not in (None, ""):
                expected_duration = snapshot.get("timeframe_days")
            regime_label = str(item.get("regime_label") or snapshot.get("regime_label") or "")
            duration_result = check_duration_gate(
                ticker,
                float(expected_duration) if expected_duration not in (None, "") else None,
                regime_label,
            )
            if not duration_result.passed:
                logger.info("Duration gate blocked %s: %s", ticker, duration_result.reason)
                continue
        atr_14 = _lookup_atr(ticker)
        beta = _lookup_beta(ticker)
        if sizing_method == "risk_budget":
            quantity = _risk_adjusted_quantity(
                role_budget,
                proposed_price,
                atr_14,
                beta,
                risk_per_share_multiplier=atr_multiplier,
                base_risk_fraction=base_risk_fraction,
            )
        else:
            quantity = math.floor(role_budget / proposed_price)
        if quantity <= 0:
            continue
        planned_keys.add(key)
        if sizing_method == "risk_budget" and atr_14 is not None and atr_14 > 0:
            risk_per_share = float(atr_14) * atr_multiplier
            rationale = (
                f"Entry Signal from discovery watchlist. "
                f"Risk-sized: ATR={atr_14:.2f}, beta={float(beta or 1.0):.2f}, "
                f"risk/share=${risk_per_share:.2f}. "
                f"{item.get('discovery_rationale') or 'Candidate meets paper-trading entry criteria.'}"
            )
        else:
            rationale = (
                f"Entry Signal from discovery watchlist. "
                f"{item.get('discovery_rationale') or 'Candidate meets paper-trading entry criteria.'}"
            )
        if hurdle_result and hurdle_result.net_return_pct is not None and hurdle_result.gross_return_pct is not None:
            rationale = (
                f"{rationale} Hurdle: {hurdle_result.net_return_pct:.2f}% net "
                f"({hurdle_result.gross_return_pct:.2f}% gross @ {hurdle_result.estimated_stcg_rate:.0%} tax)."
            )
        if duration_result and duration_result.expected_regime_duration is not None:
            rationale = (
                f"{rationale} Duration: {duration_result.expected_regime_duration:.1f}d "
                f"(min {duration_result.min_regime_duration_days:.1f}d)."
            )
        created.append(
            create_trade_plan(
                portfolio_id,
                ticker,
                "Buy",
                quantity,
                rationale,
                theme_id=theme_id or None,
                proposed_price=proposed_price,
                regime_label=str(item.get("regime_label") or ""),
                regime_probability=float(item.get("regime_probability") or 0.0) if item.get("regime_probability") is not None else None,
                crowd_score=int(item.get("crowd_score")) if item.get("crowd_score") is not None else None,
                source="discovery",
                meta_labeler_score=float(item.get("meta_labeler_probability")) if item.get("meta_labeler_probability") is not None else None,
                sizing_method="risk_budget" if sizing_method == "risk_budget" and atr_14 is not None and atr_14 > 0 else "equal_dollar",
                hurdle_gross_return_pct=hurdle_result.gross_return_pct if hurdle_result else None,
                hurdle_net_return_pct=hurdle_result.net_return_pct if hurdle_result else None,
                hurdle_passed=hurdle_result.passed if hurdle_result else None,
                duration_gate_passed=duration_result.passed if duration_result else None,
                expected_regime_duration=duration_result.expected_regime_duration if duration_result else None,
            )
        )
    return created


def generate_holdings_plans(
    portfolio_id: int,
    *,
    cached_payload: dict[str, Any] | None = None,
    config: PaperTradingConfig = DEFAULT_PAPER_TRADING_CONFIG,
) -> list[dict[str, Any]]:
    if not cached_payload:
        return []
    rows = cached_payload.get("rows", [])
    if not isinstance(rows, list) or not rows:
        return []
    portfolio = get_paper_portfolio(portfolio_id)
    if portfolio is None:
        return []
    pending_buy_keys = {
        (str(plan.get("ticker") or "").upper(), int(plan["theme_id"]) if plan.get("theme_id") is not None else None)
        for plan in get_trade_plans(portfolio_id, status="Pending")
        if str(plan.get("action") or "") == "Buy"
    }
    open_positions = _open_position_index(portfolio_id)
    seen: set[tuple[str, int | None]] = set()
    created: list[dict[str, Any]] = []
    cash = float(portfolio.get("current_cash") or portfolio.get("cash_balance") or 0.0)
    max_per_trade = float(getattr(config, "max_single_order_value", 10000.0) or 10000.0)
    role_budget = min(cash * 0.10, max_per_trade)
    if role_budget <= 0:
        return []

    for row in rows:
        if not isinstance(row, dict):
            continue
        ticker = str(row.get("ticker") or "").strip().upper()
        if not ticker or not row.get("is_portfolio_holding"):
            continue
        ai_verdict = str(row.get("ai_verdict") or "").strip()
        action = str(row.get("action") or "").strip()
        composite = str(row.get("composite_signal") or "").strip()
        is_actionable = (
            ai_verdict.lower() == "entry"
            or action.lower() in {"buy", "strong buy"}
            or composite.lower() in {"buy", "strong buy"}
        )
        if not is_actionable:
            continue
        theme_membership = row.get("theme_membership") if isinstance(row.get("theme_membership"), list) else []
        theme_entry = theme_membership[0] if theme_membership else None
        theme_id = None
        if isinstance(theme_entry, dict) and theme_entry.get("theme_id") is not None:
            theme_id = int(theme_entry["theme_id"])
        key = (ticker, theme_id)
        if key in pending_buy_keys or ticker in open_positions:
            continue
        if key in seen:
            continue
        price_targets = row.get("price_targets") if isinstance(row.get("price_targets"), dict) else {}
        proposed_price = float(price_targets.get("entry_price") or 0.0) or float(row.get("current_price") or 0.0)
        if proposed_price <= 0:
            continue
        quantity = math.floor(role_budget / proposed_price)
        if quantity <= 0:
            continue
        seen.add(key)
        parts: list[str] = []
        if ai_verdict:
            parts.append(f"AI verdict: {ai_verdict}")
        if composite:
            parts.append(f"Composite: {composite}")
        ml_prob = row.get("meta_labeler_probability")
        if ml_prob is not None:
            try:
                parts.append(f"ML confidence: {float(ml_prob):.0%}")
            except Exception:
                pass
        rationale = f"Holdings bridge — {'; '.join(parts)}" if parts else "Holdings bridge plan"
        created.append(
            create_trade_plan(
                portfolio_id,
                ticker,
                "Buy",
                quantity,
                rationale,
                theme_id=theme_id,
                proposed_price=proposed_price,
                regime_label=str(row.get("regime") or ""),
                regime_probability=float(row.get("probability") or 0.0) if row.get("probability") is not None else None,
                source="holdings",
                meta_labeler_score=float(ml_prob) if ml_prob is not None else None,
            )
        )
        pending_buy_keys.add(key)
    return created


def generate_exit_plans(
    portfolio_id: int,
    *,
    cached_regime: CachedRegimeMap | dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    portfolio = get_paper_portfolio(portfolio_id)
    if portfolio is None:
        return []
    pending_sells = _pending_plan_index(portfolio_id, "Sell")
    open_positions = get_paper_positions(portfolio_id, status="Open")
    if not open_positions:
        return []
    prices = _batch_current_prices([str(row.get("ticker") or "") for row in open_positions])
    cached_rows = _cached_regime_map(cached_regime)
    created: list[dict[str, Any]] = []
    for position in open_positions:
        ticker = str(position.get("ticker") or "").upper()
        if not ticker or ticker in pending_sells:
            continue
        current_price = prices.get(ticker)
        stop_price = float(position.get("stop_price") or 0.0)
        trigger_reason: str | None = None
        regime_label: str | None = None
        regime_probability: float | None = None
        if current_price is not None and stop_price > 0 and current_price <= stop_price:
            trigger_reason = f"Stop price hit (${current_price:.2f} <= ${stop_price:.2f})."
        row = cached_rows.get(ticker)
        if row:
            regime_label = str(row.get("regime") or "").strip() or None
            try:
                regime_probability = float(row.get("probability")) if row.get("probability") is not None else None
            except Exception:
                regime_probability = None
            action = str(row.get("composite_signal") or "").strip()
            if trigger_reason is None and regime_label == "Bear":
                trigger_reason = "Cached regime is Bear."
            if trigger_reason is None and action in {"Sell", "Strong Sell"}:
                trigger_reason = f"Cached composite signal is {action}."
        if trigger_reason is None:
            try:
                quick_label, quick_prob, _entry, _stop = _quick_regime_screen(ticker)
                regime_label = quick_label
                regime_probability = quick_prob
                if quick_label == "Bear":
                    trigger_reason = "Fallback regime screen flipped to Bear."
            except Exception as exc:
                logger.warning("Fallback regime screen failed for paper exit plan %s.", ticker, exc_info=exc)
        if trigger_reason is None:
            continue
        quantity = float(position.get("quantity") or 0.0)
        if quantity <= 0:
            continue
        proposed_price = current_price or float(position.get("entry_price") or 0.0)
        created.append(
            create_trade_plan(
                portfolio_id,
                ticker,
                "Sell",
                quantity,
                trigger_reason,
                theme_id=int(position["theme_id"]) if position.get("theme_id") is not None else None,
                proposed_price=proposed_price if proposed_price > 0 else None,
                regime_label=regime_label,
                regime_probability=regime_probability,
                source="exit_signal",
            )
        )
    return created


def generate_daily_plans(
    portfolio_id: int,
    *,
    cached_regime: CachedRegimeMap | dict[str, Any] | None = None,
    cached_payload: dict[str, Any] | None = None,
    config: PaperTradingConfig = DEFAULT_PAPER_TRADING_CONFIG,
) -> dict[str, Any]:
    buy_plans = generate_buy_plans(portfolio_id, config=config)
    holdings_plans = generate_holdings_plans(portfolio_id, cached_payload=cached_payload, config=config)
    exit_plans = generate_exit_plans(portfolio_id, cached_regime=cached_regime)
    return {
        "buy_plans": buy_plans,
        "holdings_plans": holdings_plans,
        "exit_plans": exit_plans,
        "created_count": len(buy_plans) + len(holdings_plans) + len(exit_plans),
        "generated_at": _now().isoformat(),
    }


def execute_approved_plans(portfolio_id: int) -> dict[str, Any]:
    adapter = PaperBrokerAdapter(portfolio_id)
    return execute_approved_plans_via_adapter(portfolio_id, adapter)


def auto_approve_plans(portfolio_id: int) -> dict[str, Any]:
    from .vix_freeze import is_vix_frozen

    mode = get_operating_mode()
    if mode == "manual":
        return {
            "mode": mode,
            "threshold": get_auto_approve_threshold(),
            "ceiling_pct": get_daily_capital_ceiling_pct(),
            "max_daily_capital": 0.0,
            "capital_deployed_before": 0.0,
            "capital_deployed_after": 0.0,
            "approved": 0,
            "skipped": 0,
            "blocked": 0,
            "details": [],
        }
    portfolio = get_paper_portfolio(portfolio_id)
    if portfolio is None:
        return {
            "mode": mode,
            "threshold": get_auto_approve_threshold(),
            "ceiling_pct": get_daily_capital_ceiling_pct(),
            "max_daily_capital": 0.0,
            "capital_deployed_before": 0.0,
            "capital_deployed_after": 0.0,
            "approved": 0,
            "skipped": 0,
            "blocked": 0,
            "details": [],
        }
    pending = get_trade_plans(portfolio_id, status="Pending")
    threshold = get_auto_approve_threshold()
    ceiling_pct = get_daily_capital_ceiling_pct()
    summary = get_paper_portfolio_summary(portfolio_id)
    equity = float(summary.get("total_equity") or 0.0)
    if equity <= 0:
        equity = float(summary.get("current_cash") or 0.0) + float(summary.get("total_market_value") or 0.0)
    if equity <= 0:
        equity = float(summary.get("current_value") or portfolio.get("starting_budget") or 0.0)
    max_daily_capital = equity * ceiling_pct
    deployed_before = get_daily_capital_deployed(portfolio_id)
    deployed = deployed_before
    adapter = PaperBrokerAdapter(portfolio_id)
    approved = 0
    skipped = 0
    blocked = 0
    details: list[dict[str, Any]] = []
    now_text = _now().isoformat()

    for plan in pending:
        plan_id = int(plan["id"])
        action = str(plan.get("action") or "")
        ticker = str(plan.get("ticker") or "").upper()
        quantity = float(plan.get("quantity") or 0.0)
        proposed_price = float(plan.get("proposed_price") or 0.0) or None
        score = float(plan.get("meta_labeler_score")) if plan.get("meta_labeler_score") is not None else None
        order_value = (quantity * proposed_price) if proposed_price and quantity > 0 else None
        if action == "Buy":
            if max_daily_capital > 0 and order_value is not None and deployed + order_value > max_daily_capital:
                blocked += 1
                details.append({"plan_id": plan_id, "ticker": ticker, "action": action, "result": "blocked_ceiling", "meta_labeler_score": score, "order_value": order_value})
                continue
            if is_vix_frozen():
                blocked += 1
                details.append({"plan_id": plan_id, "ticker": ticker, "action": action, "result": "blocked_vix_freeze", "meta_labeler_score": score, "order_value": order_value})
                continue
            if mode == "semi_auto":
                if score is None:
                    skipped += 1
                    details.append({"plan_id": plan_id, "ticker": ticker, "action": action, "result": "skipped_no_score", "meta_labeler_score": score, "order_value": order_value})
                    continue
                if score < threshold:
                    save_alert(
                        "meta_labeler_veto",
                        f"ML veto: {ticker} (score {score:.0%})",
                        severity="info",
                        ticker=ticker,
                        portfolio_id=portfolio_id,
                        message=f"Plan skipped — ML confidence {score:.0%} below threshold {threshold:.0%}.",
                        data={"plan_id": plan_id, "score": score, "threshold": threshold},
                    )
                    skipped += 1
                    details.append({"plan_id": plan_id, "ticker": ticker, "action": action, "result": "skipped_ml", "meta_labeler_score": score, "order_value": order_value})
                    continue
        order = OrderRequest(
            portfolio_id=portfolio_id,
            ticker=ticker,
            action=action,
            quantity=quantity,
            limit_price=proposed_price,
            theme_id=int(plan["theme_id"]) if plan.get("theme_id") is not None else None,
            source=str(plan.get("source") or "manual"),
            notes=str(plan.get("rationale") or ""),
        )
        guardrail = validate_guardrails(order, adapter, guardrails=DEFAULT_RISK_GUARDRAILS)
        if not guardrail.allowed:
            blocked += 1
            details.append({"plan_id": plan_id, "ticker": ticker, "action": action, "result": "blocked_guardrail", "meta_labeler_score": score, "order_value": order_value})
            continue
        note = f"Auto-approved ({mode})"
        if action == "Sell":
            note = f"Auto-approved exit (mode={mode})"
        elif score is not None:
            note = f"Auto-approved (mode={mode}, ML={score:.0%})"
        update_trade_plan_status(plan_id, "Approved", reviewed_at=now_text, notes=note)
        log_audit_event(
            order_id=str(uuid.uuid4()),
            portfolio_id=portfolio_id,
            event_type="auto_approved",
            ticker=ticker,
            action=action,
            quantity=quantity,
            price=proposed_price,
            actor="system",
            details=f"Auto-approved in {mode} mode. ML: {score}, Ceiling: {deployed}/{max_daily_capital}",
        )
        approved += 1
        details.append({"plan_id": plan_id, "ticker": ticker, "action": action, "result": "approved", "meta_labeler_score": score, "order_value": order_value})
        if action == "Buy" and order_value is not None:
            deployed += order_value
    return {
        "mode": mode,
        "threshold": threshold,
        "ceiling_pct": ceiling_pct,
        "max_daily_capital": max_daily_capital,
        "capital_deployed_before": deployed_before,
        "capital_deployed_after": deployed,
        "approved": approved,
        "skipped": skipped,
        "blocked": blocked,
        "details": details,
    }


def auto_execute_approved(
    portfolio_id: int,
    adapter: BrokerAdapter,
    guardrails: RiskGuardrails = DEFAULT_RISK_GUARDRAILS,
    *,
    actor: str = "system",
) -> dict[str, Any]:
    return execute_approved_plans_via_adapter(
        portfolio_id,
        adapter,
        guardrails=guardrails,
        actor=actor,
    )


def execute_approved_plans_via_adapter(
    portfolio_id: int,
    adapter: BrokerAdapter,
    *,
    guardrails: RiskGuardrails = DEFAULT_RISK_GUARDRAILS,
    actor: str = "user",
) -> dict[str, Any]:
    portfolio = get_paper_portfolio(portfolio_id)
    if portfolio is None:
        return {"executed": [], "skipped": [], "portfolio": None}
    approved = get_trade_plans(portfolio_id, status="Approved")
    if not approved:
        return {"executed": [], "skipped": [], "portfolio": portfolio}
    executed: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for plan in approved:
        plan_id = int(plan["id"])
        ticker = str(plan.get("ticker") or "").upper()
        action = str(plan.get("action") or "")
        quantity = float(plan.get("quantity") or 0.0)
        related_watchlist = get_watchlist_by_ticker(ticker)
        stop_price = None
        role = str(plan.get("role") or "Critical-Path")
        if related_watchlist:
            latest = related_watchlist[0]
            stop_price = latest.get("suggested_stop_price")
            role = str(latest.get("suggested_role") or role)
        order = OrderRequest(
            portfolio_id=portfolio_id,
            ticker=ticker,
            action=action,
            quantity=quantity,
            limit_price=float(plan.get("proposed_price") or 0.0) or None,
            stop_price=float(stop_price) if stop_price is not None else None,
            theme_id=int(plan["theme_id"]) if plan.get("theme_id") is not None else None,
            role=role,
            source=str(plan.get("source") or "manual"),
            notes=str(plan.get("rationale") or ""),
        )
        guardrail_result, result = submit_guarded_order(order, adapter, guardrails=guardrails, actor=actor)
        if result is None:
            note = "; ".join(check.message for check in guardrail_result.checks if not check.passed) or "Blocked by guardrails."
            update_trade_plan_status(plan_id, "Rejected", notes=note, reviewed_at=_now().isoformat())
            skipped.append(
                {
                    "plan_id": plan_id,
                    "ticker": ticker,
                    "reason": note,
                    "status": "guardrail_blocked",
                    "guardrail_result": asdict(guardrail_result),
                }
            )
            continue
        normalized_status = str(result.status or "").lower()
        if normalized_status in {"submitted", "pending", "partially_filled"}:
            mapped_status = "Partially Filled" if normalized_status == "partially_filled" else "Submitted"
            updated_plan = update_trade_plan_status(
                plan_id,
                mapped_status,
                reviewed_at=plan.get("reviewed_at") or _now().isoformat(),
                broker_order_id=result.order_id,
                broker_status=result.status,
                filled_quantity=float(result.quantity or 0.0) if normalized_status == "partially_filled" else 0.0,
                notes=result.message or "",
            )
            executed.append(
                {
                    "plan_id": plan_id,
                    "ticker": ticker,
                    "action": action,
                    "execution_price": result.filled_price,
                    "quantity": quantity,
                    "order_id": result.order_id,
                    "status": result.status,
                    "guardrail_result": asdict(guardrail_result),
                    "plan": updated_plan,
                }
            )
            continue
        if normalized_status != "filled":
            mapped_status = "Cancelled" if normalized_status == "cancelled" else "Rejected"
            update_trade_plan_status(
                plan_id,
                mapped_status,
                notes=result.message or "Adapter rejected order.",
                reviewed_at=_now().isoformat(),
                broker_order_id=result.order_id,
                broker_status=result.status,
            )
            skipped.append(
                {
                    "plan_id": plan_id,
                    "ticker": ticker,
                    "reason": result.message or "Adapter rejected order.",
                    "status": result.status,
                    "guardrail_result": asdict(guardrail_result),
                }
            )
            continue
        if not isinstance(adapter, PaperBrokerAdapter):
            _apply_filled_execution(
                portfolio_id,
                plan,
                result,
            )
        update_trade_plan_status(
            plan_id,
            "Executed",
            executed_at=result.filled_at or _now().isoformat(),
            execution_price=result.filled_price,
            reviewed_at=plan.get("reviewed_at") or _now().isoformat(),
            broker_order_id=result.order_id,
            broker_status=result.status,
            filled_quantity=quantity,
        )
        executed.append(
            {
                "plan_id": plan_id,
                "ticker": ticker,
                "action": action,
                "execution_price": result.filled_price,
                "quantity": quantity,
                "order_id": result.order_id,
                "guardrail_result": asdict(guardrail_result),
            }
        )
    return {"executed": executed, "skipped": skipped, "portfolio": get_paper_portfolio(portfolio_id)}


def expire_stale_plans(portfolio_id: int | None = None, *, max_age_days: int = 2) -> int:
    portfolios = [get_paper_portfolio(portfolio_id)] if portfolio_id is not None else list_paper_portfolios(include_closed=False)
    cutoff = _now() - dt.timedelta(days=max_age_days)
    expired = 0
    for portfolio in portfolios:
        if not portfolio:
            continue
        for plan in get_trade_plans(int(portfolio["id"]), status="Pending"):
            created_at = _parse_timestamp(plan.get("created_at"))
            if created_at is None or created_at > cutoff:
                continue
            if update_trade_plan_status(int(plan["id"]), "Expired"):
                expired += 1
    return expired


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
        "benchmark_return": benchmark_return,
        "benchmark_return_pct": (benchmark_return * 100.0) if benchmark_return is not None else None,
        "portfolio_return": portfolio_return,
        "paper_return_pct": portfolio_return * 100.0,
        "alpha": alpha,
        "alpha_pct": (alpha * 100.0) if alpha is not None else None,
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
    wins = [row for row in closed_positions if float(row.get("realized_pnl") or 0.0) > 0]
    started_at = _parse_timestamp(portfolio.get("created_at")) or _now()
    days = max(30, (_now() - started_at).days + 5)
    benchmark_data = None
    try:
        benchmark_data = download_daily_bars("SPY", period=f"{days}d", auto_adjust=False)
    except Exception as exc:
        logger.warning("Unable to prefetch benchmark data for paper trading.", exc_info=exc)
    try:
        benchmark = compute_benchmark_comparison(portfolio_id, benchmark_data=benchmark_data)
    except TypeError:
        benchmark = compute_benchmark_comparison(portfolio_id)
    snapshots = get_performance_timeseries(portfolio_id)
    return {
        **summary,
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


def record_trade_outcome(portfolio_id: int, position: dict[str, Any], close_price: float) -> dict[str, Any]:
    del portfolio_id
    entry_price = float(position.get("entry_price") or 0.0)
    exit_date = _parse_timestamp(position.get("exit_date")) or _now()
    entry_date = _parse_timestamp(position.get("entry_date")) or exit_date
    return_pct = ((float(close_price) - entry_price) / entry_price) if entry_price > 0 else 0.0
    holding_days = max(0, (exit_date - entry_date).days)
    return {
        "ticker": str(position.get("ticker") or "").upper(),
        "return_pct": return_pct,
        "holding_days": holding_days,
        "outcome": "win" if return_pct > 0 else "loss",
    }


def kill_switch(
    portfolio_id: int,
    *,
    actor: str = "user",
    reason: str = "Manual kill switch activated",
) -> dict[str, Any] | None:
    portfolio = get_paper_portfolio(portfolio_id)
    if portfolio is None:
        return None
    rejected_count = 0
    now_text = _now().isoformat()
    for status in ("Pending", "Approved"):
        for plan in get_trade_plans(portfolio_id, status=status):
            updated = update_trade_plan_status(
                int(plan["id"]),
                "Rejected",
                reviewed_at=now_text,
                notes=f"Kill switch: {reason}",
            )
            if updated is not None:
                rejected_count += 1
    update_paper_portfolio(portfolio_id, status="Paused")
    log_audit_event(
        order_id=f"kill-switch-{portfolio_id}-{int(_now().timestamp())}",
        portfolio_id=portfolio_id,
        event_type="cancelled",
        ticker="*",
        action="kill_switch",
        actor=actor,
        details=reason,
        created_at=now_text,
    )
    return {
        "rejected_count": rejected_count,
        "portfolio_status": "Paused",
        "reason": reason,
        "killed_at": now_text,
    }


def _apply_filled_execution(
    portfolio_id: int,
    plan: dict[str, Any],
    result: Any,
) -> None:
    ticker = str(plan.get("ticker") or "").upper()
    action = str(plan.get("action") or "")
    quantity = float(plan.get("quantity") or 0.0)
    fill_price = float(result.filled_price or 0.0)
    if quantity <= 0 or fill_price <= 0:
        return
    portfolio = get_paper_portfolio(portfolio_id)
    if portfolio is None:
        return
    current_cash = float(portfolio.get("current_cash") or 0.0)
    now_text = result.filled_at or _now().isoformat()
    if action == "Buy":
        open_paper_position(
            portfolio_id,
            ticker,
            quantity,
            fill_price,
            now_text,
            theme_id=int(plan["theme_id"]) if plan.get("theme_id") is not None else None,
            role=str(plan.get("role") or "Critical-Path"),
        )
        update_paper_portfolio(portfolio_id, current_cash=current_cash - (quantity * fill_price))
        return
    open_positions = [
        row for row in get_paper_positions(portfolio_id, status="Open")
        if str(row.get("ticker") or "").upper() == ticker
    ]
    remaining = quantity
    credited = 0.0
    for position in sorted(open_positions, key=lambda row: str(row.get("entry_date") or "")):
        if remaining <= 0:
            break
        pos_qty = float(position.get("quantity") or 0.0)
        if pos_qty <= 0:
            continue
        if remaining < pos_qty - 1e-9:
            continue
        close_paper_position(int(position["id"]), fill_price, now_text, str(plan.get("source") or "broker_adapter"))
        credited += pos_qty * fill_price
        remaining -= pos_qty
    if credited > 0:
        update_paper_portfolio(portfolio_id, current_cash=current_cash + credited)


def _serialize_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{str(key): value for key, value in row.items()} for row in rows]
