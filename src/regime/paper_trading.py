from __future__ import annotations

import datetime as dt
import json
import logging
import math
import uuid
from dataclasses import asdict
from typing import Any

import pandas as pd

from .agent_competition import active_ticker_owners, configured_beta_portfolio_ids, diversification_settings
from .agent_policy import (
    agent_candidate_policy,
    buy_pause_status,
    earnings_blackout_status,
    near_close_cancel_active,
    setting_bool as policy_setting_bool,
    setting_float as policy_setting_float,
    setting_int as policy_setting_int,
)
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
from .ltcg_override import get_ltcg_override_settings
from .market_data_client import download_daily_bars, get_ticker_info
from .order_routing import decide_routing
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
    update_trade_plan_status,
)
from .ib_types import ET
from .signal_quality import ACTIONABLE_SIGNAL_SCORE, SignalQuality, evaluate_signal_quality

logger = logging.getLogger(__name__)

CachedRegimeValue = tuple[str, float] | dict[str, Any]
CachedRegimeMap = dict[str, CachedRegimeValue]
DEFAULT_SIZING_METHOD = "risk_budget"
DEFAULT_SIZING_BASE_RISK_FRACTION = 0.02
DEFAULT_SIZING_ATR_MULTIPLIER = 2.0
DEFAULT_BETA_TARGET_MONTHLY_RETURN = 0.02
DEFAULT_BETA_TARGET_ROLLING_MONTHS = 6
DEFAULT_BETA_TARGET_BENCHMARKS = ("SPY", "QQQ", "SOXX")
LONG_TERM_HOLDING_DAYS = 365


def _routing_time_in_force_from_plan(plan: dict[str, Any]) -> str:
    strategy = str(plan.get("routing_strategy") or "")
    if "IOC" in strategy or str(plan.get("order_type") or "").lower() == "marketable_limit":
        return "IOC"
    if "Patient" in strategy or "Passive" in strategy:
        return "GTC"
    return "DAY"


def _normalize_close_series(frame: pd.DataFrame | None) -> pd.Series:
    if frame is None or frame.empty or "Close" not in frame.columns:
        return pd.Series(dtype=float)
    close = frame["Close"]
    if getattr(close, "ndim", 1) > 1:
        close = close.iloc[:, 0]
    return pd.to_numeric(close, errors="coerce").dropna()


def _last_price_from_series(series: Any) -> float | None:
    cleaned = series.dropna()
    if cleaned.empty:
        return None
    value = cleaned.iloc[-1]
    if isinstance(value, pd.Series):
        value = value.dropna()
        if value.empty:
            return None
        value = value.iloc[0]
    return float(value)


def _publish_ltcg_override_events(
    portfolio_id: int,
    ticker: str,
    original_stop: float | None,
    ltcg_result: Any,
) -> None:
    if not getattr(ltcg_result, "override_active", False):
        return
    try:
        from .event_bus import get_event_bus
        from .events import BarrierOverrideEvent

        bus = get_event_bus()
    except Exception:
        bus = None
    for lot_detail in getattr(ltcg_result, "lot_details", []):
        if not getattr(lot_detail, "override_active", False):
            continue
        expiry = (dt.datetime.now(dt.timezone.utc) + dt.timedelta(days=max(0, int(getattr(lot_detail, "days_to_ltcg", 0))))).isoformat()
        try:
            log_barrier_override(
                portfolio_id,
                ticker,
                lot_id=int(getattr(lot_detail, "lot_id", 0) or 0),
                original_stop=original_stop,
                overridden_stop=getattr(lot_detail, "overridden_stop", None),
                days_to_ltcg=int(getattr(lot_detail, "days_to_ltcg", 0) or 0),
                tax_savings_estimate=float(getattr(lot_detail, "tax_savings_estimate", 0.0) or 0.0),
                additional_risk=float(getattr(lot_detail, "additional_risk", 0.0) or 0.0),
                expires_at=expiry,
            )
        except Exception:
            logger.debug("Unable to persist barrier override log for %s", ticker, exc_info=True)
        if bus is not None:
            try:
                bus.publish_sync(
                    BarrierOverrideEvent(
                        ticker=ticker,
                        portfolio_id=int(portfolio_id),
                        lot_id=int(getattr(lot_detail, "lot_id", 0) or 0),
                        original_stop=original_stop,
                        overridden_stop=getattr(lot_detail, "overridden_stop", None),
                        reason="ltcg_preservation",
                        days_to_ltcg=int(getattr(lot_detail, "days_to_ltcg", 0) or 0),
                        tax_savings_estimate=float(getattr(lot_detail, "tax_savings_estimate", 0.0) or 0.0),
                        max_additional_risk=float(getattr(lot_detail, "additional_risk", 0.0) or 0.0),
                        expiry=expiry,
                    )
                )
            except Exception:
                logger.debug("Unable to publish barrier override event for %s", ticker, exc_info=True)


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


def _aware_utc(value: dt.datetime | None) -> dt.datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=dt.timezone.utc)
    return value.astimezone(dt.timezone.utc)


def _holding_days(row: dict[str, Any], *, now: dt.datetime | None = None) -> int:
    start = _aware_utc(_parse_timestamp(row.get("entry_date") or row.get("acquisition_date") or row.get("created_at")))
    end = _aware_utc(_parse_timestamp(row.get("exit_date") or row.get("closed_date"))) or _aware_utc(now or _now())
    if start is None or end is None:
        return 0
    return max(0, int((end - start).total_seconds() // 86400))


def _tax_term(row: dict[str, Any], *, now: dt.datetime | None = None) -> str:
    explicit = str(row.get("gain_loss_term") or "").upper()
    if explicit.startswith("LT"):
        return "LT"
    if explicit.startswith("ST"):
        return "ST"
    return "LT" if _holding_days(row, now=now) >= LONG_TERM_HOLDING_DAYS else "ST"


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
                        price = _last_price_from_series(series)
                        if price is not None:
                            prices[ticker] = price
            else:
                close_series = frame["Close"] if "Close" in frame.columns else None
                if close_series is not None:
                    price = _last_price_from_series(close_series)
                    if price is not None:
                        prices[normalized[0]] = price
        except Exception as exc:
            logger.warning("Unable to parse batch prices for paper trading.", exc_info=exc)
    missing = [ticker for ticker in normalized if ticker not in prices]
    for ticker in missing:
        try:
            history = download_daily_bars(ticker, period="5d", auto_adjust=False)
            if history is not None and not history.empty and "Close" in history.columns:
                price = _last_price_from_series(history["Close"])
                if price is not None:
                    prices[ticker] = price
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


def get_beta_target_settings() -> dict[str, Any]:
    try:
        monthly_return = float(get_setting("beta_target_monthly_return") or DEFAULT_BETA_TARGET_MONTHLY_RETURN)
    except Exception:
        monthly_return = DEFAULT_BETA_TARGET_MONTHLY_RETURN
    try:
        rolling_months = int(get_setting("beta_target_rolling_months") or DEFAULT_BETA_TARGET_ROLLING_MONTHS)
    except Exception:
        rolling_months = DEFAULT_BETA_TARGET_ROLLING_MONTHS
    raw_benchmarks = str(get_setting("beta_target_benchmarks") or ",".join(DEFAULT_BETA_TARGET_BENCHMARKS))
    benchmarks = tuple(
        symbol
        for symbol in (item.strip().upper() for item in raw_benchmarks.split(","))
        if symbol
    ) or DEFAULT_BETA_TARGET_BENCHMARKS
    return {
        "monthly_return": max(0.0, min(monthly_return, 1.0)),
        "rolling_months": max(1, min(rolling_months, 24)),
        "benchmarks": benchmarks,
    }


def is_portfolio_autonomy_enabled(portfolio_id: int) -> bool:
    raw = str(get_setting("autonomous_portfolio_ids") or "").strip()
    if not raw:
        return True
    enabled_ids: set[int] = set()
    for item in raw.split(","):
        try:
            enabled_ids.add(int(item.strip()))
        except Exception:
            continue
    return int(portfolio_id) in enabled_ids


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
    from .anti_churn import check_anti_churn, get_anti_churn_settings
    from .slippage import estimate_execution_cost
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
    anti_churn_settings = get_anti_churn_settings()
    sizing_method = str(sizing_settings.get("sizing_method") or DEFAULT_SIZING_METHOD)
    base_risk_fraction = float(sizing_settings.get("sizing_base_risk_fraction") or DEFAULT_SIZING_BASE_RISK_FRACTION)
    atr_multiplier = float(sizing_settings.get("sizing_atr_multiplier") or DEFAULT_SIZING_ATR_MULTIPLIER)
    theme_budgets = {int(item["theme_id"]): item for item in allocation.get("themes", [])}
    pending_buys = _pending_plan_index(portfolio_id, "Buy")
    open_positions = _open_position_index(portfolio_id)
    planned_keys: set[tuple[str, int]] = set()
    created: list[dict[str, Any]] = []
    watchlist_items = get_watchlist(status=["Entry Signal", "Added"])
    fresh_prices = _batch_current_prices([str(item.get("ticker") or "") for item in watchlist_items if isinstance(item, dict)])
    for item in watchlist_items:
        ticker = str(item.get("ticker") or "").upper()
        theme_id = int(item.get("theme_id") or 0)
        key = (ticker, theme_id)
        if not ticker or ticker in pending_buys or ticker in open_positions or key in planned_keys:
            continue
        mandate = agent_candidate_policy(portfolio_id, ticker, source="discovery", candidate=item)
        if not mandate.get("allowed", True):
            continue
        theme_budget = theme_budgets.get(theme_id)
        if not theme_budget:
            continue
        role = str(item.get("suggested_role") or "Critical-Path")
        role_budget = float((theme_budget.get("by_role") or {}).get(role) or 0.0)
        entry_price = float(item.get("suggested_entry_price") or 0.0)
        snapshot = get_latest_signal_snapshot(ticker, max_age_days=7) or {}
        snapshot_current = float(snapshot.get("current_price") or 0.0)
        current_price = float(fresh_prices.get(ticker) or 0.0) or snapshot_current or entry_price
        if role_budget <= 0 or entry_price <= 0 or current_price <= 0:
            continue
        signal_row = {**snapshot, **item}
        if "price_targets" not in signal_row:
            signal_row["price_targets"] = {
                "entry_price": entry_price,
                "exit_price": item.get("suggested_exit_price") or snapshot.get("exit_price"),
                "stop_price": item.get("suggested_stop_price") or snapshot.get("stop_price"),
                "risk_reward_ratio": snapshot.get("risk_reward_ratio"),
            }
        quality = evaluate_signal_quality(
            signal_row,
            action="Buy",
            source="discovery",
            current_price=current_price,
            reference_price=entry_price,
        )
        if not quality.actionable:
            logger.info("Signal quality gate skipped discovery buy %s: %s", ticker, quality.summary())
            continue
        exit_price = item.get("suggested_exit_price")
        if exit_price in (None, ""):
            exit_price = snapshot.get("exit_price")
        anti_churn_result = None
        if bool(anti_churn_settings.get("anti_churn_enabled", True)):
            anti_churn_result = check_anti_churn(portfolio_id, ticker)
            if not anti_churn_result.passed:
                logger.info("Anti-churn gate blocked %s: %s", ticker, anti_churn_result.reason)
                continue
        atr_14 = _lookup_atr(ticker)
        beta = _lookup_beta(ticker)
        if sizing_method == "risk_budget":
            quantity = _risk_adjusted_quantity(
                role_budget,
                current_price,
                atr_14,
                beta,
                risk_per_share_multiplier=atr_multiplier,
                base_risk_fraction=base_risk_fraction,
            )
        else:
            quantity = math.floor(role_budget / current_price)
        if quantity <= 0:
            continue
        routing = decide_routing(
            ticker=ticker,
            action="Buy",
            quantity=quantity,
            last_price=current_price,
            urgency="patient",
        )
        exec_cost = estimate_execution_cost(
            ticker=ticker,
            routing_strategy=routing.strategy_name,
            algo_strategy=routing.algo_strategy,
            portfolio_id=portfolio_id,
        )
        hurdle_result = None
        duration_result = None
        if bool(hurdle_settings.get("hurdle_enabled", True)):
            hurdle_result = check_hurdle_rate(
                ticker,
                current_price,
                float(exit_price) if exit_price not in (None, "") else None,
                estimated_execution_cost_pct=exec_cost,
            )
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
        planned_keys.add(key)
        routed_price = float(routing.limit_price or current_price)
        if sizing_method == "risk_budget" and atr_14 is not None and atr_14 > 0:
            risk_per_share = float(atr_14) * atr_multiplier
            rationale = (
                f"Entry Signal from discovery watchlist. "
                f"Risk-sized: ATR={atr_14:.2f}, beta={float(beta or 1.0):.2f}, "
                f"risk/share=${risk_per_share:.2f}. "
                f"{_signal_quality_note(quality)} "
                f"{item.get('discovery_rationale') or 'Candidate meets paper-trading entry criteria.'}"
            )
        else:
            rationale = (
                f"Entry Signal from discovery watchlist. "
                f"{_signal_quality_note(quality)} "
                f"{item.get('discovery_rationale') or 'Candidate meets paper-trading entry criteria.'}"
            )
        if anti_churn_result:
            rationale = (
                f"{rationale} Anti-churn: {anti_churn_result.round_trip_count}/"
                f"{anti_churn_result.max_round_trips} completed round trips in lookback."
            )
        if mandate.get("reason"):
            rationale = f"{rationale} Agent mandate: {mandate.get('reason')}."
        if hurdle_result and hurdle_result.net_return_pct is not None and hurdle_result.gross_return_pct is not None:
            rationale = (
                f"{rationale} Hurdle: {hurdle_result.net_return_pct:.2f}% net "
                f"({hurdle_result.gross_return_pct:.2f}% gross - {hurdle_result.estimated_execution_cost_pct:.2f}% exec @ {hurdle_result.estimated_stcg_rate:.0%} tax)."
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
                proposed_price=routed_price,
                arrival_price=current_price,
                regime_label=str(item.get("regime_label") or snapshot.get("regime_label") or ""),
                regime_probability=float(item.get("regime_probability") or snapshot.get("regime_probability") or 0.0) if (item.get("regime_probability") is not None or snapshot.get("regime_probability") is not None) else None,
                crowd_score=int(item.get("crowd_score")) if item.get("crowd_score") is not None else None,
                source="discovery",
                order_type=routing.order_type,
                routing_strategy=routing.strategy_name,
                algo_strategy=routing.algo_strategy,
                meta_labeler_score=float(item.get("meta_labeler_probability")) if item.get("meta_labeler_probability") is not None else None,
                sizing_method="risk_budget" if sizing_method == "risk_budget" and atr_14 is not None and atr_14 > 0 else "equal_dollar",
                hurdle_gross_return_pct=hurdle_result.gross_return_pct if hurdle_result else None,
                hurdle_net_return_pct=hurdle_result.net_return_pct if hurdle_result else None,
                hurdle_passed=hurdle_result.passed if hurdle_result else None,
                duration_gate_passed=duration_result.passed if duration_result else None,
                expected_regime_duration=duration_result.expected_regime_duration if duration_result else None,
                anti_churn_passed=anti_churn_result.passed if anti_churn_result else None,
                **_signal_quality_plan_kwargs(quality),
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
    candidate_tickers = [
        str(row.get("ticker") or "").strip().upper()
        for row in rows
        if isinstance(row, dict) and row.get("is_portfolio_holding") and str(row.get("ticker") or "").strip()
    ]
    fresh_prices = _batch_current_prices(candidate_tickers)
    payload_timestamp = _payload_signal_timestamp(cached_payload)

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
        mandate = agent_candidate_policy(portfolio_id, ticker, source="holdings", candidate=row)
        if not mandate.get("allowed", True):
            continue
        price_targets = row.get("price_targets") if isinstance(row.get("price_targets"), dict) else {}
        current_price = float(fresh_prices.get(ticker) or 0.0) or float(row.get("current_price") or 0.0)
        entry_price = float(price_targets.get("entry_price") or 0.0)
        proposed_price = current_price or entry_price
        if proposed_price <= 0:
            continue
        quality = evaluate_signal_quality(
            row,
            action="Buy",
            source="holdings",
            current_price=current_price or proposed_price,
            reference_price=entry_price or proposed_price,
            source_timestamp=payload_timestamp,
        )
        if not quality.actionable:
            logger.info("Signal quality gate skipped holdings buy %s: %s", ticker, quality.summary())
            continue
        quantity = math.floor(role_budget / proposed_price)
        if quantity <= 0:
            continue
        routing = decide_routing(
            ticker=ticker,
            action="Buy",
            quantity=quantity,
            last_price=proposed_price,
            urgency="normal",
        )
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
        parts.append(_signal_quality_note(quality))
        rationale = f"Holdings bridge — {'; '.join(parts)}" if parts else "Holdings bridge plan"
        if mandate.get("reason"):
            rationale = f"{rationale}. Agent mandate: {mandate.get('reason')}."
        created.append(
            create_trade_plan(
                portfolio_id,
                ticker,
                "Buy",
                quantity,
                rationale,
                theme_id=theme_id,
                proposed_price=float(routing.limit_price or proposed_price),
                arrival_price=proposed_price,
                regime_label=str(row.get("regime") or ""),
                regime_probability=float(row.get("probability") or 0.0) if row.get("probability") is not None else None,
                source="holdings",
                order_type=routing.order_type,
                routing_strategy=routing.strategy_name,
                algo_strategy=routing.algo_strategy,
                meta_labeler_score=float(ml_prob) if ml_prob is not None else None,
                **_signal_quality_plan_kwargs(quality),
            )
        )
        pending_buy_keys.add(key)
    return created


def generate_exit_plans(
    portfolio_id: int,
    *,
    cached_regime: CachedRegimeMap | dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    from .ltcg_override import check_ltcg_override, get_ltcg_override_settings

    portfolio = get_paper_portfolio(portfolio_id)
    if portfolio is None:
        return []
    pending_sells = _pending_plan_index(portfolio_id, "Sell")
    open_positions = get_paper_positions(portfolio_id, status="Open")
    if not open_positions:
        return []
    prices = _batch_current_prices([str(row.get("ticker") or "") for row in open_positions])
    cached_rows = _cached_regime_map(cached_regime)
    payload_timestamp = _payload_signal_timestamp(cached_regime if isinstance(cached_regime, dict) else None)
    ltcg_settings = get_ltcg_override_settings()
    created: list[dict[str, Any]] = []
    for position in open_positions:
        ticker = str(position.get("ticker") or "").upper()
        if not ticker or ticker in pending_sells:
            continue
        current_price = prices.get(ticker)
        stop_price = float(position.get("stop_price") or 0.0)
        trigger_reason: str | None = None
        is_stop_triggered = False
        regime_label: str | None = None
        regime_probability: float | None = None
        signal_quality: SignalQuality | None = None
        if current_price is not None and stop_price > 0 and current_price <= stop_price:
            trigger_reason = f"Stop price hit (${current_price:.2f} <= ${stop_price:.2f})."
            is_stop_triggered = True
            signal_quality = evaluate_signal_quality(
                position,
                action="Sell",
                source="exit_signal",
                current_price=current_price,
                reference_price=stop_price,
                stop_triggered=True,
            )
        row = cached_rows.get(ticker)
        if row:
            regime_label = str(row.get("regime") or "").strip() or None
            try:
                regime_probability = float(row.get("probability")) if row.get("probability") is not None else None
            except Exception:
                regime_probability = None
            action = str(row.get("composite_signal") or "").strip()
            cached_trigger_reason: str | None = None
            if regime_label == "Bear":
                cached_trigger_reason = "Cached regime is Bear."
            if cached_trigger_reason is None and action in {"Sell", "Strong Sell"}:
                cached_trigger_reason = f"Cached composite signal is {action}."
            if trigger_reason is None and cached_trigger_reason is not None:
                quality = evaluate_signal_quality(
                    row,
                    action="Sell",
                    source="exit_signal",
                    current_price=current_price,
                    source_timestamp=payload_timestamp,
                )
                if quality.actionable:
                    signal_quality = quality
                    trigger_reason = f"{cached_trigger_reason} {_signal_quality_note(quality)}"
                else:
                    logger.info("Signal quality gate skipped cached exit %s: %s", ticker, quality.summary())
        if trigger_reason is None:
            try:
                quick_label, quick_prob, _entry, _stop = _quick_regime_screen(ticker)
                regime_label = quick_label
                regime_probability = quick_prob
                if quick_label == "Bear":
                    quality = evaluate_signal_quality(
                        {"regime": quick_label, "probability": quick_prob, "signal_generated_at": _now().isoformat()},
                        action="Sell",
                        source="exit_signal",
                        current_price=current_price,
                    )
                    if quality.actionable:
                        signal_quality = quality
                        trigger_reason = f"Fallback regime screen flipped to Bear. {_signal_quality_note(quality)}"
            except Exception as exc:
                logger.warning("Fallback regime screen failed for paper exit plan %s.", ticker, exc_info=exc)
        if trigger_reason is None:
            continue
        quantity = float(position.get("quantity") or 0.0)
        if quantity <= 0:
            continue
        ltcg_result = None
        if bool(ltcg_settings.get("ltcg_override_enabled", True)):
            ltcg_result = check_ltcg_override(
                portfolio_id,
                ticker,
                current_price=float(current_price or 0.0),
                position_stop=stop_price if stop_price > 0 else None,
                atr_14=_lookup_atr(ticker),
            )
            if ltcg_result.override_active:
                _publish_ltcg_override_events(portfolio_id, ticker, stop_price if stop_price > 0 else None, ltcg_result)
                if ltcg_result.sellable_quantity <= 0:
                    logger.info("LTCG override suppressed exit for %s: %s", ticker, ltcg_result.reason)
                    continue
                quantity = float(ltcg_result.sellable_quantity)
                trigger_reason = (
                    f"{trigger_reason} LTCG override: protecting "
                    f"{ltcg_result.protected_quantity:.0f} shares "
                    f"({ltcg_result.lots_overridden} lots near LTCG). "
                    f"Tax savings: ${ltcg_result.total_tax_savings:.2f}."
                )
        proposed_price = current_price or float(position.get("entry_price") or 0.0)
        routing = decide_routing(
            ticker=ticker,
            action="Sell",
            quantity=quantity,
            last_price=float(proposed_price or 0.0),
            urgency="urgent" if is_stop_triggered else "normal",
            is_stop_triggered=is_stop_triggered,
        )
        created.append(
            create_trade_plan(
                portfolio_id,
                ticker,
                "Sell",
                quantity,
                trigger_reason,
                theme_id=int(position["theme_id"]) if position.get("theme_id") is not None else None,
                proposed_price=float(routing.limit_price or proposed_price) if proposed_price > 0 else None,
                arrival_price=float(proposed_price) if proposed_price not in (None, "") else None,
                regime_label=regime_label,
                regime_probability=regime_probability,
                source="exit_signal",
                order_type=routing.order_type,
                routing_strategy=routing.strategy_name,
                algo_strategy=routing.algo_strategy,
                ltcg_override_active=ltcg_result.override_active if ltcg_result else None,
                ltcg_protected_quantity=ltcg_result.protected_quantity if ltcg_result else None,
                ltcg_tax_savings=ltcg_result.total_tax_savings if ltcg_result else None,
                **(_signal_quality_plan_kwargs(signal_quality) if signal_quality else {}),
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
            "portfolio_autonomy_enabled": False,
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
            "portfolio_autonomy_enabled": False,
            "details": [],
        }
    if not is_portfolio_autonomy_enabled(portfolio_id):
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
            "portfolio_autonomy_enabled": False,
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
    beta_portfolio_ids = configured_beta_portfolio_ids()
    beta_portfolio_set = {int(item) for item in beta_portfolio_ids}
    diversification = diversification_settings()
    diversification_enabled = (
        bool(diversification.get("enabled"))
        and bool(diversification.get("enforce_orders"))
        and bool(beta_portfolio_set)
        and int(portfolio_id) in beta_portfolio_set
    )
    max_active_portfolios_per_ticker = int(diversification.get("max_active_portfolios_per_ticker") or 1)
    approved = 0
    skipped = 0
    blocked = 0
    details: list[dict[str, Any]] = []
    now_text = _now().isoformat()
    buy_pause = buy_pause_status(portfolio_id)

    for plan in pending:
        plan_id = int(plan["id"])
        action = str(plan.get("action") or "")
        ticker = str(plan.get("ticker") or "").upper()
        quantity = float(plan.get("quantity") or 0.0)
        proposed_price = float(plan.get("proposed_price") or 0.0) or None
        score = float(plan.get("meta_labeler_score")) if plan.get("meta_labeler_score") is not None else None
        signal_quality_score = float(plan.get("signal_quality_score")) if plan.get("signal_quality_score") is not None else None
        signal_quality_grade = str(plan.get("signal_quality_grade") or "")
        order_value = (quantity * proposed_price) if proposed_price and quantity > 0 else None
        if action in {"Buy", "Sell"} and signal_quality_score is not None:
            if signal_quality_grade == "blocked" or signal_quality_score < ACTIONABLE_SIGNAL_SCORE:
                blocked += 1
                reason = (
                    f"Signal quality blocked {ticker}: "
                    f"{signal_quality_score:.0f}/{signal_quality_grade or 'ungraded'}."
                )
                details.append(
                    {
                        "plan_id": plan_id,
                        "ticker": ticker,
                        "action": action,
                        "result": "blocked_signal_quality",
                        "signal_quality_score": signal_quality_score,
                        "signal_quality_grade": signal_quality_grade,
                        "meta_labeler_score": score,
                        "order_value": order_value,
                    }
                )
                log_audit_event(
                    order_id=str(uuid.uuid4()),
                    portfolio_id=portfolio_id,
                    event_type="guardrail_blocked",
                    ticker=ticker,
                    action=action,
                    quantity=quantity,
                    price=proposed_price,
                    actor="system",
                    details=reason,
                )
                continue
        if action == "Buy":
            mandate = agent_candidate_policy(portfolio_id, ticker, source=str(plan.get("source") or "plan"), candidate=plan)
            if not mandate.get("allowed", True):
                blocked += 1
                details.append(
                    {
                        "plan_id": plan_id,
                        "ticker": ticker,
                        "action": action,
                        "result": "blocked_agent_mandate",
                        "meta_labeler_score": score,
                        "order_value": order_value,
                        "mandate": mandate,
                    }
                )
                log_audit_event(
                    order_id=str(uuid.uuid4()),
                    portfolio_id=portfolio_id,
                    event_type="guardrail_blocked",
                    ticker=ticker,
                    action=action,
                    quantity=quantity,
                    price=proposed_price,
                    actor="system",
                    details=f"Agent mandate blocked {ticker}: {mandate.get('reason') or 'not assigned'}.",
                )
                continue
            if not buy_pause.get("allowed", True):
                blocked += 1
                reasons = list(buy_pause.get("reasons") or [])
                details.append(
                    {
                        "plan_id": plan_id,
                        "ticker": ticker,
                        "action": action,
                        "result": "blocked_buy_pause",
                        "meta_labeler_score": score,
                        "order_value": order_value,
                        "reasons": reasons,
                    }
                )
                log_audit_event(
                    order_id=str(uuid.uuid4()),
                    portfolio_id=portfolio_id,
                    event_type="guardrail_blocked",
                    ticker=ticker,
                    action=action,
                    quantity=quantity,
                    price=proposed_price,
                    actor="system",
                    details="; ".join(str(reason.get("message") or reason.get("code")) for reason in reasons) or "Buy pause active.",
                )
                continue
            earnings_status = earnings_blackout_status(ticker)
            if not earnings_status.get("allowed", True):
                blocked += 1
                details.append(
                    {
                        "plan_id": plan_id,
                        "ticker": ticker,
                        "action": action,
                        "result": "blocked_earnings_blackout",
                        "meta_labeler_score": score,
                        "order_value": order_value,
                        "earnings": earnings_status,
                    }
                )
                log_audit_event(
                    order_id=str(uuid.uuid4()),
                    portfolio_id=portfolio_id,
                    event_type="guardrail_blocked",
                    ticker=ticker,
                    action=action,
                    quantity=quantity,
                    price=proposed_price,
                    actor="system",
                    details=(
                        f"Earnings blackout blocked {ticker}; earnings {earnings_status.get('earnings_date')} "
                        f"within {earnings_status.get('limit_days')} days."
                    ),
                )
                continue
            if diversification_enabled and ticker:
                owners = active_ticker_owners(
                    ticker,
                    current_portfolio_id=portfolio_id,
                    portfolio_ids=beta_portfolio_ids,
                )
                if len(owners) >= max_active_portfolios_per_ticker:
                    blocked += 1
                    owner_labels = ", ".join(str(owner.get("agent_label") or owner.get("portfolio_id")) for owner in owners)
                    reason = (
                        f"Cross-agent diversification blocked {ticker}; already active in "
                        f"{owner_labels or len(owners)}."
                    )
                    details.append(
                        {
                            "plan_id": plan_id,
                            "ticker": ticker,
                            "action": action,
                            "result": "blocked_cross_agent_overlap",
                            "meta_labeler_score": score,
                            "order_value": order_value,
                            "owners": owners,
                            "max_active_portfolios_per_ticker": max_active_portfolios_per_ticker,
                        }
                    )
                    log_audit_event(
                        order_id=str(uuid.uuid4()),
                        portfolio_id=portfolio_id,
                        event_type="guardrail_blocked",
                        ticker=ticker,
                        action=action,
                        quantity=quantity,
                        price=proposed_price,
                        actor="system",
                        details=reason,
                    )
                    continue
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
            order_type=str(plan.get("order_type") or "limit"),
            limit_price=proposed_price,
            time_in_force=_routing_time_in_force_from_plan(plan),
            routing_strategy=str(plan.get("routing_strategy") or ""),
            algo_strategy=str(plan.get("algo_strategy") or ""),
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
        "portfolio_autonomy_enabled": True,
        "details": details,
    }


def _approved_buy_execution_policy_block(
    portfolio_id: int,
    plan: dict[str, Any],
    order: OrderRequest,
    adapter: BrokerAdapter,
) -> dict[str, Any] | None:
    ticker = str(order.ticker or "").upper()
    quantity = float(order.quantity or 0.0)
    proposed_price = float(order.limit_price or 0.0) or None
    order_value = quantity * proposed_price if proposed_price and quantity > 0 else None
    existing_quality_score = float(plan.get("signal_quality_score")) if plan.get("signal_quality_score") is not None else None
    existing_quality_grade = str(plan.get("signal_quality_grade") or "")
    if existing_quality_score is not None and (existing_quality_grade == "blocked" or existing_quality_score < ACTIONABLE_SIGNAL_SCORE):
        return {
            "result": "blocked_signal_quality",
            "message": f"Signal quality blocked {ticker}: {existing_quality_score:.0f}/{existing_quality_grade or 'ungraded'}.",
            "signal_quality_score": existing_quality_score,
            "signal_quality_grade": existing_quality_grade,
            "order_value": order_value,
        }

    mandate = agent_candidate_policy(portfolio_id, ticker, source=str(plan.get("source") or "plan"), candidate=plan)
    if not mandate.get("allowed", True):
        return {
            "result": "blocked_agent_mandate",
            "message": f"Agent mandate blocked {ticker}: {mandate.get('reason') or 'not assigned'}.",
            "mandate": mandate,
            "order_value": order_value,
        }

    pause = buy_pause_status(portfolio_id)
    if not pause.get("allowed", True):
        reasons = list(pause.get("reasons") or [])
        message = "; ".join(str(reason.get("message") or reason.get("code")) for reason in reasons) or "Buy pause active."
        return {
            "result": "blocked_buy_pause",
            "message": message,
            "reasons": reasons,
            "order_value": order_value,
        }

    earnings = earnings_blackout_status(ticker)
    if not earnings.get("allowed", True):
        return {
            "result": "blocked_earnings_blackout",
            "message": (
                f"Earnings blackout blocked {ticker}; earnings {earnings.get('earnings_date')} "
                f"within {earnings.get('limit_days')} days."
            ),
            "earnings": earnings,
            "order_value": order_value,
        }

    beta_portfolio_ids = configured_beta_portfolio_ids()
    beta_portfolio_set = {int(item) for item in beta_portfolio_ids}
    diversification = diversification_settings()
    diversification_enabled = (
        bool(diversification.get("enabled"))
        and bool(diversification.get("enforce_orders"))
        and bool(beta_portfolio_set)
        and int(portfolio_id) in beta_portfolio_set
    )
    if diversification_enabled and ticker:
        max_active = int(diversification.get("max_active_portfolios_per_ticker") or 1)
        owners = active_ticker_owners(
            ticker,
            current_portfolio_id=portfolio_id,
            portfolio_ids=beta_portfolio_ids,
        )
        if len(owners) >= max_active:
            owner_labels = ", ".join(str(owner.get("agent_label") or owner.get("portfolio_id")) for owner in owners)
            return {
                "result": "blocked_cross_agent_overlap",
                "message": f"Cross-agent diversification blocked {ticker}; already active in {owner_labels or len(owners)}.",
                "owners": owners,
                "max_active_portfolios_per_ticker": max_active,
                "order_value": order_value,
            }

    if order_value is not None:
        summary = get_paper_portfolio_summary(portfolio_id)
        equity = float(summary.get("total_equity") or 0.0)
        if equity <= 0:
            equity = float(summary.get("current_cash") or 0.0) + float(summary.get("total_market_value") or 0.0)
        max_daily_capital = equity * get_daily_capital_ceiling_pct()
        deployed = get_daily_capital_deployed(portfolio_id)
        if max_daily_capital > 0 and deployed + order_value > max_daily_capital:
            return {
                "result": "blocked_ceiling",
                "message": f"Daily capital ceiling blocked {ticker}: ${deployed + order_value:,.2f} > ${max_daily_capital:,.2f}.",
                "order_value": order_value,
                "capital_deployed_before": deployed,
                "max_daily_capital": max_daily_capital,
            }

    from .vix_freeze import is_vix_frozen

    if is_vix_frozen():
        return {
            "result": "blocked_vix_freeze",
            "message": "VIX freeze is active. New buy execution blocked.",
            "order_value": order_value,
        }
    current_price = _policy_current_price(adapter, ticker, "Buy")
    quality = evaluate_signal_quality(
        plan,
        action="Buy",
        source=str(plan.get("source") or "plan"),
        current_price=current_price,
        reference_price=float(plan.get("arrival_price") or proposed_price or 0.0) or None,
        source_timestamp=plan.get("created_at"),
    )
    if not quality.actionable:
        return {
            "result": "blocked_signal_quality",
            "message": f"Signal quality blocked {ticker}: {quality.score:.0f}/{quality.grade} ({quality.summary()}).",
            "signal_quality": quality.to_dict(),
            "order_value": order_value,
        }
    return None


def _payload_signal_timestamp(payload: dict[str, Any] | None) -> Any:
    if not isinstance(payload, dict):
        return None
    for key in ("last_run_timestamp", "generated_at", "cached_at", "updated_at"):
        value = payload.get(key)
        if value not in (None, ""):
            return value
    return None


def _signal_quality_plan_kwargs(quality: SignalQuality) -> dict[str, Any]:
    return {
        "signal_quality_score": quality.score,
        "signal_quality_grade": quality.grade,
        "signal_quality_reasons": [*quality.blockers, *quality.warnings, *quality.reasons],
    }


def _signal_quality_note(quality: SignalQuality) -> str:
    summary = quality.summary()
    return f"Signal quality: {quality.score:.0f}/{quality.grade}" + (f" ({summary})." if summary else ".")


def _policy_current_price(adapter: BrokerAdapter, ticker: str, action: str = "Buy") -> float | None:
    getter = getattr(adapter, "get_current_price", None)
    if callable(getter):
        try:
            value = getter(ticker, action)
            if value is not None and float(value) > 0:
                return float(value)
        except Exception:
            logger.debug("Policy quote adapter lookup failed for %s.", ticker, exc_info=True)
    return _batch_current_prices([ticker]).get(str(ticker or "").upper())


def _overlap_cancel_plan_ids(portfolio_ids: list[int], max_active: int) -> set[int]:
    position_owners: dict[str, set[int]] = {}
    submitted_plans: dict[str, list[dict[str, Any]]] = {}
    for pid in portfolio_ids:
        for position in get_paper_positions(int(pid), status="Open"):
            ticker = str(position.get("ticker") or "").upper()
            if ticker and float(position.get("quantity") or 0.0) > 0:
                position_owners.setdefault(ticker, set()).add(int(pid))
        for plan in get_trade_plans(int(pid), status="all"):
            ticker = str(plan.get("ticker") or "").upper()
            if (
                ticker
                and str(plan.get("action") or "") == "Buy"
                and str(plan.get("status") or "") in {"Submitted", "Partially Filled"}
            ):
                submitted_plans.setdefault(ticker, []).append(plan)

    cancel_ids: set[int] = set()
    for ticker, plans in submitted_plans.items():
        open_owner_count = len(position_owners.get(ticker, set()))
        available_slots = max(0, int(max_active) - open_owner_count)
        ranked = sorted(
            plans,
            key=lambda plan: (
                float(plan.get("proposed_price") or 0.0),
                str(plan.get("updated_at") or plan.get("created_at") or ""),
                int(plan.get("id") or 0),
            ),
        )
        keep_ids = {int(plan["id"]) for plan in ranked[:available_slots]}
        for plan in ranked:
            plan_id = int(plan["id"])
            if plan_id not in keep_ids:
                cancel_ids.add(plan_id)
    return cancel_ids


def cancel_submitted_orders_by_policy(
    portfolio_id: int,
    adapter: BrokerAdapter,
    *,
    now: dt.datetime | None = None,
) -> dict[str, Any]:
    """Cancel submitted buy orders that violate beta-agent policy.

    This is intentionally scoped to submitted/partially-filled broker orders. It
    does not close existing positions; it only cancels resting order quantity.
    """

    if not policy_setting_bool("agent_submitted_order_cancel_enabled", True):
        return {"cancelled": [], "failed": [], "checked": 0}
    current = now or _now()
    portfolio_ids = configured_beta_portfolio_ids()
    diversification = diversification_settings()
    max_active = int(diversification.get("max_active_portfolios_per_ticker") or 1)
    overlap_cancel_ids = (
        _overlap_cancel_plan_ids(portfolio_ids, max_active)
        if portfolio_ids and bool(diversification.get("enforce_orders"))
        else set()
    )
    max_age_minutes = policy_setting_int("agent_stale_order_max_age_minutes", 45, minimum=1, maximum=480)
    max_price_deviation = policy_setting_float("agent_stale_order_price_deviation_pct", 0.01, minimum=0.0, maximum=1.0)
    near_close = near_close_cancel_active(current)

    cancelled: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []
    checked = 0
    for plan in get_trade_plans(portfolio_id, status="all"):
        plan_id = int(plan.get("id") or 0)
        status = str(plan.get("status") or "")
        action = str(plan.get("action") or "")
        ticker = str(plan.get("ticker") or "").upper()
        if status not in {"Submitted", "Partially Filled"} or action != "Buy" or not ticker:
            continue
        checked += 1
        reasons: list[str] = []
        submitted_at = _parse_timestamp(plan.get("updated_at")) or _parse_timestamp(plan.get("reviewed_at")) or _parse_timestamp(plan.get("created_at"))
        if submitted_at is not None:
            if submitted_at.tzinfo is None:
                submitted_at = submitted_at.replace(tzinfo=dt.timezone.utc)
            age_minutes = (current - submitted_at.astimezone(dt.timezone.utc)).total_seconds() / 60.0
            if age_minutes >= max_age_minutes:
                reasons.append(f"stale_age_{age_minutes:.0f}m")
        proposed_price = float(plan.get("proposed_price") or 0.0)
        if proposed_price > 0 and max_price_deviation > 0:
            current_price = _policy_current_price(adapter, ticker, action)
            if current_price is not None and current_price > 0:
                deviation = abs(proposed_price - current_price) / current_price
                if deviation >= max_price_deviation:
                    reasons.append(f"price_deviation_{deviation:.2%}")
        if near_close:
            reasons.append("near_market_close")
        if plan_id in overlap_cancel_ids:
            reasons.append("cross_agent_overlap")
        if not reasons:
            continue
        broker_order_id = str(plan.get("broker_order_id") or "")
        cancelled_at_broker = True
        if broker_order_id:
            try:
                cancelled_at_broker = bool(adapter.cancel_order(broker_order_id))
            except Exception:
                cancelled_at_broker = False
        if not cancelled_at_broker:
            failed.append({"plan_id": plan_id, "ticker": ticker, "broker_order_id": broker_order_id, "reasons": reasons})
            continue
        note = f"Policy cancelled submitted buy order: {', '.join(reasons)}"
        update_trade_plan_status(plan_id, "Cancelled", reviewed_at=current.isoformat(), notes=note, broker_status="cancelled")
        log_audit_event(
            order_id=broker_order_id or str(uuid.uuid4()),
            portfolio_id=portfolio_id,
            event_type="cancelled",
            ticker=ticker,
            action=action,
            quantity=float(plan.get("quantity") or 0.0),
            price=proposed_price or None,
            actor="system",
            details=note,
        )
        cancelled.append({"plan_id": plan_id, "ticker": ticker, "broker_order_id": broker_order_id, "reasons": reasons})
    return {"cancelled": cancelled, "failed": failed, "checked": checked}


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
            order_type=str(plan.get("order_type") or "limit"),
            limit_price=float(plan.get("proposed_price") or 0.0) or None,
            stop_price=float(stop_price) if stop_price is not None else None,
            time_in_force=_routing_time_in_force_from_plan(plan),
            routing_strategy=str(plan.get("routing_strategy") or ""),
            algo_strategy=str(plan.get("algo_strategy") or ""),
            theme_id=int(plan["theme_id"]) if plan.get("theme_id") is not None else None,
            role=role,
            source=str(plan.get("source") or "manual"),
            notes=str(plan.get("rationale") or ""),
        )
        if action == "Buy":
            policy_block = _approved_buy_execution_policy_block(portfolio_id, plan, order, adapter)
            if policy_block is not None:
                note = str(policy_block.get("message") or "Blocked by beta-agent policy.")
                update_trade_plan_status(plan_id, "Rejected", notes=note, reviewed_at=_now().isoformat())
                log_audit_event(
                    order_id=str(uuid.uuid4()),
                    portfolio_id=portfolio_id,
                    event_type="guardrail_blocked",
                    ticker=ticker,
                    action=action,
                    quantity=quantity,
                    price=float(order.limit_price or 0.0) or None,
                    actor=actor,
                    details=note,
                )
                skipped.append(
                    {
                        "plan_id": plan_id,
                        "ticker": ticker,
                        "reason": note,
                        "status": str(policy_block.get("result") or "policy_blocked"),
                        "policy_block": policy_block,
                    }
                )
                continue
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
