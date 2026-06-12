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


def _guardrail_block_details(result: Any) -> tuple[list[str], list[dict[str, Any]]]:
    messages: list[str] = []
    checks: list[dict[str, Any]] = []
    for check in list(getattr(result, "checks", None) or []):
        if isinstance(check, dict):
            row = dict(check)
        else:
            try:
                row = asdict(check)
            except TypeError:
                row = dict(getattr(check, "__dict__", {}) or {})
        passed = row.get("passed", row.get("allowed", True))
        checks.append(row)
        if passed is False:
            message = str(row.get("message") or row.get("name") or "Guardrail check failed.")
            messages.append(message)
    return messages, checks


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


def _positive_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        parsed = float(value)
    except Exception:
        return None
    if not math.isfinite(parsed) or parsed <= 0:
        return None
    return parsed


def _entry_signal_max_age_days() -> int:
    return policy_setting_int("entry_signal_max_age_days", 3, minimum=1, maximum=30)


def _lookup_atr(ticker: str) -> float | None:
    snapshot = get_latest_signal_snapshot(str(ticker or "").upper(), max_age_days=_entry_signal_max_age_days())
    if snapshot:
        atr = _positive_float(snapshot.get("atr_14"))
        if atr is not None:
            return atr
        current = _positive_float(snapshot.get("current_price"))
        stop = _positive_float(snapshot.get("stop_price"))
        if current is not None and stop is not None and current > stop:
            return (current - stop) / DEFAULT_SIZING_ATR_MULTIPLIER
    try:
        history = download_daily_bars(str(ticker or "").upper(), period="3mo", auto_adjust=False)
        if history is None or history.empty or "Close" not in history.columns:
            return None
        from ..signals import compute_technicals

        close = pd.to_numeric(history["Close"], errors="coerce")
        volume = (
            pd.to_numeric(history["Volume"], errors="coerce")
            if "Volume" in history.columns
            else pd.Series([1.0] * len(history), index=history.index, dtype=float)
        )
        high = pd.to_numeric(history["High"], errors="coerce") if "High" in history.columns else None
        low = pd.to_numeric(history["Low"], errors="coerce") if "Low" in history.columns else None
        technicals = compute_technicals(close, volume, high_series=high, low_series=low)
        atr_series = pd.to_numeric(technicals.get("atr_14"), errors="coerce").dropna()
        if not atr_series.empty:
            return _positive_float(atr_series.iloc[-1])
    except Exception as exc:
        from ..decision_health import record_fallback

        record_fallback("paper_trading.lookup_atr", f"{ticker}: {exc}")
        logger.debug("ATR fallback failed for %s.", ticker, exc_info=True)
    return None


def _lookup_beta(ticker: str) -> float | None:
    try:
        info = fetch_financial_statements(str(ticker or "").upper()).info or {}
    except Exception as exc:
        from ..decision_health import record_fallback

        record_fallback("paper_trading.lookup_beta", f"{ticker}: {exc}")
        info = {}
    beta = info.get("beta")
    try:
        return float(beta) if beta is not None else None
    except Exception:
        return None


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _serialize_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{str(key): value for key, value in row.items()} for row in rows]
