from __future__ import annotations

import datetime as dt
import hashlib
from typing import Any

from .agent_competition import configured_beta_portfolio_ids, setting_bool, setting_int
from .beta_agents import BETA_AGENT_PORTFOLIOS
from .ib_types import ET
from .persistence import (
    get_audit_trail,
    get_daily_snapshots,
    get_paper_portfolio,
    get_paper_portfolio_summary,
    get_setting,
)


def setting_float(key: str, default: float, *, minimum: float = 0.0, maximum: float | None = None) -> float:
    try:
        value = float(get_setting(key) or default)
    except Exception:
        value = float(default)
    value = max(float(minimum), value)
    if maximum is not None:
        value = min(float(maximum), value)
    return value


def _agent_index_for_portfolio(portfolio_id: int) -> int | None:
    ids = configured_beta_portfolio_ids()
    try:
        return ids.index(int(portfolio_id))
    except ValueError:
        return None


def beta_agent_key_for_portfolio(portfolio_id: int) -> str | None:
    index = _agent_index_for_portfolio(portfolio_id)
    if index is None or index >= len(BETA_AGENT_PORTFOLIOS):
        return None
    return str(BETA_AGENT_PORTFOLIOS[index]["key"])


def _ticker_bucket(ticker: str, bucket_count: int) -> int:
    if bucket_count <= 1:
        return 0
    digest = hashlib.sha256(str(ticker or "").upper().encode("utf-8")).hexdigest()
    return int(digest[:8], 16) % bucket_count


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        parsed = float(value)
        return parsed if parsed == parsed else default
    except Exception:
        return default


def _as_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "on", "pass", "passed"}:
        return True
    if normalized in {"0", "false", "no", "off", "fail", "failed", "blocked"}:
        return False
    return None


def agent_candidate_policy(
    portfolio_id: int,
    ticker: str,
    *,
    source: str,
    candidate: dict[str, Any] | None = None,
) -> dict[str, Any]:
    ids = configured_beta_portfolio_ids()
    index = _agent_index_for_portfolio(portfolio_id)
    candidate = candidate or {}
    key = beta_agent_key_for_portfolio(portfolio_id) or (f"agent_{index + 1}" if index is not None else None)
    symbol = str(ticker or "").upper()
    bucket = _ticker_bucket(symbol, len(ids)) if ids else 0

    if setting_bool("agent_strategy_flexibility_enabled", True):
        return {
            "allowed": True,
            "reason": "strategy_flexible_policy",
            "agent": key,
            "bucket": bucket,
            "policy": "strategy_flexible",
        }
    if not setting_bool("agent_mandate_diversification_enabled", True):
        return {"allowed": True, "reason": "mandate_filter_disabled", "agent": key, "bucket": bucket}
    if index is None or len(ids) <= 1:
        return {"allowed": True, "reason": "not_beta_agent"}

    bucket_allowed = bucket == index
    source_name = str(source or "").strip().lower()
    role = str(candidate.get("suggested_role") or candidate.get("role") or "Critical-Path")

    regime_probability = _as_float(candidate.get("regime_probability", candidate.get("probability")), 0.0)
    ml_probability = _as_float(candidate.get("meta_labeler_probability"), 0.0)
    quality_passed = _as_bool(candidate.get("fundamental_gate_passed"))
    piotroski = _as_float(candidate.get("piotroski_score"), -1.0)
    altman = _as_float(candidate.get("altman_z_score"), -1.0)
    beta = _as_float(candidate.get("beta"), 1.0)
    spread_pct = _as_float(candidate.get("spread_pct"), 0.0)

    strategy_allowed = False
    strategy_reason = ""
    if key == "quant":
        strategy_allowed = source_name == "discovery" and (
            regime_probability >= 0.65 or ml_probability >= 0.72
        )
        strategy_reason = "quant_high_signal"
    elif key == "fundamental":
        strategy_allowed = quality_passed is True or piotroski >= 6.0 or altman >= 2.99
        strategy_reason = "fundamental_quality"
    elif key == "portfolio_tax":
        strategy_allowed = source_name == "holdings" or (role != "Speculative" and beta <= 1.5)
        strategy_reason = "portfolio_tax_low_churn"
    elif key == "execution":
        strategy_allowed = spread_pct <= 0.005 or source_name in {"holdings", "manual"}
        strategy_reason = "execution_liquidity"

    if strategy_allowed:
        return {"allowed": True, "reason": strategy_reason, "agent": key, "bucket": bucket}
    if bucket_allowed:
        return {"allowed": True, "reason": "assigned_ticker_bucket", "agent": key, "bucket": bucket}
    return {
        "allowed": False,
        "reason": f"ticker_assigned_to_agent_{bucket + 1}",
        "agent": key,
        "bucket": bucket,
    }


def current_portfolio_drawdown_pct(portfolio_id: int) -> float:
    summary = get_paper_portfolio_summary(portfolio_id)
    portfolio = get_paper_portfolio(portfolio_id) or {}
    current_equity = _as_float(summary.get("current_cash")) + _as_float(summary.get("total_market_value"))
    starting_budget = _as_float(portfolio.get("starting_budget"))
    peak = max(starting_budget, current_equity)
    for row in get_daily_snapshots(portfolio_id):
        peak = max(peak, _as_float(row.get("equity")))
    if peak <= 0:
        return 0.0
    return (current_equity - peak) / peak


def aggregate_beta_drawdown_pct(portfolio_ids: list[int] | None = None) -> float:
    ids = list(portfolio_ids or configured_beta_portfolio_ids())
    if not ids:
        return 0.0
    current_total = 0.0
    starting_total = 0.0
    totals_by_date: dict[str, float] = {}
    for portfolio_id in ids:
        summary = get_paper_portfolio_summary(int(portfolio_id))
        portfolio = get_paper_portfolio(int(portfolio_id)) or {}
        current_total += _as_float(summary.get("current_cash")) + _as_float(summary.get("total_market_value"))
        starting_total += _as_float(portfolio.get("starting_budget"))
        for row in get_daily_snapshots(int(portfolio_id)):
            date_key = str(row.get("snapshot_date") or "")
            if not date_key:
                continue
            totals_by_date[date_key] = totals_by_date.get(date_key, 0.0) + _as_float(row.get("equity"))
    peak = max([starting_total, current_total, *totals_by_date.values()])
    if peak <= 0:
        return 0.0
    return (current_total - peak) / peak


def recent_policy_event_count(portfolio_id: int) -> int:
    events = get_audit_trail(portfolio_id=portfolio_id, days=1, limit=500)
    counted = {"guardrail_blocked", "rejected", "error"}
    return sum(1 for event in events if str(event.get("event_type") or "") in counted)


def buy_pause_status(portfolio_id: int) -> dict[str, Any]:
    reasons: list[dict[str, Any]] = []
    if setting_bool("agent_drawdown_pause_enabled", True):
        portfolio_limit = setting_float("agent_max_drawdown_pause_pct", 0.05, minimum=0.0, maximum=1.0)
        portfolio_drawdown = abs(min(0.0, current_portfolio_drawdown_pct(portfolio_id)))
        if portfolio_drawdown >= portfolio_limit:
            reasons.append(
                {
                    "code": "portfolio_drawdown_pause",
                    "message": f"Portfolio drawdown {portfolio_drawdown:.1%} exceeds {portfolio_limit:.1%}.",
                    "actual": portfolio_drawdown,
                    "limit": portfolio_limit,
                }
            )
        aggregate_limit = setting_float("agent_beta_max_drawdown_pause_pct", 0.07, minimum=0.0, maximum=1.0)
        aggregate_drawdown = abs(min(0.0, aggregate_beta_drawdown_pct()))
        if aggregate_drawdown >= aggregate_limit:
            reasons.append(
                {
                    "code": "aggregate_drawdown_pause",
                    "message": f"Aggregate beta drawdown {aggregate_drawdown:.1%} exceeds {aggregate_limit:.1%}.",
                    "actual": aggregate_drawdown,
                    "limit": aggregate_limit,
                }
            )
    if setting_bool("agent_guardrail_cooldown_enabled", True):
        limit = setting_int("agent_guardrail_cooldown_event_limit", 5, minimum=1, maximum=100)
        count = recent_policy_event_count(portfolio_id)
        if count >= limit:
            reasons.append(
                {
                    "code": "guardrail_cooldown",
                    "message": f"Recent guardrail/rejection events {count} exceed {limit}.",
                    "actual": count,
                    "limit": limit,
                }
            )
    return {"allowed": not reasons, "reasons": reasons}


def earnings_blackout_status(ticker: str, *, now: dt.datetime | None = None) -> dict[str, Any]:
    if not setting_bool("earnings_blackout_enabled", True):
        return {"allowed": True, "reason": "earnings_blackout_disabled"}
    days = setting_int("earnings_blackout_days", 2, minimum=0, maximum=30)
    if days <= 0:
        return {"allowed": True, "reason": "earnings_blackout_zero"}
    try:
        from .data import get_next_earnings_date

        earnings = get_next_earnings_date(str(ticker or "").upper())
    except Exception:
        earnings = None
    if earnings is None:
        return {"allowed": True, "reason": "earnings_unknown"}
    current = now or dt.datetime.now(dt.timezone.utc)
    event = earnings if earnings.tzinfo is not None else earnings.replace(tzinfo=dt.timezone.utc)
    days_to_event = (event - current).total_seconds() / 86400.0
    if 0 <= days_to_event <= float(days):
        return {
            "allowed": False,
            "reason": "earnings_blackout",
            "earnings_date": event.isoformat(),
            "days_to_earnings": days_to_event,
            "limit_days": days,
        }
    return {
        "allowed": True,
        "reason": "outside_earnings_blackout",
        "earnings_date": event.isoformat(),
        "days_to_earnings": days_to_event,
        "limit_days": days,
    }


def near_close_cancel_active(now: dt.datetime | None = None) -> bool:
    minutes = setting_int("agent_cancel_before_close_minutes", 15, minimum=0, maximum=120)
    if minutes <= 0:
        return False
    current = (now or dt.datetime.now(dt.timezone.utc)).astimezone(ET)
    close_at = current.replace(hour=16, minute=0, second=0, microsecond=0)
    delta_minutes = (close_at - current).total_seconds() / 60.0
    return 0 <= delta_minutes <= float(minutes)
