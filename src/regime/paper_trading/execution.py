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


def execute_approved_plans(portfolio_id: int) -> dict[str, Any]:
    adapter = PaperBrokerAdapter(portfolio_id)
    return execute_approved_plans_via_adapter(portfolio_id, adapter)


def auto_approve_plans(portfolio_id: int) -> dict[str, Any]:
    from ..vix_freeze import is_vix_frozen

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
    try:
        from ..meta_labeler import meta_labeler_gate_enabled

        meta_gate_enabled = meta_labeler_gate_enabled(get_setting)
    except Exception:
        meta_gate_enabled = True

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
            if mode == "semi_auto" and meta_gate_enabled:
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
            stop_price=_positive_float(plan.get("stop_price")),
            target_price=_positive_float(plan.get("target_price")),
            time_in_force=_routing_time_in_force_from_plan(plan),
            routing_strategy=str(plan.get("routing_strategy") or ""),
            algo_strategy=str(plan.get("algo_strategy") or ""),
            theme_id=int(plan["theme_id"]) if plan.get("theme_id") is not None else None,
            source=str(plan.get("source") or "manual"),
            notes=str(plan.get("rationale") or ""),
        )
        guardrail = validate_guardrails(order, adapter, guardrails=DEFAULT_RISK_GUARDRAILS)
        if not guardrail.allowed:
            failures, checks = _guardrail_block_details(guardrail)
            reason = "; ".join(failures) or "Blocked by guardrails."
            blocked += 1
            details.append(
                {
                    "plan_id": plan_id,
                    "ticker": ticker,
                    "action": action,
                    "result": "blocked_guardrail",
                    "reason": reason,
                    "guardrail_failures": failures,
                    "guardrail_checks": checks,
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

    from ..vix_freeze import is_vix_frozen

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
        stop_price = plan.get("stop_price")
        target_price = plan.get("target_price")
        role = str(plan.get("role") or "Critical-Path")
        if related_watchlist:
            latest = related_watchlist[0]
            if stop_price in (None, ""):
                stop_price = latest.get("suggested_stop_price")
            if target_price in (None, ""):
                target_price = latest.get("suggested_exit_price") or latest.get("target_price")
            role = str(latest.get("suggested_role") or role)
        order = OrderRequest(
            portfolio_id=portfolio_id,
            ticker=ticker,
            action=action,
            quantity=quantity,
            order_type=str(plan.get("order_type") or "limit"),
            limit_price=float(plan.get("proposed_price") or 0.0) or None,
            stop_price=_positive_float(stop_price),
            target_price=_positive_float(target_price),
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
            stop_price=_positive_float(plan.get("stop_price")),
            target_price=_positive_float(plan.get("target_price")),
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
            update_paper_position_quantity(int(position["id"]), pos_qty - remaining)
            credited += remaining * fill_price
            remaining = 0.0
            break
        close_paper_position(int(position["id"]), fill_price, now_text, str(plan.get("source") or "broker_adapter"))
        credited += pos_qty * fill_price
        remaining -= pos_qty
    if credited > 0:
        update_paper_portfolio(portfolio_id, current_cash=current_cash + credited)
