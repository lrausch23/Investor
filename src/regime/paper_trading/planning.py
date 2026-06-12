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

from .sizing import allocate_budget, _risk_adjusted_quantity

def _publish_ltcg_override_events(
    portfolio_id: int,
    ticker: str,
    original_stop: float | None,
    ltcg_result: Any,
) -> None:
    if not getattr(ltcg_result, "override_active", False):
        return
    try:
        from ..event_bus import get_event_bus
        from ..events import BarrierOverrideEvent

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


def _timeframe_days_from_sources(*sources: dict[str, Any] | None) -> int:
    for source in sources:
        if not isinstance(source, dict):
            continue
        for key in ("timeframe_days", "expected_regime_duration"):
            value = _positive_float(source.get(key))
            if value is not None:
                return max(1, int(round(value)))
    return DEFAULT_EXIT_TIME_STOP_DAYS


def _actual_fill_trade_geometry(
    actual_entry: float,
    *,
    signal_row: dict[str, Any] | None = None,
    atr_14: float | None = None,
    atr_multiplier: float = DEFAULT_SIZING_ATR_MULTIPLIER,
) -> dict[str, Any]:
    entry = _positive_float(actual_entry)
    if entry is None:
        return {
            "stop_price": None,
            "target_price": None,
            "risk_reward_ratio": None,
            "timeframe_days": DEFAULT_EXIT_TIME_STOP_DAYS,
            "trade_geometry_source": "missing_entry",
        }
    row = dict(signal_row or {})
    targets = row.get("price_targets") if isinstance(row.get("price_targets"), dict) else {}
    atr = _positive_float(atr_14) or _positive_float(row.get("atr_14"))
    suggested_stop = (
        _positive_float(row.get("suggested_stop_price"))
        or _positive_float(row.get("stop_price"))
        or _positive_float(targets.get("stop_price"))
    )
    suggested_target = (
        _positive_float(row.get("suggested_exit_price"))
        or _positive_float(row.get("target_price"))
        or _positive_float(row.get("exit_price"))
        or _positive_float(targets.get("exit_price"))
        or _positive_float(targets.get("target_price"))
    )
    source = "actual_fill_atr"
    if atr is not None:
        risk = atr * max(0.1, float(atr_multiplier or DEFAULT_SIZING_ATR_MULTIPLIER))
        stop = max(0.01, entry - risk)
    elif suggested_stop is not None and suggested_stop < entry:
        stop = suggested_stop
        risk = entry - stop
        source = "actual_fill_existing_stop"
    else:
        risk = max(entry * 0.05, 0.01)
        stop = max(0.01, entry - risk)
        source = "actual_fill_conservative_pct"
    target = suggested_target if suggested_target is not None and suggested_target > entry else entry + risk
    reward = max(target - entry, 0.0)
    risk_reward = (reward / risk) if risk > 0 else None
    return {
        "stop_price": round(float(stop), 4),
        "target_price": round(float(target), 4),
        "risk_reward_ratio": round(float(risk_reward), 4) if risk_reward is not None else None,
        "timeframe_days": _timeframe_days_from_sources(row, targets),
        "trade_geometry_source": source,
    }


def generate_buy_plans(
    portfolio_id: int,
    *,
    config: PaperTradingConfig = DEFAULT_PAPER_TRADING_CONFIG,
) -> list[dict[str, Any]]:
    from ..anti_churn import check_anti_churn, get_anti_churn_settings
    from ..meta_labeler import meta_labeler_gate_enabled
    from ..slippage import estimate_execution_cost
    from ..vix_freeze import is_vix_frozen

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
        snapshot = get_latest_signal_snapshot(ticker, max_age_days=_entry_signal_max_age_days()) or {}
        if not snapshot:
            logger.info("Signal freshness gate skipped discovery buy %s: no signal snapshot within freshness window.", ticker)
            continue
        if _truthy(snapshot.get("regime_ambiguous")):
            logger.info("Ambiguity gate blocked %s: multi-seed HMM agreement below threshold.", ticker)
            continue
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
        ml_size_note: str | None = None
        if quantity > 0 and not meta_labeler_gate_enabled(get_setting):
            # size_only veto mode: the calibrated probability never blocks an
            # entry but scales its size 0.5-1.0x (mirrors compute_position_size
            # and the pipeline-backtest harness). Gate mode is unchanged.
            ml_probability = item.get("meta_labeler_probability")
            if ml_probability is None:
                ml_probability = snapshot.get("meta_labeler_probability")
            if ml_probability is not None:
                try:
                    clamped = max(0.0, min(1.0, float(ml_probability)))
                    scaled = math.floor(quantity * (0.5 + 0.5 * clamped))
                    if scaled != quantity:
                        ml_size_note = f"ML size scaling: {0.5 + 0.5 * clamped:.2f}x ({quantity} -> {scaled} shares)."
                    quantity = scaled
                except (TypeError, ValueError):
                    pass
        if quantity <= 0:
            continue
        routing = decide_routing(
            ticker=ticker,
            action="Buy",
            quantity=quantity,
            last_price=current_price,
            urgency="patient",
        )
        routed_price = float(routing.limit_price or current_price)
        geometry = _actual_fill_trade_geometry(
            routed_price,
            signal_row=signal_row,
            atr_14=atr_14,
            atr_multiplier=atr_multiplier,
        )
        exit_price = geometry.get("target_price")
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
                routed_price,
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
        if ml_size_note:
            rationale = f"{rationale} {ml_size_note}"
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
                stop_price=geometry.get("stop_price"),
                target_price=geometry.get("target_price"),
                risk_reward_ratio=geometry.get("risk_reward_ratio"),
                timeframe_days=geometry.get("timeframe_days"),
                trade_geometry_source=str(geometry.get("trade_geometry_source") or ""),
                sizing_method="risk_budget" if sizing_method == "risk_budget" else "equal_dollar",
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
        atr_14 = _lookup_atr(ticker)
        geometry = _actual_fill_trade_geometry(
            float(routing.limit_price or proposed_price),
            signal_row={**row, "price_targets": price_targets},
            atr_14=atr_14,
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
                stop_price=geometry.get("stop_price"),
                target_price=geometry.get("target_price"),
                risk_reward_ratio=geometry.get("risk_reward_ratio"),
                timeframe_days=geometry.get("timeframe_days"),
                trade_geometry_source=str(geometry.get("trade_geometry_source") or ""),
                **_signal_quality_plan_kwargs(quality),
            )
        )
        pending_buy_keys.add(key)
    return created


def _deterministic_exit_quality(reason: str, current_price: float | None, reference_price: float | None = None) -> SignalQuality:
    return SignalQuality(
        action="Sell",
        score=100.0,
        grade="actionable",
        actionable=True,
        reasons=(reason,),
        warnings=(),
        blockers=(),
        current_price=current_price,
        reference_price=reference_price,
    )


def _position_time_stop_days(position: dict[str, Any]) -> int:
    return _timeframe_days_from_sources(position)


def _position_risk_reward(position: dict[str, Any], stop_price: float | None, target_price: float | None) -> float | None:
    entry = _positive_float(position.get("entry_price"))
    stop = _positive_float(stop_price)
    target = _positive_float(target_price)
    if entry is None or stop is None or target is None or entry <= stop:
        return None
    risk = entry - stop
    reward = target - entry
    if risk <= 0 or reward <= 0:
        return None
    return round(reward / risk, 4)


def trailing_stop_level(
    *,
    entry_price: float | None,
    current_price: float | None,
    atr_14: float | None,
    existing_stop: float | None = None,
    atr_multiplier: float = DEFAULT_SIZING_ATR_MULTIPLIER,
    activation_atr: float = TRAILING_STOP_ACTIVATION_ATR,
) -> float | None:
    current = _positive_float(current_price)
    entry = _positive_float(entry_price)
    atr = _positive_float(atr_14)
    stop = _positive_float(existing_stop)
    if current is None or entry is None or atr is None:
        return stop
    if current <= entry + (max(0.0, float(activation_atr or 0.0)) * atr):
        return stop
    candidate = max(0.01, current - (max(0.1, float(atr_multiplier or DEFAULT_SIZING_ATR_MULTIPLIER)) * atr))
    if candidate >= current:
        return stop
    if stop is not None and candidate <= stop + 0.005:
        return stop
    return round(candidate, 4)


def _ratchet_trailing_stop(position: dict[str, Any], current_price: float | None, atr_14: float | None) -> float | None:
    candidate = trailing_stop_level(
        entry_price=position.get("entry_price"),
        current_price=current_price,
        atr_14=atr_14,
        existing_stop=position.get("stop_price"),
        activation_atr=TRAILING_STOP_ACTIVATION_ATR,
    )
    existing_stop = _positive_float(position.get("stop_price"))
    if candidate is None or candidate == existing_stop:
        return existing_stop
    updated = update_paper_position_risk(int(position["id"]), stop_price=round(candidate, 4))
    if updated and updated.get("stop_price") is not None:
        return _positive_float(updated.get("stop_price"))
    return candidate


def _neutral_reduce_reason(row: dict[str, Any]) -> str | None:
    regime_label = str(row.get("regime") or row.get("regime_label") or "").strip()
    if regime_label != "Neutral":
        return None
    action = str(row.get("composite_signal") or row.get("action") or "").strip()
    if action in {"Reduce", "Take partial profits"}:
        return f"Cached composite signal is {action} in a Neutral regime."
    previous = str(row.get("previous_regime") or row.get("prior_regime") or "").strip()

    def _probability(*keys: str) -> float | None:
        # 0.0 is a valid (maximally bearish/bullish) probability; _positive_float
        # would coerce it to None and silently disable the comparisons below.
        for key in keys:
            value = row.get(key)
            if value in (None, ""):
                continue
            try:
                parsed = float(value)
            except (TypeError, ValueError):
                continue
            if math.isfinite(parsed) and 0.0 <= parsed <= 1.0:
                return parsed
        return None

    p_bull = _probability("p_bull_day5", "bull_probability_day5", "bull_day5")
    p_bear = _probability("p_bear_day5", "bear_probability_day5", "bear_day5")
    if previous == "Bull" and (p_bull is None or p_bull < 0.55):
        detail = f" with day-5 Bull probability {p_bull:.0%}" if p_bull is not None else ""
        return f"Regime deteriorated from Bull to Neutral{detail}."
    if p_bull is not None and p_bull < 0.45:
        return f"Neutral regime has weak forward Bull probability ({p_bull:.0%})."
    if p_bull is not None and p_bear is not None and p_bear > p_bull:
        return f"Neutral regime has Bear probability above Bull probability ({p_bear:.0%} vs {p_bull:.0%})."
    return None


def _reduced_exit_quantity(quantity: float, fraction: float = DEFAULT_NEUTRAL_REDUCE_FRACTION) -> float:
    if quantity <= 0:
        return 0.0
    if quantity < 1:
        return quantity * max(0.0, min(1.0, fraction))
    return max(1.0, math.floor(quantity * max(0.0, min(1.0, fraction))))


def generate_exit_plans(
    portfolio_id: int,
    *,
    cached_regime: CachedRegimeMap | dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    from ..ltcg_override import check_ltcg_override, get_ltcg_override_settings

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
        current_price = _positive_float(prices.get(ticker))
        stop_price = _positive_float(position.get("stop_price")) or 0.0
        target_price = _positive_float(position.get("target_price")) or 0.0
        exit_timeframe_days = _position_time_stop_days(position)
        trigger_reason: str | None = None
        is_stop_triggered = False
        quantity_fraction = 1.0
        regime_label: str | None = None
        regime_probability: float | None = None
        signal_quality: SignalQuality | None = None
        if current_price is not None and target_price > 0 and current_price >= target_price:
            trigger_reason = f"Profit target hit (${current_price:.2f} >= ${target_price:.2f})."
            signal_quality = _deterministic_exit_quality("Profit target is triggered.", current_price, target_price)
        if trigger_reason is None and current_price is not None:
            atr_14 = _lookup_atr(ticker)
            ratcheted_stop = _ratchet_trailing_stop(position, current_price, atr_14)
            if ratcheted_stop is not None:
                stop_price = float(ratcheted_stop)
            if stop_price > 0 and current_price <= stop_price:
                trigger_reason = f"Stop price hit (${current_price:.2f} <= ${stop_price:.2f})."
                is_stop_triggered = True
                signal_quality = _deterministic_exit_quality("Stop price is triggered.", current_price, stop_price)
        if trigger_reason is None and _holding_days(position) >= exit_timeframe_days:
            trigger_reason = f"Time stop reached ({_holding_days(position)}d >= {exit_timeframe_days}d)."
            signal_quality = _deterministic_exit_quality("Time stop is triggered.", current_price, target_price or None)
        row = cached_rows.get(ticker)
        if row:
            regime_label = str(row.get("regime") or "").strip() or None
            try:
                regime_probability = float(row.get("probability")) if row.get("probability") is not None else None
            except Exception:
                regime_probability = None
            action = str(row.get("composite_signal") or "").strip()
            cached_trigger_reason: str | None = None
            cached_quantity_fraction = 1.0
            if regime_label == "Bear":
                cached_trigger_reason = "Cached regime is Bear."
            if cached_trigger_reason is None and action in {"Sell", "Strong Sell"}:
                cached_trigger_reason = f"Cached composite signal is {action}."
            if cached_trigger_reason is None:
                neutral_reduce_reason = _neutral_reduce_reason(row)
                if neutral_reduce_reason is not None:
                    cached_trigger_reason = neutral_reduce_reason
                    cached_quantity_fraction = DEFAULT_NEUTRAL_REDUCE_FRACTION
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
                    quantity_fraction = cached_quantity_fraction
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
        if quantity_fraction < 1.0:
            quantity = _reduced_exit_quantity(quantity, quantity_fraction)
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
                quantity = min(quantity, float(ltcg_result.sellable_quantity))
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
                stop_price=stop_price if stop_price > 0 else None,
                target_price=target_price if target_price > 0 else None,
                risk_reward_ratio=_position_risk_reward(position, stop_price if stop_price > 0 else None, target_price if target_price > 0 else None),
                timeframe_days=exit_timeframe_days,
                trade_geometry_source="exit_management",
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
