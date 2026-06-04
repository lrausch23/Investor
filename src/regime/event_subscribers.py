from __future__ import annotations

import logging
import re

from .events import BaseEvent, EnrichedSignalEvent, OrderExecutionEvent, TradeDecisionEvent, TradeIntentEvent
from .persistence import save_alert

logger = logging.getLogger(__name__)

_HURDLE_RE = re.compile(r"hurdle=(?P<net>-?\d+(?:\.\d+)?)%net\((?P<gross>-?\d+(?:\.\d+)?)%gross@")
_DURATION_RE = re.compile(r"duration=(?P<duration>-?\d+(?:\.\d+)?)d\(min=(?P<minimum>-?\d+(?:\.\d+)?)\)")
_ANTI_CHURN_RE = re.compile(r"churn=pass\(count=(?P<count>\d+),max=(?P<max>\d+)\)")
_LTCG_RE = re.compile(r"ltcg=shield\(protected=(?P<protected>-?\d+(?:\.\d+)?),tax=(?P<tax>-?\d+(?:\.\d+)?),lots=(?P<lots>\d+)\)")


async def audit_log_subscriber(event: BaseEvent) -> None:
    """Persist every bus event as an informational alert for traceability."""
    try:
        save_alert(
            alert_type="bus_event",
            title=f"Event: {event.event_type}",
            severity="info",
            message=f"correlation_id={event.correlation_id}",
            data={"event_type": event.event_type, "correlation_id": event.correlation_id},
        )
    except Exception as exc:
        logger.debug("Audit subscriber failed: %s", exc)


async def enriched_signal_logger(event: BaseEvent) -> None:
    """Log enriched-signal events for operational visibility."""
    if not isinstance(event, EnrichedSignalEvent):
        return
    logger.info(
        "EnrichedSignalEvent: ticker=%s regime=%s action=%s confidence=%.1f ml=%.3f corr=%s",
        event.ticker,
        event.regime_label,
        event.composite_action,
        event.unified_confidence,
        event.meta_labeler_score or 0.0,
        event.correlation_id[:8],
    )


async def trade_intent_logger(event: BaseEvent) -> None:
    """Log trade-intent events."""
    if not isinstance(event, TradeIntentEvent):
        return
    logger.info(
        "TradeIntentEvent: ticker=%s action=%s portfolio=%d source=%s corr=%s",
        event.ticker,
        event.action,
        event.portfolio_id,
        event.source,
        event.correlation_id[:8],
    )


async def trade_decision_subscriber(event: BaseEvent) -> None:
    """Persist approved agent decisions as trade plans."""
    from .persistence import create_trade_plan
    from .order_routing import decide_routing

    if not isinstance(event, TradeDecisionEvent):
        return
    if event.decision != "approved":
        return
    rationale = str(event.sizing_rationale or "")
    agent_trace = ""
    hurdle_gross_return_pct = None
    hurdle_net_return_pct = None
    duration_gate_passed = None
    expected_regime_duration = None
    anti_churn_passed = None
    ltcg_override_active = None
    ltcg_protected_quantity = None
    ltcg_tax_savings = None
    order_type = "limit"
    routing_strategy = ""
    algo_strategy = ""
    arrival_price = float(event.proposed_price) if event.proposed_price is not None else None
    if "[agents:" in rationale:
        trace_start = rationale.index("[agents:")
        agent_trace = rationale[trace_start:].strip()
        rationale = rationale[:trace_start].strip()
    hurdle_match = _HURDLE_RE.search(rationale)
    if hurdle_match:
        hurdle_net_return_pct = float(hurdle_match.group("net"))
        hurdle_gross_return_pct = float(hurdle_match.group("gross"))
    duration_match = _DURATION_RE.search(rationale)
    if duration_match:
        expected_regime_duration = float(duration_match.group("duration"))
        duration_gate_passed = True
    anti_churn_match = _ANTI_CHURN_RE.search(rationale)
    if anti_churn_match:
        anti_churn_passed = True
    ltcg_match = _LTCG_RE.search(rationale)
    if ltcg_match:
        ltcg_override_active = True
        ltcg_protected_quantity = float(ltcg_match.group("protected"))
        ltcg_tax_savings = float(ltcg_match.group("tax"))
    try:
        routing = decide_routing(
            ticker=event.ticker,
            action=event.action,
            quantity=float(event.quantity or 0.0),
            last_price=float(event.proposed_price or 0.0),
            urgency=str(event.urgency or "normal"),
            is_stop_triggered=bool(str(event.source or "").lower() == "exit_signal"),
        )
        order_type = routing.order_type
        routing_strategy = routing.strategy_name
        algo_strategy = routing.algo_strategy
    except Exception:
        logger.debug("trade_decision_subscriber: routing fallback for %s", event.ticker, exc_info=True)
    try:
        create_trade_plan(
            portfolio_id=event.portfolio_id,
            ticker=event.ticker,
            action=event.action,
            quantity=float(event.quantity or 0.0),
            rationale=rationale or f"Agent decision: {event.decision}",
            proposed_price=event.proposed_price,
            regime_label=event.regime_label or None,
            source="discovery" if str(event.action).lower() == "buy" else "exit_signal",
            order_type=order_type,
            routing_strategy=routing_strategy,
            algo_strategy=algo_strategy,
            arrival_price=arrival_price,
            meta_labeler_score=event.meta_labeler_score,
            agent_trace=agent_trace,
            hurdle_gross_return_pct=hurdle_gross_return_pct,
            hurdle_net_return_pct=hurdle_net_return_pct,
            hurdle_passed=True if hurdle_match else None,
            duration_gate_passed=duration_gate_passed,
            expected_regime_duration=expected_regime_duration,
            anti_churn_passed=anti_churn_passed,
            ltcg_override_active=ltcg_override_active,
            ltcg_protected_quantity=ltcg_protected_quantity,
            ltcg_tax_savings=ltcg_tax_savings,
            agent_key=event.agent_key,
            llm_used=event.llm_used,
            llm_influenced=event.llm_influenced,
            llm_influence=event.llm_influence,
            llm_source=event.llm_source,
            llm_provider=event.llm_provider,
            llm_model=event.llm_model,
            llm_model_display=event.llm_model_display,
            llm_verdict=event.llm_verdict,
            llm_confidence=event.llm_confidence,
        )
    except Exception as exc:
        logger.error("trade_decision_subscriber: persistence failed: %s", exc)


async def order_execution_logger(event: BaseEvent) -> None:
    """Log execution outcomes for the agent pipeline."""
    if not isinstance(event, OrderExecutionEvent):
        return
    logger.info(
        "OrderExecutionEvent: ticker=%s action=%s status=%s price=%s corr=%s",
        event.ticker,
        event.action,
        event.status,
        event.filled_price,
        event.correlation_id[:8],
    )
