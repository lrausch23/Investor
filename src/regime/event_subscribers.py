from __future__ import annotations

import logging

from .events import BaseEvent, EnrichedSignalEvent, OrderExecutionEvent, TradeDecisionEvent, TradeIntentEvent
from .persistence import save_alert

logger = logging.getLogger(__name__)


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

    if not isinstance(event, TradeDecisionEvent):
        return
    if event.decision != "approved":
        return
    rationale = str(event.sizing_rationale or "")
    agent_trace = ""
    if "[agents:" in rationale:
        trace_start = rationale.index("[agents:")
        agent_trace = rationale[trace_start:].strip()
        rationale = rationale[:trace_start].strip()
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
            meta_labeler_score=event.meta_labeler_score,
            agent_trace=agent_trace,
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
