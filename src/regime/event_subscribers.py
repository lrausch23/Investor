from __future__ import annotations

import logging

from .events import BaseEvent, EnrichedSignalEvent, TradeIntentEvent
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
