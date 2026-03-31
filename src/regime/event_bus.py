from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from typing import Any, Awaitable, Callable

from .events import BaseEvent

logger = logging.getLogger(__name__)

Subscriber = Callable[[BaseEvent], Awaitable[None]]
SyncSubscriber = Callable[[BaseEvent], None]


class AsyncEventBus:
    """
    In-process async event bus with wildcard subscriptions and ring-buffer history.
    """

    def __init__(self, *, max_history: int = 500) -> None:
        self._subscribers: dict[str, list[Subscriber]] = defaultdict(list)
        self._history: list[BaseEvent] = []
        self._max_history = max_history
        self._running = True

    def subscribe(self, event_type: str, callback: Subscriber) -> None:
        subscribers = self._subscribers[event_type]
        if callback not in subscribers:
            subscribers.append(callback)
            logger.debug("Subscriber added for event_type=%s", event_type)

    def unsubscribe(self, event_type: str, callback: Subscriber) -> None:
        subscribers = self._subscribers.get(event_type, [])
        if callback in subscribers:
            subscribers.remove(callback)

    async def publish(self, event: BaseEvent) -> None:
        if not self._running:
            logger.warning("Bus stopped — dropping event %s", event.event_type)
            return

        self._record(event)
        targets = list(self._subscribers.get(event.event_type, []))
        targets.extend(self._subscribers.get("*", []))
        if not targets:
            logger.debug("No subscribers for event_type=%s", event.event_type)
            return

        results = await asyncio.gather(
            *(self._safe_call(callback, event) for callback in targets),
            return_exceptions=True,
        )
        for index, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error("Subscriber %s failed on %s: %s", targets[index].__qualname__, event.event_type, result)

    def publish_sync(self, event: BaseEvent) -> None:
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self.publish(event))
        except RuntimeError:
            asyncio.run(self.publish(event))

    def get_history(self, event_type: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
        events = self._history if event_type is None else [event for event in self._history if event.event_type == event_type]
        return [event.to_dict() for event in events[-limit:]]

    def subscriber_count(self, event_type: str | None = None) -> int:
        if event_type is not None:
            return len(self._subscribers.get(event_type, []))
        return sum(len(subscribers) for subscribers in self._subscribers.values())

    def stop(self) -> None:
        self._running = False
        logger.info("EventBus stopped.")

    def start(self) -> None:
        self._running = True

    async def _safe_call(self, callback: Subscriber, event: BaseEvent) -> None:
        try:
            await callback(event)
        except Exception as exc:
            logger.error("Subscriber %s raised: %s", callback.__qualname__, exc)
            raise

    def _record(self, event: BaseEvent) -> None:
        self._history.append(event)
        if len(self._history) > self._max_history:
            self._history = self._history[-self._max_history :]


_bus_instance: AsyncEventBus | None = None


def get_event_bus() -> AsyncEventBus:
    global _bus_instance
    if _bus_instance is None:
        _bus_instance = AsyncEventBus()
    return _bus_instance


def reset_event_bus() -> None:
    global _bus_instance
    if _bus_instance is not None:
        _bus_instance.stop()
    _bus_instance = None


def register_default_subscribers(bus: AsyncEventBus | None = None) -> None:
    """Wire the built-in audit and logging subscribers."""
    from .event_subscribers import audit_log_subscriber, enriched_signal_logger, trade_intent_logger

    active_bus = bus or get_event_bus()
    active_bus.subscribe("*", audit_log_subscriber)
    active_bus.subscribe("enriched_signal", enriched_signal_logger)
    active_bus.subscribe("trade_intent", trade_intent_logger)
