from __future__ import annotations

from abc import ABC, abstractmethod
import logging
from typing import Any, Callable

from ..event_bus import AsyncEventBus
from ..events import BaseEvent

logger = logging.getLogger(__name__)

RuntimeLoader = Callable[[], tuple[dict[str, Any] | None, str | None]]


class AgentBase(ABC):
    """Base class for event-driven regime agents."""

    def __init__(
        self,
        bus: AsyncEventBus,
        *,
        enabled: bool = True,
        runtime: dict[str, Any] | None = None,
        runtime_loader: RuntimeLoader | None = None,
    ) -> None:
        self._bus = bus
        self.enabled = bool(enabled)
        self._runtime = runtime
        self._runtime_loader = runtime_loader
        self._registered = False
        self._subscriber = self._dispatch

    @property
    @abstractmethod
    def name(self) -> str:
        ...

    @property
    @abstractmethod
    def subscriptions(self) -> list[str]:
        ...

    @abstractmethod
    async def handle(self, event: BaseEvent) -> None:
        ...

    def register(self) -> None:
        if self._registered:
            return
        for event_type in self.subscriptions:
            self._bus.subscribe(event_type, self._subscriber)
        self._registered = True

    def set_runtime(self, runtime: dict[str, Any] | None) -> None:
        self._runtime = runtime

    def _get_runtime(self) -> dict[str, Any] | None:
        if self._runtime is not None:
            return self._runtime
        if self._runtime_loader is None:
            return None
        try:
            runtime, _error = self._runtime_loader()
            self._runtime = runtime
        except Exception:
            logger.debug("%s runtime load failed", self.name, exc_info=True)
            self._runtime = None
        return self._runtime

    async def _dispatch(self, event: BaseEvent) -> None:
        if not self.enabled:
            return
        await self.handle(event)


class AgentRegistry:
    """Singleton registry for the runtime agent topology."""

    def __init__(self) -> None:
        self._agents: dict[str, AgentBase] = {}

    def register(self, agent: AgentBase) -> AgentBase:
        existing = self._agents.get(agent.name)
        if existing is not None:
            return existing
        agent.register()
        self._agents[agent.name] = agent
        return agent

    def get(self, name: str) -> AgentBase | None:
        return self._agents.get(name)

    def all_agents(self) -> list[AgentBase]:
        return list(self._agents.values())

    def status(self) -> list[dict[str, Any]]:
        return [
            {
                "name": agent.name,
                "enabled": bool(agent.enabled),
                "subscriptions": list(agent.subscriptions),
            }
            for agent in self.all_agents()
        ]


_agent_registry: AgentRegistry | None = None


def get_agent_registry() -> AgentRegistry:
    global _agent_registry
    if _agent_registry is None:
        _agent_registry = AgentRegistry()
    return _agent_registry


def reset_agent_registry() -> None:
    global _agent_registry
    _agent_registry = None


__all__ = [
    "AgentBase",
    "AgentRegistry",
    "RuntimeLoader",
    "get_agent_registry",
    "reset_agent_registry",
]
