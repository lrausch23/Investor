from __future__ import annotations

import asyncio
import logging
import inspect
from dataclasses import asdict, dataclass
from typing import Any, cast

from . import AgentBase, get_agent_registry
from ..agent_frontier import agent_key_for_portfolio_id
from ..events import BaseEvent, EnrichedSignalEvent, FundamentalAssessmentEvent, TradeDecisionEvent

logger = logging.getLogger(__name__)


@dataclass
class OrchestratorConfig:
    """Configurable sequencing and timeout behavior for the agent pipeline."""

    fundamental_timeout_seconds: float = 30.0
    portfolio_timeout_seconds: float = 10.0
    skip_fundamental_on_timeout: bool = True
    fundamental_veto_respected: bool = True


class AgentOrchestrator(AgentBase):
    """Own the enriched-signal -> fundamental -> portfolio decision sequence."""

    def __init__(self, bus, *, config: OrchestratorConfig | None = None, enabled: bool = True) -> None:
        super().__init__(bus, enabled=enabled)
        self.config = config or OrchestratorConfig()

    @property
    def name(self) -> str:
        return "orchestrator"

    @property
    def subscriptions(self) -> list[str]:
        return ["enriched_signal"]

    async def handle(self, event: BaseEvent) -> None:
        if not isinstance(event, EnrichedSignalEvent):
            return
        if event.source != "quant_agent":
            return
        portfolio_contexts = self._portfolio_contexts()
        if len(portfolio_contexts) <= 1:
            fundamental_result = await self._run_fundamental(event)
            decisions = await self._run_portfolio(event, fundamental_result)
            for decision in decisions:
                await self._bus.publish(decision)
            return

        for portfolio_id, agent_key in portfolio_contexts:
            fundamental_result = await self._run_fundamental(event, portfolio_id=portfolio_id, agent_key=agent_key)
            decisions = await self._run_portfolio(event, fundamental_result, portfolio_id=portfolio_id)
            for decision in decisions:
                await self._bus.publish(decision)

    def _portfolio_contexts(self) -> list[tuple[int, str]]:
        runtime = self._get_runtime()
        if runtime is None or not callable(runtime.get("list_paper_portfolios")):
            return []
        try:
            portfolios = [
                row for row in runtime["list_paper_portfolios"](include_closed=False)
                if str(row.get("status") or "") == "Active"
            ]
        except Exception:
            return []
        contexts: list[tuple[int, str]] = []
        get_setting_fn = runtime["get_setting"] if callable(runtime.get("get_setting")) else lambda _key: None
        for portfolio in portfolios:
            try:
                portfolio_id = int(portfolio.get("id") or 0)
            except Exception:
                continue
            if portfolio_id <= 0:
                continue
            contexts.append((portfolio_id, agent_key_for_portfolio_id(portfolio_id, get_setting_fn=get_setting_fn)))
        return contexts

    async def _run_fundamental(
        self,
        event: EnrichedSignalEvent,
        *,
        portfolio_id: int | None = None,
        agent_key: str = "",
    ) -> FundamentalAssessmentEvent | None:
        registry = get_agent_registry()
        agent = registry.get("fundamental")
        if agent is None or not agent.enabled:
            logger.debug("Orchestrator: fundamental agent unavailable for %s", event.ticker)
            return None
        run_for_orchestrator = getattr(agent, "run_for_orchestrator", None)
        if not callable(run_for_orchestrator):
            logger.debug("Orchestrator: fundamental agent missing run_for_orchestrator for %s", event.ticker)
            return None
        try:
            kwargs = {"portfolio_id": portfolio_id, "agent_key": agent_key} if portfolio_id is not None or agent_key else {}
            result = cast(
                FundamentalAssessmentEvent | None,
                await asyncio.wait_for(
                    _invoke_agent_method(cast(Any, run_for_orchestrator), event, **kwargs),
                    timeout=float(self.config.fundamental_timeout_seconds),
                ),
            )
            if result is not None:
                await self._bus.publish(result)
            return result
        except asyncio.TimeoutError:
            logger.warning(
                "Orchestrator: fundamental agent timed out after %.1fs for %s",
                float(self.config.fundamental_timeout_seconds),
                event.ticker,
            )
            if not self.config.skip_fundamental_on_timeout:
                return None
            timeout_result = FundamentalAssessmentEvent(
                correlation_id=event.correlation_id,
                ticker=event.ticker,
                regime_label=event.regime_label,
                verdict="timeout",
                confidence_score=None,
                catalyst_sentiment="",
                vetoed=False,
                veto_reason="LLM timeout",
                source="timeout_fallback",
                enriched_signal_id=event.correlation_id,
                meta_labeler_score=event.meta_labeler_score,
                portfolio_id=portfolio_id,
                agent_key=agent_key,
                llm_used=False,
                llm_influenced=False,
                llm_influence="timeout",
                llm_source="timeout_fallback",
                details={"reason": "Fundamental agent timed out"},
            )
            await self._bus.publish(timeout_result)
            return timeout_result
        except Exception as exc:
            logger.error("Orchestrator: fundamental agent failed for %s: %s", event.ticker, exc)
            return None

    async def _run_portfolio(
        self,
        event: EnrichedSignalEvent,
        fundamental: FundamentalAssessmentEvent | None,
        *,
        portfolio_id: int | None = None,
    ) -> list[TradeDecisionEvent]:
        registry = get_agent_registry()
        agent = registry.get("portfolio_tax")
        if agent is None or not agent.enabled:
            logger.debug("Orchestrator: portfolio agent unavailable for %s", event.ticker)
            return []
        run_for_orchestrator = getattr(agent, "run_for_orchestrator", None)
        if not callable(run_for_orchestrator):
            logger.debug("Orchestrator: portfolio agent missing run_for_orchestrator for %s", event.ticker)
            return []
        fundamental_context = fundamental if self.config.fundamental_veto_respected else None
        try:
            kwargs = {"portfolio_id": portfolio_id} if portfolio_id is not None else {}
            decisions = cast(
                list[TradeDecisionEvent],
                await asyncio.wait_for(
                    _invoke_agent_method(cast(Any, run_for_orchestrator), event, fundamental_context, **kwargs),
                    timeout=float(self.config.portfolio_timeout_seconds),
                ),
            )
            return [self._add_agent_trace(decision, event, fundamental_context) for decision in (decisions or [])]
        except asyncio.TimeoutError:
            logger.warning(
                "Orchestrator: portfolio agent timed out after %.1fs for %s",
                float(self.config.portfolio_timeout_seconds),
                event.ticker,
            )
            return []
        except Exception as exc:
            logger.error("Orchestrator: portfolio agent failed for %s: %s", event.ticker, exc)
            return []

    def _add_agent_trace(
        self,
        decision: TradeDecisionEvent,
        signal: EnrichedSignalEvent,
        fundamental: FundamentalAssessmentEvent | None,
    ) -> TradeDecisionEvent:
        trace_parts = [f"quant:signal={signal.composite_action}"]
        if fundamental is not None:
            trace_parts.append(
                f"fundamental:verdict={fundamental.verdict},vetoed={str(bool(fundamental.vetoed)).lower()}"
            )
            if getattr(fundamental, "llm_provider", "") or getattr(fundamental, "llm_model_display", ""):
                trace_parts.append(
                    "llm:"
                    f"provider={getattr(fundamental, 'llm_provider', '')},"
                    f"model={getattr(fundamental, 'llm_model_display', '') or getattr(fundamental, 'llm_model', '')},"
                    f"used={str(bool(getattr(fundamental, 'llm_used', False))).lower()},"
                    f"influence={getattr(fundamental, 'llm_influence', '')}"
                )
        trace_parts.append(f"portfolio:decision={decision.decision}")
        agent_trace = " | ".join(trace_parts)
        rationale = str(decision.sizing_rationale or "").strip()
        combined = f"{rationale} [agents: {agent_trace}]".strip() if rationale else f"[agents: {agent_trace}]"
        payload = asdict(decision)
        payload.pop("event_type", None)
        payload["sizing_rationale"] = combined
        return TradeDecisionEvent(**payload)


async def _invoke_agent_method(method: Any, *args: Any, **kwargs: Any) -> Any:
    if not kwargs:
        return await method(*args)
    try:
        signature = inspect.signature(method)
        accepts_kwargs = any(param.kind == inspect.Parameter.VAR_KEYWORD for param in signature.parameters.values())
        allowed = {key for key in signature.parameters if key in kwargs}
        if accepts_kwargs:
            return await method(*args, **kwargs)
        if allowed:
            return await method(*args, **{key: kwargs[key] for key in allowed})
    except (TypeError, ValueError):
        pass
    return await method(*args)
