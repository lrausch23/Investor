from __future__ import annotations

import asyncio
import logging
from typing import Any

from . import AgentBase
from ..events import BaseEvent, EnrichedSignalEvent, FundamentalAssessmentEvent

logger = logging.getLogger(__name__)


class FundamentalAgent(AgentBase):
    META_LABELER_VETO_THRESHOLD = 0.30

    @property
    def name(self) -> str:
        return "fundamental"

    @property
    def subscriptions(self) -> list[str]:
        return []

    async def handle(self, event: BaseEvent) -> None:
        if not isinstance(event, EnrichedSignalEvent):
            return
        if event.source != "quant_agent":
            return
        try:
            assessment = await self.run_for_orchestrator(event)
        except Exception as exc:
            logger.error("FundamentalAgent failed for %s: %s", event.ticker, exc)
            return
        if assessment is not None:
            await self._bus.publish(assessment)

    async def run_for_orchestrator(self, event: EnrichedSignalEvent) -> FundamentalAssessmentEvent | None:
        """Run the qualitative assessment directly for orchestrated sequencing."""
        runtime = self._get_runtime()
        if runtime is None:
            logger.warning("FundamentalAgent skipped %s: runtime unavailable", event.ticker)
            return None
        return await asyncio.to_thread(self._evaluate, runtime, event)

    def _evaluate(self, runtime: dict[str, Any], event: EnrichedSignalEvent) -> FundamentalAssessmentEvent | None:
        if event.meta_labeler_score is not None and float(event.meta_labeler_score) < self.META_LABELER_VETO_THRESHOLD:
            return FundamentalAssessmentEvent(
                correlation_id=event.correlation_id,
                ticker=event.ticker,
                regime_label=event.regime_label,
                verdict="Veto",
                confidence_score=None,
                catalyst_sentiment="",
                vetoed=True,
                veto_reason=f"meta_labeler_score={float(event.meta_labeler_score):.3f} < {self.META_LABELER_VETO_THRESHOLD:.2f}",
                source="quant_veto",
                enriched_signal_id=event.correlation_id,
                meta_labeler_score=event.meta_labeler_score,
                details={"reason": "Meta-labeler score below threshold"},
            )

        gate_enabled = str(runtime["get_setting"]("fundamental_gate_enabled") or "true").lower() == "true" if callable(runtime.get("get_setting")) else True
        if gate_enabled:
            try:
                from ..fundamental_gating import get_fundamental_gate_settings, run_fundamental_gate

                settings = get_fundamental_gate_settings()
                gate = run_fundamental_gate(
                    event.ticker,
                    piotroski_min=int(settings["piotroski_min"]),
                    require_roic_above_wacc=bool(settings["require_roic_above_wacc"]),
                    roic_lookback_years=int(settings["roic_lookback_years"]),
                    pass_on_insufficient_data=bool(settings["pass_on_insufficient_data"]),
                )
                if not gate.passed:
                    return FundamentalAssessmentEvent(
                        correlation_id=event.correlation_id,
                        ticker=event.ticker,
                        regime_label=event.regime_label,
                        verdict="Veto",
                        confidence_score=None,
                        catalyst_sentiment="",
                        vetoed=True,
                        veto_reason=f"fundamental_gate: {'; '.join(gate.veto_reasons)}",
                        source="fundamental_gating",
                        enriched_signal_id=event.correlation_id,
                        meta_labeler_score=event.meta_labeler_score,
                        details={
                            "piotroski_score": gate.piotroski.score if gate.piotroski else None,
                            "roic_avg": gate.roic.roic_avg if gate.roic else None,
                            "wacc": gate.roic.wacc_estimate if gate.roic else None,
                            "veto_reasons": gate.veto_reasons,
                        },
                    )
            except Exception as exc:
                logger.warning("Fundamental gate failed for %s; proceeding to LLM: %s", event.ticker, exc)

        frontier_provider = str(runtime["get_setting"]("frontier_provider") or "auto") if callable(runtime.get("get_setting")) else "auto"
        qualitative = runtime["build_qualitative_assessment"](
            ticker=event.ticker,
            regime_signal=event.composite_action or event.regime_label,
            state_name=event.regime_label,
            latest_probability=float(event.regime_probability or 0.0),
            context_symbols=[event.benchmark] if event.benchmark else None,
            frontier_enabled=True,
            frontier_provider=frontier_provider,
            meta_labeler_score=event.meta_labeler_score,
        )
        llm_response = getattr(qualitative, "llm_response", None) or {}
        institutional = llm_response.get("institutional_report", {}) if isinstance(llm_response, dict) else {}
        verdict = str(institutional.get("verdict") or llm_response.get("verdict") or "")
        confidence_score = institutional.get("confidence_score")
        try:
            confidence_value = int(confidence_score) if confidence_score is not None else None
        except Exception:
            confidence_value = None
        return FundamentalAssessmentEvent(
            correlation_id=event.correlation_id,
            ticker=event.ticker,
            regime_label=event.regime_label,
            verdict=verdict,
            confidence_score=confidence_value,
            catalyst_sentiment=str(getattr(qualitative, "catalyst_sentiment", "") or ""),
            vetoed=False,
            veto_reason=None,
            source=str(getattr(event, "source", "") or "quant_agent"),
            enriched_signal_id=event.correlation_id,
            meta_labeler_score=event.meta_labeler_score,
            details={
                "source": str(getattr(qualitative, "source", "") or ""),
                "catalysts": getattr(qualitative, "catalysts", []) or [],
                "llm_response": llm_response,
            },
        )
