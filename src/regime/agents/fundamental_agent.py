from __future__ import annotations

import asyncio
import logging
from typing import Any

from . import AgentBase
from ..agent_frontier import get_agent_frontier_config
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

    async def run_for_orchestrator(
        self,
        event: EnrichedSignalEvent,
        *,
        portfolio_id: int | None = None,
        agent_key: str = "",
    ) -> FundamentalAssessmentEvent | None:
        """Run the qualitative assessment directly for orchestrated sequencing."""
        runtime = self._get_runtime()
        if runtime is None:
            logger.warning("FundamentalAgent skipped %s: runtime unavailable", event.ticker)
            return None
        return await asyncio.to_thread(self._evaluate, runtime, event, portfolio_id, agent_key)

    def _evaluate(
        self,
        runtime: dict[str, Any],
        event: EnrichedSignalEvent,
        portfolio_id: int | None = None,
        agent_key: str = "",
    ) -> FundamentalAssessmentEvent | None:
        agent_config = get_agent_frontier_config(
            agent_key=agent_key,
            portfolio_id=portfolio_id,
            get_setting_fn=runtime["get_setting"] if callable(runtime.get("get_setting")) else lambda _key: None,
        )
        resolved_agent_key = str(agent_config.get("agent_key") or agent_key or "")
        try:
            from ..meta_labeler import meta_labeler_gate_enabled

            meta_gate_enabled = meta_labeler_gate_enabled(runtime.get("get_setting"))
        except Exception:
            meta_gate_enabled = True
        if meta_gate_enabled and event.meta_labeler_score is not None and float(event.meta_labeler_score) < self.META_LABELER_VETO_THRESHOLD:
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
                portfolio_id=portfolio_id,
                agent_key=resolved_agent_key,
                llm_used=False,
                llm_influenced=False,
                llm_influence="meta_labeler_veto",
                llm_source="quant_veto",
                llm_provider=str(agent_config.get("provider") or ""),
                llm_model=str(agent_config.get("model") or ""),
                llm_model_display=str(agent_config.get("configured_model") or ""),
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
                    altman_z_enabled=bool(settings.get("altman_z_enabled", True)),
                    altman_z_distress_threshold=float(settings.get("altman_z_distress_threshold", 1.81)),
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
                            "altman_z_score": gate.altman_z.z_score if gate.altman_z else None,
                            "altman_z_interpretation": gate.altman_z.interpretation if gate.altman_z else "",
                            "veto_reasons": gate.veto_reasons,
                        },
                        portfolio_id=portfolio_id,
                        agent_key=resolved_agent_key,
                        llm_used=False,
                        llm_influenced=False,
                        llm_influence="fundamental_gate_veto",
                        llm_source="fundamental_gating",
                        llm_provider=str(agent_config.get("provider") or ""),
                        llm_model=str(agent_config.get("model") or ""),
                        llm_model_display=str(agent_config.get("configured_model") or ""),
                    )
            except Exception as exc:
                logger.warning("Fundamental gate failed for %s; proceeding to LLM: %s", event.ticker, exc)

        frontier_provider = str(agent_config.get("provider") or "auto")
        frontier_model = str(agent_config.get("model") or "")
        qualitative = runtime["build_qualitative_assessment"](
            ticker=event.ticker,
            regime_signal=event.composite_action or event.regime_label,
            state_name=event.regime_label,
            latest_probability=float(event.regime_probability or 0.0),
            context_symbols=[event.benchmark] if event.benchmark else None,
            frontier_enabled=True,
            frontier_provider=frontier_provider,
            frontier_model=frontier_model or None,
            meta_labeler_score=event.meta_labeler_score,
        )
        llm_response = getattr(qualitative, "llm_response", None) or {}
        institutional = llm_response.get("institutional_report", {}) if isinstance(llm_response, dict) else {}
        verdict = str(institutional.get("verdict") or llm_response.get("verdict") or "")
        confidence_score = institutional.get("confidence_score")
        moat_classification = str(
            institutional.get("moat_classification")
            or llm_response.get("moat_classification")
            or ""
        )
        moat_justification = str(
            institutional.get("moat_justification")
            or llm_response.get("moat_justification")
            or ""
        )
        try:
            confidence_value = int(confidence_score) if confidence_score is not None else None
        except Exception:
            confidence_value = None
        qualitative_source = str(getattr(qualitative, "source", "") or "")
        llm_used = bool(getattr(qualitative, "llm_used", False) or qualitative_source == "llm")
        model_display = str(getattr(qualitative, "model_name", "") or agent_config.get("configured_model") or "")
        llm_payload: dict[str, Any] = {
            "portfolio_id": portfolio_id,
            "agent_key": resolved_agent_key,
            "llm_used": llm_used,
            "llm_source": qualitative_source,
            "llm_provider": str(getattr(qualitative, "frontier_provider", "") or frontier_provider),
            "llm_model": str(getattr(qualitative, "frontier_model", "") or frontier_model),
            "llm_model_display": model_display,
            "llm_verdict": verdict,
            "llm_confidence": float(confidence_value) if confidence_value is not None else None,
        }
        moat_veto = False
        if gate_enabled and moat_classification.lower() in {"", "none"}:
            moat_veto = True
        if moat_veto:
            return FundamentalAssessmentEvent(
                correlation_id=event.correlation_id,
                ticker=event.ticker,
                regime_label=event.regime_label,
                verdict="Veto",
                confidence_score=None,
                catalyst_sentiment=str(getattr(qualitative, "catalyst_sentiment", "") or ""),
                vetoed=True,
                veto_reason=f"moat_classification={moat_classification or 'none'}: no durable competitive advantage identified",
                source="moat_veto",
                enriched_signal_id=event.correlation_id,
                meta_labeler_score=event.meta_labeler_score,
                details={
                    "source": str(getattr(qualitative, "source", "") or ""),
                    "catalysts": getattr(qualitative, "catalysts", []) or [],
                    "llm_response": llm_response,
                },
                moat_classification=moat_classification,
                moat_justification=moat_justification,
                **llm_payload,
                llm_influenced=llm_used,
                llm_influence="vetoed" if llm_used else "moat_veto",
            )
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
            moat_classification=moat_classification,
            moat_justification=moat_justification,
            **llm_payload,
            llm_influenced=llm_used and _verdict_aligns_with_signal(verdict, event.composite_action),
            llm_influence=(
                "confirmed" if llm_used and _verdict_aligns_with_signal(verdict, event.composite_action)
                else "reviewed" if llm_used
                else qualitative_source or "fallback"
            ),
        )


def _verdict_aligns_with_signal(verdict: str, action: str) -> bool:
    verdict_text = str(verdict or "").strip().lower()
    action_text = str(action or "").strip().lower()
    if not verdict_text or not action_text:
        return False
    if action_text in {"buy", "strong buy"}:
        return any(token in verdict_text for token in ("buy", "accumulate", "increase", "add"))
    if action_text in {"sell", "strong sell"}:
        return any(token in verdict_text for token in ("sell", "exit", "reduce", "trim"))
    if action_text == "hold":
        return "hold" in verdict_text
    return False
