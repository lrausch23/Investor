from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from . import AgentBase
from ..events import BarrierOverrideEvent, BaseEvent, EnrichedSignalEvent, TradeDecisionEvent
from ..persistence import log_barrier_override

logger = logging.getLogger(__name__)


def _publish_ltcg_override_events(
    portfolio_id: int,
    ticker: str,
    original_stop: float | None,
    ltcg_result: Any,
) -> None:
    if not getattr(ltcg_result, "override_active", False):
        return
    for lot_detail in getattr(ltcg_result, "lot_details", []):
        if not getattr(lot_detail, "override_active", False):
            continue
        expiry = (datetime.now(timezone.utc) + timedelta(days=max(0, int(getattr(lot_detail, "days_to_ltcg", 0))))).isoformat()
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
            logger.debug("Unable to persist LTCG override log for %s lot %s", ticker, getattr(lot_detail, "lot_id", None), exc_info=True)
        try:
            from ..event_bus import get_event_bus

            get_event_bus().publish_sync(
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
            logger.debug("Unable to publish LTCG override event for %s lot %s", ticker, getattr(lot_detail, "lot_id", None), exc_info=True)


class PortfolioTaxAgent(AgentBase):
    @property
    def name(self) -> str:
        return "portfolio_tax"

    @property
    def subscriptions(self) -> list[str]:
        return []

    async def handle(self, event: BaseEvent) -> None:
        if not isinstance(event, EnrichedSignalEvent):
            return
        if event.source != "quant_agent":
            return
        try:
            decisions = await self.run_for_orchestrator(event, None)
        except Exception as exc:
            logger.error("PortfolioTaxAgent failed for %s: %s", event.ticker, exc)
            return
        for decision in decisions:
            await self._bus.publish(decision)

    async def run_for_orchestrator(
        self,
        event: EnrichedSignalEvent,
        fundamental: Any | None = None,
    ) -> list[TradeDecisionEvent]:
        runtime = self._get_runtime()
        if runtime is None:
            logger.warning("PortfolioTaxAgent skipped %s: runtime unavailable", event.ticker)
            return []
        return await asyncio.to_thread(self._evaluate_with_fundamental, runtime, event, fundamental)

    def _evaluate(self, runtime: dict[str, Any], event: EnrichedSignalEvent) -> list[TradeDecisionEvent]:
        action = str(event.composite_action or "").strip()
        if action in {"", "Hold"}:
            return []
        trade_action = "Buy" if action in {"Buy", "Strong Buy"} else "Sell" if action in {"Sell", "Strong Sell"} else None
        if trade_action is None:
            return []

        decisions: list[TradeDecisionEvent] = []
        for portfolio in runtime["list_paper_portfolios"](include_closed=False):
            if str(portfolio.get("status") or "") != "Active":
                continue
            decision = self._size_and_check(runtime, event, trade_action, portfolio)
            if decision is not None:
                decisions.append(decision)
        return decisions

    def _evaluate_with_fundamental(
        self,
        runtime: dict[str, Any],
        event: EnrichedSignalEvent,
        fundamental: Any | None,
    ) -> list[TradeDecisionEvent]:
        if fundamental is not None and bool(getattr(fundamental, "vetoed", False)):
            decisions: list[TradeDecisionEvent] = []
            action = str(event.composite_action or "").strip()
            if action in {"Buy", "Strong Buy"}:
                trade_action = "Buy"
            elif action in {"Sell", "Strong Sell"}:
                trade_action = "Sell"
            else:
                trade_action = "Hold"
            for portfolio in runtime["list_paper_portfolios"](include_closed=False):
                if str(portfolio.get("status") or "") != "Active":
                    continue
                decisions.append(
                    TradeDecisionEvent(
                        correlation_id=event.correlation_id,
                        ticker=event.ticker,
                        portfolio_id=int(portfolio["id"]),
                        action=trade_action,
                        decision="vetoed",
                        veto_reason=f"fundamental_veto: {getattr(fundamental, 'veto_reason', None) or 'LLM vetoed'}",
                        source=event.source,
                        regime_label=event.regime_label,
                        meta_labeler_score=event.meta_labeler_score,
                        enriched_signal_id=event.correlation_id,
                    )
                )
            return decisions
        return self._evaluate(runtime, event)

    def _size_and_check(
        self,
        runtime: dict[str, Any],
        event: EnrichedSignalEvent,
        trade_action: str,
        portfolio: dict[str, Any],
    ) -> TradeDecisionEvent | None:
        portfolio_id = int(portfolio["id"])
        ticker = str(event.ticker or "").upper()
        if trade_action == "Buy" and runtime["is_wash_sale_restricted"](portfolio_id, ticker):
            return TradeDecisionEvent(
                correlation_id=event.correlation_id,
                ticker=ticker,
                portfolio_id=portfolio_id,
                action=trade_action,
                decision="vetoed",
                veto_reason="wash_sale_restricted",
                source=event.source,
                regime_label=event.regime_label,
                meta_labeler_score=event.meta_labeler_score,
                enriched_signal_id=event.correlation_id,
            )

        anti_churn_result = None
        hurdle_result = None
        duration_result = None
        if trade_action == "Buy":
            from ..anti_churn import check_anti_churn, get_anti_churn_settings
            from ..hurdle_rate import check_duration_gate, check_hurdle_rate, get_hurdle_settings

            anti_churn_settings = get_anti_churn_settings()
            if bool(anti_churn_settings.get("anti_churn_enabled", True)):
                anti_churn_result = check_anti_churn(portfolio_id, ticker)
                if not anti_churn_result.passed:
                    logger.info("PortfolioTaxAgent anti-churn gate blocked %s: %s", ticker, anti_churn_result.reason)
                    return TradeDecisionEvent(
                        correlation_id=event.correlation_id,
                        ticker=ticker,
                        portfolio_id=portfolio_id,
                        action=trade_action,
                        decision="vetoed",
                        veto_reason=f"anti_churn: {anti_churn_result.reason}",
                        source=event.source,
                        regime_label=event.regime_label,
                        meta_labeler_score=event.meta_labeler_score,
                        enriched_signal_id=event.correlation_id,
                    )
            hurdle_settings = get_hurdle_settings()
            if bool(hurdle_settings.get("hurdle_enabled", True)):
                hurdle_result = check_hurdle_rate(ticker, event.entry_price, event.exit_price)
                if not hurdle_result.passed:
                    logger.info("PortfolioTaxAgent hurdle gate blocked %s: %s", ticker, hurdle_result.reason)
                    return TradeDecisionEvent(
                        correlation_id=event.correlation_id,
                        ticker=ticker,
                        portfolio_id=portfolio_id,
                        action=trade_action,
                        decision="vetoed",
                        veto_reason=f"hurdle_rate: {hurdle_result.reason}",
                        source=event.source,
                        regime_label=event.regime_label,
                        meta_labeler_score=event.meta_labeler_score,
                        enriched_signal_id=event.correlation_id,
                    )
            if bool(hurdle_settings.get("duration_gate_enabled", True)):
                duration_result = check_duration_gate(ticker, event.expected_regime_duration, event.regime_label)
                if not duration_result.passed:
                    logger.info("PortfolioTaxAgent duration gate blocked %s: %s", ticker, duration_result.reason)
                    return TradeDecisionEvent(
                        correlation_id=event.correlation_id,
                        ticker=ticker,
                        portfolio_id=portfolio_id,
                        action=trade_action,
                        decision="vetoed",
                        veto_reason=f"duration_gate: {duration_result.reason}",
                        source=event.source,
                        regime_label=event.regime_label,
                        meta_labeler_score=event.meta_labeler_score,
                        enriched_signal_id=event.correlation_id,
                    )

        current_price = float(event.current_price or 0.0)
        if trade_action == "Sell":
            positions = runtime["get_paper_positions"](portfolio_id, status="Open")
            quantity = sum(
                float(row.get("quantity") or 0.0)
                for row in positions
                if str(row.get("ticker") or "").upper() == ticker
                )
            if quantity <= 0:
                return TradeDecisionEvent(
                    correlation_id=event.correlation_id,
                    ticker=ticker,
                    portfolio_id=portfolio_id,
                    action=trade_action,
                    decision="vetoed",
                    veto_reason="no_open_position",
                    source=event.source,
                    regime_label=event.regime_label,
                    meta_labeler_score=event.meta_labeler_score,
                    enriched_signal_id=event.correlation_id,
                )
            from ..ltcg_override import check_ltcg_override, get_ltcg_override_settings

            ltcg_result = None
            ltcg_rationale_text = "Exit existing open position"
            ltcg_settings = get_ltcg_override_settings()
            if bool(ltcg_settings.get("ltcg_override_enabled", True)):
                ltcg_result = check_ltcg_override(
                    portfolio_id,
                    ticker,
                    current_price=current_price,
                    position_stop=None,
                    atr_14=float(event.atr_14) if event.atr_14 is not None else None,
                )
                if ltcg_result.override_active:
                    _publish_ltcg_override_events(portfolio_id, ticker, None, ltcg_result)
                    if ltcg_result.sellable_quantity <= 0:
                        return TradeDecisionEvent(
                            correlation_id=event.correlation_id,
                            ticker=ticker,
                            portfolio_id=portfolio_id,
                            action=trade_action,
                            decision="vetoed",
                            veto_reason=f"ltcg_override: {ltcg_result.reason}",
                            source=event.source,
                            regime_label=event.regime_label,
                            meta_labeler_score=event.meta_labeler_score,
                            enriched_signal_id=event.correlation_id,
                        )
                    quantity = float(ltcg_result.sellable_quantity)
                    ltcg_rationale_text = (
                        f"LTCG shield protecting {ltcg_result.protected_quantity:.0f} shares "
                        f"across {ltcg_result.lots_overridden} lot(s); estimated tax savings "
                        f"${ltcg_result.total_tax_savings:.2f}. "
                        f"ltcg=shield(protected={ltcg_result.protected_quantity:.4f},tax={ltcg_result.total_tax_savings:.2f},lots={ltcg_result.lots_overridden})"
                    )
            return TradeDecisionEvent(
                correlation_id=event.correlation_id,
                ticker=ticker,
                portfolio_id=portfolio_id,
                action=trade_action,
                decision="approved",
                quantity=float(quantity),
                proposed_price=current_price if current_price > 0 else None,
                source=event.source,
                regime_label=event.regime_label,
                meta_labeler_score=event.meta_labeler_score,
                sizing_rationale=f"Exit {quantity:.0f} shares. {ltcg_rationale_text}",
                enriched_signal_id=event.correlation_id,
            )

        budget = float(portfolio.get("current_cash") or portfolio.get("starting_budget") or 0.0)
        base_allocation = min(0.10, max(0.02, float(event.composite_strength or 0.0) * 0.15))
        ml_multiplier = float(event.ensemble_sizing_multiplier or 1.0)
        allocation = max(0.0, base_allocation * ml_multiplier)
        position_value = budget * allocation
        quantity = max(1, int(position_value / current_price)) if current_price > 0 and position_value > 0 else 0
        if quantity <= 0:
            return TradeDecisionEvent(
                correlation_id=event.correlation_id,
                ticker=ticker,
                portfolio_id=portfolio_id,
                action=trade_action,
                decision="vetoed",
                veto_reason="computed_quantity_zero",
                source=event.source,
                regime_label=event.regime_label,
                meta_labeler_score=event.meta_labeler_score,
                enriched_signal_id=event.correlation_id,
            )
        rationale = f"allocation={allocation:.3f} ml_mult={ml_multiplier:.2f} budget={budget:.0f}"
        if anti_churn_result:
            rationale = (
                f"{rationale} churn=pass(count={anti_churn_result.round_trip_count},"
                f"max={anti_churn_result.max_round_trips})"
            )
        if hurdle_result and hurdle_result.net_return_pct is not None and hurdle_result.gross_return_pct is not None:
            rationale = (
                f"{rationale} hurdle={hurdle_result.net_return_pct:.1f}%net"
                f"({hurdle_result.gross_return_pct:.1f}%gross@{hurdle_result.estimated_stcg_rate:.0%}tax)"
            )
        if duration_result and duration_result.expected_regime_duration is not None:
            rationale = (
                f"{rationale} duration={duration_result.expected_regime_duration:.1f}d"
                f"(min={duration_result.min_regime_duration_days:.1f})"
            )
        return TradeDecisionEvent(
            correlation_id=event.correlation_id,
            ticker=ticker,
            portfolio_id=portfolio_id,
            action=trade_action,
            decision="approved",
            quantity=float(quantity),
            proposed_price=current_price if current_price > 0 else None,
            source=event.source,
            regime_label=event.regime_label,
            meta_labeler_score=event.meta_labeler_score,
            sizing_rationale=rationale,
            enriched_signal_id=event.correlation_id,
        )
