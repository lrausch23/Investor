from __future__ import annotations

import asyncio
import logging
from typing import Any

from . import AgentBase
from ..events import BaseEvent, EnrichedSignalEvent, TradeDecisionEvent

logger = logging.getLogger(__name__)


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
                sizing_rationale="Exit existing open position",
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
            sizing_rationale=f"allocation={allocation:.3f} ml_mult={ml_multiplier:.2f} budget={budget:.0f}",
            enriched_signal_id=event.correlation_id,
        )
