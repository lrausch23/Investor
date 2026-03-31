from __future__ import annotations

import asyncio
import logging
from typing import Any

from . import AgentBase
from ..events import BaseEvent, OrderExecutionEvent, TradeDecisionEvent

logger = logging.getLogger(__name__)


def _blocked_message(guardrail_result: Any) -> str:
    for check in getattr(guardrail_result, "checks", []) or []:
        if not bool(getattr(check, "passed", False)):
            return str(getattr(check, "message", "") or "Guardrails blocked the order")
    return "Guardrails blocked the order"


class ExecutionAgent(AgentBase):
    @property
    def name(self) -> str:
        return "execution"

    @property
    def subscriptions(self) -> list[str]:
        return ["trade_decision"]

    async def handle(self, event: BaseEvent) -> None:
        if not isinstance(event, TradeDecisionEvent):
            return
        if event.decision != "approved":
            return
        runtime = self._get_runtime()
        try:
            result = await asyncio.to_thread(self._execute, runtime, event)
        except Exception as exc:
            logger.error("ExecutionAgent failed for %s: %s", event.ticker, exc)
            return
        if result is not None:
            await self._bus.publish(result)

    def _execute(self, runtime: dict[str, Any] | None, event: TradeDecisionEvent) -> OrderExecutionEvent:
        paper_broker_adapter_ctor: Any
        submit_guarded_order_fn: Any
        order_request_ctor: Any
        default_risk_guardrails: Any
        if runtime is None:
            from .. import broker_adapter as broker_adapter_module
            from .. import config as config_module

            paper_broker_adapter_ctor = broker_adapter_module.PaperBrokerAdapter
            submit_guarded_order_fn = broker_adapter_module.submit_guarded_order
            order_request_ctor = broker_adapter_module.OrderRequest
            default_risk_guardrails = config_module.DEFAULT_RISK_GUARDRAILS
        else:
            paper_broker_adapter_ctor = runtime["PaperBrokerAdapter"]
            submit_guarded_order_fn = runtime["submit_guarded_order"]
            order_request_ctor = runtime["OrderRequest"]
            default_risk_guardrails = runtime["DEFAULT_RISK_GUARDRAILS"]

        try:
            adapter = paper_broker_adapter_ctor(event.portfolio_id)
            order = order_request_ctor(
                portfolio_id=event.portfolio_id,
                ticker=event.ticker,
                action=str(event.action or "").lower(),
                quantity=float(event.quantity or 0.0),
                order_type="market",
                limit_price=float(event.proposed_price) if event.proposed_price is not None else None,
                source="agent",
            )
            guardrail_result, order_result = submit_guarded_order_fn(
                order=order,
                adapter=adapter,
                guardrails=default_risk_guardrails,
                actor="agent",
            )
            if order_result is None:
                return OrderExecutionEvent(
                    correlation_id=event.correlation_id,
                    ticker=event.ticker,
                    portfolio_id=event.portfolio_id,
                    order_id="",
                    action=event.action,
                    quantity=float(event.quantity or 0.0),
                    status="rejected",
                    broker_type="paper",
                    trade_decision_id=event.correlation_id,
                    message=_blocked_message(guardrail_result),
                )
            return OrderExecutionEvent(
                correlation_id=event.correlation_id,
                ticker=event.ticker,
                portfolio_id=event.portfolio_id,
                order_id=str(getattr(order_result, "order_id", "") or ""),
                action=event.action,
                quantity=float(getattr(order_result, "quantity", event.quantity) or 0.0),
                status=str(getattr(order_result, "status", "") or ""),
                broker_type="paper",
                trade_decision_id=event.correlation_id,
                filled_price=float(getattr(order_result, "filled_price", 0.0) or 0.0) if getattr(order_result, "filled_price", None) is not None else None,
                filled_at=str(getattr(order_result, "filled_at", "") or "") or None,
                message=str(getattr(order_result, "message", "") or ""),
            )
        except Exception as exc:
            logger.error("ExecutionAgent: order failed for %s: %s", event.ticker, exc)
            return OrderExecutionEvent(
                correlation_id=event.correlation_id,
                ticker=event.ticker,
                portfolio_id=event.portfolio_id,
                order_id="",
                action=event.action,
                quantity=float(event.quantity or 0.0),
                status="rejected",
                broker_type="paper",
                trade_decision_id=event.correlation_id,
                message=str(exc),
            )
