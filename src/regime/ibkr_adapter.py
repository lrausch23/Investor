from __future__ import annotations

import datetime as dt
import logging
from typing import Any

from .broker_adapter import AccountSummary, BrokerAdapter, OrderRequest, OrderResult, PositionInfo
from .ib_connection import IBConnectionBackend, IBConnectionManager
from .ib_order_translator import translate_account_summary, translate_order_request, translate_order_state, translate_position
from .ib_types import get_market_hours_status, is_market_open, next_market_open
from .persistence import get_paper_portfolio, get_trade_plans, log_audit_event, update_trade_plan_status

logger = logging.getLogger(__name__)


class IBKRBrokerAdapter(BrokerAdapter):
    def __init__(
        self,
        backend: IBConnectionBackend,
        portfolio_id: int,
        *,
        allow_outside_rth: bool = False,
        host: str = "127.0.0.1",
        port: int = 7497,
        client_id: int = 1,
    ):
        self._backend = backend
        self._portfolio_id = int(portfolio_id)
        self._allow_outside_rth = allow_outside_rth
        self._manager = IBConnectionManager(backend, host=host, port=port, client_id=client_id)
        self._manager.ensure_connected()

    def _ensure_connected(self, *, ticker: str = "", action: str = "", quantity: float = 0.0) -> bool:
        was_connected = bool(self._manager.backend.is_connected())
        connected = self._manager.ensure_connected()
        if connected and not was_connected:
            log_audit_event(
                order_id=f"connection-restored-{self._portfolio_id}-{int(dt.datetime.now(dt.timezone.utc).timestamp())}",
                portfolio_id=self._portfolio_id,
                event_type="error",
                ticker=str(ticker or "").upper() or "*",
                action=action or "connect",
                quantity=quantity or None,
                actor="system",
                details="connection_restored: IBKR connection restored",
            )
        if not connected:
            log_audit_event(
                order_id=f"connection-lost-{self._portfolio_id}-{int(dt.datetime.now(dt.timezone.utc).timestamp())}",
                portfolio_id=self._portfolio_id,
                event_type="error",
                ticker=str(ticker or "").upper() or "*",
                action=action or "connect",
                quantity=quantity or None,
                actor="system",
                details="connection_lost: IBKR connection unavailable",
            )
        return connected

    def submit_order(self, order: OrderRequest) -> OrderResult:
        if not self._ensure_connected(
            ticker=str(order.ticker or ""),
            action=str(order.action or ""),
            quantity=float(order.quantity or 0.0),
        ):
            return OrderResult(order_id="", status="rejected", ticker=order.ticker.upper(), action=order.action, quantity=order.quantity, message="IBKR connection unavailable.")
        account_id = str(
            getattr(
                self._manager.backend,
                "_account_id",
                getattr(self._manager.backend, "account_id", ""),
            )
            or ""
        )
        if not account_id.startswith("DU"):
            logger.critical("REFUSING ORDER: account %s is not a paper account (DU* prefix)", account_id)
            log_audit_event(
                order_id=f"order-rejected-{self._portfolio_id}-{int(dt.datetime.now(dt.timezone.utc).timestamp())}",
                portfolio_id=self._portfolio_id,
                event_type="rejected",
                ticker=str(order.ticker or "").upper(),
                action=str(order.action or ""),
                quantity=float(order.quantity or 0.0),
                actor="system",
                details=f"Live account detected: {account_id}",
            )
            return OrderResult(
                order_id="",
                status="rejected",
                ticker=str(order.ticker or "").upper(),
                action=order.action,
                quantity=float(order.quantity or 0.0),
                message="Live account detected — paper only",
            )
        if not is_market_open() and not self._allow_outside_rth:
            return OrderResult(
                order_id="",
                status="rejected",
                ticker=str(order.ticker or "").upper(),
                action=order.action,
                quantity=float(order.quantity or 0.0),
                message=f"Market closed ({get_market_hours_status().value}). Next open: {next_market_open().isoformat()}",
            )
        ib_order = translate_order_request(order, self._backend.next_order_id())
        state = self._backend.place_order(ib_order)
        return translate_order_state(state, ticker=order.ticker, action=order.action)

    def cancel_order(self, order_id: str) -> bool:
        if not self._ensure_connected(action="cancel"):
            return False
        state = self._backend.cancel_order(int(order_id))
        return state.status.value in {"Cancelled", "ApiCancelled"}

    def get_order_status(self, order_id: str) -> OrderResult | None:
        if not self._ensure_connected(action="status"):
            return None
        for plan in get_trade_plans(self._portfolio_id, status="all"):
            if str(plan.get("broker_order_id") or "") == str(order_id):
                state = self._backend.get_order_status(int(order_id))
                return translate_order_state(state, ticker=str(plan.get("ticker") or ""), action=str(plan.get("action") or ""))
        return None

    def get_positions(self) -> list[PositionInfo]:
        if not self._ensure_connected(action="positions"):
            return []
        return [translate_position(position) for position in self._backend.get_positions()]

    def get_account_summary(self) -> AccountSummary:
        if not self._ensure_connected(action="summary"):
            portfolio = get_paper_portfolio(self._portfolio_id) or {}
            return AccountSummary(
                portfolio_id=self._portfolio_id,
                equity=float(portfolio.get("starting_budget") or 0.0),
                cash=float(portfolio.get("current_cash") or 0.0),
                market_value=0.0,
                realized_pnl=0.0,
                unrealized_pnl=0.0,
                daily_pnl=0.0,
                exposure_pct=0.0,
            )
        return translate_account_summary(self._backend.get_account_summary(), portfolio_id=self._portfolio_id)

    def health(self) -> dict[str, Any]:
        return self._manager.health_check() | {"market_hours": get_market_hours_status().value}


def poll_pending_orders(adapter: IBKRBrokerAdapter, portfolio_id: int) -> list[OrderResult]:
    changed: list[OrderResult] = []
    from .paper_trading import _apply_filled_execution

    for plan in get_trade_plans(portfolio_id, status="all"):
        plan_status = str(plan.get("status") or "")
        broker_order_id = str(plan.get("broker_order_id") or "")
        if plan_status not in {"Submitted", "Partially Filled"} or not broker_order_id:
            continue
        result = adapter.get_order_status(broker_order_id)
        if result is None:
            continue
        current_broker_status = str(plan.get("broker_status") or "")
        if current_broker_status == result.status:
            continue
        update_fields: dict[str, Any] = {
            "broker_status": result.status,
            "notes": result.message or plan.get("notes") or "",
        }
        next_status = plan_status
        event_type = "submitted"
        if result.status == "filled":
            next_status = "Executed"
            update_fields["executed_at"] = result.filled_at or dt.datetime.now(dt.timezone.utc).isoformat()
            update_fields["execution_price"] = result.filled_price
            update_fields["filled_quantity"] = float(plan.get("quantity") or 0.0)
            event_type = "filled"
            _apply_filled_execution(portfolio_id, plan, result)
        elif result.status == "partially_filled":
            next_status = "Partially Filled"
            update_fields["filled_quantity"] = float(result.quantity or 0.0)
            event_type = "partially_filled"
        elif result.status == "cancelled":
            next_status = "Cancelled"
            event_type = "cancelled"
        elif result.status == "rejected":
            next_status = "Rejected"
            event_type = "rejected"
        else:
            next_status = "Submitted"
        updated = update_trade_plan_status(int(plan["id"]), next_status, broker_order_id=broker_order_id, **update_fields)
        log_audit_event(
            order_id=broker_order_id,
            portfolio_id=portfolio_id,
            event_type=event_type,
            ticker=str(plan.get("ticker") or ""),
            action=str(plan.get("action") or ""),
            quantity=float(plan.get("quantity") or 0.0),
            price=result.filled_price,
            actor="system",
            details=result.message or "",
        )
        if updated is not None:
            changed.append(result)
    return changed
