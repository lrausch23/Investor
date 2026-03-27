from __future__ import annotations

import datetime as dt
import logging
from typing import Callable

from .ib_connection import IBConnectionBackend
from .ib_types import IBAccountSummary, IBOrder, IBOrderState, IBOrderStatus, IBOrderType, IBPosition

logger = logging.getLogger(__name__)


class LiveIBBackend(IBConnectionBackend):
    """Real IB connection via ib_insync.

    Import is intentionally lazy so the mock-backed system still works when
    `ib_insync` is not installed.
    """

    def __init__(self, *, account_id: str = "DUP579027"):
        from ib_insync import IB

        self._ib = IB()
        self._account_id = account_id
        self._callbacks: list[Callable[[IBOrderState], None]] = []
        self._order_map: dict[int, object] = {}

    def connect(self, host: str, port: int, client_id: int) -> bool:
        try:
            self._ib.connect(host, port, clientId=client_id, timeout=10)
            self._ib.orderStatusEvent += self._on_order_status
            return bool(self._ib.isConnected())
        except Exception as exc:
            logger.warning("IB connect failed: %s", exc)
            return False

    def disconnect(self) -> None:
        if self._ib.isConnected():
            self._ib.disconnect()

    def is_connected(self) -> bool:
        return bool(self._ib.isConnected())

    def next_order_id(self) -> int:
        return int(self._ib.client.getReqId())

    def place_order(self, order: IBOrder) -> IBOrderState:
        from ib_insync import Contract, LimitOrder, MarketOrder, StopOrder

        contract = Contract(symbol=order.contract_symbol, secType="STK", exchange="SMART", currency="USD")
        self._ib.qualifyContracts(contract)
        if order.order_type == IBOrderType.MARKET:
            ib_order = MarketOrder(order.action.value, order.quantity)
        elif order.order_type == IBOrderType.LIMIT:
            ib_order = LimitOrder(order.action.value, order.quantity, order.limit_price)
        elif order.order_type == IBOrderType.STOP:
            ib_order = StopOrder(order.action.value, order.quantity, order.stop_price)
        else:
            ib_order = MarketOrder(order.action.value, order.quantity)
        ib_order.outsideRth = order.outside_rth
        ib_order.tif = order.time_in_force.value
        trade = self._ib.placeOrder(contract, ib_order)
        self._order_map[order.order_id] = trade
        return self._trade_to_state(trade, order.order_id)

    def cancel_order(self, order_id: int) -> IBOrderState:
        trade = self._order_map.get(int(order_id))
        if trade is None:
            return IBOrderState(int(order_id), IBOrderStatus.API_CANCELLED, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, dt.datetime.now(dt.timezone.utc).isoformat(), "Unknown order")
        self._ib.cancelOrder(trade.order)
        self._ib.sleep(0.5)
        return self._trade_to_state(trade, int(order_id))

    def get_order_status(self, order_id: int) -> IBOrderState:
        trade = self._order_map[int(order_id)]
        return self._trade_to_state(trade, int(order_id))

    def get_positions(self) -> list[IBPosition]:
        positions = self._ib.positions()
        return [
            IBPosition(
                account_id=pos.account,
                contract_symbol=pos.contract.symbol,
                quantity=float(pos.position),
                avg_cost=float(pos.avgCost),
                market_value=float(pos.position) * float(pos.avgCost),
                unrealized_pnl=0.0,
            )
            for pos in positions
            if pos.account == self._account_id
        ]

    def get_account_summary(self) -> IBAccountSummary:
        summary = self._ib.accountSummary(self._account_id)
        values = {}
        for item in summary:
            if item.account == self._account_id:
                try:
                    values[item.tag] = float(item.value)
                except Exception:
                    continue
        return IBAccountSummary(
            account_id=self._account_id,
            net_liquidation=values.get("NetLiquidation", 0.0),
            total_cash=values.get("TotalCashValue", 0.0),
            buying_power=values.get("BuyingPower", 0.0),
            gross_position_value=values.get("GrossPositionValue", 0.0),
            maintenance_margin=values.get("MaintMarginReq", 0.0),
            available_funds=values.get("AvailableFunds", 0.0),
            unrealized_pnl=values.get("UnrealizedPnL"),
        )

    def register_order_callback(self, callback: Callable[[IBOrderState], None]) -> None:
        self._callbacks.append(callback)

    def _on_order_status(self, trade) -> None:
        for order_id, current in self._order_map.items():
            if current is trade:
                state = self._trade_to_state(trade, order_id)
                for callback in self._callbacks:
                    callback(state)
                break

    def _trade_to_state(self, trade, order_id: int) -> IBOrderState:
        status_text = str(getattr(trade.orderStatus, "status", "") or "Submitted")
        try:
            status = IBOrderStatus(status_text)
        except Exception:
            status = IBOrderStatus.SUBMITTED
        avg_price = float(getattr(trade.orderStatus, "avgFillPrice", 0.0) or 0.0)
        filled = float(getattr(trade.orderStatus, "filled", 0.0) or 0.0)
        remaining = float(getattr(trade.orderStatus, "remaining", 0.0) or 0.0)
        return IBOrderState(
            order_id=int(order_id),
            status=status,
            filled_qty=filled,
            remaining_qty=remaining,
            avg_fill_price=avg_price,
            last_fill_price=avg_price,
            commission=0.0,
            realized_pnl=0.0,
            timestamp=dt.datetime.now(dt.timezone.utc).isoformat(),
            message=str(getattr(trade.orderStatus, "whyHeld", "") or ""),
        )
