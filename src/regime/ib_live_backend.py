from __future__ import annotations

import asyncio
import datetime as dt
import logging
from typing import Any, Callable

from .ib_connection import IBConnectionBackend
from .ib_thread import get_ib_thread
from .ib_types import IBAccountSummary, IBOrder, IBOrderState, IBOrderStatus, IBOrderType, IBPosition

logger = logging.getLogger(__name__)


class LiveIBBackend(IBConnectionBackend):
    """Real IB connection via ib_insync dispatched through the dedicated IB thread."""

    def __init__(self, *, account_id: str = "DUP579027"):
        self._ib: Any = None
        self._account_id = account_id
        self._callbacks: list[Callable[[IBOrderState], None]] = []
        self._order_map: dict[int, object] = {}
        self._status_callback_registered = False

    def connect(self, host: str, port: int, client_id: int) -> bool:
        thread = get_ib_thread()

        async def _connect() -> bool:
            if self._ib is None:
                from ib_insync import IB

                self._ib = IB()
            if self._ib.isConnected():
                return True
            if hasattr(self._ib, "connectAsync"):
                await self._ib.connectAsync(host, port, clientId=client_id, timeout=10)
            else:
                self._ib.connect(host, port, clientId=client_id, timeout=10)
            if not self._status_callback_registered:
                self._ib.orderStatusEvent += self._on_order_status
                self._status_callback_registered = True
            if self._ib.isConnected():
                accounts = list(self._ib.managedAccounts() or [])
                if self._account_id not in accounts:
                    logger.error("Account %s not found in managed accounts: %s", self._account_id, accounts)
                    self._ib.disconnect()
                    return False
                logger.info("Connected to IBKR, account %s verified", self._account_id)
            return bool(self._ib.isConnected())

        try:
            return thread.run(_connect, timeout=15)
        except Exception as exc:
            logger.warning("IB connect failed: %s", exc)
            return False

    def disconnect(self) -> None:
        if self._ib is None:
            return
        get_ib_thread().run(lambda: self._ib.disconnect() if self._ib.isConnected() else None)

    def is_connected(self) -> bool:
        if self._ib is None:
            return False
        return bool(get_ib_thread().run(self._ib.isConnected))

    def next_order_id(self) -> int:
        if self._ib is None:
            raise RuntimeError("IBKR backend is not connected.")
        return int(get_ib_thread().run(lambda: self._ib.client.getReqId()))

    def place_order(self, order: IBOrder) -> IBOrderState:
        if self._ib is None:
            raise RuntimeError("IBKR backend is not connected.")

        async def _place() -> IBOrderState:
            from ib_insync import Contract, LimitOrder, MarketOrder, StopOrder

            contract = Contract(symbol=order.contract_symbol, secType="STK", exchange="SMART", currency="USD")
            await self._ib.qualifyContractsAsync(contract)
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

        return get_ib_thread().run(_place)

    def cancel_order(self, order_id: int) -> IBOrderState:
        if self._ib is None:
            raise RuntimeError("IBKR backend is not connected.")

        async def _cancel() -> IBOrderState:
            trade = self._order_map.get(int(order_id))
            if trade is None:
                return IBOrderState(
                    int(order_id),
                    IBOrderStatus.API_CANCELLED,
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                    dt.datetime.now(dt.timezone.utc).isoformat(),
                    "Unknown order",
                )
            self._ib.cancelOrder(trade.order)
            await asyncio.sleep(0.5)
            return self._trade_to_state(trade, int(order_id))

        return get_ib_thread().run(_cancel)

    def get_order_status(self, order_id: int) -> IBOrderState:
        if self._ib is None:
            raise RuntimeError("IBKR backend is not connected.")
        return get_ib_thread().run(lambda: self._trade_to_state(self._order_map[int(order_id)], int(order_id)))

    def get_positions(self) -> list[IBPosition]:
        if self._ib is None:
            return []

        async def _positions() -> list[IBPosition]:
            positions = await self._ib.reqPositionsAsync()
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

        return get_ib_thread().run(_positions)

    def get_account_summary(self) -> IBAccountSummary:
        if self._ib is None:
            raise RuntimeError("IBKR backend is not connected.")

        async def _summary() -> IBAccountSummary:
            summary = await self._ib.accountSummaryAsync(self._account_id)
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

        return get_ib_thread().run(_summary)

    def cancel_all_orders(self) -> list[dict[str, Any]]:
        if self._ib is None:
            raise RuntimeError("IBKR backend is not connected.")

        def _cancel_all() -> list[dict[str, Any]]:
            cancelled: list[dict[str, Any]] = []
            for order in list(self._ib.openOrders() or []):
                self._ib.cancelOrder(order)
                cancelled.append({"order_id": int(getattr(order, "orderId", 0) or 0), "status": "cancel_requested"})
            return cancelled

        return get_ib_thread().run(_cancel_all, timeout=15)

    def flatten_position(self, ticker: str, quantity: float, side: str = "long") -> dict[str, Any]:
        if self._ib is None:
            raise RuntimeError("IBKR backend is not connected.")

        def _flatten() -> dict[str, Any]:
            from ib_insync import Contract, MarketOrder

            action = "SELL" if str(side).lower() == "long" else "BUY"
            contract = Contract(symbol=str(ticker).upper(), secType="STK", exchange="SMART", currency="USD")
            order = MarketOrder(action, abs(float(quantity)))
            trade = self._ib.placeOrder(contract, order)
            order_id = int(getattr(trade.order, "orderId", 0) or 0)
            self._order_map[order_id] = trade
            return {
                "ticker": str(ticker).upper(),
                "action": action,
                "quantity": abs(float(quantity)),
                "order_id": order_id,
                "status": "submitted",
            }

        return get_ib_thread().run(_flatten, timeout=15)

    def flatten_all_positions(self) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        for pos in self.get_positions():
            if abs(float(pos.quantity)) < 0.01:
                continue
            ticker = getattr(pos, "ticker", None) or getattr(pos, "contract_symbol", "")
            try:
                results.append(
                    self.flatten_position(
                        str(ticker),
                        abs(float(pos.quantity)),
                        "long" if float(pos.quantity) > 0 else "short",
                    )
                )
            except Exception as exc:
                results.append({"ticker": str(ticker), "status": "failed", "error": str(exc)})
        return results

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
