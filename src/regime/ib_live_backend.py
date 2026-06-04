from __future__ import annotations

import asyncio
import datetime as dt
import logging
import math
from typing import Any, Callable

from .exceptions import BrokerConnectionError
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
            raise BrokerConnectionError("IBKR backend is not connected.")
        return int(get_ib_thread().run(lambda: self._ib.client.getReqId()))

    def place_order(self, order: IBOrder) -> IBOrderState:
        if self._ib is None:
            raise BrokerConnectionError("IBKR backend is not connected.")

        async def _place() -> IBOrderState:
            from ib_insync import Contract, LimitOrder, MarketOrder, StopOrder, TagValue

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
            if order.algo_strategy:
                strategy_name = str(order.algo_strategy or "").strip().upper()
                ib_order.algoStrategy = "Twap" if strategy_name == "TWAP" else "Vwap" if strategy_name == "VWAP" else str(order.algo_strategy)
                if order.algo_params:
                    ib_order.algoParams = [TagValue(str(tag), str(value)) for tag, value in order.algo_params]
            ib_order.account = self._account_id
            ib_order.transmit = True
            trade = self._ib.placeOrder(contract, ib_order)
            self._order_map[order.order_id] = trade
            actual_order_id = int(getattr(trade.order, "orderId", 0) or order.order_id)
            self._order_map[actual_order_id] = trade
            state = self._trade_to_state(trade, actual_order_id)
            for _ in range(20):
                if state.status != IBOrderStatus.PENDING_SUBMIT:
                    return state
                await asyncio.sleep(0.25)
                actual_order_id = int(getattr(trade.order, "orderId", 0) or actual_order_id)
                self._order_map[actual_order_id] = trade
                state = self._trade_to_state(trade, actual_order_id)
            return state

        return get_ib_thread().run(_place)

    def cancel_order(self, order_id: int) -> IBOrderState:
        if self._ib is None:
            raise BrokerConnectionError("IBKR backend is not connected.")

        async def _cancel() -> IBOrderState:
            normalized_order_id = int(order_id)
            trade = self._order_map.get(normalized_order_id)
            if trade is None:
                for candidate in list(self._ib.openTrades() or []) + list(self._ib.trades() or []):
                    try:
                        candidate_order_id = int(getattr(candidate.order, "orderId", 0) or 0)
                    except Exception:
                        candidate_order_id = 0
                    if candidate_order_id == normalized_order_id:
                        trade = candidate
                        self._order_map[normalized_order_id] = candidate
                        break
            if trade is None:
                try:
                    await self._ib.reqAllOpenOrdersAsync()
                except Exception:
                    logger.debug("Unable to refresh all open orders before cancelling %s", normalized_order_id, exc_info=True)
                for candidate in list(self._ib.openTrades() or []):
                    try:
                        candidate_order_id = int(getattr(candidate.order, "orderId", 0) or 0)
                    except Exception:
                        candidate_order_id = 0
                    if candidate_order_id == normalized_order_id:
                        trade = candidate
                        self._order_map[normalized_order_id] = candidate
                        break
            if trade is None:
                return IBOrderState(
                    normalized_order_id,
                    IBOrderStatus.INACTIVE,
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                    dt.datetime.now(dt.timezone.utc).isoformat(),
                    "Unknown order; cancel not sent.",
                )
            actual_order_id = int(getattr(trade.order, "orderId", 0) or normalized_order_id)
            self._order_map[actual_order_id] = trade
            self._ib.cancelOrder(trade.order)
            for _ in range(20):
                await asyncio.sleep(0.25)
                state = self._trade_to_state(trade, actual_order_id)
                if state.status in {IBOrderStatus.CANCELLED, IBOrderStatus.API_CANCELLED, IBOrderStatus.INACTIVE}:
                    return state
            return self._trade_to_state(trade, actual_order_id)

        return get_ib_thread().run(_cancel)

    def get_order_status(self, order_id: int) -> IBOrderState:
        if self._ib is None:
            raise BrokerConnectionError("IBKR backend is not connected.")

        async def _status() -> IBOrderState:
            trade = self._order_map.get(int(order_id))
            if trade is not None:
                actual_order_id = int(getattr(trade.order, "orderId", 0) or order_id)
                return self._trade_to_state(trade, actual_order_id)
            for candidate in list(self._ib.openTrades() or []) + list(self._ib.trades() or []):
                try:
                    candidate_order_id = int(getattr(candidate.order, "orderId", 0) or 0)
                except Exception:
                    candidate_order_id = 0
                if candidate_order_id == int(order_id):
                    self._order_map[int(order_id)] = candidate
                    return self._trade_to_state(candidate, int(order_id))
            try:
                await self._ib.reqAllOpenOrdersAsync()
            except Exception:
                logger.debug("Unable to refresh all open orders before status lookup %s", order_id, exc_info=True)
            try:
                if hasattr(self._ib, "reqCompletedOrdersAsync"):
                    await self._ib.reqCompletedOrdersAsync(apiOnly=False)
            except Exception:
                logger.debug("Unable to refresh completed orders before status lookup %s", order_id, exc_info=True)
            for candidate in list(self._ib.openTrades() or []) + list(self._ib.trades() or []):
                try:
                    candidate_order_id = int(getattr(candidate.order, "orderId", 0) or 0)
                except Exception:
                    candidate_order_id = 0
                if candidate_order_id == int(order_id):
                    self._order_map[int(order_id)] = candidate
                    return self._trade_to_state(candidate, int(order_id))
            execution_state = await self._execution_state_for_order(int(order_id))
            if execution_state is not None:
                return execution_state
            return IBOrderState(
                order_id=int(order_id),
                status=IBOrderStatus.INACTIVE,
                filled_qty=0.0,
                remaining_qty=0.0,
                avg_fill_price=0.0,
                last_fill_price=0.0,
                commission=0.0,
                realized_pnl=0.0,
                timestamp=dt.datetime.now(dt.timezone.utc).isoformat(),
                message="Order not found in IBKR open or completed orders; treating as inactive.",
            )

        return get_ib_thread().run(_status)

    async def _execution_state_for_order(self, order_id: int) -> IBOrderState | None:
        if self._ib is None:
            return None
        try:
            from ib_insync import ExecutionFilter
        except Exception:
            return None

        query = ExecutionFilter()
        query.acctCode = self._account_id
        client_id = int(getattr(self, "_client_id", 0) or 0)
        if client_id > 0:
            query.clientId = client_id
        query.time = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=2)).strftime("%Y%m%d %H:%M:%S")
        try:
            executions = await self._ib.reqExecutionsAsync(query)
        except Exception:
            logger.debug("Unable to query IBKR executions for order %s", order_id, exc_info=True)
            return None

        matches = []
        for fill in list(executions or []):
            execution = getattr(fill, "execution", None)
            if execution is None:
                continue
            try:
                execution_order_id = int(getattr(execution, "orderId", 0) or 0)
            except Exception:
                execution_order_id = 0
            if execution_order_id == int(order_id):
                matches.append(fill)
        if not matches:
            return None

        filled_qty = 0.0
        weighted_price = 0.0
        latest_time: dt.datetime | None = None
        for fill in matches:
            execution = getattr(fill, "execution", None)
            if execution is None:
                continue
            try:
                shares = float(getattr(execution, "shares", 0.0) or 0.0)
                price = float(getattr(execution, "price", 0.0) or getattr(execution, "avgPrice", 0.0) or 0.0)
            except Exception:
                continue
            if shares <= 0 or price <= 0:
                continue
            filled_qty += shares
            weighted_price += shares * price
            execution_time = getattr(execution, "time", None)
            if isinstance(execution_time, dt.datetime):
                if execution_time.tzinfo is None:
                    execution_time = execution_time.replace(tzinfo=dt.timezone.utc)
                if latest_time is None or execution_time > latest_time:
                    latest_time = execution_time
        if filled_qty <= 0:
            return None
        avg_price = weighted_price / filled_qty
        return IBOrderState(
            order_id=int(order_id),
            status=IBOrderStatus.FILLED,
            filled_qty=filled_qty,
            remaining_qty=0.0,
            avg_fill_price=avg_price,
            last_fill_price=avg_price,
            commission=0.0,
            realized_pnl=0.0,
            timestamp=(latest_time or dt.datetime.now(dt.timezone.utc)).isoformat(),
            message="Filled from IBKR execution report.",
        )

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
            raise BrokerConnectionError("IBKR backend is not connected.")

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

    def get_market_quote(self, ticker: str) -> dict[str, float | str | None]:
        if self._ib is None:
            raise BrokerConnectionError("IBKR backend is not connected.")

        async def _quote() -> dict[str, float | str | None]:
            from ib_insync import Contract

            symbol = str(ticker or "").strip().upper()
            contract = Contract(symbol=symbol, secType="STK", exchange="SMART", currency="USD")
            await self._ib.qualifyContractsAsync(contract)

            def clean(value: Any) -> float | None:
                try:
                    numeric = float(value)
                except Exception:
                    return None
                if not math.isfinite(numeric) or numeric <= 0:
                    return None
                return numeric

            def payload(quote: Any) -> dict[str, float | str | None]:
                market_price = None
                try:
                    market_price = clean(quote.marketPrice())
                except Exception:
                    market_price = None
                bid = clean(getattr(quote, "bid", None))
                ask = clean(getattr(quote, "ask", None))
                last = clean(getattr(quote, "last", None))
                close = clean(getattr(quote, "close", None))
                midpoint = ((bid + ask) / 2.0) if bid is not None and ask is not None else None
                return {
                    "ticker": symbol,
                    "bid": bid,
                    "ask": ask,
                    "last": last,
                    "market_price": market_price or midpoint or last or close,
                    "close": close,
                    "source": "ibkr",
                }

            def has_price(data: dict[str, float | str | None]) -> bool:
                return any(data.get(key) is not None for key in ("bid", "ask", "last", "market_price", "close"))

            try:
                snapshots = await self._ib.reqTickersAsync(contract)
                if snapshots:
                    data = payload(snapshots[0])
                    if has_price(data):
                        return data
            except Exception:
                logger.debug("IBKR quote snapshot failed for %s", symbol, exc_info=True)

            for market_data_type in (1, 2, 3, 4):
                quote = None
                try:
                    if hasattr(self._ib, "reqMarketDataType"):
                        self._ib.reqMarketDataType(market_data_type)
                    quote = self._ib.reqMktData(contract, "", False, False)
                    for _ in range(15):
                        await asyncio.sleep(0.2)
                        data = payload(quote)
                        if has_price(data):
                            return data
                except Exception:
                    logger.debug("IBKR streaming quote failed for %s type=%s", symbol, market_data_type, exc_info=True)
                finally:
                    try:
                        self._ib.cancelMktData(contract)
                    except Exception:
                        pass
            return {
                "ticker": symbol,
                "bid": None,
                "ask": None,
                "last": None,
                "market_price": None,
                "close": None,
                "source": "ibkr",
            }

        return get_ib_thread().run(_quote, timeout=10)

    def cancel_all_orders(self) -> list[dict[str, Any]]:
        if self._ib is None:
            raise BrokerConnectionError("IBKR backend is not connected.")

        def _cancel_all() -> list[dict[str, Any]]:
            cancelled: list[dict[str, Any]] = []
            for order in list(self._ib.openOrders() or []):
                self._ib.cancelOrder(order)
                cancelled.append({"order_id": int(getattr(order, "orderId", 0) or 0), "status": "cancel_requested"})
            return cancelled

        return get_ib_thread().run(_cancel_all, timeout=15)

    def flatten_position(self, ticker: str, quantity: float, side: str = "long") -> dict[str, Any]:
        if self._ib is None:
            raise BrokerConnectionError("IBKR backend is not connected.")

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
