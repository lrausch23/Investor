from __future__ import annotations

import datetime as dt
import logging
import threading
import time
from dataclasses import dataclass
from typing import Callable, Protocol

from .config import DEFAULT_IBKR_CONFIG, IBKRConfig
from .ib_types import (
    IBAccountSummary,
    IBOrder,
    IBOrderAction,
    IBOrderState,
    IBOrderStatus,
    IBPosition,
)
from .ib_types import get_market_hours_status

logger = logging.getLogger(__name__)


class IBConnectionBackend(Protocol):
    def connect(self, host: str, port: int, client_id: int) -> bool: ...
    def disconnect(self) -> None: ...
    def is_connected(self) -> bool: ...
    def next_order_id(self) -> int: ...
    def place_order(self, order: IBOrder) -> IBOrderState: ...
    def cancel_order(self, order_id: int) -> IBOrderState: ...
    def get_order_status(self, order_id: int) -> IBOrderState: ...
    def get_positions(self) -> list[IBPosition]: ...
    def get_account_summary(self) -> IBAccountSummary: ...
    def register_order_callback(self, callback: Callable[[IBOrderState], None]) -> None: ...


@dataclass
class MockFillConfig:
    mode: str = "instant"
    delay_seconds: float = 0.0
    partial_pct: float = 0.5
    reject_reason: str = ""


class MockIBBackend:
    def __init__(
        self,
        *,
        account_id: str = "DU000000",
        starting_cash: float = 100000.0,
        fill_config: MockFillConfig | None = None,
        mark_price: float = 100.0,
    ):
        self.account_id = account_id
        self._cash = float(starting_cash)
        self._fill_config = fill_config or MockFillConfig()
        self._mark_price = float(mark_price)
        self._connected = False
        self._next_order_id = 1000
        self._callbacks: list[Callable[[IBOrderState], None]] = []
        self._orders: dict[int, dict[str, object]] = {}
        self._positions: dict[str, IBPosition] = {}

    def connect(self, host: str, port: int, client_id: int) -> bool:
        del host, port, client_id
        self._connected = True
        return True

    def disconnect(self) -> None:
        self._connected = False

    def is_connected(self) -> bool:
        return self._connected

    def next_order_id(self) -> int:
        order_id = self._next_order_id
        self._next_order_id += 1
        return order_id

    def register_order_callback(self, callback: Callable[[IBOrderState], None]) -> None:
        self._callbacks.append(callback)

    def place_order(self, order: IBOrder) -> IBOrderState:
        timestamp = dt.datetime.now(dt.timezone.utc).isoformat()
        mode = self._fill_config.mode
        if mode == "reject":
            state = IBOrderState(
                order_id=order.order_id,
                status=IBOrderStatus.INACTIVE,
                filled_qty=0.0,
                remaining_qty=float(order.quantity),
                avg_fill_price=0.0,
                last_fill_price=0.0,
                commission=0.0,
                realized_pnl=0.0,
                timestamp=timestamp,
                message=self._fill_config.reject_reason or "Mock rejection",
            )
            self._orders[order.order_id] = {"order": order, "state": state}
            self._emit(state)
            return state
        if mode == "partial":
            filled = round(float(order.quantity) * float(self._fill_config.partial_pct), 6)
            remaining = max(0.0, float(order.quantity) - filled)
            state = IBOrderState(
                order_id=order.order_id,
                status=IBOrderStatus.PARTIALLY_FILLED,
                filled_qty=filled,
                remaining_qty=remaining,
                avg_fill_price=self._mark_price,
                last_fill_price=self._mark_price,
                commission=0.0,
                realized_pnl=0.0,
                timestamp=timestamp,
                message="Partial fill",
            )
            self._orders[order.order_id] = {"order": order, "state": state, "partial_done": False, "placed_at": time.monotonic()}
            self._emit(state)
            return state
        if mode == "delayed":
            state = IBOrderState(
                order_id=order.order_id,
                status=IBOrderStatus.SUBMITTED,
                filled_qty=0.0,
                remaining_qty=float(order.quantity),
                avg_fill_price=0.0,
                last_fill_price=0.0,
                commission=0.0,
                realized_pnl=0.0,
                timestamp=timestamp,
                message="Submitted",
            )
            self._orders[order.order_id] = {"order": order, "state": state, "placed_at": time.monotonic()}
            self._emit(state)
            return state
        state = self._fill_order(order, float(order.quantity))
        self._orders[order.order_id] = {"order": order, "state": state, "placed_at": time.monotonic()}
        self._emit(state)
        return state

    def cancel_order(self, order_id: int) -> IBOrderState:
        entry = self._orders.get(int(order_id))
        if not entry:
            return IBOrderState(order_id=int(order_id), status=IBOrderStatus.API_CANCELLED, filled_qty=0.0, remaining_qty=0.0, avg_fill_price=0.0, last_fill_price=0.0, commission=0.0, realized_pnl=0.0, timestamp=dt.datetime.now(dt.timezone.utc).isoformat(), message="Unknown order")
        state = IBOrderState(
            order_id=int(order_id),
            status=IBOrderStatus.CANCELLED,
            filled_qty=float(entry["state"].filled_qty),  # type: ignore[attr-defined]
            remaining_qty=float(entry["state"].remaining_qty),  # type: ignore[attr-defined]
            avg_fill_price=float(entry["state"].avg_fill_price),  # type: ignore[attr-defined]
            last_fill_price=float(entry["state"].last_fill_price),  # type: ignore[attr-defined]
            commission=0.0,
            realized_pnl=0.0,
            timestamp=dt.datetime.now(dt.timezone.utc).isoformat(),
            message="Cancelled",
        )
        entry["state"] = state
        self._emit(state)
        return state

    def get_order_status(self, order_id: int) -> IBOrderState:
        entry = self._orders[int(order_id)]
        order = entry["order"]
        state = entry["state"]
        mode = self._fill_config.mode
        elapsed = time.monotonic() - float(entry.get("placed_at", time.monotonic()))
        if mode == "delayed" and state.status == IBOrderStatus.SUBMITTED and elapsed >= float(self._fill_config.delay_seconds):
            new_state = self._fill_order(order, float(order.quantity))
            entry["state"] = new_state
            self._emit(new_state)
            return new_state
        if mode == "partial" and state.status == IBOrderStatus.PARTIALLY_FILLED and not bool(entry.get("partial_done")):
            remaining = float(state.remaining_qty)
            new_state = self._fill_order(order, float(order.quantity), prior_filled=float(state.filled_qty))
            entry["state"] = new_state
            entry["partial_done"] = True
            if remaining > 0:
                self._emit(new_state)
            return new_state
        return state

    def get_positions(self) -> list[IBPosition]:
        return list(self._positions.values())

    def get_account_summary(self) -> IBAccountSummary:
        gross = sum(float(position.market_value) for position in self._positions.values())
        net = self._cash + gross
        return IBAccountSummary(
            account_id=self.account_id,
            net_liquidation=net,
            total_cash=self._cash,
            buying_power=max(0.0, self._cash * 2.0),
            gross_position_value=gross,
            maintenance_margin=gross * 0.25,
            available_funds=self._cash,
            unrealized_pnl=sum(float(position.unrealized_pnl) for position in self._positions.values()),
        )

    def _fill_order(self, order: IBOrder, fill_qty: float, *, prior_filled: float = 0.0) -> IBOrderState:
        symbol = order.contract_symbol.upper()
        total_filled = prior_filled + fill_qty
        remaining = max(0.0, float(order.quantity) - total_filled)
        if order.action == IBOrderAction.BUY:
            position = self._positions.get(symbol)
            existing_qty = float(position.quantity) if position else 0.0
            existing_cost = float(position.avg_cost) if position else 0.0
            new_qty = existing_qty + fill_qty
            avg_cost = ((existing_qty * existing_cost) + (fill_qty * self._mark_price)) / new_qty if new_qty > 0 else self._mark_price
            market_value = new_qty * self._mark_price
            self._positions[symbol] = IBPosition(self.account_id, symbol, new_qty, avg_cost, market_value, (self._mark_price - avg_cost) * new_qty)
            self._cash -= fill_qty * self._mark_price
        else:
            position = self._positions.get(symbol)
            existing_qty = float(position.quantity) if position else 0.0
            new_qty = max(0.0, existing_qty - fill_qty)
            avg_cost = float(position.avg_cost) if position else self._mark_price
            if new_qty <= 0:
                self._positions.pop(symbol, None)
            else:
                market_value = new_qty * self._mark_price
                self._positions[symbol] = IBPosition(self.account_id, symbol, new_qty, avg_cost, market_value, (self._mark_price - avg_cost) * new_qty)
            self._cash += fill_qty * self._mark_price
        return IBOrderState(
            order_id=order.order_id,
            status=IBOrderStatus.FILLED,
            filled_qty=total_filled,
            remaining_qty=remaining,
            avg_fill_price=self._mark_price,
            last_fill_price=self._mark_price,
            commission=0.0,
            realized_pnl=0.0,
            timestamp=dt.datetime.now(dt.timezone.utc).isoformat(),
            message="Filled",
        )

    def _emit(self, state: IBOrderState) -> None:
        for callback in self._callbacks:
            callback(state)


class IBConnectionManager:
    def __init__(self, backend: IBConnectionBackend, host: str = "127.0.0.1", port: int = 7497, client_id: int = 1):
        self.backend = backend
        self.host = host
        self.port = port
        self.client_id = client_id
        self._connect_lock = threading.Lock()

    def connect(self) -> bool:
        for attempt in range(3):
            if self.backend.connect(self.host, self.port, self.client_id):
                logger.info(
                    "IBKR connection established host=%s port=%s client_id=%s",
                    self.host,
                    self.port,
                    self.client_id,
                )
                return True
            time.sleep(0.25 * (2 ** attempt))
        logger.warning(
            "Unable to establish IBKR connection host=%s port=%s client_id=%s",
            self.host,
            self.port,
            self.client_id,
        )
        return False

    def disconnect(self, cancel_pending: bool = False) -> None:
        if cancel_pending and hasattr(self.backend, "_orders"):
            for order_id, entry in list(getattr(self.backend, "_orders").items()):
                state = entry.get("state")
                if state is None or getattr(state, "status", None) in {IBOrderStatus.FILLED, IBOrderStatus.CANCELLED, IBOrderStatus.API_CANCELLED, IBOrderStatus.INACTIVE}:
                    continue
                try:
                    self.backend.cancel_order(int(order_id))
                except Exception:
                    pass
        self.backend.disconnect()

    def ensure_connected(self) -> bool:
        if self.backend.is_connected():
            return True
        with self._connect_lock:
            if self.backend.is_connected():
                return True
            logger.warning("IBKR connection lost, attempting reconnect...")
            restored = self.connect()
            if restored:
                logger.info("IBKR connection restored")
            return restored

    def health_check(self) -> dict[str, object]:
        return {
            "connected": self.backend.is_connected(),
            "host": self.host,
            "port": self.port,
            "client_id": self.client_id,
            "account_id": getattr(self.backend, "_account_id", "unknown"),
            "market_hours": get_market_hours_status().value,
            "last_check": dt.datetime.now(dt.timezone.utc).isoformat(),
        }


_MOCK_BACKENDS: dict[int, MockIBBackend] = {}
_LIVE_BACKENDS: dict[int, IBConnectionBackend] = {}
_LIVE_BACKENDS_LOCK = threading.Lock()


def get_mock_ib_backend(portfolio_id: int, *, starting_cash: float = 100000.0) -> MockIBBackend:
    backend = _MOCK_BACKENDS.get(int(portfolio_id))
    if backend is None:
        backend = MockIBBackend(starting_cash=starting_cash)
        backend.connect("127.0.0.1", 7497, int(portfolio_id))
        _MOCK_BACKENDS[int(portfolio_id)] = backend
    return backend


def get_ib_backend(
    portfolio_id: int,
    *,
    live: bool = False,
    account_id: str = DEFAULT_IBKR_CONFIG.account_id,
    starting_cash: float = 100000.0,
    config: IBKRConfig = DEFAULT_IBKR_CONFIG,
) -> IBConnectionBackend:
    if live:
        from .ib_live_backend import LiveIBBackend

        derived_client_id = int(config.client_id) + max(1, int(portfolio_id))

        with _LIVE_BACKENDS_LOCK:
            backend = _LIVE_BACKENDS.get(int(portfolio_id))
            if backend is not None and backend.is_connected():
                return backend
            backend = LiveIBBackend(account_id=account_id)
            setattr(backend, "_client_id", derived_client_id)
            connected = backend.connect(config.host, config.port, derived_client_id)
            if connected:
                _LIVE_BACKENDS[int(portfolio_id)] = backend
            return backend
    return get_mock_ib_backend(portfolio_id, starting_cash=starting_cash)
