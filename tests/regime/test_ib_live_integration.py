from __future__ import annotations

import time

import pytest

SKIP_REASON = "Requires live TWS/Gateway connection and paper trading account"


@pytest.fixture()
def live_backend():
    pytest.importorskip("ib_insync", reason=SKIP_REASON)
    from src.regime.ib_live_backend import LiveIBBackend

    backend = LiveIBBackend(account_id="DUP579027")
    connected = backend.connect("127.0.0.1", 7497, 99)
    if not connected:
        pytest.skip(SKIP_REASON)
    yield backend
    backend.disconnect()


def test_live_connection(live_backend):
    assert live_backend.is_connected()


def test_live_account_summary(live_backend):
    summary = live_backend.get_account_summary()
    assert summary.account_id == "DUP579027"
    assert summary.net_liquidation >= 0


def test_live_positions(live_backend):
    positions = live_backend.get_positions()
    assert isinstance(positions, list)


def test_live_order_lifecycle(live_backend):
    summary = live_backend.get_account_summary()
    assert summary.net_liquidation > 0

    from src.regime.ib_types import IBOrder, IBOrderAction, IBOrderType

    order = IBOrder(
        order_id=live_backend.next_order_id(),
        contract_symbol="SPY",
        action=IBOrderAction.BUY,
        quantity=1,
        order_type=IBOrderType.MARKET,
    )
    result = live_backend.place_order(order)
    assert result.order_id

    for _ in range(5):
        time.sleep(2)
        status = live_backend.get_order_status(result.order_id)
        if status.status.value.lower() in {"filled", "submitted", "presubmitted"}:
            break
    assert status.status.value.lower() in {"filled", "submitted", "pendingsubmit", "presubmitted"}

    positions = live_backend.get_positions()
    assert isinstance(positions, list)


def test_live_order_cancel(live_backend):
    from src.regime.ib_types import IBOrder, IBOrderAction, IBOrderType

    order = IBOrder(
        order_id=live_backend.next_order_id(),
        contract_symbol="SPY",
        action=IBOrderAction.BUY,
        quantity=1,
        order_type=IBOrderType.LIMIT,
        limit_price=1.0,
    )
    result = live_backend.place_order(order)
    assert result.order_id

    time.sleep(1)
    cancelled = live_backend.cancel_order(result.order_id)
    assert cancelled.status.value.lower() in {"cancelled", "apicancelled", "inactive", "submitted", "pendingcancel"}

    time.sleep(1)
    status = live_backend.get_order_status(result.order_id)
    assert status.status.value.lower() in {"cancelled", "inactive", "submitted", "apicancelled"}


def test_live_account_validation(live_backend):
    summary = live_backend.get_account_summary()
    assert summary.account_id == "DUP579027"
