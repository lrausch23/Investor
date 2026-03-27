from __future__ import annotations

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
