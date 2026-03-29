"""Sprint 50c - Shared IBKR backend warm-up tests."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.app.main import create_app
from src.app.routes import regime as regime_route
from src.regime import ib_connection as ib_connection_module
from src.regime.config import IBKRConfig


def test_warm_shared_returns_true_when_connected(monkeypatch) -> None:
    class FakeIB:
        def isConnected(self):
            return True

    class FakeBackend:
        _ib = FakeIB()

    monkeypatch.setattr(ib_connection_module, "get_shared_ib_backend", lambda **kwargs: FakeBackend())

    result = ib_connection_module.warm_shared_ib_backend()
    assert result is True


def test_warm_shared_returns_false_when_no_gateway(monkeypatch) -> None:
    monkeypatch.setattr(ib_connection_module, "get_shared_ib_backend", lambda **kwargs: None)
    result = ib_connection_module.warm_shared_ib_backend()
    assert result is False


def test_warm_shared_returns_false_on_exception(monkeypatch) -> None:
    def explode(**kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(ib_connection_module, "get_shared_ib_backend", explode)
    result = ib_connection_module.warm_shared_ib_backend()
    assert result is False


def test_warm_shared_never_raises(monkeypatch) -> None:
    def explode(**kwargs):
        raise ConnectionRefusedError("port closed")

    monkeypatch.setattr(ib_connection_module, "get_shared_ib_backend", explode)
    result = ib_connection_module.warm_shared_ib_backend()
    assert result is False


def test_startup_calls_warm_shared(monkeypatch) -> None:
    warm_called = {"count": 0, "config": None}
    class FakeBackend:
        def is_connected(self):
            return True

        def connect(self, host, port, client_id):
            del host, port, client_id
            return True

    def fake_warm(*, config=None):
        warm_called["count"] += 1
        warm_called["config"] = config
        return True

    with patch("src.app.main.init_db", return_value=None), patch(
        "src.regime.recovery.run_startup_recovery",
        return_value={"stuck_orders_found": 0, "reconciled": 0, "expired": 0},
    ), patch(
        "src.regime.ib_connection.get_ib_backend",
        return_value=FakeBackend(),
    ), patch(
        "src.regime.watchdog.start_watchdog",
        return_value=None,
    ), patch(
        "src.regime.ib_connection.warm_shared_ib_backend",
        side_effect=fake_warm,
    ), patch(
        "src.regime.config.IBKRConfig",
        return_value=IBKRConfig(live_backend=True),
    ):
        app = create_app()
        with TestClient(app):
            pass

    assert warm_called["count"] == 1
    assert isinstance(warm_called["config"], IBKRConfig)


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(regime_route.router)
    app.dependency_overrides[regime_route.require_actor] = lambda: "tester"
    app.dependency_overrides[regime_route.db_session] = lambda: iter([None])
    return TestClient(app)


def test_test_connection_warms_shared_on_success(monkeypatch) -> None:
    import socket

    warm_called = {"count": 0}

    monkeypatch.setattr(socket, "create_connection", lambda addr, timeout=None: MagicMock())

    mock_summary = MagicMock()
    mock_summary.account_id = "DUP579027"
    mock_summary.net_liquidation = 100000.0

    mock_backend = MagicMock()
    mock_backend.connect.return_value = True
    mock_backend.get_account_summary.return_value = mock_summary
    mock_backend.disconnect.return_value = None

    with patch("src.regime.ib_live_backend.LiveIBBackend", return_value=mock_backend):
        def fake_warm(*, config=None):
            del config
            warm_called["count"] += 1
            return True

        with patch("src.regime.ib_connection.warm_shared_ib_backend", fake_warm):
            client = _client()
            response = client.post("/regime/ibkr/test-connection")
            data = response.json()
            assert response.status_code == 200
            assert data["ibkr_connected"] is True
            assert data.get("market_data_connected") is True
            assert warm_called["count"] == 1


def test_test_connection_no_warm_on_failure(monkeypatch) -> None:
    import socket

    def fail_connect(addr, timeout=None):
        raise ConnectionRefusedError("port closed")

    monkeypatch.setattr(socket, "create_connection", fail_connect)

    warm_called = {"count": 0}

    with patch("src.regime.ib_connection.warm_shared_ib_backend") as mock_warm:
        mock_warm.side_effect = lambda **kwargs: (warm_called.update(count=warm_called["count"] + 1) or True)
        client = _client()
        response = client.post("/regime/ibkr/test-connection")
        data = response.json()
        assert response.status_code == 200
        assert data["tcp_reachable"] is False
        assert warm_called["count"] == 0


def test_is_available_true_after_warm(monkeypatch) -> None:
    from src.regime import ibkr_market_data as ibkr_md

    class FakeIB:
        def isConnected(self):
            return True

    class FakeBackend:
        _ib = FakeIB()

    monkeypatch.setattr(ibkr_md, "get_shared_ib_backend", lambda **kwargs: FakeBackend())

    provider = ibkr_md.IBKRMarketDataProvider()
    assert provider.is_available() is True
