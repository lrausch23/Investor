"""Sprint 50c - Shared IBKR backend warm-up tests."""
from __future__ import annotations

from types import SimpleNamespace
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


def test_startup_starts_watchdog_for_ibkr_paper_backend(monkeypatch) -> None:
    calls = {"watchdog": 0, "warm": 0, "account_id": None, "client_id": None}

    class FakeBackend:
        _client_id = 2

        def is_connected(self):
            return True

        def connect(self, host, port, client_id):
            del host, port
            calls["client_id"] = client_id
            return True

    def fake_get_backend(portfolio_id, **kwargs):
        del portfolio_id
        calls["account_id"] = kwargs.get("account_id")
        return FakeBackend()

    def fake_start_watchdog(health_fn, reconnect_fn, interval=60):
        del interval
        calls["watchdog"] += 1
        assert health_fn()["connected"] is True
        assert reconnect_fn() is True
        return None

    def fake_warm(*, config=None):
        del config
        calls["warm"] += 1
        return True

    with patch("src.app.main.init_db", return_value=None), patch(
        "src.regime.recovery.run_startup_recovery",
        return_value={"stuck_orders_found": 0, "reconciled": 0, "expired": 0},
    ), patch(
        "src.regime.ib_connection.get_ib_backend",
        side_effect=fake_get_backend,
    ), patch(
        "src.regime.watchdog.start_watchdog",
        side_effect=fake_start_watchdog,
    ), patch(
        "src.regime.ib_connection.warm_shared_ib_backend",
        side_effect=fake_warm,
    ), patch(
        "src.regime.config.IBKRConfig",
        return_value=IBKRConfig(
            paper_backend=True,
            live_backend=False,
            host="127.0.0.1",
            port=7497,
            account_id="DUP579027",
        ),
    ):
        app = create_app()
        with TestClient(app):
            pass

    assert calls == {"watchdog": 1, "warm": 1, "account_id": "DUP579027", "client_id": 2}


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


def test_account_snapshot_ignores_zero_quantity_broker_positions(monkeypatch) -> None:
    del monkeypatch

    class FakeBackend:
        def get_account_summary(self):
            return SimpleNamespace(
                account_id="DUP579027",
                net_liquidation=1_000_000.0,
                total_cash=990_000.0,
                buying_power=2_000_000.0,
                gross_position_value=10_000.0,
                maintenance_margin=1_000.0,
                available_funds=990_000.0,
                unrealized_pnl=0.0,
            )

        def get_positions(self):
            return [
                SimpleNamespace(
                    account_id="DUP579027",
                    contract_symbol="NVDA",
                    quantity=44.0,
                    avg_cost=215.0,
                    market_value=9_460.0,
                    unrealized_pnl=0.0,
                ),
                SimpleNamespace(
                    account_id="DUP579027",
                    contract_symbol="SPY",
                    quantity=0.0,
                    avg_cost=0.0,
                    market_value=0.0,
                    unrealized_pnl=0.0,
                ),
                SimpleNamespace(
                    account_id="DUP579027",
                    contract_symbol="AVGO",
                    quantity=20.0,
                    avg_cost=486.0,
                    market_value=9_720.0,
                    unrealized_pnl=0.0,
                ),
            ]

    with patch("src.regime.ib_connection.get_ib_backend", return_value=FakeBackend()), patch(
        "src.regime.persistence.get_paper_portfolio",
        return_value={"id": 5, "name": "Agent 1"},
    ), patch(
        "src.regime.persistence.get_paper_portfolio_summary",
        return_value={
            "positions": [
                {"ticker": "AVGO", "quantity": 5.0, "current_value": 2_430.0},
                {"ticker": "NVDA", "quantity": 11.0, "current_value": 2_365.0},
            ]
        },
    ), patch("src.regime.persistence.get_setting", return_value="5"):
        response = _client().get("/regime/ibkr/account-snapshot?portfolio_id=5")

    assert response.status_code == 200
    payload = response.json()
    assert [row["ticker"] for row in payload["positions"]] == ["NVDA", "AVGO"]
    reconciliation = payload["reconciliation"]
    assert [row["ticker"] for row in reconciliation["rows"]] == ["AVGO", "NVDA"]
    assert reconciliation["broker_only_count"] == 0
    assert reconciliation["mismatch_count"] == 2


def test_account_snapshot_agent_beta_scope_reconciles_aggregate_ledgers(monkeypatch) -> None:
    del monkeypatch

    class FakeBackend:
        def get_account_summary(self):
            return SimpleNamespace(
                account_id="DUP579027",
                net_liquidation=1_000_000.0,
                total_cash=990_000.0,
                buying_power=2_000_000.0,
                gross_position_value=19_180.0,
                maintenance_margin=1_000.0,
                available_funds=990_000.0,
                unrealized_pnl=0.0,
            )

        def get_positions(self):
            return [
                SimpleNamespace(
                    account_id="DUP579027",
                    contract_symbol="NVDA",
                    quantity=44.0,
                    avg_cost=215.0,
                    market_value=9_460.0,
                    unrealized_pnl=0.0,
                ),
                SimpleNamespace(
                    account_id="DUP579027",
                    contract_symbol="AVGO",
                    quantity=20.0,
                    avg_cost=486.0,
                    market_value=9_720.0,
                    unrealized_pnl=0.0,
                ),
            ]

    summaries = {
        item_id: {
            "positions": [
                {"ticker": "AVGO", "quantity": 5.0, "current_value": 2_430.0},
                {"ticker": "NVDA", "quantity": 11.0, "current_value": 2_365.0},
            ]
        }
        for item_id in (5, 6, 7, 8)
    }

    with patch("src.regime.ib_connection.get_ib_backend", return_value=FakeBackend()), patch(
        "src.regime.persistence.get_paper_portfolio_summary",
        side_effect=lambda item_id: summaries[int(item_id)],
    ), patch("src.regime.persistence.get_setting", return_value="5,6,7,8"):
        response = _client().get("/regime/ibkr/account-snapshot?portfolio_id=5&scope=agent_beta")

    assert response.status_code == 200
    reconciliation = response.json()["reconciliation"]
    assert reconciliation["broker_only_count"] == 0
    assert reconciliation["app_only_count"] == 0
    assert reconciliation["mismatch_count"] == 0
    assert [(row["ticker"], row["status"], row["ibkr_quantity"], row["app_quantity"]) for row in reconciliation["rows"]] == [
        ("AVGO", "matched", 20.0, 20.0),
        ("NVDA", "matched", 44.0, 44.0),
    ]


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
