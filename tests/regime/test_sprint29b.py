from __future__ import annotations

import datetime as dt
import importlib
import inspect
from pathlib import Path
from types import SimpleNamespace

import pytest

from tests.regime import test_sprint29a as sprint29a
from tests import test_regime_route as route_tests

from src.app.main import create_app
from src.app.routes import regime as regime_route
from src.regime import config as config_module
from src.regime import ib_connection as ib_connection_module
from src.regime import ib_types as ib_types_module
from src.regime import ibkr_adapter as ibkr_adapter_module
from src.regime import paper_trading as paper_trading_module
from src.regime import persistence as persistence_module
from src.regime import scheduled_runner as scheduled_runner_module


@pytest.fixture()
def temp_modules(tmp_path, monkeypatch):
    monkeypatch.setenv("HMM_DATA_DIR", str(tmp_path))
    monkeypatch.delenv("IBKR_LIVE_BACKEND", raising=False)
    monkeypatch.delenv("IBKR_PORT", raising=False)
    monkeypatch.delenv("IBKR_HOST", raising=False)
    monkeypatch.delenv("IBKR_CLIENT_ID", raising=False)
    monkeypatch.delenv("IBKR_ACCOUNT_ID", raising=False)
    monkeypatch.delenv("IBKR_TIMEOUT", raising=False)
    store = importlib.reload(persistence_module)
    monkeypatch.setattr(store, "DB_PATH", tmp_path / "regime_watch.db")
    config = importlib.reload(config_module)
    ib_types = importlib.reload(ib_types_module)
    ib_connection = importlib.reload(ib_connection_module)
    ibkr = importlib.reload(ibkr_adapter_module)
    paper = importlib.reload(paper_trading_module)
    scheduled = importlib.reload(scheduled_runner_module)
    return store, config, ib_types, ib_connection, ibkr, paper, scheduled


def test_ibkr_adapter_no_db_path_param() -> None:
    signature = inspect.signature(ibkr_adapter_module.IBKRBrokerAdapter)
    assert "db_path" not in signature.parameters


def test_holiday_calendar_2027(temp_modules) -> None:
    _store, _config, ib_types, _ib_connection, _ibkr, _paper, _scheduled = temp_modules
    now = dt.datetime(2027, 1, 1, 10, 0, tzinfo=ib_types.ET)
    assert ib_types.get_market_hours_status(now) == ib_types.MarketHoursStatus.CLOSED


def test_regime_page_renders_control_bar(monkeypatch) -> None:
    monkeypatch.setattr(
        regime_route,
        "load_payload",
        lambda: {"rows": [{"ticker": "NVDA", "regime": "Bull"}], "last_run_display": "2026-03-26 09:00:00 EDT", "warnings": []},
    )
    client = route_tests._client(monkeypatch)
    response = client.get("/regime")
    assert response.status_code == 200
    assert 'class="regime-control-grid"' in response.text
    assert 'id="regimeMonitoringSection"' in response.text
    assert "holdings-aside" not in response.text


def test_regime_page_secondary_column_toggle_markup(monkeypatch) -> None:
    monkeypatch.setattr(regime_route, "load_payload", lambda: {"rows": [{"ticker": "NVDA", "regime": "Bull"}], "warnings": []})
    client = route_tests._client(monkeypatch)
    response = client.get("/regime")
    assert response.status_code == 200
    assert 'id="regimeToggleColumns"' in response.text


def test_regime_css_contains_responsive_breakpoints() -> None:
    css = Path("/Volumes/T9/Projects/Dev/Investor/src/app/static/app.css").read_text()
    assert "@media (max-width: 1200px)" in css
    assert "@media (max-width: 980px)" in css
    assert "@media (max-width: 768px)" in css


def test_paper_trading_status_border_colors_css() -> None:
    css = Path("/Volumes/T9/Projects/Dev/Investor/src/app/static/app.css").read_text()
    assert ".regime-plan-card--pending" in css
    assert ".regime-plan-card--approved" in css
    assert ".regime-plan-card--executed" in css
    assert ".regime-plan-card--rejected" in css
    assert ".regime-plan-card--submitted" in css


def test_get_ib_backend_live_false(temp_modules) -> None:
    _store, config, _ib_types, ib_connection, _ibkr, _paper, _scheduled = temp_modules
    backend = ib_connection.get_ib_backend(1, live=False, account_id=config.DEFAULT_IBKR_CONFIG.account_id)
    assert backend.__class__.__name__ == "MockIBBackend"


def test_get_ib_backend_live_true_import(temp_modules) -> None:
    _store, config, _ib_types, ib_connection, _ibkr, _paper, _scheduled = temp_modules
    import sys

    class _FakeIB:
        def __init__(self):
            self.orderStatusEvent = []
            self.client = SimpleNamespace(getReqId=lambda: 1)

        def connect(self, *args, **kwargs):
            return True

        def isConnected(self):
            return False

    sys.modules["ib_insync"] = SimpleNamespace(IB=_FakeIB)
    backend = ib_connection.get_ib_backend(1, live=True, account_id=config.DEFAULT_IBKR_CONFIG.account_id)
    assert backend.__class__.__name__ == "LiveIBBackend"
    assert getattr(backend, "_client_id", None) == 2


def test_get_ib_backend_live_caches_per_portfolio(temp_modules, monkeypatch) -> None:
    _store, config, _ib_types, ib_connection, _ibkr, _paper, _scheduled = temp_modules
    import sys

    class _FakeIB:
        def __init__(self):
            class _Event:
                def __iadd__(self, callback):
                    return self

            self.orderStatusEvent = _Event()
            self.client = SimpleNamespace(getReqId=lambda: 1)
            self._connected = False

        def connect(self, *args, **kwargs):
            self._connected = True
            return True

        def isConnected(self):
            return self._connected

        def managedAccounts(self):
            return [config.DEFAULT_IBKR_CONFIG.account_id]

    monkeypatch.setitem(sys.modules, "ib_insync", SimpleNamespace(IB=_FakeIB))
    backend1 = ib_connection.get_ib_backend(2, live=True, account_id=config.DEFAULT_IBKR_CONFIG.account_id)
    backend2 = ib_connection.get_ib_backend(2, live=True, account_id=config.DEFAULT_IBKR_CONFIG.account_id)
    backend3 = ib_connection.get_ib_backend(3, live=True, account_id=config.DEFAULT_IBKR_CONFIG.account_id)
    assert backend1 is backend2
    assert backend1 is not backend3
    assert getattr(backend1, "_client_id", None) == 3
    assert getattr(backend3, "_client_id", None) == 4


def test_get_ib_backend_live_caches_per_execution_client(temp_modules, monkeypatch) -> None:
    _store, config, _ib_types, ib_connection, _ibkr, _paper, _scheduled = temp_modules
    import sys

    class _FakeIB:
        def __init__(self):
            class _Event:
                def __iadd__(self, callback):
                    return self

            self.orderStatusEvent = _Event()
            self.client = SimpleNamespace(getReqId=lambda: 1)
            self._connected = False

        def connect(self, *args, **kwargs):
            self._connected = True
            return True

        def isConnected(self):
            return self._connected

        def managedAccounts(self):
            return [config.DEFAULT_IBKR_CONFIG.account_id]

    monkeypatch.setitem(sys.modules, "ib_insync", SimpleNamespace(IB=_FakeIB))
    monitor_backend = ib_connection.get_ib_backend(2, live=True, account_id=config.DEFAULT_IBKR_CONFIG.account_id)
    execution_backend = ib_connection.get_ib_backend(2, live=True, account_id=config.DEFAULT_IBKR_CONFIG.account_id, client_id_offset=20)

    assert monitor_backend is not execution_backend
    assert getattr(monitor_backend, "_client_id", None) == 3
    assert getattr(execution_backend, "_client_id", None) == 23


def test_ibkr_config_defaults(temp_modules) -> None:
    _store, config, _ib_types, _ib_connection, _ibkr, _paper, _scheduled = temp_modules
    defaults = config.DEFAULT_IBKR_CONFIG
    assert defaults.host == "127.0.0.1"
    assert defaults.port == 7497
    assert defaults.account_id == "DUP579027"
    assert defaults.live_backend is False


def test_monitoring_endpoint_returns_account_summary(temp_modules, monkeypatch) -> None:
    store, config, _ib_types, ib_connection, ibkr, paper, _scheduled = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0, broker_type="ibkr")
    runtime = sprint29a._runtime(store, paper, sprint29a.broker_module, ib_connection, ibkr, config)
    runtime["get_ib_backend"] = ib_connection.get_ib_backend
    runtime["DEFAULT_IBKR_CONFIG"] = config.DEFAULT_IBKR_CONFIG
    runtime["count_todays_trades"] = store.count_todays_trades
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (runtime, None))
    monkeypatch.setattr(regime_route, "load_payload", lambda: {"rows": []})
    app = create_app()
    from fastapi.testclient import TestClient

    client = TestClient(app)
    response = client.get(f"/regime/paper-portfolio/{portfolio['id']}/monitoring")
    assert response.status_code == 200
    payload = response.json()
    assert "account" in payload
    assert "guardrails" in payload
    assert "connection" in payload


def test_save_and_retrieve_daily_snapshot(temp_modules) -> None:
    store, _config, _ib_types, _ib_connection, _ibkr, _paper, _scheduled = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    saved = store.save_daily_snapshot(portfolio["id"], "2026-03-26", equity=101000.0, cash=50000.0, market_value=51000.0, realized_pnl=400.0, unrealized_pnl=600.0, position_count=3, trades_today=2)
    rows = store.get_daily_snapshots(portfolio["id"])
    assert saved["snapshot_date"] == "2026-03-26"
    assert rows[0]["equity"] == pytest.approx(101000.0)


def test_compute_daily_snapshot_values(temp_modules, monkeypatch) -> None:
    store, _config, _ib_types, _ib_connection, _ibkr, paper, _scheduled = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    store.open_paper_position(portfolio["id"], "NVDA", 10, 100.0, "2026-03-20T00:00:00+00:00")
    monkeypatch.setattr(store, "count_todays_trades", lambda portfolio_id: 3)
    snapshot = paper.compute_daily_snapshot(portfolio["id"])
    assert snapshot["portfolio_id"] == portfolio["id"]
    assert snapshot["position_count"] == 1
    assert "equity" in snapshot


@pytest.mark.parametrize(("exit_price", "outcome"), [(120.0, "win"), (80.0, "loss")])
def test_record_trade_outcome(temp_modules, exit_price, outcome) -> None:
    _store, _config, _ib_types, _ib_connection, _ibkr, paper, _scheduled = temp_modules
    position = {"ticker": "NVDA", "entry_price": 100.0, "entry_date": "2026-03-20T00:00:00+00:00", "exit_date": "2026-03-25T00:00:00+00:00"}
    result = paper.record_trade_outcome(1, position, exit_price)
    assert result["outcome"] == outcome
    assert result["holding_days"] == 5


def test_end_of_day_processing_saves_snapshots(temp_modules, monkeypatch) -> None:
    store, config, _ib_types, ib_connection, _ibkr, paper, scheduled = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    monkeypatch.setattr(scheduled, "list_paper_portfolios", lambda include_closed=False: [portfolio])
    monkeypatch.setattr(scheduled, "compute_daily_snapshot", lambda portfolio_id: {"snapshot_date": "2026-03-26", "portfolio_id": portfolio_id, "equity": 100500.0, "cash": 50000.0, "market_value": 50500.0, "realized_pnl": 250.0, "unrealized_pnl": 250.0, "position_count": 2, "trades_today": 1})
    monkeypatch.setattr(scheduled, "get_paper_positions", lambda portfolio_id, status="Closed": [])
    monkeypatch.setattr(scheduled, "DEFAULT_IBKR_CONFIG", config.DEFAULT_IBKR_CONFIG)
    monkeypatch.setattr(scheduled, "get_ib_backend", lambda *args, **kwargs: ib_connection.MockIBBackend())
    result = scheduled.run_end_of_day_processing()
    assert result["snapshot_count"] == 1
    assert store.get_daily_snapshots(portfolio["id"])[0]["equity"] == pytest.approx(100500.0)
