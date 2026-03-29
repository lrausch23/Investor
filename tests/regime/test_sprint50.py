from __future__ import annotations

import importlib
import logging
import sqlite3
import time
from pathlib import Path
from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest

from src.app.routes import regime as regime_route
from src.regime import backup as backup_module
from src.regime import logging_config as logging_config_module
from src.regime import oob_watchdog as oob_watchdog_module
from src.regime import persistence as persistence_module
from src.regime import scheduled_runner as scheduled_runner_module


@pytest.fixture()
def temp_modules(tmp_path, monkeypatch):
    monkeypatch.setenv("HMM_DATA_DIR", str(tmp_path))
    store = importlib.reload(persistence_module)
    monkeypatch.setattr(store, "DB_PATH", tmp_path / "regime_watch.db")
    store = importlib.reload(persistence_module)
    backup = importlib.reload(backup_module)
    scheduled = importlib.reload(scheduled_runner_module)
    watchdog = importlib.reload(oob_watchdog_module)
    logging_config = importlib.reload(logging_config_module)
    store._connect().close()
    return store, backup, scheduled, watchdog, logging_config


def _route_client(monkeypatch, runtime: dict) -> TestClient:
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (runtime, None))
    app = FastAPI()
    app.include_router(regime_route.router)
    app.dependency_overrides[regime_route.require_actor] = lambda: "tester"
    app.dependency_overrides[regime_route.db_session] = lambda: iter([None])
    return TestClient(app)


def test_setup_regime_logging_adds_rotating_file_handler(temp_modules, tmp_path) -> None:
    _store, _backup, _scheduled, _watchdog, logging_config = temp_modules
    logging_config.setup_regime_logging(log_dir=str(tmp_path))
    root_logger = logging.getLogger()
    assert any("investor.log" in str(getattr(handler, "baseFilename", "")) for handler in root_logger.handlers)


def test_check_app_liveness_prefers_epoch(temp_modules) -> None:
    store, _backup, _scheduled, watchdog, _logging = temp_modules
    now = time.time()
    store.set_setting("heartbeat_epoch", str(now))
    result = watchdog.check_app_liveness(str(store.DB_PATH))
    assert result["alive"] is True
    assert result["age_seconds"] is not None


def test_check_daily_pnl_reads_latest_snapshots(temp_modules) -> None:
    store, _backup, _scheduled, watchdog, _logging = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    store.save_daily_snapshot(portfolio["id"], "2026-03-28", equity=100000.0, cash=100000.0, market_value=0.0, realized_pnl=100.0, unrealized_pnl=-20.0)
    result = watchdog.check_daily_pnl(str(store.DB_PATH))
    assert result["total_daily_pnl"] == 80.0
    assert result["portfolios"][0]["portfolio_id"] == int(portfolio["id"])


def test_run_watchdog_loop_dry_run_triggers_on_stale_heartbeat(temp_modules, monkeypatch) -> None:
    store, _backup, _scheduled, watchdog, _logging = temp_modules
    stale = time.time() - 600
    store.set_setting("heartbeat_epoch", str(stale))
    monkeypatch.setattr(watchdog, "check_daily_pnl", lambda db_path: {"total_daily_pnl": 0.0, "loss_limit": 5000.0, "limit_breached": False, "portfolios": []})
    result = watchdog.run_watchdog_loop(str(store.DB_PATH), dry_run=True, stop_after_one=True)
    assert result["triggered"] is True
    assert result["reason"] == "app_unresponsive"


def test_live_ib_backend_cancel_and_flatten(monkeypatch) -> None:
    from src.regime.ib_live_backend import LiveIBBackend

    class FakeTrade:
        def __init__(self, order_id):
            self.order = SimpleNamespace(orderId=order_id)

    class FakeOrder:
        def __init__(self, order_id):
            self.orderId = order_id

    class FakeIB:
        def openOrders(self):
            return [FakeOrder(1), FakeOrder(2)]

        def cancelOrder(self, order):
            return None

        def placeOrder(self, contract, order):
            return FakeTrade(99)

    class FakeThread:
        def run(self, fn, *args, **kwargs):
            return fn(*args)

    backend = LiveIBBackend(account_id="DUP579027")
    backend._ib = FakeIB()
    monkeypatch.setattr("src.regime.ib_live_backend.get_ib_thread", lambda: FakeThread())
    monkeypatch.setattr(backend, "get_positions", lambda: [SimpleNamespace(contract_symbol="NVDA", quantity=10.0)])
    cancelled = backend.cancel_all_orders()
    flattened = backend.flatten_all_positions()
    assert len(cancelled) == 2
    assert flattened[0]["ticker"] == "NVDA"


def test_health_route_exposes_sprint50_fields(temp_modules, monkeypatch) -> None:
    store, _backup, _scheduled, _watchdog, _logging = temp_modules
    models_dir = Path(store.DB_PATH).parent / "models"
    models_dir.mkdir(parents=True, exist_ok=True)
    model_path = models_dir / "meta_labeler_v2.json"
    model_path.write_text('{"ok": true}', encoding="utf-8")
    store.set_setting("meta_labeler_active_version", "2")
    store.set_setting("watchdog_heartbeat", "2026-03-29T12:00:00+00:00")
    store.set_setting("heartbeat_epoch", str(time.time()))
    store.set_setting("last_regime_check_at", "2026-03-29T11:55:00+00:00")
    store.set_setting("last_paper_plans_at", "2026-03-29T11:58:00+00:00")
    runtime = {
        "get_setting": store.get_setting,
        "get_alerts": store.get_alerts,
        "validate_ibkr_readiness": lambda: {"all_clear": True},
    }
    monkeypatch.setattr(regime_route, "_APP_STARTED_AT", time.time() - 5)
    client = _route_client(monkeypatch, runtime)
    payload = client.get("/regime/health").json()
    assert "db_size_bytes" in payload
    assert "model" in payload
    assert "heartbeat_epoch" in payload
    assert "last_paper_plans" in payload


def test_scheduled_runner_writes_heartbeat_and_timestamps(temp_modules, monkeypatch) -> None:
    store, _backup, scheduled, _watchdog, _logging = temp_modules
    store.create_paper_portfolio("Sandbox", 100000.0)
    monkeypatch.setattr(scheduled, "check_vix_freeze", lambda: {"vix": 20.0, "frozen": False})
    monkeypatch.setattr(scheduled, "load_payload", lambda: {"rows": []})
    monkeypatch.setattr(scheduled, "run_pre_trade_validation", lambda *args, **kwargs: {"valid": True, "issues": []})
    monkeypatch.setattr(scheduled, "sweep_monitoring_alerts", lambda portfolio_id: [])
    monkeypatch.setattr(scheduled, "expire_stale_plans", lambda portfolio_id: 0)
    monkeypatch.setattr(scheduled, "generate_daily_plans", lambda *args, **kwargs: {"buy_plans": [], "exit_plans": []})
    monkeypatch.setattr(scheduled, "auto_approve_plans", lambda portfolio_id: {"approved": 0})
    result = scheduled.run_scheduled_paper_plans()
    assert result["cached_regime_count"] == 0
    assert store.get_setting("watchdog_heartbeat")
    assert store.get_setting("heartbeat_epoch")
    assert store.get_setting("last_paper_plans_at")


def test_end_of_day_writes_heartbeat(temp_modules, monkeypatch) -> None:
    store, _backup, scheduled, _watchdog, _logging = temp_modules
    store.create_paper_portfolio("Sandbox", 100000.0)
    monkeypatch.setattr(scheduled, "compute_daily_snapshot", lambda portfolio_id: {"snapshot_date": "2026-03-29", "equity": 100000.0, "cash": 100000.0, "market_value": 0.0, "realized_pnl": 0.0, "unrealized_pnl": 0.0, "position_count": 0, "trades_today": 0})
    monkeypatch.setattr(scheduled, "run_performance_snapshot", lambda: {"portfolios": []})
    monkeypatch.setattr(scheduled, "check_loss_breach", lambda *args, **kwargs: None)
    scheduled.run_end_of_day_processing()
    assert store.get_setting("watchdog_heartbeat")
    assert store.get_setting("heartbeat_epoch")


def test_watchdog_logging_creates_watchdog_file(temp_modules, tmp_path) -> None:
    _store, _backup, _scheduled, watchdog, _logging = temp_modules
    watchdog.setup_watchdog_logging(str(tmp_path))
    assert any("watchdog.log" in str(getattr(handler, "baseFilename", "")) for handler in logging.getLogger("oob_watchdog").handlers)


def test_oob_emergency_liquidate_handles_connection_failure(temp_modules, monkeypatch) -> None:
    store, _backup, _scheduled, watchdog, _logging = temp_modules
    monkeypatch.setattr(watchdog, "connect_ib_direct", lambda **kwargs: None)
    result = watchdog.emergency_liquidate(str(store.DB_PATH))
    assert result["connected"] is False
