from __future__ import annotations

import importlib
import sqlite3
import time

from fastapi import FastAPI
from fastapi.testclient import TestClient
import pandas as pd
import pytest

from src.app.routes import regime as regime_route
from src.regime import backup as backup_module
from src.regime import data_validator as data_validator_module
from src.regime import persistence as persistence_module
from src.regime import recovery as recovery_module
from src.regime import scheduled_runner as scheduled_runner_module
from src.regime import watchdog as watchdog_module


@pytest.fixture()
def temp_modules(tmp_path, monkeypatch):
    monkeypatch.setenv("HMM_DATA_DIR", str(tmp_path))
    store = importlib.reload(persistence_module)
    monkeypatch.setattr(store, "DB_PATH", tmp_path / "regime_watch.db")
    store = importlib.reload(persistence_module)
    backup = importlib.reload(backup_module)
    recovery = importlib.reload(recovery_module)
    watchdog = importlib.reload(watchdog_module)
    validator = importlib.reload(data_validator_module)
    scheduled = importlib.reload(scheduled_runner_module)
    store._connect().close()
    return store, backup, recovery, watchdog, validator, scheduled


def _route_client(monkeypatch, runtime: dict) -> TestClient:
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (runtime, None))
    app = FastAPI()
    app.include_router(regime_route.router)
    app.dependency_overrides[regime_route.require_actor] = lambda: "tester"
    app.dependency_overrides[regime_route.db_session] = lambda: iter([None])
    return TestClient(app)


def test_create_backup_produces_valid_copy(temp_modules) -> None:
    store, backup, _recovery, _watchdog, _validator, _scheduled = temp_modules
    store.create_theme("AI")
    result = backup.create_backup("daily")
    assert sqlite3.connect(result["path"]).execute("SELECT 1").fetchone()[0] == 1


def test_cleanup_old_backups_respects_retention(temp_modules) -> None:
    _store, backup, _recovery, _watchdog, _validator, _scheduled = temp_modules
    backup.create_backup("one")
    time.sleep(1)
    backup.create_backup("two")
    time.sleep(1)
    backup.create_backup("three")
    persistence_module.set_setting("backup_max_count", "2")
    result = backup.cleanup_old_backups()
    assert result["remaining"] <= 2


def test_detect_stuck_orders_and_reconcile(temp_modules) -> None:
    store, _backup, recovery, _watchdog, _validator, _scheduled = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    plan = store.create_trade_plan(portfolio["id"], "NVDA", "Buy", 10, "test", proposed_price=100.0)
    with store._connect() as conn:
        conn.execute(
            "UPDATE paper_trade_plan SET status = 'Submitted', updated_at = '2026-03-20T00:00:00+00:00' WHERE id = ?",
            (int(plan["id"]),),
        )
    stuck = recovery.detect_stuck_orders()
    assert len(stuck) == 1
    result = recovery.reconcile_stuck_orders(adapter=None)
    assert result["expired"] == 1


def test_db_integrity_check_passes(temp_modules) -> None:
    _store, _backup, recovery, _watchdog, _validator, _scheduled = temp_modules
    result = recovery.check_db_integrity()
    assert result["integrity"] == "ok"


def test_watchdog_starts_and_stops(temp_modules) -> None:
    _store, _backup, _recovery, watchdog, _validator, _scheduled = temp_modules
    wd = watchdog.start_watchdog(lambda: {"connected": True}, lambda: True, interval=1)
    assert wd.is_running is True
    watchdog.stop_watchdog()
    assert watchdog.get_watchdog() is None


def test_watchdog_fires_connection_lost_alert(temp_modules) -> None:
    store, _backup, _recovery, watchdog, _validator, _scheduled = temp_modules
    wd = watchdog.ConnectionWatchdog(lambda: {"connected": False}, lambda: False, check_interval=1)
    wd._check_connection()
    alerts = store.get_alerts(alert_type="connection_lost", limit=10)
    assert alerts


def test_price_staleness_and_macro_defaults(temp_modules) -> None:
    _store, _backup, _recovery, _watchdog, validator, _scheduled = temp_modules
    stale_frame = pd.DataFrame({"Close": [100.0]}, index=pd.to_datetime(["2026-03-20"]))
    stale = validator.check_price_staleness("NVDA", stale_frame)
    assert stale["valid"] is False
    macro = validator.check_macro_data_quality(20.0, 4.0)
    assert macro["valid"] is False


def test_run_pre_trade_validation_aggregates(temp_modules) -> None:
    _store, _backup, _recovery, _watchdog, validator, _scheduled = temp_modules
    frames = {"NVDA": pd.DataFrame({"Close": [100.0]}, index=pd.to_datetime(["2026-03-20"]))}
    result = validator.run_pre_trade_validation(["NVDA"], price_frames=frames, vix=20.0, yield_10y=4.0)
    assert result["valid"] is False
    assert "NVDA" in result["ticker_results"]


def test_health_and_backup_routes(temp_modules, monkeypatch) -> None:
    store, backup, recovery, watchdog, validator, _scheduled = temp_modules
    runtime = {
        "get_setting": store.get_setting,
        "get_alerts": store.get_alerts,
        "validate_ibkr_readiness": lambda: {"all_clear": True},
    }
    monkeypatch.setattr(regime_route, "_APP_STARTED_AT", time.time() - 5)
    client = _route_client(monkeypatch, runtime)
    assert client.get("/regime/health").status_code == 200
    assert client.post("/regime/backup/create", json={"label": "manual"}).status_code == 200
    assert client.get("/regime/backup/list").status_code == 200
    assert client.post("/regime/recovery/run").status_code == 200
    assert client.get("/regime/data-validation").status_code == 200


def test_run_end_of_day_processing_includes_backup(temp_modules, monkeypatch) -> None:
    store, _backup, _recovery, _watchdog, _validator, scheduled = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    monkeypatch.setattr(scheduled, "compute_daily_snapshot", lambda portfolio_id: {"snapshot_date": "2026-03-29", "equity": 100000.0, "cash": 100000.0, "market_value": 0.0, "realized_pnl": 0.0, "unrealized_pnl": 0.0, "position_count": 0, "trades_today": 0})
    monkeypatch.setattr(scheduled, "run_performance_snapshot", lambda: {"portfolios": []})
    monkeypatch.setattr(scheduled, "check_loss_breach", lambda *args, **kwargs: None)
    result = scheduled.run_end_of_day_processing()
    assert "backup" in result
