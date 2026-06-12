from __future__ import annotations

import importlib

from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest

from src.app.routes import regime as regime_route
from src.regime import persistence as persistence_module
from src.regime import scheduled_runner as scheduled_runner_module
from src.regime import thesis_monitor as thesis_monitor_module


@pytest.fixture()
def temp_modules(tmp_path, monkeypatch):
    monkeypatch.setenv("HMM_DATA_DIR", str(tmp_path))
    store = importlib.reload(persistence_module)
    monitor = importlib.reload(thesis_monitor_module)
    scheduled = importlib.reload(scheduled_runner_module)
    store._connect().close()
    return store, monitor, scheduled


def test_hbm_monitor_alerts_on_supply_loosening(temp_modules, monkeypatch) -> None:
    store, monitor, _scheduled = temp_modules

    def fake_news(ticker: str, limit: int = 8) -> list[dict]:
        if ticker == "MU":
            return [
                {
                    "title": "Samsung HBM4 capacity ramp improves availability and adds pricing pressure",
                    "summary": "New HBM capacity and yields improve as lead times shorten.",
                    "publisher": "Example Wire",
                    "link": "https://example.com/hbm-supply",
                    "published_at": "2026-06-05T12:00:00+00:00",
                }
            ]
        return []

    monkeypatch.setattr(monitor, "fetch_recent_news", fake_news)

    result = monitor.run_hbm_thesis_monitor(save=True, dispatch=False)

    assert result["status"] in {"watch", "reunderwrite"}
    assert result["severity"] in {"warning", "critical"}
    assert result["should_alert"] is True
    alerts = store.get_alerts(alert_type="thesis_monitor", limit=5)
    assert len(alerts) == 1
    assert alerts[0]["ticker"] == "MU"
    assert alerts[0]["data"]["monitor_key"] == "hbm_mu"
    latest = store.get_latest_thesis_monitor_run("hbm_mu")
    assert latest is not None
    assert latest["alert_id"] == alerts[0]["id"]


def test_hbm_monitor_intact_on_tight_supply_evidence(temp_modules, monkeypatch) -> None:
    store, monitor, _scheduled = temp_modules

    monkeypatch.setattr(
        monitor,
        "fetch_recent_news",
        lambda ticker, limit=8: [
            {
                "title": "HBM remains sold out through 2027 as supply constrained demand exceeds supply",
                "summary": "Limited availability continues for AI accelerators.",
                "publisher": "Example Wire",
                "link": "https://example.com/hbm-tight",
                "published_at": "2026-06-05T12:00:00+00:00",
            }
        ]
        if ticker == "MU"
        else [],
    )

    result = monitor.run_hbm_thesis_monitor(save=True, dispatch=False)

    assert result["status"] == "intact"
    assert result["severity"] == "info"
    assert result["should_alert"] is False
    assert store.get_alerts(alert_type="thesis_monitor", limit=5) == []
    latest = store.get_latest_thesis_monitor_run("hbm_mu")
    assert latest is not None
    assert latest["status"] == "intact"
    assert latest["evidence"][0]["direction"] == "support"


def test_scheduled_thesis_monitor_honors_enabled_setting(temp_modules) -> None:
    store, _monitor, scheduled = temp_modules
    store.set_setting("thesis_monitor_hbm_enabled", "false")

    result = scheduled.run_scheduled_thesis_monitors()

    assert result["enabled"] is False
    assert result["runs"] == []
    assert store.get_latest_thesis_monitor_run("hbm_mu") is None


def test_hbm_thesis_monitor_routes(monkeypatch) -> None:
    latest = {
        "id": 7,
        "monitor_key": "hbm_mu",
        "primary_ticker": "MU",
        "status": "watch",
        "severity": "warning",
        "risk_score": 42.0,
        "thesis": "HBM tightness thesis",
        "evidence": [],
        "tickers_scanned": ["MU"],
        "created_at": "2026-06-05T12:00:00+00:00",
    }
    runtime = {
        "get_latest_thesis_monitor_run": lambda monitor_key="hbm_mu": latest,
        "get_thesis_monitor_runs": lambda monitor_key="hbm_mu", limit=10: [latest],
        "hbm_thesis_monitor_config": lambda: {"enabled": True, "tickers": ["MU"]},
        "run_hbm_thesis_monitor": lambda save=True, dispatch=True: {**latest, "should_alert": True},
    }
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (runtime, None))
    app = FastAPI()
    app.include_router(regime_route.router)
    app.dependency_overrides[regime_route.require_actor] = lambda: "tester"
    app.dependency_overrides[regime_route.db_session] = lambda: iter([None])
    client = TestClient(app)

    status = client.get("/regime/thesis-monitor/hbm")
    manual_run = client.post("/regime/thesis-monitor/hbm/run")

    assert status.status_code == 200
    assert status.json()["latest"]["status"] == "watch"
    assert status.json()["config"]["enabled"] is True
    assert manual_run.status_code == 200
    assert manual_run.json()["should_alert"] is True
