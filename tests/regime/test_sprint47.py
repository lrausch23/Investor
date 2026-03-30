from __future__ import annotations

import importlib
from types import SimpleNamespace

import pandas as pd
import pytest

from src.regime import notifications as notifications_module
from src.regime import paper_trading as paper_trading_module
from src.regime import persistence as persistence_module
from src.regime import scheduled_runner as scheduled_runner_module
from src.regime import vix_freeze as vix_freeze_module


@pytest.fixture()
def temp_modules(tmp_path, monkeypatch):
    monkeypatch.setenv("HMM_DATA_DIR", str(tmp_path))
    store = importlib.reload(persistence_module)
    monkeypatch.setattr(store, "DB_PATH", tmp_path / "regime_watch.db")
    paper = importlib.reload(paper_trading_module)
    vix = importlib.reload(vix_freeze_module)
    notifications = importlib.reload(notifications_module)
    scheduled = importlib.reload(scheduled_runner_module)
    return store, paper, vix, notifications, scheduled


def test_save_alert_persists_and_returns(temp_modules) -> None:
    store, _paper, _vix, _notifications, _scheduled = temp_modules
    alert = store.save_alert("vix_freeze", "VIX freeze", severity="critical", message="Frozen", data={"vix": 40.0})
    assert alert["title"] == "VIX freeze"
    loaded = store.get_alerts(limit=1)
    assert loaded[0]["severity"] == "critical"
    assert loaded[0]["data"]["vix"] == 40.0


def test_get_alerts_filters_by_type_and_unacknowledged(temp_modules) -> None:
    store, _paper, _vix, _notifications, _scheduled = temp_modules
    first = store.save_alert("vix_freeze", "Freeze")
    store.save_alert("vix_resume", "Resume")
    store.acknowledge_alert(first["id"])
    filtered = store.get_alerts(alert_type="vix_freeze", limit=10)
    assert len(filtered) == 1
    unacked = store.get_alerts(unacknowledged_only=True, limit=10)
    assert all(not row["acknowledged"] for row in unacked)


def test_acknowledge_all_alerts(temp_modules) -> None:
    store, _paper, _vix, _notifications, _scheduled = temp_modules
    store.save_alert("vix_freeze", "A")
    store.save_alert("vix_resume", "B")
    assert store.acknowledge_all_alerts() == 2
    assert store.get_alerts(unacknowledged_only=True, limit=10) == []


def test_fetch_current_vix_returns_cached_value(temp_modules, monkeypatch) -> None:
    _store, _paper, vix, _notifications, _scheduled = temp_modules
    calls = {"count": 0}

    def fake_current_vix():
        calls["count"] += 1
        return 31.2

    monkeypatch.setattr(vix, "get_current_vix", fake_current_vix)
    first = vix.fetch_current_vix()
    second = vix.fetch_current_vix()
    assert first == pytest.approx(31.2)
    assert second == pytest.approx(31.2)
    assert calls["count"] == 1


def test_vix_freeze_activation_and_resume(temp_modules, monkeypatch) -> None:
    store, _paper, vix, _notifications, _scheduled = temp_modules
    monkeypatch.setattr(vix, "fetch_current_vix", lambda: 40.0)
    status = vix.check_vix_freeze()
    assert status["frozen"] is True
    assert store.get_setting("vix_freeze_active") == "true"
    monkeypatch.setattr(vix, "fetch_current_vix", lambda: 28.0)
    resumed = vix.check_vix_freeze()
    assert resumed["frozen"] is False
    assert store.get_setting("vix_freeze_active") == "false"


def test_vix_freeze_hysteresis(temp_modules, monkeypatch) -> None:
    store, _paper, vix, _notifications, _scheduled = temp_modules
    store.set_setting("vix_freeze_active", "true")
    monkeypatch.setattr(vix, "fetch_current_vix", lambda: 32.0)
    status = vix.check_vix_freeze()
    assert status["frozen"] is True


def test_manual_override_vix_freeze(temp_modules) -> None:
    store, _paper, vix, _notifications, _scheduled = temp_modules
    status = vix.manual_override_vix_freeze(unfreeze=False)
    assert status["frozen"] is True
    assert store.get_setting("vix_freeze_active") == "true"


def test_generate_buy_plans_returns_empty_when_vix_frozen(temp_modules, monkeypatch) -> None:
    store, paper, _vix, _notifications, _scheduled = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    theme = store.create_theme("AI", conviction=4, status="Active")
    store.upsert_watchlist_candidate(theme["id"], "WOLF", suggested_entry_price=20.0, regime_label="Bull", regime_probability=0.7, status="Entry Signal")
    monkeypatch.setattr("src.regime.vix_freeze.is_vix_frozen", lambda: True)
    assert paper.generate_buy_plans(portfolio["id"]) == []


def test_auto_approve_blocks_buys_when_vix_frozen(temp_modules, monkeypatch) -> None:
    store, paper, _vix, _notifications, _scheduled = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    store.set_operating_mode("semi_auto")
    plan = store.create_trade_plan(portfolio["id"], "NVDA", "Buy", 10, "rationale", proposed_price=100.0, meta_labeler_score=0.9)
    monkeypatch.setattr("src.regime.vix_freeze.is_vix_frozen", lambda: True)
    result = paper.auto_approve_plans(portfolio["id"])
    assert result["blocked"] == 1
    assert result["details"][0]["plan_id"] == plan["id"]
    assert result["details"][0]["result"] == "blocked_vix_freeze"


def test_dispatch_notification_skips_info_severity(temp_modules, monkeypatch) -> None:
    _store, _paper, _vix, notifications, _scheduled = temp_modules
    email = {"called": 0}
    slack = {"called": 0}
    monkeypatch.setattr(notifications, "send_email_notification", lambda *args, **kwargs: email.__setitem__("called", email["called"] + 1) or True)
    monkeypatch.setattr(notifications, "send_slack_notification", lambda *args, **kwargs: slack.__setitem__("called", slack["called"] + 1) or True)
    result = notifications.dispatch_notification("vix_freeze", "Freeze", "Frozen", severity="info")
    assert result == {"in_app": True}
    assert email["called"] == 0
    assert slack["called"] == 0


def test_scheduled_paper_plans_returns_vix_status(temp_modules, monkeypatch) -> None:
    store, _paper, _vix, _notifications, scheduled = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    monkeypatch.setattr(scheduled, "check_vix_freeze", lambda: {"vix": 22.0, "frozen": False})
    monkeypatch.setattr(scheduled, "load_payload", lambda: {"rows": []})
    monkeypatch.setattr(scheduled, "generate_daily_plans", lambda *args, **kwargs: {"buy_plans": [], "holdings_plans": [], "exit_plans": []})
    monkeypatch.setattr(scheduled, "expire_stale_plans", lambda portfolio_id: 0)
    monkeypatch.setattr(scheduled, "auto_approve_plans", lambda portfolio_id: {"approved": 0})
    monkeypatch.setattr(scheduled, "sweep_monitoring_alerts", lambda portfolio_id: [])
    payload = scheduled.run_scheduled_paper_plans()
    assert payload["portfolios"][0]["portfolio_id"] == portfolio["id"]
    assert payload["vix_status"]["vix"] == 22.0


def test_check_loss_breach_does_not_double_fire(temp_modules) -> None:
    store, _paper, _vix, _notifications, _scheduled = temp_modules
    from src.regime.alerts import check_loss_breach

    first = check_loss_breach(1, -1200.0, 1000.0)
    second = check_loss_breach(1, -1200.0, 1000.0)
    assert first is not None
    assert second is None
    assert len(store.get_alerts(alert_type="daily_loss_breach", limit=10)) == 1
