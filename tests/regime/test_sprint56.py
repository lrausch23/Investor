from __future__ import annotations

import asyncio
import importlib
from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest

from src.app.routes import regime as regime_route
from src.regime import notifications as notifications_module
from src.regime import persistence as persistence_module


@pytest.fixture()
def temp_modules(tmp_path, monkeypatch):
    monkeypatch.setenv("HMM_DATA_DIR", str(tmp_path))
    monkeypatch.setattr(persistence_module, "DB_PATH", tmp_path / "regime_watch.db")
    store = importlib.reload(persistence_module)
    notifications = importlib.reload(notifications_module)
    return store, notifications


def _client(runtime: dict[str, object]) -> TestClient:
    app = FastAPI()
    app.include_router(regime_route.router)
    app.dependency_overrides[regime_route.require_actor] = lambda: "tester"
    regime_route.router.dependency_overrides_provider = app
    app.dependency_overrides[regime_route.db_session] = lambda: iter([None])
    app.dependency_overrides[regime_route.require_actor] = lambda: "tester"
    return TestClient(app)


def test_notification_preferences_seed_and_update(temp_modules) -> None:
    store, _notifications = temp_modules
    prefs = store.get_notification_preferences()
    assert any(row["alert_type"] == "vix_freeze" and row["channel"] == "email" for row in prefs)
    store.set_notification_preference("vix_freeze", "email", False)
    channels = store.get_channels_for_alert("vix_freeze")
    assert "email" not in channels
    assert "in_app" in channels


def test_dispatch_notification_buffers_digest_when_enabled(temp_modules, monkeypatch) -> None:
    store, notifications = temp_modules
    store.set_setting("notify_digest_enabled", "true")
    result = notifications.dispatch_notification("vix_freeze", "Freeze", "Frozen", severity="warning")
    assert result["in_app"] is True
    assert result["email"] == "buffered"


def test_notification_preferences_route_round_trip(temp_modules, monkeypatch) -> None:
    store, notifications = temp_modules
    runtime = {
        "set_notification_preference": store.set_notification_preference,
        "set_setting": store.set_setting,
        "notification_preferences_payload": notifications.notification_preferences_payload,
    }
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (runtime, None))
    client = _client(runtime)
    response = client.put(
        "/regime/notifications/preferences",
        json={
            "preferences": [
                {"alert_type": "vix_freeze", "channel": "email", "enabled": False},
                {"alert_type": "vix_freeze", "channel": "slack", "enabled": True},
            ],
            "settings": {
                "quiet_hours_start": "22:00",
                "quiet_hours_end": "06:00",
                "quiet_hours_tz": "America/New_York",
                "digest_enabled": True,
            },
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["settings"]["digest_enabled"] is True
    assert store.get_setting("notify_quiet_hours_start") == "22:00"
    assert "email" not in store.get_channels_for_alert("vix_freeze")


def test_dispatch_notification_normalizes_uppercase_critical(temp_modules, monkeypatch) -> None:
    _store, notifications = temp_modules
    monkeypatch.setattr(notifications, "get_channels_for_alert", lambda alert_type: ["email", "slack"])
    monkeypatch.setattr(notifications, "_is_quiet_hours", lambda: True)
    monkeypatch.setattr(notifications, "get_setting", lambda key: "false" if key == "notify_digest_enabled" else "")

    sent = {"email": 0, "slack": 0}

    async def fake_email(*args, **kwargs):
        sent["email"] += 1
        return True

    async def fake_slack(*args, **kwargs):
        sent["slack"] += 1
        return True

    monkeypatch.setattr(notifications, "send_email_notification", fake_email)
    monkeypatch.setattr(notifications, "send_slack_notification", fake_slack)
    result = asyncio.run(
        notifications.dispatch_notification_async(
            "drawdown_breach",
            "Critical alert",
            "message",
            severity="CRITICAL",
        )
    )
    assert result["email"] is True
    assert result["slack"] is True
    assert sent == {"email": 1, "slack": 1}


def test_notification_ui_disables_unconfigured_channels() -> None:
    content = (Path(__file__).resolve().parents[2] / "src" / "app" / "static" / "regime.js").read_text(encoding="utf-8")
    assert 'notificationPrefs.settings.email_configured ? "" : "disabled"' in content
    assert 'notificationPrefs.settings.slack_configured ? "" : "disabled"' in content
