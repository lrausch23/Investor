"""Sprint 50d - IBKR settings save sync tests."""
from __future__ import annotations

import os
from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.app.routes import regime as regime_route


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(regime_route.router)
    app.dependency_overrides[regime_route.require_actor] = lambda: "tester"
    app.dependency_overrides[regime_route.db_session] = lambda: iter([None])
    return TestClient(app)


def test_update_env_file_syncs_os_environ(tmp_path: Path, monkeypatch) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("IBKR_PORT=7496\nIBKR_LIVE_BACKEND=true\n")

    monkeypatch.setattr(regime_route, "_env_file_path", lambda: env_file)
    monkeypatch.setenv("IBKR_PORT", "7496")
    monkeypatch.setenv("IBKR_LIVE_BACKEND", "true")

    regime_route._update_env_file(
        {
            "IBKR_PORT": "7497",
            "IBKR_LIVE_BACKEND": "false",
        }
    )

    content = env_file.read_text()
    assert "IBKR_PORT=7497" in content
    assert "IBKR_LIVE_BACKEND=false" in content
    assert os.environ["IBKR_PORT"] == "7497"
    assert os.environ["IBKR_LIVE_BACKEND"] == "false"


def test_save_then_get_returns_new_values(tmp_path: Path, monkeypatch) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "IBKR_HOST=127.0.0.1\n"
        "IBKR_PORT=7496\n"
        "IBKR_CLIENT_ID=1\n"
        "IBKR_ACCOUNT_ID=DUP579027\n"
        "IBKR_LIVE_ACCOUNT_ID=U123456\n"
        "IBKR_LIVE_BACKEND=true\n"
        "IBKR_TIMEOUT=10\n"
    )

    monkeypatch.setattr(regime_route, "_env_file_path", lambda: env_file)
    monkeypatch.setenv("IBKR_HOST", "127.0.0.1")
    monkeypatch.setenv("IBKR_PORT", "7496")
    monkeypatch.setenv("IBKR_CLIENT_ID", "1")
    monkeypatch.setenv("IBKR_ACCOUNT_ID", "DUP579027")
    monkeypatch.setenv("IBKR_LIVE_ACCOUNT_ID", "U123456")
    monkeypatch.setenv("IBKR_LIVE_BACKEND", "true")
    monkeypatch.setenv("IBKR_TIMEOUT", "10")

    client = _client()

    response = client.post(
        "/regime/ibkr/settings",
        data={
            "host": "127.0.0.1",
            "port": "7497",
            "client_id": "1",
            "account_id": "DUP579027",
            "live_account_id": "U123456",
            "live_backend": "false",
            "timeout": "10",
        },
    )
    assert response.status_code == 200
    assert response.json()["saved"] is True

    response = client.get("/regime/ibkr/settings")
    assert response.status_code == 200
    config = response.json()["config"]
    assert config["port"] == 7497
    assert config["live_backend"] is False


def test_env_sync_happens_after_file_write(tmp_path: Path, monkeypatch) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("IBKR_PORT=4002\n")

    monkeypatch.setattr(regime_route, "_env_file_path", lambda: env_file)
    monkeypatch.setenv("IBKR_PORT", "4002")

    regime_route._update_env_file({"IBKR_PORT": "7497"})

    assert "IBKR_PORT=7497" in env_file.read_text()
    assert os.environ["IBKR_PORT"] == "7497"


def test_env_sync_new_keys(tmp_path: Path, monkeypatch) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("# existing config\n")

    monkeypatch.setattr(regime_route, "_env_file_path", lambda: env_file)

    regime_route._update_env_file({"IBKR_PORT": "7497", "IBKR_HOST": "127.0.0.1"})

    content = env_file.read_text()
    assert "IBKR_PORT=7497" in content
    assert "IBKR_HOST=127.0.0.1" in content
    assert os.environ.get("IBKR_PORT") == "7497"
    assert os.environ.get("IBKR_HOST") == "127.0.0.1"
