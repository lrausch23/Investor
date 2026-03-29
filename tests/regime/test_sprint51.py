"""Sprint 51 - Deployment automation + settings save guard tests."""
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


def test_empty_account_id_preserves_existing(tmp_path: Path, monkeypatch) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "IBKR_HOST=127.0.0.1\nIBKR_PORT=7497\nIBKR_CLIENT_ID=1\n"
        "IBKR_ACCOUNT_ID=DUP579027\nIBKR_LIVE_ACCOUNT_ID=U123456\n"
        "IBKR_LIVE_BACKEND=false\nIBKR_TIMEOUT=10\n"
    )
    monkeypatch.setattr(regime_route, "_env_file_path", lambda: env_file)
    monkeypatch.setenv("IBKR_HOST", "127.0.0.1")
    monkeypatch.setenv("IBKR_PORT", "7497")
    monkeypatch.setenv("IBKR_CLIENT_ID", "1")
    monkeypatch.setenv("IBKR_ACCOUNT_ID", "DUP579027")
    monkeypatch.setenv("IBKR_LIVE_ACCOUNT_ID", "U123456")
    monkeypatch.setenv("IBKR_LIVE_BACKEND", "false")
    monkeypatch.setenv("IBKR_TIMEOUT", "10")

    client = _client()
    response = client.post(
        "/regime/ibkr/settings",
        data={
            "host": "127.0.0.1",
            "port": "7497",
            "client_id": "1",
            "account_id": "",
            "live_account_id": "",
            "live_backend": "false",
            "timeout": "10",
        },
    )
    assert response.status_code == 200

    content = env_file.read_text()
    assert "IBKR_ACCOUNT_ID=DUP579027" in content
    assert "IBKR_LIVE_ACCOUNT_ID=U123456" in content
    assert os.environ["IBKR_ACCOUNT_ID"] == "DUP579027"
    assert os.environ["IBKR_LIVE_ACCOUNT_ID"] == "U123456"


def test_explicit_account_id_overwrites(tmp_path: Path, monkeypatch) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("IBKR_ACCOUNT_ID=OLD_ACCOUNT\n")
    monkeypatch.setattr(regime_route, "_env_file_path", lambda: env_file)
    monkeypatch.setenv("IBKR_ACCOUNT_ID", "OLD_ACCOUNT")

    regime_route._update_env_file({"IBKR_ACCOUNT_ID": "NEW_ACCOUNT"})

    content = env_file.read_text()
    assert "IBKR_ACCOUNT_ID=NEW_ACCOUNT" in content
    assert os.environ["IBKR_ACCOUNT_ID"] == "NEW_ACCOUNT"


def test_guard_only_protects_nonempty_existing(tmp_path: Path, monkeypatch) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("IBKR_ACCOUNT_ID=\n")
    monkeypatch.setattr(regime_route, "_env_file_path", lambda: env_file)
    monkeypatch.setenv("IBKR_ACCOUNT_ID", "")

    regime_route._update_env_file({"IBKR_ACCOUNT_ID": ""})

    content = env_file.read_text()
    assert "IBKR_ACCOUNT_ID=" in content
    assert os.environ["IBKR_ACCOUNT_ID"] == ""


def test_get_returns_preserved_account_after_empty_save(tmp_path: Path, monkeypatch) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "IBKR_HOST=127.0.0.1\nIBKR_PORT=7497\nIBKR_CLIENT_ID=1\n"
        "IBKR_ACCOUNT_ID=DUP579027\nIBKR_LIVE_ACCOUNT_ID=\n"
        "IBKR_LIVE_BACKEND=false\nIBKR_TIMEOUT=10\n"
    )
    monkeypatch.setattr(regime_route, "_env_file_path", lambda: env_file)
    monkeypatch.setenv("IBKR_HOST", "127.0.0.1")
    monkeypatch.setenv("IBKR_PORT", "7497")
    monkeypatch.setenv("IBKR_CLIENT_ID", "1")
    monkeypatch.setenv("IBKR_ACCOUNT_ID", "DUP579027")
    monkeypatch.setenv("IBKR_LIVE_ACCOUNT_ID", "")
    monkeypatch.setenv("IBKR_LIVE_BACKEND", "false")
    monkeypatch.setenv("IBKR_TIMEOUT", "10")

    client = _client()
    client.post(
        "/regime/ibkr/settings",
        data={
            "host": "127.0.0.1",
            "port": "7497",
            "client_id": "1",
            "account_id": "",
            "live_account_id": "",
            "live_backend": "false",
            "timeout": "10",
        },
    )

    response = client.get("/regime/ibkr/settings")
    config = response.json()["config"]
    assert config["account_id"] == "DUP579027"


def test_startup_checks_import() -> None:
    from src.app.startup_checks import run_all_checks

    assert callable(run_all_checks)


def test_check_env_file_missing(tmp_path: Path, monkeypatch) -> None:
    import src.app.startup_checks as startup_checks

    monkeypatch.setattr(startup_checks, "_project_root", lambda: tmp_path / "missing")
    errors = startup_checks.check_env_file()
    assert len(errors) == 1
    assert ".env file not found" in errors[0]


def test_check_api_keys_none_set(monkeypatch) -> None:
    from src.app.startup_checks import check_api_keys

    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("GOOGLE_API_KEY", raising=False)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    errors = check_api_keys()
    assert len(errors) == 1
    assert "No LLM API key" in errors[0]


def test_check_api_keys_one_set(monkeypatch) -> None:
    from src.app.startup_checks import check_api_keys

    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("GOOGLE_API_KEY", raising=False)
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test-123")
    errors = check_api_keys()
    assert len(errors) == 0


def test_check_python_dependencies() -> None:
    from src.app.startup_checks import check_python_dependencies

    errors = check_python_dependencies()
    assert len(errors) == 0


def test_run_all_checks_returns_tuple(monkeypatch, tmp_path: Path) -> None:
    import src.app.startup_checks as startup_checks

    env_file = tmp_path / ".env"
    env_file.write_text("IBKR_HOST=127.0.0.1\nIBKR_PORT=7497\nIBKR_ACCOUNT_ID=DUP579027\n")
    monkeypatch.setattr(startup_checks, "_project_root", lambda: tmp_path)
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
    result = startup_checks.run_all_checks()
    assert isinstance(result, tuple)
    assert len(result) == 2
    errors, warnings = result
    assert isinstance(errors, list)
    assert isinstance(warnings, list)
