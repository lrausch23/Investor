from __future__ import annotations

import importlib
import json
import re

from fastapi.testclient import TestClient

from src.app.main import create_app
from src.app.routes import regime as regime_route
from src.regime import persistence as persistence_module
from src.regime.exceptions import DuplicateThemeError
from tests import test_regime_route as route_tests


def _client_with_store(store, monkeypatch) -> TestClient:
    runtime = route_tests._fake_runtime()
    runtime.update(
        {
            "create_theme": store.create_theme,
            "update_theme": store.update_theme,
            "delete_theme": store.delete_theme,
            "list_themes": store.list_themes,
            "get_theme": store.get_theme,
            "get_supply_chain": store.get_supply_chain,
        }
    )
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (runtime, None))
    monkeypatch.setattr(regime_route, "_EXECUTOR", route_tests._ImmediateExecutor())
    monkeypatch.setattr(
        regime_route,
        "get_available_portfolio_scopes",
        lambda session: [
            {"value": "household", "label": "All Portfolios", "ticker_count": 2, "accounts": []},
            {"value": "personal", "label": "Personal", "ticker_count": 1, "accounts": []},
        ],
    )
    monkeypatch.setattr(
        regime_route,
        "get_current_tickers_by_scope",
        lambda session, scope, account_id=None: ["NVDA", "AVGO"],
    )
    return TestClient(create_app())


def test_create_theme_duplicate_raises_error(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("HMM_DATA_DIR", str(tmp_path))
    store = importlib.reload(persistence_module)
    monkeypatch.setattr(store, "DB_PATH", tmp_path / "regime_watch.db")
    store.create_theme("AI")
    try:
        store.create_theme("AI")
        raise AssertionError("expected DuplicateThemeError")
    except DuplicateThemeError as exc:
        assert "AI" in str(exc)


def test_update_theme_duplicate_name_raises_error(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("HMM_DATA_DIR", str(tmp_path))
    store = importlib.reload(persistence_module)
    monkeypatch.setattr(store, "DB_PATH", tmp_path / "regime_watch.db")
    alpha = store.create_theme("Alpha")
    beta = store.create_theme("Beta")
    try:
        store.update_theme(beta["id"], name="Alpha")
        raise AssertionError("expected DuplicateThemeError")
    except DuplicateThemeError:
        pass
    assert store.get_theme(alpha["id"])["name"] == "Alpha"


def test_create_theme_unique_name_succeeds(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("HMM_DATA_DIR", str(tmp_path))
    store = importlib.reload(persistence_module)
    monkeypatch.setattr(store, "DB_PATH", tmp_path / "regime_watch.db")
    alpha = store.create_theme("Alpha")
    beta = store.create_theme("Beta")
    assert alpha["id"] != beta["id"]


def test_route_create_duplicate_theme_returns_409(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("HMM_DATA_DIR", str(tmp_path))
    store = importlib.reload(persistence_module)
    monkeypatch.setattr(store, "DB_PATH", tmp_path / "regime_watch.db")
    client = _client_with_store(store, monkeypatch)
    first = client.post("/regime/themes", data={"name": "AI", "narrative": "Narrative", "conviction": "4", "status": "Active"})
    second = client.post("/regime/themes", data={"name": "AI", "narrative": "Narrative", "conviction": "4", "status": "Active"})
    assert first.status_code == 200
    assert second.status_code == 409
    assert "already exists" in second.json()["detail"].lower()


def test_route_update_duplicate_theme_returns_409(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("HMM_DATA_DIR", str(tmp_path))
    store = importlib.reload(persistence_module)
    monkeypatch.setattr(store, "DB_PATH", tmp_path / "regime_watch.db")
    client = _client_with_store(store, monkeypatch)
    alpha = client.post("/regime/themes", data={"name": "Alpha"}).json()
    beta = client.post("/regime/themes", data={"name": "Beta"}).json()
    response = client.put(f"/regime/themes/{beta['id']}", data={"name": alpha["name"]})
    assert response.status_code == 409
    assert "already exists" in response.json()["detail"].lower()


def test_initial_payload_includes_themes(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("HMM_DATA_DIR", str(tmp_path))
    store = importlib.reload(persistence_module)
    monkeypatch.setattr(store, "DB_PATH", tmp_path / "regime_watch.db")
    store.create_theme("AI Infra", narrative="Narrative", conviction=4, status="Active")
    monkeypatch.setattr(regime_route, "load_payload", lambda: None)
    client = _client_with_store(store, monkeypatch)
    response = client.get("/regime")
    assert response.status_code == 200
    match = re.search(r'<script type="application/json" id="regimeConfig">(.*?)</script>', response.text, re.DOTALL)
    assert match is not None
    config = json.loads(match.group(1))
    themes = config["initial_payload"]["themes"]
    assert any(theme["name"] == "AI Infra" for theme in themes)
