from __future__ import annotations

import importlib

from fastapi.testclient import TestClient

from src.app.main import create_app
from src.app.routes import regime as regime_route
from src.regime import llm_layer as llm_layer_module
from src.regime import persistence as persistence_module


def _client_with_store(store, monkeypatch) -> TestClient:
    runtime = {
        "get_setting": store.get_setting,
        "set_setting": store.set_setting,
        "get_all_settings": store.get_all_settings,
        "delete_setting": store.delete_setting,
        "list_provider_models": lambda provider: [{"id": f"{provider}-model", "name": f"{provider.title()} Model"}],
    }
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (runtime, None))
    return TestClient(create_app())


def test_apply_saved_model_ignores_cross_provider_model(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("HMM_DATA_DIR", str(tmp_path))
    store = importlib.reload(persistence_module)
    monkeypatch.setattr(store, "DB_PATH", tmp_path / "regime_watch.db")
    llm = importlib.reload(llm_layer_module)
    store.set_setting("frontier_provider", "ollama")
    store.set_setting("frontier_model", "minimax-m2.7:cloud")
    monkeypatch.setenv("OPENAI_MODEL", "gpt-4o")
    llm._apply_saved_model("openai")
    assert llm.os.getenv("OPENAI_MODEL") == "gpt-4o"


def test_route_put_settings_empty_model_clears(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("HMM_DATA_DIR", str(tmp_path))
    store = importlib.reload(persistence_module)
    monkeypatch.setattr(store, "DB_PATH", tmp_path / "regime_watch.db")
    client = _client_with_store(store, monkeypatch)
    response = client.put("/regime/frontier/settings", data={"provider": "openai", "model": ""})
    assert response.status_code == 200
    payload = client.get("/regime/frontier/settings").json()
    assert payload == {"provider": "openai", "model": ""}


def test_route_put_settings_then_switch_provider_clears_model(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("HMM_DATA_DIR", str(tmp_path))
    store = importlib.reload(persistence_module)
    monkeypatch.setattr(store, "DB_PATH", tmp_path / "regime_watch.db")
    client = _client_with_store(store, monkeypatch)
    first = client.put("/regime/frontier/settings", data={"provider": "ollama", "model": "minimax-m2.7:cloud"})
    assert first.status_code == 200
    second = client.put("/regime/frontier/settings", data={"provider": "openai", "model": ""})
    assert second.status_code == 200
    payload = client.get("/regime/frontier/settings").json()
    assert payload == {"provider": "openai", "model": ""}
