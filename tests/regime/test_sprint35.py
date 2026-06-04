from __future__ import annotations

import importlib
import json
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from src.app.main import create_app
from src.app.routes import regime as regime_route
from src.regime import llm_layer as llm_layer_module
from src.regime import persistence as persistence_module
from src.regime.exceptions import LLMProviderError


@pytest.fixture()
def temp_modules(tmp_path, monkeypatch):
    monkeypatch.setenv("HMM_DATA_DIR", str(tmp_path))
    store = importlib.reload(persistence_module)
    monkeypatch.setattr(store, "DB_PATH", tmp_path / "regime_watch.db")
    llm = importlib.reload(llm_layer_module)
    return store, llm


def test_regime_settings_crud(temp_modules) -> None:
    store, _llm = temp_modules
    store.set_setting("frontier_provider", "openai")
    assert store.get_setting("frontier_provider") == "openai"
    store.set_setting("frontier_provider", "gemini")
    assert store.get_setting("frontier_provider") == "gemini"
    assert store.delete_setting("frontier_provider") is True
    assert store.get_setting("frontier_provider") is None


def test_get_setting_returns_none_for_missing_key(temp_modules) -> None:
    store, _llm = temp_modules
    assert store.get_setting("missing") is None


def test_get_all_settings_with_prefix(temp_modules) -> None:
    store, _llm = temp_modules
    store.set_setting("frontier_provider", "openai")
    store.set_setting("frontier_model", "gpt-4o")
    store.set_setting("ibkr_host", "127.0.0.1")
    assert store.get_all_settings("frontier_") == {
        "frontier_model": "gpt-4o",
        "frontier_provider": "openai",
    }


def test_set_setting_upserts(temp_modules) -> None:
    store, _llm = temp_modules
    store.set_setting("frontier_model", "gpt-4o")
    store.set_setting("frontier_model", "gpt-4.1")
    assert store.get_setting("frontier_model") == "gpt-4.1"
    assert list(store.get_all_settings().keys()).count("frontier_model") == 1


def test_list_openai_models_filters_chat_only(temp_modules, monkeypatch) -> None:
    _store, llm = temp_modules

    class FakeClient:
        def __init__(self, **_kwargs):
            self.models = SimpleNamespace(
                list=lambda: [
                    SimpleNamespace(id="gpt-4o", owned_by="openai"),
                    SimpleNamespace(id="text-embedding-3-large", owned_by="openai"),
                    SimpleNamespace(id="o3-mini", owned_by="openai"),
                    SimpleNamespace(id="whisper-1", owned_by="openai"),
                ]
            )

    monkeypatch.setenv("OPENAI_API_KEY", "x")
    monkeypatch.setattr(llm, "OpenAI", FakeClient)
    models = llm._list_openai_models()
    assert [model["id"] for model in models] == ["gpt-4o", "o3-mini"]


def test_list_openai_models_returns_empty_without_key(temp_modules, monkeypatch) -> None:
    _store, llm = temp_modules
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    assert llm._list_openai_models() == []


def test_list_gemini_models_filters_by_supported_actions(temp_modules, monkeypatch) -> None:
    _store, llm = temp_modules
    monkeypatch.setenv("GEMINI_API_KEY", "x")
    fake_genai = SimpleNamespace(
        Client=lambda api_key: SimpleNamespace(
            models=SimpleNamespace(
                list=lambda: [
                    SimpleNamespace(name="models/gemini-2.5-pro", display_name="Gemini Pro", supported_actions=["generateContent"]),
                    SimpleNamespace(name="models/embed", display_name="Embed", supported_actions=["embedContent"]),
                ]
            )
        )
    )
    google_module = ModuleType("google")
    google_module.genai = fake_genai
    monkeypatch.setitem(sys.modules, "google", google_module)
    models = llm._list_gemini_models()
    assert models == [{"id": "gemini-2.5-pro", "name": "Gemini Pro"}]


def test_list_claude_models_returns_all(temp_modules, monkeypatch) -> None:
    _store, llm = temp_modules
    monkeypatch.setenv("ANTHROPIC_API_KEY", "x")
    anthropic_module = ModuleType("anthropic")
    anthropic_module.Anthropic = lambda api_key: SimpleNamespace(
        models=SimpleNamespace(
            list=lambda: [
                SimpleNamespace(id="claude-sonnet", display_name="Claude Sonnet"),
                SimpleNamespace(id="claude-opus", display_name="Claude Opus"),
            ]
        )
    )
    monkeypatch.setitem(sys.modules, "anthropic", anthropic_module)
    models = llm._list_claude_models()
    assert [model["id"] for model in models] == ["claude-opus", "claude-sonnet"]


def test_list_ollama_models_parses_tags(temp_modules, monkeypatch) -> None:
    _store, llm = temp_modules

    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self):
            return json.dumps(
                {
                    "models": [
                        {
                            "name": "qwen3:32b",
                            "details": {"parameter_size": "32B", "quantization_level": "Q4_K_M"},
                        }
                    ]
                }
            ).encode()

    monkeypatch.setattr(llm.urllib.request, "urlopen", lambda req, timeout=5: FakeResponse())
    models = llm._list_ollama_models()
    assert models == [{"id": "qwen3:32b", "name": "qwen3:32b (32B, Q4_K_M)"}]


def test_list_ollama_models_handles_timeout(temp_modules, monkeypatch) -> None:
    _store, llm = temp_modules
    monkeypatch.setattr(
        llm.urllib.request,
        "urlopen",
        lambda req, timeout=5: (_ for _ in ()).throw(TimeoutError("boom")),
    )
    with pytest.raises(LLMProviderError):
        llm._list_ollama_models()


def test_list_provider_models_raises_on_unknown(temp_modules) -> None:
    _store, llm = temp_modules
    with pytest.raises(LLMProviderError):
        llm.list_provider_models("unknown")


def _settings_runtime() -> tuple[dict, dict]:
    settings: dict[str, str] = {}
    live_calls = {"count": 0}

    def get_setting(key: str) -> str | None:
        return settings.get(key)

    def set_setting(key: str, value: str) -> None:
        settings[key] = value

    def get_all_settings(prefix: str = "") -> dict[str, str]:
        return {k: v for k, v in settings.items() if not prefix or k.startswith(prefix)}

    def delete_setting(key: str) -> bool:
        return settings.pop(key, None) is not None

    def list_provider_models(provider: str):
        live_calls["count"] += 1
        if provider == "broken":
            raise RuntimeError("boom")
        return [{"id": f"{provider}-model", "name": f"{provider.title()} Model"}]

    runtime = {
        "get_setting": get_setting,
        "set_setting": set_setting,
        "get_all_settings": get_all_settings,
        "delete_setting": delete_setting,
        "list_provider_models": list_provider_models,
    }
    return runtime, live_calls


def test_route_frontier_models_returns_list(monkeypatch) -> None:
    runtime, _calls = _settings_runtime()
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (runtime, None))
    client = TestClient(create_app())
    response = client.get("/regime/frontier/models?provider=openai")
    assert response.status_code == 200
    payload = response.json()
    assert payload["provider"] == "openai"
    assert payload["models"][0]["id"] == "openai-model"


def test_route_frontier_models_caches(monkeypatch) -> None:
    runtime, calls = _settings_runtime()
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (runtime, None))
    client = TestClient(create_app())
    first = client.get("/regime/frontier/models?provider=openai")
    second = client.get("/regime/frontier/models?provider=openai")
    assert first.status_code == 200 and second.status_code == 200
    assert first.json()["cached"] is False
    assert second.json()["cached"] is True
    assert calls["count"] == 1


def test_route_frontier_models_refresh_bypasses_cache(monkeypatch) -> None:
    runtime, calls = _settings_runtime()
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (runtime, None))
    client = TestClient(create_app())
    client.get("/regime/frontier/models?provider=openai")
    refreshed = client.get("/regime/frontier/models?provider=openai&refresh=1")
    assert refreshed.status_code == 200
    assert refreshed.json()["cached"] is False
    assert calls["count"] == 2


def test_route_frontier_models_returns_502_on_failure(monkeypatch) -> None:
    runtime, _calls = _settings_runtime()
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (runtime, None))
    client = TestClient(create_app())
    response = client.get("/regime/frontier/models?provider=broken")
    assert response.status_code == 502


def test_route_frontier_settings_get(monkeypatch) -> None:
    runtime, _calls = _settings_runtime()
    runtime["set_setting"]("frontier_provider", "openai")
    runtime["set_setting"]("frontier_model", "gpt-4o")
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (runtime, None))
    client = TestClient(create_app())
    response = client.get("/regime/frontier/settings")
    assert response.status_code == 200
    assert response.json() == {"provider": "openai", "model": "gpt-4o"}


def test_route_frontier_settings_put(monkeypatch) -> None:
    runtime, _calls = _settings_runtime()
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (runtime, None))
    client = TestClient(create_app())
    response = client.put("/regime/frontier/settings", data={"provider": "claude", "model": "claude-sonnet"})
    assert response.status_code == 200
    assert runtime["get_setting"]("frontier_provider") == "claude"
    assert runtime["get_setting"]("frontier_model") == "claude-sonnet"


def test_route_agent_frontier_settings_put(monkeypatch) -> None:
    runtime, _calls = _settings_runtime()
    runtime["set_setting"]("frontier_provider", "ollama")
    runtime["set_setting"]("frontier_model", "fallback-model")
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (runtime, None))
    client = TestClient(create_app())

    response = client.put(
        "/regime/agents/frontier-settings",
        json={"agent_key": "quant", "provider": "openai", "model": "gpt-4o-mini"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["agent_key"] == "quant"
    assert payload["provider"] == "openai"
    assert payload["model"] == "gpt-4o-mini"
    assert runtime["get_setting"]("agent_frontier_provider_quant") == "openai"
    assert runtime["get_setting"]("agent_frontier_model_quant") == "gpt-4o-mini"


def test_route_frontier_settings_empty_returns_defaults(monkeypatch) -> None:
    runtime, _calls = _settings_runtime()
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (runtime, None))
    client = TestClient(create_app())
    response = client.get("/regime/frontier/settings")
    assert response.status_code == 200
    assert response.json() == {"provider": "auto", "model": ""}


def test_apply_saved_model_overrides_env(temp_modules, monkeypatch) -> None:
    store, llm = temp_modules
    store.set_setting("frontier_provider", "openai")
    store.set_setting("frontier_model", "gpt-4.1")
    monkeypatch.setenv("OPENAI_MODEL", "gpt-4o")
    llm._apply_saved_model("openai")
    assert llm.os.getenv("OPENAI_MODEL") == "gpt-4.1"


def test_saved_model_does_not_override_when_provider_mismatch(temp_modules, monkeypatch) -> None:
    store, llm = temp_modules
    store.set_setting("frontier_provider", "openai")
    store.set_setting("frontier_model", "gpt-4.1")
    monkeypatch.setenv("GEMINI_MODEL", "gemini-2.5-pro")
    llm._apply_saved_model("gemini")
    assert llm.os.getenv("GEMINI_MODEL") == "gemini-2.5-pro"
