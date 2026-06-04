from __future__ import annotations

import importlib
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest

from src.regime import discovery as discovery_module
from src.regime import llm_layer
from src.regime import persistence as persistence_module


def test_strip_code_fences_json_block() -> None:
    payload = "```json\n{\"ticker\": \"NVDA\"}\n```"
    assert llm_layer._strip_code_fences(payload) == '{"ticker": "NVDA"}'


def test_strip_code_fences_plain_block() -> None:
    payload = "```\n[1, 2, 3]\n```"
    assert llm_layer._strip_code_fences(payload) == "[1, 2, 3]"


def test_strip_code_fences_passthrough() -> None:
    payload = '{"ticker": "AVGO"}'
    assert llm_layer._strip_code_fences(payload) == payload


def test_request_openai_parses_fenced_json(monkeypatch) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "x")

    class FakeClient:
        def __init__(self, **_kwargs):
            self.responses = SimpleNamespace(create=lambda **_kwargs: SimpleNamespace(output_text='```json\n{"ok": true}\n```'))

    monkeypatch.setattr(llm_layer, "OpenAI", FakeClient)
    assert llm_layer._request_openai("prompt") == {"ok": True}


def test_request_gemini_parses_fenced_json(monkeypatch) -> None:
    monkeypatch.setenv("GEMINI_API_KEY", "x")

    class FakeModels:
        @staticmethod
        def generate_content(**_kwargs):
            return SimpleNamespace(text='```json\n{"provider": "gemini"}\n```')

    fake_genai = SimpleNamespace(Client=lambda api_key: SimpleNamespace(models=FakeModels()))
    google_module = ModuleType("google")
    google_module.genai = fake_genai
    monkeypatch.setitem(sys.modules, "google", google_module)

    assert llm_layer._request_gemini("prompt") == {"provider": "gemini"}


def test_request_claude_parses_fenced_json(monkeypatch) -> None:
    monkeypatch.setenv("ANTHROPIC_API_KEY", "x")

    class FakeAnthropic:
        def __init__(self, **_kwargs):
            self.messages = SimpleNamespace(
                create=lambda **_kwargs: SimpleNamespace(content=[SimpleNamespace(text='```json\n{"provider": "claude"}\n```')])
            )

    anthropic_module = ModuleType("anthropic")
    anthropic_module.Anthropic = FakeAnthropic
    monkeypatch.setitem(sys.modules, "anthropic", anthropic_module)

    assert llm_layer._request_claude("prompt") == {"provider": "claude"}


def test_request_ollama_parses_fenced_json(monkeypatch) -> None:
    class FakeClient:
        def __init__(self, **_kwargs):
            self.chat = SimpleNamespace(
                completions=SimpleNamespace(
                    create=lambda **_kwargs: SimpleNamespace(
                        choices=[SimpleNamespace(message=SimpleNamespace(content='```json\n{"provider": "ollama"}\n```'))]
                    )
                )
            )

    monkeypatch.setattr(llm_layer, "OpenAI", FakeClient)
    assert llm_layer._request_ollama("prompt") == {"provider": "ollama"}


def test_request_openai_logs_warning_on_bad_json(monkeypatch, caplog) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "x")

    class FakeClient:
        def __init__(self, **_kwargs):
            self.responses = SimpleNamespace(create=lambda **_kwargs: SimpleNamespace(output_text="not json"))

    monkeypatch.setattr(llm_layer, "OpenAI", FakeClient)
    with caplog.at_level("WARNING"):
        payload = llm_layer._request_openai("prompt")
    assert payload == {"raw_response": "not json"}
    assert "OpenAI response was not valid JSON" in caplog.text


def test_request_gemini_logs_warning_on_bad_json(monkeypatch, caplog) -> None:
    monkeypatch.setenv("GEMINI_API_KEY", "x")

    class FakeModels:
        @staticmethod
        def generate_content(**_kwargs):
            return SimpleNamespace(text="bad json")

    fake_genai = SimpleNamespace(Client=lambda api_key: SimpleNamespace(models=FakeModels()))
    google_module = ModuleType("google")
    google_module.genai = fake_genai
    monkeypatch.setitem(sys.modules, "google", google_module)

    with caplog.at_level("WARNING"):
        payload = llm_layer._request_gemini("prompt")
    assert payload == {"raw_response": "bad json"}
    assert "Gemini response was not valid JSON" in caplog.text


def test_request_claude_logs_warning_on_bad_json(monkeypatch, caplog) -> None:
    monkeypatch.setenv("ANTHROPIC_API_KEY", "x")

    class FakeAnthropic:
        def __init__(self, **_kwargs):
            self.messages = SimpleNamespace(create=lambda **_kwargs: SimpleNamespace(content=[SimpleNamespace(text="bad json")]))

    anthropic_module = ModuleType("anthropic")
    anthropic_module.Anthropic = FakeAnthropic
    monkeypatch.setitem(sys.modules, "anthropic", anthropic_module)

    with caplog.at_level("WARNING"):
        payload = llm_layer._request_claude("prompt")
    assert payload == {"raw_response": "bad json"}
    assert "Claude response was not valid JSON" in caplog.text


def test_request_ollama_logs_warning_on_bad_json(monkeypatch, caplog) -> None:
    class FakeClient:
        def __init__(self, **_kwargs):
            self.chat = SimpleNamespace(
                completions=SimpleNamespace(
                    create=lambda **_kwargs: SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content="bad json"))])
                )
            )

    monkeypatch.setattr(llm_layer, "OpenAI", FakeClient)
    with caplog.at_level("WARNING"):
        payload = llm_layer._request_ollama("prompt")
    assert payload == {"raw_response": "bad json"}
    assert "Ollama response was not valid JSON" in caplog.text


def test_request_frontier_decision_direct_ollama(monkeypatch) -> None:
    monkeypatch.setattr(llm_layer, "_request_ollama", lambda prompt: {"provider": "ollama", "prompt": prompt})
    assert llm_layer.request_frontier_decision("hello", enabled=True, provider="ollama") == {"provider": "ollama", "prompt": "hello"}


def test_request_frontier_decision_auto_falls_back_to_ollama(monkeypatch) -> None:
    monkeypatch.setattr(llm_layer, "_request_openai", lambda prompt: None)
    monkeypatch.setattr(llm_layer, "_request_gemini", lambda prompt: None)
    monkeypatch.setattr(llm_layer, "_request_claude", lambda prompt: None)
    monkeypatch.setattr(llm_layer, "_request_ollama", lambda prompt: {"provider": "ollama"})
    assert llm_layer.request_frontier_decision("hello", enabled=True, provider="auto") == {"provider": "ollama"}


def test_request_frontier_decision_best_falls_back_to_ollama(monkeypatch) -> None:
    monkeypatch.setattr(llm_layer, "_provider_request", lambda prompt, provider, use_best=False: {"provider": provider} if provider == "ollama" else None)
    assert llm_layer.request_frontier_decision("hello", enabled=True, provider="best") == {"provider": "ollama"}


def test_configured_frontier_model_supports_ollama(monkeypatch) -> None:
    monkeypatch.setenv("OLLAMA_MODEL", "qwen3:32b")
    monkeypatch.setattr(llm_layer, "_apply_saved_model", lambda provider: None)
    assert llm_layer.configured_frontier_model("ollama") == "Ollama: qwen3:32b"


def test_configured_frontier_model_best_uses_ollama_without_cloud_keys(monkeypatch) -> None:
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.setenv("OLLAMA_MODEL", "qwen3:32b")
    assert llm_layer.configured_frontier_model("best") == "Ollama: qwen3:32b (best)"


def test_provider_request_best_restores_ollama_model(monkeypatch) -> None:
    monkeypatch.setenv("OLLAMA_MODEL", "custom-model")
    seen: list[str] = []
    monkeypatch.setattr(llm_layer, "_request_ollama", lambda prompt: seen.append(str(llm_layer.os.getenv("OLLAMA_MODEL"))) or {"ok": True})
    llm_layer._provider_request("prompt", "ollama", use_best=True)
    assert seen == ["qwen3:32b"]
    assert llm_layer.os.getenv("OLLAMA_MODEL") == "custom-model"


def test_request_frontier_decision_uses_model_override(monkeypatch) -> None:
    monkeypatch.setenv("OLLAMA_MODEL", "global-model")
    seen: list[str] = []
    monkeypatch.setattr(llm_layer, "_request_ollama", lambda prompt: seen.append(str(llm_layer.os.getenv("OLLAMA_MODEL"))) or {"ok": True})

    result = llm_layer.request_frontier_decision("prompt", enabled=True, provider="ollama", model="agent-model")

    assert result == {"ok": True}
    assert seen == ["agent-model"]
    assert llm_layer.os.getenv("OLLAMA_MODEL") == "global-model"


@pytest.fixture()
def temp_modules(tmp_path, monkeypatch):
    monkeypatch.setenv("HMM_DATA_DIR", str(tmp_path))
    store = importlib.reload(persistence_module)
    monkeypatch.setattr(store, "DB_PATH", tmp_path / "regime_watch.db")
    discovery = importlib.reload(discovery_module)
    return store, discovery


def test_discovery_scan_recovers_candidates_from_fenced_json(temp_modules, monkeypatch) -> None:
    store, discovery = temp_modules
    theme = store.create_theme("Generative AI", "AI infra", 5, "Active", sector_hint="Semiconductors")
    monkeypatch.setattr(discovery, "_validate_ticker", lambda ticker: True)
    monkeypatch.setattr(discovery, "compute_crowd_score", lambda ticker, crowd_assessment=None: (20, {"seed": crowd_assessment}))
    monkeypatch.setattr(discovery, "_quick_regime_screen", lambda ticker: ("Bull", 0.61, 21.0, 17.0))
    monkeypatch.setattr(discovery.time, "sleep", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        discovery,
        "request_frontier_decision",
        lambda *args, **kwargs: discovery._extract_json_list(
            llm_layer._strip_code_fences(
                '```json\n[{"ticker":"WOLF","company_name":"Wolfspeed","sector_layer":"Power","rationale":"Critical provider","suggested_role":"Critical-Path","crowd_assessment":2}]\n```'
            )
        ),
    )
    rows = discovery.run_discovery_scan(theme["id"], frontier_provider="ollama")
    assert [row["ticker"] for row in rows] == ["WOLF"]


def test_ollama_request_uses_custom_base_url_and_model(monkeypatch) -> None:
    monkeypatch.setenv("OLLAMA_BASE_URL", "http://127.0.0.1:11434/v1")
    monkeypatch.setenv("OLLAMA_MODEL", "mistral")
    seen: dict[str, str] = {}

    class FakeClient:
        def __init__(self, **kwargs):
            seen["base_url"] = kwargs["base_url"]
            seen["api_key"] = kwargs["api_key"]
            self.chat = SimpleNamespace(
                completions=SimpleNamespace(
                    create=lambda **kwargs: seen.update({"model": kwargs["model"]}) or SimpleNamespace(
                        choices=[SimpleNamespace(message=SimpleNamespace(content='{"ok": true}'))]
                    )
                )
            )

    monkeypatch.setattr(llm_layer, "OpenAI", FakeClient)
    assert llm_layer._request_ollama("prompt") == {"ok": True}
    assert seen == {"base_url": "http://127.0.0.1:11434/v1", "api_key": "ollama", "model": "mistral"}
