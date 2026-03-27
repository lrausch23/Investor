from __future__ import annotations

import importlib
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from src.app.main import create_app
from src.app.routes import regime as regime_route
from src.regime import llm_layer, persistence


@pytest.fixture()
def temp_persistence(tmp_path, monkeypatch):
    monkeypatch.setenv("HMM_DATA_DIR", str(tmp_path))
    module = importlib.reload(persistence)
    monkeypatch.setattr(module, "DB_PATH", tmp_path / "regime_watch.db")
    return module


def _theme_runtime(store):
    return {
        "create_theme": store.create_theme,
        "update_theme": store.update_theme,
        "delete_theme": store.delete_theme,
        "list_themes": store.list_themes,
        "get_theme": store.get_theme,
        "add_ticker_to_theme": store.add_ticker_to_theme,
        "remove_ticker_from_theme": store.remove_ticker_from_theme,
        "update_ticker_in_theme": store.update_ticker_in_theme,
        "get_ticker_themes": store.get_ticker_themes,
        "get_theme_tickers": store.get_theme_tickers,
    }


def test_create_theme(temp_persistence) -> None:
    theme = temp_persistence.create_theme("Generative AI", "Narrative", 5, "Active")
    assert theme["name"] == "Generative AI"
    assert theme["conviction"] == 5


def test_create_theme_duplicate_name(temp_persistence) -> None:
    temp_persistence.create_theme("Generative AI")
    with pytest.raises(Exception):
        temp_persistence.create_theme("Generative AI")


def test_update_theme(temp_persistence) -> None:
    theme = temp_persistence.create_theme("AI")
    updated = temp_persistence.update_theme(theme["id"], name="AI Infra", conviction=4, status="Monitoring")
    assert updated["name"] == "AI Infra"
    assert updated["status"] == "Monitoring"


def test_delete_theme_cascades(temp_persistence) -> None:
    theme = temp_persistence.create_theme("AI")
    temp_persistence.add_ticker_to_theme(theme["id"], "NVDA")
    assert temp_persistence.delete_theme(theme["id"]) is True
    assert temp_persistence.get_theme_tickers(theme["id"]) == []


def test_list_themes_excludes_closed(temp_persistence) -> None:
    temp_persistence.create_theme("Open")
    temp_persistence.create_theme("Closed Theme", status="Closed")
    assert [item["name"] for item in temp_persistence.list_themes()] == ["Open"]
    assert len(temp_persistence.list_themes(include_closed=True)) == 2


def test_add_ticker_to_theme(temp_persistence) -> None:
    theme = temp_persistence.create_theme("AI")
    row = temp_persistence.add_ticker_to_theme(theme["id"], "NVDA", role="Critical-Path", target_price=150.0, stop_price=95.0)
    assert row["ticker"] == "NVDA"
    assert row["role"] == "Critical-Path"


def test_add_ticker_upsert(temp_persistence) -> None:
    theme = temp_persistence.create_theme("AI")
    temp_persistence.add_ticker_to_theme(theme["id"], "NVDA", role="Core")
    row = temp_persistence.add_ticker_to_theme(theme["id"], "NVDA", role="Speculative")
    assert row["role"] == "Speculative"
    assert len(temp_persistence.get_theme_tickers(theme["id"])) == 1


def test_remove_ticker_from_theme(temp_persistence) -> None:
    theme = temp_persistence.create_theme("AI")
    temp_persistence.add_ticker_to_theme(theme["id"], "NVDA")
    assert temp_persistence.remove_ticker_from_theme(theme["id"], "NVDA") is True
    assert temp_persistence.remove_ticker_from_theme(theme["id"], "NVDA") is False


def test_update_ticker_in_theme(temp_persistence) -> None:
    theme = temp_persistence.create_theme("AI")
    temp_persistence.add_ticker_to_theme(theme["id"], "NVDA")
    row = temp_persistence.update_ticker_in_theme(theme["id"], "NVDA", role="Critical-Path", target_price=140.0)
    assert row["role"] == "Critical-Path"
    assert row["target_price"] == pytest.approx(140.0)


def test_get_ticker_themes(temp_persistence) -> None:
    ai = temp_persistence.create_theme("AI")
    infra = temp_persistence.create_theme("Infra")
    temp_persistence.add_ticker_to_theme(ai["id"], "NVDA")
    temp_persistence.add_ticker_to_theme(infra["id"], "NVDA")
    rows = temp_persistence.get_ticker_themes("NVDA")
    assert len(rows) == 2


def test_get_theme_tickers(temp_persistence) -> None:
    theme = temp_persistence.create_theme("AI")
    temp_persistence.add_ticker_to_theme(theme["id"], "NVDA")
    temp_persistence.add_ticker_to_theme(theme["id"], "AVGO")
    assert [row["ticker"] for row in temp_persistence.get_theme_tickers(theme["id"])] == ["AVGO", "NVDA"]


def test_build_theme_context_single_theme(temp_persistence) -> None:
    theme = temp_persistence.create_theme("AI", "Macro narrative", 5, "Active")
    temp_persistence.add_ticker_to_theme(theme["id"], "NVDA", role="Core", rationale="Leader", time_horizon="strategic")
    context = regime_route._build_theme_context("NVDA", _theme_runtime(temp_persistence))
    assert "Theme: AI" in context
    assert "Role: Core" in context


def test_build_theme_context_no_themes(temp_persistence) -> None:
    assert regime_route._build_theme_context("NVDA", _theme_runtime(temp_persistence)) is None


def test_thesis_check_prompt_with_theme() -> None:
    prompt = llm_layer.build_thesis_check_prompt("NVDA", "Theme: AI\nRole: Core\nTime Horizon: strategic", "Bull", "Bear", "Sell", "declining", [])
    assert "Investment Context" in prompt
    assert "urgency" in prompt


def test_fallback_thesis_check_urgency() -> None:
    assert llm_layer._fallback_thesis_check("Bull", "Bear", "x", "trade")["urgency"] == "immediate"
    assert llm_layer._fallback_thesis_check("Bull", "Neutral", "x", "strategic")["urgency"] == "monitor"


def test_stop_proximity_uses_theme_stop() -> None:
    row = {"current_price": 101.0, "theme_stop_price": 100.0, "price_targets": {"stop_price": 90.0, "current_price": 101.0}}
    result = regime_route._stop_proximity(row)
    assert result["level"] == "critical"


def test_stop_proximity_falls_back_to_atr() -> None:
    row = {"current_price": 101.0, "price_targets": {"stop_price": 100.0, "current_price": 101.0}}
    result = regime_route._stop_proximity(row)
    assert result["level"] == "critical"


def test_theme_health_rotation_warning() -> None:
    themes = [{"id": 1, "name": "AI", "conviction": 5, "status": "Active", "tickers": [{"ticker": "NVDA", "role": "Core"}, {"ticker": "AVGO", "role": "Critical-Path"}]}]
    rows = [{"ticker": "NVDA", "regime": "Bull", "current_price": 100}, {"ticker": "AVGO", "regime": "Bear", "current_price": 100}]
    health = regime_route._compute_theme_health(themes, rows)
    assert "rotation" in health[0]["health_warning"].lower()


def test_theme_health_under_pressure() -> None:
    themes = [{"id": 1, "name": "AI", "conviction": 5, "status": "Active", "tickers": [{"ticker": "NVDA", "role": "Core"}, {"ticker": "AVGO", "role": "Core"}]}]
    rows = [{"ticker": "NVDA", "regime": "Bear", "current_price": 100}, {"ticker": "AVGO", "regime": "Bear", "current_price": 100}]
    health = regime_route._compute_theme_health(themes, rows)
    assert health[0]["health_warning"] == "Theme under pressure"


def test_theme_crud_routes(temp_persistence, monkeypatch) -> None:
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (_theme_runtime(temp_persistence), None))
    app = create_app()
    client = TestClient(app)
    created = client.post("/regime/themes", data={"name": "AI", "narrative": "Narrative", "conviction": "4", "status": "Active"})
    assert created.status_code == 200
    theme_id = created.json()["id"]
    fetched = client.get(f"/regime/themes/{theme_id}")
    assert fetched.status_code == 200
    updated = client.put(f"/regime/themes/{theme_id}", data={"status": "Monitoring"})
    assert updated.status_code == 200
    deleted = client.delete(f"/regime/themes/{theme_id}")
    assert deleted.status_code == 200


def test_theme_ticker_routes(temp_persistence, monkeypatch) -> None:
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (_theme_runtime(temp_persistence), None))
    app = create_app()
    client = TestClient(app)
    theme = temp_persistence.create_theme("AI")
    added = client.post(f"/regime/themes/{theme['id']}/tickers", data={"ticker": "NVDA", "role": "Core", "time_horizon": "strategic"})
    assert added.status_code == 200
    updated = client.put(f"/regime/themes/{theme['id']}/tickers/NVDA", data={"role": "Speculative"})
    assert updated.status_code == 200
    deleted = client.delete(f"/regime/themes/{theme['id']}/tickers/NVDA")
    assert deleted.status_code == 200


def test_theme_health_route(temp_persistence, monkeypatch) -> None:
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (_theme_runtime(temp_persistence), None))
    theme = temp_persistence.create_theme("AI")
    temp_persistence.add_ticker_to_theme(theme["id"], "NVDA", role="Core")
    monkeypatch.setattr(regime_route, "load_payload", lambda: {"rows": [{"ticker": "NVDA", "regime": "Bull", "current_price": 100.0}]})
    app = create_app()
    client = TestClient(app)
    response = client.get("/regime/theme-health")
    assert response.status_code == 200
    assert response.json()["themes"][0]["name"] == "AI"

