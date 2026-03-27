from __future__ import annotations

import importlib
import json
import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from src.app.main import create_app
from src.app.routes import regime as regime_route
from src.regime import discovery as discovery_module
from src.regime import persistence as persistence_module


@pytest.fixture()
def temp_modules(tmp_path, monkeypatch):
    monkeypatch.setenv("HMM_DATA_DIR", str(tmp_path))
    store = importlib.reload(persistence_module)
    monkeypatch.setattr(store, "DB_PATH", tmp_path / "regime_watch.db")
    discovery = importlib.reload(discovery_module)
    return store, discovery


def _runtime(store, discovery):
    return {
        "list_themes": store.list_themes,
        "get_theme": store.get_theme,
        "create_theme": store.create_theme,
        "update_theme": store.update_theme,
        "delete_theme": store.delete_theme,
        "get_supply_chain": store.get_supply_chain,
        "generate_supply_chain": discovery.generate_supply_chain,
        "delete_supply_chain": store.delete_supply_chain,
        "get_watchlist": store.get_watchlist,
        "get_watchlist_entry": store.get_watchlist_entry,
        "get_watchlist_stats": store.get_watchlist_stats,
        "update_watchlist_status": store.update_watchlist_status,
        "delete_watchlist_entry": store.delete_watchlist_entry,
        "promote_candidate": discovery.promote_candidate,
        "check_entry_signals": discovery.check_entry_signals,
        "run_discovery_scan": discovery.run_discovery_scan,
        "run_full_discovery": discovery.run_full_discovery,
        "expire_stale_candidates": discovery.expire_stale_candidates,
    }


def test_sector_hint_schema_migration(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("HMM_DATA_DIR", str(tmp_path))
    db_path = tmp_path / "regime_watch.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE investment_theme (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            narrative TEXT NOT NULL DEFAULT '',
            conviction INTEGER NOT NULL DEFAULT 3,
            status TEXT NOT NULL DEFAULT 'Active',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.commit()
    conn.close()

    store = importlib.reload(persistence_module)
    monkeypatch.setattr(store, "DB_PATH", db_path)
    store.list_themes()
    with store._connect() as migrated:
        columns = {
            str(row["name"]): row
            for row in migrated.execute("PRAGMA table_info(investment_theme)").fetchall()
        }
    assert "sector_hint" in columns
    theme = store.create_theme("Semis", sector_hint="Semiconductors")
    assert theme["sector_hint"] == "Semiconductors"


def test_create_theme_with_sector_hint(temp_modules) -> None:
    store, _discovery = temp_modules
    theme = store.create_theme("Test Sector Theme", narrative="Test", conviction=4, sector_hint="Cloud Computing")
    assert theme["sector_hint"] == "Cloud Computing"


def test_update_theme_sector_hint(temp_modules) -> None:
    store, _discovery = temp_modules
    theme = store.create_theme("Update Test", narrative="Test")
    assert theme["sector_hint"] == ""
    updated = store.update_theme(theme["id"], sector_hint="AI/ML Software")
    assert updated["sector_hint"] == "AI/ML Software"


def test_build_sector_discovery_prompt_includes_sector(temp_modules) -> None:
    store, discovery = temp_modules
    theme = store.create_theme("Generative AI", narrative="AI infra", conviction=5, sector_hint="Artificial Intelligence / Machine Learning")
    prompt = discovery.build_sector_discovery_prompt(theme, ["NVDA"], ["WOLF"])
    assert "Artificial Intelligence / Machine Learning" in prompt
    assert "Generative AI" in prompt
    assert "AI infra" in prompt
    assert "NVDA" in prompt
    assert "WOLF" in prompt
    assert "Supply Chain Map" not in prompt


def test_discovery_scan_uses_sector_path_when_no_supply_chain(temp_modules, monkeypatch) -> None:
    store, discovery = temp_modules
    theme = store.create_theme("Generative AI", "AI infra", 5, "Active", sector_hint="Semiconductors")
    generated = []
    prompts = []
    monkeypatch.setattr(discovery, "generate_supply_chain", lambda *args, **kwargs: generated.append(True) or [])
    monkeypatch.setattr(discovery, "_validate_ticker", lambda ticker: True)
    monkeypatch.setattr(discovery, "compute_crowd_score", lambda ticker, crowd_assessment=None: (20, {"seed": crowd_assessment}))
    monkeypatch.setattr(discovery, "_quick_regime_screen", lambda ticker: ("Bull", 0.61, 21.0, 17.0))
    monkeypatch.setattr(discovery.time, "sleep", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        discovery,
        "request_frontier_decision",
        lambda prompt, enabled, provider="auto": prompts.append(prompt) or [{"ticker": "WOLF", "company_name": "Wolfspeed", "sector_layer": "Power", "rationale": "Critical provider", "suggested_role": "Critical-Path", "crowd_assessment": 2}],
    )
    rows = discovery.run_discovery_scan(theme["id"])
    assert generated == []
    assert rows[0]["ticker"] == "WOLF"
    assert "Sector / Industry Focus" in prompts[0]


def test_discovery_scan_prefers_supply_chain_over_sector_hint(temp_modules, monkeypatch) -> None:
    store, discovery = temp_modules
    theme = store.create_theme("Generative AI", "AI infra", 5, "Active", sector_hint="Semiconductors")
    store.save_supply_chain_layers(theme["id"], [{"layer": "Memory", "description": "HBM", "example_companies": "MU"}])
    calls: list[str] = []
    monkeypatch.setattr(discovery, "build_discovery_prompt", lambda *args, **kwargs: calls.append("supply") or "prompt-a")
    monkeypatch.setattr(discovery, "build_sector_discovery_prompt", lambda *args, **kwargs: calls.append("sector") or "prompt-b")
    monkeypatch.setattr(discovery, "request_frontier_decision", lambda *args, **kwargs: [])
    discovery.run_discovery_scan(theme["id"])
    assert calls == ["supply"]


def test_discovery_scan_falls_back_to_supply_chain_generation(temp_modules, monkeypatch) -> None:
    store, discovery = temp_modules
    theme = store.create_theme("Unseeded Theme")
    generated: list[int] = []
    monkeypatch.setattr(discovery, "generate_supply_chain", lambda theme_id, frontier_enabled=True, frontier_provider="auto": generated.append(int(theme_id)) or [{"layer": "Memory"}])
    monkeypatch.setattr(discovery, "request_frontier_decision", lambda *args, **kwargs: [])
    discovery.run_discovery_scan(theme["id"])
    assert generated == [int(theme["id"])]


def test_sector_layer_alias_accepted(temp_modules, monkeypatch) -> None:
    store, discovery = temp_modules
    theme = store.create_theme("Generative AI", "AI infra", 5, "Active", sector_hint="Semiconductors")
    monkeypatch.setattr(discovery, "_validate_ticker", lambda ticker: True)
    monkeypatch.setattr(discovery, "compute_crowd_score", lambda ticker, crowd_assessment=None: (20, {"seed": crowd_assessment}))
    monkeypatch.setattr(discovery, "_quick_regime_screen", lambda ticker: ("Bull", 0.61, 21.0, 17.0))
    monkeypatch.setattr(discovery.time, "sleep", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        discovery,
        "request_frontier_decision",
        lambda *args, **kwargs: [{"ticker": "WOLF", "company_name": "Wolfspeed", "sector_layer": "Inference Infrastructure", "rationale": "Critical provider", "suggested_role": "Critical-Path", "crowd_assessment": 2}],
    )
    discovery.run_discovery_scan(theme["id"])
    assert store.get_watchlist(theme_id=theme["id"])[0]["supply_chain_layer"] == "Inference Infrastructure"


def test_generative_ai_narrative_seed(temp_modules) -> None:
    store, _discovery = temp_modules
    theme = store.create_theme("Generative AI")
    store._connect().close()
    updated = store.get_theme(theme["id"])
    assert "generative ai" in updated["narrative"].lower()
    assert updated["sector_hint"] == "Artificial Intelligence / Machine Learning"
    store.update_theme(theme["id"], narrative="Custom narrative", sector_hint="Custom Sector")
    store._connect().close()
    unchanged = store.get_theme(theme["id"])
    assert unchanged["narrative"] == "Custom narrative"
    assert unchanged["sector_hint"] == "Custom Sector"


def test_route_theme_create_with_sector_hint(temp_modules, monkeypatch) -> None:
    store, discovery = temp_modules
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (_runtime(store, discovery), None))
    client = TestClient(create_app())
    response = client.post("/regime/themes", data={"name": "AI", "narrative": "Narrative", "conviction": "4", "status": "Active", "sector_hint": "Semiconductors"})
    assert response.status_code == 200
    assert response.json()["sector_hint"] == "Semiconductors"


def test_route_theme_update_sector_hint(temp_modules, monkeypatch) -> None:
    store, discovery = temp_modules
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (_runtime(store, discovery), None))
    client = TestClient(create_app())
    theme = store.create_theme("AI")
    response = client.put(f"/regime/themes/{theme['id']}", data={"sector_hint": "Cloud Infrastructure"})
    assert response.status_code == 200
    assert response.json()["sector_hint"] == "Cloud Infrastructure"


def test_normalize_theme_sector_hint_validates_length() -> None:
    with pytest.raises(Exception):
        regime_route._normalize_theme_sector_hint("x" * 201)


def test_discovery_job_skips_supply_chain_for_sector_hint_themes(temp_modules, monkeypatch) -> None:
    store, discovery = temp_modules
    theme = store.create_theme("Generative AI", sector_hint="Artificial Intelligence / Machine Learning")
    generated: list[int] = []
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (_runtime(store, discovery) | {
        "generate_supply_chain": lambda theme_id, frontier_enabled=True, frontier_provider="auto": generated.append(int(theme_id)) or [],
        "run_discovery_scan": lambda theme_id, frontier_enabled=True, frontier_provider="auto": [],
        "check_entry_signals": lambda theme_id=None: [],
    }, None))
    job_id = "job-sector-hint"
    regime_route._DISCOVERY_JOBS[job_id] = regime_route.DiscoveryJob(
        job_id=job_id,
        status="queued",
        theme_ids=[int(theme["id"])],
        progress=0,
        total=1,
        current_theme=None,
        results=None,
        error=None,
        created_at=regime_route.dt.datetime.now(regime_route.dt.timezone.utc),
        frontier_provider="auto",
        regenerate_supply_chain=False,
    )
    try:
        regime_route._run_discovery_job(job_id)
        assert generated == []
    finally:
        regime_route._DISCOVERY_JOBS.pop(job_id, None)
