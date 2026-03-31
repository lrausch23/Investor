from __future__ import annotations

import importlib
import json
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from src.app.main import create_app
from src.app.routes import regime as regime_route
from src.regime import discovery, persistence


@pytest.fixture()
def temp_modules(tmp_path, monkeypatch):
    monkeypatch.setenv("HMM_DATA_DIR", str(tmp_path))
    persistence_module = importlib.reload(persistence)
    monkeypatch.setattr(persistence_module, "DB_PATH", tmp_path / "regime_watch.db")
    discovery_module = importlib.reload(discovery)
    return persistence_module, discovery_module


def _runtime(store, discovery_module):
    return {
        "list_themes": store.list_themes,
        "get_theme": store.get_theme,
        "create_theme": store.create_theme,
        "delete_theme": store.delete_theme,
        "get_supply_chain": store.get_supply_chain,
        "generate_supply_chain": discovery_module.generate_supply_chain,
        "delete_supply_chain": store.delete_supply_chain,
        "get_watchlist": store.get_watchlist,
        "get_watchlist_entry": store.get_watchlist_entry,
        "get_watchlist_stats": store.get_watchlist_stats,
        "update_watchlist_status": store.update_watchlist_status,
        "delete_watchlist_entry": store.delete_watchlist_entry,
        "promote_candidate": discovery_module.promote_candidate,
        "check_entry_signals": discovery_module.check_entry_signals,
        "run_discovery_scan": discovery_module.run_discovery_scan,
        "run_full_discovery": discovery_module.run_full_discovery,
        "expire_stale_candidates": discovery_module.expire_stale_candidates,
    }


def test_save_and_get_supply_chain(temp_modules) -> None:
    store, _ = temp_modules
    theme = store.create_theme("Generative AI")
    store.save_supply_chain_layers(theme["id"], [{"layer": "GPU Silicon", "description": "Critical compute", "example_companies": "NVDA, AVGO"}])
    rows = store.get_supply_chain(theme["id"])
    assert rows[0]["layer"] == "GPU Silicon"


def test_supply_chain_cascade_delete(temp_modules) -> None:
    store, _ = temp_modules
    theme = store.create_theme("Generative AI")
    store.save_supply_chain_layers(theme["id"], [{"layer": "GPU Silicon"}])
    store.delete_theme(theme["id"])
    assert store.get_supply_chain(theme["id"]) == []


def test_build_supply_chain_prompt_includes_theme_context(temp_modules) -> None:
    store, discovery_module = temp_modules
    theme = store.create_theme("Generative AI", "AI infra", 5, "Active")
    store.add_ticker_to_theme(theme["id"], "NVDA", role="Core")
    prompt = discovery_module.build_supply_chain_prompt(store.get_theme(theme["id"]))
    assert "Generative AI" in prompt
    assert "NVDA" in prompt


def test_generate_supply_chain_parses_llm(temp_modules, monkeypatch) -> None:
    store, discovery_module = temp_modules
    theme = store.create_theme("Generative AI")
    monkeypatch.setattr(
        discovery_module,
        "request_frontier_decision",
        lambda prompt, enabled, provider="auto": [{"layer": "Memory", "description": "HBM bottleneck", "example_companies": "MU, WDC"}],
    )
    rows = discovery_module.generate_supply_chain(theme["id"])
    assert rows[0]["layer"] == "Memory"


def test_crowd_score_low_coverage(temp_modules, monkeypatch) -> None:
    _, discovery_module = temp_modules
    discovery_module._CROWD_SCORE_CACHE.clear()
    monkeypatch.setattr(discovery_module, "get_ticker_info", lambda ticker: {"numberOfAnalystOpinions": 3, "heldPercentInstitutions": 0.2, "averageVolume": 100_000, "regularMarketPrice": 10.0, "shortPercentOfFloat": 0.01})
    score, _details = discovery_module.compute_crowd_score("SMALL")
    assert score <= 20


def test_crowd_score_high_coverage(temp_modules, monkeypatch) -> None:
    _, discovery_module = temp_modules
    discovery_module._CROWD_SCORE_CACHE.clear()
    monkeypatch.setattr(discovery_module, "get_ticker_info", lambda ticker: {"numberOfAnalystOpinions": 30, "heldPercentInstitutions": 0.9, "averageVolume": 5_000_000, "regularMarketPrice": 100.0, "shortPercentOfFloat": 0.2})
    score, _details = discovery_module.compute_crowd_score("MEGA")
    assert score >= 80


def test_crowd_score_missing_data_defaults_to_50(temp_modules, monkeypatch) -> None:
    _, discovery_module = temp_modules
    discovery_module._CROWD_SCORE_CACHE.clear()
    monkeypatch.setattr(discovery_module, "get_ticker_info", lambda ticker: {})
    score, details = discovery_module.compute_crowd_score("MISS")
    assert score == 50
    assert details["note"] == "insufficient data"


def test_crowd_score_llm_seed_fallback(temp_modules, monkeypatch) -> None:
    _, discovery_module = temp_modules
    discovery_module._CROWD_SCORE_CACHE.clear()
    monkeypatch.setattr(discovery_module, "get_ticker_info", lambda ticker: {})
    score, _details = discovery_module.compute_crowd_score("MISS", crowd_assessment=3)
    assert score == 30


def test_build_discovery_prompt_excludes_held_and_watchlist(temp_modules) -> None:
    store, discovery_module = temp_modules
    theme = store.create_theme("Generative AI", "AI infra", 5, "Active")
    prompt = discovery_module.build_discovery_prompt(store.get_theme(theme["id"]), [{"layer": "Memory", "description": "HBM", "example_companies": "MU"}], ["NVDA"], ["WOLF"])
    assert "NVDA" in prompt
    assert "WOLF" in prompt


def test_run_discovery_scan_mocked(temp_modules, monkeypatch) -> None:
    store, discovery_module = temp_modules
    theme = store.create_theme("Generative AI", "AI infra", 5, "Active")
    store.save_supply_chain_layers(theme["id"], [{"layer": "Power", "description": "Power", "example_companies": "WOLF"}])
    monkeypatch.setattr(discovery_module, "request_frontier_decision", lambda *args, **kwargs: [{"ticker": "WOLF", "company_name": "Wolfspeed", "supply_chain_layer": "Power", "rationale": "Critical SiC provider", "suggested_role": "Critical-Path", "crowd_assessment": 2}])
    monkeypatch.setattr(discovery_module, "_validate_ticker", lambda ticker: True)
    monkeypatch.setattr(discovery_module, "compute_crowd_score", lambda ticker, crowd_assessment=None: (20, {"seed": crowd_assessment}))
    monkeypatch.setattr(discovery_module, "_quick_regime_screen", lambda ticker: ("Bull", 0.61, 21.0, 17.0))
    monkeypatch.setattr(discovery_module.time, "sleep", lambda *_args, **_kwargs: None)
    rows = discovery_module.run_discovery_scan(theme["id"])
    assert rows[0]["ticker"] == "WOLF"
    assert store.get_watchlist(theme_id=theme["id"])[0]["crowd_score"] == 20


def test_discovery_skips_invalid_tickers(temp_modules, monkeypatch) -> None:
    store, discovery_module = temp_modules
    theme = store.create_theme("Generative AI")
    store.save_supply_chain_layers(theme["id"], [{"layer": "Power"}])
    monkeypatch.setattr(discovery_module, "request_frontier_decision", lambda *args, **kwargs: [{"ticker": "BAD", "company_name": "Bad"}])
    monkeypatch.setattr(discovery_module, "_validate_ticker", lambda ticker: False)
    assert discovery_module.run_discovery_scan(theme["id"]) == []


def test_discovery_auto_generates_supply_chain(temp_modules, monkeypatch) -> None:
    store, discovery_module = temp_modules
    theme = store.create_theme("Unseeded Theme")
    generated = []
    monkeypatch.setattr(discovery_module, "generate_supply_chain", lambda theme_id, frontier_enabled=True, frontier_provider="auto": generated.append(theme_id) or [{"layer": "Memory"}])
    monkeypatch.setattr(discovery_module, "request_frontier_decision", lambda *args, **kwargs: [])
    discovery_module.run_discovery_scan(theme["id"])
    assert generated == [theme["id"]]


def test_upsert_watchlist_candidate_create_update(temp_modules) -> None:
    store, _ = temp_modules
    theme = store.create_theme("Generative AI")
    first = store.upsert_watchlist_candidate(theme["id"], "WOLF", company_name="Wolfspeed", crowd_score=25)
    second = store.upsert_watchlist_candidate(theme["id"], "WOLF", company_name="Wolfspeed Inc.", crowd_score=20)
    assert first["ticker"] == "WOLF"
    assert second["crowd_score"] == 20


def test_get_watchlist_filters(temp_modules) -> None:
    store, _ = temp_modules
    theme = store.create_theme("Generative AI")
    store.upsert_watchlist_candidate(theme["id"], "WOLF", status="Watching", crowd_score=25)
    store.upsert_watchlist_candidate(theme["id"], "LITE", status="Passed", crowd_score=60)
    assert len(store.get_watchlist(theme_id=theme["id"])) == 1
    assert store.get_watchlist(theme_id=theme["id"], status="Passed")[0]["ticker"] == "LITE"
    assert store.get_watchlist(theme_id=theme["id"], max_crowd_score=30)[0]["ticker"] == "WOLF"


def test_watchlist_cascade_delete(temp_modules) -> None:
    store, _ = temp_modules
    theme = store.create_theme("Generative AI")
    store.upsert_watchlist_candidate(theme["id"], "WOLF")
    store.delete_theme(theme["id"])
    assert store.get_watchlist(theme_id=theme["id"], status="Watching") == []


def test_get_watchlist_stats(temp_modules) -> None:
    store, _ = temp_modules
    theme = store.create_theme("Generative AI")
    store.upsert_watchlist_candidate(theme["id"], "WOLF", status="Watching", crowd_score=25)
    stats = store.get_watchlist_stats()
    assert stats["total"] == 1
    assert stats["by_status"]["Watching"] == 1


def test_entry_signal_fires(temp_modules) -> None:
    store, discovery_module = temp_modules
    theme = store.create_theme("Generative AI", conviction=4, status="Active")
    store.upsert_watchlist_candidate(theme["id"], "WOLF", regime_label="Bull", regime_probability=0.6, crowd_score=30, status="Watching")
    discovery_module.get_setting = lambda key: "false" if key == "fundamental_gate_enabled" else None
    rows = discovery_module.check_entry_signals(theme["id"])
    assert rows[0]["status"] == "Entry Signal"


def test_entry_signal_blocked_conditions(temp_modules) -> None:
    store, discovery_module = temp_modules
    theme = store.create_theme("Generative AI", conviction=2, status="Active")
    store.upsert_watchlist_candidate(theme["id"], "WOLF", regime_label="Bear", regime_probability=0.8, crowd_score=30, status="Watching")
    discovery_module.get_setting = lambda key: "false" if key == "fundamental_gate_enabled" else None
    assert discovery_module.check_entry_signals(theme["id"]) == []


def test_promote_candidate(temp_modules) -> None:
    store, discovery_module = temp_modules
    theme = store.create_theme("Generative AI")
    entry = store.upsert_watchlist_candidate(theme["id"], "WOLF", discovery_rationale="Power", suggested_role="Critical-Path", suggested_entry_price=20.0, suggested_stop_price=15.0)
    promoted = discovery_module.promote_candidate(int(entry["id"]))
    assert promoted["ticker"] == "WOLF"
    assert store.get_watchlist_entry(int(entry["id"]))["status"] == "Added"


def test_expire_stale_candidates(temp_modules) -> None:
    store, discovery_module = temp_modules
    theme = store.create_theme("Generative AI")
    entry = store.upsert_watchlist_candidate(theme["id"], "WOLF", status="Watching")
    with store._connect() as conn:
        conn.execute("UPDATE discovery_watchlist SET last_scanned_at = ? WHERE id = ?", ("2000-01-01T00:00:00+00:00", int(entry["id"])))
    assert discovery_module.expire_stale_candidates(max_age_days=90) == 1


def test_supply_chain_routes(temp_modules, monkeypatch) -> None:
    store, discovery_module = temp_modules
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (_runtime(store, discovery_module), None))
    monkeypatch.setattr(discovery_module, "request_frontier_decision", lambda *args, **kwargs: [{"layer": "Memory", "description": "HBM", "example_companies": "MU"}])
    app = create_app()
    client = TestClient(app)
    theme = store.create_theme("Generative AI")
    created = client.post(f"/regime/themes/{theme['id']}/supply-chain", data={"frontier_provider": "auto"})
    assert created.status_code == 200
    fetched = client.get(f"/regime/themes/{theme['id']}/supply-chain")
    assert fetched.status_code == 200
    deleted = client.delete(f"/regime/themes/{theme['id']}/supply-chain")
    assert deleted.status_code == 200


def test_watchlist_routes(temp_modules, monkeypatch) -> None:
    store, discovery_module = temp_modules
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (_runtime(store, discovery_module), None))
    app = create_app()
    client = TestClient(app)
    theme = store.create_theme("Generative AI")
    entry = store.upsert_watchlist_candidate(theme["id"], "WOLF", status="Watching")
    listed = client.get("/regime/watchlist")
    assert listed.status_code == 200
    fetched = client.get(f"/regime/watchlist/{entry['id']}")
    assert fetched.status_code == 200
    updated = client.put(f"/regime/watchlist/{entry['id']}", data={"status": "Passed", "notes": "Too crowded"})
    assert updated.status_code == 200
    deleted = client.delete(f"/regime/watchlist/{entry['id']}")
    assert deleted.status_code == 200


def test_discovery_scan_route(temp_modules, monkeypatch) -> None:
    store, discovery_module = temp_modules
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (_runtime(store, discovery_module), None))

    class _ImmediateExecutor:
        def submit(self, fn, *args, **kwargs):
            fn(*args, **kwargs)
            return SimpleNamespace(result=lambda: None)

    monkeypatch.setattr(regime_route, "_EXECUTOR", _ImmediateExecutor())
    monkeypatch.setattr(discovery_module, "generate_supply_chain", lambda theme_id, frontier_enabled=True, frontier_provider="auto": [{"layer": "Memory"}])
    monkeypatch.setattr(discovery_module, "run_discovery_scan", lambda theme_id, frontier_enabled=True, frontier_provider="auto": [store.upsert_watchlist_candidate(theme_id, "WOLF")])
    monkeypatch.setattr(discovery_module, "check_entry_signals", lambda theme_id=None: [])
    monkeypatch.setattr(discovery_module, "expire_stale_candidates", lambda max_age_days=90: 0)
    app = create_app()
    client = TestClient(app)
    theme = store.create_theme("Generative AI")
    response = client.post("/regime/discovery/scan", data={"theme_ids": str(theme["id"])})
    assert response.status_code == 200
    job_id = response.json()["job_id"]
    status = client.get(f"/regime/discovery/scan/{job_id}")
    assert status.status_code == 200
    assert status.json()["status"] == "done"


def test_entry_signals_route(temp_modules, monkeypatch) -> None:
    store, discovery_module = temp_modules
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (_runtime(store, discovery_module), None))
    app = create_app()
    client = TestClient(app)
    theme = store.create_theme("Generative AI", conviction=4, status="Active")
    store.upsert_watchlist_candidate(theme["id"], "WOLF", regime_label="Bull", regime_probability=0.7, crowd_score=20, status="Watching")
    response = client.get("/regime/discovery/signals")
    assert response.status_code == 200
    assert response.json()["count"] == 1
