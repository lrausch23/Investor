from __future__ import annotations

import importlib
from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from src.app.routes import regime as regime_route
from src.regime import config as regime_config
from src.regime import discovery, paper_trading, persistence


@pytest.fixture()
def temp_modules(tmp_path, monkeypatch):
    monkeypatch.setenv("HMM_DATA_DIR", str(tmp_path))
    persistence_module = importlib.reload(persistence)
    monkeypatch.setattr(persistence_module, "DB_PATH", tmp_path / "regime_watch.db")
    discovery_module = importlib.reload(discovery)
    paper_module = importlib.reload(paper_trading)
    config_module = importlib.reload(regime_config)
    return persistence_module, discovery_module, paper_module, config_module


def test_get_watchlist_entry_direct_query(temp_modules) -> None:
    store, _discovery, _paper, _config = temp_modules
    theme = store.create_theme("AI")
    entry = store.upsert_watchlist_candidate(theme["id"], "WOLF", company_name="Wolfspeed")
    fetched = store.get_watchlist_entry(int(entry["id"]))
    assert fetched is not None
    assert fetched["ticker"] == "WOLF"
    assert fetched["theme_name"] == "AI"


def test_crowd_cache_ttl_eviction(temp_modules, monkeypatch) -> None:
    _store, discovery_module, _paper, config_module = temp_modules
    discovery_module._CROWD_SCORE_CACHE.clear()
    stale_ts = 1.0
    fresh_ts = float(config_module.DEFAULT_DISCOVERY_THRESHOLDS.crowd_cache_ttl_seconds) + 10.0
    monkeypatch.setattr(discovery_module.time, "time", lambda: fresh_ts)
    discovery_module._CROWD_SCORE_CACHE["OLD"] = (stale_ts, 10, {"ticker": "OLD"})
    monkeypatch.setattr(discovery_module, "get_ticker_info", lambda ticker: {"numberOfAnalystOpinions": 2, "heldPercentInstitutions": 0.1, "averageVolume": 1000, "regularMarketPrice": 10.0, "shortPercentOfFloat": 0.01})
    discovery_module.compute_crowd_score("NEW")
    assert "OLD" not in discovery_module._CROWD_SCORE_CACHE


def test_quick_regime_screen_single_download(temp_modules, monkeypatch) -> None:
    _store, discovery_module, _paper, _config = temp_modules
    calls: list[str] = []
    frame = pd.DataFrame(
        {
            "price": [100 + idx * 0.1 for idx in range(300)],
            "high": [101 + idx * 0.1 for idx in range(300)],
            "low": [99 + idx * 0.1 for idx in range(300)],
            "volume": [1_000_000] * 300,
            "vix": [20.0] * 300,
            "yield_10y": [4.0] * 300,
        }
    )
    monkeypatch.setattr(
        discovery_module,
        "download_market_frame",
        lambda ticker, period="2y", interval="1d": calls.append(ticker) or type("Market", (), {"frame": frame})(),
    )
    monkeypatch.setattr(
        discovery_module,
        "fit_regime_model",
        lambda ticker, market_frame, training_window=252, refit_step=21: type("Regime", (), {"latest_label": "Bull", "latest_probability": 0.66})(),
    )
    label, probability, entry_price, stop_price = discovery_module._quick_regime_screen("WOLF")
    assert calls == ["WOLF"]
    assert label == "Bull"
    assert probability == pytest.approx(0.66)
    assert entry_price is not None and stop_price is not None


def test_discovery_job_regenerate_field() -> None:
    job = regime_route.DiscoveryJob(
        job_id="job-1",
        status="pending",
        theme_ids=[1],
        progress=0,
        total=1,
        current_theme=None,
        results=None,
        error=None,
        created_at=datetime.now(timezone.utc),
        regenerate_supply_chain=True,
    )
    payload = regime_route._serialize_discovery_job(job)
    assert payload["regenerate_supply_chain"] is True


def test_discovery_thresholds_from_config(temp_modules) -> None:
    store, discovery_module, _paper, config_module = temp_modules
    theme = store.create_theme("AI", conviction=3, status="Active")
    store.upsert_watchlist_candidate(theme["id"], "WOLF", regime_label="Bull", regime_probability=0.60, crowd_score=35, status="Watching")
    thresholds = config_module.DiscoveryThresholds(entry_signal_min_probability=0.65)
    assert discovery_module.check_entry_signals(theme["id"], thresholds=thresholds) == []


def test_run_full_discovery_orchestration(temp_modules, monkeypatch) -> None:
    store, discovery_module, _paper, _config = temp_modules
    theme_a = store.create_theme("AI", conviction=5, status="Active")
    theme_b = store.create_theme("Power", conviction=4, status="Active")
    monkeypatch.setattr(discovery_module, "run_discovery_scan", lambda theme_id, frontier_enabled=True, frontier_provider="auto": [{"ticker": "WOLF"}] if int(theme_id) == int(theme_a["id"]) else [{"ticker": "SMCI"}, {"ticker": "ANET"}])
    monkeypatch.setattr(discovery_module, "check_entry_signals", lambda theme_id=None, thresholds=discovery_module.DEFAULT_DISCOVERY_THRESHOLDS: [{"ticker": "WOLF"}] if int(theme_id) == int(theme_a["id"]) else [])
    payload = discovery_module.run_full_discovery(theme_ids=[theme_a["id"], theme_b["id"]])
    assert payload["themes_scanned"] == 2
    assert payload["candidates_found"] == 3
    assert payload["entry_signals"] == 1
    assert payload["watchlist_stats"] is not None
    assert payload["results"][0]["errors"] == []


def test_create_paper_portfolio(temp_modules) -> None:
    store, _discovery, _paper, _config = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 250000.0)
    assert portfolio["name"] == "Sandbox"
    assert portfolio["starting_budget"] == pytest.approx(250000.0)


def test_paper_portfolio_cascade_delete(temp_modules) -> None:
    store, _discovery, _paper, _config = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox")
    position = store.open_paper_position(portfolio["id"], "NVDA", 10, 100.0, "2026-03-01")
    plan = store.create_trade_plan(portfolio["id"], "NVDA", "Buy", 10, "Rationale")
    assert position["ticker"] == "NVDA"
    assert plan["ticker"] == "NVDA"
    assert store.delete_paper_portfolio(portfolio["id"]) is True
    assert store.get_paper_positions(portfolio["id"], status="all") == []
    assert store.get_trade_plans(portfolio["id"], status="all") == []


def test_budget_allocation_conviction_tiers(temp_modules) -> None:
    store, _discovery, paper_module, _config = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    themes = [
        store.create_theme("Low", conviction=2, status="Active"),
        store.create_theme("High", conviction=5, status="Active"),
    ]
    allocation = paper_module.allocate_budget(portfolio["id"], themes)
    by_name = {row["theme_name"]: row for row in allocation["themes"]}
    assert by_name["Low"]["allocated"] == pytest.approx(10000.0)
    assert by_name["High"]["allocated"] == pytest.approx(30000.0)


def test_budget_allocation_speculative_cap(temp_modules) -> None:
    store, _discovery, paper_module, _config = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    theme_budget = 30000.0
    cap = paper_module.compute_position_budget(theme_budget, "Speculative", float(portfolio["starting_budget"]))
    assert cap == pytest.approx(4500.0)


def test_budget_allocation_overflow_scaling(temp_modules) -> None:
    store, _discovery, paper_module, _config = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    themes = [
        {"id": 1, "name": "Theme 1", "conviction": 5, "status": "Active"},
        {"id": 2, "name": "Theme 2", "conviction": 5, "status": "Active"},
        {"id": 3, "name": "Theme 3", "conviction": 5, "status": "Active"},
        {"id": 4, "name": "Theme 4", "conviction": 5, "status": "Active"},
    ]
    allocation = paper_module.allocate_budget(portfolio["id"], themes)
    allocated_total = sum(float(row["allocated"]) for row in allocation["themes"])
    assert allocated_total <= allocation["allocatable"] + 1e-9


def test_paper_portfolio_summary(temp_modules) -> None:
    store, _discovery, _paper, _config = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    store.open_paper_position(portfolio["id"], "NVDA", 10, 100.0, "2026-03-01")
    position = store.open_paper_position(portfolio["id"], "AVGO", 5, 200.0, "2026-03-02")
    store.close_paper_position(position["id"], 220.0, "2026-03-10", "manual")
    summary = store.get_paper_portfolio_summary(portfolio["id"])
    assert summary["positions_open"] == 1
    assert summary["positions_closed"] == 1
    assert summary["realized_pnl"] == pytest.approx(100.0)
