from __future__ import annotations

import importlib

import pytest

from src.regime import config as regime_config
from src.regime import paper_trading as paper_trading_module
from src.regime import persistence as persistence_module


@pytest.fixture()
def temp_modules(tmp_path, monkeypatch):
    monkeypatch.setenv("HMM_DATA_DIR", str(tmp_path))
    store = importlib.reload(persistence_module)
    monkeypatch.setattr(store, "DB_PATH", tmp_path / "regime_watch.db")
    config = importlib.reload(regime_config)
    paper = importlib.reload(paper_trading_module)
    return store, paper, config


def test_get_watchlist_single_status(temp_modules) -> None:
    store, _paper, _config = temp_modules
    theme = store.create_theme("Generative AI")
    store.upsert_watchlist_candidate(theme["id"], "LSCC", status="Entry Signal")
    store.upsert_watchlist_candidate(theme["id"], "AVGO", status="Added")
    rows = store.get_watchlist(theme_id=theme["id"], status="Entry Signal")
    assert [row["ticker"] for row in rows] == ["LSCC"]


def test_get_watchlist_multiple_statuses(temp_modules) -> None:
    store, _paper, _config = temp_modules
    theme = store.create_theme("Generative AI")
    store.upsert_watchlist_candidate(theme["id"], "LSCC", status="Entry Signal")
    store.upsert_watchlist_candidate(theme["id"], "AVGO", status="Added")
    store.upsert_watchlist_candidate(theme["id"], "MU", status="Watching")
    rows = store.get_watchlist(theme_id=theme["id"], status=["Entry Signal", "Added"])
    assert [row["ticker"] for row in rows] == ["LSCC", "AVGO"]


def test_get_watchlist_no_status_excludes_expired_passed(temp_modules) -> None:
    store, _paper, _config = temp_modules
    theme = store.create_theme("Generative AI")
    store.upsert_watchlist_candidate(theme["id"], "LSCC", status="Watching")
    store.upsert_watchlist_candidate(theme["id"], "AVGO", status="Expired")
    store.upsert_watchlist_candidate(theme["id"], "MU", status="Passed")
    rows = store.get_watchlist(theme_id=theme["id"])
    assert [row["ticker"] for row in rows] == ["LSCC"]


def test_generate_buy_plans_includes_added_tickers(temp_modules, monkeypatch) -> None:
    store, paper, _config = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    theme = store.create_theme("Generative AI", conviction=4, status="Active")
    store.upsert_watchlist_candidate(
        theme["id"],
        "LSCC",
        discovery_rationale="Promoted candidate still qualifies.",
        suggested_role="Critical-Path",
        suggested_entry_price=50.0,
        crowd_score=24,
        regime_label="Bull",
        regime_probability=0.65,
        status="Added",
    )
    monkeypatch.setattr(paper, "_batch_current_prices", lambda tickers: {"LSCC": 50.0})
    plans = paper.generate_buy_plans(portfolio["id"])
    assert [plan["ticker"] for plan in plans] == ["LSCC"]


def test_generate_buy_plans_includes_entry_signal_tickers(temp_modules, monkeypatch) -> None:
    store, paper, _config = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    theme = store.create_theme("Physical AI", conviction=4, status="Active")
    store.upsert_watchlist_candidate(
        theme["id"],
        "WOLF",
        discovery_rationale="Entry signal is active.",
        suggested_role="Critical-Path",
        suggested_entry_price=20.0,
        crowd_score=28,
        regime_label="Bull",
        regime_probability=0.61,
        status="Entry Signal",
    )
    monkeypatch.setattr(paper, "_batch_current_prices", lambda tickers: {"WOLF": 20.0})
    plans = paper.generate_buy_plans(portfolio["id"])
    assert [plan["ticker"] for plan in plans] == ["WOLF"]


def test_generate_buy_plans_skips_watching_tickers(temp_modules) -> None:
    store, paper, _config = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    theme = store.create_theme("Physical AI", conviction=4, status="Active")
    store.upsert_watchlist_candidate(
        theme["id"],
        "WOLF",
        suggested_role="Critical-Path",
        suggested_entry_price=20.0,
        status="Watching",
    )
    assert paper.generate_buy_plans(portfolio["id"]) == []


def test_generate_buy_plans_dedup_same_ticker_different_themes(temp_modules, monkeypatch) -> None:
    store, paper, _config = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    theme_a = store.create_theme("Generative AI", conviction=4, status="Active")
    theme_b = store.create_theme("Physical AI", conviction=4, status="Active")
    store.upsert_watchlist_candidate(
        theme_a["id"],
        "LSCC",
        discovery_rationale="AI exposure.",
        suggested_role="Critical-Path",
        suggested_entry_price=50.0,
        status="Added",
    )
    store.upsert_watchlist_candidate(
        theme_b["id"],
        "LSCC",
        discovery_rationale="Robotics exposure.",
        suggested_role="Critical-Path",
        suggested_entry_price=50.0,
        status="Added",
    )
    monkeypatch.setattr(paper, "_batch_current_prices", lambda tickers: {"LSCC": 50.0})
    plans = paper.generate_buy_plans(portfolio["id"])
    assert len(plans) == 2
    assert {(plan["ticker"], int(plan["theme_id"])) for plan in plans} == {
        ("LSCC", int(theme_a["id"])),
        ("LSCC", int(theme_b["id"])),
    }


def test_generate_buy_plans_dedup_same_ticker_same_theme(temp_modules, monkeypatch) -> None:
    store, paper, _config = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    theme = store.create_theme("Generative AI", conviction=4, status="Active")
    candidate = store.upsert_watchlist_candidate(
        theme["id"],
        "LSCC",
        discovery_rationale="Duplicate row guard.",
        suggested_role="Critical-Path",
        suggested_entry_price=50.0,
        status="Added",
    )
    monkeypatch.setattr(paper, "get_watchlist", lambda status=None: [candidate, dict(candidate)])
    monkeypatch.setattr(paper, "_batch_current_prices", lambda tickers: {"LSCC": 50.0})
    plans = paper.generate_buy_plans(portfolio["id"])
    assert len(plans) == 1
    assert plans[0]["ticker"] == "LSCC"
