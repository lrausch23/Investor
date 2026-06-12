from __future__ import annotations

import importlib
from pathlib import Path

import pytest


@pytest.fixture
def temp_modules(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("HMM_DATA_DIR", str(tmp_path))
    import src.regime.agent_candidate_intake as intake
    import src.regime.paper_trading as paper
    import src.regime.persistence as store

    store = importlib.reload(store)
    store.DB_PATH = tmp_path / "regime_watch.db"
    paper = importlib.reload(paper)
    intake = importlib.reload(intake)
    monkeypatch.setattr(intake, "universe_screen_enabled", lambda: False)
    return store, paper, intake


def _seed_portfolio(store):
    theme = store.create_theme("AI Enablers", conviction=3)
    portfolio = store.create_paper_portfolio("Agent 1", starting_budget=25_000, broker_type="ibkr")
    return theme, portfolio


def test_candidate_intake_blocks_stale_research_signal(temp_modules, monkeypatch: pytest.MonkeyPatch) -> None:
    store, paper, intake = temp_modules
    theme, portfolio = _seed_portfolio(store)
    candidate = store.upsert_watchlist_candidate(
        theme["id"],
        "ARW",
        company_name="Arrow",
        suggested_role="Critical-Path",
        suggested_entry_price=100.0,
        suggested_stop_price=90.0,
        regime_label="Bull",
        regime_probability=0.9,
        status="Entry Signal",
    )
    store.create_trade_plan(portfolio["id"], "ARW", "Buy", 1, "old plan", source="discovery")
    for plan in store.get_trade_plans(portfolio["id"], status="Pending"):
        store.update_trade_plan_status(plan["id"], "Expired")
    old_ts = "2026-01-01T14:30:00+00:00"
    with store._connect() as conn:
        conn.execute(
            """
            UPDATE discovery_watchlist
            SET discovered_at = ?, last_scanned_at = ?, entry_signal_at = ?
            WHERE id = ?
            """,
            (old_ts, old_ts, old_ts, int(candidate["id"])),
        )

    monkeypatch.setattr(intake, "_batch_current_prices", lambda tickers: {"ARW": 125.0})

    payload = intake.compute_agent_candidate_intake(portfolio["id"])
    row = next(item for item in payload["candidates"] if item["ticker"] == candidate["ticker"])

    assert row["decision"] == "blocked_signal_quality"
    assert row["signal_quality"]["grade"] == "blocked"
    assert "Signal is stale" in row["reason"]
    assert row["latest_plan"]["status"] == "Expired"


def test_candidate_intake_marks_fresh_candidate_ready(temp_modules, monkeypatch: pytest.MonkeyPatch) -> None:
    store, paper, intake = temp_modules
    theme, portfolio = _seed_portfolio(store)
    store.upsert_watchlist_candidate(
        theme["id"],
        "AVT",
        company_name="Avnet",
        suggested_role="Critical-Path",
        suggested_entry_price=100.0,
        suggested_stop_price=90.0,
        regime_label="Bull",
        regime_probability=0.92,
        status="Entry Signal",
    )

    monkeypatch.setattr(intake, "_batch_current_prices", lambda tickers: {"AVT": 101.0})
    monkeypatch.setattr(intake, "_lookup_atr", lambda ticker: None)
    monkeypatch.setattr(intake, "_lookup_beta", lambda ticker: None)
    monkeypatch.setattr(intake, "estimate_execution_cost", lambda **kwargs: 0.0)
    monkeypatch.setattr(intake, "check_hurdle_rate", lambda *args, **kwargs: type("Hurdle", (), {"passed": True, "reason": "pass", "gross_return_pct": 5.0, "net_return_pct": 4.0})())
    monkeypatch.setattr(intake, "check_duration_gate", lambda *args, **kwargs: type("Duration", (), {"passed": True, "reason": "pass", "expected_regime_duration": 10.0})())

    payload = intake.compute_agent_candidate_intake(portfolio["id"])
    row = next(item for item in payload["candidates"] if item["ticker"] == "AVT")

    assert row["decision"] == "would_create_buy_plan"
    assert row["signal_quality"]["grade"] == "actionable"
    assert row["quantity"] > 0
