from __future__ import annotations

import datetime as dt
import importlib
import sqlite3

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.app.routes import regime as regime_route
from src.regime import config as config_module
from src.regime import llm_layer as llm_layer_module
from src.regime import paper_trading as paper_trading_module
from src.regime import persistence as persistence_module


@pytest.fixture()
def temp_modules(tmp_path, monkeypatch):
    monkeypatch.setenv("HMM_DATA_DIR", str(tmp_path))
    db_path = tmp_path / "regime_watch.db"
    store = importlib.reload(persistence_module)
    monkeypatch.setattr(store, "DB_PATH", db_path)
    config = importlib.reload(config_module)
    paper = importlib.reload(paper_trading_module)
    llm = importlib.reload(llm_layer_module)
    return store, paper, llm, config, db_path


def _holding_row(
    ticker: str = "NVDA",
    *,
    is_portfolio_holding: bool = True,
    ai_verdict: str | None = "Entry",
    action: str | None = None,
    composite_signal: str | None = None,
    current_price: float = 100.0,
    entry_price: float | None = None,
    theme_id: int | None = None,
    meta_prob: float | None = 0.71,
    regime: str = "Bull",
    probability: float = 0.82,
) -> dict:
    membership = []
    if theme_id is not None:
        membership.append({"theme_id": int(theme_id), "theme_name": f"Theme {theme_id}"})
    return {
        "ticker": ticker,
        "is_portfolio_holding": is_portfolio_holding,
        "ai_verdict": ai_verdict,
        "action": action,
        "composite_signal": composite_signal,
        "current_price": current_price,
        "price_targets": {"entry_price": entry_price} if entry_price is not None else {},
        "theme_membership": membership,
        "meta_labeler_probability": meta_prob,
        "regime": regime,
        "probability": probability,
    }


def _save_buy_signal_snapshot(store, ticker: str, price: float) -> None:
    store.save_signal_snapshot(
        ticker=ticker,
        snapshot_date=dt.date.today().isoformat(),
        action="Buy",
        regime_label="Bull",
        regime_probability=0.8,
        composite_strength=0.7,
        benchmark="SPY",
        current_price=price,
        entry_price=price,
        exit_price=price * 1.10,
        stop_price=price * 0.95,
        risk_reward_ratio=2.0,
        timeframe_days=10,
        expected_regime_duration=12.0,
    )


def _route_client(monkeypatch, runtime: dict):
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (runtime, None))
    app = FastAPI()
    app.include_router(regime_route.router)
    app.dependency_overrides[regime_route.require_actor] = lambda: "tester"
    app.dependency_overrides[regime_route.db_session] = lambda: iter([None])
    return TestClient(app)


def test_trade_plan_source_migration_accepts_holdings(temp_modules) -> None:
    _store, _paper, _llm, _config, db_path = temp_modules
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA foreign_keys = OFF")
        conn.execute(
            """
            CREATE TABLE paper_portfolio (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                starting_budget REAL NOT NULL DEFAULT 100000.0,
                current_cash REAL NOT NULL DEFAULT 100000.0,
                broker_type TEXT NOT NULL DEFAULT 'paper',
                status TEXT NOT NULL DEFAULT 'Active',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE paper_trade_plan (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                portfolio_id INTEGER NOT NULL,
                theme_id INTEGER,
                ticker TEXT NOT NULL,
                action TEXT NOT NULL CHECK (action IN ('Buy', 'Sell')),
                quantity REAL NOT NULL,
                proposed_price REAL,
                rationale TEXT NOT NULL DEFAULT '',
                regime_label TEXT,
                regime_probability REAL,
                crowd_score INTEGER,
                source TEXT NOT NULL DEFAULT 'discovery'
                    CHECK (source IN ('discovery', 'exit_signal', 'manual', 'rebalance')),
                status TEXT NOT NULL DEFAULT 'Pending',
                reviewed_at TEXT,
                executed_at TEXT,
                execution_price REAL,
                broker_order_id TEXT,
                broker_status TEXT,
                filled_quantity REAL NOT NULL DEFAULT 0,
                notes TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.commit()
    store = importlib.reload(persistence_module)
    store.DB_PATH = db_path
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    plan = store.create_trade_plan(portfolio["id"], "NVDA", "Buy", 10, "bridge", source="holdings")
    assert plan["source"] == "holdings"


def test_generate_holdings_plans_entry_verdict(temp_modules) -> None:
    store, paper, _llm, _config, _db_path = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    payload = {"rows": [_holding_row(ai_verdict="Entry")]}
    plans = paper.generate_holdings_plans(portfolio["id"], cached_payload=payload)
    assert len(plans) == 1
    assert plans[0]["ticker"] == "NVDA"
    assert plans[0]["source"] == "holdings"
    assert plans[0]["action"] == "Buy"


def test_generate_holdings_plans_buy_action(temp_modules) -> None:
    store, paper, _llm, _config, _db_path = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    payload = {"rows": [_holding_row(ai_verdict=None, action="Buy", meta_prob=None)]}
    plans = paper.generate_holdings_plans(portfolio["id"], cached_payload=payload)
    assert [plan["ticker"] for plan in plans] == ["NVDA"]


def test_generate_holdings_plans_skips_non_holdings(temp_modules) -> None:
    store, paper, _llm, _config, _db_path = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    payload = {"rows": [_holding_row(is_portfolio_holding=False)]}
    assert paper.generate_holdings_plans(portfolio["id"], cached_payload=payload) == []


def test_generate_holdings_plans_skips_pending_and_open(temp_modules) -> None:
    store, paper, _llm, _config, _db_path = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    store.create_trade_plan(portfolio["id"], "NVDA", "Buy", 5, "pending", source="manual")
    store.open_paper_position(portfolio["id"], "AVGO", 3, 100.0, "2026-03-01")
    payload = {"rows": [_holding_row("NVDA"), _holding_row("AVGO")]}
    assert paper.generate_holdings_plans(portfolio["id"], cached_payload=payload) == []


def test_generate_holdings_plans_same_ticker_different_themes(temp_modules) -> None:
    store, paper, _llm, _config, _db_path = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    theme_a = store.create_theme("Generative AI", conviction=4, status="Active")
    theme_b = store.create_theme("Physical AI", conviction=4, status="Active")
    payload = {
        "rows": [
            _holding_row("LSCC", theme_id=theme_a["id"]),
            _holding_row("LSCC", theme_id=theme_b["id"]),
        ]
    }
    plans = paper.generate_holdings_plans(portfolio["id"], cached_payload=payload)
    assert {(plan["ticker"], int(plan["theme_id"])) for plan in plans} == {
        ("LSCC", int(theme_a["id"])),
        ("LSCC", int(theme_b["id"])),
    }


def test_generate_holdings_plans_same_ticker_same_theme_dedup(temp_modules) -> None:
    store, paper, _llm, _config, _db_path = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    theme = store.create_theme("Generative AI", conviction=4, status="Active")
    payload = {"rows": [_holding_row("LSCC", theme_id=theme["id"]), _holding_row("LSCC", theme_id=theme["id"])]}
    plans = paper.generate_holdings_plans(portfolio["id"], cached_payload=payload)
    assert len(plans) == 1


def test_generate_daily_plans_includes_holdings_plans(temp_modules) -> None:
    store, paper, _llm, _config, _db_path = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    payload = {"rows": [_holding_row("NVDA", ai_verdict="Entry")]}
    result = paper.generate_daily_plans(portfolio["id"], cached_regime={}, cached_payload=payload)
    assert len(result["holdings_plans"]) == 1
    assert result["created_count"] == 1


def test_generate_daily_plans_discovery_priority_over_holdings(temp_modules, monkeypatch) -> None:
    store, paper, _llm, _config, _db_path = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    theme = store.create_theme("Generative AI", conviction=4, status="Active")
    store.upsert_watchlist_candidate(
        theme["id"],
        "NVDA",
        discovery_rationale="Discovery candidate",
        suggested_role="Critical-Path",
        suggested_entry_price=100.0,
        regime_label="Bull",
        regime_probability=0.8,
        status="Entry Signal",
    )
    payload = {"rows": [_holding_row("NVDA", ai_verdict="Entry", theme_id=theme["id"])]}
    _save_buy_signal_snapshot(store, "NVDA", 100.0)
    monkeypatch.setattr(paper, "_batch_current_prices", lambda tickers: {"NVDA": 100.0})
    result = paper.generate_daily_plans(portfolio["id"], cached_regime={}, cached_payload=payload)
    assert len(result["buy_plans"]) == 1
    assert result["buy_plans"][0]["source"] == "discovery"
    assert result["holdings_plans"] == []


def test_deterministic_override_fires_below_threshold(temp_modules, monkeypatch) -> None:
    _store, _paper, llm, _config, _db_path = temp_modules
    called: list[str] = []
    monkeypatch.setattr(llm, "analyze_catalysts", lambda *args, **kwargs: ([], 0, "Neutral"))
    monkeypatch.setattr(llm, "request_frontier_decision", lambda *args, **kwargs: called.append("llm") or {})
    result = llm.build_qualitative_assessment(
        ticker="NVDA",
        regime_signal="Bull",
        state_name="Bull",
        latest_probability=0.8,
        frontier_enabled=True,
        meta_labeler_score=0.20,
    )
    assert result.source == "meta_labeler_override"
    assert result.llm_response["institutional_report"]["verdict"] == "Hold"
    assert called == []


def test_deterministic_override_bear_regime_exits(temp_modules, monkeypatch) -> None:
    _store, _paper, llm, _config, _db_path = temp_modules
    monkeypatch.setattr(llm, "analyze_catalysts", lambda *args, **kwargs: ([], 0, "Negative"))
    result = llm.build_qualitative_assessment(
        ticker="NVDA",
        regime_signal="Bear",
        state_name="Bear",
        latest_probability=0.8,
        frontier_enabled=True,
        meta_labeler_score=0.20,
    )
    assert result.source == "meta_labeler_override"
    assert result.llm_response["institutional_report"]["verdict"] == "Exit"


def test_deterministic_override_skips_above_threshold(temp_modules, monkeypatch) -> None:
    _store, _paper, llm, _config, _db_path = temp_modules
    called: list[str] = []
    monkeypatch.setattr(llm, "analyze_catalysts", lambda *args, **kwargs: ([], 0, "Neutral"))
    monkeypatch.setattr(llm, "request_frontier_decision", lambda *args, **kwargs: called.append("llm") or None)
    result = llm.build_qualitative_assessment(
        ticker="NVDA",
        regime_signal="Bull",
        state_name="Bull",
        latest_probability=0.8,
        frontier_enabled=True,
        meta_labeler_score=0.50,
    )
    assert result.source != "meta_labeler_override"
    assert called == ["llm"]


def test_deterministic_override_skips_none_score(temp_modules, monkeypatch) -> None:
    _store, _paper, llm, _config, _db_path = temp_modules
    called: list[str] = []
    monkeypatch.setattr(llm, "analyze_catalysts", lambda *args, **kwargs: ([], 0, "Neutral"))
    monkeypatch.setattr(llm, "request_frontier_decision", lambda *args, **kwargs: called.append("llm") or None)
    result = llm.build_qualitative_assessment(
        ticker="NVDA",
        regime_signal="Bull",
        state_name="Bull",
        latest_probability=0.8,
        frontier_enabled=True,
        meta_labeler_score=None,
    )
    assert result.source != "meta_labeler_override"
    assert called == ["llm"]


def test_deterministic_override_at_exact_threshold(temp_modules, monkeypatch) -> None:
    _store, _paper, llm, _config, _db_path = temp_modules
    called: list[str] = []
    monkeypatch.setattr(llm, "analyze_catalysts", lambda *args, **kwargs: ([], 0, "Neutral"))
    monkeypatch.setattr(llm, "request_frontier_decision", lambda *args, **kwargs: called.append("llm") or None)
    result = llm.build_qualitative_assessment(
        ticker="NVDA",
        regime_signal="Bull",
        state_name="Bull",
        latest_probability=0.8,
        frontier_enabled=True,
        meta_labeler_score=0.30,
    )
    assert result.source != "meta_labeler_override"
    assert called == ["llm"]


def test_override_threshold_from_settings(temp_modules, monkeypatch) -> None:
    store, _paper, llm, _config, _db_path = temp_modules
    monkeypatch.setattr(llm, "analyze_catalysts", lambda *args, **kwargs: ([], 0, "Neutral"))
    monkeypatch.setattr(llm, "request_frontier_decision", lambda *args, **kwargs: None)
    store.set_setting("meta_labeler_override_threshold", "0.45")
    overridden = llm.build_qualitative_assessment(
        ticker="NVDA",
        regime_signal="Bull",
        state_name="Bull",
        latest_probability=0.8,
        frontier_enabled=True,
        meta_labeler_score=0.40,
    )
    store.delete_setting("meta_labeler_override_threshold")
    normal = llm.build_qualitative_assessment(
        ticker="NVDA",
        regime_signal="Bull",
        state_name="Bull",
        latest_probability=0.8,
        frontier_enabled=True,
        meta_labeler_score=0.40,
    )
    assert overridden.source == "meta_labeler_override"
    assert normal.source != "meta_labeler_override"


def test_override_indicator_in_payload() -> None:
    panel = regime_route._frontier_panel(
        qualitative={
            "source": "meta_labeler_override",
            "llm_response": {
                "institutional_report": {
                    "verdict": "Hold",
                    "risk_trigger": "Meta-labeler below 30% threshold",
                }
            },
        },
        label="Bull",
        probability=0.8,
        regime_days=5,
        model_name="OpenAI: gpt-4o",
    )
    assert panel["llm_override"] is True
    assert "threshold" in str(panel["llm_override_reason"]).lower()


def test_route_generate_plans_passes_cached_payload(monkeypatch) -> None:
    captured: dict[str, object] = {}
    runtime = {
        "generate_daily_plans": lambda portfolio_id, cached_regime=None, cached_payload=None, config=None: captured.update(
            portfolio_id=portfolio_id,
            cached_regime=cached_regime,
            cached_payload=cached_payload,
        ) or {"buy_plans": [], "holdings_plans": [], "exit_plans": [], "created_count": 0, "generated_at": "2026-03-28T12:00:00+00:00"},
    }
    monkeypatch.setattr(regime_route, "load_payload", lambda: {"rows": [{"ticker": "NVDA", "ai_verdict": "Entry"}]})
    monkeypatch.setattr(regime_route, "_cached_regime_for_paper_trading", lambda payload=None: {"NVDA": ("Bull", 0.8)})
    client = _route_client(monkeypatch, runtime)
    response = client.post("/regime/paper-portfolio/7/plans/generate")
    assert response.status_code == 200
    assert captured["portfolio_id"] == 7
    assert captured["cached_payload"]["rows"][0]["ticker"] == "NVDA"
    assert captured["cached_regime"]["NVDA"] == ("Bull", 0.8)


def test_holdings_plans_with_override(temp_modules) -> None:
    store, paper, _llm, _config, _db_path = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    payload = {"rows": [_holding_row(ai_verdict="Hold", meta_prob=0.20)]}
    assert paper.generate_holdings_plans(portfolio["id"], cached_payload=payload) == []
