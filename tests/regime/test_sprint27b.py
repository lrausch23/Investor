from __future__ import annotations

import importlib
from pathlib import Path

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from src.app.main import create_app
from src.app.routes import regime as regime_route
from src.regime import config as regime_config
from src.regime import paper_trading as paper_trading_module
from src.regime import persistence as persistence_module
from src.regime import scheduled_runner as scheduled_runner_module


@pytest.fixture()
def temp_modules(tmp_path, monkeypatch):
    monkeypatch.setenv("HMM_DATA_DIR", str(tmp_path))
    store = importlib.reload(persistence_module)
    monkeypatch.setattr(store, "DB_PATH", tmp_path / "regime_watch.db")
    config = importlib.reload(regime_config)
    paper = importlib.reload(paper_trading_module)
    scheduled = importlib.reload(scheduled_runner_module)
    return store, paper, scheduled, config


def test_generate_buy_plans_from_entry_signals(temp_modules, monkeypatch) -> None:
    store, paper, _scheduled, _config = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    theme = store.create_theme("AI", conviction=4, status="Active")
    store.upsert_watchlist_candidate(
        theme["id"],
        "WOLF",
        discovery_rationale="Critical substrate supplier",
        suggested_role="Critical-Path",
        suggested_entry_price=20.0,
        crowd_score=30,
        regime_label="Bull",
        regime_probability=0.61,
        status="Entry Signal",
    )
    monkeypatch.setattr(paper, "_batch_current_prices", lambda tickers: {"WOLF": 20.0})
    plans = paper.generate_buy_plans(portfolio["id"])
    assert len(plans) == 1
    assert plans[0]["ticker"] == "WOLF"
    assert float(plans[0]["quantity"]) >= 1
    assert "Critical substrate supplier" in plans[0]["rationale"]


def test_generate_buy_plans_skips_existing_positions(temp_modules) -> None:
    store, paper, _scheduled, _config = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    theme = store.create_theme("AI", conviction=4, status="Active")
    store.upsert_watchlist_candidate(
        theme["id"],
        "WOLF",
        suggested_role="Critical-Path",
        suggested_entry_price=20.0,
        status="Entry Signal",
    )
    store.open_paper_position(portfolio["id"], "WOLF", 10, 20.0, "2026-03-01", theme_id=theme["id"])
    assert paper.generate_buy_plans(portfolio["id"]) == []


def test_generate_exit_plans_regime_flip(temp_modules, monkeypatch) -> None:
    store, paper, _scheduled, _config = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    store.open_paper_position(portfolio["id"], "NVDA", 10, 100.0, "2026-03-01", stop_price=90.0)
    monkeypatch.setattr(paper, "_batch_current_prices", lambda tickers: {"NVDA": 110.0})
    plans = paper.generate_exit_plans(portfolio["id"], cached_regime={"NVDA": ("Bear", 0.72)})
    assert len(plans) == 1
    assert plans[0]["action"] == "Sell"


def test_generate_exit_plans_stop_hit(temp_modules, monkeypatch) -> None:
    store, paper, _scheduled, _config = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    store.open_paper_position(portfolio["id"], "NVDA", 10, 100.0, "2026-03-01", stop_price=95.0)
    monkeypatch.setattr(paper, "_batch_current_prices", lambda tickers: {"NVDA": 90.0})
    monkeypatch.setattr(paper, "_quick_regime_screen", lambda ticker: (_ for _ in ()).throw(AssertionError("quick screen should not run on stop hit")))
    plans = paper.generate_exit_plans(portfolio["id"], cached_regime={})
    assert len(plans) == 1
    assert "Stop price hit" in plans[0]["rationale"]


def test_generate_exit_plans_uses_cache(temp_modules, monkeypatch) -> None:
    store, paper, _scheduled, _config = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    store.open_paper_position(portfolio["id"], "NVDA", 10, 100.0, "2026-03-01", stop_price=80.0)
    monkeypatch.setattr(paper, "_batch_current_prices", lambda tickers: {"NVDA": 110.0})
    called = {"count": 0}

    def _quick(_ticker):
        called["count"] += 1
        return ("Bear", 0.7, None, None)

    monkeypatch.setattr(paper, "_quick_regime_screen", _quick)
    paper.generate_exit_plans(portfolio["id"], cached_regime={"NVDA": ("Bear", 0.8)})
    assert called["count"] == 0


def test_execute_approved_buy(temp_modules, monkeypatch) -> None:
    store, paper, _scheduled, _config = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    plan = store.create_trade_plan(portfolio["id"], "NVDA", "Buy", 10, "Entry signal", proposed_price=100.0)
    store.update_trade_plan_status(plan["id"], "Approved")
    monkeypatch.setattr(paper, "_batch_current_prices", lambda tickers: {"NVDA": 101.0})
    payload = paper.execute_approved_plans(portfolio["id"])
    assert len(payload["executed"]) == 1
    portfolio_row = store.get_paper_portfolio(portfolio["id"])
    assert float(portfolio_row["current_cash"]) == pytest.approx(100000.0 - 1000.0)
    positions = store.get_paper_positions(portfolio["id"], status="Open")
    assert len(positions) == 1
    assert positions[0]["ticker"] == "NVDA"


def test_execute_approved_sell(temp_modules, monkeypatch) -> None:
    store, paper, _scheduled, _config = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    position = store.open_paper_position(portfolio["id"], "NVDA", 10, 100.0, "2026-03-01")
    plan = store.create_trade_plan(portfolio["id"], "NVDA", "Sell", 10, "Regime flip", proposed_price=110.0, source="exit_signal")
    store.update_trade_plan_status(plan["id"], "Approved")
    monkeypatch.setattr(paper, "_batch_current_prices", lambda tickers: {"NVDA": 110.0})
    payload = paper.execute_approved_plans(portfolio["id"])
    assert len(payload["executed"]) == 1
    closed = store.get_paper_position(position["id"])
    assert closed["status"] == "Closed"
    assert float(closed["realized_pnl"]) == pytest.approx(100.0)


def test_generate_daily_plans_wrapper(temp_modules, monkeypatch) -> None:
    store, paper, _scheduled, _config = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    theme = store.create_theme("AI", conviction=4, status="Active")
    store.upsert_watchlist_candidate(theme["id"], "WOLF", suggested_entry_price=20.0, regime_label="Bull", regime_probability=0.7, status="Entry Signal")
    store.open_paper_position(portfolio["id"], "NVDA", 10, 100.0, "2026-03-01", stop_price=95.0)
    monkeypatch.setattr(paper, "_batch_current_prices", lambda tickers: {"WOLF": 20.0, "NVDA": 90.0})
    payload = paper.generate_daily_plans(portfolio["id"], cached_regime={"NVDA": ("Bear", 0.7)})
    assert len(payload["buy_plans"]) == 1
    assert len(payload["exit_plans"]) == 1
    assert "generated_at" in payload


def test_expire_stale_plans(temp_modules) -> None:
    store, paper, _scheduled, _config = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    plan = store.create_trade_plan(portfolio["id"], "NVDA", "Buy", 10, "Old plan", proposed_price=100.0)
    with store._connect() as conn:
        conn.execute(
            "UPDATE paper_trade_plan SET created_at = ?, updated_at = ? WHERE id = ?",
            ("2026-03-01T00:00:00+00:00", "2026-03-01T00:00:00+00:00", int(plan["id"])),
        )
    assert paper.expire_stale_plans(portfolio["id"], max_age_days=2) == 1
    assert store.get_trade_plans(portfolio["id"], status="Expired")[0]["id"] == plan["id"]


def _paper_runtime(store, paper):
    class FakePaperBrokerAdapter:
        def __init__(self, portfolio_id):
            self.portfolio_id = int(portfolio_id)

    return {
        "create_paper_portfolio": store.create_paper_portfolio,
        "get_paper_portfolio": store.get_paper_portfolio,
        "list_paper_portfolios": store.list_paper_portfolios,
        "update_paper_portfolio": store.update_paper_portfolio,
        "get_paper_positions": store.get_paper_positions,
        "get_paper_portfolio_summary": store.get_paper_portfolio_summary,
        "get_trade_plans": store.get_trade_plans,
        "update_trade_plan_status": store.update_trade_plan_status,
        "allocate_budget": paper.allocate_budget,
        "generate_daily_plans": paper.generate_daily_plans,
        "execute_approved_plans": paper.execute_approved_plans,
        "execute_approved_plans_via_adapter": lambda portfolio_id, adapter, guardrails, actor="user": paper.execute_approved_plans(portfolio_id),
        "PaperBrokerAdapter": FakePaperBrokerAdapter,
        "DEFAULT_RISK_GUARDRAILS": object(),
        "compute_paper_performance": paper.compute_paper_performance,
        "compute_benchmark_comparison": paper.compute_benchmark_comparison,
    }


def _client(monkeypatch, store, paper) -> TestClient:
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (_paper_runtime(store, paper), None))
    monkeypatch.setattr(regime_route, "load_payload", lambda: {"rows": [{"ticker": "NVDA", "regime": "Bull", "probability": 0.7}]})
    app = create_app()
    return TestClient(app)


def test_paper_portfolio_routes_crud(temp_modules, monkeypatch) -> None:
    store, paper, _scheduled, _config = temp_modules
    client = _client(monkeypatch, store, paper)
    created = client.post("/regime/paper-portfolio", data={"name": "Sandbox", "starting_budget": "100000"})
    assert created.status_code == 200
    listed = client.get("/regime/paper-portfolio")
    assert listed.status_code == 200
    detail = client.get("/regime/paper-portfolio/1")
    assert detail.status_code == 200
    updated = client.put("/regime/paper-portfolio/1", data={"status": "Paused"})
    assert updated.status_code == 200


def test_trade_plan_routes(temp_modules, monkeypatch) -> None:
    store, paper, _scheduled, _config = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    theme = store.create_theme("AI", conviction=4, status="Active")
    store.upsert_watchlist_candidate(theme["id"], "WOLF", suggested_entry_price=20.0, regime_label="Bull", regime_probability=0.7, status="Entry Signal")
    monkeypatch.setattr(paper, "_batch_current_prices", lambda tickers: {"WOLF": 20.0})
    client = _client(monkeypatch, store, paper)
    generated = client.post(f"/regime/paper-portfolio/{portfolio['id']}/plans/generate")
    assert generated.status_code == 200
    plans = client.get(f"/regime/paper-portfolio/{portfolio['id']}/plans?status=all")
    assert plans.status_code == 200
    plan_id = plans.json()["plans"][0]["id"]
    approved = client.put(f"/regime/paper-portfolio/{portfolio['id']}/plans/{plan_id}", data={"status": "Approved"})
    assert approved.status_code == 200
    executed = client.post(f"/regime/paper-portfolio/{portfolio['id']}/plans/execute")
    assert executed.status_code == 200


def test_performance_route(temp_modules, monkeypatch) -> None:
    store, paper, _scheduled, _config = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    store.open_paper_position(portfolio["id"], "NVDA", 10, 100.0, "2026-03-01")
    monkeypatch.setattr(paper, "_batch_current_prices", lambda tickers: {"NVDA": 110.0, "SPY": 105.0})
    monkeypatch.setattr(paper, "compute_benchmark_comparison", lambda portfolio_id, benchmark_ticker="SPY": {"benchmark": "SPY", "paper_return_pct": 1.0, "benchmark_return_pct": 0.5, "alpha_pct": 0.5})
    client = _client(monkeypatch, store, paper)
    response = client.get(f"/regime/paper-portfolio/{portfolio['id']}/performance")
    assert response.status_code == 200
    assert "performance" in response.json()


def test_budget_route(temp_modules, monkeypatch) -> None:
    store, paper, _scheduled, _config = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    store.create_theme("AI", conviction=4, status="Active")
    client = _client(monkeypatch, store, paper)
    response = client.get(f"/regime/paper-portfolio/{portfolio['id']}/budget")
    assert response.status_code == 200
    assert "themes" in response.json()


def test_compute_benchmark_comparison_handles_multicolumn_close(temp_modules) -> None:
    store, paper, _scheduled, _config = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    frame = pd.DataFrame(
        {
            ("Close", "SPY"): [100.0, 110.0],
            ("Close", "QQQ"): [200.0, 220.0],
        }
    )
    payload = paper.compute_benchmark_comparison(portfolio["id"], benchmark_data=frame)
    assert payload["benchmark_return_pct"] == pytest.approx(10.0)


def test_scheduled_plan_generation(temp_modules, monkeypatch) -> None:
    store, paper, scheduled, _config = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    calls = []
    monkeypatch.setattr(scheduled, "load_payload", lambda: {"rows": [{"ticker": "NVDA", "regime": "Bull", "probability": 0.7}]})
    monkeypatch.setattr(scheduled, "generate_daily_plans", lambda portfolio_id, cached_regime=None, cached_payload=None: calls.append((portfolio_id, cached_regime, cached_payload)) or {"buy_plans": [], "holdings_plans": [], "exit_plans": []})
    monkeypatch.setattr(scheduled, "expire_stale_plans", lambda portfolio_id: 0)
    payload = scheduled.run_scheduled_paper_plans()
    assert payload["portfolios"][0]["portfolio_id"] == portfolio["id"]
    assert calls[0][0] == portfolio["id"]
    assert calls[0][1]["NVDA"] == ("Bull", 0.7)
    assert calls[0][2]["rows"][0]["ticker"] == "NVDA"
