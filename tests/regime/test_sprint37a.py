from __future__ import annotations

import importlib

import pytest
from fastapi.testclient import TestClient

from src.app.main import create_app
from src.app.routes import regime as regime_route
from src.regime import broker_adapter as broker_module
from src.regime import config as config_module
from src.regime import paper_trading as paper_trading_module
from src.regime import persistence as persistence_module


@pytest.fixture()
def temp_modules(tmp_path, monkeypatch):
    monkeypatch.setenv("HMM_DATA_DIR", str(tmp_path))
    store = importlib.reload(persistence_module)
    store.DB_PATH = tmp_path / "regime_watch.db"
    config = importlib.reload(config_module)
    broker = importlib.reload(broker_module)
    paper = importlib.reload(paper_trading_module)
    return store, paper, broker, config


def _runtime(store, paper, broker, config):
    return {
        "create_paper_portfolio": store.create_paper_portfolio,
        "get_paper_portfolio": store.get_paper_portfolio,
        "list_paper_portfolios": store.list_paper_portfolios,
        "update_paper_portfolio": store.update_paper_portfolio,
        "delete_paper_portfolio": store.delete_paper_portfolio,
        "get_paper_portfolio_summary": store.get_paper_portfolio_summary,
        "get_paper_positions": store.get_paper_positions,
        "get_trade_plans": store.get_trade_plans,
        "update_trade_plan_status": store.update_trade_plan_status,
        "allocate_budget": paper.allocate_budget,
        "generate_daily_plans": paper.generate_daily_plans,
        "execute_approved_plans_via_adapter": paper.execute_approved_plans_via_adapter,
        "execute_approved_plans": paper.execute_approved_plans,
        "compute_paper_performance": paper.compute_paper_performance,
        "compute_benchmark_comparison": lambda portfolio_id, benchmark="SPY": {"benchmark": benchmark, "paper_return_pct": 0.02, "benchmark_return_pct": 0.01, "alpha_pct": 0.01},
        "PaperBrokerAdapter": broker.PaperBrokerAdapter,
        "IBKRBrokerAdapter": broker.PaperBrokerAdapter,
        "get_mock_ib_backend": lambda portfolio_id, starting_cash=100000.0: object(),
        "poll_pending_orders": lambda adapter, portfolio_id: [],
        "get_market_hours_status": lambda: type("Status", (), {"value": "regular"})(),
        "DEFAULT_RISK_GUARDRAILS": config.DEFAULT_RISK_GUARDRAILS,
        "OrderRequest": broker.OrderRequest,
        "validate_guardrails": broker.validate_guardrails,
        "get_audit_trail": store.get_audit_trail,
        "get_daily_audit_summary": store.get_daily_audit_summary,
        "kill_switch": paper.kill_switch,
    }


def _client(monkeypatch, store, paper, broker, config) -> TestClient:
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (_runtime(store, paper, broker, config), None))
    monkeypatch.setattr(regime_route, "load_payload", lambda: {"rows": [{"ticker": "NVDA", "regime": "Bull", "probability": 0.7}]})
    return TestClient(create_app())


def test_execute_falls_back_to_paper_adapter_on_ibkr_failure(temp_modules, monkeypatch) -> None:
    store, paper, broker, config = temp_modules
    portfolio = store.create_paper_portfolio("UAT-Sandbox", 100000.0, broker_type="ibkr")
    plan = store.create_trade_plan(portfolio["id"], "NVDA", "Buy", 10, "Entry", proposed_price=100.0)
    store.update_trade_plan_status(plan["id"], "Approved")

    async def fake_adapter(runtime_arg, portfolio_id_arg):
        del runtime_arg, portfolio_id_arg
        return object()

    def execute_with_fallback(portfolio_id, adapter, guardrails, actor="user"):
        del actor
        if isinstance(adapter, broker.PaperBrokerAdapter):
            return paper.execute_approved_plans_via_adapter(portfolio_id, adapter, guardrails=guardrails)
        raise RuntimeError("no event loop")

    monkeypatch.setattr(paper, "_batch_current_prices", lambda tickers: {"NVDA": 100.0})
    monkeypatch.setattr(regime_route, "_get_broker_adapter_safe_async", fake_adapter)
    monkeypatch.setattr(
        regime_route,
        "_load_hmm_runtime",
        lambda: ({**_runtime(store, paper, broker, config), "execute_approved_plans_via_adapter": execute_with_fallback}, None),
    )
    client = TestClient(create_app())
    response = client.post(f"/regime/paper-portfolio/{portfolio['id']}/plans/execute")
    assert response.status_code == 200
    payload = response.json()
    assert len(payload["executed"]) == 1
    assert payload["fallback"] is True
    assert "local paper adapter" in payload["fallback_reason"].lower()
    assert "immediate_fills" not in payload


def test_execute_uses_ibkr_when_available(temp_modules, monkeypatch) -> None:
    store, paper, broker, config = temp_modules
    portfolio = store.create_paper_portfolio("UAT-Sandbox", 100000.0, broker_type="ibkr")
    plan = store.create_trade_plan(portfolio["id"], "NVDA", "Buy", 10, "Entry", proposed_price=100.0)
    store.update_trade_plan_status(plan["id"], "Approved")

    class FakeIbkrAdapter:
        pass

    async def fake_adapter(runtime_arg, portfolio_id_arg):
        del runtime_arg, portfolio_id_arg
        return FakeIbkrAdapter()

    called = {"primary": 0, "poll": 0}

    def execute_primary(portfolio_id, adapter, guardrails, actor="user"):
        del guardrails, actor
        called["primary"] += 1
        assert isinstance(adapter, FakeIbkrAdapter)
        return {"executed": [{"plan_id": 1, "ticker": "NVDA"}], "skipped": [], "portfolio": {"id": int(portfolio_id), "current_cash": 99000.0}}

    def poll_pending(adapter, portfolio_id):
        del adapter, portfolio_id
        called["poll"] += 1
        return []

    monkeypatch.setattr(regime_route, "_get_broker_adapter_safe_async", fake_adapter)
    monkeypatch.setattr(
        regime_route,
        "_load_hmm_runtime",
        lambda: ({**_runtime(store, paper, broker, config), "IBKRBrokerAdapter": FakeIbkrAdapter, "execute_approved_plans_via_adapter": execute_primary, "poll_pending_orders": poll_pending}, None),
    )
    client = TestClient(create_app())
    response = client.post(f"/regime/paper-portfolio/{portfolio['id']}/plans/execute")
    assert response.status_code == 200
    payload = response.json()
    assert "fallback" not in payload
    assert called["primary"] == 1
    assert called["poll"] == 1


def test_execute_fallback_records_fills_in_sqlite(temp_modules, monkeypatch) -> None:
    store, paper, broker, config = temp_modules
    portfolio = store.create_paper_portfolio("UAT-Sandbox", 100000.0, broker_type="ibkr")
    plan = store.create_trade_plan(portfolio["id"], "NVDA", "Buy", 10, "Entry", proposed_price=100.0)
    store.update_trade_plan_status(plan["id"], "Approved")

    async def fake_adapter(runtime_arg, portfolio_id_arg):
        del runtime_arg, portfolio_id_arg
        return object()

    def execute_with_fallback(portfolio_id, adapter, guardrails, actor="user"):
        del actor
        if isinstance(adapter, broker.PaperBrokerAdapter):
            return paper.execute_approved_plans_via_adapter(portfolio_id, adapter, guardrails=guardrails)
        raise RuntimeError("no event loop")

    monkeypatch.setattr(paper, "_batch_current_prices", lambda tickers: {"NVDA": 101.0})
    monkeypatch.setattr(regime_route, "_get_broker_adapter_safe_async", fake_adapter)
    monkeypatch.setattr(
        regime_route,
        "_load_hmm_runtime",
        lambda: ({**_runtime(store, paper, broker, config), "execute_approved_plans_via_adapter": execute_with_fallback}, None),
    )
    client = TestClient(create_app())
    response = client.post(f"/regime/paper-portfolio/{portfolio['id']}/plans/execute")
    assert response.status_code == 200
    payload = response.json()
    assert len(payload["executed"]) == 1
    assert float(payload["executed"][0]["execution_price"]) > 0
    portfolio_row = store.get_paper_portfolio(portfolio["id"])
    assert float(portfolio_row["current_cash"]) == pytest.approx(100000.0 - 1010.0)
    positions = store.get_paper_positions(portfolio["id"], status="Open")
    assert len(positions) == 1
    assert positions[0]["ticker"] == "NVDA"


def test_execute_fallback_still_enforces_guardrails(temp_modules, monkeypatch) -> None:
    store, paper, broker, config = temp_modules
    portfolio = store.create_paper_portfolio("UAT-Sandbox", 100000.0, broker_type="ibkr")
    plan = store.create_trade_plan(portfolio["id"], "NVDA", "Buy", 200, "Too large", proposed_price=100.0)
    store.update_trade_plan_status(plan["id"], "Approved")

    async def fake_adapter(runtime_arg, portfolio_id_arg):
        del runtime_arg, portfolio_id_arg
        return object()

    def execute_with_fallback(portfolio_id, adapter, guardrails, actor="user"):
        del actor
        if isinstance(adapter, broker.PaperBrokerAdapter):
            return paper.execute_approved_plans_via_adapter(portfolio_id, adapter, guardrails=guardrails)
        raise RuntimeError("no event loop")

    monkeypatch.setattr(paper, "_batch_current_prices", lambda tickers: {"NVDA": 100.0})
    monkeypatch.setattr(regime_route, "_get_broker_adapter_safe_async", fake_adapter)
    monkeypatch.setattr(
        regime_route,
        "_load_hmm_runtime",
        lambda: ({**_runtime(store, paper, broker, config), "execute_approved_plans_via_adapter": execute_with_fallback}, None),
    )
    client = TestClient(create_app())
    response = client.post(f"/regime/paper-portfolio/{portfolio['id']}/plans/execute")
    assert response.status_code == 200
    payload = response.json()
    assert payload["executed"] == []
    assert len(payload["skipped"]) == 1
    assert payload["skipped"][0]["status"] == "guardrail_blocked"
    assert store.get_paper_positions(portfolio["id"], status="Open") == []


def test_execute_adapter_none_returns_error(temp_modules, monkeypatch) -> None:
    store, paper, broker, config = temp_modules
    portfolio = store.create_paper_portfolio("UAT-Sandbox", 100000.0, broker_type="ibkr")

    async def no_adapter(runtime_arg, portfolio_id_arg):
        del runtime_arg, portfolio_id_arg
        return None

    monkeypatch.setattr(regime_route, "_get_broker_adapter_safe_async", no_adapter)
    client = _client(monkeypatch, store, paper, broker, config)
    response = client.post(f"/regime/paper-portfolio/{portfolio['id']}/plans/execute")
    assert response.status_code == 200
    payload = response.json()
    assert payload["executed"] == []
    assert payload["errors"] == ["IBKR connection unavailable."]
