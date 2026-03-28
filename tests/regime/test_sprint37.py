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


def test_precheck_falls_back_to_paper_adapter_on_ibkr_failure(temp_modules, monkeypatch) -> None:
    store, paper, broker, config = temp_modules
    portfolio = store.create_paper_portfolio("UAT-Sandbox", 100000.0, broker_type="ibkr")
    store.create_trade_plan(portfolio["id"], "NVDA", "Buy", 50, "Entry", proposed_price=100.0)
    client = _client(monkeypatch, store, paper, broker, config)

    async def fake_adapter(runtime_arg, portfolio_id_arg):
        del runtime_arg, portfolio_id_arg
        return object()

    def validating(order, adapter, guardrails):
        if isinstance(adapter, broker.PaperBrokerAdapter):
            return broker.validate_guardrails(order, adapter, guardrails)
        raise RuntimeError("no event loop")

    monkeypatch.setattr(regime_route, "_get_broker_adapter_safe_async", fake_adapter)
    monkeypatch.setitem(_runtime(store, paper, broker, config), "validate_guardrails", validating)
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: ({**_runtime(store, paper, broker, config), "validate_guardrails": validating}, None))

    response = client.post(f"/regime/paper-portfolio/{portfolio['id']}/plans/precheck")
    assert response.status_code == 200
    payload = response.json()
    assert len(payload["plans"]) == 1
    assert payload["plans"][0]["guardrail_passed"] is True
    assert payload["plans"][0]["broker_type"] == "paper_fallback"
    assert payload["plans"][0]["fallback"] is True


def test_precheck_fallback_still_enforces_guardrails(temp_modules, monkeypatch) -> None:
    store, paper, broker, config = temp_modules
    portfolio = store.create_paper_portfolio("UAT-Sandbox", 100000.0, broker_type="ibkr")
    store.create_trade_plan(portfolio["id"], "NVDA", "Buy", 200, "Too large", proposed_price=100.0)

    async def fake_adapter(runtime_arg, portfolio_id_arg):
        del runtime_arg, portfolio_id_arg
        return object()

    def validating(order, adapter, guardrails):
        if isinstance(adapter, broker.PaperBrokerAdapter):
            return broker.validate_guardrails(order, adapter, guardrails)
        raise RuntimeError("no event loop")

    monkeypatch.setattr(regime_route, "_get_broker_adapter_safe_async", fake_adapter)
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: ({**_runtime(store, paper, broker, config), "validate_guardrails": validating}, None))
    client = TestClient(create_app())
    response = client.post(f"/regime/paper-portfolio/{portfolio['id']}/plans/precheck")
    assert response.status_code == 200
    plan = response.json()["plans"][0]
    assert plan["guardrail_passed"] is False
    checks = {row["name"]: row for row in plan["guardrail_checks"]}
    assert checks["max_single_order_value"]["passed"] is False


def test_precheck_fallback_adapter_created_once_for_batch(temp_modules, monkeypatch) -> None:
    store, paper, broker, config = temp_modules
    portfolio = store.create_paper_portfolio("UAT-Sandbox", 100000.0, broker_type="ibkr")
    for ticker in ("NVDA", "AVGO", "MU"):
        store.create_trade_plan(portfolio["id"], ticker, "Buy", 10, "Entry", proposed_price=100.0)

    created = {"count": 0}

    class CountingPaperBrokerAdapter(broker.PaperBrokerAdapter):
        def __init__(self, portfolio_id: int):
            created["count"] += 1
            super().__init__(portfolio_id)

    async def fake_adapter(runtime_arg, portfolio_id_arg):
        del runtime_arg, portfolio_id_arg
        return object()

    def validating(order, adapter, guardrails):
        if isinstance(adapter, broker.PaperBrokerAdapter):
            return broker.validate_guardrails(order, adapter, guardrails)
        raise RuntimeError("no event loop")

    monkeypatch.setattr(regime_route, "_get_broker_adapter_safe_async", fake_adapter)
    monkeypatch.setattr(
        regime_route,
        "_load_hmm_runtime",
        lambda: ({**_runtime(store, paper, broker, config), "validate_guardrails": validating, "PaperBrokerAdapter": CountingPaperBrokerAdapter}, None),
    )
    client = TestClient(create_app())
    response = client.post(f"/regime/paper-portfolio/{portfolio['id']}/plans/precheck")
    assert response.status_code == 200
    payload = response.json()
    assert len(payload["plans"]) == 3
    assert all("error" not in row for row in payload["plans"])
    assert created["count"] == 1


def test_precheck_mixed_primary_and_fallback(temp_modules, monkeypatch) -> None:
    store, paper, broker, config = temp_modules
    portfolio = store.create_paper_portfolio("UAT-Sandbox", 100000.0, broker_type="ibkr")
    store.create_trade_plan(portfolio["id"], "NVDA", "Buy", 10, "Primary works", proposed_price=100.0)
    store.create_trade_plan(portfolio["id"], "MU", "Buy", 10, "Needs fallback", proposed_price=100.0)

    async def fake_adapter(runtime_arg, portfolio_id_arg):
        del runtime_arg, portfolio_id_arg
        return object()

    def validating(order, adapter, guardrails):
        if getattr(order, "ticker", "") == "NVDA" and not isinstance(adapter, broker.PaperBrokerAdapter):
            return broker.validate_guardrails(order, broker.PaperBrokerAdapter(portfolio["id"]), guardrails)
        if isinstance(adapter, broker.PaperBrokerAdapter):
            return broker.validate_guardrails(order, adapter, guardrails)
        raise RuntimeError("no event loop")

    monkeypatch.setattr(regime_route, "_get_broker_adapter_safe_async", fake_adapter)
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: ({**_runtime(store, paper, broker, config), "validate_guardrails": validating}, None))
    client = TestClient(create_app())
    response = client.post(f"/regime/paper-portfolio/{portfolio['id']}/plans/precheck")
    assert response.status_code == 200
    by_ticker = {row["ticker"]: row for row in response.json()["plans"]}
    assert by_ticker["NVDA"]["guardrail_passed"] is True
    assert by_ticker["NVDA"]["broker_type"] == "ibkr"
    assert "fallback" not in by_ticker["NVDA"]
    assert by_ticker["MU"]["guardrail_passed"] is True
    assert by_ticker["MU"]["broker_type"] == "paper_fallback"
    assert by_ticker["MU"]["fallback"] is True


def test_precheck_double_failure_returns_error_entry(temp_modules, monkeypatch) -> None:
    store, paper, broker, config = temp_modules
    portfolio = store.create_paper_portfolio("UAT-Sandbox", 100000.0, broker_type="ibkr")
    store.create_trade_plan(portfolio["id"], "NVDA", "Buy", 10, "Entry", proposed_price=100.0)

    class BrokenPaperBrokerAdapter:
        def __init__(self, portfolio_id: int):
            del portfolio_id
            raise RuntimeError("paper fallback unavailable")

    async def fake_adapter(runtime_arg, portfolio_id_arg):
        del runtime_arg, portfolio_id_arg
        return object()

    def validating(order, adapter, guardrails):
        del order, adapter, guardrails
        raise RuntimeError("no event loop")

    monkeypatch.setattr(regime_route, "_get_broker_adapter_safe_async", fake_adapter)
    monkeypatch.setattr(
        regime_route,
        "_load_hmm_runtime",
        lambda: ({**_runtime(store, paper, broker, config), "validate_guardrails": validating, "PaperBrokerAdapter": BrokenPaperBrokerAdapter}, None),
    )
    client = TestClient(create_app())
    response = client.post(f"/regime/paper-portfolio/{portfolio['id']}/plans/precheck")
    assert response.status_code == 200
    plan = response.json()["plans"][0]
    assert plan["guardrail_passed"] is False
    assert "paper fallback unavailable" in plan["error"]


def test_paper_broker_adapter_in_runtime(temp_modules) -> None:
    store, paper, broker, config = temp_modules
    runtime = _runtime(store, paper, broker, config)
    assert runtime["PaperBrokerAdapter"] is broker.PaperBrokerAdapter
