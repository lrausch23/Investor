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
    app = create_app()
    return TestClient(app)


def test_guardrail_precheck_endpoint(temp_modules, monkeypatch) -> None:
    store, paper, broker, config = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    plan = store.create_trade_plan(portfolio["id"], "NVDA", "Buy", 10, "Entry", proposed_price=100.0)
    monkeypatch.setattr(paper, "_batch_current_prices", lambda tickers: {"NVDA": 100.0})
    client = _client(monkeypatch, store, paper, broker, config)
    response = client.post(f"/regime/paper-portfolio/{portfolio['id']}/plans/precheck")
    assert response.status_code == 200
    assert response.json()["plans"][0]["plan_id"] == plan["id"]


def test_guardrail_precheck_blocked_plan(temp_modules, monkeypatch) -> None:
    store, paper, broker, config = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    store.create_trade_plan(portfolio["id"], "NVDA", "Buy", 500, "Too large", proposed_price=100.0)
    monkeypatch.setattr(paper, "_batch_current_prices", lambda tickers: {"NVDA": 100.0})
    client = _client(monkeypatch, store, paper, broker, config)
    response = client.post(f"/regime/paper-portfolio/{portfolio['id']}/plans/precheck")
    assert response.status_code == 200
    assert response.json()["plans"][0]["guardrail_passed"] is False


def test_guardrail_precheck_all_pass(temp_modules, monkeypatch) -> None:
    store, paper, broker, config = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    store.create_trade_plan(portfolio["id"], "NVDA", "Buy", 5, "Okay", proposed_price=100.0)
    monkeypatch.setattr(paper, "_batch_current_prices", lambda tickers: {"NVDA": 100.0})
    client = _client(monkeypatch, store, paper, broker, config)
    response = client.post(f"/regime/paper-portfolio/{portfolio['id']}/plans/precheck")
    assert response.status_code == 200
    assert response.json()["plans"][0]["guardrail_passed"] is True


def test_kill_switch_rejects_pending_plans(temp_modules) -> None:
    store, paper, _broker, _config = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    plan = store.create_trade_plan(portfolio["id"], "NVDA", "Buy", 10, "Entry", proposed_price=100.0)
    result = paper.kill_switch(portfolio["id"])
    assert result["rejected_count"] >= 1
    assert store.get_trade_plan(plan["id"])["status"] == "Rejected"


def test_kill_switch_rejects_approved_plans(temp_modules) -> None:
    store, paper, _broker, _config = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    plan = store.create_trade_plan(portfolio["id"], "NVDA", "Buy", 10, "Entry", proposed_price=100.0)
    store.update_trade_plan_status(plan["id"], "Approved")
    paper.kill_switch(portfolio["id"])
    assert store.get_trade_plan(plan["id"])["status"] == "Rejected"


def test_kill_switch_pauses_portfolio(temp_modules) -> None:
    store, paper, _broker, _config = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    result = paper.kill_switch(portfolio["id"])
    assert result["portfolio_status"] == "Paused"
    assert store.get_paper_portfolio(portfolio["id"])["status"] == "Paused"


def test_kill_switch_logs_audit_event(temp_modules) -> None:
    store, paper, _broker, _config = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    paper.kill_switch(portfolio["id"], reason="panic")
    rows = store.get_audit_trail(portfolio_id=portfolio["id"])
    assert any(row["event_type"] == "cancelled" and row["details"] == "panic" for row in rows)


def test_audit_trail_route_filters(temp_modules, monkeypatch) -> None:
    store, paper, broker, config = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    store.log_audit_event(order_id="a", portfolio_id=portfolio["id"], event_type="filled", ticker="NVDA")
    store.log_audit_event(order_id="b", portfolio_id=portfolio["id"], event_type="rejected", ticker="AVGO")
    client = _client(monkeypatch, store, paper, broker, config)
    response = client.get(f"/regime/paper-portfolio/{portfolio['id']}/audit?event_type=filled&ticker=NVDA")
    assert response.status_code == 200
    assert len(response.json()["audit"]) == 1


def test_audit_summary_route(temp_modules, monkeypatch) -> None:
    store, paper, broker, config = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    store.log_audit_event(order_id="a", portfolio_id=portfolio["id"], event_type="filled", ticker="NVDA")
    client = _client(monkeypatch, store, paper, broker, config)
    response = client.get(f"/regime/paper-portfolio/{portfolio['id']}/audit/summary")
    assert response.status_code == 200
    assert response.json()["trades_today"] == 1


def test_audit_trail_after_execution(temp_modules, monkeypatch) -> None:
    store, paper, broker, config = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    plan = store.create_trade_plan(portfolio["id"], "NVDA", "Buy", 5, "Entry", proposed_price=100.0)
    store.update_trade_plan_status(plan["id"], "Approved")
    monkeypatch.setattr(paper, "_batch_current_prices", lambda tickers: {"NVDA": 100.0})
    paper.execute_approved_plans_via_adapter(portfolio["id"], broker.PaperBrokerAdapter(portfolio["id"]), guardrails=config.DEFAULT_RISK_GUARDRAILS)
    rows = store.get_audit_trail(portfolio_id=portfolio["id"])
    assert any(row["event_type"] == "filled" for row in rows)


def test_order_timeline_events(temp_modules, monkeypatch) -> None:
    store, paper, broker, config = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    plan = store.create_trade_plan(portfolio["id"], "NVDA", "Buy", 5, "Entry", proposed_price=100.0)
    store.update_trade_plan_status(plan["id"], "Approved")
    monkeypatch.setattr(paper, "_batch_current_prices", lambda tickers: {"NVDA": 100.0})
    payload = paper.execute_approved_plans_via_adapter(portfolio["id"], broker.PaperBrokerAdapter(portfolio["id"]), guardrails=config.DEFAULT_RISK_GUARDRAILS)
    order_id = payload["executed"][0]["order_id"]
    rows = store.get_audit_trail(portfolio_id=portfolio["id"], order_id=order_id)
    event_types = {row["event_type"] for row in rows}
    assert {"submitted", "filled"}.issubset(event_types)


def test_execution_result_includes_guardrail_details(temp_modules, monkeypatch) -> None:
    store, paper, broker, config = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    plan = store.create_trade_plan(portfolio["id"], "NVDA", "Buy", 5, "Entry", proposed_price=100.0)
    store.update_trade_plan_status(plan["id"], "Approved")
    monkeypatch.setattr(paper, "_batch_current_prices", lambda tickers: {"NVDA": 100.0})
    payload = paper.execute_approved_plans_via_adapter(portfolio["id"], broker.PaperBrokerAdapter(portfolio["id"]), guardrails=config.DEFAULT_RISK_GUARDRAILS)
    assert payload["executed"][0]["guardrail_result"]["allowed"] is True


def test_delete_portfolio_route(temp_modules, monkeypatch) -> None:
    store, paper, broker, config = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    client = _client(monkeypatch, store, paper, broker, config)
    response = client.delete(f"/regime/paper-portfolio/{portfolio['id']}")
    assert response.status_code == 200
    assert response.json()["deleted"] is True


def test_paused_portfolio_blocks_execution(temp_modules, monkeypatch) -> None:
    store, paper, broker, config = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    store.update_paper_portfolio(portfolio["id"], status="Paused")
    client = _client(monkeypatch, store, paper, broker, config)
    response = client.post(f"/regime/paper-portfolio/{portfolio['id']}/plans/execute")
    assert response.status_code == 409


def test_portfolio_status_transitions(temp_modules, monkeypatch) -> None:
    store, paper, broker, config = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    client = _client(monkeypatch, store, paper, broker, config)
    paused = client.put(f"/regime/paper-portfolio/{portfolio['id']}", data={"status": "Paused"})
    active = client.put(f"/regime/paper-portfolio/{portfolio['id']}", data={"status": "Active"})
    closed = client.put(f"/regime/paper-portfolio/{portfolio['id']}", data={"status": "Closed"})
    assert paused.status_code == 200
    assert active.status_code == 200
    assert closed.status_code == 200


def test_kill_switch_route(temp_modules, monkeypatch) -> None:
    store, paper, broker, config = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    client = _client(monkeypatch, store, paper, broker, config)
    response = client.post(f"/regime/paper-portfolio/{portfolio['id']}/kill-switch", data={"reason": "panic"})
    assert response.status_code == 200
    assert response.json()["reason"] == "panic"


def test_kill_switch_missing_portfolio_returns_empty(temp_modules) -> None:
    _store, paper, _broker, _config = temp_modules
    assert paper.kill_switch(9999) is None


def test_precheck_missing_portfolio_returns_404(temp_modules, monkeypatch) -> None:
    store, paper, broker, config = temp_modules
    client = _client(monkeypatch, store, paper, broker, config)
    response = client.post("/regime/paper-portfolio/9999/plans/precheck")
    assert response.status_code == 404


def test_audit_summary_includes_last_trade_at(temp_modules) -> None:
    store, _paper, _broker, _config = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    store.log_audit_event(order_id="a", portfolio_id=portfolio["id"], event_type="filled", ticker="NVDA")
    summary = store.get_daily_audit_summary(portfolio["id"])
    assert summary["last_trade_at"] is not None


def test_execute_closed_portfolio_blocks_execution(temp_modules, monkeypatch) -> None:
    store, paper, broker, config = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    store.update_paper_portfolio(portfolio["id"], status="Closed")
    client = _client(monkeypatch, store, paper, broker, config)
    response = client.post(f"/regime/paper-portfolio/{portfolio['id']}/plans/execute")
    assert response.status_code == 409


def test_delete_missing_portfolio_route(temp_modules, monkeypatch) -> None:
    store, paper, broker, config = temp_modules
    client = _client(monkeypatch, store, paper, broker, config)
    response = client.delete("/regime/paper-portfolio/9999")
    assert response.status_code == 404


def test_portfolio_update_invalid_status_rejected(temp_modules, monkeypatch) -> None:
    store, paper, broker, config = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    client = _client(monkeypatch, store, paper, broker, config)
    response = client.put(f"/regime/paper-portfolio/{portfolio['id']}", data={"status": "Broken"})
    assert response.status_code == 422


def test_audit_trail_filter_by_order_id(temp_modules) -> None:
    store, _paper, _broker, _config = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    store.log_audit_event(order_id="one", portfolio_id=portfolio["id"], event_type="filled", ticker="NVDA")
    store.log_audit_event(order_id="two", portfolio_id=portfolio["id"], event_type="filled", ticker="AVGO")
    rows = store.get_audit_trail(portfolio_id=portfolio["id"], order_id="one")
    assert len(rows) == 1
    assert rows[0]["order_id"] == "one"


def test_guardrail_precheck_preserves_check_details(temp_modules, monkeypatch) -> None:
    store, paper, broker, config = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    store.create_trade_plan(portfolio["id"], "NVDA", "Buy", 500, "Too large", proposed_price=100.0)
    monkeypatch.setattr(paper, "_batch_current_prices", lambda tickers: {"NVDA": 100.0})
    client = _client(monkeypatch, store, paper, broker, config)
    response = client.post(f"/regime/paper-portfolio/{portfolio['id']}/plans/precheck")
    checks = response.json()["plans"][0]["guardrail_checks"]
    assert checks
    assert checks[0]["name"] == "max_position_pct"


def test_kill_switch_rejects_both_pending_and_approved(temp_modules) -> None:
    store, paper, _broker, _config = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    pending = store.create_trade_plan(portfolio["id"], "NVDA", "Buy", 10, "Pending", proposed_price=100.0)
    approved = store.create_trade_plan(portfolio["id"], "AVGO", "Buy", 10, "Approved", proposed_price=100.0)
    store.update_trade_plan_status(approved["id"], "Approved")
    result = paper.kill_switch(portfolio["id"])
    assert result["rejected_count"] == 2
    assert store.get_trade_plan(pending["id"])["status"] == "Rejected"
    assert store.get_trade_plan(approved["id"])["status"] == "Rejected"
