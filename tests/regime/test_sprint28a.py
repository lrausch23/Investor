from __future__ import annotations

import importlib

import pytest

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
    return store, broker, paper, config


def test_order_request_dataclass(temp_modules) -> None:
    _store, broker, _paper, _config = temp_modules
    order = broker.OrderRequest(portfolio_id=1, ticker="nvda", action="Buy", quantity=10)
    assert order.portfolio_id == 1
    assert order.ticker == "nvda"
    assert order.order_type == "market"


def test_order_result_dataclass(temp_modules) -> None:
    _store, broker, _paper, _config = temp_modules
    result = broker.OrderResult(order_id="abc", status="filled", ticker="NVDA", action="Buy", quantity=5, filled_price=123.0)
    assert result.status == "filled"
    assert result.filled_price == 123.0


def test_paper_broker_adapter_buy(temp_modules, monkeypatch) -> None:
    store, broker, paper, _config = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    monkeypatch.setattr(paper, "_batch_current_prices", lambda tickers: {"NVDA": 101.0})
    adapter = broker.PaperBrokerAdapter(portfolio["id"])
    result = adapter.submit_order(broker.OrderRequest(portfolio_id=portfolio["id"], ticker="NVDA", action="Buy", quantity=10))
    assert result.status == "filled"
    assert store.get_paper_positions(portfolio["id"], status="Open")[0]["ticker"] == "NVDA"
    assert float(store.get_paper_portfolio(portfolio["id"])["current_cash"]) == pytest.approx(98990.0)


def test_paper_broker_adapter_sell(temp_modules, monkeypatch) -> None:
    store, broker, paper, _config = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    store.open_paper_position(portfolio["id"], "NVDA", 10, 100.0, "2026-03-01T00:00:00+00:00")
    monkeypatch.setattr(paper, "_batch_current_prices", lambda tickers: {"NVDA": 110.0})
    adapter = broker.PaperBrokerAdapter(portfolio["id"])
    result = adapter.submit_order(broker.OrderRequest(portfolio_id=portfolio["id"], ticker="NVDA", action="Sell", quantity=10))
    assert result.status == "filled"
    closed = store.get_paper_positions(portfolio["id"], status="Closed")
    assert len(closed) == 1
    assert float(closed[0]["realized_pnl"]) == pytest.approx(100.0)


def test_paper_broker_adapter_rejects_insufficient_cash(temp_modules, monkeypatch) -> None:
    store, broker, paper, _config = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 500.0)
    monkeypatch.setattr(paper, "_batch_current_prices", lambda tickers: {"NVDA": 101.0})
    adapter = broker.PaperBrokerAdapter(portfolio["id"])
    result = adapter.submit_order(broker.OrderRequest(portfolio_id=portfolio["id"], ticker="NVDA", action="Buy", quantity=10))
    assert result.status == "rejected"
    assert "Insufficient cash" in str(result.message)


def test_paper_broker_adapter_positions(temp_modules, monkeypatch) -> None:
    store, broker, paper, _config = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    position = store.open_paper_position(portfolio["id"], "NVDA", 10, 100.0, "2026-03-01T00:00:00+00:00", stop_price=95.0)
    monkeypatch.setattr(paper, "_batch_current_prices", lambda tickers: {"NVDA": 111.0})
    adapter = broker.PaperBrokerAdapter(portfolio["id"])
    positions = adapter.get_positions()
    assert positions[0].position_id == position["id"]
    assert positions[0].market_value == pytest.approx(1110.0)
    assert positions[0].unrealized_pnl == pytest.approx(110.0)


def test_paper_broker_adapter_account_summary(temp_modules, monkeypatch) -> None:
    store, broker, paper, _config = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    store.open_paper_position(portfolio["id"], "NVDA", 10, 100.0, "2026-03-01T00:00:00+00:00")
    monkeypatch.setattr(paper, "_batch_current_prices", lambda tickers: {"NVDA": 100.0})
    adapter = broker.PaperBrokerAdapter(portfolio["id"])
    summary = adapter.get_account_summary()
    assert summary.portfolio_id == portfolio["id"]
    assert summary.cash == pytest.approx(100000.0)
    assert summary.market_value == pytest.approx(1000.0)


def test_paper_broker_adapter_order_status_lookup(temp_modules, monkeypatch) -> None:
    store, broker, paper, _config = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    monkeypatch.setattr(paper, "_batch_current_prices", lambda tickers: {"NVDA": 101.0})
    adapter = broker.PaperBrokerAdapter(portfolio["id"])
    result = adapter.submit_order(broker.OrderRequest(portfolio_id=portfolio["id"], ticker="NVDA", action="Buy", quantity=1))
    looked_up = adapter.get_order_status(result.order_id)
    assert looked_up is not None
    assert looked_up.status == "filled"


def test_validate_guardrails_blocks_large_order(temp_modules) -> None:
    _store, broker, _paper, config = temp_modules
    adapter = broker.MockBrokerAdapter()
    order = broker.OrderRequest(portfolio_id=1, ticker="NVDA", action="Buy", quantity=200)
    result = broker.validate_guardrails(order, adapter, config.DEFAULT_RISK_GUARDRAILS)
    assert result.allowed is False
    assert any(check.name == "max_single_order_value" and not check.passed for check in result.checks)


def test_validate_guardrails_passes_reasonable_order(temp_modules) -> None:
    _store, broker, _paper, config = temp_modules
    adapter = broker.MockBrokerAdapter(fill_price=100.0)
    order = broker.OrderRequest(portfolio_id=1, ticker="NVDA", action="Buy", quantity=5)
    result = broker.validate_guardrails(order, adapter, config.DEFAULT_RISK_GUARDRAILS)
    assert result.allowed is True


def test_submit_guarded_order_logs_blocked_event(temp_modules) -> None:
    store, broker, _paper, config = temp_modules
    adapter = broker.MockBrokerAdapter()
    order = broker.OrderRequest(portfolio_id=1, ticker="NVDA", action="Buy", quantity=500)
    guardrail_result, order_result = broker.submit_guarded_order(order, adapter, config.DEFAULT_RISK_GUARDRAILS, actor="user")
    assert order_result is None
    assert guardrail_result.allowed is False
    trail = store.get_audit_trail(portfolio_id=1, days=30)
    event_types = {row["event_type"] for row in trail}
    assert "guardrail_check" in event_types
    assert "guardrail_blocked" in event_types


def test_log_audit_event_roundtrip(temp_modules) -> None:
    store, _broker, _paper, _config = temp_modules
    store.log_audit_event(order_id="abc", portfolio_id=1, event_type="submitted", ticker="NVDA", action="Buy", quantity=10, price=100.0)
    rows = store.get_audit_trail(portfolio_id=1)
    assert rows[0]["order_id"] == "abc"
    assert rows[0]["ticker"] == "NVDA"


def test_count_todays_trades_counts_fills(temp_modules) -> None:
    store, _broker, _paper, _config = temp_modules
    store.log_audit_event(order_id="abc", portfolio_id=1, event_type="filled", ticker="NVDA")
    store.log_audit_event(order_id="def", portfolio_id=1, event_type="rejected", ticker="AVGO")
    assert store.count_todays_trades(1) == 1


def test_daily_audit_summary(temp_modules) -> None:
    store, _broker, _paper, _config = temp_modules
    store.log_audit_event(order_id="abc", portfolio_id=1, event_type="filled", ticker="NVDA")
    store.log_audit_event(order_id="def", portfolio_id=1, event_type="guardrail_blocked", ticker="AVGO")
    summary = store.get_daily_audit_summary(1)
    assert summary["filled_count"] == 1
    assert summary["blocked_count"] == 1


def test_execute_approved_plans_via_adapter_buy(temp_modules, monkeypatch) -> None:
    store, broker, paper, config = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    plan = store.create_trade_plan(portfolio["id"], "NVDA", "Buy", 10, "Entry", proposed_price=100.0)
    store.update_trade_plan_status(plan["id"], "Approved")
    monkeypatch.setattr(paper, "_batch_current_prices", lambda tickers: {"NVDA": 101.0})
    payload = paper.execute_approved_plans_via_adapter(portfolio["id"], broker.PaperBrokerAdapter(portfolio["id"]), guardrails=config.DEFAULT_RISK_GUARDRAILS)
    assert len(payload["executed"]) == 1
    assert store.get_trade_plan(plan["id"])["status"] == "Executed"


def test_execute_approved_plans_via_adapter_sell(temp_modules, monkeypatch) -> None:
    store, broker, paper, config = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    store.open_paper_position(portfolio["id"], "NVDA", 10, 100.0, "2026-03-01T00:00:00+00:00")
    plan = store.create_trade_plan(portfolio["id"], "NVDA", "Sell", 10, "Exit", proposed_price=110.0, source="exit_signal")
    store.update_trade_plan_status(plan["id"], "Approved")
    monkeypatch.setattr(paper, "_batch_current_prices", lambda tickers: {"NVDA": 110.0})
    payload = paper.execute_approved_plans_via_adapter(portfolio["id"], broker.PaperBrokerAdapter(portfolio["id"]), guardrails=config.DEFAULT_RISK_GUARDRAILS)
    assert len(payload["executed"]) == 1
    assert store.get_trade_plan(plan["id"])["status"] == "Executed"


def test_execute_approved_plans_via_adapter_marks_guardrail_reject(temp_modules) -> None:
    store, broker, paper, config = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    plan = store.create_trade_plan(portfolio["id"], "NVDA", "Buy", 500, "Huge order", proposed_price=100.0)
    store.update_trade_plan_status(plan["id"], "Approved")
    payload = paper.execute_approved_plans_via_adapter(portfolio["id"], broker.PaperBrokerAdapter(portfolio["id"]), guardrails=config.DEFAULT_RISK_GUARDRAILS)
    assert payload["executed"] == []
    assert store.get_trade_plan(plan["id"])["status"] == "Rejected"


def test_execute_approved_plans_wrapper_uses_adapter(temp_modules, monkeypatch) -> None:
    store, broker, paper, _config = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    captured = {}

    def fake_execute(portfolio_id, adapter, **kwargs):
        captured["portfolio_id"] = portfolio_id
        captured["adapter_type"] = type(adapter).__name__
        return {"executed": [], "skipped": [], "portfolio": {"id": portfolio_id}}

    monkeypatch.setattr(paper, "execute_approved_plans_via_adapter", fake_execute)
    payload = paper.execute_approved_plans(portfolio["id"])
    assert payload["portfolio"]["id"] == portfolio["id"]
    assert captured["adapter_type"] == broker.PaperBrokerAdapter.__name__


def test_generate_exit_plans_uses_cached_regime_without_quick_screen(temp_modules, monkeypatch) -> None:
    store, _broker, paper, _config = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    store.open_paper_position(portfolio["id"], "NVDA", 10, 100.0, "2026-03-01T00:00:00+00:00", stop_price=90.0)
    monkeypatch.setattr(paper, "_batch_current_prices", lambda tickers: {"NVDA": 110.0})
    monkeypatch.setattr(paper, "_quick_regime_screen", lambda ticker: (_ for _ in ()).throw(AssertionError("should not run")))
    plans = paper.generate_exit_plans(portfolio["id"], cached_regime={"NVDA": ("Bear", 0.8)})
    assert len(plans) == 1


def test_generate_daily_plans_returns_timestamp(temp_modules, monkeypatch) -> None:
    store, _broker, paper, _config = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    monkeypatch.setattr(paper, "generate_buy_plans", lambda *args, **kwargs: [])
    monkeypatch.setattr(paper, "generate_exit_plans", lambda *args, **kwargs: [])
    payload = paper.generate_daily_plans(portfolio["id"], cached_regime={})
    assert "generated_at" in payload
