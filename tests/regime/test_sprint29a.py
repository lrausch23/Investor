from __future__ import annotations

import importlib
import datetime as dt

import pytest
from fastapi.testclient import TestClient

from src.app.main import create_app
from src.app.routes import regime as regime_route
from src.regime import broker_adapter as broker_module
from src.regime import config as config_module
from src.regime import ib_connection as ib_connection_module
from src.regime import ib_order_translator as translator_module
from src.regime import ib_types as ib_types_module
from src.regime import ibkr_adapter as ibkr_adapter_module
from src.regime import paper_trading as paper_trading_module
from src.regime import persistence as persistence_module
from src.regime import scheduled_runner as scheduled_runner_module


@pytest.fixture()
def temp_modules(tmp_path, monkeypatch):
    monkeypatch.setenv("HMM_DATA_DIR", str(tmp_path))
    store = importlib.reload(persistence_module)
    monkeypatch.setattr(store, "DB_PATH", tmp_path / "regime_watch.db")
    config = importlib.reload(config_module)
    broker = importlib.reload(broker_module)
    ib_types = importlib.reload(ib_types_module)
    translator = importlib.reload(translator_module)
    ib_connection = importlib.reload(ib_connection_module)
    ibkr = importlib.reload(ibkr_adapter_module)
    paper = importlib.reload(paper_trading_module)
    scheduled = importlib.reload(scheduled_runner_module)
    return store, config, broker, ib_types, translator, ib_connection, ibkr, paper, scheduled


def test_portfolio_summary_marks_open_positions_to_market(temp_modules, monkeypatch) -> None:
    store, _config, _broker, _ib_types, _translator, _ib_connection, _ibkr, paper, _scheduled = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    store.open_paper_position(portfolio["id"], "NVDA", 10, 100.0, "2026-03-01T00:00:00+00:00")
    monkeypatch.setattr(paper, "_batch_current_prices", lambda tickers: {"NVDA": 115.0})
    summary = store.get_paper_portfolio_summary(portfolio["id"])
    assert summary["total_market_value"] == pytest.approx(1150.0)
    assert summary["unrealized_pnl"] == pytest.approx(150.0)
    assert summary["positions"][0]["current_value"] == pytest.approx(1150.0)


def test_compute_paper_performance_downloads_benchmark_once(temp_modules, monkeypatch) -> None:
    store, _config, _broker, _ib_types, _translator, _ib_connection, _ibkr, paper, _scheduled = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    calls: list[tuple[tuple, dict]] = []

    def fake_download(*args, **kwargs):
        calls.append((args, kwargs))
        import pandas as pd

        return pd.DataFrame({"Close": [100.0, 101.0, 102.0]})

    monkeypatch.setattr(paper.yf, "download", fake_download)
    payload = paper.compute_paper_performance(portfolio["id"])
    assert payload["benchmark"]["benchmark_ticker"] == "SPY"
    assert len(calls) == 1


def test_kill_switch_missing_portfolio_returns_none(temp_modules) -> None:
    _store, _config, _broker, _ib_types, _translator, _ib_connection, _ibkr, paper, _scheduled = temp_modules
    assert paper.kill_switch(9999) is None


def test_translate_order_request_supports_market_and_limit(temp_modules) -> None:
    _store, _config, broker, _ib_types, translator, _ib_connection, _ibkr, _paper, _scheduled = temp_modules
    market = translator.translate_order_request(broker.OrderRequest(portfolio_id=1, ticker="NVDA", action="Buy", quantity=5), 101)
    limit = translator.translate_order_request(broker.OrderRequest(portfolio_id=1, ticker="NVDA", action="Sell", quantity=5, limit_price=125.0), 102)
    assert market.order_type.value == "MKT"
    assert limit.order_type.value == "LMT"
    assert limit.limit_price == pytest.approx(125.0)


def test_market_hours_status_regular_weekday(temp_modules) -> None:
    _store, _config, _broker, ib_types, _translator, _ib_connection, _ibkr, _paper, _scheduled = temp_modules
    now = dt.datetime(2026, 3, 26, 10, 0, tzinfo=ib_types.ET)
    assert ib_types.get_market_hours_status(now) == ib_types.MarketHoursStatus.REGULAR


def test_market_hours_status_holiday_closed(temp_modules) -> None:
    _store, _config, _broker, ib_types, _translator, _ib_connection, _ibkr, _paper, _scheduled = temp_modules
    now = dt.datetime(2026, 12, 25, 10, 0, tzinfo=ib_types.ET)
    assert ib_types.get_market_hours_status(now) == ib_types.MarketHoursStatus.CLOSED


def test_mock_ib_backend_reject_mode(temp_modules) -> None:
    _store, _config, _broker, ib_types, _translator, ib_connection, _ibkr, _paper, _scheduled = temp_modules
    backend = ib_connection.MockIBBackend(fill_config=ib_connection.MockFillConfig(mode="reject", reject_reason="No route"))
    backend.connect("127.0.0.1", 7497, 1)
    state = backend.place_order(
        ib_types.IBOrder(
            order_id=1,
            contract_symbol="NVDA",
            action=ib_types.IBOrderAction.BUY,
            order_type=ib_types.IBOrderType.MARKET,
            quantity=10,
        )
    )
    assert state.status == ib_types.IBOrderStatus.INACTIVE
    assert "No route" in state.message


def test_mock_ib_backend_delayed_fill_on_poll(temp_modules) -> None:
    _store, _config, _broker, ib_types, _translator, ib_connection, _ibkr, _paper, _scheduled = temp_modules
    backend = ib_connection.MockIBBackend(fill_config=ib_connection.MockFillConfig(mode="delayed", delay_seconds=0.0))
    backend.connect("127.0.0.1", 7497, 1)
    order = ib_types.IBOrder(order_id=1, contract_symbol="NVDA", action=ib_types.IBOrderAction.BUY, order_type=ib_types.IBOrderType.MARKET, quantity=10)
    first = backend.place_order(order)
    second = backend.get_order_status(1)
    assert first.status == ib_types.IBOrderStatus.SUBMITTED
    assert second.status == ib_types.IBOrderStatus.FILLED


def test_ibkr_adapter_rejects_when_market_closed(temp_modules, monkeypatch) -> None:
    store, _config, broker, _ib_types, _translator, ib_connection, ibkr, _paper, _scheduled = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0, broker_type="ibkr")
    backend = ib_connection.MockIBBackend()
    monkeypatch.setattr(ibkr, "is_market_open", lambda now=None: False)
    monkeypatch.setattr(ibkr, "get_market_hours_status", lambda now=None: type("State", (), {"value": "closed"})())
    monkeypatch.setattr(ibkr, "next_market_open", lambda now=None: dt.datetime(2026, 3, 27, 9, 30))
    adapter = ibkr.IBKRBrokerAdapter(backend, portfolio["id"])
    result = adapter.submit_order(broker.OrderRequest(portfolio_id=portfolio["id"], ticker="NVDA", action="Buy", quantity=5))
    assert result.status == "rejected"
    assert "Market closed" in str(result.message)


def test_poll_pending_orders_executes_delayed_fill(temp_modules, monkeypatch) -> None:
    store, _config, broker, _ib_types, _translator, ib_connection, ibkr, _paper, _scheduled = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0, broker_type="ibkr")
    plan = store.create_trade_plan(portfolio["id"], "NVDA", "Buy", 10, "Entry", proposed_price=100.0)
    store.update_trade_plan_status(plan["id"], "Submitted", broker_order_id="1001", broker_status="submitted")
    backend = ib_connection.MockIBBackend(fill_config=ib_connection.MockFillConfig(mode="delayed", delay_seconds=0.0))
    backend.connect("127.0.0.1", 7497, 1)
    backend.place_order(
        _translator.translate_order_request(
            broker.OrderRequest(portfolio_id=portfolio["id"], ticker="NVDA", action="Buy", quantity=10),
            1001,
        )
    )
    adapter = ibkr.IBKRBrokerAdapter(backend, portfolio["id"])
    changed = ibkr.poll_pending_orders(adapter, portfolio["id"])
    assert changed
    assert store.get_trade_plan(plan["id"])["status"] == "Executed"


def _runtime(store, paper, broker, ib_connection, ibkr, config):
    return {
        "create_paper_portfolio": store.create_paper_portfolio,
        "get_paper_portfolio": store.get_paper_portfolio,
        "list_paper_portfolios": store.list_paper_portfolios,
        "update_paper_portfolio": store.update_paper_portfolio,
        "delete_paper_portfolio": store.delete_paper_portfolio,
        "get_paper_positions": store.get_paper_positions,
        "get_paper_portfolio_summary": store.get_paper_portfolio_summary,
        "get_trade_plans": store.get_trade_plans,
        "update_trade_plan_status": store.update_trade_plan_status,
        "allocate_budget": paper.allocate_budget,
        "generate_daily_plans": paper.generate_daily_plans,
        "execute_approved_plans_via_adapter": paper.execute_approved_plans_via_adapter,
        "execute_approved_plans": paper.execute_approved_plans,
        "compute_paper_performance": paper.compute_paper_performance,
        "compute_benchmark_comparison": paper.compute_benchmark_comparison,
        "PaperBrokerAdapter": broker.PaperBrokerAdapter,
        "IBKRBrokerAdapter": ibkr.IBKRBrokerAdapter,
        "get_ib_backend": ib_connection.get_ib_backend,
        "get_mock_ib_backend": ib_connection.get_mock_ib_backend,
        "poll_pending_orders": ibkr.poll_pending_orders,
        "get_market_hours_status": ib_types_module.get_market_hours_status,
        "DEFAULT_IBKR_CONFIG": config.DEFAULT_IBKR_CONFIG,
        "DEFAULT_RISK_GUARDRAILS": config.DEFAULT_RISK_GUARDRAILS,
        "OrderRequest": broker.OrderRequest,
        "validate_guardrails": broker.validate_guardrails,
        "get_audit_trail": store.get_audit_trail,
        "get_daily_audit_summary": store.get_daily_audit_summary,
        "kill_switch": paper.kill_switch,
    }


def _client(monkeypatch, store, paper, broker, ib_connection, ibkr, config) -> TestClient:
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (_runtime(store, paper, broker, ib_connection, ibkr, config), None))
    monkeypatch.setattr(regime_route, "load_payload", lambda: {"rows": [{"ticker": "NVDA", "regime": "Bull", "probability": 0.7}]})
    app = create_app()
    return TestClient(app)


def test_create_ibkr_portfolio_route(temp_modules, monkeypatch) -> None:
    store, config, broker, _ib_types, _translator, ib_connection, ibkr, paper, _scheduled = temp_modules
    client = _client(monkeypatch, store, paper, broker, ib_connection, ibkr, config)
    response = client.post("/regime/paper-portfolio", data={"name": "IB Sandbox", "starting_budget": "100000", "broker_type": "ibkr"})
    assert response.status_code == 200
    assert response.json()["broker_type"] == "ibkr"


def test_pending_orders_route_for_ibkr_portfolio(temp_modules, monkeypatch) -> None:
    store, config, broker, _ib_types, _translator, ib_connection, ibkr, paper, _scheduled = temp_modules
    portfolio = store.create_paper_portfolio("IB Sandbox", 100000.0, broker_type="ibkr")
    plan = store.create_trade_plan(portfolio["id"], "NVDA", "Buy", 10, "Entry", proposed_price=100.0)
    store.update_trade_plan_status(plan["id"], "Submitted", broker_order_id="1234", broker_status="submitted")
    monkeypatch.setattr(ibkr, "poll_pending_orders", lambda adapter, portfolio_id: [])
    client = _client(monkeypatch, store, paper, broker, ib_connection, ibkr, config)
    response = client.get(f"/regime/paper-portfolio/{portfolio['id']}/orders/pending")
    assert response.status_code == 200
    assert response.json()["orders"][0]["broker_order_id"] == "1234"


def test_kill_switch_route_missing_portfolio_404(temp_modules, monkeypatch) -> None:
    store, config, broker, _ib_types, _translator, ib_connection, ibkr, paper, _scheduled = temp_modules
    client = _client(monkeypatch, store, paper, broker, ib_connection, ibkr, config)
    response = client.post("/regime/paper-portfolio/9999/kill-switch")
    assert response.status_code == 404


def test_scheduled_runner_polls_ibkr_portfolios(temp_modules, monkeypatch) -> None:
    store, _config, _broker, _ib_types, _translator, ib_connection, _ibkr, _paper, scheduled = temp_modules
    store.create_paper_portfolio("IB Sandbox", 100000.0, broker_type="ibkr")
    monkeypatch.setattr(scheduled, "load_payload", lambda: {"rows": []})
    monkeypatch.setattr(scheduled, "generate_daily_plans", lambda portfolio_id, cached_regime=None: {"buy_plans": [], "exit_plans": []})
    monkeypatch.setattr(scheduled, "expire_stale_plans", lambda portfolio_id: 1)
    monkeypatch.setattr(scheduled, "get_mock_ib_backend", lambda portfolio_id, starting_cash=100000.0: ib_connection.MockIBBackend())
    monkeypatch.setattr(scheduled, "poll_pending_orders", lambda adapter, portfolio_id: ["changed"])
    payload = scheduled.run_scheduled_paper_plans()
    assert payload["portfolios"][0]["broker_type"] == "ibkr"
    assert payload["portfolios"][0]["polled_orders"] == 1
