from __future__ import annotations

import importlib

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from src.app.main import create_app
from src.regime import broker_adapter as broker_adapter_module
from src.regime import event_bus as event_bus_module
from src.regime import events as events_module
from src.regime import ib_order_translator as translator_module
from src.regime import order_routing as order_routing_module
from src.regime import paper_trading as paper_trading_module
from src.regime import persistence as persistence_module
from src.regime.agents import execution_agent as execution_agent_module


@pytest.fixture()
def temp_modules(tmp_path, monkeypatch):
    monkeypatch.setenv("HMM_DATA_DIR", str(tmp_path))
    store = importlib.reload(persistence_module)
    monkeypatch.setattr(store, "DB_PATH", tmp_path / "regime_watch.db")
    broker = importlib.reload(broker_adapter_module)
    translator = importlib.reload(translator_module)
    routing = importlib.reload(order_routing_module)
    paper = importlib.reload(paper_trading_module)
    exec_mod = importlib.reload(execution_agent_module)
    events = events_module
    event_bus = event_bus_module
    routing._ADV_CACHE.clear()
    return store, broker, translator, routing, paper, exec_mod, events, event_bus


def _client() -> TestClient:
    return TestClient(create_app())


def test_compute_adv_normal(temp_modules, monkeypatch) -> None:
    _store, _broker, _translator, routing, _paper, _exec_mod, _events, _event_bus = temp_modules
    monkeypatch.setattr(
        routing,
        "download_daily_bars",
        lambda ticker, period="1mo", auto_adjust=False: pd.DataFrame({"Volume": [100.0, 200.0, 300.0]}),
    )
    assert routing.compute_adv("AAPL", lookback_days=3) == pytest.approx(200.0)


def test_compute_adv_cache(temp_modules, monkeypatch) -> None:
    _store, _broker, _translator, routing, _paper, _exec_mod, _events, _event_bus = temp_modules
    calls: list[str] = []

    def fake_download(ticker, period="1mo", auto_adjust=False):
        calls.append(str(ticker))
        return pd.DataFrame({"Volume": [100.0] * 20})

    monkeypatch.setattr(routing, "download_daily_bars", fake_download)
    assert routing.compute_adv("AAPL") == pytest.approx(100.0)
    assert routing.compute_adv("AAPL") == pytest.approx(100.0)
    assert calls == ["AAPL"]


def test_estimate_nbbo_unknown_adv(temp_modules) -> None:
    _store, _broker, _translator, routing, _paper, _exec_mod, _events, _event_bus = temp_modules
    nbbo = routing.estimate_nbbo("AAPL", 100.0, adv=None)
    assert nbbo.mid == pytest.approx(100.0)
    assert nbbo.ask > nbbo.bid
    assert nbbo.source == "estimated"


def test_routing_high_liq_patient_buy(temp_modules) -> None:
    _store, _broker, _translator, routing, _paper, _exec_mod, _events, _event_bus = temp_modules
    nbbo = routing.NBBOEstimate(bid=99.99, ask=100.01, mid=100.0, spread=0.02, source="estimated")
    decision = routing.decide_routing("AAPL", "Buy", 10, 100.0, urgency="patient", adv_override=2_000_000.0, nbbo_override=nbbo)
    assert decision.order_type == "limit"
    assert decision.time_in_force == "GTC"
    assert decision.limit_price == pytest.approx(100.0)
    assert decision.strategy_name == "Passive Limit (Mid)"


def test_routing_high_liq_urgent_sell(temp_modules) -> None:
    _store, _broker, _translator, routing, _paper, _exec_mod, _events, _event_bus = temp_modules
    nbbo = routing.NBBOEstimate(bid=99.99, ask=100.01, mid=100.0, spread=0.02, source="estimated")
    decision = routing.decide_routing("AAPL", "Sell", 10, 100.0, urgency="urgent", adv_override=2_000_000.0, nbbo_override=nbbo)
    assert decision.order_type == "marketable_limit"
    assert decision.time_in_force == "IOC"
    assert decision.limit_price == pytest.approx(99.98)


def test_routing_never_returns_market(temp_modules) -> None:
    _store, _broker, _translator, routing, _paper, _exec_mod, _events, _event_bus = temp_modules
    for action in ("Buy", "Sell"):
        for urgency in ("patient", "normal", "urgent"):
            for adv in (None, 100_000.0, 700_000.0, 2_000_000.0):
                decision = routing.decide_routing("AAPL", action, 10, 100.0, urgency=urgency, adv_override=adv)
                assert decision.order_type != "market"


def test_get_set_routing_settings_roundtrip(temp_modules) -> None:
    _store, _broker, _translator, routing, _paper, _exec_mod, _events, _event_bus = temp_modules
    payload = routing.set_routing_settings(
        {
            "adv_high_threshold": 1500000,
            "adv_low_threshold": 450000,
            "adv_lookback_days": 15,
            "price_improvement_pct": 0.002,
        }
    )
    assert payload["adv_high_threshold"] == pytest.approx(1500000)
    assert payload["adv_low_threshold"] == pytest.approx(450000)
    assert payload["adv_lookback_days"] == 15
    assert payload["price_improvement_pct"] == pytest.approx(0.002)


def test_execution_agent_uses_routing(temp_modules, monkeypatch) -> None:
    _store, broker, _translator, _routing, _paper, exec_mod, events, event_bus = temp_modules
    captured: dict[str, object] = {}

    def fake_submit_guarded_order(order, adapter, guardrails, actor="user"):
        del adapter, guardrails
        captured["order"] = order
        captured["actor"] = actor
        return (
            broker.GuardrailResult(allowed=True, estimated_price=101.0, estimated_order_value=1010.0, checks=[]),
            broker.OrderResult(order_id="abc", status="filled", ticker="NVDA", action="Buy", quantity=10.0, filled_price=101.0, filled_at="2026-03-31T00:00:00+00:00"),
        )

    monkeypatch.setattr(
        exec_mod,
        "decide_routing",
        lambda **kwargs: importlib.import_module("src.regime.order_routing").RoutingDecision(
            order_type="marketable_limit",
            time_in_force="IOC",
            limit_price=101.25,
            strategy_name="Marketable Limit (IOC)",
            rationale="urgent exit",
            adv=2_000_000.0,
            adv_bucket="high_liquidity",
            urgency=str(kwargs.get("urgency") or "normal"),
        ),
    )
    runtime = {
        "PaperBrokerAdapter": broker.PaperBrokerAdapter,
        "submit_guarded_order": fake_submit_guarded_order,
        "OrderRequest": broker.OrderRequest,
        "DEFAULT_RISK_GUARDRAILS": object(),
    }
    agent = exec_mod.ExecutionAgent(event_bus.AsyncEventBus(), runtime=runtime)
    result = agent._execute(
        runtime,
        events.TradeDecisionEvent(
            ticker="NVDA",
            portfolio_id=1,
            action="Sell",
            decision="approved",
            quantity=10.0,
            proposed_price=101.0,
            urgency="urgent",
            source="exit_signal",
        ),
    )
    order = captured["order"]
    assert isinstance(order, broker.OrderRequest)
    assert order.order_type == "marketable_limit"
    assert order.time_in_force == "IOC"
    assert order.limit_price == pytest.approx(101.25)
    assert order.routing_strategy == "Marketable Limit (IOC)"
    assert result.routing_strategy == "Marketable Limit (IOC)"


def test_translate_limit_no_price_fallback(temp_modules, caplog) -> None:
    _store, broker, translator, _routing, _paper, _exec_mod, _events, _event_bus = temp_modules
    with caplog.at_level("WARNING"):
        translated = translator.translate_order_request(
            broker.OrderRequest(portfolio_id=1, ticker="NVDA", action="Buy", quantity=5, order_type="limit"),
            1001,
        )
    assert translated.order_type.value == "MKT"
    assert "falling back to MARKET" in caplog.text


def test_translate_tif_gtc(temp_modules) -> None:
    _store, broker, translator, _routing, _paper, _exec_mod, _events, _event_bus = temp_modules
    translated = translator.translate_order_request(
        broker.OrderRequest(portfolio_id=1, ticker="NVDA", action="Buy", quantity=5, order_type="limit", limit_price=100.0, time_in_force="GTC"),
        1001,
    )
    assert translated.time_in_force.value == "GTC"
    assert translated.order_type.value == "LMT"


def test_paper_adapter_limit_buy_fill(temp_modules, monkeypatch) -> None:
    store, broker, _translator, _routing, paper, _exec_mod, _events, _event_bus = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    monkeypatch.setattr(paper, "_batch_current_prices", lambda tickers: {"NVDA": 105.0})
    adapter = broker.PaperBrokerAdapter(portfolio["id"])
    result = adapter.submit_order(
        broker.OrderRequest(portfolio_id=portfolio["id"], ticker="NVDA", action="Buy", quantity=10, order_type="limit", limit_price=100.0)
    )
    assert result.filled_price == pytest.approx(100.0)


def test_paper_adapter_limit_sell_fill(temp_modules, monkeypatch) -> None:
    store, broker, _translator, _routing, paper, _exec_mod, _events, _event_bus = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    store.open_paper_position(portfolio["id"], "NVDA", 10, 100.0, "2026-03-01T00:00:00+00:00")
    monkeypatch.setattr(paper, "_batch_current_prices", lambda tickers: {"NVDA": 95.0})
    adapter = broker.PaperBrokerAdapter(portfolio["id"])
    result = adapter.submit_order(
        broker.OrderRequest(portfolio_id=portfolio["id"], ticker="NVDA", action="Sell", quantity=10, order_type="limit", limit_price=100.0)
    )
    assert result.filled_price == pytest.approx(100.0)


def test_trade_plan_has_routing_columns(temp_modules) -> None:
    store, _broker, _translator, _routing, _paper, _exec_mod, _events, _event_bus = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    plan = store.create_trade_plan(portfolio["id"], "NVDA", "Buy", 10, "Entry")
    assert plan["order_type"] == "limit"
    assert plan["routing_strategy"] == ""


def test_create_trade_plan_with_routing(temp_modules) -> None:
    store, _broker, _translator, _routing, _paper, _exec_mod, _events, _event_bus = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    plan = store.create_trade_plan(
        portfolio["id"],
        "NVDA",
        "Buy",
        10,
        "Entry",
        order_type="limit",
        routing_strategy="Limit (Ask)",
        proposed_price=100.5,
    )
    assert plan["order_type"] == "limit"
    assert plan["routing_strategy"] == "Limit (Ask)"


def test_route_routing_settings_get(temp_modules) -> None:
    _store, _broker, _translator, _routing, _paper, _exec_mod, _events, _event_bus = temp_modules
    client = _client()
    response = client.get("/regime/order-routing/settings")
    assert response.status_code == 200
    assert "adv_high_threshold" in response.json()


def test_route_routing_settings_put(temp_modules) -> None:
    _store, _broker, _translator, _routing, _paper, _exec_mod, _events, _event_bus = temp_modules
    client = _client()
    response = client.put(
        "/regime/order-routing/settings",
        json={"adv_high_threshold": 1200000, "adv_low_threshold": 400000, "adv_lookback_days": 18, "price_improvement_pct": 0.002},
    )
    assert response.status_code == 200
    assert response.json()["adv_high_threshold"] == pytest.approx(1200000)


def test_route_routing_diagnostic(temp_modules, monkeypatch) -> None:
    _store, _broker, _translator, routing, _paper, _exec_mod, _events, _event_bus = temp_modules
    market_data_client = importlib.import_module("src.regime.market_data_client")
    monkeypatch.setattr(routing, "download_daily_bars", lambda ticker, period="1mo", auto_adjust=False: pd.DataFrame({"Volume": [2_000_000.0] * 20}))
    monkeypatch.setattr(market_data_client, "download_daily_bars", lambda ticker, period="5d", auto_adjust=False: pd.DataFrame({"Close": [100.0, 101.0]}))
    client = _client()
    response = client.get("/regime/order-routing/NVDA")
    assert response.status_code == 200
    payload = response.json()
    assert payload["ticker"] == "NVDA"
    assert "buy_routing" in payload
    assert "sell_routing" in payload


def test_generate_buy_plans_has_routing(temp_modules, monkeypatch) -> None:
    store, _broker, _translator, routing, paper, _exec_mod, _events, _event_bus = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    theme = store.create_theme("Generative AI", conviction=4, status="Active")
    store.upsert_watchlist_candidate(
        theme["id"],
        "NVDA",
        discovery_rationale="Entry candidate.",
        suggested_role="Critical-Path",
        suggested_entry_price=100.0,
        status="Entry Signal",
    )
    monkeypatch.setattr(routing, "compute_adv", lambda ticker, lookback_days=20: 2_000_000.0)
    monkeypatch.setattr(paper, "_batch_current_prices", lambda tickers: {"NVDA": 100.0})
    plans = paper.generate_buy_plans(portfolio["id"])
    assert plans[0]["order_type"] == "limit"
    assert plans[0]["routing_strategy"]


def test_generate_exit_plans_has_routing(temp_modules, monkeypatch) -> None:
    store, _broker, _translator, routing, paper, _exec_mod, _events, _event_bus = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    store.open_paper_position(portfolio["id"], "NVDA", 10, 100.0, "2026-03-01T00:00:00+00:00", stop_price=95.0)
    monkeypatch.setattr(paper, "_batch_current_prices", lambda tickers: {"NVDA": 94.0})
    monkeypatch.setattr(routing, "compute_adv", lambda ticker, lookback_days=20: 2_000_000.0)
    plans = paper.generate_exit_plans(portfolio["id"])
    assert plans[0]["order_type"] == "marketable_limit"
    assert "IOC" in plans[0]["routing_strategy"]
