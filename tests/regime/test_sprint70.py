from __future__ import annotations

import asyncio
import datetime as dt
import importlib
import inspect
import sys
from types import SimpleNamespace

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from src.app.main import create_app
from src.regime import broker_adapter as broker_adapter_module
from src.regime import event_bus as event_bus_module
from src.regime import events as events_module
from src.regime import ib_live_backend as ib_live_backend_module
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
    live_mod = importlib.reload(ib_live_backend_module)
    events = events_module
    event_bus = event_bus_module
    routing._ADV_CACHE.clear()
    return store, broker, translator, routing, paper, exec_mod, live_mod, events, event_bus


def _client() -> TestClient:
    return TestClient(create_app())


def test_needs_algo_execution_threshold(temp_modules) -> None:
    _store, _broker, _translator, routing, _paper, _exec_mod, _live_mod, _events, _event_bus = temp_modules
    assert routing.needs_algo_execution(101, 10_000, adv_pct_threshold=0.01)
    assert not routing.needs_algo_execution(100, 10_000, adv_pct_threshold=0.01)


def test_select_algo_prefers_vwap_for_high_liquidity_normal(temp_modules) -> None:
    _store, _broker, _translator, routing, _paper, _exec_mod, _live_mod, _events, _event_bus = temp_modules
    decision = routing.select_algo(
        "NVDA",
        "Buy",
        25_000,
        1_000_000,
        urgency="normal",
        adv_bucket="high_liquidity",
        max_volume_rate=0.20,
        adv_pct_threshold=0.01,
    )
    assert decision.algo_strategy == "VWAP"
    assert decision.algo_params["maxPctVol"] == "0.20"


def test_select_algo_caps_max_volume_for_very_large_order(temp_modules) -> None:
    _store, _broker, _translator, routing, _paper, _exec_mod, _live_mod, _events, _event_bus = temp_modules
    decision = routing.select_algo(
        "NVDA",
        "Sell",
        120_000,
        1_000_000,
        urgency="urgent",
        adv_bucket="high_liquidity",
        max_volume_rate=0.20,
        adv_pct_threshold=0.01,
    )
    assert decision.algo_strategy == "TWAP"
    assert decision.algo_params["maxPctVol"] == "0.05"


def test_decide_routing_attaches_algo_strategy(temp_modules) -> None:
    _store, _broker, _translator, routing, _paper, _exec_mod, _live_mod, _events, _event_bus = temp_modules
    routing.set_routing_settings(
        {
            "adv_high_threshold": 1_000,
            "adv_low_threshold": 500,
            "algo_enabled": True,
            "algo_adv_pct_threshold": 0.01,
            "algo_max_volume_rate": 0.20,
        }
    )
    decision = routing.decide_routing("NVDA", "Buy", 1_000, 100.0, urgency="normal", adv_override=50_000)
    assert decision.algo_strategy == "VWAP"
    assert decision.algo_params["maxPctVol"] == "0.20"
    assert "VWAP Algo" in decision.strategy_name


def test_get_set_routing_settings_roundtrip_algo_fields(temp_modules) -> None:
    _store, _broker, _translator, routing, _paper, _exec_mod, _live_mod, _events, _event_bus = temp_modules
    payload = routing.set_routing_settings(
        {
            "algo_enabled": False,
            "algo_adv_pct_threshold": 0.025,
            "algo_max_volume_rate": 0.15,
        }
    )
    assert payload["algo_enabled"] is False
    assert payload["algo_adv_pct_threshold"] == pytest.approx(0.025)
    assert payload["algo_max_volume_rate"] == pytest.approx(0.15)


def test_translate_order_request_includes_algo_fields(temp_modules) -> None:
    _store, broker, translator, _routing, _paper, _exec_mod, _live_mod, _events, _event_bus = temp_modules
    translated = translator.translate_order_request(
        broker.OrderRequest(
            portfolio_id=1,
            ticker="NVDA",
            action="Buy",
            quantity=50,
            order_type="limit",
            limit_price=100.0,
            algo_strategy="VWAP",
            algo_params={"maxPctVol": "0.10"},
        ),
        1001,
    )
    assert translated.algo_strategy == "VWAP"
    assert translated.algo_params == [("maxPctVol", "0.10")]


def test_paper_adapter_algo_buy_applies_slippage(temp_modules, monkeypatch) -> None:
    store, broker, _translator, _routing, paper, _exec_mod, _live_mod, _events, _event_bus = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    monkeypatch.setattr(paper, "_batch_current_prices", lambda tickers: {"NVDA": 100.0})
    adapter = broker.PaperBrokerAdapter(portfolio["id"])
    result = adapter.submit_order(
        broker.OrderRequest(
            portfolio_id=portfolio["id"],
            ticker="NVDA",
            action="Buy",
            quantity=10,
            order_type="limit",
            limit_price=101.0,
            algo_strategy="TWAP",
        )
    )
    assert result.filled_price == pytest.approx(100.05)


def test_paper_adapter_algo_sell_applies_slippage(temp_modules, monkeypatch) -> None:
    store, broker, _translator, _routing, paper, _exec_mod, _live_mod, _events, _event_bus = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    store.open_paper_position(portfolio["id"], "NVDA", 10, 100.0, "2026-03-01T00:00:00+00:00")
    monkeypatch.setattr(paper, "_batch_current_prices", lambda tickers: {"NVDA": 100.0})
    adapter = broker.PaperBrokerAdapter(portfolio["id"])
    result = adapter.submit_order(
        broker.OrderRequest(
            portfolio_id=portfolio["id"],
            ticker="NVDA",
            action="Sell",
            quantity=10,
            order_type="limit",
            limit_price=99.0,
            algo_strategy="VWAP",
        )
    )
    assert result.filled_price == pytest.approx(99.95)


def test_create_trade_plan_persists_algo_strategy(temp_modules) -> None:
    store, _broker, _translator, _routing, _paper, _exec_mod, _live_mod, _events, _event_bus = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox", 100000.0)
    plan = store.create_trade_plan(
        portfolio["id"],
        "NVDA",
        "Buy",
        10,
        "Entry",
        order_type="limit",
        routing_strategy="VWAP Algo (Limit (Ask))",
        algo_strategy="VWAP",
        proposed_price=100.5,
        signal_quality_score=82.0,
        signal_quality_grade="actionable",
        signal_quality_reasons=["fresh", "confirmed"],
    )
    assert plan["algo_strategy"] == "VWAP"
    assert plan["signal_quality_score"] == pytest.approx(82.0)
    assert plan["signal_quality_grade"] == "actionable"
    assert "fresh" in plan["signal_quality_reasons"]


def test_route_routing_settings_put_roundtrip_algo_fields(temp_modules) -> None:
    _store, _broker, _translator, _routing, _paper, _exec_mod, _live_mod, _events, _event_bus = temp_modules
    client = _client()
    response = client.put(
        "/regime/order-routing/settings",
        json={"algo_enabled": True, "algo_adv_pct_threshold": 0.03, "algo_max_volume_rate": 0.12},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["algo_enabled"] is True
    assert payload["algo_adv_pct_threshold"] == pytest.approx(0.03)
    assert payload["algo_max_volume_rate"] == pytest.approx(0.12)


def test_route_routing_diagnostic_includes_algo_analysis(temp_modules, monkeypatch) -> None:
    _store, _broker, _translator, routing, _paper, _exec_mod, _live_mod, _events, _event_bus = temp_modules
    market_data_client = importlib.import_module("src.regime.market_data_client")
    monkeypatch.setattr(routing, "download_daily_bars", lambda ticker, period="1mo", auto_adjust=False: pd.DataFrame({"Volume": [2_000_000.0] * 20}))
    monkeypatch.setattr(market_data_client, "download_daily_bars", lambda ticker, period="5d", auto_adjust=False: pd.DataFrame({"Close": [100.0, 101.0]}))
    client = _client()
    response = client.get("/regime/order-routing/NVDA")
    assert response.status_code == 200
    payload = response.json()
    assert "algo_analysis" in payload
    assert "algo_strategy" in payload["buy_routing"]
    assert "algo_params" in payload["sell_routing"]


def test_generate_buy_plans_persists_algo_strategy(temp_modules, monkeypatch) -> None:
    store, _broker, _translator, routing, paper, _exec_mod, _live_mod, _events, _event_bus = temp_modules
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
    store.save_signal_snapshot(
        ticker="NVDA",
        snapshot_date=dt.datetime.now(dt.timezone.utc).date().isoformat(),
        action="Buy",
        regime_label="Bull",
        regime_probability=0.90,
        composite_strength=0.80,
        benchmark="SPY",
        current_price=100.0,
        entry_price=100.0,
        exit_price=110.0,
        stop_price=98.0,
        risk_reward_ratio=5.0,
        timeframe_days=21,
        expected_regime_duration=30.0,
    )
    monkeypatch.setattr(routing, "compute_adv", lambda ticker, lookback_days=20: 5_000.0)
    monkeypatch.setattr(paper, "_batch_current_prices", lambda tickers: {"NVDA": 100.0})
    monkeypatch.setattr(paper, "_lookup_atr", lambda ticker: 1.0)
    monkeypatch.setattr(paper, "_lookup_beta", lambda ticker: 1.0)
    plans = paper.generate_buy_plans(portfolio["id"])
    assert plans[0]["algo_strategy"] in {"TWAP", "VWAP"}


def test_execution_agent_includes_algo_fields(temp_modules, monkeypatch) -> None:
    _store, broker, _translator, _routing, _paper, exec_mod, _live_mod, events, event_bus = temp_modules
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
            order_type="limit",
            time_in_force="DAY",
            limit_price=101.25,
            strategy_name="VWAP Algo (Limit (Ask))",
            rationale="algo path",
            adv=2_000_000.0,
            adv_bucket="high_liquidity",
            urgency=str(kwargs.get("urgency") or "normal"),
            algo_strategy="VWAP",
            algo_params={"maxPctVol": "0.10"},
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
            action="Buy",
            decision="approved",
            quantity=10.0,
            proposed_price=101.0,
            urgency="normal",
            source="discovery",
        ),
    )
    order = captured["order"]
    assert isinstance(order, broker.OrderRequest)
    assert order.algo_strategy == "VWAP"
    assert order.algo_params == {"maxPctVol": "0.10"}
    assert result.algo_strategy == "VWAP"


def test_live_backend_sets_algo_strategy_and_params(temp_modules, monkeypatch) -> None:
    _store, _broker, _translator, _routing, _paper, _exec_mod, live_mod, _events, _event_bus = temp_modules

    class FakeThread:
        def run(self, fn, timeout=None):
            del timeout
            value = fn()
            if inspect.isawaitable(value):
                return asyncio.run(value)
            return value

    class FakeContract:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    class FakeLimitOrder:
        def __init__(self, action, quantity, limit_price):
            self.action = action
            self.totalQuantity = quantity
            self.lmtPrice = limit_price
            self.outsideRth = False
            self.tif = "DAY"
            self.algoStrategy = None
            self.algoParams = None

    class FakeMarketOrder(FakeLimitOrder):
        def __init__(self, action, quantity):
            super().__init__(action, quantity, None)

    class FakeStopOrder(FakeLimitOrder):
        def __init__(self, action, quantity, stop_price):
            super().__init__(action, quantity, None)
            self.auxPrice = stop_price

    class FakeTagValue:
        def __init__(self, tag, value):
            self.tag = tag
            self.value = value

    class FakeIB:
        def __init__(self):
            self.placed_order = None

        async def qualifyContractsAsync(self, contract):
            self.contract = contract

        def placeOrder(self, contract, order):
            self.placed_order = order
            return SimpleNamespace(
                order=order,
                orderStatus=SimpleNamespace(status="Submitted", avgFillPrice=0.0, filled=0.0, remaining=float(getattr(order, "totalQuantity", 0.0) or 0.0), whyHeld=""),
            )

    fake_ib_module = SimpleNamespace(
        Contract=FakeContract,
        LimitOrder=FakeLimitOrder,
        MarketOrder=FakeMarketOrder,
        StopOrder=FakeStopOrder,
        TagValue=FakeTagValue,
    )
    monkeypatch.setitem(sys.modules, "ib_insync", fake_ib_module)
    monkeypatch.setattr(live_mod, "get_ib_thread", lambda: FakeThread())
    backend = live_mod.LiveIBBackend(account_id="DUP579027")
    backend._ib = FakeIB()

    from src.regime.ib_types import IBOrder, IBOrderAction, IBOrderType

    backend.place_order(
        IBOrder(
            order_id=1,
            contract_symbol="NVDA",
            action=IBOrderAction.BUY,
            order_type=IBOrderType.LIMIT,
            quantity=100,
            limit_price=100.0,
            algo_strategy="TWAP",
            algo_params=[("maxPctVol", "0.10")],
        )
    )

    assert backend._ib.placed_order is not None
    assert backend._ib.placed_order.algoStrategy == "Twap"
    assert backend._ib.placed_order.algoParams[0].tag == "maxPctVol"
    assert backend._ib.placed_order.algoParams[0].value == "0.10"


def test_live_backend_status_falls_back_to_execution_report(temp_modules, monkeypatch) -> None:
    _store, _broker, _translator, _routing, _paper, _exec_mod, live_mod, _events, _event_bus = temp_modules

    class FakeThread:
        def run(self, fn, timeout=None):
            del timeout
            value = fn()
            if inspect.isawaitable(value):
                return asyncio.run(value)
            return value

    class FakeExecutionFilter:
        def __init__(self):
            self.acctCode = ""
            self.clientId = 0
            self.time = ""

    class FakeIB:
        def openTrades(self):
            return []

        def trades(self):
            return []

        async def reqAllOpenOrdersAsync(self):
            return []

        async def reqCompletedOrdersAsync(self, apiOnly=False):
            del apiOnly
            return []

        async def reqExecutionsAsync(self, query):
            assert query.acctCode == "DUP579027"
            assert query.clientId == 27
            return [
                SimpleNamespace(
                    contract=SimpleNamespace(symbol="AVGO"),
                    execution=SimpleNamespace(
                        orderId=43,
                        shares=5.0,
                        price=486.30,
                        avgPrice=486.30,
                        time=pd.Timestamp("2026-06-03T19:21:47Z").to_pydatetime(),
                    ),
                )
            ]

    fake_ib_module = SimpleNamespace(ExecutionFilter=FakeExecutionFilter)
    monkeypatch.setitem(sys.modules, "ib_insync", fake_ib_module)
    monkeypatch.setattr(live_mod, "get_ib_thread", lambda: FakeThread())
    backend = live_mod.LiveIBBackend(account_id="DUP579027")
    backend._ib = FakeIB()
    backend._client_id = 27

    status = backend.get_order_status(43)

    assert status.status.value == "Filled"
    assert status.filled_qty == pytest.approx(5.0)
    assert status.avg_fill_price == pytest.approx(486.30)
    assert status.message == "Filled from IBKR execution report."
