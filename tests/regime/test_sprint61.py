from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pandas as pd
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from _fixtures import FakeRegime
from src.app.routes import regime as regime_route
from src.regime.agents import AgentBase, get_agent_registry, reset_agent_registry
from src.regime.agents.execution_agent import ExecutionAgent
from src.regime.agents.fundamental_agent import FundamentalAgent
from src.regime.agents.orchestrator import AgentOrchestrator, OrchestratorConfig
from src.regime.agents.portfolio_agent import PortfolioTaxAgent
from src.regime.agents.quant_agent import QuantAgent
from src.regime.ensemble import AnalystResult, EnsembleConfig
from src.regime.event_bus import AsyncEventBus, get_event_bus, register_default_subscribers, reset_event_bus
from src.regime.events import (
    AnalysisRequestEvent,
    BaseEvent,
    EnrichedSignalEvent,
    FundamentalAssessmentEvent,
    OrderExecutionEvent,
    TradeDecisionEvent,
)
from src.regime.signals import CompositeSignal, ConfidenceScore, PriceTargets, SignalResult


@pytest.fixture(autouse=True)
def reset_state() -> None:
    reset_event_bus()
    reset_agent_registry()
    yield
    reset_event_bus()
    reset_agent_registry()


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(regime_route.router)
    app.dependency_overrides[regime_route.require_actor] = lambda: "tester"
    app.dependency_overrides[regime_route.db_session] = lambda: iter([None])
    return TestClient(app)


def _composite(action: str = "Buy") -> CompositeSignal:
    return CompositeSignal(
        regime_signal="Bull detected",
        regime_probability=0.91,
        forward_signal=SignalResult(action=action, timeframe="short", strength=0.72, expected_holding_days=10, rationale="test"),
        technical_signal="Buy the dip",
        composite_action=action,
        composite_strength=0.81,
        short_term_view="short",
        medium_term_view="medium",
    )


def _price_targets() -> PriceTargets:
    return PriceTargets(
        current_price=101.0,
        entry_price=100.0,
        exit_price=120.0,
        stop_price=95.0,
        risk_reward_ratio=2.0,
        timeframe_days=10,
        atr_value=3.0,
        confidence_multiplier=1.0,
        price_position="In target range",
    )


def _confidence() -> ConfidenceScore:
    return ConfidenceScore(value=78.5, label="High", calibrated=True, components={"regime_probability": 91.0})


def _runtime() -> dict[str, object]:
    market_frame = pd.DataFrame(
        {
            "price": [100.0, 101.0],
            "volume": [1_000_000.0, 1_100_000.0],
            "high": [101.0, 102.0],
            "low": [99.0, 100.0],
        }
    )
    regime = FakeRegime(
        "NVDA",
        "Bull",
        latest_price=101.0,
        price_frame=pd.DataFrame({"state_probability": [0.80, 0.84, 0.91]}),
    )

    class Registry:
        def list_analysts(self):
            return ["xgboost_meta_labeler"]

        def get(self, name):
            if name != "xgboost_meta_labeler":
                return None
            return SimpleNamespace(
                name="xgboost_meta_labeler",
                is_ready=lambda: True,
                analyze=lambda ticker, features, regime_result: AnalystResult(
                    analyst_name="xgboost_meta_labeler",
                    confidence=0.74,
                    signal="confirm",
                    details={"ticker": ticker, "feature_count": len(features)},
                ),
            )

    class FakeOrderRequest:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    return {
        "download_market_frame": lambda **kwargs: SimpleNamespace(frame=market_frame.copy()),
        "fit_regime_model": lambda ticker, market_frame: regime,
        "get_next_earnings_date": lambda ticker: None,
        "forward_regime_curve": lambda *args, **kwargs: pd.DataFrame({"day": [1, 2], "p_bull": [0.7, 0.72], "p_neutral": [0.2, 0.18], "p_bear": [0.1, 0.1]}),
        "signal_from_forward_curve": lambda *args, **kwargs: SignalResult(action="Buy", timeframe="short", strength=0.72, expected_holding_days=10, rationale="test"),
        "compute_technicals": lambda *args, **kwargs: pd.DataFrame({"rsi_14": [45.0, 50.0], "bb_pct": [0.4, 0.5], "macd_histogram": [0.1, 0.2], "atr_14": [2.8, 3.0]}),
        "intra_regime_signal": lambda *args, **kwargs: "Buy the dip",
        "build_composite_signal": lambda *args, **kwargs: _composite(),
        "compute_price_targets": lambda **kwargs: _price_targets(),
        "compute_unified_confidence": lambda *args, **kwargs: _confidence(),
        "get_registry": lambda: Registry(),
        "extract_meta_features": lambda row: {"feat": float(row["state_probability"])},
        "aggregate_analysts": lambda results, config: SimpleNamespace(signal="confirm", composite_confidence=0.74, sizing_multiplier=1.0, veto_reason=None),
        "EnsembleConfig": EnsembleConfig,
        "get_setting": lambda key: None,
        "build_qualitative_assessment": lambda **kwargs: SimpleNamespace(
            ticker=kwargs["ticker"],
            catalyst_sentiment="Positive",
            catalysts=[{"headline": "Positive catalyst"}],
            llm_response={"institutional_report": {"verdict": "Buy", "confidence_score": 7}},
            source="llm",
        ),
        "list_paper_portfolios": lambda include_closed=False: [
            {"id": 1, "status": "Active", "current_cash": 100000.0, "starting_budget": 100000.0},
        ],
        "is_wash_sale_restricted": lambda portfolio_id, ticker: False,
        "get_paper_positions": lambda portfolio_id, status="Open": [{"ticker": "NVDA", "quantity": 5.0}] if status == "Open" else [],
        "PaperBrokerAdapter": lambda portfolio_id: SimpleNamespace(portfolio_id=int(portfolio_id)),
        "OrderRequest": FakeOrderRequest,
        "DEFAULT_RISK_GUARDRAILS": object(),
        "submit_guarded_order": lambda order, adapter, guardrails, actor="agent": (
            SimpleNamespace(allowed=True, checks=[]),
            SimpleNamespace(order_id="ord-1", status="filled", quantity=order.quantity, filled_price=101.0, filled_at="2026-03-30T12:00:00+00:00", message="ok"),
        ),
    }


class _DummyAgent(AgentBase):
    def __init__(self, bus: AsyncEventBus, **kwargs):
        super().__init__(bus, **kwargs)
        self.calls = 0

    @property
    def name(self) -> str:
        return "dummy"

    @property
    def subscriptions(self) -> list[str]:
        return ["analysis_request"]

    async def handle(self, event: BaseEvent) -> None:
        self.calls += 1


def test_agent_base_register_subscribes_to_bus() -> None:
    bus = AsyncEventBus()
    agent = _DummyAgent(bus)
    assert bus.subscriber_count("analysis_request") == 0
    agent.register()
    assert bus.subscriber_count("analysis_request") == 1


def test_agent_dispatch_skips_when_disabled() -> None:
    bus = AsyncEventBus()
    agent = _DummyAgent(bus, enabled=False)
    agent.register()
    asyncio.run(bus.publish(AnalysisRequestEvent(tickers=("NVDA",))))
    assert agent.calls == 0


def test_agent_registry_status() -> None:
    bus = AsyncEventBus()
    registry = get_agent_registry()
    registry.register(_DummyAgent(bus))
    registry.register(QuantAgent(bus, runtime=_runtime()))
    statuses = registry.status()
    assert [item["name"] for item in statuses] == ["dummy", "quant"]
    assert statuses[0]["subscriptions"] == ["analysis_request"]


def test_analysis_request_event_fields() -> None:
    event = AnalysisRequestEvent(tickers=("NVDA", "AVGO"), benchmark="SOXX", period="3y", requested_by="scheduler")
    payload = event.to_dict()
    assert payload["event_type"] == "analysis_request"
    assert payload["tickers"] == ["NVDA", "AVGO"]


def test_fundamental_assessment_event_fields() -> None:
    event = FundamentalAssessmentEvent(ticker="NVDA", verdict="Buy", confidence_score=7)
    payload = event.to_dict()
    assert payload["event_type"] == "fundamental_assessment"
    assert payload["ticker"] == "NVDA"


def test_trade_decision_event_fields() -> None:
    event = TradeDecisionEvent(ticker="NVDA", portfolio_id=1, action="Buy", decision="approved", quantity=10.0)
    payload = event.to_dict()
    assert payload["event_type"] == "trade_decision"
    assert payload["portfolio_id"] == 1


def test_order_execution_event_fields() -> None:
    event = OrderExecutionEvent(ticker="NVDA", portfolio_id=1, action="Buy", status="filled")
    payload = event.to_dict()
    assert payload["event_type"] == "order_execution"
    assert payload["status"] == "filled"


def test_quant_agent_publishes_enriched_signal() -> None:
    bus = AsyncEventBus()
    seen: list[EnrichedSignalEvent] = []

    async def capture(event: BaseEvent) -> None:
        if isinstance(event, EnrichedSignalEvent):
            seen.append(event)

    bus.subscribe("enriched_signal", capture)
    agent = QuantAgent(bus, runtime=_runtime())
    agent.register()
    asyncio.run(bus.publish(AnalysisRequestEvent(tickers=("NVDA",), benchmark="SOXX")))
    assert len(seen) == 1
    assert seen[0].ticker == "NVDA"


def test_quant_agent_skips_without_runtime() -> None:
    bus = AsyncEventBus()
    seen: list[BaseEvent] = []

    async def capture(event: BaseEvent) -> None:
        seen.append(event)

    bus.subscribe("enriched_signal", capture)
    agent = QuantAgent(bus)
    agent.register()
    asyncio.run(bus.publish(AnalysisRequestEvent(tickers=("NVDA",))))
    assert seen == []


def test_quant_agent_handles_hmm_failure() -> None:
    bus = AsyncEventBus()
    seen: list[BaseEvent] = []
    runtime = _runtime()
    runtime["fit_regime_model"] = lambda ticker, market_frame: (_ for _ in ()).throw(RuntimeError("boom"))

    async def capture(event: BaseEvent) -> None:
        seen.append(event)

    bus.subscribe("enriched_signal", capture)
    agent = QuantAgent(bus, runtime=runtime)
    agent.register()
    asyncio.run(bus.publish(AnalysisRequestEvent(tickers=("NVDA",))))
    assert seen == []


def test_quant_agent_multiple_tickers() -> None:
    bus = AsyncEventBus()
    seen: list[EnrichedSignalEvent] = []

    async def capture(event: BaseEvent) -> None:
        if isinstance(event, EnrichedSignalEvent):
            seen.append(event)

    bus.subscribe("enriched_signal", capture)
    agent = QuantAgent(bus, runtime=_runtime())
    agent.register()
    asyncio.run(bus.publish(AnalysisRequestEvent(tickers=("A", "B"))))
    assert [item.ticker for item in seen] == ["A", "B"]


def test_quant_agent_source_is_quant_agent() -> None:
    bus = AsyncEventBus()
    seen: list[EnrichedSignalEvent] = []

    async def capture(event: BaseEvent) -> None:
        if isinstance(event, EnrichedSignalEvent):
            seen.append(event)

    bus.subscribe("enriched_signal", capture)
    agent = QuantAgent(bus, runtime=_runtime())
    agent.register()
    asyncio.run(bus.publish(AnalysisRequestEvent(tickers=("NVDA",))))
    assert seen[0].source == "quant_agent"


def test_fundamental_agent_publishes_assessment() -> None:
    bus = AsyncEventBus()
    seen: list[FundamentalAssessmentEvent] = []

    async def capture(event: BaseEvent) -> None:
        if isinstance(event, FundamentalAssessmentEvent):
            seen.append(event)

    bus.subscribe("fundamental_assessment", capture)
    agent = FundamentalAgent(bus, runtime=_runtime())
    asyncio.run(agent.handle(EnrichedSignalEvent(ticker="NVDA", source="quant_agent", regime_label="Bull", regime_probability=0.8, composite_action="Buy", meta_labeler_score=0.6, benchmark="SOXX")))
    assert len(seen) == 1
    assert seen[0].verdict == "Buy"


def test_fundamental_agent_veto_on_low_ml_score() -> None:
    bus = AsyncEventBus()
    seen: list[FundamentalAssessmentEvent] = []
    runtime = _runtime()
    called = {"llm": 0}

    def build_qualitative_assessment(**kwargs):
        called["llm"] += 1
        return SimpleNamespace(catalyst_sentiment="Positive", catalysts=[], llm_response={}, source="llm")

    runtime["build_qualitative_assessment"] = build_qualitative_assessment

    async def capture(event: BaseEvent) -> None:
        if isinstance(event, FundamentalAssessmentEvent):
            seen.append(event)

    bus.subscribe("fundamental_assessment", capture)
    agent = FundamentalAgent(bus, runtime=runtime)
    asyncio.run(agent.handle(EnrichedSignalEvent(ticker="NVDA", source="quant_agent", regime_label="Bull", regime_probability=0.8, composite_action="Buy", meta_labeler_score=0.2)))
    assert len(seen) == 1
    assert seen[0].vetoed is True
    assert called["llm"] == 0


def test_fundamental_agent_skips_non_agent_source() -> None:
    bus = AsyncEventBus()
    seen: list[BaseEvent] = []

    async def capture(event: BaseEvent) -> None:
        seen.append(event)

    bus.subscribe("fundamental_assessment", capture)
    agent = FundamentalAgent(bus, runtime=_runtime())
    agent.register()
    asyncio.run(bus.publish(EnrichedSignalEvent(ticker="NVDA", source="dashboard")))
    assert seen == []


def test_fundamental_agent_handles_llm_failure() -> None:
    bus = AsyncEventBus()
    seen: list[BaseEvent] = []
    runtime = _runtime()
    runtime["build_qualitative_assessment"] = lambda **kwargs: (_ for _ in ()).throw(RuntimeError("llm failed"))

    async def capture(event: BaseEvent) -> None:
        seen.append(event)

    bus.subscribe("fundamental_assessment", capture)
    agent = FundamentalAgent(bus, runtime=runtime)
    agent.register()
    asyncio.run(bus.publish(EnrichedSignalEvent(ticker="NVDA", source="quant_agent", regime_label="Bull", regime_probability=0.8, composite_action="Buy", meta_labeler_score=0.6)))
    assert seen == []


def test_portfolio_agent_approves_buy() -> None:
    bus = AsyncEventBus()
    seen: list[TradeDecisionEvent] = []

    async def capture(event: BaseEvent) -> None:
        if isinstance(event, TradeDecisionEvent):
            seen.append(event)

    bus.subscribe("trade_decision", capture)
    agent = PortfolioTaxAgent(bus, runtime=_runtime())
    asyncio.run(agent.handle(EnrichedSignalEvent(ticker="NVDA", source="quant_agent", composite_action="Buy", composite_strength=0.8, current_price=100.0, ensemble_sizing_multiplier=1.0, regime_label="Bull", meta_labeler_score=0.7)))
    assert len(seen) == 1
    assert seen[0].decision == "approved"


def test_portfolio_agent_vetoes_wash_sale() -> None:
    bus = AsyncEventBus()
    seen: list[TradeDecisionEvent] = []
    runtime = _runtime()
    runtime["is_wash_sale_restricted"] = lambda portfolio_id, ticker: True

    async def capture(event: BaseEvent) -> None:
        if isinstance(event, TradeDecisionEvent):
            seen.append(event)

    bus.subscribe("trade_decision", capture)
    agent = PortfolioTaxAgent(bus, runtime=runtime)
    asyncio.run(agent.handle(EnrichedSignalEvent(ticker="NVDA", source="quant_agent", composite_action="Buy", composite_strength=0.8, current_price=100.0)))
    assert seen[0].decision == "vetoed"
    assert seen[0].veto_reason == "wash_sale_restricted"


def test_portfolio_agent_skips_hold_signals() -> None:
    bus = AsyncEventBus()
    seen: list[BaseEvent] = []

    async def capture(event: BaseEvent) -> None:
        seen.append(event)

    bus.subscribe("trade_decision", capture)
    agent = PortfolioTaxAgent(bus, runtime=_runtime())
    agent.register()
    asyncio.run(bus.publish(EnrichedSignalEvent(ticker="NVDA", source="quant_agent", composite_action="Hold")))
    assert seen == []


def test_portfolio_agent_skips_non_agent_source() -> None:
    bus = AsyncEventBus()
    seen: list[BaseEvent] = []

    async def capture(event: BaseEvent) -> None:
        seen.append(event)

    bus.subscribe("trade_decision", capture)
    agent = PortfolioTaxAgent(bus, runtime=_runtime())
    agent.register()
    asyncio.run(bus.publish(EnrichedSignalEvent(ticker="NVDA", source="regime_analysis", composite_action="Buy")))
    assert seen == []


def test_execution_agent_fills_order() -> None:
    bus = AsyncEventBus()
    seen: list[OrderExecutionEvent] = []

    async def capture(event: BaseEvent) -> None:
        if isinstance(event, OrderExecutionEvent):
            seen.append(event)

    bus.subscribe("order_execution", capture)
    agent = ExecutionAgent(bus, runtime=_runtime())
    agent.register()
    asyncio.run(bus.publish(TradeDecisionEvent(ticker="NVDA", portfolio_id=1, action="Buy", decision="approved", quantity=10.0, proposed_price=100.0)))
    assert len(seen) == 1
    assert seen[0].status == "filled"


def test_execution_agent_skips_vetoed() -> None:
    bus = AsyncEventBus()
    seen: list[BaseEvent] = []

    async def capture(event: BaseEvent) -> None:
        seen.append(event)

    bus.subscribe("order_execution", capture)
    agent = ExecutionAgent(bus, runtime=_runtime())
    agent.register()
    asyncio.run(bus.publish(TradeDecisionEvent(ticker="NVDA", portfolio_id=1, action="Buy", decision="vetoed", quantity=10.0)))
    assert seen == []


def test_execution_agent_handles_broker_failure() -> None:
    bus = AsyncEventBus()
    seen: list[OrderExecutionEvent] = []
    runtime = _runtime()
    runtime["submit_guarded_order"] = lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("broker failed"))

    async def capture(event: BaseEvent) -> None:
        if isinstance(event, OrderExecutionEvent):
            seen.append(event)

    bus.subscribe("order_execution", capture)
    agent = ExecutionAgent(bus, runtime=runtime)
    agent.register()
    asyncio.run(bus.publish(TradeDecisionEvent(ticker="NVDA", portfolio_id=1, action="Buy", decision="approved", quantity=10.0)))
    assert len(seen) == 1
    assert seen[0].status == "rejected"


def test_full_agent_pipeline_end_to_end(monkeypatch) -> None:
    bus = get_event_bus()
    register_default_subscribers(bus)
    runtime = _runtime()
    created_plans: list[dict[str, object]] = []
    monkeypatch.setattr("src.regime.event_subscribers.save_alert", lambda *args, **kwargs: {"ok": True})
    def fake_create_trade_plan(*args, **kwargs):
        payload = dict(kwargs)
        if args:
            payload.setdefault("portfolio_id", args[0] if len(args) > 0 else None)
            payload.setdefault("ticker", args[1] if len(args) > 1 else None)
            payload.setdefault("action", args[2] if len(args) > 2 else None)
            payload.setdefault("quantity", args[3] if len(args) > 3 else None)
            payload.setdefault("rationale", args[4] if len(args) > 4 else None)
        created_plans.append(payload)
        return {"id": 1, **payload}

    monkeypatch.setattr("src.regime.persistence.create_trade_plan", fake_create_trade_plan)
    registry = get_agent_registry()
    registry.register(QuantAgent(bus, runtime=runtime))
    registry.register(FundamentalAgent(bus, runtime=runtime))
    registry.register(PortfolioTaxAgent(bus, runtime=runtime))
    registry.register(ExecutionAgent(bus, runtime=runtime))
    registry.register(AgentOrchestrator(bus, config=OrchestratorConfig()))

    seen: dict[str, list[BaseEvent]] = {"enriched": [], "fundamental": [], "decision": [], "execution": []}

    async def capture(event: BaseEvent) -> None:
        if isinstance(event, EnrichedSignalEvent):
            seen["enriched"].append(event)
        elif isinstance(event, FundamentalAssessmentEvent):
            seen["fundamental"].append(event)
        elif isinstance(event, TradeDecisionEvent):
            seen["decision"].append(event)
        elif isinstance(event, OrderExecutionEvent):
            seen["execution"].append(event)

    for name in ("enriched_signal", "fundamental_assessment", "trade_decision", "order_execution"):
        bus.subscribe(name, capture)

    request = AnalysisRequestEvent(tickers=("NVDA",), benchmark="SOXX", requested_by="scheduler")
    asyncio.run(bus.publish(request))

    assert len(seen["enriched"]) == 1
    assert len(seen["fundamental"]) == 1
    assert len(seen["decision"]) == 1
    assert len(seen["execution"]) == 1
    assert created_plans
    assert seen["enriched"][0].correlation_id == request.correlation_id
    assert seen["fundamental"][0].correlation_id == request.correlation_id
    assert seen["decision"][0].correlation_id == request.correlation_id
    assert seen["execution"][0].correlation_id == request.correlation_id


def test_agent_startup_registration() -> None:
    bus = get_event_bus()
    register_default_subscribers(bus)
    registry = get_agent_registry()
    runtime = _runtime()
    registry.register(QuantAgent(bus, runtime=runtime))
    registry.register(FundamentalAgent(bus, runtime=runtime))
    registry.register(PortfolioTaxAgent(bus, runtime=runtime))
    registry.register(ExecutionAgent(bus, runtime=runtime))
    assert len(registry.all_agents()) == 4
    assert [item["name"] for item in registry.status()] == ["quant", "fundamental", "portfolio_tax", "execution"]


def test_agent_status_route() -> None:
    bus = get_event_bus()
    registry = get_agent_registry()
    runtime = _runtime()
    registry.register(QuantAgent(bus, runtime=runtime))
    registry.register(FundamentalAgent(bus, runtime=runtime))
    registry.register(PortfolioTaxAgent(bus, runtime=runtime))
    registry.register(ExecutionAgent(bus, runtime=runtime))

    client = _client()
    response = client.get("/regime/agents/status")
    assert response.status_code == 200
    payload = response.json()
    assert payload["agent_count"] == 4
    assert len(payload["agents"]) == 4
