from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from _fixtures import FakeRegime
from src.app.routes import regime as regime_route
from src.regime import event_subscribers as subscribers_module
from src.regime import persistence as persistence_module
from src.regime.agents import get_agent_registry, reset_agent_registry
from src.regime.agents.execution_agent import ExecutionAgent
from src.regime.agents.fundamental_agent import FundamentalAgent
from src.regime.agents.orchestrator import AgentOrchestrator, OrchestratorConfig
from src.regime.agents.portfolio_agent import PortfolioTaxAgent
from src.regime.agents.quant_agent import QuantAgent
from src.regime.event_bus import AsyncEventBus, get_event_bus, reset_event_bus
from src.regime.events import (
    AnalysisRequestEvent,
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


def _event() -> EnrichedSignalEvent:
    return EnrichedSignalEvent(
        ticker="NVDA",
        source="quant_agent",
        benchmark="SOXX",
        regime_label="Bull",
        regime_probability=0.91,
        composite_action="Buy",
        composite_strength=0.82,
        current_price=101.0,
        unified_confidence=78.5,
        meta_labeler_score=0.74,
        ensemble_sizing_multiplier=1.0,
    )


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
                analyze=lambda ticker, features, regime_result: SimpleNamespace(
                    analyst_name="xgboost_meta_labeler",
                    confidence=0.74,
                    signal="confirm",
                    details={"ticker": ticker},
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
        "build_composite_signal": lambda *args, **kwargs: CompositeSignal(
            regime_signal="Bull detected",
            regime_probability=0.91,
            forward_signal=SignalResult(action="Buy", timeframe="short", strength=0.72, expected_holding_days=10, rationale="test"),
            technical_signal="Buy the dip",
            composite_action="Buy",
            composite_strength=0.81,
            short_term_view="short",
            medium_term_view="medium",
        ),
        "compute_price_targets": lambda **kwargs: PriceTargets(
            current_price=101.0,
            entry_price=100.0,
            exit_price=120.0,
            stop_price=95.0,
            risk_reward_ratio=2.0,
            timeframe_days=10,
            atr_value=3.0,
            confidence_multiplier=1.0,
            price_position="In target range",
        ),
        "compute_unified_confidence": lambda *args, **kwargs: ConfidenceScore(value=78.5, label="High", calibrated=True, components={"regime_probability": 91.0}),
        "get_registry": lambda: Registry(),
        "extract_meta_features": lambda row: {"feat": float(row["state_probability"])},
        "aggregate_analysts": lambda results, config: SimpleNamespace(signal="confirm", composite_confidence=0.74, sizing_multiplier=1.0, veto_reason=None),
        "EnsembleConfig": SimpleNamespace,
        "get_setting": lambda key: "false" if key == "fundamental_gate_enabled" else None,
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


def test_fundamental_and_portfolio_agents_unsubscribed_from_enriched_signal() -> None:
    bus = AsyncEventBus()
    assert FundamentalAgent(bus).subscriptions == []
    assert PortfolioTaxAgent(bus).subscriptions == []


def test_orchestrator_sequences_fundamental_then_portfolio(monkeypatch: pytest.MonkeyPatch) -> None:
    bus = AsyncEventBus()
    registry = get_agent_registry()
    fundamental = FundamentalAgent(bus, runtime={})
    portfolio = PortfolioTaxAgent(bus, runtime={})
    orchestrator = AgentOrchestrator(bus)
    order: list[str] = []
    seen_fundamental: list[FundamentalAssessmentEvent] = []
    seen_decisions: list[TradeDecisionEvent] = []

    async def fake_fundamental(event: EnrichedSignalEvent) -> FundamentalAssessmentEvent:
        order.append("fundamental")
        return FundamentalAssessmentEvent(
            correlation_id=event.correlation_id,
            ticker=event.ticker,
            verdict="Buy",
            source="llm",
            enriched_signal_id=event.correlation_id,
        )

    async def fake_portfolio(event: EnrichedSignalEvent, assessment: FundamentalAssessmentEvent | None) -> list[TradeDecisionEvent]:
        assert order == ["fundamental"]
        assert assessment is not None
        order.append("portfolio")
        return [
            TradeDecisionEvent(
                correlation_id=event.correlation_id,
                ticker=event.ticker,
                portfolio_id=1,
                action="Buy",
                decision="approved",
                quantity=10.0,
                proposed_price=event.current_price,
                source=event.source,
                regime_label=event.regime_label,
                meta_labeler_score=event.meta_labeler_score,
                sizing_rationale="allocation=0.10",
                enriched_signal_id=event.correlation_id,
            )
        ]

    monkeypatch.setattr(fundamental, "run_for_orchestrator", fake_fundamental)
    monkeypatch.setattr(portfolio, "run_for_orchestrator", fake_portfolio)
    registry.register(fundamental)
    registry.register(portfolio)
    registry.register(orchestrator)

    async def capture_fundamental(event):
        if isinstance(event, FundamentalAssessmentEvent):
            seen_fundamental.append(event)

    async def capture_decision(event):
        if isinstance(event, TradeDecisionEvent):
            seen_decisions.append(event)

    bus.subscribe("fundamental_assessment", capture_fundamental)
    bus.subscribe("trade_decision", capture_decision)
    asyncio.run(bus.publish(_event()))

    assert order == ["fundamental", "portfolio"]
    assert len(seen_fundamental) == 1
    assert len(seen_decisions) == 1
    assert "[agents:" in str(seen_decisions[0].sizing_rationale)


def test_orchestrator_fundamental_timeout_proceeds_quant_only(monkeypatch: pytest.MonkeyPatch) -> None:
    bus = AsyncEventBus()
    registry = get_agent_registry()
    fundamental = FundamentalAgent(bus, runtime={})
    portfolio = PortfolioTaxAgent(bus, runtime={})
    orchestrator = AgentOrchestrator(bus, config=OrchestratorConfig(fundamental_timeout_seconds=0.01))
    seen_fundamental: list[FundamentalAssessmentEvent] = []
    portfolio_inputs: list[FundamentalAssessmentEvent | None] = []

    async def slow_fundamental(_event: EnrichedSignalEvent) -> FundamentalAssessmentEvent | None:
        await asyncio.sleep(0.05)
        return None

    async def fake_portfolio(event: EnrichedSignalEvent, assessment: FundamentalAssessmentEvent | None) -> list[TradeDecisionEvent]:
        portfolio_inputs.append(assessment)
        return [
            TradeDecisionEvent(
                correlation_id=event.correlation_id,
                ticker=event.ticker,
                portfolio_id=1,
                action="Buy",
                decision="approved",
                quantity=5.0,
                source=event.source,
                regime_label=event.regime_label,
                sizing_rationale="ok",
                enriched_signal_id=event.correlation_id,
            )
        ]

    monkeypatch.setattr(fundamental, "run_for_orchestrator", slow_fundamental)
    monkeypatch.setattr(portfolio, "run_for_orchestrator", fake_portfolio)
    registry.register(fundamental)
    registry.register(portfolio)
    registry.register(orchestrator)

    async def capture_fundamental(event):
        if isinstance(event, FundamentalAssessmentEvent):
            seen_fundamental.append(event)

    bus.subscribe("fundamental_assessment", capture_fundamental)
    asyncio.run(bus.publish(_event()))

    assert len(seen_fundamental) == 1
    assert seen_fundamental[0].verdict == "timeout"
    assert portfolio_inputs and portfolio_inputs[0] is not None
    assert portfolio_inputs[0].verdict == "timeout"


def test_trade_decision_subscriber_extracts_agent_trace(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def fake_create_trade_plan(**kwargs):
        captured.update(kwargs)
        return {"id": 1}

    monkeypatch.setattr(persistence_module, "create_trade_plan", fake_create_trade_plan)
    asyncio.run(
        subscribers_module.trade_decision_subscriber(
            TradeDecisionEvent(
                ticker="NVDA",
                portfolio_id=1,
                action="Buy",
                decision="approved",
                quantity=10.0,
                sizing_rationale="allocation=0.10 [agents: quant:signal=Buy | portfolio:decision=approved]",
            )
        )
    )
    assert captured["rationale"] == "allocation=0.10"
    assert captured["agent_trace"] == "[agents: quant:signal=Buy | portfolio:decision=approved]"


def test_agent_trace_column_and_persistence(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(persistence_module, "DB_PATH", tmp_path / "regime_watch.db")
    plan = persistence_module.create_trade_plan(
        portfolio_id=persistence_module.create_paper_portfolio("Sandbox")["id"],
        ticker="NVDA",
        action="Buy",
        quantity=10.0,
        rationale="allocation=0.10",
        agent_trace="[agents: quant:signal=Buy | portfolio:decision=approved]",
    )
    assert plan["agent_trace"] == "[agents: quant:signal=Buy | portfolio:decision=approved]"
    with persistence_module._connect() as conn:
        columns = {str(row["name"]) for row in conn.execute("PRAGMA table_info(paper_trade_plan)").fetchall()}
    assert "agent_trace" in columns


def test_orchestrator_settings_routes() -> None:
    bus = AsyncEventBus()
    registry = get_agent_registry()
    registry.register(AgentOrchestrator(bus, config=OrchestratorConfig()))
    client = _client()
    response = client.get("/regime/orchestrator/settings")
    assert response.status_code == 200
    assert response.json()["fundamental_timeout_seconds"] == 30.0

    update = client.put(
        "/regime/orchestrator/settings",
        json={"fundamental_timeout_seconds": 60, "portfolio_timeout_seconds": 15, "fundamental_veto_respected": False},
    )
    assert update.status_code == 200
    payload = update.json()
    assert payload["fundamental_timeout_seconds"] == 60.0
    assert payload["portfolio_timeout_seconds"] == 15.0
    assert payload["fundamental_veto_respected"] is False


def test_agents_status_includes_orchestrator() -> None:
    bus = AsyncEventBus()
    registry = get_agent_registry()
    registry.register(AgentOrchestrator(bus, config=OrchestratorConfig()))
    client = _client()
    response = client.get("/regime/agents/status")
    assert response.status_code == 200
    payload = response.json()
    assert payload["orchestrator"]["registered"] is True
    assert payload["orchestrator"]["config"]["fundamental_timeout_seconds"] == 30.0


def test_consensus_route_returns_latest_per_ticker() -> None:
    bus = get_event_bus()
    bus.publish_sync(_event())
    bus.publish_sync(EnrichedSignalEvent(ticker="NVDA", source="quant_agent", composite_action="Hold", regime_label="Neutral", unified_confidence=55.0))
    bus.publish_sync(FundamentalAssessmentEvent(ticker="NVDA", verdict="Buy", catalyst_sentiment="Positive"))
    bus.publish_sync(TradeDecisionEvent(ticker="NVDA", portfolio_id=1, action="Buy", decision="approved", quantity=10.0))
    bus.publish_sync(OrderExecutionEvent(ticker="NVDA", portfolio_id=1, action="Buy", status="filled", filled_price=101.0))
    client = _client()
    response = client.get("/regime/agents/consensus")
    assert response.status_code == 200
    payload = response.json()
    assert payload["ticker_count"] == 1
    assert payload["consensus"]["NVDA"]["quant"]["action"] == "Hold"
    assert payload["consensus"]["NVDA"]["execution"]["status"] == "filled"


def test_full_orchestrated_pipeline_from_analysis_request() -> None:
    bus = AsyncEventBus()
    registry = get_agent_registry()
    runtime = _runtime()
    registry.register(QuantAgent(bus, runtime=runtime))
    registry.register(FundamentalAgent(bus, runtime=runtime))
    registry.register(PortfolioTaxAgent(bus, runtime=runtime))
    registry.register(ExecutionAgent(bus, runtime=runtime))
    registry.register(AgentOrchestrator(bus, config=OrchestratorConfig()))

    seen: list[OrderExecutionEvent] = []

    async def capture_execution(event):
        if isinstance(event, OrderExecutionEvent):
            seen.append(event)

    bus.subscribe("order_execution", capture_execution)
    asyncio.run(bus.publish(AnalysisRequestEvent(tickers=("NVDA",), benchmark="SOXX", source="scheduler")))
    assert len(seen) == 1
    assert seen[0].status == "filled"
