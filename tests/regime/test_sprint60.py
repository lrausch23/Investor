from __future__ import annotations

import asyncio
import datetime as dt
import importlib
import json
import uuid
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.app.routes import regime as regime_route
from src.regime import event_bus as event_bus_module
from src.regime import event_subscribers as subscribers_module
from src.regime import events as events_module
from src.regime import persistence as persistence_module
from src.regime.signals import CompositeSignal, ConfidenceScore, PriceTargets, SignalResult

from _fixtures import FakeRegime


@pytest.fixture(autouse=True)
def reset_bus() -> None:
    event_bus_module.reset_event_bus()
    yield
    event_bus_module.reset_event_bus()


def _route_client() -> TestClient:
    app = FastAPI()
    app.include_router(regime_route.router)
    app.dependency_overrides[regime_route.require_actor] = lambda: "tester"
    app.dependency_overrides[regime_route.db_session] = lambda: iter([None])
    return TestClient(app)


def _composite() -> CompositeSignal:
    return CompositeSignal(
        regime_signal="Bull detected",
        regime_probability=0.91,
        forward_signal=SignalResult(action="Buy", timeframe="short", strength=0.72, expected_holding_days=10, rationale="test"),
        technical_signal="Buy the dip",
        composite_action="Buy",
        composite_strength=0.81,
        short_term_view="short",
        medium_term_view="medium",
    )


def _price_targets() -> PriceTargets:
    return PriceTargets(
        current_price=123.45,
        entry_price=120.0,
        exit_price=140.0,
        stop_price=115.0,
        risk_reward_ratio=2.0,
        timeframe_days=10,
        atr_value=3.5,
        confidence_multiplier=1.0,
        price_position="In target range",
    )


def _confidence() -> ConfidenceScore:
    return ConfidenceScore(value=78.5, label="High", calibrated=True, components={"regime_probability": 91.0})


def _dashboard_runtime() -> dict[str, object]:
    technicals = pd.DataFrame(
        {
            "rsi_14": [45.0, 50.0],
            "bb_pct": [0.4, 0.5],
            "macd_histogram": [0.1, 0.2],
            "bb_lower": [120.0, 121.0],
            "bb_upper": [135.0, 136.0],
            "atr_14": [3.2, 3.5],
        }
    )
    market_frame = pd.DataFrame(
        {
            "price": [120.0, 123.45],
            "volume": [1_000_000.0, 1_050_000.0],
            "high": [121.0, 124.0],
            "low": [119.0, 122.0],
            "vix": [20.0, 21.0],
            "yield_10y": [4.0, 4.1],
        }
    )
    regime = FakeRegime(
        "NVDA",
        "Bull",
        latest_price=123.45,
        price_frame=pd.DataFrame({"state_probability": [0.80, 0.84, 0.91]}),
    )

    class Registry:
        def get(self, name):
            return None

    return {
        "DEFAULT_TICKERS": ["NVDA"],
        "get_registry": lambda: Registry(),
        "get_setting": lambda key: None,
        "download_market_frame": lambda **kwargs: type("MarketSeries", (), {"frame": market_frame.copy()})(),
        "generate_weekly_digest": lambda **kwargs: type("Digest", (), {"action_items": [], "entries": [], "regime_changes": [], "sentiment_divergences": [], "tax_alerts": [], "generated_at": "2026-03-30T12:00:00+00:00"})(),
        "fit_regime_model": lambda ticker, market_frame: regime,
        "fit_regime_model_weekly": lambda ticker, market_frame: FakeRegime(ticker, "Bull", latest_price=123.45),
        "configured_frontier_model": lambda provider="auto": "OpenAI: gpt-4o",
        "get_investor_db_path": lambda: "/tmp/investor.db",
        "get_latest_prices": lambda db_path, tickers: {"NVDA": 123.45},
        "get_pending_outcomes": lambda: [],
        "get_signal_effectiveness": lambda: {"summary": {}, "by_action": {}, "rows": []},
        "get_portfolio_positions": lambda db_path, tickers=None, account_id=None: [],
        "get_portfolio_tickers": lambda db_path: ["NVDA"],
        "get_tax_assumptions": lambda db_path: {},
        "get_wash_sale_risk": lambda db_path, ticker: "NONE",
        "positions_by_ticker_and_account": lambda positions: {"NVDA": []},
        "save_regime_event": lambda ticker, label, state_id: {"previous_label": "Neutral", "days_in_regime": 1},
        "save_signal_snapshot": lambda **kwargs: None,
        "update_signal_outcome": lambda snapshot_id, interval, current_price: None,
        "forward_regime_curve": lambda *args, **kwargs: pd.DataFrame({"day": [1, 2], "p_bull": [0.7, 0.72], "p_neutral": [0.2, 0.18], "p_bear": [0.1, 0.1]}),
        "signal_from_forward_curve": lambda *args, **kwargs: SignalResult(action="Buy", timeframe="short", strength=0.72, expected_holding_days=10, rationale="test"),
        "compute_technicals": lambda *args, **kwargs: technicals.copy(),
        "intra_regime_signal": lambda *args, **kwargs: "Buy the dip",
        "build_composite_signal": lambda *args, **kwargs: _composite(),
        "compute_price_targets": lambda **kwargs: _price_targets(),
        "compute_unified_confidence": lambda *args, **kwargs: _confidence(),
        "confidence_trajectory": lambda *args, **kwargs: type("Trajectory", (), {"trend": "rising"})(),
        "sentiment_momentum": lambda *args, **kwargs: (type("Sentiment", (), {"trend": "improving"})(), pd.DataFrame({"recorded_at": ["2026-03-30"], "score": [1]})),
        "tax_adjusted_signals": lambda *args, **kwargs: [],
        "list_theses": lambda: [],
        "upsert_thesis": lambda ticker, thesis=None: None,
    }


def test_enriched_signal_event_defaults() -> None:
    event = events_module.EnrichedSignalEvent(ticker="NVDA")
    assert event.event_type == "enriched_signal"
    assert uuid.UUID(event.correlation_id)
    assert "T" in event.created_at
    assert event.regime_state_vector == ()


def test_enriched_signal_event_to_dict_serializable() -> None:
    event = events_module.EnrichedSignalEvent(
        ticker="NVDA",
        regime_state_vector=(0.8, 0.1, 0.1),
        transition_matrix=((0.9, 0.1, 0.0), (0.1, 0.8, 0.1), (0.0, 0.2, 0.8)),
    )
    payload = event.to_dict()
    assert payload["regime_state_vector"] == [0.8, 0.1, 0.1]
    assert payload["transition_matrix"][0] == [0.9, 0.1, 0.0]
    json.dumps(payload)


def test_trade_intent_event_fields_round_trip() -> None:
    event = events_module.TradeIntentEvent(ticker="NVDA", portfolio_id=1, action="Buy", source="discovery", quantity=10)
    payload = event.to_dict()
    assert payload["event_type"] == "trade_intent"
    assert payload["ticker"] == "NVDA"


def test_signal_snapshot_event_fields() -> None:
    event = events_module.SignalSnapshotEvent(ticker="NVDA", action="Buy", current_price=123.0)
    assert event.event_type == "signal_snapshot"
    assert event.current_price == 123.0


def test_enriched_signal_from_payload_factory_maps_fields() -> None:
    event = events_module.enriched_signal_from_payload(
        ticker="NVDA",
        regime_result=FakeRegime("NVDA", "Bull", latest_price=123.45),
        composite_signal=_composite(),
        price_targets=_price_targets(),
        confidence=_confidence(),
        ensemble_verdict=SimpleNamespace(signal="confirm", composite_confidence=0.84, sizing_multiplier=1.0, veto_reason=None),
        benchmark="SOXX",
        source="regime_analysis",
        meta_labeler_score=0.77,
        volume=1_050_000.0,
    )
    assert event.ticker == "NVDA"
    assert event.regime_label == "Bull"
    assert event.composite_action == "Buy"
    assert event.unified_confidence == pytest.approx(78.5)
    assert event.meta_labeler_score == pytest.approx(0.77)


def test_enriched_signal_from_payload_handles_missing_ensemble() -> None:
    event = events_module.enriched_signal_from_payload(
        ticker="NVDA",
        regime_result=FakeRegime("NVDA", "Bull"),
        composite_signal=_composite(),
        price_targets=_price_targets(),
        confidence=_confidence(),
        ensemble_verdict=None,
    )
    assert event.ensemble_signal == ""
    assert event.ensemble_veto_reason is None


def test_bus_publish_to_subscriber() -> None:
    bus = event_bus_module.AsyncEventBus()
    seen: list[str] = []

    async def subscriber(event):
        seen.append(event.ticker)

    bus.subscribe("enriched_signal", subscriber)
    asyncio.run(bus.publish(events_module.EnrichedSignalEvent(ticker="NVDA")))
    assert seen == ["NVDA"]


def test_bus_wildcard_subscriber_receives_all() -> None:
    bus = event_bus_module.AsyncEventBus()
    seen: list[str] = []

    async def subscriber(event):
        seen.append(event.event_type)

    bus.subscribe("*", subscriber)
    asyncio.run(bus.publish(events_module.SignalSnapshotEvent(ticker="NVDA")))
    assert seen == ["signal_snapshot"]


def test_bus_no_subscriber_no_error() -> None:
    bus = event_bus_module.AsyncEventBus()
    asyncio.run(bus.publish(events_module.SignalSnapshotEvent(ticker="NVDA")))


def test_bus_subscriber_error_isolated() -> None:
    bus = event_bus_module.AsyncEventBus()
    seen: list[str] = []

    async def bad(_event):
        raise ValueError("boom")

    async def good(event):
        seen.append(event.ticker)

    bus.subscribe("enriched_signal", bad)
    bus.subscribe("enriched_signal", good)
    asyncio.run(bus.publish(events_module.EnrichedSignalEvent(ticker="NVDA")))
    assert seen == ["NVDA"]


def test_bus_publish_sync_delivers() -> None:
    bus = event_bus_module.AsyncEventBus()
    seen: list[str] = []

    async def subscriber(event):
        seen.append(event.ticker)

    bus.subscribe("trade_intent", subscriber)
    bus.publish_sync(events_module.TradeIntentEvent(ticker="AVGO"))
    assert seen == ["AVGO"]


def test_bus_history_and_filter() -> None:
    bus = event_bus_module.AsyncEventBus()
    bus.publish_sync(events_module.SignalSnapshotEvent(ticker="A"))
    bus.publish_sync(events_module.TradeIntentEvent(ticker="B"))
    history = bus.get_history(limit=10)
    filtered = bus.get_history(event_type="trade_intent", limit=10)
    assert len(history) == 2
    assert len(filtered) == 1
    assert filtered[0]["event_type"] == "trade_intent"


def test_bus_history_limit() -> None:
    bus = event_bus_module.AsyncEventBus(max_history=5)
    for idx in range(10):
        bus.publish_sync(events_module.SignalSnapshotEvent(ticker=f"T{idx}"))
    history = bus.get_history(limit=10)
    assert len(history) == 5
    assert history[0]["ticker"] == "T5"


def test_bus_stop_drops_events_and_start_resumes() -> None:
    bus = event_bus_module.AsyncEventBus()
    seen: list[str] = []

    async def subscriber(event):
        seen.append(event.ticker)

    bus.subscribe("signal_snapshot", subscriber)
    bus.stop()
    bus.publish_sync(events_module.SignalSnapshotEvent(ticker="DROP"))
    bus.start()
    bus.publish_sync(events_module.SignalSnapshotEvent(ticker="KEEP"))
    assert seen == ["KEEP"]


def test_get_event_bus_singleton() -> None:
    first = event_bus_module.get_event_bus()
    second = event_bus_module.get_event_bus()
    assert first is second


def test_reset_event_bus_creates_new_instance() -> None:
    first = event_bus_module.get_event_bus()
    event_bus_module.reset_event_bus()
    second = event_bus_module.get_event_bus()
    assert first is not second


def test_audit_log_subscriber_writes_alert(monkeypatch) -> None:
    seen: list[dict[str, object]] = []
    monkeypatch.setattr(subscribers_module, "save_alert", lambda **kwargs: seen.append(kwargs) or {"id": 1, **kwargs})
    asyncio.run(subscribers_module.audit_log_subscriber(events_module.EnrichedSignalEvent(ticker="NVDA")))
    assert seen[0]["alert_type"] == "bus_event"


def test_enriched_signal_logger(monkeypatch) -> None:
    calls: list[str] = []
    monkeypatch.setattr(subscribers_module.logger, "info", lambda msg, *args: calls.append(msg % args))
    asyncio.run(subscribers_module.enriched_signal_logger(events_module.EnrichedSignalEvent(ticker="NVDA", regime_label="Bull", composite_action="Buy", unified_confidence=80.0)))
    assert "EnrichedSignalEvent" in calls[0]


def test_trade_intent_logger(monkeypatch) -> None:
    calls: list[str] = []
    monkeypatch.setattr(subscribers_module.logger, "info", lambda msg, *args: calls.append(msg % args))
    asyncio.run(subscribers_module.trade_intent_logger(events_module.TradeIntentEvent(ticker="NVDA", action="Buy", portfolio_id=1, source="discovery")))
    assert "TradeIntentEvent" in calls[0]


def test_publish_points_signal_snapshot(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(persistence_module, "DB_PATH", tmp_path / "regime_watch.db")
    published: list[object] = []
    monkeypatch.setattr(event_bus_module, "get_event_bus", lambda: SimpleNamespace(publish_sync=lambda event: published.append(event)))
    persistence_module.save_signal_snapshot(
        ticker="NVDA",
        snapshot_date=dt.date.today().isoformat(),
        action="Buy",
        regime_label="Bull",
        regime_probability=0.9,
        composite_strength=0.8,
        benchmark="SOXX",
        current_price=123.0,
        entry_price=120.0,
        exit_price=140.0,
        stop_price=115.0,
        risk_reward_ratio=2.0,
        timeframe_days=10,
    )
    assert isinstance(published[0], events_module.SignalSnapshotEvent)


def test_publish_point_bus_failure_non_fatal(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(persistence_module, "DB_PATH", tmp_path / "regime_watch.db")
    monkeypatch.setattr(event_bus_module, "get_event_bus", lambda: (_ for _ in ()).throw(RuntimeError("bus down")))
    persistence_module.save_signal_snapshot(
        ticker="NVDA",
        snapshot_date=(dt.date.today() - dt.timedelta(days=40)).isoformat(),
        action="Buy",
        regime_label="Bull",
        regime_probability=0.9,
        composite_strength=0.8,
        benchmark="SOXX",
        current_price=123.0,
        entry_price=120.0,
        exit_price=140.0,
        stop_price=115.0,
        risk_reward_ratio=2.0,
        timeframe_days=10,
    )
    rows = persistence_module.get_pending_outcomes(as_of=dt.datetime.now(dt.timezone.utc).isoformat())
    assert rows


def test_create_trade_plan_publishes_trade_intent(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(persistence_module, "DB_PATH", tmp_path / "regime_watch.db")
    portfolio = persistence_module.create_paper_portfolio("Sandbox", 100000.0)
    published: list[object] = []
    monkeypatch.setattr(event_bus_module, "get_event_bus", lambda: SimpleNamespace(publish_sync=lambda event: published.append(event)))
    plan = persistence_module.create_trade_plan(portfolio["id"], "NVDA", "Buy", 10, "Entry")
    assert plan["ticker"] == "NVDA"
    assert isinstance(published[0], events_module.TradeIntentEvent)


def test_dashboard_build_publishes_enriched_signal(monkeypatch) -> None:
    runtime = _dashboard_runtime()
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (runtime, None))
    published: list[object] = []
    monkeypatch.setattr(event_bus_module, "get_event_bus", lambda: SimpleNamespace(publish_sync=lambda event: published.append(event)))
    payload = regime_route._build_regime_dashboard_payload(benchmark="SOXX", period="3y", tickers=["NVDA"], force_refresh=True)
    assert payload["rows"]
    assert any(isinstance(item, events_module.EnrichedSignalEvent) for item in published)


def test_event_bus_status_route() -> None:
    event_bus_module.get_event_bus().publish_sync(events_module.SignalSnapshotEvent(ticker="NVDA"))
    client = _route_client()
    response = client.get("/regime/event-bus/status")
    assert response.status_code == 200
    payload = response.json()
    assert {"running", "subscriber_count", "history_size"} <= set(payload.keys())


def test_event_bus_history_route() -> None:
    bus = event_bus_module.get_event_bus()
    bus.publish_sync(events_module.SignalSnapshotEvent(ticker="NVDA"))
    bus.publish_sync(events_module.TradeIntentEvent(ticker="AVGO"))
    client = _route_client()
    response = client.get("/regime/event-bus/history")
    assert response.status_code == 200
    payload = response.json()
    assert payload["count"] >= 2
    assert isinstance(payload["events"], list)


def test_health_endpoint_includes_event_bus(monkeypatch, tmp_path) -> None:
    runtime = {
        "get_alerts": lambda **kwargs: [],
        "get_setting": lambda key: None,
        "validate_ibkr_readiness": lambda: {"all_clear": True},
    }
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (runtime, None))
    monkeypatch.setattr("src.regime.backup.get_backup_status", lambda: {"last_backup_at": None})
    monkeypatch.setattr("src.regime.data_validator.check_database_health", lambda: {"healthy": True, "integrity": "ok"})
    monkeypatch.setattr("src.regime.meta_labeler.list_saved_versions", lambda: [])
    monkeypatch.setattr("src.regime.recovery.detect_stuck_orders", lambda: [])
    monkeypatch.setattr("src.regime.watchdog.get_watchdog", lambda: None)
    monkeypatch.setattr("src.regime.persistence.DB_PATH", tmp_path / "regime_watch.db")
    client = _route_client()
    response = client.get("/regime/health")
    assert response.status_code == 200
    assert "event_bus" in response.json()
