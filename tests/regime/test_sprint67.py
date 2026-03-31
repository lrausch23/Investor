from __future__ import annotations

import importlib
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.app.routes import regime as regime_route


@pytest.fixture
def temp_modules(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("HMM_DATA_DIR", str(tmp_path))
    import src.regime.anti_churn as anti_churn
    import src.regime.event_bus as event_bus
    import src.regime.events as events
    import src.regime.ltcg_override as ltcg_override
    import src.regime.paper_trading as paper_trading
    import src.regime.persistence as store
    import src.regime.agents.portfolio_agent as portfolio_agent

    store = importlib.reload(store)
    store.DB_PATH = tmp_path / "regime_watch.db"
    anti_churn = importlib.reload(anti_churn)
    ltcg_override = importlib.reload(ltcg_override)
    paper_trading = importlib.reload(paper_trading)
    portfolio_agent = importlib.reload(portfolio_agent)
    event_bus = importlib.reload(event_bus)
    events = importlib.reload(events)
    return store, anti_churn, ltcg_override, paper_trading, portfolio_agent, event_bus, events


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(regime_route.router)
    app.dependency_overrides[regime_route.require_actor] = lambda: "tester"
    return TestClient(app)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _days_ago(days: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()


def _buy_event(events_module, **overrides):
    payload = {
        "ticker": "NVDA",
        "source": "quant_agent",
        "regime_label": "Bull",
        "current_price": 100.0,
        "entry_price": 100.0,
        "exit_price": 108.0,
        "expected_regime_duration": 12.0,
        "composite_action": "Buy",
        "composite_strength": 0.4,
        "ensemble_sizing_multiplier": 1.0,
        "meta_labeler_score": 0.7,
    }
    payload.update(overrides)
    return events_module.EnrichedSignalEvent(**payload)


def _sell_event(events_module, **overrides):
    payload = {
        "ticker": "NVDA",
        "source": "quant_agent",
        "regime_label": "Bear",
        "current_price": 120.0,
        "atr_14": 2.0,
        "composite_action": "Sell",
        "meta_labeler_score": 0.7,
    }
    payload.update(overrides)
    return events_module.EnrichedSignalEvent(**payload)


def _make_runtime(positions):
    return {
        "is_wash_sale_restricted": lambda portfolio_id, ticker: False,
        "get_paper_positions": lambda portfolio_id, status="Open": positions,
        "list_paper_portfolios": lambda include_closed=False: [{"id": 1, "status": "Active", "current_cash": 100000.0, "starting_budget": 100000.0}],
    }


def _record_executed_sell(store, portfolio_id: int, ticker: str, executed_at: str) -> None:
    plan = store.create_trade_plan(portfolio_id, ticker, "Sell", 5, "round trip", proposed_price=100.0, source="exit_signal")
    store.update_trade_plan_status(plan["id"], "Executed", executed_at=executed_at, execution_price=100.0)


def test_count_round_trips_none(temp_modules) -> None:
    store, anti_churn, *_rest = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox")
    assert anti_churn.count_round_trips(portfolio["id"], "NVDA") == 0


def test_count_round_trips_within_window(temp_modules) -> None:
    store, anti_churn, *_rest = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox")
    _record_executed_sell(store, portfolio["id"], "NVDA", _days_ago(5))
    _record_executed_sell(store, portfolio["id"], "NVDA", _days_ago(10))
    assert anti_churn.count_round_trips(portfolio["id"], "NVDA") == 2


def test_count_round_trips_outside_window(temp_modules) -> None:
    store, anti_churn, *_rest = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox")
    _record_executed_sell(store, portfolio["id"], "NVDA", _days_ago(45))
    assert anti_churn.count_round_trips(portfolio["id"], "NVDA") == 0


def test_check_anti_churn_blocks_at_limit(temp_modules) -> None:
    store, anti_churn, *_rest = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox")
    _record_executed_sell(store, portfolio["id"], "NVDA", _days_ago(5))
    _record_executed_sell(store, portfolio["id"], "NVDA", _days_ago(10))
    result = anti_churn.check_anti_churn(portfolio["id"], "NVDA")
    assert result.passed is False
    assert result.cooldown_expires is not None


def test_anti_churn_settings_defaults_and_set(temp_modules) -> None:
    _store, anti_churn, *_rest = temp_modules
    defaults = anti_churn.get_anti_churn_settings()
    assert defaults["anti_churn_enabled"] is True
    assert defaults["anti_churn_max_round_trips_30d"] == 2
    payload = anti_churn.set_anti_churn_settings({"anti_churn_enabled": False, "anti_churn_max_round_trips_30d": 4, "anti_churn_cooldown_days": 45})
    assert payload["anti_churn_enabled"] is False
    assert payload["anti_churn_max_round_trips_30d"] == 4
    assert payload["anti_churn_cooldown_days"] == 45


def test_ltcg_override_profitable_near_threshold(temp_modules) -> None:
    store, _anti_churn, ltcg_override, *_rest = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox")
    position = store.open_paper_position(portfolio["id"], "NVDA", 25, 100.0, _days_ago(355), stop_price=150.0)
    store.create_tax_lot(portfolio["id"], position["id"], "NVDA", 25, 100.0, _days_ago(355))
    result = ltcg_override.check_ltcg_override(portfolio["id"], "NVDA", current_price=160.0, position_stop=150.0, atr_14=2.0)
    assert result.override_active is True
    assert result.protected_quantity == pytest.approx(25.0)
    assert result.total_tax_savings > 0


def test_ltcg_override_risk_exceeds_savings(temp_modules) -> None:
    store, _anti_churn, ltcg_override, *_rest = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox")
    position = store.open_paper_position(portfolio["id"], "NVDA", 25, 198.0, _days_ago(355), stop_price=197.0)
    store.create_tax_lot(portfolio["id"], position["id"], "NVDA", 25, 198.0, _days_ago(355))
    result = ltcg_override.check_ltcg_override(portfolio["id"], "NVDA", current_price=200.0, position_stop=197.0, atr_14=5.0)
    assert result.override_active is False


def test_ltcg_override_partial_position(temp_modules) -> None:
    store, _anti_churn, ltcg_override, *_rest = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox")
    position = store.open_paper_position(portfolio["id"], "NVDA", 50, 100.0, _days_ago(355), stop_price=90.0)
    store.create_tax_lot(portfolio["id"], position["id"], "NVDA", 25, 50.0, _days_ago(355))
    store.create_tax_lot(portfolio["id"], position["id"], "NVDA", 25, 50.0, _days_ago(200))
    result = ltcg_override.check_ltcg_override(portfolio["id"], "NVDA", current_price=89.0, position_stop=90.0, atr_14=1.0)
    assert result.override_active is True
    assert result.protected_quantity == pytest.approx(25.0)
    assert result.sellable_quantity == pytest.approx(25.0)


def test_agent_anti_churn_veto(temp_modules) -> None:
    store, anti_churn, _ltcg, _paper, portfolio_agent, event_bus, events = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox")
    _record_executed_sell(store, portfolio["id"], "NVDA", _days_ago(3))
    _record_executed_sell(store, portfolio["id"], "NVDA", _days_ago(8))
    anti_churn.set_anti_churn_settings({"anti_churn_enabled": True, "anti_churn_max_round_trips_30d": 2})
    agent = portfolio_agent.PortfolioTaxAgent(event_bus.AsyncEventBus(), runtime=_make_runtime([]))
    decision = agent._size_and_check(_make_runtime([]), _buy_event(events), "Buy", {"id": 1, "current_cash": 100000.0, "starting_budget": 100000.0})
    assert decision is not None
    assert decision.decision == "vetoed"
    assert "anti_churn" in str(decision.veto_reason)


def test_agent_ltcg_sell_veto(temp_modules) -> None:
    store, _anti_churn, ltcg_override, _paper, portfolio_agent, event_bus, events = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox")
    position = store.open_paper_position(portfolio["id"], "NVDA", 50, 100.0, _days_ago(355))
    store.create_tax_lot(portfolio["id"], position["id"], "NVDA", 50, 100.0, _days_ago(355))
    ltcg_override.set_ltcg_override_settings({"ltcg_override_enabled": True})
    positions = [{"id": position["id"], "ticker": "NVDA", "quantity": 50.0, "status": "Open"}]
    agent = portfolio_agent.PortfolioTaxAgent(event_bus.AsyncEventBus(), runtime=_make_runtime(positions))
    decision = agent._size_and_check(_make_runtime(positions), _sell_event(events, current_price=120.0, atr_14=None), "Sell", {"id": 1, "current_cash": 100000.0, "starting_budget": 100000.0})
    assert decision is not None
    assert decision.decision == "vetoed"
    assert "ltcg_override" in str(decision.veto_reason)


def test_agent_ltcg_partial_sell(temp_modules) -> None:
    store, _anti_churn, ltcg_override, _paper, portfolio_agent, event_bus, events = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox")
    position = store.open_paper_position(portfolio["id"], "NVDA", 50, 100.0, _days_ago(355))
    store.create_tax_lot(portfolio["id"], position["id"], "NVDA", 25, 50.0, _days_ago(355))
    store.create_tax_lot(portfolio["id"], position["id"], "NVDA", 25, 50.0, _days_ago(200))
    ltcg_override.set_ltcg_override_settings({"ltcg_override_enabled": True})
    positions = [{"id": position["id"], "ticker": "NVDA", "quantity": 50.0, "status": "Open"}]
    agent = portfolio_agent.PortfolioTaxAgent(event_bus.AsyncEventBus(), runtime=_make_runtime(positions))
    decision = agent._size_and_check(_make_runtime(positions), _sell_event(events, current_price=89.0, atr_14=1.0), "Sell", {"id": 1, "current_cash": 100000.0, "starting_budget": 100000.0})
    assert decision is not None
    assert decision.decision == "approved"
    assert decision.quantity == pytest.approx(25.0)
    assert "ltcg=shield" in str(decision.sizing_rationale)


def test_buy_plans_anti_churn_filter(temp_modules, monkeypatch: pytest.MonkeyPatch) -> None:
    store, anti_churn, _ltcg, paper_trading, *_rest = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox")
    theme = store.create_theme("AI", conviction=5)
    store.upsert_watchlist_candidate(theme["id"], "NVDA", suggested_entry_price=100.0, crowd_score=30, regime_label="Bull", regime_probability=0.8, status="Entry Signal")
    store.save_signal_snapshot(
        ticker="NVDA",
        snapshot_date="2026-03-31",
        action="Buy",
        regime_label="Bull",
        regime_probability=0.8,
        composite_strength=0.7,
        benchmark="SPY",
        current_price=100.0,
        entry_price=100.0,
        exit_price=110.0,
        stop_price=95.0,
        risk_reward_ratio=2.0,
        timeframe_days=10,
        expected_regime_duration=12.0,
    )
    _record_executed_sell(store, portfolio["id"], "NVDA", _days_ago(4))
    _record_executed_sell(store, portfolio["id"], "NVDA", _days_ago(8))
    anti_churn.set_anti_churn_settings({"anti_churn_enabled": True, "anti_churn_max_round_trips_30d": 2})
    monkeypatch.setattr(paper_trading, "allocate_budget", lambda portfolio_id, config=None: {"themes": [{"theme_id": theme["id"], "by_role": {"Critical-Path": 10000.0}}]})
    monkeypatch.setattr(paper_trading, "_lookup_atr", lambda ticker: None)
    monkeypatch.setattr(paper_trading, "_lookup_beta", lambda ticker: None)
    plans = paper_trading.generate_buy_plans(portfolio["id"])
    assert plans == []


def test_exit_plans_ltcg_suppressed(temp_modules, monkeypatch: pytest.MonkeyPatch) -> None:
    store, _anti_churn, ltcg_override, paper_trading, *_rest = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox")
    position = store.open_paper_position(portfolio["id"], "NVDA", 50, 100.0, _days_ago(355), stop_price=90.0)
    store.create_tax_lot(portfolio["id"], position["id"], "NVDA", 50, 50.0, _days_ago(355))
    ltcg_override.set_ltcg_override_settings({"ltcg_override_enabled": True})
    monkeypatch.setattr(paper_trading, "_batch_current_prices", lambda tickers: {"NVDA": 89.0})
    monkeypatch.setattr(paper_trading, "_lookup_atr", lambda ticker: 1.0)
    plans = paper_trading.generate_exit_plans(portfolio["id"])
    assert plans == []


def test_exit_plans_ltcg_partial_exit(temp_modules, monkeypatch: pytest.MonkeyPatch) -> None:
    store, _anti_churn, ltcg_override, paper_trading, *_rest = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox")
    position = store.open_paper_position(portfolio["id"], "NVDA", 50, 100.0, _days_ago(355), stop_price=90.0)
    store.create_tax_lot(portfolio["id"], position["id"], "NVDA", 25, 50.0, _days_ago(355))
    store.create_tax_lot(portfolio["id"], position["id"], "NVDA", 25, 50.0, _days_ago(200))
    ltcg_override.set_ltcg_override_settings({"ltcg_override_enabled": True})
    monkeypatch.setattr(paper_trading, "_batch_current_prices", lambda tickers: {"NVDA": 89.0})
    monkeypatch.setattr(paper_trading, "_lookup_atr", lambda ticker: 1.0)
    plans = paper_trading.generate_exit_plans(portfolio["id"])
    assert len(plans) == 1
    assert float(plans[0]["quantity"]) == pytest.approx(25.0)
    assert plans[0]["ltcg_override_active"] == 1


def test_barrier_override_event_fields(temp_modules) -> None:
    _store, _anti_churn, _ltcg, _paper, _agent, _bus, events = temp_modules
    event = events.BarrierOverrideEvent(
        ticker="NVDA",
        portfolio_id=1,
        lot_id=42,
        original_stop=95.0,
        overridden_stop=90.0,
        reason="ltcg_preservation",
        days_to_ltcg=11,
        tax_savings_estimate=312.5,
        max_additional_risk=187.0,
        expiry="2026-04-15T00:00:00+00:00",
    )
    payload = event.to_dict()
    assert payload["event_type"] == "barrier_override"
    assert payload["lot_id"] == 42
    assert payload["days_to_ltcg"] == 11


def test_anti_churn_settings_route(temp_modules) -> None:
    _store, _anti, _ltcg, *_rest = temp_modules
    client = _client()
    response = client.get("/regime/anti-churn/settings")
    assert response.status_code == 200
    put_response = client.put("/regime/anti-churn/settings", json={"anti_churn_enabled": False, "anti_churn_max_round_trips_30d": 3})
    assert put_response.status_code == 200
    assert put_response.json()["anti_churn_enabled"] is False


def test_anti_churn_diagnostic_route(temp_modules) -> None:
    store, _anti, _ltcg, *_rest = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox")
    _record_executed_sell(store, portfolio["id"], "NVDA", _days_ago(5))
    client = _client()
    response = client.get("/regime/anti-churn/NVDA")
    assert response.status_code == 200
    payload = response.json()
    assert payload["ticker"] == "NVDA"
    assert payload["portfolios"][0]["round_trip_count"] == 1


def test_ltcg_override_settings_route(temp_modules) -> None:
    _store, _anti, _ltcg, *_rest = temp_modules
    client = _client()
    response = client.get("/regime/ltcg-override/settings")
    assert response.status_code == 200
    put_response = client.put("/regime/ltcg-override/settings", json={"ltcg_trigger_days_to_threshold": 12, "ltcg_rate": 0.18})
    assert put_response.status_code == 200
    assert put_response.json()["ltcg_trigger_days_to_threshold"] == 12


def test_ltcg_override_diagnostic_route(temp_modules, monkeypatch: pytest.MonkeyPatch) -> None:
    store, _anti, _ltcg, paper_trading, *_rest = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox")
    position = store.open_paper_position(portfolio["id"], "NVDA", 25, 100.0, _days_ago(355), stop_price=90.0)
    store.create_tax_lot(portfolio["id"], position["id"], "NVDA", 25, 50.0, _days_ago(355))
    monkeypatch.setattr(paper_trading, "_batch_current_prices", lambda tickers: {"NVDA": 89.0})
    monkeypatch.setattr(paper_trading, "_lookup_atr", lambda ticker: 1.0)
    client = _client()
    response = client.get(f"/regime/ltcg-override/NVDA?portfolio_id={portfolio['id']}")
    assert response.status_code == 200
    payload = response.json()
    assert payload["ticker"] == "NVDA"
    assert payload["lots_checked"] >= 1
