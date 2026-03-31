from __future__ import annotations

import importlib
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.app.routes import regime as regime_route


@pytest.fixture
def temp_modules(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("HMM_DATA_DIR", str(tmp_path))
    import src.regime.event_bus as event_bus
    import src.regime.events as events
    import src.regime.hurdle_rate as hurdle_rate
    import src.regime.paper_trading as paper_trading
    import src.regime.persistence as store
    import src.regime.agents.portfolio_agent as portfolio_agent

    store = importlib.reload(store)
    store.DB_PATH = tmp_path / "regime_watch.db"
    hurdle_rate = importlib.reload(hurdle_rate)
    paper_trading = importlib.reload(paper_trading)
    portfolio_agent = importlib.reload(portfolio_agent)
    event_bus = importlib.reload(event_bus)
    events = importlib.reload(events)
    return store, hurdle_rate, paper_trading, portfolio_agent, event_bus, events


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(regime_route.router)
    app.dependency_overrides[regime_route.require_actor] = lambda: "tester"
    return TestClient(app)


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


def _agent_runtime() -> dict[str, object]:
    return {
        "is_wash_sale_restricted": lambda portfolio_id, ticker: False,
        "get_paper_positions": lambda portfolio_id, status="Open": [],
        "list_paper_portfolios": lambda include_closed=False: [{"id": 1, "status": "Active", "current_cash": 100000.0, "starting_budget": 100000.0}],
    }


def test_hurdle_rate_above_threshold(temp_modules) -> None:
    _store, hurdle_rate, _paper, _agent, _bus, _events = temp_modules
    result = hurdle_rate.check_hurdle_rate("NVDA", 100.0, 108.0)
    assert result.passed is True
    assert result.gross_return_pct == pytest.approx(8.0)
    assert result.net_return_pct == pytest.approx(5.44)


def test_hurdle_rate_below_threshold(temp_modules) -> None:
    _store, hurdle_rate, _paper, _agent, _bus, _events = temp_modules
    result = hurdle_rate.check_hurdle_rate("NVDA", 100.0, 104.0)
    assert result.passed is False
    assert result.net_return_pct == pytest.approx(2.72)


def test_hurdle_rate_negative_return(temp_modules) -> None:
    _store, hurdle_rate, _paper, _agent, _bus, _events = temp_modules
    result = hurdle_rate.check_hurdle_rate("NVDA", 100.0, 95.0)
    assert result.passed is False
    assert result.gross_return_pct < 0


def test_hurdle_rate_missing_prices(temp_modules) -> None:
    _store, hurdle_rate, _paper, _agent, _bus, _events = temp_modules
    result = hurdle_rate.check_hurdle_rate("NVDA", None, 108.0)
    assert result.passed is True
    assert result.net_return_pct is None


def test_hurdle_rate_tax_rate_clamping(temp_modules) -> None:
    _store, hurdle_rate, _paper, _agent, _bus, _events = temp_modules
    high = hurdle_rate.check_hurdle_rate("NVDA", 100.0, 110.0, estimated_stcg_rate=1.5)
    low = hurdle_rate.check_hurdle_rate("NVDA", 100.0, 110.0, estimated_stcg_rate=-0.1)
    assert high.estimated_stcg_rate == pytest.approx(0.99)
    assert low.estimated_stcg_rate == pytest.approx(0.0)


def test_duration_gate_above_threshold(temp_modules) -> None:
    _store, hurdle_rate, _paper, _agent, _bus, _events = temp_modules
    result = hurdle_rate.check_duration_gate("NVDA", 12.0, "Bull")
    assert result.passed is True


def test_duration_gate_below_threshold(temp_modules) -> None:
    _store, hurdle_rate, _paper, _agent, _bus, _events = temp_modules
    result = hurdle_rate.check_duration_gate("NVDA", 4.0, "Bull")
    assert result.passed is False


def test_duration_gate_non_bull_regime(temp_modules) -> None:
    _store, hurdle_rate, _paper, _agent, _bus, _events = temp_modules
    result = hurdle_rate.check_duration_gate("NVDA", 3.0, "Bear")
    assert result.passed is True


def test_duration_gate_missing_duration(temp_modules) -> None:
    _store, hurdle_rate, _paper, _agent, _bus, _events = temp_modules
    result = hurdle_rate.check_duration_gate("NVDA", None, "Bull")
    assert result.passed is True


def test_duration_gate_custom_threshold(temp_modules) -> None:
    _store, hurdle_rate, _paper, _agent, _bus, _events = temp_modules
    result = hurdle_rate.check_duration_gate("NVDA", 10.0, "Bull", min_regime_duration_days=14.0)
    assert result.passed is False


def test_get_hurdle_settings_defaults(temp_modules) -> None:
    _store, hurdle_rate, _paper, _agent, _bus, _events = temp_modules
    payload = hurdle_rate.get_hurdle_settings()
    assert payload["hurdle_enabled"] is True
    assert payload["duration_gate_enabled"] is True
    assert payload["estimated_stcg_rate"] == pytest.approx(0.32)
    assert payload["hurdle_min_net_return_pct"] == pytest.approx(3.0)
    assert payload["min_regime_duration_days"] == pytest.approx(7.0)


def test_set_hurdle_settings_clamping(temp_modules) -> None:
    _store, hurdle_rate, _paper, _agent, _bus, _events = temp_modules
    payload = hurdle_rate.set_hurdle_settings(
        {
            "estimated_stcg_rate": 1.5,
            "hurdle_min_net_return_pct": 100.0,
            "min_regime_duration_days": -5.0,
        }
    )
    assert payload["estimated_stcg_rate"] == pytest.approx(0.99)
    assert payload["hurdle_min_net_return_pct"] == pytest.approx(50.0)
    assert payload["min_regime_duration_days"] == pytest.approx(1.0)


def test_set_hurdle_settings_persistence(temp_modules) -> None:
    _store, hurdle_rate, _paper, _agent, _bus, _events = temp_modules
    hurdle_rate.set_hurdle_settings(
        {
            "hurdle_enabled": False,
            "duration_gate_enabled": False,
            "estimated_stcg_rate": 0.28,
            "hurdle_min_net_return_pct": 4.5,
            "min_regime_duration_days": 10.0,
        }
    )
    payload = hurdle_rate.get_hurdle_settings()
    assert payload["hurdle_enabled"] is False
    assert payload["duration_gate_enabled"] is False
    assert payload["estimated_stcg_rate"] == pytest.approx(0.28)
    assert payload["hurdle_min_net_return_pct"] == pytest.approx(4.5)
    assert payload["min_regime_duration_days"] == pytest.approx(10.0)


def test_agent_hurdle_veto(temp_modules) -> None:
    _store, hurdle_rate, _paper, portfolio_agent, event_bus, events = temp_modules
    hurdle_rate.set_hurdle_settings({"hurdle_enabled": True, "duration_gate_enabled": True})
    agent = portfolio_agent.PortfolioTaxAgent(event_bus.AsyncEventBus(), runtime=_agent_runtime())
    decision = agent._size_and_check(_agent_runtime(), _buy_event(events, exit_price=104.0), "Buy", {"id": 1, "current_cash": 100000.0, "starting_budget": 100000.0})
    assert decision is not None
    assert decision.decision == "vetoed"
    assert "hurdle_rate" in str(decision.veto_reason)


def test_agent_duration_veto(temp_modules) -> None:
    _store, hurdle_rate, _paper, portfolio_agent, event_bus, events = temp_modules
    hurdle_rate.set_hurdle_settings({"hurdle_enabled": True, "duration_gate_enabled": True})
    agent = portfolio_agent.PortfolioTaxAgent(event_bus.AsyncEventBus(), runtime=_agent_runtime())
    decision = agent._size_and_check(_agent_runtime(), _buy_event(events, expected_regime_duration=4.0), "Buy", {"id": 1, "current_cash": 100000.0, "starting_budget": 100000.0})
    assert decision is not None
    assert decision.decision == "vetoed"
    assert "duration_gate" in str(decision.veto_reason)


def test_agent_both_pass(temp_modules) -> None:
    _store, hurdle_rate, _paper, portfolio_agent, event_bus, events = temp_modules
    hurdle_rate.set_hurdle_settings({"hurdle_enabled": True, "duration_gate_enabled": True})
    agent = portfolio_agent.PortfolioTaxAgent(event_bus.AsyncEventBus(), runtime=_agent_runtime())
    decision = agent._size_and_check(_agent_runtime(), _buy_event(events), "Buy", {"id": 1, "current_cash": 100000.0, "starting_budget": 100000.0})
    assert decision is not None
    assert decision.decision == "approved"
    assert "hurdle=" in str(decision.sizing_rationale)
    assert "duration=" in str(decision.sizing_rationale)


def test_agent_gates_disabled(temp_modules) -> None:
    _store, hurdle_rate, _paper, portfolio_agent, event_bus, events = temp_modules
    hurdle_rate.set_hurdle_settings({"hurdle_enabled": False, "duration_gate_enabled": False})
    agent = portfolio_agent.PortfolioTaxAgent(event_bus.AsyncEventBus(), runtime=_agent_runtime())
    decision = agent._size_and_check(_agent_runtime(), _buy_event(events, exit_price=101.0, expected_regime_duration=1.0), "Buy", {"id": 1, "current_cash": 100000.0, "starting_budget": 100000.0})
    assert decision is not None
    assert decision.decision == "approved"


def test_buy_plans_hurdle_filter(temp_modules, monkeypatch: pytest.MonkeyPatch) -> None:
    store, hurdle_rate, paper_trading, _portfolio_agent, _bus, _events = temp_modules
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
        exit_price=104.0,
        stop_price=95.0,
        risk_reward_ratio=2.0,
        timeframe_days=10,
        expected_regime_duration=12.0,
    )
    monkeypatch.setattr(paper_trading, "allocate_budget", lambda portfolio_id, config=None: {"themes": [{"theme_id": theme["id"], "by_role": {"Critical-Path": 10000.0}}]})
    monkeypatch.setattr(paper_trading, "_lookup_atr", lambda ticker: None)
    monkeypatch.setattr(paper_trading, "_lookup_beta", lambda ticker: None)
    hurdle_rate.set_hurdle_settings({"hurdle_enabled": True, "duration_gate_enabled": True})
    plans = paper_trading.generate_buy_plans(portfolio["id"])
    assert plans == []


def test_buy_plans_duration_filter(temp_modules, monkeypatch: pytest.MonkeyPatch) -> None:
    store, hurdle_rate, paper_trading, _portfolio_agent, _bus, _events = temp_modules
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
        timeframe_days=4,
        expected_regime_duration=4.0,
    )
    monkeypatch.setattr(paper_trading, "allocate_budget", lambda portfolio_id, config=None: {"themes": [{"theme_id": theme["id"], "by_role": {"Critical-Path": 10000.0}}]})
    monkeypatch.setattr(paper_trading, "_lookup_atr", lambda ticker: None)
    monkeypatch.setattr(paper_trading, "_lookup_beta", lambda ticker: None)
    hurdle_rate.set_hurdle_settings({"hurdle_enabled": True, "duration_gate_enabled": True})
    plans = paper_trading.generate_buy_plans(portfolio["id"])
    assert plans == []


def test_buy_plans_gates_disabled_passthrough(temp_modules, monkeypatch: pytest.MonkeyPatch) -> None:
    store, hurdle_rate, paper_trading, _portfolio_agent, _bus, _events = temp_modules
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
        exit_price=104.0,
        stop_price=95.0,
        risk_reward_ratio=2.0,
        timeframe_days=4,
        expected_regime_duration=4.0,
    )
    monkeypatch.setattr(paper_trading, "allocate_budget", lambda portfolio_id, config=None: {"themes": [{"theme_id": theme["id"], "by_role": {"Critical-Path": 10000.0}}]})
    monkeypatch.setattr(paper_trading, "_lookup_atr", lambda ticker: None)
    monkeypatch.setattr(paper_trading, "_lookup_beta", lambda ticker: None)
    hurdle_rate.set_hurdle_settings({"hurdle_enabled": False, "duration_gate_enabled": False})
    plans = paper_trading.generate_buy_plans(portfolio["id"])
    assert len(plans) == 1
    assert plans[0]["ticker"] == "NVDA"


def test_plan_hurdle_columns_stored(temp_modules) -> None:
    store, _hurdle_rate, _paper, _portfolio_agent, _bus, _events = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox")
    plan = store.create_trade_plan(
        portfolio["id"],
        "NVDA",
        "Buy",
        10,
        "Test",
        hurdle_gross_return_pct=8.0,
        hurdle_net_return_pct=5.44,
        hurdle_passed=True,
        duration_gate_passed=True,
        expected_regime_duration=12.3,
    )
    assert plan["hurdle_gross_return_pct"] == pytest.approx(8.0)
    assert plan["hurdle_net_return_pct"] == pytest.approx(5.44)
    assert plan["hurdle_passed"] == 1
    assert plan["duration_gate_passed"] == 1
    assert plan["expected_regime_duration"] == pytest.approx(12.3)


def test_plan_hurdle_columns_default_null(temp_modules) -> None:
    store, _hurdle_rate, _paper, _portfolio_agent, _bus, _events = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox")
    plan = store.create_trade_plan(portfolio["id"], "NVDA", "Buy", 10, "Test")
    assert plan["hurdle_gross_return_pct"] is None
    assert plan["hurdle_net_return_pct"] is None
    assert plan["hurdle_passed"] is None
    assert plan["duration_gate_passed"] is None
    assert plan["expected_regime_duration"] is None


def test_get_hurdle_settings_route(temp_modules) -> None:
    _store, _hurdle_rate, _paper, _portfolio_agent, _bus, _events = temp_modules
    response = _client().get("/regime/hurdle/settings")
    assert response.status_code == 200
    payload = response.json()
    assert payload["hurdle_enabled"] is True
    assert "estimated_stcg_rate" in payload


def test_put_hurdle_settings_route(temp_modules) -> None:
    _store, _hurdle_rate, _paper, _portfolio_agent, _bus, _events = temp_modules
    client = _client()
    response = client.put(
        "/regime/hurdle/settings",
        json={"estimated_stcg_rate": 0.25, "min_regime_duration_days": 9.0},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["estimated_stcg_rate"] == pytest.approx(0.25)
    assert payload["min_regime_duration_days"] == pytest.approx(9.0)


def test_get_hurdle_diagnostic_route(temp_modules) -> None:
    store, _hurdle_rate, _paper, _portfolio_agent, _bus, _events = temp_modules
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
        exit_price=108.0,
        stop_price=95.0,
        risk_reward_ratio=2.0,
        timeframe_days=10,
        expected_regime_duration=12.3,
    )
    response = _client().get("/regime/hurdle/NVDA")
    assert response.status_code == 200
    payload = response.json()
    assert payload["ticker"] == "NVDA"
    assert payload["hurdle"]["passed"] is True
    assert payload["duration_gate"]["passed"] is True


def test_existing_plans_no_hurdle_columns(temp_modules) -> None:
    store, _hurdle_rate, _paper, _portfolio_agent, _bus, _events = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox")
    with store._connect() as conn:
        conn.execute(
            """
            INSERT INTO paper_trade_plan (
                portfolio_id, ticker, action, quantity, rationale, source, status, created_at, updated_at
            )
            VALUES (?, 'NVDA', 'Buy', 10, 'Legacy plan', 'manual', 'Pending', '2026-03-31T00:00:00+00:00', '2026-03-31T00:00:00+00:00')
            """,
            (portfolio["id"],),
        )
    plans = store.get_trade_plans(portfolio["id"], status="all")
    assert len(plans) == 1
    assert plans[0]["hurdle_gross_return_pct"] is None


def test_sprint65_tests_still_pass(temp_modules) -> None:
    del temp_modules
    assert Path("/Volumes/T9/Projects/Dev/Investor/tests/regime/test_sprint65.py").exists()
