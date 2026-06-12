from __future__ import annotations

import datetime as dt
import importlib
import json
from pathlib import Path

import pytest


@pytest.fixture()
def temp_modules(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("HMM_DATA_DIR", str(tmp_path))
    import src.regime.agent_dashboard as agent_dashboard
    import src.regime.decision_constants as decision_constants
    import src.regime.decision_health as decision_health
    import src.regime.paper_trading as paper_trading
    import src.regime.persistence as store

    store = importlib.reload(store)
    store.DB_PATH = tmp_path / "regime_watch.db"
    decision_constants = importlib.reload(decision_constants)
    decision_health = importlib.reload(decision_health)
    paper_trading = importlib.reload(paper_trading)
    agent_dashboard = importlib.reload(agent_dashboard)
    return store, paper_trading, decision_constants, decision_health, agent_dashboard


def _save_snapshot(store, ticker: str, snapshot_date: str) -> None:
    store.save_signal_snapshot(
        ticker=ticker,
        snapshot_date=snapshot_date,
        action="Buy",
        regime_label="Bull",
        regime_probability=0.90,
        composite_strength=0.80,
        benchmark="SPY",
        current_price=120.0,
        entry_price=119.0,
        exit_price=150.0,
        stop_price=90.0,
        risk_reward_ratio=1.0,
        timeframe_days=21,
        expected_regime_duration=30.0,
    )


def test_decision_constants_version_changes_when_constant_changes(temp_modules, monkeypatch: pytest.MonkeyPatch) -> None:
    _store, _paper, constants, _health, _dashboard = temp_modules
    baseline = constants.decision_constants_version()

    monkeypatch.setattr(constants, "COMPOSITE_AGREEMENT_BOOST", constants.COMPOSITE_AGREEMENT_BOOST + 0.01)

    assert constants.decision_constants_version() != baseline


def test_create_trade_plan_stamps_decision_constants_version(temp_modules) -> None:
    store, _paper, constants, _health, _dashboard = temp_modules
    portfolio = store.create_paper_portfolio("Sandbox")

    plan = store.create_trade_plan(portfolio["id"], "NVDA", "Buy", 1, "test")

    assert plan["decision_constants_version"] == constants.decision_constants_version()


def test_split_packages_reexport_legacy_public_names(temp_modules, tmp_path: Path) -> None:
    store, paper, _constants, _health, _dashboard = temp_modules
    import src.regime.paper_trading.planning as planning
    import src.regime.persistence.plans as plans

    assert callable(store.create_trade_plan)
    assert callable(plans.create_trade_plan)
    assert callable(paper.generate_buy_plans)
    assert callable(planning.generate_buy_plans)

    store.DB_PATH = tmp_path / "forwarded.db"
    assert store.core.DB_PATH == tmp_path / "forwarded.db"

    paper._lookup_atr = lambda ticker: 9.0
    assert paper.core._lookup_atr("NVDA") == 9.0


def test_decision_health_counts_and_alert_dedupes(temp_modules) -> None:
    store, _paper, _constants, health, _dashboard = temp_modules
    store.set_setting("decision_health_fallback_alert_threshold", "1")
    today = dt.datetime.now(dt.timezone.utc).date().isoformat()

    health.record_fallback("unit.component", "first")
    health.record_fallback("unit.component", "second")
    health.record_fallback("unit.component", "third")

    assert store.get_setting(f"decision_health:{today}:unit.component:count") == "3"
    alerts = store.get_alerts(alert_type="decision_health", limit=10)
    assert len(alerts) == 1
    assert "unit.component" in alerts[0]["title"]


def test_entry_signal_freshness_defaults_to_three_days_and_is_setting_gated(temp_modules, monkeypatch: pytest.MonkeyPatch) -> None:
    store, paper, _constants, _health, _dashboard = temp_modules
    theme = store.create_theme("AI Enablers", conviction=5)
    stale_date = (dt.datetime.now(dt.timezone.utc).date() - dt.timedelta(days=4)).isoformat()
    _save_snapshot(store, "NVDA", stale_date)

    def watchlist(status=None):
        del status
        return [
            {
                "id": 1,
                "ticker": "NVDA",
                "theme_id": int(theme["id"]),
                "suggested_role": "Critical-Path",
                "suggested_entry_price": 119.0,
                "suggested_exit_price": 150.0,
                "suggested_stop_price": 90.0,
                "regime_label": "Bull",
                "regime_probability": 0.90,
                "crowd_score": 20,
                "status": "Entry Signal",
                "discovery_rationale": "test candidate",
            }
        ]

    monkeypatch.setattr(paper, "get_watchlist", watchlist)
    monkeypatch.setattr(paper, "_batch_current_prices", lambda tickers: {"NVDA": 120.0})
    monkeypatch.setattr(paper, "_lookup_atr", lambda ticker: 5.0)
    monkeypatch.setattr(paper, "_lookup_beta", lambda ticker: 1.0)
    portfolio = store.create_paper_portfolio("Freshness", 25_000.0, broker_type="ibkr")

    assert paper.generate_buy_plans(int(portfolio["id"])) == []

    store.set_setting("entry_signal_max_age_days", "7")
    plans = paper.generate_buy_plans(int(portfolio["id"]))
    assert len(plans) == 1
    assert plans[0]["ticker"] == "NVDA"


def test_llm_plan_outcome_records_audit_and_summary(temp_modules) -> None:
    store, paper, _constants, _health, dashboard = temp_modules
    portfolio = store.create_paper_portfolio("LLM Attribution")
    entry_date = dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()
    plan = store.create_trade_plan(
        portfolio["id"],
        "NVDA",
        "Buy",
        10,
        "LLM confirmed entry",
        proposed_price=100.0,
        llm_used=True,
        llm_influenced=True,
        llm_influence="confirmed",
        llm_provider="Ollama",
        llm_model="model-a",
        llm_model_display="Ollama: model-a",
        llm_verdict="confirm",
        llm_confidence=0.82,
    )
    store.update_trade_plan_status(
        int(plan["id"]),
        "Executed",
        executed_at=entry_date,
        execution_price=100.0,
        filled_quantity=10,
    )
    position = store.open_paper_position(portfolio["id"], "NVDA", 10, 100.0, entry_date)
    position = {**position, "exit_date": (dt.datetime.now(dt.timezone.utc) + dt.timedelta(days=5)).isoformat()}

    outcome = paper.record_trade_outcome(int(portfolio["id"]), position, 110.0)

    assert outcome["outcome"] == "win"
    events = store.get_audit_trail(portfolio_id=int(portfolio["id"]), event_type="llm_attribution", days=30)
    assert len(events) == 1
    details = json.loads(str(events[0]["details"]))
    assert details["verdict"] == "confirm"
    assert details["realized_net_pnl"] == pytest.approx(100.0)
    summary = store.get_llm_attribution_summary(days=30)
    assert summary[0]["verdict"] == "confirm"
    assert summary[0]["trade_count"] == 1
    assert summary[0]["win_rate"] == pytest.approx(100.0)
    payload = dashboard.compute_agent_portfolio_dashboard(int(portfolio["id"]))
    assert payload["llm_outcome_attribution"][0]["verdict"] == "confirm"
