from __future__ import annotations

import importlib
import json
import time
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.app.routes import regime as regime_route


@pytest.fixture
def temp_modules(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("HMM_DATA_DIR", str(tmp_path))
    import src.regime.persistence as store
    import src.regime.scenarios as scenarios
    import src.regime.stress_test as stress_test
    import src.regime.calibration as calibration

    store = importlib.reload(store)
    store.DB_PATH = tmp_path / "regime_watch.db"
    scenarios = importlib.reload(scenarios)
    stress_test = importlib.reload(stress_test)
    calibration = importlib.reload(calibration)
    return store, scenarios, stress_test, calibration


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(regime_route.router)
    app.dependency_overrides[regime_route.require_actor] = lambda: "tester"
    return TestClient(app)


def _daily_bars(start=None, end=None, symbol="SPY") -> pd.DataFrame:
    index = pd.date_range(start=start or "2019-01-01", end=end or "2020-12-31", freq="B")
    base = pd.Series(range(len(index)), index=index, dtype=float)
    prices = 100.0 + (base * 0.2)
    if symbol in {"^VIX", "^TNX"}:
        prices = 20.0 + (base * 0.01)
    return pd.DataFrame(
        {
            "Open": prices,
            "High": prices + 1.0,
            "Low": prices - 1.0,
            "Close": prices,
            "Volume": 1_000_000 + base,
        },
        index=index,
    )


def _patch_basic_replay(monkeypatch: pytest.MonkeyPatch, stress_test) -> None:
    monkeypatch.setattr(stress_test, "download_daily_bars", lambda ticker, **kwargs: _daily_bars(kwargs.get("start"), kwargs.get("end"), ticker))
    monkeypatch.setattr(
        stress_test,
        "fit_regime_model",
        lambda ticker, market_frame, training_window=504, refit_step=21: SimpleNamespace(
            ticker=ticker,
            latest_label="Bull",
            latest_state_id=0,
            latest_probability=0.8,
            latest_price=float(market_frame["price"].iloc[-1]),
            latest_state_vector=(0.8, 0.15, 0.05),
            transition_matrix=((0.9, 0.08, 0.02), (0.1, 0.8, 0.1), (0.05, 0.1, 0.85)),
            recent_state_mean_return=0.01,
            expected_regime_duration=12.0,
            transition_risk=0.1,
            price_frame=market_frame,
            regime_days=14,
        ),
    )
    monkeypatch.setattr(stress_test, "_safe_meta_score", lambda ticker, regime: 0.72)


def _ticker_result(stress_test, **overrides):
    payload = {
        "ticker": "NVDA",
        "trades": [{"entry_date": "2020-01-01", "exit_date": "2020-01-05", "return": 0.05}],
        "total_return": 0.12,
        "max_drawdown": -0.08,
        "sharpe_ratio": 1.1,
        "win_rate": 1.0,
        "avg_win": 0.05,
        "avg_loss": None,
        "buy_and_hold_return": 0.09,
        "equity_curve": [{"date": "2020-01-01", "equity": 100000.0}, {"date": "2020-01-05", "equity": 112000.0}],
        "stop_outs": 0,
        "round_trip_count": 1,
        "recovery_days": 5,
        "churn_vetoes": 0,
        "hurdle_vetoes": 0,
        "duration_vetoes": 0,
        "fundamental_vetoes": 0,
        "ltcg_overrides_triggered": 0,
        "ltcg_tax_savings_total": 0.0,
        "ltcg_lots_protected": 0,
        "net_return_after_tax": 0.08,
        "ml_signals_generated": 1,
        "ml_avg_score": 0.72,
    }
    payload.update(overrides)
    return stress_test.TickerResult(**payload)


def test_scenario_library_has_five_entries(temp_modules) -> None:
    _store, scenarios, _stress, _calibration = temp_modules
    assert len(scenarios.SCENARIOS) == 5
    assert all(item.pre_buffer_days == 504 for item in scenarios.SCENARIOS.values())


def test_get_scenario_valid_and_invalid(temp_modules) -> None:
    _store, scenarios, _stress, _calibration = temp_modules
    assert scenarios.get_scenario("gfc_2008").name == "2008 Global Financial Crisis"
    with pytest.raises(ValueError):
        scenarios.get_scenario("missing")


def test_stress_test_config_defaults(temp_modules) -> None:
    _store, _scenarios, stress_test, _calibration = temp_modules
    config = stress_test.StressTestConfig(scenario_id="gfc_2008")
    assert config.training_window == 504
    assert config.hurdle_rate_enabled is True
    assert config.anti_churn_enabled is True


def test_anti_churn_tracker_behaviour(temp_modules) -> None:
    _store, _scenarios, stress_test, _calibration = temp_modules
    tracker = stress_test._AntiChurnTracker(max_round_trips=2, cooldown_days=30)
    now = pd.Timestamp("2020-06-30").to_pydatetime()
    tracker.record_sell("NVDA", pd.Timestamp("2020-06-20").to_pydatetime())
    assert tracker.is_restricted("NVDA", now) is False
    tracker.record_sell("NVDA", pd.Timestamp("2020-06-25").to_pydatetime())
    assert tracker.is_restricted("NVDA", now) is True
    assert tracker.is_restricted("NVDA", pd.Timestamp("2020-08-10").to_pydatetime()) is False


def test_ltcg_tracker_behaviour(temp_modules) -> None:
    _store, _scenarios, stress_test, _calibration = temp_modules
    tracker = stress_test._LTCGTracker(trigger_days=16, max_risk_atr=2.0)
    tracker.open_lot("NVDA", 100.0, pd.Timestamp("2025-01-01").to_pydatetime(), 10)
    early = tracker.should_override_exit("NVDA", pd.Timestamp("2025-10-01").to_pydatetime(), 120.0, 2.0)
    assert early[0] is False
    near = tracker.should_override_exit("NVDA", pd.Timestamp("2025-12-25").to_pydatetime(), 120.0, 2.0)
    assert near[0] is True
    assert tracker.estimate_tax_savings("NVDA", 120.0) > 0
    tracker.close_lot("NVDA")
    assert tracker.close_lot("NVDA") is None


def test_run_stress_test_returns_result(temp_modules, monkeypatch: pytest.MonkeyPatch) -> None:
    _store, _scenarios, stress_test, _calibration = temp_modules
    monkeypatch.setattr(stress_test, "_run_ticker_replay", lambda ticker, scenario_start, scenario_end, config: _ticker_result(stress_test))
    monkeypatch.setattr(stress_test, "_benchmark_return", lambda benchmark, start, end: 0.03)
    result = stress_test.run_stress_test(stress_test.StressTestConfig(scenario_id="covid_2020", tickers=["NVDA"], training_window=120, refit_step=5))
    assert result.scenario_id == "covid_2020"
    assert len(result.ticker_results) == 1
    assert result.total_trades >= 1
    assert result.benchmark_return is not None


def test_stress_test_fundamental_veto_skips_ticker(temp_modules, monkeypatch: pytest.MonkeyPatch) -> None:
    _store, _scenarios, stress_test, _calibration = temp_modules
    _patch_basic_replay(monkeypatch, stress_test)
    monkeypatch.setattr(stress_test, "run_fundamental_gate", lambda *args, **kwargs: SimpleNamespace(passed=False))
    result = stress_test.run_stress_test(stress_test.StressTestConfig(scenario_id="covid_2020", tickers=["NVDA"], training_window=120))
    row = result.ticker_results[0]
    assert row.fundamental_vetoes == 1
    assert row.trades == []


def test_stress_test_hurdle_veto_counted(temp_modules, monkeypatch: pytest.MonkeyPatch) -> None:
    _store, _scenarios, stress_test, _calibration = temp_modules
    monkeypatch.setattr(stress_test, "_run_ticker_replay", lambda ticker, scenario_start, scenario_end, config: _ticker_result(stress_test, trades=[], total_return=0.0, hurdle_vetoes=3, ml_signals_generated=0, ml_avg_score=None))
    result = stress_test.run_stress_test(stress_test.StressTestConfig(scenario_id="covid_2020", tickers=["NVDA"], training_window=120, refit_step=5))
    assert result.ticker_results[0].hurdle_vetoes > 0


def test_stress_test_duration_veto_counted(temp_modules, monkeypatch: pytest.MonkeyPatch) -> None:
    _store, _scenarios, stress_test, _calibration = temp_modules
    monkeypatch.setattr(stress_test, "_run_ticker_replay", lambda ticker, scenario_start, scenario_end, config: _ticker_result(stress_test, trades=[], total_return=0.0, duration_vetoes=2, ml_signals_generated=0, ml_avg_score=None))
    result = stress_test.run_stress_test(stress_test.StressTestConfig(scenario_id="covid_2020", tickers=["NVDA"], training_window=120, refit_step=5))
    assert result.ticker_results[0].duration_vetoes > 0


def test_stress_test_anti_churn_veto_counted(temp_modules, monkeypatch: pytest.MonkeyPatch) -> None:
    _store, _scenarios, stress_test, _calibration = temp_modules
    monkeypatch.setattr(stress_test, "_run_ticker_replay", lambda ticker, scenario_start, scenario_end, config: _ticker_result(stress_test, trades=[], total_return=0.0, churn_vetoes=1, ml_signals_generated=0, ml_avg_score=None))
    result = stress_test.run_stress_test(stress_test.StressTestConfig(scenario_id="covid_2020", tickers=["NVDA"], training_window=120, refit_step=5, max_round_trips=2, anti_churn_cooldown_days=30))
    assert result.ticker_results[0].churn_vetoes > 0


def test_stress_test_ltcg_override_counted(temp_modules, monkeypatch: pytest.MonkeyPatch) -> None:
    _store, _scenarios, stress_test, _calibration = temp_modules
    monkeypatch.setattr(stress_test, "_run_ticker_replay", lambda ticker, scenario_start, scenario_end, config: _ticker_result(stress_test, trades=[], total_return=0.0, ltcg_overrides_triggered=2, ltcg_tax_savings_total=84.0, ltcg_lots_protected=2, ml_signals_generated=0, ml_avg_score=None))
    result = stress_test.run_stress_test(stress_test.StressTestConfig(scenario_id="gfc_2008", tickers=["NVDA"], training_window=120, refit_step=5))
    assert result.ticker_results[0].ltcg_overrides_triggered > 0
    assert result.ticker_results[0].ltcg_tax_savings_total > 0


def test_stress_test_tax_simulation_and_disable_all_guardrails(temp_modules, monkeypatch: pytest.MonkeyPatch) -> None:
    _store, _scenarios, stress_test, _calibration = temp_modules
    _patch_basic_replay(monkeypatch, stress_test)
    actions = iter(["Buy", "Sell", "Buy", "Sell", "Buy", "Sell"])
    monkeypatch.setattr(stress_test, "signal_from_forward_curve", lambda *args, **kwargs: SimpleNamespace(action="Buy", strength=0.6, expected_holding_days=10, rationale="cycle"))
    monkeypatch.setattr(stress_test, "intra_regime_signal", lambda *args, **kwargs: "Buy")
    monkeypatch.setattr(stress_test, "build_composite_signal", lambda *args, **kwargs: SimpleNamespace(composite_action=next(actions, "Hold"), composite_strength=0.6, forward_signal=SimpleNamespace(action="Buy", strength=0.6, expected_holding_days=10, rationale="cycle"), technical_signal="Buy"))
    monkeypatch.setattr(stress_test, "compute_price_targets", lambda **kwargs: SimpleNamespace(current_price=kwargs["current_price"], entry_price=kwargs["current_price"], exit_price=kwargs["current_price"] * 1.12, stop_price=kwargs["current_price"] * 0.96, risk_reward_ratio=2.0, timeframe_days=10, atr_value=1.0))
    guarded = stress_test.run_stress_test(stress_test.StressTestConfig(scenario_id="covid_2020", tickers=["NVDA"], training_window=120, refit_step=5, hurdle_min_net_return_pct=15.0))
    unguarded = stress_test.run_stress_test(stress_test.StressTestConfig(scenario_id="covid_2020", tickers=["NVDA"], training_window=120, refit_step=5, fundamental_gate_enabled=False, hurdle_rate_enabled=False, duration_gate_enabled=False, anti_churn_enabled=False, ltcg_override_enabled=False))
    assert unguarded.total_trades >= guarded.total_trades
    assert unguarded.ticker_results[0].net_return_after_tax <= unguarded.ticker_results[0].total_return


def test_calibration_returns_five_results(temp_modules, monkeypatch: pytest.MonkeyPatch) -> None:
    _store, _scenarios, _stress_test, calibration = temp_modules
    monkeypatch.setattr(
        calibration,
        "run_stress_test",
        lambda config: SimpleNamespace(
            portfolio_total_return=0.10 if getattr(config, "hurdle_rate_enabled", True) else 0.04,
            portfolio_max_drawdown=-0.12 if getattr(config, "hurdle_rate_enabled", True) else -0.09,
            portfolio_sharpe=1.1 if getattr(config, "hurdle_rate_enabled", True) else 0.8,
        ),
    )
    results = calibration.run_guardrail_calibration("covid_2020")
    assert len(results) == 5


def test_calibration_recommendation_reduce(temp_modules, monkeypatch: pytest.MonkeyPatch) -> None:
    _store, _scenarios, _stress_test, calibration = temp_modules
    responses = iter([
        SimpleNamespace(portfolio_total_return=0.00, portfolio_max_drawdown=-0.10, portfolio_sharpe=0.4),
        SimpleNamespace(portfolio_total_return=0.08, portfolio_max_drawdown=-0.12, portfolio_sharpe=0.8),
    ] + [SimpleNamespace(portfolio_total_return=0.05, portfolio_max_drawdown=-0.10, portfolio_sharpe=0.5)] * 8)
    monkeypatch.setattr(calibration, "run_stress_test", lambda config: next(responses))
    result = calibration.run_guardrail_calibration("covid_2020")[0]
    assert result.recommendation == "reduce"


def test_save_and_get_stress_test_result(temp_modules) -> None:
    store, _scenarios, _stress_test, _calibration = temp_modules
    result_id = store.save_stress_test_result("covid_2020", json.dumps({"scenario_id": "covid_2020"}), json.dumps({"portfolio_total_return": 0.1}))
    row = store.get_stress_test_result_by_id(result_id)
    assert row is not None
    assert row["scenario_id"] == "covid_2020"


def test_list_results_and_mark_status(temp_modules) -> None:
    store, _scenarios, _stress_test, _calibration = temp_modules
    result_id = store.save_stress_test_result("gfc_2008", "{}", "{}", status="running")
    store.mark_stress_test_status(result_id, "completed", result_json=json.dumps({"portfolio_total_return": 0.2}))
    rows = store.get_stress_test_results("gfc_2008")
    assert len(rows) == 1
    assert rows[0]["status"] == "completed"


def test_route_scenarios_and_run_status(temp_modules, monkeypatch: pytest.MonkeyPatch) -> None:
    store, _scenarios, stress_test, _calibration = temp_modules
    monkeypatch.setattr(stress_test, "run_stress_test", lambda config: stress_test.StressTestResult(
        scenario_id=config.scenario_id,
        scenario_name="Scenario",
        config=config,
        ticker_results=[],
        portfolio_total_return=0.1,
        portfolio_max_drawdown=-0.05,
        portfolio_sharpe=1.0,
        total_trades=1,
        total_stop_outs=0,
        total_round_trips=1,
        worst_recovery_days=None,
        total_churn_vetoes=0,
        total_hurdle_vetoes=0,
        total_duration_vetoes=0,
        total_fundamental_vetoes=0,
        total_ltcg_overrides=0,
        total_ltcg_tax_savings=0.0,
        net_portfolio_return_after_tax=0.08,
        benchmark_return=0.02,
        alpha=0.08,
        started_at="2026-03-31T00:00:00+00:00",
        completed_at="2026-03-31T00:00:01+00:00",
        duration_seconds=1.0,
    ))
    client = _client()
    response = client.get("/regime/stress-test/scenarios")
    assert response.status_code == 200
    assert len(response.json()["scenarios"]) == 5
    run_response = client.post("/regime/stress-test/run", json={"scenario_id": "covid_2020"})
    assert run_response.status_code == 200
    result_id = run_response.json()["result_id"]
    time.sleep(0.1)
    status_response = client.get(f"/regime/stress-test/result/{result_id}")
    assert status_response.status_code == 200
    assert status_response.json()["status"] == "completed"
    history = client.get("/regime/stress-test/results?limit=5")
    assert history.status_code == 200
    assert any(int(row["id"]) == int(result_id) for row in history.json()["results"])


def test_route_calibration_result(temp_modules, monkeypatch: pytest.MonkeyPatch) -> None:
    _store, _scenarios, _stress_test, calibration = temp_modules
    monkeypatch.setattr(
        calibration,
        "run_guardrail_calibration",
        lambda scenario_id, ticker=None: [calibration.GuardrailCalibrationResult(
            guardrail_name="hurdle_rate",
            enabled_return=0.1,
            disabled_return=0.08,
            enabled_drawdown=-0.1,
            disabled_drawdown=-0.12,
            enabled_sharpe=1.0,
            disabled_sharpe=0.8,
            impact_pct=2.0,
            recommendation="keep",
        )],
    )
    client = _client()
    run_response = client.post("/regime/stress-test/calibrate", json={"scenario_id": "covid_2020"})
    assert run_response.status_code == 200
    result_id = run_response.json()["result_id"]
    time.sleep(0.1)
    status_response = client.get(f"/regime/stress-test/calibration/{result_id}")
    assert status_response.status_code == 200
    payload = status_response.json()
    assert payload["status"] == "completed"
    assert payload["result"][0]["guardrail_name"] == "hurdle_rate"
