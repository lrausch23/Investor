from __future__ import annotations

from types import SimpleNamespace

import numpy as np
import pandas as pd

from src.regime import pipeline_backtest
from src.regime.pipeline_backtest import PipelineBacktestConfig, PipelineSignal, _ProductionSignalProvider, run_pipeline_backtest
from src.regime.stress_windows import StressWindow


def _frame(days: int = 80) -> pd.DataFrame:
    dates = pd.bdate_range("2024-01-02", periods=days)
    prices = [100.0 + index * 0.03 + float(np.sin(index / 3.0)) for index in range(days)]
    return pd.DataFrame(
        {
            "open": prices,
            "high": [price * 1.01 for price in prices],
            "low": [price * 0.99 for price in prices],
            "price": prices,
            "volume": [2_000_000.0 + index * 1000.0 for index in range(days)],
            "vix": [20.0] * days,
            "yield_10y": [4.0] * days,
        },
        index=dates,
    )


def test_pipeline_result_includes_stress_window_metrics(monkeypatch) -> None:
    monkeypatch.setattr(
        pipeline_backtest,
        "get_stress_windows",
        lambda: [StressWindow("unit_window", "Unit Window", "2024-01-10", "2024-01-31")],
    )

    def provider(ticker, date, history, config, previous_regime):
        del ticker, history, config, previous_regime
        action = "Buy" if date == pd.Timestamp("2024-01-10") else "Hold"
        regime = "Bear" if date >= pd.Timestamp("2024-01-17") else "Bull"
        return PipelineSignal(
            date=date.date().isoformat(),
            regime=regime,
            probability=0.8,
            composite_action=action,
            composite_strength=0.9,
            expected_duration=21.0,
            transition_risk=0.1,
            regime_days=5,
            price_targets={"entry_price": 100.0, "target_price": 130.0, "stop_price": 90.0, "timeframe_days": 21},
        )

    config = PipelineBacktestConfig(
        training_window=0,
        enable_hurdle_gate=False,
        enable_duration_gate=False,
        enable_anti_churn_gate=False,
        enable_signal_quality_gate=False,
        enforce_universe_screen=False,
    )
    result = run_pipeline_backtest("TEST", _frame(), config=config, signal_provider=provider)

    assert result.stress_windows[0]["key"] == "unit_window"
    assert result.stress_windows[0]["strategy_total_return"] is not None
    assert result.stress_windows[0]["benchmark_total_return"] is not None
    assert result.stress_windows[0]["days_to_bear_flag"] == 7


def test_production_signal_provider_caps_refit_history(monkeypatch) -> None:
    frame = _frame(days=180)
    seen_lengths: list[int] = []

    def fake_fit_regime_model(**kwargs):
        market_frame = kwargs["market_frame"]
        seen_lengths.append(len(market_frame))
        latest_price = float(market_frame["price"].iloc[-1])
        return SimpleNamespace(
            transition_matrix=np.array([[0.9, 0.08, 0.02], [0.1, 0.8, 0.1], [0.05, 0.15, 0.8]]),
            latest_state_vector=np.array([0.8, 0.15, 0.05]),
            latest_label="Bull",
            transition_risk=0.1,
            expected_regime_duration=21.0,
            latest_probability=0.8,
            latest_price=latest_price,
            recent_state_mean_return=0.001,
            regime_days=10,
            empirical_duration_quantiles=None,
            seed_agreement=1.0,
            regime_ambiguous=False,
        )

    monkeypatch.setattr(pipeline_backtest, "fit_regime_model", fake_fit_regime_model)
    provider = _ProductionSignalProvider()
    config = PipelineBacktestConfig(training_window=120, lookback_window=5, refit_step=3, enforce_universe_screen=False)
    signal = provider("TEST", frame.index[-1], frame, config, previous_regime=None)

    assert signal is not None
    assert seen_lengths == [128]
