from __future__ import annotations

import csv
import json
from pathlib import Path

import pandas as pd
import pytest

from src.regime.config import SignalThresholds
from src.regime.hmm_engine import empirical_regime_duration_quantiles
from src.regime.pipeline_backtest import PipelineBacktestConfig, PipelineSignal
from src.regime.signals import SignalResult, build_composite_signal, signal_from_forward_curve
from src.regime.threshold_sweep import run_threshold_sweep, write_sweep_rows


def _curve(*, bull: float, neutral: float, bear: float) -> pd.DataFrame:
    return pd.DataFrame(
        [{"day": day, "p_bull": bull, "p_neutral": neutral, "p_bear": bear} for day in range(1, 22)]
    )


def _frame() -> pd.DataFrame:
    dates = pd.bdate_range("2025-01-02", periods=5)
    return pd.DataFrame(
        {
            "open": [100.0, 100.0, 101.0, 102.0, 103.0],
            "high": [101.0, 102.0, 103.0, 104.0, 105.0],
            "low": [99.0, 99.0, 100.0, 101.0, 102.0],
            "price": [100.0, 101.0, 102.0, 103.0, 104.0],
            "volume": [1_000_000.0] * 5,
            "vix": [20.0] * 5,
            "yield_10y": [4.0] * 5,
        },
        index=dates,
    )


class _StubProvider:
    def __call__(self, ticker, date, history, config, previous_regime):
        del ticker, config, previous_regime
        if len(history) > 1:
            action = "Hold"
        else:
            action = "Buy"
        return PipelineSignal(
            date=pd.Timestamp(date).date().isoformat(),
            regime="Bull",
            probability=0.82,
            composite_action=action,
            composite_strength=0.80,
            expected_duration=30.0,
            transition_risk=0.10,
            regime_days=5,
            p_bull_day5=0.80,
            p_bear_day5=0.05,
            p_neutral_day5=0.15,
            forward_action=action,
            signal_source="neutral_bull_tilt" if action == "Buy" else "bull_weakened",
            price_targets={
                "entry_price": 100.0,
                "exit_price": 200.0,
                "target_price": 200.0,
                "stop_price": 50.0,
                "timeframe_days": 30,
            },
        )


def test_empirical_duration_quantiles_exclude_trailing_spell() -> None:
    labels = ["Bull"] * 5 + ["Bear"] * 3 + ["Bull"] * 7
    quantiles = empirical_regime_duration_quantiles(pd.Series(labels))
    assert quantiles["Bull"]["p50"] == pytest.approx(5.0)
    assert quantiles["Bear"]["p50"] == pytest.approx(3.0)


def test_forward_curve_gates_and_modal_neutral_tilt_flags() -> None:
    thresholds = SignalThresholds(use_forward_curve_gates=True, buy_min_p_bull_day5=0.60)
    signal = signal_from_forward_curve(_curve(bull=0.58, neutral=0.32, bear=0.10), "Bull", 0.01, 12, 0.8, thresholds=thresholds)
    assert signal.action == "Hold"

    relaxed = SignalThresholds(use_forward_curve_gates=True, buy_min_p_bull_day5=0.55)
    signal = signal_from_forward_curve(_curve(bull=0.58, neutral=0.32, bear=0.10), "Bull", 0.99, 12, 0.8, thresholds=relaxed)
    assert signal.action == "Buy"

    neutral = SignalThresholds(neutral_tilt_requires_modal=True, neutral_bull_tilt_probability=0.40)
    blocked = signal_from_forward_curve(_curve(bull=0.42, neutral=0.48, bear=0.10), "Neutral", 0.12, 10, 0.8, thresholds=neutral)
    assert blocked.action == "Hold"
    legacy = signal_from_forward_curve(_curve(bull=0.42, neutral=0.48, bear=0.10), "Neutral", 0.12, 10, 0.8)
    assert legacy.action == "Buy"


def test_empirical_duration_flag_replaces_matrix_duration() -> None:
    thresholds = SignalThresholds(use_empirical_durations=True)
    signal = signal_from_forward_curve(
        _curve(bull=0.85, neutral=0.10, bear=0.05),
        "Bull",
        0.02,
        20,
        0.95,
        thresholds=thresholds,
        empirical_duration_quantiles={"Bull": {"p25": 4.0, "p50": 7.0, "p75": 9.0}},
    )
    assert signal.expected_holding_days == 7
    assert signal.expected_duration == pytest.approx(7.0)


def test_composite_ablation_skips_adjustments_and_overrides() -> None:
    forward = SignalResult("Buy", "short", 0.60, 10, "test")
    adjusted = build_composite_signal("Bull", 0.9, forward, "Sell the rally")
    ablated = build_composite_signal("Bull", 0.9, forward, "Sell the rally", adjustments_enabled=False)
    assert adjusted.composite_strength == pytest.approx(0.40)
    assert ablated.composite_strength == pytest.approx(0.60)

    defensive = SignalResult("Strong Sell", "short", 0.80, 10, "test")
    assert build_composite_signal("Bear", 0.9, defensive, "Cover short / tactical bounce").composite_action == "Hold"
    assert build_composite_signal("Bear", 0.9, defensive, "Cover short / tactical bounce", adjustments_enabled=False).composite_action == "Strong Sell"


def test_threshold_sweep_grid_and_round_trip_outputs(tmp_path: Path) -> None:
    grid = {
        "use_empirical_durations": [False, True],
        "composite_adjustments_enabled": [True, False],
    }
    config = PipelineBacktestConfig(
        training_window=0,
        sizing_method="equal_dollar",
        starting_cash=10_000.0,
        enable_hurdle_gate=False,
        enable_duration_gate=False,
        enable_anti_churn_gate=False,
        enable_signal_quality_gate=False,
        enforce_universe_screen=False,
        entry_cost_bps=0.0,
        exit_cost_bps=0.0,
        oos_start="2025-01-06",
    )
    rows = run_threshold_sweep(
        tickers=["TEST"],
        market_frames={"TEST": _frame()},
        grid=grid,
        base_config=config,
        signal_provider_factory=lambda _ticker, _combo: _StubProvider(),
    )
    assert len([row for row in rows if row["ticker"] == "TEST"]) == 4
    assert len([row for row in rows if row["ticker"] == "__AGGREGATE__"]) == 4
    assert {row["full_neutral_tilt_trade_count"] for row in rows if row["ticker"] == "TEST"} == {1.0}

    json_path = tmp_path / "sweep.json"
    csv_path = tmp_path / "sweep.csv"
    write_sweep_rows(rows, json_path=json_path, csv_path=csv_path)
    assert len(json.loads(json_path.read_text())) == len(rows)
    with csv_path.open(newline="", encoding="utf-8") as handle:
        assert len(list(csv.DictReader(handle))) == len(rows)
