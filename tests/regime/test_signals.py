from __future__ import annotations

import numpy as np
import pandas as pd

import pytest

from src.regime.signals import (
    SignalResult,
    build_composite_signal,
    compute_technicals,
    confidence_trajectory,
    forward_regime_curve,
    intra_regime_signal,
    regime_crossover_day,
    signal_from_forward_curve,
)


def _sample_forward_curve(p_bull_day5: float, p_bear_day5: float) -> pd.DataFrame:
    rows = []
    for day in range(1, 22):
        bull = p_bull_day5 if day >= 5 else max(0.0, p_bull_day5 - 0.05)
        bear = p_bear_day5 if day >= 5 else max(0.0, p_bear_day5 - 0.05)
        neutral = max(0.0, 1.0 - bull - bear)
        total = bull + neutral + bear
        rows.append({"day": day, "p_bull": bull / total, "p_neutral": neutral / total, "p_bear": bear / total})
    return pd.DataFrame(rows)


def test_forward_regime_curve_shape() -> None:
    matrix = np.array(
        [
            [0.9, 0.08, 0.02],
            [0.1, 0.8, 0.1],
            [0.05, 0.1, 0.85],
        ]
    )
    curve = forward_regime_curve(matrix, np.array([0.7, 0.2, 0.1]), horizon=12)
    assert list(curve.columns) == ["day", "p_bull", "p_neutral", "p_bear"]
    assert len(curve) == 12
    assert np.allclose(curve[["p_bull", "p_neutral", "p_bear"]].sum(axis=1), 1.0, atol=1e-6)


def test_forward_regime_curve_convergence() -> None:
    stationary = np.array([0.5, 0.3, 0.2])
    matrix = np.tile(stationary, (3, 1))
    curve = forward_regime_curve(matrix, np.array([1.0, 0.0, 0.0]), horizon=100)
    last_row = curve.iloc[-1][["p_bull", "p_neutral", "p_bear"]].to_numpy(dtype=float)
    assert np.allclose(last_row, stationary, atol=1e-6)


def test_regime_crossover_day() -> None:
    no_cross = pd.DataFrame(
        {
            "day": [1, 2, 3],
            "p_bull": [0.05, 0.06, 0.07],
            "p_neutral": [0.05, 0.05, 0.05],
            "p_bear": [0.9, 0.89, 0.88],
        }
    )
    assert regime_crossover_day(no_cross, "Bear", "Bull") is None

    cross = pd.DataFrame(
        {
            "day": list(range(1, 11)),
            "p_bull": [0.1, 0.12, 0.15, 0.18, 0.21, 0.28, 0.4, 0.45, 0.48, 0.5],
            "p_neutral": [0.05] * 10,
            "p_bear": [0.85, 0.83, 0.8, 0.77, 0.74, 0.67, 0.55, 0.5, 0.47, 0.45],
        }
    )
    assert regime_crossover_day(cross, "Bear", "Bull") == 9


def test_signal_strong_buy() -> None:
    signal = signal_from_forward_curve(_sample_forward_curve(0.85, 0.05), "Bull", 0.02, 20, 0.95)
    assert signal.action == "Strong Buy"


def test_signal_strong_sell() -> None:
    signal = signal_from_forward_curve(_sample_forward_curve(0.05, 0.8), "Bear", 0.02, 15, 0.95)
    assert signal.action == "Strong Sell"


def test_signal_hold_neutral() -> None:
    signal = signal_from_forward_curve(_sample_forward_curve(0.32, 0.28), "Neutral", 0.12, 10, 0.7)
    assert signal.action == "Hold"


def test_compute_technicals_shape() -> None:
    index = pd.date_range("2024-01-01", periods=100, freq="D")
    prices = pd.Series(np.linspace(100, 130, 100) + np.sin(np.arange(100)), index=index)
    volume = pd.Series(np.linspace(1_000_000, 1_200_000, 100), index=index)
    high = prices + 1.5
    low = prices - 1.5
    technicals = compute_technicals(prices, volume, high, low)
    expected_cols = {
        "rsi_14",
        "macd_line",
        "macd_signal",
        "macd_histogram",
        "bb_upper",
        "bb_lower",
        "bb_width",
        "bb_pct",
        "atr_14",
        "obv",
    }
    assert set(technicals.columns) == expected_cols
    assert len(technicals) == len(prices)


def test_rsi_range() -> None:
    index = pd.date_range("2024-01-01", periods=100, freq="D")
    prices = pd.Series(np.linspace(100, 140, 100) + np.cos(np.arange(100) / 3), index=index)
    volume = pd.Series(1_000_000 + np.arange(100) * 1_000, index=index)
    technicals = compute_technicals(prices, volume, prices + 1, prices - 1)
    rsi = technicals["rsi_14"].dropna()
    assert ((rsi >= 0) & (rsi <= 100)).all()


def test_intra_regime_signal_bull_dip() -> None:
    technicals = pd.DataFrame(
        [
            {"rsi_14": 35, "bb_pct": 0.3, "macd_histogram": 0.1},
            {"rsi_14": 25, "bb_pct": 0.08, "macd_histogram": 0.05},
        ]
    )
    assert intra_regime_signal(technicals, "Bull") == "Buy the dip"


def test_intra_regime_signal_bear_rally() -> None:
    technicals = pd.DataFrame(
        [
            {"rsi_14": 65, "bb_pct": 0.7, "macd_histogram": -0.1},
            {"rsi_14": 75, "bb_pct": 0.95, "macd_histogram": -0.2},
        ]
    )
    assert intra_regime_signal(technicals, "Bear") == "Sell the rally"


def test_composite_agreement_boost() -> None:
    forward_signal = SignalResult("Buy", "short", 0.5, 10, "Bull remains firm.")
    composite = build_composite_signal("Bull", 0.9, forward_signal, "Buy the dip")
    assert composite.composite_strength == 0.65


def test_composite_disagreement_penalty() -> None:
    forward_signal = SignalResult("Buy", "short", 0.6, 10, "Bull remains firm.")
    composite = build_composite_signal("Bull", 0.9, forward_signal, "Sell the rally")
    assert composite.composite_strength == pytest.approx(0.4)


def test_composite_bear_tactical_override() -> None:
    forward_signal = SignalResult("Strong Sell", "short", 0.8, 10, "Bear remains firm.")
    composite = build_composite_signal("Bear", 0.9, forward_signal, "Cover short / tactical bounce")
    assert composite.composite_action == "Hold"


def test_confidence_trajectory_declining() -> None:
    series = pd.Series(np.linspace(0.9, 0.4, 12))
    trajectory = confidence_trajectory(series, window=10)
    assert trajectory.trend == "declining"
    assert trajectory.days_declining > 0
    assert trajectory.short_ma_latest < trajectory.long_ma_latest


def test_confidence_trajectory_stable() -> None:
    series = pd.Series([0.75] * 12)
    trajectory = confidence_trajectory(series, window=10)
    assert trajectory.trend == "stable"
