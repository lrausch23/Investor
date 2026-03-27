from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.regime.hmm_engine import fit_regime_model


@pytest.fixture(scope="module")
def synthetic_market_frame() -> pd.DataFrame:
    rng = np.random.default_rng(7)
    dates = pd.date_range("2022-01-03", periods=600, freq="B")
    returns = rng.normal(0.0006, 0.018, len(dates))
    price = 100 * np.exp(np.cumsum(returns))
    close = pd.Series(price, index=dates)
    high = close * (1 + rng.uniform(0.001, 0.02, len(dates)))
    low = close * (1 - rng.uniform(0.001, 0.02, len(dates)))
    volume = pd.Series(rng.integers(800_000, 2_500_000, len(dates)), index=dates, dtype=float)
    vix = pd.Series(18 + rng.normal(0, 2, len(dates)).cumsum() * 0.05, index=dates).clip(lower=10)
    yield_10y = pd.Series(3.5 + rng.normal(0, 0.03, len(dates)).cumsum() * 0.05, index=dates).clip(lower=2.0)
    return pd.DataFrame(
        {
            "price": close,
            "high": high,
            "low": low,
            "volume": volume,
            "vix": vix,
            "yield_10y": yield_10y,
        }
    )


@pytest.fixture(scope="module")
def regime_result(synthetic_market_frame: pd.DataFrame):
    return fit_regime_model(
        ticker="TEST",
        market_frame=synthetic_market_frame,
        lookback_window=20,
        training_window=504,
        refit_step=21,
        macro_weighting=False,
    )


def test_transition_matrix_rows_sum_to_one(regime_result) -> None:
    assert np.allclose(regime_result.transition_matrix.sum(axis=1), 1.0, atol=1e-6)


def test_transition_matrix_canonical_order(regime_result) -> None:
    assert regime_result.transition_matrix.shape == (3, 3)
    assert regime_result.state_statistics["state_id"].tolist() == [0, 1, 2]


def test_expected_duration_positive(regime_result) -> None:
    assert regime_result.expected_regime_duration > 0


def test_expected_duration_capped(regime_result) -> None:
    assert regime_result.expected_regime_duration <= 999.0


def test_state_vector_sums_to_one(regime_result) -> None:
    assert np.isclose(regime_result.latest_state_vector.sum(), 1.0, atol=1e-6)


def test_regime_days_positive(regime_result) -> None:
    assert regime_result.regime_days >= 1
