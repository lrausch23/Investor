from __future__ import annotations

from dataclasses import fields

import numpy as np
import pandas as pd
import pytest

from src.regime.backtest import run_backtest
from src.regime.diagnostics import fit_probability_calibrator
from src.regime.exceptions import DataFetchError, InsufficientDataError, InvestorError, ModelFittingError, PersistenceError, RegimeError
from src.regime.hmm_engine import STATE_META, _rank_state_labels, build_features, fit_regime_model
from src.regime.llm_layer import QualitativeAssessment, build_qualitative_assessment
from src.regime.signals import compute_unified_confidence


@pytest.fixture()
def market_frame() -> pd.DataFrame:
    rng = np.random.default_rng(7)
    dates = pd.date_range("2022-01-03", periods=620, freq="B")
    returns = rng.normal(0.0007, 0.017, len(dates))
    prices = 100 * np.exp(np.cumsum(returns))
    close = pd.Series(prices, index=dates)
    high = close * (1 + rng.uniform(0.001, 0.02, len(dates)))
    low = close * (1 - rng.uniform(0.001, 0.02, len(dates)))
    volume = pd.Series(rng.integers(800_000, 2_500_000, len(dates)), index=dates, dtype=float)
    vix = pd.Series(18 + rng.normal(0, 0.05, len(dates)).cumsum(), index=dates).clip(lower=10)
    yield_10y = pd.Series(3.5 + rng.normal(0, 0.01, len(dates)).cumsum(), index=dates).clip(lower=2)
    return pd.DataFrame({"price": close, "high": high, "low": low, "volume": volume, "vix": vix, "yield_10y": yield_10y})


def test_rank_state_labels_uses_training_statistics_only() -> None:
    features = pd.DataFrame(
        {
            "return": [0.01, 0.012, 0.0002, 0.0001, -0.01, -0.012],
            "volatility": [0.10, 0.11, 0.14, 0.15, 0.30, 0.32],
            "trend": [0.03, 0.04, 0.00, 0.00, -0.03, -0.04],
            "volume_zscore": [0.1, 0.2, 0.0, 0.0, -0.1, -0.2],
            "vix_change": [-0.2, -0.1, 0.0, 0.0, 0.2, 0.3],
            "yield_10y_change": [-0.02, -0.01, 0.0, 0.0, 0.02, 0.03],
        }
    )
    states = pd.Series([0, 0, 1, 1, 2, 2], name="hidden_state")
    state_map, canonical_map, stats = _rank_state_labels(states, features)
    assert state_map[0] == "Bull"
    assert state_map[2] == "Bear"
    assert stats.columns.tolist()[0:3] == ["state_id", "label", "regime_score"]
    assert canonical_map[0] == STATE_META["Bull"]["state_id"]


def test_fit_regime_model_uses_diagonal_covariance(market_frame: pd.DataFrame) -> None:
    result = fit_regime_model("TEST", market_frame, training_window=504, refit_step=21)
    assert result.model.covariance_type == "diag"


def test_build_features_removes_collinear_level_columns(market_frame: pd.DataFrame) -> None:
    features = build_features(market_frame)
    assert "vix_level" not in features.columns
    assert "yield_10y_level" not in features.columns
    assert {"vix_change", "yield_10y_change"} <= set(features.columns)


def test_regime_error_hierarchy() -> None:
    assert issubclass(DataFetchError, RegimeError)
    assert issubclass(InsufficientDataError, RegimeError)
    assert issubclass(ModelFittingError, RegimeError)
    assert issubclass(PersistenceError, InvestorError)
    assert not issubclass(PersistenceError, RegimeError)


def test_qualitative_assessment_has_source_field(monkeypatch) -> None:
    monkeypatch.setattr("src.regime.llm_layer.fetch_recent_news", lambda *_args, **_kwargs: [])
    result = build_qualitative_assessment(
        ticker="NVDA",
        regime_signal="Bull",
        state_name="Bull",
        latest_probability=0.82,
        frontier_enabled=False,
    )
    field_names = {field.name for field in fields(QualitativeAssessment)}
    assert "source" in field_names
    assert result.source == "vader_fallback"


def test_oos_backtest_splits_data_correctly(monkeypatch) -> None:
    dates = pd.date_range("2021-01-01", periods=400, freq="B")
    market = pd.DataFrame(
        {
            "price": np.linspace(100, 150, len(dates)),
            "high": np.linspace(101, 151, len(dates)),
            "low": np.linspace(99, 149, len(dates)),
            "volume": np.linspace(1_000_000, 1_500_000, len(dates)),
            "vix": np.linspace(18, 22, len(dates)),
            "yield_10y": np.linspace(3.0, 4.0, len(dates)),
        },
        index=dates,
    )
    monkeypatch.setattr("src.regime.backtest.download_market_frame", lambda **_kwargs: type("Series", (), {"frame": market})())

    class FakeRegime:
        latest_label = "Bull"
        transition_matrix = np.array([[0.9, 0.1, 0.0], [0.1, 0.8, 0.1], [0.0, 0.1, 0.9]])
        latest_state_vector = np.array([0.8, 0.2, 0.0])
        transition_risk = 0.1
        expected_regime_duration = 15.0
        latest_probability = 0.8

    monkeypatch.setattr("src.regime.backtest.fit_regime_model", lambda **_kwargs: FakeRegime())
    monkeypatch.setattr("src.regime.backtest.compute_technicals", lambda *_args, **_kwargs: pd.DataFrame({"rsi_14": [50.0], "bb_pct": [0.5], "macd_histogram": [0.1]}, index=[dates[-1]]))
    monkeypatch.setattr("src.regime.backtest.forward_regime_curve", lambda *_args, **_kwargs: pd.DataFrame([{"day": 1, "p_bull": 0.8, "p_neutral": 0.2, "p_bear": 0.0}] * 21))
    monkeypatch.setattr("src.regime.backtest.signal_from_forward_curve", lambda *_args, **_kwargs: type("Signal", (), {"action": "Buy", "strength": 0.8, "expected_holding_days": 10, "rationale": "test"})())
    monkeypatch.setattr("src.regime.backtest.intra_regime_signal", lambda *_args, **_kwargs: "Buy the dip")
    monkeypatch.setattr("src.regime.backtest.build_composite_signal", lambda *_args, **_kwargs: type("Composite", (), {"composite_action": "Buy", "composite_strength": 0.8})())

    result = run_backtest("TEST", period="5y", refit_step=21, oos_fraction=0.2)
    assert result.oos_total_return is not None
    assert result.oos_win_rate is not None


def test_unified_confidence_score_computation() -> None:
    calibrator = fit_probability_calibrator([0.2, 0.4, 0.6, 0.8], [0.0, 0.0, 1.0, 1.0])
    score = compute_unified_confidence(0.75, 0.6, calibrator=calibrator)
    assert 0.0 <= score.value <= 100.0
    assert score.label in {"Low", "Medium", "High", "Very High"}
    assert score.calibrated is True
