from __future__ import annotations

from types import SimpleNamespace

import numpy as np
import pandas as pd

from src.regime import backtest as backtest_module
from src.regime import portfolio as portfolio_module
from src.regime import signals as signals_module


def test_compute_return_correlations() -> None:
    dates = pd.date_range("2025-01-01", periods=90, freq="D")
    base = pd.Series(np.linspace(100, 130, len(dates)), index=dates)
    market_frames = {
        "AAA": pd.DataFrame({"price": base}),
        "BBB": pd.DataFrame({"price": base * 1.05}),
    }
    correlations = portfolio_module.compute_return_correlations(market_frames, window=63)
    assert ("AAA", "BBB") in correlations
    assert correlations[("AAA", "BBB")] > 0.9


def test_concentration_adjusted_strength_no_overlap() -> None:
    adjusted, warning, penalty = signals_module.concentration_adjusted_strength(
        ticker="NVDA",
        composite_strength=0.8,
        regime_label="Bull",
        sector="Tech",
        portfolio_tickers=["NVDA", "XOM"],
        correlations={},
        sector_map={"NVDA": "Tech", "XOM": "Energy"},
        regime_map={"NVDA": "Bull", "XOM": "Bull"},
    )
    assert adjusted == 0.8
    assert warning is None
    assert penalty == 0.0


def test_concentration_adjusted_strength_high_correlation() -> None:
    adjusted, warning, penalty = signals_module.concentration_adjusted_strength(
        ticker="NVDA",
        composite_strength=0.8,
        regime_label="Bull",
        sector="Tech",
        portfolio_tickers=["NVDA", "AVGO", "AMD", "MU"],
        correlations={("NVDA", "AVGO"): 0.9, ("NVDA", "AMD"): 0.8, ("NVDA", "MU"): 0.85},
        sector_map={"NVDA": "Tech", "AVGO": "Tech", "AMD": "Tech", "MU": "Tech"},
        regime_map={"NVDA": "Bull", "AVGO": "Bull", "AMD": "Bull", "MU": "Bull"},
    )
    assert adjusted < 0.8
    assert penalty > 0.1
    assert warning is not None and "Signal reduced" in warning


def test_divergence_severity_aligned() -> None:
    result = signals_module.divergence_severity("Bull", "Bull", [], "NVDA")
    assert result["score"] == 0.0
    assert result["interpretation"] == "Aligned"


def test_divergence_severity_insufficient_history() -> None:
    history = [{"ticker": "NVDA", "current_label": "Bear"}]
    result = signals_module.divergence_severity("Bull", "Bear", history, "NVDA")
    assert result["score"] == 0.5


def test_divergence_severity_strong_signal() -> None:
    history = [
        {"ticker": "NVDA", "current_label": "Bear"},
        {"ticker": "NVDA", "current_label": "Bear"},
        {"ticker": "NVDA", "current_label": "Bear"},
        {"ticker": "NVDA", "current_label": "Bull"},
    ]
    result = signals_module.divergence_severity("Neutral", "Bear", history, "NVDA")
    assert result["score"] >= 0.7


def test_position_size_backward_compatible() -> None:
    result = signals_module.compute_position_size(
        regime_probability=0.75,
        composite_action="Buy",
        risk_reward_ratio=2.0,
        atr_value=2.0,
        current_price=100.0,
        portfolio_value=100000.0,
    )
    assert result.suggested_pct >= 0
    assert result.portfolio_adjustment == 1.0


def test_position_size_adjusts_for_regime_and_sector_concentration() -> None:
    result = signals_module.compute_position_size(
        regime_probability=0.85,
        composite_action="Buy",
        risk_reward_ratio=2.0,
        atr_value=2.0,
        current_price=100.0,
        portfolio_value=100000.0,
        regime_exposure={"Bull": 0.95, "Neutral": 0.03, "Bear": 0.02},
        sector_exposure_pct=45.0,
        correlation_penalty=0.10,
    )
    assert result.portfolio_adjustment < 1.0
    assert result.adjustment_rationale is not None


def test_backtest_regime_conditional_stats(monkeypatch) -> None:
    periods = 260
    dates = pd.date_range("2024-01-01", periods=periods, freq="D")
    prices = pd.Series(np.linspace(100, 150, periods), index=dates)
    market = pd.DataFrame(
        {
            "price": prices,
            "volume": pd.Series(np.linspace(1_000_000, 1_500_000, periods), index=dates),
            "high": prices + 1,
            "low": prices - 1,
        },
        index=dates,
    )
    monkeypatch.setattr(backtest_module, "download_market_frame", lambda **kwargs: SimpleNamespace(frame=market))
    monkeypatch.setattr(
        backtest_module,
        "compute_technicals",
        lambda *args, **kwargs: pd.DataFrame(
            {
                "rsi_14": pd.Series(np.full(periods, 50.0), index=dates),
                "bb_pct": pd.Series(np.full(periods, 0.5), index=dates),
                "macd_histogram": pd.Series(np.full(periods, 0.1), index=dates),
            },
            index=dates,
        ),
    )
    monkeypatch.setattr(
        backtest_module,
        "fit_regime_model",
        lambda **kwargs: SimpleNamespace(
            latest_label="Bull",
            latest_probability=0.9,
            transition_matrix=np.eye(3),
            latest_state_vector=np.array([1.0, 0.0, 0.0]),
            transition_risk=0.05,
            expected_regime_duration=21,
        ),
    )
    monkeypatch.setattr(backtest_module, "signal_from_forward_curve", lambda *args, **kwargs: SimpleNamespace(action="Buy"))
    monkeypatch.setattr(backtest_module, "intra_regime_signal", lambda *args, **kwargs: "Hold / add on weakness")
    monkeypatch.setattr(
        backtest_module,
        "build_composite_signal",
        lambda *args, **kwargs: SimpleNamespace(composite_action="Buy"),
    )
    result = backtest_module.run_backtest("NVDA", period="1y", refit_step=5)
    assert result.regime_conditional
    assert result.regime_conditional[0]["regime"] == "Bull"
