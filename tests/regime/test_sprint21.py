from __future__ import annotations

from types import SimpleNamespace

import pandas as pd
import pytest

from src.app.routes import regime as regime_route
from src.regime.charts import build_confidence_timeline, build_regime_price_chart, build_transition_heatmap
from src.regime.config import DEFAULT_SIGNAL_THRESHOLDS, SignalThresholds, ticker_candidates
from src.regime.signals import CompositeSignal, PositionSize, SignalResult, compute_position_size, compute_price_targets, signal_from_forward_curve


def _technicals(atr: float = 2.0) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "bb_lower": [95.0],
            "bb_upper": [105.0],
            "atr_14": [atr],
            "rsi_14": [50.0],
            "bb_pct": [0.5],
            "macd_histogram": [0.1],
        }
    )


def _composite(action: str = "Buy", strength: float = 0.8) -> CompositeSignal:
    return CompositeSignal(
        regime_signal="Bull",
        regime_probability=0.8,
        forward_signal=SignalResult(action=action, timeframe="short", strength=strength, expected_holding_days=10, rationale="test"),
        technical_signal="Buy the dip",
        composite_action=action,
        composite_strength=strength,
        short_term_view="short",
        medium_term_view="medium",
    )


def test_signal_threshold_defaults_match_current_values() -> None:
    assert DEFAULT_SIGNAL_THRESHOLDS.strong_buy_max_transition_risk == 0.05
    assert DEFAULT_SIGNAL_THRESHOLDS.buy_max_transition_risk == 0.15
    assert DEFAULT_SIGNAL_THRESHOLDS.hold_bull_max_transition_risk == 0.30


def test_signal_from_forward_curve_accepts_custom_thresholds() -> None:
    forward_curve = pd.DataFrame([{"day": day, "p_bull": 0.6, "p_neutral": 0.25, "p_bear": 0.15} for day in range(1, 22)])
    thresholds = SignalThresholds(buy_max_transition_risk=0.30, strong_buy_min_probability=0.95)
    signal = signal_from_forward_curve(forward_curve, "Bull", 0.2, 12, 0.8, thresholds=thresholds)
    assert signal.action == "Buy"


def test_atr_based_price_targets_produce_non_linear_projection() -> None:
    result = compute_price_targets(
        current_price=100.0,
        technicals_df=_technicals(atr=2.0),
        composite_signal=_composite("Buy", 0.8),
        expected_duration=16,
        state_mean_return=0.01,
    )
    assert result.exit_price > 100.0
    assert result.exit_price != pytest.approx(100.0 * (1 + 0.01 * 16 * 1.15))


def test_mean_reversion_dampening_for_long_timeframes() -> None:
    short = compute_price_targets(current_price=100.0, technicals_df=_technicals(), composite_signal=_composite("Buy", 0.8), expected_duration=8, state_mean_return=0.01)
    long = compute_price_targets(current_price=100.0, technicals_df=_technicals(), composite_signal=_composite("Buy", 0.8), expected_duration=30, state_mean_return=0.01)
    assert (long.exit_price - 100.0) < ((2.0 * (30 ** 0.5) * 1.15))
    assert long.exit_price > short.exit_price


def test_compute_position_size_various_inputs() -> None:
    size = compute_position_size(regime_probability=0.8, composite_action="Buy", risk_reward_ratio=2.0, atr_value=2.0, current_price=100.0, portfolio_value=100000.0)
    assert isinstance(size, PositionSize)
    assert size.suggested_pct >= 0
    assert size.suggested_dollars is not None


def test_half_kelly_caps_position_size() -> None:
    size = compute_position_size(regime_probability=0.55, composite_action="Buy", risk_reward_ratio=1.2, atr_value=2.0, current_price=100.0, portfolio_value=100000.0)
    assert size.kelly_fraction is not None
    assert size.suggested_pct <= size.kelly_fraction * 100.0 + 0.1


def test_chart_builders_produce_plotly_dicts() -> None:
    frame = pd.DataFrame({"price": [100.0, 101.0], "state_probability": [0.7, 0.8], "regime": ["Bull", "Bull"]}, index=pd.date_range("2026-01-01", periods=2))
    assert "data" in build_regime_price_chart(frame, "NVDA")
    assert "data" in build_transition_heatmap([[0.9, 0.1, 0.0], [0.1, 0.8, 0.1], [0.0, 0.1, 0.9]])
    assert "data" in build_confidence_timeline(frame)


def test_regime_job_partial_results_populates_during_analysis(monkeypatch) -> None:
    job = regime_route.RegimeJob(job_id="job", status="running", tickers=["NVDA"], benchmark="SOXX", period="3y", progress=0, total=1, payload=None, error=None, created_at=pd.Timestamp.utcnow().to_pydatetime())
    regime_route._JOBS["job"] = job
    regime_route._set_job_state("job", partial_result={"ticker": "NVDA", "regime": "Bull"})
    assert regime_route._JOBS["job"].partial_results["NVDA"]["regime"] == "Bull"
    regime_route._JOBS.clear()


def test_ticker_candidates_shared_function() -> None:
    assert ticker_candidates("BRK B")[0] == "BRK-B"
    assert "BRK-B" in ticker_candidates("BRK B")
    assert "BRKB" in ticker_candidates("BRK.B")
