from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

HMM_ROOT = Path("/Volumes/T9/Projects/Dev/HMM")
if str(HMM_ROOT) not in sys.path:
    sys.path.insert(0, str(HMM_ROOT))

from src.regime import persistence
from src.regime.backtest import compare_to_benchmark, run_backtest
from src.regime.diagnostics import calibration_payload, compute_calibration_curve
from src.regime.exceptions import InsufficientDataError
from src.regime.hmm_engine import fit_regime_model_weekly
from src.regime.signals import CompositeSignal, SignalResult, multi_timeframe_signal


def _market_frame() -> pd.DataFrame:
    dates = pd.date_range("2022-01-03", periods=800, freq="B")
    prices = pd.Series(range(100, 900), index=dates, dtype=float)
    return pd.DataFrame(
        {
            "price": prices,
            "high": prices + 1.0,
            "low": prices - 1.0,
            "volume": pd.Series(range(1_000_000, 1_000_800), index=dates, dtype=float),
            "vix": pd.Series([18.0 + (idx % 15) * 0.1 for idx in range(len(dates))], index=dates, dtype=float),
            "yield_10y": pd.Series([4.0 + (idx % 20) * 0.01 for idx in range(len(dates))], index=dates, dtype=float),
        }
    )


def test_fit_regime_model_weekly_returns_regime() -> None:
    result = fit_regime_model_weekly("TEST", _market_frame())
    assert result.latest_label in {"Bull", "Neutral", "Bear"}
    assert not result.price_frame.empty


def test_fit_regime_model_weekly_adapts_for_short_history(monkeypatch) -> None:
    calls = []

    def fake_fit_regime_model(ticker, market_frame, lookback_window=8, training_window=104, refit_step=4, **kwargs):
        calls.append(training_window)
        if training_window == 104:
            raise InsufficientDataError("Insufficient history")
        return type("Result", (), {"latest_label": "Bull", "price_frame": market_frame})()

    monkeypatch.setattr("src.regime.hmm_engine.fit_regime_model", fake_fit_regime_model)
    short_frame = _market_frame().iloc[:260]
    result = fit_regime_model_weekly("TEST", short_frame)
    assert result.latest_label == "Bull"
    assert calls[0] == 104
    assert calls[1] < 104


def test_multi_timeframe_signal_variants() -> None:
    assert multi_timeframe_signal("Bull", "Bull") == "Strong trend, high confidence"
    assert multi_timeframe_signal("Bear", "Bull") == "Pullback in uptrend, potential buy"


def test_calibration_curve_and_payload() -> None:
    snapshots = [
        {"action": "Buy", "regime_probability": 0.8, "return_1m": 0.05},
        {"action": "Sell", "regime_probability": 0.7, "return_1m": -0.03},
        {"action": "Hold", "regime_probability": 0.5, "return_1m": 0.0},
    ]
    curve = compute_calibration_curve(snapshots, bins=5)
    payload = calibration_payload(snapshots)
    assert curve.brier_score is not None
    assert payload["sharpness"]["count"] == 3


def test_get_calibration_data_reads_snapshots(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(persistence, "DB_PATH", tmp_path / "regime_watch.db")
    persistence.save_signal_snapshot(
        ticker="NVDA",
        snapshot_date="2026-03-01",
        action="Buy",
        regime_label="Bull",
        regime_probability=0.8,
        composite_strength=0.7,
        benchmark="SOXX",
        current_price=100.0,
        entry_price=95.0,
        exit_price=110.0,
        stop_price=90.0,
        risk_reward_ratio=2.0,
        timeframe_days=10,
    )
    pending = persistence.get_pending_outcomes(as_of="2026-06-30T00:00:00+00:00")
    for row in pending:
        persistence.update_signal_outcome(int(row["id"]), str(row["interval"]), 110.0)
    rows = persistence.get_calibration_data(lookback_days=3650)
    assert rows


def test_run_backtest_returns_trades(monkeypatch) -> None:
    monkeypatch.setattr("src.regime.backtest.download_market_frame", lambda ticker, period, interval: type("Market", (), {"frame": _market_frame()})())
    result = run_backtest("NVDA", period="5y", refit_step=63)
    assert result.buy_and_hold_return > 0
    assert isinstance(result.trades, list)


def test_compare_to_benchmark(monkeypatch) -> None:
    monkeypatch.setattr("src.regime.backtest.download_market_frame", lambda ticker, period, interval: type("Market", (), {"frame": _market_frame()})())
    result = run_backtest("NVDA", period="5y", refit_step=63)
    comparison = compare_to_benchmark(result, benchmark_ticker="SPY", period="5y")
    assert "alpha" in comparison


def test_composite_signal_accepts_weekly_fields() -> None:
    composite = CompositeSignal(
        regime_signal="Bull",
        regime_probability=0.9,
        forward_signal=SignalResult("Buy", "short", 0.8, 10, "test"),
        technical_signal="Buy the dip",
        composite_action="Buy",
        composite_strength=0.8,
        short_term_view="short",
        medium_term_view="medium",
        weekly_regime="Bull",
        multi_timeframe_note="Strong trend, high confidence",
    )
    assert composite.weekly_regime == "Bull"
