from __future__ import annotations

import json
import logging
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest
from sklearn.metrics import brier_score_loss

from src.regime.meta_labeler import META_FEATURES, MetaLabelerConfig, MetaLabelerEngine
from src.regime.pipeline_backtest import PipelineBacktestConfig, PipelinePosition, _manage_position
from src.regime.probability_calibration import ProbabilityCalibrator, fit_calibrator, load_calibrator
from src.regime.triple_barrier import (
    ManagedExitConfig,
    apply_managed_exit_labels,
    build_multi_ticker_managed_frame,
    sample_uniqueness_weights,
)


def _managed_frame(closes, *, highs=None, lows=None, dates=None, regimes=None, atr=5.0) -> pd.DataFrame:
    close_values = [float(value) for value in closes]
    index = pd.to_datetime(dates) if dates is not None else pd.date_range("2026-01-01", periods=len(close_values), freq="D")
    return pd.DataFrame(
        {
            "price": close_values,
            "high": highs or close_values,
            "low": lows or close_values,
            "regime": regimes or ["Bull"] * len(close_values),
            "atr_14": [atr] * len(close_values),
        },
        index=index,
    )


def _meta_training_frame(rows: int = 80, *, with_dates: bool = True, with_label_end_idx: bool = True) -> pd.DataFrame:
    dates = pd.date_range("2026-01-01", periods=rows, freq="D")
    frame = pd.DataFrame(
        {
            "ticker": ["AAA" if i % 2 == 0 else "BBB" for i in range(rows)],
            "canonical_state": [i % 3 for i in range(rows)],
            "return": np.linspace(-0.03, 0.03, rows),
            "volatility": np.linspace(0.12, 0.35, rows),
            "volume_zscore": np.linspace(-1.5, 1.5, rows),
            "vix": np.linspace(18.0, 30.0, rows),
            "vix_change": np.linspace(-0.3, 0.3, rows),
            "yield_10y": np.linspace(4.0, 4.8, rows),
            "yield_10y_change": np.linspace(-0.02, 0.02, rows),
            "barrier_outcome": [1 if i % 4 in (0, 1) else 0 for i in range(rows)],
        }
    )
    if with_label_end_idx:
        frame["label_end_idx"] = np.arange(rows) + 2
    if with_dates:
        frame["label_entry_date"] = dates
        frame["label_end_date"] = dates + pd.Timedelta(days=2)
    return frame


def _label_first(frame: pd.DataFrame, **config_overrides):
    labeled = apply_managed_exit_labels(frame, config=ManagedExitConfig(**config_overrides))
    return labeled.iloc[0]


def test_managed_exit_labels_cover_production_outcomes() -> None:
    target = _label_first(_managed_frame([100, 101], highs=[100, 111], lows=[100, 99]))
    assert target["barrier_outcome"] == 1.0
    assert target["barrier_type"] == "target"
    assert target["label_end_idx"] == 1

    trailing_win = _label_first(
        _managed_frame([100, 112, 101], highs=[100, 113, 112], lows=[100, 100, 101.5]),
        profit_target_atr_mult=20.0,
    )
    assert trailing_win["barrier_type"] == "trailing"
    assert trailing_win["barrier_outcome"] == 1.0

    trailing_loss = _label_first(
        _managed_frame([100, 112, 101], highs=[100, 113, 112], lows=[100, 100, 101.5]),
        profit_target_atr_mult=20.0,
        cost_bps=300.0,
    )
    assert trailing_loss["barrier_type"] == "trailing"
    assert trailing_loss["barrier_outcome"] == 0.0

    static_stop = _label_first(_managed_frame([100, 99], highs=[100, 101], lows=[100, 89]))
    assert static_stop["barrier_outcome"] == 0.0
    assert static_stop["barrier_type"] == "stop"

    timeout_win = _label_first(
        _managed_frame([100, 100, 100, 101], highs=[100, 101, 101, 101], lows=[100, 99, 99, 99], atr=50.0),
        time_stop_days=3,
    )
    assert timeout_win["barrier_outcome"] == 1.0
    assert timeout_win["barrier_type"] == "time_win"

    timeout_loss = _label_first(
        _managed_frame([100, 100, 100, 99], highs=[100, 101, 101, 101], lows=[100, 99, 99, 99], atr=50.0),
        time_stop_days=3,
    )
    assert timeout_loss["barrier_outcome"] == 0.0
    assert timeout_loss["barrier_type"] == "time_loss"

    marginal_no_cost = _label_first(
        _managed_frame([100, 100, 100, 100.1], highs=[100, 101, 101, 101], lows=[100, 99, 99, 99], atr=50.0),
        time_stop_days=3,
        cost_bps=0.0,
    )
    marginal_with_cost = _label_first(
        _managed_frame([100, 100, 100, 100.1], highs=[100, 101, 101, 101], lows=[100, 99, 99, 99], atr=50.0),
        time_stop_days=3,
        cost_bps=20.0,
    )
    assert marginal_no_cost["barrier_type"] == "time_win"
    assert marginal_with_cost["barrier_type"] == "time_loss"


def test_managed_exit_labels_match_reference_ladder_for_trailing_path() -> None:
    frame = _managed_frame(
        [100, 111, 101],
        highs=[100, 120, 103],
        lows=[100, 109.5, 100.5],
    )
    label = _label_first(frame, profit_target_atr_mult=20.0, cost_bps=0.0)

    position = PipelinePosition(
        ticker="TEST",
        quantity=1,
        entry_price=100.0,
        entry_date=pd.Timestamp(frame.index[0]).date().isoformat(),
        entry_idx=0,
        stop_price=90.0,
        target_price=200.0,
        risk_reward_ratio=None,
        timeframe_days=21,
        atr_14=5.0,
        trade_geometry_source="test",
    )
    config = PipelineBacktestConfig(enable_cost_model=False)
    trade = None
    exit_idx = None
    for idx in range(1, len(frame)):
        trade, position, _cost = _manage_position("TEST", position, None, frame.iloc[idx], pd.Timestamp(frame.index[idx]), idx, config)
        if trade is not None:
            exit_idx = idx
            break

    assert trade is not None
    assert label["barrier_type"] == trade.exit_type
    assert label["label_end_idx"] == exit_idx
    assert label["barrier_outcome"] == 1.0


def test_managed_exit_labels_use_calendar_day_time_stop() -> None:
    label = _label_first(
        _managed_frame(
            [100, 100, 100],
            highs=[100, 101, 101],
            lows=[100, 99, 99],
            dates=["2026-01-02", "2026-01-05", "2026-01-06"],
            atr=50.0,
        ),
        time_stop_days=3,
    )
    assert label["barrier_type"] == "time_loss"
    assert label["barrier_days"] == 3
    assert label["label_end_idx"] == 1


def test_same_bar_high_does_not_ratchet_before_low_is_tested() -> None:
    label = _label_first(
        _managed_frame(
            [100, 118, 107],
            highs=[100, 130, 119],
            lows=[100, 105, 107],
        ),
        profit_target_atr_mult=20.0,
    )
    assert label["barrier_type"] == "trailing"
    assert label["label_end_idx"] == 2


def test_sample_uniqueness_weights_disjoint_and_fully_overlapping() -> None:
    disjoint = pd.DataFrame({"barrier_outcome": [1, 0, 1], "label_end_idx": [0, 1, 2]})
    pd.testing.assert_series_equal(sample_uniqueness_weights(disjoint), pd.Series([1.0, 1.0, 1.0]), check_names=False)

    overlapping = pd.DataFrame(
        {
            "barrier_outcome": [1, 0, 1],
            "_label_start_idx": [0, 0, 0],
            "label_end_idx": [2, 2, 2],
        }
    )
    weights = sample_uniqueness_weights(overlapping)
    assert weights.tolist() == pytest.approx([1 / 3, 1 / 3, 1 / 3])


def test_build_multi_ticker_managed_frame_stamps_date_coordinates() -> None:
    first = SimpleNamespace(
        price_frame=_managed_frame(
            [100, 111, 112],
            highs=[100, 111, 112],
            lows=[100, 99, 100],
            dates=["2026-01-01", "2026-01-02", "2026-01-03"],
        )
    )
    second = SimpleNamespace(
        price_frame=_managed_frame(
            [50, 61, 62],
            highs=[50, 61, 62],
            lows=[50, 49, 50],
            dates=["2026-01-01", "2026-01-02", "2026-01-03"],
        )
    )

    labeled = build_multi_ticker_managed_frame([("aaa", first), ("bbb", second)])

    assert labeled["ticker"].tolist() == ["AAA", "AAA", "BBB", "BBB"]
    assert set(["label_entry_date", "label_end_date", "label_end_idx"]).issubset(labeled.columns)
    target_rows = labeled.loc[labeled["barrier_type"] == "target"].reset_index(drop=True)
    assert target_rows["ticker"].tolist() == ["AAA", "BBB"]
    assert pd.to_datetime(target_rows["label_entry_date"]).dt.date.astype(str).tolist() == ["2026-01-01", "2026-01-01"]
    assert pd.to_datetime(target_rows["label_end_date"]).dt.date.astype(str).tolist() == ["2026-01-02", "2026-01-02"]


def test_date_uniqueness_weights_are_per_ticker_not_cross_ticker() -> None:
    frame = pd.DataFrame(
        {
            "ticker": ["AAA", "AAA", "BBB"],
            "barrier_outcome": [1, 0, 1],
            "label_entry_date": pd.to_datetime(["2026-01-01", "2026-01-02", "2026-01-01"]),
            "label_end_date": pd.to_datetime(["2026-01-03", "2026-01-03", "2026-01-03"]),
        }
    )

    weights = sample_uniqueness_weights(frame)

    assert weights.iloc[0] == pytest.approx((1.0 + 0.5 + 0.5) / 3.0)
    assert weights.iloc[1] == pytest.approx(0.5)
    assert weights.iloc[2] == pytest.approx(1.0)


def test_purged_walk_forward_cv_respects_purge_and_embargo() -> None:
    engine = MetaLabelerEngine(MetaLabelerConfig(min_training_samples=10, n_folds=3, embargo_bars=2))
    frame = pd.DataFrame({"_label_start_idx": np.arange(30), "label_end_idx": np.arange(30) + 3})
    splits = engine._purged_walk_forward_splits(frame)
    assert splits
    for train_idx, test_idx, meta in splits:
        train_starts = frame.iloc[train_idx]["_label_start_idx"]
        train_ends = frame.iloc[train_idx]["label_end_idx"]
        assert train_idx.max() < test_idx.min()
        assert train_starts.max() < meta["test_start_bar"] - 2
        assert not ((train_ends >= meta["test_start_bar"]) & (train_ends <= meta["test_end_bar"])).any()


def test_date_purged_cv_sorts_chronologically_and_embargoes_by_date() -> None:
    engine = MetaLabelerEngine(MetaLabelerConfig(min_training_samples=10, n_folds=3, embargo_days=2))
    frame = pd.concat(
        [
            _meta_training_frame(18).assign(ticker="BBB"),
            _meta_training_frame(18).assign(ticker="AAA"),
        ],
        ignore_index=True,
    )

    _X, _y, prepared, _weights = engine._prepare_training_data(frame)
    splits = engine._purged_walk_forward_splits(prepared)

    assert prepared["label_entry_date"].is_monotonic_increasing
    assert splits
    for train_idx, test_idx, meta in splits:
        test_start = pd.Timestamp(meta["test_start_date"])
        train_entries = pd.to_datetime(prepared.iloc[train_idx]["label_entry_date"])
        train_ends = pd.to_datetime(prepared.iloc[train_idx]["label_end_date"])
        assert train_idx.max() < test_idx.min()
        assert train_entries.max() < test_start - pd.Timedelta(days=2)
        assert train_ends.max() < test_start


def test_meta_labeler_reports_degenerate_one_bar_lifespans(caplog) -> None:
    engine = MetaLabelerEngine(MetaLabelerConfig(min_training_samples=20, n_folds=2))
    frame = _meta_training_frame(with_dates=False, with_label_end_idx=False)

    with caplog.at_level(logging.WARNING):
        metrics = engine.train(frame, label_mode="legacy")

    assert metrics["weights_degenerate"] is True
    assert metrics["lifespan_fallback_ratio"] == pytest.approx(1.0)
    assert "degenerate" in caplog.text


def test_managed_date_lifespans_are_attributed_and_not_degenerate() -> None:
    engine = MetaLabelerEngine(MetaLabelerConfig(min_training_samples=20, n_folds=2))
    metrics = engine.train(
        _meta_training_frame(with_dates=True, with_label_end_idx=True),
        label_mode="managed",
        label_config={"time_stop_days": 21},
    )

    assert metrics["status"] == "trained"
    assert metrics["weights_degenerate"] is False
    assert metrics["lifespan_fallback_ratio"] == pytest.approx(0.0)
    assert metrics["label_mode"] == "managed"
    assert metrics["label_config"] == {"time_stop_days": 21}


def test_calibrator_json_round_trip_and_improves_brier(tmp_path: Path) -> None:
    probabilities = np.array([0.1] * 40 + [0.9] * 40)
    outcomes = np.array([1] * 40 + [0] * 40)
    calibrator = fit_calibrator(probabilities, outcomes)
    calibrated = calibrator.calibrate(probabilities)
    assert brier_score_loss(outcomes, calibrated) < brier_score_loss(outcomes, probabilities)

    path = tmp_path / "meta_labeler_v1_calibrator.json"
    calibrator.save_calibrator(path)
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["calibrator"] == "isotonic"
    assert "pickle" not in path.read_text(encoding="utf-8").lower()
    loaded = load_calibrator(path)
    np.testing.assert_allclose(loaded.calibrate(probabilities), calibrated)


class _StubModel:
    feature_importances_ = np.zeros(len(META_FEATURES))

    def predict_proba(self, _frame):
        return np.array([[0.2, 0.8]])


def _ready_engine() -> MetaLabelerEngine:
    engine = MetaLabelerEngine(MetaLabelerConfig(min_training_samples=1))
    engine._model = _StubModel()
    engine._trained = True
    return engine


def test_analyze_returns_calibrated_probability_and_raw_detail() -> None:
    engine = _ready_engine()
    engine._calibrator = ProbabilityCalibrator(x_thresholds=[0.0, 1.0], y_thresholds=[0.0, 0.4])
    result = engine.analyze("NVDA", {feature: 1.0 for feature in META_FEATURES}, None)
    assert result.confidence == pytest.approx(0.32)
    assert result.signal == "veto"
    assert result.details["raw_probability"] == pytest.approx(0.8)
    assert result.details["calibrated"] is True


def test_analyze_degraded_features_passes_through_after_three_missing_features() -> None:
    engine = _ready_engine()
    allowed_count = len(META_FEATURES) - int(len(META_FEATURES) * 0.25)
    scored = engine.analyze("NVDA", {feature: 1.0 for feature in META_FEATURES[:allowed_count]}, None)
    assert scored.details.get("status") != "degraded_features"

    degraded = engine.analyze("NVDA", {feature: 1.0 for feature in META_FEATURES[: allowed_count - 1]}, None)
    assert degraded.confidence == 1.0
    assert degraded.signal == "neutral"
    assert degraded.details["status"] == "degraded_features"
