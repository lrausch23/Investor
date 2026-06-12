from __future__ import annotations

from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from src.regime import cli
from src.regime import hmm_engine
from src.regime import meta_labeler as meta_module
from src.regime.meta_labeler import (
    LEGACY_META_FEATURES,
    META_FEATURES,
    MetaLabelerConfig,
    MetaLabelerEngine,
    extract_meta_features,
    meta_labeler_result_can_influence,
)


class _StubModel:
    def __init__(self, probability: float, *, feature_count: int | None = None) -> None:
        self.probability = float(probability)
        self.n_features_in_ = feature_count or len(META_FEATURES)
        self.feature_importances_ = np.zeros(self.n_features_in_)
        self.seen_columns: list[str] | None = None

    def predict_proba(self, frame):
        self.seen_columns = list(frame.columns)
        if len(frame.columns) != self.n_features_in_:
            raise AssertionError("predict_proba reached with mismatched feature count")
        return np.array([[1.0 - self.probability, self.probability]])


def _ready_engine(probability: float, *, roc_auc: float | None = 0.60, feature_names: list[str] | None = None) -> MetaLabelerEngine:
    names = list(feature_names or META_FEATURES)
    engine = MetaLabelerEngine(MetaLabelerConfig(min_training_samples=1))
    engine._model = _StubModel(probability, feature_count=len(names))
    engine._trained = True
    engine._feature_names = names
    engine._training_metrics = {"positive_rate_train": 0.50}
    if roc_auc is not None:
        engine._training_metrics["roc_auc"] = roc_auc
    return engine


def test_low_skill_model_self_disqualifies_and_does_not_reach_size_only(monkeypatch) -> None:
    engine = _ready_engine(0.10, roc_auc=0.468)
    result = engine.analyze("NVDA", {feature: 1.0 for feature in META_FEATURES}, None)
    assert result.signal == "neutral"
    assert result.details["status"] == "insufficient_model_skill"
    assert result.details["oof_roc_auc"] == pytest.approx(0.468)
    assert meta_labeler_result_can_influence(result) is False

    date = pd.Timestamp("2026-01-02")
    signal = cli.PipelineSignal(
        date=date.date().isoformat(),
        regime="Bull",
        probability=0.80,
        composite_action="Buy",
        composite_strength=0.75,
        expected_duration=12.0,
        transition_risk=0.10,
        regime_days=4,
    )

    class BaseProvider:
        def __call__(self, *_args, **_kwargs):
            return signal

    history = pd.DataFrame(
        {
            "price": [100.0, 101.0],
            "high": [101.0, 102.0],
            "low": [99.0, 100.0],
            "volume": [1_000_000.0, 1_000_000.0],
            "vix": [20.0, 20.1],
            "yield_10y": [4.0, 4.0],
        },
        index=pd.date_range("2026-01-01", periods=2),
    )
    monkeypatch.setattr(cli, "_ProductionSignalProvider", lambda: BaseProvider())
    provider = cli._MetaLabelerVetoProvider(engine, veto_mode="size_only")

    scored = provider("NVDA", date, history, cli.PipelineBacktestConfig(), None)

    assert scored.composite_action == "Buy"
    assert scored.meta_labeler_probability is None
    assert provider.evidence_summary()["passthrough_signals"] == 1


def test_skill_gate_passes_high_auc_can_be_disabled_and_missing_auc_is_unknown(monkeypatch) -> None:
    features = {feature: 1.0 for feature in META_FEATURES}
    skilled = _ready_engine(0.30, roc_auc=0.60).analyze("NVDA", features, None)
    assert skilled.signal == "veto"
    assert skilled.details["skill_gate"] == "passed"

    monkeypatch.setattr(meta_module, "_read_setting", lambda key: "false" if key == "meta_labeler_skill_gate_enabled" else None)
    disabled = _ready_engine(0.30, roc_auc=0.468).analyze("NVDA", features, None)
    assert disabled.signal == "veto"
    assert disabled.details["skill_gate"] == "disabled"

    monkeypatch.setattr(meta_module, "_read_setting", lambda _key: None)
    unknown = _ready_engine(0.49, roc_auc=None).analyze("NVDA", features, None)
    assert unknown.details["skill_gate"] == "unknown_skill"
    assert unknown.details["threshold_mode"] == "base_rate_relative"


def test_metadata_round_trip_preserves_auc_and_feature_schema(tmp_path) -> None:
    rows = 90
    frame = pd.DataFrame(
        {
            "canonical_state": [index % 3 for index in range(rows)],
            "return": np.linspace(-0.02, 0.02, rows),
            "volatility": np.linspace(0.10, 0.30, rows),
            "volume_zscore": np.linspace(-1.0, 1.0, rows),
            "vix": np.linspace(18.0, 24.0, rows),
            "vix_change": np.linspace(-0.2, 0.2, rows),
            "yield_10y": np.linspace(4.0, 4.5, rows),
            "yield_10y_change": np.linspace(-0.02, 0.02, rows),
            "p_bull_day5": np.linspace(0.3, 0.8, rows),
            "p_bear_day5": np.linspace(0.4, 0.1, rows),
            "transition_risk": np.linspace(0.05, 0.25, rows),
            "regime_days": np.arange(rows) % 12 + 1,
            "barrier_entry": np.full(rows, 100.0),
            "barrier_target": np.full(rows, 110.0),
            "barrier_stop": np.full(rows, 95.0),
            "atr_14": np.full(rows, 2.5),
            "barrier_outcome": [1 if index % 4 in (0, 1) else 0 for index in range(rows)],
            "label_entry_date": pd.date_range("2026-01-01", periods=rows, freq="D"),
            "label_end_date": pd.date_range("2026-01-03", periods=rows, freq="D"),
        }
    )
    engine = MetaLabelerEngine(MetaLabelerConfig(n_estimators=5, min_training_samples=20, n_folds=2))
    metrics = engine.train(frame)
    assert metrics["status"] == "trained"
    engine._training_metrics["roc_auc"] = 0.61
    target = tmp_path / "meta_labeler_v5.json"

    saved = engine.save_model(str(target))
    loaded = MetaLabelerEngine()
    load_result = loaded.load_model(str(target))

    assert saved["metadata_path"] is not None
    assert load_result["training_metrics"]["roc_auc"] == pytest.approx(0.61)
    assert load_result["feature_names"] == META_FEATURES
    assert load_result["feature_set_version"] == meta_module.FEATURE_SET_VERSION


def test_legacy_feature_schema_scores_after_feature_set_grows() -> None:
    engine = _ready_engine(0.80, roc_auc=0.60, feature_names=list(LEGACY_META_FEATURES))
    result = engine.analyze("NVDA", {feature: 1.0 for feature in LEGACY_META_FEATURES}, None)

    assert result.signal == "confirm"
    assert engine._model.seen_columns == list(LEGACY_META_FEATURES)


def test_schema_mismatch_passes_through_before_model_prediction() -> None:
    engine = _ready_engine(0.80, roc_auc=0.60, feature_names=list(META_FEATURES))
    engine._model.n_features_in_ = len(LEGACY_META_FEATURES)

    result = engine.analyze("NVDA", {feature: 1.0 for feature in META_FEATURES}, None)

    assert result.signal == "neutral"
    assert result.details["status"] == "feature_schema_mismatch"


def test_walk_forward_forward_probabilities_use_refit_time_matrix(monkeypatch) -> None:
    index = pd.date_range("2026-01-01", periods=8, freq="D")
    feature_frame = pd.DataFrame(
        {
            "price": np.linspace(100.0, 107.0, 8),
            "high": np.linspace(101.0, 108.0, 8),
            "low": np.linspace(99.0, 106.0, 8),
            "volume": np.full(8, 1_000_000.0),
            "vix": np.full(8, 20.0),
            "yield_10y": np.full(8, 4.0),
            "return": [0.05, 0.0, -0.05, 0.05, 0.0, -0.05, 0.05, 0.0],
            "volatility": np.full(8, 0.2),
            "trend": [0.05, 0.0, -0.05, 0.05, 0.0, -0.05, 0.05, 0.0],
            "volume_zscore": np.zeros(8),
            "vix_change": np.zeros(8),
            "yield_10y_change": np.zeros(8),
        },
        index=index,
    )
    matrices = [
        np.array([[0.90, 0.10, 0.00], [0.10, 0.80, 0.10], [0.00, 0.10, 0.90]]),
        np.array([[0.50, 0.40, 0.10], [0.10, 0.80, 0.10], [0.10, 0.40, 0.50]]),
        np.array([[0.20, 0.70, 0.10], [0.20, 0.60, 0.20], [0.10, 0.70, 0.20]]),
    ]

    class FakeHMM:
        calls = 0

        def __init__(self, **_kwargs) -> None:
            self.monitor_ = SimpleNamespace(converged=True)

        def fit(self, _x):
            matrix = matrices[min(FakeHMM.calls, len(matrices) - 1)]
            FakeHMM.calls += 1
            self.transmat_ = matrix
            return self

        def predict(self, x):
            return np.array([idx % 3 for idx in range(len(x))])

        def predict_proba(self, x):
            states = self.predict(x)
            probs = np.zeros((len(states), 3), dtype=float)
            probs[np.arange(len(states)), states] = 1.0
            return probs

    monkeypatch.setattr(hmm_engine, "build_features", lambda *_args, **_kwargs: feature_frame)
    monkeypatch.setattr(hmm_engine, "GaussianHMM", FakeHMM)

    result = hmm_engine.fit_regime_model(
        "TEST",
        pd.DataFrame(index=index),
        training_window=4,
        refit_step=3,
        record_forward_probabilities=True,
    )

    first_recorded = float(result.price_frame["p_bull_day5"].iloc[0])
    expected_first = float(np.linalg.matrix_power(matrices[0], 5)[0, 0])
    final_matrix_value = float(np.linalg.matrix_power(matrices[-1], 5)[0, 0])
    assert first_recorded == pytest.approx(expected_first)
    assert first_recorded != pytest.approx(final_matrix_value)


def test_training_and_inference_feature_extraction_match_for_fixed_bar() -> None:
    row = {
        "canonical_state": 0,
        "return": 0.01,
        "volatility": 0.2,
        "volume_zscore": 1.2,
        "vix": 20.0,
        "vix_change": -0.1,
        "yield_10y": 4.2,
        "yield_10y_change": 0.01,
        "p_bull_day5": 0.7,
        "p_bear_day5": 0.1,
        "transition_risk": 0.12,
        "regime_days": 6,
        "barrier_entry": 100.0,
        "barrier_target": 112.0,
        "barrier_stop": 94.0,
        "atr_14": 3.0,
        "rsi_14": 64.0,
        "macd_histogram": 0.3,
        "barrier_outcome": 1,
    }
    engine = MetaLabelerEngine(MetaLabelerConfig(min_training_samples=1))
    X, _y, _frame, _weights = engine._prepare_training_data(pd.DataFrame([row]))
    inferred = extract_meta_features(row)

    for feature in META_FEATURES:
        if feature == "hmm_state":
            assert int(X.iloc[0][feature]) == int(inferred[feature])
        else:
            assert float(X.iloc[0][feature]) == pytest.approx(float(inferred[feature]))


def test_ab_evidence_reports_skill_and_constant_probability_dispersion() -> None:
    engine = _ready_engine(0.52, roc_auc=0.468)
    engine._training_metrics.update(
        {
            "positive_rate_train": 0.525,
            "brier_score_calibrated": 0.2482,
        }
    )
    evidence = cli._meta_labeler_evidence_summary(engine, [0.525, 0.525, 0.525], analyzed_count=3)
    assert evidence["oof_roc_auc"] == pytest.approx(0.468)
    assert evidence["base_rate_brier"] == pytest.approx(0.525 * 0.475)
    assert evidence["calibration_lift_vs_base_rate"] == pytest.approx((0.525 * 0.475) - 0.2482)
    assert evidence["probability_std"] == pytest.approx(0.0)
    assert evidence["probability_iqr"] == pytest.approx(0.0)

    output = cli._format_meta_labeler_ab(
        {
            "results": [
                {
                    "mode_label": "meta_size_only",
                    "baseline": {"total_return": 0.0, "sharpe_ratio": 0.0, "max_drawdown": 0.0, "trade_count": 0},
                    "meta_veto": {
                        "total_return": 0.0,
                        "sharpe_ratio": 0.0,
                        "max_drawdown": 0.0,
                        "trade_count": 0,
                        **evidence,
                    },
                }
            ]
        }
    )
    assert "oof_roc_auc,base_rate_brier,calibration_lift_vs_base_rate,probability_std,probability_iqr" in output
    assert "0.468000" in output
