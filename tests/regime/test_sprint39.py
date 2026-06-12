from __future__ import annotations

import importlib
import json
from types import SimpleNamespace

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from src.app.main import create_app
from src.app.routes import regime as regime_route
from src.regime import ensemble as ensemble_module
from src.regime import meta_labeler as meta_labeler_module
from src.regime import persistence as persistence_module


@pytest.fixture()
def temp_modules(tmp_path, monkeypatch):
    monkeypatch.setenv("HMM_DATA_DIR", str(tmp_path))
    store = importlib.reload(persistence_module)
    monkeypatch.setattr(store, "DB_PATH", tmp_path / "regime_watch.db")
    ensemble = importlib.reload(ensemble_module)
    meta = importlib.reload(meta_labeler_module)
    return store, ensemble, meta


def _synthetic_labeled_frame(
    rows: int = 240,
    *,
    positive_until: int | None = None,
    positive_train_only: bool = False,
) -> pd.DataFrame:
    positive_until = positive_until if positive_until is not None else rows // 2
    index = pd.date_range("2024-01-01", periods=rows, freq="D")
    frame = pd.DataFrame(
        {
            "canonical_state": [0 if i % 3 == 0 else 2 if i % 3 == 1 else 1 for i in range(rows)],
            "return": [0.03 if i < positive_until else -0.03 for i in range(rows)],
            "volatility": [0.15 if i < positive_until else 0.35 for i in range(rows)],
            "volume_zscore": [1.2 if i < positive_until else -1.2 for i in range(rows)],
            "vix": [18.0 if i < positive_until else 30.0 for i in range(rows)],
            "vix_change": [-0.2 if i < positive_until else 0.2 for i in range(rows)],
            "yield_10y": [4.0 if i < positive_until else 4.8 for i in range(rows)],
            "yield_10y_change": [-0.01 if i < positive_until else 0.02 for i in range(rows)],
            "barrier_outcome": [1 if i < positive_until else 0 for i in range(rows)],
        },
        index=index,
    )
    if positive_train_only:
        split_idx = int(rows * 0.8)
        frame.loc[frame.index[: split_idx - 5], "barrier_outcome"] = 1
        frame.loc[frame.index[split_idx:], "barrier_outcome"] = 0
        frame.loc[frame.index[split_idx - 6], "barrier_outcome"] = 0
    return frame


def test_meta_labeler_implements_analyst_base(temp_modules) -> None:
    _store, ensemble, meta = temp_modules
    engine = meta.MetaLabelerEngine()
    assert isinstance(engine, ensemble.AnalystBase)
    for attr in ("name", "is_ready", "analyze", "train"):
        assert hasattr(engine, attr)


def test_meta_labeler_name(temp_modules) -> None:
    _store, _ensemble, meta = temp_modules
    assert meta.MetaLabelerEngine().name == "xgboost_meta_labeler"


def test_meta_labeler_not_ready_before_training(temp_modules) -> None:
    _store, _ensemble, meta = temp_modules
    assert meta.MetaLabelerEngine().is_ready() is False


def test_meta_labeler_analyze_passthrough_when_not_trained(temp_modules) -> None:
    _store, _ensemble, meta = temp_modules
    result = meta.MetaLabelerEngine().analyze("NVDA", {}, None)
    assert result.confidence == 1.0
    assert result.signal == "neutral"
    assert result.details["status"] == "not_trained"


def test_meta_labeler_train_on_synthetic_data(temp_modules) -> None:
    _store, _ensemble, meta = temp_modules
    frame = _synthetic_labeled_frame()
    engine = meta.MetaLabelerEngine(meta.MetaLabelerConfig(min_training_samples=100))
    metrics = engine.train(frame)
    assert engine.is_ready() is True
    for key in ("accuracy", "precision", "recall", "f1"):
        assert 0.0 <= metrics[key] <= 1.0
    assert set(metrics["feature_importances"]) == set(meta.META_FEATURES)


def test_meta_labeler_predict_returns_probability(temp_modules) -> None:
    _store, _ensemble, meta = temp_modules
    frame = _synthetic_labeled_frame()
    engine = meta.MetaLabelerEngine(meta.MetaLabelerConfig(min_training_samples=100))
    engine.train(frame)
    result = engine.analyze(
        "NVDA",
        {
            "hmm_state": 0,
            "log_ret": 0.03,
            "volatility": 0.14,
            "vol_z": 1.1,
            "vix_level": 18.0,
            "vix_change": -0.2,
            "yield_10y_level": 4.0,
            "yield_10y_change": -0.01,
        },
        None,
    )
    assert 0.0 <= result.confidence <= 1.0
    assert result.signal in {"confirm", "veto", "neutral"}


def test_meta_labeler_veto_on_low_confidence(temp_modules) -> None:
    _store, _ensemble, meta = temp_modules
    frame = _synthetic_labeled_frame(rows=240, positive_until=20)
    engine = meta.MetaLabelerEngine(meta.MetaLabelerConfig(min_training_samples=100))
    engine.train(frame)
    features = {feature: 0.0 for feature in meta.META_FEATURES}
    features.update(
        {
            "hmm_state": 0,
            "log_ret": -0.04,
            "volatility": 0.4,
            "vol_z": -1.1,
            "vix_level": 32.0,
            "vix_change": 0.3,
            "yield_10y_level": 5.0,
            "yield_10y_change": 0.03,
            "composite_strength": 0.1,
            "transition_risk": 0.8,
            "regime_days": 1.0,
            "p_bull_day5": 0.1,
            "p_bear_day5": 0.8,
            "risk_reward_ratio": 0.5,
            "atr_distance_to_stop": 0.2,
            "atr_distance_to_target": 0.4,
            "rsi_bucket": -1.0,
            "macd_hist_sign": -1.0,
            "signal_quality_score": 0.2,
        }
    )
    result = engine.analyze(
        "NVDA",
        features,
        None,
    )
    assert result.signal == "neutral"
    assert result.confidence < 0.50
    assert result.details["threshold_mode"] == "base_rate_relative"
    assert result.confidence < result.details["confirm_threshold"]


def test_meta_labeler_insufficient_data_guard(temp_modules) -> None:
    _store, _ensemble, meta = temp_modules
    frame = _synthetic_labeled_frame(rows=50)
    engine = meta.MetaLabelerEngine(meta.MetaLabelerConfig(min_training_samples=500))
    metrics = engine.train(frame)
    assert engine.is_ready() is False
    assert metrics["status"] == "insufficient_data"


def test_meta_labeler_walk_forward_split(temp_modules) -> None:
    _store, _ensemble, meta = temp_modules
    frame = _synthetic_labeled_frame(rows=240, positive_train_only=True)
    engine = meta.MetaLabelerEngine(meta.MetaLabelerConfig(min_training_samples=100, walk_forward_gap=5, embargo_bars=0))
    metrics = engine.train(frame)
    assert metrics["status"] == "trained"
    assert metrics["train_samples"] == 240
    assert metrics["test_samples"] > 0
    assert metrics["cv_folds"] >= 1
    assert metrics["folds"] == sorted(metrics["folds"], key=lambda item: item["test_start_idx"])


def test_meta_labeler_categorical_hmm_state(temp_modules) -> None:
    _store, _ensemble, meta = temp_modules
    frame = _synthetic_labeled_frame()
    engine = meta.MetaLabelerEngine(meta.MetaLabelerConfig(min_training_samples=100))
    engine.train(frame)
    feature_types = engine._model.get_booster().feature_types
    assert feature_types[0] == "c"


def test_extract_meta_features_from_price_frame_row(temp_modules) -> None:
    _store, _ensemble, meta = temp_modules
    row = pd.Series(
        {
            "canonical_state": 2,
            "return": 0.01,
            "volatility": 0.25,
            "volume_zscore": 1.5,
            "vix": 22.0,
            "vix_change": -0.1,
            "yield_10y": 4.2,
            "yield_10y_change": 0.01,
        }
    )
    features = meta.extract_meta_features(row)
    expected = {
        "hmm_state": 2.0,
        "log_ret": 0.01,
        "volatility": 0.25,
        "vol_z": 1.5,
        "vix_level": 22.0,
        "vix_change": -0.1,
        "yield_10y_level": 4.2,
        "yield_10y_change": 0.01,
    }
    assert {key: features[key] for key in expected} == expected
    assert set(meta.META_FEATURES).issubset(features)


def test_extract_meta_features_missing_columns(temp_modules) -> None:
    _store, _ensemble, meta = temp_modules
    features = meta.extract_meta_features(pd.Series({"canonical_state": 1}))
    assert set(features) == set(meta.META_FEATURES)
    assert features["hmm_state"] == 1.0
    assert features["vix_level"] == 0.0


def test_create_and_register(temp_modules) -> None:
    _store, ensemble, meta = temp_modules
    registry = ensemble.get_registry()
    engine = meta.create_and_register()
    assert registry.get("xgboost_meta_labeler") is engine


def test_registry_lists_meta_labeler(temp_modules) -> None:
    _store, ensemble, meta = temp_modules
    meta.create_and_register()
    assert "xgboost_meta_labeler" in ensemble.get_registry().list_analysts()


def _route_runtime(meta, ensemble, labeled_frame):
    registry = ensemble.AnalystRegistry()
    training_log: list[dict[str, object]] = []

    def get_registry():
        return registry

    def create_and_register_meta_labeler(config=None):
        engine = meta.MetaLabelerEngine(config or meta.DEFAULT_META_LABELER_CONFIG)
        registry.register(engine)
        return engine

    settings: dict[str, str] = {}
    fake_market = pd.DataFrame({"price": [100.0], "high": [101.0], "low": [99.0], "volume": [1_000_000], "vix": [20.0], "yield_10y": [4.0]})
    fake_regime = SimpleNamespace(price_frame=labeled_frame.copy())

    def log_training_run(*, version, ticker, model_path, metrics, config, status="active", notes=None):
        entry = {
            "version": int(version),
            "ticker": ticker,
            "model_path": model_path,
            "status": status,
            "notes": notes,
            "accuracy": metrics.get("accuracy"),
            "f1": metrics.get("f1"),
            "train_samples": metrics.get("train_samples"),
            "test_samples": metrics.get("test_samples"),
            "feature_importances": json.dumps(metrics.get("feature_importances", {})),
            "config_json": json.dumps(config),
            "trained_at": "2026-03-28T00:00:00+00:00",
        }
        training_log.insert(0, entry)
        return entry

    def get_training_history(limit=20):
        return training_log[:limit]

    def update_training_status(version, status):
        for entry in training_log:
            if int(entry["version"]) == int(version):
                entry["status"] = status
        return None

    return {
        "download_market_frame": lambda ticker, period="3y": SimpleNamespace(ticker=ticker, frame=fake_market),
        "fit_regime_model": lambda ticker, market_frame, training_window=504, refit_step=21: fake_regime,
        "build_labeled_frame": lambda ticker, market_frame, regime_result: labeled_frame.copy(),
        "get_registry": get_registry,
        "create_and_register_meta_labeler": create_and_register_meta_labeler,
        "MetaLabelerConfig": meta.MetaLabelerConfig,
        "DEFAULT_META_LABELER_CONFIG": meta.DEFAULT_META_LABELER_CONFIG,
        "get_setting": lambda key: settings.get(key),
        "set_setting": lambda key, value: settings.__setitem__(str(key), str(value)),
        "get_next_version": meta.get_next_version,
        "_version_path": meta._version_path,
        "list_saved_versions": meta.list_saved_versions,
        "auto_load_active_model": meta.auto_load_active_model,
        "log_training_run": log_training_run,
        "get_training_history": get_training_history,
        "update_training_status": update_training_status,
        "META_FEATURES": meta.META_FEATURES,
    }


def test_meta_labeler_train_route(temp_modules, monkeypatch) -> None:
    _store, ensemble, meta = temp_modules
    runtime = _route_runtime(meta, ensemble, _synthetic_labeled_frame())
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (runtime, None))
    client = TestClient(create_app())
    response = client.post("/regime/ensemble/meta-labeler/train", json={"ticker": "TEST"})
    assert response.status_code == 200
    payload = response.json()
    assert payload["ticker"] == "TEST"
    assert payload["ready"] is True
    assert "accuracy" in payload["metrics"]


def test_meta_labeler_status_route_not_created(temp_modules, monkeypatch) -> None:
    _store, ensemble, meta = temp_modules
    runtime = _route_runtime(meta, ensemble, _synthetic_labeled_frame())
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (runtime, None))
    client = TestClient(create_app())
    response = client.get("/regime/ensemble/meta-labeler/status")
    assert response.status_code == 200
    assert response.json() == {"ready": False, "status": "not_created"}


def test_meta_labeler_status_route_after_training(temp_modules, monkeypatch) -> None:
    _store, ensemble, meta = temp_modules
    runtime = _route_runtime(meta, ensemble, _synthetic_labeled_frame())
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (runtime, None))
    client = TestClient(create_app())
    train = client.post("/regime/ensemble/meta-labeler/train", json={"ticker": "TEST"})
    assert train.status_code == 200
    status = client.get("/regime/ensemble/meta-labeler/status")
    assert status.status_code == 200
    payload = status.json()
    assert payload["ready"] is True
    assert payload["status"] == "trained"
    assert "accuracy" in payload["metrics"]


def test_meta_labeler_train_route_missing_ticker(temp_modules, monkeypatch) -> None:
    _store, ensemble, meta = temp_modules
    runtime = _route_runtime(meta, ensemble, _synthetic_labeled_frame())
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (runtime, None))
    client = TestClient(create_app())
    response = client.post("/regime/ensemble/meta-labeler/train", json={})
    assert response.status_code == 400
