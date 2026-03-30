from __future__ import annotations

import importlib
from types import SimpleNamespace

import pandas as pd
import pytest

from src.regime.exceptions import DataValidationError
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
    return store, ensemble, meta, tmp_path


def _synthetic_labeled_frame(rows: int = 240) -> pd.DataFrame:
    index = pd.date_range("2024-01-01", periods=rows, freq="D")
    return pd.DataFrame(
        {
            "canonical_state": [0 if i % 3 == 0 else 2 if i % 3 == 1 else 1 for i in range(rows)],
            "return": [0.03 if i < rows // 2 else -0.03 for i in range(rows)],
            "volatility": [0.15 if i < rows // 2 else 0.35 for i in range(rows)],
            "volume_zscore": [1.2 if i < rows // 2 else -1.2 for i in range(rows)],
            "vix": [18.0 if i < rows // 2 else 30.0 for i in range(rows)],
            "vix_change": [-0.2 if i < rows // 2 else 0.2 for i in range(rows)],
            "yield_10y": [4.0 if i < rows // 2 else 4.8 for i in range(rows)],
            "yield_10y_change": [-0.01 if i < rows // 2 else 0.02 for i in range(rows)],
            "barrier_outcome": [1 if i < rows // 2 else 0 for i in range(rows)],
        },
        index=index,
    )


def _train_engine(meta):
    engine = meta.MetaLabelerEngine(meta.MetaLabelerConfig(min_training_samples=100))
    metrics = engine.train(_synthetic_labeled_frame())
    return engine, metrics


def _route_runtime(store, ensemble, meta, labeled_frame):
    registry = ensemble.AnalystRegistry()

    def get_registry():
        return registry

    def create_and_register_meta_labeler(config=None):
        engine = meta.MetaLabelerEngine(config or meta.DEFAULT_META_LABELER_CONFIG)
        registry.register(engine)
        return engine

    fake_market = pd.DataFrame({"price": [100.0], "high": [101.0], "low": [99.0], "volume": [1_000_000]})
    fake_regime = SimpleNamespace(price_frame=labeled_frame.copy())

    return {
        "download_market_frame": lambda ticker, period="3y", interval="1d": SimpleNamespace(ticker=ticker, frame=fake_market),
        "fit_regime_model": lambda ticker, market_frame, training_window=504, refit_step=21: fake_regime,
        "build_labeled_frame": lambda ticker, market_frame, regime_result: labeled_frame.copy(),
        "get_registry": get_registry,
        "create_and_register_meta_labeler": create_and_register_meta_labeler,
        "MetaLabelerConfig": meta.MetaLabelerConfig,
        "DEFAULT_META_LABELER_CONFIG": meta.DEFAULT_META_LABELER_CONFIG,
        "get_setting": store.get_setting,
        "set_setting": store.set_setting,
        "log_training_run": store.log_training_run,
        "get_training_history": store.get_training_history,
        "get_training_run": store.get_training_run,
        "update_training_status": store.update_training_status,
        "get_next_version": meta.get_next_version,
        "list_saved_versions": meta.list_saved_versions,
        "_version_path": meta._version_path,
        "auto_load_active_model": meta.auto_load_active_model,
        "should_retrain": meta.should_retrain,
        "META_FEATURES": meta.META_FEATURES,
    }


def _client(monkeypatch, runtime) -> TestClient:
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (runtime, None))
    return TestClient(create_app())


def test_save_model_creates_json_file(temp_modules) -> None:
    _store, _ensemble, meta, tmp_path = temp_modules
    engine, _metrics = _train_engine(meta)
    target = tmp_path / "models" / "meta_labeler_v1.json"
    result = engine.save_model(str(target))
    assert target.exists()
    assert target.suffix == ".json"
    assert target.stat().st_size > 0
    assert result["format"] == "json"


def test_load_model_restores_ready_state(temp_modules) -> None:
    _store, _ensemble, meta, tmp_path = temp_modules
    engine, _metrics = _train_engine(meta)
    target = tmp_path / "models" / "meta_labeler_v1.json"
    engine.save_model(str(target))
    loaded = meta.MetaLabelerEngine()
    loaded.load_model(str(target))
    result = loaded.analyze("NVDA", {key: 0.1 for key in meta.META_FEATURES}, None)
    assert loaded.is_ready() is True
    assert result.confidence != 1.0


def test_load_model_file_not_found(temp_modules) -> None:
    _store, _ensemble, meta, tmp_path = temp_modules
    with pytest.raises(FileNotFoundError):
        meta.MetaLabelerEngine().load_model(str(tmp_path / "models" / "missing.json"))


def test_save_model_untrained_raises(temp_modules) -> None:
    _store, _ensemble, meta, tmp_path = temp_modules
    with pytest.raises(DataValidationError):
        meta.MetaLabelerEngine().save_model(str(tmp_path / "models" / "meta_labeler_v1.json"))


def test_get_next_version_empty_dir(temp_modules) -> None:
    _store, _ensemble, meta, _tmp_path = temp_modules
    assert meta.get_next_version() == 1


def test_get_next_version_increments(temp_modules) -> None:
    _store, _ensemble, meta, _tmp_path = temp_modules
    meta._version_path(1)
    open(meta._version_path(1), "w", encoding="utf-8").write("{}")
    open(meta._version_path(3), "w", encoding="utf-8").write("{}")
    assert meta.get_next_version() == 4


def test_list_saved_versions(temp_modules) -> None:
    _store, _ensemble, meta, _tmp_path = temp_modules
    open(meta._version_path(1), "w", encoding="utf-8").write("{}")
    open(meta._version_path(2), "w", encoding="utf-8").write("{}")
    versions = meta.list_saved_versions()
    assert [item["version"] for item in versions] == [1, 2]
    assert all(item["size_bytes"] > 0 for item in versions)


def test_log_training_run(temp_modules) -> None:
    store, _ensemble, _meta, _tmp_path = temp_modules
    store.log_training_run(version=1, ticker="NVDA", model_path="/tmp/model.json", metrics={"accuracy": 0.8, "feature_importances": {"a": 1.0}})
    history = store.get_training_history()
    assert history[0]["version"] == 1
    assert history[0]["ticker"] == "NVDA"


def test_get_training_run_by_version(temp_modules) -> None:
    store, _ensemble, _meta, _tmp_path = temp_modules
    store.log_training_run(version=1, ticker="NVDA", model_path="/tmp/model1.json", metrics={"accuracy": 0.7, "feature_importances": {}})
    store.log_training_run(version=2, ticker="AVGO", model_path="/tmp/model2.json", metrics={"accuracy": 0.8, "feature_importances": {}})
    row = store.get_training_run(1)
    assert row is not None
    assert row["version"] == 1


def test_update_training_status(temp_modules) -> None:
    store, _ensemble, _meta, _tmp_path = temp_modules
    store.log_training_run(version=1, ticker="NVDA", model_path="/tmp/model.json", metrics={"accuracy": 0.8, "feature_importances": {}})
    assert store.update_training_status(1, "superseded") is True
    assert store.get_training_run(1)["status"] == "superseded"


def test_auto_load_active_model(temp_modules) -> None:
    _store, _ensemble, meta, _tmp_path = temp_modules
    engine, _metrics = _train_engine(meta)
    engine.save_model(meta._version_path(1))
    fresh = meta.MetaLabelerEngine()
    result = meta.auto_load_active_model(fresh, 1)
    assert result["loaded"] is True
    assert fresh.is_ready() is True


def test_auto_load_no_version(temp_modules) -> None:
    _store, _ensemble, meta, _tmp_path = temp_modules
    fresh = meta.MetaLabelerEngine()
    result = meta.auto_load_active_model(fresh, None)
    assert result["loaded"] is False
    assert fresh.is_ready() is False


def test_auto_load_missing_file(temp_modules) -> None:
    _store, _ensemble, meta, _tmp_path = temp_modules
    result = meta.auto_load_active_model(meta.MetaLabelerEngine(), 99)
    assert result["loaded"] is False
    assert result["status"] == "file_not_found"


def test_should_retrain_no_history(temp_modules) -> None:
    _store, _ensemble, meta, _tmp_path = temp_modules
    assert meta.should_retrain(None) is True


def test_should_retrain_wrong_day(temp_modules, monkeypatch) -> None:
    _store, _ensemble, meta, _tmp_path = temp_modules
    class FakeDateTime(meta.datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 3, 30, tzinfo=tz or meta.timezone.utc)
    monkeypatch.setattr(meta, "datetime", FakeDateTime)
    assert meta.should_retrain("2026-03-20T00:00:00+00:00", "Sunday") is False


def test_should_retrain_recent_training(temp_modules, monkeypatch) -> None:
    _store, _ensemble, meta, _tmp_path = temp_modules
    class FakeDateTime(meta.datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 3, 29, tzinfo=tz or meta.timezone.utc)
    monkeypatch.setattr(meta, "datetime", FakeDateTime)
    assert meta.should_retrain("2026-03-27T00:00:00+00:00", "Sunday") is False


def test_train_route_saves_model(temp_modules, monkeypatch) -> None:
    store, ensemble, meta, _tmp_path = temp_modules
    runtime = _route_runtime(store, ensemble, meta, _synthetic_labeled_frame())
    client = _client(monkeypatch, runtime)
    response = client.post("/regime/ensemble/meta-labeler/train", json={"ticker": "NVDA"})
    assert response.status_code == 200
    payload = response.json()
    assert payload["version"] == 1
    assert meta._version_path(1).endswith(".json")
    assert payload["path"].endswith(".json")


def test_rollback_route(temp_modules, monkeypatch) -> None:
    store, ensemble, meta, _tmp_path = temp_modules
    runtime = _route_runtime(store, ensemble, meta, _synthetic_labeled_frame())
    engine, metrics = _train_engine(meta)
    engine.save_model(meta._version_path(1))
    store.log_training_run(version=1, ticker="NVDA", model_path=meta._version_path(1), metrics=metrics)
    engine.save_model(meta._version_path(2))
    store.log_training_run(version=2, ticker="NVDA", model_path=meta._version_path(2), metrics=metrics)
    store.set_setting("meta_labeler_active_version", "2")
    client = _client(monkeypatch, runtime)
    response = client.post("/regime/ensemble/meta-labeler/rollback", json={"version": 1})
    assert response.status_code == 200
    assert response.json()["active_version"] == 1


def test_versions_route(temp_modules, monkeypatch) -> None:
    store, ensemble, meta, _tmp_path = temp_modules
    runtime = _route_runtime(store, ensemble, meta, _synthetic_labeled_frame())
    open(meta._version_path(1), "w", encoding="utf-8").write("{}")
    open(meta._version_path(2), "w", encoding="utf-8").write("{}")
    store.log_training_run(version=1, ticker="NVDA", model_path=meta._version_path(1), metrics={"accuracy": 0.70, "f1": 0.68, "train_samples": 100, "feature_importances": {"hmm_state": 0.1}})
    store.log_training_run(version=2, ticker="NVDA", model_path=meta._version_path(2), metrics={"accuracy": 0.80, "f1": 0.78, "train_samples": 120, "feature_importances": {"hmm_state": 0.2}})
    store.set_setting("meta_labeler_active_version", "2")
    client = _client(monkeypatch, runtime)
    response = client.get("/regime/ensemble/meta-labeler/versions")
    assert response.status_code == 200
    payload = response.json()
    assert len(payload["versions"]) == 2
    assert payload["active_version"] == 2
    assert payload["comparison"]["accuracy_delta"] == 0.1


def test_status_route_includes_version(temp_modules, monkeypatch) -> None:
    store, ensemble, meta, _tmp_path = temp_modules
    runtime = _route_runtime(store, ensemble, meta, _synthetic_labeled_frame())
    engine, metrics = _train_engine(meta)
    runtime["get_registry"]().register(engine)
    store.log_training_run(version=1, ticker="NVDA", model_path=meta._version_path(1), metrics=metrics)
    store.set_setting("meta_labeler_active_version", "1")
    client = _client(monkeypatch, runtime)
    response = client.get("/regime/ensemble/meta-labeler/status")
    assert response.status_code == 200
    assert response.json()["active_version"] == 1
