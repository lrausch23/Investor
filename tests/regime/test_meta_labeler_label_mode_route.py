from __future__ import annotations

from types import SimpleNamespace

import pandas as pd
from fastapi.testclient import TestClient

from src.app.main import create_app
from src.app.routes import regime as regime_route
from src.regime.meta_labeler import DEFAULT_META_LABELER_CONFIG, MetaLabelerConfig
from src.regime.triple_barrier import DEFAULT_MANAGED_EXIT_CONFIG, ManagedExitConfig


class _StubRegistry:
    def get(self, _name):
        return None


class _StubEngine:
    def __init__(self):
        self._config = MetaLabelerConfig(min_training_samples=1)
        self.train_calls: list[dict[str, object]] = []

    def train(self, frame, **kwargs):
        self.train_calls.append({"frame": frame.copy(), "kwargs": dict(kwargs)})
        return {
            "status": "trained",
            "train_samples": int(len(frame)),
            "test_samples": 0,
            "accuracy": 1.0,
            "feature_importances": {},
            **kwargs,
        }

    def is_ready(self):
        return True

    def save_model(self, path):
        return {"path": str(path), "format": "json", "training_metrics": {}, "feature_importances": {}}


def _small_labeled_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "canonical_state": [0, 1],
            "return": [0.01, -0.01],
            "volatility": [0.2, 0.3],
            "volume_zscore": [1.0, -1.0],
            "vix": [18.0, 24.0],
            "vix_change": [-0.1, 0.1],
            "yield_10y": [4.0, 4.2],
            "yield_10y_change": [-0.01, 0.01],
            "barrier_outcome": [1, 0],
            "label_end_idx": [1, 1],
            "label_entry_date": pd.to_datetime(["2026-01-01", "2026-01-02"]),
            "label_end_date": pd.to_datetime(["2026-01-02", "2026-01-03"]),
        }
    )


def _runtime(settings: dict[str, str], engine: _StubEngine, calls: dict[str, int], logged_configs: list[dict[str, object]], tmp_path):
    frame = _small_labeled_frame()

    def build_managed_labeled_frame(ticker, regime_result, config=None):
        calls["managed"] += 1
        assert ticker == "NVDA"
        assert regime_result.price_frame is frame
        assert isinstance(config, ManagedExitConfig)
        return frame.copy()

    def build_labeled_frame(ticker, market_frame, regime_result):
        calls["legacy"] += 1
        assert ticker == "NVDA"
        assert market_frame is frame
        assert regime_result.price_frame is frame
        return frame.copy()

    def log_training_run(*, version, ticker, model_path, metrics, config, notes=None):
        del version, ticker, model_path, metrics, notes
        logged_configs.append(dict(config))
        return {"logged": True}

    return {
        "download_market_frame": lambda ticker, period="3y": SimpleNamespace(ticker=ticker, frame=frame),
        "fit_regime_model": lambda ticker, market_frame, training_window=504, refit_step=21: SimpleNamespace(price_frame=market_frame),
        "build_managed_labeled_frame": build_managed_labeled_frame,
        "build_labeled_frame": build_labeled_frame,
        "ManagedExitConfig": ManagedExitConfig,
        "DEFAULT_MANAGED_EXIT_CONFIG": DEFAULT_MANAGED_EXIT_CONFIG,
        "get_registry": lambda: _StubRegistry(),
        "create_and_register_meta_labeler": lambda config=None: engine,
        "MetaLabelerConfig": MetaLabelerConfig,
        "DEFAULT_META_LABELER_CONFIG": DEFAULT_META_LABELER_CONFIG,
        "get_setting": lambda key: settings.get(str(key)),
        "set_setting": lambda key, value: settings.__setitem__(str(key), str(value)),
        "update_training_status": lambda version, status: True,
        "get_next_version": lambda: 1,
        "_version_path": lambda version: str(tmp_path / f"meta_labeler_v{int(version)}.json"),
        "log_training_run": log_training_run,
    }


def test_meta_labeler_train_route_defaults_to_managed_label_mode(monkeypatch, tmp_path) -> None:
    settings: dict[str, str] = {}
    calls = {"managed": 0, "legacy": 0}
    logged_configs: list[dict[str, object]] = []
    engine = _StubEngine()
    runtime = _runtime(settings, engine, calls, logged_configs, tmp_path)
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (runtime, None))
    app = create_app()
    app.dependency_overrides[regime_route.require_actor] = lambda: "tester"
    client = TestClient(app)

    response = client.post("/regime/ensemble/meta-labeler/train", json={"ticker": "NVDA"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["label_mode"] == "managed"
    assert payload["managed_exit_config"]["time_stop_days"] == DEFAULT_MANAGED_EXIT_CONFIG.time_stop_days
    assert calls == {"managed": 1, "legacy": 0}
    assert engine.train_calls[0]["kwargs"]["label_mode"] == "managed"
    assert logged_configs[0]["label_mode"] == "managed"
    assert logged_configs[0]["managed_exit_config"]["time_stop_days"] == DEFAULT_MANAGED_EXIT_CONFIG.time_stop_days


def test_meta_labeler_train_route_can_select_legacy_label_mode(monkeypatch, tmp_path) -> None:
    settings = {"meta_labeler_label_mode": "legacy"}
    calls = {"managed": 0, "legacy": 0}
    logged_configs: list[dict[str, object]] = []
    engine = _StubEngine()
    runtime = _runtime(settings, engine, calls, logged_configs, tmp_path)
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (runtime, None))
    app = create_app()
    app.dependency_overrides[regime_route.require_actor] = lambda: "tester"
    client = TestClient(app)

    response = client.post("/regime/ensemble/meta-labeler/train", json={"ticker": "NVDA"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["label_mode"] == "legacy"
    assert payload["managed_exit_config"] is None
    assert calls == {"managed": 0, "legacy": 1}
    assert engine.train_calls[0]["kwargs"]["label_mode"] == "legacy"
    assert logged_configs[0]["label_mode"] == "legacy"
    assert "managed_exit_config" not in logged_configs[0]
