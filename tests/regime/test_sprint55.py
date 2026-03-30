from __future__ import annotations

import importlib
from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient
import pandas as pd
import pytest

from src.app.routes import regime as regime_route
from src.regime import ensemble as ensemble_module
from src.regime import meta_labeler as meta_labeler_module
from src.regime import persistence as persistence_module
from src.regime import triple_barrier as triple_barrier_module
from src.regime.analysts import KalmanFilterAnalyst, LSTMSequenceAnalyst


@pytest.fixture()
def temp_modules(tmp_path, monkeypatch):
    monkeypatch.setenv("HMM_DATA_DIR", str(tmp_path))
    store = importlib.reload(persistence_module)
    monkeypatch.setattr(store, "DB_PATH", tmp_path / "regime_watch.db")
    store = importlib.reload(persistence_module)
    ensemble = importlib.reload(ensemble_module)
    meta = importlib.reload(meta_labeler_module)
    triple = importlib.reload(triple_barrier_module)
    store._connect().close()
    return store, ensemble, meta, triple


def _synthetic_price_frame(trend: str = "up", neutral_every: int | None = None, rows: int = 40) -> pd.DataFrame:
    price = 100.0
    records = []
    for index in range(rows):
        if trend == "up":
            price += 1.2
            regime = "Neutral" if neutral_every and index % neutral_every == 0 else "Bull"
            high = price + 2.0
            low = price - 1.0
        else:
            price -= 1.2
            regime = "Neutral" if neutral_every and index % neutral_every == 0 else "Bear"
            high = price + 1.0
            low = price - 2.0
        records.append({"price": price, "high": high, "low": low, "regime": regime})
    return pd.DataFrame(records, index=pd.date_range("2025-01-01", periods=rows, freq="D"))


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


def _route_runtime(store, ensemble, meta, labeled_frame, *, failing_tickers: set[str] | None = None):
    registry = ensemble.AnalystRegistry()
    registry.register(ensemble.PassthroughAnalyst())
    registry.register(LSTMSequenceAnalyst())
    registry.register(KalmanFilterAnalyst())
    failing = {ticker.upper() for ticker in (failing_tickers or set())}

    def get_registry():
        return registry

    def create_and_register_meta_labeler(config=None):
        engine = meta.MetaLabelerEngine(config or meta.DEFAULT_META_LABELER_CONFIG)
        registry.register(engine)
        return engine

    def download_market_frame(ticker, period="3y", interval="1d"):
        del period, interval
        ticker = str(ticker).upper()
        if ticker in failing:
            raise RuntimeError(f"download failed for {ticker}")
        return SimpleNamespace(ticker=ticker, frame=pd.DataFrame({"price": [100.0], "high": [101.0], "low": [99.0], "volume": [1_000_000]}))

    def fit_regime_model(ticker, market_frame, training_window=504, refit_step=21):
        del ticker, market_frame, training_window, refit_step
        return SimpleNamespace(price_frame=labeled_frame.copy())

    def build_multi_ticker_labeled_frame(pairs):
        frames = []
        for ticker, _regime_result in pairs:
            frame = labeled_frame.copy()
            frame["ticker"] = str(ticker).upper()
            frames.append(frame)
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=list(labeled_frame.columns) + ["ticker"])

    return {
        "download_market_frame": download_market_frame,
        "fit_regime_model": fit_regime_model,
        "build_multi_ticker_labeled_frame": build_multi_ticker_labeled_frame,
        "get_registry": get_registry,
        "create_and_register_meta_labeler": create_and_register_meta_labeler,
        "MetaLabelerConfig": meta.MetaLabelerConfig,
        "DEFAULT_META_LABELER_CONFIG": meta.DEFAULT_META_LABELER_CONFIG,
        "EnsembleConfig": ensemble.EnsembleConfig,
        "get_setting": store.get_setting,
        "set_setting": store.set_setting,
        "get_all_settings": store.get_all_settings,
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
    app = FastAPI()
    app.include_router(regime_route.router)
    app.dependency_overrides[regime_route.require_actor] = lambda: "tester"
    app.dependency_overrides[regime_route.db_session] = lambda: iter([None])
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (runtime, None))
    return TestClient(app)


class TestMultiTickerLabeling:
    def test_combines_two_tickers(self, temp_modules) -> None:
        _store, _ensemble, _meta, triple = temp_modules
        first = SimpleNamespace(price_frame=_synthetic_price_frame("up", neutral_every=6))
        second = SimpleNamespace(price_frame=_synthetic_price_frame("down", neutral_every=5))
        frame = triple.build_multi_ticker_labeled_frame([("NVDA", first), ("MSFT", second)])
        assert "ticker" in frame.columns
        assert set(frame["ticker"]) == {"NVDA", "MSFT"}
        assert frame["barrier_outcome"].notna().all()

    def test_empty_pairs_returns_empty_frame(self, temp_modules) -> None:
        _store, _ensemble, _meta, triple = temp_modules
        frame = triple.build_multi_ticker_labeled_frame([])
        assert frame.empty
        assert "ticker" in frame.columns
        assert "barrier_outcome" in frame.columns

    def test_skips_nan_outcomes(self, temp_modules) -> None:
        _store, _ensemble, _meta, triple = temp_modules
        result = SimpleNamespace(price_frame=_synthetic_price_frame("up", neutral_every=2))
        frame = triple.build_multi_ticker_labeled_frame([("AAPL", result)])
        assert frame["barrier_outcome"].notna().all()
        assert len(frame) < len(result.price_frame)

    def test_single_ticker_equivalent(self, temp_modules) -> None:
        _store, _ensemble, _meta, triple = temp_modules
        result = SimpleNamespace(price_frame=_synthetic_price_frame("down"))
        single = triple.build_labeled_frame("AVGO", pd.DataFrame(), result)
        multi = triple.build_multi_ticker_labeled_frame([("AVGO", result)])
        comparable = multi.drop(columns=["ticker"]).reset_index(drop=True)
        expected = single.loc[single["barrier_outcome"].notna()].reset_index(drop=True)
        pd.testing.assert_frame_equal(comparable, expected, check_dtype=False)


class TestMultiTickerTrainRoute:
    def test_train_multi_two_tickers(self, temp_modules, monkeypatch) -> None:
        store, ensemble, meta, _triple = temp_modules
        runtime = _route_runtime(store, ensemble, meta, _synthetic_labeled_frame())
        client = _client(monkeypatch, runtime)
        response = client.post("/regime/ensemble/meta-labeler/train-multi", json={"tickers": ["MSFT", "AAPL"]})
        assert response.status_code == 200
        payload = response.json()
        assert payload["status"] == "trained"
        assert payload["tickers_used"] == ["AAPL", "MSFT"]
        assert payload["combined_samples"] == 480
        assert payload["version"] == 1
        assert store.get_training_history(limit=1)[0]["ticker"] == "AAPL,MSFT"

    def test_train_multi_empty_tickers_400(self, temp_modules, monkeypatch) -> None:
        store, ensemble, meta, _triple = temp_modules
        runtime = _route_runtime(store, ensemble, meta, _synthetic_labeled_frame())
        client = _client(monkeypatch, runtime)
        response = client.post("/regime/ensemble/meta-labeler/train-multi", json={"tickers": []})
        assert response.status_code == 400

    def test_train_multi_one_ticker_skipped(self, temp_modules, monkeypatch) -> None:
        store, ensemble, meta, _triple = temp_modules
        runtime = _route_runtime(store, ensemble, meta, _synthetic_labeled_frame(), failing_tickers={"MSFT"})
        client = _client(monkeypatch, runtime)
        response = client.post("/regime/ensemble/meta-labeler/train-multi", json={"tickers": ["AAPL", "MSFT"]})
        assert response.status_code == 200
        payload = response.json()
        assert payload["tickers_used"] == ["AAPL"]
        assert payload["tickers_skipped"] == ["MSFT"]
        assert payload["combined_samples"] == 240

    def test_train_multi_insufficient_data(self, temp_modules, monkeypatch) -> None:
        store, ensemble, meta, _triple = temp_modules
        runtime = _route_runtime(store, ensemble, meta, _synthetic_labeled_frame(rows=40))
        client = _client(monkeypatch, runtime)
        response = client.post("/regime/ensemble/meta-labeler/train-multi", json={"tickers": ["AAPL"]})
        assert response.status_code == 200
        payload = response.json()
        assert payload["status"] == "insufficient_data"
        assert payload["ready"] is False

    def test_train_multi_all_failed_400(self, temp_modules, monkeypatch) -> None:
        store, ensemble, meta, _triple = temp_modules
        runtime = _route_runtime(store, ensemble, meta, _synthetic_labeled_frame(), failing_tickers={"AAPL", "MSFT"})
        client = _client(monkeypatch, runtime)
        response = client.post("/regime/ensemble/meta-labeler/train-multi", json={"tickers": ["AAPL", "MSFT"]})
        assert response.status_code == 400
        assert "No tickers produced valid regime results" in response.json()["detail"]


class TestStubAnalysts:
    def test_lstm_stub_registered(self, temp_modules) -> None:
        _store, ensemble, _meta, _triple = temp_modules
        assert "lstm_sequence" in ensemble.get_registry().list_analysts()

    def test_kalman_stub_registered(self, temp_modules) -> None:
        _store, ensemble, _meta, _triple = temp_modules
        assert "kalman_filter" in ensemble.get_registry().list_analysts()

    def test_lstm_not_ready(self) -> None:
        assert LSTMSequenceAnalyst().is_ready() is False

    def test_kalman_not_ready(self) -> None:
        assert KalmanFilterAnalyst().is_ready() is False

    def test_lstm_analyze_returns_neutral(self) -> None:
        result = LSTMSequenceAnalyst().analyze("NVDA", {}, None)
        assert result.signal == "neutral"
        assert result.confidence == 0.5

    def test_kalman_analyze_returns_neutral(self) -> None:
        result = KalmanFilterAnalyst().analyze("NVDA", {}, None)
        assert result.signal == "neutral"
        assert result.confidence == 0.5

    def test_stubs_excluded_from_ready_analysts(self, temp_modules) -> None:
        _store, ensemble, _meta, _triple = temp_modules
        ready = {analyst.name for analyst in ensemble.get_registry().ready_analysts()}
        assert "lstm_sequence" not in ready
        assert "kalman_filter" not in ready


class TestWeightsRoutes:
    def test_get_weights_default(self, temp_modules, monkeypatch) -> None:
        store, ensemble, meta, _triple = temp_modules
        runtime = _route_runtime(store, ensemble, meta, _synthetic_labeled_frame())
        client = _client(monkeypatch, runtime)
        payload = client.get("/regime/ensemble/weights").json()
        analysts = {item["name"]: item for item in payload["analysts"]}
        assert payload["aggregation_method"] == "mean"
        assert analysts["passthrough"]["enabled"] is True
        assert analysts["lstm_sequence"]["enabled"] is False
        assert analysts["kalman_filter"]["enabled"] is False

    def test_put_weights_valid(self, temp_modules, monkeypatch) -> None:
        store, ensemble, meta, _triple = temp_modules
        runtime = _route_runtime(store, ensemble, meta, _synthetic_labeled_frame())
        client = _client(monkeypatch, runtime)
        response = client.put(
            "/regime/ensemble/weights",
            json={
                "analysts": {
                    "passthrough": {"enabled": True, "weight": 0.8},
                    "lstm_sequence": {"enabled": True, "weight": 1.5},
                },
                "aggregation_method": "weighted",
            },
        )
        assert response.status_code == 200
        payload = response.json()
        analysts = {item["name"]: item for item in payload["analysts"]}
        assert payload["aggregation_method"] == "weighted"
        assert analysts["lstm_sequence"]["enabled"] is True
        assert analysts["lstm_sequence"]["weight"] == pytest.approx(1.5)
        assert store.get_setting("ensemble_analyst_lstm_sequence_enabled") == "true"

    def test_put_weights_invalid_range(self, temp_modules, monkeypatch) -> None:
        store, ensemble, meta, _triple = temp_modules
        runtime = _route_runtime(store, ensemble, meta, _synthetic_labeled_frame())
        client = _client(monkeypatch, runtime)
        response = client.put(
            "/regime/ensemble/weights",
            json={"analysts": {"passthrough": {"enabled": True, "weight": 5.5}}, "aggregation_method": "mean"},
        )
        assert response.status_code == 400

    def test_put_weights_unknown_analyst(self, temp_modules, monkeypatch) -> None:
        store, ensemble, meta, _triple = temp_modules
        runtime = _route_runtime(store, ensemble, meta, _synthetic_labeled_frame())
        client = _client(monkeypatch, runtime)
        response = client.put(
            "/regime/ensemble/weights",
            json={"analysts": {"unknown_analyst": {"enabled": True, "weight": 1.0}}, "aggregation_method": "mean"},
        )
        assert response.status_code == 400

    def test_put_weights_invalid_method(self, temp_modules, monkeypatch) -> None:
        store, ensemble, meta, _triple = temp_modules
        runtime = _route_runtime(store, ensemble, meta, _synthetic_labeled_frame())
        client = _client(monkeypatch, runtime)
        response = client.put(
            "/regime/ensemble/weights",
            json={"analysts": {"passthrough": {"enabled": True, "weight": 1.0}}, "aggregation_method": "median"},
        )
        assert response.status_code == 400
