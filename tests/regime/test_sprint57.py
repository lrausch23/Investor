from __future__ import annotations

import datetime as dt
from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient
import numpy as np
import pandas as pd
import pytest

from src.app.routes import regime as regime_route
from src.regime.analysts import KalmanFilterAnalyst, LSTMConfig, LSTMSequenceAnalyst
from src.regime.ensemble import get_registry
from src.regime.meta_labeler import META_FEATURES


def _labeled_frame(rows: int = 260) -> pd.DataFrame:
    index = pd.date_range("2024-01-01", periods=rows, freq="B")
    frame = pd.DataFrame(index=index)
    frame["hmm_state"] = np.where(np.arange(rows) % 3 == 0, 2, np.where(np.arange(rows) % 3 == 1, 1, 0))
    frame["log_ret"] = np.linspace(-0.02, 0.03, rows)
    frame["volatility"] = 0.2 + (np.arange(rows) % 10) / 100
    frame["vol_z"] = np.sin(np.arange(rows) / 8)
    frame["vix_level"] = 18 + (np.arange(rows) % 7)
    frame["vix_change"] = np.cos(np.arange(rows) / 9) / 10
    frame["yield_10y_level"] = 4.0 + (np.arange(rows) % 5) / 10
    frame["yield_10y_change"] = np.sin(np.arange(rows) / 10) / 20
    frame["barrier_outcome"] = (np.arange(rows) % 2 == 0).astype(int)
    return frame


def test_lstm_train_save_load_and_analyze(tmp_path: Path) -> None:
    analyst = LSTMSequenceAnalyst()
    metrics = analyst.train(_labeled_frame())
    assert metrics["status"] == "trained"
    assert analyst.is_ready() is True
    model_path = tmp_path / "lstm.json"
    analyst.save_model(model_path)
    loaded = LSTMSequenceAnalyst()
    loaded.load_model(model_path)
    regime_result = type("RegimeResult", (), {"price_frame": _labeled_frame(40)[META_FEATURES]})()
    result = loaded.analyze("NVDA", {}, regime_result)
    assert result.signal in {"confirm", "neutral", "veto"}
    assert 0.0 <= result.confidence <= 1.0


def test_kalman_ready_after_min_observations_and_reset() -> None:
    analyst = KalmanFilterAnalyst()
    regime_result = type("RegimeResult", (), {"latest_label": "Bull"})()
    for _ in range(5):
        analyst.analyze("NVDA", {}, regime_result)
    assert analyst.is_ready() is True
    analyst.reset()
    assert analyst.is_ready() is False


def test_lstm_training_route(monkeypatch, tmp_path: Path) -> None:
    registry = get_registry()
    runtime = {
        "get_registry": lambda: registry,
        "LSTMConfig": LSTMConfig,
        "LSTMSequenceAnalyst": LSTMSequenceAnalyst,
        "download_market_frame": lambda ticker, period="3y": type("MarketSeries", (), {"frame": _labeled_frame(260)})(),
        "fit_regime_model": lambda ticker, market_frame, training_window=504, refit_step=21: type("RegimeResult", (), {"price_frame": market_frame})(),
        "build_multi_ticker_labeled_frame": lambda pairs: pd.concat([pair[1].price_frame for pair in pairs], axis=0),
        "set_setting": lambda key, value: None,
    }
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (runtime, None))
    monkeypatch.setattr(regime_route, "_lstm_model_dir", lambda: tmp_path)

    app = FastAPI()
    app.include_router(regime_route.router)
    app.dependency_overrides[regime_route.require_actor] = lambda: "tester"
    app.dependency_overrides[regime_route.db_session] = lambda: iter([None])
    client = TestClient(app)

    response = client.post("/regime/ensemble/lstm/train", json={"tickers": ["NVDA", "MSFT"], "epochs": 5})
    assert response.status_code == 200
    payload = response.json()
    assert payload["ready"] is True
    assert payload["version"] == 1
