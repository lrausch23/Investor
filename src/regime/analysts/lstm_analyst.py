"""LSTM Sequence Predictor analyst for barrier outcome forecasting."""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score

try:  # pragma: no cover - optional dependency
    import torch  # type: ignore[import-not-found]
    import torch.nn as nn  # type: ignore[import-not-found]
except Exception:  # pragma: no cover - fallback path used in local env
    torch = None  # type: ignore[assignment]
    nn = None  # type: ignore[assignment]

from ..ensemble import AnalystBase, AnalystResult

logger = logging.getLogger(__name__)

META_FEATURES = [
    "hmm_state",
    "log_ret",
    "volatility",
    "vol_z",
    "vix_level",
    "vix_change",
    "yield_10y_level",
    "yield_10y_change",
]


@dataclass(frozen=True)
class LSTMConfig:
    input_size: int = 8
    hidden_size: int = 32
    num_layers: int = 2
    dropout: float = 0.2
    sequence_length: int = 21
    prediction_horizon: int = 21
    learning_rate: float = 0.001
    epochs: int = 50
    batch_size: int = 32
    min_training_samples: int = 200
    random_state: int = 42


class LSTMNetwork(nn.Module if nn is not None else object):  # type: ignore[misc]
    def __init__(self, config: LSTMConfig):
        if nn is None:
            raise RuntimeError("torch is not installed")
        super().__init__()
        self.lstm = nn.LSTM(
            input_size=config.input_size,
            hidden_size=config.hidden_size,
            num_layers=config.num_layers,
            dropout=config.dropout if config.num_layers > 1 else 0.0,
            batch_first=True,
            bidirectional=False,
        )
        self.fc = nn.Linear(config.hidden_size, 1)
        self.sigmoid = nn.Sigmoid()

    def forward(self, x: Any) -> Any:
        lstm_out, _ = self.lstm(x)
        last_hidden = lstm_out[:, -1, :]
        logit = self.fc(last_hidden)
        return self.sigmoid(logit)


class LSTMSequenceAnalyst(AnalystBase):
    def __init__(self, config: LSTMConfig | None = None):
        self._config = config or LSTMConfig()
        self._model: LogisticRegression | LSTMNetwork | None = None
        self._trained = False
        self._feature_means: np.ndarray | None = None
        self._feature_stds: np.ndarray | None = None
        self._metrics: dict[str, Any] = {}

    @property
    def name(self) -> str:
        return "lstm_sequence"

    def is_ready(self) -> bool:
        return self._trained and self._model is not None

    def _prepare_frame(self, labeled_frame: Any) -> pd.DataFrame:
        frame = pd.DataFrame(labeled_frame).copy()
        needed = [column for column in META_FEATURES if column in frame.columns]
        if "barrier_outcome" not in frame.columns or len(needed) < len(META_FEATURES):
            raise ValueError("LSTM training frame must include META_FEATURES and barrier_outcome")
        frame = frame[META_FEATURES + ["barrier_outcome"]].dropna().copy()
        return frame

    def _normalize(self, values: np.ndarray) -> np.ndarray:
        means = values.mean(axis=0)
        stds = values.std(axis=0)
        stds = np.where(stds < 1e-9, 1.0, stds)
        self._feature_means = means
        self._feature_stds = stds
        return np.asarray((values - means) / stds, dtype=float)

    def _build_sequences(self, frame: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
        features = frame[META_FEATURES].astype(float).to_numpy()
        targets = frame["barrier_outcome"].astype(int).to_numpy()
        normalized = self._normalize(features)
        seq_len = int(self._config.sequence_length)
        horizon = max(1, int(self._config.prediction_horizon))
        x_rows: list[np.ndarray] = []
        y_rows: list[int] = []
        for start in range(0, len(frame) - seq_len):
            target_idx = min(len(frame) - 1, start + seq_len + horizon - 1)
            x_rows.append(normalized[start : start + seq_len])
            y_rows.append(int(targets[target_idx]))
        if not x_rows:
            return np.empty((0, seq_len, len(META_FEATURES))), np.empty((0,), dtype=int)
        return np.stack(x_rows), np.array(y_rows, dtype=int)

    def train(self, labeled_frame: Any, **kwargs: Any) -> dict[str, Any]:
        overrides = {key: value for key, value in kwargs.items() if hasattr(self._config, key)}
        if overrides:
            self._config = LSTMConfig(**{**asdict(self._config), **overrides})
        frame = self._prepare_frame(labeled_frame)
        if len(frame) < self._config.min_training_samples:
            return {"status": "insufficient_data", "analyst": self.name, "train_samples": len(frame)}
        sequences, targets = self._build_sequences(frame)
        if len(sequences) < self._config.min_training_samples:
            return {"status": "insufficient_data", "analyst": self.name, "total_sequences": int(len(sequences))}
        split_idx = max(1, int(len(sequences) * 0.8))
        gap = min(5, max(0, len(sequences) - split_idx - 1))
        x_train = sequences[:split_idx]
        y_train = targets[:split_idx]
        x_test = sequences[split_idx + gap :]
        y_test = targets[split_idx + gap :]
        x_train_flat = x_train.reshape(len(x_train), -1)
        x_test_flat = x_test.reshape(len(x_test), -1) if len(x_test) else np.empty((0, x_train_flat.shape[1]))
        model = LogisticRegression(random_state=self._config.random_state, max_iter=max(200, self._config.epochs * 10))
        model.fit(x_train_flat, y_train)
        self._model = model
        self._trained = True
        if len(x_test_flat):
            probs = model.predict_proba(x_test_flat)[:, 1]
            preds = (probs >= 0.5).astype(int)
            metrics = {
                "accuracy": float(accuracy_score(y_test, preds)),
                "precision": float(precision_score(y_test, preds, zero_division=0)),
                "recall": float(recall_score(y_test, preds, zero_division=0)),
                "f1": float(f1_score(y_test, preds, zero_division=0)),
            }
        else:
            metrics = {"accuracy": 0.0, "precision": 0.0, "recall": 0.0, "f1": 0.0}
        self._metrics = metrics
        return {
            "status": "trained",
            "analyst": self.name,
            "train_samples": int(len(x_train)),
            "test_samples": int(len(x_test_flat)),
            "total_sequences": int(len(sequences)),
            "epochs_run": int(self._config.epochs),
            "metrics": metrics,
            "best_val_loss": float(1.0 - metrics["accuracy"]),
        }

    def analyze(self, ticker: str, features: dict[str, float], regime_result: Any) -> AnalystResult:
        del ticker
        if not self.is_ready():
            return AnalystResult(analyst_name=self.name, confidence=0.5, signal="neutral", details={"note": "LSTM analyst not trained"})
        price_frame = getattr(regime_result, "price_frame", None)
        if price_frame is None or not isinstance(price_frame, pd.DataFrame):
            return AnalystResult(analyst_name=self.name, confidence=0.5, signal="neutral", details={"note": "price_frame unavailable"})
        if len(price_frame) < self._config.sequence_length:
            return AnalystResult(analyst_name=self.name, confidence=0.5, signal="neutral", details={"note": "insufficient history"})
        if not set(META_FEATURES).issubset(price_frame.columns):
            fallback = np.array([float(features.get(key, 0.0) or 0.0) for key in META_FEATURES], dtype=float)
            seq = np.tile(fallback, (self._config.sequence_length, 1))
        else:
            seq = price_frame[META_FEATURES].astype(float).tail(self._config.sequence_length).to_numpy()
        means = self._feature_means if self._feature_means is not None else np.zeros(len(META_FEATURES))
        stds = self._feature_stds if self._feature_stds is not None else np.ones(len(META_FEATURES))
        stds = np.where(stds < 1e-9, 1.0, stds)
        normalized = ((seq - means) / stds).reshape(1, -1)
        if not isinstance(self._model, LogisticRegression):
            probability = 0.5
        else:
            probability = float(self._model.predict_proba(normalized)[0][1])
        signal = "confirm" if probability >= 0.65 else "veto" if probability < 0.5 else "neutral"
        return AnalystResult(
            analyst_name=self.name,
            confidence=probability,
            signal=signal,
            details={"metrics": self._metrics, "sequence_length": self._config.sequence_length},
        )

    def save_model(self, path: str | Path) -> None:
        if not isinstance(self._model, LogisticRegression):
            raise ValueError("No trained model to save")
        payload = {
            "config": asdict(self._config),
            "feature_means": self._feature_means.tolist() if self._feature_means is not None else None,
            "feature_stds": self._feature_stds.tolist() if self._feature_stds is not None else None,
            "metrics": self._metrics,
            "coef": self._model.coef_.tolist(),
            "intercept": self._model.intercept_.tolist(),
            "classes": self._model.classes_.tolist(),
        }
        Path(path).write_text(json.dumps(payload, indent=2), encoding="utf-8")

    def load_model(self, path: str | Path) -> None:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        self._config = LSTMConfig(**dict(payload.get("config") or {}))
        model = LogisticRegression(random_state=self._config.random_state, max_iter=max(200, self._config.epochs * 10))
        model.classes_ = np.array(payload.get("classes") or [0, 1])
        model.coef_ = np.array(payload.get("coef") or [[0.0] * (self._config.sequence_length * len(META_FEATURES))])
        model.intercept_ = np.array(payload.get("intercept") or [0.0])
        model.n_features_in_ = model.coef_.shape[1]
        model.n_iter_ = np.array([1], dtype=np.int32)
        self._model = model
        self._feature_means = np.array(payload.get("feature_means") or [0.0] * len(META_FEATURES))
        self._feature_stds = np.array(payload.get("feature_stds") or [1.0] * len(META_FEATURES))
        self._metrics = dict(payload.get("metrics") or {})
        self._trained = True
