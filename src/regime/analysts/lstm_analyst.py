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

TORCH_AVAILABLE = torch is not None

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
    """
    LSTM-based sequence predictor that forecasts short-horizon barrier outcomes
    from sliding windows of META_FEATURES.

    Two backends:
    - PyTorch (primary): trains a real LSTM neural network with BCELoss,
      Adam optimizer, and early stopping. Preserves temporal structure.
    - sklearn fallback: when PyTorch is unavailable, falls back to
      LogisticRegression on flattened sequences.

    Saved models include a backend field so load_model() restores the correct type.
    Pre-hotfix models without a backend field load as sklearn_fallback.
    """

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
        if not TORCH_AVAILABLE and not isinstance(self._model, LogisticRegression):
            return False
        return self._trained and self._model is not None

    def _prepare_frame(self, labeled_frame: Any) -> pd.DataFrame:
        frame = pd.DataFrame(labeled_frame).copy()
        needed = [column for column in META_FEATURES if column in frame.columns]
        if "barrier_outcome" not in frame.columns or len(needed) < len(META_FEATURES):
            raise ValueError("LSTM training frame must include META_FEATURES and barrier_outcome")
        return frame[META_FEATURES + ["barrier_outcome"]].dropna().copy()

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

    def _compute_test_metrics_torch(self, model: Any, x_test: np.ndarray, y_test: np.ndarray) -> dict[str, float]:
        if len(x_test) == 0 or torch is None:
            return {"accuracy": 0.0, "precision": 0.0, "recall": 0.0, "f1": 0.0}
        model.eval()
        with torch.no_grad():
            probs = model(torch.tensor(x_test, dtype=torch.float32)).squeeze(1).numpy()
        preds = (probs >= 0.5).astype(int)
        return {
            "accuracy": float(accuracy_score(y_test, preds)),
            "precision": float(precision_score(y_test, preds, zero_division=0)),
            "recall": float(recall_score(y_test, preds, zero_division=0)),
            "f1": float(f1_score(y_test, preds, zero_division=0)),
        }

    def _train_lstm(
        self,
        x_train: np.ndarray,
        y_train: np.ndarray,
        x_test: np.ndarray,
        y_test: np.ndarray,
        total_sequences: int,
    ) -> dict[str, Any]:
        if torch is None or nn is None:
            raise RuntimeError("PyTorch unavailable")
        from torch.utils.data import DataLoader, TensorDataset  # type: ignore[import-not-found]

        torch.manual_seed(self._config.random_state)
        model = LSTMNetwork(self._config)
        model.train()

        criterion = nn.BCELoss()
        optimizer = torch.optim.Adam(model.parameters(), lr=self._config.learning_rate)  # type: ignore[attr-defined]

        train_x = torch.tensor(x_train, dtype=torch.float32)
        train_y = torch.tensor(y_train, dtype=torch.float32).unsqueeze(1)
        train_ds = TensorDataset(train_x, train_y)
        train_loader = DataLoader(train_ds, batch_size=self._config.batch_size, shuffle=True)

        test_x = torch.tensor(x_test, dtype=torch.float32) if len(x_test) > 0 else None
        test_y = torch.tensor(y_test, dtype=torch.float32).unsqueeze(1) if len(y_test) > 0 else None

        best_val_loss = float("inf")
        best_state_dict = None
        patience = 10
        patience_counter = 0
        epochs_run = 0

        for epoch in range(self._config.epochs):
            model.train()
            for batch_x, batch_y in train_loader:
                optimizer.zero_grad()
                output = model(batch_x)
                loss = criterion(output, batch_y)
                loss.backward()
                optimizer.step()
            epochs_run = epoch + 1
            if test_x is not None and test_y is not None and len(test_x) > 0:
                model.eval()
                with torch.no_grad():
                    val_output = model(test_x)
                    val_loss = criterion(val_output, test_y).item()
                if val_loss < best_val_loss:
                    best_val_loss = float(val_loss)
                    best_state_dict = {key: value.clone() for key, value in model.state_dict().items()}
                    patience_counter = 0
                else:
                    patience_counter += 1
                    if patience_counter >= patience:
                        logger.info("LSTM early stopping at epoch %d (patience=%d)", epochs_run, patience)
                        break

        if best_state_dict is not None:
            model.load_state_dict(best_state_dict)
        model.eval()
        self._model = model
        self._trained = True
        metrics = self._compute_test_metrics_torch(model, x_test, y_test)
        self._metrics = metrics
        return {
            "status": "trained",
            "analyst": self.name,
            "backend": "pytorch",
            "train_samples": int(len(x_train)),
            "test_samples": int(len(x_test)),
            "total_sequences": int(total_sequences),
            "epochs_run": epochs_run,
            "metrics": metrics,
            "best_val_loss": float(best_val_loss) if best_val_loss < float("inf") else None,
        }

    def _train_fallback(
        self,
        x_train: np.ndarray,
        y_train: np.ndarray,
        x_test: np.ndarray,
        y_test: np.ndarray,
        total_sequences: int,
    ) -> dict[str, Any]:
        logger.warning("PyTorch not available — training LSTM fallback with LogisticRegression")
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
            "backend": "sklearn_fallback",
            "train_samples": int(len(x_train)),
            "test_samples": int(len(x_test_flat)),
            "total_sequences": int(total_sequences),
            "epochs_run": int(self._config.epochs),
            "metrics": metrics,
            "best_val_loss": float(1.0 - metrics["accuracy"]),
        }

    def train(self, labeled_frame: Any, **kwargs: Any) -> dict[str, Any]:
        """
        Train LSTM on barrier-labeled data using walk-forward validation.

        If PyTorch is not available, falls back to LogisticRegression on
        flattened sequences.
        """
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

        if torch is None:
            return self._train_fallback(x_train, y_train, x_test, y_test, len(sequences))
        return self._train_lstm(x_train, y_train, x_test, y_test, len(sequences))

    def analyze(self, ticker: str, features: dict[str, float], regime_result: Any) -> AnalystResult:
        del ticker
        if not self.is_ready():
            return AnalystResult(
                analyst_name=self.name,
                confidence=0.5,
                signal="neutral",
                details={"note": "LSTM analyst not trained"},
            )
        price_frame = getattr(regime_result, "price_frame", None)
        if price_frame is None or not isinstance(price_frame, pd.DataFrame):
            return AnalystResult(
                analyst_name=self.name,
                confidence=0.5,
                signal="neutral",
                details={"note": "price_frame unavailable"},
            )
        if len(price_frame) < self._config.sequence_length:
            return AnalystResult(
                analyst_name=self.name,
                confidence=0.5,
                signal="neutral",
                details={"note": "insufficient history"},
            )

        if not set(META_FEATURES).issubset(price_frame.columns):
            fallback = np.array([float(features.get(key, 0.0) or 0.0) for key in META_FEATURES], dtype=float)
            seq = np.tile(fallback, (self._config.sequence_length, 1))
        else:
            seq = price_frame[META_FEATURES].astype(float).tail(self._config.sequence_length).to_numpy()

        means = self._feature_means if self._feature_means is not None else np.zeros(len(META_FEATURES))
        stds = self._feature_stds if self._feature_stds is not None else np.ones(len(META_FEATURES))
        stds = np.where(stds < 1e-9, 1.0, stds)
        normalized = (seq - means) / stds

        if torch is not None and isinstance(self._model, LSTMNetwork):
            self._model.eval()
            with torch.no_grad():
                tensor_input = torch.tensor(normalized, dtype=torch.float32).unsqueeze(0)
                probability = float(self._model(tensor_input).squeeze().item())
            backend = "pytorch"
        elif isinstance(self._model, LogisticRegression):
            flat = normalized.reshape(1, -1)
            probability = float(self._model.predict_proba(flat)[0][1])
            backend = "sklearn_fallback"
        else:
            probability = 0.5
            backend = "unknown"

        signal = "confirm" if probability >= 0.65 else "veto" if probability < 0.5 else "neutral"
        return AnalystResult(
            analyst_name=self.name,
            confidence=probability,
            signal=signal,
            details={
                "metrics": self._metrics,
                "sequence_length": self._config.sequence_length,
                "backend": backend,
            },
        )

    def save_model(self, path: str | Path) -> None:
        if self._model is None:
            raise ValueError("No trained model to save")
        payload: dict[str, Any] = {
            "config": asdict(self._config),
            "feature_means": self._feature_means.tolist() if self._feature_means is not None else None,
            "feature_stds": self._feature_stds.tolist() if self._feature_stds is not None else None,
            "metrics": self._metrics,
        }
        if torch is not None and isinstance(self._model, LSTMNetwork):
            payload["backend"] = "pytorch"
            payload["state_dict"] = {key: value.tolist() for key, value in self._model.state_dict().items()}
        elif isinstance(self._model, LogisticRegression):
            payload["backend"] = "sklearn_fallback"
            payload["coef"] = self._model.coef_.tolist()
            payload["intercept"] = self._model.intercept_.tolist()
            payload["classes"] = self._model.classes_.tolist()
        else:
            raise ValueError(f"Unknown model type: {type(self._model)}")
        Path(path).write_text(json.dumps(payload, indent=2), encoding="utf-8")

    def load_model(self, path: str | Path) -> None:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        self._config = LSTMConfig(**dict(payload.get("config") or {}))
        backend = payload.get("backend", "sklearn_fallback")

        if backend == "pytorch" and torch is not None and payload.get("state_dict"):
            model = LSTMNetwork(self._config)
            state_dict = {key: torch.tensor(value) for key, value in payload["state_dict"].items()}
            model.load_state_dict(state_dict)
            model.eval()
            self._model = model
        else:
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
