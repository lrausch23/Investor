from __future__ import annotations

import glob
import logging
import os
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import Any

import pandas as pd
import xgboost as xgb
from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score

from .ensemble import AnalystBase, AnalystResult, get_registry

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

_COL_MAP = {
    "canonical_state": "hmm_state",
    "return": "log_ret",
    "volume_zscore": "vol_z",
    "vix": "vix_level",
    "yield_10y": "yield_10y_level",
}


@dataclass(frozen=True)
class MetaLabelerConfig:
    """XGBoost Meta-Labeler hyperparameters and training settings."""

    n_estimators: int = 100
    learning_rate: float = 0.05
    max_depth: int = 4
    subsample: float = 0.8
    colsample_bytree: float = 0.8
    random_state: int = 42
    min_training_samples: int = 100
    walk_forward_gap: int = 5


DEFAULT_META_LABELER_CONFIG = MetaLabelerConfig()


def _default_models_dir() -> str:
    configured = os.getenv("HMM_DATA_DIR")
    if configured:
        return os.path.join(os.path.abspath(configured), "models")
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "data", "regime", "models"))


_MODELS_DIR = _default_models_dir()


def _models_dir() -> str:
    d = os.path.abspath(_MODELS_DIR)
    os.makedirs(d, exist_ok=True)
    return d


def _version_path(version: int) -> str:
    return os.path.join(_models_dir(), f"meta_labeler_v{int(version)}.json")


def get_next_version() -> int:
    existing = glob.glob(os.path.join(_models_dir(), "meta_labeler_v*.json"))
    if not existing:
        return 1
    versions: list[int] = []
    for path in existing:
        base = os.path.basename(path)
        try:
            versions.append(int(base.replace("meta_labeler_v", "").replace(".json", "")))
        except ValueError:
            continue
    return (max(versions) + 1) if versions else 1


def list_saved_versions() -> list[dict[str, Any]]:
    existing = sorted(glob.glob(os.path.join(_models_dir(), "meta_labeler_v*.json")))
    results: list[dict[str, Any]] = []
    for path in existing:
        base = os.path.basename(path)
        try:
            version = int(base.replace("meta_labeler_v", "").replace(".json", ""))
        except ValueError:
            continue
        stat = os.stat(path)
        results.append(
            {
                "version": version,
                "path": path,
                "filename": base,
                "size_bytes": stat.st_size,
                "modified_at": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
            }
        )
    return results


def auto_load_active_model(engine: "MetaLabelerEngine", active_version: int | str | None) -> dict[str, Any]:
    if active_version is None:
        return {"status": "no_active_version", "loaded": False}
    try:
        version = int(active_version)
    except (ValueError, TypeError):
        return {"status": "invalid_version", "loaded": False, "version": str(active_version)}
    path = _version_path(version)
    if not os.path.isfile(path):
        return {"status": "file_not_found", "loaded": False, "version": version, "path": path}
    try:
        result = engine.load_model(path)
        return {"status": "loaded", "loaded": True, "version": version, **result}
    except Exception as exc:
        logger.warning("Failed to auto-load meta-labeler v%d: %s", version, exc)
        return {"status": "load_error", "loaded": False, "version": version, "error": str(exc)}


def should_retrain(last_trained_at: str | None, retrain_day: str = "Sunday") -> bool:
    if not last_trained_at:
        return True
    now = datetime.now(timezone.utc)
    if now.strftime("%A") != str(retrain_day or "Sunday"):
        return False
    try:
        last = datetime.fromisoformat(last_trained_at)
        days_since = (now - last).total_seconds() / 86400.0
        return days_since >= 5.0
    except (ValueError, TypeError):
        return True


def extract_meta_features(price_frame_row: pd.Series) -> dict[str, float]:
    """
    Convert a single row from RegimeResult.price_frame into the features dict
    expected by MetaLabelerEngine.analyze().
    """

    mapped: dict[str, float] = {}
    for src_col, feat_name in _COL_MAP.items():
        try:
            mapped[feat_name] = float(price_frame_row.get(src_col, 0.0) or 0.0)
        except Exception:
            mapped[feat_name] = 0.0
    for col in ("volatility", "vix_change", "yield_10y_change"):
        try:
            mapped[col] = float(price_frame_row.get(col, 0.0) or 0.0)
        except Exception:
            mapped[col] = 0.0
    for key in META_FEATURES:
        mapped.setdefault(key, 0.0)
    return mapped


class MetaLabelerEngine(AnalystBase):
    """
    XGBoost Meta-Labeler — predicts probability that the current HMM signal will succeed.
    """

    def __init__(self, config: MetaLabelerConfig = DEFAULT_META_LABELER_CONFIG):
        self._config = config
        self._model: xgb.XGBClassifier | None = None
        self._trained = False
        self._training_metrics: dict[str, Any] = {}
        self._feature_importances: dict[str, float] = {}

    @property
    def name(self) -> str:
        return "xgboost_meta_labeler"

    def is_ready(self) -> bool:
        return self._trained and self._model is not None

    def _prepare_training_frame(self, labeled_frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
        frame = labeled_frame.copy()
        frame = frame.rename(columns=_COL_MAP)
        for col in META_FEATURES:
            if col not in frame.columns:
                frame[col] = 0.0
        frame = frame.dropna(subset=["barrier_outcome"]).copy()
        X = frame[META_FEATURES].copy()
        X["hmm_state"] = X["hmm_state"].fillna(0).astype(int).astype("category")
        for col in [name for name in META_FEATURES if name != "hmm_state"]:
            X[col] = pd.to_numeric(X[col], errors="coerce").fillna(0.0).astype(float)
        y = pd.to_numeric(frame["barrier_outcome"], errors="coerce").fillna(0.0).astype(int)
        return X, y

    def train(self, labeled_frame: pd.DataFrame, **kwargs: Any) -> dict[str, Any]:
        del kwargs
        X, y = self._prepare_training_frame(labeled_frame)
        if len(X) < self._config.min_training_samples:
            logger.warning(
                "Meta-labeler training skipped: %d samples below minimum %d",
                len(X),
                self._config.min_training_samples,
            )
            self._trained = False
            self._model = None
            self._training_metrics = {
                "status": "insufficient_data",
                "samples": int(len(X)),
                "min_training_samples": int(self._config.min_training_samples),
            }
            self._feature_importances = {}
            return self._training_metrics

        split_idx = int(len(X) * 0.8)
        gap = min(self._config.walk_forward_gap, max(0, split_idx - 1))
        train_end = max(1, split_idx - gap)
        X_train = X.iloc[:train_end]
        y_train = y.iloc[:train_end]
        X_test = X.iloc[split_idx:]
        y_test = y.iloc[split_idx:]

        self._model = xgb.XGBClassifier(
            n_estimators=self._config.n_estimators,
            learning_rate=self._config.learning_rate,
            max_depth=self._config.max_depth,
            subsample=self._config.subsample,
            colsample_bytree=self._config.colsample_bytree,
            objective="binary:logistic",
            random_state=self._config.random_state,
            enable_categorical=True,
            eval_metric="logloss",
        )
        self._model.fit(X_train, y_train)

        y_pred = self._model.predict(X_test)
        y_prob = self._model.predict_proba(X_test)[:, 1]
        self._training_metrics = {
            "accuracy": float(accuracy_score(y_test, y_pred)),
            "precision": float(precision_score(y_test, y_pred, zero_division=0.0)),
            "recall": float(recall_score(y_test, y_pred, zero_division=0.0)),
            "f1": float(f1_score(y_test, y_pred, zero_division=0.0)),
            "train_samples": int(len(X_train)),
            "test_samples": int(len(X_test)),
            "positive_rate_train": float(y_train.mean()),
            "positive_rate_test": float(y_test.mean()),
            "avg_probability_test": float(y_prob.mean()) if len(y_prob) else 0.0,
        }
        self._feature_importances = dict(zip(META_FEATURES, map(float, self._model.feature_importances_)))
        self._trained = True
        return {
            **self._training_metrics,
            "feature_importances": self._feature_importances,
        }

    def save_model(self, path: str) -> dict[str, Any]:
        if not self.is_ready():
            raise RuntimeError("Cannot save untrained model.")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self._model.get_booster().save_model(path)
        return {
            "path": path,
            "format": "json",
            "training_metrics": self._training_metrics.copy(),
            "feature_importances": self._feature_importances.copy(),
        }

    def load_model(self, path: str) -> dict[str, Any]:
        if not os.path.isfile(path):
            raise FileNotFoundError(f"Model file not found: {path}")
        loaded = xgb.XGBClassifier()
        loaded.load_model(path)
        self._model = loaded
        self._trained = True
        self._training_metrics = {}
        try:
            self._feature_importances = dict(zip(META_FEATURES, map(float, self._model.feature_importances_)))
        except Exception:
            self._feature_importances = {}
        return {
            "path": path,
            "format": "json",
            "feature_importances": self._feature_importances.copy(),
        }

    def analyze(
        self,
        ticker: str,
        features: dict[str, float],
        regime_result: Any,
    ) -> AnalystResult:
        del regime_result
        if not self.is_ready():
            return AnalystResult(
                analyst_name=self.name,
                confidence=1.0,
                signal="neutral",
                details={"status": "not_trained", "note": "Meta-labeler not yet trained; passing through."},
            )

        feature_values = [float(features.get(feature, 0.0) or 0.0) for feature in META_FEATURES]
        X = pd.DataFrame([feature_values], columns=META_FEATURES)
        X["hmm_state"] = X["hmm_state"].astype(int).astype("category")
        prob_success = float(self._model.predict_proba(X)[0][1])
        if prob_success < 0.50:
            signal = "veto"
        elif prob_success >= 0.65:
            signal = "confirm"
        else:
            signal = "neutral"
        return AnalystResult(
            analyst_name=self.name,
            confidence=prob_success,
            signal=signal,
            details={
                "probability_of_success": round(prob_success, 4),
                "feature_importances": self._feature_importances,
                "training_metrics": self._training_metrics,
                "ticker": ticker,
            },
        )


def create_and_register(
    config: MetaLabelerConfig = DEFAULT_META_LABELER_CONFIG,
) -> MetaLabelerEngine:
    """Create a MetaLabelerEngine and register it with the global analyst registry."""

    engine = MetaLabelerEngine(config)
    get_registry().register(engine)
    return engine
