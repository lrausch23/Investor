from __future__ import annotations

import glob
import json
import logging
import os
from datetime import datetime, timezone
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.metrics import accuracy_score, brier_score_loss, f1_score, precision_score, recall_score, roc_auc_score

from .ensemble import AnalystBase, AnalystResult, get_registry
from .exceptions import DataValidationError
from .probability_calibration import ProbabilityCalibrator, fit_calibrator, load_calibrator, save_calibrator
from .triple_barrier import sample_uniqueness_weights

logger = logging.getLogger(__name__)

DEFAULT_META_LABELER_VETO_MODE = "gate"
META_LABELER_VETO_MODES = {"gate", "size_only"}

LEGACY_META_FEATURES = [
    "hmm_state",
    "log_ret",
    "volatility",
    "vol_z",
    "vix_level",
    "vix_change",
    "yield_10y_level",
    "yield_10y_change",
]

META_FEATURES = [
    *LEGACY_META_FEATURES,
    "composite_strength",
    "transition_risk",
    "regime_days",
    "p_bull_day5",
    "p_bear_day5",
    "risk_reward_ratio",
    "stop_distance_atr",
    "target_distance_atr",
    "rsi_bucket",
    "macd_hist_sign",
    "signal_quality_score",
]

LEGACY_FEATURE_SET_VERSION = "meta_features_v1_environment"
FEATURE_SET_VERSION = "meta_features_v2_signal_context"
DEFAULT_META_LABELER_MIN_OOF_AUC = 0.55
META_LABELER_PASSTHROUGH_STATUSES = {
    "not_trained",
    "degraded_features",
    "feature_schema_mismatch",
    "insufficient_model_skill",
}

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
    n_folds: int = 5
    embargo_bars: int = 21
    embargo_days: int = 30
    veto_margin: float = 0.10
    confirm_margin: float = 0.15


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


def _calibrator_path(model_path: str | os.PathLike[str]) -> str:
    path = Path(model_path)
    return str(path.with_name(f"{path.stem}_calibrator{path.suffix}"))


def _metadata_path(model_path: str | os.PathLike[str]) -> str:
    path = Path(model_path)
    return str(path.with_name(f"{path.stem}_metadata{path.suffix}"))


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return float(value)
    if isinstance(value, (np.bool_,)):
        return bool(value)
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()
    return value


def _finite_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        parsed = float(value)
    except Exception:
        return None
    return parsed if np.isfinite(parsed) else None


def _clip_probability(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def _coerce_bool(value: Any, default: bool) -> bool:
    if value in (None, ""):
        return default
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return default


def _read_setting(key: str) -> Any:
    try:
        from .persistence import get_setting

        return get_setting(key)
    except Exception:
        return None


def meta_labeler_skill_gate_enabled(get_setting_fn: Any | None = None) -> bool:
    raw = None
    if callable(get_setting_fn):
        try:
            raw = get_setting_fn("meta_labeler_skill_gate_enabled")
        except Exception:
            raw = None
    if raw is None:
        raw = _read_setting("meta_labeler_skill_gate_enabled")
    return _coerce_bool(raw, True)


def meta_labeler_min_oof_auc(get_setting_fn: Any | None = None) -> float:
    raw = None
    if callable(get_setting_fn):
        try:
            raw = get_setting_fn("meta_labeler_min_oof_auc")
        except Exception:
            raw = None
    if raw is None:
        raw = _read_setting("meta_labeler_min_oof_auc")
    parsed = _finite_float(raw)
    return parsed if parsed is not None else DEFAULT_META_LABELER_MIN_OOF_AUC


def normalize_meta_labeler_veto_mode(value: Any) -> str:
    mode = str(value or DEFAULT_META_LABELER_VETO_MODE).strip().lower()
    return mode if mode in META_LABELER_VETO_MODES else DEFAULT_META_LABELER_VETO_MODE


def meta_labeler_gate_enabled(get_setting_fn: Any | None = None) -> bool:
    raw = None
    if callable(get_setting_fn):
        try:
            raw = get_setting_fn("meta_labeler_veto_mode")
        except Exception:
            raw = None
    return normalize_meta_labeler_veto_mode(raw) == "gate"


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


def _row_get(row: pd.Series | dict[str, Any], key: str, default: Any = None) -> Any:
    try:
        if isinstance(row, pd.Series):
            return row.get(key, default)
        return row.get(key, default)
    except Exception:
        return default


def _nested_get(row: pd.Series | dict[str, Any], key: str, nested_key: str, default: Any = None) -> Any:
    value = _row_get(row, key)
    if isinstance(value, dict):
        return value.get(nested_key, default)
    return default


def _numeric_feature(row: pd.Series | dict[str, Any], *keys: str, default: float = 0.0) -> float:
    for key in keys:
        value = _row_get(row, key)
        parsed = _finite_float(value)
        if parsed is not None:
            return parsed
    return float(default)


def _state_feature(value: Any) -> float:
    if isinstance(value, str):
        mapped = {"bull": 0, "neutral": 1, "bear": 2}.get(value.strip().lower())
        if mapped is not None:
            return float(mapped)
    parsed = _finite_float(value)
    if parsed is None:
        return 0.0
    return float(int(max(0, min(2, round(parsed)))))


def _rsi_bucket(value: Any) -> float:
    parsed = _finite_float(value)
    if parsed is None:
        return 1.0
    if parsed < 30.0:
        return 0.0
    if parsed > 70.0:
        return 2.0
    return 1.0


def _sign_feature(value: Any) -> float:
    parsed = _finite_float(value)
    if parsed is None:
        return 0.0
    if parsed > 0:
        return 1.0
    if parsed < 0:
        return -1.0
    return 0.0


def _price_target_value(row: pd.Series | dict[str, Any], *keys: str) -> float | None:
    for key in keys:
        parsed = _finite_float(_row_get(row, key))
        if parsed is not None:
            return parsed
        parsed = _finite_float(_nested_get(row, "price_targets", key))
        if parsed is not None:
            return parsed
    return None


def _risk_reward_from_prices(entry: float | None, target: float | None, stop: float | None) -> float:
    if entry is None or target is None or stop is None:
        return 0.0
    risk = abs(float(entry) - float(stop))
    reward = abs(float(target) - float(entry))
    return float(reward / risk) if risk > 0 else 0.0


def _distance_atr(price_a: float | None, price_b: float | None, atr: float | None) -> float:
    if price_a is None or price_b is None or atr is None or atr <= 0:
        return 0.0
    return float(abs(float(price_a) - float(price_b)) / float(atr))


def extract_meta_features(price_frame_row: pd.Series | dict[str, Any], feature_names: list[str] | tuple[str, ...] | None = None) -> dict[str, float]:
    """
    Convert a single row from RegimeResult.price_frame into the features dict
    expected by MetaLabelerEngine.analyze().
    """

    requested_features = list(feature_names or META_FEATURES)
    mapped: dict[str, float] = {}
    for src_col, feat_name in _COL_MAP.items():
        if feat_name == "hmm_state":
            mapped[feat_name] = _state_feature(
                _row_get(price_frame_row, feat_name, _row_get(price_frame_row, src_col, _row_get(price_frame_row, "regime")))
            )
        else:
            mapped[feat_name] = _numeric_feature(price_frame_row, feat_name, src_col)
    for col in ("volatility", "vix_change", "yield_10y_change"):
        mapped[col] = _numeric_feature(price_frame_row, col)

    current_price = _price_target_value(price_frame_row, "current_price", "price", "Close", "close")
    entry_price = _price_target_value(price_frame_row, "entry_price", "barrier_entry") or current_price
    target_price = _price_target_value(price_frame_row, "target_price", "exit_price", "barrier_target")
    stop_price = _price_target_value(price_frame_row, "stop_price", "barrier_stop")
    atr_value = _price_target_value(price_frame_row, "atr_14", "atr_value")
    risk_reward = _price_target_value(price_frame_row, "risk_reward_ratio")
    mapped.update(
        {
            "composite_strength": _numeric_feature(price_frame_row, "composite_strength"),
            "transition_risk": _numeric_feature(price_frame_row, "transition_risk"),
            "regime_days": _numeric_feature(price_frame_row, "regime_days", default=1.0),
            "p_bull_day5": _numeric_feature(price_frame_row, "p_bull_day5"),
            "p_bear_day5": _numeric_feature(price_frame_row, "p_bear_day5"),
            "risk_reward_ratio": risk_reward if risk_reward is not None else _risk_reward_from_prices(entry_price, target_price, stop_price),
            "stop_distance_atr": _distance_atr(entry_price, stop_price, atr_value),
            "target_distance_atr": _distance_atr(target_price, entry_price, atr_value),
            "rsi_bucket": _numeric_feature(price_frame_row, "rsi_bucket", default=_rsi_bucket(_row_get(price_frame_row, "rsi_14"))),
            "macd_hist_sign": _numeric_feature(
                price_frame_row,
                "macd_hist_sign",
                default=_sign_feature(_row_get(price_frame_row, "macd_histogram")),
            ),
            "signal_quality_score": _numeric_feature(price_frame_row, "signal_quality_score"),
        }
    )
    if mapped.get("composite_strength", 0.0) == 0.0 and {"p_bull_day5", "p_bear_day5"}.issubset(mapped):
        state_probability = _numeric_feature(price_frame_row, "state_probability", "probability", default=1.0)
        mapped["composite_strength"] = max(0.0, min(1.0, abs(mapped["p_bull_day5"] - mapped["p_bear_day5"]) * state_probability))
    for key in requested_features:
        mapped.setdefault(key, 0.0)
    return {key: float(mapped.get(key, 0.0) or 0.0) for key in requested_features}


def meta_labeler_result_can_influence(result: Any) -> bool:
    details = getattr(result, "details", {}) or {}
    status = str(details.get("status") or "")
    return status not in META_LABELER_PASSTHROUGH_STATUSES


class MetaLabelerEngine(AnalystBase):
    """
    XGBoost Meta-Labeler — predicts probability that the current HMM signal will succeed.
    """

    def __init__(self, config: MetaLabelerConfig = DEFAULT_META_LABELER_CONFIG):
        self._config = config
        self._model: xgb.XGBClassifier | None = None
        self._calibrator: ProbabilityCalibrator | None = None
        self._trained = False
        self._training_metrics: dict[str, Any] = {}
        self._feature_importances: dict[str, float] = {}
        self._feature_names: list[str] = list(META_FEATURES)
        self._feature_set_version: str = FEATURE_SET_VERSION

    @property
    def name(self) -> str:
        return "xgboost_meta_labeler"

    def is_ready(self) -> bool:
        return self._trained and self._model is not None

    def _prepare_training_frame(self, labeled_frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
        X, y, _frame, _weights = self._prepare_training_data(labeled_frame)
        return X, y

    def _prepare_training_data(self, labeled_frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series, pd.DataFrame, pd.Series]:
        frame = labeled_frame.copy()
        frame = frame.rename(columns=_COL_MAP)
        frame["_label_start_idx"] = np.arange(len(frame), dtype=int)
        has_label_end_idx = "label_end_idx" in frame.columns
        has_date_lifespans = {"label_entry_date", "label_end_date"}.issubset(frame.columns)
        if has_date_lifespans:
            frame["label_entry_date"] = pd.to_datetime(frame["label_entry_date"], errors="coerce")
            frame["label_end_date"] = pd.to_datetime(frame["label_end_date"], errors="coerce")
        uniqueness = (
            sample_uniqueness_weights(frame)
            if has_label_end_idx or has_date_lifespans
            else pd.Series(1.0, index=frame.index, dtype=float)
        )
        frame["_sample_weight"] = uniqueness.reindex(frame.index).fillna(1.0).clip(lower=0.0).astype(float)
        feature_names = list(META_FEATURES)
        extracted = pd.DataFrame(
            [extract_meta_features(row, feature_names=feature_names) for _idx, row in frame.iterrows()],
            index=frame.index,
        )
        for col in feature_names:
            frame[col] = extracted[col] if col in extracted.columns else 0.0
        frame = frame.dropna(subset=["barrier_outcome"]).copy()
        if has_date_lifespans:
            frame["_lifespan_fallback"] = (
                frame["label_entry_date"].isna()
                | frame["label_end_date"].isna()
                | (frame["label_end_date"] <= frame["label_entry_date"])
            )
            frame = frame.sort_values("label_entry_date", kind="mergesort").copy()
        else:
            frame["_lifespan_fallback"] = True
        if "label_end_idx" not in frame.columns:
            frame["label_end_idx"] = np.arange(len(frame), dtype=int)
        frame["label_end_idx"] = pd.to_numeric(frame["label_end_idx"], errors="coerce")
        missing_end = frame["label_end_idx"].isna()
        if missing_end.any():
            frame.loc[missing_end, "label_end_idx"] = frame.loc[missing_end, "_label_start_idx"]
        if not has_date_lifespans:
            if has_label_end_idx:
                frame["_lifespan_fallback"] = frame["label_end_idx"] <= frame["_label_start_idx"]
            else:
                frame["_lifespan_fallback"] = True
        if "sample_weight" in frame.columns:
            sample_weight = pd.to_numeric(frame["sample_weight"], errors="coerce").fillna(1.0).clip(lower=0.0)
        else:
            sample_weight = frame["_sample_weight"].fillna(1.0).clip(lower=0.0)
        sample_weight = sample_weight.astype(float)
        X = self._coerce_feature_frame(frame, feature_names)
        y = pd.to_numeric(frame["barrier_outcome"], errors="coerce").fillna(0.0).astype(int)
        return X, y, frame, sample_weight

    def _coerce_feature_frame(self, frame: pd.DataFrame, feature_names: list[str] | tuple[str, ...]) -> pd.DataFrame:
        names = list(feature_names)
        X = frame.reindex(columns=names, fill_value=0.0).copy()
        if "hmm_state" in X.columns:
            X["hmm_state"] = (
                pd.to_numeric(X["hmm_state"], errors="coerce")
                .fillna(0)
                .astype(int)
                .clip(lower=0, upper=2)
                .astype(pd.CategoricalDtype(categories=[0, 1, 2]))
            )
        for col in [name for name in names if name != "hmm_state"]:
            X[col] = pd.to_numeric(X[col], errors="coerce").fillna(0.0).astype(float)
        return X

    def _model_feature_count(self) -> int | None:
        model = self._model
        if model is None:
            return None
        count = getattr(model, "n_features_in_", None)
        if count is not None:
            try:
                return int(count)
            except Exception:
                pass
        try:
            booster = model.get_booster()
            return int(booster.num_features())
        except Exception:
            return None

    def _feature_importances_from_model(self) -> dict[str, float]:
        if self._model is None:
            return {}
        try:
            values = list(map(float, getattr(self._model, "feature_importances_", [])))
        except Exception:
            return {}
        names = self._feature_names
        if len(values) != len(names):
            expected = self._model_feature_count()
            if expected == len(META_FEATURES):
                names = list(META_FEATURES)
            elif expected == len(LEGACY_META_FEATURES):
                names = list(LEGACY_META_FEATURES)
            else:
                return {}
        if len(values) != len(names):
            return {}
        return dict(zip(names, values))

    def _new_model(self) -> xgb.XGBClassifier:
        return xgb.XGBClassifier(
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

    def _purged_walk_forward_splits(
        self,
        frame: pd.DataFrame,
    ) -> list[tuple[np.ndarray, np.ndarray, dict[str, Any]]]:
        n_samples = len(frame)
        if n_samples < 2:
            return []
        n_folds = max(1, int(self._config.n_folds or 1))
        initial_train = max(1, n_samples // (n_folds + 1))
        if initial_train >= n_samples:
            return []
        remaining = np.arange(initial_train, n_samples)
        test_folds = [fold for fold in np.array_split(remaining, n_folds) if len(fold)]
        if {"label_entry_date", "label_end_date"}.issubset(frame.columns):
            entry_dates = pd.to_datetime(frame["label_entry_date"], errors="coerce")
            end_dates = pd.to_datetime(frame["label_end_date"], errors="coerce")
            if entry_dates.notna().all() and end_dates.notna().all():
                embargo = max(0, int(self._config.embargo_days or 0))
                sample_positions = np.arange(n_samples)
                date_splits: list[tuple[np.ndarray, np.ndarray, dict[str, Any]]] = []
                for fold_id, test_idx in enumerate(test_folds, start=1):
                    test_entries = entry_dates.iloc[test_idx]
                    test_start_date = test_entries.min()
                    test_end_date = test_entries.max()
                    train_mask = entry_dates < (test_start_date - pd.Timedelta(days=embargo))
                    train_mask &= end_dates < test_start_date
                    train_idx = sample_positions[train_mask.to_numpy()]
                    if len(train_idx) == 0:
                        continue
                    date_splits.append(
                        (
                            train_idx.astype(int),
                            test_idx.astype(int),
                            {
                                "fold": fold_id,
                                "train_samples": int(len(train_idx)),
                                "test_samples": int(len(test_idx)),
                                "test_start_idx": int(test_idx[0]),
                                "test_end_idx": int(test_idx[-1]),
                                "test_start_date": pd.Timestamp(test_start_date).date().isoformat(),
                                "test_end_date": pd.Timestamp(test_end_date).date().isoformat(),
                                "embargo_days": embargo,
                            },
                        )
                    )
                return date_splits
        default_positions = pd.Series(np.arange(n_samples), index=frame.index, dtype=float)
        label_start = pd.to_numeric(frame["_label_start_idx"], errors="coerce").fillna(default_positions).to_numpy(dtype=float)
        label_end = pd.to_numeric(frame["label_end_idx"], errors="coerce").fillna(frame["_label_start_idx"]).to_numpy(dtype=float)
        embargo = max(0, int(self._config.embargo_bars or 0))
        sample_positions = np.arange(n_samples)
        positional_splits: list[tuple[np.ndarray, np.ndarray, dict[str, Any]]] = []
        for fold_id, test_idx in enumerate(test_folds, start=1):
            test_start_bar = int(label_start[test_idx[0]])
            test_end_bar = int(label_start[test_idx[-1]])
            embargo_cutoff = test_start_bar - embargo
            train_mask = label_start < test_start_bar
            train_mask &= label_start < embargo_cutoff
            train_mask &= label_end < test_start_bar
            train_idx = sample_positions[train_mask]
            if len(train_idx) == 0:
                continue
            positional_splits.append(
                (
                    train_idx.astype(int),
                    test_idx.astype(int),
                    {
                        "fold": fold_id,
                        "train_samples": int(len(train_idx)),
                        "test_samples": int(len(test_idx)),
                        "test_start_idx": int(test_idx[0]),
                        "test_end_idx": int(test_idx[-1]),
                        "test_start_bar": test_start_bar,
                        "test_end_bar": test_end_bar,
                        "embargo_bars": embargo,
                    },
                )
            )
        return positional_splits

    def _classification_metrics(self, y_true: pd.Series | np.ndarray, probabilities: np.ndarray) -> dict[str, float | None]:
        y_array = np.asarray(y_true, dtype=int)
        probs = np.asarray(probabilities, dtype=float)
        y_pred = (probs >= 0.5).astype(int)
        metrics: dict[str, float | None] = {
            "accuracy": float(accuracy_score(y_array, y_pred)),
            "precision": float(precision_score(y_array, y_pred, zero_division=0.0)),
            "recall": float(recall_score(y_array, y_pred, zero_division=0.0)),
            "f1": float(f1_score(y_array, y_pred, zero_division=0.0)),
            "brier_score": float(brier_score_loss(y_array, probs)),
        }
        metrics["roc_auc"] = float(roc_auc_score(y_array, probs)) if len(np.unique(y_array)) > 1 else None
        return metrics

    def train(self, labeled_frame: pd.DataFrame, **kwargs: Any) -> dict[str, Any]:
        label_mode = kwargs.get("label_mode")
        label_config = kwargs.get("label_config")
        X, y, frame, sample_weight = self._prepare_training_data(labeled_frame)
        fallback_ratio = float(frame["_lifespan_fallback"].mean()) if "_lifespan_fallback" in frame.columns and len(frame) else 1.0
        weights_degenerate = fallback_ratio > 0.5
        if weights_degenerate:
            logger.warning(
                "Meta-labeler sample lifespans are degenerate: %.1f%% of %d samples fell back to one-bar lifespans.",
                fallback_ratio * 100.0,
                len(frame),
            )
        attribution: dict[str, Any] = {
            "weights_degenerate": bool(weights_degenerate),
            "lifespan_fallback_ratio": fallback_ratio,
        }
        if label_mode is not None:
            attribution["label_mode"] = str(label_mode)
        if label_config is not None:
            attribution["label_config"] = dict(label_config) if isinstance(label_config, dict) else label_config
        if len(X) < self._config.min_training_samples:
            logger.warning(
                "Meta-labeler training skipped: %d samples below minimum %d",
                len(X),
                self._config.min_training_samples,
            )
            self._trained = False
            self._model = None
            self._calibrator = None
            self._training_metrics = {
                "status": "insufficient_data",
                "samples": int(len(X)),
                "min_training_samples": int(self._config.min_training_samples),
                **attribution,
            }
            self._feature_importances = {}
            return self._training_metrics
        if y.nunique() < 2:
            logger.warning("Meta-labeler training skipped: only one outcome class present.")
            self._trained = False
            self._model = None
            self._calibrator = None
            self._training_metrics = {"status": "single_class", "samples": int(len(X)), "positive_rate": float(y.mean()), **attribution}
            self._feature_importances = {}
            return self._training_metrics

        splits = self._purged_walk_forward_splits(frame)
        oof_prob = np.full(len(X), np.nan, dtype=float)
        fold_metrics: list[dict[str, Any]] = []
        for train_idx, test_idx, fold_meta in splits:
            y_train = y.iloc[train_idx]
            y_test = y.iloc[test_idx]
            if y_train.nunique() < 2 or len(y_test) == 0:
                continue
            fold_model = self._new_model()
            fold_model.fit(X.iloc[train_idx], y_train, sample_weight=sample_weight.iloc[train_idx])
            probabilities = fold_model.predict_proba(X.iloc[test_idx])[:, 1]
            oof_prob[test_idx] = probabilities
            fold_metrics.append(
                {
                    **fold_meta,
                    **self._classification_metrics(y_test, probabilities),
                    "positive_rate_train": float(y_train.mean()),
                    "positive_rate_test": float(y_test.mean()) if len(y_test) else 0.0,
                    "avg_probability_test": float(probabilities.mean()) if len(probabilities) else 0.0,
                }
            )

        oof_mask = np.isfinite(oof_prob)
        aggregate = self._classification_metrics(y.iloc[oof_mask], oof_prob[oof_mask]) if oof_mask.any() else {}
        self._calibrator = None
        calibration_metrics: dict[str, Any] = {"calibrated": False}
        if oof_mask.any() and y.iloc[oof_mask].nunique() > 1:
            try:
                calibrator = fit_calibrator(oof_prob[oof_mask], y.iloc[oof_mask])
                calibrated_oof = calibrator.calibrate(oof_prob[oof_mask])
                raw_brier = float(brier_score_loss(y.iloc[oof_mask], oof_prob[oof_mask]))
                calibrated_brier = float(brier_score_loss(y.iloc[oof_mask], calibrated_oof))
                if calibrated_brier > raw_brier:
                    logger.warning(
                        "Meta-labeler isotonic calibration worsened Brier score: raw=%.6f calibrated=%.6f",
                        raw_brier,
                        calibrated_brier,
                    )
                self._calibrator = calibrator
                calibration_metrics = {
                    "calibrated": True,
                    "brier_score_raw": raw_brier,
                    "brier_score_calibrated": calibrated_brier,
                    "brier_score_delta": calibrated_brier - raw_brier,
                }
            except Exception as exc:
                logger.warning("Meta-labeler calibration skipped: %s", exc)
                calibration_metrics = {"calibrated": False, "calibration_error": str(exc)}

        self._model = self._new_model()
        self._model.fit(X, y, sample_weight=sample_weight)
        self._feature_names = list(X.columns)
        self._feature_set_version = FEATURE_SET_VERSION
        self._training_metrics = {
            "status": "trained",
            **aggregate,
            "folds": fold_metrics,
            "cv_folds": int(len(fold_metrics)),
            "train_samples": int(len(X)),
            "test_samples": int(oof_mask.sum()),
            "positive_rate_train": float(y.mean()),
            "positive_rate_test": float(y.iloc[oof_mask].mean()) if oof_mask.any() else None,
            "avg_probability_test": float(oof_prob[oof_mask].mean()) if oof_mask.any() else 0.0,
            "sample_weight_mean": float(sample_weight.mean()) if len(sample_weight) else 1.0,
            **attribution,
            **calibration_metrics,
            "feature_set_version": self._feature_set_version,
            "feature_names": list(self._feature_names),
        }
        self._feature_importances = self._feature_importances_from_model()
        self._trained = True
        return {
            **self._training_metrics,
            "feature_importances": self._feature_importances,
        }

    def save_model(self, path: str) -> dict[str, Any]:
        if not self.is_ready():
            raise DataValidationError("Cannot save untrained model.")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self._model.get_booster().save_model(path)
        calibrator_path = None
        if self._calibrator is not None:
            calibrator_path = _calibrator_path(path)
            save_calibrator(self._calibrator, calibrator_path)
        metadata_path = _metadata_path(path)
        positive_rate_train = _finite_float(self._training_metrics.get("positive_rate_train"))
        if positive_rate_train is None:
            positive_rate_train = _finite_float(self._training_metrics.get("positive_rate"))
        metadata = {
            "format": "meta_labeler_metadata",
            "positive_rate_train": positive_rate_train,
            "training_metrics": self._training_metrics.copy(),
            "feature_importances": self._feature_importances.copy(),
            "feature_names": list(self._feature_names),
            "feature_set_version": self._feature_set_version,
            "config": asdict(self._config),
        }
        Path(metadata_path).write_text(json.dumps(_json_safe(metadata), indent=2) + "\n", encoding="utf-8")
        return {
            "path": path,
            "format": "json",
            "calibrator_path": calibrator_path,
            "metadata_path": metadata_path,
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
        self._calibrator = None
        calibrator_path = _calibrator_path(path)
        if os.path.isfile(calibrator_path):
            self._calibrator = load_calibrator(calibrator_path)
        self._training_metrics = {}
        self._feature_names = list(LEGACY_META_FEATURES)
        self._feature_set_version = LEGACY_FEATURE_SET_VERSION
        metadata_path = _metadata_path(path)
        metadata: dict[str, Any] = {}
        if os.path.isfile(metadata_path):
            try:
                loaded_metadata = json.loads(Path(metadata_path).read_text(encoding="utf-8"))
                if isinstance(loaded_metadata, dict):
                    metadata = loaded_metadata
                    metrics = loaded_metadata.get("training_metrics")
                    if isinstance(metrics, dict):
                        self._training_metrics = dict(metrics)
                    positive_rate_train = _finite_float(loaded_metadata.get("positive_rate_train"))
                    if positive_rate_train is not None:
                        self._training_metrics.setdefault("positive_rate_train", positive_rate_train)
                    feature_names = loaded_metadata.get("feature_names")
                    if isinstance(feature_names, list) and feature_names:
                        self._feature_names = [str(feature) for feature in feature_names]
                    elif isinstance(metrics, dict) and isinstance(metrics.get("feature_names"), list):
                        self._feature_names = [str(feature) for feature in metrics["feature_names"]]
                    feature_set_version = loaded_metadata.get("feature_set_version") or (
                        metrics.get("feature_set_version") if isinstance(metrics, dict) else None
                    )
                    if feature_set_version:
                        self._feature_set_version = str(feature_set_version)
            except Exception as exc:
                logger.warning("Failed to load meta-labeler metadata %s: %s", metadata_path, exc)
        expected = self._model_feature_count()
        if expected is not None and expected != len(self._feature_names):
            if expected == len(LEGACY_META_FEATURES):
                self._feature_names = list(LEGACY_META_FEATURES)
                self._feature_set_version = LEGACY_FEATURE_SET_VERSION
            elif expected == len(META_FEATURES):
                self._feature_names = list(META_FEATURES)
                self._feature_set_version = FEATURE_SET_VERSION
        self._feature_importances = self._feature_importances_from_model()
        if isinstance(metadata.get("feature_importances"), dict) and not self._feature_importances:
            self._feature_importances = {str(key): float(value) for key, value in metadata["feature_importances"].items()}
        return {
            "path": path,
            "format": "json",
            "calibrator_path": calibrator_path if self._calibrator is not None else None,
            "metadata_path": metadata_path if metadata else None,
            "training_metrics": self._training_metrics.copy(),
            "feature_importances": self._feature_importances.copy(),
            "feature_names": list(self._feature_names),
            "feature_set_version": self._feature_set_version,
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

        feature_names = list(self._feature_names or META_FEATURES)
        oof_roc_auc = _finite_float(self._training_metrics.get("roc_auc"))
        skill_gate = "disabled"
        if meta_labeler_skill_gate_enabled():
            if oof_roc_auc is None:
                skill_gate = "unknown_skill"
            else:
                minimum_auc = float(meta_labeler_min_oof_auc())
                if oof_roc_auc < minimum_auc:
                    return AnalystResult(
                        analyst_name=self.name,
                        confidence=1.0,
                        signal="neutral",
                        details={
                            "status": "insufficient_model_skill",
                            "skill_gate": "blocked",
                            "oof_roc_auc": round(oof_roc_auc, 4),
                            "min_oof_auc": round(minimum_auc, 4),
                            "note": "Meta-labeler OOF ROC-AUC is below the configured skill bar; passing through.",
                            "ticker": ticker,
                            "feature_set_version": self._feature_set_version,
                            "feature_names": feature_names,
                        },
                    )
                skill_gate = "passed"

        expected_feature_count = self._model_feature_count()
        if expected_feature_count is not None and expected_feature_count != len(feature_names):
            return AnalystResult(
                analyst_name=self.name,
                confidence=1.0,
                signal="neutral",
                details={
                    "status": "feature_schema_mismatch",
                    "model_feature_count": expected_feature_count,
                    "feature_count": len(feature_names),
                    "feature_set_version": self._feature_set_version,
                    "note": "Meta-labeler model feature count does not match its loaded schema; passing through.",
                    "ticker": ticker,
                },
            )

        missing_features = [feature for feature in feature_names if feature not in features or features.get(feature) is None]
        if len(feature_names) and (len(missing_features) / len(feature_names)) > 0.25:
            logger.warning(
                "Meta-labeler skipped for %s: degraded feature vector missing %d/%d features.",
                ticker,
                len(missing_features),
                len(feature_names),
            )
            return AnalystResult(
                analyst_name=self.name,
                confidence=1.0,
                signal="neutral",
                details={
                    "status": "degraded_features",
                    "missing_features": missing_features,
                    "note": "Meta-labeler feature vector is too sparse; passing through.",
                    "ticker": ticker,
                    "feature_set_version": self._feature_set_version,
                    "feature_names": feature_names,
                },
            )

        feature_values = extract_meta_features(features, feature_names=feature_names)
        X = self._coerce_feature_frame(pd.DataFrame([feature_values]), feature_names)
        raw_prob_success = float(self._model.predict_proba(X)[0][1])
        calibrated = self._calibrator is not None
        prob_success = (
            float(self._calibrator.calibrate([raw_prob_success])[0])
            if self._calibrator is not None
            else raw_prob_success
        )
        positive_rate_train = _finite_float(self._training_metrics.get("positive_rate_train"))
        if positive_rate_train is None:
            positive_rate_train = _finite_float(self._training_metrics.get("positive_rate"))
        if positive_rate_train is not None:
            threshold_mode = "base_rate_relative"
            veto_threshold = _clip_probability(positive_rate_train - float(self._config.veto_margin))
            confirm_threshold = _clip_probability(positive_rate_train + float(self._config.confirm_margin))
        else:
            threshold_mode = "absolute_fallback"
            veto_threshold = 0.50
            confirm_threshold = 0.65
        if prob_success < veto_threshold:
            signal = "veto"
        elif prob_success >= confirm_threshold:
            signal = "confirm"
        else:
            signal = "neutral"
        return AnalystResult(
            analyst_name=self.name,
            confidence=prob_success,
            signal=signal,
            details={
                "probability_of_success": round(prob_success, 4),
                "raw_probability": round(raw_prob_success, 4),
                "calibrated": calibrated,
                "threshold_mode": threshold_mode,
                "veto_threshold": round(veto_threshold, 4),
                "confirm_threshold": round(confirm_threshold, 4),
                "positive_rate_train": round(positive_rate_train, 4) if positive_rate_train is not None else None,
                "feature_importances": self._feature_importances,
                "training_metrics": self._training_metrics,
                "skill_gate": skill_gate,
                "oof_roc_auc": round(oof_roc_auc, 4) if oof_roc_auc is not None else None,
                "feature_set_version": self._feature_set_version,
                "feature_names": feature_names,
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
