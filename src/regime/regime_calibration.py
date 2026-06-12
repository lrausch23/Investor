from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pandas as pd

from .probability_calibration import ProbabilityCalibrator, load_calibrator, save_calibrator

REGIME_LABELS = ("Bull", "Neutral", "Bear")
DEFAULT_REGIME_CALIBRATION_HORIZON_DAYS = 5


def default_models_dir() -> Path:
    configured = os.getenv("HMM_DATA_DIR")
    if configured:
        return Path(configured) / "models"
    return Path(__file__).resolve().parents[2] / "data" / "regime" / "models"


def regime_calibrator_path(label: str, *, models_dir: str | Path | None = None) -> Path:
    normalized = str(label or "").strip().lower()
    if normalized not in {"bull", "neutral", "bear"}:
        raise ValueError(f"Unsupported regime label: {label!r}")
    base = Path(models_dir) if models_dir is not None else default_models_dir()
    return base / f"regime_calibrator_{normalized}.json"


def build_persistence_calibration_samples(
    price_frame: pd.DataFrame,
    *,
    horizon_days: int = DEFAULT_REGIME_CALIBRATION_HORIZON_DAYS,
) -> dict[str, dict[str, list[float]]]:
    """Pair active-state posterior with whether that label persisted N bars."""
    if price_frame is None or price_frame.empty:
        return {label: {"probabilities": [], "outcomes": []} for label in REGIME_LABELS}
    frame = price_frame.copy()
    samples: dict[str, dict[str, list[float]]] = {label: {"probabilities": [], "outcomes": []} for label in REGIME_LABELS}
    horizon = max(1, int(horizon_days or 1))
    if "regime" not in frame.columns or "state_probability" not in frame.columns:
        return samples
    for index in range(0, max(0, len(frame) - horizon)):
        label = str(frame["regime"].iloc[index])
        if label not in samples:
            continue
        try:
            probability = float(frame["state_probability"].iloc[index])
        except Exception:
            continue
        if not (probability == probability and 0.0 <= probability <= 1.0):
            continue
        future_label = str(frame["regime"].iloc[index + horizon])
        samples[label]["probabilities"].append(probability)
        samples[label]["outcomes"].append(1.0 if future_label == label else 0.0)
    return samples


def fit_regime_calibrators(
    price_frame: pd.DataFrame,
    *,
    horizon_days: int = DEFAULT_REGIME_CALIBRATION_HORIZON_DAYS,
    models_dir: str | Path | None = None,
) -> dict[str, dict[str, Any]]:
    samples = build_persistence_calibration_samples(price_frame, horizon_days=horizon_days)
    results: dict[str, dict[str, Any]] = {}
    for label, payload in samples.items():
        probabilities = payload["probabilities"]
        outcomes = payload["outcomes"]
        try:
            calibrator = ProbabilityCalibrator.fit(probabilities, outcomes)
        except ValueError as exc:
            results[label] = {
                "saved": False,
                "reason": str(exc),
                "sample_count": len(probabilities),
                "positive_rate": _positive_rate(outcomes),
            }
            continue
        path = regime_calibrator_path(label, models_dir=models_dir)
        save_result = save_calibrator(calibrator, path)
        metadata = {
            "label": label,
            "horizon_days": int(horizon_days),
            "sample_count": len(probabilities),
            "positive_rate": _positive_rate(outcomes),
            **save_result,
        }
        path.write_text(json.dumps({**json.loads(path.read_text(encoding="utf-8")), **metadata}, indent=2) + "\n", encoding="utf-8")
        results[label] = {"saved": True, **metadata}
    return results


def load_regime_calibrator(label: str, *, models_dir: str | Path | None = None) -> ProbabilityCalibrator | None:
    path = regime_calibrator_path(label, models_dir=models_dir)
    if not path.exists():
        return None
    return load_calibrator(path)


def calibrate_regime_probability(
    label: str,
    probability: float,
    *,
    models_dir: str | Path | None = None,
) -> float:
    calibrator = load_regime_calibrator(label, models_dir=models_dir)
    if calibrator is None:
        return float(probability)
    return float(calibrator.calibrate([float(probability)])[0])


def _positive_rate(outcomes: list[float]) -> float | None:
    if not outcomes:
        return None
    return float(sum(float(value) for value in outcomes) / len(outcomes))
