from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import numpy as np
from sklearn.isotonic import IsotonicRegression


@dataclass
class ProbabilityCalibrator:
    """JSON-serializable isotonic probability calibrator."""

    x_thresholds: list[float]
    y_thresholds: list[float]

    @classmethod
    def fit(cls, probabilities: Iterable[float], outcomes: Iterable[float]) -> "ProbabilityCalibrator":
        probs = np.asarray(list(probabilities), dtype=float)
        labels = np.asarray(list(outcomes), dtype=float)
        mask = np.isfinite(probs) & np.isfinite(labels)
        probs = probs[mask]
        labels = labels[mask]
        if len(probs) < 2 or len(np.unique(labels)) < 2:
            raise ValueError("Calibration requires at least two samples and both outcome classes.")
        model = IsotonicRegression(out_of_bounds="clip")
        model.fit(probs, labels)
        return cls(
            x_thresholds=[float(value) for value in model.X_thresholds_],
            y_thresholds=[float(value) for value in model.y_thresholds_],
        )

    def calibrate(self, probabilities: Iterable[float] | np.ndarray) -> np.ndarray:
        probs = np.asarray(probabilities, dtype=float)
        if not self.x_thresholds or not self.y_thresholds:
            return probs.copy()
        calibrated = np.interp(
            probs,
            np.asarray(self.x_thresholds, dtype=float),
            np.asarray(self.y_thresholds, dtype=float),
            left=float(self.y_thresholds[0]),
            right=float(self.y_thresholds[-1]),
        )
        return np.asarray(np.clip(calibrated, 0.0, 1.0), dtype=float)

    def save_calibrator(self, path: str | Path) -> dict[str, object]:
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "format": "json",
            "calibrator": "isotonic",
            "version": 1,
            "x_thresholds": self.x_thresholds,
            "y_thresholds": self.y_thresholds,
        }
        target.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
        return {"path": str(target), "format": "json", "calibrator": "isotonic"}


def fit_calibrator(probabilities: Iterable[float], outcomes: Iterable[float]) -> ProbabilityCalibrator:
    return ProbabilityCalibrator.fit(probabilities, outcomes)


def load_calibrator(path: str | Path) -> ProbabilityCalibrator:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    return ProbabilityCalibrator(
        x_thresholds=[float(value) for value in payload.get("x_thresholds", [])],
        y_thresholds=[float(value) for value in payload.get("y_thresholds", [])],
    )


def save_calibrator(calibrator: ProbabilityCalibrator, path: str | Path) -> dict[str, object]:
    return calibrator.save_calibrator(path)


def calibrate(calibrator: ProbabilityCalibrator, probabilities: Iterable[float] | np.ndarray) -> np.ndarray:
    return calibrator.calibrate(probabilities)
