from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from sklearn.isotonic import IsotonicRegression


@dataclass(frozen=True)
class CalibrationResult:
    bins: list[dict[str, Any]]
    brier_score: float | None
    reliability_diagram_data: list[dict[str, Any]]


def _realized_success(row: dict[str, Any]) -> float | None:
    action = str(row.get("action") or "Hold")
    realized = row.get("return_1m")
    if realized is None:
        realized = row.get("return_1w")
    if realized is None:
        realized = row.get("return_3m")
    if realized is None:
        return None
    value = float(realized)
    if action in {"Buy", "Strong Buy"}:
        return 1.0 if value > 0 else 0.0
    if action in {"Sell", "Strong Sell"}:
        return 1.0 if value < 0 else 0.0
    return 1.0 if abs(value) < 0.02 else 0.0


def compute_calibration_curve(snapshots: list[dict[str, Any]], bins: int = 10) -> CalibrationResult:
    step = 1.0 / max(1, bins)
    bucket_rows: list[list[tuple[float, float]]] = [[] for _ in range(max(1, bins))]
    squared_errors: list[float] = []
    for row in snapshots:
        probability = float(row.get("regime_probability") or 0.0)
        observed = _realized_success(row)
        if observed is None:
            continue
        index = min(len(bucket_rows) - 1, max(0, int(probability / step)))
        bucket_rows[index].append((probability, observed))
        squared_errors.append((probability - observed) ** 2)
    points = []
    for idx, entries in enumerate(bucket_rows):
        if not entries:
            continue
        predicted = sum(item[0] for item in entries) / len(entries)
        observed = sum(item[1] for item in entries) / len(entries)
        points.append({"predicted": predicted, "observed": observed, "count": len(entries), "bin": idx})
    return CalibrationResult(
        bins=points,
        brier_score=(sum(squared_errors) / len(squared_errors)) if squared_errors else None,
        reliability_diagram_data=points,
    )


def compute_sharpness(snapshots: list[dict[str, Any]]) -> dict[str, Any]:
    histogram = [0] * 10
    for row in snapshots:
        probability = float(row.get("regime_probability") or 0.0)
        index = min(9, max(0, int(probability * 10)))
        histogram[index] += 1
    return {"histogram": histogram, "count": sum(histogram)}


def compute_regime_accuracy(regime_results: list[dict[str, Any]], actual_returns: list[dict[str, Any]]) -> dict[str, Any]:
    actual_map = {str(row.get("ticker") or "").upper(): float(row.get("return") or 0.0) for row in actual_returns}
    grouped: dict[str, list[float]] = {"Bull": [], "Neutral": [], "Bear": []}
    for row in regime_results:
        label = str(row.get("regime") or "Neutral")
        ticker = str(row.get("ticker") or "").upper()
        if label in grouped and ticker in actual_map:
            grouped[label].append(actual_map[ticker])
    return {
        key: {"avg_return": (sum(values) / len(values)) if values else None, "count": len(values)}
        for key, values in grouped.items()
    }


def calibration_payload(snapshots: list[dict[str, Any]]) -> dict[str, Any]:
    curve = compute_calibration_curve(snapshots)
    return {"calibration": asdict(curve), "sharpness": compute_sharpness(snapshots)}


def fit_probability_calibrator(
    predicted_probs: list[float],
    actual_outcomes: list[float],
) -> IsotonicRegression:
    """Fit isotonic regression to map raw HMM probabilities to calibrated confidence."""
    calibrator = IsotonicRegression(out_of_bounds="clip")
    calibrator.fit(predicted_probs, actual_outcomes)
    return calibrator


def duration_accuracy(expected_duration: float, historical_durations: dict[str, Any], regime_label: str) -> dict[str, Any]:
    stats = historical_durations.get(regime_label) if isinstance(historical_durations, dict) else None
    if not stats:
        return {
            "expected": expected_duration,
            "historical_avg": None,
            "historical_median": None,
            "accuracy_note": "No historical duration data available yet.",
        }
    historical_avg = float(stats.get("avg") or 0.0)
    historical_median = float(stats.get("median") or 0.0)
    if historical_avg <= 0:
        note = "Historical duration data is not yet sufficient."
    else:
        diff_ratio = abs(expected_duration - historical_avg) / historical_avg
        if diff_ratio <= 0.20:
            note = f"Model expects {expected_duration:.1f} days; historically {regime_label} regimes lasted {historical_avg:.1f} days on average."
        elif expected_duration < historical_avg:
            note = f"Model expects {expected_duration:.1f} days; historically {regime_label} regimes lasted {historical_avg:.1f} days on average — model may be underestimating."
        else:
            note = f"Model expects {expected_duration:.1f} days; historically {regime_label} regimes lasted {historical_avg:.1f} days on average — model may be overstating persistence."
    return {
        "expected": expected_duration,
        "historical_avg": historical_avg,
        "historical_median": historical_median,
        "accuracy_note": note,
    }
