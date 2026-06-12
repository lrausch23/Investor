from __future__ import annotations

import csv
import itertools
import json
from dataclasses import asdict, replace
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from .config import DEFAULT_SIGNAL_THRESHOLDS, SignalThresholds
from .pipeline_backtest import PipelineBacktestConfig, run_pipeline_backtest


DEFAULT_THRESHOLD_GRID: dict[str, list[Any]] = {
    "use_empirical_durations": [False, True],
    "use_forward_curve_gates": [False, True],
    "neutral_tilt_requires_modal": [False, True],
    "composite_adjustments_enabled": [True, False],
}


def expand_threshold_grid(grid: dict[str, list[Any]] | None = None) -> list[dict[str, Any]]:
    raw_grid = grid or DEFAULT_THRESHOLD_GRID
    normalized = {
        str(key): (list(value) if isinstance(value, (list, tuple)) else [value])
        for key, value in raw_grid.items()
    }
    keys = list(normalized)
    combos: list[dict[str, Any]] = []
    for values in itertools.product(*(normalized[key] for key in keys)):
        combo = {key: values[index] for index, key in enumerate(keys)}
        combo["combo_id"] = _combo_id(combo)
        combos.append(combo)
    return combos


def load_threshold_grid(path: str | Path | None) -> dict[str, list[Any]] | None:
    if not path:
        return None
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if isinstance(payload, list):
        return {f"combo_{index}": [item] for index, item in enumerate(payload)}
    if not isinstance(payload, dict):
        raise ValueError("Threshold grid JSON must be an object mapping parameter names to values.")
    return {str(key): (value if isinstance(value, list) else [value]) for key, value in payload.items()}


def thresholds_from_combo(combo: dict[str, Any], base: SignalThresholds = DEFAULT_SIGNAL_THRESHOLDS) -> SignalThresholds:
    threshold_fields = set(SignalThresholds.__dataclass_fields__)
    updates = {key: value for key, value in combo.items() if key in threshold_fields}
    return replace(base, **updates)


def run_threshold_sweep(
    *,
    tickers: list[str],
    market_frames: dict[str, pd.DataFrame],
    benchmark_frame: pd.DataFrame | None = None,
    grid: dict[str, list[Any]] | None = None,
    base_config: PipelineBacktestConfig | None = None,
    signal_provider_factory: Callable[[str, dict[str, Any]], Any] | None = None,
    include_stress_windows: bool = False,
) -> list[dict[str, Any]]:
    config = base_config or PipelineBacktestConfig()
    rows: list[dict[str, Any]] = []
    for combo in expand_threshold_grid(grid):
        thresholds = thresholds_from_combo(combo, base=config.signal_thresholds)
        combo_config = replace(
            config,
            signal_thresholds=thresholds,
            composite_adjustments_enabled=bool(combo.get("composite_adjustments_enabled", config.composite_adjustments_enabled)),
        )
        combo_rows: list[dict[str, Any]] = []
        for ticker in tickers:
            normalized = str(ticker or "").strip().upper()
            frame = market_frames.get(normalized)
            if frame is None:
                frame = market_frames.get(str(ticker))
            if frame is None:
                continue
            result = run_pipeline_backtest(
                normalized,
                frame,
                config=combo_config,
                benchmark_frame=benchmark_frame,
                signal_provider=signal_provider_factory(normalized, combo) if signal_provider_factory is not None else None,
            )
            row = _result_row(normalized, combo, result.metrics, result.in_sample, result.out_of_sample)
            if include_stress_windows:
                row.update(_stress_metric_prefix(result.stress_windows))
            combo_rows.append(row)
            rows.append(row)
        if combo_rows:
            rows.append(_aggregate_row(combo, combo_rows))
    return rows


def write_sweep_rows(rows: list[dict[str, Any]], *, json_path: str | Path | None = None, csv_path: str | Path | None = None) -> None:
    if json_path:
        Path(json_path).write_text(json.dumps(rows, indent=2) + "\n", encoding="utf-8")
    if csv_path:
        all_fields: list[str] = []
        for row in rows:
            for key in row:
                if key not in all_fields:
                    all_fields.append(key)
        with Path(csv_path).open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=all_fields)
            writer.writeheader()
            writer.writerows(rows)


def _result_row(
    ticker: str,
    combo: dict[str, Any],
    metrics: dict[str, Any],
    in_sample: dict[str, Any],
    out_of_sample: dict[str, Any] | None,
) -> dict[str, Any]:
    row = {
        "ticker": ticker,
        "combo_id": combo["combo_id"],
        **{f"param_{key}": value for key, value in combo.items() if key != "combo_id"},
    }
    row.update(_metric_prefix("full", metrics))
    row.update(_metric_prefix("is", in_sample))
    row.update(_metric_prefix("oos", out_of_sample or {}))
    return row


def _aggregate_row(combo: dict[str, Any], rows: list[dict[str, Any]]) -> dict[str, Any]:
    aggregate = {
        "ticker": "__AGGREGATE__",
        "combo_id": combo["combo_id"],
        **{f"param_{key}": value for key, value in combo.items() if key != "combo_id"},
        "constituent_count": len(rows),
    }
    for prefix in ("full", "is", "oos"):
        for metric in ("total_return", "sharpe_ratio", "max_drawdown"):
            values = [_to_float(row.get(f"{prefix}_{metric}")) for row in rows]
            finite = [value for value in values if value is not None]
            aggregate[f"{prefix}_{metric}_avg"] = sum(finite) / len(finite) if finite else None
        for metric in ("trade_count", "neutral_tilt_trade_count", "neutral_tilt_net_pnl"):
            values = [_to_float(row.get(f"{prefix}_{metric}")) for row in rows]
            finite = [value for value in values if value is not None]
            aggregate[f"{prefix}_{metric}_sum"] = sum(finite) if finite else None
    stress_keys = sorted({key for row in rows for key in row if key.startswith("stress_")})
    for key in stress_keys:
        values = [_to_float(row.get(key)) for row in rows]
        finite = [value for value in values if value is not None]
        if not finite:
            aggregate[key] = None
        elif key.endswith("_trade_count"):
            aggregate[f"{key}_sum"] = sum(finite)
        else:
            aggregate[f"{key}_avg"] = sum(finite) / len(finite)
    return aggregate


def _metric_prefix(prefix: str, metrics: dict[str, Any]) -> dict[str, Any]:
    return {
        f"{prefix}_total_return": _to_float(metrics.get("total_return")),
        f"{prefix}_sharpe_ratio": _to_float(metrics.get("sharpe_ratio")),
        f"{prefix}_max_drawdown": _to_float(metrics.get("max_drawdown")),
        f"{prefix}_trade_count": _to_float(metrics.get("trade_count")),
        f"{prefix}_neutral_tilt_trade_count": _to_float(metrics.get("neutral_tilt_trade_count")),
        f"{prefix}_neutral_tilt_net_pnl": _to_float(metrics.get("neutral_tilt_net_pnl")),
    }


def _stress_metric_prefix(windows: list[dict[str, Any]]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for row in windows or []:
        if not isinstance(row, dict):
            continue
        key = str(row.get("key") or "").strip()
        if not key:
            continue
        prefix = f"stress_{key}"
        payload[f"{prefix}_strategy_total_return"] = _to_float(row.get("strategy_total_return"))
        payload[f"{prefix}_benchmark_total_return"] = _to_float(row.get("benchmark_total_return"))
        payload[f"{prefix}_strategy_max_drawdown"] = _to_float(row.get("strategy_max_drawdown"))
        payload[f"{prefix}_benchmark_max_drawdown"] = _to_float(row.get("benchmark_max_drawdown"))
        payload[f"{prefix}_exposure_pct"] = _to_float(row.get("exposure_pct"))
        payload[f"{prefix}_trade_count"] = _to_float(row.get("trade_count"))
        payload[f"{prefix}_days_to_bear_flag"] = _to_float(row.get("days_to_bear_flag"))
    return payload


def _combo_id(combo: dict[str, Any]) -> str:
    parts = []
    for key in sorted(combo):
        value = combo[key]
        if isinstance(value, bool):
            rendered = "on" if value else "off"
        else:
            rendered = str(value).replace(" ", "_")
        parts.append(f"{key}={rendered}")
    return "|".join(parts)


def _to_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        parsed = float(value)
    except Exception:
        return None
    return parsed if parsed == parsed and parsed not in (float("inf"), float("-inf")) else None


def config_payload(config: PipelineBacktestConfig) -> dict[str, Any]:
    payload = asdict(config)
    thresholds = payload.get("signal_thresholds")
    if isinstance(thresholds, SignalThresholds):
        payload["signal_thresholds"] = asdict(thresholds)
    return payload
