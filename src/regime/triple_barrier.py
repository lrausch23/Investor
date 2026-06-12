from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd

from .exceptions import DataValidationError
from .paper_trading import trailing_stop_level


@dataclass(frozen=True)
class BarrierConfig:
    """Configuration for Triple-Barrier labeling."""

    atr_period: int = 14
    profit_target_atr_mult: float = 2.0
    stop_loss_atr_mult: float = 2.0
    max_holding_days: int = 21
    min_atr: float = 0.01


DEFAULT_BARRIER_CONFIG = BarrierConfig()


@dataclass(frozen=True)
class ManagedExitConfig:
    """Configuration for production-management-aware long-entry labeling."""

    profit_target_atr_mult: float = 2.0
    stop_atr_mult: float = 2.0
    trailing_atr_mult: float = 2.0
    trailing_activation_atr: float = 1.0
    time_stop_days: int = 21
    cost_bps: float = 20.0
    min_atr: float = 0.01


DEFAULT_MANAGED_EXIT_CONFIG = ManagedExitConfig()


def compute_atr(
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    period: int = 14,
) -> pd.Series:
    """Compute Average True Range using Wilder's smoothing."""

    tr = pd.concat(
        [
            high - low,
            (high - close.shift(1)).abs(),
            (low - close.shift(1)).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return tr.ewm(alpha=1.0 / period, min_periods=period, adjust=False).mean()


def _label_single_bar(
    idx: int,
    close: np.ndarray,
    high: np.ndarray,
    low: np.ndarray,
    atr: np.ndarray,
    regime: str,
    config: BarrierConfig,
) -> dict[str, Any]:
    """Label a single bar by scanning forward."""

    entry_price = float(close[idx])
    atr_raw = atr[idx]
    atr_val = max(float(atr_raw) if np.isfinite(atr_raw) else config.min_atr, config.min_atr)

    if regime == "Bull":
        target = entry_price + config.profit_target_atr_mult * atr_val
        stop = entry_price - config.stop_loss_atr_mult * atr_val
    elif regime == "Bear":
        target = entry_price - config.profit_target_atr_mult * atr_val
        stop = entry_price + config.stop_loss_atr_mult * atr_val
    else:
        return {
            "barrier_outcome": np.nan,
            "barrier_type": None,
            "barrier_days": None,
            "barrier_entry": entry_price,
            "barrier_target": None,
            "barrier_stop": None,
        }

    max_scan = min(idx + config.max_holding_days, len(close) - 1)

    for j in range(idx + 1, max_scan + 1):
        days = j - idx
        if regime == "Bull":
            if high[j] >= target:
                return {
                    "barrier_outcome": 1.0,
                    "barrier_type": "upper",
                    "barrier_days": days,
                    "barrier_entry": entry_price,
                    "barrier_target": target,
                    "barrier_stop": stop,
                }
            if low[j] <= stop:
                return {
                    "barrier_outcome": 0.0,
                    "barrier_type": "lower",
                    "barrier_days": days,
                    "barrier_entry": entry_price,
                    "barrier_target": target,
                    "barrier_stop": stop,
                }
        else:
            if low[j] <= target:
                return {
                    "barrier_outcome": 1.0,
                    "barrier_type": "upper",
                    "barrier_days": days,
                    "barrier_entry": entry_price,
                    "barrier_target": target,
                    "barrier_stop": stop,
                }
            if high[j] >= stop:
                return {
                    "barrier_outcome": 0.0,
                    "barrier_type": "lower",
                    "barrier_days": days,
                    "barrier_entry": entry_price,
                    "barrier_target": target,
                    "barrier_stop": stop,
                }

    return {
        "barrier_outcome": 0.0,
        "barrier_type": "vertical",
        "barrier_days": (max_scan - idx) if max_scan > idx else config.max_holding_days,
        "barrier_entry": entry_price,
        "barrier_target": target,
        "barrier_stop": stop,
    }


def apply_triple_barrier_labels(
    price_frame: pd.DataFrame,
    regime_col: str = "regime",
    close_col: str = "Close",
    high_col: str = "High",
    low_col: str = "Low",
    config: BarrierConfig = DEFAULT_BARRIER_CONFIG,
) -> pd.DataFrame:
    """
    Add triple-barrier labels to a price frame that already has HMM regime labels.
    """

    frame = price_frame.copy()
    close_name = close_col if close_col in frame.columns else "price"
    high_name = high_col if high_col in frame.columns else "high"
    low_name = low_col if low_col in frame.columns else "low"
    if regime_col not in frame.columns:
        raise KeyError(f"Missing regime column: {regime_col}")
    for column in (close_name, high_name, low_name):
        if column not in frame.columns:
            raise KeyError(f"Missing price column: {column}")

    atr = compute_atr(
        frame[high_name].astype(float),
        frame[low_name].astype(float),
        frame[close_name].astype(float),
        period=config.atr_period,
    )
    close = frame[close_name].astype(float).to_numpy()
    high = frame[high_name].astype(float).to_numpy()
    low = frame[low_name].astype(float).to_numpy()
    atr_values = atr.astype(float).to_numpy()

    labels = [
        _label_single_bar(
            idx,
            close,
            high,
            low,
            atr_values,
            str(frame.iloc[idx][regime_col] or ""),
            config,
        )
        for idx in range(len(frame))
    ]
    label_frame = pd.DataFrame(labels, index=frame.index)
    return frame.join(label_frame)


def _calendar_days(index: pd.Index, start_idx: int, end_idx: int) -> int:
    try:
        start = pd.Timestamp(index[start_idx]).normalize()
        end = pd.Timestamp(index[end_idx]).normalize()
        return max(0, int((end - start).days))
    except Exception:
        return max(0, int(end_idx - start_idx))


def _managed_label_single_bar(
    idx: int,
    index: pd.Index,
    close: np.ndarray,
    high: np.ndarray,
    low: np.ndarray,
    atr: np.ndarray,
    regimes: list[str],
    config: ManagedExitConfig,
) -> dict[str, Any]:
    entry_price = float(close[idx])
    atr_raw = atr[idx]
    atr_val = max(float(atr_raw) if np.isfinite(atr_raw) else config.min_atr, config.min_atr)
    if regimes[idx] != "Bull" or not np.isfinite(entry_price) or entry_price <= 0:
        return {
            "barrier_outcome": np.nan,
            "barrier_type": None,
            "barrier_days": None,
            "barrier_entry": entry_price if np.isfinite(entry_price) else None,
            "barrier_target": None,
            "barrier_stop": None,
            "label_end_idx": np.nan,
            "label_entry_date": pd.Timestamp(index[idx]),
            "label_end_date": pd.NaT,
        }

    target = entry_price + config.profit_target_atr_mult * atr_val
    initial_stop = entry_price - config.stop_atr_mult * atr_val
    current_stop = initial_stop
    cost_fraction = max(0.0, float(config.cost_bps or 0.0)) / 10_000.0

    def resolved(j: int, outcome: float, barrier_type: str) -> dict[str, Any]:
        return {
            "barrier_outcome": float(outcome),
            "barrier_type": barrier_type,
            "barrier_days": _calendar_days(index, idx, j),
            "barrier_entry": entry_price,
            "barrier_target": target,
            "barrier_stop": current_stop,
            "label_end_idx": int(j),
            "label_entry_date": pd.Timestamp(index[idx]),
            "label_end_date": pd.Timestamp(index[j]),
        }

    for j in range(idx + 1, len(close)):
        stop_touched = bool(np.isfinite(low[j]) and low[j] <= current_stop)
        if stop_touched:
            if current_stop > entry_price:
                net_return = (current_stop / entry_price) - 1.0 - cost_fraction
                return resolved(j, 1.0 if net_return > 0.0 else 0.0, "trailing")
            return resolved(j, 0.0, "stop")

        target_touched = bool(np.isfinite(high[j]) and high[j] >= target)
        if target_touched:
            return resolved(j, 1.0, "target")

        holding_days = _calendar_days(index, idx, j)
        if holding_days >= int(config.time_stop_days):
            net_return = (float(close[j]) / entry_price) - 1.0 - cost_fraction
            return resolved(j, 1.0 if net_return > 0.0 else 0.0, "time_win" if net_return > 0.0 else "time_loss")

        if regimes[j] == "Bear":
            net_return = (float(close[j]) / entry_price) - 1.0 - cost_fraction
            return resolved(j, 1.0 if net_return > 0.0 else 0.0, "regime")

        ratcheted = trailing_stop_level(
            entry_price=entry_price,
            current_price=float(close[j]),
            atr_14=atr_val,
            existing_stop=current_stop,
            atr_multiplier=config.trailing_atr_mult,
            activation_atr=config.trailing_activation_atr,
        )
        if ratcheted is not None:
            current_stop = float(ratcheted)

    return {
        "barrier_outcome": np.nan,
        "barrier_type": None,
        "barrier_days": None,
        "barrier_entry": entry_price,
        "barrier_target": target,
        "barrier_stop": current_stop,
        "label_end_idx": np.nan,
        "label_entry_date": pd.Timestamp(index[idx]),
        "label_end_date": pd.NaT,
    }


def apply_managed_exit_labels(
    price_frame: pd.DataFrame,
    regime_col: str = "regime",
    close_col: str = "price",
    high_col: str = "high",
    low_col: str = "low",
    config: ManagedExitConfig = DEFAULT_MANAGED_EXIT_CONFIG,
) -> pd.DataFrame:
    """Label Bull entries using the same exit ladder as the production backtest."""

    frame = price_frame.copy()
    close_name = close_col if close_col in frame.columns else "Close"
    high_name = high_col if high_col in frame.columns else "High"
    low_name = low_col if low_col in frame.columns else "Low"
    if regime_col not in frame.columns:
        raise KeyError(f"Missing regime column: {regime_col}")
    for column in (close_name, high_name, low_name):
        if column not in frame.columns:
            raise KeyError(f"Missing price column: {column}")

    close_series = frame[close_name].astype(float)
    high_series = frame[high_name].astype(float)
    low_series = frame[low_name].astype(float)
    if "atr_14" in frame.columns:
        atr = pd.to_numeric(frame["atr_14"], errors="coerce")
    else:
        atr = compute_atr(high_series, low_series, close_series, period=14)
    atr = atr.fillna(float(config.min_atr)).clip(lower=float(config.min_atr))

    close = close_series.to_numpy()
    high = high_series.to_numpy()
    low = low_series.to_numpy()
    atr_values = atr.astype(float).to_numpy()
    regimes = [str(value or "") for value in frame[regime_col].tolist()]
    labels = [
        _managed_label_single_bar(idx, frame.index, close, high, low, atr_values, regimes, config)
        for idx in range(len(frame))
    ]
    label_frame = pd.DataFrame(labels, index=frame.index)
    return frame.join(label_frame)


def sample_uniqueness_weights(labeled_frame: pd.DataFrame) -> pd.Series:
    """Compute average-uniqueness sample weights from label lifespans."""

    if labeled_frame.empty:
        return pd.Series(1.0, index=labeled_frame.index, dtype=float)

    weights = pd.Series(1.0, index=labeled_frame.index, dtype=float)
    group_iter = (
        labeled_frame.groupby("ticker", sort=False)
        if "ticker" in labeled_frame.columns
        else [(None, labeled_frame)]
    )

    if {"label_entry_date", "label_end_date"}.issubset(labeled_frame.columns):
        for _group, group in group_iter:
            valid = group.loc[group["barrier_outcome"].notna()].copy()
            if valid.empty:
                continue
            valid["_entry_date"] = pd.to_datetime(valid["label_entry_date"], errors="coerce")
            valid["_end_date"] = pd.to_datetime(valid["label_end_date"], errors="coerce")
            valid = valid.loc[valid["_entry_date"].notna() & valid["_end_date"].notna()]
            if valid.empty:
                continue
            date_intervals: list[tuple[Any, list[pd.Timestamp]]] = []
            date_concurrency: dict[pd.Timestamp, int] = {}
            for index_value, row in valid.iterrows():
                start = pd.Timestamp(row["_entry_date"]).normalize()
                end = pd.Timestamp(row["_end_date"]).normalize()
                if end < start:
                    continue
                days = list(pd.date_range(start, end, freq="D"))
                if not days:
                    continue
                date_intervals.append((index_value, days))
                for day in days:
                    date_concurrency[day] = date_concurrency.get(day, 0) + 1
            for index_value, days in date_intervals:
                values = [1.0 / date_concurrency[day] for day in days if date_concurrency.get(day, 0) > 0]
                if values:
                    weights.loc[index_value] = float(np.mean(values))
        return weights

    if "label_end_idx" not in labeled_frame.columns:
        return weights

    group_iter = (
        labeled_frame.groupby("ticker", sort=False)
        if "ticker" in labeled_frame.columns
        else [(None, labeled_frame)]
    )
    for _group, group in group_iter:
        valid = group.loc[group["barrier_outcome"].notna() & group["label_end_idx"].notna()]
        if valid.empty:
            continue
        group_positions = {index_value: position for position, index_value in enumerate(group.index)}
        positional_intervals: list[tuple[Any, int, int]] = []
        for index_value, row in valid.iterrows():
            try:
                start = int(row["_label_start_idx"]) if "_label_start_idx" in valid.columns else group_positions[index_value]
            except Exception:
                start = group_positions[index_value]
            try:
                end = int(row["label_end_idx"])
            except Exception:
                continue
            start = max(0, min(start, len(group) - 1))
            end = max(start, min(end, len(group) - 1))
            positional_intervals.append((index_value, start, end))
        if not positional_intervals:
            continue
        positional_concurrency = np.zeros(len(group), dtype=float)
        for _index_value, start, end in positional_intervals:
            positional_concurrency[start : end + 1] += 1.0
        for index_value, start, end in positional_intervals:
            active = positional_concurrency[start : end + 1]
            active = active[active > 0]
            if len(active):
                weights.loc[index_value] = float(np.mean(1.0 / active))
    return weights


def build_managed_labeled_frame(
    ticker: str,
    regime_result: Any,
    config: ManagedExitConfig = DEFAULT_MANAGED_EXIT_CONFIG,
) -> pd.DataFrame:
    """Build production-management-aware labels from a RegimeResult price frame."""

    del ticker
    price_frame = getattr(regime_result, "price_frame", None)
    if price_frame is None:
        raise DataValidationError("regime_result must provide a price_frame")
    return apply_managed_exit_labels(
        price_frame,
        regime_col="regime",
        close_col="price",
        high_col="high",
        low_col="low",
        config=config,
    )


def build_multi_ticker_managed_frame(
    ticker_regime_pairs: list[tuple[str, Any]],
    config: ManagedExitConfig | None = None,
) -> pd.DataFrame:
    """Build a multi-ticker managed-label frame with date-stable coordinates."""

    managed_config = config or DEFAULT_MANAGED_EXIT_CONFIG
    expected_columns = [
        "ticker",
        "barrier_outcome",
        "barrier_type",
        "barrier_days",
        "barrier_entry",
        "barrier_target",
        "barrier_stop",
        "label_end_idx",
        "label_entry_date",
        "label_end_date",
    ]
    if not ticker_regime_pairs:
        return pd.DataFrame(columns=expected_columns)

    labeled_frames: list[pd.DataFrame] = []
    for ticker, regime_result in ticker_regime_pairs:
        labeled = build_managed_labeled_frame(str(ticker), regime_result, config=managed_config).copy()
        labeled["ticker"] = str(ticker).upper()
        labeled_frames.append(labeled)

    combined = pd.concat(labeled_frames, ignore_index=True) if labeled_frames else pd.DataFrame(columns=expected_columns)
    if "barrier_outcome" not in combined.columns:
        return pd.DataFrame(columns=expected_columns)
    combined = combined.loc[combined["barrier_outcome"].notna()].reset_index(drop=True)
    all_columns = list(dict.fromkeys(expected_columns + list(combined.columns)))
    if combined.empty:
        return pd.DataFrame(columns=all_columns)
    return combined.reindex(columns=all_columns)


def build_labeled_frame(
    ticker: str,
    market_frame: pd.DataFrame,
    regime_result: Any,
    config: BarrierConfig = DEFAULT_BARRIER_CONFIG,
) -> pd.DataFrame:
    """
    Merge RegimeResult's price_frame with triple-barrier labels.
    """

    del ticker, market_frame
    price_frame = getattr(regime_result, "price_frame", None)
    if price_frame is None:
        raise DataValidationError("regime_result must provide a price_frame")
    return apply_triple_barrier_labels(
        price_frame,
        regime_col="regime",
        close_col="price",
        high_col="high",
        low_col="low",
        config=config,
    )


def build_multi_ticker_labeled_frame(
    ticker_regime_pairs: list[tuple[str, Any]],
    config: BarrierConfig | None = None,
) -> pd.DataFrame:
    """
    Build a single labeled DataFrame from multiple tickers' regime results.
    """

    barrier_config = config or DEFAULT_BARRIER_CONFIG
    expected_columns = [
        "ticker",
        "barrier_outcome",
        "barrier_type",
        "barrier_days",
        "barrier_entry",
        "barrier_target",
        "barrier_stop",
    ]
    if not ticker_regime_pairs:
        return pd.DataFrame(columns=expected_columns)

    labeled_frames: list[pd.DataFrame] = []
    for ticker, regime_result in ticker_regime_pairs:
        labeled = build_labeled_frame(str(ticker), pd.DataFrame(), regime_result, config=barrier_config).copy()
        labeled["ticker"] = str(ticker).upper()
        labeled_frames.append(labeled)

    combined = pd.concat(labeled_frames, ignore_index=True) if labeled_frames else pd.DataFrame(columns=expected_columns)
    if "barrier_outcome" not in combined.columns:
        return pd.DataFrame(columns=expected_columns)
    combined = combined.loc[combined["barrier_outcome"].notna()].reset_index(drop=True)
    if combined.empty:
        all_columns = list(dict.fromkeys(expected_columns + list(combined.columns)))
        return pd.DataFrame(columns=all_columns)
    return combined
