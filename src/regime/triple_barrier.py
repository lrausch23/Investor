from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class BarrierConfig:
    """Configuration for Triple-Barrier labeling."""

    atr_period: int = 14
    profit_target_atr_mult: float = 2.0
    stop_loss_atr_mult: float = 2.0
    max_holding_days: int = 21
    min_atr: float = 0.01


DEFAULT_BARRIER_CONFIG = BarrierConfig()


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
        raise ValueError("regime_result must provide a price_frame")
    return apply_triple_barrier_labels(
        price_frame,
        regime_col="regime",
        close_col="price",
        high_col="high",
        low_col="low",
        config=config,
    )
