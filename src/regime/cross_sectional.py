from __future__ import annotations

from dataclasses import dataclass
import datetime as dt
import math
from typing import Any

import numpy as np
import pandas as pd

from .market_data_client import download_daily_bars

DEFAULT_BETA_LOOKBACK_DAYS = 252
DEFAULT_VOL_BASELINE_DAYS = 200
DEFAULT_BENCHMARK_TICKER = "SPY"


@dataclass
class BetaAdjustedResult:
    ticker: str
    raw_return: float | None
    beta: float | None
    beta_adjusted_return: float | None
    benchmark_return: float | None
    alpha: float | None
    data_quality: str


@dataclass
class VolatilityZResult:
    ticker: str
    current_vol: float | None
    baseline_vol: float | None
    baseline_std: float | None
    vol_z_score: float | None
    interpretation: str
    data_quality: str


@dataclass
class PeerNormResult:
    ticker: str
    metric_name: str
    raw_value: float | None
    percentile: float | None
    peer_count: int
    peer_median: float | None
    peer_mean: float | None


def _fetch_closes(ticker: str, days: int) -> pd.Series:
    buffer_days = max(int(days), 1) + 30
    end = dt.date.today()
    start = end - dt.timedelta(days=buffer_days)
    try:
        history = download_daily_bars(str(ticker or "").upper(), start=start, end=end, auto_adjust=True)
    except Exception:
        return pd.Series(dtype=float)
    if history.empty:
        return pd.Series(dtype=float)
    close = history["Close"] if "Close" in history.columns else history.iloc[:, 0]
    if getattr(close, "ndim", 1) > 1:
        close = close.iloc[:, 0]
    series = pd.to_numeric(close, errors="coerce").dropna()
    if series.empty:
        return pd.Series(dtype=float)
    series.index = pd.to_datetime(series.index).tz_localize(None)
    return pd.Series(series, copy=False).sort_index()


def _trailing_return(series: pd.Series, period_days: int) -> float | None:
    clean = pd.to_numeric(series, errors="coerce").dropna()
    if len(clean) < 2:
        return None
    window = clean.tail(max(int(period_days), 1) + 1)
    if len(window) < 2:
        return None
    base = float(window.iloc[0])
    if base == 0:
        return None
    return (float(window.iloc[-1]) - base) / base


def calculate_beta_adjusted_return(
    ticker: str,
    *,
    period_days: int = 63,
    lookback_days: int = DEFAULT_BETA_LOOKBACK_DAYS,
    benchmark: str = DEFAULT_BENCHMARK_TICKER,
) -> BetaAdjustedResult:
    ticker_key = str(ticker or "").upper()
    series = _fetch_closes(ticker_key, max(int(period_days), int(lookback_days)))
    benchmark_series = _fetch_closes(str(benchmark or DEFAULT_BENCHMARK_TICKER).upper(), max(int(period_days), int(lookback_days)))
    raw_return = _trailing_return(series, int(period_days))
    benchmark_return = _trailing_return(benchmark_series, int(period_days))
    if series.empty or benchmark_series.empty:
        return BetaAdjustedResult(
            ticker=ticker_key,
            raw_return=raw_return,
            beta=None,
            beta_adjusted_return=None,
            benchmark_return=benchmark_return,
            alpha=None,
            data_quality="insufficient",
        )

    joined = pd.concat([series.rename("ticker"), benchmark_series.rename("benchmark")], axis=1).dropna()
    returns = joined.pct_change().dropna()
    if len(returns) < 60:
        return BetaAdjustedResult(
            ticker=ticker_key,
            raw_return=raw_return,
            beta=None,
            beta_adjusted_return=None,
            benchmark_return=benchmark_return,
            alpha=None,
            data_quality="insufficient",
        )

    x = returns["benchmark"].to_numpy(dtype=float)
    y = returns["ticker"].to_numpy(dtype=float)
    design = np.column_stack([np.ones(len(x), dtype=float), x])
    coeffs, *_ = np.linalg.lstsq(design, y, rcond=None)
    beta = float(coeffs[1])
    if raw_return is None or benchmark_return is None:
        return BetaAdjustedResult(
            ticker=ticker_key,
            raw_return=raw_return,
            beta=beta,
            beta_adjusted_return=None,
            benchmark_return=benchmark_return,
            alpha=None,
            data_quality="partial",
        )
    beta_floor = max(abs(beta), 0.1)
    return BetaAdjustedResult(
        ticker=ticker_key,
        raw_return=float(raw_return),
        beta=beta,
        beta_adjusted_return=float(raw_return) / beta_floor,
        benchmark_return=float(benchmark_return),
        alpha=float(raw_return) - (beta * float(benchmark_return)),
        data_quality="full",
    )


def calculate_volatility_z_score(
    ticker: str,
    *,
    current_window: int = 20,
    baseline_days: int = DEFAULT_VOL_BASELINE_DAYS,
) -> VolatilityZResult:
    ticker_key = str(ticker or "").upper()
    series = _fetch_closes(ticker_key, max(int(baseline_days) + int(current_window), 90))
    if series.empty:
        return VolatilityZResult(
            ticker=ticker_key,
            current_vol=None,
            baseline_vol=None,
            baseline_std=None,
            vol_z_score=None,
            interpretation="insufficient",
            data_quality="insufficient",
        )
    returns = series.pct_change().dropna()
    if len(returns) < max(int(baseline_days // 2), 60):
        return VolatilityZResult(
            ticker=ticker_key,
            current_vol=None,
            baseline_vol=None,
            baseline_std=None,
            vol_z_score=None,
            interpretation="insufficient",
            data_quality="insufficient",
        )
    rolling_vol = returns.rolling(int(current_window)).std().dropna() * math.sqrt(252.0)
    if len(rolling_vol) < max(30, int(current_window)):
        return VolatilityZResult(
            ticker=ticker_key,
            current_vol=None,
            baseline_vol=None,
            baseline_std=None,
            vol_z_score=None,
            interpretation="insufficient",
            data_quality="insufficient",
        )
    current_vol = float(rolling_vol.iloc[-1])
    baseline_mean = float(rolling_vol.mean())
    baseline_std = float(rolling_vol.std(ddof=0) or 0.0)
    vol_z = (current_vol - baseline_mean) / max(baseline_std, 1e-8)
    if vol_z > 1.5:
        interpretation = "Elevated"
    elif vol_z < -1.0:
        interpretation = "Subdued"
    else:
        interpretation = "Normal"
    return VolatilityZResult(
        ticker=ticker_key,
        current_vol=current_vol,
        baseline_vol=baseline_mean,
        baseline_std=baseline_std,
        vol_z_score=float(vol_z),
        interpretation=interpretation,
        data_quality="full",
    )


def _percentile_rank(value: float, peers: list[float]) -> float | None:
    if not peers:
        return None
    if len(peers) == 1:
        return None
    sorted_values = sorted(float(item) for item in peers)
    less = sum(1 for item in sorted_values if item < value)
    equal = sum(1 for item in sorted_values if item == value)
    return ((less + 0.5 * equal) / len(sorted_values)) * 100.0


def compute_peer_percentiles(
    tickers: list[str],
    metrics: dict[str, dict[str, float | None]],
) -> dict[str, list[PeerNormResult]]:
    metric_names: set[str] = set()
    for payload in metrics.values():
        metric_names.update(str(name) for name in payload.keys())

    results: dict[str, list[PeerNormResult]] = {str(ticker or "").upper(): [] for ticker in tickers}
    for metric_name in sorted(metric_names):
        values = {
            str(ticker or "").upper(): float(value)
            for ticker, payload in metrics.items()
            for name, value in payload.items()
            if str(name) == metric_name and value is not None
        }
        peer_values = list(values.values())
        peer_count = len(peer_values)
        peer_median = float(np.median(peer_values)) if peer_values else None
        peer_mean = float(np.mean(peer_values)) if peer_values else None
        for ticker in tickers:
            ticker_key = str(ticker or "").upper()
            raw_value = metrics.get(ticker_key, {}).get(metric_name)
            percentile = None
            if raw_value is not None:
                percentile = _percentile_rank(float(raw_value), peer_values)
            results.setdefault(ticker_key, []).append(
                PeerNormResult(
                    ticker=ticker_key,
                    metric_name=metric_name,
                    raw_value=float(raw_value) if raw_value is not None else None,
                    percentile=percentile,
                    peer_count=peer_count,
                    peer_median=peer_median,
                    peer_mean=peer_mean,
                )
            )
    return results
