from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Iterable

import pandas as pd


def _as_datetime_index(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if not isinstance(out.index, pd.DatetimeIndex):
        out.index = pd.to_datetime(out.index, errors="coerce")
    out.index = out.index.tz_localize(None)
    out = out[~out.index.isna()]
    out = out.sort_index()
    return out


def preferred_close(df: pd.DataFrame) -> pd.Series:
    """
    Returns the preferred close series (adj_close if present, else close).
    """
    df = _as_datetime_index(df)
    if "adj_close" in df.columns and df["adj_close"].notna().any():
        return pd.to_numeric(df["adj_close"], errors="coerce")
    return pd.to_numeric(df.get("close"), errors="coerce")


def last_value_on_or_before(series: pd.Series, when: dt.date) -> tuple[dt.date, float] | None:
    s = series.dropna()
    if s.empty:
        return None
    idx = s.index
    if not isinstance(idx, pd.DatetimeIndex):
        idx = pd.to_datetime(idx, errors="coerce")
    # Find last timestamp <= when.
    cutoff = pd.Timestamp(dt.datetime(when.year, when.month, when.day, 23, 59, 59))
    hit = s.loc[s.index <= cutoff]
    if hit.empty:
        return None
    d = hit.index[-1].date()
    v = float(hit.iloc[-1])
    if not (v > 0):
        return None
    return d, v


def pct_return_from_lookback(series: pd.Series, lookback: int) -> float | None:
    """
    Percent return from N trading observations back (approx. trading-day lookback).
    """
    s = series.dropna()
    if len(s) < (lookback + 1):
        return None
    end = float(s.iloc[-1])
    start = float(s.iloc[-(lookback + 1)])
    if start <= 0:
        return None
    return end / start - 1.0


def ytd_return(series: pd.Series, as_of: dt.date) -> float | None:
    """
    YTD return = last_close(as_of) / close(last trading day of previous year) - 1.
    """
    hit = last_value_on_or_before(series, as_of)
    if not hit:
        return None
    _d_end, end = hit
    prev_year_end = dt.date(as_of.year - 1, 12, 31)
    base_hit = last_value_on_or_before(series, prev_year_end)
    if not base_hit:
        return None
    _d0, base = base_hit
    if base <= 0:
        return None
    return end / base - 1.0


def sma(series: pd.Series, window: int) -> pd.Series:
    s = series.astype(float)
    return s.rolling(window=window, min_periods=window).mean()


def sma_slope(series: pd.Series, window: int, slope_window: int = 20) -> float | None:
    """
    Approximate slope of SMA(window) over slope_window trading observations:
      (SMA_t - SMA_(t - slope_window)) / slope_window
    """
    s = sma(series, window=window).dropna()
    if len(s) < (slope_window + 1):
        return None
    a = float(s.iloc[-1])
    b = float(s.iloc[-(slope_window + 1)])
    return (a - b) / float(slope_window)


def dist_to_52w_high_pct(series: pd.Series, window: int = 252) -> float | None:
    s = series.dropna()
    if len(s) < 2:
        return None
    tail = s.iloc[-window:] if len(s) >= window else s
    high = float(tail.max())
    last = float(s.iloc[-1])
    if high <= 0:
        return None
    return last / high - 1.0


def avg_dollar_vol(series_close: pd.Series, series_vol: pd.Series, window: int = 20) -> float | None:
    c = series_close.dropna()
    v = series_vol.dropna()
    if c.empty or v.empty:
        return None
    # Align by index.
    df = pd.DataFrame({"c": c, "v": v}).dropna()
    if len(df) < 3:
        return None
    tail = df.iloc[-window:] if len(df) >= window else df
    dv = (tail["c"].astype(float) * tail["v"].astype(float)).mean()
    return float(dv) if dv == dv else None


@dataclass(frozen=True)
class StockMomentum:
    ticker: str
    sector: str
    ytd: float | None
    ret_3m: float | None
    ret_1m: float | None
    close: float | None
    above_sma200: bool | None
    sma50_gt_sma200: bool | None
    sma50_slope_20d: float | None
    dist_52w_high: float | None
    avg_dvol_20d: float | None

    @property
    def uptrend(self) -> bool | None:
        if self.above_sma200 is None or self.sma50_gt_sma200 is None or self.sma50_slope_20d is None:
            return None
        return bool(self.above_sma200 and self.sma50_gt_sma200 and (self.sma50_slope_20d > 0))


@dataclass(frozen=True)
class SectorMomentum:
    sector: str
    ytd: float | None
    ret_3m: float | None
    ret_1m: float | None
    breadth_above_sma200: float | None
    leaders: list[str]


def equal_weight_sector_return(values: Iterable[float | None]) -> float | None:
    xs = [float(v) for v in values if v is not None]
    if not xs:
        return None
    return sum(xs) / float(len(xs))

