import datetime as dt

import pandas as pd

from src.investor.momentum.calcs import (
    StockMomentum,
    avg_dollar_vol,
    dist_to_52w_high_pct,
    equal_weight_sector_return,
    pct_return_from_lookback,
    sma,
    sma_slope,
    ytd_return,
)


def _series(start: dt.date, n: int, *, start_px: float = 100.0, step: float = 1.0) -> pd.Series:
    dates = [start + dt.timedelta(days=i) for i in range(n)]
    # Use business-like dates for deterministic tests (no missing).
    idx = pd.to_datetime(dates)
    vals = [start_px + step * i for i in range(n)]
    return pd.Series(vals, index=idx, dtype=float)


def test_ytd_return_uses_prior_year_last_trading_day() -> None:
    s = pd.Series(
        [100.0, 110.0, 120.0],
        index=pd.to_datetime([dt.date(2024, 12, 31), dt.date(2025, 1, 2), dt.date(2025, 1, 3)]),
        dtype=float,
    )
    r = ytd_return(s, as_of=dt.date(2025, 1, 3))
    assert r is not None
    assert abs(r - (120.0 / 100.0 - 1.0)) < 1e-12


def test_pct_return_lookback_trading_observations() -> None:
    s = _series(dt.date(2025, 1, 1), 70, start_px=100.0, step=1.0)
    # 21 lookback compares last vs 22nd from end.
    r1m = pct_return_from_lookback(s, 21)
    assert r1m is not None
    end = float(s.iloc[-1])
    base = float(s.iloc[-22])
    assert abs(r1m - (end / base - 1.0)) < 1e-12


def test_sma_and_slope() -> None:
    s = _series(dt.date(2025, 1, 1), 120, start_px=100.0, step=1.0)
    sma50 = sma(s, 50)
    assert sma50.dropna().shape[0] >= 1
    # Slope should be positive for increasing series.
    slope = sma_slope(s, window=50, slope_window=20)
    assert slope is not None
    assert slope > 0


def test_dist_to_52w_high() -> None:
    s = pd.Series(
        [100.0, 120.0, 110.0],
        index=pd.to_datetime([dt.date(2025, 1, 1), dt.date(2025, 1, 2), dt.date(2025, 1, 3)]),
        dtype=float,
    )
    d = dist_to_52w_high_pct(s, window=252)
    assert d is not None
    assert abs(d - (110.0 / 120.0 - 1.0)) < 1e-12


def test_avg_dollar_vol() -> None:
    close = _series(dt.date(2025, 1, 1), 30, start_px=10.0, step=0.0)
    vol = pd.Series([100.0] * 30, index=close.index, dtype=float)
    dv = avg_dollar_vol(close, vol, window=20)
    assert dv is not None
    assert abs(dv - 1000.0) < 1e-12


def test_uptrend_rule_property() -> None:
    r = StockMomentum(
        ticker="AAA",
        sector="Tech",
        ytd=0.10,
        ret_3m=0.05,
        ret_1m=0.02,
        close=110.0,
        above_sma200=True,
        sma50_gt_sma200=True,
        sma50_slope_20d=0.01,
        dist_52w_high=-0.05,
        avg_dvol_20d=1_000_000.0,
    )
    assert r.uptrend is True
    r2 = StockMomentum(
        ticker="BBB",
        sector="Tech",
        ytd=0.10,
        ret_3m=0.05,
        ret_1m=0.02,
        close=110.0,
        above_sma200=True,
        sma50_gt_sma200=True,
        sma50_slope_20d=-0.01,
        dist_52w_high=-0.05,
        avg_dvol_20d=1_000_000.0,
    )
    assert r2.uptrend is False


def test_equal_weight_sector_return() -> None:
    assert equal_weight_sector_return([0.10, 0.00]) == 0.05
    assert equal_weight_sector_return([None, None]) is None
