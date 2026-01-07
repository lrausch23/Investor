from __future__ import annotations

import datetime as dt

from src.core.performance import _bench_series_for_period


def test_benchmark_month_end_does_not_synthesize_non_trading_start_date():
    # Missing 2025-01-01; no prior date exists; first available is 2025-01-02.
    series = [
        (dt.date(2025, 1, 2), 100.0),
        (dt.date(2025, 1, 31), 110.0),
        (dt.date(2025, 12, 31), 120.0),
    ]
    pts = _bench_series_for_period(series, start_date=dt.date(2025, 1, 1), end_date=dt.date(2025, 12, 31), frequency="month_end")
    assert pts[0][0] == dt.date(2025, 1, 2)
    assert pts[-1][0] == dt.date(2025, 12, 31)

def test_benchmark_month_end_prefers_prior_trading_day_anchor_when_available():
    # When 12/31 is available, use it as the period-start anchor for calendar-year style returns.
    series = [
        (dt.date(2024, 12, 31), 99.0),
        (dt.date(2025, 1, 2), 100.0),
        (dt.date(2025, 1, 31), 110.0),
        (dt.date(2025, 12, 31), 120.0),
    ]
    pts = _bench_series_for_period(series, start_date=dt.date(2025, 1, 1), end_date=dt.date(2025, 12, 31), frequency="month_end")
    assert pts[0][0] == dt.date(2024, 12, 31)
    assert pts[-1][0] == dt.date(2025, 12, 31)
