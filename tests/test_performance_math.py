from __future__ import annotations

import datetime as dt

from src.core.performance import sharpe_ratio, twr_from_series, xirr


def test_xirr_simple_one_year():
    cfs = [
        (dt.date(2025, 1, 1), -100.0),
        (dt.date(2026, 1, 1), 110.0),
    ]
    r = xirr(cfs)
    assert r is not None
    assert abs(r - 0.10) < 1e-3


def test_twr_single_period_with_flow():
    # Modified Dietz (single interval):
    # Start 100, deposit +10 mid-period, end 115.
    # Numerator = 115 - 100 - 10 = 5
    # Denominator = 100 + 10 * w, where w is fraction of period remaining after the flow date.
    # Here w = (Jan31 - Jan15) / (Jan31 - Jan01) = 16/30
    # Return ≈ 5 / (100 + 10*16/30)
    vals = [(dt.date(2025, 1, 1), 100.0), (dt.date(2025, 1, 31), 115.0)]
    flows = [(dt.date(2025, 1, 15), 10.0)]
    twr, subrets, warnings = twr_from_series(values=vals, flows=flows)
    assert warnings == []
    assert len(subrets) == 1
    assert twr is not None
    expected = 5.0 / (100.0 + 10.0 * (16.0 / 30.0))
    assert abs(twr - expected) < 1e-9


def test_sharpe_ratio_monthly():
    rets = [0.01, 0.02, 0.03]
    s = sharpe_ratio(period_returns=rets, risk_free_annual=0.0, periods_per_year=12.0)
    assert s is not None
    # Mean=0.02, std(sample)=0.01 => sharpe = 2.0 * sqrt(12)
    assert abs(s - (2.0 * (12.0 ** 0.5))) < 1e-9
