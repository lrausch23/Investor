from __future__ import annotations

import datetime as dt

from portfolio_report.returns import chain_link, modified_dietz_return, xirr


def test_modified_dietz_return_mid_period_flow():
    # Begin 100, deposit +10 mid-period, end 115.
    # r = (115 - 100 - 10) / (100 + 0.5*10) = 5 / 105
    r = modified_dietz_return(begin_value=100.0, end_value=115.0, net_external_flow=10.0, flow_weight=0.5)
    assert r is not None
    assert abs(r - (5.0 / 105.0)) < 1e-12


def test_chain_link():
    r = chain_link([0.10, -0.05])
    assert r is not None
    assert abs(r - ((1.1 * 0.95) - 1.0)) < 1e-12


def test_xirr_one_year():
    cfs = [(dt.date(2025, 1, 1), -100.0), (dt.date(2026, 1, 1), 110.0)]
    r = xirr(cfs)
    assert r is not None
    assert abs(r - 0.10) < 1e-3

