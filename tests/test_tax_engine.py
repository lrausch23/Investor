from __future__ import annotations

from src.core.tax_engine import TaxAssumptions, estimate_tax_delta


def test_estimate_tax_delta_basic():
    a = TaxAssumptions(ordinary_rate=0.4, ltcg_rate=0.2, state_rate=0.0, niit_enabled=False, niit_rate=0.0, qualified_dividend_pct=0.0)
    tax = estimate_tax_delta(
        st_gains=100.0,
        lt_gains=200.0,
        ordinary_income=0.0,
        qualified_dividends=0.0,
        nonqualified_dividends=0.0,
        interest=0.0,
        assumptions=a,
    )
    assert tax == 0.4 * 100.0 + 0.2 * 200.0

