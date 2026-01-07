from __future__ import annotations

import datetime as dt

from portfolio_report.fifo import fifo_realized_pnl
from portfolio_report.transactions import NormalizedTransaction


def test_fifo_realized_pnl_basic():
    txs = [
        NormalizedTransaction(
            date=dt.date(2025, 1, 2),
            symbol="AAA",
            tx_type="BUY",
            qty=10.0,
            price=10.0,
            amount=-100.0,
            fees=0.0,
            account=None,
            description=None,
            cash_impact_portfolio=-100.0,
            is_external=False,
            external_cashflow_investor=None,
        ),
        NormalizedTransaction(
            date=dt.date(2025, 2, 2),
            symbol="AAA",
            tx_type="BUY",
            qty=10.0,
            price=20.0,
            amount=-200.0,
            fees=0.0,
            account=None,
            description=None,
            cash_impact_portfolio=-200.0,
            is_external=False,
            external_cashflow_investor=None,
        ),
        NormalizedTransaction(
            date=dt.date(2025, 3, 2),
            symbol="AAA",
            tx_type="SELL",
            qty=15.0,
            price=30.0,
            amount=450.0,
            fees=0.0,
            account=None,
            description=None,
            cash_impact_portfolio=450.0,
            is_external=False,
            external_cashflow_investor=None,
        ),
    ]
    matches, warnings = fifo_realized_pnl(txs, symbol="AAA")
    assert warnings == []
    assert len(matches) == 1
    m = matches[0]
    assert m.qty == 15.0
    assert abs(m.proceeds - 450.0) < 1e-12
    # Cost: 10@10 + 5@20 = 200
    assert abs(m.cost - 200.0) < 1e-12
    assert abs(m.pnl - 250.0) < 1e-12
    assert m.carry_in_basis_unknown is False


def test_fifo_infers_unit_price_from_amount_when_missing_price():
    txs = [
        NormalizedTransaction(
            date=dt.date(2025, 1, 2),
            symbol="AAA",
            tx_type="BUY",
            qty=10.0,
            price=None,
            amount=-100.0,
            fees=0.0,
            account=None,
            description=None,
            cash_impact_portfolio=-100.0,
            is_external=False,
            external_cashflow_investor=None,
        ),
        NormalizedTransaction(
            date=dt.date(2025, 2, 2),
            symbol="AAA",
            tx_type="SELL",
            qty=10.0,
            price=None,
            amount=150.0,
            fees=0.0,
            account=None,
            description=None,
            cash_impact_portfolio=150.0,
            is_external=False,
            external_cashflow_investor=None,
        ),
    ]
    matches, warnings = fifo_realized_pnl(txs, symbol="AAA")
    assert warnings == []
    assert len(matches) == 1
    assert abs(matches[0].pnl - 50.0) < 1e-12


def test_fifo_flags_carry_in_basis_unknown():
    txs = [
        NormalizedTransaction(
            date=dt.date(2025, 1, 2),
            symbol="AAA",
            tx_type="SELL",
            qty=1.0,
            price=10.0,
            amount=10.0,
            fees=0.0,
            account=None,
            description=None,
            cash_impact_portfolio=10.0,
            is_external=False,
            external_cashflow_investor=None,
        )
    ]
    matches, warnings = fifo_realized_pnl(txs, symbol="AAA")
    assert len(matches) == 1
    assert matches[0].carry_in_basis_unknown is True
    assert matches[0].cost is None
    assert matches[0].pnl is None
    assert warnings and "carry-in basis unknown" in warnings[0].lower()
