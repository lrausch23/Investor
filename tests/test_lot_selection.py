from __future__ import annotations

import datetime as dt

from src.core.lot_selection import select_lots_tax_min
from src.core.portfolio import LotView


def test_tax_min_prefers_losses_then_lt_then_st():
    sale_date = dt.date(2025, 12, 20)
    price = 100.0
    lots = [
        LotView(id=1, account_id=1, ticker="AAA", acquisition_date=dt.date(2023, 1, 1), qty=1.0, basis_total=150.0, adjusted_basis_total=None),  # loss, LT
        LotView(id=2, account_id=1, ticker="AAA", acquisition_date=dt.date(2022, 1, 1), qty=1.0, basis_total=90.0, adjusted_basis_total=None),   # gain, LT
        LotView(id=3, account_id=1, ticker="AAA", acquisition_date=dt.date(2025, 8, 1), qty=1.0, basis_total=80.0, adjusted_basis_total=None),   # gain, ST
    ]
    picks = select_lots_tax_min(lots=lots, sell_qty=1.0, sale_price=price, sale_date=sale_date)
    assert [p.lot_id for p in picks] == [1]


def test_tax_min_skips_definite_wash_loss_lots_when_configured():
    sale_date = dt.date(2025, 12, 20)
    price = 100.0
    lots = [
        LotView(id=1, account_id=1, ticker="AAA", acquisition_date=dt.date(2023, 1, 1), qty=1.0, basis_total=150.0, adjusted_basis_total=None),  # loss
        LotView(id=2, account_id=1, ticker="AAA", acquisition_date=dt.date(2022, 1, 1), qty=1.0, basis_total=90.0, adjusted_basis_total=None),   # gain
    ]
    picks = select_lots_tax_min(
        lots=lots,
        sell_qty=1.0,
        sale_price=price,
        sale_date=sale_date,
        wash_risk_by_lot_id={1: "DEFINITE", 2: "DEFINITE"},
        avoid_definite_wash_loss_sales=True,
    )
    assert [p.lot_id for p in picks] == [2]

