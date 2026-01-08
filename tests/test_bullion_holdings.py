from __future__ import annotations

import datetime as dt

from src.core.external_holdings import build_holdings_view
from src.db.models import Account, BullionHolding, TaxpayerEntity


def test_bullion_holdings_appear_in_holdings_view_with_manual_price(session):
    trust = TaxpayerEntity(name="Trust", type="TRUST")
    session.add(trust)
    session.flush()

    acct = Account(name="Bullion", broker="MANUAL", account_type="OTHER", taxpayer_entity_id=trust.id)
    session.add(acct)
    session.flush()

    session.add_all(
        [
            BullionHolding(
                account_id=acct.id,
                metal="GOLD",
                quantity=2.5,
                unit="oz",
                unit_price=2100.0,
                cost_basis_total=4800.0,
                currency="USD",
                as_of_date=dt.date(2026, 1, 7),
            ),
            BullionHolding(
                account_id=acct.id,
                metal="SILVER",
                quantity=100.0,
                unit="oz",
                unit_price=24.5,
                currency="USD",
                as_of_date=dt.date(2026, 1, 7),
            ),
        ]
    )
    session.commit()

    view = build_holdings_view(session, scope="trust", account_id=acct.id, today=dt.date(2026, 1, 8))
    syms = {p.symbol for p in view.positions}
    assert {"BULLION:GOLD", "BULLION:SILVER"}.issubset(syms)

    gold = next(p for p in view.positions if p.symbol == "BULLION:GOLD")
    assert float(gold.qty or 0.0) == 2.5
    assert float(gold.latest_price or 0.0) == 2100.0
    assert gold.latest_price_as_of == dt.date(2026, 1, 7)
    assert float(gold.market_value or 0.0) == 5250.0
    assert float(gold.cost_basis_total or 0.0) == 4800.0
    assert float(gold.pnl_amount or 0.0) == 450.0

    silver = next(p for p in view.positions if p.symbol == "BULLION:SILVER")
    assert float(silver.qty or 0.0) == 100.0
    assert float(silver.latest_price or 0.0) == 24.5
    assert silver.latest_price_as_of == dt.date(2026, 1, 7)
    assert float(silver.market_value or 0.0) == 2450.0

    assert float(view.cash_total) == 0.0
    assert float(view.total_market_value) == 7700.0
    assert float(view.total_value) == 7700.0
