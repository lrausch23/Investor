from __future__ import annotations

import datetime as dt

from src.core.wash_sale import wash_risk_for_loss_sale
from src.db.models import Account, Security, SubstituteGroup, TaxpayerEntity, Transaction


def test_wash_sale_is_scoped_to_taxpayer(session):
    trust = TaxpayerEntity(name="Trust", type="TRUST")
    personal = TaxpayerEntity(name="Personal", type="PERSONAL")
    session.add_all([trust, personal])
    session.flush()
    a_trust = Account(name="Trust Taxable", broker="IB", account_type="TAXABLE", taxpayer_entity_id=trust.id)
    a_ira = Account(name="Chase IRA", broker="CHASE", account_type="IRA", taxpayer_entity_id=personal.id)
    session.add_all([a_trust, a_ira])
    session.flush()

    grp = SubstituteGroup(name="US Total", description="")
    session.add(grp)
    session.flush()
    session.add_all(
        [
            Security(ticker="AAA", name="AAA", asset_class="EQUITY", expense_ratio=0.0, substitute_group_id=grp.id, metadata_json={"last_price": 100}),
            Security(ticker="BBB", name="BBB", asset_class="EQUITY", expense_ratio=0.0, substitute_group_id=grp.id, metadata_json={"last_price": 100}),
        ]
    )
    session.flush()

    # BUY happens in PERSONAL IRA: should not trigger TRUST wash risk.
    session.add(Transaction(account_id=a_ira.id, date=dt.date(2025, 12, 10), type="BUY", ticker="AAA", qty=1, amount=-100, lot_links_json={}))
    session.commit()

    risk, matches = wash_risk_for_loss_sale(
        session,
        taxpayer_entity_id=trust.id,
        sale_ticker="AAA",
        sale_date=dt.date(2025, 12, 20),
        proposed_buys=[],
        window_days=30,
    )
    assert risk == "NONE"
    assert matches == []


def test_substitute_group_is_treated_as_substantially_identical(session):
    trust = TaxpayerEntity(name="Trust", type="TRUST")
    session.add(trust)
    session.flush()
    acct = Account(name="Trust Taxable", broker="IB", account_type="TAXABLE", taxpayer_entity_id=trust.id)
    session.add(acct)
    session.flush()

    grp = SubstituteGroup(name="US Total", description="")
    session.add(grp)
    session.flush()
    session.add_all(
        [
            Security(ticker="AAA", name="AAA", asset_class="EQUITY", expense_ratio=0.0, substitute_group_id=grp.id, metadata_json={"last_price": 100}),
            Security(ticker="BBB", name="BBB", asset_class="EQUITY", expense_ratio=0.0, substitute_group_id=grp.id, metadata_json={"last_price": 100}),
        ]
    )
    session.flush()

    session.add(Transaction(account_id=acct.id, date=dt.date(2025, 12, 1), type="BUY", ticker="BBB", qty=1, amount=-100, lot_links_json={}))
    session.commit()

    risk, matches = wash_risk_for_loss_sale(
        session,
        taxpayer_entity_id=trust.id,
        sale_ticker="AAA",
        sale_date=dt.date(2025, 12, 20),
        proposed_buys=[],
        window_days=30,
    )
    assert risk == "DEFINITE"
    assert len(matches) == 1

