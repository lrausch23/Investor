from __future__ import annotations

import datetime as dt

from src.app.routes.reports import _is_internal_transfer_expr
from src.db.models import Account, TaxpayerEntity, Transaction


def test_withdrawals_report_excludes_internal_shado_transfer(session):
    tp = TaxpayerEntity(name="Trust", type="TRUST")
    session.add(tp)
    session.flush()
    acct = Account(name="RJ Taxable", broker="RJ", taxpayer_entity_id=tp.id, account_type="TAXABLE")
    session.add(acct)
    session.flush()

    # Internal transfer (SHADO) should be excluded.
    t1 = Transaction(
        account_id=acct.id,
        date=dt.date(2025, 10, 3),
        type="TRANSFER",
        ticker=None,
        qty=None,
        amount=-650.33,
        lot_links_json={"description": "Cash", "additional_detail": "TRSF TO SHADO ACCT FOR FX TRAD"},
    )
    # External wire should remain.
    t2 = Transaction(
        account_id=acct.id,
        date=dt.date(2025, 10, 3),
        type="TRANSFER",
        ticker=None,
        qty=None,
        amount=-651.74,
        lot_links_json={"description": "Euro", "additional_detail": "WIRE TO Vendor"},
    )
    session.add_all([t1, t2])
    session.commit()

    kept = (
        session.query(Transaction)
        .filter(Transaction.type == "TRANSFER", Transaction.amount < 0)
        .filter(~_is_internal_transfer_expr())
        .all()
    )
    assert len(kept) == 1
    assert float(kept[0].amount) == -651.74

