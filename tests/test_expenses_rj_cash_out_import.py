from __future__ import annotations

import datetime as dt

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from src.db.models import Account, Base, ExpenseTransaction, TaxpayerEntity, Transaction
from src.investor.expenses.rj_cash_out import import_rj_cash_outs


def test_import_rj_cash_outs_creates_expense_account_and_dedupes() -> None:
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    session = Session(engine)
    try:
        te = TaxpayerEntity(name="Kolozsi Trust", type="TRUST", tax_id_last4=None, notes=None)
        session.add(te)
        session.flush()
        rj = Account(name="RJ Kolozsi Trust", broker="RJ", account_type="TAXABLE", taxpayer_entity_id=te.id)
        session.add(rj)
        session.flush()

        # Simulate RJ rows:
        # - vendor wire should be imported with merchant details
        # - internal FX transfer leg should be skipped
        session.add_all(
            [
                Transaction(
                    account_id=rj.id,
                    date=dt.date(2025, 8, 1),
                    type="TRANSFER",
                    ticker="EUR",
                    qty=0,
                    amount=-651.74,
                    lot_links_json={
                        "provider_txn_id": "RJ:HASH:abc",
                        "description": "Euro",
                        "additional_detail": "WIRE TO North Sails d.o.o. €(555.00)",
                        "source_file": "RJFPostedActivity.csv",
                        "source_row": 26,
                    },
                ),
                Transaction(
                    account_id=rj.id,
                    date=dt.date(2025, 8, 1),
                    type="TRANSFER",
                    ticker="UNKNOWN",
                    qty=0,
                    amount=-650.33,
                    lot_links_json={
                        "provider_txn_id": "RJ:HASH:def",
                        "description": "Cash",
                        "additional_detail": "TRSF TO SHADO ACCT FOR FX TRAD",
                    },
                ),
                Transaction(account_id=rj.id, date=dt.date(2025, 8, 2), type="TRANSFER", ticker="UNKNOWN", qty=0, amount=500, lot_links_json={}),  # not cash out
            ]
        )
        session.commit()

        res = import_rj_cash_outs(session=session, rj_account_id=rj.id, expense_account_name="Kolozsi Trust")
        assert res.row_count == 1
        assert res.inserted == 1
        tx = session.query(ExpenseTransaction).one()
        assert tx.amount < 0
        assert tx.merchant_norm == "North Sails d.o.o."
        assert (tx.category_system or "Unknown") == "Unknown"

        # Re-import is deduped by txn_id.
        res2 = import_rj_cash_outs(session=session, rj_account_id=rj.id, expense_account_name="Kolozsi Trust")
        assert res2.inserted == 0
        assert session.query(ExpenseTransaction).count() == 1
    finally:
        session.close()
