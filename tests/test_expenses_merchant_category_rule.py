from __future__ import annotations

import datetime as dt

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from src.db.models import Base, ExpenseAccount, ExpenseImportBatch, ExpenseTransaction
from src.investor.expenses.merchant_category import set_merchant_category


def test_set_merchant_category_updates_existing_charges() -> None:
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    session = Session(engine)
    try:
        acct = ExpenseAccount(institution="Test", name="Card", last4_masked="1234", type="CREDIT")
        batch = ExpenseImportBatch(source="CSV", file_name="seed.csv", file_hash="x" * 64, row_count=0, duplicates_skipped=0, metadata_json={})
        session.add_all([acct, batch])
        session.flush()
        session.add_all(
            [
                ExpenseTransaction(
                    txn_id="t1",
                    expense_account_id=acct.id,
                    institution=acct.institution,
                    account_name=acct.name,
                    posted_date=dt.date(2025, 1, 1),
                    transaction_date=None,
                    description_raw="x",
                    description_norm="x",
                    merchant_norm="MCDONALD'S",
                    amount=-5.0,
                    currency="USD",
                    tags_json=[],
                    import_batch_id=batch.id,
                    category_system="Unknown",
                ),
                ExpenseTransaction(
                    txn_id="t1b",
                    expense_account_id=acct.id,
                    institution=acct.institution,
                    account_name=acct.name,
                    posted_date=dt.date(2025, 1, 3),
                    transaction_date=None,
                    description_raw="z",
                    description_norm="z",
                    merchant_norm="Mcdonald's",
                    amount=-6.0,
                    currency="USD",
                    tags_json=[],
                    import_batch_id=batch.id,
                    category_system="Unknown",
                ),
                ExpenseTransaction(
                    txn_id="t2",
                    expense_account_id=acct.id,
                    institution=acct.institution,
                    account_name=acct.name,
                    posted_date=dt.date(2025, 1, 2),
                    transaction_date=None,
                    description_raw="y",
                    description_norm="y",
                    merchant_norm="MCDONALD'S",
                    amount=2.0,  # credit/refund; shouldn't be recategorized by merchant charge rule
                    currency="USD",
                    tags_json=[],
                    import_batch_id=batch.id,
                    category_system="Merchant Credits",
                ),
            ]
        )
        session.commit()

        updated = set_merchant_category(session=session, merchant="MCDONALD'S", category="Dining")
        session.commit()
        assert updated == 2

        t1 = session.query(ExpenseTransaction).filter(ExpenseTransaction.txn_id == "t1").one()
        t1b = session.query(ExpenseTransaction).filter(ExpenseTransaction.txn_id == "t1b").one()
        t2 = session.query(ExpenseTransaction).filter(ExpenseTransaction.txn_id == "t2").one()
        assert t1.category_system == "Dining"
        assert t1b.category_system == "Dining"
        assert t2.category_system == "Merchant Credits"
    finally:
        session.close()
