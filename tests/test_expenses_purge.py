from __future__ import annotations

import datetime as dt

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from src.db.models import Base, ExpenseAccount, ExpenseImportBatch, ExpenseTransaction
from src.investor.expenses.purge import purge_account_data, purge_all_expenses_data


def test_purge_account_deletes_only_that_account() -> None:
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    session = Session(engine)
    try:
        a1 = ExpenseAccount(institution="Chase", name="Amazon", last4_masked="0000", type="CREDIT")
        a2 = ExpenseAccount(institution="AMEX", name="AMEX", last4_masked="3026", type="CREDIT")
        b = ExpenseImportBatch(source="CSV", file_name="x.csv", file_hash="x" * 64, row_count=2, duplicates_skipped=0, metadata_json={})
        session.add_all([a1, a2, b])
        session.flush()
        session.add_all(
            [
                ExpenseTransaction(
                    txn_id="t1",
                    expense_account_id=a1.id,
                    institution=a1.institution,
                    account_name=a1.name,
                    posted_date=dt.date(2025, 1, 1),
                    transaction_date=None,
                    description_raw="x",
                    description_norm="x",
                    merchant_norm="M",
                    amount=-1.0,
                    currency="USD",
                    tags_json=[],
                    import_batch_id=b.id,
                ),
                ExpenseTransaction(
                    txn_id="t2",
                    expense_account_id=a2.id,
                    institution=a2.institution,
                    account_name=a2.name,
                    posted_date=dt.date(2025, 1, 1),
                    transaction_date=None,
                    description_raw="y",
                    description_norm="y",
                    merchant_norm="N",
                    amount=-2.0,
                    currency="USD",
                    tags_json=[],
                    import_batch_id=b.id,
                ),
            ]
        )
        session.commit()

        res = purge_account_data(session=session, account_id=a1.id)
        session.commit()
        assert res["transactions_deleted"] == 1
        assert session.query(ExpenseAccount).count() == 1
        assert session.query(ExpenseTransaction).count() == 1
    finally:
        session.close()


def test_purge_all_deletes_everything() -> None:
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    session = Session(engine)
    try:
        a1 = ExpenseAccount(institution="Chase", name="Amazon", last4_masked="0000", type="CREDIT")
        b = ExpenseImportBatch(source="CSV", file_name="x.csv", file_hash="x" * 64, row_count=1, duplicates_skipped=0, metadata_json={})
        session.add_all([a1, b])
        session.flush()
        session.add(
            ExpenseTransaction(
                txn_id="t1",
                expense_account_id=a1.id,
                institution=a1.institution,
                account_name=a1.name,
                posted_date=dt.date(2025, 1, 1),
                transaction_date=None,
                description_raw="x",
                description_norm="x",
                merchant_norm="M",
                amount=-1.0,
                currency="USD",
                tags_json=[],
                import_batch_id=b.id,
            )
        )
        session.commit()

        res = purge_all_expenses_data(session=session, include_rules=False, include_categories=False)
        session.commit()
        assert res["transactions_deleted"] == 1
        assert session.query(ExpenseTransaction).count() == 0
        assert session.query(ExpenseAccount).count() == 0
        assert session.query(ExpenseImportBatch).count() == 0
    finally:
        session.close()
