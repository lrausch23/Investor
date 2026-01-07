from __future__ import annotations

import datetime as dt
from decimal import Decimal

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from src.db.models import Base, ExpenseAccount, ExpenseImportBatch, ExpenseTransaction
from src.investor.expenses.reports import category_summary, merchants_by_spend


def _seed(
    session: Session,
    *,
    account: ExpenseAccount,
    posted: dt.date,
    merchant: str,
    amount: Decimal,
    category: str,
) -> None:
    batch = session.query(ExpenseImportBatch).first()
    if batch is None:
        batch = ExpenseImportBatch(source="CSV", file_name="seed.csv", file_hash="x" * 64, row_count=0, duplicates_skipped=0)
        session.add(batch)
        session.flush()
    session.add(
        ExpenseTransaction(
            txn_id=f"seed_{account.id}_{posted.isoformat()}_{merchant}_{amount}_{category}",
            expense_account_id=account.id,
            institution=account.institution,
            account_name=account.name,
            posted_date=posted,
            transaction_date=None,
            description_raw=merchant,
            description_norm=merchant,
            merchant_norm=merchant,
            amount=float(amount),
            currency="USD",
            account_last4_masked=account.last4_masked,
            cardholder_name=None,
            category_hint=None,
            category_user=None,
            category_system=category,
            tags_json=[],
            notes=None,
            import_batch_id=batch.id,
            original_row_json=None,
        )
    )
    session.flush()


def test_reports_account_filter_limits_rows() -> None:
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    session = Session(engine)
    try:
        a1 = ExpenseAccount(institution="Bank", name="Checking", last4_masked="1111", type="BANK")
        a2 = ExpenseAccount(institution="Card", name="Visa", last4_masked="2222", type="CREDIT")
        session.add_all([a1, a2])
        session.flush()
        _seed(session, account=a1, posted=dt.date(2025, 12, 1), merchant="Amazon", amount=Decimal("-10.00"), category="Shopping")
        _seed(session, account=a2, posted=dt.date(2025, 12, 1), merchant="Amazon", amount=Decimal("-90.00"), category="Shopping")
        session.commit()

        all_cat = category_summary(session=session, year=2025, month=12, account_id=None)
        a2_cat = category_summary(session=session, year=2025, month=12, account_id=a2.id)
        assert sum(float(r.spend) for r in all_cat.rows) == 100.0
        assert sum(float(r.spend) for r in a2_cat.rows) == 90.0

        all_merch = merchants_by_spend(session=session, year=2025, month=12, limit=10, account_id=None)
        a1_merch = merchants_by_spend(session=session, year=2025, month=12, limit=10, account_id=a1.id)
        assert all_merch[0].spend == Decimal("100.00")
        assert a1_merch[0].spend == Decimal("10.00")
    finally:
        session.close()

