from __future__ import annotations

import datetime as dt
from decimal import Decimal

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from src.db.models import Base, ExpenseAccount, ExpenseImportBatch, ExpenseTransaction
from src.investor.expenses.reports import category_summary


def _seed(
    session: Session,
    *,
    posted: dt.date,
    category: str,
    amount: Decimal,
) -> None:
    acct = session.query(ExpenseAccount).first()
    if acct is None:
        acct = ExpenseAccount(institution="AMEX", name="AMEX", last4_masked="0000", type="CREDIT")
        session.add(acct)
        session.flush()
    batch = session.query(ExpenseImportBatch).first()
    if batch is None:
        batch = ExpenseImportBatch(source="CSV", file_name="seed.csv", file_hash="x" * 64, row_count=0, duplicates_skipped=0)
        session.add(batch)
        session.flush()
    session.add(
        ExpenseTransaction(
            txn_id=f"seed_{posted.isoformat()}_{category}_{amount}",
            expense_account_id=acct.id,
            institution=acct.institution,
            account_name=acct.name,
            posted_date=posted,
            transaction_date=None,
            description_raw=category,
            description_norm=category,
            merchant_norm=category,
            amount=float(amount),
            currency="USD",
            account_last4_masked="0000",
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


def test_payments_bucketed_to_payment_column_even_if_negative() -> None:
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    session = Session(engine)
    try:
        # Simulate legacy sign issues:
        # - charge is negative
        # - payment was imported as negative (wrong sign)
        _seed(session, posted=dt.date(2025, 1, 2), category="Shopping", amount=Decimal("-100.00"))
        _seed(session, posted=dt.date(2025, 1, 3), category="Payments", amount=Decimal("-60.00"))
        session.commit()

        rep = category_summary(session=session, year=2025, month=1, account_id=None)
        by = {r.key: r for r in rep.rows}
        assert by["Shopping"].spend == Decimal("100.00")
        assert by["Shopping"].income == Decimal("0.00")

        # Payment-like categories should not contribute to Spend, regardless of sign.
        assert by["Payments"].spend == Decimal("0.00")
        assert by["Payments"].income == Decimal("60.00")
        assert by["Payments"].net == Decimal("-60.00")
    finally:
        session.close()


def test_credit_card_payment_category_not_counted_as_spend() -> None:
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    session = Session(engine)
    try:
        _seed(session, posted=dt.date(2025, 1, 2), category="Shopping", amount=Decimal("-100.00"))
        _seed(session, posted=dt.date(2025, 1, 3), category="Credit Card Payment", amount=Decimal("-60.00"))
        session.commit()

        rep = category_summary(session=session, year=2025, month=1, account_id=None)
        by = {r.key: r for r in rep.rows}
        assert by["Shopping"].spend == Decimal("100.00")
        assert by["Credit Card Payment"].spend == Decimal("0.00")
        assert by["Credit Card Payment"].income == Decimal("60.00")
    finally:
        session.close()

def test_payment_sign_flip_duplicate_counted_once() -> None:
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    session = Session(engine)
    try:
        # Same payment imported twice with opposite signs (e.g., before/after importer fix).
        _seed(session, posted=dt.date(2025, 1, 3), category="Payments", amount=Decimal("-60.00"))
        _seed(session, posted=dt.date(2025, 1, 3), category="Payments", amount=Decimal("60.00"))
        session.commit()
        rep = category_summary(session=session, year=2025, month=1, account_id=None)
        by = {r.key: r for r in rep.rows}
        assert by["Payments"].spend == Decimal("0.00")
        assert by["Payments"].income == Decimal("60.00")
        assert by["Payments"].net == Decimal("-60.00")
    finally:
        session.close()
