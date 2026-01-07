from __future__ import annotations

import datetime as dt
from decimal import Decimal

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from src.db.models import Base, ExpenseAccount, ExpenseImportBatch, ExpenseTransaction
from src.investor.expenses.reports import merchants_by_spend


def _seed(
    session: Session,
    *,
    posted: dt.date,
    merchant: str,
    amount: Decimal,
    category: str,
) -> None:
    acct = session.query(ExpenseAccount).first()
    if acct is None:
        acct = ExpenseAccount(institution="Test", name="Card", last4_masked="1234", type="CREDIT")
        session.add(acct)
        session.flush()
    batch = session.query(ExpenseImportBatch).first()
    if batch is None:
        batch = ExpenseImportBatch(source="CSV", file_name="seed.csv", file_hash="x" * 64, row_count=0, duplicates_skipped=0)
        session.add(batch)
        session.flush()
    session.add(
        ExpenseTransaction(
            txn_id=f"seed_{posted.isoformat()}_{merchant}_{amount}_{category}",
            expense_account_id=acct.id,
            institution=acct.institution,
            account_name=acct.name,
            posted_date=posted,
            transaction_date=None,
            description_raw=merchant,
            description_norm=merchant,
            merchant_norm=merchant,
            amount=float(amount),
            currency="USD",
            account_last4_masked="1234",
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


def test_merchants_by_spend_excludes_payments_and_credits() -> None:
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    session = Session(engine)
    try:
        # Charges
        _seed(session, posted=dt.date(2025, 12, 1), merchant="Amazon", amount=Decimal("-50.00"), category="Shopping")
        _seed(session, posted=dt.date(2025, 12, 2), merchant="Amazon", amount=Decimal("-25.00"), category="Shopping")
        _seed(session, posted=dt.date(2025, 12, 3), merchant="Uber", amount=Decimal("-10.00"), category="Travel")
        # Excluded categories
        _seed(session, posted=dt.date(2025, 12, 4), merchant="AUTOPAY PAYMENT", amount=Decimal("300.00"), category="Payments")
        _seed(session, posted=dt.date(2025, 12, 5), merchant="MERCHANT CREDIT", amount=Decimal("5.00"), category="Merchant Credits")
        session.commit()

        rows = merchants_by_spend(session=session, year=2025, month=12, limit=10)
        assert [r.merchant for r in rows] == ["Amazon", "Uber"]
        assert rows[0].spend == Decimal("75.00")
        assert rows[0].txn_count == 2
    finally:
        session.close()


def test_merchants_by_spend_includes_dominant_category() -> None:
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    session = Session(engine)
    try:
        _seed(session, posted=dt.date(2025, 12, 1), merchant="Amazon", amount=Decimal("-20.00"), category="Shopping")
        _seed(session, posted=dt.date(2025, 12, 2), merchant="Amazon", amount=Decimal("-10.00"), category="Subscriptions")
        session.commit()
        rows = merchants_by_spend(session=session, year=2025, month=12, limit=10)
        amazon = next(r for r in rows if r.merchant == "Amazon")
        assert amazon.category == "Shopping"
    finally:
        session.close()


def test_merchants_by_spend_groups_case_insensitively() -> None:
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    session = Session(engine)
    try:
        _seed(session, posted=dt.date(2025, 12, 1), merchant="MARINA DALMACIJA", amount=Decimal("-20.00"), category="Dining")
        _seed(session, posted=dt.date(2025, 12, 2), merchant="Marina Dalmacija", amount=Decimal("-10.00"), category="Dining")
        session.commit()
        rows = merchants_by_spend(session=session, year=2025, month=12, limit=10)
        assert len(rows) == 1
        assert rows[0].spend == Decimal("30.00")
        assert rows[0].txn_count == 2
    finally:
        session.close()
