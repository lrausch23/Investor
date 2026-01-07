from __future__ import annotations

import datetime as dt
from decimal import Decimal

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from src.db.models import Base, ExpenseAccount, ExpenseImportBatch, ExpenseTransaction
from src.investor.expenses.reports import cardholders_by_spend


def _seed(
    session: Session,
    *,
    posted: dt.date,
    cardholder: str | None,
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
            txn_id=f"seed_{posted.isoformat()}_{cardholder}_{amount}_{category}",
            expense_account_id=acct.id,
            institution=acct.institution,
            account_name=acct.name,
            posted_date=posted,
            transaction_date=None,
            description_raw="x",
            description_norm="x",
            merchant_norm="M",
            amount=float(amount),
            currency="USD",
            account_last4_masked="1234",
            cardholder_name=cardholder,
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


def test_cardholders_by_spend_groups_unknown() -> None:
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    session = Session(engine)
    try:
        _seed(session, posted=dt.date(2025, 12, 1), cardholder="Alice", amount=Decimal("-20.00"), category="Shopping")
        _seed(session, posted=dt.date(2025, 12, 2), cardholder="Alice", amount=Decimal("-10.00"), category="Dining")
        _seed(session, posted=dt.date(2025, 12, 3), cardholder=None, amount=Decimal("-5.00"), category="Shopping")
        _seed(session, posted=dt.date(2025, 12, 4), cardholder="Alice", amount=Decimal("100.00"), category="Payments")  # excluded
        session.commit()
        rows = cardholders_by_spend(session=session, year=2025, month=12, limit=10)
        assert [r.cardholder for r in rows] == ["Alice", "Unknown"]
        assert rows[0].spend == Decimal("30.00")
        assert rows[0].txn_count == 2
    finally:
        session.close()


def test_cardholders_by_spend_groups_case_insensitively() -> None:
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    session = Session(engine)
    try:
        _seed(session, posted=dt.date(2025, 1, 1), cardholder="Laszlo Rausch", amount=Decimal("-20.00"), category="Shopping")
        _seed(session, posted=dt.date(2025, 1, 2), cardholder="LASZLO RAUSCH", amount=Decimal("-10.00"), category="Shopping")
        session.commit()
        rows = cardholders_by_spend(session=session, year=2025, month=1, limit=10)
        assert [r.cardholder for r in rows] == ["Laszlo Rausch"]
        assert rows[0].spend == Decimal("30.00")
        assert rows[0].txn_count == 2
    finally:
        session.close()
