from __future__ import annotations

import datetime as dt
from decimal import Decimal

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from src.db.models import Base, ExpenseAccount, ExpenseImportBatch, ExpenseTransaction, RecurringBill, RecurringBillRule
from src.investor.cash_bills.recurring import (
    _normalize_name,
    active_bills_summary,
    detect_suggestions,
    recurring_due_total,
)


def _seed_account(session: Session) -> ExpenseAccount:
    acct = ExpenseAccount(institution="TestBank", name="Checking", last4_masked="0000", type="CHECKING", scope="PERSONAL")
    session.add(acct)
    session.flush()
    return acct


def _seed_batch(session: Session) -> ExpenseImportBatch:
    batch = ExpenseImportBatch(source="CSV", file_name="seed.csv", file_hash="x" * 64, row_count=0, duplicates_skipped=0)
    session.add(batch)
    session.flush()
    return batch


def _seed_txn(
    session: Session,
    *,
    acct: ExpenseAccount,
    batch: ExpenseImportBatch,
    posted: dt.date,
    merchant: str,
    desc: str,
    amount: Decimal,
    raw: dict[str, object] | None = None,
) -> None:
    session.add(
        ExpenseTransaction(
            txn_id=f"seed_{posted.isoformat()}_{merchant}_{amount}",
            expense_account_id=acct.id,
            institution=acct.institution,
            account_name=acct.name,
            posted_date=posted,
            transaction_date=None,
            description_raw=desc,
            description_norm=desc,
            merchant_norm=merchant,
            amount=float(amount),
            currency="USD",
            category_user=None,
            category_system=None,
            tags_json=[],
            notes=None,
            import_batch_id=batch.id,
            original_row_json=raw or {},
        )
    )
    session.flush()


def test_detect_suggestions_fixed_monthly() -> None:
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    session = Session(engine)
    try:
        acct = _seed_account(session)
        batch = _seed_batch(session)
        _seed_txn(
            session,
            acct=acct,
            batch=batch,
            posted=dt.date(2025, 10, 5),
            merchant="Netflix",
            desc="NETFLIX.COM",
            amount=Decimal("-15.99"),
        )
        _seed_txn(
            session,
            acct=acct,
            batch=batch,
            posted=dt.date(2025, 11, 5),
            merchant="Netflix",
            desc="NETFLIX.COM",
            amount=Decimal("-15.99"),
        )
        _seed_txn(
            session,
            acct=acct,
            batch=batch,
            posted=dt.date(2025, 12, 5),
            merchant="Netflix",
            desc="NETFLIX.COM",
            amount=Decimal("-15.99"),
        )
        session.commit()
        suggestions = detect_suggestions(session=session, scope="PERSONAL", as_of=dt.date(2025, 12, 31))
        assert suggestions
        item = suggestions[0]
        assert item["amount_mode"] == "FIXED"
        assert item["due_day_of_month"] == 5
    finally:
        session.close()


def test_active_bill_paid_status() -> None:
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    session = Session(engine)
    try:
        acct = _seed_account(session)
        batch = _seed_batch(session)
        _seed_txn(
            session,
            acct=acct,
            batch=batch,
            posted=dt.date(2026, 1, 4),
            merchant="Netflix",
            desc="Netflix",
            amount=Decimal("-15.99"),
        )
        bill = RecurringBill(
            scope="PERSONAL",
            name="Netflix",
            source_account_id=acct.id,
            cadence="MONTHLY",
            amount_mode="FIXED",
            amount_expected=15.99,
            due_day_of_month=5,
            is_active=True,
            is_user_confirmed=True,
        )
        session.add(bill)
        session.flush()
        session.add(
            RecurringBillRule(
                recurring_bill_id=bill.id,
                rule_type="NAME_NORMALIZED",
                rule_value=_normalize_name("Netflix"),
                priority=0,
            )
        )
        session.commit()
        summary = active_bills_summary(session=session, scope="PERSONAL", as_of=dt.date(2026, 1, 10))
        bills = summary["bills"]
        assert bills
        assert bills[0]["status"] == "paid"
    finally:
        session.close()


def test_recurring_due_total() -> None:
    as_of = dt.date(2026, 1, 10)
    bills = [
        {
            "amount_mode": "FIXED",
            "amount_expected": 20.0,
            "due_date": "2026-01-15",
            "status": "upcoming",
        },
        {
            "amount_mode": "FIXED",
            "amount_expected": 30.0,
            "due_date": "2026-02-15",
            "status": "upcoming",
        },
        {
            "amount_mode": "FIXED",
            "amount_expected": 40.0,
            "due_date": "2026-01-08",
            "status": "paid",
        },
    ]
    total = recurring_due_total(bills, as_of=as_of, range_days=30)
    assert total == 20.0
