from __future__ import annotations

import datetime as dt

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from src.db.models import Base, ExpenseAccount, ExpenseImportBatch, ExpenseTransaction
from src.investor.expenses.categorize import apply_rules_to_db
from src.investor.expenses.config import CategorizationConfig
from src.investor.expenses.learning import learn_unknown_merchant_category


def _seed_min_txn(
    session: Session,
    *,
    merchant: str,
    category_system: str | None = None,
) -> ExpenseTransaction:
    acct = ExpenseAccount(institution="AMEX", name="AMEX", last4_masked="3026", type="CREDIT")
    batch = ExpenseImportBatch(source="CSV", file_name="x.csv", file_hash="0" * 64, row_count=1, duplicates_skipped=0, metadata_json={})
    session.add_all([acct, batch])
    session.flush()
    # txn_id must be unique; keep deterministic per call within a session.
    uniq = f"{merchant}-{category_system or ''}-{len(session.identity_map)}"
    t = ExpenseTransaction(
        txn_id=f"txn-{uniq}",
        expense_account_id=acct.id,
        institution="AMEX",
        account_name="AMEX",
        posted_date=dt.date(2025, 12, 31),
        transaction_date=dt.date(2025, 12, 31),
        description_raw=f"{merchant} test",
        description_norm=f"{merchant} test",
        merchant_norm=merchant,
        amount=-12.34,
        currency="USD",
        account_last4_masked="3026",
        cardholder_name="Test User",
        category_hint=None,
        category_user=None,
        category_system=category_system,
        tags_json=[],
        notes=None,
        import_batch_id=batch.id,
        original_row_json=None,
    )
    session.add(t)
    session.flush()
    return t


def test_learn_unknown_merchant_category_updates_existing_and_persists_rule(tmp_path) -> None:
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    session = Session(engine)
    try:
        # Two Unknown HBO charges + one Unknown Netflix charge.
        _seed_min_txn(session, merchant="HBO", category_system="Unknown")
        _seed_min_txn(session, merchant="HBO", category_system=None)
        _seed_min_txn(session, merchant="Netflix", category_system="Unknown")
        session.commit()

        updated = learn_unknown_merchant_category(session=session, merchant_norm="HBO", category="Subscriptions")
        session.commit()
        assert updated == 2

        hbo = (
            session.query(ExpenseTransaction)
            .filter(ExpenseTransaction.merchant_norm == "HBO")
            .order_by(ExpenseTransaction.id.asc())
            .all()
        )
        assert all((t.category_system or "").strip() == "Subscriptions" for t in hbo)
        netflix = session.query(ExpenseTransaction).filter(ExpenseTransaction.merchant_norm == "Netflix").one()
        assert (netflix.category_system or "Unknown") == "Unknown"

        # Future categorization runs should pick up the learned DB rule.
        for t in session.query(ExpenseTransaction).all():
            t.category_system = None
        session.commit()

        rules_path = tmp_path / "rules.yaml"
        rules_path.write_text(
            "\n".join(
                [
                    "version: 1",
                    "categories: []",
                    "transfer_keywords: []",
                    "income_keywords: []",
                    "rules: []",
                    "",
                ]
            )
        )
        apply_rules_to_db(session=session, rules_path=rules_path, config=CategorizationConfig(), rebuild=True)

        hbo2 = (
            session.query(ExpenseTransaction)
            .filter(ExpenseTransaction.merchant_norm == "HBO")
            .order_by(ExpenseTransaction.id.asc())
            .all()
        )
        assert all((t.category_system or "").strip() == "Subscriptions" for t in hbo2)
    finally:
        session.close()


def test_learn_unknown_merchant_category_handles_small_merchant_variants() -> None:
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    session = Session(engine)
    try:
        _seed_min_txn(session, merchant="PP*HAPPY POUNDS LLC FLAGLER BEACH FL", category_system="Unknown")
        _seed_min_txn(session, merchant="TST* HAPPY POUNDS LL FLAGLER BEACH FL", category_system="Unknown")
        session.commit()

        updated = learn_unknown_merchant_category(
            session=session,
            merchant_norm="PP*HAPPY POUNDS LLC FLAGLER BEACH FL",
            category="Dining",
        )
        session.commit()
        assert updated == 2
        rows = session.query(ExpenseTransaction).order_by(ExpenseTransaction.id.asc()).all()
        assert all((t.category_system or "").strip() == "Dining" for t in rows)
    finally:
        session.close()


def test_learning_clears_unknown_user_override_so_system_category_applies() -> None:
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    session = Session(engine)
    try:
        t1 = _seed_min_txn(session, merchant="HBO", category_system="Unknown")
        t2 = _seed_min_txn(session, merchant="HBO", category_system="Unknown")
        t2.category_user = "Unknown"  # user previously set/kept as Unknown
        session.commit()

        updated = learn_unknown_merchant_category(session=session, merchant_norm="HBO", category="Subscriptions")
        session.commit()
        assert updated == 2

        rows = session.query(ExpenseTransaction).order_by(ExpenseTransaction.id.asc()).all()
        assert all((t.category_system or "").strip() == "Subscriptions" for t in rows)
        assert rows[1].category_user is None
    finally:
        session.close()


def test_learning_updates_rows_with_user_unknown_even_if_system_is_not_unknown() -> None:
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    session = Session(engine)
    try:
        _seed_min_txn(session, merchant="HBO", category_system="Unknown")
        t2 = _seed_min_txn(session, merchant="HBO", category_system="Transfers")
        t2.category_user = "Unknown"
        session.commit()

        updated = learn_unknown_merchant_category(
            session=session,
            merchant_norm="HBO",
            category="Subscriptions",
            from_category="Unknown",
        )
        session.commit()
        assert updated == 2

        rows = session.query(ExpenseTransaction).order_by(ExpenseTransaction.id.asc()).all()
        assert all((t.category_system or "").strip() == "Subscriptions" for t in rows)
        assert rows[1].category_user is None
    finally:
        session.close()


def test_learning_updates_rows_even_if_system_category_differs() -> None:
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    session = Session(engine)
    try:
        _seed_min_txn(session, merchant="Marina Dalmacija", category_system="Shopping")
        _seed_min_txn(session, merchant="MARINA DALMACIJA", category_system="Entertainment")
        session.commit()

        updated = learn_unknown_merchant_category(session=session, merchant_norm="MARINA DALMACIJA", category="ZP Rose")
        session.commit()
        assert updated == 2

        rows = session.query(ExpenseTransaction).order_by(ExpenseTransaction.id.asc()).all()
        assert all((t.category_system or "").strip() == "ZP Rose" for t in rows)
    finally:
        session.close()


def test_learning_handles_apostrophes_and_simple_variants() -> None:
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    session = Session(engine)
    try:
        _seed_min_txn(session, merchant="MCDONALD'S", category_system="Unknown")
        _seed_min_txn(session, merchant="MCDONALDS 1234", category_system="Unknown")
        _seed_min_txn(session, merchant="MCDONALD'S FLAGLER BEACH FL", category_system="Unknown")
        session.commit()

        updated = learn_unknown_merchant_category(
            session=session,
            merchant_norm="MCDONALD'S",
            category="Dining",
            from_category="Unknown",
        )
        session.commit()
        assert updated == 3

        rows = session.query(ExpenseTransaction).order_by(ExpenseTransaction.id.asc()).all()
        assert all((t.category_system or "").strip() == "Dining" for t in rows)
    finally:
        session.close()
