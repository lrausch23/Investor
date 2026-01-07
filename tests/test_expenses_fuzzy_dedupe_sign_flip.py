from __future__ import annotations

import datetime as dt
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from src.db.models import Base, ExpenseAccount, ExpenseImportBatch, ExpenseTransaction
from src.investor.expenses.config import ExpensesConfig
from src.investor.expenses.db import ImportOptions, import_csv_statement


def test_fuzzy_dedupe_detects_sign_flip_duplicates(tmp_path: Path) -> None:
    """
    If a legacy import stored a charge with the wrong sign, importing the same row again
    shouldn't create a duplicate. Fuzzy dedupe should match using +/- amount.
    """
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    session = Session(engine)
    try:
        acct = ExpenseAccount(institution="Chase", name="Amazon", last4_masked="0000", type="CREDIT")
        batch = ExpenseImportBatch(source="CSV", file_name="seed.csv", file_hash="x" * 64, row_count=0, duplicates_skipped=0)
        session.add_all([acct, batch])
        session.flush()
        session.add(
            ExpenseTransaction(
                txn_id="seed_wrong_sign",
                expense_account_id=acct.id,
                institution="Chase",
                account_name="Amazon",
                posted_date=dt.date(2025, 12, 30),
                transaction_date=dt.date(2025, 12, 30),
                description_raw="AMAZON MKTPL*184NU62V3",
                description_norm="AMAZON MKTPL*184NU62V3",
                merchant_norm="Amazon",
                amount=19.25,  # wrong sign (should be negative for charges)
                currency="USD",
                account_last4_masked="0000",
                cardholder_name="Laszlo Rausch",
                category_hint="Shopping",
                category_user=None,
                category_system="Shopping",
                tags_json=[],
                notes=None,
                import_batch_id=batch.id,
                original_row_json=None,
            )
        )
        session.commit()

        p = tmp_path / "chase.tsv"
        p.write_text(Path("tests/fixtures/expenses/chase_card_sample_2.tsv").read_text())
        cfg = ExpensesConfig()
        res = import_csv_statement(
            session=session,
            cfg=cfg,
            file_path=p,
            institution="Chase",
            account_name="Amazon",
            account_type="CREDIT",
            account_last4="0000",
            default_cardholder_name="Laszlo Rausch",
            options=ImportOptions(format_name="chase_card_csv", fuzzy_dedupe=True, store_original_rows=False),
        )
        assert res.inserted == 0
        assert res.fuzzy_duplicates_skipped == 1
        assert session.query(ExpenseTransaction).count() == 1
    finally:
        session.close()

