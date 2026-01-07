from __future__ import annotations

from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from src.db.models import Base
from src.investor.expenses.config import ExpensesConfig
from src.investor.expenses.db import ImportOptions, import_csv_statement


def test_file_hash_guard_blocks_importing_same_file_into_different_account(tmp_path: Path) -> None:
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    session = Session(engine)
    try:
        p = tmp_path / "chase.tsv"
        p.write_text(Path("tests/fixtures/expenses/chase_card_sample_2.tsv").read_text())
        cfg = ExpensesConfig()

        res1 = import_csv_statement(
            session=session,
            cfg=cfg,
            file_path=p,
            institution="Chase",
            account_name="Chase ****4549",
            account_type="CREDIT",
            account_last4="4549",
            options=ImportOptions(format_name="chase_card_csv", fuzzy_dedupe=True),
        )
        assert res1.inserted == 1

        try:
            import_csv_statement(
                session=session,
                cfg=cfg,
                file_path=p,
                institution="Chase",
                account_name="Chase ****4477",
                account_type="CREDIT",
                account_last4="4477",
                options=ImportOptions(format_name="chase_card_csv", fuzzy_dedupe=True),
            )
            assert False, "Expected ValueError"
        except ValueError as e:
            assert "File already imported" in str(e)
    finally:
        session.close()


def test_file_hash_guard_allows_reimport_into_same_account(tmp_path: Path) -> None:
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    session = Session(engine)
    try:
        p = tmp_path / "chase.tsv"
        p.write_text(Path("tests/fixtures/expenses/chase_card_sample_2.tsv").read_text())
        cfg = ExpensesConfig()

        res1 = import_csv_statement(
            session=session,
            cfg=cfg,
            file_path=p,
            institution="Chase",
            account_name="Chase ****4549",
            account_type="CREDIT",
            account_last4="4549",
            options=ImportOptions(format_name="chase_card_csv", fuzzy_dedupe=True),
        )
        assert res1.inserted == 1
        res2 = import_csv_statement(
            session=session,
            cfg=cfg,
            file_path=p,
            institution="Chase",
            account_name="Chase ****4549",
            account_type="CREDIT",
            account_last4="4549",
            options=ImportOptions(format_name="chase_card_csv", fuzzy_dedupe=True),
        )
        # Re-import should be idempotent (dedupe by txn_id).
        assert res2.inserted == 0
    finally:
        session.close()

