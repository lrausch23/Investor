from __future__ import annotations

from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from src.db.models import Base, ExpenseTransaction
from src.investor.expenses.config import ExpensesConfig
from src.investor.expenses.db import ImportOptions, import_csv_statement


def test_default_cardholder_applies_to_chase_import(tmp_path: Path) -> None:
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    session = Session(engine)
    try:
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
            options=ImportOptions(format_name="chase_card_csv", fuzzy_dedupe=False, store_original_rows=False),
        )
        assert res.inserted == 1
        t = session.query(ExpenseTransaction).one()
        assert t.cardholder_name == "Laszlo Rausch"
    finally:
        session.close()

