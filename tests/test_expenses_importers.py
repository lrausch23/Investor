from __future__ import annotations

import datetime as dt
from decimal import Decimal
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from src.db.models import Base, ExpenseTransaction
from src.investor.expenses.config import ExpensesConfig
from src.investor.expenses.db import ImportOptions, import_csv_statement
from src.investor.expenses.importers import default_importers
from src.investor.expenses.importers.base import read_csv_rows
from src.investor.expenses.normalize import (
    normalize_bank_merchant,
    normalize_description,
    normalize_merchant,
    stable_txn_id,
)


def test_detect_and_parse_chase_sample() -> None:
    content = Path("tests/fixtures/expenses/chase_card_sample.csv").read_text()
    headers, rows = read_csv_rows(content)
    imp = next(i for i in default_importers() if i.format_name == "chase_card_csv")
    assert imp.detect(headers)
    txns = imp.parse_rows(rows=rows, default_currency="USD")
    assert len(txns) == 3
    assert txns[0].posted_date == dt.date(2025, 12, 2)
    assert txns[0].amount == Decimal("-19.99")
    assert "AMAZON" in txns[0].description.upper()
    assert txns[2].amount == Decimal("250.00")


def test_detect_and_parse_chase_tsv_with_signed_amount() -> None:
    content = Path("tests/fixtures/expenses/chase_card_sample_2.tsv").read_text()
    headers, rows = read_csv_rows(content)
    imp = next(i for i in default_importers() if i.format_name == "chase_card_csv")
    assert imp.detect(headers)
    txns = imp.parse_rows(rows=rows, default_currency="USD")
    assert len(txns) == 1
    assert txns[0].posted_date == dt.date(2025, 12, 30)
    assert txns[0].amount == Decimal("-19.25")


def test_detect_and_parse_chase_fee_row() -> None:
    content = Path("tests/fixtures/expenses/chase_card_fee_sample.tsv").read_text()
    headers, rows = read_csv_rows(content)
    imp = next(i for i in default_importers() if i.format_name == "chase_card_csv")
    assert imp.detect(headers)
    txns = imp.parse_rows(rows=rows, default_currency="USD")
    assert len(txns) == 1
    assert txns[0].posted_date == dt.date(2025, 1, 19)
    assert txns[0].category_hint == "Fees & Adjustments"
    assert txns[0].amount == Decimal("-8.31")


def test_detect_and_parse_chase_bank_sample_includes_last4() -> None:
    content = Path("tests/fixtures/expenses/chase_bank_sample.tsv").read_text()
    headers, rows = read_csv_rows(content)
    imp = next(i for i in default_importers() if i.format_name == "chase_bank_csv")
    assert imp.detect(headers)
    txns = imp.parse_rows(rows=rows, default_currency="USD")
    assert len(txns) == 1
    assert txns[0].posted_date == dt.date(2026, 1, 2)
    assert txns[0].amount == Decimal("-250.00")
    assert txns[0].account_last4 == "0787"


def test_detect_and_parse_amex_sample_includes_cardholder_and_last4() -> None:
    content = Path("tests/fixtures/expenses/amex_sample.csv").read_text()
    headers, rows = read_csv_rows(content)
    imp = next(i for i in default_importers() if i.format_name == "amex_csv")
    assert imp.detect(headers)
    txns = imp.parse_rows(rows=rows, default_currency="USD")
    assert len(txns) == 4
    assert txns[0].cardholder_name == "MILANA KULYNYCH"
    assert txns[0].account_last4 == "1038"
    # Charges become negative; payments/credits become positive.
    assert txns[0].amount == Decimal("-19.99")
    assert any(t.description.lower().startswith("payment") and t.amount == Decimal("300.00") for t in txns)
    assert any("merchant credit" in t.description.lower() and t.amount == Decimal("10.00") for t in txns)


def test_no_ambiguous_detect_for_amex_vs_generic() -> None:
    content = Path("tests/fixtures/expenses/amex_sample.csv").read_text()
    headers, _rows = read_csv_rows(content)
    amex = next(i for i in default_importers() if i.format_name == "amex_csv")
    generic = next(i for i in default_importers() if i.format_name == "generic_bank_csv")
    assert amex.detect(headers) is True
    assert generic.detect(headers) is False


def test_normalize_description_and_merchant() -> None:
    raw = "POS PURCHASE AMAZON.COM AMZN MKTP US 123456"
    desc = normalize_description(raw)
    assert "POS" not in desc.upper()
    assert "123456" not in desc
    merchant = normalize_merchant(desc)
    assert merchant == "Amazon"

    merchant2 = normalize_merchant("Monthly Installment iPhone 15 Pro")
    assert merchant2 == "Apple"

    assert normalize_merchant("MCDONALD'S F6717 000PALM COAST FL") == "McDonald's"
    assert normalize_merchant("GOOGLE *FI 42M7RZ G.CO/HELPPAY# CA") == "Google Fi"
    assert normalize_merchant("AplPay PUBLIX FLAGLER BEACH FL") == "Publix"
    assert normalize_merchant("SQSP* INV*** NEW YORK NY") == "Squarespace"

    fee_merchant = normalize_merchant("PLAN FEE - SENECA ARMS CO")
    assert fee_merchant == "SENECA ARMS CO"

    bank1 = "ONLINE TRANSFER TO CHK ...0787 TRANSACTION#: 27555130795 01/02"
    bank2 = "ONLINE TRANSFER TO CHK ...1234 TRANSACTION#: 99999999999 12/31"
    assert normalize_bank_merchant(bank1) == normalize_bank_merchant(bank2)
    assert normalize_bank_merchant(bank1) == "ONLINE TRANSFER TO CHK"

    payee1 = "CONDOMINIUM ASSO CONDOMINIU ST-J9M7U7P3T5I2 WEB ID: ***"
    payee2 = "CONDOMINIUM ASSO CONDOMINIU ST-X7L4H0R1M1R1 WEB ID: ***"
    assert normalize_bank_merchant(payee1) == normalize_bank_merchant(payee2)
    assert normalize_bank_merchant(payee1) == "Condominium Asso"
    assert normalize_bank_merchant("Condominium Asso L PPD ID: ***") == "Condominium Asso"
    assert normalize_bank_merchant("Hammock Dunes Ow L PPD ID: ***") == "Hammock Dunes Ow"

    mazda1 = "MAZDA FINANCIAL *** EL7297GJKEQ3MDU WEB ID: ***"
    mazda2 = "MAZDA FINANCIAL *** LDB0282DID5GYCO WEB ID: ***"
    assert normalize_bank_merchant(mazda1) == normalize_bank_merchant(mazda2)
    assert normalize_bank_merchant(mazda1) == "MAZDA FINANCIAL"

    ins1 = "UNITED WORLD HTH APR INSPRM PPD ID: ***"
    ins2 = "UNITED WORLD HTH MAY INSPRM PPD ID: ***"
    assert normalize_bank_merchant(ins1) == normalize_bank_merchant(ins2)


def test_stable_txn_id_deterministic() -> None:
    txn_id_1 = stable_txn_id(
        institution="Chase",
        account_name="Freedom",
        posted_date=dt.date(2025, 12, 2),
        amount=Decimal("-19.99"),
        description_norm="Amazon",
        currency="USD",
        external_id=None,
    )
    txn_id_2 = stable_txn_id(
        institution="Chase",
        account_name="Freedom",
        posted_date=dt.date(2025, 12, 2),
        amount=Decimal("-19.99"),
        description_norm="Amazon",
        currency="USD",
        external_id=None,
    )
    assert txn_id_1 == txn_id_2


def test_import_statement_dedupes(tmp_path: Path) -> None:
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    session = Session(engine)
    try:
        p = tmp_path / "chase.csv"
        p.write_text(Path("tests/fixtures/expenses/chase_card_sample.csv").read_text())
        cfg = ExpensesConfig()
        res1 = import_csv_statement(
            session=session,
            cfg=cfg,
            file_path=p,
            institution="Chase",
            account_name="Freedom",
            account_type="CREDIT",
            account_last4="1234",
            options=ImportOptions(format_name="chase_card_csv", fuzzy_dedupe=False, store_original_rows=False),
        )
        assert res1.inserted == 3
        res2 = import_csv_statement(
            session=session,
            cfg=cfg,
            file_path=p,
            institution="Chase",
            account_name="Freedom",
            account_type="CREDIT",
            account_last4="1234",
            options=ImportOptions(format_name="chase_card_csv", fuzzy_dedupe=False, store_original_rows=False),
        )
        assert res2.inserted == 0
        assert res2.duplicates_skipped >= 3
        assert session.query(ExpenseTransaction).count() == 3
    finally:
        session.close()
