from __future__ import annotations

import datetime as dt
from decimal import Decimal
from pathlib import Path

import yaml
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from src.db.models import Base, ExpenseAccount, ExpenseImportBatch, ExpenseMerchantSetting, ExpenseTransaction
from src.investor.expenses.categorize import apply_rules_to_db, categorize_one
from src.investor.expenses.config import CategorizationConfig
from src.investor.expenses.recurring import detect_recurring
from src.investor.expenses.categorize import load_rules


def _seed_txn(
    session: Session,
    *,
    posted: dt.date,
    merchant: str,
    desc: str,
    amount: Decimal,
    category_system: str | None = None,
) -> None:
    acct = session.query(ExpenseAccount).first()
    if acct is None:
        acct = ExpenseAccount(institution="TestBank", name="Checking", last4_masked="0000", type="BANK")
        session.add(acct)
        session.flush()
    batch = session.query(ExpenseImportBatch).first()
    if batch is None:
        batch = ExpenseImportBatch(source="CSV", file_name="seed.csv", file_hash="x" * 64, row_count=0, duplicates_skipped=0)
        session.add(batch)
        session.flush()
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
            category_system=category_system,
            tags_json=[],
            notes=None,
            import_batch_id=batch.id,
            original_row_json=None,
        )
    )
    session.flush()


def test_categorization_rule_priority(tmp_path: Path) -> None:
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    session = Session(engine)
    try:
        _seed_txn(session, posted=dt.date(2025, 12, 2), merchant="Amazon", desc="Amazon Marketplace", amount=Decimal("-19.99"))
        rules_path = tmp_path / "rules.yaml"
        rules_doc = {
            "version": 1,
            "rules": [
                {"name": "low", "priority": 1, "category": "Shopping", "match": {"merchant_exact": "Amazon"}},
                {"name": "high", "priority": 100, "category": "Subscriptions", "match": {"merchant_exact": "Amazon"}},
            ],
        }
        rules_path.write_text(yaml.safe_dump(rules_doc, sort_keys=False))
        updated, skipped_user = apply_rules_to_db(
            session=session,
            rules_path=rules_path,
            config=CategorizationConfig(),
            rebuild=True,
        )
        assert updated == 1
        assert skipped_user == 0
        t = session.query(ExpenseTransaction).one()
        assert t.category_system == "Subscriptions"
    finally:
        session.close()


def test_recurring_detection_monthly() -> None:
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    session = Session(engine)
    try:
        _seed_txn(session, posted=dt.date(2025, 10, 5), merchant="Netflix", desc="NETFLIX.COM", amount=Decimal("-15.99"))
        _seed_txn(session, posted=dt.date(2025, 11, 5), merchant="Netflix", desc="NETFLIX.COM", amount=Decimal("-15.99"))
        _seed_txn(session, posted=dt.date(2025, 12, 5), merchant="Netflix", desc="NETFLIX.COM", amount=Decimal("-15.99"))
        session.commit()
        items = detect_recurring(session=session, year=2025, min_months=3, include_income=False)
        assert items
        n = next(i for i in items if i.merchant == "Netflix")
        assert n.months_present == 3
        assert n.amount == Decimal("15.99")
        assert n.cadence in {"MONTHLY", "UNKNOWN"}
    finally:
        session.close()


def test_recurring_detection_variable_amount_monthly() -> None:
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    session = Session(engine)
    try:
        _seed_txn(
            session,
            posted=dt.date(2025, 10, 2),
            merchant="FPL",
            desc="FPL DIRECT DEBIT",
            amount=Decimal("-101.11"),
            category_system="Utilities",
        )
        _seed_txn(
            session,
            posted=dt.date(2025, 11, 2),
            merchant="FPL",
            desc="FPL DIRECT DEBIT",
            amount=Decimal("-115.50"),
            category_system="Utilities",
        )
        _seed_txn(
            session,
            posted=dt.date(2025, 12, 2),
            merchant="FPL",
            desc="FPL DIRECT DEBIT",
            amount=Decimal("-109.25"),
            category_system="Utilities",
        )
        session.commit()
        items = detect_recurring(session=session, year=2025, min_months=3, include_income=False)
        fpl = next(i for i in items if i.merchant == "FPL")
        assert fpl.months_present == 3
        assert fpl.category == "Utilities"
        # Median of [101.11, 109.25, 115.50]
        assert fpl.amount == Decimal("109.25")
    finally:
        session.close()


def test_manual_annual_recurring_included_with_single_occurrence() -> None:
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    session = Session(engine)
    try:
        _seed_txn(
            session,
            posted=dt.date(2025, 6, 15),
            merchant="Condo HOA",
            desc="CONDOMINIUM ASSO",
            amount=Decimal("-1200.00"),
            category_system="Housing",
        )
        session.add(
            ExpenseMerchantSetting(
                merchant_key="condo hoa",
                merchant_display="Condo HOA",
                recurring_enabled=True,
                cadence="ANNUAL",
            )
        )
        session.commit()
        items = detect_recurring(session=session, year=2025, min_months=3, include_income=False)
        it = next(i for i in items if i.merchant == "Condo HOA")
        assert it.months_present == 1
        assert it.occurrences == 1
        assert it.cadence == "ANNUAL"
        assert it.amount == Decimal("1200.00")
        assert it.monthly_equivalent == Decimal("100.00")
    finally:
        session.close()


def test_manual_semiannual_recurring_monthly_equivalent() -> None:
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    session = Session(engine)
    try:
        _seed_txn(
            session,
            posted=dt.date(2025, 3, 1),
            merchant="Car Insurance",
            desc="INSURANCE PREM",
            amount=Decimal("-600.00"),
            category_system="Insurance",
        )
        session.add(
            ExpenseMerchantSetting(
                merchant_key="car insurance",
                merchant_display="Car Insurance",
                recurring_enabled=True,
                cadence="SEMIANNUAL",
            )
        )
        session.commit()
        items = detect_recurring(session=session, year=2025, min_months=3, include_income=False)
        it = next(i for i in items if i.merchant == "Car Insurance")
        assert it.cadence == "SEMIANNUAL"
        assert it.amount == Decimal("600.00")
        assert it.monthly_equivalent == Decimal("100.00")
    finally:
        session.close()

def test_keyword_payment_classified_as_transfer(tmp_path: Path) -> None:
    cfg = CategorizationConfig()
    # Minimal compiled rules; no custom rules required.
    tmp_rules_doc = {
        "version": 1,
        "transfer_keywords": cfg.transfer_keywords,
        "income_keywords": cfg.income_keywords,
        "rules": [],
    }
    rules_path = tmp_path / "rules.yaml"
    rules_path.write_text(yaml.safe_dump(tmp_rules_doc, sort_keys=False))
    rules = load_rules(rules_path, defaults=cfg)

    cat, rule = categorize_one(
        merchant_norm="AUTOPAY PAYMENT",
        description_norm="AUTOPAY PAYMENT AMEX",
        amount=-100.00,
        category_hint="Payments",
        rules=rules,
    )
    assert cat == "Payments"
    assert rule in {"keyword", "hint"}


def test_negative_refund_classified_as_merchant_credit(tmp_path: Path) -> None:
    cfg = CategorizationConfig()
    rules_path = tmp_path / "rules.yaml"
    rules_path.write_text(yaml.safe_dump({"version": 1, "rules": []}, sort_keys=False))
    rules = load_rules(rules_path, defaults=cfg)
    cat, _rule = categorize_one(
        merchant_norm="Some Store",
        description_norm="MERCHANT CREDIT Some Store",
        amount=-10.00,
        category_hint=None,
        rules=rules,
    )
    assert cat == "Merchant Credits"


def test_aplpay_classified_as_apple_pay(tmp_path: Path) -> None:
    cfg = CategorizationConfig()
    rules_path = tmp_path / "rules.yaml"
    rules_path.write_text(yaml.safe_dump({"version": 1, "rules": []}, sort_keys=False))
    rules = load_rules(rules_path, defaults=cfg)
    cat, rule = categorize_one(
        merchant_norm="Some Store",
        description_norm="AplPay SOME STORE",
        amount=-12.34,
        category_hint=None,
        rules=rules,
    )
    assert cat == "Apple Pay"
    assert rule == "keyword"


def test_monthly_installment_classified_as_apple_installment_payment(tmp_path: Path) -> None:
    cfg = CategorizationConfig()
    rules_path = tmp_path / "rules.yaml"
    rules_path.write_text(yaml.safe_dump({"version": 1, "rules": []}, sort_keys=False))
    rules = load_rules(rules_path, defaults=cfg)
    cat, rule = categorize_one(
        merchant_norm="Apple",
        description_norm="Monthly Installment iPhone",
        amount=-50.00,
        category_hint=None,
        rules=rules,
    )
    assert cat == "Apple Installment Payment"
    assert rule == "keyword"


def test_gs_apple_card_transfer_payment_classified_as_payments(tmp_path: Path) -> None:
    cfg = CategorizationConfig()
    rules_path = tmp_path / "rules.yaml"
    rules_path.write_text(yaml.safe_dump({"version": 1, "rules": []}, sort_keys=False))
    rules = load_rules(rules_path, defaults=cfg)
    cat, _rule = categorize_one(
        merchant_norm="Ach Deposit Internet Transfer From Account Ending In 5896",
        description_norm="ACH DEPOSIT INTERNET TRANSFER FROM ACCOUNT ENDING IN 5896",
        amount=312.39,
        category_hint="Transfer",
        rules=rules,
    )
    assert cat == "Payments"


def test_chase_category_hint_shopping_maps_to_shopping(tmp_path: Path) -> None:
    cfg = CategorizationConfig()
    rules_path = tmp_path / "rules.yaml"
    rules_path.write_text(yaml.safe_dump({"version": 1, "rules": []}, sort_keys=False))
    rules = load_rules(rules_path, defaults=cfg)
    cat, rule = categorize_one(
        merchant_norm="Amazon",
        description_norm="AMAZON MKTPL*184NU62V3",
        amount=-19.25,
        category_hint="Shopping",
        rules=rules,
    )
    assert cat == "Shopping"
    assert rule == "hint"
