from __future__ import annotations

import datetime as dt
from typing import Any, Optional

try:
    from sqlalchemy import (
        JSON,
        Boolean,
        Date,
        Enum,
        Float,
        ForeignKey,
        Index,
        Integer,
        Numeric,
        String,
        Text,
        UniqueConstraint,
    )
    from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
except Exception as e:  # pragma: no cover
    raise RuntimeError(
        "Failed to import SQLAlchemy.\n\n"
        "Common cause on macOS: running with system Python 3.13 + an older/incompatible SQLAlchemy, "
        "which can raise errors mentioning 'TypingOnly'.\n\n"
        "Fix:\n"
        "  1) Use Python 3.11/3.12 (recommended), or ensure SQLAlchemy is upgraded for Python 3.13.\n"
        "  2) Create a virtualenv and install dependencies:\n"
        "     python -m venv .venv\n"
        "     source .venv/bin/activate\n"
        "     pip install -r requirements.txt\n\n"
        f"Original error: {type(e).__name__}: {e}"
    ) from e

from src.utils.time import utcnow
from src.db.types import UTCDateTime


class Base(DeclarativeBase):
    pass


TaxpayerType = Enum("TRUST", "PERSONAL", name="taxpayer_type")
BrokerType = Enum("IB", "RJ", "CHASE", "MANUAL", name="broker_type")
AccountType = Enum("TAXABLE", "IRA", "OTHER", name="account_type")
IncomeType = Enum("DIVIDEND", "INTEREST", "WITHHOLDING", "FEE", name="income_type")
TxnType = Enum("BUY", "SELL", "DIV", "INT", "FEE", "WITHHOLDING", "TRANSFER", "OTHER", name="txn_type")
PlanScope = Enum("TRUST", "PERSONAL", "BOTH", name="plan_scope")
PlanStatus = Enum("DRAFT", "FINAL", "CANCELED", name="plan_status")
SyncMode = Enum("FULL", "INCREMENTAL", name="sync_mode")
SyncStatus = Enum("SUCCESS", "PARTIAL", "ERROR", name="sync_status")
HouseholdEntityType = Enum("USER", "SPOUSE", "DEPENDENT", "TRUST", "BUSINESS", "HOUSEHOLD", name="household_entity_type")
TaxDocType = Enum("W2", "K1", "1099INT", "1099DIV", "1099B", "1099R", "1095A", "1098", "SSA1099", "OTHER", name="tax_doc_type")
TaxDocStatus = Enum(
    "UPLOADED",
    "EXTRACTING",
    "EXTRACTED",
    "NEEDS_REVIEW",
    "CONFIRMED",
    "ERROR",
    name="tax_doc_status",
)
TaxFactType = Enum(
    "WAGES",
    "FED_WITHHOLDING",
    "IRA_DISTRIBUTION",
    "IRA_WITHHOLDING",
    "INT_INCOME",
    "DIV_ORDINARY",
    "DIV_QUALIFIED",
    "CAP_GAIN_DIST",
    "K1_ORD_INCOME",
    "K1_INTEREST",
    "K1_DIVIDENDS",
    "K1_RENTAL",
    "K1_OTHER",
    "ACA_PREMIUM",
    "ACA_SLCSP",
    "ACA_APTC",
    "B_PROCEEDS",
    "B_COST_BASIS",
    "B_GAIN_LOSS",
    name="tax_fact_type",
)


class TaxpayerEntity(Base):
    __tablename__ = "taxpayer_entities"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(200), unique=True, nullable=False)
    type: Mapped[str] = mapped_column(TaxpayerType, nullable=False)
    tax_id_last4: Mapped[Optional[str]] = mapped_column(String(4))
    notes: Mapped[Optional[str]] = mapped_column(Text)

    accounts: Mapped[list["Account"]] = relationship(back_populates="taxpayer_entity")


class HouseholdEntity(Base):
    __tablename__ = "household_entities"
    __table_args__ = (
        UniqueConstraint("tax_year", "entity_type", "display_name"),
        Index("ix_household_entities_year", "tax_year"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    tax_year: Mapped[int] = mapped_column(Integer, nullable=False)
    entity_type: Mapped[str] = mapped_column(HouseholdEntityType, nullable=False)
    display_name: Mapped[str] = mapped_column(String(200), nullable=False)
    tin_last4: Mapped[Optional[str]] = mapped_column(String(4))
    notes: Mapped[Optional[str]] = mapped_column(Text)
    created_at: Mapped[dt.datetime] = mapped_column(UTCDateTime(), default=utcnow, nullable=False)
    updated_at: Mapped[dt.datetime] = mapped_column(UTCDateTime(), default=utcnow, nullable=False)


class Account(Base):
    __tablename__ = "accounts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(200), unique=True, nullable=False)
    broker: Mapped[str] = mapped_column(BrokerType, nullable=False)
    account_type: Mapped[str] = mapped_column(AccountType, nullable=False)
    taxpayer_entity_id: Mapped[int] = mapped_column(ForeignKey("taxpayer_entities.id"), nullable=False)

    taxpayer_entity: Mapped["TaxpayerEntity"] = relationship(back_populates="accounts")
    lots: Mapped[list["PositionLot"]] = relationship(back_populates="account")
    cash_balances: Mapped[list["CashBalance"]] = relationship(back_populates="account")
    income_events: Mapped[list["IncomeEvent"]] = relationship(back_populates="account")
    transactions: Mapped[list["Transaction"]] = relationship(back_populates="account")


class SubstituteGroup(Base):
    __tablename__ = "substitute_groups"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(200), unique=True, nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text)

    securities: Mapped[list["Security"]] = relationship(back_populates="substitute_group")


class Security(Base):
    __tablename__ = "securities"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    ticker: Mapped[str] = mapped_column(String(32), unique=True, nullable=False)
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    asset_class: Mapped[str] = mapped_column(String(64), nullable=False)
    expense_ratio: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    substitute_group_id: Mapped[Optional[int]] = mapped_column(ForeignKey("substitute_groups.id"))
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)

    substitute_group: Mapped[Optional["SubstituteGroup"]] = relationship(back_populates="securities")


class PositionLot(Base):
    __tablename__ = "position_lots"
    __table_args__ = (UniqueConstraint("account_id", "ticker", "acquisition_date", "qty", "basis_total"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    account_id: Mapped[int] = mapped_column(ForeignKey("accounts.id"), nullable=False)
    ticker: Mapped[str] = mapped_column(String(32), nullable=False)
    acquisition_date: Mapped[dt.date] = mapped_column(Date, nullable=False)
    qty: Mapped[float] = mapped_column(Numeric(20, 6), nullable=False)
    basis_total: Mapped[float] = mapped_column(Numeric(20, 2), nullable=False)
    adjusted_basis_total: Mapped[Optional[float]] = mapped_column(Numeric(20, 2))

    account: Mapped["Account"] = relationship(back_populates="lots")


class CashBalance(Base):
    __tablename__ = "cash_balances"
    __table_args__ = (UniqueConstraint("account_id", "as_of_date"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    account_id: Mapped[int] = mapped_column(ForeignKey("accounts.id"), nullable=False)
    as_of_date: Mapped[dt.date] = mapped_column(Date, nullable=False)
    amount: Mapped[float] = mapped_column(Numeric(20, 2), nullable=False)

    account: Mapped["Account"] = relationship(back_populates="cash_balances")


class BullionHolding(Base):
    """
    Manual holdings for physical bullion.

    Stored as a single "current" row per (account, metal). Edits overwrite values; changes are still
    captured in the AuditLog.
    """

    __tablename__ = "bullion_holdings"
    __table_args__ = (
        UniqueConstraint("account_id", "metal"),
        Index("ix_bullion_holdings_account", "account_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    account_id: Mapped[int] = mapped_column(ForeignKey("accounts.id"), nullable=False)
    metal: Mapped[str] = mapped_column(String(16), nullable=False)  # GOLD|SILVER
    quantity: Mapped[float] = mapped_column(Numeric(20, 6), nullable=False)
    unit: Mapped[str] = mapped_column(String(16), nullable=False, default="oz")
    unit_price: Mapped[float] = mapped_column(Numeric(20, 2), nullable=False)  # USD per unit
    cost_basis_total: Mapped[Optional[float]] = mapped_column(Numeric(20, 2))  # optional: total USD cost
    currency: Mapped[str] = mapped_column(String(8), nullable=False, default="USD")
    as_of_date: Mapped[dt.date] = mapped_column(Date, nullable=False)
    notes: Mapped[Optional[str]] = mapped_column(Text)
    created_at: Mapped[dt.datetime] = mapped_column(UTCDateTime(), default=utcnow, nullable=False)
    updated_at: Mapped[dt.datetime] = mapped_column(UTCDateTime(), default=utcnow, nullable=False)

    account: Mapped["Account"] = relationship()


class IncomeEvent(Base):
    __tablename__ = "income_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    account_id: Mapped[int] = mapped_column(ForeignKey("accounts.id"), nullable=False)
    date: Mapped[dt.date] = mapped_column(Date, nullable=False)
    type: Mapped[str] = mapped_column(IncomeType, nullable=False)
    ticker: Mapped[Optional[str]] = mapped_column(String(32))
    amount: Mapped[float] = mapped_column(Numeric(20, 2), nullable=False)

    account: Mapped["Account"] = relationship(back_populates="income_events")


class BucketPolicy(Base):
    __tablename__ = "bucket_policies"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    effective_date: Mapped[dt.date] = mapped_column(Date, nullable=False)
    json_definition: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)

    buckets: Mapped[list["Bucket"]] = relationship(back_populates="policy", cascade="all, delete-orphan")
    assignments: Mapped[list["BucketAssignment"]] = relationship(
        back_populates="policy", cascade="all, delete-orphan"
    )


class Bucket(Base):
    __tablename__ = "buckets"
    __table_args__ = (UniqueConstraint("policy_id", "code"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    policy_id: Mapped[int] = mapped_column(ForeignKey("bucket_policies.id"), nullable=False)
    code: Mapped[str] = mapped_column(String(2), nullable=False)  # B1..B4
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    min_pct: Mapped[float] = mapped_column(Float, nullable=False)
    target_pct: Mapped[float] = mapped_column(Float, nullable=False)
    max_pct: Mapped[float] = mapped_column(Float, nullable=False)
    allowed_asset_classes_json: Mapped[list[str]] = mapped_column(JSON, default=list, nullable=False)
    constraints_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)

    policy: Mapped["BucketPolicy"] = relationship(back_populates="buckets")


class BucketAssignment(Base):
    __tablename__ = "bucket_assignments"
    __table_args__ = (UniqueConstraint("policy_id", "ticker"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    policy_id: Mapped[int] = mapped_column(ForeignKey("bucket_policies.id"), nullable=False)
    ticker: Mapped[str] = mapped_column(String(32), nullable=False)
    bucket_code: Mapped[str] = mapped_column(String(2), nullable=False)

    policy: Mapped["BucketPolicy"] = relationship(back_populates="assignments")


class Transaction(Base):
    __tablename__ = "transactions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    account_id: Mapped[int] = mapped_column(ForeignKey("accounts.id"), nullable=False)
    date: Mapped[dt.date] = mapped_column(Date, nullable=False)
    type: Mapped[str] = mapped_column(TxnType, nullable=False)
    ticker: Mapped[Optional[str]] = mapped_column(String(32))
    qty: Mapped[Optional[float]] = mapped_column(Numeric(20, 6))
    amount: Mapped[float] = mapped_column(Numeric(20, 2), nullable=False)  # signed cashflow
    lot_links_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)

    account: Mapped["Account"] = relationship(back_populates="transactions")


class Plan(Base):
    __tablename__ = "plans"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    created_at: Mapped[dt.datetime] = mapped_column(UTCDateTime(), default=utcnow, nullable=False)
    policy_id: Mapped[int] = mapped_column(ForeignKey("bucket_policies.id"), nullable=False)
    taxpayer_scope: Mapped[str] = mapped_column(PlanScope, nullable=False)
    goal_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    inputs_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    outputs_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    status: Mapped[str] = mapped_column(PlanStatus, nullable=False, default="DRAFT")


class AuditLog(Base):
    __tablename__ = "audit_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    at: Mapped[dt.datetime] = mapped_column(UTCDateTime(), default=utcnow, nullable=False)
    actor: Mapped[str] = mapped_column(String(200), nullable=False)
    action: Mapped[str] = mapped_column(String(50), nullable=False)
    entity: Mapped[str] = mapped_column(String(100), nullable=False)
    entity_id: Mapped[Optional[str]] = mapped_column(String(100))
    old_json: Mapped[Optional[dict[str, Any]]] = mapped_column(JSON)
    new_json: Mapped[Optional[dict[str, Any]]] = mapped_column(JSON)
    note: Mapped[Optional[str]] = mapped_column(Text)


class TaxAssumptionsSet(Base):
    __tablename__ = "tax_assumptions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(200), nullable=False, unique=True)
    effective_date: Mapped[dt.date] = mapped_column(Date, nullable=False)
    json_definition: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)


class TaxProfile(Base):
    __tablename__ = "tax_profiles"
    __table_args__ = (UniqueConstraint("year"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    year: Mapped[int] = mapped_column(Integer, nullable=False)
    filing_status: Mapped[str] = mapped_column(String(16), nullable=False, default="MFJ")
    state_code: Mapped[Optional[str]] = mapped_column(String(8))
    deductions_mode: Mapped[str] = mapped_column(String(16), nullable=False, default="standard")  # standard|itemized
    itemized_amount: Mapped[Optional[float]] = mapped_column(Numeric(20, 2))
    household_size: Mapped[int] = mapped_column(Integer, nullable=False, default=3)
    dependents_count: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    trust_income_taxable_to_user: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[dt.datetime] = mapped_column(UTCDateTime(), default=utcnow, nullable=False)
    updated_at: Mapped[dt.datetime] = mapped_column(UTCDateTime(), default=utcnow, nullable=False)


class TaxInput(Base):
    __tablename__ = "tax_inputs"
    __table_args__ = (UniqueConstraint("year"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    year: Mapped[int] = mapped_column(Integer, nullable=False)
    data_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    created_at: Mapped[dt.datetime] = mapped_column(UTCDateTime(), default=utcnow, nullable=False)
    updated_at: Mapped[dt.datetime] = mapped_column(UTCDateTime(), default=utcnow, nullable=False)


class TaxTag(Base):
    __tablename__ = "tax_tags"
    __table_args__ = (
        UniqueConstraint("transaction_id"),
        Index("ix_tax_tags_category", "category"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    transaction_id: Mapped[int] = mapped_column(ForeignKey("transactions.id"), nullable=False)
    category: Mapped[str] = mapped_column(String(50), nullable=False)
    note: Mapped[Optional[str]] = mapped_column(Text)
    created_at: Mapped[dt.datetime] = mapped_column(UTCDateTime(), default=utcnow, nullable=False)
    updated_at: Mapped[dt.datetime] = mapped_column(UTCDateTime(), default=utcnow, nullable=False)


class TaxDocument(Base):
    __tablename__ = "tax_documents"
    __table_args__ = (
        Index("ix_tax_documents_year", "tax_year"),
        Index("ix_tax_documents_status", "status"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[Optional[str]] = mapped_column(String(100))
    household_id: Mapped[Optional[str]] = mapped_column(String(100))
    tax_year: Mapped[int] = mapped_column(Integer, nullable=False)
    doc_type: Mapped[str] = mapped_column(TaxDocType, nullable=False, default="OTHER")
    filename: Mapped[str] = mapped_column(String(255), nullable=False)
    uploaded_at: Mapped[dt.datetime] = mapped_column(UTCDateTime(), default=utcnow, nullable=False)
    sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(TaxDocStatus, nullable=False, default="UPLOADED")
    notes: Mapped[Optional[str]] = mapped_column(Text)
    raw_file_path: Mapped[Optional[str]] = mapped_column(Text)
    page_count: Mapped[Optional[int]] = mapped_column(Integer)
    is_corrected: Mapped[Optional[bool]] = mapped_column(Boolean)
    is_authoritative: Mapped[Optional[bool]] = mapped_column(Boolean)
    owner_entity_id: Mapped[Optional[int]] = mapped_column(ForeignKey("household_entities.id"))

    extractions: Mapped[list["TaxDocumentExtraction"]] = relationship(
        back_populates="tax_document", cascade="all, delete-orphan"
    )
    facts: Mapped[list["TaxFact"]] = relationship(back_populates="tax_document", cascade="all, delete-orphan")
    owner_entity: Mapped[Optional["HouseholdEntity"]] = relationship()


class TaxDocumentExtraction(Base):
    __tablename__ = "tax_document_extractions"
    __table_args__ = (Index("ix_tax_doc_extract_doc", "tax_document_id"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    tax_document_id: Mapped[int] = mapped_column(ForeignKey("tax_documents.id"), nullable=False)
    extracted_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    confidence_overall: Mapped[Optional[float]] = mapped_column(Float)
    warnings: Mapped[list[str]] = mapped_column(JSON, default=list, nullable=False)
    extracted_at: Mapped[dt.datetime] = mapped_column(UTCDateTime(), default=utcnow, nullable=False)
    extractor_version: Mapped[Optional[str]] = mapped_column(String(50))

    tax_document: Mapped["TaxDocument"] = relationship(back_populates="extractions")


class TaxFact(Base):
    __tablename__ = "tax_facts"
    __table_args__ = (Index("ix_tax_facts_year", "tax_year"), Index("ix_tax_facts_doc", "source_doc_id"))

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    tax_year: Mapped[int] = mapped_column(Integer, nullable=False)
    source_doc_id: Mapped[int] = mapped_column(ForeignKey("tax_documents.id"), nullable=False)
    fact_type: Mapped[str] = mapped_column(TaxFactType, nullable=False)
    payer_name: Mapped[Optional[str]] = mapped_column(String(200))
    recipient_name: Mapped[Optional[str]] = mapped_column(String(200))
    amount: Mapped[float] = mapped_column(Numeric(20, 2), nullable=False)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    confidence: Mapped[Optional[float]] = mapped_column(Float)
    user_confirmed: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    owner_entity_id: Mapped[Optional[int]] = mapped_column(ForeignKey("household_entities.id"))
    created_at: Mapped[dt.datetime] = mapped_column(UTCDateTime(), default=utcnow, nullable=False)

    tax_document: Mapped["TaxDocument"] = relationship(back_populates="facts")
    owner_entity: Mapped[Optional["HouseholdEntity"]] = relationship()


class TaxReconciliationSnapshot(Base):
    __tablename__ = "tax_reconciliation_snapshots"
    __table_args__ = (Index("ix_tax_reconcile_year", "tax_year"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    tax_year: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[dt.datetime] = mapped_column(UTCDateTime(), default=utcnow, nullable=False)
    summary_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    document_ids: Mapped[list[int]] = mapped_column(JSON, default=list, nullable=False)
    notes: Mapped[Optional[str]] = mapped_column(Text)


# --- Expense analysis (local-first) ---


class ExpenseAccount(Base):
    __tablename__ = "expense_accounts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    institution: Mapped[str] = mapped_column(String(100), nullable=False)
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    last4_masked: Mapped[Optional[str]] = mapped_column(String(8))
    type: Mapped[str] = mapped_column(String(50), nullable=False, default="UNKNOWN")  # CREDIT|BANK|UNKNOWN
    scope: Mapped[str] = mapped_column(String(20), nullable=False, default="PERSONAL")  # PERSONAL|FAMILY|BUSINESS
    provider_account_id: Mapped[Optional[str]] = mapped_column(String(200))
    created_at: Mapped[dt.datetime] = mapped_column(UTCDateTime(), default=utcnow, nullable=False)


class ExpenseAccountBalance(Base):
    __tablename__ = "expense_account_balances"
    __table_args__ = (Index("ix_expense_account_balance_account", "expense_account_id"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    expense_account_id: Mapped[int] = mapped_column(ForeignKey("expense_accounts.id"), nullable=False)
    as_of_date: Mapped[dt.datetime] = mapped_column(UTCDateTime(), default=utcnow, nullable=False)
    balance_current: Mapped[Optional[float]] = mapped_column(Numeric(20, 2))
    balance_available: Mapped[Optional[float]] = mapped_column(Numeric(20, 2))
    currency: Mapped[str] = mapped_column(String(8), nullable=False, default="USD")
    source: Mapped[str] = mapped_column(String(50), nullable=False, default="PLAID")
    created_at: Mapped[dt.datetime] = mapped_column(UTCDateTime(), default=utcnow, nullable=False)

    expense_account: Mapped["ExpenseAccount"] = relationship()


class ExpenseImportBatch(Base):
    __tablename__ = "expense_import_batches"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    source: Mapped[str] = mapped_column(String(50), nullable=False, default="CSV")
    imported_at: Mapped[dt.datetime] = mapped_column(UTCDateTime(), default=utcnow, nullable=False)
    file_name: Mapped[str] = mapped_column(String(260), nullable=False)
    file_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    row_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    duplicates_skipped: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)


class ExpenseTransaction(Base):
    __tablename__ = "expense_transactions"
    __table_args__ = (
        UniqueConstraint("txn_id"),
        Index("ix_expense_txns_posted_date", "posted_date"),
        Index("ix_expense_txns_merchant", "merchant_norm"),
        Index("ix_expense_txns_account_date", "expense_account_id", "posted_date"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    txn_id: Mapped[str] = mapped_column(String(64), nullable=False)
    expense_account_id: Mapped[int] = mapped_column(ForeignKey("expense_accounts.id"), nullable=False)
    institution: Mapped[str] = mapped_column(String(100), nullable=False)
    account_name: Mapped[str] = mapped_column(String(200), nullable=False)
    posted_date: Mapped[dt.date] = mapped_column(Date, nullable=False)
    transaction_date: Mapped[Optional[dt.date]] = mapped_column(Date)
    description_raw: Mapped[str] = mapped_column(Text, nullable=False)
    description_norm: Mapped[str] = mapped_column(Text, nullable=False)
    merchant_norm: Mapped[str] = mapped_column(String(200), nullable=False)
    amount: Mapped[float] = mapped_column(Numeric(20, 2), nullable=False)  # debit negative, credit positive
    currency: Mapped[str] = mapped_column(String(8), nullable=False, default="USD")
    account_last4_masked: Mapped[Optional[str]] = mapped_column(String(8))
    cardholder_name: Mapped[Optional[str]] = mapped_column(String(200))
    category_hint: Mapped[Optional[str]] = mapped_column(String(100))
    category_user: Mapped[Optional[str]] = mapped_column(String(100))
    category_system: Mapped[Optional[str]] = mapped_column(String(100))
    tags_json: Mapped[list[str]] = mapped_column(JSON, default=list, nullable=False)
    notes: Mapped[Optional[str]] = mapped_column(Text)
    import_batch_id: Mapped[int] = mapped_column(ForeignKey("expense_import_batches.id"), nullable=False)
    original_row_json: Mapped[Optional[dict[str, Any]]] = mapped_column(JSON)
    created_at: Mapped[dt.datetime] = mapped_column(UTCDateTime(), default=utcnow, nullable=False)

    expense_account: Mapped["ExpenseAccount"] = relationship()


# --- Momentum screener (local-first market data + classification) ---


class PriceDaily(Base):
    """
    Daily OHLCV cache for end-of-day analytics (momentum, benchmarks, etc.).

    - Stored per-ticker, per-date.
    - Adjusted close is optional; callers should prefer it when present.
    """

    __tablename__ = "price_daily"
    __table_args__ = (
        UniqueConstraint("ticker", "date"),
        Index("ix_price_daily_ticker_date", "ticker", "date"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    ticker: Mapped[str] = mapped_column(String(32), nullable=False)
    date: Mapped[dt.date] = mapped_column(Date, nullable=False)
    adj_close: Mapped[Optional[float]] = mapped_column(Numeric(20, 6))
    close: Mapped[Optional[float]] = mapped_column(Numeric(20, 6))
    volume: Mapped[Optional[float]] = mapped_column(Numeric(20, 2))
    source: Mapped[str] = mapped_column(String(32), nullable=False, default="cache")
    updated_at: Mapped[dt.datetime] = mapped_column(UTCDateTime(), default=utcnow, nullable=False)


class TickerClassification(Base):
    __tablename__ = "ticker_classification"
    __table_args__ = (UniqueConstraint("ticker"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    ticker: Mapped[str] = mapped_column(String(32), nullable=False)
    sector: Mapped[Optional[str]] = mapped_column(String(100))
    industry: Mapped[Optional[str]] = mapped_column(String(120))
    as_of_date: Mapped[Optional[dt.date]] = mapped_column(Date)
    source: Mapped[str] = mapped_column(String(32), nullable=False, default="manual")
    updated_at: Mapped[dt.datetime] = mapped_column(UTCDateTime(), default=utcnow, nullable=False)


class UniverseMembership(Base):
    __tablename__ = "universe_membership"
    __table_args__ = (
        UniqueConstraint("universe", "ticker"),
        Index("ix_universe_membership_universe", "universe"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    universe: Mapped[str] = mapped_column(String(32), nullable=False)  # SP500|NASDAQ100|CUSTOM
    ticker: Mapped[str] = mapped_column(String(32), nullable=False)
    as_of_date: Mapped[Optional[dt.date]] = mapped_column(Date)
    source: Mapped[str] = mapped_column(String(32), nullable=False, default="manual")
    created_at: Mapped[dt.datetime] = mapped_column(UTCDateTime(), default=utcnow, nullable=False)


class WatchlistItem(Base):
    __tablename__ = "watchlist_items"
    __table_args__ = (UniqueConstraint("ticker"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    ticker: Mapped[str] = mapped_column(String(32), nullable=False)
    created_at: Mapped[dt.datetime] = mapped_column(UTCDateTime(), default=utcnow, nullable=False)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)


class ExpenseRule(Base):
    __tablename__ = "expense_rules"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(200), nullable=False, unique=True)
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    json_definition: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    created_at: Mapped[dt.datetime] = mapped_column(UTCDateTime(), default=utcnow, nullable=False)


class ExpenseCategory(Base):
    __tablename__ = "expense_categories"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False, unique=True)


class RecurringBill(Base):
    __tablename__ = "recurring_bill"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    scope: Mapped[str] = mapped_column(String(20), nullable=False, default="PERSONAL")
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    source_account_id: Mapped[Optional[int]] = mapped_column(ForeignKey("expense_accounts.id"))
    cadence: Mapped[str] = mapped_column(String(20), nullable=False, default="MONTHLY")
    amount_mode: Mapped[str] = mapped_column(String(20), nullable=False, default="VARIABLE")  # FIXED|RANGE|VARIABLE
    amount_expected: Mapped[Optional[float]] = mapped_column(Numeric(20, 2))
    amount_min: Mapped[Optional[float]] = mapped_column(Numeric(20, 2))
    amount_max: Mapped[Optional[float]] = mapped_column(Numeric(20, 2))
    due_day_of_month: Mapped[Optional[int]] = mapped_column(Integer)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    is_user_confirmed: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    autodetect_confidence: Mapped[Optional[float]] = mapped_column(Numeric(6, 3))
    created_at: Mapped[dt.datetime] = mapped_column(UTCDateTime(), default=utcnow, nullable=False)
    updated_at: Mapped[dt.datetime] = mapped_column(UTCDateTime(), default=utcnow, nullable=False, onupdate=utcnow)

    source_account: Mapped[Optional["ExpenseAccount"]] = relationship()


class RecurringBillRule(Base):
    __tablename__ = "recurring_bill_rule"
    __table_args__ = (Index("ix_recurring_bill_rule_bill", "recurring_bill_id"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    recurring_bill_id: Mapped[int] = mapped_column(ForeignKey("recurring_bill.id"), nullable=False)
    rule_type: Mapped[str] = mapped_column(String(40), nullable=False)  # PLAID_MERCHANT_ID|NAME_NORMALIZED
    rule_value: Mapped[str] = mapped_column(String(200), nullable=False)
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    recurring_bill: Mapped["RecurringBill"] = relationship()


class RecurringBillIgnore(Base):
    __tablename__ = "recurring_bill_ignore"
    __table_args__ = (UniqueConstraint("scope", "rule_type", "rule_value"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    scope: Mapped[str] = mapped_column(String(20), nullable=False)
    rule_type: Mapped[str] = mapped_column(String(40), nullable=False)
    rule_value: Mapped[str] = mapped_column(String(200), nullable=False)
    created_at: Mapped[dt.datetime] = mapped_column(UTCDateTime(), default=utcnow, nullable=False)


class RecurringCardCharge(Base):
    __tablename__ = "recurring_card_charge"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    scope: Mapped[str] = mapped_column(String(20), nullable=False, default="PERSONAL")
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    source_account_id: Mapped[Optional[int]] = mapped_column(ForeignKey("expense_accounts.id"))
    cadence: Mapped[str] = mapped_column(String(20), nullable=False, default="MONTHLY")
    amount_mode: Mapped[str] = mapped_column(String(20), nullable=False, default="VARIABLE")  # FIXED|RANGE|VARIABLE
    amount_expected: Mapped[Optional[float]] = mapped_column(Numeric(20, 2))
    amount_min: Mapped[Optional[float]] = mapped_column(Numeric(20, 2))
    amount_max: Mapped[Optional[float]] = mapped_column(Numeric(20, 2))
    due_day_of_month: Mapped[Optional[int]] = mapped_column(Integer)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    is_user_confirmed: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    autodetect_confidence: Mapped[Optional[float]] = mapped_column(Numeric(6, 3))
    created_at: Mapped[dt.datetime] = mapped_column(UTCDateTime(), default=utcnow, nullable=False)
    updated_at: Mapped[dt.datetime] = mapped_column(UTCDateTime(), default=utcnow, nullable=False, onupdate=utcnow)

    source_account: Mapped[Optional["ExpenseAccount"]] = relationship()


class RecurringCardChargeRule(Base):
    __tablename__ = "recurring_card_charge_rule"
    __table_args__ = (Index("ix_recurring_card_charge_rule_charge", "recurring_card_charge_id"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    recurring_card_charge_id: Mapped[int] = mapped_column(ForeignKey("recurring_card_charge.id"), nullable=False)
    rule_type: Mapped[str] = mapped_column(String(40), nullable=False)  # PLAID_MERCHANT_ID|NAME_NORMALIZED
    rule_value: Mapped[str] = mapped_column(String(200), nullable=False)
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    recurring_card_charge: Mapped["RecurringCardCharge"] = relationship()


class RecurringCardChargeIgnore(Base):
    __tablename__ = "recurring_card_charge_ignore"
    __table_args__ = (UniqueConstraint("scope", "rule_type", "rule_value"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    scope: Mapped[str] = mapped_column(String(20), nullable=False)
    rule_type: Mapped[str] = mapped_column(String(40), nullable=False)
    rule_value: Mapped[str] = mapped_column(String(200), nullable=False)
    created_at: Mapped[dt.datetime] = mapped_column(UTCDateTime(), default=utcnow, nullable=False)


class ExpenseMerchantSetting(Base):
    __tablename__ = "expense_merchant_settings"
    __table_args__ = (UniqueConstraint("merchant_key"), Index("ix_expense_merchant_settings_key", "merchant_key"))

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    merchant_key: Mapped[str] = mapped_column(String(200), nullable=False)
    merchant_display: Mapped[str] = mapped_column(String(200), nullable=False)
    recurring_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    cadence: Mapped[str] = mapped_column(String(20), nullable=False, default="UNKNOWN")  # WEEKLY|MONTHLY|QUARTERLY|SEMIANNUAL|ANNUAL|UNKNOWN
    created_at: Mapped[dt.datetime] = mapped_column(UTCDateTime(), default=utcnow, nullable=False)
    updated_at: Mapped[dt.datetime] = mapped_column(UTCDateTime(), default=utcnow, nullable=False)


class ExternalConnection(Base):
    __tablename__ = "external_connections"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(200), unique=True, nullable=False)
    provider: Mapped[str] = mapped_column(String(50), nullable=False)  # e.g. YODLEE
    broker: Mapped[str] = mapped_column(String(50), nullable=False)  # e.g. IB
    connector: Mapped[Optional[str]] = mapped_column(String(50))  # e.g. IB_FLEX_OFFLINE
    taxpayer_entity_id: Mapped[int] = mapped_column(ForeignKey("taxpayer_entities.id"), nullable=False)
    status: Mapped[str] = mapped_column(String(50), default="ACTIVE", nullable=False)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)

    # Sync pointers / coverage
    last_successful_sync_at: Mapped[Optional[dt.datetime]] = mapped_column(UTCDateTime())
    last_successful_txn_end: Mapped[Optional[dt.date]] = mapped_column(Date)
    txn_earliest_available: Mapped[Optional[dt.date]] = mapped_column(Date)
    holdings_last_asof: Mapped[Optional[dt.datetime]] = mapped_column(UTCDateTime())
    last_full_sync_at: Mapped[Optional[dt.datetime]] = mapped_column(UTCDateTime())
    coverage_status: Mapped[Optional[str]] = mapped_column(String(20))  # UNKNOWN|PARTIAL|COMPLETE
    last_error_json: Mapped[Optional[str]] = mapped_column(Text)

    taxpayer_entity: Mapped["TaxpayerEntity"] = relationship()
    sync_runs: Mapped[list["SyncRun"]] = relationship(back_populates="connection")


class SyncRun(Base):
    __tablename__ = "sync_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    connection_id: Mapped[int] = mapped_column(ForeignKey("external_connections.id"), nullable=False)
    started_at: Mapped[dt.datetime] = mapped_column(UTCDateTime(), default=utcnow, nullable=False)
    finished_at: Mapped[Optional[dt.datetime]] = mapped_column(UTCDateTime())
    status: Mapped[str] = mapped_column(SyncStatus, nullable=False, default="ERROR")

    mode: Mapped[str] = mapped_column(SyncMode, nullable=False)
    requested_start_date: Mapped[Optional[dt.date]] = mapped_column(Date)
    requested_end_date: Mapped[Optional[dt.date]] = mapped_column(Date)
    effective_start_date: Mapped[Optional[dt.date]] = mapped_column(Date)
    effective_end_date: Mapped[Optional[dt.date]] = mapped_column(Date)
    store_payloads: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)

    pages_fetched: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    txn_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    new_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    dupes_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    parse_fail_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    missing_symbol_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    error_json: Mapped[Optional[str]] = mapped_column(Text)

    coverage_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)

    connection: Mapped["ExternalConnection"] = relationship(back_populates="sync_runs")


class ExternalTransactionMap(Base):
    __tablename__ = "external_transaction_map"
    __table_args__ = (UniqueConstraint("connection_id", "provider_txn_id"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    connection_id: Mapped[int] = mapped_column(ForeignKey("external_connections.id"), nullable=False)
    provider_txn_id: Mapped[str] = mapped_column(String(200), nullable=False)
    transaction_id: Mapped[int] = mapped_column(ForeignKey("transactions.id"), nullable=False)
    created_at: Mapped[dt.datetime] = mapped_column(UTCDateTime(), default=utcnow, nullable=False)


class ExternalCredential(Base):
    __tablename__ = "external_credentials"
    __table_args__ = (UniqueConstraint("connection_id", "key"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    connection_id: Mapped[int] = mapped_column(ForeignKey("external_connections.id"), nullable=False)
    key: Mapped[str] = mapped_column(String(100), nullable=False)
    value_encrypted: Mapped[str] = mapped_column(Text, nullable=False)  # fernet token (base64 text)
    created_at: Mapped[dt.datetime] = mapped_column(UTCDateTime(), default=utcnow, nullable=False)
    updated_at: Mapped[dt.datetime] = mapped_column(UTCDateTime(), default=utcnow, nullable=False)


class ExternalAccountMap(Base):
    __tablename__ = "external_account_map"
    __table_args__ = (UniqueConstraint("connection_id", "provider_account_id"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    connection_id: Mapped[int] = mapped_column(ForeignKey("external_connections.id"), nullable=False)
    provider_account_id: Mapped[str] = mapped_column(String(200), nullable=False)
    account_id: Mapped[int] = mapped_column(ForeignKey("accounts.id"), nullable=False)
    created_at: Mapped[dt.datetime] = mapped_column(UTCDateTime(), default=utcnow, nullable=False)


class ExternalPayloadSnapshot(Base):
    __tablename__ = "external_payload_snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    sync_run_id: Mapped[int] = mapped_column(ForeignKey("sync_runs.id"), nullable=False)
    kind: Mapped[str] = mapped_column(String(50), nullable=False)  # accounts|transactions_page|holdings
    cursor: Mapped[Optional[str]] = mapped_column(String(200))
    payload_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    created_at: Mapped[dt.datetime] = mapped_column(UTCDateTime(), default=utcnow, nullable=False)


class ExternalHoldingSnapshot(Base):
    __tablename__ = "external_holding_snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    connection_id: Mapped[int] = mapped_column(ForeignKey("external_connections.id"), nullable=False)
    as_of: Mapped[dt.datetime] = mapped_column(UTCDateTime(), nullable=False)
    payload_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    created_at: Mapped[dt.datetime] = mapped_column(UTCDateTime(), default=utcnow, nullable=False)


class ExternalLiabilitySnapshot(Base):
    __tablename__ = "external_liability_snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    connection_id: Mapped[int] = mapped_column(ForeignKey("external_connections.id"), nullable=False)
    as_of: Mapped[dt.datetime] = mapped_column(UTCDateTime(), nullable=False)
    payload_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    created_at: Mapped[dt.datetime] = mapped_column(UTCDateTime(), default=utcnow, nullable=False)


class ExternalCardStatement(Base):
    __tablename__ = "external_card_statements"
    __table_args__ = (
        UniqueConstraint("connection_id", "file_hash"),
        Index("ix_external_card_statements_conn_last4", "connection_id", "last4"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    connection_id: Mapped[int] = mapped_column(ForeignKey("external_connections.id"), nullable=False)
    expense_account_id: Mapped[Optional[int]] = mapped_column(ForeignKey("expense_accounts.id"))
    last4: Mapped[Optional[str]] = mapped_column(String(8))
    statement_period_start: Mapped[Optional[dt.date]] = mapped_column(Date)
    statement_period_end: Mapped[Optional[dt.date]] = mapped_column(Date)
    payment_due_date: Mapped[Optional[dt.date]] = mapped_column(Date)
    statement_balance: Mapped[Optional[float]] = mapped_column(Numeric(20, 2))
    interest_saving_balance: Mapped[Optional[float]] = mapped_column(Numeric(20, 2))
    minimum_payment_due: Mapped[Optional[float]] = mapped_column(Numeric(20, 2))
    pay_over_time_json: Mapped[Optional[dict[str, Any]]] = mapped_column(JSON)
    source_file: Mapped[Optional[str]] = mapped_column(Text)
    file_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[dt.datetime] = mapped_column(UTCDateTime(), default=utcnow, nullable=False)

    connection: Mapped["ExternalConnection"] = relationship()
    expense_account: Mapped[Optional["ExpenseAccount"]] = relationship()


class ExternalFileIngest(Base):
    __tablename__ = "external_file_ingests"
    __table_args__ = (UniqueConstraint("connection_id", "file_hash"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    connection_id: Mapped[int] = mapped_column(ForeignKey("external_connections.id"), nullable=False)
    kind: Mapped[str] = mapped_column(String(50), nullable=False, default="TRANSACTIONS")
    file_name: Mapped[str] = mapped_column(String(260), nullable=False)
    file_hash: Mapped[str] = mapped_column(String(100), nullable=False)
    file_bytes: Mapped[Optional[int]] = mapped_column(Integer)
    file_mtime: Mapped[Optional[dt.datetime]] = mapped_column(UTCDateTime())
    imported_at: Mapped[dt.datetime] = mapped_column(UTCDateTime(), default=utcnow, nullable=False)
    stored_path: Mapped[Optional[str]] = mapped_column(Text)
    start_date_hint: Mapped[Optional[dt.date]] = mapped_column(Date)
    end_date_hint: Mapped[Optional[dt.date]] = mapped_column(Date)
    metadata_json: Mapped[Optional[dict[str, Any]]] = mapped_column(JSON)


class BrokerLotClosure(Base):
    __tablename__ = "broker_lot_closures"
    __table_args__ = (
        UniqueConstraint(
            "connection_id",
            "ib_trade_id",
            "open_datetime_raw",
            "quantity_closed",
            "cost_basis",
            "realized_pl_fifo",
            name="uq_broker_lot_closure",
        ),
        Index("ix_broker_lot_closure_scope", "connection_id", "provider_account_id", "symbol", "trade_date"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    connection_id: Mapped[int] = mapped_column(ForeignKey("external_connections.id"), nullable=False)
    taxpayer_entity_id: Mapped[Optional[int]] = mapped_column(ForeignKey("taxpayer_entities.id"))
    provider_account_id: Mapped[str] = mapped_column(String(200), nullable=False)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    conid: Mapped[Optional[str]] = mapped_column(String(64))
    trade_date: Mapped[dt.date] = mapped_column(Date, nullable=False)
    datetime_raw: Mapped[Optional[str]] = mapped_column(String(32))
    open_datetime_raw: Mapped[Optional[str]] = mapped_column(String(32))
    quantity_closed: Mapped[float] = mapped_column(Numeric(20, 6), nullable=False)
    cost_basis: Mapped[Optional[float]] = mapped_column(Numeric(20, 2))
    realized_pl_fifo: Mapped[Optional[float]] = mapped_column(Numeric(20, 2))
    proceeds_derived: Mapped[Optional[float]] = mapped_column(Numeric(20, 2))
    currency: Mapped[Optional[str]] = mapped_column(String(16))
    fx_rate_to_base: Mapped[Optional[float]] = mapped_column(Numeric(20, 8))
    ib_transaction_id: Mapped[Optional[str]] = mapped_column(String(64))
    ib_trade_id: Mapped[Optional[str]] = mapped_column(String(64))
    source_file_hash: Mapped[str] = mapped_column(String(100), nullable=False)
    raw_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    created_at: Mapped[dt.datetime] = mapped_column(UTCDateTime(), default=utcnow, nullable=False)


class BrokerWashSaleEvent(Base):
    __tablename__ = "broker_wash_sale_events"
    __table_args__ = (
        UniqueConstraint(
            "connection_id",
            "ib_trade_id",
            "when_realized_raw",
            "quantity",
            "realized_pl_fifo",
            name="uq_broker_wash_sale",
        ),
        Index("ix_broker_wash_sale_scope", "connection_id", "provider_account_id", "symbol", "trade_date"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    connection_id: Mapped[int] = mapped_column(ForeignKey("external_connections.id"), nullable=False)
    linked_closure_id: Mapped[Optional[int]] = mapped_column(ForeignKey("broker_lot_closures.id"))
    link_confidence: Mapped[Optional[int]] = mapped_column(Integer)  # 0..100
    provider_account_id: Mapped[str] = mapped_column(String(200), nullable=False)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    trade_date: Mapped[dt.date] = mapped_column(Date, nullable=False)
    holding_period_datetime_raw: Mapped[Optional[str]] = mapped_column(String(32))
    when_realized_raw: Mapped[Optional[str]] = mapped_column(String(32))
    when_reopened_raw: Mapped[Optional[str]] = mapped_column(String(32))
    quantity: Mapped[float] = mapped_column(Numeric(20, 6), nullable=False)
    realized_pl_fifo: Mapped[Optional[float]] = mapped_column(Numeric(20, 2))
    realized_pl_effective: Mapped[Optional[float]] = mapped_column(Numeric(20, 2))
    cost_basis: Mapped[Optional[float]] = mapped_column(Numeric(20, 2))
    proceeds_derived: Mapped[Optional[float]] = mapped_column(Numeric(20, 2))
    basis_effective: Mapped[Optional[float]] = mapped_column(Numeric(20, 2))
    proceeds_effective: Mapped[Optional[float]] = mapped_column(Numeric(20, 2))
    disallowed_loss: Mapped[Optional[float]] = mapped_column(Numeric(20, 2))
    reason_notes: Mapped[Optional[dict[str, Any]]] = mapped_column(JSON)
    ib_transaction_id: Mapped[Optional[str]] = mapped_column(String(64))
    ib_trade_id: Mapped[Optional[str]] = mapped_column(String(64))
    source_file_hash: Mapped[str] = mapped_column(String(100), nullable=False)
    raw_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    created_at: Mapped[dt.datetime] = mapped_column(UTCDateTime(), default=utcnow, nullable=False)

    linked_closure: Mapped[Optional["BrokerLotClosure"]] = relationship(foreign_keys=[linked_closure_id])


class BrokerSymbolSummary(Base):
    __tablename__ = "broker_symbol_summaries"
    __table_args__ = (
        UniqueConstraint("connection_id", "source_file_hash", "source_row", name="uq_broker_symbol_summary"),
        Index("ix_broker_symbol_summary_scope", "connection_id", "provider_account_id", "symbol", "as_of_date"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    connection_id: Mapped[int] = mapped_column(ForeignKey("external_connections.id"), nullable=False)
    provider_account_id: Mapped[str] = mapped_column(String(200), nullable=False)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    as_of_date: Mapped[dt.date] = mapped_column(Date, nullable=False)
    quantity: Mapped[Optional[float]] = mapped_column(Numeric(20, 6))
    cost_basis: Mapped[Optional[float]] = mapped_column(Numeric(20, 2))
    proceeds: Mapped[Optional[float]] = mapped_column(Numeric(20, 2))
    realized_pl: Mapped[Optional[float]] = mapped_column(Numeric(20, 2))
    currency: Mapped[Optional[str]] = mapped_column(String(16))
    source_file_hash: Mapped[str] = mapped_column(String(100), nullable=False)
    source_row: Mapped[int] = mapped_column(Integer, nullable=False)
    raw_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    created_at: Mapped[dt.datetime] = mapped_column(UTCDateTime(), default=utcnow, nullable=False)


class TaxEstimateRun(Base):
    __tablename__ = "tax_estimate_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    created_at: Mapped[dt.datetime] = mapped_column(UTCDateTime(), default=utcnow, nullable=False)
    actor: Mapped[str] = mapped_column(String(200), nullable=False)
    year: Mapped[int] = mapped_column(Integer, nullable=False)
    scope: Mapped[str] = mapped_column(String(32), nullable=False)  # household|trust|personal
    settings_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    results_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    note: Mapped[Optional[str]] = mapped_column(Text)

# --- Reconstructed tax-lot engine tables (planning-grade) ---


class TaxLot(Base):
    __tablename__ = "tax_lots"
    __table_args__ = (Index("ix_tax_lots_scope", "taxpayer_id", "account_id", "security_id"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    taxpayer_id: Mapped[int] = mapped_column(ForeignKey("taxpayer_entities.id"), nullable=False)
    account_id: Mapped[int] = mapped_column(ForeignKey("accounts.id"), nullable=False)
    security_id: Mapped[int] = mapped_column(ForeignKey("securities.id"), nullable=False)
    acquired_date: Mapped[dt.date] = mapped_column(Date, nullable=False)
    quantity_open: Mapped[float] = mapped_column(Numeric(20, 6), nullable=False)
    basis_open: Mapped[Optional[float]] = mapped_column(Numeric(20, 2))
    source: Mapped[str] = mapped_column(String(20), nullable=False, default="RECONSTRUCTED")  # RECONSTRUCTED|AUTHORITATIVE
    created_from_txn_id: Mapped[Optional[int]] = mapped_column(ForeignKey("transactions.id"))
    created_at: Mapped[dt.datetime] = mapped_column(UTCDateTime(), default=utcnow, nullable=False)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)

    taxpayer: Mapped["TaxpayerEntity"] = relationship()
    account: Mapped["Account"] = relationship()
    security: Mapped["Security"] = relationship()
    created_from_txn: Mapped["Transaction"] = relationship(foreign_keys=[created_from_txn_id])


class LotDisposal(Base):
    __tablename__ = "lot_disposals"
    __table_args__ = (Index("ix_lot_disposals_sale", "sell_txn_id"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    sell_txn_id: Mapped[int] = mapped_column(ForeignKey("transactions.id"), nullable=False)
    tax_lot_id: Mapped[int] = mapped_column(ForeignKey("tax_lots.id"), nullable=False)
    quantity_sold: Mapped[float] = mapped_column(Numeric(20, 6), nullable=False)
    proceeds_allocated: Mapped[float] = mapped_column(Numeric(20, 2), nullable=False)
    basis_allocated: Mapped[Optional[float]] = mapped_column(Numeric(20, 2))
    realized_gain: Mapped[Optional[float]] = mapped_column(Numeric(20, 2))
    term: Mapped[str] = mapped_column(String(10), nullable=False, default="—")  # ST|LT|—
    as_of_date: Mapped[dt.date] = mapped_column(Date, nullable=False)
    created_at: Mapped[dt.datetime] = mapped_column(UTCDateTime(), default=utcnow, nullable=False)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)

    sell_txn: Mapped["Transaction"] = relationship(foreign_keys=[sell_txn_id])
    tax_lot: Mapped["TaxLot"] = relationship(foreign_keys=[tax_lot_id])


class WashSaleAdjustment(Base):
    __tablename__ = "wash_sale_adjustments"
    __table_args__ = (Index("ix_wash_sale_loss_sale", "loss_sale_txn_id"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    loss_sale_txn_id: Mapped[int] = mapped_column(ForeignKey("transactions.id"), nullable=False)
    replacement_buy_txn_id: Mapped[Optional[int]] = mapped_column(ForeignKey("transactions.id"))
    replacement_lot_id: Mapped[Optional[int]] = mapped_column(ForeignKey("tax_lots.id"))
    deferred_loss: Mapped[float] = mapped_column(Numeric(20, 2), nullable=False)
    basis_increase: Mapped[float] = mapped_column(Numeric(20, 2), nullable=False, default=0.0)
    window_start: Mapped[dt.date] = mapped_column(Date, nullable=False)
    window_end: Mapped[dt.date] = mapped_column(Date, nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="APPLIED")  # APPLIED|FLAGGED
    created_at: Mapped[dt.datetime] = mapped_column(UTCDateTime(), default=utcnow, nullable=False)
    notes_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)

    loss_sale_txn: Mapped["Transaction"] = relationship(foreign_keys=[loss_sale_txn_id])
    replacement_buy_txn: Mapped["Transaction"] = relationship(foreign_keys=[replacement_buy_txn_id])
    replacement_lot: Mapped["TaxLot"] = relationship(foreign_keys=[replacement_lot_id])


class CorporateActionEvent(Base):
    __tablename__ = "corporate_action_events"
    __table_args__ = (Index("ix_corp_actions_scope", "taxpayer_id", "action_date"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    taxpayer_id: Mapped[int] = mapped_column(ForeignKey("taxpayer_entities.id"), nullable=False)
    account_id: Mapped[Optional[int]] = mapped_column(ForeignKey("accounts.id"))
    security_id: Mapped[Optional[int]] = mapped_column(ForeignKey("securities.id"))
    action_date: Mapped[dt.date] = mapped_column(Date, nullable=False)
    action_type: Mapped[str] = mapped_column(String(30), nullable=False)  # SPLIT/REVERSE_SPLIT/MERGER/...
    ratio: Mapped[Optional[float]] = mapped_column(Numeric(20, 8))
    applied: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    created_at: Mapped[dt.datetime] = mapped_column(UTCDateTime(), default=utcnow, nullable=False)
    apply_notes: Mapped[Optional[str]] = mapped_column(Text)
    details_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)

    taxpayer: Mapped["TaxpayerEntity"] = relationship()
    account: Mapped["Account"] = relationship()
    security: Mapped["Security"] = relationship()
