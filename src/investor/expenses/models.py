from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Optional

from pydantic import BaseModel, Field


@dataclass(frozen=True)
class RawTxn:
    posted_date: dt.date
    transaction_date: Optional[dt.date]
    description: str
    amount: Decimal  # debit negative, credit positive
    currency: str
    category_hint: Optional[str]
    external_id: Optional[str]
    raw: dict[str, Any]
    account_last4: Optional[str] = None  # masked last4 only
    cardholder_name: Optional[str] = None


class ImportFileResult(BaseModel):
    file_name: str
    file_hash: str
    format_name: str
    institution: str
    account_name: str
    row_count: int
    inserted: int
    duplicates_skipped: int
    fuzzy_duplicates_skipped: int = 0
    parse_fail_count: int = 0
    warnings: list[str] = Field(default_factory=list)


class CategorizeResult(BaseModel):
    updated: int
    skipped_user_categorized: int
    rules_path: str


class ReportRow(BaseModel):
    key: str
    spend: Decimal
    income: Decimal
    net: Decimal
    txn_count: int = 0


class BudgetRow(BaseModel):
    category: str
    spend: Decimal
    budget: Decimal
    over_under: Decimal


class MerchantSpendRow(BaseModel):
    merchant: str
    spend: Decimal
    txn_count: int
    category: str = "Unknown"


class CardholderSpendRow(BaseModel):
    cardholder: str
    spend: Decimal
    txn_count: int


class RecurringItem(BaseModel):
    merchant: str
    amount: Decimal
    occurrences: int
    months_present: int
    cadence: str  # WEEKLY|MONTHLY|QUARTERLY|SEMIANNUAL|ANNUAL|UNKNOWN
    category: Optional[str] = None
    monthly_equivalent: Decimal = Decimal("0.00")
