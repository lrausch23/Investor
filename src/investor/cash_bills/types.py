from __future__ import annotations

import datetime as dt
from typing import Any, Literal, Optional, TypedDict


BillStatus = Literal["overdue", "due_soon", "upcoming", "paid"]
AutopayStatus = Literal["on", "off", "unknown"]


class CreditCardBillRow(TypedDict):
    id: str
    card_name: str
    issuer: Optional[str]
    last4: Optional[str]
    due_date: str
    current_balance: Optional[float]
    statement_balance: float
    interest_saving_balance: Optional[float]
    pay_over_time: Optional[dict[str, Any]]
    minimum_due: Optional[float]
    last_payment_date: Optional[str]
    last_payment_amount: Optional[float]
    last_statement_issue_date: Optional[str]
    balance_subject_to_apr: Optional[float]
    interest_charge_amount: Optional[float]
    aprs_count: Optional[int]
    autopay: AutopayStatus
    status: BillStatus


class CashAccountRow(TypedDict):
    id: str
    account_name: str
    available_balance: float
    current_balance: Optional[float]
    last_updated: Optional[str]


class DashboardSummary(TypedDict):
    cash_available_total: float
    checking_accounts_count: int
    cc_due_total_30d: float
    net_after_bills_30d: float
    next_due_amount: Optional[float]
    next_due_date: Optional[str]
    next_due_card: Optional[str]


class CashBillsDashboard(TypedDict, total=False):
    as_of: str
    bills: list[CreditCardBillRow]
    cash_accounts: list[CashAccountRow]
    error: str


def parse_date(value: str | dt.date) -> dt.date:
    if isinstance(value, dt.date):
        return value
    return dt.date.fromisoformat(value[:10])
