from __future__ import annotations

import datetime as dt

from src.investor.cash_bills.types import BillStatus, CashAccountRow, CreditCardBillRow, DashboardSummary, parse_date


def derive_status(bill: CreditCardBillRow, as_of: dt.date) -> BillStatus:
    if bill.get("status") == "paid":
        return "paid"
    due = parse_date(bill["due_date"])
    days = (due - as_of).days
    if days < 0:
        return "overdue"
    if days <= 7:
        return "due_soon"
    return "upcoming"


def within_range(days_to_due: int, range_days: int, status: BillStatus) -> bool:
    if status == "overdue":
        return days_to_due < 0 and abs(days_to_due) <= range_days
    if status in {"due_soon", "upcoming"}:
        return 0 <= days_to_due <= range_days
    return abs(days_to_due) <= range_days


def filter_bills(
    bills: list[CreditCardBillRow],
    *,
    as_of: dt.date,
    range_days: int,
    status_filter: BillStatus | "all",
) -> list[CreditCardBillRow]:
    out: list[CreditCardBillRow] = []
    for b in bills:
        status = derive_status(b, as_of)
        due = parse_date(b["due_date"])
        days = (due - as_of).days
        if status_filter != "all" and status != status_filter:
            continue
        if not within_range(days, range_days, status if status_filter == "all" else status):
            continue
        out.append(b)
    return sorted(out, key=lambda r: r["due_date"])


def compute_summary(
    bills: list[CreditCardBillRow],
    cash_accounts: list[CashAccountRow],
    *,
    as_of: dt.date,
    range_days: int,
) -> DashboardSummary:
    def _liquidity_amount(bill: CreditCardBillRow) -> float:
        val = bill.get("interest_saving_balance")
        if val is not None and float(val) > 0:
            return float(val)
        return float(bill.get("statement_balance") or 0.0)

    cash_total = 0.0
    for c in cash_accounts:
        val = c.get("available_balance")
        if val is None:
            val = c.get("current_balance") or 0.0
        cash_total += float(val or 0.0)

    due_total = 0.0
    next_due_date: dt.date | None = None
    next_due_amount: float | None = None
    next_due_card: str | None = None
    for b in bills:
        status = derive_status(b, as_of)
        if status == "paid":
            continue
        due = parse_date(b["due_date"])
        days = (due - as_of).days
        if abs(days) > range_days:
            continue
        due_total += _liquidity_amount(b)
        if next_due_date is None or due < next_due_date:
            next_due_date = due
            next_due_amount = _liquidity_amount(b)
            next_due_card = b.get("card_name")

    return {
        "cash_available_total": cash_total,
        "checking_accounts_count": len(cash_accounts),
        "cc_due_total_30d": due_total,
        "net_after_bills_30d": cash_total - due_total,
        "next_due_amount": next_due_amount,
        "next_due_date": next_due_date.isoformat() if next_due_date else None,
        "next_due_card": next_due_card,
    }


def coverage_status(cash_total: float, bills_total: float) -> str:
    if bills_total <= 0:
        return "covered"
    ratio = cash_total / bills_total if bills_total else 0.0
    if ratio < 1.0:
        return "shortfall"
    if ratio <= 1.1:
        return "tight"
    return "covered"
