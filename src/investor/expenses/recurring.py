from __future__ import annotations

import datetime as dt
from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

from sqlalchemy.orm import Session

from src.db.models import ExpenseTransaction
from src.investor.expenses.models import RecurringItem
from src.investor.expenses.merchant_settings import merchant_key
from src.investor.expenses.normalize import money_2dp


@dataclass(frozen=True)
class _Txn:
    posted_date: dt.date
    merchant: str
    amount: Decimal
    category: Optional[str]


def _year_range(year: int) -> tuple[dt.date, dt.date]:
    return dt.date(year, 1, 1), dt.date(year, 12, 31)


def _cadence(dates: list[dt.date]) -> str:
    if len(dates) < 3:
        return "UNKNOWN"
    deltas = sorted((dates[i] - dates[i - 1]).days for i in range(1, len(dates)))
    if all(26 <= d <= 34 for d in deltas):
        return "MONTHLY"
    if all(6 <= d <= 8 for d in deltas):
        return "WEEKLY"
    return "UNKNOWN"


def _effective_category(t: ExpenseTransaction) -> str:
    c = (t.category_user or t.category_system or "").strip()
    return c if c else "Unknown"


def _is_excluded_category(category: str) -> bool:
    s = (category or "").strip().casefold()
    if not s:
        return False
    if s in {"transfers", "merchant credits", "income", "payments"}:
        return True
    if s in {"credit card payment", "card payment", "cc payment"}:
        return True
    if ("payment" in s) and any(k in s for k in ["credit card", "card", "cc"]):
        return True
    return False


def _median(values: list[Decimal]) -> Decimal:
    if not values:
        return Decimal("0.00")
    s = sorted(values)
    n = len(s)
    mid = n // 2
    if n % 2 == 1:
        return s[mid]
    return money_2dp((s[mid - 1] + s[mid]) / Decimal("2"))


def _monthly_equivalent(*, amount: Decimal, cadence: str) -> Decimal:
    a = money_2dp(amount)
    if cadence == "WEEKLY":
        return money_2dp(a * Decimal("52") / Decimal("12"))
    if cadence == "QUARTERLY":
        return money_2dp(a * Decimal("4") / Decimal("12"))
    if cadence == "SEMIANNUAL":
        return money_2dp(a * Decimal("2") / Decimal("12"))
    if cadence == "ANNUAL":
        return money_2dp(a / Decimal("12"))
    return a


def detect_recurring(
    *,
    session: Session,
    year: int,
    min_months: int = 3,
    include_income: bool = False,
) -> list[RecurringItem]:
    from src.db.models import ExpenseMerchantSetting

    start, end = _year_range(year)
    settings = {s.merchant_key: s for s in session.query(ExpenseMerchantSetting).all()}
    q = (
        session.query(ExpenseTransaction)
        .filter(ExpenseTransaction.posted_date >= start, ExpenseTransaction.posted_date <= end)
        .order_by(ExpenseTransaction.posted_date.asc(), ExpenseTransaction.id.asc())
    )
    txns: list[_Txn] = []
    for t in q:
        amt = Decimal(str(t.amount))
        if not include_income and amt > 0:
            continue
        cat = _effective_category(t)
        if _is_excluded_category(cat):
            continue
        txns.append(
            _Txn(
                posted_date=t.posted_date,
                merchant=t.merchant_norm or "Unknown",
                amount=money_2dp(amt.copy_abs()),
                category=(cat if cat != "Unknown" else None),
            )
        )

    def _rank(display: str) -> int:
        s = (display or "").strip()
        if not s or s == "Unknown":
            return 0
        has_upper = any(ch.isalpha() and ch.isupper() for ch in s)
        has_lower = any(ch.isalpha() and ch.islower() for ch in s)
        if has_upper and has_lower:
            return 3
        if has_upper and not has_lower:
            return 2
        return 1

    by_merchant: dict[str, list[_Txn]] = defaultdict(list)
    best_display: dict[str, str] = {}
    for t in txns:
        k = merchant_key(t.merchant) or "unknown"
        by_merchant[k].append(t)
        cur = best_display.get(k, "Unknown")
        if _rank(t.merchant) > _rank(cur):
            best_display[k] = t.merchant

    out: list[RecurringItem] = []
    for k, rows in by_merchant.items():
        s = settings.get(k)
        manual = bool(s and s.recurring_enabled)
        min_required = 1 if manual else min_months
        if len(rows) < min_required:
            continue
        months = {(r.posted_date.year, r.posted_date.month) for r in rows}
        if len(months) < min_required:
            continue
        dates = sorted({r.posted_date for r in rows})
        cadence = _cadence(dates)
        if manual and s and (s.cadence or "").strip().upper() != "UNKNOWN":
            cadence = (s.cadence or "").strip().upper()
        typical = _median([r.amount for r in rows])

        by_cat: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
        for r in rows:
            if r.category:
                by_cat[r.category] += r.amount
        category = None
        if by_cat:
            category = sorted(by_cat.items(), key=lambda kv: (-float(kv[1]), kv[0]))[0][0]

        out.append(
            RecurringItem(
                merchant=best_display.get(k, "Unknown"),
                amount=typical,
                occurrences=len(rows),
                months_present=len(months),
                cadence=cadence,
                category=category,
                monthly_equivalent=_monthly_equivalent(amount=typical, cadence=cadence),
            )
        )

    out.sort(key=lambda r: (-r.months_present, -float(r.monthly_equivalent), r.merchant))
    return out
