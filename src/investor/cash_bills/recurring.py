from __future__ import annotations

import datetime as dt
import json
import math
import re
from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Iterable, Optional

from sqlalchemy import func
from sqlalchemy.orm import Session

from src.db.models import ExpenseAccount, ExpenseTransaction, RecurringBill, RecurringBillIgnore, RecurringBillRule


_STOP_TOKENS = {
    "ACH",
    "POS",
    "DEBIT",
    "CREDIT",
    "PAYMENT",
    "PURCHASE",
    "WITHDRAWAL",
    "DEPOSIT",
    "TRANSFER",
    "ONLINE",
    "MOBILE",
    "RECURRING",
    "CARD",
    "CHECK",
    "ATM",
    "AUTOPAY",
    "AUTOPAYMENT",
    "PAYPAL",
    "VENMO",
    "ZELLE",
    "CASHAPP",
}
_TOKEN_RE = re.compile(r"[A-Z0-9]+")
_NON_ALPHA_RE = re.compile(r"[^A-Z ]+")


def _normalize_name(value: str) -> str:
    raw = (value or "").upper()
    raw = _NON_ALPHA_RE.sub(" ", raw)
    tokens = [t for t in _TOKEN_RE.findall(raw) if t and t not in _STOP_TOKENS]
    return " ".join(tokens).strip()


def _last4(value: str | None) -> str:
    digits = "".join(ch for ch in (value or "") if ch.isdigit())
    if len(digits) >= 4:
        return digits[-4:]
    return digits


def _is_checking_account(acct: ExpenseAccount) -> bool:
    t = (acct.type or "").strip().upper()
    return t in {"BANK", "CHECKING", "DEPOSITORY"}


def _scope_norm(value: str | None) -> str:
    s = (value or "").strip().upper() or "PERSONAL"
    return s


def _raw_json_dict(value: object | None) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


def _plaid_account_id_from_txn(txn: ExpenseTransaction | None) -> str | None:
    if txn is None:
        return None
    raw = _raw_json_dict(txn.original_row_json)
    acct = str(raw.get("account_id") or raw.get("provider_account_id") or "").strip()
    return acct or None


def _plaid_id_from_provider(value: str | None) -> str | None:
    if not value:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    if ":" in raw:
        parts = [p for p in raw.split(":") if p]
        if parts:
            return parts[-1]
    return raw


def _is_excluded_transaction(t: ExpenseTransaction) -> bool:
    desc = (t.description_norm or t.description_raw or "").upper()
    cat = (t.category_user or t.category_system or "").upper()
    if any(k in cat for k in ["TRANSFER", "PAYMENT", "CREDIT CARD", "CARD PAYMENT", "MERCHANT CREDIT", "INCOME"]):
        return True
    if any(k in desc for k in ["TRANSFER", "ZELLE", "VENMO", "PAYPAL", "CARD PAYMENT", "AUTOPAY", "THANK YOU", "ATM"]):
        return True
    if "BANKLINK" in desc and "ACH" in desc:
        return True
    return False


def _best_display_name(rows: Iterable[ExpenseTransaction]) -> str:
    def _clean(value: str | None) -> str:
        if not value:
            return ""
        cleaned = value.strip()
        if not cleaned:
            return ""
        if cleaned.lower() == "unknown":
            return ""
        return cleaned

    def _rank(s: str) -> int:
        if not s or s == "UNKNOWN":
            return 0
        has_upper = any(ch.isalpha() and ch.isupper() for ch in s)
        has_lower = any(ch.isalpha() and ch.islower() for ch in s)
        if has_upper and has_lower:
            return 3
        if has_upper:
            return 2
        return 1

    best = "Unknown"
    for t in rows:
        candidate = (
            _clean(t.merchant_norm)
            or _clean(t.description_norm)
            or _clean(t.description_raw)
        )
        if _rank(candidate) > _rank(best):
            best = candidate
    return best or "Unknown"


def _merchant_rule(t: ExpenseTransaction) -> tuple[str, str]:
    raw = t.original_row_json or {}
    merchant_id = ""
    if isinstance(raw, dict):
        merchant_id = str(raw.get("merchant_id") or "").strip()
    if merchant_id:
        return "PLAID_MERCHANT_ID", merchant_id
    name = (
        (t.merchant_norm or "").strip()
        if (t.merchant_norm or "").strip().lower() != "unknown"
        else ""
    )
    if not name:
        name = (
            (t.description_norm or "").strip()
            if (t.description_norm or "").strip().lower() != "unknown"
            else ""
        )
    if not name:
        name = (
            (t.description_raw or "").strip()
            if (t.description_raw or "").strip().lower() != "unknown"
            else ""
        )
    return "NAME_NORMALIZED", _normalize_name(name)


def _merchant_rule_from_text(text: str) -> tuple[str, str]:
    return "NAME_NORMALIZED", _normalize_name(text or "")


def _monthly_consistency(dates: list[dt.date]) -> float:
    if len(dates) < 2:
        return 0.0
    deltas = [(dates[i] - dates[i - 1]).days for i in range(1, len(dates))]
    hits = sum(1 for d in deltas if 28 <= d <= 33)
    return hits / max(1, len(deltas))


def _infer_due_day(dates: list[dt.date]) -> int | None:
    if not dates:
        return None
    counts: dict[int, int] = defaultdict(int)
    for d in dates:
        counts[d.day] += 1
    day, freq = sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))[0]
    if freq / len(dates) >= 0.5:
        return int(day)
    return None


def _amount_stats(amounts: list[Decimal]) -> dict[str, Any]:
    vals = [float(a) for a in amounts if a is not None]
    if not vals:
        return {"mean": 0.0, "cv": 0.0, "min": None, "max": None}
    mean = sum(vals) / len(vals)
    if mean <= 0:
        return {"mean": mean, "cv": 0.0, "min": min(vals), "max": max(vals)}
    var = sum((v - mean) ** 2 for v in vals) / len(vals)
    std = math.sqrt(var)
    return {"mean": mean, "cv": std / mean, "min": min(vals), "max": max(vals)}


def _amount_mode(stats: dict[str, Any]) -> str:
    cv = float(stats.get("cv") or 0.0)
    if cv <= 0.05:
        return "FIXED"
    if cv <= 0.20:
        return "RANGE"
    return "VARIABLE"


def _month_list(as_of: dt.date, months: int) -> list[tuple[int, int]]:
    months_i = max(1, int(months or 6))
    year = as_of.year
    month = as_of.month
    items: list[tuple[int, int]] = []
    for _ in range(months_i):
        items.append((year, month))
        month -= 1
        if month <= 0:
            month = 12
            year -= 1
    return items


def monthly_deposits_summary(
    *,
    session: Session,
    scope: str,
    as_of: dt.date,
    months: int = 6,
    account_id: int | None = None,
) -> dict[str, Any]:
    scope_u = _scope_norm(scope)
    acct_q = session.query(ExpenseAccount).filter(ExpenseAccount.type.in_(["BANK", "CHECKING", "DEPOSITORY"]))
    if scope_u != "ALL":
        acct_q = acct_q.filter(ExpenseAccount.scope == scope_u)
    accounts = acct_q.order_by(ExpenseAccount.institution.asc(), ExpenseAccount.name.asc()).all()
    account_ids = [a.id for a in accounts if _is_checking_account(a)]
    if not account_ids:
        return {
            "as_of": as_of.isoformat(),
            "months": months,
            "accounts": [],
            "selected_account_id": None,
            "monthly": [],
        }

    selected_id = account_id if account_id in account_ids else account_ids[0]
    months_list = _month_list(as_of, months)
    start_year, start_month = months_list[-1]
    start_date = dt.date(int(start_year), int(start_month), 1)

    rows = (
        session.query(
            func.strftime("%Y", ExpenseTransaction.posted_date).label("year"),
            func.strftime("%m", ExpenseTransaction.posted_date).label("month"),
            func.sum(ExpenseTransaction.amount).label("amount"),
        )
        .filter(
            ExpenseTransaction.expense_account_id == selected_id,
            ExpenseTransaction.posted_date >= start_date,
            ExpenseTransaction.posted_date <= as_of,
            ExpenseTransaction.amount > 0,
        )
        .group_by("year", "month")
        .all()
    )
    totals: dict[tuple[int, int], float] = {}
    for row in rows:
        try:
            year = int(row.year)
            month = int(row.month)
        except Exception:
            continue
        totals[(year, month)] = float(row.amount or 0)

    monthly: list[dict[str, Any]] = []
    for year, month in months_list:
        amount = totals.get((year, month), 0.0)
        if amount == 0:
            continue
        monthly.append({"year": int(year), "month": int(month), "amount": float(amount)})

    account_rows: list[dict[str, Any]] = []
    for acct in accounts:
        last4 = _last4(acct.last4_masked)
        label = f"{acct.name} • {last4}" if last4 else acct.name
        account_rows.append(
            {
                "id": int(acct.id),
                "name": acct.name,
                "last4": last4,
                "label": label,
                "scope": acct.scope or "PERSONAL",
            }
        )

    return {
        "as_of": as_of.isoformat(),
        "months": months,
        "accounts": account_rows,
        "selected_account_id": int(selected_id),
        "monthly": monthly,
    }


def detect_suggestions(
    *,
    session: Session,
    scope: str,
    as_of: dt.date,
    lookback_days: int = 365,
    min_occurrences: int = 3,
) -> list[dict[str, Any]]:
    scope_u = _scope_norm(scope)
    start = as_of - dt.timedelta(days=lookback_days)
    acct_q = session.query(ExpenseAccount).filter(ExpenseAccount.type.in_(["BANK", "CHECKING", "DEPOSITORY"]))
    if scope_u != "ALL":
        acct_q = acct_q.filter(ExpenseAccount.scope == scope_u)
    accounts = acct_q.all()
    account_ids = [a.id for a in accounts if _is_checking_account(a)]
    if not account_ids:
        return []
    ignore = {
        (i.rule_type, i.rule_value)
        for i in session.query(RecurringBillIgnore)
        .filter(RecurringBillIgnore.scope == scope_u)
        .all()
    }
    active_rules = {
        (r.rule_type, r.rule_value)
        for r in session.query(RecurringBillRule)
        .join(RecurringBill, RecurringBillRule.recurring_bill_id == RecurringBill.id)
        .filter(RecurringBill.is_active.is_(True))
        .all()
    }

    rows = (
        session.query(ExpenseTransaction)
        .filter(
            ExpenseTransaction.expense_account_id.in_(account_ids),
            ExpenseTransaction.posted_date >= start,
            ExpenseTransaction.posted_date <= as_of,
        )
        .order_by(ExpenseTransaction.posted_date.asc(), ExpenseTransaction.id.asc())
        .all()
    )
    grouped: dict[tuple[str, str], list[ExpenseTransaction]] = defaultdict(list)
    account_ids_by_group: dict[tuple[str, str], set[int]] = defaultdict(set)
    for t in rows:
        if t.amount >= 0:
            continue
        if _is_excluded_transaction(t):
            continue
        rule_type, rule_value = _merchant_rule(t)
        if not rule_value:
            continue
        key = (rule_type, rule_value)
        if key in ignore or key in active_rules:
            continue
        grouped[key].append(t)
        account_ids_by_group[key].add(int(t.expense_account_id))

    out: list[dict[str, Any]] = []
    for (rule_type, rule_value), txns in grouped.items():
        if len(txns) < min_occurrences:
            continue
        dates = sorted({t.posted_date for t in txns})
        if len(dates) < min_occurrences:
            continue
        cadence = _monthly_consistency(dates)
        if cadence < 0.6:
            continue
        amounts = [Decimal(str(abs(t.amount))) for t in txns]
        stats = _amount_stats(amounts)
        mode = _amount_mode(stats)
        due_day = _infer_due_day(dates)
        score = 0.0
        if rule_type == "PLAID_MERCHANT_ID":
            score += 0.35
        score += 0.35 * min(1.0, cadence)
        if mode == "FIXED":
            score += 0.20
        elif mode == "RANGE":
            score += 0.10
        if due_day:
            score += 0.10
        score = max(0.0, min(1.0, score))
        last_seen = max(dates) if dates else None
        display = _best_display_name(txns)
        latest = max(txns, key=lambda t: (t.posted_date, t.id))
        desc_sample = (latest.description_raw or latest.description_norm or "").strip()
        acct_ids = account_ids_by_group.get((rule_type, rule_value), set())
        source_account_id = next(iter(acct_ids)) if len(acct_ids) == 1 else None
        out.append(
            {
                "key": {"rule_type": rule_type, "rule_value": rule_value},
                "name": display,
                "merchant_display": display,
                "description_sample": desc_sample or None,
                "amount_mode": mode,
                "amount_expected": float(stats.get("mean") or 0.0) if mode == "FIXED" else None,
                "amount_min": float(stats.get("min")) if mode == "RANGE" else None,
                "amount_max": float(stats.get("max")) if mode == "RANGE" else None,
                "due_day_of_month": due_day,
                "confidence": round(score, 3),
                "last_seen_date": last_seen.isoformat() if last_seen else None,
                "occurrences": len(txns),
                "source_account_id": source_account_id,
            }
        )
    out.sort(key=lambda r: (-float(r.get("confidence") or 0), -(r.get("occurrences") or 0), r.get("name") or ""))
    return out[:50]


def _expected_display(bill: RecurringBill) -> str:
    if (bill.amount_mode or "").upper() == "FIXED" and bill.amount_expected is not None:
        return f"{float(bill.amount_expected):.2f}"
    if (bill.amount_mode or "").upper() == "RANGE" and (bill.amount_min is not None or bill.amount_max is not None):
        if bill.amount_min is None:
            return f"≤{float(bill.amount_max):.2f}"
        if bill.amount_max is None:
            return f"≥{float(bill.amount_min):.2f}"
        return f"{float(bill.amount_min):.2f}-{float(bill.amount_max):.2f}"
    return "Varies"


def _expected_amount_for_total(bill: dict[str, Any]) -> float:
    if bill.get("amount_mode") == "FIXED" and bill.get("amount_expected") is not None:
        return float(bill["amount_expected"])
    if bill.get("amount_mode") == "RANGE":
        if bill.get("amount_max") is not None:
            return float(bill["amount_max"])
        if bill.get("amount_min") is not None:
            return float(bill["amount_min"])
    if bill.get("last_payment_amount") is not None:
        return float(bill["last_payment_amount"])
    return 0.0


def _due_date_for_month(year: int, month: int, day: int) -> dt.date:
    # Cap to last day of month.
    try:
        return dt.date(year, month, day)
    except ValueError:
        last = dt.date(year, month + 1, 1) - dt.timedelta(days=1) if month < 12 else dt.date(year, 12, 31)
        return last


def active_bills_summary(
    *,
    session: Session,
    scope: str,
    as_of: dt.date,
    include_inactive: bool = False,
) -> dict[str, Any]:
    scope_u = _scope_norm(scope)
    acct_q = session.query(ExpenseAccount).filter(ExpenseAccount.type.in_(["BANK", "CHECKING", "DEPOSITORY"]))
    if scope_u != "ALL":
        acct_q = acct_q.filter(ExpenseAccount.scope == scope_u)
    accounts = {a.id: a for a in acct_q.all() if _is_checking_account(a)}
    account_ids = list(accounts.keys())

    bill_q = session.query(RecurringBill)
    if not include_inactive:
        bill_q = bill_q.filter(RecurringBill.is_active.is_(True))
    if scope_u != "ALL":
        bill_q = bill_q.filter(RecurringBill.scope.in_([scope_u, "ALL"]))
    bills = bill_q.all()
    rules_by_bill: dict[int, list[RecurringBillRule]] = defaultdict(list)
    if bills:
        for r in session.query(RecurringBillRule).filter(RecurringBillRule.recurring_bill_id.in_([b.id for b in bills])).all():
            rules_by_bill[int(r.recurring_bill_id)].append(r)

    # Prefetch recent transactions for matching (90 days).
    lookback = as_of - dt.timedelta(days=120)
    tx_q = session.query(ExpenseTransaction).filter(ExpenseTransaction.posted_date >= lookback)
    if account_ids:
        tx_q = tx_q.filter(ExpenseTransaction.expense_account_id.in_(account_ids))
    txns = tx_q.order_by(ExpenseTransaction.posted_date.desc(), ExpenseTransaction.id.desc()).all()

    def _match_rule(t: ExpenseTransaction, rule: RecurringBillRule) -> bool:
        if rule.rule_type == "PLAID_MERCHANT_ID":
            raw = t.original_row_json or {}
            if isinstance(raw, dict):
                return str(raw.get("merchant_id") or "").strip() == rule.rule_value
            return False
        if rule.rule_type == "NAME_NORMALIZED":
            name = t.merchant_norm or t.description_norm or t.description_raw or ""
            return _normalize_name(name) == rule.rule_value
        return False

    out: list[dict[str, Any]] = []
    for b in bills:
        b_scope = _scope_norm(b.scope)
        if scope_u != "ALL" and b_scope not in {scope_u, "ALL"}:
            continue
        bill_rules = rules_by_bill.get(int(b.id), [])
        if not bill_rules:
            continue
        candidate_txns = []
        for t in txns:
            if b.source_account_id and int(t.expense_account_id) != int(b.source_account_id):
                continue
            if t.amount >= 0:
                continue
            if _is_excluded_transaction(t):
                continue
            if any(_match_rule(t, r) for r in bill_rules):
                candidate_txns.append(t)
        last_payment = candidate_txns[0] if candidate_txns else None
        last_payment_date = last_payment.posted_date.isoformat() if last_payment else None
        last_payment_amount = float(abs(last_payment.amount)) if last_payment else None
        merchant_display = _best_display_name(candidate_txns) if candidate_txns else b.name
        desc_sample = ""
        if last_payment:
            desc_sample = (last_payment.description_raw or last_payment.description_norm or "").strip()

        due_day = b.due_day_of_month if b.due_day_of_month and b.due_day_of_month > 0 else None
        if not due_day and last_payment:
            due_day = last_payment.posted_date.day
        due_date = None
        status = "unknown"
        if due_day:
            due_date = _due_date_for_month(as_of.year, as_of.month, int(due_day))
            paid_in_cycle = False
            if last_payment and last_payment.posted_date.year == as_of.year and last_payment.posted_date.month == as_of.month:
                paid_in_cycle = True
            if paid_in_cycle:
                status = "paid"
            else:
                days = (due_date - as_of).days
                if days < 0:
                    status = "overdue"
                elif days <= 7:
                    status = "due_soon"
                else:
                    status = "upcoming"

        source_name = None
        if b.source_account_id and b.source_account_id in accounts:
            source_name = accounts[b.source_account_id].name
        out.append(
            {
                "id": int(b.id),
                "name": b.name,
                "merchant_display": merchant_display,
                "description_sample": desc_sample or None,
                "scope": b_scope,
                "is_active": bool(b.is_active),
                "source_account_id": int(b.source_account_id) if b.source_account_id else None,
                "source_account_name": source_name,
                "amount_mode": (b.amount_mode or "VARIABLE").upper(),
                "amount_expected": float(b.amount_expected) if b.amount_expected is not None else None,
                "amount_min": float(b.amount_min) if b.amount_min is not None else None,
                "amount_max": float(b.amount_max) if b.amount_max is not None else None,
                "due_day_of_month": int(due_day) if due_day else None,
                "due_date": due_date.isoformat() if due_date else None,
                "last_payment_date": last_payment_date,
                "last_payment_amount": last_payment_amount,
                "status": status,
                "expected_display": _expected_display(b),
            }
        )

    return {"bills": out}


def recent_charges(
    *,
    session: Session,
    scope: str,
    as_of: dt.date,
    lookback_days: int = 30,
    limit: int = 500,
) -> dict[str, Any]:
    scope_u = _scope_norm(scope)
    start = as_of - dt.timedelta(days=lookback_days)
    acct_q = session.query(ExpenseAccount).filter(ExpenseAccount.type.in_(["BANK", "CHECKING", "DEPOSITORY"]))
    if scope_u != "ALL":
        acct_q = acct_q.filter(ExpenseAccount.scope == scope_u)
    accounts = {a.id: a for a in acct_q.all() if _is_checking_account(a)}
    account_ids = list(accounts.keys())
    if not account_ids:
        return {"charges": []}

    rows = (
        session.query(ExpenseTransaction)
        .filter(
            ExpenseTransaction.expense_account_id.in_(account_ids),
            ExpenseTransaction.posted_date >= start,
            ExpenseTransaction.posted_date <= as_of,
        )
        .order_by(ExpenseTransaction.posted_date.desc(), ExpenseTransaction.id.desc())
        .limit(limit)
        .all()
    )
    out: list[dict[str, Any]] = []
    for t in rows:
        if t.amount >= 0:
            continue
        if _is_excluded_transaction(t):
            continue
        rule_type, rule_value = _merchant_rule(t)
        if not rule_value:
            rule_type, rule_value = _merchant_rule_from_text(t.description_norm or t.description_raw or "")
        if not rule_value:
            continue
        merchant_display = (t.merchant_norm or t.description_norm or t.description_raw or "").strip() or "Unknown"
        desc_sample = (t.description_raw or t.description_norm or "").strip()
        acct = accounts.get(int(t.expense_account_id))
        acct_name = acct.name if acct else (t.account_name or None)
        acct_mask = getattr(acct, "last4_masked", None) if acct else None
        if not acct_mask:
            acct_mask = getattr(t, "account_last4_masked", None)
        plaid_account_id = _plaid_account_id_from_txn(t)
        if not plaid_account_id and acct is not None:
            plaid_account_id = _plaid_id_from_provider(getattr(acct, "provider_account_id", None))
        out.append(
            {
                "id": int(t.id),
                "posted_date": t.posted_date.isoformat(),
                "amount": float(abs(t.amount)),
                "merchant_display": merchant_display,
                "description_sample": desc_sample or None,
                "rule_type": rule_type,
                "rule_value": rule_value,
                "source_account_id": int(t.expense_account_id),
                "source_account_name": acct_name,
                "source_account_mask": acct_mask,
                "plaid_account_id": plaid_account_id,
            }
        )
    return {"charges": out}


def recurring_due_total(
    bills: list[dict[str, Any]],
    *,
    as_of: dt.date,
    range_days: int,
) -> float:
    total = 0.0
    for b in bills:
        status = b.get("status") or "unknown"
        if status == "paid":
            continue
        due = b.get("due_date")
        if not due:
            continue
        due_date = dt.date.fromisoformat(str(due)[:10])
        days = (due_date - as_of).days
        if abs(days) > range_days:
            continue
        total += _expected_amount_for_total(b)
    return total
