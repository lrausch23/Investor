from __future__ import annotations

import datetime as dt
from collections import defaultdict
import json
from decimal import Decimal
from typing import Any, Iterable

from sqlalchemy import and_
from sqlalchemy.orm import Session

from src.db.models import (
    ExpenseAccount,
    ExpenseTransaction,
    ExternalConnection,
    RecurringCardCharge,
    RecurringCardChargeIgnore,
    RecurringCardChargeRule,
    TaxpayerEntity,
)
from src.investor.cash_bills.recurring import (
    _amount_mode,
    _amount_stats,
    _best_display_name,
    _infer_due_day,
    _is_excluded_transaction,
    _merchant_rule,
    _merchant_rule_from_text,
    _monthly_consistency,
    _due_date_for_month,
    _scope_norm,
)


def _is_credit_account(acct: ExpenseAccount) -> bool:
    return (acct.type or "").upper() in {"CREDIT", "CARD"}

def _raw_json_dict(value: Any) -> dict[str, Any]:
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


def _plaid_item_id_from_provider(value: str | None) -> str | None:
    if not value:
        return None
    raw = str(value).strip()
    if not raw or ":" not in raw:
        return None
    parts = [p for p in raw.split(":") if p]
    if len(parts) >= 3:
        return parts[1]
    return None


def _plaid_item_owner_map(session: Session) -> dict[str, str]:
    rows = (
        session.query(ExternalConnection, TaxpayerEntity)
        .join(TaxpayerEntity, ExternalConnection.taxpayer_entity_id == TaxpayerEntity.id)
        .filter(ExternalConnection.connector.in_(["CHASE_PLAID", "AMEX_PLAID"]))
        .all()
    )
    out: dict[str, str] = {}
    for conn, entity in rows:
        item_id = str((conn.metadata_json or {}).get("plaid_item_id") or "").strip()
        if item_id:
            out[item_id] = entity.name
    return out


def _cardholder_from_account(account: ExpenseAccount | None, item_owner_map: dict[str, str]) -> str | None:
    if account is None:
        return None
    item_id = _plaid_item_id_from_provider(getattr(account, "provider_account_id", None))
    if not item_id:
        return None
    return item_owner_map.get(item_id)


def _cardholder_with_source(
    txn: ExpenseTransaction | None,
    account: ExpenseAccount | None,
    item_owner_map: dict[str, str],
) -> tuple[str | None, str | None]:
    if txn is None:
        owner = _cardholder_from_account(account, item_owner_map)
        if owner:
            return owner, "Plaid"
        return None, None
    raw = _raw_json_dict(txn.original_row_json)
    raw_name = (
        str(raw.get("authorized_user") or raw.get("authorized_user_name") or raw.get("account_owner") or "")
        .strip()
        or None
    )
    if raw_name:
        return raw_name, "Plaid"
    plaid_marker = any(raw.get(k) for k in ("plaid_transaction_id", "pending_transaction_id", "account_id", "provider_account_id"))
    stored = (txn.cardholder_name or "").strip() or None
    if stored:
        return stored, "Plaid" if plaid_marker else "Manual"
    owner = _cardholder_from_account(account, item_owner_map)
    if owner:
        return owner, "Plaid"
    return None, None


def detect_card_suggestions(
    *,
    session: Session,
    scope: str,
    as_of: dt.date,
    lookback_days: int = 365,
    min_occurrences: int = 3,
) -> list[dict[str, Any]]:
    scope_u = _scope_norm(scope)
    start = as_of - dt.timedelta(days=lookback_days)
    item_owner_map = _plaid_item_owner_map(session)
    acct_q = session.query(ExpenseAccount).filter(ExpenseAccount.type.in_(["CREDIT", "CARD"]))
    if scope_u != "ALL":
        acct_q = acct_q.filter(ExpenseAccount.scope == scope_u)
    accounts = acct_q.all()
    accounts_by_id = {a.id: a for a in accounts}
    account_ids = [a.id for a in accounts if _is_credit_account(a)]
    if not account_ids:
        return []
    ignore = {
        (i.rule_type, i.rule_value)
        for i in session.query(RecurringCardChargeIgnore)
        .filter(RecurringCardChargeIgnore.scope == scope_u)
        .all()
    }
    active_rules = {
        (r.rule_type, r.rule_value)
        for r in session.query(RecurringCardChargeRule)
        .join(RecurringCardCharge, RecurringCardChargeRule.recurring_card_charge_id == RecurringCardCharge.id)
        .filter(RecurringCardCharge.is_active.is_(True))
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
            rule_type, rule_value = _merchant_rule_from_text(t.description_norm or t.description_raw or "")
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
        if (display or "").strip().lower() == "unknown" and desc_sample:
            display = desc_sample
        plaid_account_id = _plaid_account_id_from_txn(latest)
        acct_ids = account_ids_by_group.get((rule_type, rule_value), set())
        source_account_id = next(iter(acct_ids)) if len(acct_ids) == 1 else None
        account = accounts_by_id.get(source_account_id) if source_account_id else None
        if not plaid_account_id and account is not None:
            plaid_account_id = _plaid_id_from_provider(getattr(account, "provider_account_id", None))
        acct_name = account.name if account else (latest.account_name if latest else None)
        acct_mask = getattr(account, "last4_masked", None) if account else None
        if not acct_mask and latest is not None:
            acct_mask = getattr(latest, "account_last4_masked", None)
        cardholder, cardholder_source = _cardholder_with_source(latest, account, item_owner_map)
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
                "source_account_name": acct_name,
                "source_account_mask": acct_mask,
                "plaid_account_id": plaid_account_id,
                "cardholder_name": cardholder,
                "cardholder_source": cardholder_source,
            }
        )
    out.sort(key=lambda r: (-float(r.get("confidence") or 0), -(r.get("occurrences") or 0), r.get("name") or ""))
    return out[:50]


def _expected_display(charge: RecurringCardCharge) -> str:
    if (charge.amount_mode or "").upper() == "FIXED" and charge.amount_expected is not None:
        return f"{float(charge.amount_expected):.2f}"
    if (charge.amount_mode or "").upper() == "RANGE" and (charge.amount_min is not None or charge.amount_max is not None):
        if charge.amount_min is None:
            return f"≤{float(charge.amount_max):.2f}"
        if charge.amount_max is None:
            return f"≥{float(charge.amount_min):.2f}"
        return f"{float(charge.amount_min):.2f}–{float(charge.amount_max):.2f}"
    return "Varies"


def _is_finance_charge_txn(txn: ExpenseTransaction) -> bool:
    desc = (txn.description_raw or txn.description_norm or "").upper()
    if any(k in desc for k in ["PLAN FEE", "INTEREST CHARGE", "FINANCE CHARGE", "LATE FEE"]):
        return True
    cat = (txn.category_user or txn.category_system or "").upper()
    if "INTEREST" in cat or "FINANCE" in cat:
        return True
    return False


def finance_charges_summary(
    *,
    session: Session,
    scope: str,
    as_of: dt.date,
    months: int = 12,
) -> list[dict[str, Any]]:
    scope_u = _scope_norm(scope)
    acct_q = session.query(ExpenseAccount).filter(ExpenseAccount.type.in_(["CREDIT", "CARD"]))
    if scope_u != "ALL":
        acct_q = acct_q.filter(ExpenseAccount.scope == scope_u)
    accounts = {a.id: a for a in acct_q.all() if _is_credit_account(a)}
    account_ids = list(accounts.keys())
    if not account_ids:
        return []

    months = max(1, min(int(months or 12), 36))
    start_month = _shift_month(dt.date(as_of.year, as_of.month, 1), -(months - 1))

    rows = (
        session.query(ExpenseTransaction)
        .filter(
            ExpenseTransaction.expense_account_id.in_(account_ids),
            ExpenseTransaction.posted_date >= start_month,
            ExpenseTransaction.posted_date <= as_of,
        )
        .order_by(ExpenseTransaction.posted_date.desc(), ExpenseTransaction.id.desc())
        .all()
    )

    buckets: dict[tuple[int, int, int], dict[str, Any]] = {}
    for t in rows:
        if t.amount >= 0:
            continue
        if not _is_finance_charge_txn(t):
            continue
        acct = accounts.get(int(t.expense_account_id))
        if not acct:
            continue
        key = (t.posted_date.year, t.posted_date.month, int(t.expense_account_id))
        entry = buckets.setdefault(
            key,
            {
                "year": t.posted_date.year,
                "month": t.posted_date.month,
                "account_id": int(t.expense_account_id),
                "card_name": acct.name,
                "card_last4": getattr(acct, "last4_masked", None),
                "amount": 0.0,
                "count": 0,
            },
        )
        entry["amount"] += float(abs(t.amount))
        entry["count"] += 1

    output = list(buckets.values())
    output.sort(key=lambda r: (-int(r["year"]), -int(r["month"]), r["card_name"]))
    return output


def finance_charges_transactions(
    *,
    session: Session,
    scope: str,
    as_of: dt.date,
    year: int,
    month: int,
    account_id: int,
) -> list[dict[str, Any]]:
    scope_u = _scope_norm(scope)
    acct_q = session.query(ExpenseAccount).filter(ExpenseAccount.type.in_(["CREDIT", "CARD"]))
    if scope_u != "ALL":
        acct_q = acct_q.filter(ExpenseAccount.scope == scope_u)
    accounts = {a.id: a for a in acct_q.all() if _is_credit_account(a)}
    if int(account_id) not in accounts:
        return []

    start_date = dt.date(int(year), int(month), 1)
    end_date = _shift_month(start_date, 1) - dt.timedelta(days=1)
    if end_date > as_of:
        end_date = as_of

    rows = (
        session.query(ExpenseTransaction)
        .filter(
            ExpenseTransaction.expense_account_id == int(account_id),
            ExpenseTransaction.posted_date >= start_date,
            ExpenseTransaction.posted_date <= end_date,
        )
        .order_by(ExpenseTransaction.posted_date.desc(), ExpenseTransaction.id.desc())
        .all()
    )

    out: list[dict[str, Any]] = []
    for txn in rows:
        if txn.amount >= 0:
            continue
        if not _is_finance_charge_txn(txn):
            continue
        out.append(
            {
                "id": int(txn.id),
                "posted_date": txn.posted_date.isoformat(),
                "description": txn.description_raw,
                "amount": float(abs(txn.amount)),
                "txn_id": txn.txn_id,
            }
        )
    return out


def _shift_month(base: dt.date, months: int) -> dt.date:
    total = base.year * 12 + (base.month - 1) + months
    year = total // 12
    month = total % 12 + 1
    return dt.date(year, month, 1)


def active_card_charges_summary(
    *,
    session: Session,
    scope: str,
    as_of: dt.date,
    include_inactive: bool = False,
) -> dict[str, Any]:
    scope_u = _scope_norm(scope)
    q = session.query(RecurringCardCharge)
    item_owner_map = _plaid_item_owner_map(session)
    if scope_u != "ALL":
        q = q.filter(RecurringCardCharge.scope == scope_u)
    if not include_inactive:
        q = q.filter(RecurringCardCharge.is_active.is_(True))
    charges = q.order_by(RecurringCardCharge.id.asc()).all()
    if not charges:
        return {"charges": []}

    acct_q = session.query(ExpenseAccount).filter(ExpenseAccount.type.in_(["CREDIT", "CARD"]))
    if scope_u != "ALL":
        acct_q = acct_q.filter(ExpenseAccount.scope == scope_u)
    accounts = {a.id: a for a in acct_q.all() if _is_credit_account(a)}
    account_ids = list(accounts.keys())
    lookback = as_of - dt.timedelta(days=90)
    tx_q = session.query(ExpenseTransaction).filter(ExpenseTransaction.posted_date >= lookback)
    if account_ids:
        tx_q = tx_q.filter(ExpenseTransaction.expense_account_id.in_(account_ids))
    txns = tx_q.order_by(ExpenseTransaction.posted_date.desc(), ExpenseTransaction.id.desc()).all()

    def _match_rule(t: ExpenseTransaction, rule: RecurringCardChargeRule) -> bool:
        if rule.rule_type == "PLAID_MERCHANT_ID":
            raw = t.original_row_json or {}
            merchant_id = ""
            if isinstance(raw, dict):
                merchant_id = str(raw.get("merchant_id") or "").strip()
            return merchant_id == rule.rule_value
        if rule.rule_type == "NAME_NORMALIZED":
            name = t.merchant_norm or t.description_norm or t.description_raw or ""
            return _merchant_rule_from_text(name)[1] == rule.rule_value
        return False

    out: list[dict[str, Any]] = []
    for c in charges:
        c_scope = (c.scope or "PERSONAL").upper()
        if scope_u != "ALL" and c_scope != scope_u:
            continue
        rules = (
            session.query(RecurringCardChargeRule)
            .filter(RecurringCardChargeRule.recurring_card_charge_id == c.id)
            .order_by(RecurringCardChargeRule.priority.desc())
            .all()
        )
        candidate_txns = []
        for t in txns:
            if c.source_account_id and int(t.expense_account_id) != int(c.source_account_id):
                continue
            if t.amount >= 0:
                continue
            if _is_excluded_transaction(t):
                continue
            if any(_match_rule(t, r) for r in rules):
                candidate_txns.append(t)
        last_charge = candidate_txns[0] if candidate_txns else None
        plaid_account_id = _plaid_account_id_from_txn(last_charge)
        last_charge_date = last_charge.posted_date.isoformat() if last_charge else None
        last_charge_amount = float(abs(last_charge.amount)) if last_charge else None
        merchant_display = _best_display_name(candidate_txns) if candidate_txns else c.name
        desc_sample = ""
        if last_charge:
            desc_sample = (last_charge.description_raw or last_charge.description_norm or "").strip()
        if (merchant_display or "").strip().lower() == "unknown" and desc_sample:
            merchant_display = desc_sample
        charge_name = (c.name or "").strip() or "Unknown"
        if charge_name.strip().lower() == "unknown":
            if merchant_display and merchant_display.strip().lower() != "unknown":
                charge_name = merchant_display
            elif desc_sample:
                charge_name = desc_sample

        due_day = c.due_day_of_month if c.due_day_of_month and c.due_day_of_month > 0 else None
        if not due_day and last_charge:
            due_day = last_charge.posted_date.day
        due_date = None
        status = "unknown"
        if due_day:
            due_date = _due_date_for_month(as_of.year, as_of.month, int(due_day))
            paid_in_cycle = False
            if last_charge and last_charge.posted_date.year == as_of.year and last_charge.posted_date.month == as_of.month:
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
        source_mask = None
        if c.source_account_id and c.source_account_id in accounts:
            source_name = accounts[c.source_account_id].name
            source_mask = getattr(accounts[c.source_account_id], "last4_masked", None)
            if not plaid_account_id:
                plaid_account_id = _plaid_id_from_provider(getattr(accounts[c.source_account_id], "provider_account_id", None))
        if not source_name and last_charge:
            source_name = last_charge.account_name or None
        if not source_mask and last_charge:
            source_mask = getattr(last_charge, "account_last4_masked", None)
        cardholder, cardholder_source = _cardholder_with_source(last_charge, accounts.get(c.source_account_id), item_owner_map)
        out.append(
            {
                "id": int(c.id),
                "name": charge_name,
                "merchant_display": merchant_display,
                "description_sample": desc_sample or None,
                "scope": c_scope,
                "is_active": bool(c.is_active),
                "source_account_id": int(c.source_account_id) if c.source_account_id else None,
                "source_account_name": source_name,
                "source_account_mask": source_mask,
                "plaid_account_id": plaid_account_id,
                "amount_mode": (c.amount_mode or "VARIABLE").upper(),
                "amount_expected": float(c.amount_expected) if c.amount_expected is not None else None,
                "amount_min": float(c.amount_min) if c.amount_min is not None else None,
                "amount_max": float(c.amount_max) if c.amount_max is not None else None,
                "due_day_of_month": int(due_day) if due_day else None,
                "due_date": due_date.isoformat() if due_date else None,
                "last_charge_date": last_charge_date,
                "last_charge_amount": last_charge_amount,
                "cardholder_name": cardholder,
                "cardholder_source": cardholder_source,
                "status": status,
                "expected_display": _expected_display(c),
            }
        )
    return {"charges": out}


def recent_card_charges(
    *,
    session: Session,
    scope: str,
    as_of: dt.date,
    lookback_days: int = 30,
    limit: int = 500,
) -> dict[str, Any]:
    scope_u = _scope_norm(scope)
    start = as_of - dt.timedelta(days=lookback_days)
    item_owner_map = _plaid_item_owner_map(session)
    acct_q = session.query(ExpenseAccount).filter(ExpenseAccount.type.in_(["CREDIT", "CARD"]))
    if scope_u != "ALL":
        acct_q = acct_q.filter(ExpenseAccount.scope == scope_u)
    accounts = {a.id: a for a in acct_q.all() if _is_credit_account(a)}
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
        merchant_display = _best_display_name([t])
        desc_sample = (t.description_raw or t.description_norm or "").strip()
        if (merchant_display or "").strip().lower() == "unknown" and desc_sample:
            merchant_display = desc_sample
        acct = accounts.get(int(t.expense_account_id))
        plaid_account_id = _plaid_account_id_from_txn(t)
        acct_name = acct.name if acct else (t.account_name or None)
        acct_mask = getattr(acct, "last4_masked", None) if acct else None
        if not acct_mask:
            acct_mask = getattr(t, "account_last4_masked", None)
        if not plaid_account_id and acct is not None:
            plaid_account_id = _plaid_id_from_provider(getattr(acct, "provider_account_id", None))
        cardholder, cardholder_source = _cardholder_with_source(t, acct, item_owner_map)
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
                "cardholder_name": cardholder,
                "cardholder_source": cardholder_source,
            }
        )
    return {"charges": out}
