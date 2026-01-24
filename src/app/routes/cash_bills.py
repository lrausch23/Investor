from __future__ import annotations

import datetime as dt
import json
from typing import Any

from fastapi import APIRouter, Body, Depends, HTTPException, Request
from sqlalchemy import func, or_
from sqlalchemy.orm import Session

from src.app.auth import auth_banner_message, require_actor
from src.app.db import db_session
from src.db.models import (
    ExpenseAccount,
    ExpenseAccountBalance,
    ExpenseTransaction,
    ExternalConnection,
    ExternalCardStatement,
    ExternalLiabilitySnapshot,
    RecurringCardCharge,
    RecurringCardChargeIgnore,
    RecurringCardChargeRule,
    RecurringBill,
    RecurringBillIgnore,
    RecurringBillRule,
)
from src.investor.cash_bills.recurring import (
    active_bills_summary,
    detect_suggestions,
    monthly_deposits_summary,
    recent_charges,
    recurring_due_total,
)
from src.investor.cash_bills.recurring_cards import (
    active_card_charges_summary,
    detect_card_suggestions,
    finance_charges_summary,
    finance_charges_transactions,
    recent_card_charges,
)


router = APIRouter(prefix="/cash-bills", tags=["cash-bills"])
api_router = APIRouter(prefix="/api/cash-bills", tags=["cash-bills-api"])


def _as_str(v: Any) -> str:
    return str(v).strip() if v is not None else ""


def _norm_inst(value: str | None) -> str:
    v = _as_str(value).upper().replace("AMERICAN EXPRESS", "AMEX").replace("AM EX", "AMEX")
    v = v.replace("JPMORGAN", "CHASE").replace("J.P. MORGAN", "CHASE").replace("JP MORGAN", "CHASE")
    v = v.replace("CHASE BANK", "CHASE")
    v = " ".join(v.split())
    return v or "UNKNOWN"


def _last4(value: str | None) -> str:
    digits = "".join(ch for ch in _as_str(value) if ch.isdigit())
    if len(digits) >= 4:
        return digits[-4:]
    return digits


def _plaid_cash_bills_data(session: Session) -> dict[str, Any]:
    today = dt.date.today()
    bills: list[dict[str, Any]] = []
    cash_accounts: list[dict[str, Any]] = []
    errors: list[str] = []
    missing_snapshots: list[str] = []
    snapshots_as_of: list[dt.datetime] = []
    scope_by_key: dict[str, str] = {}
    acct_id_by_key: dict[str, int] = {}
    acct_ids_by_last4: dict[str, list[int]] = {}

    exp_accounts = session.query(ExpenseAccount).order_by(ExpenseAccount.id.asc()).all()
    for a in exp_accounts:
        last4 = _last4(a.last4_masked)
        if not last4:
            continue
        key = f"{_norm_inst(a.institution)}:{last4}"
        if key not in scope_by_key:
            scope_by_key[key] = (a.scope or "PERSONAL").upper()
        if key not in acct_id_by_key:
            acct_id_by_key[key] = int(a.id)
        acct_ids_by_last4.setdefault(last4, []).append(int(a.id))

    payment_by_account: dict[int, dict[str, Any]] = {}
    if acct_id_by_key:
        acct_ids = [a.id for a in exp_accounts if (a.type or "").upper() == "CREDIT"]
        payment_q = (
            session.query(ExpenseTransaction)
            .filter(ExpenseTransaction.expense_account_id.in_(acct_ids))
            .filter(ExpenseTransaction.amount != 0)
            .filter(
                or_(
                    func.lower(func.coalesce(ExpenseTransaction.category_user, "")).like("%payment%"),
                    func.lower(func.coalesce(ExpenseTransaction.category_system, "")).like("%payment%"),
                    func.lower(func.coalesce(ExpenseTransaction.description_raw, "")).like("%payment%"),
                    func.lower(func.coalesce(ExpenseTransaction.description_raw, "")).like("%thank you%"),
                )
            )
            .order_by(ExpenseTransaction.posted_date.desc(), ExpenseTransaction.id.desc())
        )
        for row in payment_q.all():
            if row.expense_account_id not in payment_by_account:
                payment_by_account[row.expense_account_id] = {
                    "date": row.posted_date,
                    "amount": float(abs(row.amount or 0)),
                }

    connections = (
        session.query(ExternalConnection)
        .filter(ExternalConnection.connector.in_(["CHASE_PLAID", "AMEX_PLAID"]))
        .filter(ExternalConnection.status == "ACTIVE")
        .order_by(ExternalConnection.id.asc())
        .all()
    )

    stmt_by_key: dict[tuple[int, str], ExternalCardStatement] = {}
    if connections:
        stmt_rows = (
            session.query(ExternalCardStatement)
            .filter(ExternalCardStatement.connection_id.in_([c.id for c in connections]))
            .all()
        )

        def _stmt_rank(row: ExternalCardStatement) -> tuple[int, dt.datetime]:
            if row.statement_period_end:
                return (row.statement_period_end.toordinal(), row.created_at)
            if row.statement_period_start:
                return (row.statement_period_start.toordinal(), row.created_at)
            return (row.created_at.date().toordinal(), row.created_at)

        for row in stmt_rows:
            last4 = _last4(row.last4)
            if not last4:
                continue
            key = (int(row.connection_id), last4)
            existing = stmt_by_key.get(key)
            if existing is None or _stmt_rank(row) > _stmt_rank(existing):
                stmt_by_key[key] = row

    for conn in connections:
        snapshot = (
            session.query(ExternalLiabilitySnapshot)
            .filter(ExternalLiabilitySnapshot.connection_id == conn.id)
            .order_by(ExternalLiabilitySnapshot.as_of.desc(), ExternalLiabilitySnapshot.id.desc())
            .first()
        )
        if snapshot is None:
            missing_snapshots.append(conn.name)
            continue
        payload = snapshot.payload_json or {}
        snapshots_as_of.append(snapshot.as_of)

        accounts = payload.get("accounts") or []
        if not isinstance(accounts, list):
            accounts = []
        acct_by_id: dict[str, dict[str, Any]] = {
            _as_str(a.get("account_id")): a for a in accounts if isinstance(a, dict) and _as_str(a.get("account_id"))
        }

        issuer = _as_str(conn.broker or conn.provider or "Plaid")
        issuer_norm = _norm_inst(issuer)
        for acct in accounts:
            if not isinstance(acct, dict):
                continue
            if _as_str(acct.get("type")).lower() != "depository":
                continue
            subtype = _as_str(acct.get("subtype")).lower()
            if subtype not in {"checking", "savings"}:
                continue
            balances = acct.get("balances") or {}
            if not isinstance(balances, dict):
                balances = {}
            available = balances.get("available")
            current = balances.get("current")
            if available is None and current is None:
                continue
            name = _as_str(acct.get("name") or acct.get("official_name") or "Checking")
            mask = _last4(acct.get("mask"))
            scope = scope_by_key.get(f"{issuer_norm}:{mask}", "PERSONAL")
            label = f"{name} • {mask}" if mask else name
            cash_accounts.append(
                {
                    "id": f"{conn.id}:{_as_str(acct.get('account_id'))}",
                    "account_name": label,
                    "available_balance": float(available if available is not None else current or 0),
                    "current_balance": float(current) if current is not None else None,
                    "last_updated": snapshot.as_of.date().isoformat() if snapshot.as_of else None,
                    "scope": scope,
                }
            )

        liabilities = payload.get("liabilities") or {}
        if not isinstance(liabilities, dict):
            liabilities = {}
        credit = liabilities.get("credit") or []
        if not isinstance(credit, list):
            credit = []
        for row in credit:
            if not isinstance(row, dict):
                continue
            plaid_account_id = _as_str(row.get("account_id"))
            acct = acct_by_id.get(plaid_account_id, {})
            due_date = _as_str(row.get("next_payment_due_date"))
            if not due_date:
                continue
            aprs = row.get("aprs") or []
            if not isinstance(aprs, list):
                aprs = []
            aprs_count = 0
            balance_subject_to_apr = None
            interest_charge_amount = None
            for apr in aprs:
                if not isinstance(apr, dict):
                    continue
                aprs_count += 1
                if apr.get("balance_subject_to_apr") is not None:
                    balance_subject_to_apr = (balance_subject_to_apr or 0.0) + float(apr.get("balance_subject_to_apr") or 0.0)
                if apr.get("interest_charge_amount") is not None:
                    interest_charge_amount = (interest_charge_amount or 0.0) + float(apr.get("interest_charge_amount") or 0.0)
            last4 = _last4(acct.get("mask"))
            key = f"{issuer_norm}:{last4}"
            scope = scope_by_key.get(key, "PERSONAL")
            expense_account_id = acct_id_by_key.get(key)
            if expense_account_id is None and last4:
                candidates = acct_ids_by_last4.get(last4) or []
                if len(candidates) == 1:
                    expense_account_id = candidates[0]
            card_name = _as_str(acct.get("name") or acct.get("official_name") or issuer or "Card")
            balances = acct.get("balances") or {}
            if not isinstance(balances, dict):
                balances = {}
            current_balance = balances.get("current")
            statement_balance = row.get("last_statement_balance")
            if statement_balance is None:
                statement_balance = current_balance if current_balance is not None else 0.0
            last_payment_date = _as_str(row.get("last_payment_date")) or None
            last_payment_amount = float(row.get("last_payment_amount") or 0) if row.get("last_payment_amount") is not None else None
            if expense_account_id and expense_account_id in payment_by_account:
                p = payment_by_account[expense_account_id]
                try:
                    if p.get("date"):
                        existing_date = dt.date.fromisoformat(last_payment_date) if last_payment_date else None
                        if (existing_date is None) or (p["date"] > existing_date):
                            last_payment_date = p["date"].isoformat()
                            last_payment_amount = float(p["amount"] or 0)
                except Exception:
                    pass
            interest_saving_balance = None
            pay_over_time = None
            if last4:
                stmt = stmt_by_key.get((conn.id, last4))
                if stmt is not None and stmt.interest_saving_balance is not None:
                    interest_saving_balance = float(stmt.interest_saving_balance)
                if stmt is not None and stmt.pay_over_time_json:
                    if isinstance(stmt.pay_over_time_json, dict) and stmt.pay_over_time_json.get("rows"):
                        pay_over_time = stmt.pay_over_time_json
            bills.append(
                {
                    "id": f"{conn.id}:{plaid_account_id or expense_account_id}",
                    "card_name": card_name,
                    "issuer": issuer,
                    "last4": last4 or None,
                    "scope": scope,
                    "due_date": due_date,
                    "current_balance": float(current_balance) if current_balance is not None else None,
                    "statement_balance": float(statement_balance or 0),
                    "interest_saving_balance": interest_saving_balance,
                    "pay_over_time": pay_over_time,
                    "minimum_due": float(row.get("minimum_payment_amount") or 0) if row.get("minimum_payment_amount") is not None else None,
                    "last_payment_date": last_payment_date,
                    "last_payment_amount": last_payment_amount,
                    "last_statement_issue_date": _as_str(row.get("last_statement_issue_date")) or None,
                    "balance_subject_to_apr": float(balance_subject_to_apr) if balance_subject_to_apr is not None else None,
                    "interest_charge_amount": float(interest_charge_amount) if interest_charge_amount is not None else None,
                    "aprs_count": int(aprs_count),
                    "autopay": "unknown",
                    "status": "overdue" if bool(row.get("is_overdue")) else "upcoming",
                }
            )

    if not cash_accounts:
        balances = (
            session.query(ExpenseAccountBalance)
            .order_by(ExpenseAccountBalance.as_of_date.desc(), ExpenseAccountBalance.id.desc())
            .all()
        )
        balances_by_account: dict[int, ExpenseAccountBalance] = {}
        for bal in balances:
            if bal.expense_account_id not in balances_by_account:
                balances_by_account[bal.expense_account_id] = bal
        fallback_accounts = (
            session.query(ExpenseAccount)
            .filter(ExpenseAccount.type.in_(["BANK", "CHECKING", "DEPOSITORY"]))
            .order_by(ExpenseAccount.institution.asc(), ExpenseAccount.name.asc())
            .all()
        )
        for acct in fallback_accounts:
            bal = balances_by_account.get(acct.id)
            if bal is None:
                continue
            available = bal.balance_available if bal.balance_available is not None else bal.balance_current
            current = bal.balance_current
            if available is None and current is None:
                continue
            mask = _last4(acct.last4_masked)
            label = f"{acct.name} • {mask}" if mask else acct.name
            cash_accounts.append(
                {
                    "id": f"db:{acct.id}",
                    "account_name": label,
                    "available_balance": float(available if available is not None else current or 0),
                    "current_balance": float(current) if current is not None else None,
                    "last_updated": bal.as_of_date.date().isoformat() if bal.as_of_date else None,
                    "scope": (acct.scope or "PERSONAL").upper(),
                }
            )

    as_of_date = max(snapshots_as_of).date() if snapshots_as_of else today
    # Fallback: if no liabilities snapshot is available, show balances from ExpenseAccountBalance
    # so the UI can still render while due dates remain unknown.
    fallback_used = False
    if not bills:
        balances = (
            session.query(ExpenseAccountBalance)
            .order_by(ExpenseAccountBalance.as_of_date.desc(), ExpenseAccountBalance.id.desc())
            .all()
        )
        balances_by_account: dict[int, ExpenseAccountBalance] = {}
        for bal in balances:
            if bal.expense_account_id not in balances_by_account:
                balances_by_account[bal.expense_account_id] = bal
        credit_accounts = (
            session.query(ExpenseAccount)
            .filter(ExpenseAccount.type == "CREDIT")
            .order_by(ExpenseAccount.institution.asc(), ExpenseAccount.name.asc())
            .all()
        )
        for acct in credit_accounts:
            bal = balances_by_account.get(acct.id)
            if bal is None:
                continue
            balance_current = bal.balance_current if bal.balance_current is not None else bal.balance_available
            if balance_current is None:
                continue
            last4 = _last4(acct.last4_masked)
            bills.append(
                {
                    "id": f"db:{acct.id}",
                    "card_name": _as_str(acct.name) or "Card",
                    "issuer": _as_str(acct.institution),
                    "last4": last4 or None,
                    "scope": (acct.scope or "PERSONAL").upper(),
                    "due_date": None,
                    "current_balance": float(balance_current),
                    "statement_balance": float(balance_current),
                    "interest_saving_balance": None,
                    "pay_over_time": None,
                    "minimum_due": None,
                    "last_payment_date": None,
                    "last_payment_amount": None,
                    "last_statement_issue_date": None,
                    "balance_subject_to_apr": None,
                    "interest_charge_amount": None,
                    "aprs_count": 0,
                    "autopay": "unknown",
                    "status": "unknown",
                }
            )
        if bills:
            fallback_used = True

    data: dict[str, Any] = {
        "as_of": as_of_date.isoformat(),
        "bills": bills,
        "cash_accounts": cash_accounts,
    }
    if missing_snapshots and fallback_used:
        names = ", ".join(sorted(set(missing_snapshots)))
        data["bills_warning"] = (
            f"Due dates unavailable for {names}. Showing balances only."
        )
    if errors:
        error_msg = "; ".join(errors)
        if any("ADDITIONAL_CONSENT_REQUIRED" in e for e in errors):
            error_msg = (
                "Additional Plaid consent required. Re-link the Chase/Amex connection in Sync → Connections → Credentials."
            )
        if not bills:
            data["bills_error"] = error_msg
        if not cash_accounts:
            data["cash_error"] = error_msg
    if missing_snapshots and not bills:
        data["bills_error"] = (
            "No liabilities snapshot. Run Sync to capture liabilities."
        )
    return data


@router.get("")
def cash_bills_home(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    data = _plaid_cash_bills_data(session)
    data_json = json.dumps(data)
    balances = (
        session.query(ExpenseAccountBalance)
        .order_by(ExpenseAccountBalance.as_of_date.desc(), ExpenseAccountBalance.id.desc())
        .all()
    )
    balances_by_account: dict[int, ExpenseAccountBalance] = {}
    for b in balances:
        if b.expense_account_id not in balances_by_account:
            balances_by_account[b.expense_account_id] = b
    credit_accounts = (
        session.query(ExpenseAccount)
        .filter(ExpenseAccount.type == "CREDIT")
        .order_by(ExpenseAccount.institution.asc(), ExpenseAccount.name.asc())
        .all()
    )
    card_balances: list[dict[str, object]] = []
    for acct in credit_accounts:
        bal = balances_by_account.get(acct.id)
        if bal is None or bal.balance_current is None:
            continue
        card_balances.append(
            {
                "institution": acct.institution,
                "name": acct.name,
                "last4": acct.last4_masked,
                "balance_current": float(bal.balance_current),
            }
        )
    card_balances_json = json.dumps(card_balances)
    auth_banner_detail = auth_banner_message()
    from src.app.main import templates

    return templates.TemplateResponse(
        "cash_bills.html",
        {
            "request": request,
            "actor": actor,
            "auth_banner": None,
            "auth_banner_detail": auth_banner_detail,
            "cash_bills_data_json": data_json,
            "cash_bills_card_balances_json": card_balances_json,
            "title": "Cash & Bills",
        },
    )


def _parse_date(value: str | None, *, fallback: dt.date) -> dt.date:
    if not value:
        return fallback
    try:
        return dt.date.fromisoformat(value[:10])
    except Exception:
        return fallback


@api_router.get("/recurring/summary")
def recurring_summary(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    as_of: str = "",
    scope: str = "personal",
    range_days: str = "30",
    include_inactive: str = "",
):
    as_of_date = _parse_date(as_of, fallback=dt.date.today())
    try:
        range_i = max(7, min(int(range_days or 30), 120))
    except Exception:
        range_i = 30
    include_flag = str(include_inactive or "").strip().lower() in {"1", "true", "yes", "on"}
    summary = active_bills_summary(session=session, scope=scope, as_of=as_of_date, include_inactive=include_flag)
    bills = summary.get("bills") or []
    due_total = recurring_due_total(bills, as_of=as_of_date, range_days=range_i)
    return {"as_of": as_of_date.isoformat(), "range_days": range_i, "due_total": float(due_total), "bills": bills}


@api_router.get("/deposits/summary")
def deposits_summary(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    as_of: str = "",
    scope: str = "personal",
    months: str = "6",
    account_id: str = "",
):
    as_of_date = _parse_date(as_of, fallback=dt.date.today())
    try:
        months_i = max(1, min(int(months or 6), 24))
    except Exception:
        months_i = 6
    try:
        account_id_i = int(account_id) if account_id not in ("", None) else None
    except Exception:
        account_id_i = None
    summary = monthly_deposits_summary(
        session=session,
        scope=scope,
        as_of=as_of_date,
        months=months_i,
        account_id=account_id_i,
    )
    return summary


@api_router.get("/deposits/transactions")
def deposits_transactions(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    account_id: str = "",
    year: str = "",
    month: str = "",
):
    try:
        account_id_i = int(account_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Missing account_id.")
    try:
        year_i = int(year)
        month_i = int(month)
    except Exception:
        raise HTTPException(status_code=400, detail="Missing year/month.")
    if month_i < 1 or month_i > 12:
        raise HTTPException(status_code=400, detail="Invalid month.")
    start = dt.date(year_i, month_i, 1)
    end = (start.replace(day=28) + dt.timedelta(days=4)).replace(day=1) - dt.timedelta(days=1)
    rows = (
        session.query(ExpenseTransaction)
        .filter(
            ExpenseTransaction.expense_account_id == account_id_i,
            ExpenseTransaction.posted_date >= start,
            ExpenseTransaction.posted_date <= end,
            ExpenseTransaction.amount > 0,
        )
        .order_by(ExpenseTransaction.posted_date.desc(), ExpenseTransaction.id.desc())
        .all()
    )
    account = session.query(ExpenseAccount).filter(ExpenseAccount.id == account_id_i).one_or_none()
    account_label = account.name if account else "Account"
    last4 = _last4(account.last4_masked) if account else ""
    if last4:
        account_label = f"{account_label} • {last4}"
    payload = [
        {
            "posted_date": r.posted_date.isoformat(),
            "description": r.description_raw,
            "amount": float(r.amount or 0),
        }
        for r in rows
    ]
    return {
        "account_id": account_id_i,
        "account_label": account_label,
        "year": year_i,
        "month": month_i,
        "rows": payload,
    }


@api_router.get("/recurring/suggestions")
def recurring_suggestions(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    as_of: str = "",
    scope: str = "personal",
):
    as_of_date = _parse_date(as_of, fallback=dt.date.today())
    suggestions = detect_suggestions(session=session, scope=scope, as_of=as_of_date)
    return {"as_of": as_of_date.isoformat(), "suggestions": suggestions}


@api_router.get("/recurring/recent")
def recurring_recent(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    as_of: str = "",
    scope: str = "personal",
    days: int = 30,
):
    as_of_date = _parse_date(as_of, fallback=dt.date.today())
    try:
        days_i = max(7, min(int(days or 30), 365))
    except Exception:
        days_i = 30
    recent = recent_charges(session=session, scope=scope, as_of=as_of_date, lookback_days=days_i)
    return {"as_of": as_of_date.isoformat(), "days": days_i, "charges": recent.get("charges", [])}


@api_router.post("/recurring/activate")
def recurring_activate(
    payload: dict[str, Any] = Body(default={}),
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    key = payload.get("candidate_key") or {}
    rule_type = str((key.get("rule_type") or payload.get("rule_type") or "")).strip().upper()
    rule_value = str((key.get("rule_value") or payload.get("rule_value") or "")).strip()
    if not rule_type or not rule_value:
        raise HTTPException(status_code=400, detail="Missing rule_type/rule_value.")
    name = str(payload.get("name") or "Monthly bill").strip()
    scope = str(payload.get("scope") or "PERSONAL").strip().upper() or "PERSONAL"
    due_day = payload.get("due_day_of_month")
    try:
        due_day = int(due_day) if due_day not in (None, "") else None
    except Exception:
        due_day = None
    amount_mode = str(payload.get("amount_mode") or "VARIABLE").strip().upper()
    amount_expected = payload.get("amount_expected")
    amount_min = payload.get("amount_min")
    amount_max = payload.get("amount_max")
    source_account_id = payload.get("source_account_id")
    try:
        source_account_id = int(source_account_id) if source_account_id not in (None, "") else None
    except Exception:
        source_account_id = None
    confidence = payload.get("autodetect_confidence")
    try:
        confidence = float(confidence) if confidence not in (None, "") else None
    except Exception:
        confidence = None

    bill = RecurringBill(
        scope=scope,
        name=name,
        source_account_id=source_account_id,
        cadence="MONTHLY",
        amount_mode=amount_mode,
        amount_expected=amount_expected if amount_expected not in (None, "") else None,
        amount_min=amount_min if amount_min not in (None, "") else None,
        amount_max=amount_max if amount_max not in (None, "") else None,
        due_day_of_month=due_day,
        is_active=True,
        is_user_confirmed=True,
        autodetect_confidence=confidence,
    )
    session.add(bill)
    session.flush()
    session.add(RecurringBillRule(recurring_bill_id=int(bill.id), rule_type=rule_type, rule_value=rule_value, priority=0))
    session.commit()
    return {"status": "ok", "bill_id": int(bill.id)}


@api_router.post("/recurring/ignore")
def recurring_ignore(
    payload: dict[str, Any] = Body(default={}),
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    key = payload.get("candidate_key") or {}
    rule_type = str((key.get("rule_type") or payload.get("rule_type") or "")).strip().upper()
    rule_value = str((key.get("rule_value") or payload.get("rule_value") or "")).strip()
    scope = str(payload.get("scope") or "PERSONAL").strip().upper() or "PERSONAL"
    if not rule_type or not rule_value:
        raise HTTPException(status_code=400, detail="Missing rule_type/rule_value.")
    existing = (
        session.query(RecurringBillIgnore)
        .filter(
            RecurringBillIgnore.scope == scope,
            RecurringBillIgnore.rule_type == rule_type,
            RecurringBillIgnore.rule_value == rule_value,
        )
        .one_or_none()
    )
    if existing is None:
        session.add(RecurringBillIgnore(scope=scope, rule_type=rule_type, rule_value=rule_value))
        session.commit()
    return {"status": "ok"}


@api_router.patch("/recurring/{bill_id}")
def recurring_update(
    bill_id: int,
    payload: dict[str, Any] = Body(default={}),
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    bill = session.query(RecurringBill).filter(RecurringBill.id == bill_id).one_or_none()
    if bill is None:
        raise HTTPException(status_code=404, detail="Bill not found.")
    if "name" in payload:
        bill.name = str(payload.get("name") or bill.name).strip()
    if "due_day_of_month" in payload:
        try:
            v = payload.get("due_day_of_month")
            bill.due_day_of_month = int(v) if v not in (None, "") else None
        except Exception:
            bill.due_day_of_month = None
    if "amount_mode" in payload:
        bill.amount_mode = str(payload.get("amount_mode") or bill.amount_mode).strip().upper()
    for fld in ("amount_expected", "amount_min", "amount_max"):
        if fld in payload:
            val = payload.get(fld)
            setattr(bill, fld, val if val not in (None, "") else None)
    if "is_active" in payload:
        bill.is_active = bool(payload.get("is_active"))
    if "source_account_id" in payload:
        try:
            v = payload.get("source_account_id")
            bill.source_account_id = int(v) if v not in (None, "") else None
        except Exception:
            bill.source_account_id = None
    session.commit()
    return {"status": "ok"}


@api_router.get("/card-recurring/summary")
def card_recurring_summary(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    as_of: str = "",
    scope: str = "personal",
    include_inactive: str = "",
):
    as_of_date = _parse_date(as_of, fallback=dt.date.today())
    include_flag = str(include_inactive or "").strip().lower() in {"1", "true", "yes", "on"}
    summary = active_card_charges_summary(session=session, scope=scope, as_of=as_of_date, include_inactive=include_flag)
    charges = summary.get("charges") or []
    return {"as_of": as_of_date.isoformat(), "charges": charges}


@api_router.get("/card-recurring/suggestions")
def card_recurring_suggestions(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    as_of: str = "",
    scope: str = "personal",
):
    as_of_date = _parse_date(as_of, fallback=dt.date.today())
    suggestions = detect_card_suggestions(session=session, scope=scope, as_of=as_of_date)
    return {"as_of": as_of_date.isoformat(), "suggestions": suggestions}


@api_router.get("/card-recurring/recent")
def card_recurring_recent(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    as_of: str = "",
    scope: str = "personal",
    days: int = 30,
):
    as_of_date = _parse_date(as_of, fallback=dt.date.today())
    try:
        days_i = max(7, min(int(days or 30), 365))
    except Exception:
        days_i = 30
    recent = recent_card_charges(session=session, scope=scope, as_of=as_of_date, lookback_days=days_i)
    return {"as_of": as_of_date.isoformat(), "days": days_i, "charges": recent.get("charges", [])}


@api_router.get("/card-finance")
def card_finance_summary(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    as_of: str = "",
    scope: str = "personal",
    months: str = "12",
):
    as_of_date = _parse_date(as_of, fallback=dt.date.today())
    try:
        months_i = int(months or "12")
    except Exception:
        months_i = 12
    rows = finance_charges_summary(session=session, scope=scope, as_of=as_of_date, months=months_i)
    return {"as_of": as_of_date.isoformat(), "months": months_i, "rows": rows}


@api_router.get("/card-finance/transactions")
def card_finance_transactions(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    as_of: str = "",
    scope: str = "personal",
    year: int = 0,
    month: int = 0,
    account_id: int = 0,
):
    as_of_date = _parse_date(as_of, fallback=dt.date.today())
    try:
        year_i = int(year)
        month_i = int(month)
        account_i = int(account_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid parameters.")
    if year_i <= 0 or month_i < 1 or month_i > 12:
        raise HTTPException(status_code=400, detail="Invalid year/month.")
    if account_i <= 0:
        raise HTTPException(status_code=400, detail="Missing account_id.")
    rows = finance_charges_transactions(
        session=session, scope=scope, as_of=as_of_date, year=year_i, month=month_i, account_id=account_i
    )
    return {"as_of": as_of_date.isoformat(), "year": year_i, "month": month_i, "account_id": account_i, "rows": rows}


@api_router.post("/card-recurring/activate")
def card_recurring_activate(
    payload: dict[str, Any] = Body(default={}),
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    key = payload.get("candidate_key") or {}
    rule_type = str((key.get("rule_type") or payload.get("rule_type") or "")).strip().upper()
    rule_value = str((key.get("rule_value") or payload.get("rule_value") or "")).strip()
    if not rule_type or not rule_value:
        raise HTTPException(status_code=400, detail="Missing rule_type/rule_value.")
    name = str(payload.get("name") or "Card charge").strip()
    scope = str(payload.get("scope") or "PERSONAL").strip().upper() or "PERSONAL"
    due_day = payload.get("due_day_of_month")
    try:
        due_day = int(due_day) if due_day not in (None, "") else None
    except Exception:
        due_day = None
    amount_mode = str(payload.get("amount_mode") or "VARIABLE").strip().upper()
    amount_expected = payload.get("amount_expected")
    amount_min = payload.get("amount_min")
    amount_max = payload.get("amount_max")
    source_account_id = payload.get("source_account_id")
    try:
        source_account_id = int(source_account_id) if source_account_id not in (None, "") else None
    except Exception:
        source_account_id = None
    confidence = payload.get("autodetect_confidence")
    try:
        confidence = float(confidence) if confidence not in (None, "") else None
    except Exception:
        confidence = None

    charge = RecurringCardCharge(
        scope=scope,
        name=name,
        source_account_id=source_account_id,
        cadence="MONTHLY",
        amount_mode=amount_mode,
        amount_expected=amount_expected if amount_expected not in (None, "") else None,
        amount_min=amount_min if amount_min not in (None, "") else None,
        amount_max=amount_max if amount_max not in (None, "") else None,
        due_day_of_month=due_day,
        is_active=True,
        is_user_confirmed=True,
        autodetect_confidence=confidence,
    )
    session.add(charge)
    session.flush()
    session.add(
        RecurringCardChargeRule(
            recurring_card_charge_id=int(charge.id), rule_type=rule_type, rule_value=rule_value, priority=0
        )
    )
    session.commit()
    return {"status": "ok", "charge_id": int(charge.id)}


@api_router.post("/card-recurring/ignore")
def card_recurring_ignore(
    payload: dict[str, Any] = Body(default={}),
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    key = payload.get("candidate_key") or {}
    rule_type = str((key.get("rule_type") or payload.get("rule_type") or "")).strip().upper()
    rule_value = str((key.get("rule_value") or payload.get("rule_value") or "")).strip()
    scope = str(payload.get("scope") or "PERSONAL").strip().upper() or "PERSONAL"
    if not rule_type or not rule_value:
        raise HTTPException(status_code=400, detail="Missing rule_type/rule_value.")
    existing = (
        session.query(RecurringCardChargeIgnore)
        .filter(
            RecurringCardChargeIgnore.scope == scope,
            RecurringCardChargeIgnore.rule_type == rule_type,
            RecurringCardChargeIgnore.rule_value == rule_value,
        )
        .one_or_none()
    )
    if existing is None:
        session.add(RecurringCardChargeIgnore(scope=scope, rule_type=rule_type, rule_value=rule_value))
        session.commit()
    return {"status": "ok"}


@api_router.patch("/card-recurring/{charge_id}")
def card_recurring_update(
    charge_id: int,
    payload: dict[str, Any] = Body(default={}),
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    charge = session.query(RecurringCardCharge).filter(RecurringCardCharge.id == charge_id).one_or_none()
    if charge is None:
        raise HTTPException(status_code=404, detail="Charge not found.")
    if "name" in payload:
        charge.name = str(payload.get("name") or charge.name)
    if "due_day_of_month" in payload:
        try:
            val = payload.get("due_day_of_month")
            charge.due_day_of_month = int(val) if val not in (None, "") else None
        except Exception:
            pass
    if "amount_mode" in payload:
        charge.amount_mode = str(payload.get("amount_mode") or charge.amount_mode).strip().upper()
    if "amount_expected" in payload:
        charge.amount_expected = payload.get("amount_expected") if payload.get("amount_expected") not in ("", None) else None
    if "amount_min" in payload:
        charge.amount_min = payload.get("amount_min") if payload.get("amount_min") not in ("", None) else None
    if "amount_max" in payload:
        charge.amount_max = payload.get("amount_max") if payload.get("amount_max") not in ("", None) else None
    if "is_active" in payload:
        charge.is_active = bool(payload.get("is_active"))
    session.commit()
    return {"status": "ok"}
