from __future__ import annotations

import datetime as dt
import json

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import RedirectResponse
from sqlalchemy.orm import Session

from src.app.auth import auth_banner_message, require_actor
from src.app.db import db_session
from src.app.utils import jsonable
from src.db.audit import log_change
from src.db.models import (
    Account,
    BucketAssignment,
    BucketPolicy,
    CashBalance,
    IncomeEvent,
    PositionLot,
    Security,
    SubstituteGroup,
    Transaction,
)

router = APIRouter(prefix="/holdings", tags=["holdings"])


@router.get("")
def holdings_readonly(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    from src.app.main import templates
    from src.core.dashboard_service import parse_scope
    from src.core.external_holdings import build_holdings_view

    scope = parse_scope(request.query_params.get("scope"))
    account_id_raw = (request.query_params.get("account_id") or "").strip()
    account_id = int(account_id_raw) if account_id_raw.isdigit() else None

    today = dt.date.today()
    view = build_holdings_view(session, scope=scope, account_id=account_id, today=today)

    # Account selector options (within scope).
    from src.db.models import TaxpayerEntity

    q = session.query(Account).join(TaxpayerEntity, TaxpayerEntity.id == Account.taxpayer_entity_id).order_by(Account.name)
    if scope == "trust":
        q = q.filter(TaxpayerEntity.type == "TRUST")
    elif scope == "personal":
        q = q.filter(TaxpayerEntity.type == "PERSONAL")
    accounts = q.all()

    return templates.TemplateResponse(
        "holdings_readonly.html",
        {
            "request": request,
            "actor": actor,
            "auth_banner": auth_banner_message(),
            "scope": scope,
            "scope_label": view.scope_label,
            "account_id": account_id,
            "accounts": accounts,
            "view": view,
            "today": today,
        },
    )


@router.get("/securities")
def securities_list(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    securities = session.query(Security).order_by(Security.ticker).all()
    groups = session.query(SubstituteGroup).order_by(SubstituteGroup.name).all()
    from src.app.main import templates

    return templates.TemplateResponse(
        "securities.html",
        {
            "request": request,
            "actor": actor,
            "auth_banner": auth_banner_message(),
            "securities": securities,
            "groups": groups,
        },
    )


@router.post("/securities")
def securities_create(
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    ticker: str = Form(...),
    name: str = Form(...),
    asset_class: str = Form(...),
    expense_ratio: float = Form(default=0.0),
    substitute_group_id: str = Form(default=""),
    last_price: float = Form(default=0.0),
    note: str = Form(default=""),
):
    group_id = int(substitute_group_id) if substitute_group_id.strip() else None
    sec = Security(
        ticker=ticker.strip().upper(),
        name=name.strip(),
        asset_class=asset_class.strip().upper(),
        expense_ratio=float(expense_ratio or 0.0),
        substitute_group_id=group_id,
        metadata_json={"last_price": float(last_price or 0.0)},
    )
    session.add(sec)
    session.flush()
    log_change(
        session,
        actor=actor,
        action="CREATE",
        entity="Security",
        entity_id=str(sec.id),
        old=None,
        new=jsonable({"ticker": sec.ticker, "asset_class": sec.asset_class, "expense_ratio": sec.expense_ratio}),
        note=note or "Create security",
    )
    session.commit()
    return RedirectResponse(url="/holdings/securities", status_code=303)


@router.post("/groups")
def groups_create(
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    name: str = Form(...),
    description: str = Form(default=""),
    note: str = Form(default=""),
):
    grp = SubstituteGroup(name=name.strip(), description=description.strip() or None)
    session.add(grp)
    session.flush()
    log_change(
        session,
        actor=actor,
        action="CREATE",
        entity="SubstituteGroup",
        entity_id=str(grp.id),
        old=None,
        new=jsonable({"name": grp.name}),
        note=note or "Create substitute group",
    )
    session.commit()
    return RedirectResponse(url="/holdings/securities", status_code=303)


@router.get("/lots")
def lots_list(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    accounts = session.query(Account).order_by(Account.name).all()
    lots = session.query(PositionLot).order_by(PositionLot.acquisition_date.desc()).limit(500).all()
    from src.app.main import templates

    return templates.TemplateResponse(
        "lots.html",
        {
            "request": request,
            "actor": actor,
            "auth_banner": auth_banner_message(),
            "accounts": accounts,
            "lots": lots,
            "today": dt.date.today().isoformat(),
        },
    )


@router.post("/lots")
def lots_create(
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    account_id: int = Form(...),
    ticker: str = Form(...),
    acquisition_date: str = Form(...),
    qty: float = Form(...),
    basis_total: float = Form(...),
    adjusted_basis_total: str = Form(default=""),
    note: str = Form(default=""),
):
    lot = PositionLot(
        account_id=account_id,
        ticker=ticker.strip().upper(),
        acquisition_date=dt.date.fromisoformat(acquisition_date),
        qty=float(qty),
        basis_total=float(basis_total),
        adjusted_basis_total=float(adjusted_basis_total) if adjusted_basis_total.strip() else None,
    )
    session.add(lot)
    session.flush()
    log_change(
        session,
        actor=actor,
        action="CREATE",
        entity="PositionLot",
        entity_id=str(lot.id),
        old=None,
        new=jsonable(
            {
                "account_id": lot.account_id,
                "ticker": lot.ticker,
                "acquisition_date": lot.acquisition_date.isoformat(),
                "qty": float(lot.qty),
                "basis_total": float(lot.basis_total),
            }
        ),
        note=note or "Create lot",
    )
    session.commit()
    return RedirectResponse(url="/holdings/lots", status_code=303)


@router.get("/cash")
def cash_list(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    accounts = session.query(Account).order_by(Account.name).all()
    balances = session.query(CashBalance).order_by(CashBalance.as_of_date.desc()).limit(200).all()
    from src.app.main import templates

    return templates.TemplateResponse(
        "cash.html",
        {
            "request": request,
            "actor": actor,
            "auth_banner": auth_banner_message(),
            "accounts": accounts,
            "balances": balances,
            "today": dt.date.today().isoformat(),
        },
    )


@router.post("/cash")
def cash_create(
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    account_id: int = Form(...),
    as_of_date: str = Form(...),
    amount: float = Form(...),
    note: str = Form(default=""),
):
    cb = CashBalance(account_id=account_id, as_of_date=dt.date.fromisoformat(as_of_date), amount=float(amount))
    session.add(cb)
    session.flush()
    log_change(
        session,
        actor=actor,
        action="CREATE",
        entity="CashBalance",
        entity_id=str(cb.id),
        old=None,
        new=jsonable({"account_id": cb.account_id, "as_of_date": cb.as_of_date.isoformat(), "amount": float(cb.amount)}),
        note=note or "Create cash balance",
    )
    session.commit()
    return RedirectResponse(url="/holdings/cash", status_code=303)


@router.get("/income")
def income_list(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    accounts = session.query(Account).order_by(Account.name).all()
    events = session.query(IncomeEvent).order_by(IncomeEvent.date.desc()).limit(300).all()
    from src.app.main import templates

    return templates.TemplateResponse(
        "income.html",
        {
            "request": request,
            "actor": actor,
            "auth_banner": auth_banner_message(),
            "accounts": accounts,
            "events": events,
            "today": dt.date.today().isoformat(),
        },
    )


@router.post("/income")
def income_create(
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    account_id: int = Form(...),
    date: str = Form(...),
    type: str = Form(...),
    ticker: str = Form(default=""),
    amount: float = Form(...),
    note: str = Form(default=""),
):
    ev = IncomeEvent(
        account_id=account_id,
        date=dt.date.fromisoformat(date),
        type=type.strip().upper(),
        ticker=ticker.strip().upper() or None,
        amount=float(amount),
    )
    session.add(ev)
    session.flush()
    log_change(
        session,
        actor=actor,
        action="CREATE",
        entity="IncomeEvent",
        entity_id=str(ev.id),
        old=None,
        new=jsonable({"account_id": ev.account_id, "date": ev.date.isoformat(), "type": ev.type, "amount": float(ev.amount)}),
        note=note or "Create income event",
    )
    session.commit()
    return RedirectResponse(url="/holdings/income", status_code=303)


@router.get("/transactions")
def transactions_list(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    accounts = session.query(Account).order_by(Account.name).all()
    acct_by_id = {a.id: a for a in accounts}

    # Optional source filters:
    # - source=all|manual|imported (default all)
    # - connection_id=<id> to show only txns imported via that connection.
    source = (request.query_params.get("source") or "all").strip().lower()
    connection_id_raw = (request.query_params.get("connection_id") or "").strip()
    connection_id = int(connection_id_raw) if connection_id_raw.isdigit() else None

    from src.db.models import ExternalConnection, ExternalTransactionMap

    connections = session.query(ExternalConnection).order_by(ExternalConnection.id.desc()).all()

    q = (
        session.query(Transaction, ExternalTransactionMap, ExternalConnection)
        .outerjoin(ExternalTransactionMap, ExternalTransactionMap.transaction_id == Transaction.id)
        .outerjoin(ExternalConnection, ExternalConnection.id == ExternalTransactionMap.connection_id)
    )
    if connection_id is not None:
        q = q.filter(ExternalTransactionMap.connection_id == connection_id)
        source = "imported"
    elif source == "manual":
        q = q.filter(ExternalTransactionMap.id.is_(None))
    elif source == "imported":
        q = q.filter(ExternalTransactionMap.id.is_not(None))

    rows = q.order_by(Transaction.date.desc(), Transaction.id.desc()).limit(300).all()
    from src.app.main import templates

    return templates.TemplateResponse(
        "transactions.html",
        {
            "request": request,
            "actor": actor,
            "auth_banner": auth_banner_message(),
            "accounts": accounts,
            "acct_by_id": acct_by_id,
            "rows": rows,
            "connections": connections,
            "source": source,
            "connection_id": connection_id,
            "today": dt.date.today().isoformat(),
        },
    )


@router.post("/transactions")
def transactions_create(
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    account_id: int = Form(...),
    date: str = Form(...),
    type: str = Form(...),
    ticker: str = Form(default=""),
    qty: str = Form(default=""),
    amount: float = Form(...),
    lot_basis_total: str = Form(default=""),
    lot_acquisition_date: str = Form(default=""),
    term: str = Form(default=""),
    note: str = Form(default=""),
):
    links = {}
    if lot_basis_total.strip():
        links["basis_total"] = float(lot_basis_total)
    if lot_acquisition_date.strip():
        links["acquisition_date"] = lot_acquisition_date.strip()
    if term.strip():
        links["term"] = term.strip().upper()
    tx = Transaction(
        account_id=account_id,
        date=dt.date.fromisoformat(date),
        type=type.strip().upper(),
        ticker=ticker.strip().upper() or None,
        qty=float(qty) if qty.strip() else None,
        amount=float(amount),
        lot_links_json=links,
    )
    session.add(tx)
    session.flush()
    log_change(
        session,
        actor=actor,
        action="CREATE",
        entity="Transaction",
        entity_id=str(tx.id),
        old=None,
        new=jsonable({"account_id": tx.account_id, "date": tx.date.isoformat(), "type": tx.type, "amount": float(tx.amount)}),
        note=note or "Create transaction",
    )
    session.commit()
    return RedirectResponse(url="/holdings/transactions", status_code=303)


@router.get("/assignments")
def assignments_list(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    policy = session.query(BucketPolicy).order_by(BucketPolicy.effective_date.desc()).first()
    securities = session.query(Security).order_by(Security.ticker).all()
    assignments = []
    if policy:
        assignments = session.query(BucketAssignment).filter(BucketAssignment.policy_id == policy.id).all()
    map_by_ticker = {a.ticker: a.bucket_code for a in assignments}
    from src.app.main import templates

    return templates.TemplateResponse(
        "assignments.html",
        {
            "request": request,
            "actor": actor,
            "auth_banner": auth_banner_message(),
            "policy": policy,
            "securities": securities,
            "map_by_ticker": map_by_ticker,
        },
    )


@router.post("/assignments")
def assignments_upsert(
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    ticker: str = Form(...),
    bucket_code: str = Form(...),
    note: str = Form(default=""),
):
    policy = session.query(BucketPolicy).order_by(BucketPolicy.effective_date.desc()).first()
    if policy is None:
        return RedirectResponse(url="/policy/new", status_code=303)

    ticker_u = ticker.strip().upper()
    existing = (
        session.query(BucketAssignment)
        .filter(BucketAssignment.policy_id == policy.id, BucketAssignment.ticker == ticker_u)
        .one_or_none()
    )
    old = jsonable({"ticker": ticker_u, "bucket_code": existing.bucket_code}) if existing else None
    if existing:
        existing.bucket_code = bucket_code.strip().upper()
        entity_id = str(existing.id)
        action = "UPDATE"
        new = jsonable({"ticker": existing.ticker, "bucket_code": existing.bucket_code})
    else:
        ba = BucketAssignment(policy_id=policy.id, ticker=ticker_u, bucket_code=bucket_code.strip().upper())
        session.add(ba)
        session.flush()
        entity_id = str(ba.id)
        action = "CREATE"
        new = jsonable({"ticker": ba.ticker, "bucket_code": ba.bucket_code})

    log_change(
        session,
        actor=actor,
        action=action,
        entity="BucketAssignment",
        entity_id=entity_id,
        old=old,
        new=new,
        note=note or "Upsert bucket assignment",
    )
    session.commit()
    return RedirectResponse(url="/holdings/assignments", status_code=303)
