from __future__ import annotations

import datetime as dt

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import RedirectResponse
from sqlalchemy.orm import Session

from src.app.auth import auth_banner_message, require_actor
from src.app.db import db_session
from src.app.utils import jsonable
from src.core.defaults import ensure_default_setup
from src.db.audit import log_change
from src.db.models import Account, BucketPolicy, TaxpayerEntity

router = APIRouter(prefix="/setup", tags=["setup"])


@router.get("")
def setup_home(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    taxpayers = session.query(TaxpayerEntity).order_by(TaxpayerEntity.id).all()
    accounts = session.query(Account).order_by(Account.id).all()
    policies = session.query(BucketPolicy).order_by(BucketPolicy.effective_date.desc()).all()

    from src.app.main import templates

    return templates.TemplateResponse(
        "setup.html",
        {
            "request": request,
            "actor": actor,
            "auth_banner": auth_banner_message(),
            "taxpayers": taxpayers,
            "accounts": accounts,
            "policies": policies,
        },
    )


@router.post("/defaults")
def setup_create_defaults(
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    note: str = Form(default=""),
):
    old_state = {"taxpayers": session.query(TaxpayerEntity).count(), "accounts": session.query(Account).count()}
    created = ensure_default_setup(session=session, effective_date=dt.date.today())
    session.flush()
    log_change(
        session,
        actor=actor,
        action="CREATE_DEFAULTS",
        entity="Setup",
        entity_id=None,
        old=jsonable(old_state),
        new=jsonable(created),
        note=note or "Create default setup",
    )
    session.commit()
    return RedirectResponse(url="/setup", status_code=303)


@router.post("/taxpayers")
def setup_create_taxpayer(
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    name: str = Form(...),
    type: str = Form(...),
    tax_id_last4: str = Form(default=""),
    notes: str = Form(default=""),
    note: str = Form(default=""),
):
    tp = TaxpayerEntity(
        name=name.strip(),
        type=type.strip().upper(),
        tax_id_last4=tax_id_last4.strip() or None,
        notes=notes.strip() or None,
    )
    session.add(tp)
    session.flush()
    log_change(
        session,
        actor=actor,
        action="CREATE",
        entity="TaxpayerEntity",
        entity_id=str(tp.id),
        old=None,
        new=jsonable({"id": tp.id, "name": tp.name, "type": tp.type}),
        note=note or "Create taxpayer",
    )
    session.commit()
    return RedirectResponse(url="/setup", status_code=303)


@router.post("/accounts")
def setup_create_account(
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    name: str = Form(...),
    broker: str = Form(...),
    account_type: str = Form(...),
    taxpayer_entity_id: int = Form(...),
    note: str = Form(default=""),
):
    acct = Account(
        name=name.strip(),
        broker=broker.strip().upper(),
        account_type=account_type.strip().upper(),
        taxpayer_entity_id=taxpayer_entity_id,
    )
    session.add(acct)
    session.flush()
    log_change(
        session,
        actor=actor,
        action="CREATE",
        entity="Account",
        entity_id=str(acct.id),
        old=None,
        new=jsonable({"id": acct.id, "name": acct.name, "broker": acct.broker, "type": acct.account_type}),
        note=note or "Create account",
    )
    session.commit()
    return RedirectResponse(url="/setup", status_code=303)
