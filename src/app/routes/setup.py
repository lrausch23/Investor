from __future__ import annotations

import datetime as dt
import urllib.parse

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import RedirectResponse
from sqlalchemy import func
from sqlalchemy.orm import Session

from src.app.auth import auth_banner_message, require_actor
from src.app.db import db_session
from src.app.utils import jsonable
from src.core.defaults import ensure_default_setup
from src.db.audit import log_change
from src.db.models import (
    Account,
    BucketPolicy,
    CashBalance,
    CorporateActionEvent,
    ExternalAccountMap,
    IncomeEvent,
    PositionLot,
    TaxLot,
    TaxpayerEntity,
    Transaction,
)

router = APIRouter(prefix="/setup", tags=["setup"])


@router.get("")
def setup_home(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    error = request.query_params.get("error")
    taxpayers = session.query(TaxpayerEntity).order_by(TaxpayerEntity.id).all()
    tp_by_id = {t.id: t for t in taxpayers}
    accounts = session.query(Account).order_by(Account.id).all()
    policies = session.query(BucketPolicy).order_by(BucketPolicy.effective_date.desc()).all()
    acct_ids = [a.id for a in accounts]

    # Best-effort usage counts to avoid deleting accounts with imported data.
    # NOTE: ExternalAccountMap rows are safe to remove when deleting a placeholder account; we treat them separately.
    deps_by_account_id: dict[int, dict[str, int]] = {int(aid): {} for aid in acct_ids}

    def _counts(model, label: str) -> dict[int, int]:
        col = getattr(model, "account_id", None)
        if col is None or not acct_ids:
            return {}
        rows = (
            session.query(col, func.count(getattr(model, "id")))
            .filter(col.in_(acct_ids))
            .group_by(col)
            .all()
        )
        out: dict[int, int] = {}
        for aid, cnt in rows:
            if aid is None:
                continue
            out[int(aid)] = int(cnt or 0)
        for aid, cnt in out.items():
            deps_by_account_id.setdefault(int(aid), {})[label] = int(cnt)
        return out

    tx_counts = _counts(Transaction, "transactions")
    lot_counts = _counts(PositionLot, "lots")
    cash_counts = _counts(CashBalance, "cash_balances")
    income_counts = _counts(IncomeEvent, "income_events")
    map_counts = _counts(ExternalAccountMap, "external_account_maps")
    taxlot_counts = _counts(TaxLot, "tax_lots")
    corp_counts = _counts(CorporateActionEvent, "corporate_actions")

    usage_by_account_id: dict[int, int] = {}
    delete_blocked_by_account_id: dict[int, bool] = {}
    maps_by_account_id: dict[int, int] = {}
    for aid in acct_ids:
        aid_i = int(aid)
        deps = deps_by_account_id.get(aid_i) or {}
        maps_by_account_id[aid_i] = int(map_counts.get(aid_i) or 0)
        usage_by_account_id[aid_i] = int(sum(int(v or 0) for v in deps.values()))
        # Block deletion only for real data dependencies (mappings can be safely removed).
        delete_blocked_by_account_id[aid_i] = any(
            int(deps.get(k) or 0) > 0
            for k in ("transactions", "lots", "cash_balances", "income_events", "tax_lots", "corporate_actions")
        )

    from src.app.main import templates

    return templates.TemplateResponse(
        "setup.html",
        {
            "request": request,
            "actor": actor,
            "auth_banner": auth_banner_message(),
            "error": error,
            "taxpayers": taxpayers,
            "tp_by_id": tp_by_id,
            "accounts": accounts,
            "usage_by_account_id": usage_by_account_id,
            "maps_by_account_id": maps_by_account_id,
            "delete_blocked_by_account_id": delete_blocked_by_account_id,
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


@router.post("/accounts/{account_id}/update")
def setup_update_account(
    account_id: int,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    name: str = Form(...),
    broker: str = Form(...),
    account_type: str = Form(...),
    taxpayer_entity_id: int = Form(...),
    note: str = Form(default=""),
):
    acct = session.query(Account).filter(Account.id == int(account_id)).one_or_none()
    if acct is None:
        return RedirectResponse(url="/setup?error=Account+not+found", status_code=303)

    name_u = name.strip()
    if not name_u:
        return RedirectResponse(url="/setup?error=Account+name+is+required", status_code=303)

    existing = session.query(Account).filter(Account.name == name_u, Account.id != acct.id).one_or_none()
    if existing is not None:
        return RedirectResponse(url="/setup?error=Account+name+already+exists", status_code=303)

    tp = session.query(TaxpayerEntity).filter(TaxpayerEntity.id == int(taxpayer_entity_id)).one_or_none()
    if tp is None:
        return RedirectResponse(url="/setup?error=Taxpayer+not+found", status_code=303)

    old = {"id": acct.id, "name": acct.name, "broker": acct.broker, "account_type": acct.account_type, "taxpayer_entity_id": acct.taxpayer_entity_id}
    acct.name = name_u
    acct.broker = broker.strip().upper()
    acct.account_type = account_type.strip().upper()
    acct.taxpayer_entity_id = int(taxpayer_entity_id)
    session.flush()
    new = {"id": acct.id, "name": acct.name, "broker": acct.broker, "account_type": acct.account_type, "taxpayer_entity_id": acct.taxpayer_entity_id}
    log_change(
        session,
        actor=actor,
        action="UPDATE",
        entity="Account",
        entity_id=str(acct.id),
        old=jsonable(old),
        new=jsonable(new),
        note=note or "Update account",
    )
    session.commit()
    return RedirectResponse(url="/setup", status_code=303)


@router.post("/accounts/{account_id}/delete")
def setup_delete_account(
    account_id: int,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    note: str = Form(default=""),
):
    acct = session.query(Account).filter(Account.id == int(account_id)).one_or_none()
    if acct is None:
        return RedirectResponse(url="/setup?error=Account+not+found", status_code=303)

    # Hard-delete only when no dependent records exist.
    deps: dict[str, int] = {}
    deps["transactions"] = int(session.query(func.count(Transaction.id)).filter(Transaction.account_id == acct.id).scalar() or 0)
    deps["lots"] = int(session.query(func.count(PositionLot.id)).filter(PositionLot.account_id == acct.id).scalar() or 0)
    deps["cash_balances"] = int(session.query(func.count(CashBalance.id)).filter(CashBalance.account_id == acct.id).scalar() or 0)
    deps["income_events"] = int(session.query(func.count(IncomeEvent.id)).filter(IncomeEvent.account_id == acct.id).scalar() or 0)
    deps["external_account_maps"] = int(session.query(func.count(ExternalAccountMap.id)).filter(ExternalAccountMap.account_id == acct.id).scalar() or 0)
    deps["tax_lots"] = int(session.query(func.count(TaxLot.id)).filter(TaxLot.account_id == acct.id).scalar() or 0)
    deps["corporate_actions"] = int(session.query(func.count(CorporateActionEvent.id)).filter(CorporateActionEvent.account_id == acct.id).scalar() or 0)
    used_hard = sum(int(deps.get(k) or 0) for k in ("transactions", "lots", "cash_balances", "income_events", "tax_lots", "corporate_actions"))
    if used_hard > 0:
        detail = ", ".join(f"{k}={int(v)}" for k, v in deps.items() if int(v) > 0)
        msg = urllib.parse.quote(f"Account is in use and cannot be deleted ({detail}).")
        return RedirectResponse(url=f"/setup?error={msg}", status_code=303)

    # Safe cleanup: remove external account mappings pointing at this account (common for placeholder portfolios).
    session.query(ExternalAccountMap).filter(ExternalAccountMap.account_id == acct.id).delete(synchronize_session=False)

    old = {"id": acct.id, "name": acct.name, "broker": acct.broker, "account_type": acct.account_type, "taxpayer_entity_id": acct.taxpayer_entity_id}
    session.delete(acct)
    session.flush()
    log_change(
        session,
        actor=actor,
        action="DELETE",
        entity="Account",
        entity_id=str(old["id"]),
        old=jsonable(old),
        new=None,
        note=note or "Delete account",
    )
    session.commit()
    return RedirectResponse(url="/setup", status_code=303)
