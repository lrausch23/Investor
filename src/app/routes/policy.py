from __future__ import annotations

import datetime as dt
import json

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import RedirectResponse
from sqlalchemy.orm import Session

from src.app.auth import auth_banner_message, require_actor
from src.app.db import db_session
from src.app.utils import jsonable
from src.core.policy_engine import create_policy_version
from src.db.audit import log_change
from src.db.models import BucketPolicy

router = APIRouter(prefix="/policy", tags=["policy"])


@router.get("")
def policy_list(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    policies = session.query(BucketPolicy).order_by(BucketPolicy.effective_date.desc()).all()
    from src.app.main import templates

    return templates.TemplateResponse(
        "policy_list.html",
        {
            "request": request,
            "actor": actor,
            "auth_banner": auth_banner_message(),
            "policies": policies,
        },
    )


@router.get("/new")
def policy_new(
    request: Request,
    actor: str = Depends(require_actor),
):
    from src.app.main import templates

    default_json = {
        "notes": "MVP policy definition",
        "constraints": {"max_single_name_pct": 0.15},
    }
    return templates.TemplateResponse(
        "policy_new.html",
        {
            "request": request,
            "actor": actor,
            "auth_banner": auth_banner_message(),
            "default_json": json.dumps(default_json, indent=2),
            "today": dt.date.today().isoformat(),
        },
    )


@router.post("/new")
def policy_create(
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    name: str = Form(...),
    effective_date: str = Form(...),
    json_definition: str = Form(default="{}"),
    note: str = Form(default=""),
    b1_min: float = Form(...),
    b1_target: float = Form(...),
    b1_max: float = Form(...),
    b2_min: float = Form(...),
    b2_target: float = Form(...),
    b2_max: float = Form(...),
    b3_min: float = Form(...),
    b3_target: float = Form(...),
    b3_max: float = Form(...),
    b4_min: float = Form(...),
    b4_target: float = Form(...),
    b4_max: float = Form(...),
):
    policy = create_policy_version(
        session=session,
        name=name,
        effective_date=dt.date.fromisoformat(effective_date),
        json_definition=json.loads(json_definition or "{}"),
        buckets=[
            ("B1", "Liquidity", b1_min, b1_target, b1_max, ["CASH", "MMF"], {}),
            ("B2", "Defensive / Income", b2_min, b2_target, b2_max, ["BOND", "CREDIT", "DIVIDEND"], {}),
            ("B3", "Growth", b3_min, b3_target, b3_max, ["EQUITY", "INDEX", "GROWTH"], {}),
            ("B4", "Alpha / Opportunistic", b4_min, b4_target, b4_max, ["ALTERNATIVE", "THEMATIC", "ALPHA"], {}),
        ],
    )
    session.flush()
    log_change(
        session,
        actor=actor,
        action="CREATE",
        entity="BucketPolicy",
        entity_id=str(policy.id),
        old=None,
        new=jsonable({"id": policy.id, "name": policy.name, "effective_date": policy.effective_date.isoformat()}),
        note=note or "Create policy version",
    )
    session.commit()
    return RedirectResponse(url="/policy", status_code=303)

