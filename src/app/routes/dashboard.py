from __future__ import annotations

import datetime as dt

from fastapi import APIRouter, Depends, Request
from sqlalchemy.orm import Session

from src.app.auth import auth_banner_message, require_actor
from src.app.db import db_session
from src.core.dashboard_service import build_dashboard, parse_scope

router = APIRouter()


@router.get("/")
def dashboard(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    scope = parse_scope(request.query_params.get("scope"))
    data = build_dashboard(session, scope=scope, as_of=dt.date.today())

    from src.app.main import templates

    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "actor": actor,
            "auth_banner": auth_banner_message(),
            "policy": data.policy,
            "report": data.drift,
            "tax": data.tax,
            "fees": data.fees,
            "breakdown": data.breakdown,
            "st_exposure": data.st_exposure,
            "wash": data.wash,
            "cashflows": data.cashflows,
            "preview": data.preview,
            "scope": data.scope,
            "scope_label": data.scope_label,
            "partial_dataset_warning": data.partial_dataset_warning,
            "sync_connections": data.sync_connections,
        },
    )
