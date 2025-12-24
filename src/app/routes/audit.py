from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from sqlalchemy.orm import Session

from src.app.auth import auth_banner_message, require_actor
from src.app.db import db_session
from src.db.models import AuditLog

router = APIRouter(prefix="/audit", tags=["audit"])


@router.get("")
def audit_list(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    rows = session.query(AuditLog).order_by(AuditLog.at.desc()).limit(300).all()
    from src.app.main import templates

    return templates.TemplateResponse(
        "audit.html",
        {"request": request, "actor": actor, "auth_banner": auth_banner_message(), "rows": rows},
    )

