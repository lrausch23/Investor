from __future__ import annotations

from fastapi import APIRouter, Depends, Request

from src.app.auth import require_actor
from src.app.db import db_session


router = APIRouter(prefix="/maintenance", tags=["maintenance"])


@router.get("")
def maintenance_home(request: Request, actor: str = Depends(require_actor)):
    from src.app.main import templates

    return templates.TemplateResponse(
        "maintenance.html",
        {
            "request": request,
            "actor": actor,
            "title": "Maintenance",
        },
    )
