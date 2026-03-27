from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse

from src.app.auth import auth_banner_message, require_actor

router = APIRouter(tags=["docs"])


@router.get("/docs", response_class=HTMLResponse)
def docs_page(
    request: Request,
    actor: str = Depends(require_actor),
) -> HTMLResponse:
    from src.app.main import templates

    return templates.TemplateResponse(
        "docs.html",
        {
            "request": request,
            "actor": actor,
            "auth_banner": auth_banner_message(),
            "title": "Investor Docs",
        },
    )
