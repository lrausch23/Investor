from __future__ import annotations

from fastapi import APIRouter, Depends, File, Form, Request, UploadFile
from fastapi.responses import RedirectResponse
from sqlalchemy.orm import Session

from src.app.auth import auth_banner_message, require_actor
from src.app.db import db_session
from src.importers.csv_import import import_csv

router = APIRouter(prefix="/imports", tags=["imports"])


@router.get("")
def imports_home(
    request: Request,
    actor: str = Depends(require_actor),
):
    from src.app.main import templates

    return templates.TemplateResponse(
        "imports.html",
        {"request": request, "actor": actor, "auth_banner": auth_banner_message()},
    )


@router.post("")
async def imports_upload(
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    kind: str = Form(...),
    note: str = Form(default=""),
    file: UploadFile = File(...),
):
    content = await file.read()
    result = import_csv(session=session, kind=kind, content=content.decode("utf-8"), actor=actor, note=note)
    session.commit()
    return RedirectResponse(url=f"/imports?imported={result.get('rows', 0)}", status_code=303)

