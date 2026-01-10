from __future__ import annotations

import datetime as dt
import os
import shutil
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, File, Form, UploadFile
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session

from src.app.auth import require_actor
from src.app.db import db_session
from src.core.sync_runner import run_sync
from src.db.models import ExternalConnection


router = APIRouter(prefix="/api/connectors/rj", tags=["api"])


def _safe_name(name: str) -> str:
    orig = Path(name or "upload.bin").name
    safe = "".join(ch for ch in orig if ch.isalnum() or ch in {".", "_", "-"}).strip("._")
    return safe or "upload.bin"


@router.post("/qfx-import")
def rj_qfx_import(
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    connection_id: int = Form(...),
    since: str = Form(default=""),
    mode: str = Form(default="INCREMENTAL"),
    dry_run: bool = Form(default=False),
    uploads: list[UploadFile] = File(...),
):
    """
    Upload one or more RJ QFX/OFX files and trigger a sync run.
    """
    conn = session.query(ExternalConnection).filter(ExternalConnection.id == int(connection_id)).one()
    if (conn.connector or "").upper() != "RJ_OFFLINE":
        return JSONResponse(status_code=400, content={"ok": False, "error": "connection is not RJ_OFFLINE"})

    mode_u = (mode or "").strip().upper()
    if mode_u not in {"INCREMENTAL", "FULL"}:
        return JSONResponse(status_code=400, content={"ok": False, "error": "mode must be INCREMENTAL or FULL"})

    meta = dict(conn.metadata_json or {})
    dd = os.path.expanduser(str(meta.get("data_dir") or ""))
    base_dir = Path(dd) if dd else (Path("data") / "external" / f"conn_{conn.id}")
    base_dir.mkdir(parents=True, exist_ok=True)

    saved: list[dict[str, Any]] = []
    for up in uploads:
        safe = _safe_name(up.filename or "upload.qfx")
        ext = Path(safe).suffix.lower()
        if ext not in {".qfx", ".ofx"}:
            continue
        dest = base_dir / safe
        if dest.exists():
            dest = base_dir / f"{Path(safe).stem}_{dt.datetime.now(dt.timezone.utc).timestamp():.0f}{ext}"
        with dest.open("wb") as f:
            shutil.copyfileobj(up.file, f)
        saved.append({"name": dest.name, "bytes": int(dest.stat().st_size), "path": str(dest)})

    if not saved:
        return JSONResponse(status_code=400, content={"ok": False, "error": "no .qfx/.ofx files uploaded"})

    if not meta.get("data_dir"):
        meta["data_dir"] = str(base_dir)
        conn.metadata_json = dict(meta)

    if dry_run:
        from src.adapters.rj_offline.qfx_parser import parse_positions, parse_security_list, parse_transactions

        totals = {"files": len(saved), "txns": 0, "positions": 0}
        for s in saved:
            txt = Path(str(s["path"])).read_text(encoding="utf-8-sig", errors="ignore")
            try:
                sec = parse_security_list(txt)
            except Exception:
                sec = {}
            _asof, pos, _meta = parse_positions(txt, securities=sec)
            tx = parse_transactions(txt)
            totals["txns"] += len(tx)
            totals["positions"] += len(pos)
        return {"ok": True, "saved": saved, "dry_run": True, "totals": totals}

    sd = dt.date.fromisoformat(since) if since.strip() else None
    run = run_sync(session, connection_id=int(conn.id), mode=mode_u, start_date=sd, end_date=None, actor=actor)
    return {"ok": True, "saved": saved, "run_id": run.id, "status": run.status, "coverage": run.coverage_json}

