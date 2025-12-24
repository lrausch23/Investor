from __future__ import annotations

import datetime as dt
import os
import urllib.parse
from pathlib import Path
import re

from fastapi import APIRouter, Depends, Form, Request
from fastapi import HTTPException
from fastapi import File, UploadFile
from fastapi.responses import RedirectResponse
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from sqlalchemy import func

from src.app.auth import auth_banner_message, require_actor
from src.app.db import db_session
from src.app.utils import jsonable
from src.core.credential_store import (
    CredentialError,
    get_credential,
    get_credential_masked,
    secret_key_available,
    upsert_credential,
)
from src.core.sync_runner import run_sync
from src.core.lot_reconstruction import rebuild_reconstructed_tax_lots_for_taxpayer
from src.db.audit import log_change
from src.db.models import (
    Account,
    ExternalConnection,
    ExternalFileIngest,
    ExternalHoldingSnapshot,
    ExternalPayloadSnapshot,
    ExternalTransactionMap,
    SyncRun,
    TaxpayerEntity,
    Transaction,
)
from src.core.sync_runner import AdapterConnectionContext, _adapter_for
from src.utils.time import utcfromtimestamp


router = APIRouter(prefix="/sync", tags=["sync"])

_QUERY_SPLIT_RE = re.compile(r"[\s,;]+")


def _split_query_tokens(raw: str) -> list[str]:
    s = (raw or "").strip()
    if not s:
        return []
    parts = [p.strip() for p in _QUERY_SPLIT_RE.split(s) if p.strip()]
    out: list[str] = []
    for p in parts:
        if p not in out:
            out.append(p)
    return out


@router.get("/connections")
def connections_list(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    error = request.query_params.get("error")
    conns = session.query(ExternalConnection).order_by(ExternalConnection.id.desc()).all()
    taxpayers = session.query(TaxpayerEntity).order_by(TaxpayerEntity.id).all()
    tp_by_id = {t.id: t for t in taxpayers}
    latest_by_conn: dict[int, SyncRun] = {}
    runs = (
        session.query(SyncRun)
        .order_by(SyncRun.started_at.desc())
        .limit(200)
        .all()
    )
    for r in runs:
        if r.connection_id not in latest_by_conn:
            latest_by_conn[r.connection_id] = r

    since_default_by_conn: dict[int, str | None] = {}
    for c in conns:
        if c.last_successful_txn_end:
            since_default_by_conn[c.id] = (c.last_successful_txn_end - dt.timedelta(days=7)).isoformat()
        elif c.last_successful_sync_at:
            since_default_by_conn[c.id] = (c.last_successful_sync_at.date() - dt.timedelta(days=7)).isoformat()
        else:
            since_default_by_conn[c.id] = None

    secret_ok = secret_key_available()

    from src.app.main import templates

    return templates.TemplateResponse(
        "sync_connections.html",
        {
            "request": request,
            "actor": actor,
            "auth_banner": auth_banner_message(),
            "secret_key_ok": secret_ok,
            "error": error,
            "connections": conns,
            "taxpayers": taxpayers,
            "tp_by_id": tp_by_id,
            "latest_by_conn": latest_by_conn,
            "since_default_by_conn": since_default_by_conn,
            "today": dt.date.today().isoformat(),
            "ten_years_ago": (dt.date.today() - dt.timedelta(days=365 * 10)).isoformat(),
        },
    )


@router.post("/connections")
def connections_create(
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    connection_kind: str = Form(default="IB_FLEX_WEB"),
    name: str = Form(...),
    taxpayer_entity_id: int | None = Form(default=None),
    token: str = Form(default=""),
    query_id: str = Form(default=""),
    extra_query_ids: str = Form(default=""),
    data_dir: str = Form(default=""),
    note: str = Form(default=""),
):
    name_u = name.strip()
    if not name_u:
        raise HTTPException(status_code=400, detail="Name is required.")
    if taxpayer_entity_id is None:
        msg = urllib.parse.quote("Create a Taxpayer first (Setup → Create defaults), then create a connection.")
        return RedirectResponse(url=f"/sync/connections?error={msg}", status_code=303)
    existing = session.query(ExternalConnection).filter(ExternalConnection.name == name_u).one_or_none()
    if existing is not None:
        msg = urllib.parse.quote(f"Connection name already exists: {name_u}. Choose a unique name.")
        return RedirectResponse(url=f"/sync/connections?error={msg}", status_code=303)

    kind = (connection_kind or "").strip().upper()
    if kind == "IB_FLEX_WEB":
        if not token.strip() or not query_id.strip():
            msg = urllib.parse.quote(
                "IB Flex (Web Service) requires Token + Flex Query ID(s). "
                "Use the numeric Query ID from IB Portal (Reports → Flex Queries → click the query)."
            )
            return RedirectResponse(url=f"/sync/connections?error={msg}", status_code=303)
        conn = ExternalConnection(
            name=name_u,
            provider="IB",
            broker="IB",
            connector="IB_FLEX_WEB",
            taxpayer_entity_id=taxpayer_entity_id,
            status="ACTIVE",
            metadata_json={
                "extra_query_ids": _split_query_tokens(extra_query_ids),
            },
        )
    elif kind == "CHASE_OFFLINE":
        dd = data_dir.strip()
        conn = ExternalConnection(
            name=name_u,
            provider="CHASE",
            broker="CHASE",
            connector="CHASE_OFFLINE",
            taxpayer_entity_id=taxpayer_entity_id,
            status="ACTIVE",
            metadata_json={
                "data_dir": os.path.expanduser(dd) if dd else None,
            },
        )
    else:
        msg = urllib.parse.quote(f"Unsupported connector: {kind}")
        return RedirectResponse(url=f"/sync/connections?error={msg}", status_code=303)

    session.add(conn)
    try:
        session.flush()
    except IntegrityError:
        session.rollback()
        msg = urllib.parse.quote(f"Connection name already exists: {name_u}. Choose a unique name.")
        return RedirectResponse(url=f"/sync/connections?error={msg}", status_code=303)
    if kind == "IB_FLEX_WEB":
        if not secret_key_available():
            raise HTTPException(status_code=400, detail="APP_SECRET_KEY required to save credentials.")
        upsert_credential(session, connection_id=conn.id, key="IB_FLEX_TOKEN", plaintext=token.strip())
        upsert_credential(session, connection_id=conn.id, key="IB_FLEX_QUERY_ID", plaintext=query_id.strip())
    log_change(
        session,
        actor=actor,
        action="CREATE",
        entity="ExternalConnection",
        entity_id=str(conn.id),
        old=None,
        new=jsonable({"id": conn.id, "name": conn.name, "provider": conn.provider, "broker": conn.broker, "connector": conn.connector}),
        note=note or "Create external connection",
    )
    session.commit()
    return RedirectResponse(url=f"/sync/connections/{conn.id}", status_code=303)


@router.get("/connections/{connection_id}")
def connection_detail(
    connection_id: int,
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    conn = session.query(ExternalConnection).filter(ExternalConnection.id == connection_id).one()
    test_ok = request.query_params.get("test_ok")
    test_msg = request.query_params.get("test_msg")
    meta = conn.metadata_json or {}
    extra_queries_str = ""
    try:
        extra = meta.get("extra_query_ids") or []
        if isinstance(extra, list):
            extra_queries_str = ", ".join([str(x).strip() for x in extra if str(x).strip()])
    except Exception:
        extra_queries_str = ""
    runs = (
        session.query(SyncRun)
        .filter(SyncRun.connection_id == connection_id)
        .order_by(SyncRun.started_at.desc())
        .limit(50)
        .all()
    )
    last_run = runs[0] if runs else None
    since_default = (
        (conn.last_successful_txn_end - dt.timedelta(days=7)).isoformat()
        if conn.last_successful_txn_end
        else ((conn.last_successful_sync_at.date() - dt.timedelta(days=7)).isoformat() if conn.last_successful_sync_at else None)
    )
    taxpayer = session.query(TaxpayerEntity).filter(TaxpayerEntity.id == conn.taxpayer_entity_id).one()

    cred_token_key = "IB_YODLEE_TOKEN"
    cred_qid_key = "IB_YODLEE_QUERY_ID"
    if (conn.connector or "").upper() == "IB_FLEX_WEB":
        cred_token_key = "IB_FLEX_TOKEN"
        cred_qid_key = "IB_FLEX_QUERY_ID"
    token_masked = get_credential_masked(session, connection_id=conn.id, key=cred_token_key)
    qid_masked = get_credential_masked(session, connection_id=conn.id, key=cred_qid_key)
    # Query ids / query names are not secrets (unlike tokens). Show full value when available so users can verify.
    query_id_display = qid_masked
    if cred_qid_key == "IB_FLEX_QUERY_ID":
        qid_plain = get_credential(session, connection_id=conn.id, key=cred_qid_key)
        if qid_plain:
            query_id_display = qid_plain
    secret_ok = secret_key_available()

    holdings_row = (
        session.query(ExternalHoldingSnapshot)
        .filter(ExternalHoldingSnapshot.connection_id == conn.id)
        .order_by(ExternalHoldingSnapshot.as_of.desc(), ExternalHoldingSnapshot.id.desc())
        .first()
    )
    holdings_items = []
    if holdings_row is not None:
        holdings_items = list((holdings_row.payload_json or {}).get("items") or [])

    recent_txns = (
        session.query(Transaction, Account, ExternalTransactionMap)
        .join(ExternalTransactionMap, ExternalTransactionMap.transaction_id == Transaction.id)
        .join(Account, Account.id == Transaction.account_id)
        .filter(ExternalTransactionMap.connection_id == conn.id)
        .order_by(Transaction.date.desc(), Transaction.id.desc())
        .limit(50)
        .all()
    )

    # Simple YTD distribution/withdrawal summary for this connection (useful for IRA monitoring).
    ytd_start = dt.date(dt.date.today().year, 1, 1)
    w_sum, w_cnt = (
        session.query(func.sum(Transaction.amount), func.count(Transaction.id))
        .join(ExternalTransactionMap, ExternalTransactionMap.transaction_id == Transaction.id)
        .filter(
            ExternalTransactionMap.connection_id == conn.id,
            Transaction.type == "TRANSFER",
            Transaction.amount < 0,
            Transaction.date >= ytd_start,
        )
        .one()
    )
    withdrawals_ytd = abs(float(w_sum or 0.0))
    withdrawals_count_ytd = int(w_cnt or 0)

    connector = (conn.connector or "").upper()
    is_offline_files = connector in {"IB_FLEX_OFFLINE", "CHASE_OFFLINE"}
    is_offline_flex = (conn.provider or "").upper() == "IB" and connector == "IB_FLEX_OFFLINE"
    is_chase_offline = (conn.provider or "").upper() == "CHASE" and connector == "CHASE_OFFLINE"
    data_dir_raw = str(meta.get("data_dir") or (Path("data") / "external" / f"conn_{conn.id}"))
    data_dir = os.path.expanduser(data_dir_raw)
    files_on_disk = []
    if is_offline_files:
        p = Path(data_dir)
        if p.exists() and p.is_dir():
            for f in sorted(p.glob("**/*")):
                if f.is_file() and f.suffix.lower() in {".csv", ".tsv", ".xml"}:
                    st = f.stat()
                    files_on_disk.append(
                        {
                            "name": f.name,
                            "path": str(f),
                            "bytes": int(st.st_size),
                            "mtime": utcfromtimestamp(st.st_mtime).isoformat(),
                        }
                    )
    ingested_files = (
        session.query(ExternalFileIngest)
        .filter(ExternalFileIngest.connection_id == conn.id)
        .order_by(ExternalFileIngest.imported_at.desc(), ExternalFileIngest.id.desc())
        .limit(50)
        .all()
    )
    from src.app.main import templates

    return templates.TemplateResponse(
        "sync_connection_detail.html",
        {
            "request": request,
            "actor": actor,
            "auth_banner": auth_banner_message(),
            "conn": conn,
            "taxpayer": taxpayer,
            "runs": runs,
            "last_run": last_run,
            "since_default": since_default,
            "token_masked": token_masked,
            "query_id_masked": qid_masked,
            "query_id_display": query_id_display,
            "cred_token_key": cred_token_key,
            "cred_qid_key": cred_qid_key,
            "secret_key_ok": secret_ok,
            "is_fixtures": bool(
                meta.get("fixture_dir") or meta.get("fixture_accounts") or meta.get("fixture_transactions_pages")
            ),
            "fixture_dir": meta.get("fixture_dir"),
            "is_offline_flex": is_offline_flex,
            "is_offline_files": is_offline_files,
            "is_chase_offline": is_chase_offline,
            "data_dir": data_dir,
            "files_on_disk": files_on_disk,
            "ingested_files": ingested_files,
            "latest_holdings": holdings_row,
            "holdings_items": holdings_items,
            "recent_txns": recent_txns,
            "today": dt.date.today().isoformat(),
            "ten_years_ago": (dt.date.today() - dt.timedelta(days=365 * 10)).isoformat(),
            "test_ok": test_ok,
            "test_msg": test_msg,
            "extra_queries_str": extra_queries_str,
            "withdrawals_ytd": withdrawals_ytd,
            "withdrawals_count_ytd": withdrawals_count_ytd,
        },
    )


@router.post("/connections/{connection_id}/settings")
def connection_update_settings(
    connection_id: int,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    data_dir: str = Form(default=""),
    extra_query_ids: str = Form(default=""),
    note: str = Form(default=""),
):
    conn = session.query(ExternalConnection).filter(ExternalConnection.id == connection_id).one()
    meta = conn.metadata_json or {}
    old = {"data_dir": meta.get("data_dir"), "extra_query_ids": meta.get("extra_query_ids")}
    dd = data_dir.strip()
    meta["data_dir"] = os.path.expanduser(dd) if dd else None
    eq = (extra_query_ids or "").strip()
    if eq:
        meta["extra_query_ids"] = _split_query_tokens(eq)
    elif (conn.connector or "").upper() == "IB_FLEX_WEB":
        # If user clears the field for IB Flex Web, treat as "no extra queries".
        meta["extra_query_ids"] = []
    conn.metadata_json = meta
    session.flush()
    log_change(
        session,
        actor=actor,
        action="UPDATE",
        entity="ExternalConnection",
        entity_id=str(conn.id),
        old=old,
        new={"data_dir": meta.get("data_dir"), "extra_query_ids": meta.get("extra_query_ids")},
        note=note or "Updated connection settings",
    )
    session.commit()
    return RedirectResponse(url=f"/sync/connections/{connection_id}", status_code=303)


@router.post("/connections/{connection_id}/upload")
def connection_upload_file(
    connection_id: int,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    upload: UploadFile = File(...),
    note: str = Form(default=""),
):
    conn = session.query(ExternalConnection).filter(ExternalConnection.id == connection_id).one()
    meta = conn.metadata_json or {}
    dd = os.path.expanduser(str(meta.get("data_dir") or ""))
    base_dir = Path(dd) if dd else (Path("data") / "external" / f"conn_{conn.id}")
    base_dir.mkdir(parents=True, exist_ok=True)

    orig = Path(upload.filename or "upload.bin").name
    safe = "".join(ch for ch in orig if ch.isalnum() or ch in {".", "_", "-"}).strip("._")
    if not safe:
        safe = "upload.bin"
    dest = base_dir / safe
    data = upload.file.read()
    dest.write_bytes(data)

    if not meta.get("data_dir"):
        meta["data_dir"] = str(base_dir)
        conn.metadata_json = meta

    log_change(
        session,
        actor=actor,
        action="UPLOAD",
        entity="ExternalConnection",
        entity_id=str(conn.id),
        old=None,
        new={"file": safe, "bytes": len(data), "data_dir": str(base_dir)},
        note=note or "Uploaded offline statement file",
    )
    session.commit()
    return RedirectResponse(url=f"/sync/connections/{connection_id}", status_code=303)


@router.post("/connections/{connection_id}/run")
def connection_run_sync(
    connection_id: int,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    mode: str = Form(...),
    start_date: str = Form(default=""),
    end_date: str = Form(default=""),
    overlap_days: int = Form(default=7),
    store_payloads: str = Form(default=""),
    reprocess_files: str = Form(default=""),
    note: str = Form(default=""),
):
    conn = session.query(ExternalConnection).filter(ExternalConnection.id == connection_id).one()
    if (conn.status or "").upper() != "ACTIVE":
        msg = urllib.parse.quote("Connection is disabled. Re-enable it before syncing.")
        return RedirectResponse(url=f"/sync/connections/{connection_id}?error={msg}", status_code=303)
    mode_u = mode.strip().upper()
    sd = dt.date.fromisoformat(start_date) if start_date.strip() else None
    ed = dt.date.fromisoformat(end_date) if end_date.strip() else None
    store = store_payloads == "on"
    reprocess = reprocess_files == "on"

    try:
        run = run_sync(
            session,
            connection_id=connection_id,
            mode=mode_u,
            start_date=sd,
            end_date=ed,
            overlap_days=int(overlap_days),
            store_payloads=store if mode_u == "INCREMENTAL" else None,
            actor=actor,
            reprocess_files=reprocess,
        )
    except Exception as e:
        msg = urllib.parse.quote(f"Sync failed: {type(e).__name__}: {e}")
        return RedirectResponse(url=f"/sync/connections/{connection_id}?error={msg}", status_code=303)
    # Additional audit note (no secrets).
    if note.strip():
        log_change(
            session,
            actor=actor,
            action="NOTE",
            entity="SyncRun",
            entity_id=str(run.id),
            old=None,
            new=None,
            note=note.strip(),
        )
        session.commit()

    return RedirectResponse(url=f"/sync/connections/{connection_id}", status_code=303)

@router.post("/connections/{connection_id}/disable")
def connection_disable(
    connection_id: int,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    note: str = Form(default=""),
):
    conn = session.query(ExternalConnection).filter(ExternalConnection.id == connection_id).one()
    old = {"status": conn.status}
    conn.status = "DISABLED"
    session.flush()
    log_change(
        session,
        actor=actor,
        action="UPDATE",
        entity="ExternalConnection",
        entity_id=str(conn.id),
        old=old,
        new={"status": conn.status},
        note=note.strip() or "Disabled connection (to avoid duplicates)",
    )
    session.commit()
    return RedirectResponse(url="/sync/connections", status_code=303)


@router.get("/connections/{connection_id}/auth")
def connection_auth(
    connection_id: int,
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    conn = session.query(ExternalConnection).filter(ExternalConnection.id == connection_id).one()
    connector = (conn.connector or "").upper()
    if connector in {"IB_FLEX_OFFLINE", "CHASE_OFFLINE"}:
        raise HTTPException(status_code=400, detail="This connector does not use stored credentials.")
    cred_token_key = "IB_YODLEE_TOKEN"
    cred_qid_key = "IB_YODLEE_QUERY_ID"
    if (conn.connector or "").upper() == "IB_FLEX_WEB":
        cred_token_key = "IB_FLEX_TOKEN"
        cred_qid_key = "IB_FLEX_QUERY_ID"
    token_masked = get_credential_masked(session, connection_id=conn.id, key=cred_token_key)
    qid_masked = get_credential_masked(session, connection_id=conn.id, key=cred_qid_key)
    query_id_display = qid_masked
    if cred_qid_key == "IB_FLEX_QUERY_ID":
        qid_plain = get_credential(session, connection_id=conn.id, key=cred_qid_key)
        if qid_plain:
            query_id_display = qid_plain
    from src.app.main import templates

    return templates.TemplateResponse(
        "sync_auth.html",
        {
            "request": request,
            "actor": actor,
            "auth_banner": auth_banner_message(),
            "conn": conn,
            "token_masked": token_masked,
            "query_id_masked": qid_masked,
            "query_id_display": query_id_display,
            "cred_token_key": cred_token_key,
            "cred_qid_key": cred_qid_key,
            "secret_key_ok": secret_key_available(),
        },
    )


@router.post("/connections/{connection_id}/auth")
def connection_auth_save(
    connection_id: int,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    token: str = Form(default=""),
    query_id: str = Form(default=""),
    note: str = Form(default=""),
):
    conn = session.query(ExternalConnection).filter(ExternalConnection.id == connection_id).one()
    connector = (conn.connector or "").upper()
    if connector in {"IB_FLEX_OFFLINE", "CHASE_OFFLINE"}:
        raise HTTPException(status_code=400, detail="This connector does not use stored credentials.")
    if not secret_key_available():
        raise HTTPException(status_code=400, detail="APP_SECRET_KEY is required to save credentials.")
    try:
        cred_token_key = "IB_YODLEE_TOKEN"
        cred_qid_key = "IB_YODLEE_QUERY_ID"
        if (conn.connector or "").upper() == "IB_FLEX_WEB":
            cred_token_key = "IB_FLEX_TOKEN"
            cred_qid_key = "IB_FLEX_QUERY_ID"
        if token.strip():
            upsert_credential(session, connection_id=conn.id, key=cred_token_key, plaintext=token.strip())
        if query_id.strip():
            upsert_credential(session, connection_id=conn.id, key=cred_qid_key, plaintext=query_id.strip())
    except CredentialError as e:
        raise HTTPException(status_code=400, detail=str(e))

    log_change(
        session,
        actor=actor,
        action="UPDATE",
        entity="ExternalCredential",
        entity_id=str(conn.id),
        old=None,
        new={"connection_id": conn.id, "keys": ["token", "query_id"]},
        note=note or "Updated external connection credentials",
    )
    session.commit()
    return RedirectResponse(url=f"/sync/connections/{connection_id}", status_code=303)


@router.post("/connections/{connection_id}/test")
def connection_test(
    connection_id: int,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    conn = session.query(ExternalConnection).filter(ExternalConnection.id == connection_id).one()
    adapter = _adapter_for(conn)
    ctx = AdapterConnectionContext(
        connection=conn,
        credentials={
            "IB_YODLEE_TOKEN": get_credential(session, connection_id=conn.id, key="IB_YODLEE_TOKEN"),
            "IB_YODLEE_QUERY_ID": get_credential(session, connection_id=conn.id, key="IB_YODLEE_QUERY_ID"),
            "IB_FLEX_TOKEN": get_credential(session, connection_id=conn.id, key="IB_FLEX_TOKEN"),
            "IB_FLEX_QUERY_ID": get_credential(session, connection_id=conn.id, key="IB_FLEX_QUERY_ID"),
        },
        run_settings={},
    )
    result = adapter.test_connection(ctx)
    log_change(
        session,
        actor=actor,
        action="TEST_CONNECTION",
        entity="ExternalConnection",
        entity_id=str(conn.id),
        old=None,
        new={"ok": bool(result.get("ok")), "message": str(result.get("message"))[:200]},
        note="Test connection (no secrets)",
    )
    session.commit()
    ok = "1" if bool(result.get("ok")) else "0"
    msg = str(result.get("message") or "")
    msg = msg[:300]
    qp = urllib.parse.urlencode({"test_ok": ok, "test_msg": msg})
    return RedirectResponse(url=f"/sync/connections/{connection_id}?{qp}", status_code=303)


@router.post("/connections/{connection_id}/purge")
def connection_purge_imported_data(
    connection_id: int,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    confirm: str = Form(default=""),
    note: str = Form(default=""),
):
    """
    Purge imported artifacts for a connection:
    - Transactions imported via ExternalTransactionMap
    - Holding snapshots + payload snapshots + sync runs + file ingest hashes
    Does NOT delete Accounts/Securities/Lots (manual data safety).
    """
    conn = session.query(ExternalConnection).filter(ExternalConnection.id == connection_id).one()
    if (confirm or "").strip().upper() != "PURGE":
        raise HTTPException(status_code=400, detail="Type PURGE to confirm.")

    txn_ids = [
        r[0]
        for r in session.query(ExternalTransactionMap.transaction_id)
        .filter(ExternalTransactionMap.connection_id == conn.id)
        .all()
    ]
    tx_deleted = 0
    if txn_ids:
        # Remove maps first (FK), then transactions.
        session.query(ExternalTransactionMap).filter(ExternalTransactionMap.connection_id == conn.id).delete(
            synchronize_session=False
        )
        tx_deleted = (
            session.query(Transaction).filter(Transaction.id.in_(txn_ids)).delete(synchronize_session=False)
        )
    else:
        session.query(ExternalTransactionMap).filter(ExternalTransactionMap.connection_id == conn.id).delete(
            synchronize_session=False
        )

    hs_deleted = session.query(ExternalHoldingSnapshot).filter(ExternalHoldingSnapshot.connection_id == conn.id).delete(
        synchronize_session=False
    )
    fi_deleted = session.query(ExternalFileIngest).filter(ExternalFileIngest.connection_id == conn.id).delete(
        synchronize_session=False
    )
    run_ids = [
        r[0]
        for r in session.query(SyncRun.id).filter(SyncRun.connection_id == conn.id).all()
    ]
    ps_deleted = 0
    if run_ids:
        ps_deleted = (
            session.query(ExternalPayloadSnapshot)
            .filter(ExternalPayloadSnapshot.sync_run_id.in_(run_ids))
            .delete(synchronize_session=False)
        )
    runs_deleted = session.query(SyncRun).filter(SyncRun.connection_id == conn.id).delete(synchronize_session=False)

    # Reset pointers/coverage.
    conn.last_successful_sync_at = None
    conn.last_successful_txn_end = None
    conn.holdings_last_asof = None
    conn.txn_earliest_available = None
    conn.last_full_sync_at = None
    conn.coverage_status = "UNKNOWN"
    conn.last_error_json = None
    session.flush()

    log_change(
        session,
        actor=actor,
        action="PURGE_IMPORTED",
        entity="ExternalConnection",
        entity_id=str(conn.id),
        old=None,
        new={
            "transactions_deleted": int(tx_deleted),
            "holdings_snapshots_deleted": int(hs_deleted),
            "payload_snapshots_deleted": int(ps_deleted),
            "sync_runs_deleted": int(runs_deleted),
            "file_ingests_deleted": int(fi_deleted),
        },
        note=note.strip() or "Purged imported sync data (no secrets)",
    )
    session.commit()
    return RedirectResponse(url=f"/sync/connections/{connection_id}", status_code=303)


@router.post("/connections/{connection_id}/rebuild-lots")
def connection_rebuild_lots(
    connection_id: int,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    note: str = Form(default=""),
):
    conn = session.query(ExternalConnection).filter(ExternalConnection.id == connection_id).one()
    rebuild_reconstructed_tax_lots_for_taxpayer(
        session,
        taxpayer_id=conn.taxpayer_entity_id,
        actor=actor,
        note=note or f"Rebuild reconstructed lots from transactions (connection #{conn.id})",
    )
    return RedirectResponse(url=f"/sync/connections/{connection_id}", status_code=303)
