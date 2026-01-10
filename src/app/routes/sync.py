from __future__ import annotations

import datetime as dt
import os
import shutil
import urllib.parse
from pathlib import Path
import re

from fastapi import APIRouter, Depends, Form, Request
from fastapi import HTTPException
from fastapi import File, UploadFile
from fastapi.responses import RedirectResponse
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import flag_modified
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

def _parse_form_date(value: str) -> dt.date | None:
    s = (value or "").strip()
    if not s:
        return None
    # Preferred: ISO 8601 (YYYY-MM-DD)
    try:
        return dt.date.fromisoformat(s[:10])
    except Exception:
        pass
    # Common US format (MM/DD/YYYY)
    for fmt in ("%m/%d/%Y", "%m/%d/%y", "%Y/%m/%d", "%m-%d-%Y", "%m-%d-%y", "%Y-%m-%d"):
        try:
            return dt.datetime.strptime(s.split()[0], fmt).date()
        except Exception:
            continue
    raise ValueError(f"Invalid date: {value!r} (use YYYY-MM-DD or MM/DD/YYYY)")


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
    ok = request.query_params.get("ok")
    show_legacy = (request.query_params.get("show_legacy") or "").strip().lower() in {"1", "true", "yes", "y", "on"}
    q = session.query(ExternalConnection)
    if not show_legacy:
        # Hide legacy/experimental connectors to reduce confusion.
        q = q.filter(ExternalConnection.connector.in_(["IB_FLEX_WEB", "CHASE_OFFLINE", "RJ_OFFLINE"]))
    conns = q.order_by(ExternalConnection.id.desc()).all()
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
    legacy_ib_flex_offline_count = int(
        session.query(func.count(ExternalConnection.id))
        .filter(func.upper(func.coalesce(ExternalConnection.connector, "")) == "IB_FLEX_OFFLINE")
        .scalar()
        or 0
    )

    # Avoid global banner on this page; show inline (Holdings-style) instead.
    auth_banner_detail = auth_banner_message()

    # Bust CSS/JS cache for this page during development (same rationale as Holdings).
    static_version: str = "0"
    try:
        css_path = Path(__file__).resolve().parents[1] / "static" / "app.css"
        static_version = str(int(css_path.stat().st_mtime))
    except Exception:
        static_version = "0"

    # Derived UI helpers (presentation only).
    now_utc = dt.datetime.now(dt.timezone.utc)
    data_dir_by_conn: dict[int, str] = {}
    extra_queries_str_by_conn: dict[int, str] = {}
    conn_warnings_by_id: dict[int, list[str]] = {}
    health_by_conn: dict[int, dict[str, str]] = {}
    time_display_by_conn: dict[int, dict[str, str]] = {}
    creds_status_by_conn: dict[int, str] = {}
    active_count = 0
    complete_count = 0
    partial_count = 0
    unknown_count = 0
    unhealthy_count = 0
    attention_count = 0
    last_success_at: dt.datetime | None = None

    def _cred_keys(connector_u: str) -> tuple[str, str]:
        if connector_u == "IB_FLEX_WEB":
            return ("IB_FLEX_TOKEN", "IB_FLEX_QUERY_ID")
        if connector_u == "CHASE_YODLEE":
            return ("YODLEE_ACCESS_TOKEN", "YODLEE_REFRESH_TOKEN")
        return ("IB_YODLEE_TOKEN", "IB_YODLEE_QUERY_ID")

    def _relative_time(ts: dt.datetime | None) -> str:
        if ts is None:
            return "—"
        try:
            delta = now_utc - ts
        except Exception:
            return "—"
        seconds = max(0, int(delta.total_seconds()))
        if seconds < 60:
            return "just now"
        if seconds < 3600:
            return f"{seconds // 60}m ago"
        if seconds < 86400:
            return f"{seconds // 3600}h ago"
        days = seconds // 86400
        if days < 7:
            return f"{days}d ago"
        # Fall back to a compact date label (month + day).
        try:
            m = ts.strftime("%b")
            return f"{m} {int(ts.day)}"
        except Exception:
            return ts.date().isoformat()

    def _compute_health(*, connector_u: str, status_u: str, coverage_u: str, missing_creds: bool, last_success: dt.datetime | None) -> dict[str, str]:
        if status_u == "DISABLED":
            return {"level": "disabled", "label": "Disabled", "icon": "⏸", "reason": "Connection is disabled", "threshold": "—"}

        if missing_creds:
            return {"level": "unhealthy", "label": "Unhealthy", "icon": "⛔", "reason": "Missing credentials", "threshold": "Credentials required"}

        if last_success is None:
            return {"level": "unhealthy", "label": "Unhealthy", "icon": "⛔", "reason": "Never synced successfully", "threshold": "—"}

        is_web = connector_u in {"IB_FLEX_WEB", "CHASE_YODLEE"}
        if is_web:
            attention_hours = 18
            stale_hours = 36
            threshold = f"Web: attention >{attention_hours}h · unhealthy >{stale_hours}h"
            age_h = (now_utc - last_success).total_seconds() / 3600.0
            if age_h > stale_hours:
                return {"level": "unhealthy", "label": "Unhealthy", "icon": "⛔", "reason": f"Stale (last success {_relative_time(last_success)})", "threshold": threshold}
            if age_h > attention_hours:
                return {"level": "attention", "label": "Attention", "icon": "⚠️", "reason": f"Aging (last success {_relative_time(last_success)})", "threshold": threshold}
        else:
            attention_days = 7
            stale_days = 14
            threshold = f"Offline: attention >{attention_days}d · unhealthy >{stale_days}d"
            age_d = (now_utc - last_success).total_seconds() / 86400.0
            if age_d > stale_days:
                return {"level": "unhealthy", "label": "Unhealthy", "icon": "⛔", "reason": f"Stale (last success {_relative_time(last_success)})", "threshold": threshold}
            if age_d > attention_days:
                return {"level": "attention", "label": "Attention", "icon": "⚠️", "reason": f"Aging (last success {_relative_time(last_success)})", "threshold": threshold}

        if coverage_u in {"PARTIAL", "UNKNOWN"}:
            return {"level": "attention", "label": "Attention", "icon": "⚠️", "reason": f"Coverage {coverage_u.lower()}", "threshold": "Coverage should be COMPLETE"}

        return {"level": "healthy", "label": "Healthy", "icon": "✅", "reason": "Up to date", "threshold": "—"}

    for c in conns:
        cid = int(c.id)
        connector_u = (c.connector or "").upper()
        status_u = (c.status or "").upper()
        coverage_u = (c.coverage_status or "UNKNOWN").upper()
        meta = c.metadata_json or {}
        dd = str(meta.get("data_dir") or "").strip()
        data_dir_by_conn[cid] = dd or f"data/external/conn_{cid}"
        extra_str = ""
        try:
            extra = meta.get("extra_query_ids") or []
            if isinstance(extra, list):
                parts = [str(x).strip() for x in extra if str(x).strip()]
                extra_str = ", ".join(parts)
        except Exception:
            extra_str = ""
        extra_queries_str_by_conn[cid] = extra_str

        warnings: list[str] = []
        if status_u == "ACTIVE":
            active_count += 1
            if coverage_u == "COMPLETE":
                complete_count += 1
            elif coverage_u == "PARTIAL":
                partial_count += 1
            else:
                unknown_count += 1

        if status_u != "ACTIVE":
            warnings.append("Disabled")
        if coverage_u != "COMPLETE":
            warnings.append(f"Coverage: {coverage_u.lower()}")
        if c.last_error_json:
            warnings.append("Last error recorded")
        last_run = latest_by_conn.get(cid)
        if last_run is not None and getattr(last_run, "status", None) not in {None, "SUCCESS"}:
            warnings.append(f"Last run: {last_run.status}")

        if c.last_successful_sync_at is not None:
            if last_success_at is None or c.last_successful_sync_at > last_success_at:
                last_success_at = c.last_successful_sync_at

        uses_credentials = connector_u not in {"CHASE_OFFLINE", "RJ_OFFLINE", "IB_FLEX_OFFLINE"}
        missing_creds = False
        if uses_credentials:
            token_key, qid_key = _cred_keys(connector_u)
            try:
                token_masked = get_credential_masked(session, connection_id=cid, key=token_key)
                qid_masked = get_credential_masked(session, connection_id=cid, key=qid_key)
                if connector_u == "IB_FLEX_WEB":
                    missing_creds = (token_masked == "—") or (qid_masked == "—")
                elif connector_u == "CHASE_YODLEE":
                    # Tokens may be set later; treat as missing only if both are absent.
                    missing_creds = (token_masked == "—") and (qid_masked == "—")
                else:
                    missing_creds = (token_masked == "—") or (qid_masked == "—")
                if missing_creds:
                    warnings.append("Missing credentials")
            except Exception:
                warnings.append("Credentials unreadable (check APP_SECRET_KEY)")
                missing_creds = True
            creds_status_by_conn[cid] = "Missing" if missing_creds else "Stored encrypted"
        else:
            creds_status_by_conn[cid] = "Not used"

        health = _compute_health(
            connector_u=connector_u,
            status_u=status_u,
            coverage_u=coverage_u,
            missing_creds=missing_creds,
            last_success=c.last_successful_sync_at,
        )
        health_by_conn[cid] = health
        if health["level"] == "unhealthy":
            unhealthy_count += 1
        elif health["level"] == "attention":
            attention_count += 1

        time_display_by_conn[cid] = {
            "last_success_rel": _relative_time(c.last_successful_sync_at),
            "last_full_rel": _relative_time(c.last_full_sync_at),
        }

        conn_warnings_by_id[cid] = warnings

    if active_count <= 0:
        coverage_value = "—"
        coverage_subtext = "No active connections"
        coverage_tone = "neutral"
    elif complete_count == active_count:
        coverage_value = "Complete"
        coverage_subtext = f"{complete_count} complete · {partial_count} partial · {unknown_count} unknown"
        coverage_tone = "positive"
    else:
        coverage_value = "Needs attention"
        coverage_subtext = f"{complete_count} complete · {partial_count} partial · {unknown_count} unknown"
        coverage_tone = "warning"

    warnings_count = unhealthy_count + attention_count
    warnings_tone = "warning" if warnings_count > 0 else "neutral"
    warnings_subtext = f"{unhealthy_count} unhealthy · {attention_count} attention"

    from src.app.main import templates

    return templates.TemplateResponse(
        "sync_connections.html",
        {
            "request": request,
            "actor": actor,
            # Connections page shows auth warning inline (non-intrusive) vs global banner.
            "auth_banner": None,
            "auth_banner_detail": auth_banner_detail,
            "static_version": static_version,
            "secret_key_ok": secret_ok,
            "error": error,
            "ok": ok,
            "show_legacy": show_legacy,
            "connections": conns,
            "taxpayers": taxpayers,
            "tp_by_id": tp_by_id,
            "latest_by_conn": latest_by_conn,
            "since_default_by_conn": since_default_by_conn,
            "today": dt.date.today().isoformat(),
            "ten_years_ago": (dt.date.today() - dt.timedelta(days=365 * 10)).isoformat(),
            "legacy_ib_flex_offline_count": legacy_ib_flex_offline_count,
            # KPI cards
            "connections_total_count": str(len(conns)),
            "connections_active_count": str(active_count),
            "coverage_value": coverage_value,
            "coverage_subtext": coverage_subtext,
            "coverage_tone": coverage_tone,
            "last_success_at": last_success_at,
            "warnings_count": str(warnings_count),
            "warnings_tone": warnings_tone,
            "warnings_subtext": warnings_subtext,
            # Per-connection UI helpers
            "data_dir_by_conn": data_dir_by_conn,
            "extra_queries_str_by_conn": extra_queries_str_by_conn,
            "conn_warnings_by_id": conn_warnings_by_id,
            "health_by_conn": health_by_conn,
            "time_display_by_conn": time_display_by_conn,
            "creds_status_by_conn": creds_status_by_conn,
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
    refresh_token: str = Form(default=""),
    query_id: str = Form(default=""),
    extra_query_ids: str = Form(default=""),
    data_dir: str = Form(default=""),
    yodlee_base_url: str = Form(default=""),
    yodlee_api_version: str = Form(default=""),
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
    elif kind == "RJ_OFFLINE":
        dd = data_dir.strip()
        conn = ExternalConnection(
            name=name_u,
            provider="RJ",
            broker="RJ",
            connector="RJ_OFFLINE",
            taxpayer_entity_id=taxpayer_entity_id,
            status="ACTIVE",
            metadata_json={
                "data_dir": os.path.expanduser(dd) if dd else None,
            },
        )
    elif kind == "CHASE_YODLEE":
        conn = ExternalConnection(
            name=name_u,
            provider="YODLEE",
            broker="CHASE",
            connector="CHASE_YODLEE",
            taxpayer_entity_id=taxpayer_entity_id,
            status="ACTIVE",
            metadata_json={
                "yodlee_base_url": yodlee_base_url.strip() or None,
                "yodlee_api_version": yodlee_api_version.strip() or None,
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
    if kind == "CHASE_YODLEE":
        # Tokens are optional at create time; user can link later via the Credentials page.
        if (token.strip() or refresh_token.strip()) and not secret_key_available():
            raise HTTPException(status_code=400, detail="APP_SECRET_KEY required to save credentials.")
        if token.strip():
            upsert_credential(session, connection_id=conn.id, key="YODLEE_ACCESS_TOKEN", plaintext=token.strip())
        if refresh_token.strip():
            upsert_credential(session, connection_id=conn.id, key="YODLEE_REFRESH_TOKEN", plaintext=refresh_token.strip())
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
    error_msg = request.query_params.get("error")
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
    connector_u = (conn.connector or "").upper()
    if connector_u == "IB_FLEX_WEB":
        cred_token_key = "IB_FLEX_TOKEN"
        cred_qid_key = "IB_FLEX_QUERY_ID"
    elif connector_u == "CHASE_YODLEE":
        cred_token_key = "YODLEE_ACCESS_TOKEN"
        cred_qid_key = "YODLEE_REFRESH_TOKEN"
    token_masked = get_credential_masked(session, connection_id=conn.id, key=cred_token_key)
    qid_masked = get_credential_masked(session, connection_id=conn.id, key=cred_qid_key)
    # Query ids / query names are not secrets (unlike tokens). Show full value when available so users can verify.
    query_id_display = qid_masked
    if cred_qid_key == "IB_FLEX_QUERY_ID":
        qid_plain = get_credential(session, connection_id=conn.id, key=cred_qid_key)
        if qid_plain:
            query_id_display = qid_plain
    secret_ok = secret_key_available()
    auth_banner_detail = auth_banner_message()

    # Bust CSS/JS cache for this page during development.
    static_version: str = "0"
    try:
        css_path = Path(__file__).resolve().parents[1] / "static" / "app.css"
        static_version = str(int(css_path.stat().st_mtime))
    except Exception:
        static_version = "0"

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
    is_offline_files = connector in {"IB_FLEX_OFFLINE", "CHASE_OFFLINE", "RJ_OFFLINE"}
    is_ib_flex_web = connector == "IB_FLEX_WEB"
    is_offline_flex = (conn.provider or "").upper() == "IB" and connector == "IB_FLEX_OFFLINE"
    is_chase_offline = connector == "CHASE_OFFLINE"
    is_chase_yodlee = connector == "CHASE_YODLEE"
    is_rj_offline = connector == "RJ_OFFLINE"
    default_dir = Path("data") / "external" / f"conn_{conn.id}"
    data_dir_raw = str(meta.get("data_dir") or default_dir)
    data_dir_path = Path(os.path.expanduser(data_dir_raw))
    try:
        data_dir_path = data_dir_path.resolve()
    except Exception:
        pass
    data_dir = str(data_dir_path)
    files_on_disk = []
    if is_offline_files or is_ib_flex_web:
        p = Path(data_dir)
        supported_exts = {".csv", ".tsv", ".txt", ".xml"}
        if is_rj_offline:
            supported_exts.update({".qfx", ".ofx"})
        if is_rj_offline or is_chase_offline:
            supported_exts.add(".pdf")
        if p.exists() and p.is_dir():
            for f in sorted(p.glob("**/*"))[:200]:
                if f.is_file():
                    st = f.stat()
                    ext = f.suffix.lower()
                    supported = ext in supported_exts
                    supported_label = "Yes" if supported else "No"
                    if (is_rj_offline or is_chase_offline) and ext == ".pdf":
                        supported_label = "Yes (Holdings total from statement PDF)"
                    if is_rj_offline and ext in {".qfx", ".ofx"}:
                        supported_label = "Yes (QFX/OFX investment download)"
                    files_on_disk.append(
                        {
                            "name": f.name,
                            "path": str(f),
                            "bytes": int(st.st_size),
                            "mtime": utcfromtimestamp(st.st_mtime).isoformat(),
                            "supported": supported,
                            "supported_label": supported_label,
                        }
                    )
    ingested_files = (
        session.query(ExternalFileIngest)
        .filter(ExternalFileIngest.connection_id == conn.id)
        .order_by(ExternalFileIngest.imported_at.desc(), ExternalFileIngest.id.desc())
        .limit(50)
        .all()
    )

    # Presentation-only helpers
    now_utc = dt.datetime.now(dt.timezone.utc)
    last_success_rel = "—"
    last_full_rel = "—"
    try:
        # Mirror relative time format used on Connections list.
        def _rel(ts: dt.datetime | None) -> str:
            if ts is None:
                return "—"
            seconds = max(0, int((now_utc - ts).total_seconds()))
            if seconds < 60:
                return "just now"
            if seconds < 3600:
                return f"{seconds // 60}m ago"
            if seconds < 86400:
                return f"{seconds // 3600}h ago"
            days = seconds // 86400
            if days < 7:
                return f"{days}d ago"
            try:
                m = ts.strftime("%b")
                return f"{m} {int(ts.day)}"
            except Exception:
                return ts.date().isoformat()

        last_success_rel = _rel(conn.last_successful_sync_at)
        last_full_rel = _rel(conn.last_full_sync_at)
    except Exception:
        last_success_rel = "—"
        last_full_rel = "—"

    missing_creds = False
    creds_status = "—"
    if connector_u in {"CHASE_OFFLINE", "RJ_OFFLINE", "IB_FLEX_OFFLINE"}:
        creds_status = "Not used"
    else:
        try:
            if connector_u == "CHASE_YODLEE":
                missing_creds = (token_masked == "—") and (qid_masked == "—")
            elif connector_u == "IB_FLEX_WEB":
                missing_creds = (token_masked == "—") or (qid_masked == "—")
            else:
                missing_creds = (token_masked == "—") or (qid_masked == "—")
        except Exception:
            missing_creds = True
        creds_status = "Missing" if missing_creds else "Stored encrypted"

    # Health (UI interpretation only; does not affect sync behavior).
    coverage_u = (conn.coverage_status or "UNKNOWN").upper()
    status_u = (conn.status or "").upper()
    is_web = connector_u in {"IB_FLEX_WEB", "CHASE_YODLEE"}
    health: dict[str, str] = {"level": "attention", "label": "Attention", "icon": "⚠️", "reason": "—", "threshold": "—"}
    if status_u == "DISABLED":
        health = {"level": "disabled", "label": "Disabled", "icon": "⏸", "reason": "Connection is disabled", "threshold": "—"}
    elif missing_creds:
        health = {"level": "unhealthy", "label": "Unhealthy", "icon": "⛔", "reason": "Missing credentials", "threshold": "Credentials required"}
    elif conn.last_successful_sync_at is None:
        health = {"level": "unhealthy", "label": "Unhealthy", "icon": "⛔", "reason": "Never synced successfully", "threshold": "—"}
    else:
        aging_or_stale = False
        if is_web:
            attention_hours = 18
            stale_hours = 36
            threshold = f"Web: attention >{attention_hours}h · unhealthy >{stale_hours}h"
            age_h = (now_utc - conn.last_successful_sync_at).total_seconds() / 3600.0
            if age_h > stale_hours:
                health = {
                    "level": "unhealthy",
                    "label": "Unhealthy",
                    "icon": "⛔",
                    "reason": f"Stale (last success {last_success_rel})",
                    "threshold": threshold,
                }
                aging_or_stale = True
            elif age_h > attention_hours:
                health = {
                    "level": "attention",
                    "label": "Attention",
                    "icon": "⚠️",
                    "reason": f"Aging (last success {last_success_rel})",
                    "threshold": threshold,
                }
                aging_or_stale = True
        else:
            attention_days = 7
            stale_days = 14
            threshold = f"Offline: attention >{attention_days}d · unhealthy >{stale_days}d"
            age_d = (now_utc - conn.last_successful_sync_at).total_seconds() / 86400.0
            if age_d > stale_days:
                health = {
                    "level": "unhealthy",
                    "label": "Unhealthy",
                    "icon": "⛔",
                    "reason": f"Stale (last success {last_success_rel})",
                    "threshold": threshold,
                }
                aging_or_stale = True
            elif age_d > attention_days:
                health = {
                    "level": "attention",
                    "label": "Attention",
                    "icon": "⚠️",
                    "reason": f"Aging (last success {last_success_rel})",
                    "threshold": threshold,
                }
                aging_or_stale = True

        if not aging_or_stale and coverage_u in {"PARTIAL", "UNKNOWN"}:
            health = {
                "level": "attention",
                "label": "Attention",
                "icon": "⚠️",
                "reason": f"Coverage {coverage_u.lower()}",
                "threshold": "Coverage should be COMPLETE",
            }
        elif not aging_or_stale:
            health = {"level": "healthy", "label": "Healthy", "icon": "✅", "reason": "Up to date", "threshold": "—"}

    if health["level"] == "healthy":
        health_badge = "ui-badge--safe"
    elif health["level"] == "unhealthy":
        health_badge = "ui-badge--bad"
    elif health["level"] == "disabled":
        health_badge = "ui-badge--neutral"
    else:
        health_badge = "ui-badge--risk"

    last_run_status = (getattr(last_run, "status", None) or "—") if last_run else "—"
    last_run_mode = (getattr(last_run, "mode", None) or "—") if last_run else "—"
    last_run_duration_s: int | None = None
    try:
        if last_run and last_run.started_at and last_run.finished_at:
            last_run_duration_s = int((last_run.finished_at - last_run.started_at).total_seconds())
    except Exception:
        last_run_duration_s = None
    last_run_tone = "neutral"
    if last_run_status == "SUCCESS":
        last_run_tone = "positive"
    elif last_run_status in {"PARTIAL", "FAIL", "FAILED", "ERROR"}:
        last_run_tone = "warning"

    # Files summary for collapsed accordions.
    files_count = int(len(files_on_disk or []))
    last_upload_at: dt.datetime | None = None
    try:
        if ingested_files:
            last_upload_at = ingested_files[0].imported_at
    except Exception:
        last_upload_at = None

    from src.app.main import templates

    return templates.TemplateResponse(
        "sync_connection_detail.html",
        {
            "request": request,
            "actor": actor,
            # Detail page shows auth warning inline (non-intrusive) vs global banner.
            "auth_banner": None,
            "auth_banner_detail": auth_banner_detail,
            "static_version": static_version,
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
            "is_ib_flex_web": is_ib_flex_web,
            "is_chase_offline": is_chase_offline,
            "is_chase_yodlee": is_chase_yodlee,
            "is_rj_offline": is_rj_offline,
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
            "error_msg": error_msg,
            "extra_queries_str": extra_queries_str,
            "withdrawals_ytd": withdrawals_ytd,
            "withdrawals_count_ytd": withdrawals_count_ytd,
            # Presentation helpers
            "health": health,
            "health_badge": health_badge,
            "creds_status": creds_status,
            "missing_creds": missing_creds,
            "coverage_u": coverage_u,
            "status_u": status_u,
            "last_success_rel": last_success_rel,
            "last_full_rel": last_full_rel,
            "last_run_status": last_run_status,
            "last_run_mode": last_run_mode,
            "last_run_duration_s": last_run_duration_s,
            "last_run_tone": last_run_tone,
            "files_count": files_count,
            "last_upload_at": last_upload_at,
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
    meta = dict(conn.metadata_json or {})
    old = {"data_dir": meta.get("data_dir"), "extra_query_ids": meta.get("extra_query_ids")}
    dd = data_dir.strip()
    if dd:
        dd_path = Path(os.path.expanduser(dd))
        try:
            dd_path = dd_path.resolve()
        except Exception:
            pass
        meta["data_dir"] = str(dd_path)
        # Best-effort: create dir so users can immediately drop files in (offline connectors + IB baseline statements).
        if (conn.connector or "").upper() in {"IB_FLEX_OFFLINE", "CHASE_OFFLINE", "RJ_OFFLINE", "IB_FLEX_WEB"}:
            try:
                dd_path.mkdir(parents=True, exist_ok=True)
            except Exception as e:
                msg = urllib.parse.quote(f"Failed to create data directory: {type(e).__name__}: {e}")
                return RedirectResponse(url=f"/sync/connections/{connection_id}?error={msg}", status_code=303)
    else:
        meta["data_dir"] = None
    eq = (extra_query_ids or "").strip()
    if eq:
        meta["extra_query_ids"] = _split_query_tokens(eq)
    elif (conn.connector or "").upper() == "IB_FLEX_WEB":
        # If user clears the field for IB Flex Web, treat as "no extra queries".
        meta["extra_query_ids"] = []
    conn.metadata_json = dict(meta)
    # Ensure JSON updates persist even if the ORM doesn't detect mutations for this column type.
    flag_modified(conn, "metadata_json")
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
    meta = dict(conn.metadata_json or {})
    dd = os.path.expanduser(str(meta.get("data_dir") or ""))
    base_dir = Path(dd) if dd else (Path("data") / "external" / f"conn_{conn.id}")
    base_dir.mkdir(parents=True, exist_ok=True)
    try:
        base_dir = base_dir.resolve()
    except Exception:
        pass

    orig = Path(upload.filename or "upload.bin").name
    safe = "".join(ch for ch in orig if ch.isalnum() or ch in {".", "_", "-"}).strip("._")
    if not safe:
        safe = "upload.bin"
    dest = base_dir / safe
    with dest.open("wb") as f:
        shutil.copyfileobj(upload.file, f)
    try:
        size = int(dest.stat().st_size)
    except Exception:
        size = 0

    if not meta.get("data_dir"):
        meta["data_dir"] = str(base_dir)
    conn.metadata_json = dict(meta)
    flag_modified(conn, "metadata_json")

    log_change(
        session,
        actor=actor,
        action="UPLOAD",
        entity="ExternalConnection",
        entity_id=str(conn.id),
        old=None,
        new={"file": safe, "bytes": size, "data_dir": str(base_dir)},
        note=note or "Uploaded offline statement file",
    )
    session.commit()
    ext = Path(safe).suffix.lower()
    supported_exts = {".csv", ".tsv", ".txt", ".xml"}
    if (conn.connector or "").upper() == "RJ_OFFLINE":
        supported_exts.update({".qfx", ".ofx"})
    if (conn.connector or "").upper() in {"RJ_OFFLINE", "CHASE_OFFLINE"}:
        supported_exts.add(".pdf")
    if ext and ext not in supported_exts:
        hint = ".csv/.tsv/.txt/.xml"
        if (conn.connector or "").upper() == "RJ_OFFLINE":
            hint = ".qfx/.ofx (preferred), .csv/.tsv/.txt/.xml (legacy), or .pdf statements (holdings totals only)"
        elif (conn.connector or "").upper() == "CHASE_OFFLINE":
            hint = ".csv/.tsv/.txt/.xml (or .pdf statements for holdings totals)"
        msg = urllib.parse.quote(
            f"Uploaded '{safe}', but this file type ({ext}) is not parsed. Export as {hint} and re-upload."
        )
        return RedirectResponse(url=f"/sync/connections/{connection_id}?error={msg}", status_code=303)
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

    # Guard against double-submits: SQLite will frequently error with "database is locked" if we try to run
    # two syncs concurrently in the same local DB.
    try:
        existing = (
            session.query(SyncRun)
            .filter(SyncRun.connection_id == connection_id, SyncRun.finished_at.is_(None))
            .order_by(SyncRun.started_at.desc())
            .first()
        )
        if existing is not None and existing.started_at is not None:
            age = dt.datetime.now(dt.timezone.utc) - existing.started_at
            if age < dt.timedelta(hours=6):
                msg = urllib.parse.quote(f"Sync already running (started {existing.started_at.isoformat()}).")
                return RedirectResponse(url=f"/sync/connections/{connection_id}?error={msg}", status_code=303)
    except Exception:
        pass
    mode_u = mode.strip().upper()
    try:
        sd = _parse_form_date(start_date)
        ed = _parse_form_date(end_date)
    except ValueError as e:
        msg = urllib.parse.quote(str(e))
        return RedirectResponse(url=f"/sync/connections/{connection_id}?error={msg}", status_code=303)
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
    except OperationalError as e:
        if "database is locked" in str(e).lower():
            msg = urllib.parse.quote("Sync failed: database is locked. Wait for the current sync to finish and try again.")
        else:
            msg = urllib.parse.quote(f"Sync failed: OperationalError: {e}")
        return RedirectResponse(url=f"/sync/connections/{connection_id}?error={msg}", status_code=303)
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
    if connector in {"IB_FLEX_OFFLINE", "CHASE_OFFLINE", "RJ_OFFLINE"}:
        raise HTTPException(status_code=400, detail="This connector does not use stored credentials.")
    cred_token_key = "IB_YODLEE_TOKEN"
    cred_qid_key = "IB_YODLEE_QUERY_ID"
    if connector == "IB_FLEX_WEB":
        cred_token_key = "IB_FLEX_TOKEN"
        cred_qid_key = "IB_FLEX_QUERY_ID"
    elif connector == "CHASE_YODLEE":
        cred_token_key = "YODLEE_ACCESS_TOKEN"
        cred_qid_key = "YODLEE_REFRESH_TOKEN"
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
            "connector": connector,
            "token_masked": token_masked,
            "query_id_masked": qid_masked,
            "query_id_display": query_id_display,
            "cred_token_key": cred_token_key,
            "cred_qid_key": cred_qid_key,
            "secret_key_ok": secret_key_available(),
            "yodlee_base_url": (conn.metadata_json or {}).get("yodlee_base_url") or "",
            "yodlee_api_version": (conn.metadata_json or {}).get("yodlee_api_version") or "",
        },
    )


@router.post("/connections/{connection_id}/auth")
def connection_auth_save(
    connection_id: int,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    token: str = Form(default=""),
    refresh_token: str = Form(default=""),
    query_id: str = Form(default=""),
    yodlee_base_url: str = Form(default=""),
    yodlee_api_version: str = Form(default=""),
    note: str = Form(default=""),
):
    conn = session.query(ExternalConnection).filter(ExternalConnection.id == connection_id).one()
    connector = (conn.connector or "").upper()
    if connector in {"IB_FLEX_OFFLINE", "CHASE_OFFLINE", "RJ_OFFLINE"}:
        raise HTTPException(status_code=400, detail="This connector does not use stored credentials.")
    if not secret_key_available():
        raise HTTPException(status_code=400, detail="APP_SECRET_KEY is required to save credentials.")
    try:
        cred_token_key = "IB_YODLEE_TOKEN"
        cred_qid_key = "IB_YODLEE_QUERY_ID"
        if connector == "IB_FLEX_WEB":
            cred_token_key = "IB_FLEX_TOKEN"
            cred_qid_key = "IB_FLEX_QUERY_ID"
        elif connector == "CHASE_YODLEE":
            cred_token_key = "YODLEE_ACCESS_TOKEN"
            cred_qid_key = "YODLEE_REFRESH_TOKEN"

        if connector == "CHASE_YODLEE":
            if token.strip():
                upsert_credential(session, connection_id=conn.id, key=cred_token_key, plaintext=token.strip())
            if refresh_token.strip():
                upsert_credential(session, connection_id=conn.id, key=cred_qid_key, plaintext=refresh_token.strip())
            meta = conn.metadata_json or {}
            if yodlee_base_url.strip():
                meta["yodlee_base_url"] = yodlee_base_url.strip()
            if yodlee_api_version.strip():
                meta["yodlee_api_version"] = yodlee_api_version.strip()
            conn.metadata_json = meta
        else:
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
            "YODLEE_ACCESS_TOKEN": get_credential(session, connection_id=conn.id, key="YODLEE_ACCESS_TOKEN"),
            "YODLEE_REFRESH_TOKEN": get_credential(session, connection_id=conn.id, key="YODLEE_REFRESH_TOKEN"),
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

@router.post("/connections/purge-ib-flex-offline")
def purge_all_ib_flex_offline(
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    confirm: str = Form(default=""),
    note: str = Form(default=""),
):
    """
    Bulk purge for deprecated IB_FLEX_OFFLINE connections.
    Intended to remove duplicate-imported artifacts from legacy connectors.
    """
    if (confirm or "").strip().upper() != "PURGE":
        raise HTTPException(status_code=400, detail="Type PURGE to confirm.")

    conns = (
        session.query(ExternalConnection)
        .filter(func.upper(func.coalesce(ExternalConnection.connector, "")) == "IB_FLEX_OFFLINE")
        .all()
    )
    if not conns:
        msg = urllib.parse.quote("No IB_FLEX_OFFLINE connections found.")
        return RedirectResponse(url=f"/sync/connections?show_legacy=1&ok={msg}", status_code=303)

    totals = {
        "connections": 0,
        "transactions_deleted": 0,
        "holdings_snapshots_deleted": 0,
        "payload_snapshots_deleted": 0,
        "sync_runs_deleted": 0,
        "file_ingests_deleted": 0,
    }

    for conn in conns:
        txn_ids = [
            r[0]
            for r in session.query(ExternalTransactionMap.transaction_id)
            .filter(ExternalTransactionMap.connection_id == conn.id)
            .all()
        ]
        tx_deleted = 0
        if txn_ids:
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

        hs_deleted = session.query(ExternalHoldingSnapshot).filter(
            ExternalHoldingSnapshot.connection_id == conn.id
        ).delete(synchronize_session=False)
        fi_deleted = session.query(ExternalFileIngest).filter(ExternalFileIngest.connection_id == conn.id).delete(
            synchronize_session=False
        )
        run_ids = [r[0] for r in session.query(SyncRun.id).filter(SyncRun.connection_id == conn.id).all()]
        ps_deleted = 0
        if run_ids:
            ps_deleted = (
                session.query(ExternalPayloadSnapshot)
                .filter(ExternalPayloadSnapshot.sync_run_id.in_(run_ids))
                .delete(synchronize_session=False)
            )
        runs_deleted = session.query(SyncRun).filter(SyncRun.connection_id == conn.id).delete(
            synchronize_session=False
        )

        # Reset pointers/coverage and disable connector to prevent accidental re-sync.
        conn.last_successful_sync_at = None
        conn.last_successful_txn_end = None
        conn.holdings_last_asof = None
        conn.txn_earliest_available = None
        conn.last_full_sync_at = None
        conn.coverage_status = "UNKNOWN"
        conn.last_error_json = None
        conn.status = "DISABLED"
        session.flush()

        log_change(
            session,
            actor=actor,
            action="PURGE_LEGACY_IB_FLEX_OFFLINE",
            entity="ExternalConnection",
            entity_id=str(conn.id),
            old=None,
            new={
                "transactions_deleted": int(tx_deleted),
                "holdings_snapshots_deleted": int(hs_deleted),
                "payload_snapshots_deleted": int(ps_deleted),
                "sync_runs_deleted": int(runs_deleted),
                "file_ingests_deleted": int(fi_deleted),
                "status": str(conn.status),
            },
            note=note.strip() or "Purged legacy IB_FLEX_OFFLINE imported data",
        )

        totals["connections"] += 1
        totals["transactions_deleted"] += int(tx_deleted)
        totals["holdings_snapshots_deleted"] += int(hs_deleted)
        totals["payload_snapshots_deleted"] += int(ps_deleted)
        totals["sync_runs_deleted"] += int(runs_deleted)
        totals["file_ingests_deleted"] += int(fi_deleted)

    session.commit()
    msg = urllib.parse.quote(
        f"Purged IB_FLEX_OFFLINE: conns={totals['connections']}, tx={totals['transactions_deleted']}, snaps={totals['holdings_snapshots_deleted']}"
    )
    return RedirectResponse(url=f"/sync/connections?show_legacy=1&ok={msg}", status_code=303)


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
