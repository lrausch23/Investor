from __future__ import annotations

import datetime as dt
import hashlib
import os
import time
import shutil
import urllib.parse
from pathlib import Path
import re

from fastapi import APIRouter, Depends, Form, Request
from fastapi import HTTPException
from fastapi import File, UploadFile
from fastapi.responses import RedirectResponse
from fastapi.responses import JSONResponse
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import flag_modified
from sqlalchemy import and_, func, or_, text

from src.app.auth import auth_banner_message, require_actor
from src.app.db import db_session
from src.app.utils import jsonable
from src.utils.money import format_usd
from src.core.credential_store import (
    CredentialError,
    get_credential,
    get_credential_masked,
    secret_key_available,
    upsert_credential,
)
from src.core.sync_runner import run_sync
from src.core.lot_reconstruction import rebuild_reconstructed_tax_lots_for_taxpayer
from src.core.cashflow_supplement import import_supplemental_cashflows
from src.db.audit import log_change
from src.importers.adapters import ProviderError
from src.db.models import (
    Account,
    BrokerLotClosure,
    BrokerWashSaleEvent,
    CashBalance,
    CorporateActionEvent,
    ExternalConnection,
    ExternalAccountMap,
    ExternalCardStatement,
    ExternalCredential,
    ExternalFileIngest,
    ExternalHoldingSnapshot,
    ExternalPayloadSnapshot,
    ExternalTransactionMap,
    ExpenseAccount,
    ExpenseImportBatch,
    ExpenseTransaction,
    IncomeEvent,
    LotDisposal,
    PositionLot,
    WashSaleAdjustment,
    SyncRun,
    TaxpayerEntity,
    TaxLot,
    Transaction,
)
from src.core.sync_runner import AdapterConnectionContext, _adapter_for
from src.utils.time import utcfromtimestamp
from src.adapters.plaid_chase.client import PlaidApiError, PlaidClient
from src.investor.cash_bills.card_statement_pdf import parse_chase_card_statement_pdf


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


def _sha256_path(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


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
        q = q.filter(
            ExternalConnection.connector.in_(
                ["IB_FLEX_WEB", "CHASE_PLAID", "AMEX_PLAID", "CHASE_OFFLINE", "RJ_OFFLINE"]
            )
        )
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
        if connector_u in {"CHASE_PLAID", "AMEX_PLAID"}:
            return ("PLAID_ACCESS_TOKEN", "PLAID_ITEM_ID")
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

        is_web = connector_u in {"IB_FLEX_WEB", "CHASE_YODLEE", "CHASE_PLAID", "AMEX_PLAID"}
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
                elif connector_u in {"CHASE_PLAID", "AMEX_PLAID"}:
                    # Link token exchange writes the access token; item_id is informational.
                    missing_creds = token_masked == "—"
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
    elif kind == "CHASE_PLAID":
        # Credentials are established via Plaid Link from the Credentials page after creation.
        pe_raw = (os.environ.get("PLAID_ENV") or "production").strip().lower() or "production"
        pe = "sandbox" if pe_raw in {"dev", "development"} else pe_raw
        conn = ExternalConnection(
            name=name_u,
            provider="PLAID",
            broker="CHASE",
            connector="CHASE_PLAID",
            taxpayer_entity_id=taxpayer_entity_id,
            status="ACTIVE",
            metadata_json={
                "plaid_env": pe,
            },
        )
    elif kind == "AMEX_PLAID":
        pe_raw = (os.environ.get("PLAID_ENV") or "production").strip().lower() or "production"
        pe = "sandbox" if pe_raw in {"dev", "development"} else pe_raw
        conn = ExternalConnection(
            name=name_u,
            provider="PLAID",
            broker="AMEX",
            connector="AMEX_PLAID",
            taxpayer_entity_id=taxpayer_entity_id,
            status="ACTIVE",
            metadata_json={
                "plaid_env": pe,
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
    ok_msg = request.query_params.get("ok")
    error_msg = request.query_params.get("error")
    tx_page_raw = (request.query_params.get("tx_page") or "").strip()
    tx_page = 1
    try:
        if tx_page_raw.isdigit():
            tx_page = max(1, int(tx_page_raw))
    except Exception:
        tx_page = 1
    tx_page_size = 50
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
    elif connector_u in {"CHASE_PLAID", "AMEX_PLAID"}:
        cred_token_key = "PLAID_ACCESS_TOKEN"
        cred_qid_key = "PLAID_ITEM_ID"
    token_masked = get_credential_masked(session, connection_id=conn.id, key=cred_token_key)
    qid_masked = get_credential_masked(session, connection_id=conn.id, key=cred_qid_key)
    # Query ids / query names are not secrets (unlike tokens). Show full value when available so users can verify.
    query_id_display = qid_masked
    if cred_qid_key in {"IB_FLEX_QUERY_ID", "PLAID_ITEM_ID"}:
        qid_plain = get_credential(session, connection_id=conn.id, key=cred_qid_key)
        if qid_plain:
            query_id_display = qid_plain
    if connector_u == "CHASE_PLAID":
        try:
            session.execute(
                text(
                    """
                    UPDATE transactions
                    SET type = CASE
                        WHEN upper(json_extract(lot_links_json, '$.description')) LIKE '%INTEREST%' THEN 'INT'
                        WHEN upper(json_extract(lot_links_json, '$.description')) LIKE '%DIV%' THEN 'DIV'
                        ELSE 'DIV'
                    END
                    WHERE type = 'INCOME'
                      AND id IN (
                        SELECT transaction_id FROM external_transaction_map WHERE connection_id = :conn_id
                      )
                    """
                ),
                {"conn_id": conn.id},
            )
            session.commit()
        except Exception:
            session.rollback()
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

    total_txns = (
        session.query(func.count(Transaction.id))
        .join(ExternalTransactionMap, ExternalTransactionMap.transaction_id == Transaction.id)
        .filter(ExternalTransactionMap.connection_id == conn.id)
        .scalar()
        or 0
    )
    tx_pages = max(1, int((int(total_txns) + tx_page_size - 1) // tx_page_size))
    if tx_page > tx_pages:
        tx_page = tx_pages
    tx_offset = int((tx_page - 1) * tx_page_size)

    recent_txns = (
        session.query(Transaction, Account, ExternalTransactionMap)
        .join(ExternalTransactionMap, ExternalTransactionMap.transaction_id == Transaction.id)
        .join(Account, Account.id == Transaction.account_id)
        .filter(ExternalTransactionMap.connection_id == conn.id)
        .order_by(Transaction.date.desc(), Transaction.id.desc())
        .limit(tx_page_size)
        .offset(tx_offset)
        .all()
    )

    tx_min, tx_max, tx_count = (
        session.query(func.min(Transaction.date), func.max(Transaction.date), func.count(Transaction.id))
        .join(ExternalTransactionMap, ExternalTransactionMap.transaction_id == Transaction.id)
        .filter(ExternalTransactionMap.connection_id == conn.id)
        .one()
    )
    tx_stats = {"min": tx_min, "max": tx_max, "count": int(tx_count or 0)}
    plaid_24m_start: dt.date | None = None
    plaid_24m_warn_start: dt.date | None = None
    amex_23m_start: dt.date | None = None
    try:
        if (conn.connector or "").upper() == "CHASE_PLAID":
            plaid_24m_start = dt.date.today() - dt.timedelta(days=730)
            plaid_24m_warn_start = plaid_24m_start + dt.timedelta(days=45)
        if (conn.connector or "").upper() == "AMEX_PLAID":
            amex_23m_start = dt.date.today() - dt.timedelta(days=700)
    except Exception:
        plaid_24m_start = None
        plaid_24m_warn_start = None
        amex_23m_start = None

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
    is_chase_plaid = connector == "CHASE_PLAID"
    is_amex_plaid = connector == "AMEX_PLAID"
    is_plaid = is_chase_plaid or is_amex_plaid
    is_rj_offline = connector == "RJ_OFFLINE"
    default_dir = Path("data") / "external" / f"conn_{conn.id}"
    data_dir_raw = str(meta.get("data_dir") or default_dir)
    data_dir_path = Path(os.path.expanduser(data_dir_raw))
    try:
        data_dir_path = data_dir_path.resolve()
    except Exception:
        pass
    data_dir = str(data_dir_path)
    plaid_env_raw = str((meta.get("plaid_env") or os.environ.get("PLAID_ENV") or "production")).strip().lower() or "production"
    # Plaid no longer reliably resolves a dedicated "development" hostname; normalize to sandbox.
    plaid_env = "sandbox" if plaid_env_raw in {"dev", "development"} else plaid_env_raw
    plaid_enable_investments = bool(meta.get("plaid_enable_investments") is True)
    plaid_initial_backfill_done = bool(meta.get("plaid_initial_backfill_done") is True)
    plaid_investments_backfill_done = bool(meta.get("plaid_investments_backfill_done") is True)
    plaid_transactions_update_status = str(meta.get("plaid_transactions_update_status") or "").strip()
    files_on_disk = []
    if is_offline_files or is_ib_flex_web or is_chase_plaid:
        p = Path(data_dir)
        supported_exts = {".csv", ".tsv", ".txt", ".xml"}
        if is_rj_offline:
            supported_exts.update({".qfx", ".ofx"})
        if is_rj_offline or is_chase_offline or is_chase_plaid:
            supported_exts.add(".pdf")
        if p.exists() and p.is_dir():
            for f in sorted(p.glob("**/*"))[:200]:
                if f.is_file():
                    st = f.stat()
                    ext = f.suffix.lower()
                    supported = ext in supported_exts
                    supported_label = "Yes" if supported else "No"
                    if (is_rj_offline or is_chase_offline or is_chase_plaid) and ext == ".pdf":
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
    supplemental_ingests = (
        session.query(ExternalFileIngest)
        .filter(
            ExternalFileIngest.connection_id == conn.id,
            ExternalFileIngest.kind == "SUPPLEMENTAL_CASHFLOWS",
        )
        .order_by(ExternalFileIngest.imported_at.desc(), ExternalFileIngest.id.desc())
        .all()
    )
    supplemental_summary = {
        "files": len(supplemental_ingests),
        "inserted": 0,
        "ignored": 0,
        "unmatched": 0,
    }
    for ing in supplemental_ingests:
        meta_i = ing.metadata_json or {}
        supplemental_summary["inserted"] += int(meta_i.get("inserted", 0) or 0)
        supplemental_summary["ignored"] += int(meta_i.get("ignored", 0) or 0)
        supplemental_summary["unmatched"] += int(meta_i.get("unmatched", 0) or 0)

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
            elif connector_u in {"CHASE_PLAID", "AMEX_PLAID"}:
                missing_creds = token_masked == "—"
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
    is_web = connector_u in {"IB_FLEX_WEB", "CHASE_YODLEE", "CHASE_PLAID", "AMEX_PLAID"}
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
            "is_chase_plaid": is_chase_plaid,
            "is_amex_plaid": is_amex_plaid,
            "is_plaid": is_plaid,
            "is_rj_offline": is_rj_offline,
            "data_dir": data_dir,
            "plaid_env": plaid_env,
            "plaid_enable_investments": plaid_enable_investments,
            "plaid_initial_backfill_done": plaid_initial_backfill_done,
            "plaid_investments_backfill_done": plaid_investments_backfill_done,
            "plaid_transactions_update_status": plaid_transactions_update_status,
            "files_on_disk": files_on_disk,
            "ingested_files": ingested_files,
            "supplemental_ingests": supplemental_ingests[:5],
            "supplemental_summary": supplemental_summary,
            "amex_23m_start": amex_23m_start,
            "latest_holdings": holdings_row,
            "holdings_items": holdings_items,
            "recent_txns": recent_txns,
            "tx_page": tx_page,
            "tx_page_size": tx_page_size,
            "tx_pages": tx_pages,
            "tx_total": int(total_txns),
            "tx_stats": tx_stats,
            "plaid_24m_start": plaid_24m_start,
            "plaid_24m_warn_start": plaid_24m_warn_start,
            "today": dt.date.today().isoformat(),
            "ten_years_ago": (dt.date.today() - dt.timedelta(days=365 * 10)).isoformat(),
            "test_ok": test_ok,
            "test_msg": test_msg,
            "ok_msg": ok_msg,
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
    plaid_env: str = Form(default=""),
    plaid_enable_investments: str = Form(default=""),
    note: str = Form(default=""),
):
    conn = session.query(ExternalConnection).filter(ExternalConnection.id == connection_id).one()
    meta = dict(conn.metadata_json or {})
    old = {
        "data_dir": meta.get("data_dir"),
        "extra_query_ids": meta.get("extra_query_ids"),
        "plaid_env": meta.get("plaid_env"),
        "plaid_enable_investments": meta.get("plaid_enable_investments"),
    }
    connector_u = (conn.connector or "").upper()

    # Only update data_dir for connectors that use it.
    if connector_u in {"IB_FLEX_OFFLINE", "CHASE_OFFLINE", "RJ_OFFLINE", "IB_FLEX_WEB"}:
        dd = data_dir.strip()
        if dd:
            dd_path = Path(os.path.expanduser(dd))
            try:
                dd_path = dd_path.resolve()
            except Exception:
                pass
            meta["data_dir"] = str(dd_path)
            # Best-effort: create dir so users can immediately drop files in.
            try:
                dd_path.mkdir(parents=True, exist_ok=True)
            except Exception as e:
                msg = urllib.parse.quote(f"Failed to create data directory: {type(e).__name__}: {e}")
                return RedirectResponse(url=f"/sync/connections/{connection_id}?error={msg}", status_code=303)
        else:
            meta["data_dir"] = None

    # Only update extra query IDs for IB Flex Web.
    if connector_u == "IB_FLEX_WEB":
        eq = (extra_query_ids or "").strip()
        if eq:
            meta["extra_query_ids"] = _split_query_tokens(eq)
        else:
            meta["extra_query_ids"] = []

    # Only update Plaid env for Plaid connectors.
    if connector_u in {"CHASE_PLAID", "AMEX_PLAID"}:
        pe = (plaid_env or "").strip().lower()
        if pe in {"sandbox", "production"}:
            meta["plaid_env"] = pe
        elif pe in {"dev", "development"}:
            meta["plaid_env"] = "sandbox"
        elif pe:
            msg = urllib.parse.quote("Invalid Plaid env. Use sandbox or production.")
            return RedirectResponse(url=f"/sync/connections/{connection_id}?error={msg}", status_code=303)
        # Investments toggle: stored as boolean in metadata_json (Chase only).
        if connector_u == "CHASE_PLAID":
            inv = (plaid_enable_investments or "").strip().lower()
            if inv:
                meta["plaid_enable_investments"] = inv in {"1", "true", "yes", "y", "on"}
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
        new={
            "data_dir": meta.get("data_dir"),
            "extra_query_ids": meta.get("extra_query_ids"),
            "plaid_env": meta.get("plaid_env"),
            "plaid_enable_investments": meta.get("plaid_enable_investments"),
        },
        note=note or "Updated connection settings",
    )
    session.commit()
    return RedirectResponse(url=f"/sync/connections/{connection_id}", status_code=303)


@router.post("/connections/{connection_id}/upload")
def connection_upload_file(
    connection_id: int,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    upload: list[UploadFile] = File(...),
    note: str = Form(default=""),
):
    conn = session.query(ExternalConnection).filter(ExternalConnection.id == connection_id).one()
    connector_u = (conn.connector or "").upper()
    meta = dict(conn.metadata_json or {})
    dd = os.path.expanduser(str(meta.get("data_dir") or ""))
    base_dir = Path(dd) if dd else (Path("data") / "external" / f"conn_{conn.id}")
    base_dir.mkdir(parents=True, exist_ok=True)
    try:
        base_dir = base_dir.resolve()
    except Exception:
        pass

    def _unique_dest(path: Path) -> Path:
        if not path.exists():
            return path
        stem = path.stem or "upload"
        suffix = path.suffix
        for i in range(1, 1000):
            cand = path.with_name(f"{stem}-{i}{suffix}")
            if not cand.exists():
                return cand
        return path.with_name(f"{stem}-{dt.datetime.utcnow().strftime('%Y%m%d%H%M%S')}{suffix}")

    uploaded_files: list[tuple[str, Path, int, str]] = []
    invalid_exts: list[str] = []

    for uf in upload:
        orig = Path(uf.filename or "upload.bin").name
        safe = "".join(ch for ch in orig if ch.isalnum() or ch in {".", "_", "-"}).strip("._")
        if not safe:
            safe = "upload.bin"
        dest = _unique_dest(base_dir / safe)
        with dest.open("wb") as f:
            shutil.copyfileobj(uf.file, f)
        try:
            size = int(dest.stat().st_size)
        except Exception:
            size = 0
        file_hash = _sha256_path(dest)
        uploaded_files.append((safe, dest, size, file_hash))

    if not meta.get("data_dir"):
        meta["data_dir"] = str(base_dir)
    conn.metadata_json = dict(meta)
    flag_modified(conn, "metadata_json")

    for safe, stored, size, _file_hash in uploaded_files:
        log_change(
            session,
            actor=actor,
            action="UPLOAD",
            entity="ExternalConnection",
            entity_id=str(conn.id),
            old=None,
            new={"file": stored.name, "bytes": size, "data_dir": str(base_dir)},
            note=note or "Uploaded offline statement file",
        )
    session.commit()
    supported_exts = {".csv", ".tsv", ".txt", ".xml"}
    if connector_u == "RJ_OFFLINE":
        supported_exts.update({".qfx", ".ofx"})
    if connector_u in {"RJ_OFFLINE", "CHASE_OFFLINE", "CHASE_PLAID"}:
        supported_exts.add(".pdf")
    for safe, stored, _size, _file_hash in uploaded_files:
        ext = stored.suffix.lower()
        if ext and ext not in supported_exts:
            invalid_exts.append(stored.name)
    if invalid_exts:
        hint = ".csv/.tsv/.txt/.xml"
        if connector_u == "RJ_OFFLINE":
            hint = ".qfx/.ofx (preferred), .csv/.tsv/.txt/.xml (legacy), or .pdf statements (holdings totals only)"
        elif connector_u == "CHASE_PLAID":
            hint = ".csv/.tsv/.txt/.xml (or .pdf statements for holdings totals / card int-free balance)"
        elif connector_u == "CHASE_OFFLINE":
            hint = ".csv/.tsv/.txt/.xml (or .pdf statements for holdings totals)"
        msg = urllib.parse.quote(
            f"Uploaded {len(invalid_exts)} unsupported file(s): {', '.join(invalid_exts[:5])}. "
            f"Export as {hint} and re-upload."
        )
        return RedirectResponse(url=f"/sync/connections/{connection_id}?error={msg}", status_code=303)

    parse_ok = 0
    parse_errors: list[str] = []
    if connector_u == "CHASE_PLAID":
        for _safe, stored, _size, file_hash in uploaded_files:
            if stored.suffix.lower() != ".pdf":
                continue
            try:
                existing_stmt = (
                    session.query(ExternalCardStatement)
                    .filter(
                        ExternalCardStatement.connection_id == conn.id,
                        ExternalCardStatement.file_hash == file_hash,
                    )
                    .first()
                )
                parsed = parse_chase_card_statement_pdf(stored)
                last4 = str(parsed.get("last4") or "").strip() or None
                exp_acct_id = None
                if last4:
                    q = session.query(ExpenseAccount).filter(ExpenseAccount.last4_masked == last4)
                    broker = str(conn.broker or "").strip()
                    if broker:
                        q = q.filter(func.upper(ExpenseAccount.institution).like(f"%{broker.upper()}%"))
                    match = q.order_by(ExpenseAccount.id.desc()).first()
                    if match is not None:
                        exp_acct_id = int(match.id)
                if existing_stmt is not None:
                    existing_stmt.expense_account_id = exp_acct_id
                    existing_stmt.last4 = last4
                    existing_stmt.statement_period_start = parsed.get("statement_period_start")
                    existing_stmt.statement_period_end = parsed.get("statement_period_end")
                    existing_stmt.payment_due_date = parsed.get("payment_due_date")
                    existing_stmt.statement_balance = (
                        float(parsed["statement_balance"]) if parsed.get("statement_balance") is not None else None
                    )
                    existing_stmt.interest_saving_balance = (
                        float(parsed["interest_saving_balance"]) if parsed.get("interest_saving_balance") is not None else None
                    )
                    existing_stmt.minimum_payment_due = (
                        float(parsed["minimum_payment_due"]) if parsed.get("minimum_payment_due") is not None else None
                    )
                    existing_stmt.pay_over_time_json = parsed.get("pay_over_time")
                    existing_stmt.source_file = str(parsed.get("source_file") or stored.name)
                else:
                    session.add(
                        ExternalCardStatement(
                            connection_id=conn.id,
                            expense_account_id=exp_acct_id,
                            last4=last4,
                            statement_period_start=parsed.get("statement_period_start"),
                            statement_period_end=parsed.get("statement_period_end"),
                            payment_due_date=parsed.get("payment_due_date"),
                            statement_balance=float(parsed["statement_balance"])
                            if parsed.get("statement_balance") is not None
                            else None,
                            interest_saving_balance=float(parsed["interest_saving_balance"])
                            if parsed.get("interest_saving_balance") is not None
                            else None,
                            minimum_payment_due=float(parsed["minimum_payment_due"])
                            if parsed.get("minimum_payment_due") is not None
                            else None,
                            pay_over_time_json=parsed.get("pay_over_time"),
                            source_file=str(parsed.get("source_file") or stored.name),
                            file_hash=file_hash,
                        )
                    )
                parse_ok += 1
            except ProviderError as e:
                parse_errors.append(f"{stored.name}: {e}")
            except Exception as e:
                parse_errors.append(f"{stored.name}: {type(e).__name__}: {e}")
        if parse_ok or parse_errors:
            session.commit()

    ok_msg = ""
    error_msg = ""
    if parse_ok:
        ok_msg = f"Parsed {parse_ok} Chase card statement PDF(s)."
    if parse_errors:
        suffix = "; ".join(parse_errors[:2])
        extra = "" if len(parse_errors) <= 2 else f" (+{len(parse_errors) - 2} more)"
        error_msg = f"Statement PDF parse issue: {suffix}{extra}"
    if ok_msg or error_msg:
        qs = []
        if ok_msg:
            qs.append(f"ok={urllib.parse.quote(ok_msg)}")
        if error_msg:
            qs.append(f"error={urllib.parse.quote(error_msg)}")
        return RedirectResponse(url=f"/sync/connections/{connection_id}?{'&'.join(qs)}", status_code=303)
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
        # SQLite can briefly throw "database is locked" when another write is committing.
        # Since sync runs can be long, use a short bounded retry so users don't have to resubmit.
        attempt = 0
        last_oe: OperationalError | None = None
        while attempt < 3:
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
                break
            except OperationalError as e:
                last_oe = e
                if "database is locked" not in str(e).lower():
                    raise
                time.sleep(0.4 * (2**attempt))
                attempt += 1
                continue
        else:
            raise last_oe if last_oe is not None else OperationalError("database is locked", None, None)  # type: ignore[misc]
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


@router.post("/connections/{connection_id}/delete")
def connection_delete(
    connection_id: int,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    note: str = Form(default=""),
):
    conn = session.query(ExternalConnection).filter(ExternalConnection.id == connection_id).one()
    status_u = (conn.status or "").upper()
    if status_u != "DISABLED":
        raise HTTPException(status_code=400, detail="Only DISABLED connections can be deleted.")

    # Delete dependent rows first (SQLite has FK constraints and we avoid relying on cascades).
    run_ids = [r for (r,) in session.query(SyncRun.id).filter(SyncRun.connection_id == connection_id).all()]
    if run_ids:
        session.query(ExternalPayloadSnapshot).filter(ExternalPayloadSnapshot.sync_run_id.in_(run_ids)).delete(
            synchronize_session=False
        )
        session.query(SyncRun).filter(SyncRun.id.in_(run_ids)).delete(synchronize_session=False)

    session.query(ExternalTransactionMap).filter(ExternalTransactionMap.connection_id == connection_id).delete(
        synchronize_session=False
    )
    session.query(ExternalCredential).filter(ExternalCredential.connection_id == connection_id).delete(
        synchronize_session=False
    )
    session.query(ExternalAccountMap).filter(ExternalAccountMap.connection_id == connection_id).delete(
        synchronize_session=False
    )
    session.query(ExternalHoldingSnapshot).filter(ExternalHoldingSnapshot.connection_id == connection_id).delete(
        synchronize_session=False
    )
    session.query(ExternalFileIngest).filter(ExternalFileIngest.connection_id == connection_id).delete(
        synchronize_session=False
    )
    session.query(BrokerLotClosure).filter(BrokerLotClosure.connection_id == connection_id).delete(
        synchronize_session=False
    )
    session.query(BrokerWashSaleEvent).filter(BrokerWashSaleEvent.connection_id == connection_id).delete(
        synchronize_session=False
    )

    old = {
        "name": conn.name,
        "provider": conn.provider,
        "broker": conn.broker,
        "connector": conn.connector,
        "status": conn.status,
    }
    log_change(
        session,
        actor=actor,
        action="DELETE",
        entity="ExternalConnection",
        entity_id=str(conn.id),
        old=old,
        new=None,
        note=note.strip() or "Deleted connection from Connections list",
    )
    session.delete(conn)
    session.commit()
    msg = urllib.parse.quote(f'Deleted connection "{old["name"]}".')
    return RedirectResponse(url=f"/sync/connections?ok={msg}", status_code=303)


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
    elif connector in {"CHASE_PLAID", "AMEX_PLAID"}:
        cred_token_key = "PLAID_ACCESS_TOKEN"
        cred_qid_key = "PLAID_ITEM_ID"
    token_masked = get_credential_masked(session, connection_id=conn.id, key=cred_token_key)
    qid_masked = get_credential_masked(session, connection_id=conn.id, key=cred_qid_key)
    query_id_display = qid_masked
    if cred_qid_key in {"IB_FLEX_QUERY_ID", "PLAID_ITEM_ID"}:
        qid_plain = get_credential(session, connection_id=conn.id, key=cred_qid_key)
        if qid_plain:
            query_id_display = qid_plain
    from src.app.main import templates

    plaid_env_raw = (conn.metadata_json or {}).get("plaid_env") or (os.environ.get("PLAID_ENV") or "production")
    plaid_env_norm = str(plaid_env_raw or "production").strip().lower() or "production"
    if plaid_env_norm in {"dev", "development"}:
        plaid_env_norm = "sandbox"
    plaid_enable_investments = bool((conn.metadata_json or {}).get("plaid_enable_investments") is True)

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
            "plaid_env": plaid_env_norm,
            "plaid_enable_investments": plaid_enable_investments,
            "plaid_insecure_skip_verify": (os.environ.get("PLAID_INSECURE_SKIP_VERIFY") or "").strip().lower()
            in {"1", "true", "yes", "y", "on"},
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
    if connector in {"CHASE_PLAID", "AMEX_PLAID"}:
        raise HTTPException(status_code=400, detail="This connector uses Plaid Link. Use the Connect/Re-link button on this page.")
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


@router.post("/connections/{connection_id}/plaid/link_token")
def plaid_link_token(
    connection_id: int,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    conn = session.query(ExternalConnection).filter(ExternalConnection.id == connection_id).one()
    if (conn.connector or "").upper() not in {"CHASE_PLAID", "AMEX_PLAID"}:
        raise HTTPException(status_code=400, detail="Not a Plaid connector.")
    if not secret_key_available():
        raise HTTPException(status_code=400, detail="APP_SECRET_KEY is required to save credentials.")
    try:
        redirect_uri = (os.environ.get("PLAID_REDIRECT_URI") or "").strip() or None
        env = (conn.metadata_json or {}).get("plaid_env") or (os.environ.get("PLAID_ENV") or "production")
        env_s = str(env).strip().lower() if env is not None else "production"
        if env_s in {"dev", "development"}:
            env_s = "sandbox"
        if env_s == "production" and not redirect_uri:
            return JSONResponse(
                {
                    "ok": False,
                    "error": "PLAID_REDIRECT_URI is required for Plaid OAuth in production. "
                    "Set it to an HTTPS URL registered in your Plaid Dashboard (OAuth redirect URIs). "
                    "Tip: use an HTTPS tunnel (ngrok/cloudflared) pointing at this app and set "
                    "PLAID_REDIRECT_URI to https://<your-tunnel>/sync/plaid/oauth-return.",
                },
                status_code=400,
            )
        client = PlaidClient(env=(conn.metadata_json or {}).get("plaid_env") or None)
        # Request liabilities so we can show card due dates and statement balances in Cash & Bills.
        # If the user enabled investments, include it here (requires re-linking to take effect).
        products = ["transactions", "liabilities"]
        try:
            if bool((conn.metadata_json or {}).get("plaid_enable_investments") is True):
                products.append("investments")
        except Exception:
            pass
        link_token = client.create_link_token(client_user_id=str(conn.id), redirect_uri=redirect_uri, products=products)
        # Persist for OAuth return routing (link_token is short-lived; safe to store briefly).
        meta = conn.metadata_json or {}
        meta["plaid_pending_link_token"] = link_token
        conn.metadata_json = meta
        flag_modified(conn, "metadata_json")
        session.commit()
        # Never log/return secrets beyond the link token.
        return JSONResponse({"ok": True, "link_token": link_token})
    except PlaidApiError as e:
        # Surface Plaid error codes without secrets.
        msg = f"{e.info.error_code}: {e.info.error_message}".strip(": ")
        if (e.info.error_code or "").upper() == "INVALID_FIELD" and "redirect" in (e.info.error_message or "").lower():
            msg += " (Tip: Plaid OAuth redirect URIs are configured in the Plaid Dashboard and often must be HTTPS; use an HTTPS tunnel URL for local dev.)"
        return JSONResponse({"ok": False, "error": msg}, status_code=400)
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"{type(e).__name__}: {e}"}, status_code=400)


@router.get("/plaid/oauth-return")
def plaid_oauth_return(
    request: Request,
    session: Session = Depends(db_session),
):
    """
    OAuth return landing page for Plaid Link.

    Plaid typically redirects here with query params including:
      - oauth_state_id
      - link_token

    We map `link_token` back to a pending Plaid connection and then
    redirect to that connection's Credentials page with the original query string,
    so the page JS can resume the Link flow.
    """
    qp = dict(request.query_params)
    link_token = str(qp.get("link_token") or "").strip()
    if not link_token:
        msg = urllib.parse.quote("Plaid OAuth return missing link_token. Try connecting again.")
        return RedirectResponse(url=f"/sync/connections?error={msg}", status_code=303)

    # Small-scale lookup; keep SQLite-friendly (no JSON query requirement).
    conn_id: int | None = None
    for c in (
        session.query(ExternalConnection)
        .filter(ExternalConnection.connector.in_(["CHASE_PLAID", "AMEX_PLAID"]))
        .all()
    ):
        meta = c.metadata_json or {}
        if str(meta.get("plaid_pending_link_token") or "").strip() == link_token:
            conn_id = int(c.id)
            break
    if conn_id is None:
        msg = urllib.parse.quote("No matching pending Plaid connection found for this OAuth return. Try connecting again.")
        return RedirectResponse(url=f"/sync/connections?error={msg}", status_code=303)

    qs = str(request.url.query or "")
    suffix = f"?{qs}" if qs else ""
    return RedirectResponse(url=f"/sync/connections/{conn_id}/auth{suffix}", status_code=303)


@router.post("/connections/{connection_id}/plaid/exchange_public_token")
async def plaid_exchange_public_token(
    connection_id: int,
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    conn = session.query(ExternalConnection).filter(ExternalConnection.id == connection_id).one()
    if (conn.connector or "").upper() not in {"CHASE_PLAID", "AMEX_PLAID"}:
        raise HTTPException(status_code=400, detail="Not a Plaid connector.")
    if not secret_key_available():
        raise HTTPException(status_code=400, detail="APP_SECRET_KEY is required to save credentials.")
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    public_token = str((payload or {}).get("public_token") or "").strip()
    if not public_token:
        raise HTTPException(status_code=400, detail="public_token is required.")
    try:
        client = PlaidClient(env=(conn.metadata_json or {}).get("plaid_env") or None)
        access_token, item_id = client.exchange_public_token(public_token=public_token)
        upsert_credential(session, connection_id=conn.id, key="PLAID_ACCESS_TOKEN", plaintext=access_token)
        upsert_credential(session, connection_id=conn.id, key="PLAID_ITEM_ID", plaintext=item_id)
        meta = conn.metadata_json or {}
        meta["plaid_item_id"] = item_id
        # Reset cursor after re-link so the next sync rehydrates deltas from scratch.
        meta.pop("plaid_transactions_cursor", None)
        # Reset one-time backfill flags so the next sync can rehydrate the full available history.
        meta.pop("plaid_initial_backfill_done", None)
        meta.pop("plaid_investments_backfill_done", None)
        meta.pop("plaid_pending_link_token", None)
        conn.metadata_json = meta
        flag_modified(conn, "metadata_json")
        log_change(
            session,
            actor=actor,
            action="UPDATE",
            entity="ExternalCredential",
            entity_id=str(conn.id),
            old=None,
            new={"connection_id": conn.id, "keys": ["PLAID_ACCESS_TOKEN", "PLAID_ITEM_ID"]},
            note="Connected via Plaid Link",
        )
        session.commit()
        return JSONResponse({"ok": True})
    except PlaidApiError as e:
        session.rollback()
        return JSONResponse({"ok": False, "error": f"{e.info.error_code}: {e.info.error_message}"}, status_code=400)
    except Exception as e:
        session.rollback()
        return JSONResponse({"ok": False, "error": f"{type(e).__name__}: {e}"}, status_code=400)


@router.post("/connections/{connection_id}/plaid/backfill_transactions")
def plaid_backfill_transactions(
    connection_id: int,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    note: str = Form(default=""),
):
    conn = session.query(ExternalConnection).filter(ExternalConnection.id == connection_id).one()
    if (conn.connector or "").upper() != "AMEX_PLAID":
        raise HTTPException(status_code=400, detail="Backfill is only available for AMEX Plaid connections.")
    if (conn.status or "").upper() != "ACTIVE":
        msg = urllib.parse.quote("Connection is disabled. Re-enable it before syncing.")
        return RedirectResponse(url=f"/sync/connections/{connection_id}?error={msg}", status_code=303)

    meta_before = conn.metadata_json or {}
    meta = dict(meta_before)
    meta["plaid_force_transactions_get"] = True
    conn.metadata_json = meta
    flag_modified(conn, "metadata_json")
    session.flush()

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

    try:
        attempt = 0
        last_oe: OperationalError | None = None
        while attempt < 3:
            try:
                run = run_sync(
                    session,
                    connection_id=connection_id,
                    mode="INCREMENTAL",
                    start_date=dt.date.today() - dt.timedelta(days=700),
                    end_date=None,
                    overlap_days=0,
                    store_payloads=False,
                    actor=actor,
                    reprocess_files=False,
                )
                break
            except OperationalError as e:
                last_oe = e
                if "database is locked" not in str(e).lower():
                    raise
                time.sleep(0.4 * (2**attempt))
                attempt += 1
                continue
        else:
            raise last_oe if last_oe is not None else OperationalError("database is locked", None, None)  # type: ignore[misc]
    except OperationalError as e:
        if "database is locked" in str(e).lower():
            msg = urllib.parse.quote("Sync failed: database is locked. Wait for the current sync to finish and try again.")
        else:
            msg = urllib.parse.quote(f"Sync failed: OperationalError: {e}")
        return RedirectResponse(url=f"/sync/connections/{connection_id}?error={msg}", status_code=303)
    except Exception as e:
        msg = urllib.parse.quote(f"Sync failed: {type(e).__name__}: {e}")
        return RedirectResponse(url=f"/sync/connections/{connection_id}?error={msg}", status_code=303)

    log_change(
        session,
        actor=actor,
        action="NOTE",
        entity="SyncRun",
        entity_id=str(run.id),
        old=None,
        new=None,
        note=note or "Plaid AMEX backfill (23 months)",
    )
    session.commit()
    return RedirectResponse(url=f"/sync/connections/{connection_id}?ok=Plaid AMEX backfill started", status_code=303)


@router.post("/connections/{connection_id}/plaid/investments_audit")
def plaid_investments_audit(
    connection_id: int,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    """
    Best-effort verification that the DB has all *eligible* Plaid investment transactions for a 24-month window.

    This does not change any imported data; it only compares:
      - Plaid eligible investment transactions (USD, has date/account/id) for [today-730d, today]
      - DB imported investment transactions for the same connection and date range
    """
    conn = session.query(ExternalConnection).filter(ExternalConnection.id == connection_id).one()
    if (conn.connector or "").upper() != "CHASE_PLAID":
        raise HTTPException(status_code=400, detail="Not a Plaid connector.")
    meta = conn.metadata_json or {}
    if not bool(meta.get("plaid_enable_investments") is True):
        msg = urllib.parse.quote("Investments are not enabled for this Plaid connection. Enable Investments and re-link first.")
        return RedirectResponse(url=f"/sync/connections/{connection_id}?error={msg}", status_code=303)

    access_token = get_credential(session, connection_id=conn.id, key="PLAID_ACCESS_TOKEN") or ""
    if not access_token:
        msg = urllib.parse.quote("Missing PLAID_ACCESS_TOKEN. Connect via Credentials first.")
        return RedirectResponse(url=f"/sync/connections/{connection_id}?error={msg}", status_code=303)

    end = dt.date.today()
    start = end - dt.timedelta(days=730)
    start_s = start.isoformat()
    end_s = end.isoformat()

    # Count eligible txns via Plaid (paged); eligibility mirrors our ingestion filters.
    eligible = 0
    reported_total = None
    skipped_non_usd = 0
    plaid_ids: set[str] = set()
    offset = 0
    page_size = 500
    try:
        client = PlaidClient(env=(meta.get("plaid_env") or None))
        while True:
            data = client.investments_transactions_get(
                access_token=str(access_token),
                start_date=start,
                end_date=end,
                offset=offset,
                count=page_size,
            )
            try:
                if reported_total is None and data.get("total_investment_transactions") is not None:
                    reported_total = int(data.get("total_investment_transactions"))
            except Exception:
                reported_total = reported_total
            rows = data.get("investment_transactions")
            if not isinstance(rows, list) or not rows:
                break
            for r in rows:
                if not isinstance(r, dict):
                    continue
                inv_id = str(r.get("investment_transaction_id") or "").strip()
                acct_id = str(r.get("account_id") or "").strip()
                d = str(r.get("date") or "").strip()
                if not inv_id or not acct_id or not d:
                    continue
                ccy = str(r.get("iso_currency_code") or "USD").strip().upper() or "USD"
                if ccy != "USD":
                    skipped_non_usd += 1
                    continue
                eligible += 1
                plaid_ids.add(inv_id)
            offset += len(rows)
            if reported_total is not None and offset >= int(reported_total):
                break
            if len(rows) < page_size:
                break
    except PlaidApiError as e:
        msg = urllib.parse.quote(f"Plaid audit failed: {e.info.error_code}: {e.info.error_message}")
        return RedirectResponse(url=f"/sync/connections/{connection_id}?error={msg}", status_code=303)
    except Exception as e:
        msg = urllib.parse.quote(f"Plaid audit failed: {type(e).__name__}: {e}")
        return RedirectResponse(url=f"/sync/connections/{connection_id}?error={msg}", status_code=303)

    # Count imported investment txns in DB for same range.
    db_rows = (
        session.query(ExternalTransactionMap.provider_txn_id, Transaction.date, Transaction.amount)
        .join(Transaction, Transaction.id == ExternalTransactionMap.transaction_id)
        .filter(
            ExternalTransactionMap.connection_id == conn.id,
            ExternalTransactionMap.provider_txn_id.like("PLAID_INV:%"),
            Transaction.date >= start,
            Transaction.date <= end,
        )
        .all()
    )
    db_count = int(len(db_rows))
    missing = max(0, int(eligible) - int(db_count))
    extra = max(0, int(db_count) - int(eligible))

    note_parts = []
    if reported_total is not None:
        note_parts.append(f"Plaid reported total={reported_total}")
    if skipped_non_usd:
        note_parts.append(f"skipped non-USD={skipped_non_usd}")
    note = (" (" + ", ".join(note_parts) + ")") if note_parts else ""

    # Identify DB rows that Plaid no longer reports for this window (append-only semantics).
    extra_examples: list[str] = []
    if plaid_ids and db_rows:
        for pid, d, amt in db_rows:
            inv_id = str(pid or "").split("PLAID_INV:", 1)[-1]
            if inv_id and inv_id not in plaid_ids:
                # Keep examples compact: date + amount + id suffix.
                try:
                    extra_examples.append(f"{d} {format_usd(float(amt or 0.0))} {pid}")
                except Exception:
                    extra_examples.append(f"{d} {pid}")
            if len(extra_examples) >= 6:
                break

    if missing == 0 and extra == 0:
        msg = urllib.parse.quote(
            f"Plaid investments audit OK: eligible={eligible} matches DB={db_count} for {start_s}→{end_s}{note}."
        )
        return RedirectResponse(url=f"/sync/connections/{connection_id}?ok={msg}", status_code=303)

    if missing == 0 and extra > 0:
        # DB is a superset of what's currently available from Plaid for this window. This can happen when
        # Plaid corrects/removes transactions over time; Investor ingestion is append-only for auditability.
        ex = (" Examples: " + "; ".join(extra_examples)) if extra_examples else ""
        msg = urllib.parse.quote(
            f"Plaid investments audit OK (superset): eligible={eligible}, DB={db_count}, extra={extra} for {start_s}→{end_s}{note}. "
            f"DB contains all Plaid-available txns, plus {extra} txns Plaid no longer reports.{ex}"
        )
        return RedirectResponse(url=f"/sync/connections/{connection_id}?ok={msg}", status_code=303)

    warn = urllib.parse.quote(
        f"Plaid investments audit mismatch: eligible={eligible}, DB={db_count}, missing={missing}, extra={extra} for {start_s}→{end_s}{note}. "
        f"If missing>0, run Backfill investments (24 months) and retry."
    )
    return RedirectResponse(url=f"/sync/connections/{connection_id}?error={warn}", status_code=303)


@router.post("/connections/{connection_id}/plaid/investments_reconcile")
def plaid_investments_reconcile(
    connection_id: int,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    """
    Reconcile Plaid investment transaction duplicates caused by Plaid ID churn (pending/posted corrections).

    Strategy:
      - Fetch current Plaid investment_transaction_id set for [today-730d, today]
      - Find DB groups with identical signature (account_id, date, type, ticker, qty, amount) having >1 row
      - If exactly one row in the group is currently reported by Plaid (by ID), keep it and delete the others

    This keeps DB aligned with Plaid's current view while still being deterministic and safe.
    """
    conn = session.query(ExternalConnection).filter(ExternalConnection.id == connection_id).one()
    if (conn.connector or "").upper() != "CHASE_PLAID":
        raise HTTPException(status_code=400, detail="Not a Plaid connector.")
    meta = conn.metadata_json or {}
    if not bool(meta.get("plaid_enable_investments") is True):
        msg = urllib.parse.quote("Investments are not enabled for this Plaid connection.")
        return RedirectResponse(url=f"/sync/connections/{connection_id}?error={msg}", status_code=303)
    access_token = get_credential(session, connection_id=conn.id, key="PLAID_ACCESS_TOKEN") or ""
    if not access_token:
        msg = urllib.parse.quote("Missing PLAID_ACCESS_TOKEN. Connect via Credentials first.")
        return RedirectResponse(url=f"/sync/connections/{connection_id}?error={msg}", status_code=303)

    end = dt.date.today()
    start = end - dt.timedelta(days=730)

    plaid_ids: set[str] = set()
    offset = 0
    page_size = 500
    try:
        client = PlaidClient(env=(meta.get("plaid_env") or None))
        while True:
            data = client.investments_transactions_get(
                access_token=str(access_token),
                start_date=start,
                end_date=end,
                offset=offset,
                count=page_size,
            )
            rows = data.get("investment_transactions")
            if not isinstance(rows, list) or not rows:
                break
            for r in rows:
                if not isinstance(r, dict):
                    continue
                inv_id = str(r.get("investment_transaction_id") or "").strip()
                acct_id = str(r.get("account_id") or "").strip()
                d = str(r.get("date") or "").strip()
                if not inv_id or not acct_id or not d:
                    continue
                ccy = str(r.get("iso_currency_code") or "USD").strip().upper() or "USD"
                if ccy != "USD":
                    continue
                plaid_ids.add(inv_id)
            offset += len(rows)
            total = data.get("total_investment_transactions")
            try:
                if total is not None and int(offset) >= int(total):
                    break
            except Exception:
                pass
            if len(rows) < page_size:
                break
    except PlaidApiError as e:
        msg = urllib.parse.quote(f"Plaid reconcile failed: {e.info.error_code}: {e.info.error_message}")
        return RedirectResponse(url=f"/sync/connections/{connection_id}?error={msg}", status_code=303)
    except Exception as e:
        msg = urllib.parse.quote(f"Plaid reconcile failed: {type(e).__name__}: {e}")
        return RedirectResponse(url=f"/sync/connections/{connection_id}?error={msg}", status_code=303)

    # Pull candidate DB rows in-range.
    db_rows = (
        session.query(
            Transaction.id,
            Transaction.account_id,
            Transaction.date,
            Transaction.type,
            Transaction.ticker,
            Transaction.qty,
            Transaction.amount,
            ExternalTransactionMap.provider_txn_id,
        )
        .join(ExternalTransactionMap, ExternalTransactionMap.transaction_id == Transaction.id)
        .filter(
            ExternalTransactionMap.connection_id == conn.id,
            ExternalTransactionMap.provider_txn_id.like("PLAID_INV:%"),
            Transaction.date >= start,
            Transaction.date <= end,
        )
        .all()
    )

    def _sig(r) -> tuple:
        qty = r.qty
        try:
            qty = float(qty) if qty is not None else None
        except Exception:
            qty = None
        amt = r.amount
        try:
            amt = float(amt)
        except Exception:
            amt = 0.0
        return (
            int(r.account_id),
            str(r.date),
            str(r.type or ""),
            str(r.ticker or ""),
            (None if qty is None else round(float(qty), 6)),
            round(float(amt), 2),
        )

    by_sig: dict[tuple, list[Any]] = {}
    for r in db_rows:
        by_sig.setdefault(_sig(r), []).append(r)

    deleted_txn = 0
    deleted_maps = 0
    skipped_ambiguous = 0
    for sig, rows in by_sig.items():
        if len(rows) <= 1:
            continue
        keep: list[Any] = []
        drop: list[Any] = []
        for r in rows:
            pid = str(r.provider_txn_id or "")
            inv_id = pid.split("PLAID_INV:", 1)[-1]
            if inv_id and inv_id in plaid_ids:
                keep.append(r)
            else:
                drop.append(r)
        if len(keep) != 1:
            skipped_ambiguous += 1
            continue
        # Delete the dropped rows (mapping + transaction) only if the transaction isn't referenced by other maps.
        for r in drop:
            # Delete map row
            deleted_maps += int(
                session.query(ExternalTransactionMap)
                .filter(
                    ExternalTransactionMap.connection_id == conn.id,
                    ExternalTransactionMap.provider_txn_id == r.provider_txn_id,
                    ExternalTransactionMap.transaction_id == r.id,
                )
                .delete(synchronize_session=False)
                or 0
            )
            remaining = (
                session.query(func.count(ExternalTransactionMap.id))
                .filter(ExternalTransactionMap.transaction_id == r.id)
                .scalar()
                or 0
            )
            if int(remaining) == 0:
                deleted_txn += int(
                    session.query(Transaction).filter(Transaction.id == r.id).delete(synchronize_session=False) or 0
                )

    session.commit()
    msg = urllib.parse.quote(
        f"Reconcile complete: removed {deleted_txn} duplicate transaction(s) and {deleted_maps} mapping(s); "
        f"skipped {skipped_ambiguous} ambiguous group(s)."
    )
    return RedirectResponse(url=f"/sync/connections/{connection_id}?ok={msg}", status_code=303)


@router.post("/connections/{connection_id}/plaid/fix_investment_dividends")
def plaid_fix_investment_dividends(
    connection_id: int,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    """
    Local-only maintenance: reclassify Plaid investment cash dividends that were previously imported as FEE.

    This is deterministic (no Plaid API call) and based on the stored description. It exists to repair
    historical data imported before our Plaid mapping was corrected.
    """
    conn = session.query(ExternalConnection).filter(ExternalConnection.id == connection_id).one()
    if (conn.connector or "").upper() != "CHASE_PLAID":
        raise HTTPException(status_code=400, detail="Not a Plaid connector.")
    meta = conn.metadata_json or {}
    if not bool(meta.get("plaid_enable_investments") is True):
        msg = urllib.parse.quote("Investments are not enabled for this Plaid connection.")
        return RedirectResponse(url=f"/sync/connections/{connection_id}?error={msg}", status_code=303)

    desc_u = func.upper(func.coalesce(func.json_extract(Transaction.lot_links_json, "$.description"), ""))
    is_cash_div = or_(func.instr(desc_u, "CASH DIV") > 0, func.instr(desc_u, "DIVIDEND") > 0)

    rows = (
        session.query(Transaction, ExternalTransactionMap)
        .join(ExternalTransactionMap, ExternalTransactionMap.transaction_id == Transaction.id)
        .filter(
            ExternalTransactionMap.connection_id == conn.id,
            ExternalTransactionMap.provider_txn_id.like("PLAID_INV:%"),
            Transaction.type == "FEE",
            is_cash_div,
        )
        .all()
    )

    changed = 0
    for txn, _m in rows:
        try:
            txn.type = "DIV"
            txn.amount = abs(float(txn.amount or 0.0))
            links = txn.lot_links_json or {}
            links["raw_type"] = "DIV"
            links["reclassified_by_maintenance"] = True
            links["reclassified_at"] = utcnow().isoformat()
            txn.lot_links_json = links
            changed += 1
        except Exception:
            continue

    if changed:
        log_change(
            session,
            actor=actor,
            action="UPDATE",
            entity="Transaction",
            entity_id=None,
            old=None,
            new={"connection_id": conn.id, "reclassified_dividends": changed},
            note="Reclassified Plaid investment cash dividends (FEE→DIV)",
        )
    session.commit()
    msg = urllib.parse.quote(f"Reclassified {changed} Plaid investment dividend txn(s) (FEE→DIV).")
    return RedirectResponse(url=f"/sync/connections/{connection_id}?ok={msg}", status_code=303)


@router.post("/connections/{connection_id}/plaid/fix_investment_sweeps")
def plaid_fix_investment_sweeps(
    connection_id: int,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    """
    Local-only maintenance: undo an earlier sweep conversion that incorrectly reclassified cash sweep BUY/SELL entries
    into TRANSFER cashflows.

    Chase IRA often represents internal cash mechanics via a sweep instrument (e.g., QCERQ) with descriptions like:
      - "DEPOSIT SWEEP ... INTRA-DAY WITHDRWAL"
      - "DEPOSIT SWEEP ... INTRA-DAY DEPOSIT"
    These are *not* reliable representations of external distributions/contributions, so we keep them as BUY/SELL lots
    and exclude them from cash-out reporting via the internal-mechanics filter.
    """
    conn = session.query(ExternalConnection).filter(ExternalConnection.id == connection_id).one()
    if (conn.connector or "").upper() != "CHASE_PLAID":
        raise HTTPException(status_code=400, detail="Not a Plaid connector.")
    meta = conn.metadata_json or {}
    if not bool(meta.get("plaid_enable_investments") is True):
        msg = urllib.parse.quote("Investments are not enabled for this Plaid connection.")
        return RedirectResponse(url=f"/sync/connections/{connection_id}?error={msg}", status_code=303)

    desc_u = func.upper(func.coalesce(func.json_extract(Transaction.lot_links_json, "$.description"), ""))
    is_sweep_intraday = and_(func.instr(desc_u, "DEPOSIT SWEEP") > 0, func.instr(desc_u, "INTRA-DAY") > 0)

    rows = (
        session.query(Transaction, ExternalTransactionMap)
        .join(ExternalTransactionMap, ExternalTransactionMap.transaction_id == Transaction.id)
        .filter(
            ExternalTransactionMap.connection_id == conn.id,
            ExternalTransactionMap.provider_txn_id.like("PLAID_INV:%"),
            Transaction.type == "TRANSFER",
            Transaction.ticker == "CASH:USD",
            is_sweep_intraday,
        )
        .all()
    )

    changed = 0
    for txn, _m in rows:
        try:
            d = str((txn.lot_links_json or {}).get("description") or "")
            d_u = d.upper()
            is_withdrawal = ("WITHDRW" in d_u) or ("WITHDRAW" in d_u)
            is_deposit = ("INTRA-DAY" in d_u) and ("DEPOSIT" in d_u) and not is_withdrawal

            amt = float(txn.amount or 0.0)
            # Restore sweep mechanics to BUY/SELL in the sweep instrument (QCERQ).
            if is_withdrawal:
                txn.type = "SELL"
                txn.amount = abs(amt)
            elif is_deposit:
                txn.type = "BUY"
                txn.amount = -abs(amt)
            else:
                # Fallback: infer from sign (TRANSFER outflows are negative).
                txn.type = "SELL" if amt < 0 else "BUY"
                txn.amount = abs(amt) if txn.type == "SELL" else -abs(amt)
            txn.ticker = "QCERQ"
            txn.qty = abs(float(txn.amount or 0.0))

            links = txn.lot_links_json or {}
            links["raw_type"] = txn.type
            links["reclassified_by_maintenance"] = True
            links["reclassified_at"] = utcnow().isoformat()
            txn.lot_links_json = links
            changed += 1
        except Exception:
            continue

    if changed:
        log_change(
            session,
            actor=actor,
            action="UPDATE",
            entity="Transaction",
            entity_id=None,
            old=None,
            new={"connection_id": conn.id, "reclassified_sweeps": changed},
            note="Undo sweep conversion: restored Plaid investment sweep entries (TRANSFER→BUY/SELL)",
        )
    session.commit()
    msg = urllib.parse.quote(f"Restored {changed} Plaid sweep txn(s) (TRANSFER→BUY/SELL).")
    return RedirectResponse(url=f"/sync/connections/{connection_id}?ok={msg}", status_code=303)


@router.post("/connections/{connection_id}/plaid/supplemental_cashflows")
def plaid_supplemental_cashflows(
    connection_id: int,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    upload: list[UploadFile] = File(...),
    return_to: str = Form("/sync/connections"),
):
    conn = session.query(ExternalConnection).filter(ExternalConnection.id == connection_id).one()
    if (conn.connector or "").upper() != "CHASE_PLAID":
        raise HTTPException(status_code=400, detail="Not a Plaid connector.")
    meta = conn.metadata_json or {}
    if not bool(meta.get("plaid_enable_investments") is True):
        msg = urllib.parse.quote("Investments are not enabled for this Plaid connection.")
        return RedirectResponse(url=f"{return_to}?error={msg}", status_code=303)

    uploads = upload or []
    if not uploads:
        msg = urllib.parse.quote("No files uploaded.")
        return RedirectResponse(url=f"{return_to}?error={msg}", status_code=303)

    dd = os.path.expanduser(str(meta.get("data_dir") or ""))
    base_dir = Path(dd) if dd else (Path("data") / "external" / f"conn_{conn.id}")
    base_dir = base_dir / "supplemental_cashflows"
    base_dir.mkdir(parents=True, exist_ok=True)
    try:
        base_dir = base_dir.resolve()
    except Exception:
        pass

    if not meta.get("data_dir"):
        meta["data_dir"] = str(base_dir.parent)
        conn.metadata_json = dict(meta)
        flag_modified(conn, "metadata_json")

    totals = {"inserted": 0, "duplicates": 0, "invalid": 0, "unmatched": 0, "ignored": 0, "reprocessed": 0}
    failed_files: list[str] = []
    for f in uploads:
        raw_bytes = f.file.read()
        if not raw_bytes:
            failed_files.append(f.filename or "upload")
            continue
        orig = Path(f.filename or "supplemental.csv").name
        safe = "".join(ch for ch in orig if ch.isalnum() or ch in {".", "_", "-"}).strip("._")
        if not safe:
            safe = "supplemental.csv"
        ts = dt.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        dest = base_dir / f"{ts}_{safe}"
        dest.write_bytes(raw_bytes)
        try:
            stats = import_supplemental_cashflows(
                session,
                connection=conn,
                file_name=dest.name,
                file_bytes=raw_bytes,
                stored_path=str(dest),
                actor=actor,
                purge_manual_overrides=True,
            )
        except Exception as exc:
            failed_files.append(f"{safe} ({exc})")
            continue
        if stats.get("already_imported"):
            totals["reprocessed"] += 1
        totals["inserted"] += int(stats.get("inserted", 0))
        totals["duplicates"] += int(stats.get("duplicates", 0))
        totals["invalid"] += int(stats.get("invalid", 0))
        totals["unmatched"] += int(stats.get("unmatched", 0))
        totals["ignored"] += int(stats.get("ignored", 0))

    if failed_files:
        msg = urllib.parse.quote(f"Supplemental CSV import failed for: {', '.join(failed_files)}")
        return RedirectResponse(url=f"{return_to}?error={msg}", status_code=303)

    msg = urllib.parse.quote(
        "Supplemental cashflows: "
        f"inserted {totals['inserted']}, "
        f"duplicates {totals['duplicates']}, "
        f"invalid {totals['invalid']}, "
        f"unmatched {totals['unmatched']}, "
        f"ignored {totals.get('ignored', 0)}, "
        f"reprocessed files {totals.get('reprocessed', 0)}."
    )
    return RedirectResponse(url=f"{return_to}?ok={msg}", status_code=303)


@router.post("/connections/{connection_id}/plaid/supplemental_cashflows/reprocess")
def plaid_supplemental_cashflows_reprocess(
    connection_id: int,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    return_to: str = Form("/sync/connections"),
):
    conn = session.query(ExternalConnection).filter(ExternalConnection.id == connection_id).one()
    if (conn.connector or "").upper() != "CHASE_PLAID":
        raise HTTPException(status_code=400, detail="Not a Plaid connector.")
    meta = conn.metadata_json or {}
    if not bool(meta.get("plaid_enable_investments") is True):
        msg = urllib.parse.quote("Investments are not enabled for this Plaid connection.")
        return RedirectResponse(url=f"{return_to}?error={msg}", status_code=303)

    ingests = (
        session.query(ExternalFileIngest)
        .filter(
            ExternalFileIngest.connection_id == conn.id,
            ExternalFileIngest.kind == "SUPPLEMENTAL_CASHFLOWS",
        )
        .order_by(ExternalFileIngest.imported_at.desc())
        .all()
    )
    if not ingests:
        msg = urllib.parse.quote("No supplemental files available to reprocess.")
        return RedirectResponse(url=f"{return_to}?error={msg}", status_code=303)

    totals = {"inserted": 0, "duplicates": 0, "invalid": 0, "unmatched": 0, "ignored": 0, "reprocessed": 0}
    failed_files: list[str] = []
    for ingest in ingests:
        path = ingest.stored_path or ""
        if not path or not Path(path).exists():
            failed_files.append(f"{ingest.file_name} (missing file)")
            continue
        raw_bytes = Path(path).read_bytes()
        try:
            stats = import_supplemental_cashflows(
                session,
                connection=conn,
                file_name=ingest.file_name,
                file_bytes=raw_bytes,
                stored_path=path,
                actor=actor,
                purge_manual_overrides=True,
            )
        except Exception as exc:
            failed_files.append(f"{ingest.file_name} ({exc})")
            continue
        if stats.get("already_imported"):
            totals["reprocessed"] += 1
        totals["inserted"] += int(stats.get("inserted", 0))
        totals["duplicates"] += int(stats.get("duplicates", 0))
        totals["invalid"] += int(stats.get("invalid", 0))
        totals["unmatched"] += int(stats.get("unmatched", 0))
        totals["ignored"] += int(stats.get("ignored", 0))

    if failed_files:
        msg = urllib.parse.quote(f"Supplemental reprocess failed for: {', '.join(failed_files)}")
        return RedirectResponse(url=f"{return_to}?error={msg}", status_code=303)

    msg = urllib.parse.quote(
        "Supplemental cashflows reprocessed: "
        f"files {len(ingests)}, "
        f"inserted {totals['inserted']}, "
        f"duplicates {totals['duplicates']}, "
        f"invalid {totals['invalid']}, "
        f"unmatched {totals['unmatched']}, "
        f"ignored {totals.get('ignored', 0)}."
    )
    return RedirectResponse(url=f"{return_to}?ok={msg}", status_code=303)


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


@router.post("/connections/{connection_id}/purge-chase-legacy")
def purge_legacy_chase_sources(
    connection_id: int,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    confirm: str = Form(default=""),
    purge_expenses: str = Form(default=""),
    delete_legacy_accounts: str = Form(default=""),
    note: str = Form(default=""),
):
    """
    Purge legacy Chase sources (CSV + Yodlee connections) so Plaid is the single source of truth.

    - Deletes imported artifacts for CHASE_OFFLINE + CHASE_YODLEE connections
    - Disables those connections
    - Optionally purges non-Plaid Chase expense imports (CSV batches)

    Plaid connection data is kept.
    """
    conn = session.query(ExternalConnection).filter(ExternalConnection.id == connection_id).one()
    if (conn.connector or "").upper() != "CHASE_PLAID":
        raise HTTPException(status_code=400, detail="This action is only available for Chase (Plaid) connections.")
    if (confirm or "").strip().upper() != "PURGE CHASE":
        raise HTTPException(status_code=400, detail="Type PURGE CHASE to confirm.")

    totals: dict[str, int] = {
        "connections_disabled": 0,
        "transactions_deleted": 0,
        "holdings_snapshots_deleted": 0,
        "payload_snapshots_deleted": 0,
        "sync_runs_deleted": 0,
        "file_ingests_deleted": 0,
        "account_maps_deleted": 0,
        "credentials_deleted": 0,
        "cash_balances_deleted": 0,
        "position_lots_deleted": 0,
        "income_events_deleted": 0,
        "tax_lots_deleted": 0,
        "lot_disposals_deleted": 0,
        "wash_adjustments_deleted": 0,
        "corp_actions_deleted": 0,
        "accounts_deleted": 0,
        "expense_txns_deleted": 0,
        "expense_batches_deleted": 0,
        "expense_accounts_deleted": 0,
    }

    legacy_conns = (
        session.query(ExternalConnection)
        .filter(
            ExternalConnection.id != conn.id,
            func.upper(func.coalesce(ExternalConnection.broker, "")) == "CHASE",
            func.upper(func.coalesce(ExternalConnection.connector, "")).in_(["CHASE_OFFLINE", "CHASE_YODLEE"]),
        )
        .all()
    )

    legacy_account_ids: set[int] = set()

    for lc in legacy_conns:
        # Capture mapped account ids before deleting maps.
        try:
            rows = (
                session.query(ExternalAccountMap.account_id)
                .filter(ExternalAccountMap.connection_id == lc.id)
                .all()
            )
            for (aid,) in rows:
                if aid is not None:
                    legacy_account_ids.add(int(aid))
        except Exception:
            pass

        txn_ids = [
            r[0]
            for r in session.query(ExternalTransactionMap.transaction_id)
            .filter(ExternalTransactionMap.connection_id == lc.id)
            .all()
        ]
        if txn_ids:
            # Dependent planning artifacts referencing txns.
            try:
                session.query(LotDisposal).filter(LotDisposal.sell_txn_id.in_(txn_ids)).delete(synchronize_session=False)
                session.query(WashSaleAdjustment).filter(
                    (WashSaleAdjustment.loss_sale_txn_id.in_(txn_ids))
                    | (WashSaleAdjustment.replacement_buy_txn_id.in_(txn_ids))
                ).delete(synchronize_session=False)
            except Exception:
                pass

            session.query(ExternalTransactionMap).filter(ExternalTransactionMap.connection_id == lc.id).delete(
                synchronize_session=False
            )
            deleted = session.query(Transaction).filter(Transaction.id.in_(txn_ids)).delete(synchronize_session=False)
            totals["transactions_deleted"] += int(deleted or 0)
        else:
            session.query(ExternalTransactionMap).filter(ExternalTransactionMap.connection_id == lc.id).delete(
                synchronize_session=False
            )

        hs_deleted = session.query(ExternalHoldingSnapshot).filter(ExternalHoldingSnapshot.connection_id == lc.id).delete(
            synchronize_session=False
        )
        totals["holdings_snapshots_deleted"] += int(hs_deleted or 0)

        fi_deleted = session.query(ExternalFileIngest).filter(ExternalFileIngest.connection_id == lc.id).delete(
            synchronize_session=False
        )
        totals["file_ingests_deleted"] += int(fi_deleted or 0)

        run_ids = [r[0] for r in session.query(SyncRun.id).filter(SyncRun.connection_id == lc.id).all()]
        if run_ids:
            ps_deleted = (
                session.query(ExternalPayloadSnapshot)
                .filter(ExternalPayloadSnapshot.sync_run_id.in_(run_ids))
                .delete(synchronize_session=False)
            )
            totals["payload_snapshots_deleted"] += int(ps_deleted or 0)
        runs_deleted = session.query(SyncRun).filter(SyncRun.connection_id == lc.id).delete(synchronize_session=False)
        totals["sync_runs_deleted"] += int(runs_deleted or 0)

        am_deleted = session.query(ExternalAccountMap).filter(ExternalAccountMap.connection_id == lc.id).delete(
            synchronize_session=False
        )
        totals["account_maps_deleted"] += int(am_deleted or 0)

        cred_deleted = session.query(ExternalCredential).filter(ExternalCredential.connection_id == lc.id).delete(
            synchronize_session=False
        )
        totals["credentials_deleted"] += int(cred_deleted or 0)

        lc.status = "DISABLED"
        lc.last_successful_sync_at = None
        lc.last_successful_txn_end = None
        lc.holdings_last_asof = None
        lc.txn_earliest_available = None
        lc.last_full_sync_at = None
        lc.coverage_status = "UNKNOWN"
        lc.last_error_json = None
        totals["connections_disabled"] += 1

    # Purge account-scoped artifacts for legacy Chase accounts (cash balances + lots, etc).
    # This is necessary to fully retire CSV/Yodlee Chase sources; otherwise "CASH:USD" and/or lots can linger.
    if legacy_account_ids:
        acct_ids = sorted(list(legacy_account_ids))
        try:
            deleted = session.query(CashBalance).filter(CashBalance.account_id.in_(acct_ids)).delete(synchronize_session=False)
            totals["cash_balances_deleted"] += int(deleted or 0)
        except Exception:
            pass
        try:
            deleted = session.query(PositionLot).filter(PositionLot.account_id.in_(acct_ids)).delete(synchronize_session=False)
            totals["position_lots_deleted"] += int(deleted or 0)
        except Exception:
            pass
        try:
            deleted = session.query(IncomeEvent).filter(IncomeEvent.account_id.in_(acct_ids)).delete(synchronize_session=False)
            totals["income_events_deleted"] += int(deleted or 0)
        except Exception:
            pass
        try:
            # Delete lot disposals that reference tax lots for these accounts before deleting tax lots themselves.
            tax_lot_ids = [int(r[0]) for r in session.query(TaxLot.id).filter(TaxLot.account_id.in_(acct_ids)).all()]
            if tax_lot_ids:
                try:
                    deleted = session.query(LotDisposal).filter(LotDisposal.tax_lot_id.in_(tax_lot_ids)).delete(synchronize_session=False)
                    totals["lot_disposals_deleted"] += int(deleted or 0)
                except Exception:
                    pass
                try:
                    deleted = session.query(WashSaleAdjustment).filter(
                        (WashSaleAdjustment.replacement_lot_id.in_(tax_lot_ids))
                    ).delete(synchronize_session=False)
                    totals["wash_adjustments_deleted"] += int(deleted or 0)
                except Exception:
                    pass
            deleted = session.query(TaxLot).filter(TaxLot.account_id.in_(acct_ids)).delete(synchronize_session=False)
            totals["tax_lots_deleted"] += int(deleted or 0)
        except Exception:
            pass
        try:
            # Best-effort: corporate actions tied to these accounts.
            deleted = session.query(CorporateActionEvent).filter(CorporateActionEvent.account_id.in_(acct_ids)).delete(
                synchronize_session=False
            )
            totals["corp_actions_deleted"] += int(deleted or 0)
        except Exception:
            pass

    # Optionally delete legacy Chase accounts entirely (so they disappear from Holdings/Performance selectors).
    if (delete_legacy_accounts or "").strip().lower() in {"1", "true", "yes", "y", "on"}:
        try:
            plaid_account_ids = {
                int(r[0])
                for r in session.query(ExternalAccountMap.account_id)
                .filter(ExternalAccountMap.connection_id == conn.id)
                .all()
                if r and r[0] is not None
            }
        except Exception:
            plaid_account_ids = set()
        try:
            legacy_accounts = (
                session.query(Account)
                .filter(
                    Account.broker == "CHASE",
                    Account.taxpayer_entity_id == int(conn.taxpayer_entity_id),
                    ~Account.id.in_(sorted(list(plaid_account_ids)) or [0]),
                )
                .all()
            )
        except Exception:
            legacy_accounts = []

        for a in legacy_accounts:
            aid = int(getattr(a, "id"))
            # Double safety: ensure no remaining mappings.
            try:
                mapped = (
                    session.query(ExternalAccountMap.id)
                    .filter(ExternalAccountMap.account_id == aid)
                    .limit(1)
                    .first()
                )
                if mapped is not None:
                    continue
            except Exception:
                continue

            # Ensure dependent rows are gone (best-effort).
            try:
                session.query(CashBalance).filter(CashBalance.account_id == aid).delete(synchronize_session=False)
                session.query(PositionLot).filter(PositionLot.account_id == aid).delete(synchronize_session=False)
                session.query(IncomeEvent).filter(IncomeEvent.account_id == aid).delete(synchronize_session=False)
                session.query(Transaction).filter(Transaction.account_id == aid).delete(synchronize_session=False)
                session.query(TaxLot).filter(TaxLot.account_id == aid).delete(synchronize_session=False)
                session.query(CorporateActionEvent).filter(CorporateActionEvent.account_id == aid).delete(synchronize_session=False)
            except Exception:
                pass

            try:
                session.query(Account).filter(Account.id == aid).delete(synchronize_session=False)
                totals["accounts_deleted"] += 1
            except Exception:
                pass

    if (purge_expenses or "").strip().lower() in {"1", "true", "yes", "y", "on"}:
        try:
            non_plaid_batches = [
                int(r[0])
                for r in session.query(ExpenseImportBatch.id)
                .filter(func.upper(func.coalesce(ExpenseImportBatch.source, "")) != "PLAID")
                .all()
            ]
            if non_plaid_batches:
                chase_txn_ids = [
                    int(r[0])
                    for r in session.query(ExpenseTransaction.id)
                    .filter(
                        func.lower(func.coalesce(ExpenseTransaction.institution, "")).like("%chase%"),
                        ExpenseTransaction.import_batch_id.in_(non_plaid_batches),
                    )
                    .all()
                ]
                if chase_txn_ids:
                    deleted = session.query(ExpenseTransaction).filter(ExpenseTransaction.id.in_(chase_txn_ids)).delete(
                        synchronize_session=False
                    )
                    totals["expense_txns_deleted"] += int(deleted or 0)

                batch_ids_to_delete: list[int] = []
                for bid in non_plaid_batches:
                    remaining = (
                        session.query(ExpenseTransaction.id)
                        .filter(ExpenseTransaction.import_batch_id == bid)
                        .limit(1)
                        .first()
                    )
                    if remaining is None:
                        batch_ids_to_delete.append(int(bid))
                if batch_ids_to_delete:
                    bdel = session.query(ExpenseImportBatch).filter(ExpenseImportBatch.id.in_(batch_ids_to_delete)).delete(
                        synchronize_session=False
                    )
                    totals["expense_batches_deleted"] += int(bdel or 0)

                # Clean up empty Chase expense accounts (non-Plaid) to reduce clutter.
                # Only remove accounts with 0 transactions remaining.
                try:
                    empty_accounts = (
                        session.query(ExpenseAccount.id)
                        .filter(func.lower(func.coalesce(ExpenseAccount.institution, "")).like("%chase%"))
                        .all()
                    )
                    to_delete: list[int] = []
                    for (eid,) in empty_accounts:
                        if eid is None:
                            continue
                        remaining = (
                            session.query(ExpenseTransaction.id)
                            .filter(ExpenseTransaction.expense_account_id == int(eid))
                            .limit(1)
                            .first()
                        )
                        if remaining is None:
                            to_delete.append(int(eid))
                    if to_delete:
                        deleted = session.query(ExpenseAccount).filter(ExpenseAccount.id.in_(to_delete)).delete(
                            synchronize_session=False
                        )
                        totals["expense_accounts_deleted"] += int(deleted or 0)
                except Exception:
                    pass
        except Exception:
            pass

    log_change(
        session,
        actor=actor,
        action="PURGE_LEGACY_CHASE",
        entity="ExternalConnection",
        entity_id=str(conn.id),
        old=None,
        new=totals,
        note=note.strip() or "Purged legacy Chase sources (CSV/Yodlee) in favor of Plaid",
    )
    session.commit()
    msg = urllib.parse.quote(f"Purged legacy Chase sources: {totals['connections_disabled']} connection(s) disabled.")
    return RedirectResponse(url=f"/sync/connections/{connection_id}?ok={msg}", status_code=303)

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
