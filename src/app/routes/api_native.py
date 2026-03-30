from __future__ import annotations

import datetime as dt
import json
import uuid
from collections import defaultdict
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from src.app.auth import require_actor
from src.app.db import db_session
from src.app.utils import jsonable
from src.core.dashboard_service import parse_scope, sync_connections_coverage
from src.core.native_snapshot import build_native_snapshot
from src.db.audit import log_change
from src.db.models import ExternalConnection, NativePlannerRun, NativeWorkspaceState, SyncRun
from src.utils.time import now_utc

router = APIRouter(prefix="/api/native", tags=["api-native"])


def _normalize_scope(raw: str | None) -> str:
    return parse_scope((raw or "").strip().lower())


def _default_scenario() -> dict[str, Any]:
    return {
        "desired_cash_buffer_pct": 0.18,
        "max_alpha_pct": 0.10,
        "tax_budget": 35000.0,
        "avoid_short_term_gains": True,
    }


def _workspace_payload(row: NativeWorkspaceState | None, scope: str) -> dict[str, Any]:
    if row is None:
        return {
            "scope": scope,
            "scenario": _default_scenario(),
            "notes": [],
            "updated_at": None,
            "updated_by": None,
        }
    return {
        "scope": scope,
        "scenario": row.scenario_json or _default_scenario(),
        "notes": row.notes_json or [],
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
        "updated_by": row.updated_by,
    }


def _normalize_note_item(raw: "WorkspaceNotePayload") -> dict[str, Any]:
    severity = str(raw.severity or "normal").strip().lower()
    if severity not in {"high", "medium", "low", "normal"}:
        severity = "normal"
    return {
        "id": (raw.id or uuid.uuid4().hex),
        "title": str(raw.title or "").strip()[:200],
        "text": str(raw.text or "").strip(),
        "severity": severity,
        "done": bool(raw.done),
        "created_at": str(raw.created_at or now_utc().isoformat()),
    }


class WorkspaceScenarioPayload(BaseModel):
    desired_cash_buffer_pct: float = Field(default=0.18, ge=0.0, le=1.0)
    max_alpha_pct: float = Field(default=0.10, ge=0.0, le=1.0)
    tax_budget: float = Field(default=35000.0, ge=0.0)
    avoid_short_term_gains: bool = True


class WorkspaceNotePayload(BaseModel):
    id: str | None = None
    title: str = Field(min_length=1, max_length=200)
    text: str = ""
    severity: str = "normal"
    done: bool = False
    created_at: str | None = None


class WorkspaceUpdatePayload(BaseModel):
    scenario: WorkspaceScenarioPayload | None = None
    notes: list[WorkspaceNotePayload] | None = None
    replace_notes: bool = True
    note: str | None = None


class PlannerRunCreatePayload(BaseModel):
    title: str | None = Field(default=None, max_length=200)
    scenario: WorkspaceScenarioPayload
    notes: list[WorkspaceNotePayload] = Field(default_factory=list)
    actions: list[dict[str, Any]] = Field(default_factory=list)
    summary: dict[str, Any] = Field(default_factory=dict)
    note: str | None = None


class PlannerRunRestorePayload(BaseModel):
    note: str | None = None


def _planner_run_payload(row: NativePlannerRun) -> dict[str, Any]:
    actions = list(row.actions_json or [])
    return {
        "id": row.id,
        "scope": row.scope,
        "title": row.title,
        "scenario": row.scenario_json or _default_scenario(),
        "notes": row.notes_json or [],
        "actions": actions,
        "summary": row.summary_json or {},
        "action_count": len(actions),
        "created_at": row.created_at.isoformat() if row.created_at else None,
        "created_by": row.created_by,
    }


def _parse_iso_datetime(raw: str | None) -> dt.datetime | None:
    if not raw:
        return None
    try:
        normalized = str(raw).replace("Z", "+00:00")
        parsed = dt.datetime.fromisoformat(normalized)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=dt.timezone.utc)
        return parsed
    except Exception:
        return None


def _error_summary(raw: str | None) -> str | None:
    txt = str(raw or "").strip()
    if not txt:
        return None
    try:
        parsed = json.loads(txt)
    except Exception:
        return txt[:300]
    if isinstance(parsed, dict):
        for key in ("error", "message", "detail", "reason"):
            value = parsed.get(key)
            if value:
                return str(value)[:300]
    return str(parsed)[:300]


def _sync_run_payload(run: SyncRun) -> dict[str, Any]:
    duration_seconds = None
    if run.started_at is not None and run.finished_at is not None:
        try:
            duration_seconds = max(0, int((run.finished_at - run.started_at).total_seconds()))
        except Exception:
            duration_seconds = None
    return {
        "id": int(run.id),
        "status": str(run.status or "UNKNOWN"),
        "mode": str(run.mode or "UNKNOWN"),
        "started_at": run.started_at.isoformat() if run.started_at else None,
        "finished_at": run.finished_at.isoformat() if run.finished_at else None,
        "duration_seconds": duration_seconds,
        "requested_start_date": run.requested_start_date.isoformat() if run.requested_start_date else None,
        "requested_end_date": run.requested_end_date.isoformat() if run.requested_end_date else None,
        "effective_start_date": run.effective_start_date.isoformat() if run.effective_start_date else None,
        "effective_end_date": run.effective_end_date.isoformat() if run.effective_end_date else None,
        "pages_fetched": int(run.pages_fetched or 0),
        "txn_count": int(run.txn_count or 0),
        "new_count": int(run.new_count or 0),
        "dupes_count": int(run.dupes_count or 0),
        "parse_fail_count": int(run.parse_fail_count or 0),
        "missing_symbol_count": int(run.missing_symbol_count or 0),
        "error_summary": _error_summary(run.error_json),
        "coverage": dict(run.coverage_json or {}),
    }


def _sync_health_payload(
    *,
    connector: str,
    coverage_status: str,
    last_successful_sync_at: str | None,
    last_run_status: str,
    now_utc: dt.datetime,
) -> dict[str, Any]:
    connector_u = str(connector or "").upper()
    coverage_u = str(coverage_status or "UNKNOWN").upper()
    run_u = str(last_run_status or "UNKNOWN").upper()
    is_web = connector_u in {"IB_FLEX_WEB", "CHASE_YODLEE", "CHASE_PLAID", "AMEX_PLAID"}
    last_success = _parse_iso_datetime(last_successful_sync_at)

    if last_success is None:
        return {
            "level": "unhealthy",
            "label": "Unhealthy",
            "reason": "Never synced successfully",
            "age_hours": None,
        }

    age_hours = max(0.0, (now_utc - last_success).total_seconds() / 3600.0)
    attention_hours = 18.0 if is_web else 24.0 * 7.0
    stale_hours = 36.0 if is_web else 24.0 * 14.0

    if age_hours > stale_hours:
        return {
            "level": "unhealthy",
            "label": "Unhealthy",
            "reason": "Sync is stale",
            "age_hours": age_hours,
        }
    if run_u in {"ERROR", "FAIL", "FAILED"}:
        return {
            "level": "attention",
            "label": "Attention",
            "reason": "Last run ended with error",
            "age_hours": age_hours,
        }
    if run_u == "PARTIAL":
        return {
            "level": "attention",
            "label": "Attention",
            "reason": "Last run was partial",
            "age_hours": age_hours,
        }
    if age_hours > attention_hours:
        return {
            "level": "attention",
            "label": "Attention",
            "reason": "Sync aging threshold exceeded",
            "age_hours": age_hours,
        }
    if coverage_u in {"PARTIAL", "UNKNOWN"}:
        return {
            "level": "attention",
            "label": "Attention",
            "reason": f"Coverage is {coverage_u.lower()}",
            "age_hours": age_hours,
        }
    return {
        "level": "healthy",
        "label": "Healthy",
        "reason": "Connection is up to date",
        "age_hours": age_hours,
    }


def _sync_action_items(
    *,
    coverage_status: str,
    last_run_status: str,
    latest_run: SyncRun | None,
    health_level: str,
    error_summary: str | None,
) -> list[dict[str, str]]:
    actions: list[dict[str, str]] = []
    coverage_u = str(coverage_status or "UNKNOWN").upper()
    run_u = str(last_run_status or "UNKNOWN").upper()

    if health_level == "unhealthy":
        actions.append(
            {
                "id": "run-full-sync",
                "severity": "high",
                "title": "Run full sync",
                "detail": "Connection appears stale; run a full sync to refresh transactions and holdings coverage.",
            }
        )

    if coverage_u in {"PARTIAL", "UNKNOWN"}:
        actions.append(
            {
                "id": "resolve-coverage",
                "severity": "medium",
                "title": "Resolve coverage gaps",
                "detail": "Coverage is not complete; run full mode and verify connector date window/source files.",
            }
        )

    if run_u in {"ERROR", "FAIL", "FAILED", "PARTIAL"}:
        detail = error_summary or "Review latest sync run diagnostics and connector credentials."
        actions.append(
            {
                "id": "inspect-last-error",
                "severity": "high" if run_u in {"ERROR", "FAIL", "FAILED"} else "medium",
                "title": "Inspect latest run",
                "detail": detail,
            }
        )

    if latest_run is not None:
        if int(latest_run.parse_fail_count or 0) > 0:
            actions.append(
                {
                    "id": "resolve-parse-failures",
                    "severity": "medium",
                    "title": "Fix parse failures",
                    "detail": "Recent run had parse failures; review file format assumptions and parser coverage.",
                }
            )
        if int(latest_run.missing_symbol_count or 0) > 0:
            actions.append(
                {
                    "id": "map-missing-symbols",
                    "severity": "medium",
                    "title": "Map missing symbols",
                    "detail": "Recent run had missing symbols; add ticker mappings or price-source coverage.",
                }
            )

    deduped: list[dict[str, str]] = []
    seen: set[str] = set()
    for item in actions:
        key = str(item.get("id") or "")
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped[:4]


@router.get("/snapshot")
def native_snapshot(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    scope = _normalize_scope(request.query_params.get("scope"))
    as_of_raw = (request.query_params.get("as_of") or "").strip()

    as_of = dt.date.today()
    if as_of_raw:
        try:
            as_of = dt.date.fromisoformat(as_of_raw)
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid as_of; expected YYYY-MM-DD")

    snapshot = build_native_snapshot(session, scope=scope, as_of=as_of)
    payload = jsonable(snapshot)
    payload["actor"] = actor
    payload["requested_scope"] = scope
    return JSONResponse(content=payload)


@router.get("/workspace")
def native_workspace_get(
    request: Request,
    session: Session = Depends(db_session),
    _actor: str = Depends(require_actor),
):
    scope = _normalize_scope(request.query_params.get("scope"))
    row = session.query(NativeWorkspaceState).filter(NativeWorkspaceState.scope == scope).one_or_none()
    return JSONResponse(content=jsonable(_workspace_payload(row, scope)))


@router.post("/workspace")
def native_workspace_upsert(
    request: Request,
    body: WorkspaceUpdatePayload,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    scope = _normalize_scope(request.query_params.get("scope"))
    row = session.query(NativeWorkspaceState).filter(NativeWorkspaceState.scope == scope).one_or_none()

    old = _workspace_payload(row, scope) if row is not None else None

    if row is None:
        row = NativeWorkspaceState(scope=scope, scenario_json=_default_scenario(), notes_json=[], updated_by=actor)
        session.add(row)
        session.flush()

    if body.scenario is not None:
        row.scenario_json = body.scenario.model_dump()

    if body.notes is not None:
        incoming = [_normalize_note_item(n) for n in body.notes]
        if body.replace_notes:
            row.notes_json = incoming
        else:
            existing = list(row.notes_json or [])
            seen_ids = {str(x.get("id") or "") for x in existing}
            for note_item in incoming:
                nid = str(note_item.get("id") or "")
                if nid and nid not in seen_ids:
                    existing.append(note_item)
                    seen_ids.add(nid)
            row.notes_json = existing

    row.updated_by = actor
    row.updated_at = now_utc()
    session.flush()

    new = _workspace_payload(row, scope)
    log_change(
        session,
        actor=actor,
        action="UPSERT",
        entity="NATIVE_WORKSPACE_STATE",
        entity_id=scope,
        old=old,
        new=new,
        note=(body.note or "Native workspace update"),
    )
    session.commit()
    return JSONResponse(content=jsonable(new))


@router.get("/planner-runs")
def native_planner_runs_list(
    request: Request,
    session: Session = Depends(db_session),
    _actor: str = Depends(require_actor),
):
    scope = _normalize_scope(request.query_params.get("scope"))
    limit_raw = (request.query_params.get("limit") or "25").strip()
    try:
        limit = int(limit_raw)
    except Exception:
        limit = 25
    limit = max(1, min(200, limit))

    rows = (
        session.query(NativePlannerRun)
        .filter(NativePlannerRun.scope == scope)
        .order_by(NativePlannerRun.created_at.desc(), NativePlannerRun.id.desc())
        .limit(limit)
        .all()
    )
    payload = [_planner_run_payload(r) for r in rows]
    return JSONResponse(content=jsonable({"scope": scope, "rows": payload}))


@router.post("/planner-runs")
def native_planner_runs_create(
    request: Request,
    body: PlannerRunCreatePayload,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    scope = _normalize_scope(request.query_params.get("scope"))
    title = (body.title or "").strip() or f"Scenario run {dt.datetime.now().strftime('%Y-%m-%d %H:%M')}"
    notes = [_normalize_note_item(n) for n in body.notes]

    row = NativePlannerRun(
        scope=scope,
        title=title[:200],
        scenario_json=body.scenario.model_dump(),
        notes_json=notes,
        actions_json=list(body.actions or []),
        summary_json=dict(body.summary or {}),
        created_by=actor,
    )
    session.add(row)
    session.flush()

    created = _planner_run_payload(row)
    log_change(
        session,
        actor=actor,
        action="CREATE",
        entity="NATIVE_PLANNER_RUN",
        entity_id=str(row.id),
        old=None,
        new=created,
        note=(body.note or "Native planner run saved"),
    )
    session.commit()
    return JSONResponse(content=jsonable(created))


@router.get("/planner-runs/{run_id}")
def native_planner_run_get(
    run_id: int,
    session: Session = Depends(db_session),
    _actor: str = Depends(require_actor),
):
    row = session.query(NativePlannerRun).filter(NativePlannerRun.id == int(run_id)).one_or_none()
    if row is None:
        raise HTTPException(status_code=404, detail="Planner run not found")
    return JSONResponse(content=jsonable(_planner_run_payload(row)))


@router.post("/planner-runs/{run_id}/restore")
def native_planner_run_restore(
    run_id: int,
    body: PlannerRunRestorePayload,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    run = session.query(NativePlannerRun).filter(NativePlannerRun.id == int(run_id)).one_or_none()
    if run is None:
        raise HTTPException(status_code=404, detail="Planner run not found")

    row = session.query(NativeWorkspaceState).filter(NativeWorkspaceState.scope == run.scope).one_or_none()
    old = _workspace_payload(row, run.scope) if row is not None else None

    if row is None:
        row = NativeWorkspaceState(scope=run.scope, scenario_json=_default_scenario(), notes_json=[], updated_by=actor)
        session.add(row)
        session.flush()

    row.scenario_json = dict(run.scenario_json or _default_scenario())
    row.notes_json = list(run.notes_json or [])
    row.updated_by = actor
    row.updated_at = now_utc()
    session.flush()

    new = _workspace_payload(row, run.scope)
    log_change(
        session,
        actor=actor,
        action="RESTORE",
        entity="NATIVE_PLANNER_RUN",
        entity_id=str(run.id),
        old=None,
        new={"workspace": new, "run": _planner_run_payload(run)},
        note=(body.note or "Native planner run restored to workspace"),
    )
    log_change(
        session,
        actor=actor,
        action="UPSERT",
        entity="NATIVE_WORKSPACE_STATE",
        entity_id=run.scope,
        old=old,
        new=new,
        note=f"Restored from planner run {run.id}",
    )
    session.commit()
    return JSONResponse(content=jsonable({"workspace": new, "run": _planner_run_payload(run)}))


@router.get("/holdings/drilldown")
def native_holdings_drilldown(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    # Reuse web holdings drilldown logic to keep lot valuation semantics identical.
    from src.app.routes.holdings import holdings_drilldown_json

    return holdings_drilldown_json(request=request, session=session, _actor=actor)


@router.get("/sync-diagnostics")
def native_sync_diagnostics(
    request: Request,
    session: Session = Depends(db_session),
    _actor: str = Depends(require_actor),
):
    scope = _normalize_scope(request.query_params.get("scope"))
    limit_raw = (request.query_params.get("limit_runs") or "6").strip()
    try:
        limit_runs = int(limit_raw)
    except Exception:
        limit_runs = 6
    limit_runs = max(1, min(20, limit_runs))

    coverage_rows = sync_connections_coverage(session=session, scope=scope, as_of=dt.date.today())
    if not coverage_rows:
        return JSONResponse(content=jsonable({"scope": scope, "generated_at": now_utc().isoformat(), "rows": []}))

    conn_ids = [int(c.get("id") or 0) for c in coverage_rows if int(c.get("id") or 0) > 0]
    conn_rows = (
        session.query(ExternalConnection)
        .filter(ExternalConnection.id.in_(conn_ids))
        .all()
    )
    conn_by_id = {int(c.id): c for c in conn_rows}

    ranked_runs = (
        select(
            SyncRun.id.label("sync_run_id"),
            func.row_number()
            .over(
                partition_by=SyncRun.connection_id,
                order_by=(SyncRun.started_at.desc(), SyncRun.id.desc()),
            )
            .label("run_rank"),
        )
        .where(SyncRun.connection_id.in_(conn_ids))
        .subquery()
    )
    runs = (
        session.query(SyncRun)
        .join(ranked_runs, ranked_runs.c.sync_run_id == SyncRun.id)
        .filter(ranked_runs.c.run_rank <= limit_runs)
        .order_by(SyncRun.connection_id.asc(), SyncRun.started_at.desc(), SyncRun.id.desc())
        .all()
    )
    runs_by_conn: dict[int, list[SyncRun]] = defaultdict(list)
    for run in runs:
        bucket = runs_by_conn.get(int(run.connection_id))
        if bucket is None:
            bucket = []
            runs_by_conn[int(run.connection_id)] = bucket
        if len(bucket) < limit_runs:
            bucket.append(run)

    now_dt = now_utc()
    payload_rows: list[dict[str, Any]] = []
    for row in coverage_rows:
        conn_id = int(row.get("id") or 0)
        if conn_id <= 0:
            continue

        timeline = [_sync_run_payload(r) for r in (runs_by_conn.get(conn_id) or [])]
        latest_run = (runs_by_conn.get(conn_id) or [None])[0]
        latest_run_status = str(
            (latest_run.status if latest_run is not None else row.get("last_run_status")) or "UNKNOWN"
        )
        conn = conn_by_id.get(conn_id)
        conn_error_summary = _error_summary(getattr(conn, "last_error_json", None))
        run_error_summary = _error_summary(getattr(latest_run, "error_json", None)) if latest_run is not None else None
        best_error_summary = run_error_summary or conn_error_summary
        health = _sync_health_payload(
            connector=str(row.get("connector") or ""),
            coverage_status=str(row.get("coverage_status") or ""),
            last_successful_sync_at=row.get("last_successful_sync_at"),
            last_run_status=latest_run_status,
            now_utc=now_dt,
        )
        actions = _sync_action_items(
            coverage_status=str(row.get("coverage_status") or ""),
            last_run_status=latest_run_status,
            latest_run=latest_run,
            health_level=str(health.get("level") or ""),
            error_summary=best_error_summary,
        )

        payload_rows.append(
            {
                "id": conn_id,
                "name": str(row.get("name") or ""),
                "provider": str(row.get("provider") or ""),
                "broker": str(row.get("broker") or ""),
                "connector": str(row.get("connector") or ""),
                "coverage_status": str(row.get("coverage_status") or "UNKNOWN"),
                "last_run_status": latest_run_status,
                "last_successful_sync_at": row.get("last_successful_sync_at"),
                "holdings_last_as_of": row.get("holdings_last_asof"),
                "txn_earliest_available": row.get("txn_earliest_available"),
                "last_successful_txn_end": row.get("last_successful_txn_end"),
                "broker_closed_lot_ytd_count": int(row.get("broker_closed_lot_ytd_count") or 0),
                "broker_wash_ytd_count": int(row.get("broker_wash_ytd_count") or 0),
                "last_error_summary": best_error_summary,
                "health": health,
                "action_items": actions,
                "timeline": timeline,
            }
        )

    return JSONResponse(
        content=jsonable(
                {
                    "scope": scope,
                    "generated_at": now_dt.isoformat(),
                    "limit_runs": limit_runs,
                    "rows": payload_rows,
                }
        )
    )
