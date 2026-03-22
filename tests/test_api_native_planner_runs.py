from __future__ import annotations

import json

from starlette.requests import Request

from src.app.routes.api_native import (
    PlannerRunCreatePayload,
    PlannerRunRestorePayload,
    WorkspaceNotePayload,
    WorkspaceScenarioPayload,
    native_planner_run_restore,
    native_planner_runs_create,
    native_planner_runs_list,
    native_workspace_get,
)
from src.db.models import AuditLog, NativePlannerRun, NativeWorkspaceState


def _request(path: str, query: str) -> Request:
    scope = {
        "type": "http",
        "method": "GET",
        "path": path,
        "query_string": query.encode("utf-8"),
        "headers": [],
        "client": ("127.0.0.1", 50000),
        "scheme": "http",
        "server": ("test", 80),
    }
    return Request(scope)


def test_planner_runs_create_and_list(session):
    body = PlannerRunCreatePayload(
        title="Rebalance to B1",
        scenario=WorkspaceScenarioPayload(
            desired_cash_buffer_pct=0.21,
            max_alpha_pct=0.10,
            tax_budget=35000,
            avoid_short_term_gains=True,
        ),
        notes=[WorkspaceNotePayload(title="Trim B4", text="use LT lots", severity="high")],
        actions=[{"id": "a1", "title": "Trim B4", "amount": 10000}],
        summary={"health_score": 73, "action_count": 1},
        note="save run",
    )

    create_resp = native_planner_runs_create(
        request=_request("/api/native/planner-runs", "scope=household"),
        body=body,
        session=session,
        actor="native-user",
    )
    created = json.loads(create_resp.body.decode("utf-8"))

    assert created["scope"] == "household"
    assert created["title"] == "Rebalance to B1"
    assert created["action_count"] == 1

    list_resp = native_planner_runs_list(
        request=_request("/api/native/planner-runs", "scope=household&limit=10"),
        session=session,
        _actor="native-user",
    )
    listed = json.loads(list_resp.body.decode("utf-8"))

    assert listed["scope"] == "household"
    assert len(listed["rows"]) == 1
    assert listed["rows"][0]["id"] == created["id"]

    run_row = session.query(NativePlannerRun).one()
    assert run_row.created_by == "native-user"


def test_planner_run_restore_updates_workspace(session):
    create_resp = native_planner_runs_create(
        request=_request("/api/native/planner-runs", "scope=trust"),
        body=PlannerRunCreatePayload(
            title="Trust run",
            scenario=WorkspaceScenarioPayload(
                desired_cash_buffer_pct=0.19,
                max_alpha_pct=0.09,
                tax_budget=22000,
                avoid_short_term_gains=False,
            ),
            notes=[WorkspaceNotePayload(id="n1", title="Check wash", text="", severity="medium")],
            actions=[],
            summary={},
        ),
        session=session,
        actor="u1",
    )
    created = json.loads(create_resp.body.decode("utf-8"))

    restore_resp = native_planner_run_restore(
        run_id=int(created["id"]),
        body=PlannerRunRestorePayload(note="restore test"),
        session=session,
        actor="u2",
    )
    restored = json.loads(restore_resp.body.decode("utf-8"))

    assert restored["workspace"]["scope"] == "trust"
    assert restored["workspace"]["scenario"]["max_alpha_pct"] == 0.09
    assert len(restored["workspace"]["notes"]) == 1

    ws = session.query(NativeWorkspaceState).filter(NativeWorkspaceState.scope == "trust").one()
    assert ws.updated_by == "u2"

    ws_get = native_workspace_get(request=_request("/api/native/workspace", "scope=trust"), session=session, _actor="u2")
    ws_payload = json.loads(ws_get.body.decode("utf-8"))
    assert ws_payload["scenario"]["desired_cash_buffer_pct"] == 0.19

    audit_entities = {r.entity for r in session.query(AuditLog).all()}
    assert "NATIVE_PLANNER_RUN" in audit_entities
    assert "NATIVE_WORKSPACE_STATE" in audit_entities
