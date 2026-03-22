from __future__ import annotations

import json

from starlette.requests import Request

from src.app.routes.api_native import WorkspaceNotePayload, WorkspaceScenarioPayload, WorkspaceUpdatePayload, native_workspace_get, native_workspace_upsert
from src.db.models import AuditLog, NativeWorkspaceState


def _request(query: str) -> Request:
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/api/native/workspace",
        "query_string": query.encode("utf-8"),
        "headers": [],
        "client": ("127.0.0.1", 50000),
        "scheme": "http",
        "server": ("test", 80),
    }
    return Request(scope)


def test_workspace_get_default_when_missing(session):
    resp = native_workspace_get(request=_request("scope=trust"), session=session, _actor="tester")
    payload = json.loads(resp.body.decode("utf-8"))

    assert payload["scope"] == "trust"
    assert payload["scenario"]["desired_cash_buffer_pct"] == 0.18
    assert payload["notes"] == []


def test_workspace_upsert_persists_state_and_audit(session):
    body = WorkspaceUpdatePayload(
        scenario=WorkspaceScenarioPayload(
            desired_cash_buffer_pct=0.22,
            max_alpha_pct=0.11,
            tax_budget=42000.0,
            avoid_short_term_gains=True,
        ),
        notes=[
            WorkspaceNotePayload(title="Trim B4", text="Focus on LT lots", severity="high", done=False),
            WorkspaceNotePayload(title="Raise cash", text="Target 22% buffer", severity="medium", done=False),
        ],
        replace_notes=True,
        note="save from native app",
    )

    resp = native_workspace_upsert(request=_request("scope=household"), body=body, session=session, actor="native-user")
    payload = json.loads(resp.body.decode("utf-8"))

    assert payload["scope"] == "household"
    assert payload["scenario"]["max_alpha_pct"] == 0.11
    assert len(payload["notes"]) == 2
    assert payload["updated_by"] == "native-user"

    row = session.query(NativeWorkspaceState).filter(NativeWorkspaceState.scope == "household").one()
    assert row.updated_by == "native-user"
    assert len(row.notes_json or []) == 2

    audit = session.query(AuditLog).filter(AuditLog.entity == "NATIVE_WORKSPACE_STATE").one()
    assert audit.entity_id == "household"
    assert audit.actor == "native-user"


def test_workspace_upsert_append_notes(session):
    first = WorkspaceUpdatePayload(
        notes=[WorkspaceNotePayload(id="n1", title="A", text="", severity="low", done=False)],
        replace_notes=True,
    )
    native_workspace_upsert(request=_request("scope=personal"), body=first, session=session, actor="u1")

    second = WorkspaceUpdatePayload(
        notes=[WorkspaceNotePayload(id="n2", title="B", text="", severity="low", done=False)],
        replace_notes=False,
    )
    resp = native_workspace_upsert(request=_request("scope=personal"), body=second, session=session, actor="u2")
    payload = json.loads(resp.body.decode("utf-8"))

    ids = {n.get("id") for n in payload["notes"]}
    assert ids == {"n1", "n2"}
