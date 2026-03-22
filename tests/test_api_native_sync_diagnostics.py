from __future__ import annotations

import datetime as dt
import json

from starlette.requests import Request

from src.app.routes.api_native import native_sync_diagnostics
from src.db.models import ExternalConnection, SyncRun, TaxpayerEntity


def _request(query: str) -> Request:
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/api/native/sync-diagnostics",
        "query_string": query.encode("utf-8"),
        "headers": [],
        "client": ("127.0.0.1", 50000),
        "scheme": "http",
        "server": ("test", 80),
    }
    return Request(scope)


def test_api_native_sync_diagnostics_returns_timeline_and_actions(session):
    trust = TaxpayerEntity(name="Trust", type="TRUST")
    session.add(trust)
    session.flush()

    conn = ExternalConnection(
        name="IB Native",
        provider="IB",
        broker="IB",
        connector="IB_FLEX_WEB",
        taxpayer_entity_id=trust.id,
        status="ACTIVE",
        coverage_status="PARTIAL",
        metadata_json={},
        last_successful_sync_at=dt.datetime(2026, 1, 1, 12, 0, tzinfo=dt.timezone.utc),
        last_error_json='{"error":"Credentials expired"}',
    )
    session.add(conn)
    session.flush()

    session.add(
        SyncRun(
            connection_id=conn.id,
            started_at=dt.datetime(2026, 2, 6, 10, 0, tzinfo=dt.timezone.utc),
            finished_at=dt.datetime(2026, 2, 6, 10, 8, tzinfo=dt.timezone.utc),
            status="ERROR",
            mode="FULL",
            pages_fetched=3,
            txn_count=22,
            new_count=4,
            dupes_count=1,
            parse_fail_count=2,
            missing_symbol_count=1,
            error_json='{"detail":"token revoked"}',
            coverage_json={"status": "PARTIAL"},
        )
    )
    session.add(
        SyncRun(
            connection_id=conn.id,
            started_at=dt.datetime(2026, 1, 29, 10, 0, tzinfo=dt.timezone.utc),
            finished_at=dt.datetime(2026, 1, 29, 10, 6, tzinfo=dt.timezone.utc),
            status="SUCCESS",
            mode="INCREMENTAL",
            pages_fetched=1,
            txn_count=10,
            new_count=2,
            dupes_count=0,
            parse_fail_count=0,
            missing_symbol_count=0,
            coverage_json={"status": "COMPLETE"},
        )
    )
    session.commit()

    response = native_sync_diagnostics(
        request=_request("scope=trust&limit_runs=4"),
        session=session,
        _actor="native-user",
    )

    assert response.status_code == 200
    payload = json.loads(response.body.decode("utf-8"))
    assert payload["scope"] == "trust"
    assert payload["limit_runs"] == 4
    assert len(payload["rows"]) == 1

    row = payload["rows"][0]
    assert row["id"] == conn.id
    assert row["last_run_status"] == "ERROR"
    assert row["health"]["level"] in {"attention", "unhealthy"}
    assert len(row["timeline"]) == 2
    assert row["timeline"][0]["status"] == "ERROR"
    assert row["timeline"][0]["duration_seconds"] == 480
    assert len(row["action_items"]) >= 1


def test_api_native_sync_diagnostics_keeps_recent_runs_for_each_connection(session):
    trust = TaxpayerEntity(name="Trust", type="TRUST")
    session.add(trust)
    session.flush()

    noisy = ExternalConnection(
        name="IB Web",
        provider="IB",
        broker="IB",
        connector="IB_FLEX_WEB",
        taxpayer_entity_id=trust.id,
        status="ACTIVE",
        coverage_status="COMPLETE",
        metadata_json={},
        last_successful_sync_at=dt.datetime(2026, 2, 25, 12, 0, tzinfo=dt.timezone.utc),
    )
    quiet = ExternalConnection(
        name="RJ Offline",
        provider="RJ",
        broker="RJ",
        connector="RJ_OFFLINE",
        taxpayer_entity_id=trust.id,
        status="ACTIVE",
        coverage_status="COMPLETE",
        metadata_json={},
        last_successful_sync_at=dt.datetime(2026, 2, 25, 11, 0, tzinfo=dt.timezone.utc),
    )
    session.add_all([noisy, quiet])
    session.flush()

    for idx in range(8):
        started = dt.datetime(2026, 2, 25, 12, 0, tzinfo=dt.timezone.utc) - dt.timedelta(minutes=idx)
        session.add(
            SyncRun(
                connection_id=noisy.id,
                started_at=started,
                finished_at=started + dt.timedelta(minutes=1),
                status="SUCCESS",
                mode="INCREMENTAL",
                coverage_json={"status": "COMPLETE"},
            )
        )

    quiet_started = dt.datetime(2026, 2, 25, 10, 30, tzinfo=dt.timezone.utc)
    session.add(
        SyncRun(
            connection_id=quiet.id,
            started_at=quiet_started,
            finished_at=quiet_started + dt.timedelta(minutes=2),
            status="SUCCESS",
            mode="FULL",
            coverage_json={"status": "COMPLETE"},
        )
    )
    session.commit()

    response = native_sync_diagnostics(
        request=_request("scope=trust&limit_runs=2"),
        session=session,
        _actor="native-user",
    )

    payload = json.loads(response.body.decode("utf-8"))
    rows = {int(row["id"]): row for row in payload["rows"]}

    assert len(rows[int(noisy.id)]["timeline"]) == 2
    assert len(rows[int(quiet.id)]["timeline"]) == 1
    assert rows[int(quiet.id)]["timeline"][0]["status"] == "SUCCESS"
