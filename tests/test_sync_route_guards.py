from __future__ import annotations

import datetime as dt

from src.app.routes.sync import _recover_or_block_unfinished_sync_run
from src.db.models import ExternalConnection, SyncRun, TaxpayerEntity


def _mk_connection(session) -> ExternalConnection:
    tp = TaxpayerEntity(name="Trust", type="TRUST")
    session.add(tp)
    session.flush()
    conn = ExternalConnection(
        name="TestConn",
        provider="YODLEE",
        broker="IB",
        taxpayer_entity_id=tp.id,
        status="ACTIVE",
        metadata_json={},
    )
    session.add(conn)
    session.commit()
    return conn


def test_recent_unfinished_sync_still_blocks(session):
    conn = _mk_connection(session)
    run = SyncRun(
        connection_id=conn.id,
        status="ERROR",
        mode="INCREMENTAL",
        started_at=dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=5),
        finished_at=None,
        pages_fetched=1,
        coverage_json={},
    )
    session.add(run)
    session.commit()

    msg = _recover_or_block_unfinished_sync_run(
        session=session,
        connection=conn,
        connection_id=conn.id,
        actor="test",
    )

    assert msg is not None
    assert "Sync already running" in msg


def test_stale_unfinished_sync_without_progress_is_aborted(session):
    conn = _mk_connection(session)
    run = SyncRun(
        connection_id=conn.id,
        status="ERROR",
        mode="INCREMENTAL",
        started_at=dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=30),
        finished_at=None,
        pages_fetched=0,
        coverage_json={},
    )
    session.add(run)
    session.commit()

    msg = _recover_or_block_unfinished_sync_run(
        session=session,
        connection=conn,
        connection_id=conn.id,
        actor="test",
    )

    session.refresh(run)
    session.refresh(conn)
    assert msg is None
    assert run.finished_at is not None
    assert run.status == "ERROR"
    assert "Stale run aborted" in str(run.error_json)
    assert "Stale run aborted" in str(conn.last_error_json)


def test_old_inflight_sync_with_progress_is_aborted(session):
    conn = _mk_connection(session)
    run = SyncRun(
        connection_id=conn.id,
        status="ERROR",
        mode="INCREMENTAL",
        started_at=dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=7),
        finished_at=None,
        pages_fetched=3,
        coverage_json={},
    )
    session.add(run)
    session.commit()

    msg = _recover_or_block_unfinished_sync_run(
        session=session,
        connection=conn,
        connection_id=conn.id,
        actor="test",
    )

    session.refresh(run)
    assert msg is None
    assert run.finished_at is not None
    assert run.status == "ERROR"
    assert "Orphaned run auto-aborted" in str(run.error_json)
