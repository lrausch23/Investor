from __future__ import annotations

import datetime as dt

from src.db.audit import log_change
from src.db.models import AuditLog, ExternalConnection, SyncRun, TaxpayerEntity
from src.utils.time import UTC


def test_sync_run_and_audit_log_timestamps_are_utc(session):
    tp = TaxpayerEntity(name="Trust", type="TRUST")
    session.add(tp)
    session.flush()

    conn = ExternalConnection(
        name="C",
        provider="IB",
        broker="IB",
        connector="IB_FLEX_OFFLINE",
        taxpayer_entity_id=tp.id,
        status="ACTIVE",
        metadata_json={"data_dir": "fixtures/ib_flex_offline"},
    )
    session.add(conn)
    session.flush()

    run = SyncRun(connection_id=conn.id, status="ERROR", mode="INCREMENTAL")
    session.add(run)
    session.flush()
    log_change(
        session,
        actor="test",
        action="NOTE",
        entity="SyncRun",
        entity_id=str(run.id),
        old=None,
        new=None,
        note="testing tz",
    )
    session.flush()

    assert isinstance(run.started_at, dt.datetime)
    assert run.started_at.tzinfo == UTC

    audit = session.query(AuditLog).order_by(AuditLog.id.desc()).first()
    assert audit is not None
    assert isinstance(audit.at, dt.datetime)
    assert audit.at.tzinfo == UTC
