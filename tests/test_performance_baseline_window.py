from __future__ import annotations

import datetime as dt

from src.core.performance import build_performance_report
from src.db.models import Account, ExternalAccountMap, ExternalConnection, ExternalHoldingSnapshot, TaxpayerEntity


def test_performance_accepts_baseline_snapshot_before_period_start(session):
    tp = TaxpayerEntity(name="Trust", type="TRUST")
    session.add(tp)
    session.flush()

    acct = Account(name="RJ Taxable", broker="RJ", taxpayer_entity_id=tp.id, account_type="TAXABLE")
    session.add(acct)
    session.flush()

    conn = ExternalConnection(
        name="RJ (Offline)",
        provider="RJ",
        broker="RJ",
        connector="RJ_OFFLINE",
        taxpayer_entity_id=tp.id,
        status="ACTIVE",
        metadata_json={},
    )
    session.add(conn)
    session.flush()

    session.add(ExternalAccountMap(connection_id=conn.id, provider_account_id="RJ:TAXABLE", account_id=acct.id))
    session.flush()

    snap0 = ExternalHoldingSnapshot(
        connection_id=conn.id,
        as_of=dt.datetime(2024, 12, 31, 23, 59, 59, tzinfo=dt.timezone.utc),
        payload_json={
            "as_of": "2024-12-31T23:59:59+00:00",
            "items": [{"provider_account_id": "RJ:TAXABLE", "symbol": "AAA", "market_value": 100.0}],
        },
    )
    snap1 = ExternalHoldingSnapshot(
        connection_id=conn.id,
        as_of=dt.datetime(2025, 12, 31, 23, 59, 59, tzinfo=dt.timezone.utc),
        payload_json={
            "as_of": "2025-12-31T23:59:59+00:00",
            "items": [{"provider_account_id": "RJ:TAXABLE", "symbol": "AAA", "market_value": 110.0}],
        },
    )
    session.add_all([snap0, snap1])
    session.commit()

    report = build_performance_report(
        session,
        scope="trust",
        start_date=dt.date(2025, 1, 1),
        end_date=dt.date(2025, 12, 31),
        frequency="month_end",
        benchmark_prices_path=None,
        benchmark_label="VOO",
        baseline_grace_days=14,
    )
    rows = report.get("rows") or []
    assert len(rows) == 1
    r = rows[0]

    # Baseline can come from the prior year (within grace window), enabling true YTD performance.
    assert r.period_start == dt.date(2025, 1, 1)
    assert r.period_end == dt.date(2025, 12, 31)
    assert r.coverage_start == dt.date(2024, 12, 31)
    assert r.begin_value == 100.0
    assert r.end_value == 110.0
    assert r.twr is not None
    assert r.irr is not None
    assert r.xirr == r.irr
