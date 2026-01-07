from __future__ import annotations

import datetime as dt


def test_combined_begin_end_use_anchor_points_not_downsample_artifacts(session):
    from src.core.performance import build_performance_report
    from src.db.models import Account, ExternalAccountMap, ExternalConnection, ExternalHoldingSnapshot, TaxpayerEntity

    tp = TaxpayerEntity(name="Household", type="PERSONAL")
    session.add(tp)
    session.flush()

    acct_a = Account(name="Acct A", broker="CHASE", account_type="IRA", taxpayer_entity_id=tp.id)
    acct_b = Account(name="Acct B", broker="RJ", account_type="TAXABLE", taxpayer_entity_id=tp.id)
    session.add_all([acct_a, acct_b])
    session.flush()

    conn_a = ExternalConnection(
        name="Conn A",
        provider="CHASE",
        broker="CHASE",
        connector="CHASE_OFFLINE",
        taxpayer_entity_id=tp.id,
        status="ACTIVE",
    )
    conn_b = ExternalConnection(
        name="Conn B",
        provider="RJ",
        broker="RJ",
        connector="RJ_OFFLINE",
        taxpayer_entity_id=tp.id,
        status="ACTIVE",
    )
    session.add_all([conn_a, conn_b])
    session.flush()

    session.add_all(
        [
            ExternalAccountMap(connection_id=conn_a.id, provider_account_id="A", account_id=acct_a.id),
            ExternalAccountMap(connection_id=conn_b.id, provider_account_id="B", account_id=acct_b.id),
        ]
    )
    session.flush()

    # Conn A has an exact 2025-01-01 anchor (not a month-end), plus 12/31.
    session.add_all(
        [
            ExternalHoldingSnapshot(
                connection_id=conn_a.id,
                as_of=dt.datetime(2024, 12, 31, 23, 59, 59, tzinfo=dt.timezone.utc),
                payload_json={
                    "as_of": "2024-12-31T23:59:59+00:00",
                    "items": [{"provider_account_id": "A", "symbol": "TOTAL", "market_value": 100.0, "is_total": True}],
                },
            ),
            ExternalHoldingSnapshot(
                connection_id=conn_a.id,
                as_of=dt.datetime(2025, 1, 1, 23, 59, 59, tzinfo=dt.timezone.utc),
                payload_json={
                    "as_of": "2025-01-01T23:59:59+00:00",
                    "items": [{"provider_account_id": "A", "symbol": "TOTAL", "market_value": 90.0, "is_total": True}],
                },
            ),
            ExternalHoldingSnapshot(
                connection_id=conn_a.id,
                as_of=dt.datetime(2025, 12, 31, 23, 59, 59, tzinfo=dt.timezone.utc),
                payload_json={
                    "as_of": "2025-12-31T23:59:59+00:00",
                    "items": [{"provider_account_id": "A", "symbol": "TOTAL", "market_value": 110.0, "is_total": True}],
                },
            ),
        ]
    )
    # Conn B has only month-end 12/31 points.
    session.add_all(
        [
            ExternalHoldingSnapshot(
                connection_id=conn_b.id,
                as_of=dt.datetime(2024, 12, 31, 23, 59, 59, tzinfo=dt.timezone.utc),
                payload_json={
                    "as_of": "2024-12-31T23:59:59+00:00",
                    "items": [{"provider_account_id": "B", "symbol": "TOTAL", "market_value": 200.0, "is_total": True}],
                },
            ),
            ExternalHoldingSnapshot(
                connection_id=conn_b.id,
                as_of=dt.datetime(2025, 12, 31, 23, 59, 59, tzinfo=dt.timezone.utc),
                payload_json={
                    "as_of": "2025-12-31T23:59:59+00:00",
                    "items": [{"provider_account_id": "B", "symbol": "TOTAL", "market_value": 220.0, "is_total": True}],
                },
            ),
        ]
    )
    session.commit()

    report = build_performance_report(
        session,
        scope="household",
        start_date=dt.date(2025, 1, 1),
        end_date=dt.date(2025, 12, 31),
        frequency="month_end",
        benchmark_prices_path=None,
        connection_ids=[int(conn_a.id), int(conn_b.id)],
        include_combined=True,
    )
    combined = report.get("combined")
    assert combined is not None
    assert float(combined.begin_value or 0.0) == 290.0  # 90 (A on 1/1) + 200 (B carried from 12/31)
    assert float(combined.end_value or 0.0) == 330.0  # 110 + 220
    assert str(combined.coverage_start) == dt.date(2025, 1, 1).isoformat()
    assert str(combined.coverage_end) == dt.date(2025, 12, 31).isoformat()

