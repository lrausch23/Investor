from __future__ import annotations

import datetime as dt


def test_performance_begin_uses_start_date_point_even_when_month_end_downsampling(session):
    from src.core.performance import build_performance_report
    from src.db.models import Account, ExternalAccountMap, ExternalConnection, ExternalHoldingSnapshot, TaxpayerEntity

    tp = TaxpayerEntity(name="Laszlo Rausch", type="PERSONAL")
    session.add(tp)
    session.flush()

    acct = Account(name="Chase IRA", broker="CHASE", account_type="IRA", taxpayer_entity_id=tp.id)
    session.add(acct)
    session.flush()

    conn = ExternalConnection(
        name="Chase IRA (Offline)",
        provider="CHASE",
        broker="CHASE",
        connector="CHASE_OFFLINE",
        taxpayer_entity_id=tp.id,
        status="ACTIVE",
    )
    session.add(conn)
    session.flush()
    session.add(ExternalAccountMap(connection_id=conn.id, provider_account_id="CHASE:IRA", account_id=acct.id))
    session.flush()

    # Baseline window includes both 2024-12-31 and an exact 2025-01-01 point.
    session.add_all(
        [
            ExternalHoldingSnapshot(
                connection_id=conn.id,
                as_of=dt.datetime(2024, 12, 31, 23, 59, 59, tzinfo=dt.timezone.utc),
                payload_json={
                    "as_of": "2024-12-31T23:59:59+00:00",
                    "items": [{"provider_account_id": "CHASE:IRA", "symbol": "TOTAL", "market_value": 808730.0, "is_total": True}],
                },
            ),
            ExternalHoldingSnapshot(
                connection_id=conn.id,
                as_of=dt.datetime(2025, 1, 1, 23, 59, 59, tzinfo=dt.timezone.utc),
                payload_json={
                    "as_of": "2025-01-01T23:59:59+00:00",
                    "items": [{"provider_account_id": "CHASE:IRA", "symbol": "TOTAL", "market_value": 710577.98, "is_total": True}],
                },
            ),
            ExternalHoldingSnapshot(
                connection_id=conn.id,
                as_of=dt.datetime(2025, 12, 31, 23, 59, 59, tzinfo=dt.timezone.utc),
                payload_json={
                    "as_of": "2025-12-31T23:59:59+00:00",
                    "items": [{"provider_account_id": "CHASE:IRA", "symbol": "TOTAL", "market_value": 564170.68, "is_total": True}],
                },
            ),
        ]
    )
    session.commit()

    report = build_performance_report(
        session,
        scope="ira",
        start_date=dt.date(2025, 1, 1),
        end_date=dt.date(2025, 12, 31),
        frequency="month_end",
        benchmark_prices_path=None,
        account_ids=[acct.id],
        include_combined=False,
    )
    rows = report.get("rows") or []
    assert len(rows) == 1
    r = rows[0]
    assert float(r.begin_value or 0.0) == 710577.98
    assert all("Using begin snapshot at 2024-12-31" not in w for w in (r.warnings or []))

