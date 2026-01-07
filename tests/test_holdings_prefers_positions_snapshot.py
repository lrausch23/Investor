from __future__ import annotations

import datetime as dt


def test_build_holdings_view_prefers_positions_snapshot_over_total_only(session):
    from src.core.external_holdings import build_holdings_view
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

    # Older snapshot with real positions.
    session.add(
        ExternalHoldingSnapshot(
            connection_id=conn.id,
            as_of=dt.datetime(2025, 12, 22, 0, 0, tzinfo=dt.timezone.utc),
            payload_json={
                "as_of": "2025-12-22T00:00:00+00:00",
                "items": [{"provider_account_id": "CHASE:IRA", "symbol": "NVDA", "qty": 1.0, "market_value": 100.0}],
            },
        )
    )
    # Newer valuation-only snapshot (TOTAL row only).
    session.add(
        ExternalHoldingSnapshot(
            connection_id=conn.id,
            as_of=dt.datetime(2025, 12, 31, 23, 59, 59, tzinfo=dt.timezone.utc),
            payload_json={
                "as_of": "2025-12-31T23:59:59+00:00",
                "items": [{"provider_account_id": "CHASE:IRA", "symbol": "TOTAL", "market_value": 1000.0, "is_total": True}],
            },
        )
    )
    session.commit()

    view = build_holdings_view(session, scope="household", account_id=int(acct.id), today=dt.date(2026, 1, 1))
    assert any(p.symbol == "NVDA" for p in (view.positions or []))

