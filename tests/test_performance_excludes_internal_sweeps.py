from __future__ import annotations

import datetime as dt


def test_performance_other_cash_out_excludes_internal_sweeps(session):
    from src.core.performance import build_performance_report
    from src.db.models import (
        Account,
        ExternalAccountMap,
        ExternalConnection,
        ExternalHoldingSnapshot,
        ExternalTransactionMap,
        TaxpayerEntity,
        Transaction,
    )

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

    # Single valuation point is fine; cash-out categories should still exclude sweeps.
    session.add(
        ExternalHoldingSnapshot(
            connection_id=conn.id,
            as_of=dt.datetime(2025, 12, 22, tzinfo=dt.timezone.utc),
            payload_json={
                "as_of": "2025-12-22T00:00:00+00:00",
                "items": [{"provider_account_id": "CHASE:IRA", "symbol": "TOTAL", "market_value": 1000.0, "is_total": True}],
            },
        )
    )
    tx = Transaction(
        account_id=acct.id,
        date=dt.date(2025, 5, 22),
        type="OTHER",
        amount=-999.0,
        ticker="QCERQ",
        qty=None,
        lot_links_json={"description": "JPMORGAN IRA DEPOSIT SWEEP JPMORGAN CHASE BANK NA INTRA-DAY DEPOSIT"},
    )
    session.add(tx)
    session.flush()
    session.add(ExternalTransactionMap(connection_id=conn.id, provider_txn_id="sweep-1", transaction_id=tx.id))
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
    assert float(r.other_cash_out or 0.0) == 0.0

