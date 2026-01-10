from __future__ import annotations

import datetime as dt


def test_performance_report_exposes_transfer_flows_when_include_series(session):
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

    # Provide baseline/end valuation points so the report has anchors.
    session.add_all(
        [
            ExternalHoldingSnapshot(
                connection_id=conn.id,
                as_of=dt.datetime(2024, 12, 31, 23, 59, 59, tzinfo=dt.timezone.utc),
                payload_json={
                    "as_of": "2024-12-31T23:59:59+00:00",
                    "items": [{"provider_account_id": "RJ:TAXABLE", "symbol": "TOTAL", "market_value": 100.0, "is_total": True}],
                },
            ),
            ExternalHoldingSnapshot(
                connection_id=conn.id,
                as_of=dt.datetime(2025, 12, 31, 23, 59, 59, tzinfo=dt.timezone.utc),
                payload_json={
                    "as_of": "2025-12-31T23:59:59+00:00",
                    "items": [{"provider_account_id": "RJ:TAXABLE", "symbol": "TOTAL", "market_value": 110.0, "is_total": True}],
                },
            ),
        ]
    )

    tx1 = Transaction(
        account_id=acct.id,
        date=dt.date(2025, 2, 1),
        type="TRANSFER",
        amount=1000.0,
        ticker="CASH:USD",
        qty=None,
        lot_links_json={"description": "Deposit"},
    )
    tx2 = Transaction(
        account_id=acct.id,
        date=dt.date(2025, 3, 1),
        type="TRANSFER",
        amount=-500.0,
        ticker="CASH:USD",
        qty=None,
        lot_links_json={"description": "Withdrawal"},
    )
    session.add_all([tx1, tx2])
    session.flush()
    session.add_all(
        [
            ExternalTransactionMap(connection_id=conn.id, provider_txn_id="t1", transaction_id=tx1.id),
            ExternalTransactionMap(connection_id=conn.id, provider_txn_id="t2", transaction_id=tx2.id),
        ]
    )
    session.commit()

    report = build_performance_report(
        session,
        scope="trust",
        start_date=dt.date(2025, 1, 1),
        end_date=dt.date(2025, 12, 31),
        frequency="month_end",
        benchmark_prices_path=None,
        include_combined=False,
        include_series=True,
    )
    flows = report.get("transfer_flows") or {}
    assert int(conn.id) in flows
    assert ("2025-02-01", 1000.0) in flows[int(conn.id)]
    assert ("2025-03-01", -500.0) in flows[int(conn.id)]


def test_performance_transfer_flows_respect_valuation_anchor_window(session):
    """
    When the report anchors begin/end valuations within grace windows (begin_d/end_d),
    transfer flows must be pulled for that same valuation window; otherwise early flows in
    the anchor gap get dropped and withdrawals/contributions are understated.
    """
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

    tp = TaxpayerEntity(name="Trust", type="TRUST")
    session.add(tp)
    session.flush()

    acct = Account(name="IB Account", broker="IB", taxpayer_entity_id=tp.id, account_type="TAXABLE")
    session.add(acct)
    session.flush()

    conn = ExternalConnection(
        name="IB Flex (Web)",
        provider="IB",
        broker="IB",
        connector="IB_FLEX_WEB",
        taxpayer_entity_id=tp.id,
        status="ACTIVE",
        metadata_json={},
    )
    session.add(conn)
    session.flush()
    session.add(ExternalAccountMap(connection_id=conn.id, provider_account_id="IBFLEX:IB", account_id=acct.id))
    session.flush()

    # Start date is 2025-01-09, but the available begin valuation is 2024-12-31 (within grace).
    session.add_all(
        [
            ExternalHoldingSnapshot(
                connection_id=conn.id,
                as_of=dt.datetime(2024, 12, 31, 23, 59, 59, tzinfo=dt.timezone.utc),
                payload_json={
                    "as_of": "2024-12-31T23:59:59+00:00",
                    "items": [{"provider_account_id": "IBFLEX:IB", "symbol": "TOTAL", "market_value": 100.0, "is_total": True}],
                },
            ),
            ExternalHoldingSnapshot(
                connection_id=conn.id,
                as_of=dt.datetime(2026, 1, 8, 23, 59, 59, tzinfo=dt.timezone.utc),
                payload_json={
                    "as_of": "2026-01-08T23:59:59+00:00",
                    "items": [{"provider_account_id": "IBFLEX:IB", "symbol": "TOTAL", "market_value": 110.0, "is_total": True}],
                },
            ),
        ]
    )

    # Transfer just before the requested start date (still within anchored valuation window).
    t0 = Transaction(
        account_id=acct.id,
        date=dt.date(2025, 1, 7),
        type="TRANSFER",
        amount=-20000.0,
        ticker="CASH:USD",
        qty=None,
        lot_links_json={"provider_account_id": "IBFLEX:IB", "description": "DISBURSEMENT"},
    )
    t1 = Transaction(
        account_id=acct.id,
        date=dt.date(2025, 2, 4),
        type="TRANSFER",
        amount=-20000.0,
        ticker="CASH:USD",
        qty=None,
        lot_links_json={"provider_account_id": "IBFLEX:IB", "description": "DISBURSEMENT"},
    )
    session.add_all([t0, t1])
    session.flush()
    session.add_all(
        [
            ExternalTransactionMap(connection_id=conn.id, provider_txn_id="w1", transaction_id=t0.id),
            ExternalTransactionMap(connection_id=conn.id, provider_txn_id="w2", transaction_id=t1.id),
        ]
    )
    session.commit()

    report = build_performance_report(
        session,
        scope="trust",
        start_date=dt.date(2025, 1, 9),
        end_date=dt.date(2026, 1, 9),
        frequency="month_end",
        benchmark_prices_path=None,
        include_combined=False,
        include_series=True,
    )
    row = (report.get("rows") or [None])[0]
    assert row is not None
    # Both withdrawals should be counted in the cashflow summary (valuation window anchored from 2024-12-31).
    assert float(getattr(row, "withdrawals", 0.0) or 0.0) == 40000.0
