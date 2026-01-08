from __future__ import annotations

import datetime as dt

from src.core.external_holdings import build_holdings_view
from src.db.models import (
    Account,
    CashBalance,
    ExternalAccountMap,
    ExternalConnection,
    ExternalHoldingSnapshot,
    ExternalTransactionMap,
    PositionLot,
    Security,
    TaxLot,
    Transaction,
    TaxpayerEntity,
)


def test_holdings_view_combined_includes_cash_balance(session):
    trust = TaxpayerEntity(name="Trust", type="TRUST")
    session.add(trust)
    session.flush()
    a1 = Account(name="U1", broker="IB", account_type="TAXABLE", taxpayer_entity_id=trust.id)
    session.add(a1)
    session.flush()

    conn = ExternalConnection(
        name="IB Flex",
        provider="IB",
        broker="IB",
        connector="IB_FLEX_OFFLINE",
        taxpayer_entity_id=trust.id,
        status="ACTIVE",
        metadata_json={},
    )
    session.add(conn)
    session.flush()
    session.add(ExternalAccountMap(connection_id=conn.id, provider_account_id="IBFLEX:U1", account_id=a1.id))
    session.add(
        ExternalHoldingSnapshot(
            connection_id=conn.id,
            as_of=dt.datetime(2025, 12, 21, 15, 0, 0, tzinfo=dt.timezone.utc),
            payload_json={
                "as_of": "2025-12-21T15:00:00+00:00",
                "items": [
                    {"provider_account_id": "IBFLEX:U1", "symbol": "AAPL", "qty": 10, "market_value": 2000},
                ],
            },
        )
    )
    session.add(CashBalance(account_id=a1.id, as_of_date=dt.date(2025, 12, 21), amount=123.45))
    session.commit()

    view = build_holdings_view(session, scope="household", account_id=None)
    assert view.total_value == 2000 + 123.45
    assert float(view.cash_total) == 123.45
    assert any(p.symbol == "AAPL" for p in view.positions)


def test_holdings_view_combined_separates_accounts_for_same_symbol(session):
    trust = TaxpayerEntity(name="Trust", type="TRUST")
    session.add(trust)
    session.flush()
    a1 = Account(name="U1", broker="IB", account_type="TAXABLE", taxpayer_entity_id=trust.id)
    a2 = Account(name="U2", broker="IB", account_type="TAXABLE", taxpayer_entity_id=trust.id)
    session.add_all([a1, a2])
    session.flush()

    conn = ExternalConnection(
        name="IB Flex",
        provider="IB",
        broker="IB",
        connector="IB_FLEX_OFFLINE",
        taxpayer_entity_id=trust.id,
        status="ACTIVE",
        metadata_json={},
    )
    session.add(conn)
    session.flush()
    session.add_all(
        [
            ExternalAccountMap(connection_id=conn.id, provider_account_id="IBFLEX:U1", account_id=a1.id),
            ExternalAccountMap(connection_id=conn.id, provider_account_id="IBFLEX:U2", account_id=a2.id),
        ]
    )
    session.add(
        ExternalHoldingSnapshot(
            connection_id=conn.id,
            as_of=dt.datetime(2025, 12, 21, 15, 0, 0, tzinfo=dt.timezone.utc),
            payload_json={
                "items": [
                    {"provider_account_id": "IBFLEX:U1", "symbol": "AAPL", "qty": 10, "market_value": 2000},
                    {"provider_account_id": "IBFLEX:U2", "symbol": "AAPL", "qty": 20, "market_value": 4000},
                ],
            },
        )
    )
    session.commit()

    view = build_holdings_view(session, scope="trust", account_id=None)
    aapl_rows = [p for p in view.positions if p.symbol == "AAPL"]
    assert len(aapl_rows) == 2
    assert {p.account_name for p in aapl_rows} == {"U1", "U2"}
    assert sum(float(p.qty or 0.0) for p in aapl_rows) == 30.0
    assert sum(float(p.market_value or 0.0) for p in aapl_rows) == 6000.0


def test_holdings_view_filters_by_scope(session):
    trust = TaxpayerEntity(name="Trust", type="TRUST")
    personal = TaxpayerEntity(name="Personal", type="PERSONAL")
    session.add_all([trust, personal])
    session.flush()
    a_t = Account(name="U-TRUST", broker="IB", account_type="TAXABLE", taxpayer_entity_id=trust.id)
    a_p = Account(name="U-PERS", broker="IB", account_type="IRA", taxpayer_entity_id=personal.id)
    session.add_all([a_t, a_p])
    session.flush()

    conn_t = ExternalConnection(name="C1", provider="IB", broker="IB", connector="IB_FLEX_OFFLINE", taxpayer_entity_id=trust.id, status="ACTIVE", metadata_json={})
    conn_p = ExternalConnection(name="C2", provider="IB", broker="IB", connector="IB_FLEX_OFFLINE", taxpayer_entity_id=personal.id, status="ACTIVE", metadata_json={})
    session.add_all([conn_t, conn_p])
    session.flush()
    session.add_all(
        [
            ExternalAccountMap(connection_id=conn_t.id, provider_account_id="IBFLEX:U-TRUST", account_id=a_t.id),
            ExternalAccountMap(connection_id=conn_p.id, provider_account_id="IBFLEX:U-PERS", account_id=a_p.id),
        ]
    )
    session.add_all(
        [
            ExternalHoldingSnapshot(connection_id=conn_t.id, as_of=dt.datetime(2025, 12, 21, 15, 0, 0, tzinfo=dt.timezone.utc), payload_json={"items": [{"provider_account_id": "IBFLEX:U-TRUST", "symbol": "TSM", "qty": 1, "market_value": 100}]}),
            ExternalHoldingSnapshot(connection_id=conn_p.id, as_of=dt.datetime(2025, 12, 21, 15, 0, 0, tzinfo=dt.timezone.utc), payload_json={"items": [{"provider_account_id": "IBFLEX:U-PERS", "symbol": "VTI", "qty": 1, "market_value": 200}]}),
        ]
    )
    session.commit()

    trust_view = build_holdings_view(session, scope="trust", account_id=None)
    assert any(p.symbol == "TSM" for p in trust_view.positions)
    assert all(p.symbol != "VTI" for p in trust_view.positions)


def test_holdings_view_dedupes_same_asof_snapshots(session):
    trust = TaxpayerEntity(name="Trust", type="TRUST")
    session.add(trust)
    session.flush()
    acct = Account(name="U1", broker="IB", account_type="TAXABLE", taxpayer_entity_id=trust.id)
    session.add(acct)
    session.flush()
    conn = ExternalConnection(
        name="IB Flex",
        provider="IB",
        broker="IB",
        connector="IB_FLEX_OFFLINE",
        taxpayer_entity_id=trust.id,
        status="ACTIVE",
        metadata_json={},
    )
    session.add(conn)
    session.flush()
    session.add(ExternalAccountMap(connection_id=conn.id, provider_account_id="IBFLEX:U1", account_id=acct.id))
    as_of = dt.datetime(2025, 12, 21, 15, 0, 0, tzinfo=dt.timezone.utc)
    # Two snapshots with identical as_of (common for offline mtime-based snapshots).
    session.add_all(
        [
            ExternalHoldingSnapshot(connection_id=conn.id, as_of=as_of, payload_json={"items": [{"provider_account_id": "IBFLEX:U1", "symbol": "AVGO", "qty": 500, "market_value": 170180}]}),
            ExternalHoldingSnapshot(connection_id=conn.id, as_of=as_of, payload_json={"items": [{"provider_account_id": "IBFLEX:U1", "symbol": "AVGO", "qty": 500, "market_value": 170180}]}),
        ]
    )
    session.commit()

    view = build_holdings_view(session, scope="trust", account_id=acct.id)
    avgo = next(p for p in view.positions if p.symbol == "AVGO")
    assert float(avgo.qty or 0.0) == 500.0
    assert float(avgo.market_value or 0.0) == 170180.0


def test_holdings_view_dedupes_duplicate_items_within_snapshot(session):
    trust = TaxpayerEntity(name="Trust", type="TRUST")
    session.add(trust)
    session.flush()
    acct = Account(name="U1", broker="IB", account_type="TAXABLE", taxpayer_entity_id=trust.id)
    session.add(acct)
    session.flush()
    conn = ExternalConnection(
        name="IB Flex Web",
        provider="IB",
        broker="IB",
        connector="IB_FLEX_WEB",
        taxpayer_entity_id=trust.id,
        status="ACTIVE",
        metadata_json={},
    )
    session.add(conn)
    session.flush()
    session.add(ExternalAccountMap(connection_id=conn.id, provider_account_id="IBFLEX:U1", account_id=acct.id))
    session.add(
        ExternalHoldingSnapshot(
            connection_id=conn.id,
            as_of=dt.datetime(2025, 12, 21, 15, 0, 0, tzinfo=dt.timezone.utc),
            payload_json={
                "items": [
                    {"provider_account_id": "IBFLEX:U1", "symbol": "AVGO", "qty": 500, "market_value": 170180},
                    {"provider_account_id": "IBFLEX:U1", "symbol": "AVGO", "qty": 500, "market_value": 170180},
                ]
            },
        )
    )
    session.commit()

    view = build_holdings_view(session, scope="trust", account_id=acct.id)
    avgo = next(p for p in view.positions if p.symbol == "AVGO")
    assert float(avgo.qty or 0.0) == 500.0
    assert float(avgo.market_value or 0.0) == 170180.0


def test_holdings_view_prefers_web_connection_when_both_active(session):
    trust = TaxpayerEntity(name="Trust", type="TRUST")
    session.add(trust)
    session.flush()
    acct = Account(name="U5891158", broker="IB", account_type="TAXABLE", taxpayer_entity_id=trust.id)
    session.add(acct)
    session.flush()

    c_off = ExternalConnection(
        name="IB Flex Offline",
        provider="IB",
        broker="IB",
        connector="IB_FLEX_OFFLINE",
        taxpayer_entity_id=trust.id,
        status="ACTIVE",
        metadata_json={},
    )
    c_web = ExternalConnection(
        name="IB Flex Web",
        provider="IB",
        broker="IB",
        connector="IB_FLEX_WEB",
        taxpayer_entity_id=trust.id,
        status="ACTIVE",
        metadata_json={},
    )
    session.add_all([c_off, c_web])
    session.flush()

    session.add_all(
        [
            ExternalAccountMap(connection_id=c_off.id, provider_account_id="IBFLEX:U5891158", account_id=acct.id),
            ExternalAccountMap(connection_id=c_web.id, provider_account_id="IBFLEX:U5891158", account_id=acct.id),
        ]
    )

    # Same symbol appears in both connections' snapshots; web should win (no doubling).
    session.add_all(
        [
            ExternalHoldingSnapshot(
                connection_id=c_off.id,
                as_of=dt.datetime(2025, 12, 21, 5, 0, 0, tzinfo=dt.timezone.utc),
                payload_json={"items": [{"provider_account_id": "IBFLEX:U5891158", "symbol": "TSM", "qty": 500, "market_value": 100.0}]},
            ),
            ExternalHoldingSnapshot(
                connection_id=c_web.id,
                as_of=dt.datetime(2025, 12, 21, 22, 0, 0, tzinfo=dt.timezone.utc),
                payload_json={"items": [{"provider_account_id": "IBFLEX:U5891158", "symbol": "TSM", "qty": 500, "market_value": 100.0}]},
            ),
        ]
    )
    session.commit()

    view = build_holdings_view(session, scope="trust", account_id=acct.id)
    tsm = next(p for p in view.positions if p.symbol == "TSM")
    assert float(tsm.qty or 0.0) == 500.0
    assert float(tsm.market_value or 0.0) == 100.0
    # Data sources should reflect preferred connector (web).
    assert any((s.get("connector") or "").upper() == "IB_FLEX_WEB" for s in (view.data_sources or []))
    assert all((s.get("connector") or "").upper() != "IB_FLEX_OFFLINE" for s in (view.data_sources or []))


def test_holdings_view_filters_data_sources_to_selected_account(session):
    trust = TaxpayerEntity(name="Trust", type="TRUST")
    session.add(trust)
    session.flush()
    a_rj = Account(name="RJ Taxable", broker="RJ", account_type="TAXABLE", taxpayer_entity_id=trust.id)
    a_ib = Account(name="IB Taxable", broker="IB", account_type="TAXABLE", taxpayer_entity_id=trust.id)
    session.add_all([a_rj, a_ib])
    session.flush()

    c_ib = ExternalConnection(
        name="IB Flex Web",
        provider="IB",
        broker="IB",
        connector="IB_FLEX_WEB",
        taxpayer_entity_id=trust.id,
        status="ACTIVE",
        metadata_json={},
    )
    c_rj = ExternalConnection(
        name="RJ (Offline)",
        provider="RJ",
        broker="RJ",
        connector="RJ_OFFLINE",
        taxpayer_entity_id=trust.id,
        status="ACTIVE",
        metadata_json={},
    )
    session.add_all([c_ib, c_rj])
    session.flush()

    session.add_all(
        [
            ExternalAccountMap(connection_id=c_ib.id, provider_account_id="IBFLEX:U1", account_id=a_ib.id),
            ExternalAccountMap(connection_id=c_rj.id, provider_account_id="RJ:TAXABLE", account_id=a_rj.id),
        ]
    )
    session.add_all(
        [
            ExternalHoldingSnapshot(
                connection_id=c_ib.id,
                as_of=dt.datetime(2026, 1, 6, 0, 0, tzinfo=dt.timezone.utc),
                payload_json={"items": [{"provider_account_id": "IBFLEX:U1", "symbol": "TSM", "qty": 1, "market_value": 100.0}]},
            ),
            ExternalHoldingSnapshot(
                connection_id=c_rj.id,
                as_of=dt.datetime(2026, 1, 7, 0, 0, tzinfo=dt.timezone.utc),
                payload_json={"items": [{"provider_account_id": "RJ:TAXABLE", "symbol": "SGOV", "qty": 1, "market_value": 99.0}]},
            ),
        ]
    )
    session.commit()

    view = build_holdings_view(session, scope="trust", account_id=int(a_rj.id), today=dt.date(2026, 1, 8))
    assert {p.symbol for p in view.positions} == {"SGOV"}
    assert len(view.data_sources or []) == 1
    assert (view.data_sources or [])[0].get("connector") == "RJ_OFFLINE"


def test_holdings_view_dedupes_cashflows_across_connections_by_provider_txn(session):
    trust = TaxpayerEntity(name="Trust", type="TRUST")
    session.add(trust)
    session.flush()
    acct = Account(name="U5891158", broker="IB", account_type="TAXABLE", taxpayer_entity_id=trust.id)
    session.add(acct)
    session.flush()

    c_off = ExternalConnection(
        name="IB Flex Offline",
        provider="IB",
        broker="IB",
        connector="IB_FLEX_OFFLINE",
        taxpayer_entity_id=trust.id,
        status="ACTIVE",
        metadata_json={},
    )
    c_web = ExternalConnection(
        name="IB Flex Web",
        provider="IB",
        broker="IB",
        connector="IB_FLEX_WEB",
        taxpayer_entity_id=trust.id,
        status="ACTIVE",
        metadata_json={},
    )
    session.add_all([c_off, c_web])
    session.flush()

    # Same provider transaction imported twice (offline + web) into the same internal account.
    tx1 = Transaction(
        account_id=acct.id,
        date=dt.date(2025, 2, 4),
        type="TRANSFER",
        ticker="UNKNOWN",
        qty=None,
        amount=-20000.0,
        lot_links_json={"provider_txn_id": "31211414215", "provider_account_id": "IBFLEX:U5891158", "description": "DISBURSEMENT"},
    )
    tx2 = Transaction(
        account_id=acct.id,
        date=dt.date(2025, 2, 4),
        type="TRANSFER",
        ticker="UNKNOWN",
        qty=None,
        amount=-20000.0,
        lot_links_json={"provider_txn_id": "31211414215", "provider_account_id": "IBFLEX:U5891158", "description": "DISBURSEMENT"},
    )
    session.add_all([tx1, tx2])
    session.flush()
    session.add_all(
        [
            ExternalTransactionMap(connection_id=c_off.id, provider_txn_id="31211414215", transaction_id=tx1.id),
            ExternalTransactionMap(connection_id=c_web.id, provider_txn_id="31211414215", transaction_id=tx2.id),
        ]
    )
    session.commit()

    view = build_holdings_view(session, scope="trust", account_id=acct.id, today=dt.date(2025, 12, 21))
    assert view.ytd_withdrawals == 20000.0
    assert view.ytd_withdrawal_count == 1
    assert len(view.ytd_transfers) == 1


def test_holdings_view_combined_uses_position_lots_when_other_accounts_have_taxlots(session):
    """
    Regression: when one account has reconstructed TaxLots, the combined view must still show basis/P&L for
    another account that only has PositionLots (e.g., RJ), rather than blanking its Initial cost.
    """
    trust = TaxpayerEntity(name="Trust", type="TRUST")
    session.add(trust)
    session.flush()
    a_rj = Account(name="RJ Taxable", broker="RJ", account_type="TAXABLE", taxpayer_entity_id=trust.id)
    a_ib = Account(name="IB Taxable", broker="IB", account_type="TAXABLE", taxpayer_entity_id=trust.id)
    session.add_all([a_rj, a_ib])
    session.flush()

    # RJ has PositionLots only.
    session.add(
        PositionLot(
            account_id=a_rj.id,
            ticker="MU",
            qty=10.0,
            basis_total=1000.0,
            adjusted_basis_total=1000.0,
            acquisition_date=dt.date(2025, 1, 2),
        )
    )
    # IB has reconstructed TaxLots (preferred).
    sec = Security(ticker="AVGO", name="Broadcom", asset_class="EQUITY", metadata_json={})
    session.add(sec)
    session.flush()
    session.add(
        TaxLot(
            taxpayer_id=trust.id,
            account_id=a_ib.id,
            security_id=sec.id,
            source="RECONSTRUCTED",
            acquired_date=dt.date(2025, 1, 2),
            quantity_open=1.0,
            basis_open=200.0,
        )
    )

    # Minimal snapshots for both accounts so they show up as holdings rows.
    c_ib = ExternalConnection(name="IB Flex", provider="IB", broker="IB", connector="IB_FLEX_WEB", taxpayer_entity_id=trust.id, status="ACTIVE", metadata_json={})
    c_rj = ExternalConnection(name="RJ (Offline)", provider="RJ", broker="RJ", connector="RJ_OFFLINE", taxpayer_entity_id=trust.id, status="ACTIVE", metadata_json={})
    session.add_all([c_ib, c_rj])
    session.flush()
    session.add_all(
        [
            ExternalAccountMap(connection_id=c_ib.id, provider_account_id="IBFLEX:U1", account_id=a_ib.id),
            ExternalAccountMap(connection_id=c_rj.id, provider_account_id="RJ:TAXABLE", account_id=a_rj.id),
        ]
    )
    session.add_all(
        [
            ExternalHoldingSnapshot(connection_id=c_ib.id, as_of=dt.datetime(2026, 1, 7, 0, 0, tzinfo=dt.timezone.utc), payload_json={"items": [{"provider_account_id": "IBFLEX:U1", "symbol": "AVGO", "qty": 1.0, "market_value": 250.0}]}),
            ExternalHoldingSnapshot(connection_id=c_rj.id, as_of=dt.datetime(2026, 1, 7, 0, 0, tzinfo=dt.timezone.utc), payload_json={"items": [{"provider_account_id": "RJ:TAXABLE", "symbol": "MU", "qty": 10.0, "market_value": 1100.0}]}),
        ]
    )
    session.commit()

    view = build_holdings_view(session, scope="trust", account_id=None, today=dt.date(2026, 1, 7))
    mu = next(p for p in view.positions if p.symbol == "MU")
    assert float(mu.cost_basis_total or 0.0) == 1000.0
    assert mu.pnl_amount is not None


def test_holdings_view_wash_safe_exit_uses_recent_buys_and_substitute_group(session):
    trust = TaxpayerEntity(name="Trust", type="TRUST")
    session.add(trust)
    session.flush()
    acct = Account(name="U1", broker="IB", account_type="TAXABLE", taxpayer_entity_id=trust.id)
    session.add(acct)
    session.flush()
    conn = ExternalConnection(
        name="IB Flex",
        provider="IB",
        broker="IB",
        connector="IB_FLEX_OFFLINE",
        taxpayer_entity_id=trust.id,
        status="ACTIVE",
        metadata_json={},
    )
    session.add(conn)
    session.flush()
    session.add(ExternalAccountMap(connection_id=conn.id, provider_account_id="IBFLEX:U1", account_id=acct.id))
    # Substitute group: VTI and ITOT are substantially identical for MVP.
    session.add_all(
        [
            # Only substitute_group_id matters here.
            # Security rows are normally created by sync, but tests set explicitly.
        ]
    )
    session.flush()
    # Create securities with the same substitute group id.
    from src.db.models import Security

    session.add_all(
        [
            Security(ticker="VTI", name="VTI", asset_class="EQUITY", expense_ratio=0.0, substitute_group_id=1, metadata_json={}),
            Security(ticker="ITOT", name="ITOT", asset_class="EQUITY", expense_ratio=0.0, substitute_group_id=1, metadata_json={}),
        ]
    )
    session.flush()

    as_of = dt.datetime(2025, 12, 21, 15, 0, 0, tzinfo=dt.timezone.utc)
    session.add(
        ExternalHoldingSnapshot(
            connection_id=conn.id,
            as_of=as_of,
            payload_json={"items": [{"provider_account_id": "IBFLEX:U1", "symbol": "VTI", "qty": 1, "market_value": 100}]},
        )
    )
    # Recent BUY of ITOT on 2025-12-20 triggers wash window for VTI.
    session.add(
        Transaction(account_id=acct.id, date=dt.date(2025, 12, 20), type="BUY", ticker="ITOT", qty=1, amount=-100.0, lot_links_json={})
    )
    session.commit()

    view = build_holdings_view(session, scope="trust", account_id=acct.id, today=dt.date(2025, 12, 21))
    vti = next(p for p in view.positions if p.symbol == "VTI")
    assert vti.wash_safe_exit_date == dt.date(2026, 1, 20)


def test_holdings_view_pnl_and_tax_status_from_lots(session):
    trust = TaxpayerEntity(name="Trust", type="TRUST")
    session.add(trust)
    session.flush()
    acct = Account(name="U1", broker="IB", account_type="TAXABLE", taxpayer_entity_id=trust.id)
    session.add(acct)
    session.flush()
    conn = ExternalConnection(
        name="IB Flex",
        provider="IB",
        broker="IB",
        connector="IB_FLEX_OFFLINE",
        taxpayer_entity_id=trust.id,
        status="ACTIVE",
        metadata_json={},
    )
    session.add(conn)
    session.flush()
    session.add(ExternalAccountMap(connection_id=conn.id, provider_account_id="IBFLEX:U1", account_id=acct.id))

    # Snapshot shows market value but basis comes from lots.
    as_of = dt.datetime(2025, 12, 21, 15, 0, 0, tzinfo=dt.timezone.utc)
    session.add(
        ExternalHoldingSnapshot(
            connection_id=conn.id,
            as_of=as_of,
            payload_json={"items": [{"provider_account_id": "IBFLEX:U1", "symbol": "AAPL", "qty": 1, "market_value": 1200}]},
        )
    )
    from src.db.models import PositionLot

    session.add(
        PositionLot(
            account_id=acct.id,
            ticker="AAPL",
            acquisition_date=dt.date(2024, 1, 1),
            qty=1,
            basis_total=1000.0,
            adjusted_basis_total=None,
        )
    )
    session.commit()

    view = build_holdings_view(session, scope="trust", account_id=acct.id, today=dt.date(2025, 12, 21))
    aapl = next(p for p in view.positions if p.symbol == "AAPL")
    assert float(aapl.cost_basis_total or 0.0) == 1000.0
    assert float(aapl.pnl_amount or 0.0) == 200.0
    assert abs(float(aapl.pnl_pct or 0.0) - 0.2) < 1e-9
    assert aapl.tax_status == "LT"


def test_holdings_view_calendar_year_return_simple(session):
    trust = TaxpayerEntity(name="Trust", type="TRUST")
    session.add(trust)
    session.flush()
    acct = Account(name="U1", broker="IB", account_type="TAXABLE", taxpayer_entity_id=trust.id)
    session.add(acct)
    session.flush()
    conn = ExternalConnection(
        name="IB Flex",
        provider="IB",
        broker="IB",
        connector="IB_FLEX_OFFLINE",
        taxpayer_entity_id=trust.id,
        status="ACTIVE",
        metadata_json={},
    )
    session.add(conn)
    session.flush()
    session.add(ExternalAccountMap(connection_id=conn.id, provider_account_id="IBFLEX:U1", account_id=acct.id))

    # Baseline snapshot early in the year.
    session.add(
        ExternalHoldingSnapshot(
            connection_id=conn.id,
            as_of=dt.datetime(2025, 1, 2, 15, 0, 0, tzinfo=dt.timezone.utc),
            payload_json={"items": [{"provider_account_id": "IBFLEX:U1", "symbol": "AAPL", "qty": 1, "market_value": 1000}]},
        )
    )
    # Latest snapshot later in the year.
    session.add(
        ExternalHoldingSnapshot(
            connection_id=conn.id,
            as_of=dt.datetime(2025, 12, 21, 15, 0, 0, tzinfo=dt.timezone.utc),
            payload_json={"items": [{"provider_account_id": "IBFLEX:U1", "symbol": "AAPL", "qty": 1, "market_value": 1100}]},
        )
    )
    session.commit()

    view = build_holdings_view(session, scope="trust", account_id=acct.id, today=dt.date(2025, 12, 21))
    assert view.ytd_start_value == 1000.0
    assert view.ytd_return_pct is not None
    assert abs(float(view.ytd_return_pct) - 0.10) < 1e-9


def test_holdings_view_calendar_year_return_uses_gain_value_formula(session):
    trust = TaxpayerEntity(name="Trust", type="TRUST")
    session.add(trust)
    session.flush()
    acct = Account(name="U1", broker="IB", account_type="TAXABLE", taxpayer_entity_id=trust.id)
    session.add(acct)
    session.flush()
    conn = ExternalConnection(
        name="IB Flex",
        provider="IB",
        broker="IB",
        connector="IB_FLEX_OFFLINE",
        taxpayer_entity_id=trust.id,
        status="ACTIVE",
        metadata_json={},
    )
    session.add(conn)
    session.flush()
    session.add(ExternalAccountMap(connection_id=conn.id, provider_account_id="IBFLEX:U1", account_id=acct.id))

    # Baseline 1000, end 900; contribution +100; dividend +10; withdrawal 0.
    session.add(
        ExternalHoldingSnapshot(
            connection_id=conn.id,
            as_of=dt.datetime(2025, 1, 2, 15, 0, 0, tzinfo=dt.timezone.utc),
            payload_json={"items": [{"provider_account_id": "IBFLEX:U1", "symbol": "AAPL", "qty": 1, "market_value": 1000}]},
        )
    )
    session.add(
        ExternalHoldingSnapshot(
            connection_id=conn.id,
            as_of=dt.datetime(2025, 12, 21, 15, 0, 0, tzinfo=dt.timezone.utc),
            payload_json={"items": [{"provider_account_id": "IBFLEX:U1", "symbol": "AAPL", "qty": 1, "market_value": 900}]},
        )
    )
    session.add(Transaction(account_id=acct.id, date=dt.date(2025, 2, 1), type="TRANSFER", ticker=None, qty=None, amount=100.0, lot_links_json={}))
    session.add(Transaction(account_id=acct.id, date=dt.date(2025, 3, 1), type="DIV", ticker="AAPL", qty=None, amount=10.0, lot_links_json={}))
    session.commit()

    view = build_holdings_view(session, scope="trust", account_id=acct.id, today=dt.date(2025, 12, 21))
    assert view.ytd_start_value == 1000.0
    assert view.ytd_gain_value is not None
    # GainValue = (900-1000) + 0 + 10 - 100 = -190
    assert abs(float(view.ytd_gain_value) - (-190.0)) < 1e-9
    assert view.ytd_return_pct is not None
    assert abs(float(view.ytd_return_pct) - (-0.19)) < 1e-9


def test_holdings_view_cashflows_show_even_without_year_start_baseline(session):
    trust = TaxpayerEntity(name="Trust", type="TRUST")
    session.add(trust)
    session.flush()
    acct = Account(name="U1", broker="IB", account_type="TAXABLE", taxpayer_entity_id=trust.id)
    session.add(acct)
    session.flush()
    conn = ExternalConnection(
        name="IB Flex",
        provider="IB",
        broker="IB",
        connector="IB_FLEX_OFFLINE",
        taxpayer_entity_id=trust.id,
        status="ACTIVE",
        metadata_json={},
    )
    session.add(conn)
    session.flush()
    session.add(ExternalAccountMap(connection_id=conn.id, provider_account_id="IBFLEX:U1", account_id=acct.id))

    # Only a late-year snapshot; no Jan baseline available.
    session.add(
        ExternalHoldingSnapshot(
            connection_id=conn.id,
            as_of=dt.datetime(2025, 12, 21, 15, 0, 0, tzinfo=dt.timezone.utc),
            payload_json={"items": [{"provider_account_id": "IBFLEX:U1", "symbol": "AAPL", "qty": 1, "market_value": 1100}]},
        )
    )
    session.add(Transaction(account_id=acct.id, date=dt.date(2025, 2, 1), type="TRANSFER", ticker=None, qty=None, amount=100.0, lot_links_json={}))
    session.add(Transaction(account_id=acct.id, date=dt.date(2025, 3, 1), type="DIV", ticker="AAPL", qty=None, amount=10.0, lot_links_json={}))
    session.commit()

    view = build_holdings_view(session, scope="trust", account_id=acct.id, today=dt.date(2025, 12, 21))
    assert view.ytd_start_value is None
    assert view.ytd_contributions == 100.0
    assert view.ytd_dividends_received == 10.0
    assert view.ytd_deposit_count == 1
    assert view.ytd_withdrawal_count == 0
    assert len(view.ytd_transfers) == 1
    # Still provides a planning P&L and return-on-cost when cost basis is unknown.
    assert view.pnl_planning_value == 10.0
    assert view.pnl_return_on_cost is None
    assert any("calendar-year return" in w.lower() and "estimate" in w.lower() for w in view.warnings)


def test_holdings_view_gain_return_fallback_uses_cost_plus_cash_when_no_jan_snapshot(session):
    trust = TaxpayerEntity(name="Trust", type="TRUST")
    session.add(trust)
    session.flush()
    acct = Account(name="U1", broker="IB", account_type="TAXABLE", taxpayer_entity_id=trust.id)
    session.add(acct)
    session.flush()
    conn = ExternalConnection(
        name="IB Flex",
        provider="IB",
        broker="IB",
        connector="IB_FLEX_OFFLINE",
        taxpayer_entity_id=trust.id,
        status="ACTIVE",
        metadata_json={},
    )
    session.add(conn)
    session.flush()
    session.add(ExternalAccountMap(connection_id=conn.id, provider_account_id="IBFLEX:U1", account_id=acct.id))

    # Only late-year holdings snapshot.
    session.add(
        ExternalHoldingSnapshot(
            connection_id=conn.id,
            as_of=dt.datetime(2025, 12, 21, 15, 0, 0, tzinfo=dt.timezone.utc),
            payload_json={"items": [{"provider_account_id": "IBFLEX:U1", "symbol": "AAPL", "qty": 1, "market_value": 1100}]},
        )
    )
    # Provide cash and cost basis so begin estimate is available.
    session.add(CashBalance(account_id=acct.id, as_of_date=dt.date(2025, 12, 21), amount=100.0))
    from src.db.models import PositionLot

    session.add(PositionLot(account_id=acct.id, ticker="AAPL", acquisition_date=dt.date(2024, 1, 1), qty=1, basis_total=1000.0))
    # One withdrawal during the year.
    session.add(Transaction(account_id=acct.id, date=dt.date(2025, 2, 1), type="TRANSFER", ticker=None, qty=None, amount=-100.0, lot_links_json={}))
    session.commit()

    view = build_holdings_view(session, scope="trust", account_id=acct.id, today=dt.date(2025, 12, 21))
    assert view.ytd_start_value is None
    assert view.gain_begin_value is not None
    assert abs(float(view.gain_begin_value) - 1100.0) < 1e-9  # cost 1000 + cash 100
    assert view.ytd_gain_value is not None
    # Gain = (End-Begin) + Withdrawals - Contributions (no income); End=1200, Begin=1100, Withdrawals=100 => 200
    assert abs(float(view.ytd_gain_value) - 200.0) < 1e-9
    assert view.ytd_return_pct is not None
    assert abs(float(view.ytd_return_pct) - (200.0 / 1100.0)) < 1e-9
