from __future__ import annotations

import datetime as dt

from src.core.connection_preference import preferred_active_connection_ids_for_scope
from src.db.models import Account, ExternalConnection, ExternalTransactionMap, TaxpayerEntity, Transaction


def test_cash_out_prefers_ib_web_over_ib_offline(session):
    tp = TaxpayerEntity(name="Trust", type="TRUST")
    session.add(tp)
    session.flush()

    acct = Account(name="IB Taxable", broker="IB", taxpayer_entity_id=tp.id, account_type="TAXABLE")
    session.add(acct)
    session.flush()

    conn_off = ExternalConnection(
        name="IB Flex (Offline)",
        provider="IB",
        broker="IB",
        connector="IB_FLEX_OFFLINE",
        taxpayer_entity_id=tp.id,
        status="ACTIVE",
        metadata_json={},
    )
    conn_web = ExternalConnection(
        name="IB Flex (Web)",
        provider="IB",
        broker="IB",
        connector="IB_FLEX_WEB",
        taxpayer_entity_id=tp.id,
        status="ACTIVE",
        metadata_json={},
    )
    session.add_all([conn_off, conn_web])
    session.flush()

    # Same provider txn id imported twice via two connectors.
    t1 = Transaction(
        account_id=acct.id,
        date=dt.date(2025, 1, 7),
        type="TRANSFER",
        ticker=None,
        qty=None,
        amount=-20000.0,
        lot_links_json={"provider_account_id": "IBFLEX:U1", "provider_txn_id": "30784504547", "description": "DISBURSEMENT"},
    )
    t2 = Transaction(
        account_id=acct.id,
        date=dt.date(2025, 1, 7),
        type="TRANSFER",
        ticker=None,
        qty=None,
        amount=-20000.0,
        lot_links_json={"provider_account_id": "IBFLEX:U1", "provider_txn_id": "30784504547", "description": "DISBURSEMENT"},
    )
    session.add_all([t1, t2])
    session.flush()
    session.add_all(
        [
            ExternalTransactionMap(connection_id=conn_off.id, provider_txn_id="30784504547", transaction_id=t1.id),
            ExternalTransactionMap(connection_id=conn_web.id, provider_txn_id="30784504547", transaction_id=t2.id),
        ]
    )
    session.commit()

    preferred = preferred_active_connection_ids_for_scope(session, scope="trust")
    assert int(conn_web.id) in preferred
    assert int(conn_off.id) not in preferred

