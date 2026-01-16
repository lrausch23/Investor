from __future__ import annotations

from src.core.cashflow_supplement import import_supplemental_cashflows
from src.db.models import Account, ExternalAccountMap, ExternalConnection, TaxpayerEntity, Transaction, ExternalTransactionMap


def test_import_supplemental_cashflows_basic(session):
    tp = TaxpayerEntity(name="Kolozsi LLC", type="TRUST")
    session.add(tp)
    session.flush()

    acct = Account(
        name="Chase — IRA Account ****8839",
        broker="CHASE",
        account_type="IRA",
        taxpayer_entity_id=tp.id,
    )
    conn = ExternalConnection(
        name="Chase (Automated)",
        provider="PLAID",
        broker="CHASE",
        connector="CHASE_PLAID",
        taxpayer_entity_id=tp.id,
        metadata_json={"plaid_enable_investments": True},
    )
    session.add_all([acct, conn])
    session.flush()

    session.add(
        ExternalAccountMap(
            connection_id=conn.id,
            provider_account_id="PLAID:ITEM:ACCT1",
            account_id=acct.id,
        )
    )
    session.commit()

    csv_text = (
        "Date,Type,Amount,Description,AccountLast4\n"
        "2026-01-02,Withdrawal,4500,Transfer to checking,8839\n"
        "2026-01-02,Tax,500,IRA withholding,8839\n"
    )

    stats = import_supplemental_cashflows(
        session,
        connection=conn,
        file_name="supplemental.csv",
        file_bytes=csv_text.encode("utf-8"),
        stored_path=None,
        actor="tester",
    )
    assert stats["inserted"] == 2
    assert stats["purged_manual"] == 0
    txns = session.query(Transaction).order_by(Transaction.id.asc()).all()
    assert len(txns) == 2
    assert txns[0].type == "TRANSFER"
    assert float(txns[0].amount) == -4500.0
    assert txns[1].type == "WITHHOLDING"
    assert float(txns[1].amount) == 500.0
    assert session.query(ExternalTransactionMap).count() == 2

    # Re-import same file should be skipped by hash.
    stats2 = import_supplemental_cashflows(
        session,
        connection=conn,
        file_name="supplemental.csv",
        file_bytes=csv_text.encode("utf-8"),
        stored_path=None,
        actor="tester",
    )
    assert stats2["skipped"] is True
