from __future__ import annotations

import datetime as dt

from src.core.native_snapshot import build_native_snapshot
from src.core.policy_engine import create_policy_version
from src.db.models import (
    Account,
    BucketAssignment,
    CashBalance,
    ExternalAccountMap,
    ExternalConnection,
    ExternalHoldingSnapshot,
    PositionLot,
    Security,
    TaxpayerEntity,
)


def test_native_snapshot_basic_shape_with_policy(session):
    trust = TaxpayerEntity(name="Trust", type="TRUST")
    session.add(trust)
    session.flush()

    acct = Account(name="IB Trust", broker="IB", account_type="TAXABLE", taxpayer_entity_id=trust.id)
    session.add(acct)
    session.flush()

    policy = create_policy_version(
        session=session,
        name="P1",
        effective_date=dt.date(2025, 1, 1),
        json_definition={},
        buckets=[
            ("B1", "Liquidity", 0.10, 0.18, 0.24, ["CASH"], {}),
            ("B2", "Defensive", 0.18, 0.24, 0.30, ["BOND"], {}),
            ("B3", "Growth", 0.30, 0.38, 0.46, ["EQUITY"], {}),
            ("B4", "Alpha", 0.06, 0.10, 0.12, ["ALPHA"], {}),
        ],
    )
    session.flush()

    session.add(Security(ticker="AAA", name="AAA", asset_class="EQUITY", expense_ratio=0.001, metadata_json={"last_price": 120}))
    session.add(BucketAssignment(policy_id=policy.id, ticker="AAA", bucket_code="B3"))
    conn = ExternalConnection(
        name="IB Native",
        provider="IB",
        broker="IB",
        connector="IB_FLEX_WEB",
        taxpayer_entity_id=trust.id,
        status="ACTIVE",
        metadata_json={},
    )
    session.add(conn)
    session.flush()
    session.add(ExternalAccountMap(connection_id=conn.id, provider_account_id="U1", account_id=acct.id))
    session.add(
        ExternalHoldingSnapshot(
            connection_id=conn.id,
            as_of=dt.datetime(2026, 2, 7, 12, 0, tzinfo=dt.timezone.utc),
            payload_json={
                "items": [
                    {
                        "provider_account_id": "U1",
                        "symbol": "AAA",
                        "qty": 10,
                        "market_value": 1200,
                        "cost_basis_total": 1000,
                    }
                ]
            },
        )
    )
    session.add(PositionLot(account_id=acct.id, ticker="AAA", acquisition_date=dt.date(2024, 1, 2), qty=10, basis_total=1000))
    session.add(CashBalance(account_id=acct.id, as_of_date=dt.date(2026, 2, 1), amount=2500))
    session.commit()

    snapshot = build_native_snapshot(session, scope="trust", as_of=dt.date(2026, 2, 7))

    assert snapshot["version"] == 1
    assert snapshot["scope"] == "trust"
    assert isinstance(snapshot["kpis"]["total_value"], float)
    assert len(snapshot["buckets"]) == 4
    assert any(h["symbol"] == "AAA" for h in snapshot["holdings"])
    aaa = next(h for h in snapshot["holdings"] if h["symbol"] == "AAA")
    assert aaa["bucket"] == "B3"
    assert aaa["account_id"] == acct.id


def test_native_snapshot_fallback_buckets_without_policy(session):
    trust = TaxpayerEntity(name="Trust", type="TRUST")
    session.add(trust)
    session.flush()

    acct = Account(name="No Policy Acct", broker="IB", account_type="TAXABLE", taxpayer_entity_id=trust.id)
    session.add(acct)
    session.flush()

    session.add(Security(ticker="BBB", name="BBB", asset_class="EQUITY", expense_ratio=0.0, metadata_json={"last_price": 50}))
    session.add(PositionLot(account_id=acct.id, ticker="BBB", acquisition_date=dt.date(2025, 1, 1), qty=20, basis_total=900))
    session.commit()

    snapshot = build_native_snapshot(session, scope="trust", as_of=dt.date(2026, 2, 7))

    assert len(snapshot["buckets"]) == 4
    assert {row["code"] for row in snapshot["buckets"]} == {"B1", "B2", "B3", "B4"}
