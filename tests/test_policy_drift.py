from __future__ import annotations

import datetime as dt

from src.core.policy_engine import compute_drift_report, create_policy_version
from src.db.models import Account, BucketAssignment, CashBalance, PositionLot, Security, TaxpayerEntity


def test_drift_includes_cash_in_b1(session):
    tp = TaxpayerEntity(name="Trust", type="TRUST")
    session.add(tp)
    session.flush()
    acct = Account(name="IB Taxable", broker="IB", account_type="TAXABLE", taxpayer_entity_id=tp.id)
    session.add(acct)
    session.flush()

    policy = create_policy_version(
        session=session,
        name="P",
        effective_date=dt.date(2025, 1, 1),
        json_definition={},
        buckets=[
            ("B1", "Liquidity", 0.0, 0.5, 1.0, ["CASH"], {}),
            ("B2", "Defensive", 0.0, 0.0, 1.0, ["BOND"], {}),
            ("B3", "Growth", 0.0, 0.5, 1.0, ["EQUITY"], {}),
            ("B4", "Alpha", 0.0, 0.0, 1.0, ["ALPHA"], {}),
        ],
    )
    session.flush()

    session.add(Security(ticker="AAA", name="AAA", asset_class="EQUITY", expense_ratio=0.0, substitute_group_id=None, metadata_json={"last_price": 100.0}))
    session.add(BucketAssignment(policy_id=policy.id, ticker="AAA", bucket_code="B3"))
    session.add(PositionLot(account_id=acct.id, ticker="AAA", acquisition_date=dt.date(2020, 1, 1), qty=1, basis_total=50))
    session.add(CashBalance(account_id=acct.id, as_of_date=dt.date(2025, 12, 1), amount=100))
    session.commit()

    report = compute_drift_report(session=session, policy_id=policy.id, scope="TRUST")
    b1 = [r for r in report.bucket_rows if r.code == "B1"][0]
    b3 = [r for r in report.bucket_rows if r.code == "B3"][0]
    assert b1.value == 100.0
    assert b3.value == 100.0
    assert report.total_value == 200.0

