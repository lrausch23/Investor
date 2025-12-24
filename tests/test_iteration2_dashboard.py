from __future__ import annotations

import datetime as dt

from src.core.analytics import wash_risk_summary
from src.core.policy_engine import compute_drift_report, create_policy_version
from src.core.preview import planner_preview
from src.core.tax_engine import TaxAssumptions, estimate_federal_tax_ytd, net_tax_due, tax_summary_ytd_with_net
from src.db.models import (
    Account,
    BucketAssignment,
    CashBalance,
    PositionLot,
    Security,
    TaxpayerEntity,
    Transaction,
)


def test_b4_structural_under_allocation_is_red(session):
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
            ("B1", "Liquidity", 0.0, 0.10, 1.0, ["CASH"], {}),
            ("B2", "Defensive", 0.0, 0.30, 1.0, ["BOND"], {}),
            ("B3", "Growth", 0.0, 0.45, 1.0, ["EQUITY"], {}),
            ("B4", "Alpha", 0.0, 0.15, 0.25, ["ALPHA"], {}),
        ],
    )
    session.flush()

    session.add(CashBalance(account_id=acct.id, as_of_date=dt.date(2025, 12, 1), amount=1000))
    session.commit()

    report = compute_drift_report(session=session, policy_id=policy.id, scope="TRUST")
    b4 = [r for r in report.bucket_rows if r.code == "B4"][0]
    assert b4.actual_pct == 0.0
    assert b4.traffic_light == "RED"
    assert b4.reason == "Structural under-allocation"


def test_wash_risk_empty_state_no_loss_sales(session):
    trust = TaxpayerEntity(name="Trust", type="TRUST")
    session.add(trust)
    session.flush()
    acct = Account(name="Trust Taxable", broker="IB", account_type="TAXABLE", taxpayer_entity_id=trust.id)
    session.add(acct)
    session.commit()

    summary = wash_risk_summary(session=session, as_of=dt.date(2025, 12, 20), scope="trust", lookback_days=30)
    assert summary.recent_loss_sale_count == 0
    assert summary.missing_basis_count == 0
    assert "no recent loss sales" in summary.message.lower()


def test_wash_risk_empty_state_missing_basis(session):
    trust = TaxpayerEntity(name="Trust", type="TRUST")
    session.add(trust)
    session.flush()
    acct = Account(name="Trust Taxable", broker="IB", account_type="TAXABLE", taxpayer_entity_id=trust.id)
    session.add(acct)
    session.flush()
    session.add(Transaction(account_id=acct.id, date=dt.date(2025, 12, 10), type="SELL", ticker="AAA", qty=1, amount=100, lot_links_json={}))
    session.commit()

    summary = wash_risk_summary(session=session, as_of=dt.date(2025, 12, 20), scope="trust", lookback_days=30)
    assert summary.missing_basis_count == 1
    assert summary.recent_loss_sale_count == 0
    msg = summary.message.lower()
    assert "reconstructed wash-risk" in msg
    assert "missing internal lot basis" in msg
    assert "broker" in msg


def test_wash_risk_none_flagged_when_no_buy(session):
    trust = TaxpayerEntity(name="Trust", type="TRUST")
    session.add(trust)
    session.flush()
    acct = Account(name="Trust Taxable", broker="IB", account_type="TAXABLE", taxpayer_entity_id=trust.id)
    session.add(acct)
    session.flush()
    session.add(Security(ticker="AAA", name="AAA", asset_class="EQUITY", expense_ratio=0.0, substitute_group_id=None, metadata_json={"last_price": 100}))
    session.add(Transaction(account_id=acct.id, date=dt.date(2025, 12, 10), type="SELL", ticker="AAA", qty=1, amount=80, lot_links_json={"basis_total": 100, "term": "LT"}))
    session.commit()

    summary = wash_risk_summary(session=session, as_of=dt.date(2025, 12, 20), scope="trust", lookback_days=30)
    assert summary.recent_loss_sale_count == 1
    assert summary.flagged_count == 0
    assert "no wash-risk detected" in summary.message.lower()


def test_wash_risk_flagged_when_buy_within_window(session):
    trust = TaxpayerEntity(name="Trust", type="TRUST")
    session.add(trust)
    session.flush()
    acct = Account(name="Trust Taxable", broker="IB", account_type="TAXABLE", taxpayer_entity_id=trust.id)
    session.add(acct)
    session.flush()
    session.add(Security(ticker="AAA", name="AAA", asset_class="EQUITY", expense_ratio=0.0, substitute_group_id=None, metadata_json={"last_price": 100}))
    session.add(Transaction(account_id=acct.id, date=dt.date(2025, 12, 5), type="BUY", ticker="AAA", qty=1, amount=-100, lot_links_json={}))
    session.add(Transaction(account_id=acct.id, date=dt.date(2025, 12, 10), type="SELL", ticker="AAA", qty=1, amount=80, lot_links_json={"basis_total": 100, "term": "LT"}))
    session.commit()

    summary = wash_risk_summary(session=session, as_of=dt.date(2025, 12, 20), scope="trust", lookback_days=30)
    assert summary.recent_loss_sale_count == 1
    assert summary.flagged_count == 1
    assert summary.items


def test_planner_preview_deltas_and_st_line(session):
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
            ("B1", "Liquidity", 0.0, 0.10, 1.0, ["CASH"], {}),
            ("B2", "Defensive", 0.0, 0.30, 1.0, ["BOND"], {}),
            ("B3", "Growth", 0.0, 0.60, 1.0, ["EQUITY"], {}),
            ("B4", "Alpha", 0.0, 0.0, 1.0, ["ALPHA"], {}),
        ],
    )
    session.flush()

    session.add(Security(ticker="BND", name="BND", asset_class="BOND", expense_ratio=0.0, substitute_group_id=None, metadata_json={"last_price": 100}))
    session.add(Security(ticker="VTI", name="VTI", asset_class="EQUITY", expense_ratio=0.0, substitute_group_id=None, metadata_json={"last_price": 100}))
    session.add(BucketAssignment(policy_id=policy.id, ticker="BND", bucket_code="B2"))
    session.add(BucketAssignment(policy_id=policy.id, ticker="VTI", bucket_code="B3"))
    session.add(PositionLot(account_id=acct.id, ticker="BND", acquisition_date=dt.date(2020, 1, 1), qty=3, basis_total=300))
    session.add(PositionLot(account_id=acct.id, ticker="VTI", acquisition_date=dt.date(2020, 1, 1), qty=5, basis_total=500))
    session.add(CashBalance(account_id=acct.id, as_of_date=dt.date(2025, 12, 1), amount=200))
    session.commit()

    prev = planner_preview(session=session, policy_id=policy.id, scope="TRUST")
    assert round(prev.total_value, 6) == 1000.0
    assert round(prev.b1_excess, 6) == 100.0  # target 100, current 200
    assert prev.st_sensitivity == "ST-sale avoidance: OK"


def test_net_tax_due_computation_simple():
    a = TaxAssumptions(ordinary_rate=0.4, ltcg_rate=0.2, niit_enabled=False, niit_rate=0.0, qualified_dividend_pct=0.0)
    est = estimate_federal_tax_ytd(st_gains=100, lt_gains=200, interest=0, dividends=0, assumptions=a)
    assert est == 0.4 * 100 + 0.2 * 200
    assert net_tax_due(estimated_tax=est, withholding=50) == est - 50


def test_tax_net_due_ira_is_na(session):
    personal = TaxpayerEntity(name="Personal", type="PERSONAL")
    session.add(personal)
    session.flush()
    ira = Account(name="Chase IRA", broker="CHASE", account_type="IRA", taxpayer_entity_id=personal.id)
    session.add(ira)
    session.flush()
    session.add(Transaction(account_id=ira.id, date=dt.date(2025, 12, 1), type="WITHHOLDING", ticker=None, qty=None, amount=100, lot_links_json={}))
    session.commit()

    summary = tax_summary_ytd_with_net(session=session, as_of=dt.date(2025, 12, 20), scope="personal", assumptions=TaxAssumptions())
    assert len(summary.rows) == 1
    row = summary.rows[0]
    assert row.estimated_tax is None
    assert row.net_tax_due is None
    assert row.withholding == 100.0
