from __future__ import annotations

import datetime as dt

from src.core.tax_documents import aggregate_tax_doc_overrides
from src.core.taxes import build_tax_dashboard, compute_se_tax
from src.db.models import (
    Account,
    BrokerLotClosure,
    ExternalConnection,
    HouseholdEntity,
    TaxInput,
    TaxDocument,
    TaxFact,
    TaxProfile,
    TaxTag,
    TaxpayerEntity,
    Transaction,
)


def _mk_personal(session):
    tp = TaxpayerEntity(name="Personal", type="PERSONAL")
    session.add(tp)
    session.flush()
    return tp


def test_tax_dashboard_ira_distribution(session):
    tp = _mk_personal(session)
    acct = Account(name="IRA-1", broker="IB", account_type="IRA", taxpayer_entity_id=tp.id)
    session.add(acct)
    session.flush()
    tx = Transaction(account_id=acct.id, date=dt.date(2025, 7, 1), type="TRANSFER", amount=5000.0)
    session.add(tx)
    session.flush()
    session.add(TaxTag(transaction_id=tx.id, category="IRA_DISTRIBUTION"))
    session.commit()

    dash = build_tax_dashboard(session, year=2025, as_of=dt.date(2025, 7, 15))
    assert round(dash.summary["ordinary_breakdown"]["ira_distributions"], 2) == 5000.0


def test_tax_dashboard_capital_gains(session):
    tp = _mk_personal(session)
    conn = ExternalConnection(
        name="IB Flex Web",
        provider="IB",
        broker="IB",
        connector="IB_FLEX_WEB",
        taxpayer_entity_id=tp.id,
        status="ACTIVE",
        metadata_json={},
    )
    session.add(conn)
    session.flush()
    session.add(
        BrokerLotClosure(
            connection_id=conn.id,
            provider_account_id="IBFLEX:ACCT1",
            symbol="AAA",
            trade_date=dt.date(2025, 7, 10),
            open_datetime_raw="20250601;093000",
            quantity_closed=10,
            cost_basis=1000.0,
            realized_pl_fifo=100.0,
            proceeds_derived=1100.0,
            source_file_hash="hash1",
        )
    )
    session.add(
        BrokerLotClosure(
            connection_id=conn.id,
            provider_account_id="IBFLEX:ACCT1",
            symbol="BBB",
            trade_date=dt.date(2025, 7, 10),
            open_datetime_raw="20230101;093000",
            quantity_closed=5,
            cost_basis=500.0,
            realized_pl_fifo=200.0,
            proceeds_derived=700.0,
            source_file_hash="hash2",
        )
    )
    session.commit()

    dash = build_tax_dashboard(session, year=2025, as_of=dt.date(2025, 7, 31))
    assert round(dash.summary["capital_gains"]["st"], 2) == 100.0
    assert round(dash.summary["capital_gains"]["lt"], 2) == 200.0


def test_se_tax_cap_behavior():
    params = {"se_tax": {"ss_rate": 0.124, "medicare_rate": 0.029, "additional_medicare_rate": 0.0, "ss_wage_base": 100}}
    se_tax, se_deduction = compute_se_tax(1000.0, params, "MFJ")
    assert round(se_tax, 4) == round(12.4 + (1000.0 * 0.9235 * 0.029), 4)
    assert round(se_deduction, 4) == round(se_tax * 0.5, 4)


def test_paid_vs_remaining_due(session):
    tp = _mk_personal(session)
    profile = TaxProfile(year=2025, filing_status="MFJ", deductions_mode="itemized", itemized_amount=0.0, household_size=3, dependents_count=1, trust_income_taxable_to_user=True)
    inputs = TaxInput(
        year=2025,
        data_json={
            "yoga_net_profit_monthly": [10000.0] + [0.0] * 11,
            "daughter_w2_withholding_monthly": [500.0] + [0.0] * 11,
            "estimated_payments": [],
            "state_tax_rate": 0.0,
            "qualified_dividend_pct": 0.0,
        },
    )
    session.add_all([profile, inputs])
    session.commit()

    dash = build_tax_dashboard(session, year=2025, as_of=dt.date(2025, 1, 31))
    total_tax = dash.summary["total_tax"]
    paid = dash.summary["paid_ytd"]
    assert round(dash.summary["remaining_due"], 2) == round(total_tax - paid, 2)


def test_safe_harbor_flag(session):
    tp = _mk_personal(session)
    profile = TaxProfile(year=2025, filing_status="MFJ", deductions_mode="itemized", itemized_amount=0.0, household_size=3, dependents_count=1, trust_income_taxable_to_user=True)
    inputs = TaxInput(
        year=2025,
        data_json={
            "last_year_total_tax": 12000.0,
            "safe_harbor_multiplier": 1.0,
            "yoga_net_profit_monthly": [0.0] * 12,
            "daughter_w2_withholding_monthly": [0.0] * 12,
        },
    )
    session.add_all([profile, inputs])
    session.commit()

    dash = build_tax_dashboard(session, year=2025, as_of=dt.date(2025, 1, 31))
    jan_flags = dash.monthly[0]["flags"]
    assert "behind safe harbor" in jan_flags


def test_trust_transactions_respect_cutoff(session):
    trust = TaxpayerEntity(name="Trust", type="TRUST")
    session.add(trust)
    session.flush()
    acct = Account(name="Trust-IB", broker="IB", account_type="TAXABLE", taxpayer_entity_id=trust.id)
    session.add(acct)
    session.flush()

    before = Transaction(account_id=acct.id, date=dt.date(2025, 6, 1), type="TRANSFER", amount=1000.0)
    after = Transaction(account_id=acct.id, date=dt.date(2025, 6, 10), type="TRANSFER", amount=2000.0)
    session.add_all([before, after])
    session.flush()
    session.add(TaxTag(transaction_id=before.id, category="TRUST_DISTRIBUTION"))
    session.add(TaxTag(transaction_id=after.id, category="TRUST_DISTRIBUTION"))
    session.commit()

    from src.core.taxes import tax_account_summaries

    summaries = tax_account_summaries(session, year=2025)
    trust_rows = [r for r in summaries if r.get("account_name") == "Trust-IB"]
    assert trust_rows
    assert round(float(trust_rows[0].get("trust_distributions") or 0.0), 2) == 2000.0


def test_auto_tag_ira_withholding(session):
    tp = _mk_personal(session)
    acct = Account(name="IRA-1", broker="IB", account_type="IRA", taxpayer_entity_id=tp.id)
    session.add(acct)
    session.flush()
    tx = Transaction(
        account_id=acct.id,
        date=dt.date(2025, 2, 1),
        type="WITHHOLDING",
        amount=150.0,
        lot_links_json={"description": "IRA WITHHOLDING TAX FEDERAL W/H"},
    )
    session.add(tx)
    session.commit()

    from src.core.taxes import auto_tag_tax_transactions

    auto_tag_tax_transactions(session, year=2025)
    tag = session.query(TaxTag).filter(TaxTag.transaction_id == tx.id).one_or_none()
    assert tag is not None
    assert tag.category == "IRA_WITHHOLDING"


def test_auto_tag_ira_dividend_withholding_excluded(session):
    tp = _mk_personal(session)
    acct = Account(name="IRA-1", broker="IB", account_type="IRA", taxpayer_entity_id=tp.id)
    session.add(acct)
    session.flush()
    tx = Transaction(
        account_id=acct.id,
        date=dt.date(2025, 4, 10),
        type="WITHHOLDING",
        amount=133.84,
        lot_links_json={"description": "CASH DIV ON 250 SHS PAY 04/10/25 FOREIGN TAX WITHHELD"},
    )
    session.add(tx)
    session.commit()

    from src.core.taxes import auto_tag_tax_transactions

    auto_tag_tax_transactions(session, year=2025)
    tag = session.query(TaxTag).filter(TaxTag.transaction_id == tx.id).one_or_none()
    assert tag is None


def test_other_withholding_from_taxable_accounts(session):
    tp = _mk_personal(session)
    acct = Account(name="Taxable-1", broker="IB", account_type="TAXABLE", taxpayer_entity_id=tp.id)
    session.add(acct)
    session.flush()
    tx = Transaction(account_id=acct.id, date=dt.date(2025, 3, 1), type="WITHHOLDING", amount=42.0)
    session.add(tx)
    session.commit()

    dash = build_tax_dashboard(session, year=2025, as_of=dt.date(2025, 12, 31))
    assert round(dash.summary["other_withholding_ytd"], 2) == 42.0


def test_trust_fees_reduce_passthrough(session):
    tp = TaxpayerEntity(name="Trust", type="TRUST")
    session.add(tp)
    session.flush()
    profile = TaxProfile(year=2025, filing_status="MFJ", deductions_mode="itemized", itemized_amount=0.0, household_size=3, dependents_count=1, trust_income_taxable_to_user=True)
    inputs = TaxInput(
        year=2025,
        data_json={
            "trust_passthrough_monthly": [1000.0] + [0.0] * 11,
            "trust_fees_monthly": [200.0] + [0.0] * 11,
        },
    )
    session.add_all([profile, inputs])
    session.commit()

    dash = build_tax_dashboard(session, year=2025, as_of=dt.date(2025, 12, 31))
    assert round(dash.summary["ordinary_breakdown"]["trust_passthrough_gross"], 2) == 1000.0
    assert round(dash.summary["ordinary_breakdown"]["trust_fees"], 2) == 200.0
    assert round(dash.summary["ordinary_breakdown"]["trust_passthrough"], 2) == 800.0


def test_tax_doc_precedence_manual_overrides(session):
    profile = TaxProfile(year=2025, filing_status="MFJ", deductions_mode="standard", itemized_amount=None, household_size=3, dependents_count=1, trust_income_taxable_to_user=True)
    inputs = TaxInput(
        year=2025,
        data_json={
            "daughter_w2_wages_monthly": [100.0] * 12,
            "tax_doc_overrides": {"w2_wages_total": 24000.0, "sources": {"WAGES": [1]}},
            "docs_primary": True,
            "tax_manual_overrides": {"w2_wages_total": 6000.0},
        },
    )
    session.add_all([profile, inputs])
    session.commit()

    dash = build_tax_dashboard(session, year=2025, as_of=dt.date(2025, 12, 31))
    assert round(dash.summary["ordinary_breakdown"]["w2_wages"], 2) == 6000.0
    assert dash.summary["sources"]["w2_wages_total"] == "manual"


def test_tax_doc_precedence_docs_over_investor(session):
    profile = TaxProfile(year=2025, filing_status="MFJ", deductions_mode="standard", itemized_amount=None, household_size=3, dependents_count=1, trust_income_taxable_to_user=True)
    inputs = TaxInput(
        year=2025,
        data_json={
            "daughter_w2_wages_monthly": [100.0] * 12,
            "tax_doc_overrides": {"w2_wages_total": 12000.0, "sources": {"WAGES": [1]}},
            "docs_primary": True,
        },
    )
    session.add_all([profile, inputs])
    session.commit()

    dash = build_tax_dashboard(session, year=2025, as_of=dt.date(2025, 12, 31))
    assert round(dash.summary["ordinary_breakdown"]["w2_wages"], 2) == 12000.0
    assert dash.summary["sources"]["w2_wages_total"] == "docs"


def test_tax_doc_precedence_investor_when_docs_disabled(session):
    profile = TaxProfile(year=2025, filing_status="MFJ", deductions_mode="standard", itemized_amount=None, household_size=3, dependents_count=1, trust_income_taxable_to_user=True)
    inputs = TaxInput(
        year=2025,
        data_json={
            "daughter_w2_wages_monthly": [100.0] * 12,
            "tax_doc_overrides": {"w2_wages_total": 12000.0, "sources": {"WAGES": [1]}},
            "docs_primary": False,
        },
    )
    session.add_all([profile, inputs])
    session.commit()

    dash = build_tax_dashboard(session, year=2025, as_of=dt.date(2025, 12, 31))
    assert round(dash.summary["ordinary_breakdown"]["w2_wages"], 2) == 1200.0
    assert dash.summary["sources"]["w2_wages_total"] == "investor"


def test_tax_doc_entity_totals(session):
    entity = HouseholdEntity(tax_year=2025, entity_type="USER", display_name="Me")
    session.add(entity)
    session.flush()
    doc = TaxDocument(
        tax_year=2025,
        doc_type="W2",
        filename="w2.pdf",
        sha256="hash",
        status="CONFIRMED",
        owner_entity_id=entity.id,
    )
    session.add(doc)
    session.flush()
    fact = TaxFact(
        tax_year=2025,
        source_doc_id=doc.id,
        fact_type="WAGES",
        amount=1000.0,
        user_confirmed=True,
        owner_entity_id=entity.id,
        metadata_json={},
    )
    session.add(fact)
    session.commit()

    overrides = aggregate_tax_doc_overrides(session, tax_year=2025)
    assert int(entity.id) in overrides["by_entity"]
    assert round(overrides["by_entity"][int(entity.id)]["w2_wages_total"], 2) == 1000.0
