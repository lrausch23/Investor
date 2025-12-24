from __future__ import annotations

import datetime as dt

from src.core.lot_reconstruction import rebuild_reconstructed_tax_lots_for_taxpayer
from src.db.models import Account, CorporateActionEvent, LotDisposal, Security, TaxLot, TaxpayerEntity, Transaction, WashSaleAdjustment


def _mk_taxable_account(session):
    tp = TaxpayerEntity(name="Trust", type="TRUST")
    session.add(tp)
    session.flush()
    acct = Account(name="A1", broker="IB", account_type="TAXABLE", taxpayer_entity_id=tp.id)
    session.add(acct)
    session.flush()
    session.add(Security(ticker="AAPL", name="AAPL", asset_class="EQUITY", expense_ratio=0.0, substitute_group_id=None, metadata_json={}))
    session.commit()
    return tp, acct


def test_rebuild_fifo_consumes_lots_and_terms(session):
    tp, acct = _mk_taxable_account(session)
    session.add_all(
        [
            Transaction(account_id=acct.id, date=dt.date(2024, 1, 1), type="BUY", ticker="AAPL", qty=10, amount=-1000, lot_links_json={}),
            Transaction(account_id=acct.id, date=dt.date(2025, 1, 1), type="BUY", ticker="AAPL", qty=10, amount=-2000, lot_links_json={}),
            Transaction(account_id=acct.id, date=dt.date(2025, 12, 31), type="SELL", ticker="AAPL", qty=15, amount=2250, lot_links_json={}),
        ]
    )
    session.commit()

    res = rebuild_reconstructed_tax_lots_for_taxpayer(session, taxpayer_id=tp.id, actor="test")
    assert res.lots_created == 2
    assert res.disposals_created >= 2

    lots = session.query(TaxLot).order_by(TaxLot.acquired_date.asc()).all()
    assert len(lots) >= 2
    # Remaining should be 5 shares in second lot with basis 1000.
    remaining = [l for l in lots if float(l.quantity_open) > 0]
    assert len(remaining) == 1
    assert float(remaining[0].quantity_open) == 5.0
    assert abs(float(remaining[0].basis_open or 0.0) - 1000.0) < 0.01

    disposals = session.query(LotDisposal).all()
    terms = {d.term for d in disposals}
    assert "LT" in terms
    assert "ST" in terms


def test_wash_sale_adjusts_replacement_basis(session):
    tp, acct = _mk_taxable_account(session)
    session.add_all(
        [
            Transaction(account_id=acct.id, date=dt.date(2025, 1, 1), type="BUY", ticker="AAPL", qty=10, amount=-1000, lot_links_json={}),
            Transaction(account_id=acct.id, date=dt.date(2025, 2, 1), type="SELL", ticker="AAPL", qty=10, amount=800, lot_links_json={}),  # loss 200
            Transaction(account_id=acct.id, date=dt.date(2025, 2, 15), type="BUY", ticker="AAPL", qty=10, amount=-900, lot_links_json={}),
        ]
    )
    session.commit()

    res = rebuild_reconstructed_tax_lots_for_taxpayer(session, taxpayer_id=tp.id, actor="test")
    assert res.wash_adjustments_created >= 1
    adj = session.query(WashSaleAdjustment).first()
    assert adj is not None
    assert float(adj.deferred_loss) == 200.0
    assert adj.status == "APPLIED"

    # Replacement lot basis should have increased to 1100 (900 + 200).
    repl_lot = session.query(TaxLot).filter(TaxLot.created_from_txn_id == adj.replacement_buy_txn_id).one()
    assert abs(float(repl_lot.basis_open or 0.0) - 1100.0) < 0.01


def test_split_event_adjusts_open_lot_quantity(session):
    tp, acct = _mk_taxable_account(session)
    session.add(
        Transaction(account_id=acct.id, date=dt.date(2025, 1, 1), type="BUY", ticker="AAPL", qty=10, amount=-1000, lot_links_json={})
    )
    session.add(
        CorporateActionEvent(
            taxpayer_id=tp.id,
            account_id=acct.id,
            security_id=session.query(Security).filter(Security.ticker == "AAPL").one().id,
            action_date=dt.date(2025, 6, 1),
            action_type="SPLIT",
            ratio=2.0,
            applied=False,
            details_json={},
        )
    )
    session.add(
        Transaction(account_id=acct.id, date=dt.date(2025, 7, 1), type="SELL", ticker="AAPL", qty=10, amount=600, lot_links_json={})
    )
    session.commit()

    rebuild_reconstructed_tax_lots_for_taxpayer(session, taxpayer_id=tp.id, actor="test")
    # After 2:1 split, the lot becomes 20 shares, selling 10 leaves 10.
    lot = session.query(TaxLot).first()
    assert lot is not None
    assert abs(float(lot.quantity_open) - 10.0) < 1e-6
    # Total basis should have decreased by 500 (10 shares at $50 basis/share).
    assert abs(float(lot.basis_open or 0.0) - 500.0) < 0.01


def test_rebuild_is_deterministic(session):
    tp, acct = _mk_taxable_account(session)
    session.add_all(
        [
            Transaction(account_id=acct.id, date=dt.date(2025, 1, 1), type="BUY", ticker="AAPL", qty=10, amount=-1000, lot_links_json={}),
            Transaction(account_id=acct.id, date=dt.date(2025, 2, 1), type="SELL", ticker="AAPL", qty=5, amount=600, lot_links_json={}),
        ]
    )
    session.commit()

    r1 = rebuild_reconstructed_tax_lots_for_taxpayer(session, taxpayer_id=tp.id, actor="test")
    lots1 = [(l.acquired_date, float(l.quantity_open), float(l.basis_open or 0.0)) for l in session.query(TaxLot).all()]
    disp1 = [(d.as_of_date, float(d.quantity_sold), float(d.realized_gain or 0.0), d.term) for d in session.query(LotDisposal).all()]

    r2 = rebuild_reconstructed_tax_lots_for_taxpayer(session, taxpayer_id=tp.id, actor="test")
    lots2 = [(l.acquired_date, float(l.quantity_open), float(l.basis_open or 0.0)) for l in session.query(TaxLot).all()]
    disp2 = [(d.as_of_date, float(d.quantity_sold), float(d.realized_gain or 0.0), d.term) for d in session.query(LotDisposal).all()]

    assert r1.warnings == r2.warnings
    assert lots1 == lots2
    assert disp1 == disp2

