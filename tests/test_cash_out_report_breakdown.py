from __future__ import annotations

import datetime as dt

from sqlalchemy import and_, case, func, or_

from src.app.routes.reports import _is_internal_transfer_expr, _scope_account_predicates
from src.db.models import Account, TaxpayerEntity, Transaction


def test_cash_out_breakdown_includes_fees_and_withholding_and_excludes_internal(session):
    tp = TaxpayerEntity(name="Trust", type="TRUST")
    session.add(tp)
    session.flush()
    acct = Account(name="RJ Taxable", broker="RJ", taxpayer_entity_id=tp.id, account_type="TAXABLE")
    session.add(acct)
    session.flush()

    txs = [
        Transaction(
            account_id=acct.id,
            date=dt.date(2025, 1, 4),
            type="TRANSFER",
            amount=50.0,
            ticker=None,
            qty=None,
            lot_links_json={"description": "Cash", "additional_detail": "Incoming wire"},
        ),
        Transaction(
            account_id=acct.id,
            date=dt.date(2025, 1, 5),
            type="TRANSFER",
            amount=-100.0,
            ticker=None,
            qty=None,
            lot_links_json={"description": "Cash", "additional_detail": "*WIRE TO Vendor"},
        ),
        Transaction(
            account_id=acct.id,
            date=dt.date(2025, 1, 5),
            type="OTHER",
            amount=-999.0,
            ticker=None,
            qty=None,
            lot_links_json={"description": "JPMORGAN IRA DEPOSIT SWEEP JPMORGAN CHASE BANK NA INTRA-DAY DEPOSIT"},
        ),
        Transaction(
            account_id=acct.id,
            date=dt.date(2025, 1, 6),
            type="FEE",
            amount=-5.0,
            ticker=None,
            qty=None,
            lot_links_json={"description": "Fee"},
        ),
        Transaction(
            account_id=acct.id,
            date=dt.date(2025, 1, 7),
            type="WITHHOLDING",
            amount=2.0,
            ticker=None,
            qty=None,
            lot_links_json={"description": "Foreign taxes withheld"},
        ),
        # Internal SHADO transfer should be excluded from withdrawals.
        Transaction(
            account_id=acct.id,
            date=dt.date(2025, 1, 8),
            type="TRANSFER",
            amount=-10.0,
            ticker=None,
            qty=None,
            lot_links_json={"description": "Cash", "additional_detail": "TRSF TO SHADO ACCT FOR FX TRAD"},
        ),
    ]
    session.add_all(txs)
    session.commit()

    start_date = dt.date(2025, 1, 1)
    end_date = dt.date(2025, 12, 31)
    internal = _is_internal_transfer_expr()
    is_deposit = and_(Transaction.type == "TRANSFER", Transaction.amount > 0, ~internal)
    is_withdrawal = and_(Transaction.type == "TRANSFER", Transaction.amount < 0, ~internal)
    is_fee = and_(Transaction.type == "FEE", Transaction.amount < 0)
    is_withholding = Transaction.type == "WITHHOLDING"
    is_other = and_(Transaction.type == "OTHER", Transaction.amount < 0, ~internal)

    row = (
        session.query(
            func.sum(case((is_deposit, Transaction.amount), else_=0.0)).label("deposit_total"),
            func.sum(case((is_withdrawal, func.abs(Transaction.amount)), else_=0.0)).label("withdrawal_total"),
            func.sum(case((is_fee, func.abs(Transaction.amount)), else_=0.0)).label("fee_total"),
            func.sum(case((is_withholding, func.abs(Transaction.amount)), else_=0.0)).label("withholding_total"),
            func.sum(case((is_other, func.abs(Transaction.amount)), else_=0.0)).label("other_total"),
        )
        .select_from(Transaction)
        .join(Account, Account.id == Transaction.account_id)
        .join(TaxpayerEntity, TaxpayerEntity.id == Account.taxpayer_entity_id)
        .filter(Transaction.date >= start_date, Transaction.date <= end_date)
        .filter(Transaction.type.in_(["TRANSFER", "FEE", "WITHHOLDING", "OTHER"]))
        .filter(*_scope_account_predicates("trust"))
        .one()
    )

    assert float(row.deposit_total or 0.0) == 50.0
    assert float(row.withdrawal_total or 0.0) == 100.0
    assert float(row.fee_total or 0.0) == 5.0
    assert float(row.withholding_total or 0.0) == 2.0
    assert float(row.other_total or 0.0) == 0.0
