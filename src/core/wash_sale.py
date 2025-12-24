from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Optional

from sqlalchemy.orm import Session

from src.db.models import Account, Security, TaxpayerEntity, Transaction


@dataclass(frozen=True)
class WashMatch:
    kind: str  # EXECUTED_BUY or PROPOSED_BUY
    date: str
    ticker: str
    account_id: Optional[int]


def substantially_identical(session: Session, *, ticker_a: str, ticker_b: str) -> tuple[bool, str]:
    if ticker_a == ticker_b:
        return True, "same_ticker"
    sa = session.query(Security).filter(Security.ticker == ticker_a).one_or_none()
    sb = session.query(Security).filter(Security.ticker == ticker_b).one_or_none()
    if sa is None or sb is None:
        return False, "unknown_security"
    if sa.substitute_group_id is not None and sa.substitute_group_id == sb.substitute_group_id:
        return True, "same_substitute_group"
    return False, "no_match"


def wash_risk_for_loss_sale(
    session: Session,
    *,
    taxpayer_entity_id: int,
    sale_ticker: str,
    sale_date: dt.date,
    proposed_buys: list[dict],
    window_days: int = 30,
) -> tuple[str, list[WashMatch]]:
    start = sale_date - dt.timedelta(days=window_days)
    end = sale_date + dt.timedelta(days=window_days)

    accounts = session.query(Account).filter(Account.taxpayer_entity_id == taxpayer_entity_id).all()
    # MVP rule: exclude tax-deferred accounts (IRA) from wash-sale scope.
    account_ids = [a.id for a in accounts if (a.account_type or "").upper() == "TAXABLE"]
    executed_buys = (
        session.query(Transaction)
        .filter(
            Transaction.account_id.in_(account_ids),
            Transaction.type == "BUY",
            Transaction.date >= start,
            Transaction.date <= end,
        )
        .all()
    )

    matches: list[WashMatch] = []
    possible_due_to_unknown = False

    for tx in executed_buys:
        if tx.ticker is None:
            possible_due_to_unknown = True
            continue
        ident, reason = substantially_identical(session, ticker_a=sale_ticker, ticker_b=tx.ticker)
        if reason == "unknown_security":
            possible_due_to_unknown = True
        if ident:
            matches.append(WashMatch(kind="EXECUTED_BUY", date=tx.date.isoformat(), ticker=tx.ticker, account_id=tx.account_id))

    for pb in proposed_buys:
        if pb.get("ticker") is None:
            possible_due_to_unknown = True
            continue
        ident, reason = substantially_identical(session, ticker_a=sale_ticker, ticker_b=pb["ticker"])
        if reason == "unknown_security":
            possible_due_to_unknown = True
        if ident:
            matches.append(WashMatch(kind="PROPOSED_BUY", date=str(pb.get("date") or sale_date.isoformat()), ticker=pb["ticker"], account_id=pb.get("account_id")))

    if matches:
        return "DEFINITE", matches
    if possible_due_to_unknown:
        return "POSSIBLE", matches
    return "NONE", matches


def taxpayer_id_for_account(session: Session, *, account_id: int) -> int:
    acct = session.query(Account).filter(Account.id == account_id).one()
    return acct.taxpayer_entity_id


def taxpayer_entities_by_scope(session: Session, *, scope: str) -> list[TaxpayerEntity]:
    q = session.query(TaxpayerEntity)
    if scope == "TRUST":
        q = q.filter(TaxpayerEntity.type == "TRUST")
    elif scope == "PERSONAL":
        q = q.filter(TaxpayerEntity.type == "PERSONAL")
    return q.order_by(TaxpayerEntity.id).all()
