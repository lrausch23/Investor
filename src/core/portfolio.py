from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Optional

from sqlalchemy import func
from sqlalchemy.orm import Session

from src.db.models import Account, BucketAssignment, BucketPolicy, CashBalance, PositionLot, Security, TaxLot, TaxpayerEntity


@dataclass(frozen=True)
class LotView:
    id: int
    account_id: int
    ticker: str
    acquisition_date: dt.date
    qty: float
    basis_total: float
    adjusted_basis_total: Optional[float]


@dataclass(frozen=True)
class HoldingView:
    account_id: int
    account_name: str
    taxpayer_id: int
    taxpayer_name: str
    taxpayer_type: str
    ticker: str
    qty: float
    market_value: float
    lots: list[LotView]
    price: float
    asset_class: str
    expense_ratio: float
    substitute_group_id: Optional[int]
    bucket_code: Optional[str]


@dataclass(frozen=True)
class CashView:
    account_id: int
    account_name: str
    taxpayer_id: int
    taxpayer_name: str
    amount: float
    as_of: dt.date


def _to_float(v: Any) -> float:
    if v is None:
        return 0.0
    if isinstance(v, Decimal):
        return float(v)
    return float(v)


def latest_cash_by_account(session: Session) -> dict[int, CashView]:
    subq = (
        session.query(CashBalance.account_id, func.max(CashBalance.as_of_date).label("max_date"))
        .group_by(CashBalance.account_id)
        .subquery()
    )
    rows = (
        session.query(CashBalance, Account, TaxpayerEntity)
        .join(subq, (CashBalance.account_id == subq.c.account_id) & (CashBalance.as_of_date == subq.c.max_date))
        .join(Account, Account.id == CashBalance.account_id)
        .join(TaxpayerEntity, TaxpayerEntity.id == Account.taxpayer_entity_id)
        .all()
    )
    out: dict[int, CashView] = {}
    for cb, acct, tp in rows:
        out[acct.id] = CashView(
            account_id=acct.id,
            account_name=acct.name,
            taxpayer_id=tp.id,
            taxpayer_name=tp.name,
            amount=_to_float(cb.amount),
            as_of=cb.as_of_date,
        )
    return out


def bucket_map(session: Session, *, policy_id: int) -> dict[str, str]:
    rows = session.query(BucketAssignment).filter(BucketAssignment.policy_id == policy_id).all()
    return {r.ticker: r.bucket_code for r in rows}


def securities_map(session: Session) -> dict[str, Security]:
    rows = session.query(Security).all()
    return {s.ticker: s for s in rows}


def lots_by_account_ticker(
    session: Session, *, scope: str, taxpayer_entity_id: Optional[int] = None
) -> dict[tuple[int, str], list[LotView]]:
    # Prefer reconstructed TaxLot lots when present; fall back to manual PositionLot.
    # We load both then select per (account_id, ticker).
    taxlot_q = (
        session.query(TaxLot, Account, TaxpayerEntity, Security)
        .join(Account, Account.id == TaxLot.account_id)
        .join(TaxpayerEntity, TaxpayerEntity.id == Account.taxpayer_entity_id)
        .join(Security, Security.id == TaxLot.security_id)
        .filter(TaxLot.source == "RECONSTRUCTED", TaxLot.quantity_open > 0)
    )
    q = (
        session.query(PositionLot, Account, TaxpayerEntity)
        .join(Account, Account.id == PositionLot.account_id)
        .join(TaxpayerEntity, TaxpayerEntity.id == Account.taxpayer_entity_id)
    )
    if taxpayer_entity_id is not None:
        q = q.filter(TaxpayerEntity.id == taxpayer_entity_id)
        taxlot_q = taxlot_q.filter(TaxpayerEntity.id == taxpayer_entity_id)
    else:
        if scope == "TRUST":
            q = q.filter(TaxpayerEntity.type == "TRUST")
            taxlot_q = taxlot_q.filter(TaxpayerEntity.type == "TRUST")
        elif scope == "PERSONAL":
            q = q.filter(TaxpayerEntity.type == "PERSONAL")
            taxlot_q = taxlot_q.filter(TaxpayerEntity.type == "PERSONAL")
    taxlot_rows = taxlot_q.all()
    taxlots_by_key: dict[tuple[int, str], list[LotView]] = {}
    for lot, acct, _tp, sec in taxlot_rows:
        key = (acct.id, sec.ticker)
        taxlots_by_key.setdefault(key, []).append(
            LotView(
                id=int(lot.id),
                account_id=acct.id,
                ticker=sec.ticker,
                acquisition_date=lot.acquired_date,
                qty=float(lot.quantity_open),
                basis_total=float(lot.basis_open or 0.0),
                adjusted_basis_total=None,
            )
        )

    rows = q.all()
    poslots_by_key: dict[tuple[int, str], list[LotView]] = {}
    for lot, acct, _tp in rows:
        key = (acct.id, lot.ticker)
        poslots_by_key.setdefault(key, []).append(
            LotView(
                id=lot.id,
                account_id=acct.id,
                ticker=lot.ticker,
                acquisition_date=lot.acquisition_date,
                qty=_to_float(lot.qty),
                basis_total=_to_float(lot.basis_total),
                adjusted_basis_total=_to_float(lot.adjusted_basis_total) if lot.adjusted_basis_total is not None else None,
            )
        )

    # Prefer reconstructed lots if present for a key.
    out: dict[tuple[int, str], list[LotView]] = {}
    for key, lots in poslots_by_key.items():
        out[key] = lots
    for key, lots in taxlots_by_key.items():
        out[key] = lots
    return out


def holdings_snapshot(
    session: Session,
    *,
    policy_id: int,
    scope: str,
    taxpayer_entity_id: Optional[int] = None,
    as_of: Optional[dt.date] = None,
) -> tuple[list[HoldingView], list[CashView], list[str]]:
    as_of = as_of or dt.date.today()
    warnings: list[str] = []

    policy = session.query(BucketPolicy).filter(BucketPolicy.id == policy_id).one()
    _ = policy  # for future use
    bmap = bucket_map(session, policy_id=policy_id)
    secmap = securities_map(session)
    lotmap = lots_by_account_ticker(session, scope=scope, taxpayer_entity_id=taxpayer_entity_id)

    accounts_q = session.query(Account, TaxpayerEntity).join(TaxpayerEntity)
    if taxpayer_entity_id is not None:
        accounts_q = accounts_q.filter(TaxpayerEntity.id == taxpayer_entity_id)
    else:
        if scope == "TRUST":
            accounts_q = accounts_q.filter(TaxpayerEntity.type == "TRUST")
        elif scope == "PERSONAL":
            accounts_q = accounts_q.filter(TaxpayerEntity.type == "PERSONAL")
    accounts = accounts_q.all()

    cash_by_acct = latest_cash_by_account(session)
    cash_views: list[CashView] = [cash_by_acct[a.id] for a, _tp in accounts if a.id in cash_by_acct]

    holdings: list[HoldingView] = []

    def infer_bucket(asset_class: str) -> Optional[str]:
        ac = (asset_class or "").strip().upper()
        if ac in {"CASH", "MMF"}:
            return "B1"
        if ac in {"BOND", "CREDIT", "DIVIDEND"}:
            return "B2"
        if ac in {"EQUITY", "INDEX", "GROWTH"}:
            return "B3"
        if ac in {"ALTERNATIVE", "THEMATIC", "ALPHA"}:
            return "B4"
        return None

    for acct, tp in accounts:
        tickers = {ticker for (aid, ticker) in lotmap.keys() if aid == acct.id}
        for ticker in sorted(tickers):
            lots = lotmap.get((acct.id, ticker), [])
            qty = sum(l.qty for l in lots)
            if qty == 0:
                continue
            sec = secmap.get(ticker)
            if sec is None:
                warnings.append(f"Missing Security record for ticker {ticker}; using placeholder price=1.0 and ER=0.")
                price = 1.0
                asset_class = "UNKNOWN"
                expense_ratio = 0.0
                group_id = None
            else:
                price = float((sec.metadata_json or {}).get("last_price") or 0.0)
                if price <= 0:
                    warnings.append(f"Missing/zero last_price for {ticker}; planner uses placeholder price=1.0.")
                    price = 1.0
                asset_class = sec.asset_class
                expense_ratio = float(sec.expense_ratio or 0.0)
                group_id = sec.substitute_group_id
            mv = qty * price
            bucket = bmap.get(ticker)
            if bucket is None and sec is not None:
                inferred = infer_bucket(asset_class)
                if inferred is not None:
                    bucket = inferred
                    warnings.append(f"Inferred bucket {bucket} for {ticker} from asset_class={asset_class}; set explicit bucket assignment to override.")
            holdings.append(
                HoldingView(
                    account_id=acct.id,
                    account_name=acct.name,
                    taxpayer_id=tp.id,
                    taxpayer_name=tp.name,
                    taxpayer_type=tp.type,
                    ticker=ticker,
                    qty=qty,
                    market_value=mv,
                    lots=lots,
                    price=price,
                    asset_class=asset_class,
                    expense_ratio=expense_ratio,
                    substitute_group_id=group_id,
                    bucket_code=bucket,
                )
            )

    return holdings, cash_views, warnings
