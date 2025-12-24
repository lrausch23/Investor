from __future__ import annotations

import datetime as dt
from collections import defaultdict
from dataclasses import dataclass
from typing import Any

from sqlalchemy import or_
from sqlalchemy.orm import Session

from src.core.connection_preference import preferred_active_connection_ids_for_scope
from src.core.portfolio import holdings_snapshot, latest_cash_by_account, securities_map
from src.core.wash_sale import wash_risk_for_loss_sale
from src.db.models import Account, ExternalConnection, ExternalTransactionMap, IncomeEvent, PositionLot, TaxpayerEntity, Transaction


@dataclass(frozen=True)
class StExposureRow:
    taxpayer: str
    st_value: float
    total_value: float
    st_pct: float


def st_exposure(session: Session, *, as_of: dt.date, scope: str = "household") -> list[StExposureRow]:
    secmap = securities_map(session)

    q = (
        session.query(PositionLot, Account, TaxpayerEntity)
        .join(Account, Account.id == PositionLot.account_id)
        .join(TaxpayerEntity, TaxpayerEntity.id == Account.taxpayer_entity_id)
        .filter(Account.account_type == "TAXABLE")
    )
    if scope == "trust":
        q = q.filter(TaxpayerEntity.type == "TRUST")
    elif scope == "personal":
        q = q.filter(TaxpayerEntity.type == "PERSONAL")

    totals = defaultdict(float)
    st_totals = defaultdict(float)

    for lot, acct, tp in q.all():
        _ = acct
        sec = secmap.get(lot.ticker)
        price = float((sec.metadata_json or {}).get("last_price") or 1.0) if sec else 1.0
        mv = float(lot.qty) * price
        totals[tp.name] += mv
        if (as_of - lot.acquisition_date).days < 365:
            st_totals[tp.name] += mv

    out: list[StExposureRow] = []
    for taxpayer, total in totals.items():
        stv = st_totals.get(taxpayer, 0.0)
        out.append(
            StExposureRow(taxpayer=taxpayer, st_value=stv, total_value=total, st_pct=(stv / total) if total else 0.0)
        )
    return out


def allocation_breakdown(session: Session, *, policy_id: int, scope: str = "BOTH") -> dict[str, Any]:
    holdings, cash, warnings = holdings_snapshot(session, policy_id=policy_id, scope=scope)
    cash_by_acct = latest_cash_by_account(session)
    _ = cash_by_acct

    by_account = defaultdict(lambda: defaultdict(float))
    by_taxpayer = defaultdict(lambda: defaultdict(float))

    for h in holdings:
        code = h.bucket_code or "UNASSIGNED"
        by_account[h.account_name][code] += float(h.market_value)
        by_taxpayer[h.taxpayer_name][code] += float(h.market_value)

    for c in cash:
        by_account[c.account_name]["B1"] += float(c.amount)
        by_taxpayer[c.taxpayer_name]["B1"] += float(c.amount)

    def _rows(d):
        rows = []
        for k, v in sorted(d.items()):
            total = sum(vv for kk, vv in v.items() if kk != "UNASSIGNED") + v.get("UNASSIGNED", 0.0)
            rows.append({"name": k, "total": total, "buckets": dict(v)})
        return rows

    return {"by_account": _rows(by_account), "by_taxpayer": _rows(by_taxpayer), "warnings": warnings}


@dataclass(frozen=True)
class WashRiskSummary:
    lookback_days: int
    recent_sell_count: int
    recent_loss_sale_count: int
    missing_basis_count: int
    flagged_count: int
    message: str
    items: list[dict[str, Any]]


@dataclass(frozen=True)
class CashflowRow:
    taxpayer: str
    deposits: float
    withdrawals: float
    dividends: float
    interest: float
    withholding: float
    fees: float
    net_cashflow: float


def cashflow_summary(session: Session, *, as_of: dt.date, scope: str = "household") -> list[CashflowRow]:
    """
    Planning-grade cashflow summary for the current tax year.

    Conventions:
    - Deposits are positive TRANSFER amounts.
    - Withdrawals are negative TRANSFER amounts (reported as positive absolute).
    - WITHHOLDING amounts are stored as positive credits, but treated as cash outflow here.
    - FEE amounts may be stored negative; reported as positive absolute.
    """
    start = dt.date(as_of.year, 1, 1)
    acct_rows = session.query(Account, TaxpayerEntity).join(TaxpayerEntity, TaxpayerEntity.id == Account.taxpayer_entity_id)
    if scope == "trust":
        acct_rows = acct_rows.filter(TaxpayerEntity.type == "TRUST")
    elif scope == "personal":
        acct_rows = acct_rows.filter(TaxpayerEntity.type == "PERSONAL")
    accounts = acct_rows.all()
    if not accounts:
        return []
    acct_to_tp = {a.id: tp.name for a, tp in accounts}
    totals: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))

    preferred_conn_ids = preferred_active_connection_ids_for_scope(session, scope=scope)

    # Include manual transactions (no ExternalTransactionMap) and imported transactions from ACTIVE connections only.
    txns = (
        session.query(Transaction)
        .join(Account, Account.id == Transaction.account_id)
        .outerjoin(ExternalTransactionMap, ExternalTransactionMap.transaction_id == Transaction.id)
        .filter(Transaction.date >= start, Transaction.date <= as_of)
        .filter(Account.id.in_(list(acct_to_tp.keys())))
        .filter(or_(ExternalTransactionMap.id.is_(None), ExternalTransactionMap.connection_id.in_(preferred_conn_ids)))
        .distinct()
        .all()
    )
    for tx in txns:
        tp_name = acct_to_tp.get(tx.account_id)
        if not tp_name:
            continue
        amt = float(tx.amount or 0.0)
        if tx.type == "TRANSFER":
            if amt >= 0:
                totals[tp_name]["deposits"] += amt
            else:
                totals[tp_name]["withdrawals"] += abs(amt)
        elif tx.type == "DIV":
            totals[tp_name]["dividends"] += amt
        elif tx.type == "INT":
            totals[tp_name]["interest"] += amt
        elif tx.type == "WITHHOLDING":
            totals[tp_name]["withholding"] += abs(amt)
        elif tx.type == "FEE":
            totals[tp_name]["fees"] += abs(amt)

    incs = session.query(IncomeEvent).filter(IncomeEvent.date >= start, IncomeEvent.date <= as_of).all()
    for ev in incs:
        tp_name = acct_to_tp.get(ev.account_id)
        if not tp_name:
            continue
        amt = float(ev.amount or 0.0)
        if ev.type == "DIVIDEND":
            totals[tp_name]["dividends"] += amt
        elif ev.type == "INTEREST":
            totals[tp_name]["interest"] += amt
        elif ev.type == "WITHHOLDING":
            totals[tp_name]["withholding"] += abs(amt)
        elif ev.type == "FEE":
            totals[tp_name]["fees"] += abs(amt)

    out: list[CashflowRow] = []
    for tp_name in sorted(totals.keys()):
        d = totals[tp_name]
        deposits = float(d.get("deposits") or 0.0)
        withdrawals = float(d.get("withdrawals") or 0.0)
        dividends = float(d.get("dividends") or 0.0)
        interest = float(d.get("interest") or 0.0)
        withholding = float(d.get("withholding") or 0.0)
        fees = float(d.get("fees") or 0.0)
        net = deposits - withdrawals + dividends + interest - withholding - fees
        out.append(
            CashflowRow(
                taxpayer=tp_name,
                deposits=deposits,
                withdrawals=withdrawals,
                dividends=dividends,
                interest=interest,
                withholding=withholding,
                fees=fees,
                net_cashflow=net,
            )
        )
    return out


def wash_risk_summary(
    session: Session, *, as_of: dt.date, scope: str = "household", lookback_days: int = 30
) -> WashRiskSummary:
    start = as_of - dt.timedelta(days=lookback_days)
    preferred_conn_ids = preferred_active_connection_ids_for_scope(session, scope=scope)
    q = (
        session.query(Transaction, Account, TaxpayerEntity)
        .join(Account, Account.id == Transaction.account_id)
        .join(TaxpayerEntity, TaxpayerEntity.id == Account.taxpayer_entity_id)
        .outerjoin(ExternalTransactionMap, ExternalTransactionMap.transaction_id == Transaction.id)
        .filter(Transaction.type == "SELL", Transaction.date >= start, Transaction.date <= as_of)
        .filter(Account.account_type == "TAXABLE")
        .filter(or_(ExternalTransactionMap.id.is_(None), ExternalTransactionMap.connection_id.in_(preferred_conn_ids)))
        .order_by(Transaction.date.desc())
    )
    if scope == "trust":
        q = q.filter(TaxpayerEntity.type == "TRUST")
    elif scope == "personal":
        q = q.filter(TaxpayerEntity.type == "PERSONAL")
    txns = q.all()

    out: list[dict[str, Any]] = []
    recent_sell_count = 0
    recent_loss_sale_count = 0
    missing_basis_count = 0
    flagged_count = 0
    for tx, acct, tp in txns:
        recent_sell_count += 1
        if not tx.ticker:
            continue
        links = tx.lot_links_json or {}
        if "basis_total" not in links:
            missing_basis_count += 1
            continue
        proceeds = float(tx.amount)
        basis = float(links.get("basis_total") or 0.0)
        gain = proceeds - basis
        if gain >= 0:
            continue
        recent_loss_sale_count += 1
        risk, matches = wash_risk_for_loss_sale(
            session,
            taxpayer_entity_id=tp.id,
            sale_ticker=tx.ticker,
            sale_date=tx.date,
            proposed_buys=[],
            window_days=30,
        )
        if risk in ("DEFINITE", "POSSIBLE"):
            flagged_count += 1
            out.append(
                {
                    "taxpayer": tp.name,
                    "account": acct.name,
                    "ticker": tx.ticker,
                    "sale_date": tx.date.isoformat(),
                    "loss": gain,
                    "risk": risk,
                    "matches": [m.__dict__ for m in matches],
                }
            )
    out = out[:50]

    if recent_loss_sale_count == 0 and missing_basis_count == 0:
        msg = f"Reconstructed wash-risk: no wash-risk detected (no recent loss sales in last {lookback_days} days)."
    elif recent_loss_sale_count == 0 and missing_basis_count > 0:
        msg = (
            f"Reconstructed wash-risk not computed for {missing_basis_count} sale(s) due to missing internal lot basis; "
            "import lots (or include lot basis on SELL) to enable reconstructed wash-risk. "
            "Broker-reported wash sales (if imported) are shown under Tax → Wash sales (broker)."
        )
    elif recent_loss_sale_count > 0 and flagged_count == 0:
        msg = f"Reconstructed wash-risk: no wash-risk detected in the last {lookback_days} days."
    else:
        msg = f"Reconstructed wash-risk: wash-risk flagged for {flagged_count} of {recent_loss_sale_count} recent loss sale(s)."

    return WashRiskSummary(
        lookback_days=lookback_days,
        recent_sell_count=recent_sell_count,
        recent_loss_sale_count=recent_loss_sale_count,
        missing_basis_count=missing_basis_count,
        flagged_count=flagged_count,
        message=msg,
        items=out,
    )
