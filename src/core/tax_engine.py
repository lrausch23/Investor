from __future__ import annotations

import datetime as dt
from typing import Any, Optional

from sqlalchemy import or_
from sqlalchemy.orm import Session

from src.core.connection_preference import preferred_active_connection_ids_for_scope
from src.core.lot_selection import holding_term
from src.core.types import TaxYtdRow, TaxYtdSummary
from src.db.models import (
    Account,
    BrokerLotClosure,
    BrokerWashSaleEvent,
    ExternalConnection,
    ExternalTransactionMap,
    IncomeEvent,
    LotDisposal,
    TaxAssumptionsSet,
    TaxpayerEntity,
    Transaction,
)
from pydantic import BaseModel


class TaxAssumptions(BaseModel):
    ordinary_rate: float = 0.37
    ltcg_rate: float = 0.20
    state_rate: float = 0.05
    niit_enabled: bool = True
    niit_rate: float = 0.038
    qualified_dividend_pct: float = 0.0

    def as_json(self) -> dict[str, Any]:
        return self.model_dump()


def get_or_create_tax_assumptions(session: Session) -> TaxAssumptions:
    row = session.query(TaxAssumptionsSet).filter(TaxAssumptionsSet.name == "Default").one_or_none()
    if row is None:
        row = TaxAssumptionsSet(
            name="Default",
            effective_date=dt.date.today(),
            json_definition=TaxAssumptions().model_dump(),
        )
        session.add(row)
        session.flush()
    try:
        return TaxAssumptions.model_validate(row.json_definition or {})
    except Exception:
        return TaxAssumptions()


def estimate_federal_tax_ytd(
    *,
    st_gains: float,
    lt_gains: float,
    interest: float,
    dividends: float,
    assumptions: TaxAssumptions,
) -> float:
    qualified, nonqualified = split_dividends(dividends, assumptions=assumptions)
    ordinary_base = st_gains + interest + nonqualified
    ltcg_base = lt_gains + qualified
    tax = assumptions.ordinary_rate * ordinary_base + assumptions.ltcg_rate * ltcg_base
    if assumptions.niit_enabled:
        nii = max(0.0, st_gains + lt_gains + interest + dividends)
        tax += assumptions.niit_rate * nii
    return tax


def net_tax_due(*, estimated_tax: float, withholding: float) -> float:
    return estimated_tax - withholding


def tax_summary_ytd(session: Session, *, as_of: dt.date) -> TaxYtdSummary:
    tps = session.query(TaxpayerEntity).order_by(TaxpayerEntity.id).all()
    rows: list[TaxYtdRow] = []
    start = dt.date(as_of.year, 1, 1)

    accounts = session.query(Account).all()
    acct_to_tp = {a.id: a.taxpayer_entity_id for a in accounts}
    tp_by_id = {t.id: t.name for t in tps}

    gains_by_tp = {t.id: {"st": 0.0, "lt": 0.0} for t in tps}
    income_by_tp = {t.id: 0.0 for t in tps}
    withh_by_tp = {t.id: 0.0 for t in tps}

    preferred_conn_ids = preferred_active_connection_ids_for_scope(session, scope="household")

    # Prefer reconstructed LotDisposal gains when available (planning-grade).
    disposal_rows = (
        session.query(LotDisposal, Transaction, Account)
        .join(Transaction, Transaction.id == LotDisposal.sell_txn_id)
        .join(Account, Account.id == Transaction.account_id)
        .outerjoin(ExternalTransactionMap, ExternalTransactionMap.transaction_id == Transaction.id)
        .filter(Transaction.date >= start, Transaction.date <= as_of, Account.account_type == "TAXABLE")
        .filter(or_(ExternalTransactionMap.id.is_(None), ExternalTransactionMap.connection_id.in_(preferred_conn_ids)))
        .all()
    )
    sell_ids_with_disposals = {tx.id for _d, tx, _a in disposal_rows}
    for d, tx, acct in disposal_rows:
        tp_id = acct_to_tp.get(acct.id)
        if tp_id is None:
            continue
        if d.realized_gain is None:
            continue
        if d.term == "LT":
            gains_by_tp[tp_id]["lt"] += float(d.realized_gain)
        elif d.term == "ST":
            gains_by_tp[tp_id]["st"] += float(d.realized_gain)

    txns = (
        session.query(Transaction, Account)
        .join(Account, Account.id == Transaction.account_id)
        .outerjoin(ExternalTransactionMap, ExternalTransactionMap.transaction_id == Transaction.id)
        .filter(Transaction.date >= start, Transaction.date <= as_of)
        .filter(or_(ExternalTransactionMap.id.is_(None), ExternalTransactionMap.connection_id.in_(preferred_conn_ids)))
        .distinct()
        .order_by(Transaction.date)
        .all()
    )
    for tx, acct in txns:
        tp_id = acct_to_tp.get(tx.account_id)
        if tp_id is None:
            continue
        is_taxable = (acct.account_type or "").upper() == "TAXABLE"
        if tx.type == "SELL" and tx.id in sell_ids_with_disposals:
            continue
        if tx.type == "SELL" and is_taxable:
            proceeds = float(tx.amount)
            links = tx.lot_links_json or {}
            basis = float(links.get("basis_total") or 0.0)
            term = links.get("term")
            if term not in ("ST", "LT") and links.get("acquisition_date"):
                term = holding_term(dt.date.fromisoformat(links["acquisition_date"]), tx.date)
            gain = proceeds - basis
            if term == "LT":
                gains_by_tp[tp_id]["lt"] += gain
            elif term == "ST":
                gains_by_tp[tp_id]["st"] += gain
        elif tx.type in ("DIV", "INT") and is_taxable:
            income_by_tp[tp_id] += float(tx.amount)
        elif tx.type == "WITHHOLDING":
            withh_by_tp[tp_id] += float(tx.amount)

    incs = (
        session.query(IncomeEvent, Account)
        .join(Account, Account.id == IncomeEvent.account_id)
        .filter(IncomeEvent.date >= start, IncomeEvent.date <= as_of)
        .all()
    )
    for ev, acct in incs:
        tp_id = acct_to_tp.get(ev.account_id)
        if tp_id is None:
            continue
        is_taxable = (acct.account_type or "").upper() == "TAXABLE"
        if ev.type in ("DIVIDEND", "INTEREST"):
            if is_taxable:
                income_by_tp[tp_id] += float(ev.amount)
        elif ev.type == "WITHHOLDING":
            withh_by_tp[tp_id] += float(ev.amount)

    for tp in tps:
        rows.append(
            TaxYtdRow(
                taxpayer=tp_by_id[tp.id],
                st_gains=gains_by_tp[tp.id]["st"],
                lt_gains=gains_by_tp[tp.id]["lt"],
                income=income_by_tp[tp.id],
                withholding=withh_by_tp[tp.id],
            )
        )
    return TaxYtdSummary(as_of=as_of.isoformat(), rows=rows)


def tax_summary_ytd_with_net(
    session: Session,
    *,
    as_of: dt.date,
    scope: str,
    assumptions: Optional[TaxAssumptions],
) -> TaxYtdSummary:
    assumptions = assumptions or TaxAssumptions()
    start = dt.date(as_of.year, 1, 1)
    preferred_conn_ids = preferred_active_connection_ids_for_scope(session, scope=scope)

    tpq = session.query(TaxpayerEntity).order_by(TaxpayerEntity.id)
    if scope == "trust":
        tpq = tpq.filter(TaxpayerEntity.type == "TRUST")
    elif scope == "personal":
        tpq = tpq.filter(TaxpayerEntity.type == "PERSONAL")
    tps = tpq.all()

    accounts = session.query(Account).all()
    acct_by_id = {a.id: a for a in accounts}
    acct_to_tp = {a.id: a.taxpayer_entity_id for a in accounts}

    taxable_gains = {t.id: {"st": 0.0, "lt": 0.0} for t in tps}
    taxable_interest = {t.id: 0.0 for t in tps}
    taxable_div = {t.id: 0.0 for t in tps}
    withholding = {t.id: 0.0 for t in tps}

    # Prefer broker CLOSED_LOT rows for realized gains when present (planning-grade "preferred truth").
    tp_ids = [t.id for t in tps]
    broker_used_by_tp: dict[int, bool] = {t.id: False for t in tps}
    broker_has_unknown_term = False
    if tp_ids:
        broker_rows = (
            session.query(BrokerLotClosure, ExternalConnection)
            .join(ExternalConnection, ExternalConnection.id == BrokerLotClosure.connection_id)
            .filter(
                ExternalConnection.taxpayer_entity_id.in_(tp_ids),
                ExternalConnection.id.in_(preferred_conn_ids),
                BrokerLotClosure.trade_date >= start,
                BrokerLotClosure.trade_date <= as_of,
            )
            .all()
        )
        for cl, conn in broker_rows:
            tp_id = conn.taxpayer_entity_id
            if tp_id not in taxable_gains:
                continue
            realized = float(cl.realized_pl_fifo or 0.0)
            term = "UNKNOWN"
            if cl.open_datetime_raw:
                od_s = str(cl.open_datetime_raw)
                if ";" in od_s:
                    od_s = od_s.split(";", 1)[0]
                if len(od_s) >= 8 and od_s[:8].isdigit():
                    try:
                        od = dt.datetime.strptime(od_s[:8], "%Y%m%d").date()
                        term = "LT" if (cl.trade_date - od).days >= 365 else "ST"
                    except Exception:
                        term = "UNKNOWN"
            if term == "LT":
                taxable_gains[tp_id]["lt"] += realized
            else:
                # Conservative: UNKNOWN treated as ST for dashboard totals/estimates.
                taxable_gains[tp_id]["st"] += realized
                if term == "UNKNOWN":
                    broker_has_unknown_term = True
            broker_used_by_tp[tp_id] = True

        # Add broker disallowed losses (wash sales) as an increase to taxable gains (conservative, ST bucket).
        wash_rows = (
            session.query(BrokerWashSaleEvent, ExternalConnection)
            .join(ExternalConnection, ExternalConnection.id == BrokerWashSaleEvent.connection_id)
            .filter(
                ExternalConnection.taxpayer_entity_id.in_(tp_ids),
                ExternalConnection.id.in_(preferred_conn_ids),
                BrokerWashSaleEvent.trade_date >= start,
                BrokerWashSaleEvent.trade_date <= as_of,
            )
            .all()
        )
        for ws, conn in wash_rows:
            tp_id = conn.taxpayer_entity_id
            if tp_id not in taxable_gains or not broker_used_by_tp.get(tp_id):
                continue
            dloss = None
            if ws.disallowed_loss is not None:
                dloss = float(ws.disallowed_loss)
            else:
                realized = ws.realized_pl_effective if hasattr(ws, "realized_pl_effective") else None
                if realized is None:
                    realized = ws.realized_pl_fifo
                if realized is not None and float(realized) < 0:
                    dloss = abs(float(realized))
            if dloss is not None and dloss > 0:
                taxable_gains[tp_id]["st"] += dloss

    # Reconstructed gains (LotDisposal and/or SELL txns with basis) only for taxpayers without broker data.
    disposal_rows = (
        session.query(LotDisposal, Transaction, Account)
        .join(Transaction, Transaction.id == LotDisposal.sell_txn_id)
        .join(Account, Account.id == Transaction.account_id)
        .filter(Transaction.date >= start, Transaction.date <= as_of, Account.account_type == "TAXABLE")
        .all()
    )
    sell_ids_with_disposals = {tx.id for _d, tx, _a in disposal_rows}
    for d, tx, acct in disposal_rows:
        tp_id = acct_to_tp.get(acct.id)
        if tp_id is None or tp_id not in taxable_gains or broker_used_by_tp.get(tp_id):
            continue
        if d.realized_gain is None:
            continue
        if d.term == "LT":
            taxable_gains[tp_id]["lt"] += float(d.realized_gain)
        elif d.term == "ST":
            taxable_gains[tp_id]["st"] += float(d.realized_gain)

    txns = (
        session.query(Transaction)
        .join(Account, Account.id == Transaction.account_id)
        .outerjoin(ExternalTransactionMap, ExternalTransactionMap.transaction_id == Transaction.id)
        .filter(Transaction.date >= start, Transaction.date <= as_of)
        .filter(or_(ExternalTransactionMap.id.is_(None), ExternalTransactionMap.connection_id.in_(preferred_conn_ids)))
        .distinct()
        .order_by(Transaction.date)
        .all()
    )
    for tx in txns:
        tp_id = acct_to_tp.get(tx.account_id)
        if tp_id is None or tp_id not in taxable_gains:
            continue
        acct = acct_by_id.get(tx.account_id)
        is_taxable = acct is not None and acct.account_type == "TAXABLE"

        if is_taxable and broker_used_by_tp.get(tp_id) and tx.type == "SELL":
            # Avoid double-counting when broker CLOSED_LOT data is present.
            continue
        if tx.type == "SELL" and is_taxable and tx.id in sell_ids_with_disposals:
            continue
        if tx.type == "SELL" and is_taxable:
            proceeds = float(tx.amount)
            links = tx.lot_links_json or {}
            basis = float(links.get("basis_total") or 0.0)
            term = links.get("term")
            if term not in ("ST", "LT") and links.get("acquisition_date"):
                term = holding_term(dt.date.fromisoformat(links["acquisition_date"]), tx.date)
            gain = proceeds - basis
            if term == "LT":
                taxable_gains[tp_id]["lt"] += gain
            elif term == "ST":
                taxable_gains[tp_id]["st"] += gain
        elif tx.type == "DIV" and is_taxable:
            taxable_div[tp_id] += float(tx.amount)
        elif tx.type == "INT" and is_taxable:
            taxable_interest[tp_id] += float(tx.amount)
        elif tx.type == "WITHHOLDING":
            withholding[tp_id] += float(tx.amount)

    incs = (
        session.query(IncomeEvent, Account)
        .join(Account, Account.id == IncomeEvent.account_id)
        .filter(IncomeEvent.date >= start, IncomeEvent.date <= as_of)
        .all()
    )
    for ev, acct in incs:
        tp_id = acct_to_tp.get(ev.account_id)
        if tp_id is None or tp_id not in taxable_gains:
            continue
        is_taxable = acct is not None and acct.account_type == "TAXABLE"
        if ev.type == "DIVIDEND" and is_taxable:
            taxable_div[tp_id] += float(ev.amount)
        elif ev.type == "INTEREST" and is_taxable:
            taxable_interest[tp_id] += float(ev.amount)
        elif ev.type == "WITHHOLDING":
            withholding[tp_id] += float(ev.amount)

    out_rows: list[TaxYtdRow] = []
    tax_sum = 0.0
    with_sum = 0.0
    for tp in tps:
        has_taxable_accounts = (
            session.query(Account)
            .filter(Account.taxpayer_entity_id == tp.id, Account.account_type == "TAXABLE")
            .count()
            > 0
        )
        st = taxable_gains[tp.id]["st"]
        lt = taxable_gains[tp.id]["lt"]
        div = taxable_div[tp.id]
        inte = taxable_interest[tp.id]
        withh = withholding[tp.id]
        if has_taxable_accounts:
            est = estimate_federal_tax_ytd(st_gains=st, lt_gains=lt, interest=inte, dividends=div, assumptions=assumptions)
            net = net_tax_due(estimated_tax=est, withholding=withh)
            if broker_used_by_tp.get(tp.id):
                tax_note = "PRO FORMA: broker CLOSED_LOT used; wash disallowed loss added to ST (conservative)."
                if broker_has_unknown_term:
                    tax_note += " Unknown-term lots treated as ST."
            else:
                tax_note = "Planning-grade federal estimate (approx)."
            tax_sum += est
        else:
            est = None
            net = None
            tax_note = "N/A (no taxable accounts in scope; IRA tax not modeled)."
        with_sum += withh
        out_rows.append(
            TaxYtdRow(
                taxpayer=tp.name,
                st_gains=st,
                lt_gains=lt,
                income=div + inte,
                withholding=withh,
                estimated_tax=est,
                net_tax_due=net,
                tax_note=tax_note,
            )
        )

    totals = {
        "estimated_tax": tax_sum,
        "withholding": with_sum,
        "net_tax_due": tax_sum - with_sum,
    }
    return TaxYtdSummary(as_of=as_of.isoformat(), rows=out_rows, totals=totals, assumptions=assumptions.model_dump())


def estimate_tax_delta(
    *,
    st_gains: float,
    lt_gains: float,
    ordinary_income: float,
    qualified_dividends: float,
    nonqualified_dividends: float,
    interest: float,
    assumptions: TaxAssumptions,
) -> float:
    ordinary_base = st_gains + ordinary_income + nonqualified_dividends + interest
    ltcg_base = lt_gains + qualified_dividends
    state_base = st_gains + lt_gains + qualified_dividends + nonqualified_dividends + interest

    tax = assumptions.ordinary_rate * ordinary_base + assumptions.ltcg_rate * ltcg_base + assumptions.state_rate * state_base
    if assumptions.niit_enabled:
        nii = max(0.0, ltcg_base + nonqualified_dividends + qualified_dividends + interest + st_gains)
        tax += assumptions.niit_rate * nii
    return tax


def split_dividends(amount: float, *, assumptions: TaxAssumptions) -> tuple[float, float]:
    q = max(0.0, min(1.0, assumptions.qualified_dividend_pct))
    qualified = amount * q
    nonqualified = amount - qualified
    return qualified, nonqualified


class RealizedDelta(BaseModel):
    st: float
    lt: float
    ordinary: float


def realized_delta_from_lot_picks(
    *,
    sale_date: dt.date,
    sale_price: float,
    picks: list[dict[str, Any]],
) -> RealizedDelta:
    st = 0.0
    lt = 0.0
    for p in picks:
        qty = float(p["qty"])
        basis = float(p["basis_allocated"])
        proceeds = qty * sale_price
        gain = proceeds - basis
        if p["term"] == "LT":
            lt += gain
        else:
            st += gain
    return RealizedDelta(st=st, lt=lt, ordinary=0.0)
