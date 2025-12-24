from __future__ import annotations

import datetime as dt

from fastapi import APIRouter, Depends, Request
from fastapi import Form
from fastapi.responses import RedirectResponse
from sqlalchemy import func
from sqlalchemy.orm import aliased
from sqlalchemy.orm import Session

from src.app.auth import auth_banner_message, require_actor
from src.app.db import db_session
from src.core.dashboard_service import parse_scope
from src.app.utils import jsonable
from src.db.audit import log_change
from src.db.models import (
    Account,
    BrokerLotClosure,
    BrokerWashSaleEvent,
    CorporateActionEvent,
    ExternalConnection,
    LotDisposal,
    Security,
    TaxLot,
    TaxpayerEntity,
    Transaction,
    WashSaleAdjustment,
)


router = APIRouter(prefix="/taxlots", tags=["taxlots"])


@router.get("")
def taxlots_open_lots(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    scope = parse_scope(request.query_params.get("scope"))
    account_id_raw = (request.query_params.get("account_id") or "").strip()
    account_id = int(account_id_raw) if account_id_raw.isdigit() else None
    today = dt.date.today()

    aq = session.query(Account, TaxpayerEntity).join(TaxpayerEntity, TaxpayerEntity.id == Account.taxpayer_entity_id)
    if scope == "trust":
        aq = aq.filter(TaxpayerEntity.type == "TRUST")
    elif scope == "personal":
        aq = aq.filter(TaxpayerEntity.type == "PERSONAL")
    accounts = [a for a, _tp in aq.order_by(Account.name).all()]
    acct_ids = [a.id for a in accounts]
    if account_id is not None and account_id not in acct_ids:
        account_id = None

    q = (
        session.query(TaxLot, Account, Security, TaxpayerEntity)
        .join(Account, Account.id == TaxLot.account_id)
        .join(Security, Security.id == TaxLot.security_id)
        .join(TaxpayerEntity, TaxpayerEntity.id == TaxLot.taxpayer_id)
        .filter(TaxLot.source == "RECONSTRUCTED", TaxLot.quantity_open > 0)
    )
    if scope == "trust":
        q = q.filter(TaxpayerEntity.type == "TRUST")
    elif scope == "personal":
        q = q.filter(TaxpayerEntity.type == "PERSONAL")
    if account_id is not None:
        q = q.filter(TaxLot.account_id == account_id)

    rows = q.order_by(Account.name, Security.ticker, TaxLot.acquired_date, TaxLot.id).all()
    from src.app.main import templates

    return templates.TemplateResponse(
        "taxlots_lots.html",
        {
            "request": request,
            "actor": actor,
            "auth_banner": auth_banner_message(),
            "scope": scope,
            "account_id": account_id,
            "accounts": accounts,
            "today": today,
            "rows": rows,
        },
    )


@router.get("/corporate-actions")
def corporate_actions_list(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    scope = parse_scope(request.query_params.get("scope"))
    tq = session.query(TaxpayerEntity).order_by(TaxpayerEntity.id.asc())
    if scope == "trust":
        tq = tq.filter(TaxpayerEntity.type == "TRUST")
    elif scope == "personal":
        tq = tq.filter(TaxpayerEntity.type == "PERSONAL")
    taxpayers = tq.all()

    accounts = session.query(Account).order_by(Account.name.asc()).all()
    events = session.query(CorporateActionEvent).order_by(CorporateActionEvent.action_date.desc(), CorporateActionEvent.id.desc()).limit(200).all()
    sec_by_id = {s.id: s for s in session.query(Security).all()}
    tp_by_id = {t.id: t for t in session.query(TaxpayerEntity).all()}
    acct_by_id = {a.id: a for a in accounts}

    from src.app.main import templates
    return templates.TemplateResponse(
        "taxlots_corporate_actions.html",
        {
            "request": request,
            "actor": actor,
            "auth_banner": auth_banner_message(),
            "scope": scope,
            "taxpayers": taxpayers,
            "accounts": accounts,
            "events": events,
            "sec_by_id": sec_by_id,
            "tp_by_id": tp_by_id,
            "acct_by_id": acct_by_id,
            "today": dt.date.today().isoformat(),
        },
    )


@router.post("/corporate-actions")
def corporate_actions_create(
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    taxpayer_id: int = Form(...),
    account_id: str = Form(default=""),
    ticker: str = Form(...),
    action_date: str = Form(...),
    action_type: str = Form(...),
    ratio: str = Form(default=""),
    note: str = Form(default=""),
):
    tkr = ticker.strip().upper()
    sec = session.query(Security).filter(Security.ticker == tkr).one_or_none()
    if sec is None:
        sec = Security(ticker=tkr, name=tkr, asset_class="UNKNOWN", expense_ratio=0.0, substitute_group_id=None, metadata_json={})
        session.add(sec)
        session.flush()

    acct_id = int(account_id) if account_id.strip().isdigit() else None
    r = float(ratio) if ratio.strip() else None
    ev = CorporateActionEvent(
        taxpayer_id=taxpayer_id,
        account_id=acct_id,
        security_id=sec.id,
        action_date=dt.date.fromisoformat(action_date),
        action_type=action_type.strip().upper(),
        ratio=r,
        applied=False,
        details_json={},
    )
    session.add(ev)
    session.flush()
    log_change(
        session,
        actor=actor,
        action="CREATE",
        entity="CorporateActionEvent",
        entity_id=str(ev.id),
        old=None,
        new=jsonable({"taxpayer_id": taxpayer_id, "account_id": acct_id, "ticker": tkr, "action_type": ev.action_type, "ratio": r, "action_date": ev.action_date.isoformat()}),
        note=note or "Create corporate action event (manual)",
    )
    session.commit()
    return RedirectResponse(url="/taxlots/corporate-actions", status_code=303)

@router.get("/gains")
def taxlots_realized_gains(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    scope = parse_scope(request.query_params.get("scope"))
    year_raw = (request.query_params.get("year") or "").strip()
    year = int(year_raw) if year_raw.isdigit() else dt.date.today().year
    source = (request.query_params.get("source") or "auto").strip().lower()  # auto|broker|reconstructed
    start = dt.date(year, 1, 1)
    end = dt.date(year, 12, 31)

    # Auto: if broker closed-lot data exists in this scope/year, prefer it.
    if source == "auto":
        conn_q = session.query(ExternalConnection).join(TaxpayerEntity, TaxpayerEntity.id == ExternalConnection.taxpayer_entity_id)
        if scope == "trust":
            conn_q = conn_q.filter(TaxpayerEntity.type == "TRUST")
        elif scope == "personal":
            conn_q = conn_q.filter(TaxpayerEntity.type == "PERSONAL")
        conn_ids = [c.id for c in conn_q.all()]
        broker_count = 0
        if conn_ids:
            broker_count = (
                session.query(BrokerLotClosure)
                .filter(BrokerLotClosure.connection_id.in_(conn_ids), BrokerLotClosure.trade_date >= start, BrokerLotClosure.trade_date <= end)
                .count()
            )
        source = "broker" if broker_count > 0 else "reconstructed"

    rows = []
    totals = {"st": 0.0, "lt": 0.0, "unknown": 0.0}
    source_label = "Reconstructed (LotDisposal)"

    if source == "broker":
        source_label = "Broker (CLOSED_LOT)"
        conn_q = session.query(ExternalConnection).join(TaxpayerEntity, TaxpayerEntity.id == ExternalConnection.taxpayer_entity_id)
        if scope == "trust":
            conn_q = conn_q.filter(TaxpayerEntity.type == "TRUST")
        elif scope == "personal":
            conn_q = conn_q.filter(TaxpayerEntity.type == "PERSONAL")
        conn_ids = [c.id for c in conn_q.all()]
        closures = []
        if conn_ids:
            closures = (
                session.query(BrokerLotClosure)
                .filter(BrokerLotClosure.connection_id.in_(conn_ids), BrokerLotClosure.trade_date >= start, BrokerLotClosure.trade_date <= end)
                .all()
            )

        def _parse_ib_date(raw: str | None) -> dt.date | None:
            if not raw:
                return None
            s = raw.strip()
            if ";" in s:
                s = s.split(";", 1)[0].strip()
            if len(s) >= 8 and s[:8].isdigit():
                try:
                    return dt.datetime.strptime(s[:8], "%Y%m%d").date()
                except Exception:
                    return None
            try:
                return dt.date.fromisoformat(s[:10])
            except Exception:
                return None

        agg: dict[tuple[str, str], dict[str, float]] = {}
        for r in closures:
            term = "UNKNOWN"
            od = _parse_ib_date(r.open_datetime_raw)
            if od is not None:
                term = "LT" if (r.trade_date - od).days >= 365 else "ST"
            proceeds = float(r.proceeds_derived) if r.proceeds_derived is not None else None
            basis = float(r.cost_basis) if r.cost_basis is not None else None
            gain = float(r.realized_pl_fifo) if r.realized_pl_fifo is not None else None
            key = (r.symbol, term)
            a = agg.setdefault(key, {"proceeds": 0.0, "basis": 0.0, "gain": 0.0, "missing_proceeds": 0.0})
            if proceeds is not None:
                a["proceeds"] += proceeds
            else:
                a["missing_proceeds"] += 1.0
            if basis is not None:
                a["basis"] += basis
            if gain is not None:
                a["gain"] += gain
                if term == "LT":
                    totals["lt"] += gain
                elif term == "ST":
                    totals["st"] += gain
                else:
                    totals["unknown"] += gain
        rows = [(tkr, term, a["proceeds"], a["basis"], a["gain"]) for (tkr, term), a in sorted(agg.items())]
    else:
        q = (
            session.query(Security.ticker, LotDisposal.term, func.sum(LotDisposal.proceeds_allocated), func.sum(LotDisposal.basis_allocated), func.sum(LotDisposal.realized_gain))
            .join(Transaction, Transaction.id == LotDisposal.sell_txn_id)
            .join(TaxLot, TaxLot.id == LotDisposal.tax_lot_id)
            .join(Security, Security.id == TaxLot.security_id)
            .join(Account, Account.id == Transaction.account_id)
            .join(TaxpayerEntity, TaxpayerEntity.id == Account.taxpayer_entity_id)
            .filter(Transaction.date >= start, Transaction.date <= end, Account.account_type == "TAXABLE")
            .group_by(Security.ticker, LotDisposal.term)
            .order_by(Security.ticker, LotDisposal.term)
        )
        if scope == "trust":
            q = q.filter(TaxpayerEntity.type == "TRUST")
        elif scope == "personal":
            q = q.filter(TaxpayerEntity.type == "PERSONAL")
        rows = q.all()
        for _tkr, term, _p, _b, g in rows:
            if g is None:
                continue
            if term == "LT":
                totals["lt"] += float(g)
            elif term == "ST":
                totals["st"] += float(g)
            else:
                totals["unknown"] += float(g)

    from src.app.main import templates

    return templates.TemplateResponse(
        "taxlots_gains.html",
        {
            "request": request,
            "actor": actor,
            "auth_banner": auth_banner_message(),
            "scope": scope,
            "year": year,
            "source": source,
            "source_label": source_label,
            "rows": rows,
            "totals": totals,
        },
    )


@router.get("/wash-sales-broker")
def broker_wash_sales(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    scope = parse_scope(request.query_params.get("scope"))
    year_raw = (request.query_params.get("year") or "").strip()
    year = int(year_raw) if year_raw.isdigit() else dt.date.today().year
    start = dt.date(year, 1, 1)
    end = dt.date(year, 12, 31)

    conn_q = session.query(ExternalConnection).join(TaxpayerEntity, TaxpayerEntity.id == ExternalConnection.taxpayer_entity_id)
    if scope == "trust":
        conn_q = conn_q.filter(TaxpayerEntity.type == "TRUST")
    elif scope == "personal":
        conn_q = conn_q.filter(TaxpayerEntity.type == "PERSONAL")
    conn_ids = [c.id for c in conn_q.all()]
    rows = []
    if conn_ids:
        rows = (
            session.query(BrokerWashSaleEvent)
            .filter(BrokerWashSaleEvent.connection_id.in_(conn_ids), BrokerWashSaleEvent.trade_date >= start, BrokerWashSaleEvent.trade_date <= end)
            .order_by(BrokerWashSaleEvent.trade_date.desc(), BrokerWashSaleEvent.id.desc())
            .limit(500)
            .all()
        )
    available_years: list[int] = []
    if conn_ids:
        years = (
            session.query(func.strftime("%Y", BrokerWashSaleEvent.trade_date).label("y"))
            .filter(BrokerWashSaleEvent.connection_id.in_(conn_ids))
            .group_by("y")
            .order_by("y")
            .all()
        )
        available_years = [int(y[0]) for y in years if y and y[0] and str(y[0]).isdigit()]
    totals_by_symbol: dict[str, float] = {}
    for r in rows:
        totals_by_symbol[r.symbol] = float(totals_by_symbol.get(r.symbol) or 0.0) + float(r.realized_pl_fifo or 0.0)

    from src.app.main import templates
    return templates.TemplateResponse(
        "taxlots_wash_broker.html",
        {
            "request": request,
            "actor": actor,
            "auth_banner": auth_banner_message(),
            "scope": scope,
            "year": year,
            "rows": rows,
            "totals_by_symbol": dict(sorted(totals_by_symbol.items())),
            "available_years": available_years,
        },
    )


@router.get("/wash-sales")
def taxlots_wash_sales(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    scope = parse_scope(request.query_params.get("scope"))
    year_raw = (request.query_params.get("year") or "").strip()
    year = int(year_raw) if year_raw.isdigit() else dt.date.today().year
    source = (request.query_params.get("source") or "auto").strip().lower()  # auto|broker|reconstructed
    start = dt.date(year, 1, 1)
    end = dt.date(year, 12, 31)

    # If broker wash rows exist in this scope/year, prefer redirecting there for auto/broker.
    conn_q = session.query(ExternalConnection).join(TaxpayerEntity, TaxpayerEntity.id == ExternalConnection.taxpayer_entity_id)
    if scope == "trust":
        conn_q = conn_q.filter(TaxpayerEntity.type == "TRUST")
    elif scope == "personal":
        conn_q = conn_q.filter(TaxpayerEntity.type == "PERSONAL")
    conn_ids = [c.id for c in conn_q.all()]
    broker_count = 0
    if conn_ids:
        broker_count = (
            session.query(BrokerWashSaleEvent)
            .filter(
                BrokerWashSaleEvent.connection_id.in_(conn_ids),
                BrokerWashSaleEvent.trade_date >= start,
                BrokerWashSaleEvent.trade_date <= end,
            )
            .count()
        )
    broker_available = broker_count > 0
    if source in {"auto", "broker"} and broker_available:
        return RedirectResponse(url=f"/taxlots/wash-sales-broker?scope={scope}&year={year}", status_code=303)
    # If user explicitly asked for broker but none exists for that year, fall back to reconstructed.
    if source == "broker" and not broker_available:
        source = "reconstructed"

    sale_tx = aliased(Transaction)
    buy_tx = aliased(Transaction)
    sale_acct = aliased(Account)
    buy_acct = aliased(Account)

    q = (
        session.query(WashSaleAdjustment, sale_tx, buy_tx, sale_acct, buy_acct, TaxpayerEntity)
        .join(sale_tx, sale_tx.id == WashSaleAdjustment.loss_sale_txn_id)
        .outerjoin(buy_tx, buy_tx.id == WashSaleAdjustment.replacement_buy_txn_id)
        .join(sale_acct, sale_acct.id == sale_tx.account_id)
        .outerjoin(buy_acct, buy_acct.id == buy_tx.account_id)
        .join(TaxpayerEntity, TaxpayerEntity.id == sale_acct.taxpayer_entity_id)
        .filter(sale_tx.date >= start, sale_tx.date <= end)
        .order_by(sale_tx.date.desc(), WashSaleAdjustment.id.desc())
    )
    if scope == "trust":
        q = q.filter(TaxpayerEntity.type == "TRUST")
    elif scope == "personal":
        q = q.filter(TaxpayerEntity.type == "PERSONAL")

    rows = q.all()

    from src.app.main import templates

    return templates.TemplateResponse(
        "taxlots_wash.html",
        {
            "request": request,
            "actor": actor,
            "auth_banner": auth_banner_message(),
            "scope": scope,
            "year": year,
            "source": source,
            "broker_available": broker_available,
            "rows": rows,
        },
    )
