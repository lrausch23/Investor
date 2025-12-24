from __future__ import annotations

import datetime as dt
from typing import Any

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import RedirectResponse
from sqlalchemy.orm import Session

from src.app.auth import auth_banner_message, require_actor
from src.app.db import db_session
from src.app.utils import jsonable
from src.core.tax_engine import TaxAssumptions
from src.core.broker_tax import broker_realized_gains, broker_tax_summary, rows_to_csv
from src.db.audit import log_change
from src.db.models import Account, BrokerWashSaleEvent, ExternalAccountMap, ExternalConnection, TaxAssumptionsSet, TaxpayerEntity

router = APIRouter(prefix="/tax", tags=["tax"])


@router.get("/assumptions")
def tax_assumptions(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    row = session.query(TaxAssumptionsSet).filter(TaxAssumptionsSet.name == "Default").one_or_none()
    if row is None:
        row = TaxAssumptionsSet(name="Default", effective_date=dt.date.today(), json_definition=TaxAssumptions().model_dump())
        session.add(row)
        session.commit()

    assumptions = TaxAssumptions.model_validate(row.json_definition or {})
    from src.app.main import templates

    return templates.TemplateResponse(
        "tax_assumptions.html",
        {"request": request, "actor": actor, "auth_banner": auth_banner_message(), "row": row, "a": assumptions},
    )


@router.post("/assumptions")
def tax_assumptions_update(
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    ordinary_rate: float = Form(...),
    ltcg_rate: float = Form(...),
    state_rate: float = Form(default=0.05),
    niit_enabled: str = Form(default=""),
    niit_rate: float = Form(default=0.038),
    qualified_dividend_pct: float = Form(default=0.0),
    personal_st_rate: str = Form(default=""),
    personal_lt_rate: str = Form(default=""),
    trust_st_rate: str = Form(default=""),
    trust_lt_rate: str = Form(default=""),
    note: str = Form(default=""),
):
    row = session.query(TaxAssumptionsSet).filter(TaxAssumptionsSet.name == "Default").one()
    old = jsonable(row.json_definition)
    base = TaxAssumptions(
        ordinary_rate=float(ordinary_rate),
        ltcg_rate=float(ltcg_rate),
        state_rate=float(state_rate),
        niit_enabled=(niit_enabled == "on"),
        niit_rate=float(niit_rate),
        qualified_dividend_pct=float(qualified_dividend_pct),
    ).model_dump()
    tax_rates: dict[str, dict[str, Any]] = {}
    if personal_st_rate.strip() or personal_lt_rate.strip():
        tax_rates["personal"] = {}
        if personal_st_rate.strip():
            tax_rates["personal"]["st_rate"] = float(personal_st_rate)
        if personal_lt_rate.strip():
            tax_rates["personal"]["lt_rate"] = float(personal_lt_rate)
    if trust_st_rate.strip() or trust_lt_rate.strip():
        tax_rates["trust"] = {}
        if trust_st_rate.strip():
            tax_rates["trust"]["st_rate"] = float(trust_st_rate)
        if trust_lt_rate.strip():
            tax_rates["trust"]["lt_rate"] = float(trust_lt_rate)
    if tax_rates:
        base["tax_rates"] = tax_rates
    row.json_definition = base
    session.flush()
    log_change(
        session,
        actor=actor,
        action="UPDATE",
        entity="TaxAssumptionsSet",
        entity_id=str(row.id),
        old=old,
        new=jsonable(row.json_definition),
        note=note or "Update tax assumptions",
    )
    session.commit()
    return RedirectResponse(url="/tax/assumptions", status_code=303)


@router.get("/summary")
def tax_summary(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    from src.core.dashboard_service import parse_scope
    from src.app.main import templates

    scope = parse_scope(request.query_params.get("scope"))
    year_raw = (request.query_params.get("year") or "").strip()
    year = int(year_raw) if year_raw.isdigit() else dt.date.today().year

    summary = broker_tax_summary(session, scope=scope, year=year)

    return templates.TemplateResponse(
        "tax_summary.html",
        {
            "request": request,
            "actor": actor,
            "auth_banner": auth_banner_message(),
            "scope": scope,
            "year": year,
            "summary": summary,
        },
    )


@router.get("/summary.csv")
def tax_summary_csv(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    from src.core.dashboard_service import parse_scope
    from fastapi.responses import Response

    scope = parse_scope(request.query_params.get("scope"))
    year_raw = (request.query_params.get("year") or "").strip()
    year = int(year_raw) if year_raw.isdigit() else dt.date.today().year
    summary = broker_tax_summary(session, scope=scope, year=year)

    rows = []
    for r in summary.get("rows") or []:
        rows.append(
            [
                r.get("taxpayer"),
                r.get("taxpayer_type"),
                r.get("st_realized"),
                r.get("lt_realized"),
                r.get("unknown_realized"),
                r.get("realized_total"),
                r.get("disallowed_loss"),
                r.get("net_taxable"),
                r.get("additional_tax_due"),
            ]
        )
    csv_text = rows_to_csv(
        ["taxpayer", "taxpayer_type", "st_realized", "lt_realized", "unknown_realized", "realized_total", "disallowed_loss", "net_taxable", "additional_tax_due"],
        rows,
    )
    fn = f"tax_summary_{scope}_{year}.csv"
    return Response(content=csv_text, media_type="text/csv", headers={"Content-Disposition": f'attachment; filename="{fn}"'})


@router.get("/broker/realized-gains")
def tax_broker_realized_gains(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    from src.core.dashboard_service import parse_scope
    from src.app.main import templates

    scope = parse_scope(request.query_params.get("scope"))
    year_raw = (request.query_params.get("year") or "").strip()
    year = int(year_raw) if year_raw.isdigit() else dt.date.today().year
    account_id_raw = (request.query_params.get("account_id") or "").strip()
    account_id = int(account_id_raw) if account_id_raw.isdigit() else None

    accounts = session.query(Account).order_by(Account.name.asc()).all()
    summary, by_symbol_rows, detail_rows, coverage = broker_realized_gains(session, scope=scope, year=year, account_id=account_id)

    return templates.TemplateResponse(
        "tax_broker_realized_gains.html",
        {
            "request": request,
            "actor": actor,
            "auth_banner": auth_banner_message(),
            "scope": scope,
            "year": year,
            "account_id": account_id,
            "accounts": accounts,
            "summary": summary,
            "by_symbol_rows": by_symbol_rows,
            "detail_rows": detail_rows,
            "coverage": coverage,
        },
    )


@router.get("/broker/realized-gains.csv")
def tax_broker_realized_gains_csv(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    from src.core.dashboard_service import parse_scope
    from fastapi.responses import Response

    scope = parse_scope(request.query_params.get("scope"))
    year_raw = (request.query_params.get("year") or "").strip()
    year = int(year_raw) if year_raw.isdigit() else dt.date.today().year
    account_id_raw = (request.query_params.get("account_id") or "").strip()
    account_id = int(account_id_raw) if account_id_raw.isdigit() else None

    _summary, _by, detail, _cov = broker_realized_gains(session, scope=scope, year=year, account_id=account_id)
    csv_text = rows_to_csv(
        ["trade_date", "provider_account_id", "account_name", "symbol", "qty", "open_date_raw", "proceeds", "basis", "realized", "term", "closure_id", "ib_trade_id", "ib_transaction_id"],
        [
            [
                r.trade_date.isoformat(),
                r.provider_account_id,
                r.account_name or "",
                r.symbol,
                r.quantity_closed,
                r.open_date_raw or "",
                r.proceeds,
                r.basis,
                r.realized,
                r.term,
                r.closure_id,
                r.ib_trade_id or "",
                r.ib_transaction_id or "",
            ]
            for r in detail
        ],
    )
    fn = f"broker_realized_gains_{scope}_{year}.csv"
    return Response(content=csv_text, media_type="text/csv", headers={"Content-Disposition": f'attachment; filename="{fn}"'})


@router.get("/broker/wash-sales")
def tax_broker_wash_sales(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    from src.core.dashboard_service import parse_scope
    from src.app.main import templates

    scope = parse_scope(request.query_params.get("scope"))
    year_raw = (request.query_params.get("year") or "").strip()
    year = int(year_raw) if year_raw.isdigit() else dt.date.today().year
    start = dt.date(year, 1, 1)
    end = dt.date(year, 12, 31)

    # Connections in scope.
    conn_q = session.query(ExternalConnection).join(TaxpayerEntity, TaxpayerEntity.id == ExternalConnection.taxpayer_entity_id)
    if scope == "trust":
        conn_q = conn_q.filter(TaxpayerEntity.type == "TRUST")
    elif scope == "personal":
        conn_q = conn_q.filter(TaxpayerEntity.type == "PERSONAL")
    conn_ids = [c.id for c in conn_q.all()]

    rows: list[BrokerWashSaleEvent] = []
    if conn_ids:
        rows = (
            session.query(BrokerWashSaleEvent)
            .filter(BrokerWashSaleEvent.connection_id.in_(conn_ids), BrokerWashSaleEvent.trade_date >= start, BrokerWashSaleEvent.trade_date <= end)
            .order_by(BrokerWashSaleEvent.trade_date.desc(), BrokerWashSaleEvent.id.desc())
            .limit(2000)
            .all()
        )

    total_rows = len(rows)
    linked = sum(1 for r in rows if r.linked_closure_id is not None)
    with_basis = sum(1 for r in rows if r.basis_effective is not None)
    with_proceeds = sum(1 for r in rows if r.proceeds_derived is not None)
    disallowed_total = sum(float(r.disallowed_loss or 0.0) for r in rows if r.disallowed_loss is not None)

    totals_by_symbol: dict[str, float] = {}
    for r in rows:
        if r.disallowed_loss is None:
            continue
        totals_by_symbol[r.symbol] = float(totals_by_symbol.get(r.symbol) or 0.0) + float(r.disallowed_loss or 0.0)

    return templates.TemplateResponse(
        "tax_broker_wash_sales.html",
        {
            "request": request,
            "actor": actor,
            "auth_banner": auth_banner_message(),
            "scope": scope,
            "year": year,
            "rows": rows,
            "metrics": {
                "total_rows": total_rows,
                "linked_rows": linked,
                "with_basis_rows": with_basis,
                "with_proceeds_rows": with_proceeds,
                "disallowed_total": disallowed_total,
            },
            "totals_by_symbol": dict(sorted(totals_by_symbol.items())),
        },
    )


@router.get("/broker/wash-sales.csv")
def tax_broker_wash_sales_csv(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    from src.core.dashboard_service import parse_scope
    from fastapi.responses import Response

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

    rows: list[BrokerWashSaleEvent] = []
    if conn_ids:
        rows = (
            session.query(BrokerWashSaleEvent)
            .filter(BrokerWashSaleEvent.connection_id.in_(conn_ids), BrokerWashSaleEvent.trade_date >= start, BrokerWashSaleEvent.trade_date <= end)
            .order_by(BrokerWashSaleEvent.trade_date.asc(), BrokerWashSaleEvent.id.asc())
            .all()
        )

    csv_text = rows_to_csv(
        ["trade_date", "provider_account_id", "symbol", "qty", "realized_pl_fifo", "basis_effective", "proceeds_derived", "disallowed_loss", "linked_closure_id", "when_realized_raw", "when_reopened_raw"],
        [
            [
                r.trade_date.isoformat(),
                r.provider_account_id,
                r.symbol,
                float(r.quantity),
                float(r.realized_pl_fifo) if r.realized_pl_fifo is not None else "",
                float(r.basis_effective) if r.basis_effective is not None else "",
                float(r.proceeds_derived) if r.proceeds_derived is not None else "",
                float(r.disallowed_loss) if r.disallowed_loss is not None else "",
                r.linked_closure_id or "",
                r.when_realized_raw or "",
                r.when_reopened_raw or "",
            ]
            for r in rows
        ],
    )
    fn = f"broker_wash_sales_{scope}_{year}.csv"
    return Response(content=csv_text, media_type="text/csv", headers={"Content-Disposition": f'attachment; filename="{fn}"'})
