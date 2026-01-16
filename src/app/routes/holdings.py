from __future__ import annotations

import datetime as dt
import json
import os
from pathlib import Path

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import JSONResponse, RedirectResponse
from sqlalchemy import func
from sqlalchemy.orm import Session

from src.app.auth import auth_banner_message, require_actor
from src.app.db import db_session
from src.app.utils import jsonable
from src.db.audit import log_change
from src.db.models import (
    Account,
    BullionHolding,
    BucketAssignment,
    BucketPolicy,
    CashBalance,
    IncomeEvent,
    PositionLot,
    Security,
    SubstituteGroup,
    TaxLot,
    Transaction,
)

router = APIRouter(prefix="/holdings", tags=["holdings"])

@router.get("/drilldown.json")
def holdings_drilldown_json(
    request: Request,
    session: Session = Depends(db_session),
    _actor: str = Depends(require_actor),
):
    """
    Lazy-load lots + position detail for a single holding row (UI drill-down).

    UI-only: must not change valuation logic; reuses `build_holdings_view` for position-level values.
    """
    from src.core.dashboard_service import parse_scope
    from src.core.external_holdings import build_holdings_view
    from src.utils.money import format_usd

    scope = parse_scope(request.query_params.get("scope"))
    account_id_raw = (request.query_params.get("account_id") or "").strip()
    symbol_raw = (request.query_params.get("symbol") or "").strip()

    if not account_id_raw.isdigit():
        return JSONResponse({"error": "Missing/invalid account_id"}, status_code=400)
    account_id = int(account_id_raw)
    symbol = symbol_raw.upper()
    if not symbol:
        return JSONResponse({"error": "Missing symbol"}, status_code=400)

    today = dt.date.today()
    view = build_holdings_view(session, scope=scope, account_id=account_id, today=today, prices_dir=Path("./data/prices"))
    pos = next((p for p in (view.positions or []) if int(p.account_id or 0) == int(account_id) and str(p.symbol or "").upper() == symbol), None)
    if pos is None:
        return JSONResponse({"error": "Holding not found for this account/symbol"}, status_code=404)

    px = float(pos.latest_price) if pos.latest_price is not None else None

    def _f(x: float | None) -> float | None:
        return None if x is None else float(x)

    lots_limit = 200
    lots_truncated = False
    lots_source = "none"
    lots: list[dict[str, object]] = []

    # Prefer reconstructed TaxLot lots (planning-grade) when available.
    tax_rows = (
        session.query(TaxLot, Security)
        .join(Security, Security.id == TaxLot.security_id)
        .filter(
            TaxLot.account_id == account_id,
            TaxLot.source == "RECONSTRUCTED",
            TaxLot.quantity_open > 0,
            Security.ticker == symbol,
        )
        .order_by(TaxLot.acquired_date.asc(), TaxLot.id.asc())
        .limit(lots_limit + 1)
        .all()
    )
    if tax_rows:
        lots_source = "tax_lots"
        if len(tax_rows) > lots_limit:
            lots_truncated = True
            tax_rows = tax_rows[:lots_limit]
        for lot, _sec in tax_rows:
            qty = float(lot.quantity_open or 0.0)
            basis = float(lot.basis_open) if lot.basis_open is not None else None
            acquired = lot.acquired_date
            days_held = (today - acquired).days
            term = "LT" if days_held >= 365 else "ST"
            current_value = (qty * px) if (px is not None) else None
            gain = (current_value - basis) if (current_value is not None and basis is not None) else None
            gain_pct = (gain / basis) if (gain is not None and basis and abs(basis) > 1e-9) else None
            lots.append(
                {
                    "lot_id": int(lot.id),
                    "acquired_date": acquired,
                    "qty": qty,
                    "cost_basis": basis,
                    "current_value": current_value,
                    "gain": gain,
                    "gain_pct": gain_pct,
                    "days_held": int(days_held),
                    "term": term,
                }
            )

    # Fallback: manual PositionLots (used for offline accounts / bullion cost basis).
    if not lots:
        pos_rows = (
            session.query(PositionLot)
            .filter(PositionLot.account_id == account_id, PositionLot.ticker == symbol)
            .order_by(PositionLot.acquisition_date.asc(), PositionLot.id.asc())
            .limit(lots_limit + 1)
            .all()
        )
        if pos_rows:
            lots_source = "position_lots"
            if len(pos_rows) > lots_limit:
                lots_truncated = True
                pos_rows = pos_rows[:lots_limit]
            for lot in pos_rows:
                qty = float(lot.qty or 0.0)
                basis = float(lot.adjusted_basis_total) if lot.adjusted_basis_total is not None else float(lot.basis_total or 0.0)
                acquired = lot.acquisition_date
                days_held = (today - acquired).days
                term = "LT" if days_held >= 365 else "ST"
                current_value = (qty * px) if (px is not None) else None
                gain = (current_value - basis) if (current_value is not None) else None
                gain_pct = (gain / basis) if (gain is not None and basis and abs(basis) > 1e-9) else None
                lots.append(
                    {
                        "lot_id": int(lot.id),
                        "acquired_date": acquired,
                        "qty": qty,
                        "cost_basis": basis,
                        "current_value": current_value,
                        "gain": gain,
                        "gain_pct": gain_pct,
                        "days_held": int(days_held),
                        "term": term,
                    }
                )

    st_gain = 0.0
    lt_gain = 0.0
    st_qty = 0.0
    lt_qty = 0.0
    missing_basis_lots = 0
    for r in lots:
        term = str(r.get("term") or "")
        qty = float(r.get("qty") or 0.0)
        if term == "LT":
            lt_qty += qty
        elif term == "ST":
            st_qty += qty
        gain = r.get("gain")
        if gain is None:
            cb = r.get("cost_basis")
            if cb is None:
                missing_basis_lots += 1
            continue
        g = float(gain)
        if term == "LT":
            lt_gain += g
        elif term == "ST":
            st_gain += g

    wash_exit = pos.wash_safe_exit_date
    is_wash_safe = bool(wash_exit is not None and wash_exit <= today)

    out = {
        "scope": scope,
        "account_id": account_id,
        "symbol": symbol,
        "today": today,
        "position": {
            "account_name": pos.account_name,
            "taxpayer_type": pos.taxpayer_type,
            "as_of": pos.as_of,
            "qty": _f(pos.qty),
            "price": px,
            "market_value": _f(pos.market_value),
            "cost_basis_total": _f(pos.cost_basis_total),
            "pnl_amount": _f(pos.pnl_amount),
            "pnl_pct": _f(pos.pnl_pct),
            "formatted": {
                "price": ("—" if px is None else format_usd(px)),
                "market_value": format_usd(pos.market_value or 0.0),
                "cost_basis_total": ("—" if pos.cost_basis_total is None else format_usd(pos.cost_basis_total or 0.0)),
                "pnl_amount": ("—" if pos.pnl_amount is None else format_usd(pos.pnl_amount or 0.0)),
                "pnl_pct": ("—" if pos.pnl_pct is None else f"{(float(pos.pnl_pct) * 100.0):.2f}%"),
            },
        },
        "lots_source": lots_source,
        "lots_truncated": lots_truncated,
        "lots": lots,
        "lots_summary": {
            "st_qty": st_qty,
            "lt_qty": lt_qty,
            "st_gain": st_gain,
            "lt_gain": lt_gain,
            "missing_basis_lots": missing_basis_lots,
        },
        "wash": {
            "wash_safe_exit_date": wash_exit,
            "status": ("SAFE" if is_wash_safe else ("RISK" if wash_exit is not None else "—")),
        },
    }
    return JSONResponse(content=jsonable(out))


@router.get("")
def holdings_readonly(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    from src.app.main import templates
    from src.core.dashboard_service import parse_scope
    from src.core.external_holdings import accounts_with_snapshot_positions, build_holdings_view

    scope = parse_scope(request.query_params.get("scope"))
    account_id_raw = (request.query_params.get("account_id") or "").strip()
    account_id = int(account_id_raw) if account_id_raw.isdigit() else None
    ok = (request.query_params.get("ok") or "").strip()
    error = (request.query_params.get("error") or "").strip()

    # IMPORTANT: `static_version` is normally set once at app startup. During development, that can cause
    # browsers to keep serving a cached `app.css` even after UI changes, making new components (e.g., KPI cards)
    # appear as plain stacked text. For the Holdings page, override `static_version` per-request so CSS/JS
    # updates are visible without a server restart.
    static_version: str = "0"
    try:
        css_path = Path(__file__).resolve().parents[1] / "static" / "app.css"
        static_version = str(int(css_path.stat().st_mtime))
    except Exception:
        static_version = "0"

    today = dt.date.today()
    view = build_holdings_view(session, scope=scope, account_id=account_id, today=today, prices_dir=Path("./data/prices"))
    auth_banner_detail = auth_banner_message()
    hints: list[str] = []
    if not view.positions:
        try:
            if account_id is not None:
                combined = build_holdings_view(session, scope=scope, account_id=None, today=today)
                if combined.positions:
                    hints.append("No positions for the selected portfolio; try 'Combined (all accounts)'.")
            if scope in {"trust", "personal"}:
                household = build_holdings_view(session, scope="household", account_id=None, today=today)
                if household.positions:
                    hints.append("No positions for this scope; try switching Scope to 'Household'.")
        except Exception:
            # Best-effort hints only; never break the holdings page.
            pass
    accounts_with_positions = accounts_with_snapshot_positions(session, scope=scope)
    # Manual holdings (lots + bullion) should count as "has holdings" in the selector too.
    try:
        accounts_with_positions |= {
            int(r[0]) for r in session.query(PositionLot.account_id).distinct().all() if r and r[0] is not None
        }
        accounts_with_positions |= {
            int(r[0]) for r in session.query(BullionHolding.account_id).distinct().all() if r and r[0] is not None
        }
    except Exception:
        pass

    # Account selector options (within scope).
    from src.db.models import TaxpayerEntity

    q = session.query(Account).join(TaxpayerEntity, TaxpayerEntity.id == Account.taxpayer_entity_id).order_by(Account.name)
    if scope == "trust":
        q = q.filter(TaxpayerEntity.type == "TRUST")
    elif scope == "personal":
        q = q.filter(TaxpayerEntity.type == "PERSONAL")
    accounts_all = q.all()
    acct_ids_all = [int(a.id) for a in accounts_all]

    # Hide "unused" portfolios on the Holdings page to avoid confusing empty selections.
    # Keep any currently-selected account visible even if it's otherwise unused.
    active_ids: set[int] = set()
    try:
        active_ids |= set(int(x) for x in (accounts_with_positions or set()))
        active_ids |= {
            int(r[0])
            for r in session.query(CashBalance.account_id)
            .filter(CashBalance.account_id.in_(acct_ids_all))
            .distinct()
            .all()
            if r and r[0] is not None
        }
        active_ids |= {
            int(r[0])
            for r in session.query(Transaction.account_id)
            .filter(Transaction.account_id.in_(acct_ids_all))
            .distinct()
            .all()
            if r and r[0] is not None
        }
        active_ids |= {
            int(r[0])
            for r in session.query(PositionLot.account_id)
            .filter(PositionLot.account_id.in_(acct_ids_all))
            .distinct()
            .all()
            if r and r[0] is not None
        }
        active_ids |= {
            int(r[0])
            for r in session.query(BullionHolding.account_id)
            .filter(BullionHolding.account_id.in_(acct_ids_all))
            .distinct()
            .all()
            if r and r[0] is not None
        }
        active_ids |= {
            int(r[0])
            for r in session.query(TaxLot.account_id)
            .filter(TaxLot.account_id.in_(acct_ids_all))
            .distinct()
            .all()
            if r and r[0] is not None
        }
    except Exception:
        active_ids = set(int(a.id) for a in accounts_all)

    if account_id is not None and active_ids and int(account_id) not in active_ids:
        # Selected portfolio appears unused; redirect to a sane default.
        return RedirectResponse(url=f"/holdings?scope={scope}", status_code=303)

    accounts = [
        a
        for a in accounts_all
        if int(a.id) in active_ids or (account_id is not None and int(a.id) == int(account_id))
    ]

    return templates.TemplateResponse(
        "holdings_readonly.html",
        {
            "request": request,
            "actor": actor,
            # Holdings page shows auth warning inline (non-intrusive) vs global banner.
            "auth_banner": None,
            "auth_banner_detail": auth_banner_detail,
            "static_version": static_version,
            "ok": ok,
            "error": error,
            "scope": scope,
            "scope_label": view.scope_label,
            "account_id": account_id,
            "accounts": accounts,
            "accounts_with_positions": accounts_with_positions,
            "view": view,
            "hints": hints,
            "today": today,
        },
    )


@router.post("/prices/refresh")
def holdings_refresh_prices(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    scope: str = Form(default="household"),
    account_id: str = Form(default=""),
    provider: str = Form(default="yahoo"),
):
    """
    Refresh local price cache for symbols currently displayed on the Holdings page.
    Uses Stooq daily candles (preferred) or Yahoo Finance chart API (may rate limit).
    """
    from src.core.benchmarks import download_yahoo_price_history_csv
    from src.core.dashboard_service import parse_scope
    from src.core.external_holdings import build_holdings_view
    from src.investor.marketdata.benchmarks import StooqProvider
    from market_data.symbols import normalize_ticker

    sc = parse_scope(scope)
    acct_id = int(account_id) if str(account_id).strip().isdigit() else None
    today = dt.date.today()
    provider_norm = (provider or "stooq").strip().lower()

    # Build holdings WITHOUT price override to avoid confusing the refresh list.
    view = build_holdings_view(session, scope=sc, account_id=acct_id, today=today, prices_dir=None)
    symbols = sorted({str(p.symbol or "").strip().upper() for p in (view.positions or []) if str(p.symbol or "").strip()})
    # Avoid synthetic/invalid symbols (CASH, TOTAL, etc.).
    priceable: list[tuple[str, str]] = []
    for sym in symbols:
        ns = normalize_ticker(sym, base_currency="USD")
        if ns.kind in {"invalid", "synthetic_cash"}:
            continue
        provider = ns.provider_ticker or sym
        priceable.append((sym, provider))

    # Refresh the last ~30 days; enough to capture the latest trading day.
    start = today - dt.timedelta(days=45)
    prices_dir = Path("./data/prices")
    fetched_at = dt.datetime.now(dt.timezone.utc).isoformat()

    updated = 0
    skipped = 0
    failed: list[tuple[str, str]] = []
    def _safe_err(msg: str) -> str:
        s = (msg or "").strip()
        if not s:
            return ""
        # Keep URLs out of UI messages; show only the tail.
        s = s.replace("https://", "").replace("http://", "")
        return (s[:220] + "…") if len(s) > 220 else s

    stooq = StooqProvider()
    for sym, provider_ticker in priceable:
        try:
            dest = prices_dir / f"{sym}.csv"
            if provider_norm == "cache":
                skipped += 1
                continue
            if provider_norm == "stooq":
                df = stooq.fetch(symbol=provider_ticker, start=start, end=today)
                # Write a simple, load_price_csv-compatible file.
                dest.parent.mkdir(parents=True, exist_ok=True)
                with dest.open("w", encoding="utf-8", newline="") as f:
                    f.write("Date,Close\n")
                    for ts, row in df.iterrows():
                        try:
                            d = ts.date().isoformat()
                            px = float(row.get("close") or 0.0)
                            if px > 0:
                                f.write(f"{d},{px}\n")
                        except Exception:
                            continue
                meta = {
                    "provider": "stooq_daily",
                    "provider_ticker": provider_ticker,
                    "original_ticker": sym,
                    "first_date": str(start),
                    "last_date": str(today),
                    "fetched_at": fetched_at,
                }
            else:
                res = download_yahoo_price_history_csv(
                    symbol=provider_ticker,
                    start_date=start,
                    end_date=today,
                    dest_path=dest,
                )
                meta = {"provider": "yahoo_chart", "provider_ticker": provider_ticker, "original_ticker": sym, "first_date": str(res.start_date), "last_date": str(res.end_date), "fetched_at": fetched_at}

            try:
                dest.with_suffix(".json").write_text(json.dumps(meta, indent=2, sort_keys=True), encoding="utf-8")
            except Exception:
                pass
            updated += 1
        except Exception as e:
            msg = _safe_err(str(e))
            if msg:
                failed.append((sym, f"{type(e).__name__}: {msg}"))
            else:
                failed.append((sym, type(e).__name__))

    base = f"/holdings?scope={sc}" + (f"&account_id={acct_id}" if acct_id is not None else "")
    if provider_norm == "cache":
        msg = f"Cache-only mode: no network fetch performed (skipped {skipped} symbol(s))."
        return RedirectResponse(url=f"{base}&ok={msg}", status_code=303)
    if failed and updated:
        sample = "; ".join([f"{s}: {m}" for s, m in failed[:4]])
        suffix = "…" if len(failed) > 4 else ""
        msg = f"Updated {updated} price file(s); failed {len(failed)}: {sample}{suffix}"
        return RedirectResponse(url=f"{base}&ok={msg}", status_code=303)
    if failed and not updated:
        sample = "; ".join([f"{s}: {m}" for s, m in failed[:4]])
        suffix = "…" if len(failed) > 4 else ""
        msg = f"Price refresh failed for {len(failed)} symbol(s): {sample}{suffix}"
        return RedirectResponse(url=f"{base}&error={msg}", status_code=303)
    return RedirectResponse(url=f"{base}&ok=Updated {updated} price file(s).", status_code=303)


@router.get("/securities")
def securities_list(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    securities = session.query(Security).order_by(Security.ticker).all()
    groups = session.query(SubstituteGroup).order_by(SubstituteGroup.name).all()
    from src.app.main import templates

    return templates.TemplateResponse(
        "securities.html",
        {
            "request": request,
            "actor": actor,
            "auth_banner": auth_banner_message(),
            "securities": securities,
            "groups": groups,
        },
    )


@router.post("/securities")
def securities_create(
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    ticker: str = Form(...),
    name: str = Form(...),
    asset_class: str = Form(...),
    expense_ratio: float = Form(default=0.0),
    substitute_group_id: str = Form(default=""),
    last_price: float = Form(default=0.0),
    note: str = Form(default=""),
):
    group_id = int(substitute_group_id) if substitute_group_id.strip() else None
    sec = Security(
        ticker=ticker.strip().upper(),
        name=name.strip(),
        asset_class=asset_class.strip().upper(),
        expense_ratio=float(expense_ratio or 0.0),
        substitute_group_id=group_id,
        metadata_json={"last_price": float(last_price or 0.0)},
    )
    session.add(sec)
    session.flush()
    log_change(
        session,
        actor=actor,
        action="CREATE",
        entity="Security",
        entity_id=str(sec.id),
        old=None,
        new=jsonable({"ticker": sec.ticker, "asset_class": sec.asset_class, "expense_ratio": sec.expense_ratio}),
        note=note or "Create security",
    )
    session.commit()
    return RedirectResponse(url="/holdings/securities", status_code=303)


@router.post("/groups")
def groups_create(
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    name: str = Form(...),
    description: str = Form(default=""),
    note: str = Form(default=""),
):
    grp = SubstituteGroup(name=name.strip(), description=description.strip() or None)
    session.add(grp)
    session.flush()
    log_change(
        session,
        actor=actor,
        action="CREATE",
        entity="SubstituteGroup",
        entity_id=str(grp.id),
        old=None,
        new=jsonable({"name": grp.name}),
        note=note or "Create substitute group",
    )
    session.commit()
    return RedirectResponse(url="/holdings/securities", status_code=303)


@router.get("/lots")
def lots_list(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    accounts = session.query(Account).order_by(Account.name).all()
    lots = session.query(PositionLot).order_by(PositionLot.acquisition_date.desc()).limit(500).all()
    from src.app.main import templates

    return templates.TemplateResponse(
        "lots.html",
        {
            "request": request,
            "actor": actor,
            "auth_banner": auth_banner_message(),
            "accounts": accounts,
            "lots": lots,
            "today": dt.date.today().isoformat(),
        },
    )


@router.post("/lots")
def lots_create(
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    account_id: int = Form(...),
    ticker: str = Form(...),
    acquisition_date: str = Form(...),
    qty: float = Form(...),
    basis_total: float = Form(...),
    adjusted_basis_total: str = Form(default=""),
    note: str = Form(default=""),
):
    lot = PositionLot(
        account_id=account_id,
        ticker=ticker.strip().upper(),
        acquisition_date=dt.date.fromisoformat(acquisition_date),
        qty=float(qty),
        basis_total=float(basis_total),
        adjusted_basis_total=float(adjusted_basis_total) if adjusted_basis_total.strip() else None,
    )
    session.add(lot)
    session.flush()
    log_change(
        session,
        actor=actor,
        action="CREATE",
        entity="PositionLot",
        entity_id=str(lot.id),
        old=None,
        new=jsonable(
            {
                "account_id": lot.account_id,
                "ticker": lot.ticker,
                "acquisition_date": lot.acquisition_date.isoformat(),
                "qty": float(lot.qty),
                "basis_total": float(lot.basis_total),
            }
        ),
        note=note or "Create lot",
    )
    session.commit()
    return RedirectResponse(url="/holdings/lots", status_code=303)


@router.get("/cash")
def cash_list(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    accounts = session.query(Account).order_by(Account.name).all()
    balances = session.query(CashBalance).order_by(CashBalance.as_of_date.desc()).limit(200).all()
    from src.app.main import templates

    return templates.TemplateResponse(
        "cash.html",
        {
            "request": request,
            "actor": actor,
            "auth_banner": auth_banner_message(),
            "accounts": accounts,
            "balances": balances,
            "today": dt.date.today().isoformat(),
        },
    )


@router.post("/cash")
def cash_create(
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    account_id: int = Form(...),
    as_of_date: str = Form(...),
    amount: float = Form(...),
    note: str = Form(default=""),
):
    cb = CashBalance(account_id=account_id, as_of_date=dt.date.fromisoformat(as_of_date), amount=float(amount))
    session.add(cb)
    session.flush()
    log_change(
        session,
        actor=actor,
        action="CREATE",
        entity="CashBalance",
        entity_id=str(cb.id),
        old=None,
        new=jsonable({"account_id": cb.account_id, "as_of_date": cb.as_of_date.isoformat(), "amount": float(cb.amount)}),
        note=note or "Create cash balance",
    )
    session.commit()
    return RedirectResponse(url="/holdings/cash", status_code=303)


@router.get("/income")
def income_list(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    accounts = session.query(Account).order_by(Account.name).all()
    events = session.query(IncomeEvent).order_by(IncomeEvent.date.desc()).limit(300).all()
    from src.app.main import templates

    return templates.TemplateResponse(
        "income.html",
        {
            "request": request,
            "actor": actor,
            "auth_banner": auth_banner_message(),
            "accounts": accounts,
            "events": events,
            "today": dt.date.today().isoformat(),
        },
    )


@router.post("/income")
def income_create(
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    account_id: int = Form(...),
    date: str = Form(...),
    type: str = Form(...),
    ticker: str = Form(default=""),
    amount: float = Form(...),
    note: str = Form(default=""),
):
    ev = IncomeEvent(
        account_id=account_id,
        date=dt.date.fromisoformat(date),
        type=type.strip().upper(),
        ticker=ticker.strip().upper() or None,
        amount=float(amount),
    )
    session.add(ev)
    session.flush()
    log_change(
        session,
        actor=actor,
        action="CREATE",
        entity="IncomeEvent",
        entity_id=str(ev.id),
        old=None,
        new=jsonable({"account_id": ev.account_id, "date": ev.date.isoformat(), "type": ev.type, "amount": float(ev.amount)}),
        note=note or "Create income event",
    )
    session.commit()
    return RedirectResponse(url="/holdings/income", status_code=303)


@router.get("/bullion")
def bullion_list(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    from src.app.main import templates
    from src.core.dashboard_service import parse_scope
    from src.db.models import TaxpayerEntity

    scope = parse_scope(request.query_params.get("scope"))
    account_id_raw = (request.query_params.get("account_id") or "").strip()
    account_id = int(account_id_raw) if account_id_raw.isdigit() else None
    ok = (request.query_params.get("ok") or "").strip()
    error = (request.query_params.get("error") or "").strip()

    # Only MANUAL accounts are eligible for bullion tracking.
    q = (
        session.query(Account)
        .join(TaxpayerEntity, TaxpayerEntity.id == Account.taxpayer_entity_id)
        .filter(Account.broker == "MANUAL")
    )
    if scope == "trust":
        q = q.filter(TaxpayerEntity.type == "TRUST")
    elif scope == "personal":
        q = q.filter(TaxpayerEntity.type == "PERSONAL")
    accounts = q.order_by(Account.name).all()
    acct_ids = [int(a.id) for a in accounts]

    if account_id is not None and account_id not in acct_ids:
        account_id = None

    holdings_q = session.query(BullionHolding)
    if acct_ids:
        holdings_q = holdings_q.filter(BullionHolding.account_id.in_(acct_ids))
    else:
        holdings_q = holdings_q.filter(BullionHolding.id == -1)
    if account_id is not None:
        holdings_q = holdings_q.filter(BullionHolding.account_id == int(account_id))
    holdings = holdings_q.order_by(BullionHolding.account_id.asc(), BullionHolding.metal.asc()).all()

    return templates.TemplateResponse(
        "bullion.html",
        {
            "request": request,
            "actor": actor,
            "auth_banner": auth_banner_message(),
            "ok": ok,
            "error": error,
            "scope": scope,
            "account_id": account_id,
            "accounts": accounts,
            "account_name_by_id": {int(a.id): a.name for a in accounts},
            "holdings": holdings,
            "today": dt.date.today().isoformat(),
        },
    )


@router.post("/bullion")
def bullion_upsert(
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    scope: str = Form(default="household"),
    account_id: int = Form(...),
    metal: str = Form(...),
    quantity: float = Form(...),
    unit: str = Form(default="oz"),
    unit_price: float = Form(...),
    cost_basis_total: str = Form(default=""),
    currency: str = Form(default="USD"),
    as_of_date: str = Form(...),
    note: str = Form(default=""),
):
    metal_u = (metal or "").strip().upper()
    if metal_u not in {"GOLD", "SILVER"}:
        return RedirectResponse(url="/holdings/bullion?error=Invalid+metal", status_code=303)
    acct = session.query(Account).filter(Account.id == int(account_id)).one_or_none()
    if acct is None:
        return RedirectResponse(url="/holdings/bullion?error=Account+not+found", status_code=303)
    if (acct.broker or "").upper() != "MANUAL":
        return RedirectResponse(url="/holdings/bullion?error=Bullion+requires+a+MANUAL+account", status_code=303)

    d = dt.date.fromisoformat(as_of_date)
    unit_u = (unit or "").strip() or "oz"
    ccy_u = (currency or "USD").strip().upper() or "USD"
    if ccy_u != "USD":
        return RedirectResponse(url="/holdings/bullion?error=Only+USD+is+supported+for+now", status_code=303)

    cost_basis = None
    if str(cost_basis_total or "").strip():
        try:
            cost_basis = float(str(cost_basis_total).strip().replace(",", "").replace("$", ""))
        except Exception:
            return RedirectResponse(url="/holdings/bullion?error=Invalid+cost+basis", status_code=303)

    existing = (
        session.query(BullionHolding)
        .filter(BullionHolding.account_id == int(account_id), BullionHolding.metal == metal_u)
        .one_or_none()
    )
    if existing is None:
        bh = BullionHolding(
            account_id=int(account_id),
            metal=metal_u,
            quantity=float(quantity),
            unit=unit_u,
            unit_price=float(unit_price),
            cost_basis_total=cost_basis,
            currency=ccy_u,
            as_of_date=d,
            notes=note.strip() or None,
        )
        session.add(bh)
        session.flush()
        log_change(
            session,
            actor=actor,
            action="CREATE",
            entity="BullionHolding",
            entity_id=str(bh.id),
            old=None,
            new=jsonable(
                {
                    "account_id": bh.account_id,
                    "metal": bh.metal,
                    "quantity": float(bh.quantity),
                    "unit": bh.unit,
                    "unit_price": float(bh.unit_price),
                    "cost_basis_total": float(bh.cost_basis_total) if bh.cost_basis_total is not None else None,
                    "currency": bh.currency,
                    "as_of_date": bh.as_of_date.isoformat(),
                }
            ),
            note=note or "Create bullion holding",
        )
        session.commit()
        return RedirectResponse(url=f"/holdings/bullion?scope={scope}&account_id={account_id}&ok=Saved", status_code=303)

    old = jsonable(
        {
            "account_id": existing.account_id,
            "metal": existing.metal,
            "quantity": float(existing.quantity),
            "unit": existing.unit,
            "unit_price": float(existing.unit_price),
            "cost_basis_total": float(existing.cost_basis_total) if existing.cost_basis_total is not None else None,
            "currency": existing.currency,
            "as_of_date": existing.as_of_date.isoformat(),
        }
    )
    existing.quantity = float(quantity)
    existing.unit = unit_u
    existing.unit_price = float(unit_price)
    existing.cost_basis_total = cost_basis
    existing.currency = ccy_u
    existing.as_of_date = d
    existing.notes = note.strip() or None
    existing.updated_at = dt.datetime.now(dt.timezone.utc)
    session.flush()
    new = jsonable(
        {
            "account_id": existing.account_id,
            "metal": existing.metal,
            "quantity": float(existing.quantity),
            "unit": existing.unit,
            "unit_price": float(existing.unit_price),
            "cost_basis_total": float(existing.cost_basis_total) if existing.cost_basis_total is not None else None,
            "currency": existing.currency,
            "as_of_date": existing.as_of_date.isoformat(),
        }
    )
    log_change(
        session,
        actor=actor,
        action="UPDATE",
        entity="BullionHolding",
        entity_id=str(existing.id),
        old=old,
        new=new,
        note=note or "Update bullion holding",
    )
    session.commit()
    return RedirectResponse(url=f"/holdings/bullion?scope={scope}&account_id={account_id}&ok=Saved", status_code=303)


@router.post("/bullion/{bullion_id}/delete")
def bullion_delete(
    bullion_id: int,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    scope: str = Form(default="household"),
    account_id: str = Form(default=""),
    note: str = Form(default=""),
):
    bh = session.query(BullionHolding).filter(BullionHolding.id == int(bullion_id)).one_or_none()
    if bh is None:
        return RedirectResponse(url="/holdings/bullion?error=Not+found", status_code=303)
    old = jsonable(
        {
            "account_id": bh.account_id,
            "metal": bh.metal,
            "quantity": float(bh.quantity),
            "unit": bh.unit,
            "unit_price": float(bh.unit_price),
            "cost_basis_total": float(bh.cost_basis_total) if bh.cost_basis_total is not None else None,
            "currency": bh.currency,
            "as_of_date": bh.as_of_date.isoformat(),
        }
    )
    session.delete(bh)
    session.flush()
    log_change(
        session,
        actor=actor,
        action="DELETE",
        entity="BullionHolding",
        entity_id=str(bullion_id),
        old=old,
        new=None,
        note=note or "Delete bullion holding",
    )
    session.commit()
    acct_q = f"&account_id={account_id}" if str(account_id).strip().isdigit() else ""
    return RedirectResponse(url=f"/holdings/bullion?scope={scope}{acct_q}&ok=Deleted", status_code=303)


@router.get("/transactions")
def transactions_list(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    accounts = session.query(Account).order_by(Account.name).all()
    acct_by_id = {a.id: a for a in accounts}

    # Optional source filters:
    # - source=all|manual|imported (default all)
    # - connection_id=<id> to show only txns imported via that connection.
    source = (request.query_params.get("source") or "all").strip().lower()
    connection_id_raw = (request.query_params.get("connection_id") or "").strip()
    connection_id = int(connection_id_raw) if connection_id_raw.isdigit() else None

    from src.db.models import ExternalConnection, ExternalTransactionMap

    connections = session.query(ExternalConnection).order_by(ExternalConnection.id.desc()).all()

    q = (
        session.query(Transaction, ExternalTransactionMap, ExternalConnection)
        .outerjoin(ExternalTransactionMap, ExternalTransactionMap.transaction_id == Transaction.id)
        .outerjoin(ExternalConnection, ExternalConnection.id == ExternalTransactionMap.connection_id)
    )
    if connection_id is not None:
        q = q.filter(ExternalTransactionMap.connection_id == connection_id)
        source = "imported"
    elif source == "manual":
        q = q.filter(ExternalTransactionMap.id.is_(None))
    elif source == "imported":
        q = q.filter(ExternalTransactionMap.id.is_not(None))

    page_raw = (request.query_params.get("page") or "").strip()
    page = 1
    try:
        if page_raw.isdigit():
            page = max(1, int(page_raw))
    except Exception:
        page = 1
    page_size = 50

    total = int(q.with_entities(func.count(Transaction.id)).scalar() or 0)
    pages = max(1, int((total + page_size - 1) // page_size))
    if page > pages:
        page = pages
    offset = int((page - 1) * page_size)

    rows = q.order_by(Transaction.date.desc(), Transaction.id.desc()).limit(page_size).offset(offset).all()

    min_date, max_date = q.with_entities(func.min(Transaction.date), func.max(Transaction.date)).one()
    from src.app.main import templates

    return templates.TemplateResponse(
        "transactions.html",
        {
            "request": request,
            "actor": actor,
            "auth_banner": auth_banner_message(),
            "accounts": accounts,
            "acct_by_id": acct_by_id,
            "rows": rows,
            "connections": connections,
            "source": source,
            "connection_id": connection_id,
            "page": page,
            "pages": pages,
            "page_size": page_size,
            "total": total,
            "min_date": min_date,
            "max_date": max_date,
            "today": dt.date.today().isoformat(),
        },
    )


@router.post("/transactions")
def transactions_create(
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    account_id: int = Form(...),
    date: str = Form(...),
    type: str = Form(...),
    ticker: str = Form(default=""),
    qty: str = Form(default=""),
    amount: float = Form(...),
    lot_basis_total: str = Form(default=""),
    lot_acquisition_date: str = Form(default=""),
    term: str = Form(default=""),
    note: str = Form(default=""),
):
    links = {}
    if lot_basis_total.strip():
        links["basis_total"] = float(lot_basis_total)
    if lot_acquisition_date.strip():
        links["acquisition_date"] = lot_acquisition_date.strip()
    if term.strip():
        links["term"] = term.strip().upper()
    tx = Transaction(
        account_id=account_id,
        date=dt.date.fromisoformat(date),
        type=type.strip().upper(),
        ticker=ticker.strip().upper() or None,
        qty=float(qty) if qty.strip() else None,
        amount=float(amount),
        lot_links_json=links,
    )
    session.add(tx)
    session.flush()
    log_change(
        session,
        actor=actor,
        action="CREATE",
        entity="Transaction",
        entity_id=str(tx.id),
        old=None,
        new=jsonable({"account_id": tx.account_id, "date": tx.date.isoformat(), "type": tx.type, "amount": float(tx.amount)}),
        note=note or "Create transaction",
    )
    session.commit()
    return RedirectResponse(url="/holdings/transactions", status_code=303)


@router.get("/assignments")
def assignments_list(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    policy = session.query(BucketPolicy).order_by(BucketPolicy.effective_date.desc()).first()
    securities = session.query(Security).order_by(Security.ticker).all()
    assignments = []
    if policy:
        assignments = session.query(BucketAssignment).filter(BucketAssignment.policy_id == policy.id).all()
    map_by_ticker = {a.ticker: a.bucket_code for a in assignments}
    from src.app.main import templates

    return templates.TemplateResponse(
        "assignments.html",
        {
            "request": request,
            "actor": actor,
            "auth_banner": auth_banner_message(),
            "policy": policy,
            "securities": securities,
            "map_by_ticker": map_by_ticker,
        },
    )


@router.post("/assignments")
def assignments_upsert(
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    ticker: str = Form(...),
    bucket_code: str = Form(...),
    note: str = Form(default=""),
):
    policy = session.query(BucketPolicy).order_by(BucketPolicy.effective_date.desc()).first()
    if policy is None:
        return RedirectResponse(url="/policy/new", status_code=303)

    ticker_u = ticker.strip().upper()
    existing = (
        session.query(BucketAssignment)
        .filter(BucketAssignment.policy_id == policy.id, BucketAssignment.ticker == ticker_u)
        .one_or_none()
    )
    old = jsonable({"ticker": ticker_u, "bucket_code": existing.bucket_code}) if existing else None
    if existing:
        existing.bucket_code = bucket_code.strip().upper()
        entity_id = str(existing.id)
        action = "UPDATE"
        new = jsonable({"ticker": existing.ticker, "bucket_code": existing.bucket_code})
    else:
        ba = BucketAssignment(policy_id=policy.id, ticker=ticker_u, bucket_code=bucket_code.strip().upper())
        session.add(ba)
        session.flush()
        entity_id = str(ba.id)
        action = "CREATE"
        new = jsonable({"ticker": ba.ticker, "bucket_code": ba.bucket_code})

    log_change(
        session,
        actor=actor,
        action=action,
        entity="BucketAssignment",
        entity_id=entity_id,
        old=old,
        new=new,
        note=note or "Upsert bucket assignment",
    )
    session.commit()
    return RedirectResponse(url="/holdings/assignments", status_code=303)
