from __future__ import annotations

import datetime as dt
import hashlib
import io
import json
import re
import shutil
from pathlib import Path

from fastapi import APIRouter, Depends, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, RedirectResponse, Response
from sqlalchemy import and_, case, func, or_
from sqlalchemy.orm import Session

from src.app.auth import auth_banner_message, require_actor
from src.app.db import db_session
from src.core.connection_preference import preferred_active_connection_ids_for_scope
from src.core.dashboard_service import parse_scope, scope_label
from src.db.models import (
    Account,
    ExternalAccountMap,
    ExternalConnection,
    ExternalHoldingSnapshot,
    ExternalTransactionMap,
    TaxpayerEntity,
    Transaction,
)


router = APIRouter(prefix="/reports", tags=["reports"])

_RUN_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,80}$")


def _monthly_reports_root() -> Path:
    root = Path("data") / "monthly_reports"
    root.mkdir(parents=True, exist_ok=True)
    return root


def _safe_run_id(run_id: str) -> str:
    rid = (run_id or "").strip()
    if not _RUN_ID_RE.match(rid):
        raise HTTPException(status_code=400, detail="Invalid run id.")
    return rid


def _safe_filename(name: str) -> str:
    n = (name or "").strip()
    if not n or n != Path(n).name or "/" in n or "\\" in n:
        raise HTTPException(status_code=400, detail="Invalid filename.")
    return n


def _read_small_json(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _parse_iso_date(s: str, *, label: str) -> dt.date:
    try:
        return dt.date.fromisoformat((s or "").strip()[:10])
    except Exception:
        raise HTTPException(status_code=400, detail=f"Invalid {label} date (expected YYYY-MM-DD).")


def _parse_reports_scope(raw: str | None) -> str:
    v = (raw or "").strip().lower()
    if v == "ira":
        return "ira"
    return parse_scope(raw)

def _reports_scope_label(scope: str) -> str:
    if scope == "ira":
        return "IRA only"
    return scope_label(scope)  # type: ignore[arg-type]

def _scope_account_predicates(scope: str):
    if scope == "trust":
        return [TaxpayerEntity.type == "TRUST", Account.account_type != "IRA"]
    if scope == "personal":
        return [TaxpayerEntity.type == "PERSONAL", Account.account_type != "IRA"]
    if scope == "ira":
        return [Account.account_type == "IRA"]
    return []


def _is_internal_transfer_expr():
    """
    SQL predicate to exclude internal sweeps/FX/multi-currency shuttles from cashflow reporting.
    (We store details in Transaction.lot_links_json.)
    """
    desc = func.upper(func.coalesce(func.json_extract(Transaction.lot_links_json, "$.description"), ""))
    addl = func.upper(func.coalesce(func.json_extract(Transaction.lot_links_json, "$.additional_detail"), ""))
    raw = func.upper(func.coalesce(func.json_extract(Transaction.lot_links_json, "$.raw_type"), ""))
    txt = desc + " " + addl
    return or_(
        # Chase IRA deposit sweep between internal sub-accounts (not investor deposits/withdrawals).
        func.instr(txt, "DEPOSIT SWEEP") > 0,
        func.instr(txt, "SHADO") > 0,
        func.instr(txt, "REC FR SIS") > 0,
        func.instr(txt, "REC TRSF SIS") > 0,
        func.instr(txt, "TRSF SIS") > 0,
        and_(raw == "UNKNOWN", func.instr(txt, "MULTI") > 0, func.instr(txt, "CURRENCY") > 0),
        and_(func.instr(txt, "FX") > 0, or_(func.instr(txt, "SETTLEMENT") > 0, func.instr(txt, "TRAD") > 0, func.instr(txt, "TRADE") > 0)),
    )

def _portfolio_options_for_scope(session: Session, *, scope: str) -> list[dict[str, object]]:
    # Avoid showing duplicate/legacy connectors in report selection (e.g. IB Flex Offline vs Web).
    pref_scope = "household" if (scope or "").strip().lower() == "ira" else scope
    preferred_conn_ids = preferred_active_connection_ids_for_scope(session, scope=pref_scope)
    q = (
        session.query(
            ExternalConnection.id,
            ExternalConnection.name,
            TaxpayerEntity.name,
            TaxpayerEntity.type,
        )
        .select_from(ExternalAccountMap)
        .join(ExternalConnection, ExternalConnection.id == ExternalAccountMap.connection_id)
        .join(Account, Account.id == ExternalAccountMap.account_id)
        .join(TaxpayerEntity, TaxpayerEntity.id == Account.taxpayer_entity_id)
        .filter(ExternalConnection.status == "ACTIVE")
        .filter(ExternalConnection.id.in_(sorted(preferred_conn_ids)) if preferred_conn_ids else ExternalConnection.id == -1)
        .filter(*_scope_account_predicates(scope))
        .order_by(ExternalConnection.name.asc(), ExternalConnection.id.asc())
        .all()
    )
    by_conn: dict[int, dict[str, object]] = {}
    taxpayers_by_conn: dict[int, set[str]] = {}
    types_by_conn: dict[int, set[str]] = {}
    for cid, cname, tp_name, tp_type in q:
        by_conn[int(cid)] = {"id": int(cid), "conn_name": str(cname or f"Connection {cid}")}
        taxpayers_by_conn.setdefault(int(cid), set()).add(str(tp_name or "").strip() or "—")
        types_by_conn.setdefault(int(cid), set()).add(str(tp_type or "").strip() or "—")
    out: list[dict[str, object]] = []
    for cid in sorted(by_conn.keys(), key=lambda x: (str(by_conn[x]["conn_name"]), x)):
        conn_name = str(by_conn[cid]["conn_name"])
        tp_names = sorted(taxpayers_by_conn.get(cid) or [])
        tp_types = sorted(types_by_conn.get(cid) or [])
        if len(tp_names) == 1:
            label = f"{conn_name} — {tp_names[0]}"
        else:
            label = f"{conn_name} — Mixed"
        if len(tp_types) == 1:
            label = f"{label} ({tp_types[0]})"
        out.append({"id": int(cid), "name": label})
    return out


def _account_options_for_scope(session: Session, *, scope: str, portfolio_id: int | None) -> list[dict[str, object]]:
    q = (
        session.query(Account.id, Account.name, TaxpayerEntity.name, TaxpayerEntity.type)
        .select_from(Account)
        .join(TaxpayerEntity, TaxpayerEntity.id == Account.taxpayer_entity_id)
        .filter(*_scope_account_predicates(scope))
        .order_by(Account.name.asc(), Account.id.asc())
    )
    if portfolio_id is not None:
        q = q.join(ExternalAccountMap, ExternalAccountMap.account_id == Account.id).filter(
            ExternalAccountMap.connection_id == int(portfolio_id)
        )
    out: list[dict[str, object]] = []
    for aid, aname, tp_name, tp_type in q.all():
        out.append({"id": int(aid), "name": f"{aname} — {tp_name} ({tp_type})"})
    return out


def _year_from_any(v: object) -> int | None:
    if v is None:
        return None
    if isinstance(v, dt.datetime):
        return int(v.year)
    if isinstance(v, dt.date):
        return int(v.year)
    s = str(v).strip()
    if not s:
        return None
    try:
        return int(dt.date.fromisoformat(s[:10]).year)
    except Exception:
        return None


def _available_year_options(
    session: Session,
    *,
    scope: str,
    today: dt.date,
    kind: str,
    fallback_years_back: int = 15,
) -> list[int]:
    """
    Return a descending list of calendar years that have data relevant to a report.
    Falls back to [today.year .. today.year-fallback_years_back] when no data exists.
    """
    years: set[int] = set()

    # Transactions (optionally transfers only).
    tq = session.query(func.min(Transaction.date), func.max(Transaction.date)).select_from(Transaction).join(
        Account, Account.id == Transaction.account_id
    ).join(TaxpayerEntity, TaxpayerEntity.id == Account.taxpayer_entity_id)
    tq = tq.filter(*_scope_account_predicates(scope))
    if kind == "withdrawals":
        tq = tq.filter(Transaction.type == "TRANSFER", Transaction.amount < 0)
    tmin, tmax = tq.one()
    for x in (tmin, tmax):
        y = _year_from_any(x)
        if y is not None:
            years.add(y)

    # Holdings snapshots (for performance).
    if kind in {"performance", "all"}:
        # Scope filtering is applied at the mapped-account layer (connection may have mixed accounts).
        conn_ids = (
            session.query(ExternalAccountMap.connection_id)
            .join(Account, Account.id == ExternalAccountMap.account_id)
            .join(TaxpayerEntity, TaxpayerEntity.id == Account.taxpayer_entity_id)
            .filter(*_scope_account_predicates(scope))
            .distinct()
            .all()
        )
        ids = [int(r[0]) for r in conn_ids if r and r[0] is not None]
        hq = (
            session.query(func.min(ExternalHoldingSnapshot.as_of), func.max(ExternalHoldingSnapshot.as_of))
            .select_from(ExternalHoldingSnapshot)
            .join(ExternalConnection, ExternalConnection.id == ExternalHoldingSnapshot.connection_id)
            .filter(ExternalConnection.status == "ACTIVE")
        )
        if ids:
            hq = hq.filter(ExternalHoldingSnapshot.connection_id.in_(ids))
        hmin, hmax = hq.one()
        for x in (hmin, hmax):
            y = _year_from_any(x)
            if y is not None:
                years.add(y)

    if not years:
        y = int(today.year)
        return [y - i for i in range(0, max(1, int(fallback_years_back) + 1))]

    min_y = min(years)
    max_y = max(years)
    return list(range(int(max_y), int(min_y) - 1, -1))


@router.get("/monthly")
def reports_monthly_home(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    from src.app.main import templates

    scope = _parse_reports_scope(request.query_params.get("scope"))
    portfolio_options = _portfolio_options_for_scope(session, scope=scope)

    root = _monthly_reports_root()
    runs: list[dict[str, object]] = []
    for d in sorted([p for p in root.iterdir() if p.is_dir()], key=lambda p: p.name, reverse=True)[:40]:
        meta = _read_small_json(d / "run_metadata.json")
        if not meta:
            continue
        summary = meta.get("summary") or {}
        runs.append(
            {
                "run_id": d.name,
                "start_date": meta.get("start_date"),
                "end_date": meta.get("end_date"),
                "asof": meta.get("asof"),
                "benchmark": meta.get("benchmark"),
                "twr_ytd": summary.get("twr_ytd"),
                "xirr": summary.get("xirr"),
                "warnings_count": summary.get("warnings_count"),
            }
        )

    return templates.TemplateResponse(
        "reports_monthly.html",
        {
            "request": request,
            "actor": actor,
            "auth_banner": auth_banner_message(),
            "scope": scope,
            "scope_label": _reports_scope_label(scope),
            "portfolio_options": portfolio_options,
            "runs": runs,
            "default_start": "2025-01-01",
            "default_end": "2025-12-31",
            "default_asof": "2025-12-31",
            "default_benchmark": "SPY",
            "error_msg": request.query_params.get("error"),
            "ok_msg": request.query_params.get("ok"),
        },
    )


@router.post("/monthly/run_db")
def reports_monthly_run_db(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    scope: str = Form("household"),
    portfolio_id: int = Form(...),
    start: str = Form("2025-01-01"),
    end: str = Form("2025-12-31"),
    asof: str = Form("2025-12-31"),
    benchmark: str = Form("VOO"),
    download_prices: bool = Form(False),
    include_fees_as_flow: bool = Form(False),
):
    from src.core.monthly_report_source import build_monthly_report_inputs_from_db
    from portfolio_report.pipeline import run_pipeline

    scope_n = _parse_reports_scope(scope)
    start_d = _parse_iso_date(start, label="start")
    end_d = _parse_iso_date(end, label="end")
    asof_d = _parse_iso_date(asof, label="as-of")
    if end_d < start_d:
        raise HTTPException(status_code=400, detail="End date must be >= start date.")
    if not (start_d <= asof_d <= end_d):
        raise HTTPException(status_code=400, detail="As-of date must be within [start, end].")

    # Validate portfolio selection is permitted for scope.
    opts = _portfolio_options_for_scope(session, scope=scope_n)
    if not any(int(o.get("id") or 0) == int(portfolio_id) for o in opts):
        raise HTTPException(status_code=400, detail="Selected portfolio not available in this scope.")

    bench = (benchmark or "VOO").strip().upper()
    if not bench:
        raise HTTPException(status_code=400, detail="Benchmark symbol is required.")

    inputs = build_monthly_report_inputs_from_db(
        session,
        scope=scope_n,
        connection_id=int(portfolio_id),
        start_date=start_d,
        end_date=end_d,
        asof_date=asof_d,
        grace_days=14,
    )
    if not inputs.transactions_csv_bytes or not inputs.monthly_perf_csv_bytes:
        raise HTTPException(status_code=400, detail="Could not build inputs from DB (missing data).")

    # Prefer local benchmark file (Investor uses VOO) if present.
    prices_dir = Path("data") / "prices"
    prices_dir.mkdir(parents=True, exist_ok=True)
    if bench == "VOO":
        bench_src = Path("data") / "benchmarks" / "voo.csv"
        bench_dest = prices_dir / "VOO.csv"
        if bench_src.exists() and not bench_dest.exists():
            try:
                shutil.copyfile(bench_src, bench_dest)
            except Exception:
                pass

    h = hashlib.sha256()
    h.update(b"portfolio_report_db_v1\n")
    h.update(str(scope_n).encode("utf-8"))
    h.update(b"\n")
    h.update(str(int(portfolio_id)).encode("utf-8"))
    h.update(b"\n")
    h.update(str(start_d).encode("utf-8"))
    h.update(b"\n")
    h.update(str(end_d).encode("utf-8"))
    h.update(b"\n")
    h.update(str(asof_d).encode("utf-8"))
    h.update(b"\n")
    h.update(bench.encode("utf-8"))
    h.update(b"\n")
    h.update(b"download_prices=" + (b"1" if download_prices else b"0"))
    h.update(b"\n")
    h.update(b"include_fees_as_flow=" + (b"1" if include_fees_as_flow else b"0"))
    h.update(b"\n")
    h.update(inputs.transactions_csv_bytes)
    h.update(b"\n--monthly--\n")
    h.update(inputs.monthly_perf_csv_bytes)
    if inputs.holdings_csv_bytes is not None:
        h.update(b"\n--holdings--\n")
        h.update(inputs.holdings_csv_bytes)
    digest = h.hexdigest()[:12]

    run_id = f"db_{scope_n}_{portfolio_id}_{start_d.isoformat()}_{end_d.isoformat()}_{asof_d.isoformat()}_{bench}_{digest}"
    run_dir = _monthly_reports_root() / run_id
    inputs_dir = run_dir / "inputs"
    inputs_dir.mkdir(parents=True, exist_ok=True)

    tx_path = inputs_dir / "transactions.csv"
    mon_path = inputs_dir / "monthly_perf.csv"
    hold_path = inputs_dir / "holdings.csv" if inputs.holdings_csv_bytes is not None else None
    tx_path.write_bytes(inputs.transactions_csv_bytes)
    mon_path.write_bytes(inputs.monthly_perf_csv_bytes)
    if hold_path is not None:
        hold_path.write_bytes(inputs.holdings_csv_bytes or b"")

    try:
        run_pipeline(
            transactions_csv=tx_path,
            monthly_perf_csv=mon_path,
            holdings_csv=hold_path,
            out_dir=run_dir,
            prices_dir=prices_dir,
            start_date=start_d,
            end_date=end_d,
            asof_date=asof_d,
            benchmark_symbol=bench,
            download_prices=bool(download_prices),
            include_fees_as_flow=bool(include_fees_as_flow),
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Monthly report run failed: {e}")

    # Append DB-source warnings to metadata for transparency.
    meta_path = run_dir / "run_metadata.json"
    meta = _read_small_json(meta_path)
    if meta:
        meta.setdefault("source", {})
        meta["source"] = {
            "kind": "db",
            "scope": scope_n,
            "portfolio_id": int(portfolio_id),
            "portfolio_label": next((o.get("name") for o in opts if int(o.get("id") or 0) == int(portfolio_id)), None),
            "warnings": inputs.warnings,
        }
        try:
            meta_path.write_text(json.dumps(meta, indent=2, sort_keys=True), encoding="utf-8")
        except Exception:
            pass

    return RedirectResponse(url=f"/reports/monthly/run/{run_id}?ok=ok", status_code=303)


@router.post("/monthly/run")
def reports_monthly_run(
    request: Request,
    actor: str = Depends(require_actor),
    start: str = Form("2025-01-01"),
    end: str = Form("2025-12-31"),
    asof: str = Form("2025-12-31"),
    benchmark: str = Form("SPY"),
    download_prices: bool = Form(False),
    include_fees_as_flow: bool = Form(False),
    transactions: UploadFile = File(...),
    monthly_perf: UploadFile = File(...),
    holdings: UploadFile | None = File(None),
):
    from portfolio_report.pipeline import run_pipeline

    start_d = _parse_iso_date(start, label="start")
    end_d = _parse_iso_date(end, label="end")
    asof_d = _parse_iso_date(asof, label="as-of")
    if end_d < start_d:
        raise HTTPException(status_code=400, detail="End date must be >= start date.")
    if not (start_d <= asof_d <= end_d):
        raise HTTPException(status_code=400, detail="As-of date must be within [start, end].")
    bench = (benchmark or "SPY").strip().upper()
    if not bench:
        raise HTTPException(status_code=400, detail="Benchmark symbol is required.")

    tx_bytes = transactions.file.read()
    mon_bytes = monthly_perf.file.read()
    hold_bytes = holdings.file.read() if holdings is not None else None

    h = hashlib.sha256()
    h.update(b"portfolio_report_v1\n")
    h.update(str(start_d).encode("utf-8"))
    h.update(b"\n")
    h.update(str(end_d).encode("utf-8"))
    h.update(b"\n")
    h.update(str(asof_d).encode("utf-8"))
    h.update(b"\n")
    h.update(bench.encode("utf-8"))
    h.update(b"\n")
    h.update(b"download_prices=" + (b"1" if download_prices else b"0"))
    h.update(b"\n")
    h.update(b"include_fees_as_flow=" + (b"1" if include_fees_as_flow else b"0"))
    h.update(b"\n")
    h.update(tx_bytes)
    h.update(b"\n--monthly--\n")
    h.update(mon_bytes)
    if hold_bytes is not None:
        h.update(b"\n--holdings--\n")
        h.update(hold_bytes)
    digest = h.hexdigest()[:12]

    run_id = f"{start_d.isoformat()}_{end_d.isoformat()}_{asof_d.isoformat()}_{bench}_{digest}"
    run_dir = _monthly_reports_root() / run_id
    inputs_dir = run_dir / "inputs"
    inputs_dir.mkdir(parents=True, exist_ok=True)

    tx_path = inputs_dir / "transactions.csv"
    mon_path = inputs_dir / "monthly_perf.csv"
    hold_path = inputs_dir / "holdings.csv" if hold_bytes is not None else None
    tx_path.write_bytes(tx_bytes)
    mon_path.write_bytes(mon_bytes)
    if hold_path is not None:
        hold_path.write_bytes(hold_bytes or b"")

    prices_dir = Path("data") / "prices"
    prices_dir.mkdir(parents=True, exist_ok=True)

    try:
        run_pipeline(
            transactions_csv=tx_path,
            monthly_perf_csv=mon_path,
            holdings_csv=hold_path,
            out_dir=run_dir,
            prices_dir=prices_dir,
            start_date=start_d,
            end_date=end_d,
            asof_date=asof_d,
            benchmark_symbol=bench,
            download_prices=bool(download_prices),
            include_fees_as_flow=bool(include_fees_as_flow),
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Monthly report run failed: {e}")

    return RedirectResponse(url=f"/reports/monthly/run/{run_id}?ok=ok", status_code=303)


@router.get("/monthly/run/{run_id}")
def reports_monthly_run_view(
    request: Request,
    run_id: str,
    actor: str = Depends(require_actor),
):
    from src.app.main import templates

    rid = _safe_run_id(run_id)
    run_dir = _monthly_reports_root() / rid
    meta = _read_small_json(run_dir / "run_metadata.json")
    if not meta:
        raise HTTPException(status_code=404, detail="Run not found.")

    files = []
    for p in sorted([x for x in run_dir.iterdir() if x.is_file()], key=lambda x: x.name):
        files.append({"name": p.name, "bytes": int(p.stat().st_size)})

    asof_s = str(meta.get("asof") or "")
    report_name = f"report_{asof_s[:7]}.html" if len(asof_s) >= 7 else None
    if report_name is not None and not (run_dir / report_name).exists():
        report_name = None
    if report_name is None:
        for p in sorted(run_dir.glob("report_*.html")):
            report_name = p.name
            break

    return templates.TemplateResponse(
        "reports_monthly_run.html",
        {
            "request": request,
            "actor": actor,
            "auth_banner": auth_banner_message(),
            "run_id": rid,
            "meta": meta,
            "files": files,
            "report_name": report_name,
            "ok_msg": request.query_params.get("ok"),
        },
    )


@router.get("/monthly/files/{run_id}/{filename}")
def reports_monthly_file(
    run_id: str,
    filename: str,
    actor: str = Depends(require_actor),
):
    rid = _safe_run_id(run_id)
    fname = _safe_filename(filename)
    run_dir = _monthly_reports_root() / rid
    path = run_dir / fname
    if not path.exists() or not path.is_file():
        raise HTTPException(status_code=404, detail="File not found.")

    allowed_prefixes = ("report_", "analytics_", "position_guidance_", "run_metadata")
    if not any(fname.startswith(p) for p in allowed_prefixes):
        raise HTTPException(status_code=400, detail="File not available.")

    media = "application/octet-stream"
    if fname.endswith(".html"):
        media = "text/html"
    elif fname.endswith(".csv"):
        media = "text/csv"
    elif fname.endswith(".json"):
        media = "application/json"
    return FileResponse(path, media_type=media, filename=fname)


@router.get("")
def reports_home(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    today = dt.date.today()
    scope = _parse_reports_scope(request.query_params.get("scope"))
    period = (request.query_params.get("period") or "ytd").strip().lower()
    view = (request.query_params.get("view") or "accounts").strip().lower()
    account_id_raw = (request.query_params.get("account_id") or "").strip()
    account_id = int(account_id_raw) if account_id_raw.isdigit() else None

    year_raw = (request.query_params.get("year") or "").strip()
    year_options = _available_year_options(session, scope=scope, today=today, kind="withdrawals")
    year = int(year_raw) if year_raw.isdigit() else (today.year if today.year in year_options else year_options[0])
    if year not in year_options:
        year = year_options[0]

    if period == "year":
        start_date = dt.date(year, 1, 1)
        end_date = dt.date(year, 12, 31)
        period_label = f"Calendar year {year}"
    else:
        start_date = dt.date(today.year, 1, 1)
        end_date = today
        period_label = f"YTD ({today.year})"

    preferred_conn_ids = preferred_active_connection_ids_for_scope(session, scope=scope)
    internal = _is_internal_transfer_expr()
    is_deposit = and_(Transaction.type == "TRANSFER", Transaction.amount > 0, ~internal)
    is_withdrawal = and_(Transaction.type == "TRANSFER", Transaction.amount < 0, ~internal)
    is_fee = and_(Transaction.type == "FEE", Transaction.amount < 0)
    is_other_cash_out = and_(Transaction.type == "OTHER", Transaction.amount < 0, ~internal)
    # WITHHOLDING is stored as positive (credit), but economically a cash out.
    is_withholding = Transaction.type == "WITHHOLDING"

    q = (
        session.query(
            Account.id.label("account_id"),
            Account.name.label("account_name"),
            TaxpayerEntity.name.label("taxpayer_name"),
            TaxpayerEntity.type.label("taxpayer_type"),
            func.sum(case((is_deposit, 1), else_=0)).label("deposit_count"),
            func.sum(case((is_deposit, Transaction.amount), else_=0.0)).label("deposit_total"),
            func.sum(case((is_withdrawal, 1), else_=0)).label("withdrawal_count"),
            func.sum(case((is_withdrawal, func.abs(Transaction.amount)), else_=0.0)).label("withdrawal_total"),
            func.sum(case((is_fee, 1), else_=0)).label("fee_count"),
            func.sum(case((is_fee, func.abs(Transaction.amount)), else_=0.0)).label("fee_total"),
            func.sum(case((is_withholding, 1), else_=0)).label("withholding_count"),
            func.sum(case((is_withholding, func.abs(Transaction.amount)), else_=0.0)).label("withholding_total"),
            func.sum(case((is_other_cash_out, 1), else_=0)).label("other_count"),
            func.sum(case((is_other_cash_out, func.abs(Transaction.amount)), else_=0.0)).label("other_total"),
        )
        .join(Transaction, Transaction.account_id == Account.id)
        .outerjoin(ExternalTransactionMap, ExternalTransactionMap.transaction_id == Transaction.id)
        .outerjoin(ExternalConnection, ExternalConnection.id == ExternalTransactionMap.connection_id)
        .join(TaxpayerEntity, TaxpayerEntity.id == Account.taxpayer_entity_id)
        .filter(
            Transaction.date >= start_date,
            Transaction.date <= end_date,
            Transaction.type.in_(["TRANSFER", "FEE", "WITHHOLDING", "OTHER"]),
        )
        .filter(
            or_(
                ExternalTransactionMap.connection_id.is_(None),
                ExternalTransactionMap.connection_id.in_(sorted(preferred_conn_ids)),
            )
        )
        .group_by(Account.id, Account.name, TaxpayerEntity.name, TaxpayerEntity.type)
    ).filter(*_scope_account_predicates(scope))

    rows = q.order_by(Account.name.asc()).all()

    cq = (
        session.query(
            func.sum(case((is_deposit, 1), else_=0)).label("deposit_count"),
            func.sum(case((is_deposit, Transaction.amount), else_=0.0)).label("deposit_total"),
            func.sum(case((is_withdrawal, 1), else_=0)).label("withdrawal_count"),
            func.sum(case((is_withdrawal, func.abs(Transaction.amount)), else_=0.0)).label("withdrawal_total"),
            func.sum(case((is_fee, 1), else_=0)).label("fee_count"),
            func.sum(case((is_fee, func.abs(Transaction.amount)), else_=0.0)).label("fee_total"),
            func.sum(case((is_withholding, 1), else_=0)).label("withholding_count"),
            func.sum(case((is_withholding, func.abs(Transaction.amount)), else_=0.0)).label("withholding_total"),
            func.sum(case((is_other_cash_out, 1), else_=0)).label("other_count"),
            func.sum(case((is_other_cash_out, func.abs(Transaction.amount)), else_=0.0)).label("other_total"),
        )
        .select_from(Transaction)
        .join(Account, Account.id == Transaction.account_id)
        .outerjoin(ExternalTransactionMap, ExternalTransactionMap.transaction_id == Transaction.id)
        .outerjoin(ExternalConnection, ExternalConnection.id == ExternalTransactionMap.connection_id)
        .join(TaxpayerEntity, TaxpayerEntity.id == Account.taxpayer_entity_id)
        .filter(
            Transaction.date >= start_date,
            Transaction.date <= end_date,
            Transaction.type.in_(["TRANSFER", "FEE", "WITHHOLDING", "OTHER"]),
        )
        .filter(
            or_(
                ExternalTransactionMap.connection_id.is_(None),
                ExternalTransactionMap.connection_id.in_(sorted(preferred_conn_ids)),
            )
        )
    ).filter(*_scope_account_predicates(scope))
    combined_row = cq.one()
    combined_fee_total = float(combined_row.fee_total or 0.0)
    combined_fee_count = int(combined_row.fee_count or 0)
    combined_withholding_total = float(combined_row.withholding_total or 0.0)
    combined_withholding_count = int(combined_row.withholding_count or 0)
    combined_total = float(combined_row.withdrawal_total or 0.0) + combined_fee_total + combined_withholding_total + float(combined_row.other_total or 0.0)
    combined_count = int(combined_row.withdrawal_count or 0) + combined_fee_count + combined_withholding_count + int(combined_row.other_count or 0)
    combined_deposit_total = float(combined_row.deposit_total or 0.0)
    combined_deposit_count = int(combined_row.deposit_count or 0)
    combined_net_total = combined_total - combined_deposit_total - combined_fee_total - combined_withholding_total
    show_overall = scope in {"household", "trust"}

    detail_rows: list[tuple[Transaction, ExternalTransactionMap | None, ExternalConnection | None]] = []
    selected_account: Account | None = None
    if account_id is not None:
        selected_account = (
            session.query(Account)
            .join(TaxpayerEntity, TaxpayerEntity.id == Account.taxpayer_entity_id)
            .filter(Account.id == account_id)
            .one_or_none()
        )
        if selected_account is not None:
            if scope == "trust" and (selected_account.taxpayer_entity.type != "TRUST" or selected_account.account_type == "IRA"):
                selected_account = None
            elif scope == "personal" and (selected_account.taxpayer_entity.type != "PERSONAL" or selected_account.account_type == "IRA"):
                selected_account = None
            elif scope == "ira" and selected_account.account_type != "IRA":
                selected_account = None
        if selected_account is not None:
            detail_rows = (
                session.query(Transaction, ExternalTransactionMap, ExternalConnection)
                .outerjoin(
                    ExternalTransactionMap,
                    ExternalTransactionMap.transaction_id == Transaction.id,
                )
                .outerjoin(
                    ExternalConnection,
                    ExternalConnection.id == ExternalTransactionMap.connection_id,
                )
                .filter(
                    Transaction.account_id == selected_account.id,
                    Transaction.date >= start_date,
                    Transaction.date <= end_date,
                )
                .filter(
                    or_(
                        ExternalTransactionMap.connection_id.is_(None),
                        ExternalTransactionMap.connection_id.in_(sorted(preferred_conn_ids)),
                    )
                )
                .filter(
                    or_(
                        and_(Transaction.type == "TRANSFER", ~internal),
                        and_(Transaction.type == "FEE", Transaction.amount < 0),
                        Transaction.type == "WITHHOLDING",
                        and_(Transaction.type == "OTHER", Transaction.amount < 0, ~internal),
                    )
                )
                .order_by(Transaction.date.desc(), Transaction.id.desc())
                .limit(500)
                .all()
            )

    from src.app.main import templates

    return templates.TemplateResponse(
        "reports.html",
        {
            "request": request,
            "actor": actor,
            "auth_banner": auth_banner_message(),
            "scope": scope,
            "scope_label": _reports_scope_label(scope),
            "period": period,
            "year": year,
            "view": view,
            "period_label": period_label,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "year_options": year_options,
            "rows": rows,
            "combined_total": combined_total,
            "combined_count": combined_count,
            "combined_deposit_total": combined_deposit_total,
            "combined_deposit_count": combined_deposit_count,
            "combined_fee_total": combined_fee_total,
            "combined_fee_count": combined_fee_count,
            "combined_withholding_total": combined_withholding_total,
            "combined_withholding_count": combined_withholding_count,
            "combined_net_total": combined_net_total,
            "show_overall": show_overall,
            "account_id": account_id,
            "selected_account": selected_account,
            "detail_rows": detail_rows,
        },
    )


@router.get("/performance")
def reports_performance(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    from pathlib import Path

    from src.core.performance import build_performance_report

    today = dt.date.today()
    scope = _parse_reports_scope(request.query_params.get("scope"))
    period = (request.query_params.get("period") or "ytd").strip().lower()
    freq = (request.query_params.get("freq") or "month_end").strip().lower()
    account_id_raw = (request.query_params.get("account_id") or "").strip()
    account_id = int(account_id_raw) if account_id_raw.isdigit() else None
    ok_msg = request.query_params.get("ok")
    error_msg = request.query_params.get("error")

    year_raw = (request.query_params.get("year") or "").strip()
    year_options = _available_year_options(session, scope=scope, today=today, kind="performance")
    year = int(year_raw) if year_raw.isdigit() else (today.year if today.year in year_options else year_options[0])
    if year not in year_options:
        year = year_options[0]

    if period == "year":
        start_date = dt.date(year, 1, 1)
        end_date = dt.date(year, 12, 31)
        period_label = f"Calendar year {year}"
    else:
        start_date = dt.date(today.year, 1, 1)
        end_date = today
        period_label = f"YTD ({today.year})"

    account_options = _account_options_for_scope(session, scope=scope, portfolio_id=None)
    if account_id is not None and not any(int(a.get("id") or 0) == int(account_id) for a in account_options):
        account_id = None

    benchmark_label = "VOO"
    benchmark_path = Path("data") / "benchmarks" / "voo.csv"
    benchmark_info = {
        "path": str(benchmark_path),
        "label": benchmark_label,
        "exists": benchmark_path.exists(),
        "mtime": None,
        "bytes": None,
    }
    try:
        if benchmark_path.exists():
            st = benchmark_path.stat()
            benchmark_info["mtime"] = dt.datetime.fromtimestamp(st.st_mtime).isoformat()
            benchmark_info["bytes"] = int(st.st_size)
    except Exception:
        pass

    report = build_performance_report(
        session,
        scope=scope,
        start_date=start_date,
        end_date=end_date,
        frequency=freq,
        benchmark_prices_path=benchmark_path if benchmark_path.exists() else None,
        benchmark_label=benchmark_label,
        account_ids=[int(account_id)] if account_id is not None else None,
        include_combined=(account_id is None),
    )
    # Ensure deterministic ordering in UI.
    try:
        report["rows"] = sorted(report.get("rows") or [], key=lambda r: str(getattr(r, "portfolio_name", "")))
    except Exception:
        pass

    from src.app.main import templates

    return templates.TemplateResponse(
        "reports_performance.html",
        {
            "request": request,
            "actor": actor,
            "auth_banner": auth_banner_message(),
            "scope": scope,
            "scope_label": _reports_scope_label(scope),
            "period": period,
            "freq": freq,
            "year": year,
            "year_options": year_options,
            "account_id": account_id,
            "account_options": account_options,
            "period_label": period_label,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "benchmark_info": benchmark_info,
            "report": report,
            "ok_msg": ok_msg,
            "error_msg": error_msg,
        },
    )

@router.get("/performance.csv")
def reports_performance_csv(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    from pathlib import Path

    from src.core.performance import build_performance_report

    today = dt.date.today()
    scope = _parse_reports_scope(request.query_params.get("scope"))
    period = (request.query_params.get("period") or "ytd").strip().lower()
    freq = (request.query_params.get("freq") or "month_end").strip().lower()
    account_id_raw = (request.query_params.get("account_id") or "").strip()
    account_id = int(account_id_raw) if account_id_raw.isdigit() else None

    year_raw = (request.query_params.get("year") or "").strip()
    year_options = _available_year_options(session, scope=scope, today=today, kind="performance")
    year = int(year_raw) if year_raw.isdigit() else (today.year if today.year in year_options else year_options[0])
    if year not in year_options:
        year = year_options[0]

    if period == "year":
        start_date = dt.date(year, 1, 1)
        end_date = dt.date(year, 12, 31)
    else:
        start_date = dt.date(today.year, 1, 1)
        end_date = today

    benchmark_label = "VOO"
    benchmark_path = Path("data") / "benchmarks" / "voo.csv"
    report = build_performance_report(
        session,
        scope=scope,
        start_date=start_date,
        end_date=end_date,
        frequency=freq,
        benchmark_prices_path=benchmark_path if benchmark_path.exists() else None,
        benchmark_label=benchmark_label,
        account_ids=[int(account_id)] if account_id is not None else None,
        include_combined=(account_id is None),
    )

    out = io.StringIO()
    import csv

    w = csv.writer(out)
    w.writerow(
        [
            "Portfolio",
            "Taxpayer",
            "TaxpayerType",
            "PeriodStart",
            "PeriodEnd",
            "CoverageStart",
            "CoverageEnd",
            "BeginValue",
            "EndValue",
            "Contributions",
            "Withdrawals",
            "NetFlow",
            "Fees",
            "Withholding",
            "OtherCashOut",
            "TotalCashOut",
            "GainValue",
            "IRR",
            "XIRR",
            "TWR",
            "Sharpe",
            f"{benchmark_label}TWR",
            f"{benchmark_label}Sharpe",
            "ExcessTWR",
            "Warnings",
        ]
    )
    rows = []
    if report.get("combined"):
        rows.append(report["combined"])
    rows.extend(report.get("rows") or [])
    for r in rows:
        try:
            w.writerow(
                [
                    getattr(r, "portfolio_name", ""),
                    getattr(r, "taxpayer_name", ""),
                    getattr(r, "taxpayer_type", ""),
                    getattr(r, "period_start", None) or "",
                    getattr(r, "period_end", None) or "",
                    getattr(r, "coverage_start", None) or "",
                    getattr(r, "coverage_end", None) or "",
                    getattr(r, "begin_value", None) if getattr(r, "begin_value", None) is not None else "",
                    getattr(r, "end_value", None) if getattr(r, "end_value", None) is not None else "",
                    getattr(r, "contributions", None) if getattr(r, "contributions", None) is not None else "",
                    getattr(r, "withdrawals", None) if getattr(r, "withdrawals", None) is not None else "",
                    getattr(r, "net_flow", None) if getattr(r, "net_flow", None) is not None else "",
                    getattr(r, "fees", None) if getattr(r, "fees", None) is not None else "",
                    getattr(r, "withholding", None) if getattr(r, "withholding", None) is not None else "",
                    getattr(r, "other_cash_out", None) if getattr(r, "other_cash_out", None) is not None else "",
                    getattr(r, "total_cash_out", None) if getattr(r, "total_cash_out", None) is not None else "",
                    getattr(r, "gain_value", None) if getattr(r, "gain_value", None) is not None else "",
                    getattr(r, "irr", None) if getattr(r, "irr", None) is not None else "",
                    getattr(r, "xirr", None) if getattr(r, "xirr", None) is not None else "",
                    getattr(r, "twr", None) if getattr(r, "twr", None) is not None else "",
                    getattr(r, "sharpe", None) if getattr(r, "sharpe", None) is not None else "",
                    getattr(r, "benchmark_twr", None) if getattr(r, "benchmark_twr", None) is not None else "",
                    getattr(r, "benchmark_sharpe", None) if getattr(r, "benchmark_sharpe", None) is not None else "",
                    getattr(r, "excess_twr", None) if getattr(r, "excess_twr", None) is not None else "",
                    " | ".join(getattr(r, "warnings", []) or []),
                ]
            )
        except Exception:
            continue

    data = out.getvalue().encode("utf-8")
    return Response(
        content=data,
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="performance_{scope}_{period}_{year}.csv"'},
    )


@router.post("/benchmarks/voo/upload")
def reports_voo_upload(
    request: Request,
    actor: str = Depends(require_actor),
    upload: UploadFile = File(...),
):
    # Store offline benchmark data on disk (no network fetches).
    from pathlib import Path
    import shutil
    import urllib.parse

    dest_dir = Path("data") / "benchmarks"
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / "voo.csv"

    with dest.open("wb") as f:
        shutil.copyfileobj(upload.file, f)

    msg = urllib.parse.quote(f"Uploaded VOO benchmark CSV: {dest.name}")
    # Preserve query params (scope/period/year) if present.
    qp = str(request.query_params) if request.query_params else ""
    suffix = f"&{qp}" if qp else ""
    return RedirectResponse(url=f"/reports/performance?ok={msg}{suffix}", status_code=303)


# Backward-compatible alias (older UI labeled this as "S&P 500").
@router.post("/benchmarks/sp500/upload")
def reports_sp500_upload(
    request: Request,
    actor: str = Depends(require_actor),
    upload: UploadFile = File(...),
):
    return reports_voo_upload(request=request, actor=actor, upload=upload)
