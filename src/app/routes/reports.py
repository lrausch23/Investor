from __future__ import annotations

import datetime as dt
import hashlib
import io
import json
import re
import shutil
import urllib.parse
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

def _static_version() -> str:
    # Similar to Holdings/Sync: override per-request so UI changes are visible without a server restart.
    try:
        css_path = Path(__file__).resolve().parents[1] / "static" / "app.css"
        return str(int(css_path.stat().st_mtime))
    except Exception:
        return "0"

def _static_version_for(path_relative_to_static: str) -> str:
    """
    Cache-bust helper for individual static assets under `src/app/static/`.
    """
    try:
        p = Path(__file__).resolve().parents[1] / "static" / str(path_relative_to_static).lstrip("/").strip()
        return str(int(p.stat().st_mtime))
    except Exception:
        return "0"


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


_PLACEHOLDER_ACCOUNT_NAMES = {
    "IB FLEX WEB",
    "IB FLEX (WEB)",
    "IB FLEX OFFLINE",
    "IB FLEX (OFFLINE)",
}


def _exclude_placeholder_accounts(q):
    return q.filter(~func.upper(Account.name).in_(_PLACEHOLDER_ACCOUNT_NAMES))


def _is_internal_transfer_expr():
    """
    SQL predicate to exclude internal sweeps/FX/multi-currency shuttles from cashflow reporting.
    (We store details in Transaction.lot_links_json.)
    """
    desc = func.upper(func.coalesce(func.json_extract(Transaction.lot_links_json, "$.description"), ""))
    addl = func.upper(func.coalesce(func.json_extract(Transaction.lot_links_json, "$.additional_detail"), ""))
    raw = func.upper(func.coalesce(func.json_extract(Transaction.lot_links_json, "$.raw_type"), ""))
    source = func.upper(func.coalesce(func.json_extract(Transaction.lot_links_json, "$.source"), ""))
    txt = desc + " " + addl
    return and_(
        source != "CSV_SUPPLEMENTAL",
        or_(
            # Chase IRA deposit sweep between internal sub-accounts (not investor deposits/withdrawals).
            func.instr(txt, "DEPOSIT SWEEP") > 0,
            func.instr(txt, "SHADO") > 0,
            func.instr(txt, "REC FR SIS") > 0,
            func.instr(txt, "REC TRSF SIS") > 0,
            func.instr(txt, "TRSF SIS") > 0,
            and_(raw == "UNKNOWN", func.instr(txt, "MULTI") > 0, func.instr(txt, "CURRENCY") > 0),
            and_(func.instr(txt, "FX") > 0, or_(func.instr(txt, "SETTLEMENT") > 0, func.instr(txt, "TRAD") > 0, func.instr(txt, "TRADE") > 0)),
        ),
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


def _parse_period_dates_for_performance(
    *,
    period: str,
    year: int,
    today: dt.date,
    custom_start: str | None = None,
    custom_end: str | None = None,
) -> tuple[dt.date, dt.date, str]:
    p = (period or "ytd").strip().lower()
    if p == "year":
        start_date = dt.date(int(year), 1, 1)
        end_date = dt.date(int(year), 12, 31)
        return start_date, end_date, f"Calendar year {year}"
    if p == "ytd":
        start_date = dt.date(int(year), 1, 1)
        end_date = today if int(year) == int(today.year) else dt.date(int(year), 12, 31)
        return start_date, end_date, f"YTD ({year})"
    if p in {"1y", "3y", "5y"}:
        years = int(p[0])
        end_date = today
        start_date = today - dt.timedelta(days=int(365 * years))
        return start_date, end_date, f"Last {years}y"
    if p == "custom":
        start_date = _parse_iso_date(custom_start or "", label="start")
        end_date = _parse_iso_date(custom_end or "", label="end")
        if end_date < start_date:
            raise HTTPException(status_code=400, detail="End date must be >= start date.")
        return start_date, end_date, f"{start_date.isoformat()} → {end_date.isoformat()}"
    # Default: YTD (current year).
    start_date = dt.date(today.year, 1, 1)
    end_date = today
    return start_date, end_date, f"YTD ({today.year})"


def _account_options_for_scope(session: Session, *, scope: str, portfolio_id: int | None) -> list[dict[str, object]]:
    q = (
        session.query(Account.id, Account.name, TaxpayerEntity.name, TaxpayerEntity.type)
        .select_from(Account)
        .join(TaxpayerEntity, TaxpayerEntity.id == Account.taxpayer_entity_id)
        .filter(*_scope_account_predicates(scope))
        .order_by(Account.name.asc(), Account.id.asc())
    )
    q = _exclude_placeholder_accounts(q)
    if portfolio_id is not None:
        q = q.join(ExternalAccountMap, ExternalAccountMap.account_id == Account.id).filter(
            ExternalAccountMap.connection_id == int(portfolio_id)
        )
    out: list[dict[str, object]] = []
    for aid, aname, tp_name, tp_type in q.all():
        out.append({"id": int(aid), "name": f"{aname}"})
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
    elif kind == "cash_out":
        tq = tq.filter(Transaction.type.in_(["TRANSFER", "FEE", "WITHHOLDING", "OTHER"]))
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
            "auth_banner": None,
            "auth_banner_detail": auth_banner_message(),
            "static_version": _static_version(),
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
            "auth_banner": None,
            "auth_banner_detail": auth_banner_message(),
            "static_version": _static_version(),
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
    year_options = _available_year_options(session, scope=scope, today=today, kind="cash_out")
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

    # Aggregate cashflow totals per account. We outer-join this onto the account list so that
    # accounts in scope still show up even if they have $0 cashflow activity in the selected period.
    tx_source = func.upper(func.coalesce(func.json_extract(Transaction.lot_links_json, "$.source"), ""))
    is_csv = tx_source == "CSV_SUPPLEMENTAL"
    is_csv_withdrawal = and_(is_csv, is_withdrawal)
    is_csv_fee = and_(is_csv, is_fee)
    is_csv_withholding = and_(is_csv, is_withholding)
    is_csv_other = and_(is_csv, is_other_cash_out)
    is_csv_cash_out = or_(is_csv_withdrawal, is_csv_fee, is_csv_withholding, is_csv_other)

    agg_subq = (
        session.query(
            Transaction.account_id.label("account_id"),
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
            func.sum(
                case(
                    (is_csv_withdrawal, func.abs(Transaction.amount)),
                    (is_csv_fee, func.abs(Transaction.amount)),
                    (is_csv_withholding, func.abs(Transaction.amount)),
                    (is_csv_other, func.abs(Transaction.amount)),
                    else_=0.0,
                )
            ).label("csv_total"),
            func.sum(case((is_csv_cash_out, 1), else_=0)).label("csv_count"),
        )
        .select_from(Transaction)
        .join(Account, Account.id == Transaction.account_id)
        .join(TaxpayerEntity, TaxpayerEntity.id == Account.taxpayer_entity_id)
        .outerjoin(ExternalTransactionMap, ExternalTransactionMap.transaction_id == Transaction.id)
        .outerjoin(ExternalConnection, ExternalConnection.id == ExternalTransactionMap.connection_id)
        .filter(
            Transaction.date >= start_date,
            Transaction.date <= end_date,
            Transaction.type.in_(["TRANSFER", "FEE", "WITHHOLDING", "OTHER"]),
        )
        .filter(~func.upper(Account.name).in_(_PLACEHOLDER_ACCOUNT_NAMES))
        .filter(
            or_(
                is_csv,
                ExternalTransactionMap.connection_id.is_(None),
                ExternalTransactionMap.connection_id.in_(sorted(preferred_conn_ids)),
            )
        )
        .filter(*_scope_account_predicates(scope))
        .group_by(Transaction.account_id)
        .subquery()
    )

    rows = (
        session.query(
            Account.id.label("account_id"),
            Account.name.label("account_name"),
            TaxpayerEntity.name.label("taxpayer_name"),
            TaxpayerEntity.type.label("taxpayer_type"),
            func.coalesce(agg_subq.c.deposit_count, 0).label("deposit_count"),
            func.coalesce(agg_subq.c.deposit_total, 0.0).label("deposit_total"),
            func.coalesce(agg_subq.c.withdrawal_count, 0).label("withdrawal_count"),
            func.coalesce(agg_subq.c.withdrawal_total, 0.0).label("withdrawal_total"),
            func.coalesce(agg_subq.c.fee_count, 0).label("fee_count"),
            func.coalesce(agg_subq.c.fee_total, 0.0).label("fee_total"),
            func.coalesce(agg_subq.c.withholding_count, 0).label("withholding_count"),
            func.coalesce(agg_subq.c.withholding_total, 0.0).label("withholding_total"),
            func.coalesce(agg_subq.c.other_count, 0).label("other_count"),
            func.coalesce(agg_subq.c.other_total, 0.0).label("other_total"),
            func.coalesce(agg_subq.c.csv_total, 0.0).label("csv_total"),
            func.coalesce(agg_subq.c.csv_count, 0).label("csv_count"),
        )
        .select_from(Account)
        .join(TaxpayerEntity, TaxpayerEntity.id == Account.taxpayer_entity_id)
        .outerjoin(agg_subq, agg_subq.c.account_id == Account.id)
        .filter(*_scope_account_predicates(scope))
        .filter(~func.upper(Account.name).in_(_PLACEHOLDER_ACCOUNT_NAMES))
        .order_by(Account.name.asc())
        .all()
    )

    tx_source = func.upper(func.coalesce(func.json_extract(Transaction.lot_links_json, "$.source"), ""))
    is_csv = tx_source == "CSV_SUPPLEMENTAL"

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
        .filter(~func.upper(Account.name).in_(_PLACEHOLDER_ACCOUNT_NAMES))
        .filter(
            or_(
                is_csv,
                ExternalTransactionMap.connection_id.is_(None),
                ExternalTransactionMap.connection_id.in_(sorted(preferred_conn_ids)),
            )
        )
    ).filter(*_scope_account_predicates(scope))
    combined_row = cq.one()
    combined_withdrawal_total = float(combined_row.withdrawal_total or 0.0)
    combined_withdrawal_count = int(combined_row.withdrawal_count or 0)
    combined_fee_total = float(combined_row.fee_total or 0.0)
    combined_fee_count = int(combined_row.fee_count or 0)
    combined_withholding_total = float(combined_row.withholding_total or 0.0)
    combined_withholding_count = int(combined_row.withholding_count or 0)
    combined_other_total = float(combined_row.other_total or 0.0)
    combined_other_count = int(combined_row.other_count or 0)
    combined_total = combined_withdrawal_total + combined_fee_total + combined_withholding_total + combined_other_total
    combined_count = combined_withdrawal_count + combined_fee_count + combined_withholding_count + combined_other_count
    combined_deposit_total = float(combined_row.deposit_total or 0.0)
    combined_deposit_count = int(combined_row.deposit_count or 0)
    # Net cash out is the outflows (withdrawals+fees+taxes+other) reduced by deposits (inflows).
    combined_net_total = combined_total - combined_deposit_total
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
        if selected_account and str(selected_account.name or "").strip().upper() in _PLACEHOLDER_ACCOUNT_NAMES:
            selected_account = None
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
                        is_csv,
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
            "auth_banner": None,
            "auth_banner_detail": auth_banner_message(),
            "static_version": _static_version(),
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
            "combined_withdrawal_total": combined_withdrawal_total,
            "combined_withdrawal_count": combined_withdrawal_count,
            "combined_fee_total": combined_fee_total,
            "combined_fee_count": combined_fee_count,
            "combined_withholding_total": combined_withholding_total,
            "combined_withholding_count": combined_withholding_count,
            "combined_other_total": combined_other_total,
            "combined_other_count": combined_other_count,
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
    from src.core.performance import build_performance_report
    from src.importers.adapters import ProviderError
    from src.investor.marketdata.benchmarks import BenchmarkDataClient
    from src.investor.marketdata.config import load_marketdata_config

    today = dt.date.today()
    scope = _parse_reports_scope(request.query_params.get("scope"))
    period = (request.query_params.get("period") or "ytd").strip().lower()
    freq = (request.query_params.get("freq") or "month_end").strip().lower()
    account_id_raw = (request.query_params.get("account_id") or "").strip()
    account_id = int(account_id_raw) if account_id_raw.isdigit() else None
    ok_msg = request.query_params.get("ok")
    error_msg = request.query_params.get("error")
    refresh_benchmark = str(request.query_params.get("refresh_benchmark") or "").strip().lower() in {"1", "true", "yes", "on"}
    bench_provider = (request.query_params.get("bench_provider") or "auto").strip().lower()
    # Prior-period comparison is disabled for now; keep in code for a future opt-in UI.
    compare_prior = False

    year_raw = (request.query_params.get("year") or "").strip()
    year_options = _available_year_options(session, scope=scope, today=today, kind="performance")
    year: int | None = int(year_raw) if year_raw.isdigit() else None
    if period == "ytd":
        year = int(today.year)
    else:
        if year is None:
            year = today.year if today.year in year_options else year_options[0]
        if year not in year_options:
            year = year_options[0]

    custom_start = (request.query_params.get("start") or "").strip()
    custom_end = (request.query_params.get("end") or "").strip()
    start_date, end_date, period_label = _parse_period_dates_for_performance(
        period=period,
        year=year,
        today=today,
        custom_start=custom_start or None,
        custom_end=custom_end or None,
    )
    compare_label: str | None = None
    prior_start: dt.date | None = None
    prior_end: dt.date | None = None
    if compare_prior:
        try:
            span = end_date - start_date
            if str(period or "").strip().lower() == "ytd":
                prior_start = dt.date(int(start_date.year) - 1, 1, 1)
                prior_end = prior_start + span
                compare_label = f"Prior YTD ({prior_start.isoformat()} → {prior_end.isoformat()})"
            else:
                prior_end = start_date
                prior_start = start_date - span
                compare_label = f"Prior period ({prior_start.isoformat()} → {prior_end.isoformat()})"
        except Exception:
            prior_start = None
            prior_end = None
            compare_label = None

    account_options = _account_options_for_scope(session, scope=scope, portfolio_id=None)
    if account_id is not None and not any(int(a.get("id") or 0) == int(account_id) for a in account_options):
        account_id = None

    benchmark_symbol = (request.query_params.get("benchmark") or "SPY").strip().upper()
    # Add a small buffer before start_date so month-end anchoring can use the prior trading day if needed.
    bench_anchor_start = start_date
    if prior_start is not None and prior_end is not None:
        bench_anchor_start = min(bench_anchor_start, prior_start)
    bench_fetch_start = bench_anchor_start - dt.timedelta(days=40)
    bench_fetch_end = end_date
    bench_df = None
    bench_series: list[tuple[dt.date, float]] | None = None
    bench_meta = None
    benchmark_warning: str | None = None
    if bench_provider in {"none", "off", "disabled"}:
        bench_df = None
        bench_series = None
    else:
        cfg, _cfg_path = load_marketdata_config()
        # Allow per-request provider override while keeping the cache-first strategy.
        bench_cfg = cfg.benchmarks.model_copy(deep=True)
        sel = (bench_provider or "auto").strip().lower()
        if sel in {"cache", "local"}:
            bench_cfg.provider_order = ["cache"]
        elif sel == "stooq":
            bench_cfg.provider_order = ["cache", "stooq", "yahoo"]
        elif sel == "yahoo":
            bench_cfg.provider_order = ["cache", "yahoo"]
        else:
            # auto: cache -> stooq -> yahoo
            bench_cfg.provider_order = bench_cfg.provider_order or ["cache", "stooq", "yahoo"]

        try:
            client = BenchmarkDataClient(config=bench_cfg)
            bench_df, bench_meta = client.get(
                symbol=benchmark_symbol,
                start=bench_fetch_start,
                end=bench_fetch_end,
                refresh=bool(refresh_benchmark),
            )
            # Build a simple (date, close) series for performance math (prefers adj_close when available).
            if bench_df is not None and not bench_df.empty:
                col = "adj_close" if "adj_close" in bench_df.columns else "close"
                vals = bench_df[col].copy()
                if col == "adj_close" and "close" in bench_df.columns:
                    vals = vals.fillna(bench_df["close"])
                bench_series = [(d.date(), float(v)) for d, v in vals.items() if v is not None and float(v) > 0.0]
        except Exception as e:
            benchmark_warning = str(e) if isinstance(e, ProviderError) else f"{type(e).__name__}: {e}"
            bench_series = None

    if refresh_benchmark and ok_msg is None and error_msg is None:
        if bench_series is not None and len(bench_series) > 0:
            ok_msg = f"Benchmark cache ready for {benchmark_symbol}."
            if bench_meta and bench_meta.warning:
                ok_msg = f"{ok_msg} {bench_meta.warning}"
        elif benchmark_warning:
            error_msg = benchmark_warning

    provider_label = "Disabled"
    if bench_provider not in {"none", "off", "disabled"}:
        if bench_meta is not None:
            provider_label = " → ".join(bench_meta.used_providers)
        else:
            provider_label = "Benchmark"
    benchmark_info = {
        "provider": provider_label,
        "symbol": benchmark_symbol,
        "cached": True,
        "path": None,
        "rows": (len(bench_df) if bench_df is not None else None),
        "warning": benchmark_warning or (bench_meta.warning if bench_meta else None),
    }

    report = build_performance_report(
        session,
        scope=scope,
        start_date=start_date,
        end_date=end_date,
        frequency=freq,
        benchmark_series=bench_series,
        benchmark_label=(benchmark_symbol or "SPY"),
        account_ids=[int(account_id)] if account_id is not None else None,
        include_combined=(account_id is None),
        include_series=True,
    )
    # Ensure deterministic ordering in UI.
    try:
        report["rows"] = sorted(report.get("rows") or [], key=lambda r: str(getattr(r, "portfolio_name", "")))
    except Exception:
        pass

    # Build chart + benchmark comparison stats (best-effort).
    primary_row = report.get("combined") or ((report.get("rows") or [None])[0])
    primary_pid = int(getattr(primary_row, "portfolio_id", 0) or 0) if primary_row else 0
    twr_curves = report.get("twr_curves") or {}
    portfolio_curve = twr_curves.get(primary_pid) or []
    bench_curve = report.get("benchmark_curve") or []

    prior: dict[str, object] | None = None
    if compare_prior and prior_start is not None and prior_end is not None:
        try:
            prior_report = build_performance_report(
                session,
                scope=scope,
                start_date=prior_start,
                end_date=prior_end,
                frequency=freq,
                benchmark_series=bench_series,
                benchmark_label=(benchmark_symbol or "SPY"),
                account_ids=[int(account_id)] if account_id is not None else None,
                include_combined=(account_id is None),
                include_series=True,
            )
            prior_primary = prior_report.get("combined") or ((prior_report.get("rows") or [None])[0])
            prior_pid = int(getattr(prior_primary, "portfolio_id", 0) or 0) if prior_primary else 0
            prior_twr_curves = prior_report.get("twr_curves") or {}
            prior_portfolio_curve = prior_twr_curves.get(prior_pid) or []
            prior_bench_curve = prior_report.get("benchmark_curve") or []

            def _ret_obs(curve: list[tuple[str, float]]) -> int:
                try:
                    return max(0, int(len(curve or []) - 1))
                except Exception:
                    return 0

            prior = {
                "label": compare_label,
                "start": prior_start.isoformat(),
                "end": prior_end.isoformat(),
                "primary": prior_primary,
                "comparison": {
                    "portfolio_cagr": _cagr(prior_portfolio_curve),
                    "benchmark_cagr": _cagr(prior_bench_curve),
                    "portfolio_max_drawdown": _max_drawdown(prior_portfolio_curve),
                    "benchmark_max_drawdown": _max_drawdown(prior_bench_curve),
                },
                "obs": {
                    "portfolio_returns": _ret_obs(prior_portfolio_curve),
                    "benchmark_returns": _ret_obs(prior_bench_curve),
                },
                "warnings": prior_report.get("warnings") or [],
            }
        except Exception as e:
            prior = {"label": compare_label, "start": prior_start.isoformat(), "end": prior_end.isoformat(), "error": f"{type(e).__name__}: {e}"}
    chart_data = {
        "portfolio": {"label": str(getattr(primary_row, "portfolio_name", "Portfolio")) if primary_row else "Portfolio", "curve": portfolio_curve},
        "benchmark": {"label": str(report.get("benchmark_label") or benchmark_info.get("symbol") or "Benchmark"), "curve": bench_curve},
        "frequency": str(report.get("frequency") or freq),
    }

    # Cashflow/event markers for the chart (derived from the same TRANSFER flows already used in performance math).
    def _parse_iso_date(s: str) -> dt.date | None:
        try:
            return dt.date.fromisoformat(str(s)[:10])
        except Exception:
            return None

    def _align_to_curve(d: dt.date, curve_dates: list[dt.date]) -> dt.date | None:
        if not curve_dates:
            return None
        # Prefer the first valuation date on/after the event date; otherwise carry back to last available.
        for cd in curve_dates:
            if cd >= d:
                return cd
        return curve_dates[-1]

    events: list[dict[str, object]] = []
    event_summary: dict[str, object] | None = None
    try:
        flows_by_pid = report.get("transfer_flows") or {}
        raw_flows = flows_by_pid.get(primary_pid) or []
        curve_dates = [_parse_iso_date(d) for d, _v in (portfolio_curve or [])]
        curve_dates = [d for d in curve_dates if d is not None]  # type: ignore[comparison-overlap]
        curve_dates = sorted(set(curve_dates))

        by_day: dict[dt.date, float] = {}
        # Use the chart's effective valuation window rather than the requested period bounds,
        # since performance may anchor begin/end valuations within grace windows.
        curve_start = None
        curve_end = None
        try:
            if portfolio_curve:
                curve_start = _parse_iso_date(str(portfolio_curve[0][0]))
                curve_end = _parse_iso_date(str(portfolio_curve[-1][0]))
        except Exception:
            curve_start = None
            curve_end = None

        eff_start = curve_start or start_date
        eff_end = curve_end or end_date

        for ds, amt in raw_flows:
            d0 = _parse_iso_date(str(ds))
            if d0 is None:
                continue
            if d0 < eff_start or d0 > eff_end:
                continue
            try:
                a = float(amt or 0.0)
            except Exception:
                continue
            if a == 0.0:
                continue
            by_day[d0] = float(by_day.get(d0, 0.0) + a)

        # Select large events only (plus always include the largest deposit/withdrawal).
        begin_v = getattr(primary_row, "begin_value", None) if primary_row else None
        threshold = 5000.0
        try:
            if begin_v is not None:
                threshold = max(threshold, 0.02 * float(begin_v or 0.0))
        except Exception:
            pass

        deposits = [(d, a) for d, a in by_day.items() if a > 0]
        withdrawals = [(d, a) for d, a in by_day.items() if a < 0]
        largest_deposit = max(deposits, key=lambda x: x[1])[0] if deposits else None
        largest_withdraw = min(withdrawals, key=lambda x: x[1])[0] if withdrawals else None  # most negative

        selected_days: set[dt.date] = set()
        for d, a in by_day.items():
            if abs(float(a)) >= float(threshold):
                selected_days.add(d)
        if largest_deposit:
            selected_days.add(largest_deposit)
        if largest_withdraw:
            selected_days.add(largest_withdraw)

        # Align to valuation dates used by the chart series.
        by_aligned: dict[dt.date, float] = {}
        for d in sorted(selected_days):
            ad = _align_to_curve(d, curve_dates)
            if ad is None:
                continue
            by_aligned[ad] = float(by_aligned.get(ad, 0.0) + float(by_day.get(d, 0.0)))

        # Cap marker count to avoid clutter (keep largest abs events).
        aligned_items = sorted(by_aligned.items(), key=lambda x: abs(float(x[1])), reverse=True)
        aligned_items = aligned_items[:25]

        for ad, a in aligned_items:
            kind = "deposit" if a > 0 else "withdrawal"
            events.append({"date": ad.isoformat(), "amount": float(a), "kind": kind})

        event_summary = {
            "deposits": int(len(deposits)),
            "withdrawals": int(len(withdrawals)),
            "largest_deposit": float(max((a for _d, a in deposits), default=0.0)),
            "largest_withdrawal": float(min((a for _d, a in withdrawals), default=0.0)),
            "threshold": float(threshold),
        }
    except Exception:
        events = []
        event_summary = None

    chart_data["events"] = events
    chart_data["event_summary"] = event_summary
    chart_data_json = "{}"
    try:
        chart_data_json = json.dumps(chart_data)
    except Exception:
        chart_data_json = "{}"

    def _cagr(curve: list[tuple[str, float]]) -> float | None:
        if not curve or len(curve) < 2:
            return None
        try:
            d0 = dt.date.fromisoformat(str(curve[0][0])[:10])
            d1 = dt.date.fromisoformat(str(curve[-1][0])[:10])
            days = max(1, (d1 - d0).days)
            ratio = float(curve[-1][1]) / max(1e-12, float(curve[0][1]))
            if ratio <= 0:
                return None
            return (ratio ** (365.0 / float(days))) - 1.0
        except Exception:
            return None

    def _max_drawdown(curve: list[tuple[str, float]]) -> float | None:
        if not curve or len(curve) < 2:
            return None
        peak = None
        max_dd = 0.0
        try:
            for _d, v in curve:
                x = float(v)
                if peak is None or x > peak:
                    peak = x
                if peak and peak > 0:
                    dd = (x / peak) - 1.0
                    if dd < max_dd:
                        max_dd = dd
            return float(max_dd)
        except Exception:
            return None

    def _aligned_returns(
        a: list[tuple[str, float]], b: list[tuple[str, float]]
    ) -> tuple[list[float], list[float], list[float]]:
        am = {str(d)[:10]: float(v) for d, v in (a or [])}
        bm = {str(d)[:10]: float(v) for d, v in (b or [])}
        dates = sorted(set(am.keys()) & set(bm.keys()))
        if len(dates) < 3:
            return [], [], []
        pa: list[float] = []
        pb: list[float] = []
        ex: list[float] = []
        prev = dates[0]
        for d in dates[1:]:
            av0 = am.get(prev)
            av1 = am.get(d)
            bv0 = bm.get(prev)
            bv1 = bm.get(d)
            prev = d
            if not av0 or not av1 or not bv0 or not bv1:
                continue
            ra = (av1 / av0) - 1.0
            rb = (bv1 / bv0) - 1.0
            pa.append(float(ra))
            pb.append(float(rb))
            ex.append(float(ra - rb))
        return pa, pb, ex

    def _std(vals: list[float]) -> float | None:
        if not vals or len(vals) < 2:
            return None
        m = sum(vals) / float(len(vals))
        var = sum((x - m) ** 2 for x in vals) / float(len(vals) - 1)
        return var ** 0.5

    def _corr(a: list[float], b: list[float]) -> float | None:
        if not a or not b or len(a) != len(b) or len(a) < 2:
            return None
        ma = sum(a) / float(len(a))
        mb = sum(b) / float(len(b))
        num = sum((x - ma) * (y - mb) for x, y in zip(a, b))
        da = _std(a)
        db = _std(b)
        if not da or not db or da == 0 or db == 0:
            return None
        return num / ((len(a) - 1) * da * db)

    pa, pb, ex = _aligned_returns(portfolio_curve, bench_curve)
    periods_per_year = 12.0 if freq == "month_end" else 252.0
    te = None
    ex_std = _std(ex)
    if ex_std is not None:
        te = ex_std * (periods_per_year ** 0.5)

    comparison = {
        "portfolio_cagr": _cagr(portfolio_curve),
        "benchmark_cagr": _cagr(bench_curve),
        "portfolio_max_drawdown": _max_drawdown(portfolio_curve),
        "benchmark_max_drawdown": _max_drawdown(bench_curve),
        "tracking_error": te,
        "correlation": _corr(pa, pb),
    }

    def _drawdown_date(curve: list[tuple[str, float]]) -> str | None:
        if not curve or len(curve) < 2:
            return None
        peak = None
        min_dd = 0.0
        min_date = None
        try:
            for d, v in curve:
                x = float(v)
                if peak is None or x > peak:
                    peak = x
                if peak and peak > 0:
                    dd = (x / peak) - 1.0
                    if dd < min_dd:
                        min_dd = dd
                        min_date = str(d)[:10]
            return min_date
        except Exception:
            return None

    def _fmt_pct(x: float | None) -> str | None:
        if x is None:
            return None
        try:
            return f"{float(x) * 100.0:.2f}%"
        except Exception:
            return None

    def _fmt_dd(x: float | None) -> str | None:
        if x is None:
            return None
        try:
            return f"{float(x) * 100.0:.2f}%"
        except Exception:
            return None

    def _build_commentary(*, prior_ctx: dict[str, object] | None) -> dict[str, object]:
        sentences: list[str] = []
        notes: list[str] = []

        p_twr = getattr(primary_row, "twr", None) if primary_row else None
        b_twr = getattr(primary_row, "benchmark_twr", None) if primary_row else None
        ex_twr = getattr(primary_row, "excess_twr", None) if primary_row else None
        p_sharpe = getattr(primary_row, "sharpe", None) if primary_row else None
        b_sharpe = getattr(primary_row, "benchmark_sharpe", None) if primary_row else None

        p_twr_s = _fmt_pct(p_twr)
        b_twr_s = _fmt_pct(b_twr)
        ex_twr_s = _fmt_pct(ex_twr)

        bench_sym = str(report.get("benchmark_label") or benchmark_symbol or "Benchmark")
        if p_twr_s and b_twr_s and ex_twr_s:
            sentences.append(
                f"Over {period_label}, the portfolio returned {p_twr_s} (TWR) versus {b_twr_s} for {bench_sym}, an excess return of {ex_twr_s}."
            )
        elif p_twr_s:
            sentences.append(f"Over {period_label}, the portfolio returned {p_twr_s} (TWR). Benchmark comparison is partially unavailable.")
        else:
            sentences.append(f"Over {period_label}, portfolio performance could not be computed from available valuation points.")

        if prior_ctx and prior_ctx.get("primary") is not None:
            try:
                prior_row = prior_ctx.get("primary")
                prior_twr = getattr(prior_row, "twr", None)
                prior_dd = prior_ctx.get("comparison", {}).get("portfolio_max_drawdown") if isinstance(prior_ctx.get("comparison"), dict) else None
                if p_twr is not None and prior_twr is not None:
                    dp = float(p_twr) - float(prior_twr)
                    dp_s = _fmt_pct(dp) or ""
                    improved = "improved" if dp > 0 else ("declined" if dp < 0 else "was unchanged")
                    if prior_dd is not None and comparison.get("portfolio_max_drawdown") is not None:
                        dd_delta = float(comparison.get("portfolio_max_drawdown") or 0.0) - float(prior_dd or 0.0)
                        dd_note = "and drawdowns were shallower" if dd_delta > 0.01 else ("and drawdowns were deeper" if dd_delta < -0.01 else "with similar drawdowns")
                        sentences.append(f"Compared to the prior period, return {improved} by {dp_s} {dd_note}.")
                    else:
                        sentences.append(f"Compared to the prior period, return {improved} by {dp_s}.")
            except Exception:
                pass

        if p_sharpe is not None and b_sharpe is not None:
            try:
                dp = float(p_sharpe)
                db = float(b_sharpe)
                delta = dp - db
                if delta > 0.10:
                    sentences.append(f"Risk-adjusted performance was stronger than the benchmark (Sharpe {dp:.2f} vs {db:.2f}).")
                elif delta < -0.10:
                    sentences.append(f"Risk-adjusted performance lagged the benchmark (Sharpe {dp:.2f} vs {db:.2f}).")
                else:
                    sentences.append(f"Risk-adjusted performance was broadly similar to the benchmark (Sharpe {dp:.2f} vs {db:.2f}).")
            except Exception:
                pass
        elif p_sharpe is not None:
            try:
                sentences.append(f"Sharpe for the portfolio was {float(p_sharpe):.2f} (benchmark Sharpe unavailable).")
            except Exception:
                pass

        p_dd = _fmt_dd(comparison.get("portfolio_max_drawdown"))
        b_dd = _fmt_dd(comparison.get("benchmark_max_drawdown"))
        if p_dd and b_dd:
            try:
                pdd = float(comparison.get("portfolio_max_drawdown"))  # type: ignore[arg-type]
                bdd = float(comparison.get("benchmark_max_drawdown"))  # type: ignore[arg-type]
                if (pdd - bdd) < -0.01:
                    sentences.append(f"The portfolio experienced deeper drawdowns ({p_dd}) than the benchmark ({b_dd}) during the period.")
                elif (pdd - bdd) > 0.01:
                    sentences.append(f"Drawdowns were shallower than the benchmark ({p_dd} vs {b_dd}).")
                else:
                    sentences.append(f"Drawdown severity was similar ({p_dd} vs {b_dd}).")
            except Exception:
                sentences.append(f"Drawdown severity was similar ({p_dd} vs {b_dd}).")
        elif comparison.get("tracking_error") is not None:
            try:
                te_v = float(comparison.get("tracking_error") or 0.0)
                te_pct = te_v * 100.0
                level = "low" if te_pct < 5.0 else ("moderate" if te_pct <= 10.0 else "high")
                sentences.append(f"Tracking error was {te_pct:.2f}%, indicating {level} deviation from the benchmark.")
            except Exception:
                pass

        dd_date = _drawdown_date(portfolio_curve)
        if dd_date:
            notes.append(f"Largest drawdown occurred around {dd_date}.")

        if benchmark_info.get("warning"):
            notes.append("Some benchmark ranges could not be fetched; comparisons use available overlapping dates.")

        if prior_ctx:
            try:
                obs = prior_ctx.get("obs") if isinstance(prior_ctx.get("obs"), dict) else None
                if prior_ctx.get("error") or (obs and int(obs.get("portfolio_returns") or 0) < 10):
                    notes.append("Prior-period comparison is limited due to sparse observations.")
            except Exception:
                pass

        # Clamp to 3 sentences (per UX spec).
        sentences = sentences[:3]
        return {"sentences": sentences, "notes": notes[:2]}

    commentary = _build_commentary(prior_ctx=prior)

    from src.app.main import templates

    return templates.TemplateResponse(
        "reports_performance.html",
        {
            "request": request,
            "actor": actor,
            "auth_banner": None,
            "auth_banner_detail": auth_banner_message(),
            "static_version": _static_version(),
            "perf_js_version": _static_version_for("reports_performance.js"),
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
            "custom_start": custom_start,
            "custom_end": custom_end,
            "benchmark_info": benchmark_info,
            "benchmark_symbol": benchmark_symbol,
            "bench_provider": bench_provider,
            "report": report,
            "chart_data": chart_data,
            "chart_data_json": chart_data_json,
            "comparison": comparison,
            "commentary": commentary,
            "prior": prior,
            "compare_prior": compare_prior,
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
    from src.core.performance import build_performance_report
    from src.investor.marketdata.benchmarks import BenchmarkDataClient
    from src.investor.marketdata.config import load_marketdata_config

    today = dt.date.today()
    scope = _parse_reports_scope(request.query_params.get("scope"))
    period = (request.query_params.get("period") or "ytd").strip().lower()
    freq = (request.query_params.get("freq") or "month_end").strip().lower()
    bench_provider = (request.query_params.get("bench_provider") or "auto").strip().lower()
    account_id_raw = (request.query_params.get("account_id") or "").strip()
    account_id = int(account_id_raw) if account_id_raw.isdigit() else None

    year_raw = (request.query_params.get("year") or "").strip()
    year_options = _available_year_options(session, scope=scope, today=today, kind="performance")
    year = int(year_raw) if year_raw.isdigit() else (today.year if today.year in year_options else year_options[0])
    if year not in year_options:
        year = year_options[0]

    custom_start = (request.query_params.get("start") or "").strip()
    custom_end = (request.query_params.get("end") or "").strip()
    start_date, end_date, _period_label = _parse_period_dates_for_performance(
        period=period,
        year=year,
        today=today,
        custom_start=custom_start or None,
        custom_end=custom_end or None,
    )

    benchmark_symbol = (request.query_params.get("benchmark") or "SPY").strip().upper()
    bench_fetch_start = start_date - dt.timedelta(days=40)
    bench_fetch_end = end_date
    bench_series: list[tuple[dt.date, float]] | None = None
    if bench_provider not in {"none", "off", "disabled"}:
        cfg, _cfg_path = load_marketdata_config()
        bench_cfg = cfg.benchmarks.model_copy(deep=True)
        sel = (bench_provider or "auto").strip().lower()
        if sel in {"cache", "local"}:
            bench_cfg.provider_order = ["cache"]
        elif sel == "stooq":
            bench_cfg.provider_order = ["cache", "stooq", "yahoo"]
        elif sel == "yahoo":
            bench_cfg.provider_order = ["cache", "yahoo"]
        try:
            client = BenchmarkDataClient(config=bench_cfg)
            bench_df, _bench_meta = client.get(
                symbol=benchmark_symbol,
                start=bench_fetch_start,
                end=bench_fetch_end,
                refresh=False,
            )
            if bench_df is not None and not bench_df.empty:
                col = "adj_close" if "adj_close" in bench_df.columns else "close"
                vals = bench_df[col].copy()
                if col == "adj_close" and "close" in bench_df.columns:
                    vals = vals.fillna(bench_df["close"])
                bench_series = [(d.date(), float(v)) for d, v in vals.items() if v is not None and float(v) > 0.0]
        except Exception:
            bench_series = None
    report = build_performance_report(
        session,
        scope=scope,
        start_date=start_date,
        end_date=end_date,
        frequency=freq,
        benchmark_series=bench_series,
        benchmark_label=(benchmark_symbol or "SPY"),
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
