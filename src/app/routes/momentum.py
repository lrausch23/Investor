from __future__ import annotations

import csv
import datetime as dt
import io
from typing import Any

from fastapi import APIRouter, Depends, File, Form, Request, UploadFile
from fastapi.responses import RedirectResponse, Response
from sqlalchemy.orm import Session

from src.app.auth import auth_banner_message, require_actor
from src.app.db import db_session
from src.db.models import UniverseMembership, WatchlistItem
from src.investor.momentum.classification import ClassificationRow, ClassificationService
from src.investor.momentum.screener import build_momentum_dashboard, build_sector_detail
from src.investor.momentum.stooq_universe import fetch_stooq_index_components
from src.investor.momentum.universe import get_universe, list_universe_options
from src.investor.momentum.utils import normalize_ticker, parse_ticker_list
from src.investor.momentum.prices import MarketDataService


router = APIRouter(prefix="/momentum", tags=["momentum"])


def _static_version() -> str:
    """
    Bust cache for momentum pages when CSS/JS changes.
    """
    try:
        static_dir = __file__
    except Exception:
        return "0"
    try:
        from pathlib import Path

        p = Path(__file__).resolve().parents[1] / "static"
        css_m = int((p / "app.css").stat().st_mtime) if (p / "app.css").exists() else 0
        js_m = int((p / "momentum.js").stat().st_mtime) if (p / "momentum.js").exists() else 0
        return str(max(css_m, js_m, 0))
    except Exception:
        return "0"


def _parse_date(raw: str, *, default: dt.date) -> dt.date:
    s = (raw or "").strip()
    if not s:
        return default
    try:
        return dt.date.fromisoformat(s[:10])
    except Exception:
        return default


def _period_bounds(period: str, *, as_of: dt.date) -> tuple[dt.date, dt.date]:
    p = (period or "").strip().lower() or "ytd"
    end = as_of
    if p == "1m":
        return end - dt.timedelta(days=31), end
    if p == "3m":
        return end - dt.timedelta(days=93), end
    if p == "6m":
        return end - dt.timedelta(days=186), end
    if p == "1y":
        return end - dt.timedelta(days=366), end
    if p == "ytd":
        return dt.date(end.year, 1, 1), end
    # custom handled elsewhere
    return dt.date(end.year, 1, 1), end


def _fmt_pct(v: float | None) -> str:
    if v is None:
        return "—"
    return f"{v*100.0:.2f}%"


def _fmt_bool(v: bool | None) -> str:
    if v is None:
        return "—"
    return "Yes" if v else "No"


def _csv_response(*, filename: str, rows: list[list[Any]]) -> Response:
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    for r in rows:
        w.writerow([("" if v is None else v) for v in r])
    return Response(
        content=buf.getvalue(),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.get("")
def momentum_dashboard(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    universe: str = "sp500",
    period: str = "ytd",
    as_of: str = "",
    start: str = "",
    end: str = "",
    benchmark: str = "SPY",
    price_provider: str = "stooq",
    liquid_only: str = "",
    q: str = "",
    stock_sort: str = "ytd",
    stock_dir: str = "desc",
    sector_sort: str = "ytd",
    sector_dir: str = "desc",
    custom_list: str = "",
):
    asof = _parse_date(as_of, default=dt.date.today())
    if (period or "").strip().lower() == "custom":
        start_d = _parse_date(start, default=asof - dt.timedelta(days=366))
        end_d = _parse_date(end, default=asof)
    else:
        start_d, end_d = _period_bounds(period, as_of=asof)

    uni = get_universe(session, universe=universe, custom_list=custom_list)
    liquid = (liquid_only or "").strip().lower() in {"1", "true", "yes", "y", "on"}
    dash = build_momentum_dashboard(
        session,
        tickers=uni.tickers,
        universe_label=uni.label,
        as_of=end_d,
        price_provider=price_provider,
        liquid_only=liquid,
    )

    warnings = list(dash.warnings)
    if uni.warning:
        warnings.insert(0, uni.warning)

    # Search filter (stocks only).
    q_s = (q or "").strip().upper()
    stock_rows = dash.stock_rows
    if q_s:
        stock_rows = [r for r in stock_rows if q_s in r.ticker.upper() or q_s in (r.sector or "").upper()]

    # Sorting (server-side, deterministic).
    stock_sort_key = (stock_sort or "ytd").strip().lower()
    stock_desc = (stock_dir or "desc").strip().lower() != "asc"
    sector_sort_key = (sector_sort or "ytd").strip().lower()
    sector_desc = (sector_dir or "desc").strip().lower() != "asc"

    def _stock_val(r, key: str):
        if key == "ticker":
            return r.ticker
        if key == "sector":
            return r.sector
        if key == "ytd":
            # Default sort favors confirmed uptrends, then momentum.
            up = 1 if (r.uptrend is True) else 0
            y = r.ytd if r.ytd is not None else -999.0
            return (up, y)
        if key == "3m":
            return r.ret_3m if r.ret_3m is not None else -999.0
        if key == "1m":
            return r.ret_1m if r.ret_1m is not None else -999.0
        if key == "close":
            return r.close if r.close is not None else -1.0
        if key == "avgdvol":
            return r.avg_dvol_20d if r.avg_dvol_20d is not None else -1.0
        return r.ytd if r.ytd is not None else -999.0

    def _sector_val(r, key: str):
        if key == "sector":
            return r.sector
        if key == "ytd":
            return r.ytd if r.ytd is not None else -999.0
        if key == "3m":
            return r.ret_3m if r.ret_3m is not None else -999.0
        if key == "1m":
            return r.ret_1m if r.ret_1m is not None else -999.0
        if key == "breadth":
            return r.breadth_above_sma200 if r.breadth_above_sma200 is not None else -1.0
        return r.ytd if r.ytd is not None else -999.0

    stock_rows = sorted(stock_rows, key=lambda r: (_stock_val(r, stock_sort_key), r.ticker), reverse=stock_desc)
    sector_rows = sorted(dash.sector_rows, key=lambda r: (_sector_val(r, sector_sort_key), r.sector), reverse=sector_desc)

    from src.app.main import templates

    return templates.TemplateResponse(
        "momentum_dashboard.html",
        {
            "request": request,
            "actor": actor,
            "auth_banner": None,
            "auth_banner_detail": auth_banner_message(),
            "static_version": _static_version(),
            "title": "Momentum",
            "page_badge": "Screener",
            "ok": request.query_params.get("ok"),
            "error": request.query_params.get("error"),
            "warnings": warnings,
            "universe_options": list_universe_options(),
            "universe": uni.key,
            "custom_list": custom_list,
            "period": period,
            "as_of": asof.isoformat(),
            "start": start_d.isoformat(),
            "end": end_d.isoformat(),
            "benchmark": benchmark,
            "price_provider": (price_provider or "stooq"),
            "liquid_only": liquid,
            "q": q,
            "sector_rows": sector_rows,
            "stock_rows": stock_rows,
            "universe_count": dash.universe_count,
            "rows_used": dash.rows_used,
            "fmt_pct": _fmt_pct,
            "today": dt.date.today().isoformat(),
            "stock_sort": stock_sort_key,
            "stock_dir": "desc" if stock_desc else "asc",
            "sector_sort": sector_sort_key,
            "sector_dir": "desc" if sector_desc else "asc",
        },
    )


@router.get("/export/stocks.csv")
def momentum_export_stocks(
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    universe: str = "sp500",
    period: str = "ytd",
    as_of: str = "",
    start: str = "",
    end: str = "",
    price_provider: str = "stooq",
    liquid_only: str = "",
    q: str = "",
    custom_list: str = "",
):
    asof = _parse_date(as_of, default=dt.date.today())
    if (period or "").strip().lower() == "custom":
        _start_d = _parse_date(start, default=asof - dt.timedelta(days=366))
        end_d = _parse_date(end, default=asof)
    else:
        _start_d, end_d = _period_bounds(period, as_of=asof)
    uni = get_universe(session, universe=universe, custom_list=custom_list)
    liquid = (liquid_only or "").strip().lower() in {"1", "true", "yes", "y", "on"}
    dash = build_momentum_dashboard(
        session,
        tickers=uni.tickers,
        universe_label=uni.label,
        as_of=end_d,
        price_provider=price_provider,
        liquid_only=liquid,
    )
    q_s = (q or "").strip().upper()
    rows = dash.stock_rows
    if q_s:
        rows = [r for r in rows if q_s in r.ticker.upper() or q_s in (r.sector or "").upper()]

    out: list[list[Any]] = [
        [
            "Ticker",
            "Sector",
            "YTD%",
            "3M%",
            "1M%",
            "Close",
            "AboveSMA200",
            "SMA50>SMA200",
            "SMA50Slope20d",
            "Uptrend",
            "DistTo52wHigh%",
            "Avg$Vol20d",
        ]
    ]
    for r in rows:
        out.append(
            [
                r.ticker,
                r.sector,
                _fmt_pct(r.ytd),
                _fmt_pct(r.ret_3m),
                _fmt_pct(r.ret_1m),
                ("" if r.close is None else f"{r.close:.2f}"),
                _fmt_bool(r.above_sma200),
                _fmt_bool(r.sma50_gt_sma200),
                ("" if r.sma50_slope_20d is None else f"{r.sma50_slope_20d:.6f}"),
                _fmt_bool(r.uptrend),
                _fmt_pct(r.dist_52w_high),
                ("" if r.avg_dvol_20d is None else f"{r.avg_dvol_20d:,.0f}"),
            ]
        )
    return _csv_response(filename="momentum_stocks.csv", rows=out)


@router.get("/export/sectors.csv")
def momentum_export_sectors(
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    universe: str = "sp500",
    period: str = "ytd",
    as_of: str = "",
    start: str = "",
    end: str = "",
    price_provider: str = "stooq",
    liquid_only: str = "",
    custom_list: str = "",
):
    asof = _parse_date(as_of, default=dt.date.today())
    if (period or "").strip().lower() == "custom":
        _start_d = _parse_date(start, default=asof - dt.timedelta(days=366))
        end_d = _parse_date(end, default=asof)
    else:
        _start_d, end_d = _period_bounds(period, as_of=asof)
    uni = get_universe(session, universe=universe, custom_list=custom_list)
    liquid = (liquid_only or "").strip().lower() in {"1", "true", "yes", "y", "on"}
    dash = build_momentum_dashboard(
        session,
        tickers=uni.tickers,
        universe_label=uni.label,
        as_of=end_d,
        price_provider=price_provider,
        liquid_only=liquid,
    )
    out: list[list[Any]] = [["Sector", "YTD%", "3M%", "1M%", "Breadth (% above SMA200)", "Top leaders"]]
    for r in dash.sector_rows:
        breadth = "—" if r.breadth_above_sma200 is None else f"{r.breadth_above_sma200*100.0:.0f}%"
        out.append([r.sector, _fmt_pct(r.ytd), _fmt_pct(r.ret_3m), _fmt_pct(r.ret_1m), breadth, ", ".join(r.leaders)])
    return _csv_response(filename="momentum_sectors.csv", rows=out)


@router.get("/sector/{sector}")
def momentum_sector(
    request: Request,
    sector: str,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    universe: str = "sp500",
    period: str = "ytd",
    as_of: str = "",
    start: str = "",
    end: str = "",
    benchmark: str = "SPY",
    price_provider: str = "stooq",
    liquid_only: str = "",
    custom_list: str = "",
):
    asof = _parse_date(as_of, default=dt.date.today())
    if (period or "").strip().lower() == "custom":
        start_d = _parse_date(start, default=asof - dt.timedelta(days=366))
        end_d = _parse_date(end, default=asof)
    else:
        start_d, end_d = _period_bounds(period, as_of=asof)

    uni = get_universe(session, universe=universe, custom_list=custom_list)
    liquid = (liquid_only or "").strip().lower() in {"1", "true", "yes", "y", "on"}

    # Pre-filter tickers for this sector based on stored classification.
    cls = ClassificationService()
    class_map = cls.get_map(session, uni.tickers)
    tickers = [t for t in uni.tickers if (class_map.get(t).sector if class_map.get(t) else "Unknown") == sector]
    if not tickers:
        tickers = uni.tickers  # degrade gracefully

    detail = build_sector_detail(
        session,
        sector=sector,
        tickers=tickers,
        benchmark=benchmark,
        start=start_d,
        end=end_d,
        price_provider=price_provider,
        liquid_only=liquid,
    )

    from src.app.main import templates
    import json

    chart_data = {
        "sector": {"name": sector, "curve": detail.curve_sector},
        "benchmark": {"name": detail.benchmark, "curve": detail.curve_benchmark},
    }

    return templates.TemplateResponse(
        "momentum_sector.html",
        {
            "request": request,
            "actor": actor,
            "auth_banner": None,
            "auth_banner_detail": auth_banner_message(),
            "static_version": _static_version(),
            "title": "Momentum",
            "page_badge": "Sector",
            "ok": request.query_params.get("ok"),
            "error": request.query_params.get("error"),
            "warnings": detail.warnings,
            "sector": sector,
            "universe_options": list_universe_options(),
            "universe": uni.key,
            "custom_list": custom_list,
            "period": period,
            "as_of": asof.isoformat(),
            "start": start_d.isoformat(),
            "end": end_d.isoformat(),
            "benchmark": benchmark,
            "price_provider": (price_provider or "stooq"),
            "liquid_only": liquid,
            "detail": detail,
            "chart_data_json": json.dumps(chart_data),
            "fmt_pct": _fmt_pct,
            "today": dt.date.today().isoformat(),
        },
    )


@router.post("/sector/{sector}/watchlist")
def momentum_sector_add_watchlist(
    sector: str,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    tickers: str = Form(...),
    top_n: str = Form(default="10"),
    return_to: str = Form(default=""),
):
    ts = parse_ticker_list(tickers)
    try:
        n = max(1, min(200, int((top_n or "").strip() or "10")))
    except Exception:
        n = 10
    ts = ts[:n]
    if not ts:
        return RedirectResponse(url=f"/momentum/sector/{sector}?error=No%20tickers%20to%20add", status_code=303)
    created = 0
    for t in ts:
        existing = session.query(WatchlistItem).filter(WatchlistItem.ticker == t).one_or_none()
        if existing is None:
            session.add(WatchlistItem(ticker=t, metadata_json={"source": "momentum"}))
            created += 1
    session.commit()
    dest = return_to.strip() or f"/momentum/sector/{sector}"
    return RedirectResponse(url=f"{dest}?ok=Added%20{created}%20to%20watchlist", status_code=303)


@router.post("/warm")
async def momentum_warm_cache(
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    universe: str = Form(default="sp500"),
    custom_list: str = Form(default=""),
    as_of: str = Form(default=""),
    days: str = Form(default="420"),
    limit: str = Form(default="40"),
    price_provider: str = Form(default="stooq"),
    return_to: str = Form(default="/momentum"),
):
    asof = _parse_date(as_of, default=dt.date.today())
    try:
        d = max(60, min(1200, int((days or "").strip() or "420")))
    except Exception:
        d = 420
    try:
        lim = max(1, min(200, int((limit or "").strip() or "40")))
    except Exception:
        lim = 40
    start = asof - dt.timedelta(days=d)
    uni = get_universe(session, universe=universe, custom_list=custom_list)
    md = MarketDataService(provider=price_provider)
    res = md.warm_cache(session, tickers=uni.tickers, start=start, end=asof, limit=lim)
    msg = f"Warmed prices: fetched={res.get('fetched', 0)} skipped={res.get('skipped', 0)} (of {res.get('total', 0)})"
    return RedirectResponse(url=f"{return_to}?ok={msg.replace(' ', '%20')}", status_code=303)


@router.post("/import/universe")
async def momentum_import_universe(
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    universe: str = Form(...),
    file: UploadFile = File(...),
):
    """
    CSV columns (header, case-insensitive):
      ticker, sector (optional), industry (optional), as_of_date (optional YYYY-MM-DD)

    This writes:
      - universe_membership(universe, ticker)
      - ticker_classification(ticker -> sector/industry) when provided
    """
    u = (universe or "").strip().upper()
    if u not in {"SP500", "NASDAQ100"}:
        return RedirectResponse(url="/momentum?error=Unsupported%20universe", status_code=303)
    raw = (await file.read()).decode("utf-8-sig", errors="replace")
    rdr = csv.DictReader(io.StringIO(raw))
    rows = list(rdr)
    if not rows:
        return RedirectResponse(url="/momentum?error=Empty%20CSV", status_code=303)

    cls = ClassificationService()
    class_rows: list[ClassificationRow] = []
    added = 0
    for r in rows:
        ticker = normalize_ticker(str(r.get("ticker") or r.get("Ticker") or "").strip())
        if not ticker:
            continue
        as_of_s = str(r.get("as_of_date") or r.get("AsOf") or "").strip()
        as_of_d = _parse_date(as_of_s, default=dt.date.today()) if as_of_s else None
        existing = session.query(UniverseMembership).filter(UniverseMembership.universe == u, UniverseMembership.ticker == ticker).one_or_none()
        if existing is None:
            session.add(UniverseMembership(universe=u, ticker=ticker, as_of_date=as_of_d, source="upload"))
            added += 1
        sector = str(r.get("sector") or r.get("Sector") or "").strip() or None
        industry = str(r.get("industry") or r.get("Industry") or "").strip() or None
        if sector or industry:
            class_rows.append(ClassificationRow(ticker=ticker, sector=sector, industry=industry, as_of_date=as_of_d, source="upload"))

    up = cls.upsert_many(session, rows=class_rows)
    session.commit()
    return RedirectResponse(
        url=f"/momentum?ok=Imported%20{added}%20tickers%20into%20{u}%20(%2B{up['created']}%20classifications)",
        status_code=303,
    )


@router.post("/import/stooq-universe")
def momentum_import_stooq_universe(
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    universe: str = Form(...),
):
    """
    Fetch index constituents from Stooq and persist them to universe_membership.

    This is a convenience path to avoid uploading a constituents CSV. It loads tickers only;
    sector/industry classification remains optional (upload path still available).
    """
    u = (universe or "").strip().upper()
    if u not in {"SP500", "NASDAQ100"}:
        return RedirectResponse(url="/momentum?error=Unsupported%20universe", status_code=303)

    idx = "spx" if u == "SP500" else "ndx"
    try:
        res = fetch_stooq_index_components(index_symbol=idx)
    except Exception as e:
        return RedirectResponse(url=f"/momentum?error={(str(e) or 'Failed')}".replace(' ', '%20'), status_code=303)

    added = 0
    for t in res.tickers:
        existing = session.query(UniverseMembership).filter(UniverseMembership.universe == u, UniverseMembership.ticker == t).one_or_none()
        if existing is None:
            session.add(UniverseMembership(universe=u, ticker=t, as_of_date=dt.date.today(), source="stooq"))
            added += 1
    session.commit()
    warn = f"%20(Warn:%20{';%20'.join(res.warnings)})" if res.warnings else ""
    return RedirectResponse(url=f"/momentum?ok=Fetched%20{len(res.tickers)}%20tickers%20from%20Stooq%20({u})%3B%20added%20{added}.{warn}", status_code=303)
