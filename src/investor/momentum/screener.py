from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from decimal import Decimal

import pandas as pd
from sqlalchemy.orm import Session

from src.importers.adapters import ProviderError
from src.investor.momentum.calcs import (
    SectorMomentum,
    StockMomentum,
    avg_dollar_vol,
    dist_to_52w_high_pct,
    equal_weight_sector_return,
    pct_return_from_lookback,
    preferred_close,
    sma,
    sma_slope,
    ytd_return,
)
from src.investor.momentum.classification import ClassificationService
from src.investor.momentum.prices import MarketDataService
from src.investor.momentum.utils import normalize_ticker


@dataclass(frozen=True)
class MomentumDashboardResult:
    as_of: dt.date
    universe_label: str
    universe_count: int
    rows_used: int
    warnings: list[str]
    sector_rows: list[SectorMomentum]
    stock_rows: list[StockMomentum]


def _today() -> dt.date:
    return dt.date.today()


def _as_of_date(raw: str) -> dt.date:
    s = (raw or "").strip()
    if not s:
        return _today()
    try:
        return dt.date.fromisoformat(s[:10])
    except Exception:
        return _today()


def build_momentum_dashboard(
    session: Session,
    *,
    tickers: list[str],
    universe_label: str,
    as_of: dt.date | None = None,
    price_provider: str = "stooq",
    liquid_only: bool = False,
    min_avg_dvol_20d: float = 10_000_000.0,
    max_universe: int = 800,
    auto_fetch_limit: int = 40,
) -> MomentumDashboardResult:
    """
    Builds sector + stock leadership views (MVP).

    This is deterministic given a fixed price cache and as_of date.
    """
    asof = as_of or _today()
    md = MarketDataService(provider=price_provider)
    cls = ClassificationService()

    ts = [normalize_ticker(t) for t in tickers]
    ts = [t for t in ts if t]
    ts = list(dict.fromkeys(ts))
    if max_universe and len(ts) > max_universe:
        ts = ts[: int(max_universe)]

    warnings: list[str] = []
    if not ts:
        return MomentumDashboardResult(
            as_of=asof,
            universe_label=universe_label,
            universe_count=0,
            rows_used=0,
            warnings=["No tickers selected."],
            sector_rows=[],
            stock_rows=[],
        )

    # Minimum start to compute YTD base + SMA200 + 52w high + avg $vol.
    start_needed = asof - dt.timedelta(days=420)

    # Classification lookup.
    class_map = cls.get_map(session, ts)

    stock_rows: list[StockMomentum] = []
    fetched = 0
    for t in ts:
        df, meta = md.get_daily(session, ticker=t, start=start_needed, end=asof, refresh=False)
        if meta.warning:
            warnings.append(meta.warning)
        if meta.rows_fetched > 0:
            fetched += 1

        if df is None or df.empty:
            continue
        close_s = preferred_close(df)
        close_s = close_s.dropna()
        if close_s.empty:
            continue

        # Compute "as-of" point as last available close in range.
        last_close = float(close_s.iloc[-1])
        if not (last_close > 0):
            continue

        ytd = ytd_return(close_s, as_of=asof)
        r3m = pct_return_from_lookback(close_s, 63)
        r1m = pct_return_from_lookback(close_s, 21)

        sma200_s = sma(close_s, 200)
        sma50_s = sma(close_s, 50)
        sma200_last = sma200_s.dropna().iloc[-1] if sma200_s.dropna().shape[0] else None
        sma50_last = sma50_s.dropna().iloc[-1] if sma50_s.dropna().shape[0] else None

        above200 = None
        sma50_gt_200 = None
        if sma200_last is not None and float(sma200_last) > 0:
            above200 = bool(last_close > float(sma200_last))
            if sma50_last is not None and float(sma50_last) > 0:
                sma50_gt_200 = bool(float(sma50_last) > float(sma200_last))

        slope50 = sma_slope(close_s, window=50, slope_window=20)
        dist52w = dist_to_52w_high_pct(close_s, window=252)

        vol_s = pd.to_numeric(df.get("volume"), errors="coerce") if "volume" in df.columns else pd.Series(dtype=float)
        avg_dvol = avg_dollar_vol(close_s, vol_s, window=20)

        if liquid_only and avg_dvol is not None and avg_dvol < float(min_avg_dvol_20d):
            continue
        if liquid_only and avg_dvol is None:
            continue

        c = class_map.get(t)
        sector = (c.sector if c and c.sector else None) or "Unknown"

        stock_rows.append(
            StockMomentum(
                ticker=t,
                sector=sector,
                ytd=ytd,
                ret_3m=r3m,
                ret_1m=r1m,
                close=last_close,
                above_sma200=above200,
                sma50_gt_sma200=sma50_gt_200,
                sma50_slope_20d=slope50,
                dist_52w_high=dist52w,
                avg_dvol_20d=avg_dvol,
            )
        )

    if fetched >= auto_fetch_limit:
        warnings.append(
            f"Fetched prices for {fetched} ticker(s) from the network in this request. "
            "For large universes, warm the cache in batches to avoid timeouts."
        )

    # Sector aggregation.
    by_sector: dict[str, list[StockMomentum]] = {}
    for r in stock_rows:
        by_sector.setdefault(r.sector or "Unknown", []).append(r)

    sector_rows: list[SectorMomentum] = []
    for sector, members in by_sector.items():
        y = equal_weight_sector_return((m.ytd for m in members))
        m3 = equal_weight_sector_return((m.ret_3m for m in members))
        m1 = equal_weight_sector_return((m.ret_1m for m in members))

        above_vals = [m.above_sma200 for m in members if m.above_sma200 is not None]
        breadth = None
        if above_vals:
            breadth = sum((1 for v in above_vals if v)) / float(len(above_vals))

        leaders = sorted(
            [m for m in members if m.ytd is not None],
            key=lambda r: (-(r.ytd or -999.0), r.ticker),
        )[:3]
        sector_rows.append(
            SectorMomentum(
                sector=sector,
                ytd=y,
                ret_3m=m3,
                ret_1m=m1,
                breadth_above_sma200=breadth,
                leaders=[l.ticker for l in leaders],
            )
        )

    sector_rows.sort(key=lambda r: (-(r.ytd or -999.0), r.sector))
    stock_rows.sort(key=lambda r: (-(r.ytd or -999.0), r.ticker))

    return MomentumDashboardResult(
        as_of=asof,
        universe_label=universe_label,
        universe_count=len(ts),
        rows_used=len(stock_rows),
        warnings=warnings,
        sector_rows=sector_rows,
        stock_rows=stock_rows,
    )


@dataclass(frozen=True)
class SectorDetailResult:
    sector: str
    as_of: dt.date
    start: dt.date
    end: dt.date
    benchmark: str
    members_count: int
    members_used: int
    warnings: list[str]
    curve_sector: list[tuple[str, float]]
    curve_benchmark: list[tuple[str, float]]
    stock_rows: list[StockMomentum]


def _align_on_dates(series: pd.Series, dates: list[pd.Timestamp]) -> list[float]:
    s = series.dropna()
    if s.empty:
        return [float("nan")] * len(dates)
    out: list[float] = []
    # forward-fill via last value on or before date
    for d in dates:
        hit = s.loc[s.index <= d]
        if hit.empty:
            out.append(float("nan"))
        else:
            out.append(float(hit.iloc[-1]))
    return out


def build_sector_detail(
    session: Session,
    *,
    sector: str,
    tickers: list[str],
    benchmark: str = "SPY",
    start: dt.date,
    end: dt.date,
    price_provider: str = "stooq",
    liquid_only: bool = False,
    min_avg_dvol_20d: float = 10_000_000.0,
) -> SectorDetailResult:
    md = MarketDataService(provider=price_provider)
    cls = ClassificationService()
    asof = end
    sec = (sector or "").strip() or "Unknown"

    ts = [normalize_ticker(t) for t in tickers]
    ts = [t for t in ts if t]
    ts = list(dict.fromkeys(ts))
    class_map = cls.get_map(session, ts)
    warnings: list[str] = []

    start_needed = start - dt.timedelta(days=420)
    bench = normalize_ticker(benchmark) or "SPY"
    bench_df, bench_meta = md.get_daily(session, ticker=bench, start=start_needed, end=end, refresh=False)
    if bench_meta.warning:
        warnings.append(bench_meta.warning)
    bench_close = preferred_close(bench_df).dropna()
    bench_close = bench_close[bench_close.index.date >= start]
    if bench_close.empty:
        warnings.append(f"Benchmark prices unavailable for {bench}.")
        dates = []
    else:
        dates = [pd.Timestamp(d) for d in bench_close.index]

    stock_rows: list[StockMomentum] = []
    member_curves: list[list[float]] = []
    for t in ts:
        df, meta = md.get_daily(session, ticker=t, start=start_needed, end=end, refresh=False)
        if meta.warning:
            warnings.append(meta.warning)
        if df is None or df.empty:
            continue
        close_s = preferred_close(df).dropna()
        if close_s.empty:
            continue
        # Filter to window.
        close_in = close_s[close_s.index.date >= start]
        if close_in.empty:
            continue

        # Compute liquidity before including in curve (optional).
        vol_s = pd.to_numeric(df.get("volume"), errors="coerce") if "volume" in df.columns else pd.Series(dtype=float)
        avg_dvol = avg_dollar_vol(close_s, vol_s, window=20)
        if liquid_only and (avg_dvol is None or avg_dvol < float(min_avg_dvol_20d)):
            continue

        # Normalize to growth of $1 over the displayed date axis (benchmark axis).
        if dates:
            aligned = _align_on_dates(close_in, dates)
            # base is first non-NaN
            base = next((v for v in aligned if v == v and v > 0), None)
            if base and base > 0:
                member_curves.append([float(v / base) if (v == v and v > 0) else float("nan") for v in aligned])

        last_close = float(close_s.iloc[-1])
        ytd = ytd_return(close_s, as_of=asof)
        r3m = pct_return_from_lookback(close_s, 63)
        r1m = pct_return_from_lookback(close_s, 21)

        sma200_s = sma(close_s, 200)
        sma50_s = sma(close_s, 50)
        sma200_last = sma200_s.dropna().iloc[-1] if sma200_s.dropna().shape[0] else None
        sma50_last = sma50_s.dropna().iloc[-1] if sma50_s.dropna().shape[0] else None

        above200 = None
        sma50_gt_200 = None
        if sma200_last is not None and float(sma200_last) > 0:
            above200 = bool(last_close > float(sma200_last))
            if sma50_last is not None and float(sma50_last) > 0:
                sma50_gt_200 = bool(float(sma50_last) > float(sma200_last))

        slope50 = sma_slope(close_s, window=50, slope_window=20)
        dist52w = dist_to_52w_high_pct(close_s, window=252)

        c = class_map.get(t)
        sec_label = (c.sector if c and c.sector else None) or "Unknown"
        if sec_label != sec:
            # Only include stocks that map to this sector (caller should pre-filter).
            continue

        stock_rows.append(
            StockMomentum(
                ticker=t,
                sector=sec_label,
                ytd=ytd,
                ret_3m=r3m,
                ret_1m=r1m,
                close=last_close,
                above_sma200=above200,
                sma50_gt_sma200=sma50_gt_200,
                sma50_slope_20d=slope50,
                dist_52w_high=dist52w,
                avg_dvol_20d=avg_dvol,
            )
        )

    # Equal-weight sector curve.
    curve_sector: list[tuple[str, float]] = []
    if dates and member_curves:
        for i, d in enumerate(dates):
            vals = [c[i] for c in member_curves if i < len(c) and c[i] == c[i]]
            if not vals:
                continue
            curve_sector.append((d.date().isoformat(), float(sum(vals) / float(len(vals)))))

    curve_bench: list[tuple[str, float]] = []
    if not bench_close.empty:
        base = float(bench_close.iloc[0])
        if base > 0:
            for idx, v in bench_close.items():
                if float(v) > 0:
                    curve_bench.append((idx.date().isoformat(), float(v) / base))

    stock_rows.sort(key=lambda r: (-(r.ytd or -999.0), r.ticker))

    return SectorDetailResult(
        sector=sec,
        as_of=asof,
        start=start,
        end=end,
        benchmark=bench,
        members_count=len(ts),
        members_used=len(stock_rows),
        warnings=warnings,
        curve_sector=curve_sector,
        curve_benchmark=curve_bench,
        stock_rows=stock_rows,
    )
