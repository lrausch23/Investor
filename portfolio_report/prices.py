from __future__ import annotations

import csv
import datetime as dt
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from portfolio_report.util import parse_date, parse_money, sniff_delimiter


@dataclass(frozen=True)
class PriceSeries:
    symbol: str
    points: list[tuple[dt.date, float]]  # sorted, deduped
    warnings: list[str]

    def price_on_or_before(self, d: dt.date) -> float | None:
        pts = self.points
        if not pts:
            return None
        lo, hi = 0, len(pts) - 1
        best: float | None = None
        while lo <= hi:
            mid = (lo + hi) // 2
            md, mv = pts[mid]
            if md <= d:
                best = float(mv)
                lo = mid + 1
            else:
                hi = mid - 1
        return best

    def returns_by_month_end(self, month_ends: list[dt.date]) -> dict[dt.date, float]:
        out: dict[dt.date, float] = {}
        prev_px: float | None = None
        for me in sorted(month_ends):
            px = self.price_on_or_before(me)
            if px is None or px <= 0:
                continue
            if prev_px is not None and prev_px > 0:
                out[me] = float(px / prev_px - 1.0)
            prev_px = px
        return out


def _norm_key(s: str) -> str:
    return "".join(ch.lower() if ch.isalnum() else "_" for ch in (s or "")).strip("_")


def load_price_csv(path: Path, symbol: str) -> PriceSeries:
    warnings: list[str] = []
    text = path.read_text(encoding="utf-8-sig", errors="replace")
    delim = sniff_delimiter(text)
    reader = csv.DictReader(text.splitlines(), delimiter=delim)
    out: list[tuple[dt.date, float]] = []
    for row in reader:
        if not row:
            continue
        norm = {_norm_key(k): k for k in row.keys() if k}
        dk = norm.get("date") or norm.get("day") or norm.get("as_of") or norm.get("timestamp")
        if not dk:
            continue
        d = parse_date(row.get(dk))
        if d is None:
            continue
        ck = (
            norm.get("adj_close")
            or norm.get("adjclose")
            or norm.get("close")
            or norm.get("value")
            or norm.get("price")
        )
        if not ck:
            continue
        px = parse_money(row.get(ck))
        if px is None or px <= 0:
            continue
        out.append((d, float(px)))
    out.sort(key=lambda x: x[0])
    dedup: dict[dt.date, float] = {}
    for d, px in out:
        dedup[d] = float(px)
    pts = sorted(dedup.items(), key=lambda x: x[0])
    if not pts:
        warnings.append(f"No usable prices parsed from {path.name}.")
    return PriceSeries(symbol=symbol, points=pts, warnings=warnings)


def load_prices(
    *,
    symbols: list[str],
    prices_dir: Path,
    start: dt.date,
    end: dt.date,
    download: bool,
) -> tuple[dict[str, PriceSeries], list[str]]:
    warnings: list[str] = []
    series: dict[str, PriceSeries] = {}

    # Prefer robust yfinance-backed cache when available (keeps one file per ticker under prices_dir/yfinance).
    # Fall back to the legacy ./data/prices/{TICKER}.csv cache when needed.
    yfinance_cache_dir = prices_dir / "yfinance"

    md_available = True
    try:
        from market_data.cache import PriceCache
        from market_data.symbols import normalize_ticker
        from market_data.utils import update_cache
    except Exception:
        md_available = False

    cache_update_summary = None
    if download and md_available:
        try:
            cache_update_summary = update_cache(
                tickers=symbols,
                start=start,
                end=end,
                auto_adjust=True,
                cache_dir=yfinance_cache_dir,
                base_currency="USD",
            )
        except Exception as e:
            warnings.append(f"Market data cache update failed: {e}")
    if cache_update_summary:
        try:
            failed = int(cache_update_summary.get("failed") or 0)
            updated = int(cache_update_summary.get("updated") or 0)
            skipped = int(cache_update_summary.get("skipped") or 0)
            details = cache_update_summary.get("details") or {}
            failed_syms = [k for k, v in details.items() if isinstance(v, dict) and v.get("status") == "failed"]
            if failed > 0:
                # Keep the report readable; list a small sample.
                sample = ", ".join(sorted(failed_syms)[:8])
                suffix = "..." if len(failed_syms) > 8 else ""
                warnings.append(f"Price download failed for {failed} ticker(s): {sample}{suffix}")
                # Add a tiny diagnostic sample of error reasons.
                err_samples: list[str] = []
                for sym in sorted(failed_syms)[:3]:
                    v = details.get(sym) or {}
                    e = str(v.get("error") or "").strip()
                    pt = str(v.get("provider_ticker") or "").strip()
                    if e:
                        err_samples.append(f"{sym}{'→'+pt if pt and pt!=sym else ''}: {e}")
                if err_samples:
                    warnings.append("Download errors (sample): " + " | ".join(err_samples))
            if updated > 0:
                warnings.append(f"Price cache updated for {updated} ticker(s) (skipped {skipped}).")
        except Exception:
            pass

    cache = None
    if md_available:
        try:
            cache = PriceCache(yfinance_cache_dir)
        except Exception:
            cache = None

    invalid_symbols: list[str] = []
    failed_symbols: set[str] = set()
    if cache_update_summary:
        try:
            details = cache_update_summary.get("details") or {}
            failed_symbols = {k for k, v in details.items() if isinstance(v, dict) and v.get("status") == "failed"}
        except Exception:
            failed_symbols = set()
    for sym in symbols:
        # Synthetic cash/currency handling via market_data when available.
        if md_available:
            try:
                ns = normalize_ticker(sym, base_currency="USD")
                if ns.kind == "invalid":
                    invalid_symbols.append(sym)
                    continue
                if ns.kind == "synthetic_cash":
                    series[sym] = PriceSeries(symbol=sym, points=[(start, 1.0), (end, 1.0)], warnings=[])
                    continue
            except Exception:
                pass

        # 1) yfinance cache
        if cache is not None:
            try:
                df = cache.load(sym)
                if df is not None and not df.empty:
                    # Use adjusted close semantics: for auto_adjust=True, close is adjusted.
                    col = "close"
                    if "adj_close" in getattr(df, "columns", []):
                        col = "adj_close"
                    pts: list[tuple[dt.date, float]] = []
                    for idx, row in df.iterrows():
                        try:
                            d = idx.date() if hasattr(idx, "date") else dt.date.fromisoformat(str(idx)[:10])
                            px = float(row.get(col))
                            if px > 0:
                                pts.append((d, px))
                        except Exception:
                            continue
                    pts.sort(key=lambda x: x[0])
                    # Dedup by date.
                    dedup: dict[dt.date, float] = {}
                    for d, px in pts:
                        dedup[d] = float(px)
                    pts = sorted(dedup.items(), key=lambda x: x[0])
                    if pts:
                        series[sym] = PriceSeries(symbol=sym, points=pts, warnings=[])
                        continue
            except Exception:
                pass

        # 2) legacy local CSV (symbol or normalized provider ticker)
        p = prices_dir / f"{sym}.csv"
        if not p.exists() and md_available:
            try:
                ns = normalize_ticker(sym, base_currency="USD")
                if ns.provider_ticker:
                    p2 = prices_dir / f"{ns.provider_ticker}.csv"
                    if p2.exists():
                        p = p2
            except Exception:
                pass

        if p.exists():
            series[sym] = load_price_csv(p, sym)

    # Final missing warnings.
    for sym in symbols:
        if sym in invalid_symbols:
            continue
        # Avoid repeating the same information: if a download was attempted and failed, the report already
        # includes an aggregate "Price download failed ..." message above.
        if sym in failed_symbols:
            continue
        if sym not in series or not series[sym].points:
            warnings.append(f"Missing price series for {sym}; some analytics will be excluded.")

    if invalid_symbols:
        # Keep this as a single warning to reduce noise.
        bad = ", ".join(sorted({str(s) for s in invalid_symbols})[:10])
        suffix = "..." if len(set(invalid_symbols)) > 10 else ""
        warnings.append(f"Skipped invalid/unpriced symbols: {bad}{suffix}")

    return series, warnings


def daily_returns(points: list[tuple[dt.date, float]]) -> dict[dt.date, float]:
    out: dict[dt.date, float] = {}
    prev: float | None = None
    for d, px in sorted(points, key=lambda x: x[0]):
        if prev is not None and prev > 0 and px > 0:
            out[d] = float(px / prev - 1.0)
        prev = float(px)
    return out
