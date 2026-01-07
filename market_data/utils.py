from __future__ import annotations

import datetime as dt
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from market_data.cache import CacheMetadata, PriceCache
from market_data.exceptions import DataNotFoundError, FetchError
from market_data.provider import YahooFinanceProvider
from market_data.symbols import NormalizedSymbol, normalize_ticker

logger = logging.getLogger(__name__)


def _now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def _stable_jitter_seconds(key: str, base: float) -> float:
    import hashlib

    h = hashlib.md5(key.encode("utf-8")).hexdigest()
    frac = (int(h[:8], 16) % 100) / 1000.0  # 0..0.099
    return base * frac


def _retry_sleep(attempt: int, *, key: str, initial: float = 1.0, mult: float = 2.0, max_sleep: float = 20.0) -> float:
    s = min(max_sleep, initial * (mult ** max(0, attempt)))
    s += _stable_jitter_seconds(key, s)
    return float(s)


def _ensure_pandas():  # pragma: no cover (covered implicitly when installed)
    import pandas as pd  # type: ignore

    return pd


def _synthetic_cash_df(*, ticker: str, start: dt.date, end: dt.date):
    pd = _ensure_pandas()
    idx = pd.to_datetime([start, end]).normalize()
    df = pd.DataFrame(
        {
            "open": [1.0, 1.0],
            "high": [1.0, 1.0],
            "low": [1.0, 1.0],
            "close": [1.0, 1.0],
            "volume": [0, 0],
            "dividends": [0.0, 0.0],
            "splits": [0.0, 0.0],
            "ticker": [ticker, ticker],
        },
        index=idx,
    )
    df.index.name = "date"
    return df


def _merge(existing, incoming):
    pd = _ensure_pandas()
    if existing is None or getattr(existing, "empty", False):
        return incoming
    if incoming is None or getattr(incoming, "empty", False):
        return existing
    df = pd.concat([existing, incoming], axis=0)
    df = df[~df.index.duplicated(keep="last")].sort_index()
    return df


def update_cache(
    tickers: list[str],
    start: str | dt.date,
    end: str | dt.date,
    *,
    auto_adjust: bool = True,
    cache_dir: Path = Path("data/prices/yfinance"),
    base_currency: str = "USD",
    retries: int = 5,
) -> dict[str, Any]:
    """
    Update the cache for tickers over [start, end] (end exclusive like yfinance).
    Returns a summary dict; failures are recorded and do not crash the batch.
    """
    pd = _ensure_pandas()
    provider = YahooFinanceProvider()
    cache = PriceCache(Path(cache_dir))

    start_d = dt.date.fromisoformat(str(start)[:10]) if not isinstance(start, dt.date) else start
    end_d = dt.date.fromisoformat(str(end)[:10]) if not isinstance(end, dt.date) else end
    end_fetch = end_d + dt.timedelta(days=1)

    summary = {"requested": len(tickers), "updated": 0, "skipped": 0, "failed": 0, "details": {}}
    for t in tickers:
        ns = normalize_ticker(t, base_currency=base_currency)
        if ns.kind == "invalid":
            summary["skipped"] += 1
            summary["details"][t] = {"status": "skipped", "reason": ns.note}
            continue
        if ns.kind == "synthetic_cash":
            df = _synthetic_cash_df(ticker=t, start=start_d, end=end_d)
            meta = CacheMetadata(
                provider="synthetic",
                original_ticker=t,
                provider_ticker=t,
                auto_adjust=True,
                first_date=str(start_d),
                last_date=str(end_d),
                fetched_at=_now_iso(),
                rows=int(len(df)),
            )
            cache.save(t, df, meta)
            summary["updated"] += 1
            summary["details"][t] = {"status": "synthetic", "rows": int(len(df))}
            continue

        provider_ticker = ns.provider_ticker or t
        cached = cache.load(t)
        fetch_ranges: list[tuple[dt.date, dt.date]] = [(start_d, end_fetch)]
        if cached is not None and not cached.empty:
            try:
                cached.index = pd.to_datetime(cached.index).normalize()
            except Exception:
                pass
            # Fetch only meaningful missing spans inside the requested window.
            # This avoids trying to "fill" holidays/non-trading days (e.g., Jan 1) which would return empty
            # and incorrectly mark otherwise-good tickers as failed.
            try:
                idx_dates = [d.date() for d in pd.to_datetime(cached.index).to_pydatetime()]
                spans = _missing_spans(idx_dates, start=start_d, end=end_d)
                fetch_ranges = [(a, b + dt.timedelta(days=1)) for a, b in spans]
            except Exception:
                fetch_ranges = [(start_d, end_fetch)]
            if not fetch_ranges:
                summary["skipped"] += 1
                summary["details"][t] = {"status": "cache_hit"}
                continue

        # Deduplicate/normalize fetch ranges.
        fetch_ranges = sorted({(a, b) for a, b in fetch_ranges if a is not None and b is not None and a < b}, key=lambda x: x[0])
        if not fetch_ranges:
            summary["skipped"] += 1
            summary["details"][t] = {"status": "cache_hit"}
            continue

        last_err: Exception | None = None
        fetched = None
        for fetch_start, fetch_end in fetch_ranges:
            part = None
            for attempt in range(int(retries)):
                try:
                    part = provider.fetch_prices(provider_ticker, fetch_start, fetch_end, auto_adjust=auto_adjust)
                    break
                except DataNotFoundError as e:
                    last_err = e
                    # DataNotFound can be transient (rate limiting); retry a couple of times with backoff.
                    if attempt >= 1:
                        break
                    sleep_s = _retry_sleep(attempt, key=f"{provider_ticker}:{fetch_start}:{fetch_end}")
                    logger.warning("Empty data for %s; retrying in %.1fs", provider_ticker, sleep_s)
                    time.sleep(sleep_s)
                except Exception as e:
                    last_err = e
                    sleep_s = _retry_sleep(attempt, key=f"{provider_ticker}:{fetch_start}:{fetch_end}")
                    logger.warning("Retry %s/%s for %s in %.1fs (%s)", attempt + 1, retries, provider_ticker, sleep_s, type(e).__name__)
                    time.sleep(sleep_s)
            fetched = _merge(fetched, part)

        if fetched is None or getattr(fetched, "empty", False):
            summary["failed"] += 1
            summary["details"][t] = {
                "status": "failed",
                "error": (f"{type(last_err).__name__}: {last_err}" if last_err else "unknown"),
                "provider_ticker": provider_ticker,
            }
            continue

        merged = _merge(cached, fetched)
        if merged is None or merged.empty:
            summary["failed"] += 1
            summary["details"][t] = {"status": "failed", "error": "merge produced empty"}
            continue

        meta = CacheMetadata(
            provider=provider.name,
            original_ticker=t,
            provider_ticker=provider_ticker,
            auto_adjust=bool(auto_adjust),
            first_date=str(merged.index.min().date()) if len(merged.index) else None,
            last_date=str(merged.index.max().date()) if len(merged.index) else None,
            fetched_at=_now_iso(),
            rows=int(len(merged)),
        )
        cache.save(t, merged, meta)
        summary["updated"] += 1
        summary["details"][t] = {"status": "updated", "rows": int(len(merged))}

    return summary


def get_prices(
    tickers: list[str],
    start: str | dt.date,
    end: str | dt.date,
    *,
    auto_adjust: bool = True,
    cache_dir: Path = Path("data/prices/yfinance"),
    base_currency: str = "USD",
) -> Any:
    """
    Load prices from cache (does not fetch).
    Returns a long-form DataFrame with a 'ticker' column and a 'date' index.
    """
    pd = _ensure_pandas()
    cache = PriceCache(Path(cache_dir))
    start_d = dt.date.fromisoformat(str(start)[:10]) if not isinstance(start, dt.date) else start
    end_d = dt.date.fromisoformat(str(end)[:10]) if not isinstance(end, dt.date) else end

    frames = []
    for t in tickers:
        ns = normalize_ticker(t, base_currency=base_currency)
        if ns.kind == "invalid":
            continue
        if ns.kind == "synthetic_cash":
            df = _synthetic_cash_df(ticker=t, start=start_d, end=end_d)
            frames.append(df)
            continue
        df = cache.load(t)
        if df is None or df.empty:
            continue
        try:
            df.index = pd.to_datetime(df.index).normalize()
            df = df.sort_index()
        except Exception:
            pass
        # Filter to requested range (inclusive end).
        df = df[(df.index.date >= start_d) & (df.index.date <= end_d)]
        frames.append(df)
    if not frames:
        empty = pd.DataFrame(columns=["open", "high", "low", "close", "volume", "dividends", "splits", "ticker"])
        empty.index = pd.to_datetime([])
        empty.index.name = "date"
        return empty
    out = pd.concat(frames, axis=0)
    out.index.name = "date"
    out = out.sort_index()
    return out


def _missing_spans(index_dates: list[dt.date], *, start: dt.date, end: dt.date) -> list[tuple[dt.date, dt.date]]:
    pd = _ensure_pandas()
    expected = pd.bdate_range(start=start, end=end, inclusive="both").date
    have = set(index_dates)
    missing = [d for d in expected if d not in have]
    if not missing:
        return []
    # Group into contiguous bday spans.
    spans: list[tuple[dt.date, dt.date]] = []
    cur_s = cur_e = missing[0]
    for d in missing[1:]:
        # Next business day?
        nxt = (pd.Timestamp(cur_e) + pd.tseries.offsets.BDay(1)).date()
        if d == nxt:
            cur_e = d
        else:
            spans.append((cur_s, cur_e))
            cur_s = cur_e = d
    spans.append((cur_s, cur_e))
    # Drop 1-day spans (likely holidays) for noise reduction.
    spans = [s for s in spans if s[0] != s[1]]
    return spans


def validate_cache(
    tickers: list[str],
    start: str | dt.date,
    end: str | dt.date,
    *,
    cache_dir: Path = Path("data/prices/yfinance"),
    base_currency: str = "USD",
) -> dict[str, Any]:
    """
    Validate cache coverage for tickers over [start, end] using business-day heuristics.
    Returns missing tickers and missing spans.
    """
    pd = _ensure_pandas()
    cache = PriceCache(Path(cache_dir))
    start_d = dt.date.fromisoformat(str(start)[:10]) if not isinstance(start, dt.date) else start
    end_d = dt.date.fromisoformat(str(end)[:10]) if not isinstance(end, dt.date) else end

    missing_tickers: list[str] = []
    missing_spans: dict[str, list[tuple[str, str]]] = {}
    for t in tickers:
        ns: NormalizedSymbol = normalize_ticker(t, base_currency=base_currency)
        if ns.kind == "invalid":
            continue
        if ns.kind == "synthetic_cash":
            continue
        df = cache.load(t)
        if df is None or df.empty:
            missing_tickers.append(t)
            continue
        try:
            idx = pd.to_datetime(df.index).normalize()
            dates = [d.date() for d in idx.to_pydatetime()]
        except Exception:
            missing_tickers.append(t)
            continue
        spans = _missing_spans(dates, start=start_d, end=end_d)
        if spans:
            missing_spans[t] = [(a.isoformat(), b.isoformat()) for a, b in spans]

    return {"missing_tickers": missing_tickers, "missing_spans": missing_spans}
