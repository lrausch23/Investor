from __future__ import annotations

import datetime as dt
import random
import re
import sqlite3
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Protocol

from src.core.benchmarks import download_yahoo_price_history_csv
from src.core.net import http_request
from src.importers.adapters import ProviderError
from src.investor.marketdata.config import BenchmarksConfig, load_marketdata_config

try:
    import pandas as pd
except Exception:  # pragma: no cover
    pd = None  # type: ignore[assignment]


CANON_COLS = ["open", "high", "low", "close", "adj_close", "volume"]


def _require_pandas() -> None:
    if pd is None:  # pragma: no cover
        raise ProviderError("pandas is required for benchmark candles. Install pandas and retry.")


def _as_date(v: object) -> dt.date | None:
    if v is None:
        return None
    if isinstance(v, dt.date) and not isinstance(v, dt.datetime):
        return v
    if isinstance(v, dt.datetime):
        return v.date()
    s = str(v).strip()
    if not s:
        return None
    try:
        return dt.date.fromisoformat(s[:10])
    except Exception:
        return None


def canonicalize_symbol(symbol: str, *, proxy_sp500: str = "SPY") -> tuple[str, str]:
    """
    Normalize a requested benchmark symbol into a canonical cache key.

    Returns (canonical_symbol, requested_symbol_normalized).
    """
    req = (symbol or "").strip().upper()
    if not req:
        return "", ""
    if req in {"^GSPC", "GSPC"}:
        return (proxy_sp500 or "SPY").strip().upper(), req
    # Basic safety: keep common ticker punctuation.
    canon = re.sub(r"[^A-Z0-9.^_-]+", "", req)[:24]
    return canon, req


def _normalize_df(
    df,  # pandas.DataFrame
    *,
    requested_symbol: str,
    canonical_symbol: str,
) -> "pd.DataFrame":
    _require_pandas()
    if df is None:
        raise ProviderError("Provider returned no data.")
    out = df.copy()
    if out.empty:
        raise ProviderError("Provider returned 0 rows.")

    # Normalize index to tz-naive daily timestamps, sorted.
    if not hasattr(out, "index"):
        raise ProviderError("Provider returned invalid data (missing index).")
    try:
        out.index = pd.to_datetime(out.index).tz_localize(None)  # type: ignore[union-attr]
    except Exception:
        out.index = pd.to_datetime(out.index, errors="coerce").tz_localize(None)  # type: ignore[union-attr]
    out = out[~out.index.isna()]  # type: ignore[union-attr]
    out = out.sort_index()

    # Normalize column names.
    ren = {}
    for c in list(out.columns):
        key = str(c).strip().lower()
        key = re.sub(r"[^a-z0-9]+", "_", key).strip("_")
        if key in {"adj_close", "adjclose", "adj_close_"}:
            ren[c] = "adj_close"
        elif key in {"close"}:
            ren[c] = "close"
        elif key in {"open"}:
            ren[c] = "open"
        elif key in {"high"}:
            ren[c] = "high"
        elif key in {"low"}:
            ren[c] = "low"
        elif key in {"volume", "vol"}:
            ren[c] = "volume"
    if ren:
        out = out.rename(columns=ren)

    # Keep only canonical columns (if present).
    cols = [c for c in CANON_COLS if c in out.columns]
    if "close" not in cols:
        raise ProviderError("Provider data is missing required column: close.")
    out = out[cols]

    # Coerce to float.
    for c in cols:
        try:
            out[c] = pd.to_numeric(out[c], errors="coerce").astype(float)  # type: ignore[index]
        except Exception:
            pass
    out = out.dropna(subset=["close"])
    out = out[out["close"] > 0.0]
    if out.empty:
        raise ProviderError("Provider returned 0 usable rows after normalization.")

    return out


@dataclass(frozen=True)
class MissingRange:
    start: dt.date
    end: dt.date


def _ranges_from_cached_dates(
    *,
    start: dt.date,
    end: dt.date,
    cached_dates: list[dt.date],
    gap_days_threshold: int = 7,
) -> list[MissingRange]:
    if end < start:
        return []
    if not cached_dates:
        return [MissingRange(start=start, end=end)]
    dates = sorted([d for d in cached_dates if start <= d <= end])
    if not dates:
        return [MissingRange(start=start, end=end)]
    out: list[MissingRange] = []
    if dates[0] > start:
        out.append(MissingRange(start=start, end=dates[0] - dt.timedelta(days=1)))
    for a, b in zip(dates, dates[1:]):
        gap = (b - a).days
        # Ignore normal weekend/holiday gaps; treat larger gaps as likely missing coverage.
        if gap > int(gap_days_threshold):
            seg_start = a + dt.timedelta(days=1)
            seg_end = b - dt.timedelta(days=1)
            if seg_start <= seg_end:
                out.append(MissingRange(start=seg_start, end=seg_end))
    if dates[-1] < end:
        out.append(MissingRange(start=dates[-1] + dt.timedelta(days=1), end=end))
    return out


class CandlesProvider(Protocol):
    name: str

    def fetch(self, *, symbol: str, start: dt.date, end: dt.date) -> "pd.DataFrame":
        raise NotImplementedError


class SQLiteCacheProvider:
    name = "cache"

    def __init__(self, *, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        con = sqlite3.connect(str(self.path))
        con.row_factory = sqlite3.Row
        return con

    def _init_db(self) -> None:
        with self._connect() as con:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS benchmark_candles (
                  symbol TEXT NOT NULL,
                  date TEXT NOT NULL,
                  open REAL,
                  high REAL,
                  low REAL,
                  close REAL NOT NULL,
                  adj_close REAL,
                  volume REAL,
                  PRIMARY KEY (symbol, date)
                )
                """
            )
            con.execute("CREATE INDEX IF NOT EXISTS idx_benchmark_candles_symbol_date ON benchmark_candles(symbol, date)")

    def read_dates(self, *, symbol: str, start: dt.date, end: dt.date) -> list[dt.date]:
        with self._connect() as con:
            rows = con.execute(
                "SELECT date FROM benchmark_candles WHERE symbol=? AND date>=? AND date<=? ORDER BY date ASC",
                (symbol, start.isoformat(), end.isoformat()),
            ).fetchall()
        out: list[dt.date] = []
        for r in rows:
            try:
                out.append(dt.date.fromisoformat(str(r["date"])))
            except Exception:
                continue
        return out

    def read(self, *, symbol: str, start: dt.date, end: dt.date):
        _require_pandas()
        with self._connect() as con:
            rows = con.execute(
                """
                SELECT date, open, high, low, close, adj_close, volume
                FROM benchmark_candles
                WHERE symbol=? AND date>=? AND date<=?
                ORDER BY date ASC
                """,
                (symbol, start.isoformat(), end.isoformat()),
            ).fetchall()
        if not rows:
            return pd.DataFrame(columns=CANON_COLS)
        records = []
        for r in rows:
            records.append(
                {
                    "date": str(r["date"]),
                    "open": r["open"],
                    "high": r["high"],
                    "low": r["low"],
                    "close": r["close"],
                    "adj_close": r["adj_close"],
                    "volume": r["volume"],
                }
            )
        df = pd.DataFrame.from_records(records)
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.dropna(subset=["date"])
        df = df.set_index("date").sort_index()
        # Coerce types.
        for c in CANON_COLS:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce").astype(float)
        df = df.dropna(subset=["close"])
        return df

    def write(self, *, symbol: str, df) -> int:
        _require_pandas()
        if df is None or df.empty:
            return 0
        out = df.copy()
        out = out.sort_index()
        if not isinstance(out.index, pd.DatetimeIndex):
            out.index = pd.to_datetime(out.index).tz_localize(None)
        out = out[~out.index.isna()]
        out = out.dropna(subset=["close"])
        out = out[out["close"] > 0.0]
        if out.empty:
            return 0
        rows = []
        for ts, r in out.iterrows():
            d = ts.date().isoformat()
            rows.append(
                (
                    symbol,
                    d,
                    float(r.get("open")) if r.get("open") is not None else None,
                    float(r.get("high")) if r.get("high") is not None else None,
                    float(r.get("low")) if r.get("low") is not None else None,
                    float(r.get("close")),
                    float(r.get("adj_close")) if r.get("adj_close") is not None else None,
                    float(r.get("volume")) if r.get("volume") is not None else None,
                )
            )
        with self._connect() as con:
            con.executemany(
                """
                INSERT OR REPLACE INTO benchmark_candles(symbol, date, open, high, low, close, adj_close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
        return len(rows)

    def status(self, *, symbol: str) -> dict[str, object]:
        with self._connect() as con:
            row = con.execute(
                "SELECT MIN(date) AS min_date, MAX(date) AS max_date, COUNT(*) AS n FROM benchmark_candles WHERE symbol=?",
                (symbol,),
            ).fetchone()
        if not row:
            return {"symbol": symbol, "rows": 0, "min_date": None, "max_date": None}
        return {"symbol": symbol, "rows": int(row["n"] or 0), "min_date": row["min_date"], "max_date": row["max_date"]}


class StooqProvider:
    name = "stooq"

    def fetch(self, *, symbol: str, start: dt.date, end: dt.date):
        _require_pandas()
        if end < start:
            return pd.DataFrame(columns=CANON_COLS)
        # Stooq symbols are typically lowercase, and US equities/ETFs use ".us".
        s = (symbol or "").strip().lower()
        if not s:
            raise ProviderError("Missing Stooq symbol.")
        if "." not in s and re.fullmatch(r"[a-z0-9._-]+", s):
            s = f"{s}.us"
        url = f"https://stooq.com/q/d/l/?s={s}&i=d"
        resp = http_request(
            url,
            method="GET",
            headers={"Accept": "text/csv"},
            timeout_s=30.0,
            max_retries=2,
            backoff_s=1.0,
        )
        if int(resp.status_code) != 200:
            raise ProviderError(f"Stooq request failed: status={resp.status_code}")
        text = resp.content.decode("utf-8", errors="replace").strip()
        if not text or text.lower().startswith("no data"):
            raise ProviderError("Stooq returned no data.")
        from io import StringIO

        df = pd.read_csv(StringIO(text))
        # Stooq columns: Date,Open,High,Low,Close,Volume
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
            df = df.dropna(subset=["Date"]).set_index("Date")
        df = df.rename(columns={c: str(c).strip().lower() for c in df.columns})
        df = df.rename(columns={"adj_close": "adj_close", "adj close": "adj_close"})
        df = df.rename(columns={"open": "open", "high": "high", "low": "low", "close": "close", "volume": "volume"})
        df = _normalize_df(df, requested_symbol=symbol, canonical_symbol=symbol)
        # Clip to requested range.
        df = df[(df.index.date >= start) & (df.index.date <= end)]
        if df.empty:
            raise ProviderError("Stooq returned 0 rows in requested range.")
        return df


_YAHOO_LOCK = threading.Lock()
_YAHOO_LAST_TS = 0.0


def _rate_limit_sleep(*, max_rps: float) -> None:
    global _YAHOO_LAST_TS
    rps = float(max_rps or 0.0)
    if rps <= 0:
        return
    min_interval = 1.0 / rps
    with _YAHOO_LOCK:
        now = time.time()
        wait = (_YAHOO_LAST_TS + min_interval) - now
        if wait > 0:
            time.sleep(wait)
        _YAHOO_LAST_TS = time.time()


class YahooProvider:
    name = "yahoo"

    def __init__(self, *, max_rps: float = 1.0, max_retries: int = 6, backoff_base_seconds: float = 2.0):
        self.max_rps = float(max_rps)
        self.max_retries = int(max_retries)
        self.backoff_base_seconds = float(backoff_base_seconds)

    def fetch(self, *, symbol: str, start: dt.date, end: dt.date):
        _require_pandas()
        if end < start:
            return pd.DataFrame(columns=CANON_COLS)

        from tempfile import NamedTemporaryFile

        last_err: Exception | None = None
        for attempt in range(max(0, self.max_retries) + 1):
            _rate_limit_sleep(max_rps=self.max_rps)
            try:
                with NamedTemporaryFile(prefix="bench_yahoo_", suffix=".csv", delete=True) as tmp:
                    res = download_yahoo_price_history_csv(
                        symbol=symbol,
                        start_date=start,
                        end_date=end,
                        dest_path=Path(tmp.name),
                        timeout_s=30.0,
                        max_retries=0,
                        backoff_s=0.0,
                    )
                    df = pd.read_csv(tmp.name)
                if df.empty:
                    raise ProviderError("Yahoo returned 0 rows.")
                # Normalize.
                if "Date" in df.columns:
                    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
                    df = df.dropna(subset=["Date"]).set_index("Date")
                df = df.rename(columns={c: str(c).strip().lower() for c in df.columns})
                df = df.rename(columns={"adj close": "adj_close", "adj_close": "adj_close"})
                df = df.rename(columns={"open": "open", "high": "high", "low": "low", "close": "close", "volume": "volume"})
                df = _normalize_df(df, requested_symbol=symbol, canonical_symbol=symbol)
                df = df[(df.index.date >= start) & (df.index.date <= end)]
                if df.empty:
                    raise ProviderError("Yahoo returned 0 rows in requested range.")
                return df
            except Exception as e:
                last_err = e
                msg = str(e)
                is_rate = "HTTP 429" in msg or "Rate limited" in msg or "status=429" in msg
                is_transient = is_rate or "URLError" in msg or "timed out" in msg.lower()
                if attempt >= self.max_retries or not is_transient:
                    break
                base = self.backoff_base_seconds * (2**attempt)
                jitter = random.random() * min(1.0, base * 0.1)
                time.sleep(base + jitter)
                continue

        if isinstance(last_err, ProviderError):
            raise last_err
        raise ProviderError(f"Yahoo benchmark fetch failed: {type(last_err).__name__}: {last_err}")


@dataclass(frozen=True)
class BenchmarkFetchMeta:
    requested_symbol: str
    canonical_symbol: str
    used_providers: list[str]
    cached_rows_written: int
    warning: str | None


class BenchmarkDataClient:
    def __init__(self, *, config: BenchmarksConfig):
        _require_pandas()
        self.config = config
        if (config.cache.type or "").strip().lower() != "sqlite":
            raise ProviderError(f"Unsupported benchmarks cache type: {config.cache.type}")
        self.cache = SQLiteCacheProvider(path=Path(config.cache.path))
        self.stooq = StooqProvider()
        self.yahoo = YahooProvider(
            max_rps=config.yahoo.max_rps,
            max_retries=config.yahoo.max_retries,
            backoff_base_seconds=config.yahoo.backoff_base_seconds,
        )

    def _providers_for_order(self) -> list[CandlesProvider]:
        order = [str(x or "").strip().lower() for x in (self.config.provider_order or []) if str(x or "").strip()]
        if not order:
            order = ["cache", "stooq", "yahoo"]
        providers: list[CandlesProvider] = []
        for name in order:
            if name == "cache":
                providers.append(self.cache)  # type: ignore[arg-type]
            elif name == "stooq":
                if self.config.stooq.enabled:
                    providers.append(self.stooq)
            elif name == "yahoo":
                if self.config.yahoo.enabled:
                    providers.append(self.yahoo)
        # Always keep cache first (authoritative).
        if providers and getattr(providers[0], "name", "") != "cache":
            providers = [self.cache] + [p for p in providers if getattr(p, "name", "") != "cache"]  # type: ignore[list-item]
        return providers

    def get(
        self,
        *,
        symbol: str,
        start: dt.date,
        end: dt.date,
        refresh: bool = False,
    ) -> tuple["pd.DataFrame", BenchmarkFetchMeta]:
        canon, req = canonicalize_symbol(symbol, proxy_sp500=self.config.benchmark_proxy)
        if not canon:
            raise ProviderError("Benchmark symbol is missing.")
        if end < start:
            raise ProviderError("Invalid date range: end < start.")

        providers = self._providers_for_order()
        used: list[str] = []
        rows_written = 0
        warning: str | None = None

        # Cache-first: attempt to satisfy request without network.
        if not refresh:
            cached_dates = self.cache.read_dates(symbol=canon, start=start, end=end)
            missing = _ranges_from_cached_dates(start=start, end=end, cached_dates=cached_dates)
            if not missing:
                df = self.cache.read(symbol=canon, start=start, end=end)
                return df, BenchmarkFetchMeta(
                    requested_symbol=req,
                    canonical_symbol=canon,
                    used_providers=["cache"],
                    cached_rows_written=0,
                    warning=None,
                )

        # Fill missing ranges with secondary providers.
        cached_dates = self.cache.read_dates(symbol=canon, start=start, end=end)
        missing = _ranges_from_cached_dates(start=start, end=end, cached_dates=cached_dates)
        if not missing:
            df = self.cache.read(symbol=canon, start=start, end=end)
            return df, BenchmarkFetchMeta(requested_symbol=req, canonical_symbol=canon, used_providers=["cache"], cached_rows_written=0, warning=None)

        # Providers[0] is cache; network providers are the rest.
        network_providers = [p for p in providers if getattr(p, "name", "") != "cache"]
        failures: list[str] = []
        for seg in missing:
            filled = False
            for p in network_providers:
                try:
                    df_seg = p.fetch(symbol=canon, start=seg.start, end=seg.end)
                    df_seg = _normalize_df(df_seg, requested_symbol=req, canonical_symbol=canon)
                    df_seg = df_seg[(df_seg.index.date >= seg.start) & (df_seg.index.date <= seg.end)]
                    if df_seg.empty:
                        raise ProviderError(f"{p.name} returned 0 rows for {canon} {seg.start}→{seg.end}")
                    w = self.cache.write(symbol=canon, df=df_seg)
                    rows_written += int(w)
                    used.append(p.name)
                    filled = True
                    break
                except Exception as e:
                    msg = str(e).strip()
                    failures.append(f"{p.name}: {msg or type(e).__name__}")
                    continue
            if not filled:
                # Leave remaining segments; we'll return whatever cache has (may be partial).
                warning = "Benchmark data partially available; some ranges could not be fetched."
                break

        df_out = self.cache.read(symbol=canon, start=start, end=end)
        if df_out.empty:
            detail = "; ".join(failures[:6])
            suffix = "…" if len(failures) > 6 else ""
            raise ProviderError(f"Benchmark data unavailable ({detail}{suffix})")
        if failures and warning is None:
            # Non-blocking notice in case we fell back or had partial fills.
            warning = "Some providers failed; using best available cached data."
        return df_out, BenchmarkFetchMeta(
            requested_symbol=req,
            canonical_symbol=canon,
            used_providers=(["cache"] + used) if used else ["cache"],
            cached_rows_written=rows_written,
            warning=warning,
        )


def get_benchmark_candles(
    symbol: str,
    start: dt.date,
    end: dt.date,
    *,
    refresh: bool = False,
    config: BenchmarksConfig | None = None,
) -> "pd.DataFrame":
    """
    Public API for reports: fetch daily benchmark candles for [start, end], cache-first.
    """
    cfg = config or load_marketdata_config()[0].benchmarks
    client = BenchmarkDataClient(config=cfg)
    df, _meta = client.get(symbol=symbol, start=start, end=end, refresh=bool(refresh))
    return df
