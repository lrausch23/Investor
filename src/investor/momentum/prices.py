from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd
from sqlalchemy import and_, func
from sqlalchemy.orm import Session

from src.core.net import network_enabled
from src.importers.adapters import ProviderError
from src.db.models import PriceDaily
from src.investor.marketdata.benchmarks import CANON_COLS, StooqProvider, _ranges_from_cached_dates  # type: ignore
from src.investor.momentum.finnhub_prices import FinnhubDailyProvider
from src.investor.momentum.utils import normalize_ticker


def _today() -> dt.date:
    return dt.date.today()


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


def _df_from_rows(rows: list[PriceDaily]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=CANON_COLS)
    recs: list[dict[str, object]] = []
    for r in rows:
        recs.append(
            {
                "date": r.date.isoformat(),
                "open": None,
                "high": None,
                "low": None,
                "close": float(r.close) if r.close is not None else None,
                "adj_close": float(r.adj_close) if r.adj_close is not None else None,
                "volume": float(r.volume) if r.volume is not None else None,
            }
        )
    df = pd.DataFrame.from_records(recs)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).set_index("date").sort_index()
    for c in CANON_COLS:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype(float)
    return df


@dataclass(frozen=True)
class PriceFetchResult:
    ticker: str
    start: dt.date
    end: dt.date
    rows_loaded: int
    rows_fetched: int
    source_used: list[str]
    warning: str | None = None


class PriceDailyStore:
    def read_dates(self, session: Session, *, ticker: str, start: dt.date, end: dt.date) -> list[dt.date]:
        rows = (
            session.query(PriceDaily.date)
            .filter(PriceDaily.ticker == ticker, PriceDaily.date >= start, PriceDaily.date <= end)
            .order_by(PriceDaily.date.asc())
            .all()
        )
        out: list[dt.date] = []
        for (d,) in rows:
            if isinstance(d, dt.date):
                out.append(d)
        return out

    def read(self, session: Session, *, ticker: str, start: dt.date, end: dt.date) -> pd.DataFrame:
        rows = (
            session.query(PriceDaily)
            .filter(PriceDaily.ticker == ticker, PriceDaily.date >= start, PriceDaily.date <= end)
            .order_by(PriceDaily.date.asc())
            .all()
        )
        return _df_from_rows(rows)

    def write(self, session: Session, *, ticker: str, df: pd.DataFrame, source: str) -> int:
        if df is None or df.empty:
            return 0
        out = df.copy()
        if not isinstance(out.index, pd.DatetimeIndex):
            out.index = pd.to_datetime(out.index, errors="coerce")
        out.index = out.index.tz_localize(None)
        out = out[~out.index.isna()]
        out = out.sort_index()
        rows_written = 0
        for idx, row in out.iterrows():
            d = idx.date()
            close = row.get("close")
            adj = row.get("adj_close")
            vol = row.get("volume")
            if close is None or not float(close) > 0:
                continue
            existing = (
                session.query(PriceDaily)
                .filter(PriceDaily.ticker == ticker, PriceDaily.date == d)
                .one_or_none()
            )
            if existing is None:
                session.add(
                    PriceDaily(
                        ticker=ticker,
                        date=d,
                        close=float(close) if close is not None else None,
                        adj_close=float(adj) if adj is not None else None,
                        volume=float(vol) if vol is not None else None,
                        source=str(source or "cache"),
                        updated_at=dt.datetime.now(dt.timezone.utc),
                    )
                )
                rows_written += 1
            else:
                # Cache is authoritative; do not overwrite existing rows unless fields are missing.
                changed = False
                if existing.close is None and close is not None:
                    existing.close = float(close)
                    changed = True
                if existing.adj_close is None and adj is not None:
                    existing.adj_close = float(adj)
                    changed = True
                if existing.volume is None and vol is not None:
                    existing.volume = float(vol)
                    changed = True
                if changed:
                    existing.source = str(source or existing.source or "cache")
                    existing.updated_at = dt.datetime.now(dt.timezone.utc)
                    rows_written += 1
        return rows_written


class MarketDataService:
    """
    Cache-first daily market data service for momentum analytics.

    Provider order (cache-first):
      1) SQLite app DB cache (price_daily)
      2) Network providers (configurable): Stooq and/or Finnhub

    Yahoo/yfinance is intentionally not used in this Momentum pipeline.
    """

    def __init__(self, *, provider: str = "stooq"):
        self.store = PriceDailyStore()
        self.stooq = StooqProvider()
        self.finnhub = FinnhubDailyProvider()
        self.provider = (provider or "stooq").strip().lower()

    def _provider_order(self) -> list[str]:
        """
        Network provider chain (cache is always first).

        Supported:
          - "stooq" (default)
          - "finnhub"
          - "auto" (stooq -> finnhub)
          - "cache" / "local" (no network)
        """
        p = self.provider
        if p in {"cache", "local", "off", "none"}:
            return []
        if p == "finnhub":
            return ["finnhub", "stooq"]
        if p == "auto":
            return ["stooq", "finnhub"]
        # default: stooq first, finnhub fallback
        return ["stooq", "finnhub"]

    def get_daily(
        self,
        session: Session,
        *,
        ticker: str,
        start: dt.date,
        end: dt.date,
        refresh: bool = False,
    ) -> tuple[pd.DataFrame, PriceFetchResult]:
        t = normalize_ticker(ticker)
        if not t:
            raise ProviderError("Missing ticker.")
        if end < start:
            return pd.DataFrame(columns=CANON_COLS), PriceFetchResult(
                ticker=t,
                start=start,
                end=end,
                rows_loaded=0,
                rows_fetched=0,
                source_used=["cache"],
            )

        used: list[str] = []
        cached_df = self.store.read(session, ticker=t, start=start, end=end)
        used.append("cache")
        cached_dates = self.store.read_dates(session, ticker=t, start=start, end=end)
        if refresh:
            missing = [_ranges_from_cached_dates(start=start, end=end, cached_dates=[])[0]]  # full range
        else:
            missing = _ranges_from_cached_dates(start=start, end=end, cached_dates=cached_dates)

        fetched_total = 0
        warning: str | None = None
        providers = self._provider_order()
        if missing and network_enabled() and providers:
            for r in missing:
                seg_ok = False
                seg_errs: list[str] = []
                for name in providers:
                    try:
                        if name == "stooq":
                            df = self.stooq.fetch(symbol=t, start=r.start, end=r.end)
                        elif name == "finnhub":
                            df = self.finnhub.fetch(symbol=t, start=r.start, end=r.end)
                        else:
                            continue
                        wrote = self.store.write(session, ticker=t, df=df, source=name)
                        fetched_total += wrote
                        used.append(name)
                        seg_ok = True
                        break
                    except Exception as e:
                        seg_errs.append(f"{name}: {type(e).__name__}: {e}")
                        continue
                if not seg_ok and seg_errs:
                    warning = f"Price fetch failed for {t}: " + " | ".join(seg_errs[:2])
                    break
        elif missing and not network_enabled():
            warning = "Network disabled; using cached prices only."
        elif missing and not providers:
            warning = "Using cached prices only (provider=cache)."

        if fetched_total > 0:
            session.commit()
            cached_df = self.store.read(session, ticker=t, start=start, end=end)

        return cached_df, PriceFetchResult(
            ticker=t,
            start=start,
            end=end,
            rows_loaded=int(len(cached_df.index)),
            rows_fetched=int(fetched_total),
            source_used=sorted(list(dict.fromkeys(used))),
            warning=warning,
        )

    def warm_cache(
        self,
        session: Session,
        *,
        tickers: Iterable[str],
        start: dt.date,
        end: dt.date,
        limit: int = 50,
    ) -> dict[str, object]:
        """
        Best-effort cache warm for a set of tickers. Fetches only if missing data in [start, end].
        """
        ts = [normalize_ticker(t) for t in tickers]
        ts = [t for t in ts if t]
        ts = list(dict.fromkeys(ts))
        n_total = len(ts)
        n_fetched = 0
        n_skipped = 0
        warnings: list[str] = []
        if not ts:
            return {"total": 0, "fetched": 0, "skipped": 0, "warnings": []}
        if not network_enabled():
            return {"total": n_total, "fetched": 0, "skipped": n_total, "warnings": ["Network disabled."]}
        providers = self._provider_order()
        if not providers:
            return {"total": n_total, "fetched": 0, "skipped": n_total, "warnings": ["Using cached prices only (provider=cache)."]}

        for t in ts:
            if n_fetched >= int(limit):
                break
            cached_dates = self.store.read_dates(session, ticker=t, start=start, end=end)
            missing = _ranges_from_cached_dates(start=start, end=end, cached_dates=cached_dates)
            if not missing:
                n_skipped += 1
                continue
            try:
                # One network request per ticker (full range), then write only new rows.
                df = None
                last_err: Exception | None = None
                used_provider: str | None = None
                for name in providers:
                    try:
                        if name == "stooq":
                            df = self.stooq.fetch(symbol=t, start=start, end=end)
                        elif name == "finnhub":
                            df = self.finnhub.fetch(symbol=t, start=start, end=end)
                        else:
                            continue
                        used_provider = name
                        break
                    except Exception as e:
                        last_err = e
                        continue
                if df is None or used_provider is None:
                    raise ProviderError(f"No provider succeeded. Last error: {type(last_err).__name__}: {last_err}")

                wrote = self.store.write(session, ticker=t, df=df, source=used_provider)
                if wrote > 0:
                    n_fetched += 1
                else:
                    n_skipped += 1
            except Exception as e:
                warnings.append(f"{t}: {type(e).__name__}: {e}")
                continue

        session.commit()
        return {"total": n_total, "fetched": n_fetched, "skipped": n_skipped, "warnings": warnings}
