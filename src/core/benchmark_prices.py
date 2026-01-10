from __future__ import annotations

import datetime as dt
import json
import os
import re
from dataclasses import dataclass
from pathlib import Path

from src.core.benchmarks import download_yahoo_price_history_csv
from src.core.prices_finnhub import download_finnhub_price_history_csv
from src.importers.adapters import ProviderError


@dataclass(frozen=True)
class BenchmarkPricesResult:
    provider: str
    symbol: str
    path: Path | None
    used_cache: bool
    fetched_at: dt.datetime | None
    rows: int | None
    warning: str | None


def _clean_symbol(sym: str) -> str:
    s = (sym or "").strip().upper()
    if not s:
        return ""
    # Keep path-safe characters only; Finnhub supports common tickers like "BRK.B".
    s = s.replace("/", "-")
    s = re.sub(r"[^A-Z0-9._-]+", "", s)
    return s[:24]


def _cache_paths(cache_dir: Path, *, symbol: str, start_date: dt.date, end_date: dt.date) -> tuple[Path, Path]:
    cache_dir.mkdir(parents=True, exist_ok=True)
    name = f"{symbol}_{start_date.isoformat()}_{end_date.isoformat()}.csv"
    # Prevent oddities if a symbol is empty/unsafe.
    name = re.sub(r"[^A-Za-z0-9._-]+", "_", name)
    csv_path = cache_dir / name
    meta_path = csv_path.with_suffix(".json")
    return csv_path, meta_path


def _read_meta(meta_path: Path) -> dict:
    try:
        return json.loads(meta_path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _write_meta(meta_path: Path, payload: dict) -> None:
    try:
        meta_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    except Exception:
        return


def _is_fresh(path: Path, *, ttl: dt.timedelta) -> bool:
    try:
        if not path.exists():
            return False
        age = dt.datetime.now(dt.timezone.utc) - dt.datetime.fromtimestamp(path.stat().st_mtime, tz=dt.timezone.utc)
        return age <= ttl
    except Exception:
        return False


def _get_cached_csv(
    *,
    provider: str,
    sym: str,
    start_date: dt.date,
    end_date: dt.date,
    refresh: bool,
    ttl: dt.timedelta,
    cache_dir: Path,
) -> tuple[Path, Path, BenchmarkPricesResult | None]:
    csv_path, meta_path = _cache_paths(cache_dir, symbol=sym, start_date=start_date, end_date=end_date)
    if not refresh and _is_fresh(csv_path, ttl=ttl):
        meta = _read_meta(meta_path)
        fetched_at = None
        try:
            fa = str((meta or {}).get("fetched_at") or "").strip()
            fetched_at = dt.datetime.fromisoformat(fa) if fa else None
        except Exception:
            fetched_at = None
        rows = None
        try:
            rows = int((meta or {}).get("rows")) if (meta or {}).get("rows") is not None else None
        except Exception:
            rows = None
        return csv_path, meta_path, BenchmarkPricesResult(
            provider=provider,
            symbol=sym,
            path=csv_path,
            used_cache=True,
            fetched_at=fetched_at,
            rows=rows,
            warning=None,
        )
    return csv_path, meta_path, None


def get_benchmark_prices_csv(
    *,
    symbol: str,
    start_date: dt.date,
    end_date: dt.date,
    refresh: bool = False,
    ttl: dt.timedelta = dt.timedelta(days=1),
    provider_preference: str = "finnhub",
    allow_fallback: bool = True,
    cache_root: Path | None = None,
) -> BenchmarkPricesResult:
    """
    Get a normalized benchmark price history CSV suitable for `build_performance_report(benchmark_prices_path=...)`.

    Provider preference:
      - "local": use existing local price cache under `data/prices/` (no network)
      - "finnhub": daily candles via Finnhub (requires FINNHUB_API_KEY)
      - "yahoo": Yahoo Finance chart API (no key required)
      - If `allow_fallback=True` (default), providers are tried in a safe fallback order.
    """
    sym = _clean_symbol(symbol)
    if not sym:
        return BenchmarkPricesResult(
            provider=str(provider_preference or "finnhub"),
            symbol="",
            path=None,
            used_cache=False,
            fetched_at=None,
            rows=None,
            warning="Benchmark symbol is missing.",
        )

    root = cache_root or (Path("data") / "benchmarks")
    finnhub_dir = root / "finnhub"
    yahoo_dir = root / "yahoo"

    pref = (provider_preference or "local").strip().lower()
    if pref not in {"local", "finnhub", "yahoo"}:
        pref = "local"
    if allow_fallback:
        if pref == "local":
            providers = ["local", "finnhub", "yahoo"]
        elif pref == "finnhub":
            providers = ["finnhub", "yahoo", "local"]
        else:
            providers = ["yahoo", "local", "finnhub"]
    else:
        providers = [pref]

    # Keep both provider failures (if any) so UI can tell the user what to do next.
    failures: list[str] = []
    fallback_note: str | None = None
    for provider in providers:
        if provider == "local":
            # Reuse the same symbol normalization as other cache layers.
            prices_root = Path("data") / "prices"
            candidates = [
                prices_root / f"{sym}.csv",
                prices_root / f"{sym.lower()}.csv",
                prices_root / "yfinance" / f"{sym}.csv",
                prices_root / "yfinance" / f"{sym.lower()}.csv",
            ]
            p = next((c for c in candidates if c.exists()), None)
            if p is None:
                failures.append(
                    f"Local: missing cached prices for {sym} (expected {candidates[0]} or {candidates[2]})."
                )
                continue
            fetched_at = None
            try:
                fetched_at = dt.datetime.fromtimestamp(p.stat().st_mtime, tz=dt.timezone.utc)
            except Exception:
                fetched_at = None
            return BenchmarkPricesResult(
                provider="local",
                symbol=sym,
                path=p,
                used_cache=True,
                fetched_at=fetched_at,
                rows=None,
                warning=None,
            )

        if provider == "finnhub":
            key = (os.environ.get("FINNHUB_API_KEY") or "").strip()
            if not key:
                failures.append("Finnhub: FINNHUB_API_KEY not set.")
                continue
            csv_path, meta_path, cached = _get_cached_csv(
                provider="finnhub",
                sym=sym,
                start_date=start_date,
                end_date=end_date,
                refresh=refresh,
                ttl=ttl,
                cache_dir=finnhub_dir,
            )
            if cached is not None:
                return cached

            fetched_at = dt.datetime.now(dt.timezone.utc)
            try:
                res = download_finnhub_price_history_csv(
                    symbol=sym,
                    start_date=start_date,
                    end_date=end_date,
                    dest_path=csv_path,
                    api_key=key,
                    # Performance report should fail fast and fall back; avoid long retries that feel like a "stuck" page.
                    timeout_s=10.0,
                    max_retries=1,
                    backoff_s=0.5,
                )
                _write_meta(
                    meta_path,
                    {
                        "provider": "finnhub_candles",
                        "symbol": sym,
                        "start_date": res.start_date.isoformat(),
                        "end_date": res.end_date.isoformat(),
                        "rows": int(res.rows),
                        "fetched_at": fetched_at.isoformat(),
                    },
                )
                return BenchmarkPricesResult(
                    provider="finnhub",
                    symbol=sym,
                    path=csv_path,
                    used_cache=False,
                    fetched_at=fetched_at,
                    rows=int(res.rows),
                    warning=None,
                )
            except Exception as e:
                # If Finnhub is forbidden for candles on this token/plan, allow fallback (Yahoo).
                msg = str(e) if isinstance(e, ProviderError) else f"{type(e).__name__}: {e}"
                msg = (msg or "").strip()
                if key:
                    msg = msg.replace(key, "***")
                msg = msg.replace("or upload the benchmark CSV manually.", "or select Local cache (no network).")
                failures.append(f"Finnhub: {msg}" if msg else "Finnhub: benchmark fetch failed.")
                # Fallback to cached finnhub data if present, even when refresh was requested.
                if csv_path.exists():
                    return BenchmarkPricesResult(
                        provider="finnhub",
                        symbol=sym,
                        path=csv_path,
                        used_cache=True,
                        fetched_at=None,
                        rows=None,
                        warning=f"Using cached Finnhub benchmark data (refresh failed: {msg})" if msg else "Using cached Finnhub benchmark data (refresh failed).",
                    )
                continue

        if provider == "yahoo":
            csv_path, meta_path, cached = _get_cached_csv(
                provider="yahoo",
                sym=sym,
                start_date=start_date,
                end_date=end_date,
                refresh=refresh,
                ttl=ttl,
                cache_dir=yahoo_dir,
            )
            if cached is not None:
                return cached
            fetched_at = dt.datetime.now(dt.timezone.utc)
            try:
                res = download_yahoo_price_history_csv(
                    symbol=sym,
                    start_date=start_date,
                    end_date=end_date,
                    dest_path=csv_path,
                    # Performance report should fail fast and show portfolio-only metrics when benchmark is unavailable.
                    timeout_s=10.0,
                    max_retries=2,
                    backoff_s=0.75,
                )
                _write_meta(
                    meta_path,
                    {
                        "provider": "yahoo_chart",
                        "symbol": sym,
                        "start_date": res.start_date.isoformat(),
                        "end_date": res.end_date.isoformat(),
                        "rows": int(res.rows),
                        "fetched_at": fetched_at.isoformat(),
                    },
                )
                # If we attempted Finnhub first and fell back, keep a small note in UI (but do not treat it as an error).
                if failures and any(f.startswith("Finnhub:") for f in failures) and not any(
                    f.startswith("Yahoo:") for f in failures
                ):
                    # Reduce noise: show only the Finnhub failure reason.
                    fh = next((f for f in failures if f.startswith("Finnhub:")), None)
                    if fh:
                        fallback_note = f"{fh.replace('Finnhub: ', 'Finnhub unavailable: ')} Using Yahoo instead."
                return BenchmarkPricesResult(
                    provider="yahoo",
                    symbol=sym,
                    path=csv_path,
                    used_cache=False,
                    fetched_at=fetched_at,
                    rows=int(res.rows),
                    warning=fallback_note,
                )
            except Exception as e:
                msg = str(e) if isinstance(e, ProviderError) else f"{type(e).__name__}: {e}"
                msg = (msg or "").strip()
                msg = msg.replace("or upload the benchmark CSV manually.", "or select Local cache (no network).")
                failures.append(f"Yahoo: {msg}" if msg else "Yahoo: benchmark fetch failed.")
                if csv_path.exists():
                    return BenchmarkPricesResult(
                        provider="yahoo",
                        symbol=sym,
                        path=csv_path,
                        used_cache=True,
                        fetched_at=None,
                        rows=None,
                        warning=f"Using cached Yahoo benchmark data (refresh failed: {msg})" if msg else "Using cached Yahoo benchmark data (refresh failed).",
                    )
                continue

    return BenchmarkPricesResult(
        provider=str(provider_preference or "finnhub"),
        symbol=sym,
        path=None,
        used_cache=False,
        fetched_at=None,
        rows=None,
        warning="Benchmark data unavailable." if not failures else ("Benchmark data unavailable (" + "; ".join(failures) + ")."),
    )


def get_finnhub_benchmark_prices_csv(
    *,
    symbol: str,
    start_date: dt.date,
    end_date: dt.date,
    refresh: bool = False,
    ttl: dt.timedelta = dt.timedelta(days=1),
    cache_dir: Path | None = None,
) -> BenchmarkPricesResult:
    """
    Backward-compatible wrapper (kept for callers). Prefer `get_benchmark_prices_csv()`.
    """
    return get_benchmark_prices_csv(
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        refresh=refresh,
        ttl=ttl,
        provider_preference="finnhub",
        cache_root=cache_dir,
    )
