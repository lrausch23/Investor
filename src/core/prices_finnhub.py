from __future__ import annotations

import datetime as dt
import json
from dataclasses import dataclass
from pathlib import Path

from src.core.net import http_request
from src.importers.adapters import ProviderError


@dataclass(frozen=True)
class FinnhubDownloadResult:
    symbol: str
    rows: int
    path: str
    start_date: dt.date
    end_date: dt.date


def _epoch_s(d: dt.date) -> int:
    return int(dt.datetime(d.year, d.month, d.day, tzinfo=dt.timezone.utc).timestamp())


def download_finnhub_quote_csv(
    *,
    symbol: str,
    dest_path: Path,
    api_key: str,
) -> tuple[dt.datetime, float]:
    """
    Fetch the latest quote from Finnhub and store a one-row normalized CSV:

      Date,Close

    Returns (quote_time_utc, price).
    """
    sym = (symbol or "").strip().upper()
    if not sym:
        raise ProviderError("Missing Finnhub symbol.")
    key = (api_key or "").strip()
    if not key:
        raise ProviderError("Missing Finnhub API key.")

    url = f"https://finnhub.io/api/v1/quote?symbol={sym}&token={key}"
    resp = http_request(
        url,
        method="GET",
        headers={"Accept": "application/json"},
        timeout_s=30.0,
        max_retries=4,
        backoff_s=1.0,
    )
    if int(resp.status_code) != 200:
        raise ProviderError(f"Finnhub quote request failed: status={resp.status_code}")
    try:
        payload = json.loads(resp.content.decode("utf-8"))
    except Exception as e:
        raise ProviderError(f"Finnhub quote response parse failed: {type(e).__name__}")

    try:
        price = float((payload or {}).get("c") or 0.0)
    except Exception:
        price = 0.0
    try:
        ts = int((payload or {}).get("t") or 0)
    except Exception:
        ts = 0
    if price <= 0:
        raise ProviderError(f"Finnhub quote returned missing/zero price for {sym}.")
    if ts <= 0:
        quote_dt = dt.datetime.now(dt.timezone.utc)
    else:
        quote_dt = dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc)

    dest_path.parent.mkdir(parents=True, exist_ok=True)
    with dest_path.open("w", encoding="utf-8", newline="") as f:
        f.write("Date,Close\n")
        f.write(f"{quote_dt.date().isoformat()},{price}\n")
    return quote_dt, price


def download_finnhub_price_history_csv(
    *,
    symbol: str,
    start_date: dt.date,
    end_date: dt.date,
    dest_path: Path,
    api_key: str,
    timeout_s: float = 30.0,
    max_retries: int = 4,
    backoff_s: float = 1.0,
) -> FinnhubDownloadResult:
    """
    Download daily candles from Finnhub and store a normalized CSV.

    Output CSV columns:
      - Date (YYYY-MM-DD)
      - Close

    Notes:
      - Requires NETWORK_ENABLED=1
      - Requires outbound host allowlist to include `finnhub.io` (or disable allowlist explicitly).
      - Requires an API key (FINNHUB_API_KEY).
    """
    sym = (symbol or "").strip().upper()
    if not sym:
        raise ProviderError("Missing Finnhub symbol.")
    if end_date < start_date:
        raise ProviderError("Invalid date range: end_date < start_date.")
    key = (api_key or "").strip()
    if not key:
        raise ProviderError("Missing Finnhub API key.")

    # Finnhub uses unix seconds; 'to' is inclusive.
    p1 = _epoch_s(start_date)
    p2 = _epoch_s(end_date + dt.timedelta(days=1)) - 1
    url = f"https://finnhub.io/api/v1/stock/candle?symbol={sym}&resolution=D&from={p1}&to={p2}&token={key}"

    resp = http_request(
        url,
        method="GET",
        headers={"Accept": "application/json"},
        timeout_s=float(timeout_s),
        max_retries=int(max_retries),
        backoff_s=float(backoff_s),
    )
    if int(resp.status_code) != 200:
        raise ProviderError(f"Finnhub request failed: status={resp.status_code}")
    try:
        payload = json.loads(resp.content.decode("utf-8"))
    except Exception as e:
        raise ProviderError(f"Finnhub response parse failed: {type(e).__name__}")

    status = str((payload or {}).get("s") or "").strip().lower()
    if status != "ok":
        # Don't include the token or full payload; keep the error small.
        raise ProviderError(f"Finnhub returned status={status or 'unknown'} for {sym}.")

    times = payload.get("t") or []
    closes = payload.get("c") or []
    if not isinstance(times, list) or not isinstance(closes, list) or not times or not closes:
        raise ProviderError(f"Finnhub returned 0 usable rows for {sym}.")

    dest_path.parent.mkdir(parents=True, exist_ok=True)
    rows = 0
    with dest_path.open("w", encoding="utf-8", newline="") as f:
        f.write("Date,Close\n")
        for ts, close_v in zip(times, closes):
            try:
                d = dt.datetime.fromtimestamp(int(ts), tz=dt.timezone.utc).date()
            except Exception:
                continue
            if d < start_date or d > end_date:
                continue
            try:
                px = float(close_v)
            except Exception:
                continue
            if px <= 0:
                continue
            f.write(f"{d.isoformat()},{px}\n")
            rows += 1

    if rows == 0:
        raise ProviderError("Finnhub download returned 0 usable rows (date range may be empty).")

    return FinnhubDownloadResult(symbol=sym, rows=rows, path=str(dest_path), start_date=start_date, end_date=end_date)
