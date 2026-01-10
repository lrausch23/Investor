from __future__ import annotations

import datetime as dt
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.core.net import http_request
from src.importers.adapters import ProviderError


@dataclass(frozen=True)
class YahooDownloadResult:
    symbol: str
    rows: int
    path: str
    start_date: dt.date
    end_date: dt.date


def _epoch_s(d: dt.date) -> int:
    # Use midnight UTC.
    return int(dt.datetime(d.year, d.month, d.day, tzinfo=dt.timezone.utc).timestamp())


def download_yahoo_price_history_csv(
    *,
    symbol: str,
    start_date: dt.date,
    end_date: dt.date,
    dest_path: Path,
    timeout_s: float = 30.0,
    max_retries: int = 6,
    backoff_s: float = 2.0,
) -> YahooDownloadResult:
    """
    Download daily price history from Yahoo Finance (chart API) and store a normalized CSV.

    Output CSV columns:
      - Date (YYYY-MM-DD)
      - Close
      - Adj Close

    Notes:
      - Requires NETWORK_ENABLED=1
      - Requires outbound host allowlist to include `query1.finance.yahoo.com` (or disable allowlist explicitly).
      - Uses a public endpoint; Yahoo may rate-limit.
    """
    sym = (symbol or "").strip()
    if not sym:
        raise ProviderError("Missing Yahoo Finance symbol.")
    if end_date < start_date:
        raise ProviderError("Invalid date range: end_date < start_date.")

    # Yahoo chart API: period2 is exclusive, so add one day to include end_date.
    p1 = _epoch_s(start_date)
    p2 = _epoch_s(end_date + dt.timedelta(days=1))
    url = (
        "https://query1.finance.yahoo.com/v8/finance/chart/"
        + sym.replace("^", "%5E")
        + f"?period1={p1}&period2={p2}&interval=1d&events=history"
    )

    resp = http_request(
        url,
        method="GET",
        headers={
            # Yahoo often blocks default urllib user agents.
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
            "Accept": "application/json,text/plain,*/*",
        },
        timeout_s=float(timeout_s),
        max_retries=int(max_retries),
        backoff_s=float(backoff_s),
    )
    if int(resp.status_code) != 200:
        raise ProviderError(f"Yahoo Finance request failed: status={resp.status_code}")
    try:
        payload = json.loads(resp.content.decode("utf-8"))
    except Exception as e:
        raise ProviderError(f"Yahoo Finance response parse failed: {type(e).__name__}")

    try:
        result0 = (payload.get("chart") or {}).get("result")[0]
    except Exception:
        raise ProviderError("Yahoo Finance response missing chart.result[0].")

    timestamps = result0.get("timestamp") or []
    indicators = (result0.get("indicators") or {})
    closes = (((indicators.get("quote") or [])[0] or {}).get("close") or []) if isinstance(indicators.get("quote"), list) else []
    adj = (
        (((indicators.get("adjclose") or [])[0] or {}).get("adjclose") or [])
        if isinstance(indicators.get("adjclose"), list)
        else []
    )

    if not isinstance(timestamps, list) or not timestamps:
        raise ProviderError("Yahoo Finance response contained 0 timestamps.")

    # Write a simple CSV.
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    rows = 0
    with dest_path.open("w", encoding="utf-8", newline="") as f:
        f.write("Date,Close,Adj Close\n")
        for i, ts in enumerate(timestamps):
            try:
                d = dt.datetime.fromtimestamp(int(ts), tz=dt.timezone.utc).date()
            except Exception:
                continue
            if d < start_date or d > end_date:
                continue
            close_v = None
            adj_v = None
            try:
                if isinstance(closes, list) and i < len(closes):
                    close_v = closes[i]
            except Exception:
                close_v = None
            try:
                if isinstance(adj, list) and i < len(adj):
                    adj_v = adj[i]
            except Exception:
                adj_v = None
            if close_v is None and adj_v is None:
                continue
            close_s = "" if close_v is None else str(float(close_v))
            adj_s = "" if adj_v is None else str(float(adj_v))
            f.write(f"{d.isoformat()},{close_s},{adj_s}\n")
            rows += 1

    if rows == 0:
        raise ProviderError("Yahoo Finance download returned 0 usable rows (date range may be empty).")

    return YahooDownloadResult(symbol=sym, rows=rows, path=str(dest_path), start_date=start_date, end_date=end_date)
