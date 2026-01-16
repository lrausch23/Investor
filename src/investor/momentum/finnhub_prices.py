from __future__ import annotations

import datetime as dt
import json
import os
import random
import threading
import time

import pandas as pd

from src.core.net import http_request
from src.importers.adapters import ProviderError
from src.investor.marketdata.benchmarks import CANON_COLS  # type: ignore


_LOCK = threading.Lock()
_LAST_TS = 0.0


def _rate_limit_sleep(*, max_rps: float) -> None:
    """
    Global per-process limiter to avoid spamming Finnhub.
    """
    global _LAST_TS
    rps = float(max_rps or 0.0)
    if rps <= 0:
        return
    min_interval = 1.0 / rps
    with _LOCK:
        now = time.time()
        wait = (_LAST_TS + min_interval) - now
        if wait > 0:
            time.sleep(wait)
        _LAST_TS = time.time()


def _epoch_s(d: dt.date) -> int:
    return int(dt.datetime(d.year, d.month, d.day, tzinfo=dt.timezone.utc).timestamp())


class FinnhubDailyProvider:
    """
    Daily OHLCV provider backed by Finnhub stock candles.

    Notes:
    - Finnhub does not provide adjusted close in this endpoint; we store `close` and `volume`.
    - Requires FINNHUB_API_KEY and NETWORK_ENABLED=1.
    """

    name = "finnhub"

    def __init__(
        self,
        *,
        api_key_env: str = "FINNHUB_API_KEY",
        max_rps: float = 1.0,
        max_retries: int = 4,
        backoff_base_seconds: float = 1.0,
    ):
        self.api_key_env = api_key_env
        self.max_rps = float(max_rps)
        self.max_retries = int(max_retries)
        self.backoff_base_seconds = float(backoff_base_seconds)

    def fetch(self, *, symbol: str, start: dt.date, end: dt.date) -> pd.DataFrame:
        sym = (symbol or "").strip().upper()
        if not sym:
            raise ProviderError("Missing Finnhub symbol.")
        if end < start:
            return pd.DataFrame(columns=CANON_COLS)
        key = (os.environ.get(self.api_key_env) or "").strip()
        if not key:
            raise ProviderError(f"{self.name}: {self.api_key_env} not set.")

        # Finnhub uses unix seconds; 'to' is inclusive.
        p1 = _epoch_s(start)
        p2 = _epoch_s(end + dt.timedelta(days=1)) - 1
        url = f"https://finnhub.io/api/v1/stock/candle?symbol={sym}&resolution=D&from={p1}&to={p2}&token={key}"

        attempt = 0
        last_err: Exception | None = None
        while attempt <= self.max_retries:
            try:
                _rate_limit_sleep(max_rps=self.max_rps)
                resp = http_request(
                    url,
                    method="GET",
                    headers={"Accept": "application/json"},
                    timeout_s=30.0,
                    max_retries=0,
                    backoff_s=0.0,
                )
                if int(resp.status_code) != 200:
                    raise ProviderError(f"Finnhub request failed: status={resp.status_code}")
                payload = json.loads(resp.content.decode("utf-8"))
                status = str((payload or {}).get("s") or "").strip().lower()
                if status != "ok":
                    raise ProviderError(f"Finnhub returned status={status or 'unknown'} for {sym}.")

                ts = payload.get("t") or []
                o = payload.get("o") or []
                h = payload.get("h") or []
                l = payload.get("l") or []
                c = payload.get("c") or []
                v = payload.get("v") or []
                if not isinstance(ts, list) or not isinstance(c, list) or not ts or not c:
                    raise ProviderError(f"Finnhub returned 0 usable rows for {sym}.")

                rows: list[dict[str, object]] = []
                for i in range(min(len(ts), len(c))):
                    try:
                        d = dt.datetime.fromtimestamp(int(ts[i]), tz=dt.timezone.utc).date()
                    except Exception:
                        continue
                    if d < start or d > end:
                        continue
                    try:
                        close = float(c[i])
                    except Exception:
                        continue
                    if close <= 0:
                        continue
                    rec: dict[str, object] = {
                        "date": d.isoformat(),
                        "close": close,
                        "adj_close": None,
                    }
                    # Best-effort extras.
                    try:
                        rec["open"] = float(o[i]) if i < len(o) else None
                    except Exception:
                        rec["open"] = None
                    try:
                        rec["high"] = float(h[i]) if i < len(h) else None
                    except Exception:
                        rec["high"] = None
                    try:
                        rec["low"] = float(l[i]) if i < len(l) else None
                    except Exception:
                        rec["low"] = None
                    try:
                        rec["volume"] = float(v[i]) if i < len(v) else None
                    except Exception:
                        rec["volume"] = None
                    rows.append(rec)

                if not rows:
                    raise ProviderError(f"Finnhub returned 0 rows in requested range for {sym}.")

                df = pd.DataFrame.from_records(rows)
                df["date"] = pd.to_datetime(df["date"], errors="coerce")
                df = df.dropna(subset=["date"]).set_index("date").sort_index()
                for col in CANON_COLS:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors="coerce").astype(float)
                df = df.dropna(subset=["close"])
                df = df[df["close"] > 0.0]
                return df
            except Exception as e:
                last_err = e
                msg = str(e)
                is_rate = "429" in msg or "rate" in msg.lower()
                is_transient = is_rate or "timed out" in msg.lower() or "URLError" in msg
                if attempt >= self.max_retries or not is_transient:
                    break
                base = self.backoff_base_seconds * (2**attempt)
                jitter = random.random() * min(1.0, base * 0.1)
                time.sleep(base + jitter)
                attempt += 1
                continue

        if isinstance(last_err, ProviderError):
            raise last_err
        raise ProviderError(f"Finnhub fetch failed: {type(last_err).__name__}: {last_err}")

