from __future__ import annotations

import datetime as dt
import logging
from typing import Any

from market_data.exceptions import DataNotFoundError

logger = logging.getLogger(__name__)


class YahooFinanceProvider:
    name = "yfinance"

    def _normalize_column_name(self, col: Any) -> str:
        def _norm(s: Any) -> str:
            t = str(s).strip()
            t = t.replace("*", "")
            return t

        # yfinance can return MultiIndex columns (e.g. ("Open", "AAPL")) depending on configuration/version.
        if isinstance(col, tuple):
            parts = [_norm(p) for p in col if p is not None and str(p).strip()]
            # Prefer the part that looks like a field name.
            for p in parts:
                pl = p.strip().lower()
                if pl in {"open", "high", "low", "close", "volume", "adj close", "adj_close", "adjclose", "dividends", "stock splits", "splits"}:
                    return p
            return parts[0] if parts else ""
        return _norm(col)

    def fetch_prices(self, ticker: str, start: str | dt.date, end: str | dt.date, auto_adjust: bool = True):
        """
        Fetch daily OHLCV plus actions. Returns a DataFrame indexed by 'date' with lower-case columns:
        open, high, low, close, volume, dividends, splits, (adj_close when auto_adjust=False), plus ticker column.
        """
        try:
            import pandas as pd  # type: ignore
        except Exception as e:  # pragma: no cover
            raise RuntimeError("pandas is required for market data fetching.") from e
        try:
            import yfinance as yf  # type: ignore
        except Exception as e:  # pragma: no cover
            raise RuntimeError("yfinance is required for market data fetching.") from e

        start_s = str(start) if not isinstance(start, dt.date) else start.isoformat()
        end_s = str(end) if not isinstance(end, dt.date) else end.isoformat()
        last_exc: Exception | None = None
        df = None
        # Primary path: yf.download
        try:
            df = yf.download(
                ticker,
                start=start_s,
                end=end_s,
                auto_adjust=bool(auto_adjust),
                actions=True,
                progress=False,
                threads=False,
            )
        except Exception as e:
            last_exc = e
            df = None
        # Fallback path: Ticker.history (can succeed when download fails due to Yahoo quirks)
        if df is None or getattr(df, "empty", False):
            try:
                tk = yf.Ticker(ticker)
                df = tk.history(
                    start=start_s,
                    end=end_s,
                    auto_adjust=bool(auto_adjust),
                    actions=True,
                )
            except Exception as e:
                last_exc = e
                df = None
        if df is None or getattr(df, "empty", False):
            msg = f"No data returned for {ticker}."
            if last_exc is not None:
                msg = f"{msg} Last error: {type(last_exc).__name__}: {last_exc}"
            raise DataNotFoundError(msg)

        # Flatten/normalize columns before renaming.
        try:
            if isinstance(df.columns, pd.MultiIndex):
                df = df.copy()
                df.columns = [self._normalize_column_name(c) for c in df.columns]  # type: ignore[assignment]
            else:
                df = df.copy()
                df.columns = [self._normalize_column_name(c) for c in df.columns]
        except Exception:
            df = df.copy()

        # Normalize index -> date (timezone-naive).
        idx = df.index
        try:
            idx = idx.tz_localize(None)
        except Exception:
            pass
        df.index = pd.to_datetime(idx).normalize()
        df.index.name = "date"

        # Rename columns.
        rename = {}
        for c in df.columns:
            cc = str(c).strip()
            if cc.lower() in {"open", "high", "low", "close", "volume"}:
                rename[c] = cc.lower()
            elif cc.lower() in {"adj close", "adj_close", "adjclose"}:
                rename[c] = "adj_close"
            elif cc.lower() == "dividends":
                rename[c] = "dividends"
            elif cc.lower() in {"stock splits", "splits"}:
                rename[c] = "splits"
        df = df.rename(columns=rename)

        required = ["open", "high", "low", "close", "volume"]
        for k in required:
            if k not in df.columns:
                raise DataNotFoundError(f"{ticker}: missing required column {k}.")

        if "dividends" not in df.columns:
            df["dividends"] = 0.0
        if "splits" not in df.columns:
            df["splits"] = 0.0

        # Enforce numeric types.
        for k in ["open", "high", "low", "close", "dividends", "splits"]:
            df[k] = pd.to_numeric(df[k], errors="coerce").astype(float)
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0).astype("int64")
        if not auto_adjust:
            if "adj_close" in df.columns:
                df["adj_close"] = pd.to_numeric(df["adj_close"], errors="coerce").astype(float)
            else:
                # Some tickers lack Adj Close; keep a copy of close for consistency.
                df["adj_close"] = df["close"].astype(float)

        df["ticker"] = str(ticker)

        # Dedup and sort.
        df = df[~df.index.duplicated(keep="last")].sort_index()

        # Drop rows with non-positive close (bad data).
        df = df[df["close"] > 0]
        if df.empty:
            raise DataNotFoundError(f"No usable rows after cleaning for {ticker}.")
        return df

    def fetch_actions(self, ticker: str, start: str | dt.date, end: str | dt.date):
        """
        Best-effort dividends/splits series.
        """
        df = self.fetch_prices(ticker, start, end, auto_adjust=True)
        return df[["dividends", "splits"]].copy()
