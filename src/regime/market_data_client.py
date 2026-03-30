from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any, cast

import pandas as pd
import yfinance as yf

from .exceptions import DataFetchError

logger = logging.getLogger(__name__)


def download_daily_bars(
    tickers: str | list[str],
    period: str = "1y",
    *,
    start: str | date | None = None,
    end: str | date | None = None,
    auto_adjust: bool = True,
    group_by: str = "column",
    actions: bool = False,
) -> pd.DataFrame:
    ticker_list = [tickers] if isinstance(tickers, str) else list(tickers)
    label = ",".join(ticker_list[:3]) + ("…" if len(ticker_list) > 3 else "")
    try:
        kwargs: dict[str, Any] = {
            "tickers": ticker_list if len(ticker_list) > 1 else ticker_list[0],
            "interval": "1d",
            "auto_adjust": auto_adjust,
            "progress": False,
            "threads": False,
            "actions": actions,
        }
        if start and end:
            kwargs["start"] = str(start)
            kwargs["end"] = str(end)
        else:
            kwargs["period"] = period
        if len(ticker_list) > 1:
            kwargs["group_by"] = group_by
        frame = yf.download(**kwargs)
        if frame.empty and len(ticker_list) == 1:
            logger.debug("yf.download empty for %s; trying Ticker.history fallback.", label)
            frame = _ticker_history_fallback(
                ticker_list[0],
                period=period,
                start=start,
                end=end,
                auto_adjust=auto_adjust,
                actions=actions,
            )
        return frame
    except Exception as exc:
        logger.warning("download_daily_bars(%s) failed: %s", label, exc)
        raise DataFetchError(f"Failed to download daily bars for {label}: {exc}") from exc


def _ticker_history_fallback(
    ticker: str,
    period: str = "1y",
    *,
    start: str | date | None = None,
    end: str | date | None = None,
    auto_adjust: bool = True,
    actions: bool = False,
) -> pd.DataFrame:
    try:
        tk = yf.Ticker(ticker)
        kwargs: dict[str, Any] = {"auto_adjust": auto_adjust, "actions": actions, "interval": "1d"}
        if start and end:
            kwargs["start"] = str(start)
            kwargs["end"] = str(end)
        else:
            kwargs["period"] = period
        return tk.history(**kwargs)
    except Exception:
        return pd.DataFrame()


def get_ticker_info(ticker: str) -> dict[str, Any]:
    try:
        return yf.Ticker(ticker).info or {}
    except Exception as exc:
        logger.debug("get_ticker_info(%s) failed: %s", ticker, exc)
        return {}


def get_ticker_news(ticker: str, limit: int = 8) -> list[dict[str, Any]]:
    try:
        articles = yf.Ticker(ticker).news or []
        return [item for item in articles[:limit] if isinstance(item, dict)]
    except Exception as exc:
        logger.debug("get_ticker_news(%s) failed: %s", ticker, exc)
        return []


def get_earnings_date(ticker: str) -> datetime | None:
    try:
        calendar = yf.Ticker(ticker).calendar
        if calendar is None:
            return None
        if isinstance(calendar, dict):
            raw = calendar.get("Earnings Date")
            values = raw if isinstance(raw, (list, tuple)) else [raw]
            for value in values:
                if value is None or value == "":
                    continue
                parsed = cast(datetime, pd.Timestamp(value).to_pydatetime())
                return parsed
            return None
        if isinstance(calendar, pd.DataFrame) and not calendar.empty:
            for value in calendar.to_numpy().flatten():
                if pd.isna(value):
                    continue
                parsed = cast(datetime, pd.Timestamp(value).to_pydatetime())
                return parsed
        return None
    except Exception as exc:
        logger.debug("get_earnings_date(%s) failed: %s", ticker, exc)
        return None


def get_current_vix() -> float | None:
    try:
        ticker = yf.Ticker("^VIX")
        hist = ticker.history(period="1d", interval="1m")
        if hist is not None and not hist.empty:
            return float(hist["Close"].iloc[-1])
        hist_daily = ticker.history(period="5d")
        if hist_daily is not None and not hist_daily.empty:
            return float(hist_daily["Close"].iloc[-1])
    except Exception as exc:
        logger.debug("get_current_vix() failed: %s", exc)
    return None
