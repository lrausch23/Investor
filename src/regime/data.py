from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import logging

import numpy as np
import pandas as pd
import yfinance as yf

from .config import ticker_candidates
from .exceptions import DataFetchError
from .logging_config import setup_regime_logging
from .persistence import get_cached_earnings_date, save_earnings_cache

setup_regime_logging()
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class MarketSeries:
    ticker: str
    frame: pd.DataFrame


_DEFAULT_MACRO_VALUES = {
    "vix": 20.0,
    "yield_10y": 4.0,
}


def _normalize_price_history_columns(history: pd.DataFrame, ticker: str) -> pd.DataFrame:
    if not isinstance(history.columns, pd.MultiIndex):
        return history

    if ticker in history.columns.get_level_values(-1):
        return history.xs(ticker, axis=1, level=-1)
    if ticker in history.columns.get_level_values(0):
        return history.xs(ticker, axis=1, level=0)

    return history.droplevel(-1, axis=1)


def _empty_series(index: pd.Index, name: str, default: float) -> pd.Series:
    return pd.Series(np.full(len(index), default, dtype=float), index=index, name=name)


def _extract_close_series(history: pd.DataFrame, ticker: str, series_name: str) -> pd.Series | None:
    if history.empty:
        return None

    normalized = _normalize_price_history_columns(history, ticker)
    if "Close" in normalized.columns:
        series = normalized["Close"]
    elif not normalized.columns.empty:
        series = normalized.iloc[:, 0]
    else:
        return None

    result = pd.Series(series, copy=True).rename(series_name)
    if result.empty:
        return None
    return result.astype(float)


def _download_price_history(ticker: str, period: str, interval: str) -> tuple[str, pd.DataFrame]:
    last_history = pd.DataFrame()
    candidates = ticker_candidates(ticker) or [ticker]
    logger.debug("Downloading price history for %s using candidates=%s", ticker, candidates)

    for candidate in candidates:
        history = yf.download(
            tickers=candidate,
            period=period,
            interval=interval,
            auto_adjust=True,
            progress=False,
            threads=False,
        )
        if history.empty:
            logger.debug("No price history returned for candidate %s", candidate)
            last_history = history
            continue
        logger.debug("Resolved ticker %s to candidate %s", ticker, candidate)
        return candidate, history

    raise DataFetchError(f"No price history returned for {ticker}.")


def _download_macro_inputs(index: pd.Index, period: str, interval: str) -> pd.DataFrame:
    macro_columns = {"^VIX": "vix", "^TNX": "yield_10y"}
    series_map: dict[str, pd.Series] = {}

    for symbol, series_name in macro_columns.items():
        logger.debug("Downloading macro input %s for %s", symbol, series_name)
        history = yf.download(
            tickers=symbol,
            period=period,
            interval=interval,
            auto_adjust=False,
            progress=False,
            threads=False,
        )
        extracted = _extract_close_series(history, symbol, series_name)
        if extracted is None:
            extracted = _empty_series(index, series_name, _DEFAULT_MACRO_VALUES[series_name])
        series_map[series_name] = extracted.reindex(index).ffill().bfill()

    return pd.DataFrame(series_map, index=index)


def download_market_frame(ticker: str, period: str = "3y", interval: str = "1d") -> MarketSeries:
    logger.info("Building market frame for %s period=%s interval=%s", ticker, period, interval)
    resolved_ticker, history = _download_price_history(ticker, period=period, interval=interval)
    history = _normalize_price_history_columns(history, resolved_ticker)

    close_col = "Close" if "Close" in history.columns else history.columns[0]
    high_col = "High" if "High" in history.columns else None
    low_col = "Low" if "Low" in history.columns else None
    volume_col = "Volume" if "Volume" in history.columns else None
    if volume_col is None:
        raise DataFetchError(f"Volume history is unavailable for {ticker}.")

    selected_cols = [close_col, volume_col]
    if high_col:
        selected_cols.append(high_col)
    if low_col:
        selected_cols.append(low_col)
    frame = history[selected_cols].dropna().copy()
    rename_map = {close_col: "price", volume_col: "volume"}
    if high_col:
        rename_map[high_col] = "high"
    if low_col:
        rename_map[low_col] = "low"
    frame = frame.rename(columns=rename_map)
    frame["price"] = frame["price"].astype(float)
    frame["volume"] = frame["volume"].astype(float)
    frame["high"] = frame["high"].astype(float) if "high" in frame.columns else frame["price"]
    frame["low"] = frame["low"].astype(float) if "low" in frame.columns else frame["price"]
    macro = _download_macro_inputs(frame.index, period=period, interval=interval)
    frame = frame.join(macro, how="left")
    frame = frame.ffill().dropna()
    logger.debug("Built market frame for %s with %d rows", ticker, len(frame))
    return MarketSeries(ticker=ticker, frame=frame)


def fetch_recent_news(ticker: str, limit: int = 8) -> list[dict]:
    try:
        ticker_obj = yf.Ticker(ticker)
        raw_items = getattr(ticker_obj, "news", None) or []
    except Exception as exc:
        logger.warning("Unable to fetch recent news for %s; continuing with empty catalyst set.", ticker)
        logger.debug("Recent news fetch failed for %s.", ticker, exc_info=exc)
        return []
    normalized: list[dict] = []

    for item in raw_items[:limit]:
        content = item.get("content", {})
        normalized.append(
            {
                "title": content.get("title") or item.get("title") or "",
                "summary": content.get("summary") or "",
                "publisher": content.get("provider", {}).get("displayName") or item.get("publisher") or "",
                "link": content.get("canonicalUrl", {}).get("url") or item.get("link") or "",
                "published_at": content.get("pubDate") or item.get("providerPublishTime") or "",
            }
        )
    return normalized


def get_next_earnings_date(ticker: str) -> datetime | None:
    cached = get_cached_earnings_date(ticker)
    if cached:
        try:
            return datetime.fromisoformat(cached)
        except ValueError:
            pass

    try:
        ticker_obj = yf.Ticker(ticker)
        calendar = getattr(ticker_obj, "calendar", None)
        earnings_date = None
        if isinstance(calendar, pd.DataFrame) and not calendar.empty:
            for value in calendar.to_numpy().flatten():
                if pd.isna(value):
                    continue
                ts = pd.Timestamp(value)
                if ts.tzinfo is None:
                    ts = ts.tz_localize(timezone.utc)
                earnings_date = ts.to_pydatetime()
                break
        elif isinstance(calendar, dict):
            raw_value = calendar.get("Earnings Date")
            values = raw_value if isinstance(raw_value, (list, tuple)) else [raw_value]
            for value in values:
                if value is None or value == "":
                    continue
                ts = pd.Timestamp(value)
                if ts.tzinfo is None:
                    ts = ts.tz_localize(timezone.utc)
                earnings_date = ts.to_pydatetime()
                break
    except Exception as exc:
        logger.debug("Unable to load earnings date for %s", ticker, exc_info=exc)
        earnings_date = None

    save_earnings_cache(ticker, earnings_date.isoformat() if earnings_date else None)
    return earnings_date
