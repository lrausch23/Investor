from __future__ import annotations

from dataclasses import dataclass
import datetime as dt
from datetime import datetime, timezone
import logging

import numpy as np
import pandas as pd

from .config import ticker_candidates
from .exceptions import DataFetchError
from .logging_config import setup_regime_logging
from .market_data_client import download_daily_bars, get_earnings_date, get_ticker_news
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
    end_date = dt.date.today()
    start_date = _period_to_start_date(period, end_date)

    try:
        from .ibkr_market_data import IBKRMarketDataProvider, apply_regime_provider_settings

        provider_order, enabled = apply_regime_provider_settings()
    except Exception:
        IBKRMarketDataProvider = None  # type: ignore[assignment]
        provider_order = ["yfinance"]
        enabled = {"yfinance": True}

    for candidate in candidates:
        for provider_name in provider_order or ["yfinance"]:
            provider_key = str(provider_name or "").strip().lower()
            if enabled and not bool(enabled.get(provider_key, False)):
                continue
            try:
                if provider_key == "ibkr" and IBKRMarketDataProvider is not None:
                    provider = IBKRMarketDataProvider()
                    if not provider.is_available():
                        continue
                    history = provider.fetch(symbol=candidate, start=start_date, end=end_date)
                    history = _standardize_ohlcv_columns(history)
                elif provider_key == "yfinance":
                    history = download_daily_bars(candidate, period=period, auto_adjust=True)
                else:
                    continue
            except Exception as exc:
                logger.warning("Price history fetch for %s from %s failed: %s", candidate, provider_key, exc)
                continue
            if history.empty:
                logger.debug("No price history returned for candidate %s from %s", candidate, provider_key)
                last_history = history
                continue
            logger.info("Resolved ticker %s to candidate %s using %s", ticker, candidate, provider_key)
            return candidate, history

    raise DataFetchError(f"No price history returned for {ticker}.")


def _standardize_ohlcv_columns(history: pd.DataFrame) -> pd.DataFrame:
    if history.empty:
        return history
    rename_map = {
        "open": "Open",
        "high": "High",
        "low": "Low",
        "close": "Close",
        "adj_close": "Adj Close",
        "volume": "Volume",
    }
    return history.rename(columns={column: rename_map.get(str(column), str(column)) for column in history.columns})


def _period_to_start_date(period: str, end_date: dt.date) -> dt.date:
    normalized = str(period or "3y").strip().lower()
    if normalized.endswith("y") and normalized[:-1].isdigit():
        return end_date - dt.timedelta(days=int(normalized[:-1]) * 365)
    if normalized.endswith("mo") and normalized[:-2].isdigit():
        return end_date - dt.timedelta(days=int(normalized[:-2]) * 30)
    if normalized.endswith("m") and normalized[:-1].isdigit():
        return end_date - dt.timedelta(days=int(normalized[:-1]) * 30)
    if normalized.endswith("d") and normalized[:-1].isdigit():
        return end_date - dt.timedelta(days=int(normalized[:-1]))
    return end_date - dt.timedelta(days=365 * 3)


def _download_macro_inputs(index: pd.Index, period: str, interval: str) -> pd.DataFrame:
    del interval
    from .ibkr_market_data import IBKRMarketDataProvider, _resolve_macro_contract, apply_regime_provider_settings

    provider_order, enabled = apply_regime_provider_settings()
    macro_columns = {"^VIX": "vix", "^TNX": "yield_10y"}
    series_map: dict[str, pd.Series] = {}
    end_date = dt.date.today()
    start_date = _period_to_start_date(period, end_date)

    for yf_symbol, series_name in macro_columns.items():
        extracted = None
        for provider_name in provider_order:
            if not enabled.get(provider_name, False):
                continue
            try:
                if provider_name == "ibkr":
                    contract_info = _resolve_macro_contract(yf_symbol)
                    if contract_info is None:
                        continue
                    provider = IBKRMarketDataProvider()
                    if not provider.is_available():
                        continue
                    history = provider.fetch_index(
                        symbol=contract_info["symbol"],
                        start=start_date,
                        end=end_date,
                        exchange=contract_info["exchange"],
                        what_to_show=contract_info["what_to_show"],
                    )
                    if history is not None and not history.empty:
                        extracted = pd.Series(history["close"], copy=True).rename(series_name).astype(float)
                elif provider_name == "yfinance":
                    history = download_daily_bars(yf_symbol, period=period, auto_adjust=False)
                    extracted = _extract_close_series(history, yf_symbol, series_name)
                if extracted is not None:
                    logger.info("Macro %s fetched from %s", series_name, provider_name)
                    break
            except Exception as exc:
                logger.warning("Macro %s fetch from %s failed: %s", series_name, provider_name, exc)
                continue
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
    open_col = "Open" if "Open" in history.columns else None
    volume_col = "Volume" if "Volume" in history.columns else None
    if volume_col is None:
        raise DataFetchError(f"Volume history is unavailable for {ticker}.")

    selected_cols = [close_col, volume_col]
    if high_col:
        selected_cols.append(high_col)
    if low_col:
        selected_cols.append(low_col)
    if open_col:
        selected_cols.append(open_col)
    frame = history[selected_cols].dropna().copy()
    rename_map = {close_col: "price", volume_col: "volume"}
    if high_col:
        rename_map[high_col] = "high"
    if low_col:
        rename_map[low_col] = "low"
    if open_col:
        rename_map[open_col] = "open"
    frame = frame.rename(columns=rename_map)
    frame["price"] = frame["price"].astype(float)
    frame["volume"] = frame["volume"].astype(float)
    frame["high"] = frame["high"].astype(float) if "high" in frame.columns else frame["price"]
    frame["low"] = frame["low"].astype(float) if "low" in frame.columns else frame["price"]
    frame["open"] = frame["open"].astype(float) if "open" in frame.columns else frame["price"]
    macro = _download_macro_inputs(frame.index, period=period, interval=interval)
    frame = frame.join(macro, how="left")
    frame = frame.ffill().dropna()
    logger.debug("Built market frame for %s with %d rows", ticker, len(frame))
    return MarketSeries(ticker=ticker, frame=frame)


def fetch_recent_news(ticker: str, limit: int = 8) -> list[dict]:
    try:
        raw_items = get_ticker_news(ticker, limit=limit)
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
        earnings_date = get_earnings_date(ticker)
        if earnings_date is not None and earnings_date.tzinfo is None:
            earnings_date = earnings_date.replace(tzinfo=timezone.utc)
    except Exception as exc:
        logger.debug("Unable to load earnings date for %s", ticker, exc_info=exc)
        earnings_date = None

    save_earnings_cache(ticker, earnings_date.isoformat() if earnings_date else None)
    return earnings_date
