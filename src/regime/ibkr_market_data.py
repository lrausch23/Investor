from __future__ import annotations

import datetime as dt
import json
import logging
import threading
import time
from collections import deque
from types import SimpleNamespace
from typing import Any

import pandas as pd

from src.importers.adapters import ProviderError

from .config import DEFAULT_IBKR_CONFIG
from .exceptions import DataValidationError
from .ib_connection import get_shared_ib_backend
from .ib_thread import get_ib_thread
from .persistence import get_setting, set_setting

logger = logging.getLogger(__name__)

DEFAULT_MARKET_DATA_PROVIDER_CONFIG = {
    "benchmark_provider_order": ["cache", "ibkr", "stooq", "yahoo"],
    "benchmark_enabled": {"cache": True, "ibkr": True, "stooq": True, "yahoo": False},
    "momentum_provider_order": ["ibkr", "stooq", "finnhub"],
    "momentum_enabled": {"ibkr": True, "stooq": True, "finnhub": True},
    "regime_provider_order": ["ibkr", "yfinance"],
    "regime_enabled": {"ibkr": True, "yfinance": True},
}

_MACRO_SYMBOL_MAP: dict[str, dict[str, str]] = {
    "^VIX": {"symbol": "VIX", "exchange": "CBOE", "what_to_show": "TRADES"},
    "^TNX": {"symbol": "TNX", "exchange": "CBOE", "what_to_show": "TRADES"},
}


class _IBKRRateLimiter:
    def __init__(self, *, max_requests: int = 60, window_seconds: int = 600):
        self.max_requests = int(max_requests)
        self.window_seconds = int(window_seconds)
        self._timestamps: deque[float] = deque()
        self._lock = threading.Lock()

    def acquire(self) -> None:
        while True:
            with self._lock:
                now = time.time()
                while self._timestamps and (now - self._timestamps[0]) >= self.window_seconds:
                    self._timestamps.popleft()
                if len(self._timestamps) < self.max_requests:
                    self._timestamps.append(now)
                    return
                wait_seconds = max(0.0, self.window_seconds - (now - self._timestamps[0]))
            time.sleep(min(wait_seconds, 1.0))


_RATE_LIMITER = _IBKRRateLimiter()


def get_market_data_provider_config() -> dict[str, Any]:
    raw = get_setting("market_data_provider_config")
    config = json.loads(raw) if raw else {}
    merged = json.loads(json.dumps(DEFAULT_MARKET_DATA_PROVIDER_CONFIG))
    if isinstance(config, dict):
        for key, value in config.items():
            if isinstance(value, dict) and isinstance(merged.get(key), dict):
                merged[key].update(value)
            else:
                merged[key] = value
    return merged


def save_market_data_provider_config(config: dict[str, Any]) -> dict[str, Any]:
    benchmark_order = [str(item).strip().lower() for item in (config.get("benchmark_provider_order") or []) if str(item).strip()]
    momentum_order = [str(item).strip().lower() for item in (config.get("momentum_provider_order") or []) if str(item).strip()]
    regime_order = [str(item).strip().lower() for item in (config.get("regime_provider_order") or []) if str(item).strip()]
    if not benchmark_order or benchmark_order[0] != "cache":
        raise DataValidationError("benchmark_provider_order must start with 'cache'.")
    allowed_benchmark = {"cache", "ibkr", "stooq", "yahoo"}
    allowed_momentum = {"ibkr", "stooq", "finnhub"}
    allowed_regime = {"ibkr", "yfinance"}
    if any(item not in allowed_benchmark for item in benchmark_order):
        raise DataValidationError("benchmark_provider_order contains an unsupported provider.")
    if any(item not in allowed_momentum for item in momentum_order):
        raise DataValidationError("momentum_provider_order contains an unsupported provider.")
    if regime_order and any(item not in allowed_regime for item in regime_order):
        raise DataValidationError("regime_provider_order contains an unsupported provider.")
    payload = {
        "benchmark_provider_order": benchmark_order,
        "benchmark_enabled": {key: bool(value) for key, value in dict(config.get("benchmark_enabled") or {}).items()},
        "momentum_provider_order": momentum_order,
        "momentum_enabled": {key: bool(value) for key, value in dict(config.get("momentum_enabled") or {}).items()},
        "regime_provider_order": regime_order,
        "regime_enabled": {key: bool(value) for key, value in dict(config.get("regime_enabled") or {}).items()},
    }
    merged = get_market_data_provider_config()
    for key, value in payload.items():
        if value:
            merged[key] = value
    set_setting("market_data_provider_config", json.dumps(merged))
    return merged


def apply_benchmark_provider_settings(default_order: list[str] | None = None) -> tuple[list[str], dict[str, bool]]:
    if default_order:
        return [str(item).lower() for item in default_order], {}
    config = get_market_data_provider_config()
    order = [str(item).lower() for item in (config.get("benchmark_provider_order") or default_order or DEFAULT_MARKET_DATA_PROVIDER_CONFIG["benchmark_provider_order"])]
    enabled = {**DEFAULT_MARKET_DATA_PROVIDER_CONFIG["benchmark_enabled"], **dict(config.get("benchmark_enabled") or {})}
    return order, enabled


def apply_momentum_provider_settings(default_order: list[str] | None = None) -> tuple[list[str], dict[str, bool]]:
    config = get_market_data_provider_config()
    order = [str(item).lower() for item in (config.get("momentum_provider_order") or default_order or DEFAULT_MARKET_DATA_PROVIDER_CONFIG["momentum_provider_order"])]
    enabled = {**DEFAULT_MARKET_DATA_PROVIDER_CONFIG["momentum_enabled"], **dict(config.get("momentum_enabled") or {})}
    return order, enabled


def apply_regime_provider_settings(default_order: list[str] | None = None) -> tuple[list[str], dict[str, bool]]:
    config = get_market_data_provider_config()
    order = [str(item).lower() for item in (config.get("regime_provider_order") or default_order or DEFAULT_MARKET_DATA_PROVIDER_CONFIG["regime_provider_order"])]
    enabled = {**DEFAULT_MARKET_DATA_PROVIDER_CONFIG["regime_enabled"], **dict(config.get("regime_enabled") or {})}
    return order, enabled


def _resolve_macro_contract(yf_symbol: str) -> dict[str, str] | None:
    return _MACRO_SYMBOL_MAP.get(str(yf_symbol or "").strip().upper())


class IBKRMarketDataProvider:
    name = "ibkr"

    def __init__(self, *, config=DEFAULT_IBKR_CONFIG):
        self.config = config

    def is_available(self) -> bool:
        backend = get_shared_ib_backend(
            account_id=str(self.config.account_id),
            config=self.config,
            connect_if_needed=False,
        )
        if backend is None:
            return False
        ib = getattr(backend, "_ib", None)
        if ib is None:
            return False
        try:
            return bool(ib.isConnected())
        except Exception:
            return False

    def fetch(self, *, symbol: str, start: dt.date, end: dt.date) -> pd.DataFrame:
        backend = get_shared_ib_backend(account_id=str(self.config.account_id), config=self.config, connect_if_needed=True)
        ib = getattr(backend, "_ib", None) if backend is not None else None
        if backend is None or ib is None:
            raise ProviderError("IBKR market data unavailable (gateway not connected).")
        try:
            connected = bool(ib.isConnected())
        except Exception:
            connected = False
        if not connected:
            raise ProviderError("IBKR market data unavailable (gateway not connected).")

        _RATE_LIMITER.acquire()
        duration_days = max(5, (end - start).days + 5)

        async def _fetch():
            try:
                from ib_insync import Contract
                contract = Contract(symbol=str(symbol).upper(), secType="STK", exchange="SMART", currency="USD")
            except Exception:
                contract = SimpleNamespace(symbol=str(symbol).upper(), secType="STK", exchange="SMART", currency="USD")

            await ib.qualifyContractsAsync(contract)
            bars = await ib.reqHistoricalDataAsync(
                contract,
                endDateTime=end.strftime("%Y%m%d 23:59:59"),
                durationStr=f"{duration_days} D",
                barSizeSetting="1 day",
                whatToShow="ADJUSTED_TRADES",
                useRTH=True,
                formatDate=1,
            )
            return list(bars or [])

        try:
            bars = get_ib_thread().run(_fetch, timeout=20)
        except Exception as exc:
            raise ProviderError(f"IBKR historical data failed: {exc}") from exc
        if not bars:
            raise ProviderError("IBKR returned no data.")

        rows: list[dict[str, Any]] = []
        for bar in bars:
            bar_date = getattr(bar, "date", None)
            if isinstance(bar_date, dt.datetime):
                index_date = bar_date.date()
            else:
                index_date = dt.date.fromisoformat(str(bar_date)[:10])
            rows.append(
                {
                    "date": index_date.isoformat(),
                    "open": float(getattr(bar, "open", 0.0) or 0.0),
                    "high": float(getattr(bar, "high", 0.0) or 0.0),
                    "low": float(getattr(bar, "low", 0.0) or 0.0),
                    "close": float(getattr(bar, "close", 0.0) or 0.0),
                    "adj_close": float(getattr(bar, "close", 0.0) or 0.0),
                    "volume": float(getattr(bar, "volume", 0.0) or 0.0),
                }
            )
        frame = pd.DataFrame.from_records(rows)
        frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
        frame = frame.dropna(subset=["date"]).set_index("date").sort_index()
        frame = frame[(frame.index.date >= start) & (frame.index.date <= end)]
        if frame.empty:
            raise ProviderError("IBKR returned 0 rows in requested range.")
        return frame

    def fetch_index(
        self,
        *,
        symbol: str,
        start: dt.date,
        end: dt.date,
        exchange: str = "CBOE",
        what_to_show: str = "TRADES",
    ) -> pd.DataFrame | None:
        backend = get_shared_ib_backend(account_id=str(self.config.account_id), config=self.config, connect_if_needed=True)
        ib = getattr(backend, "_ib", None) if backend is not None else None
        if backend is None or ib is None:
            return None
        try:
            if not bool(ib.isConnected()):
                return None
        except Exception:
            return None

        _RATE_LIMITER.acquire()
        duration_days = max(5, (end - start).days + 5)

        async def _fetch():
            try:
                from ib_insync import Index

                contract = Index(symbol=str(symbol).upper().lstrip("^"), exchange=exchange, currency="USD")
            except Exception:
                contract = SimpleNamespace(symbol=str(symbol).upper().lstrip("^"), secType="IND", exchange=exchange, currency="USD")
            await ib.qualifyContractsAsync(contract)
            bars = await ib.reqHistoricalDataAsync(
                contract,
                endDateTime=end.strftime("%Y%m%d 23:59:59"),
                durationStr=f"{duration_days} D",
                barSizeSetting="1 day",
                whatToShow=what_to_show,
                useRTH=True,
                formatDate=1,
            )
            return list(bars or [])

        try:
            bars = get_ib_thread().run(_fetch, timeout=20)
        except Exception as exc:
            logger.warning("IBKR macro fetch failed for %s: %s", symbol, exc)
            return None
        if not bars:
            return None
        rows: list[dict[str, Any]] = []
        for bar in bars:
            bar_date = getattr(bar, "date", None)
            if isinstance(bar_date, dt.datetime):
                index_date = bar_date.date()
            else:
                index_date = dt.date.fromisoformat(str(bar_date)[:10])
            close_value = float(getattr(bar, "close", 0.0) or 0.0)
            if str(symbol).upper().lstrip("^") == "TNX":
                close_value /= 10.0
            rows.append({"date": index_date.isoformat(), "close": close_value})
        frame = pd.DataFrame.from_records(rows)
        frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
        frame = frame.dropna(subset=["date"]).set_index("date").sort_index()
        frame = frame[(frame.index.date >= start) & (frame.index.date <= end)]
        return frame if not frame.empty else None
