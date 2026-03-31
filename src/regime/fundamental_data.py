from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any

import pandas as pd
import yfinance as yf

logger = logging.getLogger(__name__)

CACHE_TTL_SECONDS = 86400

_cache: dict[str, tuple[float, "FinancialStatements"]] = {}


@dataclass
class FinancialStatements:
    """Container for fetched annual and quarterly financial statements."""

    ticker: str
    income_statement: pd.DataFrame
    balance_sheet: pd.DataFrame
    cashflow: pd.DataFrame
    quarterly_income: pd.DataFrame
    quarterly_balance_sheet: pd.DataFrame
    quarterly_cashflow: pd.DataFrame
    info: dict[str, Any] = field(default_factory=dict)
    fetched_at: float = 0.0

    @property
    def is_empty(self) -> bool:
        return bool(self.income_statement.empty and self.balance_sheet.empty and self.cashflow.empty)

    @property
    def years_available(self) -> int:
        if self.income_statement.empty:
            return 0
        return int(len(self.income_statement.columns))


def _safe_df(value: Any) -> pd.DataFrame:
    if isinstance(value, pd.DataFrame):
        return value
    return pd.DataFrame()


def fetch_financial_statements(ticker: str, *, use_cache: bool = True) -> FinancialStatements:
    """Fetch annual and quarterly financial statements for a ticker."""
    key = str(ticker or "").upper().strip()
    now = time.time()
    if use_cache and key in _cache:
        cached_at, cached = _cache[key]
        if (now - float(cached_at)) < CACHE_TTL_SECONDS:
            return cached
    try:
        yf_ticker = yf.Ticker(key)
        result = FinancialStatements(
            ticker=key,
            income_statement=_safe_df(getattr(yf_ticker, "financials", None)),
            balance_sheet=_safe_df(getattr(yf_ticker, "balance_sheet", None)),
            cashflow=_safe_df(getattr(yf_ticker, "cashflow", None)),
            quarterly_income=_safe_df(getattr(yf_ticker, "quarterly_financials", None)),
            quarterly_balance_sheet=_safe_df(getattr(yf_ticker, "quarterly_balance_sheet", None)),
            quarterly_cashflow=_safe_df(getattr(yf_ticker, "quarterly_cashflow", None)),
            info=getattr(yf_ticker, "info", None) or {},
            fetched_at=now,
        )
    except Exception as exc:
        logger.warning("fetch_financial_statements(%s) failed: %s", key, exc)
        result = FinancialStatements(
            ticker=key,
            income_statement=pd.DataFrame(),
            balance_sheet=pd.DataFrame(),
            cashflow=pd.DataFrame(),
            quarterly_income=pd.DataFrame(),
            quarterly_balance_sheet=pd.DataFrame(),
            quarterly_cashflow=pd.DataFrame(),
            info={},
            fetched_at=now,
        )
    _cache[key] = (now, result)
    return result


def clear_cache(ticker: str | None = None) -> int:
    """Clear one ticker's cached financials or the whole cache."""
    if ticker:
        return 1 if _cache.pop(str(ticker).upper(), None) is not None else 0
    removed = len(_cache)
    _cache.clear()
    return removed
