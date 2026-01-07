from __future__ import annotations


class MarketDataError(Exception):
    pass


class DataNotFoundError(MarketDataError):
    """Raised when the provider returns no usable rows for a ticker/range."""


class FetchError(MarketDataError):
    """Raised when a ticker repeatedly fails to fetch after retries."""

