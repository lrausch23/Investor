from __future__ import annotations

import datetime as dt

import pytest

pd = pytest.importorskip("pandas")

from market_data.cache import CacheMetadata, PriceCache
from market_data.symbols import normalize_ticker, sanitize_ticker


def test_sanitize_ticker():
    assert sanitize_ticker("BRK-B") == "BRK_B"
    assert sanitize_ticker("BRK.B") == "BRK_B"
    assert sanitize_ticker("BTC-USD") == "BTC_USD"
    assert sanitize_ticker(" CASH:USD ") == "CASH_USD"


def test_normalize_ticker():
    assert normalize_ticker("BRK.B").provider_ticker == "BRK-B"
    assert normalize_ticker("BRKB").provider_ticker == "BRK-B"
    assert normalize_ticker("BRKA").provider_ticker == "BRK-A"
    assert normalize_ticker("BTC/USD").provider_ticker == "BTC-USD"
    assert normalize_ticker("CASH:USD").kind == "synthetic_cash"
    assert normalize_ticker("EUR").provider_ticker == "EURUSD=X"
    # 3-letter equities should not be treated as FX.
    assert normalize_ticker("AMD").provider_ticker == "AMD"
    # Crypto shorthand should map to Yahoo crypto pair.
    assert normalize_ticker("ETH").provider_ticker == "ETH-USD"
    # Manual physical holdings should never be sent to a market-data provider.
    assert normalize_ticker("BULLION:GOLD").kind == "invalid"


def test_cache_save_load_roundtrip(tmp_path):
    cache = PriceCache(tmp_path)
    idx = pd.to_datetime([dt.date(2025, 1, 1), dt.date(2025, 1, 2)]).normalize()
    df = pd.DataFrame(
        {
            "open": [1.0, 2.0],
            "high": [1.0, 2.0],
            "low": [1.0, 2.0],
            "close": [1.0, 2.0],
            "volume": [0, 0],
            "dividends": [0.0, 0.0],
            "splits": [0.0, 0.0],
            "ticker": ["AAA", "AAA"],
        },
        index=idx,
    )
    df.index.name = "date"
    meta = CacheMetadata(
        provider="yfinance",
        original_ticker="AAA",
        provider_ticker="AAA",
        auto_adjust=True,
        first_date="2025-01-01",
        last_date="2025-01-02",
        fetched_at="2025-01-03T00:00:00Z",
        rows=2,
    )
    cache.save("AAA", df, meta)
    df2 = cache.load("AAA")
    assert df2 is not None
    assert list(df2.columns) == list(df.columns)
    assert df2.index.min() == df.index.min()
