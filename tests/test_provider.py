from __future__ import annotations

import datetime as dt
import types

import pytest

pd = pytest.importorskip("pandas")

from market_data.provider import YahooFinanceProvider
from market_data.utils import update_cache, validate_cache


def test_provider_fetch_prices_schema(monkeypatch):
    # Stub yfinance.download.
    fake = types.SimpleNamespace()

    def download(ticker, start, end, auto_adjust, actions, progress, threads):
        idx = pd.to_datetime(["2025-01-02", "2025-01-03"])
        return pd.DataFrame(
            {
                "Open": [1.0, 2.0],
                "High": [1.0, 2.0],
                "Low": [1.0, 2.0],
                "Close": [1.0, 2.0],
                "Adj Close": [1.1, 2.2],
                "Volume": [10, 20],
                "Dividends": [0.0, 0.0],
                "Stock Splits": [0.0, 0.0],
            },
            index=idx,
        )

    fake.download = download
    monkeypatch.setitem(__import__("sys").modules, "yfinance", fake)

    p = YahooFinanceProvider()
    df = p.fetch_prices("BRK-B", dt.date(2025, 1, 1), dt.date(2025, 1, 4), auto_adjust=False)
    assert not df.empty
    for c in ["open", "high", "low", "close", "volume", "dividends", "splits", "adj_close", "ticker"]:
        assert c in df.columns
    assert df.index.name == "date"
    assert df.index.is_monotonic_increasing


def test_provider_fetch_prices_multiindex_columns(monkeypatch):
    fake = types.SimpleNamespace()

    def download(ticker, start, end, auto_adjust, actions, progress, threads):
        idx = pd.to_datetime(["2025-01-02", "2025-01-03"])
        cols = pd.MultiIndex.from_product(
            [["Open", "High", "Low", "Close", "Volume", "Dividends", "Stock Splits"], [ticker]]
        )
        data = [
            [1.0, 1.0, 1.0, 1.0, 10, 0.0, 0.0],
            [2.0, 2.0, 2.0, 2.0, 20, 0.0, 0.0],
        ]
        return pd.DataFrame(data, index=idx, columns=cols)

    fake.download = download
    monkeypatch.setitem(__import__("sys").modules, "yfinance", fake)

    p = YahooFinanceProvider()
    df = p.fetch_prices("AMD", dt.date(2025, 1, 1), dt.date(2025, 1, 4), auto_adjust=True)
    for c in ["open", "high", "low", "close", "volume", "dividends", "splits", "ticker"]:
        assert c in df.columns


def test_update_cache_merge_and_validate(tmp_path, monkeypatch):
    # Monkeypatch provider fetch to avoid network.
    from market_data import utils as md_utils

    class StubProvider:
        name = "yfinance"

        def fetch_prices(self, ticker, start, end, auto_adjust=True):
            idx = pd.to_datetime(["2025-01-01", "2025-01-02", "2025-01-03"]).normalize()
            df = pd.DataFrame(
                {
                    "open": [1.0, 1.0, 1.0],
                    "high": [1.0, 1.0, 1.0],
                    "low": [1.0, 1.0, 1.0],
                    "close": [1.0, 1.0, 1.0],
                    "volume": [0, 0, 0],
                    "dividends": [0.0, 0.0, 0.0],
                    "splits": [0.0, 0.0, 0.0],
                    "ticker": [ticker, ticker, ticker],
                },
                index=idx,
            )
            df.index.name = "date"
            return df

    monkeypatch.setattr(md_utils, "YahooFinanceProvider", lambda: StubProvider())
    monkeypatch.setattr(md_utils.time, "sleep", lambda *_args, **_kwargs: None)

    cache_dir = tmp_path / "yfinance"
    update_cache(["AAA"], "2025-01-01", "2025-01-03", cache_dir=cache_dir)
    v = validate_cache(["AAA"], "2025-01-01", "2025-01-03", cache_dir=cache_dir)
    assert v["missing_tickers"] == []


def test_update_cache_does_not_refetch_holiday_gap(tmp_path, monkeypatch):
    # If cache starts at Jan 2 (Jan 1 holiday), update_cache should treat it as complete and not refetch.
    from market_data import utils as md_utils

    calls = {"n": 0}

    class StubProvider:
        name = "yfinance"

        def fetch_prices(self, ticker, start, end, auto_adjust=True):
            calls["n"] += 1
            raise AssertionError("Should not refetch for a 1-day holiday gap.")

    monkeypatch.setattr(md_utils, "YahooFinanceProvider", lambda: StubProvider())
    monkeypatch.setattr(md_utils.time, "sleep", lambda *_args, **_kwargs: None)

    import pandas as pd

    cache_dir = tmp_path / "yfinance"
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache = md_utils.PriceCache(cache_dir)
    idx = pd.to_datetime(["2025-01-02", "2025-01-03"]).normalize()
    df = pd.DataFrame(
        {
            "open": [1.0, 1.0],
            "high": [1.0, 1.0],
            "low": [1.0, 1.0],
            "close": [1.0, 1.0],
            "volume": [0, 0],
            "dividends": [0.0, 0.0],
            "splits": [0.0, 0.0],
            "ticker": ["AAA", "AAA"],
        },
        index=idx,
    )
    df.index.name = "date"
    cache.save(
        "AAA",
        df,
        md_utils.CacheMetadata(
            provider="yfinance",
            original_ticker="AAA",
            provider_ticker="AAA",
            auto_adjust=True,
            first_date="2025-01-02",
            last_date="2025-01-03",
            fetched_at="2025-01-04T00:00:00Z",
            rows=2,
        ),
    )
    md_utils.update_cache(["AAA"], "2025-01-01", "2025-01-03", cache_dir=cache_dir)
    assert calls["n"] == 0
