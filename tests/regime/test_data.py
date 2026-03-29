from __future__ import annotations

import pandas as pd

from src.regime.data import _normalize_price_history_columns, download_market_frame, fetch_recent_news


def test_normalize_price_history_columns_multiindex() -> None:
    index = pd.date_range("2024-01-01", periods=3, freq="D")
    frame = pd.DataFrame(
        {
            ("Close", "NVDA"): [1.0, 2.0, 3.0],
            ("Volume", "NVDA"): [100, 200, 300],
        },
        index=index,
    )
    normalized = _normalize_price_history_columns(frame, "NVDA")
    assert list(normalized.columns) == ["Close", "Volume"]


def test_normalize_price_history_columns_flat() -> None:
    index = pd.date_range("2024-01-01", periods=3, freq="D")
    frame = pd.DataFrame({"Close": [1.0, 2.0, 3.0], "Volume": [100, 200, 300]}, index=index)
    normalized = _normalize_price_history_columns(frame, "NVDA")
    pd.testing.assert_frame_equal(normalized, frame)


def test_download_market_frame_falls_back_when_tnx_missing(monkeypatch) -> None:
    index = pd.date_range("2024-01-01", periods=3, freq="D")

    def fake_download(*, tickers, period, interval, auto_adjust, progress, threads):
        if tickers == "NVDA":
            return pd.DataFrame(
                {
                    "Close": [100.0, 101.0, 102.0],
                    "High": [101.0, 102.0, 103.0],
                    "Low": [99.0, 100.0, 101.0],
                    "Volume": [1_000_000, 1_100_000, 1_200_000],
                },
                index=index,
            )
        if tickers == "^VIX":
            return pd.DataFrame({"Close": [18.0, 19.0, 20.0]}, index=index)
        if tickers == "^TNX":
            return pd.DataFrame()
        raise AssertionError(f"Unexpected ticker request: {tickers}")

    monkeypatch.setattr("src.regime.data.yf.download", fake_download)

    series = download_market_frame("NVDA", period="3y", interval="1d")

    assert list(series.frame.columns) == ["price", "volume", "high", "low", "open", "vix", "yield_10y"]
    assert series.frame["vix"].tolist() == [18.0, 19.0, 20.0]
    assert series.frame["yield_10y"].tolist() == [4.0, 4.0, 4.0]


def test_download_market_frame_tries_yahoo_safe_share_class_symbol(monkeypatch) -> None:
    index = pd.date_range("2024-01-01", periods=2, freq="D")
    requested: list[str] = []

    def fake_download(*, tickers, period, interval, auto_adjust, progress, threads):
        requested.append(tickers)
        if tickers == "BRK-B":
            return pd.DataFrame(
                {
                    "Close": [500.0, 505.0],
                    "High": [501.0, 506.0],
                    "Low": [499.0, 504.0],
                    "Volume": [10, 11],
                },
                index=index,
            )
        if tickers in {"^VIX", "^TNX"}:
            return pd.DataFrame({"Close": [20.0, 21.0]}, index=index)
        return pd.DataFrame()

    monkeypatch.setattr("src.regime.data.yf.download", fake_download)

    series = download_market_frame("BRK B", period="3y", interval="1d")

    assert series.ticker == "BRK B"
    assert requested[0] == "BRK-B"
    assert series.frame["price"].tolist() == [500.0, 505.0]


def test_fetch_recent_news_timeout_returns_empty(monkeypatch) -> None:
    class BrokenTicker:
        @property
        def news(self):
            raise TimeoutError("network timeout")

    monkeypatch.setattr("src.regime.data.yf.Ticker", lambda ticker: BrokenTicker())

    assert fetch_recent_news("PLTR") == []


def test_fetch_recent_news_normalizes_items(monkeypatch) -> None:
    class FakeTicker:
        news = [
            {
                "content": {
                    "title": "Headline",
                    "summary": "Summary",
                    "provider": {"displayName": "Provider"},
                    "canonicalUrl": {"url": "https://example.com"},
                    "pubDate": "2026-03-26T12:00:00Z",
                }
            }
        ]

    monkeypatch.setattr("src.regime.data.yf.Ticker", lambda ticker: FakeTicker())

    payload = fetch_recent_news("PLTR")
    assert payload[0]["title"] == "Headline"
    assert payload[0]["link"] == "https://example.com"
