from __future__ import annotations

import datetime as dt

import pandas as pd
import pytest
from sqlalchemy.orm import Session

from src.investor.momentum.prices import MarketDataService


def _df(d0: dt.date, d1: dt.date) -> pd.DataFrame:
    rows = []
    d = d0
    px = 100.0
    while d <= d1:
        rows.append({"date": d.isoformat(), "close": px, "volume": 1_000_000.0})
        px += 1.0
        d += dt.timedelta(days=1)
    df = pd.DataFrame.from_records(rows)
    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date").sort_index()
    return df


def test_marketdata_falls_back_to_finnhub_when_stooq_fails(session: Session, monkeypatch: pytest.MonkeyPatch) -> None:
    import os

    os.environ["NETWORK_ENABLED"] = "1"

    md = MarketDataService(provider="auto")  # stooq -> finnhub

    def stooq_fail(*, symbol: str, start: dt.date, end: dt.date):
        raise RuntimeError("stooq down")

    def finnhub_ok(*, symbol: str, start: dt.date, end: dt.date):
        return _df(start, end)

    monkeypatch.setattr(md.stooq, "fetch", stooq_fail)
    monkeypatch.setattr(md.finnhub, "fetch", finnhub_ok)

    start = dt.date(2025, 1, 1)
    end = dt.date(2025, 1, 10)
    df, meta = md.get_daily(session, ticker="AAPL", start=start, end=end, refresh=False)

    assert not df.empty
    assert meta.rows_fetched > 0
    assert "finnhub" in meta.source_used


def test_marketdata_cache_only_does_not_fetch(session: Session, monkeypatch: pytest.MonkeyPatch) -> None:
    import os

    os.environ["NETWORK_ENABLED"] = "1"
    md = MarketDataService(provider="cache")

    called = {"stooq": 0, "finnhub": 0}

    def stooq_fail(*, symbol: str, start: dt.date, end: dt.date):
        called["stooq"] += 1
        raise RuntimeError("should not call")

    def finnhub_fail(*, symbol: str, start: dt.date, end: dt.date):
        called["finnhub"] += 1
        raise RuntimeError("should not call")

    monkeypatch.setattr(md.stooq, "fetch", stooq_fail)
    monkeypatch.setattr(md.finnhub, "fetch", finnhub_fail)

    start = dt.date(2025, 1, 1)
    end = dt.date(2025, 1, 10)
    df, meta = md.get_daily(session, ticker="AAPL", start=start, end=end, refresh=False)

    assert df.empty
    assert called["stooq"] == 0
    assert called["finnhub"] == 0
    assert meta.warning and "cached prices only" in meta.warning.lower()
