from __future__ import annotations

import datetime as dt
import importlib
import time
from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient
import pandas as pd
import pytest
from sqlalchemy.orm import Session

from src.app.routes import regime as regime_route
from src.investor.marketdata.config import BenchmarksConfig
from src.investor.marketdata.benchmarks import BenchmarkDataClient
from src.investor.momentum.prices import MarketDataService
from src.regime import ib_connection as ib_connection_module
from src.regime import ibkr_market_data as ibkr_market_data_module
from src.regime import persistence as persistence_module


@pytest.fixture()
def temp_modules(tmp_path, monkeypatch):
    monkeypatch.setenv("HMM_DATA_DIR", str(tmp_path))
    store = importlib.reload(persistence_module)
    monkeypatch.setattr(store, "DB_PATH", tmp_path / "regime_watch.db")
    store = importlib.reload(persistence_module)
    ibkr_md = importlib.reload(ibkr_market_data_module)
    store._connect().close()
    return store, ibkr_md


def _route_client() -> TestClient:
    app = FastAPI()
    app.include_router(regime_route.router)
    app.dependency_overrides[regime_route.require_actor] = lambda: "tester"
    app.dependency_overrides[regime_route.db_session] = lambda: iter([None])
    return TestClient(app)


def _df(d0: dt.date, d1: dt.date) -> pd.DataFrame:
    rows = []
    px = 100.0
    d = d0
    while d <= d1:
        rows.append({"date": d.isoformat(), "close": px, "adj_close": px, "volume": 1000.0})
        d += dt.timedelta(days=1)
        px += 1.0
    frame = pd.DataFrame.from_records(rows)
    frame["date"] = pd.to_datetime(frame["date"])
    return frame.set_index("date").sort_index()


def test_ibkr_provider_fetch_returns_canonical_frame(temp_modules, monkeypatch) -> None:
    _store, ibkr_md = temp_modules

    class FakeBar:
        def __init__(self, date, close):
            self.date = date
            self.open = close - 1
            self.high = close + 1
            self.low = close - 2
            self.close = close
            self.volume = 1000

    class FakeIB:
        async def qualifyContractsAsync(self, contract):
            return [contract]

        async def reqHistoricalDataAsync(self, *args, **kwargs):
            return [FakeBar("2025-01-02", 100.0), FakeBar("2025-01-03", 101.0)]

        def isConnected(self):
            return True

    class FakeBackend:
        _ib = FakeIB()

    class FakeThread:
        def run(self, fn, *args, **kwargs):
            import asyncio

            return asyncio.run(fn(*args))

    monkeypatch.setattr(ibkr_md, "get_shared_ib_backend", lambda **kwargs: FakeBackend())
    monkeypatch.setattr(ibkr_md, "get_ib_thread", lambda: FakeThread())
    provider = ibkr_md.IBKRMarketDataProvider()
    frame = provider.fetch(symbol="SPY", start=dt.date(2025, 1, 2), end=dt.date(2025, 1, 3))
    assert list(frame.columns) == ["open", "high", "low", "close", "adj_close", "volume"]
    assert len(frame) == 2


def test_ibkr_provider_unavailable_when_backend_disconnected(temp_modules, monkeypatch) -> None:
    _store, ibkr_md = temp_modules
    monkeypatch.setattr(ibkr_md, "get_shared_ib_backend", lambda **kwargs: None)
    assert ibkr_md.IBKRMarketDataProvider().is_available() is False


def test_rate_limiter_waits_when_bucket_full(temp_modules, monkeypatch) -> None:
    _store, ibkr_md = temp_modules
    limiter = ibkr_md._IBKRRateLimiter(max_requests=1, window_seconds=10)
    times = iter([100.0, 100.0, 100.0, 111.0, 111.0])
    sleeps = []
    monkeypatch.setattr(ibkr_md.time, "time", lambda: next(times))
    monkeypatch.setattr(ibkr_md.time, "sleep", lambda seconds: sleeps.append(seconds))
    limiter.acquire()
    limiter.acquire()
    assert sleeps


def test_benchmark_client_prefers_ibkr(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    cfg = BenchmarksConfig()
    cfg.cache.path = str(tmp_path / "bench.sqlite")
    client = BenchmarkDataClient(config=cfg)
    monkeypatch.setattr(client.ibkr, "is_available", lambda: True)
    monkeypatch.setattr(client.ibkr, "fetch", lambda **kwargs: _df(kwargs["start"], kwargs["end"]))
    monkeypatch.setattr(client.stooq, "fetch", lambda **kwargs: (_ for _ in ()).throw(AssertionError("stooq should not run")))
    frame, meta = client.get(symbol="SPY", start=dt.date(2025, 1, 2), end=dt.date(2025, 1, 3), refresh=True)
    assert not frame.empty
    assert "ibkr" in meta.used_providers


def test_benchmark_client_falls_back_when_ibkr_unavailable(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    cfg = BenchmarksConfig()
    cfg.cache.path = str(tmp_path / "bench.sqlite")
    client = BenchmarkDataClient(config=cfg)
    monkeypatch.setattr(client.ibkr, "is_available", lambda: False)
    monkeypatch.setattr(client.stooq, "fetch", lambda **kwargs: _df(kwargs["start"], kwargs["end"]))
    frame, meta = client.get(symbol="SPY", start=dt.date(2025, 1, 2), end=dt.date(2025, 1, 3), refresh=True)
    assert not frame.empty
    assert "stooq" in meta.used_providers


def test_momentum_auto_prefers_ibkr(session: Session, monkeypatch: pytest.MonkeyPatch) -> None:
    import os

    os.environ["NETWORK_ENABLED"] = "0"
    md = MarketDataService(provider="auto")
    monkeypatch.setattr(md.ibkr, "fetch", lambda **kwargs: _df(kwargs["start"], kwargs["end"]))
    monkeypatch.setattr(md.ibkr, "is_available", lambda: True)
    monkeypatch.setattr(md.stooq, "fetch", lambda **kwargs: (_ for _ in ()).throw(AssertionError("stooq should not run")))
    frame, meta = md.get_daily(session, ticker="AAPL", start=dt.date(2025, 1, 1), end=dt.date(2025, 1, 5), refresh=True)
    assert not frame.empty
    assert "ibkr" in meta.source_used


def test_momentum_auto_falls_back_to_stooq_then_finnhub(session: Session, monkeypatch: pytest.MonkeyPatch) -> None:
    import os

    os.environ["NETWORK_ENABLED"] = "1"
    md = MarketDataService(provider="auto")
    monkeypatch.setattr(md.ibkr, "fetch", lambda **kwargs: (_ for _ in ()).throw(RuntimeError("ibkr down")))
    monkeypatch.setattr(md.stooq, "fetch", lambda **kwargs: (_ for _ in ()).throw(RuntimeError("stooq down")))
    monkeypatch.setattr(md.finnhub, "fetch", lambda **kwargs: _df(kwargs["start"], kwargs["end"]))
    frame, meta = md.get_daily(session, ticker="AAPL", start=dt.date(2025, 1, 1), end=dt.date(2025, 1, 5), refresh=True)
    assert not frame.empty
    assert "finnhub" in meta.source_used


def test_market_data_settings_round_trip(temp_modules, monkeypatch) -> None:
    store, ibkr_md = temp_modules
    monkeypatch.setattr(regime_route, "require_actor", lambda: "tester")
    monkeypatch.setattr(ibkr_md, "get_shared_ib_backend", lambda **kwargs: None)
    client = _route_client()
    response = client.put(
        "/regime/market-data/settings",
        json={
            "benchmark_provider_order": ["cache", "ibkr", "stooq"],
            "benchmark_enabled": {"cache": True, "ibkr": True, "stooq": True, "yahoo": False},
            "momentum_provider_order": ["ibkr", "stooq", "finnhub"],
            "momentum_enabled": {"ibkr": True, "stooq": True, "finnhub": True},
        },
    )
    assert response.status_code == 200
    payload = client.get("/regime/market-data/settings").json()
    assert payload["settings"]["benchmark_provider_order"][0] == "cache"
    assert store.get_setting("market_data_provider_config")


def test_market_data_settings_validates_cache_first(temp_modules) -> None:
    _store, _ibkr_md = temp_modules
    client = _route_client()
    response = client.put(
        "/regime/market-data/settings",
        json={
            "benchmark_provider_order": ["ibkr", "cache", "stooq"],
            "momentum_provider_order": ["ibkr", "stooq", "finnhub"],
        },
    )
    assert response.status_code == 422


def test_market_data_settings_get_reports_ibkr_status(temp_modules, monkeypatch) -> None:
    _store, ibkr_md = temp_modules

    class FakeBackend:
        _ib = type("FakeIB", (), {"isConnected": lambda self: True})()

    monkeypatch.setattr(ibkr_md, "get_shared_ib_backend", lambda **kwargs: FakeBackend())
    client = _route_client()
    payload = client.get("/regime/market-data/settings").json()
    assert payload["ibkr_connected"] is True


def test_apply_benchmark_settings_uses_saved_json(temp_modules) -> None:
    store, ibkr_md = temp_modules
    store.set_setting(
        "market_data_provider_config",
        '{"benchmark_provider_order":["cache","stooq","yahoo"],"benchmark_enabled":{"cache":true,"ibkr":false,"stooq":true,"yahoo":true}}',
    )
    order, enabled = ibkr_md.apply_benchmark_provider_settings()
    assert order == ["cache", "stooq", "yahoo"]
    assert enabled["ibkr"] is False


def test_apply_momentum_settings_uses_saved_json(temp_modules) -> None:
    store, ibkr_md = temp_modules
    store.set_setting(
        "market_data_provider_config",
        '{"momentum_provider_order":["stooq","finnhub"],"momentum_enabled":{"ibkr":false,"stooq":true,"finnhub":true}}',
    )
    order, enabled = ibkr_md.apply_momentum_provider_settings()
    assert order == ["stooq", "finnhub"]
    assert enabled["ibkr"] is False
