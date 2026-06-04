from __future__ import annotations

import datetime as dt
import time
from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest

from src.app.routes import regime as regime_route
from src.investor.marketdata.config import BenchmarksConfig
from src.investor.marketdata.benchmarks import BenchmarkDataClient
from src.importers.adapters import ProviderError
from src.regime import ibkr_market_data as ibkr_market_data_module


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(regime_route.router)
    app.dependency_overrides[regime_route.require_actor] = lambda: "tester"
    app.dependency_overrides[regime_route.db_session] = lambda: iter([None])
    return TestClient(app)


def test_is_available_returns_false_immediately_when_ib_none(monkeypatch) -> None:
    class FakeBackend:
        _ib = None

    monkeypatch.setattr(ibkr_market_data_module, "get_shared_ib_backend", lambda **kwargs: FakeBackend())
    start = time.perf_counter()
    result = ibkr_market_data_module.IBKRMarketDataProvider().is_available()
    elapsed = time.perf_counter() - start
    assert result is False
    assert elapsed < 0.1


def test_is_available_returns_false_when_backend_none(monkeypatch) -> None:
    monkeypatch.setattr(ibkr_market_data_module, "get_shared_ib_backend", lambda **kwargs: None)
    assert ibkr_market_data_module.IBKRMarketDataProvider().is_available() is False


def test_is_available_returns_true_when_connected(monkeypatch) -> None:
    class FakeIB:
        def isConnected(self):
            return True

    class FakeBackend:
        _ib = FakeIB()

    monkeypatch.setattr(ibkr_market_data_module, "get_shared_ib_backend", lambda **kwargs: FakeBackend())
    assert ibkr_market_data_module.IBKRMarketDataProvider().is_available() is True


def test_is_available_does_not_use_ib_thread(monkeypatch) -> None:
    class FakeIB:
        def isConnected(self):
            return False

    class FakeBackend:
        _ib = FakeIB()

    monkeypatch.setattr(ibkr_market_data_module, "get_shared_ib_backend", lambda **kwargs: FakeBackend())
    monkeypatch.setattr(ibkr_market_data_module, "get_ib_thread", lambda: (_ for _ in ()).throw(RuntimeError("IBThread should not be called")))
    assert ibkr_market_data_module.IBKRMarketDataProvider().is_available() is False


def test_settings_route_fast_when_disconnected(monkeypatch) -> None:
    monkeypatch.setattr(ibkr_market_data_module, "get_shared_ib_backend", lambda **kwargs: None)
    client = _client()
    start = time.perf_counter()
    response = client.get("/regime/market-data/settings")
    elapsed = time.perf_counter() - start
    assert response.status_code == 200
    assert response.json()["ibkr_connected"] is False
    assert elapsed < 2.0


def test_providers_for_order_fast_when_disconnected(monkeypatch, tmp_path: Path) -> None:
    cfg = BenchmarksConfig()
    cfg.cache.path = str(tmp_path / "bench.sqlite")
    cfg.provider_order = ["cache", "ibkr", "stooq", "yahoo"]
    cfg.yahoo.enabled = True
    client = BenchmarkDataClient(config=cfg)
    monkeypatch.setattr(client.ibkr, "is_available", lambda: False)
    start = time.perf_counter()
    providers = client._providers_for_order()
    elapsed = time.perf_counter() - start
    names = [provider.name for provider in providers]
    assert elapsed < 0.1
    assert "stooq" in names
    assert "yahoo" in names


def test_fetch_pre_check_fast_when_disconnected(monkeypatch) -> None:
    class FakeBackend:
        _ib = None

    monkeypatch.setattr(ibkr_market_data_module, "get_shared_ib_backend", lambda **kwargs: FakeBackend())
    monkeypatch.setattr(ibkr_market_data_module, "get_ib_thread", lambda: (_ for _ in ()).throw(RuntimeError("IBThread should not be called")))
    provider = ibkr_market_data_module.IBKRMarketDataProvider()
    start = time.perf_counter()
    with pytest.raises(ProviderError):
        provider.fetch(symbol="SPY", start=dt.date(2025, 1, 2), end=dt.date(2025, 1, 3))
    elapsed = time.perf_counter() - start
    assert elapsed < 0.1


def test_fetch_falls_back_from_adjusted_trades_to_trades(monkeypatch) -> None:
    class FakeIB:
        def isConnected(self):
            return True

        async def qualifyContractsAsync(self, contract):
            return [contract]

        async def reqHistoricalDataAsync(self, contract, endDateTime, durationStr, barSizeSetting, whatToShow, useRTH, formatDate):
            del contract, endDateTime, durationStr, barSizeSetting, useRTH, formatDate
            if whatToShow == "ADJUSTED_TRADES":
                return []

            class Bar:
                date = dt.date(2025, 1, 2)
                open = 100.0
                high = 101.0
                low = 99.0
                close = 100.5
                volume = 1000.0

            return [Bar()]

    class FakeBackend:
        _ib = FakeIB()

    class FakeThread:
        def run(self, fn, *args, timeout=30.0):
            import asyncio

            result = fn(*args)
            if asyncio.iscoroutine(result):
                return asyncio.run(result)
            return result

    monkeypatch.setattr(ibkr_market_data_module, "get_shared_ib_backend", lambda **kwargs: FakeBackend())
    monkeypatch.setattr(ibkr_market_data_module, "get_ib_thread", lambda: FakeThread())

    provider = ibkr_market_data_module.IBKRMarketDataProvider()
    frame = provider.fetch(symbol="AAPL", start=dt.date(2025, 1, 2), end=dt.date(2025, 1, 3))

    assert not frame.empty
    assert float(frame.iloc[-1]["close"]) == 100.5
