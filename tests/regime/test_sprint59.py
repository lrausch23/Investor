from __future__ import annotations

import datetime as dt
import importlib
from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient
import pandas as pd
import pytest

from src.app.routes import regime as regime_route
from src.regime import data as data_module
from src.regime import ibkr_market_data as ibkr_market_data_module
from src.regime import persistence as persistence_module


@pytest.fixture()
def temp_modules(tmp_path, monkeypatch):
    monkeypatch.setenv("HMM_DATA_DIR", str(tmp_path))
    monkeypatch.setattr(persistence_module, "DB_PATH", tmp_path / "regime_watch.db")
    store = importlib.reload(persistence_module)
    ibkr_md = importlib.reload(ibkr_market_data_module)
    data = importlib.reload(data_module)
    return store, ibkr_md, data


def _route_client() -> TestClient:
    app = FastAPI()
    app.include_router(regime_route.router)
    app.dependency_overrides[regime_route.require_actor] = lambda: "tester"
    app.dependency_overrides[regime_route.db_session] = lambda: iter([None])
    return TestClient(app)


def test_apply_regime_settings_uses_saved_json(temp_modules) -> None:
    store, ibkr_md, _data = temp_modules
    store.set_setting(
        "market_data_provider_config",
        '{"regime_provider_order":["yfinance","ibkr"],"regime_enabled":{"ibkr":false,"yfinance":true}}',
    )
    order, enabled = ibkr_md.apply_regime_provider_settings()
    assert order == ["yfinance", "ibkr"]
    assert enabled["ibkr"] is False


def test_resolve_macro_contract_maps_vix_and_tnx(temp_modules) -> None:
    _store, ibkr_md, _data = temp_modules
    assert ibkr_md._resolve_macro_contract("^VIX")["symbol"] == "VIX"
    assert ibkr_md._resolve_macro_contract("^TNX")["symbol"] == "TNX"


def test_fetch_index_divides_tnx_by_10(temp_modules, monkeypatch) -> None:
    _store, ibkr_md, _data = temp_modules

    class FakeBar:
        def __init__(self, date: str, close: float):
            self.date = date
            self.close = close

    class FakeIB:
        async def qualifyContractsAsync(self, contract):
            return [contract]

        async def reqHistoricalDataAsync(self, *args, **kwargs):
            return [FakeBar("2025-01-02", 42.5)]

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
    frame = ibkr_md.IBKRMarketDataProvider().fetch_index(symbol="TNX", start=dt.date(2025, 1, 2), end=dt.date(2025, 1, 2))
    assert frame is not None
    assert frame["close"].iloc[0] == pytest.approx(4.25)


def test_download_macro_inputs_prefers_ibkr(temp_modules, monkeypatch) -> None:
    _store, ibkr_md, data = temp_modules

    class FakeProvider:
        def is_available(self):
            return True

        def fetch_index(self, **kwargs):
            rows = [{"date": "2025-01-02", "close": 21.0}, {"date": "2025-01-03", "close": 22.0}]
            frame = pd.DataFrame.from_records(rows)
            frame["date"] = pd.to_datetime(frame["date"])
            return frame.set_index("date")

    monkeypatch.setattr(ibkr_md, "IBKRMarketDataProvider", FakeProvider)
    monkeypatch.setattr(data, "download_daily_bars", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("yfinance fallback should not run")))
    index = pd.to_datetime(["2025-01-02", "2025-01-03"])
    frame = data._download_macro_inputs(index, period="1y", interval="1d")
    assert list(frame.columns) == ["vix", "yield_10y"]
    assert float(frame["vix"].iloc[-1]) == pytest.approx(22.0)


def test_market_data_settings_route_accepts_regime_keys(temp_modules, monkeypatch) -> None:
    _store, ibkr_md, _data = temp_modules
    monkeypatch.setattr(ibkr_md, "get_shared_ib_backend", lambda **kwargs: None)
    client = _route_client()
    response = client.put(
        "/regime/market-data/settings",
        json={
            "benchmark_provider_order": ["cache", "ibkr", "stooq"],
            "benchmark_enabled": {"cache": True, "ibkr": True, "stooq": True, "yahoo": False},
            "momentum_provider_order": ["ibkr", "stooq", "finnhub"],
            "momentum_enabled": {"ibkr": True, "stooq": True, "finnhub": True},
            "regime_provider_order": ["ibkr", "yfinance"],
            "regime_enabled": {"ibkr": True, "yfinance": True},
        },
    )
    assert response.status_code == 200
    payload = client.get("/regime/market-data/settings").json()
    assert payload["settings"]["regime_provider_order"] == ["ibkr", "yfinance"]


def test_market_data_test_macro_returns_combined_provider_payload(temp_modules, monkeypatch) -> None:
    _store, ibkr_md, _data = temp_modules

    class FakeProvider:
        def is_available(self):
            return True

        def fetch_index(self, **kwargs):
            symbol = kwargs["symbol"]
            close_value = 22.5 if symbol == "VIX" else 4.3
            frame = pd.DataFrame({"close": [close_value]}, index=pd.to_datetime(["2025-01-03"]))
            return frame

    monkeypatch.setattr(ibkr_md, "IBKRMarketDataProvider", FakeProvider)
    monkeypatch.setattr(ibkr_md, "apply_regime_provider_settings", lambda default_order=None: (["ibkr", "yfinance"], {"ibkr": True, "yfinance": True}))
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: ({}, None))
    monkeypatch.setattr("src.regime.market_data_client.download_daily_bars", lambda symbol, period="5d", auto_adjust=False: pd.DataFrame({"Close": [21.0]}, index=pd.to_datetime(["2025-01-02"])))
    client = _route_client()
    response = client.get("/regime/market-data/test-macro")
    assert response.status_code == 200
    payload = response.json()
    assert payload["ibkr_connected"] is True
    assert payload["active_provider_order"] == ["ibkr", "yfinance"]
    assert payload["vix"]["ibkr"]["available"] is True
    assert payload["yield_10y"]["ibkr"]["value"] == pytest.approx(4.3)
    assert payload["vix"]["yfinance"]["available"] is True


def test_market_data_test_macro_handles_ibkr_unavailable(temp_modules, monkeypatch) -> None:
    _store, ibkr_md, _data = temp_modules

    class OfflineProvider:
        def is_available(self):
            return False

    monkeypatch.setattr(ibkr_md, "IBKRMarketDataProvider", OfflineProvider)
    monkeypatch.setattr(ibkr_md, "apply_regime_provider_settings", lambda default_order=None: (["ibkr", "yfinance"], {"ibkr": True, "yfinance": True}))
    monkeypatch.setattr("src.regime.market_data_client.download_daily_bars", lambda symbol, period="5d", auto_adjust=False: pd.DataFrame())
    client = _route_client()
    payload = client.get("/regime/market-data/test-macro").json()
    assert payload["ibkr_connected"] is False
    assert payload["vix"]["ibkr"]["available"] is False
    assert payload["yield_10y"]["yfinance"]["available"] is False


def test_market_data_ui_uses_single_macro_test_button() -> None:
    content = (Path(__file__).resolve().parents[2] / "src" / "app" / "static" / "regime.js").read_text(encoding="utf-8")
    assert 'id="regimeTestMacroData"' in content
    assert 'id="regimeTestMacroVix"' not in content
    assert 'id="regimeTestMacroTnx"' not in content
