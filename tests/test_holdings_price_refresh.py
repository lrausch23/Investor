from __future__ import annotations

import datetime as dt
from pathlib import Path
from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest

from src.app.routes import holdings as holdings_route
from src.importers.adapters import ProviderError

pd = pytest.importorskip("pandas")


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(holdings_route.router)
    app.dependency_overrides[holdings_route.require_actor] = lambda: "tester"
    app.dependency_overrides[holdings_route.db_session] = lambda: None
    return TestClient(app)


def _fake_positions():
    return SimpleNamespace(positions=[SimpleNamespace(symbol="AAPL")])


def _df() -> "pd.DataFrame":
    frame = pd.DataFrame.from_records(
        [
            {"date": "2025-01-02", "close": 100.0},
            {"date": "2025-01-03", "close": 101.0},
        ]
    )
    frame["date"] = pd.to_datetime(frame["date"])
    return frame.set_index("date").sort_index()


def test_holdings_refresh_defaults_to_ibkr(monkeypatch, tmp_path: Path):
    import market_data.symbols as md_symbols
    import src.core.external_holdings as external_holdings
    import src.core.benchmarks as core_benchmarks
    import src.regime.ibkr_market_data as ibkr_market_data

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(external_holdings, "build_holdings_view", lambda *args, **kwargs: _fake_positions())
    monkeypatch.setattr(md_symbols, "normalize_ticker", lambda *args, **kwargs: SimpleNamespace(kind="equity", provider_ticker="AAPL"))

    class FakeIBKRProvider:
        def fetch(self, *, symbol: str, start: dt.date, end: dt.date):
            assert symbol == "AAPL"
            return _df()

    monkeypatch.setattr(ibkr_market_data, "IBKRMarketDataProvider", FakeIBKRProvider)
    monkeypatch.setattr(
        core_benchmarks,
        "download_yahoo_price_history_csv",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("Yahoo fallback should not be used")),
    )

    response = _client().post("/holdings/prices/refresh", data={"scope": "household", "account_id": ""}, follow_redirects=False)

    assert response.status_code == 303
    assert "ok=Updated%201%20price%20file(s)." in response.headers["location"]
    csv_path = tmp_path / "data" / "prices" / "AAPL.csv"
    assert csv_path.exists()
    assert csv_path.read_text(encoding="utf-8").startswith("Date,Close\n2025-01-02,100.0")


def test_holdings_refresh_ibkr_falls_back_to_yahoo(monkeypatch, tmp_path: Path):
    import market_data.symbols as md_symbols
    import src.core.net as core_net
    import src.core.external_holdings as external_holdings
    import src.core.benchmarks as core_benchmarks
    import src.regime.ibkr_market_data as ibkr_market_data

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(external_holdings, "build_holdings_view", lambda *args, **kwargs: _fake_positions())
    monkeypatch.setattr(md_symbols, "normalize_ticker", lambda *args, **kwargs: SimpleNamespace(kind="equity", provider_ticker="AAPL"))
    monkeypatch.setattr(core_net, "outbound_host_allowlist_enabled", lambda: True)
    monkeypatch.setattr(core_net, "allowed_outbound_hosts", lambda: {"query1.finance.yahoo.com"})

    class FakeIBKRProvider:
        def fetch(self, *, symbol: str, start: dt.date, end: dt.date):
            raise ProviderError("IBKR market data unavailable (gateway not connected).")

    def _yahoo_ok(*, symbol: str, start_date: dt.date, end_date: dt.date, dest_path: Path, **kwargs):
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        dest_path.write_text("Date,Close,Adj Close\n2025-01-02,100.0,100.0\n", encoding="utf-8")
        return SimpleNamespace(start_date=start_date, end_date=end_date)

    monkeypatch.setattr(ibkr_market_data, "IBKRMarketDataProvider", FakeIBKRProvider)
    monkeypatch.setattr(core_benchmarks, "download_yahoo_price_history_csv", _yahoo_ok)

    response = _client().post("/holdings/prices/refresh", data={"scope": "household", "account_id": "", "provider": "ibkr"}, follow_redirects=False)

    assert response.status_code == 303
    assert "Used%20Yahoo%20fallback%20for%201%20symbol(s)." in response.headers["location"]
    meta_path = tmp_path / "data" / "prices" / "AAPL.json"
    assert meta_path.exists()
    assert '"fallback_from": "ibkr"' in meta_path.read_text(encoding="utf-8")


def test_holdings_refresh_ibkr_reports_clear_error_when_yahoo_blocked(monkeypatch, tmp_path: Path):
    import market_data.symbols as md_symbols
    import src.core.benchmarks as core_benchmarks
    import src.core.external_holdings as external_holdings
    import src.core.net as core_net
    import src.regime.ibkr_market_data as ibkr_market_data

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(external_holdings, "build_holdings_view", lambda *args, **kwargs: _fake_positions())
    monkeypatch.setattr(md_symbols, "normalize_ticker", lambda *args, **kwargs: SimpleNamespace(kind="equity", provider_ticker="AAPL"))
    monkeypatch.setattr(core_net, "outbound_host_allowlist_enabled", lambda: True)
    monkeypatch.setattr(core_net, "allowed_outbound_hosts", lambda: {"stooq.com"})

    class FakeIBKRProvider:
        def fetch(self, *, symbol: str, start: dt.date, end: dt.date):
            raise ProviderError("IBKR market data unavailable (gateway not connected).")

    monkeypatch.setattr(ibkr_market_data, "IBKRMarketDataProvider", FakeIBKRProvider)
    monkeypatch.setattr(
        core_benchmarks,
        "download_yahoo_price_history_csv",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("Yahoo fallback should not be attempted")),
    )

    response = _client().post("/holdings/prices/refresh", data={"scope": "household", "account_id": "", "provider": "ibkr"}, follow_redirects=False)

    assert response.status_code == 303
    assert "IBKR%20refresh%20unavailable" in response.headers["location"]
    assert "Yahoo%20fallback%20is%20blocked%20by%20ALLOWED_OUTBOUND_HOSTS" in response.headers["location"]
