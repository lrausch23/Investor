from __future__ import annotations

import datetime as dt
from pathlib import Path

import pytest


pd = pytest.importorskip("pandas")


def _df_from_rows(rows: list[tuple[str, float]]) -> "pd.DataFrame":
    df = pd.DataFrame.from_records([{"date": d, "close": c} for d, c in rows])
    df["date"] = pd.to_datetime(df["date"])
    return df.set_index("date").sort_index()


def test_stitching_fills_missing_ranges(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    from src.investor.marketdata.benchmarks import BenchmarkDataClient
    from src.investor.marketdata.config import BenchmarksConfig

    cfg = BenchmarksConfig()
    cfg.cache.path = str(tmp_path / "bench.sqlite")
    cfg.provider_order = ["cache", "stooq"]
    cfg.yahoo.enabled = False

    client = BenchmarkDataClient(config=cfg)

    # Seed cache with a middle chunk (Jan 03 only).
    seed = _df_from_rows([("2025-01-03", 100.0)])
    client.cache.write(symbol="SPY", df=seed)

    calls: list[tuple[dt.date, dt.date]] = []

    def _fake_fetch(*, symbol: str, start: dt.date, end: dt.date):
        calls.append((start, end))
        # Return a superset (provider doesn't have to clip perfectly).
        return _df_from_rows(
            [
                ("2025-01-01", 98.0),
                ("2025-01-02", 99.0),
                ("2025-01-03", 100.0),
                ("2025-01-04", 101.0),
                ("2025-01-05", 102.0),
            ]
        )

    monkeypatch.setattr(client.stooq, "fetch", _fake_fetch)

    df, meta = client.get(symbol="SPY", start=dt.date(2025, 1, 1), end=dt.date(2025, 1, 5), refresh=False)
    assert not df.empty
    assert df.index.min().date() == dt.date(2025, 1, 1)
    assert df.index.max().date() == dt.date(2025, 1, 5)
    assert len(df) == 5
    assert "cache" in meta.used_providers
    assert "stooq" in meta.used_providers
    assert calls  # called at least once to fill missing segments


def test_deduplication_by_date(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    from src.investor.marketdata.benchmarks import BenchmarkDataClient
    from src.investor.marketdata.config import BenchmarksConfig

    cfg = BenchmarksConfig()
    cfg.cache.path = str(tmp_path / "bench.sqlite")
    cfg.provider_order = ["cache", "stooq"]
    cfg.yahoo.enabled = False

    client = BenchmarkDataClient(config=cfg)

    # Seed with one date.
    client.cache.write(symbol="SPY", df=_df_from_rows([("2025-01-02", 99.0)]))

    def _fake_fetch(*, symbol: str, start: dt.date, end: dt.date):
        # Overlap includes 2025-01-02 with a different value; cache should end up with a single row per date.
        return _df_from_rows([("2025-01-01", 98.0), ("2025-01-02", 999.0), ("2025-01-03", 100.0)])

    monkeypatch.setattr(client.stooq, "fetch", _fake_fetch)
    df, _meta = client.get(symbol="SPY", start=dt.date(2025, 1, 1), end=dt.date(2025, 1, 3), refresh=False)
    assert len(df) == 3
    # One row per day.
    assert len({d.date() for d in df.index}) == 3


def test_provider_fallback_when_stooq_fails(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    from src.investor.marketdata.benchmarks import BenchmarkDataClient
    from src.investor.marketdata.config import BenchmarksConfig
    from src.importers.adapters import ProviderError

    cfg = BenchmarksConfig()
    cfg.cache.path = str(tmp_path / "bench.sqlite")
    cfg.provider_order = ["cache", "stooq", "yahoo"]
    cfg.yahoo.enabled = True

    client = BenchmarkDataClient(config=cfg)

    def _stooq_fail(*, symbol: str, start: dt.date, end: dt.date):
        raise ProviderError("stooq down")

    def _yahoo_ok(*, symbol: str, start: dt.date, end: dt.date):
        return _df_from_rows([("2025-01-02", 100.0), ("2025-01-03", 101.0)])

    monkeypatch.setattr(client.stooq, "fetch", _stooq_fail)
    monkeypatch.setattr(client.yahoo, "fetch", _yahoo_ok)

    df, meta = client.get(symbol="SPY", start=dt.date(2025, 1, 2), end=dt.date(2025, 1, 3), refresh=False)
    assert len(df) == 2
    assert "yahoo" in meta.used_providers


def test_symbol_proxy_mapping_gspc(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    from src.investor.marketdata.benchmarks import BenchmarkDataClient
    from src.investor.marketdata.config import BenchmarksConfig

    cfg = BenchmarksConfig()
    cfg.cache.path = str(tmp_path / "bench.sqlite")
    cfg.provider_order = ["cache", "stooq"]
    cfg.yahoo.enabled = False
    cfg.benchmark_proxy = "SPY"

    client = BenchmarkDataClient(config=cfg)

    def _fake_fetch(*, symbol: str, start: dt.date, end: dt.date):
        assert symbol == "SPY"
        return _df_from_rows([("2025-01-02", 100.0)])

    monkeypatch.setattr(client.stooq, "fetch", _fake_fetch)
    df, meta = client.get(symbol="^GSPC", start=dt.date(2025, 1, 2), end=dt.date(2025, 1, 2), refresh=False)
    assert len(df) == 1
    assert meta.canonical_symbol == "SPY"


@pytest.mark.integration
def test_integration_stooq_fetch_small_range(tmp_path: Path):
    """
    Skippable integration test: requires network enabled + stooq allowlisted.
    """
    import os

    from src.investor.marketdata.benchmarks import BenchmarkDataClient
    from src.investor.marketdata.config import BenchmarksConfig
    from src.core.net import allowed_outbound_hosts, outbound_host_allowlist_enabled

    if (os.environ.get("NETWORK_ENABLED") or "").strip().lower() not in {"1", "true", "yes", "on"}:
        pytest.skip("NETWORK_ENABLED not set")
    if outbound_host_allowlist_enabled() and ("stooq.com" not in allowed_outbound_hosts()):
        pytest.skip("stooq.com not allowlisted (set DISABLE_OUTBOUND_HOST_ALLOWLIST=1 or allowlist stooq.com)")

    cfg = BenchmarksConfig()
    cfg.cache.path = str(tmp_path / "bench.sqlite")
    cfg.provider_order = ["cache", "stooq"]
    cfg.yahoo.enabled = False

    client = BenchmarkDataClient(config=cfg)
    df, _meta = client.get(symbol="SPY", start=dt.date(2025, 1, 2), end=dt.date(2025, 1, 10), refresh=True)
    assert not df.empty
