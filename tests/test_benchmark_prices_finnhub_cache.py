from __future__ import annotations

import datetime as dt
from pathlib import Path

import pytest


def test_finnhub_benchmark_cache_uses_fresh_file(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    from src.core.benchmark_prices import get_benchmark_prices_csv

    start = dt.date(2025, 1, 1)
    end = dt.date(2025, 1, 31)
    cache_root = tmp_path / "benchmarks"

    # Create a "fresh" cached file + meta.
    csv_path = cache_root / "finnhub" / "SPY_2025-01-01_2025-01-31.csv"
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    csv_path.write_text("Date,Close\n2025-01-02,100.0\n", encoding="utf-8")
    meta_path = csv_path.with_suffix(".json")
    meta_path.write_text('{"rows": 1, "fetched_at": "2025-01-31T00:00:00+00:00"}', encoding="utf-8")

    # Prevent network download from being invoked.
    def _boom(**_kwargs):
        raise AssertionError("download should not be called for a fresh cache")

    monkeypatch.setenv("FINNHUB_API_KEY", "testkey")
    monkeypatch.setattr("src.core.benchmark_prices.download_finnhub_price_history_csv", _boom)

    res = get_benchmark_prices_csv(
        symbol="SPY",
        start_date=start,
        end_date=end,
        refresh=False,
        ttl=dt.timedelta(days=999),
        cache_root=cache_root,
    )
    assert res.path is not None
    assert res.used_cache is True
    assert res.warning is None


def test_finnhub_benchmark_cache_refresh_calls_downloader(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    from src.core.benchmark_prices import get_benchmark_prices_csv
    from types import SimpleNamespace

    start = dt.date(2025, 1, 1)
    end = dt.date(2025, 1, 31)
    cache_root = tmp_path / "benchmarks"

    called = {"n": 0}

    def _fake_download(
        *,
        symbol: str,
        start_date: dt.date,
        end_date: dt.date,
        dest_path: Path,
        api_key: str,
        timeout_s: float = 30.0,
        max_retries: int = 4,
        backoff_s: float = 1.0,
    ):
        called["n"] += 1
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        dest_path.write_text("Date,Close\n2025-01-02,100.0\n", encoding="utf-8")
        return SimpleNamespace(rows=1, start_date=start_date, end_date=end_date)

    monkeypatch.setenv("FINNHUB_API_KEY", "testkey")
    monkeypatch.setattr("src.core.benchmark_prices.download_finnhub_price_history_csv", _fake_download)

    res = get_benchmark_prices_csv(
        symbol="SPY",
        start_date=start,
        end_date=end,
        refresh=True,
        ttl=dt.timedelta(days=999),
        cache_root=cache_root,
    )
    assert called["n"] == 1
    assert res.path is not None
    assert res.used_cache is False


def test_finnhub_benchmark_cache_missing_key(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    from src.core.benchmark_prices import get_benchmark_prices_csv

    monkeypatch.delenv("FINNHUB_API_KEY", raising=False)
    res = get_benchmark_prices_csv(
        symbol="SPY",
        start_date=dt.date(2025, 1, 1),
        end_date=dt.date(2025, 1, 31),
        cache_root=tmp_path / "benchmarks",
    )
    # With no key, Finnhub can't be used; Yahoo may still be attempted depending on allowlist/network settings.
    assert res.path is None or res.provider in {"yahoo", "finnhub"}
    assert res.warning or res.path is not None


def test_benchmark_provider_no_fallback_returns_missing_when_finnhub_key_missing(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    from src.core.benchmark_prices import get_benchmark_prices_csv

    monkeypatch.delenv("FINNHUB_API_KEY", raising=False)
    res = get_benchmark_prices_csv(
        symbol="SPY",
        start_date=dt.date(2025, 1, 1),
        end_date=dt.date(2025, 1, 31),
        provider_preference="finnhub",
        allow_fallback=False,
        cache_root=tmp_path / "benchmarks",
    )
    assert res.path is None
    assert res.provider == "finnhub"
    assert res.warning


def test_auto_fallback_to_yahoo_includes_note_but_returns_path(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    from src.core.benchmark_prices import get_benchmark_prices_csv
    from src.importers.adapters import ProviderError
    from types import SimpleNamespace

    start = dt.date(2025, 1, 1)
    end = dt.date(2025, 1, 31)
    cache_root = tmp_path / "benchmarks"

    def _finnhub_fail(**_kwargs):
        raise ProviderError("HTTP error status=403 host=finnhub.io path=/api/v1/stock/candle")

    def _yahoo_ok(
        *,
        symbol: str,
        start_date: dt.date,
        end_date: dt.date,
        dest_path: Path,
        timeout_s: float = 30.0,
        max_retries: int = 6,
        backoff_s: float = 2.0,
    ):
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        dest_path.write_text("Date,Close\n2025-01-02,100.0\n2025-01-03,101.0\n", encoding="utf-8")
        return SimpleNamespace(rows=2, start_date=start_date, end_date=end_date)

    monkeypatch.setenv("FINNHUB_API_KEY", "testkey")
    monkeypatch.setattr("src.core.benchmark_prices.download_finnhub_price_history_csv", _finnhub_fail)
    monkeypatch.setattr("src.core.benchmark_prices.download_yahoo_price_history_csv", _yahoo_ok)

    res = get_benchmark_prices_csv(
        symbol="SPY",
        start_date=start,
        end_date=end,
        refresh=True,
        provider_preference="finnhub",
        allow_fallback=True,
        cache_root=cache_root,
    )
    assert res.provider == "yahoo"
    assert res.path is not None
    assert res.warning and "Using Yahoo" in res.warning


def test_auto_failure_message_includes_both_providers(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    from src.core.benchmark_prices import get_benchmark_prices_csv
    from src.importers.adapters import ProviderError

    def _fail(**_kwargs):
        raise ProviderError("boom")

    monkeypatch.setenv("FINNHUB_API_KEY", "testkey")
    monkeypatch.setattr("src.core.benchmark_prices.download_finnhub_price_history_csv", _fail)
    monkeypatch.setattr("src.core.benchmark_prices.download_yahoo_price_history_csv", _fail)

    res = get_benchmark_prices_csv(
        symbol="SPY",
        start_date=dt.date(2025, 1, 1),
        end_date=dt.date(2025, 1, 31),
        refresh=True,
        provider_preference="finnhub",
        allow_fallback=True,
        cache_root=tmp_path / "benchmarks",
    )
    assert res.path is None
    assert res.warning
    assert "Finnhub:" in res.warning and "Yahoo:" in res.warning
