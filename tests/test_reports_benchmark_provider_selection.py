from __future__ import annotations

import datetime as dt
from pathlib import Path

import pytest

from src.app.routes.reports import (
    _benchmark_provider_order_for_selection,
    _normalize_benchmark_provider,
    _parse_additional_benchmark_symbols,
)


pd = pytest.importorskip("pandas")


def _df_from_rows(rows: list[tuple[str, float]]) -> "pd.DataFrame":
    df = pd.DataFrame.from_records([{"date": d, "close": c} for d, c in rows])
    df["date"] = pd.to_datetime(df["date"])
    return df.set_index("date").sort_index()


def test_benchmark_provider_selection_supports_ibkr_and_ibrk_alias() -> None:
    assert _normalize_benchmark_provider("IBKR") == "ibkr"
    assert _normalize_benchmark_provider("IBRK") == "ibkr"
    assert _benchmark_provider_order_for_selection("ibkr", ["cache", "stooq"]) == [
        "cache",
        "ibkr",
        "stooq",
        "yahoo",
    ]


def test_benchmark_provider_selection_preserves_auto_configured_order() -> None:
    assert _benchmark_provider_order_for_selection("auto", ["cache", "ibkr", "stooq"]) == [
        "cache",
        "ibkr",
        "stooq",
    ]
    assert _benchmark_provider_order_for_selection("auto", []) == ["cache", "ibkr", "stooq", "yahoo"]


def test_benchmark_provider_selection_keeps_existing_overrides() -> None:
    assert _benchmark_provider_order_for_selection("cache", ["cache", "ibkr", "stooq"]) == ["cache"]
    assert _benchmark_provider_order_for_selection("stooq", ["cache", "ibkr", "stooq"]) == ["cache", "stooq", "yahoo"]
    assert _benchmark_provider_order_for_selection("yahoo", ["cache", "ibkr", "stooq"]) == ["cache", "yahoo"]


def test_parse_additional_benchmark_symbols_dedupes_primary_and_limits() -> None:
    assert _parse_additional_benchmark_symbols("qqq, SPY; iwm  qqq\nagg, voo", "SPY", limit=3) == [
        "QQQ",
        "IWM",
        "AGG",
    ]


def test_parse_additional_benchmark_symbols_sanitizes_user_input() -> None:
    assert _parse_additional_benchmark_symbols(" brk.b, ^gspc, bad<script>, VTI ", "QQQ") == [
        "BRK.B",
        "^GSPC",
        "BADSCRIPT",
        "VTI",
    ]


def test_ibkr_provider_is_attempted_even_when_not_preconnected(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    from src.importers.adapters import ProviderError
    from src.investor.marketdata.benchmarks import BenchmarkDataClient
    from src.investor.marketdata.config import BenchmarksConfig

    cfg = BenchmarksConfig()
    cfg.cache.path = str(tmp_path / "bench.sqlite")
    cfg.provider_order = ["cache", "ibkr", "stooq"]
    cfg.yahoo.enabled = False

    client = BenchmarkDataClient(config=cfg)
    client.cache.write(symbol="SPY", df=_df_from_rows([("2026-01-15", 100.0)]))

    calls = {"ibkr": 0, "stooq": 0}

    def _ibkr_ok(*, symbol: str, start: dt.date, end: dt.date):
        calls["ibkr"] += 1
        return _df_from_rows([("2026-01-16", 101.0), ("2026-01-20", 102.0)])

    def _stooq_fail(*, symbol: str, start: dt.date, end: dt.date):
        calls["stooq"] += 1
        raise ProviderError("stooq should not be needed")

    monkeypatch.setattr(client.ibkr, "is_available", lambda: False)
    monkeypatch.setattr(client.ibkr, "fetch", _ibkr_ok)
    monkeypatch.setattr(client.stooq, "fetch", _stooq_fail)

    df, meta = client.get(symbol="SPY", start=dt.date(2026, 1, 15), end=dt.date(2026, 1, 20), refresh=False)

    assert calls["ibkr"] == 1
    assert calls["stooq"] == 0
    assert df.index.max().date() == dt.date(2026, 1, 20)
    assert "ibkr" in meta.used_providers


def test_benchmark_fetch_continues_after_unfilled_leading_holiday(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from src.importers.adapters import ProviderError
    from src.investor.marketdata.benchmarks import BenchmarkDataClient
    from src.investor.marketdata.config import BenchmarksConfig

    cfg = BenchmarksConfig()
    cfg.cache.path = str(tmp_path / "bench.sqlite")
    cfg.provider_order = ["cache", "ibkr"]
    cfg.yahoo.enabled = False

    client = BenchmarkDataClient(config=cfg)
    seeded = []
    price = 100.0
    d = dt.date(2026, 1, 2)
    while d <= dt.date(2026, 1, 15):
        seeded.append((d.isoformat(), price))
        price += 1.0
        d += dt.timedelta(days=1)
    client.cache.write(symbol="SPY", df=_df_from_rows(seeded))

    calls: list[tuple[dt.date, dt.date]] = []

    def _ibkr_fetch(*, symbol: str, start: dt.date, end: dt.date):
        calls.append((start, end))
        if start == dt.date(2026, 1, 1) and end == dt.date(2026, 1, 1):
            raise ProviderError("market holiday")
        return _df_from_rows([("2026-01-16", 102.0), ("2026-01-20", 103.0)])

    monkeypatch.setattr(client.ibkr, "fetch", _ibkr_fetch)

    df, meta = client.get(symbol="SPY", start=dt.date(2026, 1, 1), end=dt.date(2026, 1, 20), refresh=False)

    assert calls == [(dt.date(2026, 1, 1), dt.date(2026, 1, 1)), (dt.date(2026, 1, 16), dt.date(2026, 1, 20))]
    assert df.index.max().date() == dt.date(2026, 1, 20)
    assert "ibkr" in meta.used_providers
    assert meta.warning is None


def test_performance_benchmark_coverage_warning_allows_nearby_non_trading_boundaries(session) -> None:
    from src.core.performance import build_performance_report

    report = build_performance_report(
        session,
        scope="household",
        start_date=dt.date(2026, 1, 1),
        end_date=dt.date(2026, 1, 10),
        benchmark_series=[(dt.date(2026, 1, 2), 100.0), (dt.date(2026, 1, 9), 101.0)],
        benchmark_label="SPY",
    )

    assert not any("benchmark coverage starts" in w for w in report["warnings"])
    assert not any("benchmark coverage ends" in w for w in report["warnings"])
