from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from src.regime import ccel_campaign as ccel
from src.regime.sharadar.adapter import SharadarFrameLoader, SharadarFundamentalsProvider
from src.regime.sharadar.ingest import build_store_from_frames, ingest_sharadar
from src.regime.sharadar.readiness import certification_gate_status, classify_readiness
from src.regime.sharadar.store import SharadarStore


def _dates(start: str, periods: int) -> list[str]:
    return [date.date().isoformat() for date in pd.date_range(start, periods=periods, freq="D")]


def _fixture_tables() -> dict[str, pd.DataFrame]:
    dates = _dates("2020-01-01", 10)
    sep_rows: list[dict[str, object]] = []
    for idx, date in enumerate(dates[:5]):
        sep_rows.append({"permaticker": 1, "date": date, "open": 10 + idx, "high": 11 + idx, "low": 9 + idx, "close": 10 + idx, "closeadj": 10 + idx, "volume": 1000})
    for idx, date in enumerate(dates[5:]):
        sep_rows.append({"permaticker": 2, "date": date, "open": 100 + idx, "high": 101 + idx, "low": 99 + idx, "close": 100 + idx, "closeadj": 100 + idx, "volume": 1000})
    for idx, date in enumerate(dates[:5]):
        sep_rows.append({"permaticker": 3, "date": date, "open": 20 - idx, "high": 21 - idx, "low": 19 - idx, "close": 20 - idx, "closeadj": 20 - idx, "volume": 1000})
    for idx, date in enumerate(dates):
        sep_rows.append({"permaticker": 4, "date": date, "open": 50 + idx, "high": 51 + idx, "low": 49 + idx, "close": 50 + idx, "closeadj": 50 + idx, "volume": 1000})
    for perma, base in ((5, 200), (6, 300)):
        for idx, date in enumerate(dates):
            sep_rows.append({"permaticker": perma, "date": date, "open": base + idx, "high": base + idx + 1, "low": base + idx - 1, "close": base + idx, "closeadj": base + idx, "volume": 1000})
    sf1_rows = [
        {"permaticker": 1, "datekey": "2020-01-05", "dimension": "ARQ", "netinc": 5, "revenue": 100, "assets": 200, "liabilities": 80, "ebit": 8, "taxexp": 1, "debt": 20, "equity": 100, "assetsc": 80, "liabilitiesc": 40, "lastupdated": "2020-01-06"},
        {"permaticker": 2, "datekey": "2020-01-08", "dimension": "ARQ", "netinc": 7, "revenue": 120, "assets": 220, "liabilities": 90, "ebit": 9, "taxexp": 1, "debt": 25, "equity": 110, "assetsc": 90, "liabilitiesc": 45, "lastupdated": "2020-01-09"},
        {"permaticker": 5, "datekey": "2020-01-02", "dimension": "ARQ", "netinc": 8, "revenue": 150, "assets": 300, "liabilities": 120, "ebit": 12, "taxexp": 2, "debt": 30, "equity": 140, "assetsc": 100, "liabilitiesc": 50, "lastupdated": "2020-01-03"},
        {"permaticker": 6, "datekey": "2020-01-02", "dimension": "ARQ", "netinc": 9, "revenue": 160, "assets": 320, "liabilities": 130, "ebit": 13, "taxexp": 2, "debt": 32, "equity": 150, "assetsc": 110, "liabilitiesc": 55, "lastupdated": "2020-01-03"},
    ]
    return {
        "TICKERS": pd.DataFrame(
            [
                {"ticker": "AAA", "permaticker": 1, "firstpricedate": "2020-01-01", "lastpricedate": "2020-01-05"},
                {"ticker": "AAA", "permaticker": 2, "firstpricedate": "2020-01-06", "lastpricedate": "2020-01-10"},
                {"ticker": "DEL", "permaticker": 3, "firstpricedate": "2020-01-01", "lastpricedate": "2020-01-05"},
                {"ticker": "MISS", "permaticker": 4, "firstpricedate": "2020-01-01", "lastpricedate": "2020-01-10"},
                {"ticker": "SPY", "permaticker": 5, "firstpricedate": "2020-01-01", "lastpricedate": "2020-01-10"},
                {"ticker": "QQQ", "permaticker": 6, "firstpricedate": "2020-01-01", "lastpricedate": "2020-01-10"},
            ]
        ),
        "SEP": pd.DataFrame(sep_rows),
        "SF1": pd.DataFrame(sf1_rows),
        "ACTIONS": pd.DataFrame([{"permaticker": 3, "date": "2020-01-03", "action": "delisted", "terminal_value": 0.0}]),
        "DAILY": pd.DataFrame(
            [
                {"permaticker": perma, "date": "2020-01-02", "marketcap": cap}
                for perma, cap in ((1, 1000), (2, 2000), (3, 300), (4, 500), (5, 5000), (6, 4000))
            ]
        ),
    }


def _build_store(root: Path) -> SharadarStore:
    build_store_from_frames(root, _fixture_tables())
    return SharadarStore(root)


def test_pit_fundamentals_do_not_look_ahead(tmp_path) -> None:
    store = _build_store(tmp_path / "sharadar")

    assert store.get_fundamentals_asof(1, "2020-01-04", ["revenue"]) is None
    as_of = store.get_fundamentals_asof(1, "2020-01-05", ["revenue", "netinc"])

    assert as_of is not None
    assert as_of["revenue"] == 100
    assert as_of["netinc"] == 5


def test_ticker_reuse_resolves_by_permaticker_without_leakage(tmp_path) -> None:
    store = _build_store(tmp_path / "sharadar")

    assert store.resolve_ticker("AAA", "2020-01-04").permaticker == 1
    assert store.resolve_ticker("AAA", "2020-01-07").permaticker == 2
    prices = store.get_prices([1, 2], "2020-01-01", "2020-01-10")

    assert prices[1]["price"].max() < 20
    assert prices[2]["price"].min() >= 100
    assert prices[1].index.max() == pd.Timestamp("2020-01-05")
    assert prices[2].index.min() == pd.Timestamp("2020-01-06")


def test_actual_recycled_symbol_c_resolves_without_cross_issuer_leakage(tmp_path) -> None:
    dates = _dates("2020-01-01", 8)
    sep_rows = []
    for idx, date in enumerate(dates[:4]):
        sep_rows.append({"ticker": "C", "permaticker": 101, "date": date, "open": 10 + idx, "high": 11 + idx, "low": 9 + idx, "close": 10 + idx, "closeadj": 10 + idx, "volume": 1000})
    for idx, date in enumerate(dates[4:]):
        sep_rows.append({"ticker": "C", "permaticker": 202, "date": date, "open": 100 + idx, "high": 101 + idx, "low": 99 + idx, "close": 100 + idx, "closeadj": 100 + idx, "volume": 1000})
    tables = {
        "TICKERS": pd.DataFrame(
            [
                {"ticker": "C", "permaticker": 101, "firstpricedate": "2020-01-01", "lastpricedate": "2020-01-04"},
                {"ticker": "C", "permaticker": 202, "firstpricedate": "2020-01-05", "lastpricedate": "2020-01-08"},
            ]
        ),
        "SEP": pd.DataFrame(sep_rows),
        "SF1": pd.DataFrame(columns=["permaticker", "datekey", "dimension", "netinc", "assets", "revenue"]),
        "ACTIONS": pd.DataFrame(columns=["permaticker", "date", "action"]),
        "DAILY": pd.DataFrame(columns=["permaticker", "date", "marketcap"]),
    }
    root = tmp_path / "recycled_symbol"
    build_store_from_frames(root, tables)
    store = SharadarStore(root)

    assert store.resolve_ticker("C", "2020-01-03").permaticker == 101
    assert store.resolve_ticker("C", "2020-01-07").permaticker == 202
    prices = store.get_prices([101, 202], "2020-01-01", "2020-01-08")
    assert prices[101]["price"].max() < 20
    assert prices[202]["price"].min() >= 100


def test_readiness_accepts_selected_permatickers_without_display_ticker_gaps(tmp_path) -> None:
    store = _build_store(tmp_path / "sharadar")

    readiness = classify_readiness(store, [5, 6], ("2020-01-01", "2020-01-10"))

    assert readiness.data_readiness == "survivorship_free"
    assert readiness.price_coverage_ratio == 1.0
    assert readiness.pit_fundamental_coverage_ratio == 1.0
    assert readiness.missing_price == ()
    assert readiness.missing_pit == ()


def test_readiness_uses_primary_class_fundamentals_for_secondary_share_class(tmp_path) -> None:
    dates = _dates("2020-01-01", 5)
    tables = {
        "TICKERS": pd.DataFrame(
            [
                {"ticker": "GOOGL", "permaticker": 100, "name": "ALPHABET INC", "category": "Domestic Common Stock Primary Class", "table": "SF1", "firstpricedate": "2020-01-01", "lastpricedate": "2020-01-05"},
                {"ticker": "GOOGL", "permaticker": 100, "name": "ALPHABET INC", "category": "Domestic Common Stock Primary Class", "table": "SEP", "firstpricedate": "2020-01-01", "lastpricedate": "2020-01-05"},
                {"ticker": "GOOG", "permaticker": 200, "name": "ALPHABET INC", "category": "Domestic Common Stock Secondary Class", "table": "SEP", "firstpricedate": "2020-01-01", "lastpricedate": "2020-01-05"},
            ]
        ),
        "SEP": pd.DataFrame(
            [
                {"ticker": ticker, "permaticker": perma, "date": date, "open": price, "high": price + 1, "low": price - 1, "close": price, "closeadj": price, "volume": 1000}
                for ticker, perma, price in (("GOOGL", 100, 10.0), ("GOOG", 200, 20.0))
                for date in dates
            ]
        ),
        "SF1": pd.DataFrame([{"permaticker": 100, "datekey": "2020-01-02", "dimension": "ARQ", "netinc": 1, "assets": 2, "revenue": 3}]),
        "ACTIONS": pd.DataFrame(columns=["permaticker", "date", "action"]),
        "DAILY": pd.DataFrame(columns=["permaticker", "date", "marketcap"]),
    }
    root = tmp_path / "share_class"
    build_store_from_frames(root, tables)
    store = SharadarStore(root)

    readiness = classify_readiness(store, [200], ("2020-01-01", "2020-01-05"))

    assert readiness.data_readiness == "survivorship_free"
    assert readiness.missing_pit == ()


def test_delisted_name_price_history_stops_at_action_date_without_terminal_injection(tmp_path) -> None:
    store = _build_store(tmp_path / "sharadar")

    prices = store.get_prices([3], "2020-01-01", "2020-01-10")[3]

    assert not prices.empty
    assert prices.index.max() == pd.Timestamp("2020-01-03")
    assert prices.loc[pd.Timestamp("2020-01-03"), "price"] == 18.0


def test_terminal_value_source_order(tmp_path) -> None:
    dates = _dates("2020-01-01", 3)
    tables = {
        "TICKERS": pd.DataFrame(
            [
                {"ticker": "TAKE", "permaticker": 10, "name": "TAKE", "category": "Domestic Common Stock", "table": "SEP", "isdelisted": "Y", "firstpricedate": "2020-01-01", "lastpricedate": "2020-01-03"},
                {"ticker": "FAIL", "permaticker": 20, "name": "FAIL", "category": "Domestic Common Stock", "table": "SEP", "isdelisted": "Y", "firstpricedate": "2020-01-01", "lastpricedate": "2020-01-03"},
            ]
        ),
        "SEP": pd.DataFrame(
            [
                {"ticker": ticker, "permaticker": perma, "date": date, "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0, "closeadj": 10.0, "volume": 1000}
                for ticker, perma in (("TAKE", 10), ("FAIL", 20))
                for date in dates
            ]
        ),
        "SF1": pd.DataFrame(columns=["permaticker", "datekey", "dimension", "netinc", "assets", "revenue"]),
        "ACTIONS": pd.DataFrame(
            [
                {"ticker": "TAKE", "permaticker": 10, "date": "2020-01-03", "action": "acquisition", "terminal_value": 12.5},
                {"ticker": "FAIL", "permaticker": 20, "date": "2020-01-03", "action": "bankruptcy"},
            ]
        ),
        "DAILY": pd.DataFrame(columns=["permaticker", "date", "marketcap"]),
    }
    root = tmp_path / "terminal_source_order"
    build_store_from_frames(root, tables)
    store = SharadarStore(root)

    events = store.terminal_value_events([10, 20], end="2020-01-10")

    assert events[10].value == 10.0
    assert events[10].source == "acquisition_last_price"
    assert events[20].value == 0.0
    assert events[20].source == "actions_failure_default_zero"


def test_unknown_healthy_residual_uses_last_price_and_completes_terminal_coverage(tmp_path) -> None:
    dates = _dates("2020-01-01", 3)
    tables = {
        "TICKERS": pd.DataFrame(
            [
                {"ticker": "AMBIG", "permaticker": 30, "name": "AMBIG", "category": "Domestic Common Stock", "table": "SEP", "isdelisted": "Y", "firstpricedate": "2020-01-01", "lastpricedate": "2020-01-03"},
                {"ticker": "AMBIG", "permaticker": 30, "name": "AMBIG", "category": "Domestic Common Stock", "table": "SF1", "isdelisted": "Y", "firstpricedate": "2020-01-01", "lastpricedate": "2020-01-03"},
            ]
        ),
        "SEP": pd.DataFrame(
            [{"ticker": "AMBIG", "permaticker": 30, "date": date, "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0, "closeadj": 10.0, "volume": 1000} for date in dates]
        ),
        "SF1": pd.DataFrame([{"permaticker": 30, "datekey": "2020-01-02", "dimension": "ARQ", "netinc": 1, "assets": 2, "revenue": 3}]),
        "ACTIONS": pd.DataFrame(columns=["permaticker", "ticker", "date", "action", "value"]),
        "DAILY": pd.DataFrame(columns=["permaticker", "date", "marketcap"]),
    }
    root = tmp_path / "missing_terminal"
    build_store_from_frames(root, tables)
    store = SharadarStore(root)

    readiness = classify_readiness(store, ["AMBIG"], ("2020-01-01", "2020-01-10"))
    events = store.terminal_value_events([30], end="2020-01-10")

    assert readiness.data_readiness == "survivorship_free"
    assert readiness.price_coverage_ratio == 1.0
    assert readiness.terminal_coverage_ratio == 1.0
    assert readiness.missing_terminal == ()
    assert events[30].value == 10.0
    assert events[30].source == "unknown_healthy_last_price"
    assert events[30].requires_human_review is True


def test_missing_fundamentals_is_documented_exception(tmp_path) -> None:
    dates = _dates("2020-01-01", 3)
    tables = {
        "TICKERS": pd.DataFrame(
            [{"ticker": "FAIL", "permaticker": 40, "name": "FAIL", "category": "Domestic Common Stock", "table": "SEP", "isdelisted": "Y", "firstpricedate": "2020-01-01", "lastpricedate": "2020-01-03"}]
        ),
        "SEP": pd.DataFrame(
            [{"ticker": "FAIL", "permaticker": 40, "date": date, "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0, "closeadj": 10.0, "volume": 1000} for date in dates]
        ),
        "SF1": pd.DataFrame(columns=["permaticker", "datekey", "dimension", "netinc", "assets", "revenue"]),
        "ACTIONS": pd.DataFrame([{"ticker": "FAIL", "permaticker": 40, "date": "2020-01-03", "action": "bankruptcy"}]),
        "DAILY": pd.DataFrame(columns=["permaticker", "date", "marketcap"]),
    }
    root = tmp_path / "missing_fundamentals_exception"
    build_store_from_frames(root, tables)
    store = SharadarStore(root)

    readiness = classify_readiness(store, ["FAIL"], ("2020-01-01", "2020-01-10"))

    assert readiness.data_readiness == "survivorship_free"
    assert readiness.price_coverage_ratio == 1.0
    assert readiness.terminal_coverage_ratio == 1.0
    assert readiness.pit_fundamental_coverage_ratio == 0.0
    assert readiness.fundamental_exceptions == ("FAIL",)
    assert "missing_point_in_time_fundamentals_documented_exception" in readiness.reasons


def test_missing_pit_quality_fails_closed(tmp_path) -> None:
    store = _build_store(tmp_path / "sharadar")
    provider = SharadarFundamentalsProvider(store)
    loader = SharadarFrameLoader(store, fundamentals_provider=provider)

    signal = provider.quality_for_ticker("MISS", "2020-01-05")
    frame = loader("MISS", "2020-01-01", "2020-01-10")

    assert signal.status == "UNAVAILABLE"
    assert signal.quality_gate_pass is False
    assert set(frame["quality_signal_status"]) == {"UNAVAILABLE"}
    assert frame["quality_gate_pass"].eq(False).all()
    assert ccel._quality_fails("MISS", pd.Timestamp("2020-01-05"), {"MISS": frame}) is True


def test_readiness_blocks_certification_until_survivorship_free(tmp_path) -> None:
    store = _build_store(tmp_path / "sharadar")

    partial = classify_readiness(store, ["AAA", "UNKNOWN"], ("2020-01-01", "2020-01-10"))
    ready = classify_readiness(store, ["SPY", "QQQ"], ("2020-01-01", "2020-01-10"))

    assert partial.data_readiness == "partial_pit"
    assert certification_gate_status(partial) == "research_only_not_certifiable"
    assert ready.data_readiness == "survivorship_free"
    assert certification_gate_status(ready) == "certifiable"


def test_snapshot_hash_is_deterministic_and_stamped_on_payload(tmp_path) -> None:
    store_a = _build_store(tmp_path / "a")
    store_b = _build_store(tmp_path / "b")

    assert store_a.data_snapshot_hash == store_b.data_snapshot_hash
    readiness = classify_readiness(store_a, ["SPY", "QQQ"], ("2020-01-01", "2020-01-10")).to_dict()
    stamped = ccel._stamp_data_layer(
        {"schema": "test"},
        data_source="sharadar",
        readiness=readiness,
        snapshot_hash=store_a.data_snapshot_hash,
    )

    assert stamped["data_source"] == "sharadar"
    assert stamped["data_snapshot_hash"] == store_a.data_snapshot_hash
    assert stamped["data_readiness"] == "survivorship_free"


def test_ingestion_never_persists_api_key(tmp_path, monkeypatch) -> None:
    secret = "sharadar-secret-should-not-appear"
    store_dir = tmp_path / "sharadar"
    monkeypatch.setenv("NASDAQ_DATA_LINK_API_KEY", secret)
    build_store_from_frames(store_dir, _fixture_tables(), optional_errors={"SP500": "not subscribed"})

    for path in store_dir.rglob("*"):
        if path.is_file():
            assert secret.encode("utf-8") not in path.read_bytes()

    monkeypatch.delenv("NASDAQ_DATA_LINK_API_KEY")
    existing = ingest_sharadar(root=store_dir, tables=["TICKERS"])
    assert existing["data_snapshot_hash"]
    try:
        ingest_sharadar(root=tmp_path / "missing_key", tables=["TICKERS"])
    except Exception as exc:
        assert "NASDAQ_DATA_LINK_API_KEY" in str(exc)
    else:
        raise AssertionError("ingest_sharadar should require NASDAQ_DATA_LINK_API_KEY")

    manifest = json.loads((store_dir / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["optional_errors"] == {"SP500": "not subscribed"}
