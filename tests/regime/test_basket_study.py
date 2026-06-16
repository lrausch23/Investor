from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from src.regime.basket_study import (
    BasketStudyConfig,
    basket_study_gate_status,
    reconstitute_holdings,
    run_basket_study,
    run_basket_arm,
    select_basket_asof,
    survivorship_bias_delta,
    terminal_value_disclosure_summary,
    validate_edgar_sample,
)
from src.regime.sharadar.ingest import build_store_from_frames
from src.regime.sharadar.readiness import classify_readiness
from src.regime.sharadar.store import SharadarStore


def _business_dates(start: str, end: str) -> list[pd.Timestamp]:
    return list(pd.date_range(start, end, freq="B"))


def _price_rows(permaticker: int, ticker: str, start: str, end: str, base: float, slope: float, *, stop: str | None = None) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    dates = _business_dates(start, stop or end)
    for idx, date in enumerate(dates):
        price = base + slope * idx
        rows.append(
            {
                "ticker": ticker,
                "permaticker": permaticker,
                "date": date.date().isoformat(),
                "open": price,
                "high": price * 1.01,
                "low": price * 0.99,
                "close": price,
                "closeadj": price,
                "volume": 2_000_000,
            }
        )
    return rows


def _dynamic_price_rows(
    permaticker: int,
    ticker: str,
    start: str,
    end: str,
    base: float,
    yearly_returns: dict[int, float],
    *,
    volume: int,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    price = float(base)
    for date in _business_dates(start, end):
        annual_return = float(yearly_returns.get(int(date.year), 0.03))
        daily_return = (1.0 + annual_return) ** (1.0 / 252.0) - 1.0
        price *= 1.0 + daily_return
        rows.append(
            {
                "ticker": ticker,
                "permaticker": permaticker,
                "date": date.date().isoformat(),
                "open": price,
                "high": price * 1.01,
                "low": price * 0.99,
                "close": price,
                "closeadj": price,
                "volume": volume,
            }
        )
    return rows


def _fixture_tables() -> dict[str, pd.DataFrame]:
    sep = []
    sep.extend(_price_rows(1, "AAA", "2019-01-02", "2021-03-31", 20, 0.01))
    sep.extend(_price_rows(2, "BBB", "2019-01-02", "2021-03-31", 18, 0.035))
    sep.extend(_price_rows(3, "DEL", "2019-01-02", "2021-03-31", 15, 0.05, stop="2021-01-15"))
    sep.extend(_price_rows(4, "CCC", "2019-01-02", "2021-03-31", 16, 0.02))
    ticker_rows = []
    for perma, ticker, last in ((1, "AAA", "2021-03-31"), (2, "BBB", "2021-03-31"), (3, "DEL", "2021-01-15"), (4, "CCC", "2021-03-31")):
        for table in ("SEP", "SF1"):
            ticker_rows.append(
                {
                    "table": table,
                    "permaticker": perma,
                    "ticker": ticker,
                    "name": ticker,
                    "exchange": "NYSE",
                    "isdelisted": "Y" if ticker == "DEL" else "N",
                    "category": "Domestic Common Stock",
                    "currency": "USD",
                    "location": "U.S.A",
                    "firstpricedate": "2019-01-02",
                    "lastpricedate": last,
                }
            )
    sf1 = [
        {"permaticker": 1, "datekey": "2019-06-30", "dimension": "ARQ", "revenue": 100, "assets": 100, "liabilities": 95, "ebit": -5, "taxexp": 0, "debt": 90, "equity": 10, "fcf": -2, "gp": 5, "ebitda": -1, "netinc": -4},
        {"permaticker": 1, "datekey": "2020-03-31", "dimension": "ARQ", "revenue": 100, "assets": 100, "liabilities": 10, "ebit": 40, "taxexp": 4, "debt": 10, "equity": 90, "fcf": 35, "gp": 70, "ebitda": 42, "netinc": 30},
        {"permaticker": 2, "datekey": "2019-06-30", "dimension": "ARQ", "revenue": 100, "assets": 100, "liabilities": 30, "ebit": 30, "taxexp": 3, "debt": 20, "equity": 80, "fcf": 20, "gp": 40, "ebitda": 35, "netinc": 22},
        {"permaticker": 3, "datekey": "2019-06-30", "dimension": "ARQ", "revenue": 100, "assets": 100, "liabilities": 35, "ebit": 28, "taxexp": 3, "debt": 25, "equity": 75, "fcf": 18, "gp": 38, "ebitda": 32, "netinc": 20},
        {"permaticker": 4, "datekey": "2019-06-30", "dimension": "ARQ", "revenue": 100, "assets": 100, "liabilities": 40, "ebit": 15, "taxexp": 2, "debt": 40, "equity": 60, "fcf": 10, "gp": 20, "ebitda": 20, "netinc": 10},
    ]
    daily = [
        {"permaticker": perma, "ticker": ticker, "date": date.date().isoformat(), "marketcap": cap, "ev": cap * 1.1, "evebitda": ev_ebitda, "pe": pe}
        for perma, ticker, cap, ev_ebitda, pe in ((1, "AAA", 2_000_000_000, 60, 80), (2, "BBB", 1_800_000_000, 10, 20), (3, "DEL", 1_500_000_000, 9, 18), (4, "CCC", 1_200_000_000, 12, 22))
        for date in pd.date_range("2019-06-30", "2021-01-05", freq="2QS")
    ]
    sp500 = pd.DataFrame(
        [
            {"date": "2019-01-02", "action": "added", "ticker": "AAA", "name": "AAA", "permaticker": 1},
            {"date": "2019-01-02", "action": "added", "ticker": "BBB", "name": "BBB", "permaticker": 2},
            {"date": "2019-01-02", "action": "added", "ticker": "DEL", "name": "DEL", "permaticker": 3},
            {"date": "2020-02-01", "action": "removed", "ticker": "DEL", "name": "DEL", "permaticker": 3},
            {"date": "2020-02-01", "action": "added", "ticker": "CCC", "name": "CCC", "permaticker": 4},
        ]
    )
    return {
        "TICKERS": pd.DataFrame(ticker_rows),
        "SEP": pd.DataFrame(sep),
        "SF1": pd.DataFrame(sf1),
        "DAILY": pd.DataFrame(daily),
        "SP500": sp500,
        "ACTIONS": pd.DataFrame([{"permaticker": 3, "ticker": "DEL", "date": "2021-01-15", "action": "delisted", "terminal_value": 0.0}]),
    }


def _quality_row(permaticker: int, datekey: str, *, quality: float) -> dict[str, object]:
    revenue = 1_000_000_000.0
    assets = 800_000_000.0
    liabilities = assets * max(0.10, 0.70 - quality)
    ebit = 80_000_000.0 + quality * 120_000_000.0
    fcf = 40_000_000.0 + quality * 100_000_000.0
    gp = 160_000_000.0 + quality * 160_000_000.0
    return {
        "permaticker": permaticker,
        "datekey": datekey,
        "dimension": "ARQ",
        "revenue": revenue,
        "assets": assets,
        "liabilities": liabilities,
        "ebit": ebit,
        "taxexp": 5_000_000.0,
        "debt": 100_000_000.0,
        "equity": assets - liabilities,
        "fcf": fcf,
        "gp": gp,
        "ebitda": ebit + 10_000_000.0,
        "netinc": ebit - 20_000_000.0,
    }


def _diverse_fixture_tables() -> dict[str, pd.DataFrame]:
    specs = [
        (10, "MOMO", 25.0, 1_900_000, {2017: 0.55, 2018: -0.05, 2019: 0.35, 2020: 0.08, 2021: -0.12, 2022: 0.25}, 0.95, 150.0),
        (11, "QUAL", 28.0, 1_300_000, {2017: 0.12, 2018: 0.18, 2019: 0.06, 2020: 0.20, 2021: 0.11, 2022: 0.09}, 0.95, 15.0),
        (12, "BLND", 18.0, 1_600_000, {2017: 0.22, 2018: 0.20, 2019: 0.22, 2020: 0.24, 2021: 0.18, 2022: 0.16}, 0.70, 20.0),
        (13, "VALU", 30.0, 1_100_000, {2017: 0.05, 2018: 0.45, 2019: -0.10, 2020: 0.30, 2021: 0.06, 2022: 0.35}, 0.35, 8.0),
        (14, "BIGG", 40.0, 2_400_000, {2017: 0.08, 2018: 0.02, 2019: 0.04, 2020: 0.03, 2021: 0.02, 2022: 0.01}, 0.20, 12.0),
        (15, "TURN", 16.0, 1_500_000, {2017: -0.08, 2018: 0.50, 2019: 0.01, 2020: -0.05, 2021: 0.45, 2022: 0.04}, 0.55, 18.0),
        (16, "CHEP", 22.0, 1_200_000, {2017: 0.18, 2018: 0.04, 2019: 0.18, 2020: 0.04, 2021: 0.18, 2022: 0.04}, 0.80, 6.0),
        (17, "RISK", 20.0, 1_000_000, {2017: 0.35, 2018: -0.20, 2019: 0.30, 2020: -0.18, 2021: 0.28, 2022: -0.15}, 0.05, 55.0),
    ]
    sep: list[dict[str, object]] = []
    ticker_rows: list[dict[str, object]] = []
    sf1: list[dict[str, object]] = []
    daily: list[dict[str, object]] = []
    sp500: list[dict[str, object]] = []
    for perma, ticker, base, volume, returns, quality, valuation in specs:
        sep.extend(_dynamic_price_rows(perma, ticker, "2017-01-03", "2023-12-29", base, returns, volume=volume))
        for table in ("SEP", "SF1"):
            ticker_rows.append(
                {
                    "table": table,
                    "permaticker": perma,
                    "ticker": ticker,
                    "name": ticker,
                    "exchange": "NYSE",
                    "isdelisted": "N",
                    "category": "Domestic Common Stock",
                    "currency": "USD",
                    "location": "U.S.A",
                    "firstpricedate": "2017-01-03",
                    "lastpricedate": "2023-12-29",
                }
            )
        for year in range(2017, 2024):
            sf1.append(_quality_row(perma, f"{year}-03-31", quality=quality))
            daily.append(
                {
                    "permaticker": perma,
                    "ticker": ticker,
                    "date": f"{year}-01-02",
                    "marketcap": 800_000_000 + volume * 1_000,
                    "ev": (800_000_000 + volume * 1_000) * 1.1,
                    "evebitda": valuation,
                    "pe": valuation * 1.7,
                    "pb": 2.0,
                }
            )
        sp500.append({"date": "2017-01-03", "action": "added", "ticker": ticker, "name": ticker, "permaticker": perma})
    return {
        "TICKERS": pd.DataFrame(ticker_rows),
        "SEP": pd.DataFrame(sep),
        "SF1": pd.DataFrame(sf1),
        "DAILY": pd.DataFrame(daily),
        "SP500": pd.DataFrame(sp500),
        "ACTIONS": pd.DataFrame(columns=["permaticker", "ticker", "date", "action"]),
    }


def _store(tmp_path: Path) -> SharadarStore:
    root = tmp_path / "sharadar"
    build_store_from_frames(root, _fixture_tables())
    return SharadarStore(root)


def _diverse_store(tmp_path: Path) -> SharadarStore:
    root = tmp_path / "sharadar_diverse"
    build_store_from_frames(root, _diverse_fixture_tables())
    return SharadarStore(root)


def _failed_name_store(tmp_path: Path) -> SharadarStore:
    dates = _business_dates("2020-01-02", "2020-03-10")
    tables = {
        "TICKERS": pd.DataFrame(
            [
                {"table": "SEP", "permaticker": 30, "ticker": "FAIL", "name": "FAIL", "exchange": "NYSE", "isdelisted": "Y", "category": "Domestic Common Stock", "currency": "USD", "location": "U.S.A", "firstpricedate": "2020-01-02", "lastpricedate": "2020-03-10"},
            ]
        ),
        "SEP": pd.DataFrame(
            [
                {
                    "ticker": "FAIL",
                    "permaticker": 30,
                    "date": date.date().isoformat(),
                    "open": 10.0,
                    "high": 10.0,
                    "low": 10.0,
                    "close": 10.0,
                    "closeadj": 10.0,
                    "volume": 2_000_000,
                }
                for date in dates
            ]
        ),
        "SF1": pd.DataFrame(columns=["permaticker", "datekey", "dimension", "revenue", "assets", "liabilities", "ebit", "taxexp", "debt", "equity", "fcf", "gp", "ebitda", "netinc"]),
        "DAILY": pd.DataFrame([{"permaticker": 30, "ticker": "FAIL", "date": "2020-01-02", "marketcap": 1_000_000_000, "ev": 1_000_000_000, "evebitda": 10, "pe": 12}]),
        "SP500": pd.DataFrame([{"date": "2020-01-02", "action": "added", "ticker": "FAIL", "name": "FAIL", "permaticker": 30}]),
        "ACTIONS": pd.DataFrame([{"permaticker": 30, "ticker": "FAIL", "date": "2020-03-12", "action": "bankruptcy"}]),
    }
    root = tmp_path / "failed_name"
    build_store_from_frames(root, tables)
    return SharadarStore(root)


def _cash_acquisition_store(tmp_path: Path) -> SharadarStore:
    dates = _business_dates("2020-01-02", "2020-03-10")
    tables = {
        "TICKERS": pd.DataFrame(
            [
                {"table": "SEP", "permaticker": 31, "ticker": "CASH", "name": "CASH", "exchange": "NYSE", "isdelisted": "Y", "category": "Domestic Common Stock", "currency": "USD", "location": "U.S.A", "firstpricedate": "2020-01-02", "lastpricedate": "2020-03-10"},
            ]
        ),
        "SEP": pd.DataFrame(
            [
                {
                    "ticker": "CASH",
                    "permaticker": 31,
                    "date": date.date().isoformat(),
                    "open": 10.0,
                    "high": 10.0,
                    "low": 10.0,
                    "close": 10.0,
                    "closeadj": 10.0,
                    "volume": 2_000_000,
                }
                for date in dates
            ]
        ),
        "SF1": pd.DataFrame(columns=["permaticker", "datekey", "dimension", "revenue", "assets", "liabilities", "ebit", "taxexp", "debt", "equity", "fcf", "gp", "ebitda", "netinc"]),
        "DAILY": pd.DataFrame([{"permaticker": 31, "ticker": "CASH", "date": "2020-01-02", "marketcap": 1_000_000_000, "ev": 1_000_000_000, "evebitda": 10, "pe": 12}]),
        "SP500": pd.DataFrame([{"date": "2020-01-02", "action": "added", "ticker": "CASH", "name": "CASH", "permaticker": 31}]),
        "ACTIONS": pd.DataFrame(
            [
                {
                    "permaticker": 31,
                    "ticker": "CASH",
                    "date": "2020-03-12",
                    "action": "cash acquisition",
                    "terminal_value": 12.5,
                    "value": 999.0,
                }
            ]
        ),
    }
    root = tmp_path / "cash_acquisition"
    build_store_from_frames(root, tables)
    return SharadarStore(root)


def _ambiguous_acquisition_store(tmp_path: Path) -> SharadarStore:
    dates = _business_dates("2020-01-02", "2020-03-10")
    tables = {
        "TICKERS": pd.DataFrame(
            [
                {"table": "SEP", "permaticker": 32, "ticker": "BUY", "name": "BUY", "exchange": "NYSE", "isdelisted": "Y", "category": "Domestic Common Stock", "currency": "USD", "location": "U.S.A", "firstpricedate": "2020-01-02", "lastpricedate": "2020-03-10"},
            ]
        ),
        "SEP": pd.DataFrame(
            [
                {
                    "ticker": "BUY",
                    "permaticker": 32,
                    "date": date.date().isoformat(),
                    "open": 10.0,
                    "high": 10.0,
                    "low": 10.0,
                    "close": 10.0,
                    "closeadj": 10.0,
                    "volume": 2_000_000,
                }
                for date in dates
            ]
        ),
        "SF1": pd.DataFrame(columns=["permaticker", "datekey", "dimension", "revenue", "assets", "liabilities", "ebit", "taxexp", "debt", "equity", "fcf", "gp", "ebitda", "netinc"]),
        "DAILY": pd.DataFrame([{"permaticker": 32, "ticker": "BUY", "date": "2020-01-02", "marketcap": 1_000_000_000, "ev": 1_000_000_000, "evebitda": 10, "pe": 12}]),
        "SP500": pd.DataFrame([{"date": "2020-01-02", "action": "added", "ticker": "BUY", "name": "BUY", "permaticker": 32}]),
        "ACTIONS": pd.DataFrame([{"permaticker": 32, "ticker": "BUY", "date": "2020-03-12", "action": "acquisitionby"}]),
    }
    root = tmp_path / "ambiguous_acquisition"
    build_store_from_frames(root, tables)
    return SharadarStore(root)


def _unknown_residual_store(tmp_path: Path) -> SharadarStore:
    dates = _business_dates("2020-01-02", "2020-03-10")
    sep_rows: list[dict[str, object]] = []
    for idx, date in enumerate(dates):
        sep_rows.append(
            {
                "ticker": "HEALTH",
                "permaticker": 33,
                "date": date.date().isoformat(),
                "open": 10.0,
                "high": 10.0,
                "low": 10.0,
                "close": 10.0,
                "closeadj": 10.0,
                "volume": 2_000_000,
            }
        )
        distressed_price = max(5.0, 100.0 - idx * 5.0)
        sep_rows.append(
            {
                "ticker": "DIST",
                "permaticker": 34,
                "date": date.date().isoformat(),
                "open": distressed_price,
                "high": distressed_price,
                "low": distressed_price,
                "close": distressed_price,
                "closeadj": distressed_price,
                "volume": 2_000_000,
            }
        )
    tables = {
        "TICKERS": pd.DataFrame(
            [
                {"table": "SEP", "permaticker": 33, "ticker": "HEALTH", "name": "HEALTH", "exchange": "NYSE", "isdelisted": "Y", "category": "Domestic Common Stock", "currency": "USD", "location": "U.S.A", "firstpricedate": "2020-01-02", "lastpricedate": "2020-03-10"},
                {"table": "SEP", "permaticker": 34, "ticker": "DIST", "name": "DIST", "exchange": "NYSE", "isdelisted": "Y", "category": "Domestic Common Stock", "currency": "USD", "location": "U.S.A", "firstpricedate": "2020-01-02", "lastpricedate": "2020-03-10"},
            ]
        ),
        "SEP": pd.DataFrame(sep_rows),
        "SF1": pd.DataFrame(
            [
                {"permaticker": 33, "datekey": "2020-02-28", "dimension": "ARQ", "revenue": 100, "assets": 200, "liabilities": 50, "equity": 150, "netinc": 10},
                {"permaticker": 34, "datekey": "2020-02-28", "dimension": "ARQ", "revenue": 10, "assets": 20, "liabilities": 80, "equity": -60, "netinc": -20},
            ]
        ),
        "DAILY": pd.DataFrame(
            [
                {"permaticker": 33, "ticker": "HEALTH", "date": "2020-01-02", "marketcap": 1_000_000_000, "ev": 1_000_000_000, "evebitda": 10, "pe": 12},
                {"permaticker": 34, "ticker": "DIST", "date": "2020-01-02", "marketcap": 1_000_000_000, "ev": 1_000_000_000, "evebitda": 10, "pe": 12},
            ]
        ),
        "SP500": pd.DataFrame(
            [
                {"date": "2020-01-02", "action": "added", "ticker": "HEALTH", "name": "HEALTH", "permaticker": 33},
                {"date": "2020-01-02", "action": "added", "ticker": "DIST", "name": "DIST", "permaticker": 34},
            ]
        ),
        "ACTIONS": pd.DataFrame(columns=["permaticker", "ticker", "date", "action"]),
    }
    root = tmp_path / "unknown_residual"
    build_store_from_frames(root, tables)
    return SharadarStore(root)


def _cfg(**updates) -> BasketStudyConfig:
    payload = BasketStudyConfig(
        basket_size=2,
        min_dollar_adv=0,
        min_marketcap=0,
        min_listing_days=20,
        dollar_adv_days=5,
        formation="6_1",
        oos_start="2021-01-01",
    ).to_dict()
    payload.update(updates)
    return BasketStudyConfig(**payload)


def test_selection_is_as_of_no_lookahead(tmp_path) -> None:
    store = _store(tmp_path)

    before = select_basket_asof(store, "A2_quality_momentum", "2020-01-02", _cfg())
    after = select_basket_asof(store, "A2_quality_momentum", "2020-06-30", _cfg())

    assert before
    assert before[0].ticker != "AAA"
    assert any(row.ticker == "AAA" for row in after)


def test_survivorship_free_selection_and_delist_flow(tmp_path) -> None:
    store = _store(tmp_path)
    cfg = _cfg(basket_size=3)

    selected = select_basket_asof(store, "A1_pure_momentum", "2020-01-02", cfg)
    payload = run_basket_arm(store, "A1_pure_momentum", cfg, start="2020-01-02", end="2021-03-31", basket={}, benchmark_curve=pd.DataFrame(), windows=[])

    assert any(row.ticker == "DEL" for row in selected)
    assert any(row.get("ticker") == "P3" for row in payload["trades"])
    assert payload["metrics"]["after_tax_terminal_wealth"] is not None
    assert payload["equity_curve"][0]["position_value"] > 0
    assert payload["equity_curve"][0]["exposure"] > 0


def test_failed_name_realizes_full_loss(tmp_path) -> None:
    store = _failed_name_store(tmp_path)
    cfg = _cfg(basket_size=1, min_listing_days=0, oos_start="2020-02-01")

    payload = run_basket_arm(
        store,
        "C0_static_basket",
        cfg,
        start="2020-01-02",
        end="2020-03-31",
        basket={"tickers": ["FAIL"]},
        benchmark_curve=pd.DataFrame(),
        windows=[],
    )

    terminal_sells = [row for row in payload["trades"] if row["side"] == "Sell" and row["exit_type"] == "terminal_value"]
    assert len(terminal_sells) == 1
    assert terminal_sells[0]["price"] == 0.0
    assert terminal_sells[0]["notional"] == 0.0
    assert payload["open_lots"] == []
    realized = [row for row in payload["realized_lots"] if row["ticker"] == "P30"]
    assert realized
    assert sum(float(row["gain"]) for row in realized) < -99_000
    terminal_curve = [row for row in payload["equity_curve"] if row["date"] == "2020-03-12"]
    assert terminal_curve
    assert terminal_curve[0]["position_value"] == 0.0
    assert payload["terminal_realizations"][0]["ticker"] == "P30"


def test_bankruptcy_terminal_zero(tmp_path) -> None:
    store = _failed_name_store(tmp_path)

    event = store.terminal_value_events([30], end="2020-03-31")[30]

    assert event.value == 0.0
    assert event.source == "actions_failure_default_zero"
    assert "bankruptcy" in event.reason.lower()


def test_cash_acquisition_terminal_price(tmp_path) -> None:
    store = _cash_acquisition_store(tmp_path)
    cfg = _cfg(basket_size=1, min_listing_days=0, oos_start="2020-02-01")

    event = store.terminal_value_events([31], end="2020-03-31")[31]
    payload = run_basket_arm(
        store,
        "C0_static_basket",
        cfg,
        start="2020-01-02",
        end="2020-03-31",
        basket={"tickers": ["CASH"]},
        benchmark_curve=pd.DataFrame(),
        windows=[],
    )

    terminal_sells = [row for row in payload["trades"] if row["side"] == "Sell" and row["exit_type"] == "terminal_value"]
    assert event.value == 10.0
    assert event.source == "acquisition_last_price"
    assert terminal_sells
    assert terminal_sells[0]["price"] == 10.0
    assert terminal_sells[0]["terminal_value_source"] == "acquisition_last_price"


def test_no_lastprice_for_failures(tmp_path) -> None:
    store = _failed_name_store(tmp_path)
    cfg = _cfg(basket_size=1, min_listing_days=0, oos_start="2020-02-01")

    payload = run_basket_arm(
        store,
        "C0_static_basket",
        cfg,
        start="2020-01-02",
        end="2020-03-31",
        basket={"tickers": ["FAIL"]},
        benchmark_curve=pd.DataFrame(),
        windows=[],
    )

    terminal_sells = [row for row in payload["trades"] if row["side"] == "Sell" and row["exit_type"] == "terminal_value"]
    assert terminal_sells
    assert terminal_sells[0]["price"] == 0.0
    assert terminal_sells[0]["price"] != 10.0
    assert terminal_sells[0]["terminal_value_source"] == "actions_failure_default_zero"


def test_unknown_residual_split_disclosed(tmp_path) -> None:
    store = _unknown_residual_store(tmp_path)
    events = store.terminal_value_events([33, 34], end="2020-03-31")

    rows = [
        {
            "ticker": "HEALTH",
            "permaticker": 33,
            "use_count": 0,
            "quantity": 0,
            "arms": [],
            "terminal_value": events[33].value,
            "terminal_value_source": events[33].source,
            "terminal_value_reason": events[33].reason,
            "requires_human_review": events[33].requires_human_review,
            "held": False,
        },
        {
            "ticker": "DIST",
            "permaticker": 34,
            "use_count": 0,
            "quantity": 0,
            "arms": [],
            "terminal_value": events[34].value,
            "terminal_value_source": events[34].source,
            "terminal_value_reason": events[34].reason,
            "requires_human_review": events[34].requires_human_review,
            "held": False,
        }
    ]
    disclosure = terminal_value_disclosure_summary(rows)

    assert events[33].value == 10.0
    assert events[33].source == "unknown_healthy_last_price"
    assert events[34].value == 0.0
    assert events[34].source == "unknown_distressed_zero"
    assert disclosure["unknown_residual_count"] == 2
    assert {row["ticker"] for row in disclosure["unknown_residuals"]} == {"HEALTH", "DIST"}


def test_terminal_coverage_complete(tmp_path) -> None:
    store = _ambiguous_acquisition_store(tmp_path)

    readiness = classify_readiness(store, ["BUY"], ("2020-01-02", "2020-03-31")).to_dict()

    assert readiness["terminal_coverage_ratio"] == 1.0
    assert readiness["data_readiness"] == "survivorship_free"
    assert readiness["missing_terminal"] == ()


def test_held_name_review_list(tmp_path) -> None:
    store = _unknown_residual_store(tmp_path)
    cfg = _cfg(basket_size=1, min_listing_days=0, oos_start="2020-02-01")
    payload = run_basket_arm(
        store,
        "C0_static_basket",
        cfg,
        start="2020-01-02",
        end="2020-03-31",
        basket={"tickers": ["HEALTH"]},
        benchmark_curve=pd.DataFrame(),
        windows=[],
    )
    terminal_event = payload["terminal_events"][0]

    basket_study_terminal_rows = [
        {
            "ticker": "HEALTH",
            "permaticker": 33,
            "use_count": 1,
            "quantity": 1,
            "arms": ["C0_static_basket"],
            "terminal_value": terminal_event["value"],
            "terminal_value_source": terminal_event["source"],
            "terminal_value_reason": terminal_event["reason"],
            "requires_human_review": terminal_event["requires_human_review"],
            "held": True,
        }
    ]
    rows = terminal_value_disclosure_summary(basket_study_terminal_rows)

    assert basket_study_terminal_rows[0]["requires_human_review"] is True
    assert rows["held_name_review_count"] == 1
    assert rows["held_name_review_list"][0]["ticker"] == "HEALTH"


def test_delisted_name_retained_not_blacklisted(tmp_path) -> None:
    store = _failed_name_store(tmp_path)
    cfg = _cfg(basket_size=1, min_listing_days=0)

    selected = select_basket_asof(store, "C0b_static_pit", "2020-02-03", cfg)
    universe = store.universe_asof("2020-02-03")

    assert 30 in universe
    assert any(row.permaticker == 30 for row in selected)


def test_reconstituted_arm_marks_at_real_prices(tmp_path) -> None:
    store = _diverse_store(tmp_path)
    cfg = _cfg(basket_size=3, min_marketcap=500_000_000, reconstitution="full_reselect")

    payload = run_basket_arm(
        store,
        "A1_pure_momentum",
        cfg,
        start="2018-01-02",
        end="2023-12-29",
        basket={},
        benchmark_curve=pd.DataFrame(),
        windows=[],
    )

    marked_rows = [row for row in payload["equity_curve"] if row["open_lot_count"] > 0]
    assert marked_rows
    assert all(row["position_value"] > 0 for row in marked_rows)
    assert all(row["exposure"] > 0 for row in marked_rows)
    assert all(row["zero_mark_count"] == 0 for row in marked_rows)
    assert all(row["unresolved_mark_count"] == 0 for row in marked_rows)
    assert payload["valuation_diagnostics"]["unresolved_mark_count"] == 0
    assert not [ticker for ticker in payload["valuation_diagnostics"]["unresolved_mark_tickers"] if str(ticker).startswith("P")]


def test_distinct_selection_arms_differ(tmp_path) -> None:
    store = _diverse_store(tmp_path)
    cfg = _cfg(basket_size=3, min_marketcap=500_000_000, reconstitution="full_reselect")
    arms = ["C0b_static_pit", "A1_pure_momentum", "A2_quality_momentum", "A3_momentum_valuation_cap", "A4_quality_momentum_valuation"]

    payloads = {
        arm: run_basket_arm(store, arm, cfg, start="2018-01-02", end="2023-12-29", basket={}, benchmark_curve=pd.DataFrame(), windows=[])
        for arm in arms
    }
    signatures = {
        arm: (
            round(float(payload["metrics"]["after_tax_terminal_wealth"]), 2),
            int(payload["metrics"]["trade_count"]),
            tuple(
                tuple(row["permaticker"] for row in rows[: cfg.basket_size])
                for rows in (payload["selection_history"] or {}).values()
            ),
        )
        for arm, payload in payloads.items()
    }

    assert len(set(signatures.values())) == len(arms)


def test_reconstituted_arm_non_degenerate(tmp_path) -> None:
    store = _diverse_store(tmp_path)
    cfg = _cfg(basket_size=3, min_marketcap=500_000_000, reconstitution="full_reselect")

    payload = run_basket_arm(
        store,
        "A4_quality_momentum_valuation",
        cfg,
        start="2018-01-02",
        end="2023-12-29",
        basket={},
        benchmark_curve=pd.DataFrame(),
        windows=[],
    )

    trade_dates = {row["date"] for row in payload["trades"]}
    exposures = [float(row["exposure"]) for row in payload["equity_curve"] if row["open_lot_count"] > 0]
    assert len(payload["trades"]) > 8
    assert len(trade_dates) >= 4
    assert exposures
    assert sum(1 for value in exposures if value > 0.50) / len(exposures) > 0.90


def test_reconstitution_drop_bottom_third() -> None:
    target = reconstitute_holdings(
        current={1, 2, 3},
        ranked=[4, 2, 1, 3],
        scores={1: 0.3, 2: 0.7, 3: -0.2, 4: 1.0},
        target_size=3,
        method="drop_bottom_third",
    )

    assert target == {1, 2, 4}


def test_survivorship_bias_delta_reported() -> None:
    delta = survivorship_bias_delta(
        [
            {"arm": "C0_static_basket", "after_tax_terminal_wealth": 150_000, "total_return": 0.5, "annualized_return": 0.10},
            {"arm": "C0b_static_pit", "after_tax_terminal_wealth": 120_000, "total_return": 0.2, "annualized_return": 0.04},
        ]
    )

    assert delta["after_tax_terminal_wealth_delta"] == 30_000
    assert delta["definition"] == "C0_static_basket minus C0b_static_pit"


def test_synth_sp500_sanity(tmp_path) -> None:
    store = _store(tmp_path)

    frame = store.synth_sp500_total_return("2020-01-02", "2020-12-31")
    total_return = float(frame["price"].iloc[-1] / frame["price"].iloc[0] - 1.0)

    assert total_return == pytest.approx(0.228, abs=0.015)
    assert frame["member_count"].min() >= 2


def test_synth_benchmark_survivorship_free(tmp_path) -> None:
    store = _store(tmp_path)

    before = store.sp500_membership_asof("2020-01-15")
    after = store.sp500_membership_asof("2020-03-02")

    assert 3 in before
    assert 4 not in before
    assert 3 not in after
    assert 4 in after


def test_basket_study_runs_without_sfp(tmp_path) -> None:
    store = _store(tmp_path)
    basket_path = tmp_path / "basket.json"
    basket_path.write_text(json.dumps({"tickers": ["AAA", "BBB", "CCC"]}), encoding="utf-8")

    summary = run_basket_study(
        basket_path=basket_path,
        campaign_dir=tmp_path / "campaign",
        report_dir=tmp_path / "report",
        store_dir=store.root,
        start="2020-01-02",
        end="2021-03-31",
        oos_start="2021-01-01",
        render_report=False,
        config=_cfg(),
    )

    assert "SFP" not in store.manifest()["tables"]
    assert "SPY_buy_hold" in {row["arm"] for row in summary["rows"]}
    assert "QQQ_buy_hold" not in {row["arm"] for row in summary["rows"]}
    assert summary["readiness"]["missing_price"] == ()
    assert summary["readiness"]["price_coverage_ratio"] == 1.0
    assert summary["production_defaults_changed"] is False


def test_certifiable_requires_edgar_pass(tmp_path) -> None:
    store = _store(tmp_path)
    store.refresh_terminal_value_defaults_artifact(end="2021-03-31")
    readiness = {"data_readiness": "survivorship_free", "data_snapshot_hash": store.data_snapshot_hash}

    missing = basket_study_gate_status(readiness, {"status": "MISSING"}, oos_start="2021-01-01")
    failed = validate_edgar_sample(store_dir=store.root, sample_size=2, allow_network=False)
    passed = validate_edgar_sample(
        store_dir=store.root,
        sample_size=2,
        validator=lambda _store, sample: [{"permaticker": perma, "status": "pass"} for perma in sample],
    )
    readiness = {"data_readiness": "survivorship_free", "data_snapshot_hash": SharadarStore(store.root).data_snapshot_hash}

    assert missing != "certifiable"
    assert failed["status"] == "FAIL"
    assert basket_study_gate_status(readiness, passed, oos_start="2021-01-01") == "certifiable"


def test_snapshot_hash_stamped(tmp_path) -> None:
    store = _store(tmp_path)
    artifact = validate_edgar_sample(
        store_dir=store.root,
        sample_size=2,
        validator=lambda _store, sample: [{"permaticker": perma, "status": "pass"} for perma in sample],
    )
    written = json.loads((store.root / "edgar_validation.json").read_text(encoding="utf-8"))

    assert artifact["data_snapshot_hash"] == store.data_snapshot_hash
    assert written["data_snapshot_hash"] == store.data_snapshot_hash
