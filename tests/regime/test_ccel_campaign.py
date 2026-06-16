from __future__ import annotations

import json

import pandas as pd
import pytest

from src.regime import ccel_campaign as ccel


def _frame(start: str, prices: list[float]) -> pd.DataFrame:
    dates = pd.bdate_range(start, periods=len(prices))
    return pd.DataFrame({"open": prices, "high": prices, "low": prices, "price": prices, "volume": 1_000_000}, index=dates)


def _position_weight(lots: list[ccel.CCELLot], ticker: str, prices: dict[str, float], cash: float) -> float:
    value = sum(lot.quantity for lot in lots if lot.ticker == ticker) * float(prices[ticker])
    total = cash + ccel._position_value(lots, prices)
    return value / total if total > 0 else 0.0


def test_apply_wash_sales_disallows_same_ticker_replacement_loss() -> None:
    realizations = [
        {
            "date": "2020-01-15",
            "ticker": "AAA",
            "quantity": 10,
            "gain": -100.0,
            "term": "ST",
        }
    ]
    trades = [
        {"date": "2020-01-01", "ticker": "AAA", "side": "Buy", "quantity": 10, "price": 20.0},
        {"date": "2020-01-15", "ticker": "AAA", "side": "Sell", "quantity": 10, "price": 10.0},
        {"date": "2020-01-28", "ticker": "AAA", "side": "Buy", "quantity": 10, "price": 11.0},
    ]

    adjusted = ccel.apply_wash_sales(realizations, trades)

    assert adjusted[0]["wash_disallowed_loss"] == 100.0
    assert adjusted[0]["tax_gain"] == 0.0


def test_loss_harvest_default_redeploys_into_remaining_holdings() -> None:
    date = pd.Timestamp("2020-03-02")
    frames = {
        ticker: pd.DataFrame({"open": [price], "price": [price], "volume": [1_000_000]}, index=[date])
        for ticker, price in {"AAA": 10.0, "BBB": 10.0, "CCC": 10.0, "DDD": 10.0}.items()
    }
    lots = [
        ccel.CCELLot(1, "AAA", 10, 20.0, "2020-01-01"),
        ccel.CCELLot(2, "BBB", 5, 10.0, "2020-01-01"),
        ccel.CCELLot(3, "CCC", 5, 10.0, "2020-01-01"),
    ]

    pending = ccel._build_ccel_instruction(
        date=date,
        frames=frames,
        active=set(frames),
        lots=lots,
        cash=0.0,
        close_prices={"AAA": 10.0, "BBB": 11.0, "CCC": 11.0, "DDD": 10.0},
        no_rebuy_until={},
        pending_reentry={},
        cfg=ccel.CCELConfig(starting_cash=1_000.0, min_cash_to_deploy=1.0),
        first_day=False,
        is_month_start=True,
    )

    assert pending is not None
    assert pending.sell_tickers == {"AAA": "loss_harvest"}
    assert pending.buy_candidates == []
    assert pending.harvest_replacement_tickers == ["BBB", "CCC"]
    cash, _next_id, trades, _realized, _costs, _turnover = ccel._execute_ccel_instruction(
        date=date,
        pending=pending,
        lots=lots,
        cash=0.0,
        open_prices={"AAA": 10.0, "BBB": 10.0, "CCC": 10.0, "DDD": 10.0},
        no_rebuy_until={},
        pending_reentry={},
        cfg=ccel.CCELConfig(starting_cash=1_000.0, min_cash_to_deploy=1.0),
        next_lot_id=4,
    )

    buy_trades = [row for row in trades if row["side"] == "Buy"]
    assert {row["ticker"] for row in buy_trades} == {"BBB", "CCC"}
    assert all(row["exit_type"] == "harvest_exposure_neutral" for row in buy_trades)
    assert "DDD" not in {row["ticker"] for row in buy_trades}
    assert cash > 0


def test_harvest_restores_exposure_after_wash_window() -> None:
    harvest_date = pd.Timestamp("2020-03-02")
    reentry_date = harvest_date + pd.Timedelta(days=31)
    frames = {
        ticker: pd.DataFrame(
            {"open": [10.0, 10.0, 12.0], "price": [10.0, 10.0, 12.0], "volume": [1_000_000] * 3},
            index=[harvest_date, harvest_date + pd.Timedelta(days=15), reentry_date],
        )
        for ticker in ["AAA", "BBB", "CCC"]
    }
    lots = [
        ccel.CCELLot(1, "AAA", 10, 20.0, "2020-01-01"),
        ccel.CCELLot(2, "BBB", 5, 10.0, "2020-01-01"),
        ccel.CCELLot(3, "CCC", 5, 10.0, "2020-01-01"),
    ]
    no_rebuy_until: dict[str, pd.Timestamp] = {}
    pending_reentry: dict[str, dict] = {}
    cfg = ccel.CCELConfig(starting_cash=1_000.0, min_cash_to_deploy=1.0)

    pending = ccel._build_ccel_instruction(
        date=harvest_date,
        frames=frames,
        active=set(frames),
        lots=lots,
        cash=0.0,
        close_prices={"AAA": 10.0, "BBB": 10.0, "CCC": 10.0},
        no_rebuy_until=no_rebuy_until,
        pending_reentry=pending_reentry,
        cfg=cfg,
        first_day=False,
        is_month_start=True,
    )
    assert pending is not None
    assert pending.sell_tickers == {"AAA": "loss_harvest"}
    pre_harvest_weight = _position_weight(lots, "AAA", {"AAA": 10.0, "BBB": 10.0, "CCC": 10.0}, 0.0)

    cash, next_id, trades, realized, _costs, _turnover = ccel._execute_ccel_instruction(
        date=harvest_date,
        pending=pending,
        lots=lots,
        cash=0.0,
        open_prices={"AAA": 10.0, "BBB": 10.0, "CCC": 10.0},
        no_rebuy_until=no_rebuy_until,
        pending_reentry=pending_reentry,
        cfg=cfg,
        next_lot_id=4,
    )

    assert any(row["ticker"] == "AAA" and row["side"] == "Sell" and row["exit_type"] == "loss_harvest" for row in trades)
    assert no_rebuy_until["AAA"] == reentry_date
    assert pending_reentry["AAA"]["earliest_rebuy_date"] == "2020-04-02"
    assert "AAA" not in ccel._held_tickers(lots)

    wash_window_pending = ccel._build_ccel_instruction(
        date=harvest_date + pd.Timedelta(days=15),
        frames=frames,
        active=set(frames),
        lots=lots,
        cash=cash,
        close_prices={"AAA": 10.0, "BBB": 10.0, "CCC": 10.0},
        no_rebuy_until=no_rebuy_until,
        pending_reentry=pending_reentry,
        cfg=cfg,
        first_day=False,
        is_month_start=False,
    )
    assert wash_window_pending is None or "AAA" not in wash_window_pending.buy_candidates
    assert wash_window_pending is None or "AAA" not in wash_window_pending.harvest_reentry_targets
    assert "AAA" not in ccel._held_tickers(lots)

    pending = ccel._build_ccel_instruction(
        date=reentry_date,
        frames=frames,
        active=set(frames),
        lots=lots,
        cash=cash,
        close_prices={"AAA": 12.0, "BBB": 12.0, "CCC": 12.0},
        no_rebuy_until=no_rebuy_until,
        pending_reentry=pending_reentry,
        cfg=cfg,
        first_day=False,
        is_month_start=False,
    )
    assert pending is not None
    assert pending.harvest_reentry_targets == {"AAA": pre_harvest_weight}

    cash, _next_id, reentry_trades, reentry_realized, _costs, _turnover = ccel._execute_ccel_instruction(
        date=reentry_date,
        pending=pending,
        lots=lots,
        cash=cash,
        open_prices={"AAA": 12.0, "BBB": 12.0, "CCC": 12.0},
        no_rebuy_until=no_rebuy_until,
        pending_reentry=pending_reentry,
        cfg=cfg,
        next_lot_id=next_id,
    )
    trades.extend(reentry_trades)
    realized.extend(reentry_realized)

    assert any(row["ticker"] == "AAA" and row["side"] == "Buy" and row["exit_type"] == "harvest_reentry" for row in reentry_trades)
    assert "AAA" in ccel._held_tickers(lots)
    restored_weight = _position_weight(lots, "AAA", {"AAA": 12.0, "BBB": 12.0, "CCC": 12.0}, cash)
    assert restored_weight == pytest.approx(pre_harvest_weight, abs=0.15)
    assert "AAA" not in pending_reentry

    adjusted = ccel.apply_wash_sales(realized, trades)
    aaa_loss = next(row for row in adjusted if row["ticker"] == "AAA" and row["exit_reason"] == "loss_harvest")
    assert aaa_loss["wash_disallowed_loss"] == 0.0


def test_harvest_is_pretax_neutral() -> None:
    days = 520
    aaa_prices = []
    for idx in range(days):
        if idx < 42:
            price = 100.0
        elif idx < 90:
            price = 86.0
        else:
            price = 86.0 + (idx - 90) * (94.0 / (days - 90))
        aaa_prices.append(price)
    frames = {
        "AAA": _frame("2020-01-02", aaa_prices),
        "BBB": _frame("2020-01-02", [100.0 + idx * (25.0 / days) for idx in range(days)]),
        "CCC": _frame("2020-01-02", [100.0 + idx * (20.0 / days) for idx in range(days)]),
        "DDD": _frame("2020-01-02", [100.0 + idx * (18.0 / days) for idx in range(days)]),
    }
    base_cfg = dict(
        starting_cash=100_000.0,
        min_cash_to_deploy=25.0,
        probation_enabled=False,
        core_only=True,
        oos_start="2021-01-01",
    )

    harvest_on = ccel.run_ccel_backtest(frames, ccel.CCELConfig(**base_cfg, harvest_enabled=True))
    harvest_off = ccel.run_ccel_backtest(frames, ccel.CCELConfig(**base_cfg, harvest_enabled=False))

    on_terminal = float(harvest_on["metrics"]["pre_tax_terminal_wealth"])
    off_terminal = float(harvest_off["metrics"]["pre_tax_terminal_wealth"])
    assert abs(on_terminal - off_terminal) / off_terminal <= 0.10


def test_ccel_core_no_harvest_does_not_trim_winner() -> None:
    frames = {
        "AAA": _frame("2020-01-02", [10.0 + i * 0.05 for i in range(520)]),
        "BBB": _frame("2020-01-02", [20.0 + i * 0.01 for i in range(520)]),
    }
    payload = ccel.run_ccel_backtest(
        frames,
        ccel.CCELConfig(core_only=True, probation_enabled=False, harvest_enabled=False, oos_start="2021-01-01"),
    )

    sells = [row for row in payload["trades"] if row["side"] == "Sell"]
    assert sells == []
    assert payload["metrics"]["trade_count"] == 2
    assert payload["proxy_label"] == ccel.PROXY_LABEL


def test_ccel_campaign_runs_research_proxy_and_report(tmp_path) -> None:
    basket_path = tmp_path / "basket.json"
    basket_path.write_text(
        json.dumps(
            {
                "tickers": ["AAA", "BBB"],
                "basket_size": 2,
                "selected": [
                    {"ticker": "AAA", "sector": "Technology"},
                    {"ticker": "BBB", "sector": "Industrials"},
                ],
            }
        ),
        encoding="utf-8",
    )
    frames = {
        "AAA": _frame("2020-01-02", [10.0 + i * 0.03 for i in range(520)]),
        "BBB": _frame("2020-01-02", [20.0 - i * 0.005 for i in range(520)]),
        "SPY": _frame("2020-01-02", [100.0 + i * 0.02 for i in range(520)]),
        "QQQ": _frame("2020-01-02", [120.0 + i * 0.03 for i in range(520)]),
    }

    summary = ccel.run_ccel_campaign(
        basket_path=basket_path,
        campaign_dir=tmp_path / "campaign",
        report_dir=tmp_path / "report",
        start="2020-01-01",
        end="2021-12-31",
        oos_start="2021-01-01",
        frame_loader=lambda ticker, start, end: frames[str(ticker).upper()],
        regime_enricher=lambda payload: payload,
    )

    assert summary["schema"] == "regime_ccel_v1a_campaign.v1"
    assert summary["production_defaults_changed"] is False
    assert summary["proxy_label"] == ccel.PROXY_LABEL
    arms = {row["arm"] for row in summary["rows"]}
    assert {"CCEL_v1a", "CCEL_no_harvest", "CCEL_core_only", "L1", "Campaign3_winner", "SPY_buy_hold", "QQQ_buy_hold"} <= arms
    spy_row = next(row for row in summary["rows"] if row["arm"] == "SPY_buy_hold")
    assert 0.0 < spy_row["annualized_turnover"] < 1.0
    assert (tmp_path / "campaign" / "summary.json").exists()
    report_path = tmp_path / "report" / "management_report.html"
    assert report_path.exists()
    report_html = report_path.read_text(encoding="utf-8")
    assert ccel.PROXY_LABEL in report_html
    assert "Ablations" in report_html
    assert (tmp_path / "report" / "assets" / "ccel_total_return.png").exists()
