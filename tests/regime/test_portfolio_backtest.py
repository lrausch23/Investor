from __future__ import annotations

import math

import pandas as pd

from src.regime.portfolio_backtest import PortfolioBacktestConfig, control_specs, run_portfolio_backtest
from src.regime.strategy import StrategySpec


def _frame(prices: list[float], opens: list[float] | None = None) -> pd.DataFrame:
    dates = pd.bdate_range("2024-01-01", periods=len(prices))
    opens = opens or prices
    return pd.DataFrame({"open": opens, "high": prices, "low": prices, "price": prices, "volume": 1_000_000}, index=dates)


def test_portfolio_engine_t_plus_one_fills_and_accounting() -> None:
    frames = {
        "AAA": _frame([10, 11, 12, 13, 14], opens=[10, 21, 12, 13, 14]),
        "BBB": _frame([20, 20, 20, 20, 20], opens=[20, 20, 20, 20, 20]),
    }
    spec = StrategySpec(name="l0")
    result = run_portfolio_backtest(frames, spec, PortfolioBacktestConfig(starting_cash=1_000, entry_cost_bps=5, exit_cost_bps=5))
    first_trade = result.trades[0]
    assert first_trade["decision_date"] == "2024-01-01"
    assert first_trade["date"] == "2024-01-02"
    assert first_trade["price"] in {20.0, 21.0}
    for row in result.equity_curve:
        assert math.isclose(float(row["cash"]) + float(row["position_value"]), float(row["equity"]), abs_tol=0.01)
    assert float(result.metrics["total_costs_paid"]) == sum(float(row["costs_paid"]) for row in result.trades)
    assert float(result.metrics["annualized_turnover"]) >= 0.0


def test_costs_reconcile_against_hand_computed_fixture() -> None:
    frames = {"AAA": _frame([10, 10, 10])}
    config = PortfolioBacktestConfig(starting_cash=1_000, entry_cost_bps=10, exit_cost_bps=10)
    result = run_portfolio_backtest(frames, StrategySpec(name="single"), config)
    buy = next(row for row in result.trades if row["side"] == "Buy")
    assert buy["date"] == "2024-01-02"
    assert buy["quantity"] == 99
    assert math.isclose(float(buy["costs_paid"]), 0.99, abs_tol=1e-9)


def test_panel_availability_admits_late_ticker_without_lookahead() -> None:
    early = _frame([10, 10, 11, 11, 12, 12])
    late = _frame([20, 21, 22]).copy()
    late.index = early.index[3:]
    frames = {"AAA": early, "BBB": late}

    common = run_portfolio_backtest(frames, StrategySpec(name="common"), PortfolioBacktestConfig(starting_cash=1_000))
    panel = run_portfolio_backtest(
        frames,
        StrategySpec(name="panel"),
        PortfolioBacktestConfig(starting_cash=1_000, availability_mode="panel"),
    )

    assert common.equity_curve[0]["date"] == "2024-01-04"
    assert panel.equity_curve[0]["date"] == "2024-01-01"
    bbb_buys = [row for row in panel.trades if row["ticker"] == "BBB" and row["side"] == "Buy"]
    assert bbb_buys
    assert all(pd.Timestamp(row["date"]) >= late.index.min() for row in bbb_buys)
    assert any(int(row["active_ticker_count"]) == 1 for row in panel.equity_curve)
    assert any(int(row["active_ticker_count"]) == 2 for row in panel.equity_curve)


def test_no_lookahead_future_poison_column_does_not_enter_results() -> None:
    frame = _frame([10, 11, 12, 13, 14, 15])
    frame["future_poison"] = float("nan")
    result = run_portfolio_backtest({"AAA": frame}, StrategySpec(name="poison"), PortfolioBacktestConfig(starting_cash=1_000))
    assert all(math.isfinite(float(row["equity"])) for row in result.equity_curve)
    assert result.result_hash == run_portfolio_backtest({"AAA": frame}, StrategySpec(name="poison"), PortfolioBacktestConfig(starting_cash=1_000)).result_hash


def test_regime_brake_override_executes_without_scheduled_rebalance() -> None:
    frame = _frame([10, 10, 10, 10, 10, 10])
    frame["regime"] = ["Bull", "Bull", "Bear", "Bear", "Bear", "Bear"]
    spec = StrategySpec(name="brake", override_policy="regime_brake", override_params={"breadth_trigger": 1.0, "breadth_cap": 0.0})
    result = run_portfolio_backtest({"AAA": frame}, spec, PortfolioBacktestConfig(starting_cash=1_000))
    sell = [row for row in result.trades if row["side"] == "Sell"]
    assert sell
    assert sell[0]["decision_date"] == "2024-01-03"
    assert sell[0]["date"] == "2024-01-04"
    assert result.brake_log


def test_spy_200dma_control_is_normal_spec_with_confirmation() -> None:
    prices = [100.0] * 210 + [120.0] * 10
    frames = {"SPY": _frame(prices)}
    result = run_portfolio_backtest({"SPY": frames["SPY"]}, control_specs()["C2_spy_200dma"], PortfolioBacktestConfig(starting_cash=10_000))
    buys = [row for row in result.trades if row["side"] == "Buy"]
    assert buys
    # Confirmation happens at decision day T and fills at T+1 open.
    assert pd.Timestamp(buys[0]["date"]) > pd.Timestamp("2024-10-21")


def test_market_timing_brake_uses_shared_signal_without_spy_holding() -> None:
    frame = _frame([10.0] * 45)
    frame["market_timing_confirmed"] = False
    frame.loc[frame.index >= pd.Timestamp("2024-02-01"), "market_timing_confirmed"] = True
    spec = StrategySpec(name="spy-brake", override_policy="market_timing_brake", override_params={"cap": 0.0})

    result = run_portfolio_backtest({"AAA": frame}, spec, PortfolioBacktestConfig(starting_cash=1_000))

    assert result.brake_log
    buys = [row for row in result.trades if row["side"] == "Buy"]
    assert buys
    assert all(row["ticker"] == "AAA" for row in buys)
    assert pd.Timestamp(buys[0]["date"]) >= pd.Timestamp("2024-02-01")
