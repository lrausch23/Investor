from __future__ import annotations

from dataclasses import dataclass
from math import sqrt
from typing import Any
import logging

import pandas as pd

from .data import download_market_frame
from .hmm_engine import fit_regime_model
from .logging_config import setup_regime_logging
from .signals import build_composite_signal, compute_technicals, forward_regime_curve, intra_regime_signal, signal_from_forward_curve

setup_regime_logging()
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RegimeConditionalStats:
    regime: str
    probability_bucket: str
    entry_count: int
    avg_return_5d: float | None
    avg_return_21d: float | None
    win_rate_21d: float | None
    avg_duration_days: float | None


@dataclass(frozen=True)
class BacktestResult:
    trades: list[dict[str, Any]]
    total_return: float
    annualized_return: float
    max_drawdown: float
    sharpe_ratio: float | None
    win_rate: float | None
    avg_win: float | None
    avg_loss: float | None
    profit_factor: float | None
    buy_and_hold_return: float
    equity_curve: list[dict[str, Any]]
    oos_total_return: float | None = None
    oos_sharpe_ratio: float | None = None
    oos_win_rate: float | None = None
    regime_conditional: list[dict[str, Any]] | None = None


def _returns_summary(returns: list[float]) -> tuple[float | None, float | None]:
    if not returns:
        return None, None
    std = pd.Series(returns).std()
    sharpe = None
    if std and std > 0:
        sharpe = pd.Series(returns).mean() / std * sqrt(max(1, len(returns)))
    win_rate = len([value for value in returns if value > 0]) / len(returns)
    return float(sharpe) if sharpe is not None else None, float(win_rate)


def _probability_bucket(probability: float) -> str:
    if probability >= 0.80:
        return "high"
    if probability >= 0.50:
        return "medium"
    return "low"


def run_backtest(ticker: str, period: str = "5y", refit_step: int = 21, oos_fraction: float = 0.2) -> BacktestResult:
    logger.info("Running backtest for %s period=%s refit_step=%d oos_fraction=%.2f", ticker, period, refit_step, oos_fraction)
    market = download_market_frame(ticker=ticker, period=period, interval="1d").frame
    prices = market["price"].astype(float)
    technicals = compute_technicals(
        market["price"],
        market["volume"],
        market["high"] if "high" in market.columns else None,
        market["low"] if "low" in market.columns else None,
    )
    strategy_returns: list[float] = []
    trades: list[dict[str, Any]] = []
    in_position = False
    entry_price = None
    entry_date = None
    equity = 1.0
    curve = []
    training_window = min(120, max(90, len(market) // 6))
    start = min(len(market) - 1, max(160, training_window, refit_step))
    oos_start_index = max(start, int(len(market) * (1.0 - max(0.0, min(0.5, oos_fraction)))))
    oos_returns: list[float] = []
    regime_entries: list[dict[str, Any]] = []
    for idx in range(start, len(market), refit_step):
        window = market.iloc[: idx + 1]
        regime = fit_regime_model(
            ticker=ticker,
            market_frame=window,
            training_window=training_window,
            refit_step=refit_step,
        )
        forward_curve = forward_regime_curve(regime.transition_matrix, regime.latest_state_vector, horizon=21)
        forward_signal = signal_from_forward_curve(
            forward_curve,
            regime.latest_label,
            regime.transition_risk,
            regime.expected_regime_duration,
            regime.latest_probability,
        )
        technical_slice = technicals.iloc[: idx + 1].dropna()
        if technical_slice.empty:
            continue
        technical_signal = intra_regime_signal(technical_slice, regime.latest_label)
        composite = build_composite_signal(regime.latest_label, regime.latest_probability, forward_signal, technical_signal)
        date = window.index[-1]
        price = float(prices.iloc[idx])
        regime_entries.append(
            {
                "date": str(date.date()),
                "regime": regime.latest_label,
                "probability": float(regime.latest_probability),
                "price": price,
                "duration_days": float(regime.expected_regime_duration),
            }
        )
        if composite.composite_action in {"Strong Buy", "Buy"} and not in_position:
            in_position = True
            entry_price = price
            entry_date = date
        elif composite.composite_action in {"Sell", "Strong Sell"} and in_position and entry_price is not None:
            trade_return = (price - entry_price) / entry_price
            trades.append({"entry_date": str(entry_date.date()), "exit_date": str(date.date()), "entry_price": entry_price, "exit_price": price, "return": trade_return})
            strategy_returns.append(trade_return)
            if idx >= oos_start_index:
                oos_returns.append(trade_return)
            equity *= 1.0 + trade_return
            in_position = False
            entry_price = None
            entry_date = None
        curve.append({"date": str(date.date()), "equity": equity, "signal": composite.composite_action})
    if in_position and entry_price is not None:
        final_price = float(prices.iloc[-1])
        trade_return = (final_price - entry_price) / entry_price
        trades.append({"entry_date": str(entry_date.date()), "exit_date": str(prices.index[-1].date()), "entry_price": entry_price, "exit_price": final_price, "return": trade_return})
        strategy_returns.append(trade_return)
        if len(prices) - 1 >= oos_start_index:
            oos_returns.append(trade_return)
        equity *= 1.0 + trade_return
        curve.append({"date": str(prices.index[-1].date()), "equity": equity, "signal": "Close"})
    wins = [value for value in strategy_returns if value > 0]
    losses = [value for value in strategy_returns if value < 0]
    cumulative = pd.Series([1.0] + [item["equity"] for item in curve], dtype=float)
    rolling_peak = cumulative.cummax()
    drawdown = ((cumulative / rolling_peak) - 1.0).min() if not cumulative.empty else 0.0
    total_days = max(1, len(prices))
    annualized = equity ** (252 / total_days) - 1 if equity > 0 else -1.0
    sharpe, _ = _returns_summary(strategy_returns)
    oos_sharpe, oos_win_rate = _returns_summary(oos_returns)
    oos_total_return = None
    if oos_returns:
        oos_equity = 1.0
        for value in oos_returns:
            oos_equity *= 1.0 + value
        oos_total_return = oos_equity - 1.0
    buy_hold = float(prices.iloc[-1] / prices.iloc[0] - 1.0)
    for index, entry in enumerate(regime_entries):
        price = float(entry["price"])
        if index + 5 < len(regime_entries):
            forward_5 = float(regime_entries[index + 5]["price"])
            entry["return_5d"] = (forward_5 / price) - 1.0 if price else None
        else:
            entry["return_5d"] = None
        if index + 21 < len(regime_entries):
            forward_21 = float(regime_entries[index + 21]["price"])
            entry["return_21d"] = (forward_21 / price) - 1.0 if price else None
        else:
            entry["return_21d"] = None
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for entry in regime_entries:
        grouped.setdefault((str(entry["regime"]), _probability_bucket(float(entry["probability"]))), []).append(entry)
    regime_conditional = [
        {
            "regime": regime,
            "probability_bucket": bucket,
            "entry_count": len(entries),
            "avg_return_5d": float(pd.Series([item["return_5d"] for item in entries if item.get("return_5d") is not None]).mean()) if any(item.get("return_5d") is not None for item in entries) else None,
            "avg_return_21d": float(pd.Series([item["return_21d"] for item in entries if item.get("return_21d") is not None]).mean()) if any(item.get("return_21d") is not None for item in entries) else None,
            "win_rate_21d": float(pd.Series([1.0 if float(item["return_21d"]) > 0 else 0.0 for item in entries if item.get("return_21d") is not None]).mean()) if any(item.get("return_21d") is not None for item in entries) else None,
            "avg_duration_days": float(pd.Series([item["duration_days"] for item in entries]).mean()) if entries else None,
        }
        for (regime, bucket), entries in sorted(grouped.items())
    ]
    return BacktestResult(
        trades=trades,
        total_return=equity - 1.0,
        annualized_return=annualized,
        max_drawdown=float(drawdown),
        sharpe_ratio=float(sharpe) if sharpe is not None else None,
        win_rate=(len(wins) / len(strategy_returns)) if strategy_returns else None,
        avg_win=(sum(wins) / len(wins)) if wins else None,
        avg_loss=(sum(losses) / len(losses)) if losses else None,
        profit_factor=(sum(wins) / abs(sum(losses))) if wins and losses and sum(losses) != 0 else None,
        buy_and_hold_return=buy_hold,
        equity_curve=curve,
        oos_total_return=float(oos_total_return) if oos_total_return is not None else None,
        oos_sharpe_ratio=oos_sharpe,
        oos_win_rate=oos_win_rate,
        regime_conditional=regime_conditional,
    )


def compare_to_benchmark(backtest_result: BacktestResult, benchmark_ticker: str = "SPY", period: str = "5y") -> dict[str, Any]:
    benchmark = download_market_frame(ticker=benchmark_ticker, period=period, interval="1d").frame
    benchmark_return = float(benchmark["price"].iloc[-1] / benchmark["price"].iloc[0] - 1.0)
    alpha = backtest_result.total_return - benchmark_return
    return {
        "benchmark_ticker": benchmark_ticker,
        "benchmark_return": benchmark_return,
        "alpha": alpha,
        "beta": None,
        "information_ratio": (alpha / abs(benchmark_return)) if benchmark_return else None,
    }
