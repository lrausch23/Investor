from __future__ import annotations

import datetime as dt
import hashlib
import json
import math
import subprocess
from dataclasses import asdict, dataclass, field
from typing import Any

import pandas as pd

from .pipeline_backtest import _normalize_market_frame as _normalize_pipeline_market_frame
from .pipeline_backtest import compute_equity_metrics
from .strategy import StrategySpec, build
from .strategy.interfaces import AllocationPolicy, ExposureOverride, ExposurePolicy, OverridePolicy, RebalancePolicy, SignalMap, SignalProvider
from .stress_windows import StressWindow, get_stress_windows


@dataclass(frozen=True)
class PortfolioBacktestConfig:
    starting_cash: float = 100_000.0
    entry_cost_bps: float = 5.0
    exit_cost_bps: float = 5.0
    oos_start: str | None = None
    integer_shares: bool = True
    risk_free_rate: float = 0.0
    availability_mode: str = "common"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class PortfolioPosition:
    quantity: float = 0.0
    avg_cost: float = 0.0


@dataclass(frozen=True)
class PortfolioBacktestResult:
    config: dict[str, Any]
    strategy_spec: dict[str, Any]
    strategy_hash: str
    git_sha: str
    metrics: dict[str, Any]
    in_sample: dict[str, Any]
    out_of_sample: dict[str, Any] | None
    equity_curve: list[dict[str, Any]]
    trades: list[dict[str, Any]]
    daily_exposure: list[dict[str, Any]]
    brake_log: list[dict[str, Any]]
    stress_windows: list[dict[str, Any]]
    result_hash: str = ""

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "config": self.config,
            "strategy_spec": self.strategy_spec,
            "strategy_hash": self.strategy_hash,
            "git_sha": self.git_sha,
            "metrics": self.metrics,
            "in_sample": self.in_sample,
            "out_of_sample": self.out_of_sample,
            "equity_curve": self.equity_curve,
            "trades": self.trades,
            "daily_exposure": self.daily_exposure,
            "brake_log": self.brake_log,
            "stress_windows": self.stress_windows,
            "result_hash": self.result_hash,
        }
        return payload

    def to_json(self, path: str | None = None) -> str:
        text = json.dumps(self.to_dict(), indent=2, sort_keys=True)
        if path:
            from pathlib import Path

            Path(path).write_text(text + "\n", encoding="utf-8")
        return text


@dataclass(frozen=True)
class _TargetInstruction:
    decision_date: str
    target_exposure: float
    target_weights: dict[str, float]
    override: dict[str, Any] | None
    reason: str


def run_portfolio_backtest(
    market_frames: dict[str, pd.DataFrame],
    spec: StrategySpec,
    config: PortfolioBacktestConfig | None = None,
    *,
    benchmark_curve: pd.DataFrame | None = None,
    windows: list[StressWindow] | None = None,
) -> PortfolioBacktestResult:
    cfg = config or PortfolioBacktestConfig()
    frames = {str(ticker).upper(): _normalize_portfolio_frame(frame) for ticker, frame in market_frames.items() if not frame.empty}
    frames = {ticker: frame for ticker, frame in frames.items() if not frame.empty}
    if not frames:
        raise ValueError("Portfolio backtest requires at least one non-empty market frame.")

    signal_provider = build("signal", spec.signal_provider, spec.signal_params)
    exposure_policy = build("exposure", spec.exposure_policy, spec.exposure_params)
    override_policy = build("override", spec.override_policy, spec.override_params)
    allocation_policy = build("allocation", spec.allocation_policy, spec.allocation_params)
    rebalance_policy = build("rebalance", spec.rebalance_policy, spec.rebalance_params)
    assert isinstance(signal_provider, SignalProvider)
    assert isinstance(exposure_policy, ExposurePolicy)
    assert isinstance(allocation_policy, AllocationPolicy)
    assert isinstance(rebalance_policy, RebalancePolicy)
    if override_policy is not None:
        assert isinstance(override_policy, OverridePolicy)

    for ticker, frame in frames.items():
        signal_provider.prepare(ticker, frame)

    backtest_dates = _backtest_dates(frames, cfg.availability_mode)
    if len(backtest_dates) < 2:
        raise ValueError("Portfolio backtest requires at least two trading dates.")

    cash = float(cfg.starting_cash)
    positions: dict[str, PortfolioPosition] = {ticker: PortfolioPosition() for ticker in frames}
    pending: _TargetInstruction | None = None
    equity_curve: list[dict[str, Any]] = []
    trades: list[dict[str, Any]] = []
    brake_log: list[dict[str, Any]] = []
    daily_exposure: list[dict[str, Any]] = []
    portfolio_returns: list[float] = []
    previous_equity: float | None = None
    peak_equity = float(cfg.starting_cash)
    total_costs = 0.0
    expected_costs = 0.0
    total_turnover = 0.0
    current_targets: dict[str, float] = {}
    last_close_prices: dict[str, float] = {}

    for idx, date in enumerate(backtest_dates):
        active_tickers = sorted(ticker for ticker, frame in frames.items() if date in frame.index)
        if not active_tickers:
            continue
        open_prices = {ticker: float(frames[ticker].loc[date, "open"]) for ticker in active_tickers}
        valuation_open_prices = {
            ticker: open_prices.get(ticker, last_close_prices.get(ticker, 0.0))
            for ticker in frames
        }
        day_turnover = 0.0
        day_costs = 0.0
        if pending is not None:
            cash_delta, turnover_delta, cost_delta, expected_delta, trade_rows = _execute_instruction(
                date=date,
                instruction=pending,
                positions=positions,
                cash=cash,
                open_prices=valuation_open_prices,
                config=cfg,
                tradable_tickers=set(active_tickers),
            )
            cash = cash_delta
            day_turnover += turnover_delta
            day_costs += cost_delta
            total_costs += cost_delta
            expected_costs += expected_delta
            trades.extend(trade_rows)
            current_targets = dict(pending.target_weights)
            pending = None

        close_prices = dict(last_close_prices)
        for ticker in active_tickers:
            close_prices[ticker] = float(frames[ticker].loc[date, "price"])
        last_close_prices.update({ticker: close_prices[ticker] for ticker in active_tickers})
        position_value = _position_value(positions, close_prices)
        equity = cash + position_value
        _assert_accounting(cash, position_value, equity)
        peak_equity = max(peak_equity, equity)
        drawdown = (equity / peak_equity - 1.0) if peak_equity > 0 else 0.0
        exposure = (position_value / equity) if equity > 0 else 0.0
        if previous_equity is not None and previous_equity > 0:
            portfolio_returns.append(equity / previous_equity - 1.0)
        previous_equity = equity
        turnover_pct = day_turnover / equity if equity > 0 else 0.0
        total_turnover += turnover_pct
        row = {
            "date": _date_text(date),
            "equity": equity,
            "cash": cash,
            "position_value": position_value,
            "exposure": exposure,
            "turnover": turnover_pct,
            "costs_paid": day_costs,
            "active_ticker_count": len(active_tickers),
        }
        equity_curve.append(row)
        daily_exposure.append({"date": row["date"], "exposure": exposure})

        if idx >= len(backtest_dates) - 1:
            continue

        signal_map = {ticker: signal_provider.signals(ticker, pd.Timestamp(date)) for ticker in active_tickers}
        state = {
            "equity": equity,
            "cash": cash,
            "positions": {ticker: positions[ticker].quantity for ticker in positions},
            "portfolio_returns": list(portfolio_returns),
            "drawdown": drawdown,
            "peak_equity": peak_equity,
        }
        override = override_policy.override(pd.Timestamp(date), state, signal_map) if override_policy is not None else None
        if override is not None:
            brake_log.append({"date": _date_text(date), **override.to_dict()})
        excluded = set(override.exclude_tickers if override is not None else ())
        eligible = [ticker for ticker in active_tickers if ticker not in excluded]
        target_exposure = _clip01(float(exposure_policy.target_exposure(pd.Timestamp(date), state, signal_map)))
        if override is not None and override.exposure_cap is not None:
            target_exposure = min(target_exposure, _clip01(float(override.exposure_cap)))
        target_weights = allocation_policy.weights(pd.Timestamp(date), eligible, signal_map) if target_exposure > 0 and eligible else {}
        target_weights = _normalize_weights(target_weights)
        current_weights = {
            ticker: (positions[ticker].quantity * float(close_prices.get(ticker, 0.0)) / equity if equity > 0 else 0.0)
            for ticker in frames
        }
        drift_state = {
            "is_first_trading_day_month": idx == 0 or pd.Timestamp(date).month != pd.Timestamp(backtest_dates[idx - 1]).month,
            "relative_drifts": _relative_drifts(current_weights, target_weights, target_exposure),
            "current_weights": current_weights,
            "target_weights": target_weights,
        }
        should_rebalance = bool(override is not None) or bool(rebalance_policy.should_rebalance(pd.Timestamp(date), drift_state))
        if should_rebalance:
            pending = _TargetInstruction(
                decision_date=_date_text(date),
                target_exposure=target_exposure,
                target_weights=target_weights,
                override=override.to_dict() if override is not None else None,
                reason=(override.reason if override is not None else "rebalance"),
            )

    if abs(total_costs - expected_costs) > 0.01:
        raise AssertionError(f"Cost reconciliation failed: {total_costs:.4f} != {expected_costs:.4f}")
    equity_df = pd.DataFrame(equity_curve)
    trade_dicts = [_json_safe_trade(row) for row in trades]
    metrics = compute_equity_metrics(equity_df, trade_dicts, benchmark_curve=benchmark_curve, risk_free_rate=cfg.risk_free_rate)
    metrics["calmar_ratio"] = _calmar(metrics)
    metrics["annualized_turnover"] = _annualize_turnover(total_turnover, len(equity_curve))
    metrics["total_turnover"] = total_turnover
    metrics["total_costs_paid"] = total_costs
    oos_start = pd.Timestamp(cfg.oos_start) if cfg.oos_start else None
    in_sample = _segment_metrics(equity_df, trade_dicts, benchmark_curve, None, oos_start, cfg.risk_free_rate)
    out_of_sample = _segment_metrics(equity_df, trade_dicts, benchmark_curve, oos_start, None, cfg.risk_free_rate) if oos_start is not None else None
    stress = _stress_results(equity_df, trade_dicts, benchmark_curve, windows or get_stress_windows(), cfg.risk_free_rate)
    payload_base = {
        "config": cfg.to_dict(),
        "strategy_spec": spec.to_dict(),
        "strategy_hash": spec.hash,
        "git_sha": _git_sha(),
        "metrics": _json_safe(metrics),
        "in_sample": _json_safe(in_sample),
        "out_of_sample": _json_safe(out_of_sample),
        "equity_curve": _json_safe(equity_curve),
        "trades": _json_safe(trade_dicts),
        "daily_exposure": _json_safe(daily_exposure),
        "brake_log": _json_safe(brake_log),
        "stress_windows": _json_safe(stress),
    }
    result_hash = hashlib.sha256(json.dumps(payload_base, sort_keys=True, default=str).encode("utf-8")).hexdigest()[:16]
    return PortfolioBacktestResult(
        config=cfg.to_dict(),
        strategy_spec=spec.to_dict(),
        strategy_hash=spec.hash,
        git_sha=_git_sha(),
        metrics=dict(payload_base["metrics"]),
        in_sample=dict(payload_base["in_sample"]),
        out_of_sample=dict(payload_base["out_of_sample"]) if isinstance(payload_base["out_of_sample"], dict) else None,
        equity_curve=list(payload_base["equity_curve"]),
        trades=list(payload_base["trades"]),
        daily_exposure=list(payload_base["daily_exposure"]),
        brake_log=list(payload_base["brake_log"]),
        stress_windows=list(payload_base["stress_windows"]),
        result_hash=result_hash,
    )


def control_specs() -> dict[str, StrategySpec]:
    return {
        "C1_spy_buy_hold": StrategySpec(
            name="C1_spy_buy_hold",
            exposure_policy="always_full",
            allocation_policy="equal_weight",
            rebalance_policy="monthly_bands",
            description="SPY buy-and-hold expressed through the portfolio engine.",
        ),
        "C2_spy_200dma": StrategySpec(
            name="C2_spy_200dma",
            exposure_policy="moving_average_timing",
            exposure_params={"ticker": "SPY"},
            allocation_policy="equal_weight",
            rebalance_policy="monthly_bands",
            description="SPY 200-day moving average timing with 5-day confirmation.",
        ),
        "C3_spy_vol_target": StrategySpec(
            name="C3_spy_vol_target",
            exposure_policy="vol_target",
            exposure_params={"target_vol": 0.15, "min_exposure": 0.25},
            allocation_policy="equal_weight",
            rebalance_policy="monthly_bands",
            description="SPY volatility-targeted control.",
        ),
    }


def _execute_instruction(
    *,
    date: pd.Timestamp,
    instruction: _TargetInstruction,
    positions: dict[str, PortfolioPosition],
    cash: float,
    open_prices: dict[str, float],
    config: PortfolioBacktestConfig,
    tradable_tickers: set[str] | None = None,
) -> tuple[float, float, float, float, list[dict[str, Any]]]:
    equity_at_open = cash + _position_value(positions, open_prices)
    target_quantities: dict[str, float] = {}
    tradable = set(open_prices) if tradable_tickers is None else {str(ticker).upper() for ticker in tradable_tickers}
    for ticker, position in positions.items():
        if ticker not in tradable:
            target_quantities[ticker] = position.quantity
            continue
        weight = float(instruction.target_weights.get(ticker, 0.0)) * float(instruction.target_exposure)
        target_value = max(0.0, equity_at_open * weight)
        price = float(open_prices.get(ticker, 0.0))
        if price <= 0:
            target_quantities[ticker] = position.quantity
            continue
        gross_price = price * (1.0 + max(0.0, float(config.entry_cost_bps)) / 10_000.0)
        target_qty = target_value / gross_price
        target_quantities[ticker] = math.floor(target_qty) if config.integer_shares else target_qty

    trades: list[dict[str, Any]] = []
    turnover = 0.0
    costs = 0.0
    expected_costs = 0.0
    for ticker, target_qty in target_quantities.items():
        position = positions[ticker]
        delta = target_qty - position.quantity
        if delta >= -1e-9:
            continue
        quantity = min(position.quantity, abs(delta))
        price = float(open_prices.get(ticker, 0.0))
        if price <= 0 or ticker not in tradable:
            continue
        notional = quantity * price
        cost = notional * max(0.0, float(config.exit_cost_bps)) / 10_000.0
        cash += notional - cost
        turnover += notional
        costs += cost
        expected_costs += cost
        net_pnl = (price - position.avg_cost) * quantity - cost
        position.quantity -= quantity
        if position.quantity <= 1e-9:
            position.quantity = 0.0
            position.avg_cost = 0.0
        trades.append(_trade_row(date, instruction, ticker, "Sell", quantity, price, notional, cost, net_pnl))
    for ticker, target_qty in target_quantities.items():
        position = positions[ticker]
        delta = target_qty - position.quantity
        if delta <= 1e-9:
            continue
        price = float(open_prices.get(ticker, 0.0))
        if price <= 0 or ticker not in tradable:
            continue
        unit_cost = price * (1.0 + max(0.0, float(config.entry_cost_bps)) / 10_000.0)
        quantity = delta
        if config.integer_shares:
            quantity = math.floor(min(quantity, cash / unit_cost if unit_cost > 0 else 0.0))
        elif unit_cost * quantity > cash and unit_cost > 0:
            quantity = cash / unit_cost
        if quantity <= 1e-9:
            continue
        notional = quantity * price
        cost = notional * max(0.0, float(config.entry_cost_bps)) / 10_000.0
        total_cash = notional + cost
        cash -= total_cash
        turnover += notional
        costs += cost
        expected_costs += cost
        old_quantity = position.quantity
        new_quantity = old_quantity + quantity
        position.avg_cost = ((old_quantity * position.avg_cost) + notional) / new_quantity if new_quantity > 0 else 0.0
        position.quantity = new_quantity
        trades.append(_trade_row(date, instruction, ticker, "Buy", quantity, price, notional, cost, -cost))
    if cash < -0.01:
        raise AssertionError(f"Cash went negative after fill: {cash:.4f}")
    return cash, turnover, costs, expected_costs, trades


def _trade_row(date: pd.Timestamp, instruction: _TargetInstruction, ticker: str, side: str, quantity: float, price: float, notional: float, cost: float, net_pnl: float) -> dict[str, Any]:
    return {
        "date": _date_text(date),
        "decision_date": instruction.decision_date,
        "ticker": ticker,
        "side": side,
        "quantity": quantity,
        "price": price,
        "notional": notional,
        "costs_paid": cost,
        "net_pnl": net_pnl,
        "holding_days": 0,
        "exit_date": _date_text(date),
        "exit_type": instruction.reason,
    }


def _common_dates(frames: dict[str, pd.DataFrame]) -> list[pd.Timestamp]:
    common: set[pd.Timestamp] | None = None
    for frame in frames.values():
        dates = {pd.Timestamp(value).normalize() for value in frame.index}
        common = dates if common is None else common & dates
    return sorted(common or set())


def _backtest_dates(frames: dict[str, pd.DataFrame], availability_mode: str) -> list[pd.Timestamp]:
    mode = str(availability_mode or "common").strip().lower()
    if mode == "common":
        return _common_dates(frames)
    if mode == "panel":
        dates: set[pd.Timestamp] = set()
        for frame in frames.values():
            dates.update(pd.Timestamp(value).normalize() for value in frame.index)
        return sorted(dates)
    raise ValueError("availability_mode must be 'common' or 'panel'.")


def _position_value(positions: dict[str, PortfolioPosition], prices: dict[str, float]) -> float:
    return float(sum(float(position.quantity) * float(prices.get(ticker, 0.0)) for ticker, position in positions.items()))


def _assert_accounting(cash: float, position_value: float, equity: float) -> None:
    if abs((cash + position_value) - equity) > 0.01:
        raise AssertionError("cash + positions must equal equity")
    if cash < -0.01:
        raise AssertionError("cash cannot be negative")


def _normalize_weights(weights: dict[str, float]) -> dict[str, float]:
    cleaned = {str(ticker).upper(): max(0.0, float(weight)) for ticker, weight in weights.items()}
    total = sum(cleaned.values())
    if total <= 0:
        return {}
    return {ticker: weight / total for ticker, weight in sorted(cleaned.items())}


def _relative_drifts(current_weights: dict[str, float], target_weights: dict[str, float], target_exposure: float) -> dict[str, float]:
    tickers = sorted(set(current_weights) | set(target_weights))
    result: dict[str, float] = {}
    for ticker in tickers:
        target = float(target_weights.get(ticker, 0.0)) * float(target_exposure)
        current = float(current_weights.get(ticker, 0.0))
        if target <= 0:
            result[ticker] = math.inf if current > 0.001 else 0.0
        else:
            result[ticker] = (current - target) / target
    return result


def _segment_metrics(
    equity_df: pd.DataFrame,
    trades: list[dict[str, Any]],
    benchmark_curve: pd.DataFrame | None,
    start: pd.Timestamp | None,
    end: pd.Timestamp | None,
    risk_free_rate: float,
) -> dict[str, Any]:
    if equity_df.empty:
        return {}
    dates = pd.to_datetime(equity_df["date"])
    mask = pd.Series(True, index=equity_df.index)
    if start is not None:
        mask &= dates >= start
    if end is not None:
        mask &= dates < end
    segment = equity_df.loc[mask].copy()
    segment_trades = [
        row for row in trades
        if (start is None or pd.Timestamp(row["date"]) >= start)
        and (end is None or pd.Timestamp(row["date"]) < end)
    ]
    bench_segment = None
    if benchmark_curve is not None and not benchmark_curve.empty:
        bench = benchmark_curve.copy()
        bench_dates = pd.to_datetime(bench["date"]) if "date" in bench.columns else pd.to_datetime(bench.index)
        bench_mask = pd.Series(True, index=bench.index)
        if start is not None:
            bench_mask &= bench_dates >= start
        if end is not None:
            bench_mask &= bench_dates < end
        bench_segment = bench.loc[bench_mask].copy()
    return compute_equity_metrics(segment, segment_trades, benchmark_curve=bench_segment, risk_free_rate=risk_free_rate)


def _stress_results(
    equity_df: pd.DataFrame,
    trades: list[dict[str, Any]],
    benchmark_curve: pd.DataFrame | None,
    windows: list[StressWindow],
    risk_free_rate: float,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if equity_df.empty:
        return rows
    dates = pd.to_datetime(equity_df["date"])
    for window in windows:
        start = pd.Timestamp(window.start)
        end_exclusive = pd.Timestamp(window.end) + pd.Timedelta(days=1)
        mask = (dates >= start) & (dates < end_exclusive)
        segment = equity_df.loc[mask].copy()
        if segment.empty:
            continue
        metrics = _segment_metrics(equity_df, trades, benchmark_curve, start, end_exclusive, risk_free_rate)
        exposure = pd.to_numeric(segment.get("exposure", pd.Series(index=segment.index, data=0.0)), errors="coerce").fillna(0.0)
        derisked = segment.loc[exposure < 0.7]
        days_to_derisk = None
        if not derisked.empty:
            days_to_derisk = int((pd.Timestamp(derisked["date"].iloc[0]) - start).days)
        rows.append(
            {
                "key": window.key,
                "label": window.label,
                "start": window.start,
                "end": window.end,
                "metrics": metrics,
                "strategy_total_return": metrics.get("total_return"),
                "strategy_max_drawdown": metrics.get("max_drawdown"),
                "exposure_mean": float(exposure.mean()) if len(exposure) else None,
                "days_to_derisk": days_to_derisk,
                "trade_count": len([row for row in trades if start <= pd.Timestamp(row["date"]) < end_exclusive]),
            }
        )
    return rows


def _annualize_turnover(total_turnover: float, days: int) -> float:
    if days <= 1:
        return 0.0
    return float(total_turnover) * 252.0 / float(days)


def _calmar(metrics: dict[str, Any]) -> float | None:
    annualized = _finite(metrics.get("annualized_return"))
    drawdown = abs(_finite(metrics.get("max_drawdown")) or 0.0)
    if annualized is None or drawdown <= 0:
        return None
    return annualized / drawdown


def _clip01(value: float) -> float:
    return max(0.0, min(1.0, value))


def _finite(value: Any) -> float | None:
    try:
        parsed = float(value)
    except Exception:
        return None
    return parsed if math.isfinite(parsed) else None


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, (pd.Timestamp, dt.datetime, dt.date)):
        return value.isoformat()
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            return str(value)
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _json_safe_trade(row: dict[str, Any]) -> dict[str, Any]:
    return dict(_json_safe(row))


def _date_text(date: pd.Timestamp) -> str:
    return str(pd.Timestamp(date).date().isoformat())


def _git_sha() -> str:
    try:
        return subprocess.check_output(["git", "rev-parse", "--short", "HEAD"], text=True).strip()
    except Exception:
        return "unknown"


def _normalize_portfolio_frame(frame: pd.DataFrame) -> pd.DataFrame:
    normalized = _normalize_pipeline_market_frame(frame)
    original = frame.copy()
    if not isinstance(original.index, pd.DatetimeIndex):
        original.index = pd.to_datetime(original.index)
    original = original.sort_index()
    rename = {
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "price",
        "Adj Close": "price",
        "Volume": "volume",
    }
    original = original.rename(columns={column: rename.get(str(column), str(column)) for column in original.columns})
    for column in original.columns:
        if column not in normalized.columns:
            normalized[column] = original[column].reindex(normalized.index)
    return normalized
