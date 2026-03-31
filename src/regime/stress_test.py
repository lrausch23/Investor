from __future__ import annotations

import datetime as dt
import logging
from dataclasses import asdict, dataclass
from math import sqrt
from typing import Any

import pandas as pd

from .ensemble import aggregate_analysts, get_registry
from .fundamental_gating import run_fundamental_gate
from .hmm_engine import fit_regime_model
from .market_data_client import download_daily_bars
from .meta_labeler import extract_meta_features
from .scenarios import get_scenario
from .signals import (
    build_composite_signal,
    compute_price_targets,
    compute_technicals,
    compute_unified_confidence,
    forward_regime_curve,
    intra_regime_signal,
    signal_from_forward_curve,
)

logger = logging.getLogger(__name__)


@dataclass
class StressTestConfig:
    scenario_id: str
    tickers: list[str] | None = None
    training_window: int = 504
    refit_step: int = 21
    starting_budget: float = 100_000.0
    fundamental_gate_enabled: bool = True
    hurdle_rate_enabled: bool = True
    duration_gate_enabled: bool = True
    anti_churn_enabled: bool = True
    ltcg_override_enabled: bool = True
    hurdle_min_net_return_pct: float = 3.0
    estimated_stcg_rate: float = 0.32
    min_regime_duration_days: float = 7.0
    max_round_trips: int = 2
    anti_churn_cooldown_days: int = 30
    ltcg_trigger_days: int = 16
    ltcg_max_risk_atr: float = 2.0
    altman_z_distress_threshold: float = 1.81
    piotroski_min: int = 6


@dataclass(frozen=True)
class TickerResult:
    ticker: str
    trades: list[dict[str, Any]]
    total_return: float
    max_drawdown: float
    sharpe_ratio: float | None
    win_rate: float | None
    avg_win: float | None
    avg_loss: float | None
    buy_and_hold_return: float
    equity_curve: list[dict[str, Any]]
    stop_outs: int
    round_trip_count: int
    recovery_days: int | None
    churn_vetoes: int
    hurdle_vetoes: int
    duration_vetoes: int
    fundamental_vetoes: int
    ltcg_overrides_triggered: int
    ltcg_tax_savings_total: float
    ltcg_lots_protected: int
    net_return_after_tax: float
    ml_signals_generated: int
    ml_avg_score: float | None


@dataclass(frozen=True)
class StressTestResult:
    scenario_id: str
    scenario_name: str
    config: StressTestConfig
    ticker_results: list[TickerResult]
    portfolio_total_return: float
    portfolio_max_drawdown: float
    portfolio_sharpe: float | None
    total_trades: int
    total_stop_outs: int
    total_round_trips: int
    worst_recovery_days: int | None
    total_churn_vetoes: int
    total_hurdle_vetoes: int
    total_duration_vetoes: int
    total_fundamental_vetoes: int
    total_ltcg_overrides: int
    total_ltcg_tax_savings: float
    net_portfolio_return_after_tax: float
    benchmark_return: float
    alpha: float
    started_at: str
    completed_at: str
    duration_seconds: float


class _AntiChurnTracker:
    def __init__(self, max_round_trips: int = 2, cooldown_days: int = 30):
        self._sells: dict[str, list[dt.datetime]] = {}
        self.max_round_trips = int(max_round_trips)
        self.cooldown_days = int(cooldown_days)

    def record_sell(self, ticker: str, date: dt.datetime) -> None:
        self._sells.setdefault(str(ticker or "").upper(), []).append(date)

    def round_trip_count(self, ticker: str, as_of: dt.datetime) -> int:
        cutoff = as_of - dt.timedelta(days=self.cooldown_days)
        return len([value for value in self._sells.get(str(ticker or "").upper(), []) if value >= cutoff])

    def is_restricted(self, ticker: str, as_of: dt.datetime) -> bool:
        return self.round_trip_count(ticker, as_of) >= self.max_round_trips


class _LTCGTracker:
    def __init__(
        self,
        trigger_days: int = 16,
        max_risk_atr: float = 2.0,
        stcg_rate: float = 0.32,
        ltcg_rate: float = 0.15,
    ) -> None:
        self._lots: dict[str, dict[str, Any]] = {}
        self.trigger_days = int(trigger_days)
        self.max_risk_atr = float(max_risk_atr)
        self.stcg_rate = float(stcg_rate)
        self.ltcg_rate = float(ltcg_rate)

    def open_lot(self, ticker: str, entry_price: float, entry_date: dt.datetime, quantity: int) -> None:
        self._lots[str(ticker or "").upper()] = {
            "entry_price": float(entry_price),
            "entry_date": entry_date,
            "quantity": int(quantity),
        }

    def should_override_exit(
        self,
        ticker: str,
        current_date: dt.datetime,
        current_price: float,
        atr_14: float,
    ) -> tuple[bool, float]:
        lot = self._lots.get(str(ticker or "").upper())
        if lot is None:
            return False, 0.0
        days_held = (current_date - lot["entry_date"]).days
        if days_held < (366 - self.trigger_days):
            return False, 0.0
        if float(current_price) <= float(lot["entry_price"]):
            return False, 0.0
        overridden_stop = float(current_price) - (self.max_risk_atr * max(float(atr_14 or 0.0), 0.0))
        if float(current_price) <= overridden_stop:
            return False, 0.0
        return True, overridden_stop

    def estimate_tax_savings(self, ticker: str, exit_price: float) -> float:
        lot = self._lots.get(str(ticker or "").upper())
        if lot is None:
            return 0.0
        gain = (float(exit_price) - float(lot["entry_price"])) * float(lot["quantity"])
        return gain * (self.stcg_rate - self.ltcg_rate) if gain > 0 else 0.0

    def close_lot(self, ticker: str) -> dict[str, Any] | None:
        return self._lots.pop(str(ticker or "").upper(), None)


def _normalize_history_columns(history: pd.DataFrame, ticker: str) -> pd.DataFrame:
    if history.empty or not isinstance(history.columns, pd.MultiIndex):
        return history
    if ticker in history.columns.get_level_values(-1):
        return history.xs(ticker, axis=1, level=-1)
    if ticker in history.columns.get_level_values(0):
        return history.xs(ticker, axis=1, level=0)
    return history.droplevel(-1, axis=1)


def _history_to_series(history: pd.DataFrame, ticker: str, column: str) -> pd.Series:
    frame = _normalize_history_columns(history, ticker)
    if column not in frame.columns:
        raise ValueError(f"Column {column} unavailable for {ticker}")
    series = pd.Series(frame[column], copy=True)
    series.index = pd.to_datetime(series.index)
    return pd.to_numeric(series, errors="coerce").dropna()


def _download_macro_series(symbol: str, start: dt.date, end: dt.date, name: str, default: float) -> pd.Series:
    try:
        history = download_daily_bars(symbol, start=start, end=end, auto_adjust=False)
        close = _history_to_series(history, symbol, "Close").rename(name)
        return close
    except Exception:
        logger.info("Stress test macro fallback for %s", symbol, exc_info=True)
        index = pd.date_range(start=start, end=end, freq="B")
        return pd.Series(default, index=index, name=name, dtype=float)


def _build_market_frame(ticker: str, start: dt.date, end: dt.date) -> pd.DataFrame:
    history = download_daily_bars(ticker, start=start, end=end, auto_adjust=True)
    frame = _normalize_history_columns(history, ticker)
    close_col = "Close" if "Close" in frame.columns else frame.columns[0]
    high_col = "High" if "High" in frame.columns else close_col
    low_col = "Low" if "Low" in frame.columns else close_col
    open_col = "Open" if "Open" in frame.columns else close_col
    volume_col = "Volume" if "Volume" in frame.columns else None
    if volume_col is None:
        raise ValueError(f"Volume unavailable for {ticker}")
    rows = pd.DataFrame(
        {
            "price": pd.to_numeric(frame[close_col], errors="coerce"),
            "open": pd.to_numeric(frame[open_col], errors="coerce"),
            "high": pd.to_numeric(frame[high_col], errors="coerce"),
            "low": pd.to_numeric(frame[low_col], errors="coerce"),
            "volume": pd.to_numeric(frame[volume_col], errors="coerce"),
        }
    ).dropna()
    rows.index = pd.to_datetime(rows.index)
    vix = _download_macro_series("^VIX", start, end, "vix", 20.0)
    yield_10y = _download_macro_series("^TNX", start, end, "yield_10y", 4.0)
    rows = rows.join(vix.reindex(rows.index).ffill().bfill(), how="left")
    rows = rows.join(yield_10y.reindex(rows.index).ffill().bfill(), how="left")
    return rows.ffill().dropna()


def _returns_summary(returns: list[float]) -> tuple[float | None, float | None, float | None, float | None]:
    if not returns:
        return None, None, None, None
    series = pd.Series(returns, dtype=float)
    wins = [value for value in returns if value > 0]
    losses = [value for value in returns if value < 0]
    std = float(series.std()) if len(series) > 1 else 0.0
    sharpe = float(series.mean() / std * sqrt(len(series))) if std > 0 else None
    win_rate = float(len(wins) / len(returns)) if returns else None
    avg_win = float(sum(wins) / len(wins)) if wins else None
    avg_loss = float(sum(losses) / len(losses)) if losses else None
    return sharpe, win_rate, avg_win, avg_loss


def _max_drawdown(equity_curve: list[dict[str, Any]]) -> float:
    if not equity_curve:
        return 0.0
    values = pd.Series([float(row.get("equity") or 0.0) for row in equity_curve], dtype=float)
    peak = values.cummax()
    return float(((values / peak) - 1.0).min()) if not values.empty else 0.0


def _recovery_days(equity_curve: list[dict[str, Any]]) -> int | None:
    if not equity_curve:
        return None
    peak_value = float(equity_curve[0].get("equity") or 0.0)
    peak_date = pd.Timestamp(equity_curve[0].get("date"))
    trough_value = peak_value
    trough_date = peak_date
    recovering = False
    for row in equity_curve:
        current_value = float(row.get("equity") or 0.0)
        current_date = pd.Timestamp(row.get("date"))
        if current_value >= peak_value:
            if recovering:
                return int((current_date - trough_date).days)
            peak_value = current_value
            peak_date = current_date
            trough_value = current_value
            trough_date = current_date
            continue
        if current_value < trough_value:
            trough_value = current_value
            trough_date = current_date
            recovering = True
    return None


def _benchmark_return(benchmark: str, start: dt.date, end: dt.date) -> float:
    try:
        history = download_daily_bars(benchmark, start=start, end=end, auto_adjust=True)
        close = _history_to_series(history, benchmark, "Close")
        return float(close.iloc[-1] / close.iloc[0] - 1.0) if len(close) >= 2 and float(close.iloc[0]) else 0.0
    except Exception:
        logger.warning("Unable to compute stress-test benchmark return for %s", benchmark, exc_info=True)
        return 0.0


def _serialize_trade(
    *,
    entry_date: pd.Timestamp,
    exit_date: pd.Timestamp,
    entry_price: float,
    exit_price: float,
    quantity: int,
    reason: str,
    after_tax_return: float,
) -> dict[str, Any]:
    trade_return = (float(exit_price) - float(entry_price)) / float(entry_price) if entry_price else 0.0
    return {
        "entry_date": str(entry_date.date()),
        "exit_date": str(exit_date.date()),
        "entry_price": float(entry_price),
        "exit_price": float(exit_price),
        "quantity": int(quantity),
        "return": float(trade_return),
        "after_tax_return": float(after_tax_return),
        "reason": str(reason),
    }


def _safe_meta_score(ticker: str, regime: Any) -> float | None:
    try:
        registry = get_registry()
        features = extract_meta_features(regime.price_frame.iloc[-1])
        analyst_results = []
        meta_score = None
        for analyst_name in registry.list_analysts():
            analyst = registry.get(analyst_name)
            if analyst is None or not analyst.is_ready():
                continue
            result = analyst.analyze(ticker=ticker, features=features, regime_result=regime)
            analyst_results.append(result)
            if str(getattr(result, "analyst_name", "")) == "xgboost_meta_labeler":
                meta_score = float(getattr(result, "confidence", 0.0) or 0.0)
        if analyst_results:
            aggregate_analysts(analyst_results)
        return meta_score
    except Exception:
        logger.debug("Stress-test meta-labeler unavailable for %s", ticker, exc_info=True)
        return None


def _run_ticker_replay(
    ticker: str,
    scenario_start: dt.date,
    scenario_end: dt.date,
    config: StressTestConfig,
) -> TickerResult:
    buffer_start = scenario_start - dt.timedelta(days=max(1, int(config.training_window)))
    frame = _build_market_frame(ticker, buffer_start, scenario_end)
    scenario_frame = frame.loc[frame.index.date >= scenario_start].copy()
    if scenario_frame.empty:
        raise ValueError(f"No scenario data available for {ticker}")

    buy_hold_return = float(scenario_frame["price"].iloc[-1] / scenario_frame["price"].iloc[0] - 1.0)
    technicals = compute_technicals(frame["price"], frame["volume"], frame["high"], frame["low"])
    initial_budget = float(config.starting_budget)
    cash = initial_budget
    quantity = 0
    entry_price: float | None = None
    entry_date: pd.Timestamp | None = None
    trades: list[dict[str, Any]] = []
    equity_curve: list[dict[str, Any]] = []
    stop_outs = 0
    churn_vetoes = 0
    hurdle_vetoes = 0
    duration_vetoes = 0
    fundamental_vetoes = 0
    ltcg_overrides_triggered = 0
    ltcg_tax_savings_total = 0.0
    ltcg_lots_protected = 0
    trade_returns: list[float] = []
    after_tax_returns: list[float] = []
    ml_scores: list[float] = []
    anti_churn = _AntiChurnTracker(config.max_round_trips, config.anti_churn_cooldown_days)
    ltcg_tracker = _LTCGTracker(config.ltcg_trigger_days, config.ltcg_max_risk_atr, config.estimated_stcg_rate)
    latest_signal: dict[str, Any] | None = None
    next_refit_index = max(config.training_window, 0)

    if config.fundamental_gate_enabled:
        gate = run_fundamental_gate(
            ticker,
            piotroski_min=config.piotroski_min,
            altman_z_distress_threshold=config.altman_z_distress_threshold,
            pass_on_insufficient_data=True,
        )
        if not gate.passed:
            fundamental_vetoes = 1
            return TickerResult(
                ticker=ticker,
                trades=[],
                total_return=0.0,
                max_drawdown=0.0,
                sharpe_ratio=None,
                win_rate=None,
                avg_win=None,
                avg_loss=None,
                buy_and_hold_return=buy_hold_return,
                equity_curve=[],
                stop_outs=0,
                round_trip_count=0,
                recovery_days=None,
                churn_vetoes=0,
                hurdle_vetoes=0,
                duration_vetoes=0,
                fundamental_vetoes=1,
                ltcg_overrides_triggered=0,
                ltcg_tax_savings_total=0.0,
                ltcg_lots_protected=0,
                net_return_after_tax=0.0,
                ml_signals_generated=0,
                ml_avg_score=None,
            )

    for idx, (date, row) in enumerate(frame.iterrows()):
        if date.date() < scenario_start:
            continue
        technical_slice = technicals.iloc[: idx + 1].dropna()
        atr_14 = float(technical_slice.iloc[-1]["atr_14"]) if not technical_slice.empty and pd.notna(technical_slice.iloc[-1]["atr_14"]) else 0.0

        if idx >= next_refit_index and len(frame.iloc[: idx + 1]) >= max(120, config.training_window):
            regime = fit_regime_model(
                ticker=ticker,
                market_frame=frame.iloc[: idx + 1],
                training_window=config.training_window,
                refit_step=config.refit_step,
            )
            forward_signal = signal_from_forward_curve(
                forward_regime_curve(regime.transition_matrix, regime.latest_state_vector, horizon=21),
                regime.latest_label,
                regime.transition_risk,
                regime.expected_regime_duration,
                regime.latest_probability,
            )
            technical_signal = intra_regime_signal(technical_slice, regime.latest_label) if not technical_slice.empty else "Hold"
            composite = build_composite_signal(regime.latest_label, regime.latest_probability, forward_signal, technical_signal)
            targets = compute_price_targets(
                current_price=float(row["price"]),
                technicals_df=technical_slice if not technical_slice.empty else technicals.iloc[: idx + 1],
                composite_signal=composite,
                expected_duration=float(regime.expected_regime_duration),
                state_mean_return=float(regime.recent_state_mean_return or 0.0),
            )
            confidence = compute_unified_confidence(float(regime.latest_probability), float(composite.composite_strength), calibrator=None)
            meta_score = _safe_meta_score(ticker, regime)
            latest_signal = {
                "regime": regime,
                "composite": composite,
                "targets": targets,
                "confidence": confidence,
                "meta_score": meta_score,
            }
            if meta_score is not None:
                ml_scores.append(meta_score)
            next_refit_index = idx + max(1, int(config.refit_step))

        current_price = float(row["price"])
        current_equity = cash + (quantity * current_price if quantity > 0 else 0.0)
        equity_curve.append({"date": str(date.date()), "equity": current_equity})
        if latest_signal is None:
            continue

        composite = latest_signal["composite"]
        targets = latest_signal["targets"]
        regime = latest_signal["regime"]

        if quantity > 0 and entry_price is not None and current_price <= float(targets.stop_price or (entry_price - (2.0 * atr_14))):
            proceeds = quantity * current_price
            cash += proceeds
            holding_days = max(1, (date - entry_date).days) if entry_date is not None else 1
            tax_rate = 0.15 if holding_days >= 366 else float(config.estimated_stcg_rate)
            after_tax_return = ((current_price - entry_price) * quantity * (1 - tax_rate)) / initial_budget
            trades.append(
                _serialize_trade(
                    entry_date=entry_date or date,
                    exit_date=date,
                    entry_price=entry_price,
                    exit_price=current_price,
                    quantity=quantity,
                    reason="stop_out",
                    after_tax_return=after_tax_return,
                )
            )
            trade_returns.append((current_price - entry_price) / entry_price if entry_price else 0.0)
            after_tax_returns.append(after_tax_return)
            stop_outs += 1
            anti_churn.record_sell(ticker, date.to_pydatetime())
            ltcg_tracker.close_lot(ticker)
            quantity = 0
            entry_price = None
            entry_date = None
            continue

        action = str(getattr(composite, "composite_action", "Hold") or "Hold")
        if quantity <= 0 and action in {"Buy", "Strong Buy"}:
            if config.anti_churn_enabled and anti_churn.is_restricted(ticker, date.to_pydatetime()):
                churn_vetoes += 1
                continue
            target_price = targets.exit_price
            if config.hurdle_rate_enabled and target_price is not None and current_price > 0:
                gross_return = (float(target_price) - current_price) / current_price
                net_return = gross_return * (1.0 - float(config.estimated_stcg_rate))
                if net_return < (float(config.hurdle_min_net_return_pct) / 100.0):
                    hurdle_vetoes += 1
                    continue
            if config.duration_gate_enabled and str(regime.latest_label) == "Bull" and float(regime.expected_regime_duration or 0.0) < float(config.min_regime_duration_days):
                duration_vetoes += 1
                continue
            allocation = min(0.10, max(0.02, float(getattr(composite, "composite_strength", 0.0) or 0.0) * 0.15))
            position_value = cash * allocation
            shares = int(position_value / current_price) if current_price > 0 else 0
            if shares <= 0:
                continue
            cost = shares * current_price
            if cost > cash:
                shares = int(cash / current_price)
                cost = shares * current_price
            if shares <= 0:
                continue
            cash -= cost
            quantity = shares
            entry_price = current_price
            entry_date = date
            if config.ltcg_override_enabled:
                ltcg_tracker.open_lot(ticker, current_price, date.to_pydatetime(), shares)
            continue

        if quantity > 0 and entry_price is not None and action in {"Sell", "Strong Sell"}:
            if config.ltcg_override_enabled and atr_14 > 0:
                should_override, _overridden_stop = ltcg_tracker.should_override_exit(
                    ticker,
                    date.to_pydatetime(),
                    current_price,
                    atr_14,
                )
                if should_override:
                    ltcg_overrides_triggered += 1
                    ltcg_lots_protected += 1
                    ltcg_tax_savings_total += ltcg_tracker.estimate_tax_savings(ticker, current_price)
                    continue
            proceeds = quantity * current_price
            cash += proceeds
            holding_days = max(1, (date - entry_date).days) if entry_date is not None else 1
            tax_rate = 0.15 if holding_days >= 366 else float(config.estimated_stcg_rate)
            after_tax_return = ((current_price - entry_price) * quantity * (1 - tax_rate)) / initial_budget
            trades.append(
                _serialize_trade(
                    entry_date=entry_date or date,
                    exit_date=date,
                    entry_price=entry_price,
                    exit_price=current_price,
                    quantity=quantity,
                    reason="signal_exit",
                    after_tax_return=after_tax_return,
                )
            )
            trade_returns.append((current_price - entry_price) / entry_price if entry_price else 0.0)
            after_tax_returns.append(after_tax_return)
            anti_churn.record_sell(ticker, date.to_pydatetime())
            ltcg_tracker.close_lot(ticker)
            quantity = 0
            entry_price = None
            entry_date = None

    if quantity > 0 and entry_price is not None:
        final_date = scenario_frame.index[-1]
        final_price = float(scenario_frame["price"].iloc[-1])
        cash += quantity * final_price
        holding_days = max(1, (final_date - entry_date).days) if entry_date is not None else 1
        tax_rate = 0.15 if holding_days >= 366 else float(config.estimated_stcg_rate)
        after_tax_return = ((final_price - entry_price) * quantity * (1 - tax_rate)) / initial_budget
        trades.append(
            _serialize_trade(
                entry_date=entry_date or final_date,
                exit_date=final_date,
                entry_price=entry_price,
                exit_price=final_price,
                quantity=quantity,
                reason="final_close",
                after_tax_return=after_tax_return,
            )
        )
        trade_returns.append((final_price - entry_price) / entry_price if entry_price else 0.0)
        after_tax_returns.append(after_tax_return)
        anti_churn.record_sell(ticker, final_date.to_pydatetime())
        ltcg_tracker.close_lot(ticker)

    final_equity = cash
    total_return = (final_equity / initial_budget) - 1.0 if initial_budget > 0 else 0.0
    sharpe, win_rate, avg_win, avg_loss = _returns_summary(trade_returns)
    return TickerResult(
        ticker=ticker,
        trades=trades,
        total_return=float(total_return),
        max_drawdown=_max_drawdown(equity_curve),
        sharpe_ratio=sharpe,
        win_rate=win_rate,
        avg_win=avg_win,
        avg_loss=avg_loss,
        buy_and_hold_return=float(buy_hold_return),
        equity_curve=equity_curve,
        stop_outs=stop_outs,
        round_trip_count=len(trades),
        recovery_days=_recovery_days(equity_curve),
        churn_vetoes=churn_vetoes,
        hurdle_vetoes=hurdle_vetoes,
        duration_vetoes=duration_vetoes,
        fundamental_vetoes=fundamental_vetoes,
        ltcg_overrides_triggered=ltcg_overrides_triggered,
        ltcg_tax_savings_total=float(ltcg_tax_savings_total),
        ltcg_lots_protected=ltcg_lots_protected,
        net_return_after_tax=float(sum(after_tax_returns)),
        ml_signals_generated=len(ml_scores),
        ml_avg_score=(sum(ml_scores) / len(ml_scores)) if ml_scores else None,
    )


def run_stress_test(config: StressTestConfig) -> StressTestResult:
    started = dt.datetime.now(dt.timezone.utc)
    scenario = get_scenario(config.scenario_id)
    start_date = dt.date.fromisoformat(scenario.start_date)
    end_date = dt.date.fromisoformat(scenario.end_date)
    tickers = [str(ticker).upper() for ticker in (config.tickers or scenario.tickers)]
    per_ticker_budget = float(config.starting_budget) / max(1, len(tickers))
    ticker_results: list[TickerResult] = []
    for ticker in tickers:
        ticker_config = StressTestConfig(**{**asdict(config), "starting_budget": per_ticker_budget})
        logger.info("Stress test replay starting for scenario=%s ticker=%s", scenario.scenario_id, ticker)
        ticker_results.append(_run_ticker_replay(ticker, start_date, end_date, ticker_config))
    completed = dt.datetime.now(dt.timezone.utc)
    portfolio_returns = [row.total_return for row in ticker_results]
    after_tax_returns = [row.net_return_after_tax for row in ticker_results]
    sharpe, _, _, _ = _returns_summary(portfolio_returns)
    benchmark_return = _benchmark_return(scenario.benchmark, start_date, end_date)
    return StressTestResult(
        scenario_id=scenario.scenario_id,
        scenario_name=scenario.name,
        config=config,
        ticker_results=ticker_results,
        portfolio_total_return=(sum(portfolio_returns) / len(portfolio_returns)) if portfolio_returns else 0.0,
        portfolio_max_drawdown=min((row.max_drawdown for row in ticker_results), default=0.0),
        portfolio_sharpe=sharpe,
        total_trades=sum(len(row.trades) for row in ticker_results),
        total_stop_outs=sum(row.stop_outs for row in ticker_results),
        total_round_trips=sum(row.round_trip_count for row in ticker_results),
        worst_recovery_days=max((row.recovery_days or 0 for row in ticker_results), default=0) or None,
        total_churn_vetoes=sum(row.churn_vetoes for row in ticker_results),
        total_hurdle_vetoes=sum(row.hurdle_vetoes for row in ticker_results),
        total_duration_vetoes=sum(row.duration_vetoes for row in ticker_results),
        total_fundamental_vetoes=sum(row.fundamental_vetoes for row in ticker_results),
        total_ltcg_overrides=sum(row.ltcg_overrides_triggered for row in ticker_results),
        total_ltcg_tax_savings=sum(row.ltcg_tax_savings_total for row in ticker_results),
        net_portfolio_return_after_tax=(sum(after_tax_returns) / len(after_tax_returns)) if after_tax_returns else 0.0,
        benchmark_return=benchmark_return,
        alpha=((sum(portfolio_returns) / len(portfolio_returns)) if portfolio_returns else 0.0) - benchmark_return,
        started_at=started.isoformat(),
        completed_at=completed.isoformat(),
        duration_seconds=(completed - started).total_seconds(),
    )
