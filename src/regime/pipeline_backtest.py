from __future__ import annotations

import datetime as dt
import json
import logging
import math
from dataclasses import asdict, dataclass, field, replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable

import numpy as np
import pandas as pd

from .config import DEFAULT_SIGNAL_THRESHOLDS, SignalThresholds
from .data import download_market_frame
from .exceptions import InsufficientDataError
from .hmm_engine import build_features, fit_regime_model
from .hurdle_rate import (
    DEFAULT_ESTIMATED_STCG_RATE,
    DEFAULT_MIN_NET_RETURN_PCT,
    DEFAULT_MIN_REGIME_DURATION_DAYS,
    DurationGateResult,
    HurdleRateResult,
    check_duration_gate,
    check_hurdle_rate,
)
from .paper_trading import (
    DEFAULT_EXIT_TIME_STOP_DAYS,
    DEFAULT_NEUTRAL_REDUCE_FRACTION,
    DEFAULT_SIZING_ATR_MULTIPLIER,
    DEFAULT_SIZING_BASE_RISK_FRACTION,
    DEFAULT_SIZING_METHOD,
    _actual_fill_trade_geometry,
    _neutral_reduce_reason,
    _reduced_exit_quantity,
    _risk_adjusted_quantity,
    trailing_stop_level,
)
from .signal_quality import ACTIONABLE_SIGNAL_SCORE, evaluate_signal_quality
from .signals import (
    CompositeSignal,
    PriceTargets,
    SignalResult,
    build_composite_signal,
    compute_price_targets,
    compute_technicals,
    forward_regime_curve,
    intra_regime_signal,
    signal_from_forward_curve,
)
from .stress_windows import StressWindow, get_stress_windows
from .universe import check_universe_eligibility

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PipelineBacktestConfig:
    training_window: int = 504
    refit_step: int = 21
    lookback_window: int = 20
    sizing_method: str = DEFAULT_SIZING_METHOD
    sizing_atr_multiplier: float = DEFAULT_SIZING_ATR_MULTIPLIER
    sizing_base_risk_fraction: float = DEFAULT_SIZING_BASE_RISK_FRACTION
    max_position_pct: float = 1.0
    enable_hurdle_gate: bool = True
    estimated_stcg_rate: float = DEFAULT_ESTIMATED_STCG_RATE
    hurdle_min_net_return_pct: float = DEFAULT_MIN_NET_RETURN_PCT
    enable_duration_gate: bool = True
    min_regime_duration_days: float = DEFAULT_MIN_REGIME_DURATION_DAYS
    enable_anti_churn_gate: bool = True
    anti_churn_max_round_trips_30d: int = 2
    anti_churn_cooldown_days: int = 30
    enable_signal_quality_gate: bool = True
    min_signal_quality_score: float = ACTIONABLE_SIGNAL_SCORE
    profit_target: bool = True
    trailing_atr_multiplier: float = 2.0
    trailing_activation_atr: float = 1.0
    time_stop_days: int = DEFAULT_EXIT_TIME_STOP_DAYS
    neutral_reduce_fraction: float = DEFAULT_NEUTRAL_REDUCE_FRACTION
    enable_cost_model: bool = True
    entry_cost_bps: float = 10.0
    exit_cost_bps: float = 10.0
    starting_cash: float = 100_000.0
    random_state: int = 7
    macro_weighting: bool = False
    macro_weight: float = 1.5
    hmm_n_seeds: int = 1
    seed_agreement_min: float = 0.8
    hmm_covariance_type: str = "diag"
    oos_start: str | None = None
    risk_free_rate: float = 0.0
    signal_thresholds: SignalThresholds = field(default_factory=lambda: DEFAULT_SIGNAL_THRESHOLDS)
    composite_adjustments_enabled: bool = True
    enforce_universe_screen: bool = True


@dataclass(frozen=True)
class PipelineSignal:
    date: str
    regime: str
    probability: float
    composite_action: str
    composite_strength: float
    expected_duration: float
    transition_risk: float
    regime_days: int
    state_mean_return: float | None = None
    previous_regime: str | None = None
    p_bull_day5: float | None = None
    p_bear_day5: float | None = None
    p_neutral_day5: float | None = None
    forward_action: str | None = None
    technical_signal: str | None = None
    price_targets: dict[str, Any] = field(default_factory=dict)
    atr_14: float | None = None
    beta: float | None = None
    meta_labeler_probability: float | None = None
    signal_source: str | None = None
    seed_agreement: float | None = None
    regime_ambiguous: bool = False

    def to_signal_row(self, current_price: float) -> dict[str, Any]:
        return {
            "signal_generated_at": self.date,
            "regime": self.regime,
            "regime_label": self.regime,
            "probability": self.probability,
            "regime_probability": self.probability,
            "composite_signal": self.composite_action,
            "composite_strength": self.composite_strength,
            "action": self.composite_action,
            "current_price": current_price,
            "price_targets": dict(self.price_targets or {}),
            "expected_regime_duration": self.expected_duration,
            "transition_risk": self.transition_risk,
            "previous_regime": self.previous_regime,
            "p_bull_day5": self.p_bull_day5,
            "p_bear_day5": self.p_bear_day5,
            "p_neutral_day5": self.p_neutral_day5,
            "atr_14": self.atr_14,
            "meta_labeler_probability": self.meta_labeler_probability,
            "signal_source": self.signal_source,
            "forward_signal_source": self.signal_source,
            "seed_agreement": self.seed_agreement,
            "regime_ambiguous": self.regime_ambiguous,
        }


@dataclass
class PipelinePosition:
    ticker: str
    quantity: float
    entry_price: float
    entry_date: str
    entry_idx: int
    stop_price: float | None
    target_price: float | None
    risk_reward_ratio: float | None
    timeframe_days: int
    atr_14: float | None
    trade_geometry_source: str
    entry_signal_source: str | None = None

    def market_value(self, price: float) -> float:
        return float(self.quantity) * float(price)


@dataclass(frozen=True)
class PipelineTrade:
    ticker: str
    entry_date: str
    exit_date: str
    quantity: float
    entry_price: float
    exit_price: float
    gross_pnl: float
    net_pnl: float
    holding_days: int
    exit_type: str
    costs_paid: float
    stop_price: float | None = None
    target_price: float | None = None
    rationale: str = ""
    entry_signal_source: str | None = None


@dataclass(frozen=True)
class PipelineBacktestResult:
    ticker: str
    config: dict[str, Any]
    metrics: dict[str, Any]
    in_sample: dict[str, Any]
    out_of_sample: dict[str, Any] | None
    trades: list[dict[str, Any]]
    equity_curve: list[dict[str, Any]]
    exit_type_counts: dict[str, int]
    gate_counts: dict[str, int]
    stress_windows: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "ticker": self.ticker,
            "config": self.config,
            "metrics": self.metrics,
            "in_sample": self.in_sample,
            "out_of_sample": self.out_of_sample,
            "trades": self.trades,
            "equity_curve": self.equity_curve,
            "exit_type_counts": self.exit_type_counts,
            "gate_counts": self.gate_counts,
            "stress_windows": self.stress_windows,
        }

    def to_json(self, path: str | Path | None = None) -> str:
        payload = json.dumps(self.to_dict(), indent=2)
        if path is not None:
            Path(path).write_text(payload + "\n", encoding="utf-8")
        return payload


SignalProvider = Callable[[str, pd.Timestamp, pd.DataFrame, PipelineBacktestConfig, str | None], PipelineSignal | dict[str, Any] | None]


def _to_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        parsed = float(value)
    except Exception:
        return None
    return parsed if math.isfinite(parsed) else None


def _positive_float(value: Any) -> float | None:
    parsed = _to_float(value)
    return parsed if parsed is not None and parsed > 0 else None


def _normalize_market_frame(market_frame: pd.DataFrame) -> pd.DataFrame:
    frame = market_frame.copy()
    if not isinstance(frame.index, pd.DatetimeIndex):
        frame.index = pd.to_datetime(frame.index)
    rename = {
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "price",
        "Adj Close": "price",
        "Volume": "volume",
    }
    frame = frame.rename(columns={column: rename.get(str(column), str(column)) for column in frame.columns})
    if "price" not in frame.columns and "close" in frame.columns:
        frame["price"] = frame["close"]
    if "open" not in frame.columns:
        frame["open"] = frame["price"]
    if "high" not in frame.columns:
        frame["high"] = frame[["open", "price"]].max(axis=1)
    if "low" not in frame.columns:
        frame["low"] = frame[["open", "price"]].min(axis=1)
    if "volume" not in frame.columns:
        frame["volume"] = 1_000_000.0
    if "vix" not in frame.columns:
        frame["vix"] = 20.0
    if "yield_10y" not in frame.columns:
        frame["yield_10y"] = 4.0
    for column in ("open", "high", "low", "price", "volume", "vix", "yield_10y"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame = frame.sort_index().dropna(subset=["open", "high", "low", "price"])
    return frame[["open", "high", "low", "price", "volume", "vix", "yield_10y"]]


def _record_from_dataclass(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if hasattr(value, "__dataclass_fields__"):
        return asdict(value)
    if isinstance(value, dict):
        return dict(value)
    return dict(getattr(value, "__dict__", {}) or {})


def _forward_probability_context(curve: pd.DataFrame) -> dict[str, float | None]:
    if curve is None or curve.empty:
        return {"p_bull_day5": None, "p_neutral_day5": None, "p_bear_day5": None}
    day5 = curve.iloc[min(4, len(curve) - 1)]
    return {
        "p_bull_day5": _to_float(day5.get("p_bull")),
        "p_neutral_day5": _to_float(day5.get("p_neutral")),
        "p_bear_day5": _to_float(day5.get("p_bear")),
    }


def pure_check_hurdle_rate(
    ticker: str,
    entry_price: float | None,
    exit_price: float | None,
    *,
    estimated_stcg_rate: float = DEFAULT_ESTIMATED_STCG_RATE,
    min_net_return_pct: float = DEFAULT_MIN_NET_RETURN_PCT,
    estimated_execution_cost_pct: float = 0.0,
) -> HurdleRateResult:
    tax_rate = max(0.0, min(0.99, float(estimated_stcg_rate)))
    minimum = max(0.0, min(50.0, float(min_net_return_pct)))
    execution_cost = max(0.0, float(estimated_execution_cost_pct or 0.0))
    normalized_ticker = str(ticker or "").upper()
    if entry_price is None or exit_price is None or float(entry_price) <= 0:
        return HurdleRateResult(
            ticker=normalized_ticker,
            gross_return_pct=None,
            estimated_stcg_rate=tax_rate,
            estimated_execution_cost_pct=execution_cost,
            net_return_pct=None,
            min_net_return_pct=minimum,
            passed=True,
            reason="Insufficient price data - pass by default",
        )
    entry_value = float(entry_price)
    exit_value = float(exit_price)
    gross_return_pct = ((exit_value - entry_value) / entry_value) * 100.0
    net_return_pct = (gross_return_pct - execution_cost) * (1.0 - tax_rate)
    passed = net_return_pct >= minimum
    comparator = ">=" if passed else "<"
    return HurdleRateResult(
        ticker=normalized_ticker,
        gross_return_pct=gross_return_pct,
        estimated_stcg_rate=tax_rate,
        estimated_execution_cost_pct=execution_cost,
        net_return_pct=net_return_pct,
        min_net_return_pct=minimum,
        passed=passed,
        reason=(
            f"Net return {net_return_pct:.2f}% {comparator} minimum {minimum:.2f}% "
            f"(gross {gross_return_pct:.2f}% - exec {execution_cost:.2f}% @ tax {tax_rate:.0%})"
        ),
    )


def pure_check_duration_gate(
    ticker: str,
    expected_regime_duration: float | None,
    regime_label: str,
    *,
    min_regime_duration_days: float = DEFAULT_MIN_REGIME_DURATION_DAYS,
) -> DurationGateResult:
    minimum = max(1.0, min(90.0, float(min_regime_duration_days)))
    normalized_ticker = str(ticker or "").upper()
    normalized_regime = str(regime_label or "")
    if expected_regime_duration is None or float(expected_regime_duration) <= 0:
        return DurationGateResult(
            ticker=normalized_ticker,
            expected_regime_duration=None,
            min_regime_duration_days=minimum,
            regime_label=normalized_regime,
            passed=True,
            reason="No duration estimate - pass by default",
        )
    duration_value = float(expected_regime_duration)
    if normalized_regime != "Bull":
        return DurationGateResult(
            ticker=normalized_ticker,
            expected_regime_duration=duration_value,
            min_regime_duration_days=minimum,
            regime_label=normalized_regime,
            passed=True,
            reason="Duration gate only applies to Bull regime entries",
        )
    passed = duration_value >= minimum
    comparator = ">=" if passed else "<"
    return DurationGateResult(
        ticker=normalized_ticker,
        expected_regime_duration=duration_value,
        min_regime_duration_days=minimum,
        regime_label=normalized_regime,
        passed=passed,
        reason=f"Expected duration {duration_value:.1f} days {comparator} minimum {minimum:.1f} days",
    )


class _ProductionSignalProvider:
    def __init__(self) -> None:
        self._latest_result: Any | None = None
        self._last_refit_idx: int | None = None

    def __call__(
        self,
        ticker: str,
        date: pd.Timestamp,
        history: pd.DataFrame,
        config: PipelineBacktestConfig,
        previous_regime: str | None,
    ) -> PipelineSignal | None:
        del date
        try:
            features = build_features(history, lookback_window=config.lookback_window)
        except Exception:
            return None
        if len(features) < int(config.training_window):
            return None
        current_idx = len(features)
        should_refit = (
            self._latest_result is None
            or self._last_refit_idx is None
            or (current_idx - self._last_refit_idx) >= int(config.refit_step)
        )
        if should_refit:
            max_rows = int(config.training_window) + int(config.lookback_window) + max(1, int(config.refit_step))
            fit_history = history.tail(max_rows).copy() if len(history) > max_rows else history
            result = fit_regime_model(
                ticker=ticker,
                market_frame=fit_history,
                lookback_window=config.lookback_window,
                training_window=config.training_window,
                refit_step=config.refit_step,
                macro_weighting=config.macro_weighting,
                macro_weight=config.macro_weight,
                random_state=config.random_state,
                n_seeds=config.hmm_n_seeds,
                seed_agreement_min=config.seed_agreement_min,
                covariance_type=config.hmm_covariance_type,
            )
            self._latest_result = result
            self._last_refit_idx = current_idx
        else:
            result = self._decode_latest(ticker, history, config)
        return _signal_from_regime_result(ticker, result, history, previous_regime, config)

    def _decode_latest(self, ticker: str, history: pd.DataFrame, config: PipelineBacktestConfig) -> Any:
        if self._latest_result is None:
            raise InsufficientDataError("No fitted model is available for cached decode.")
        base = self._latest_result
        features = build_features(history, lookback_window=config.lookback_window)
        window = features.iloc[-int(config.training_window) :].copy()
        feature_cols = ["return", "volatility", "trend", "volume_zscore", "vix_change", "yield_10y_change"]
        scaled = base.scaler.transform(window[feature_cols].to_numpy())
        if config.macro_weighting:
            scaled[:, 4:6] *= float(config.macro_weight)
        decoded = pd.Series(base.model.predict(scaled), index=window.index)
        posteriors = base.model.predict_proba(scaled)
        hidden = int(decoded.iloc[-1])
        label = base.state_map[hidden]
        state_id = int(base.canonical_state_map[hidden])
        vector = np.zeros(3, dtype=float)
        for hidden_state, canonical_state in base.canonical_state_map.items():
            vector[int(canonical_state)] = float(posteriors[-1, int(hidden_state)])
        transition = np.zeros((3, 3), dtype=float)
        for from_hidden, from_canonical in base.canonical_state_map.items():
            for to_hidden, to_canonical in base.canonical_state_map.items():
                transition[int(from_canonical), int(to_canonical)] = float(base.model.transmat_[int(from_hidden), int(to_hidden)])
        stay = float(transition[state_id, state_id])
        expected_duration = 999.0 if stay >= 0.999999 else min(999.0, 1.0 / max(1e-9, 1.0 - stay))
        regime_days = 0
        for item in decoded.iloc[::-1]:
            if base.state_map[int(item)] != label:
                break
            regime_days += 1
        recent = window.tail(20).assign(hidden_state=decoded.tail(20).to_numpy())
        mean_return = recent.loc[recent["hidden_state"] == hidden, "return"].mean()
        return SimpleNamespace(
            ticker=ticker,
            latest_label=label,
            latest_state_id=state_id,
            latest_probability=float(posteriors[-1, hidden]),
            latest_price=float(window["price"].iloc[-1]),
            latest_state_vector=vector,
            transition_matrix=transition,
            expected_regime_duration=expected_duration,
            transition_risk=max(0.0, min(1.0, 1.0 - stay)),
            regime_days=max(1, regime_days),
            recent_state_mean_return=float(mean_return) if pd.notna(mean_return) else None,
            empirical_duration_quantiles=getattr(base, "empirical_duration_quantiles", None),
            seed_agreement=float(getattr(base, "seed_agreement", 1.0) or 1.0),
            regime_ambiguous=bool(getattr(base, "regime_ambiguous", False)),
        )


def _signal_from_regime_result(
    ticker: str,
    result: Any,
    history: pd.DataFrame,
    previous_regime: str | None,
    config: PipelineBacktestConfig,
) -> PipelineSignal:
    curve = forward_regime_curve(result.transition_matrix, result.latest_state_vector, horizon=21)
    forward = signal_from_forward_curve(
        curve,
        result.latest_label,
        result.transition_risk,
        result.expected_regime_duration,
        result.latest_probability,
        thresholds=config.signal_thresholds,
        empirical_duration_quantiles=getattr(result, "empirical_duration_quantiles", None),
    )
    technicals = compute_technicals(history["price"], history["volume"], history["high"], history["low"])
    technical = intra_regime_signal(technicals, result.latest_label)
    composite = build_composite_signal(
        result.latest_label,
        result.latest_probability,
        forward,
        technical,
        adjustments_enabled=config.composite_adjustments_enabled,
    )
    targets = compute_price_targets(
        current_price=float(result.latest_price),
        technicals_df=technicals,
        composite_signal=composite,
        expected_duration=float(result.expected_regime_duration),
        state_mean_return=float(result.recent_state_mean_return or 0.0),
    )
    probabilities = _forward_probability_context(curve)
    return PipelineSignal(
        date=pd.Timestamp(history.index[-1]).date().isoformat(),
        regime=str(result.latest_label),
        probability=float(result.latest_probability),
        composite_action=str(composite.composite_action),
        composite_strength=float(composite.composite_strength),
        expected_duration=float(result.expected_regime_duration),
        transition_risk=float(result.transition_risk),
        regime_days=int(result.regime_days),
        state_mean_return=result.recent_state_mean_return,
        previous_regime=previous_regime,
        forward_action=str(forward.action),
        technical_signal=technical,
        signal_source=str(getattr(forward, "source", "") or ""),
        seed_agreement=_to_float(getattr(result, "seed_agreement", None)),
        regime_ambiguous=bool(getattr(result, "regime_ambiguous", False)),
        price_targets=_record_from_dataclass(targets),
        atr_14=_to_float(_record_from_dataclass(targets).get("atr_value")),
        **probabilities,
    )


def _coerce_signal(raw: PipelineSignal | dict[str, Any] | None, date: pd.Timestamp) -> PipelineSignal | None:
    if raw is None:
        return None
    if isinstance(raw, PipelineSignal):
        return raw
    row = dict(raw)
    raw_targets = row.get("price_targets")
    targets: dict[str, Any] = dict(raw_targets) if isinstance(raw_targets, dict) else {}
    return PipelineSignal(
        date=str(row.get("date") or date.date().isoformat()),
        regime=str(row.get("regime") or row.get("regime_label") or "Neutral"),
        probability=float(row.get("probability") or row.get("regime_probability") or 0.0),
        composite_action=str(row.get("composite_action") or row.get("composite_signal") or row.get("action") or "Hold"),
        composite_strength=float(row.get("composite_strength") or 0.0),
        expected_duration=float(row.get("expected_duration") or row.get("expected_regime_duration") or DEFAULT_EXIT_TIME_STOP_DAYS),
        transition_risk=float(row.get("transition_risk") or 0.0),
        regime_days=int(row.get("regime_days") or 1),
        state_mean_return=_to_float(row.get("state_mean_return") or row.get("recent_state_mean_return")),
        previous_regime=row.get("previous_regime"),
        p_bull_day5=_to_float(row.get("p_bull_day5")),
        p_bear_day5=_to_float(row.get("p_bear_day5")),
        p_neutral_day5=_to_float(row.get("p_neutral_day5")),
        forward_action=row.get("forward_action") or row.get("forward_signal"),
        technical_signal=row.get("technical_signal"),
        signal_source=row.get("signal_source") or row.get("forward_signal_source"),
        seed_agreement=_to_float(row.get("seed_agreement")),
        regime_ambiguous=bool(row.get("regime_ambiguous", False)),
        price_targets=dict(targets),
        atr_14=_to_float(row.get("atr_14") or targets.get("atr_value")),
        beta=_to_float(row.get("beta")),
    )


def _exit_price(raw_price: float, cost_bps: float, enabled: bool) -> tuple[float, float]:
    cost = max(0.0, float(cost_bps if enabled else 0.0)) / 10_000.0
    fill = float(raw_price) * (1.0 - cost)
    return fill, max(0.0, float(raw_price) - fill)


def _entry_price(raw_price: float, cost_bps: float, enabled: bool) -> tuple[float, float]:
    cost = max(0.0, float(cost_bps if enabled else 0.0)) / 10_000.0
    fill = float(raw_price) * (1.0 + cost)
    return fill, max(0.0, fill - float(raw_price))


def _close_position(
    *,
    ticker: str,
    position: PipelinePosition,
    raw_exit_price: float,
    exit_date: str,
    exit_idx: int,
    exit_type: str,
    config: PipelineBacktestConfig,
    rationale: str,
    quantity: float | None = None,
) -> tuple[PipelineTrade, float, float]:
    sell_quantity = min(float(position.quantity), float(quantity if quantity is not None else position.quantity))
    fill_price, per_share_cost = _exit_price(raw_exit_price, config.exit_cost_bps, config.enable_cost_model)
    gross_pnl = (float(raw_exit_price) - position.entry_price) * sell_quantity
    net_pnl = (fill_price - position.entry_price) * sell_quantity
    costs = per_share_cost * sell_quantity
    trade = PipelineTrade(
        ticker=ticker,
        entry_date=position.entry_date,
        exit_date=exit_date,
        quantity=sell_quantity,
        entry_price=round(position.entry_price, 4),
        exit_price=round(fill_price, 4),
        gross_pnl=round(gross_pnl, 4),
        net_pnl=round(net_pnl, 4),
        # Calendar days, matching production paper_trading._holding_days semantics.
        holding_days=max(0, int((pd.Timestamp(exit_date) - pd.Timestamp(position.entry_date)).days)),
        exit_type=exit_type,
        costs_paid=round(costs, 4),
        stop_price=position.stop_price,
        target_price=position.target_price,
        rationale=rationale,
        entry_signal_source=position.entry_signal_source,
    )
    cash_delta = fill_price * sell_quantity
    return trade, cash_delta, costs


def _entry_allowed(
    ticker: str,
    signal: PipelineSignal,
    signal_row: dict[str, Any],
    current_price: float,
    proposed_entry: float,
    round_trip_dates: list[pd.Timestamp],
    current_date: pd.Timestamp,
    config: PipelineBacktestConfig,
) -> tuple[bool, str]:
    if signal.composite_action not in {"Buy", "Strong Buy"}:
        return False, "not_buy"
    if signal.regime_ambiguous:
        return False, "regime_ambiguous"
    if config.enable_signal_quality_gate:
        timestamp = current_date.replace(hour=16, minute=0, second=0).isoformat()
        quality = evaluate_signal_quality(
            signal_row,
            action="Buy",
            source="pipeline_backtest",
            current_price=current_price,
            reference_price=proposed_entry,
            source_timestamp=timestamp,
            now=current_date.replace(hour=16, minute=0, second=0).to_pydatetime(),
        )
        if not quality.actionable or quality.score < float(config.min_signal_quality_score):
            return False, f"signal_quality:{quality.grade}:{quality.summary()}"
    total_cost_pct = (float(config.entry_cost_bps) + float(config.exit_cost_bps)) / 100.0 if config.enable_cost_model else 0.0
    target_price = _positive_float(signal_row.get("target_price"))
    raw_targets = signal_row.get("price_targets")
    targets: dict[str, Any] = dict(raw_targets) if isinstance(raw_targets, dict) else {}
    target_price = target_price or _positive_float(targets.get("target_price") or targets.get("exit_price"))
    if config.enable_hurdle_gate:
        hurdle = pure_check_hurdle_rate(
            ticker,
            proposed_entry,
            target_price,
            estimated_stcg_rate=config.estimated_stcg_rate,
            min_net_return_pct=config.hurdle_min_net_return_pct,
            estimated_execution_cost_pct=total_cost_pct,
        )
        if not hurdle.passed:
            return False, f"hurdle:{hurdle.reason}"
    if config.enable_duration_gate:
        duration = pure_check_duration_gate(
            ticker,
            signal.expected_duration,
            signal.regime,
            min_regime_duration_days=config.min_regime_duration_days,
        )
        if not duration.passed:
            return False, f"duration:{duration.reason}"
    if config.enable_anti_churn_gate:
        cutoff = current_date - pd.Timedelta(days=max(1, int(config.anti_churn_cooldown_days)))
        recent_round_trips = [item for item in round_trip_dates if item >= cutoff]
        if len(recent_round_trips) >= int(config.anti_churn_max_round_trips_30d):
            return False, "anti_churn:round_trip_limit"
    return True, "passed"


def _build_entry_order(
    ticker: str,
    signal: PipelineSignal,
    signal_row: dict[str, Any],
    proposed_price: float,
    cash: float,
    config: PipelineBacktestConfig,
) -> dict[str, Any] | None:
    role_budget = min(float(cash), float(config.starting_cash) * max(0.0, min(1.0, float(config.max_position_pct))))
    if role_budget <= 0 or proposed_price <= 0:
        return None
    atr = _positive_float(signal.atr_14)
    if str(config.sizing_method or "").lower() == "risk_budget":
        quantity = _risk_adjusted_quantity(
            role_budget,
            proposed_price,
            atr,
            signal.beta,
            risk_per_share_multiplier=config.sizing_atr_multiplier,
            base_risk_fraction=config.sizing_base_risk_fraction,
        )
    else:
        quantity = math.floor(role_budget / proposed_price)
    # ML size scaling (0.5-1.0x), mirroring compute_position_size: applied only
    # when a meta-labeler probability accompanies the signal, so baseline runs
    # without the labeler are unaffected.
    ml_probability = signal.meta_labeler_probability
    if ml_probability is not None and quantity > 0:
        clamped = max(0.0, min(1.0, float(ml_probability)))
        quantity = math.floor(quantity * (0.5 + 0.5 * clamped))
    if quantity <= 0:
        return None
    return {"ticker": ticker, "quantity": int(quantity), "signal": signal, "signal_row": signal_row}


def _manage_position(
    ticker: str,
    position: PipelinePosition,
    signal: PipelineSignal | None,
    bar: pd.Series,
    date: pd.Timestamp,
    idx: int,
    config: PipelineBacktestConfig,
) -> tuple[PipelineTrade | None, PipelinePosition | None, float]:
    high = float(bar["high"])
    low = float(bar["low"])
    close = float(bar["price"])
    exit_date = date.date().isoformat()
    stop_price = _positive_float(position.stop_price)
    target_price = _positive_float(position.target_price)
    target_touched = bool(config.profit_target and target_price is not None and high >= target_price)
    # Conservative intraday ordering: today's bar is tested against the stop as it
    # stood at the PRIOR close. Ratcheting from today's high before testing today's
    # low would assume the high printed first — an optimistic fill on trailing
    # exits. The ratchet (from today's close, mirroring production's snapshot-price
    # ratchet) happens at the end of this function, effective tomorrow.
    stop_touched = bool(stop_price is not None and low <= stop_price)
    if stop_touched:
        assert stop_price is not None
        exit_type = "trailing" if stop_price is not None and stop_price > position.entry_price else "stop"
        trade, _cash_delta, costs = _close_position(
            ticker=ticker,
            position=position,
            raw_exit_price=float(stop_price),
            exit_date=exit_date,
            exit_idx=idx,
            exit_type=exit_type,
            config=config,
            rationale="Stop touched intraday; stop wins if target also touched.",
        )
        return trade, None, costs
    if target_touched and target_price is not None:
        trade, _cash_delta, costs = _close_position(
            ticker=ticker,
            position=position,
            raw_exit_price=float(target_price),
            exit_date=exit_date,
            exit_idx=idx,
            exit_type="target",
            config=config,
            rationale="Profit target touched intraday.",
        )
        return trade, None, costs
    # Calendar days, matching production paper_trading._holding_days semantics
    # (trading bars would stretch a 21-day stop to ~30 calendar days).
    holding_calendar_days = max(0, int((date.normalize() - pd.Timestamp(position.entry_date).normalize()).days))
    if holding_calendar_days >= int(position.timeframe_days or config.time_stop_days):
        trade, _cash_delta, costs = _close_position(
            ticker=ticker,
            position=position,
            raw_exit_price=close,
            exit_date=exit_date,
            exit_idx=idx,
            exit_type="time",
            config=config,
            rationale=f"Time stop reached ({holding_calendar_days}d).",
        )
        return trade, None, costs
    if signal is not None:
        row = signal.to_signal_row(close)
        if signal.regime == "Bear" or signal.composite_action in {"Sell", "Strong Sell"}:
            trade, _cash_delta, costs = _close_position(
                ticker=ticker,
                position=position,
                raw_exit_price=close,
                exit_date=exit_date,
                exit_idx=idx,
                exit_type="regime",
                config=config,
                rationale="Regime or composite signal flipped defensive.",
            )
            return trade, None, costs
        neutral_reason = _neutral_reduce_reason(row)
        if neutral_reason is not None:
            reduced_quantity = _reduced_exit_quantity(position.quantity, config.neutral_reduce_fraction)
            if reduced_quantity > 0:
                trade, _cash_delta, costs = _close_position(
                    ticker=ticker,
                    position=position,
                    raw_exit_price=close,
                    exit_date=exit_date,
                    exit_idx=idx,
                    exit_type="reduce",
                    config=config,
                    rationale=neutral_reason,
                    quantity=reduced_quantity,
                )
                remaining = position.quantity - reduced_quantity
                if remaining <= 0:
                    return trade, None, costs
                position.quantity = remaining
                _ratchet_position_stop(position, close, config)
                return trade, position, costs
    _ratchet_position_stop(position, close, config)
    return None, position, 0.0


def _ratchet_position_stop(position: PipelinePosition, close: float, config: PipelineBacktestConfig) -> None:
    """End-of-day trailing ratchet from the close, effective from the next bar.

    Mirrors production, which ratchets from a price snapshot during the daily
    run rather than from the intraday high.
    """
    if position.atr_14 is None:
        return
    ratcheted = trailing_stop_level(
        entry_price=position.entry_price,
        current_price=close,
        atr_14=position.atr_14,
        existing_stop=_positive_float(position.stop_price),
        atr_multiplier=config.trailing_atr_multiplier,
        activation_atr=config.trailing_activation_atr,
    )
    if ratcheted is not None:
        position.stop_price = ratcheted


def compute_equity_metrics(
    equity_curve: pd.DataFrame,
    trades: list[dict[str, Any]] | None = None,
    *,
    benchmark_curve: pd.DataFrame | None = None,
    risk_free_rate: float = 0.0,
) -> dict[str, Any]:
    if equity_curve.empty:
        return {}
    curve = equity_curve.copy()
    curve.index = pd.to_datetime(curve["date"]) if "date" in curve.columns else pd.to_datetime(curve.index)
    equity = pd.to_numeric(curve["equity"], errors="coerce").dropna()
    if equity.empty:
        return {}
    daily_returns = equity.pct_change().dropna()
    mean_daily = float(daily_returns.mean()) if not daily_returns.empty else 0.0
    std_daily = float(daily_returns.std(ddof=1)) if len(daily_returns) > 1 else 0.0
    daily_rf = float(risk_free_rate or 0.0) / 252.0
    sharpe = None
    if std_daily > 0:
        sharpe = ((mean_daily - daily_rf) * 252.0) / (std_daily * math.sqrt(252.0))
    total_return = float(equity.iloc[-1] / equity.iloc[0] - 1.0) if equity.iloc[0] else 0.0
    periods = max(1, len(equity) - 1)
    annualized_return = (1.0 + total_return) ** (252.0 / periods) - 1.0 if total_return > -1.0 else -1.0
    annualized_volatility = std_daily * math.sqrt(252.0) if std_daily > 0 else 0.0
    drawdown = equity / equity.cummax() - 1.0
    trade_rows = list(trades or [])
    wins = [row for row in trade_rows if float(row.get("net_pnl") or 0.0) > 0]
    losses = [row for row in trade_rows if float(row.get("net_pnl") or 0.0) < 0]
    neutral_tilt_trades = [row for row in trade_rows if str(row.get("entry_signal_source") or "") == "neutral_bull_tilt"]
    gross_profit = sum(float(row.get("net_pnl") or 0.0) for row in wins)
    gross_loss = abs(sum(float(row.get("net_pnl") or 0.0) for row in losses))
    benchmark_total_return = None
    excess_return = None
    information_ratio = None
    if benchmark_curve is not None and not benchmark_curve.empty:
        bench = benchmark_curve.copy()
        bench.index = pd.to_datetime(bench["date"]) if "date" in bench.columns else pd.to_datetime(bench.index)
        benchmark_equity = pd.to_numeric(bench["equity"], errors="coerce").dropna()
        benchmark_equity = benchmark_equity.reindex(equity.index).ffill().dropna()
        aligned_equity = equity.reindex(benchmark_equity.index).dropna()
        benchmark_equity = benchmark_equity.reindex(aligned_equity.index)
        if len(aligned_equity) >= 2 and benchmark_equity.iloc[0] != 0:
            benchmark_total_return = float(benchmark_equity.iloc[-1] / benchmark_equity.iloc[0] - 1.0)
            excess_return = total_return - benchmark_total_return
            active = aligned_equity.pct_change().dropna() - benchmark_equity.pct_change().dropna()
            active_std = float(active.std(ddof=1)) if len(active) > 1 else 0.0
            if active_std > 0:
                information_ratio = (float(active.mean()) * 252.0) / (active_std * math.sqrt(252.0))
    return {
        "total_return": total_return,
        "annualized_return": annualized_return,
        "annualized_volatility": annualized_volatility,
        "sharpe_ratio": sharpe,
        "max_drawdown": float(drawdown.min()) if not drawdown.empty else 0.0,
        "win_rate": (len(wins) / len(trade_rows)) if trade_rows else None,
        "profit_factor": (gross_profit / gross_loss) if gross_loss > 0 else (None if gross_profit == 0 else math.inf),
        "avg_holding_days": float(np.mean([float(row.get("holding_days") or 0.0) for row in trade_rows])) if trade_rows else None,
        "exposure_pct": float(pd.to_numeric(curve.get("exposure", pd.Series(index=curve.index, data=0.0)), errors="coerce").fillna(0.0).mean()),
        "total_costs_paid": float(sum(float(row.get("costs_paid") or 0.0) for row in trade_rows)),
        "trade_count": len(trade_rows),
        "neutral_tilt_trade_count": len(neutral_tilt_trades),
        "neutral_tilt_net_pnl": float(sum(float(row.get("net_pnl") or 0.0) for row in neutral_tilt_trades)),
        "benchmark_total_return": benchmark_total_return,
        "excess_return_vs_benchmark": excess_return,
        "information_ratio": information_ratio,
        "risk_free_rate_assumption": risk_free_rate,
    }


def _benchmark_curve(frame: pd.DataFrame, starting_cash: float) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame()
    first = float(frame["price"].iloc[0])
    shares = float(starting_cash) / first if first > 0 else 0.0
    rows = [
        {
            "date": pd.Timestamp(index).date().isoformat(),
            "equity": shares * float(row["price"]),
        }
        for index, row in frame.iterrows()
    ]
    return pd.DataFrame(rows)


def _segment_metrics(
    equity_df: pd.DataFrame,
    trades: list[dict[str, Any]],
    benchmark_df: pd.DataFrame | None,
    start: pd.Timestamp | None,
    end: pd.Timestamp | None,
    risk_free_rate: float,
) -> dict[str, Any]:
    curve = equity_df.copy()
    dates = pd.to_datetime(curve["date"])
    mask = pd.Series(True, index=curve.index)
    if start is not None:
        mask &= dates >= start
    if end is not None:
        mask &= dates < end
    segment_curve = curve.loc[mask].copy()
    segment_trades = [
        row for row in trades
        if (start is None or pd.Timestamp(row["exit_date"]) >= start)
        and (end is None or pd.Timestamp(row["exit_date"]) < end)
    ]
    bench_segment = None
    if benchmark_df is not None and not benchmark_df.empty:
        bench_dates = pd.to_datetime(benchmark_df["date"])
        bench_mask = pd.Series(True, index=benchmark_df.index)
        if start is not None:
            bench_mask &= bench_dates >= start
        if end is not None:
            bench_mask &= bench_dates < end
        bench_segment = benchmark_df.loc[bench_mask].copy()
    return compute_equity_metrics(segment_curve, segment_trades, benchmark_curve=bench_segment, risk_free_rate=risk_free_rate)


def _stress_window_results(
    equity_df: pd.DataFrame,
    trades: list[dict[str, Any]],
    benchmark_df: pd.DataFrame | None,
    windows: list[StressWindow],
    risk_free_rate: float,
) -> list[dict[str, Any]]:
    if equity_df.empty:
        return []
    results: list[dict[str, Any]] = []
    curve = equity_df.copy()
    dates = pd.to_datetime(curve["date"])
    for window in windows:
        start = pd.Timestamp(window.start)
        end_inclusive = pd.Timestamp(window.end)
        end_exclusive = end_inclusive + pd.Timedelta(days=1)
        mask = (dates >= start) & (dates < end_exclusive)
        segment_curve = curve.loc[mask].copy()
        if segment_curve.empty:
            continue
        metrics = _segment_metrics(equity_df, trades, benchmark_df, start, end_exclusive, risk_free_rate)
        benchmark_metrics = {}
        if benchmark_df is not None and not benchmark_df.empty:
            benchmark_metrics = _segment_metrics(benchmark_df, [], benchmark_df, start, end_exclusive, risk_free_rate)
        segment_trades = [
            row for row in trades
            if pd.Timestamp(row["exit_date"]) >= start and pd.Timestamp(row["exit_date"]) < end_exclusive
        ]
        exit_type_counts: dict[str, int] = {}
        for row in segment_trades:
            exit_type = str(row.get("exit_type") or "unknown")
            exit_type_counts[exit_type] = exit_type_counts.get(exit_type, 0) + 1
        days_to_bear_flag = None
        if "signal_regime" in segment_curve.columns:
            bear_rows = segment_curve.loc[segment_curve["signal_regime"].astype(str) == "Bear"]
            if not bear_rows.empty:
                days_to_bear_flag = int((pd.Timestamp(bear_rows["date"].iloc[0]) - start).days)
        results.append(
            {
                "key": window.key,
                "label": window.label,
                "start": window.start,
                "end": window.end,
                "metrics": metrics,
                "benchmark": benchmark_metrics,
                "days_to_bear_flag": days_to_bear_flag,
                "strategy_max_drawdown": metrics.get("max_drawdown"),
                "benchmark_max_drawdown": benchmark_metrics.get("max_drawdown"),
                "strategy_total_return": metrics.get("total_return"),
                "benchmark_total_return": benchmark_metrics.get("total_return"),
                "exposure_pct": metrics.get("exposure_pct"),
                "exit_type_counts": exit_type_counts,
                "trade_count": len(segment_trades),
            }
        )
    return results


def run_pipeline_backtest(
    ticker: str,
    market_frame: pd.DataFrame,
    *,
    config: PipelineBacktestConfig | None = None,
    benchmark_frame: pd.DataFrame | None = None,
    signal_provider: SignalProvider | None = None,
) -> PipelineBacktestResult:
    cfg = config or PipelineBacktestConfig()
    np.random.seed(int(cfg.random_state))
    frame = _normalize_market_frame(market_frame)
    if frame.empty:
        raise ValueError("market_frame is empty after normalization.")
    benchmark = _normalize_market_frame(benchmark_frame) if benchmark_frame is not None else frame
    universe_eligibility = (
        check_universe_eligibility(ticker, market_frame=frame, use_cache=False)
        if bool(cfg.enforce_universe_screen)
        else None
    )
    provider = signal_provider or _ProductionSignalProvider()
    cash = float(cfg.starting_cash)
    position: PipelinePosition | None = None
    pending_entry: dict[str, Any] | None = None
    trades: list[dict[str, Any]] = []
    equity_rows: list[dict[str, Any]] = []
    exit_counts: dict[str, int] = {}
    gate_counts: dict[str, int] = {}
    round_trip_dates: list[pd.Timestamp] = []
    previous_regime: str | None = None
    total_costs = 0.0
    start_idx = max(0, min(int(cfg.training_window), max(0, len(frame) - 1)))

    for idx in range(start_idx, len(frame)):
        date = pd.Timestamp(frame.index[idx])
        bar = frame.iloc[idx]
        if pending_entry is not None and int(pending_entry["fill_idx"]) <= idx:
            raw_open = float(bar["open"])
            fill_price, per_share_cost = _entry_price(raw_open, cfg.entry_cost_bps, cfg.enable_cost_model)
            quantity = int(pending_entry["quantity"])
            if fill_price * quantity > cash:
                quantity = math.floor(cash / fill_price) if fill_price > 0 else 0
            if quantity > 0:
                signal = pending_entry["signal"]
                signal_row = pending_entry["signal_row"]
                geometry = _actual_fill_trade_geometry(
                    fill_price,
                    signal_row=signal_row,
                    atr_14=signal.atr_14,
                    atr_multiplier=cfg.sizing_atr_multiplier,
                )
                position = PipelinePosition(
                    ticker=ticker,
                    quantity=float(quantity),
                    entry_price=round(fill_price, 4),
                    entry_date=date.date().isoformat(),
                    entry_idx=idx,
                    stop_price=_positive_float(geometry.get("stop_price")),
                    target_price=_positive_float(geometry.get("target_price")),
                    risk_reward_ratio=_to_float(geometry.get("risk_reward_ratio")),
                    timeframe_days=int(geometry.get("timeframe_days") or cfg.time_stop_days),
                    atr_14=_positive_float(signal.atr_14),
                    trade_geometry_source=str(geometry.get("trade_geometry_source") or ""),
                    entry_signal_source=signal.signal_source,
                )
                entry_cost = per_share_cost * quantity
                total_costs += entry_cost
                cash -= fill_price * quantity
            pending_entry = None

        history = frame.iloc[: idx + 1]
        signal = _coerce_signal(provider(ticker, date, history, cfg, previous_regime), date)
        if signal is not None:
            previous_regime = signal.regime

        if position is not None:
            trade, updated_position, exit_cost = _manage_position(ticker, position, signal, bar, date, idx, cfg)
            if trade is not None:
                trade_dict = asdict(trade)
                trades.append(trade_dict)
                exit_counts[trade.exit_type] = exit_counts.get(trade.exit_type, 0) + 1
                total_costs += exit_cost
                cash += trade.exit_price * trade.quantity
                if trade.exit_type != "reduce" or updated_position is None:
                    round_trip_dates.append(date)
                position = updated_position

        if position is None and pending_entry is None and signal is not None:
            current_price = float(bar["price"])
            signal_row = signal.to_signal_row(current_price)
            raw_targets = signal_row.get("price_targets")
            targets: dict[str, Any] = dict(raw_targets) if isinstance(raw_targets, dict) else {}
            proposed_entry = _positive_float(targets.get("entry_price")) or current_price
            allowed, reason = _entry_allowed(ticker, signal, signal_row, current_price, proposed_entry, round_trip_dates, date, cfg)
            if allowed and universe_eligibility is not None and not universe_eligibility.eligible:
                allowed = False
                reason = "universe:" + ",".join(universe_eligibility.reasons)
            if not allowed:
                gate = reason.split(":", 1)[0]
                gate_counts[gate] = gate_counts.get(gate, 0) + 1
            else:
                order = _build_entry_order(ticker, signal, signal_row, current_price, cash, cfg)
                if order is not None:
                    fill_idx = min(idx + 1, len(frame) - 1)
                    order["fill_idx"] = fill_idx
                    pending_entry = order

        close_price = float(bar["price"])
        market_value = position.market_value(close_price) if position is not None else 0.0
        equity_rows.append(
            {
                "date": date.date().isoformat(),
                "cash": round(cash, 4),
                "market_value": round(market_value, 4),
                "equity": round(cash + market_value, 4),
                "exposure": 1.0 if position is not None else 0.0,
                "position_quantity": round(position.quantity, 6) if position is not None else 0.0,
                "signal_regime": signal.regime if signal is not None else None,
                "signal_action": signal.composite_action if signal is not None else None,
            }
        )

    if position is not None and len(frame) > 0:
        final_idx = len(frame) - 1
        final_date = pd.Timestamp(frame.index[-1])
        final_close = float(frame["price"].iloc[-1])
        trade, _cash_delta, exit_cost = _close_position(
            ticker=ticker,
            position=position,
            raw_exit_price=final_close,
            exit_date=final_date.date().isoformat(),
            exit_idx=final_idx,
            exit_type="final_mark",
            config=cfg,
            rationale="Closed at final bar for backtest accounting.",
        )
        trades.append(asdict(trade))
        exit_counts[trade.exit_type] = exit_counts.get(trade.exit_type, 0) + 1
        total_costs += exit_cost

    equity_df = pd.DataFrame(equity_rows)
    benchmark_df = _benchmark_curve(benchmark.loc[frame.index.intersection(benchmark.index)], cfg.starting_cash)
    metrics = compute_equity_metrics(equity_df, trades, benchmark_curve=benchmark_df, risk_free_rate=cfg.risk_free_rate)
    metrics["total_costs_paid"] = round(float(total_costs), 4)
    metrics["exit_type_counts"] = dict(exit_counts)
    oos_start = pd.Timestamp(cfg.oos_start) if cfg.oos_start else None
    in_sample = _segment_metrics(equity_df, trades, benchmark_df, None, oos_start, cfg.risk_free_rate)
    out_of_sample = _segment_metrics(equity_df, trades, benchmark_df, oos_start, None, cfg.risk_free_rate) if oos_start is not None else None
    stress_results = _stress_window_results(equity_df, trades, benchmark_df, get_stress_windows(), cfg.risk_free_rate)
    return PipelineBacktestResult(
        ticker=str(ticker or "").upper(),
        config=asdict(cfg),
        metrics=metrics,
        in_sample=in_sample,
        out_of_sample=out_of_sample,
        trades=trades,
        equity_curve=equity_rows,
        exit_type_counts=exit_counts,
        gate_counts=gate_counts,
        stress_windows=stress_results,
    )


def run_pipeline_backtest_for_ticker(
    ticker: str,
    *,
    period: str = "10y",
    start: str | dt.date | None = None,
    end: str | dt.date | None = None,
    cache: bool = False,
    oos_start: str | None = None,
    config: PipelineBacktestConfig | None = None,
    benchmark_ticker: str = "SPY",
) -> PipelineBacktestResult:
    cfg = config or PipelineBacktestConfig(oos_start=oos_start)
    if oos_start is not None and cfg.oos_start != oos_start:
        cfg = replace(cfg, oos_start=oos_start)
    market = download_market_frame(ticker=ticker, period=period, interval="1d", start=start, end=end, cache=cache).frame
    benchmark = download_market_frame(ticker=benchmark_ticker, period=period, interval="1d", start=start, end=end, cache=cache).frame if benchmark_ticker else None
    return run_pipeline_backtest(ticker, market, config=cfg, benchmark_frame=benchmark)
