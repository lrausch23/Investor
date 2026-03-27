from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import logging
from math import sqrt
from typing import Any

import numpy as np
import pandas as pd

from .config import DEFAULT_SIGNAL_THRESHOLDS, SignalThresholds
from .logging_config import setup_regime_logging
from .persistence import get_sentiment_history

setup_regime_logging()
logger = logging.getLogger(__name__)

REGIME_TO_INDEX = {"Bull": 0, "Neutral": 1, "Bear": 2}


@dataclass
class SignalResult:
    action: str
    timeframe: str
    strength: float
    expected_holding_days: int
    rationale: str
    transition_risk: float = 0.0
    expected_duration: float = 0.0


@dataclass
class CompositeSignal:
    regime_signal: str
    regime_probability: float
    forward_signal: SignalResult
    technical_signal: str
    composite_action: str
    composite_strength: float
    short_term_view: str
    medium_term_view: str
    weekly_regime: str | None = None
    multi_timeframe_note: str | None = None
    risk_reward_conflict: bool = False
    risk_reward_warning: str | None = None
    earnings_warning: str | None = None


@dataclass
class ConfidenceTrajectory:
    slope: float
    trend: str
    days_declining: int
    days_rising: int = 0
    short_ma_latest: float = 0.0
    long_ma_latest: float = 0.0


@dataclass
class TaxAdjustedSignal:
    original_action: str
    adjusted_action: str
    tax_note: str
    ltcg_threshold_date: str | None
    estimated_tax_impact: float
    wash_sale_warning: str | None
    account_name: str = ""
    account_type: str = ""
    tax_status: str = "—"


@dataclass
class SentimentMomentum:
    short_ma: float
    long_ma: float
    trend: str
    divergence_vs_regime: bool
    warning: str | None = None


@dataclass
class PriceTargets:
    current_price: float
    entry_price: float | None
    exit_price: float | None
    stop_price: float | None
    risk_reward_ratio: float | None
    timeframe_days: int
    atr_value: float | None
    confidence_multiplier: float
    price_position: str


@dataclass
class ConfidenceScore:
    """Unified confidence metric, always 0-100 scale."""

    value: float
    label: str
    calibrated: bool
    components: dict[str, float]


@dataclass
class PositionSize:
    """Dollar-amount position sizing based on regime confidence and risk."""

    suggested_pct: float
    suggested_dollars: float | None
    max_loss_dollars: float | None
    kelly_fraction: float | None
    sizing_rationale: str
    portfolio_adjustment: float = 1.0
    adjustment_rationale: str | None = None


def forward_regime_curve(transition_matrix: np.ndarray, current_state_vector: np.ndarray, horizon: int = 21) -> pd.DataFrame:
    rows = []
    current_state_vector = np.asarray(current_state_vector, dtype=float)
    transition_matrix = np.asarray(transition_matrix, dtype=float)
    for day in range(1, horizon + 1):
        projected = current_state_vector @ np.linalg.matrix_power(transition_matrix, day)
        rows.append(
            {
                "day": day,
                "p_bull": float(projected[0]),
                "p_neutral": float(projected[1]),
                "p_bear": float(projected[2]),
            }
        )
    return pd.DataFrame(rows)


def regime_crossover_day(forward_curve: pd.DataFrame, from_regime: str, to_regime: str) -> int | None:
    from_col = f"p_{from_regime.lower()}"
    to_col = f"p_{to_regime.lower()}"
    crossed = forward_curve.loc[forward_curve[to_col] > forward_curve[from_col], "day"]
    return int(crossed.iloc[0]) if not crossed.empty else None


def signal_from_forward_curve(
    forward_curve: pd.DataFrame,
    current_regime: str,
    transition_risk: float,
    expected_duration: float,
    current_probability: float,
    earnings_date: datetime | None = None,
    thresholds: SignalThresholds = DEFAULT_SIGNAL_THRESHOLDS,
) -> SignalResult:
    day5 = forward_curve.iloc[min(4, len(forward_curve) - 1)]
    day21 = forward_curve.iloc[min(20, len(forward_curve) - 1)]
    p_bull_day5 = float(day5["p_bull"])
    p_bear_day5 = float(day5["p_bear"])
    p_bull_day21 = float(day21["p_bull"])
    p_bear_day21 = float(day21["p_bear"])
    strength = max(0.0, min(1.0, abs(p_bull_day5 - p_bear_day5) * current_probability))

    def _with_earnings_adjustment(action: str, timeframe: str, signal_strength: float, holding_days: int, rationale: str) -> SignalResult:
        adjusted_strength = signal_strength
        adjusted_rationale = rationale
        if earnings_date is not None:
            now = datetime.now(timezone.utc)
            event = earnings_date if earnings_date.tzinfo is not None else earnings_date.replace(tzinfo=timezone.utc)
            days_to_earnings = max(0.0, (event - now).total_seconds() / 86400.0)
            if days_to_earnings <= max(holding_days, 0):
                adjusted_strength = max(0.0, signal_strength - thresholds.earnings_strength_penalty)
                adjusted_rationale = f"{rationale} Note: earnings on {event.date().isoformat()} fall within the expected holding period. Regime may shift on the event."
        return SignalResult(
            action,
            timeframe,
            adjusted_strength,
            holding_days,
            adjusted_rationale,
            transition_risk=float(transition_risk or 0.0),
            expected_duration=float(expected_duration or 0.0),
        )

    if current_regime == "Bull" and transition_risk < thresholds.strong_buy_max_transition_risk and expected_duration > thresholds.strong_buy_min_duration and p_bull_day5 >= thresholds.strong_buy_min_probability:
        return _with_earnings_adjustment("Strong Buy", "short", strength, int(round(expected_duration)), "Bull regime is persistent with low transition risk.")
    if current_regime == "Bull" and transition_risk < thresholds.buy_max_transition_risk:
        return _with_earnings_adjustment("Buy", "short", strength, int(round(expected_duration)), "Bull regime remains dominant over the next week.")
    if current_regime == "Neutral" and p_bull_day5 > thresholds.neutral_bull_tilt_probability:
        return _with_earnings_adjustment("Buy", "short", strength, int(round(expected_duration)), "Neutral regime is tilting toward Bull over the next five days.")
    if current_regime == "Bear" and transition_risk < thresholds.strong_sell_max_transition_risk and expected_duration > thresholds.strong_sell_min_duration and p_bear_day5 >= thresholds.strong_sell_min_probability:
        return _with_earnings_adjustment("Strong Sell", "short", strength, int(round(expected_duration)), "Bear regime is persistent with high downside continuation probability.")
    if current_regime == "Bear" and transition_risk < thresholds.sell_max_transition_risk:
        return _with_earnings_adjustment("Sell", "short", strength, int(round(expected_duration)), "Bear regime remains established with limited reversal risk.")
    if current_regime in {"Bull", "Neutral"} and p_bear_day5 > thresholds.bear_emerging_probability:
        return _with_earnings_adjustment("Sell", "short", strength, int(round(expected_duration)), "Forward curve shows Bear becoming material within five days.")

    medium_strength = max(0.0, min(1.0, abs(p_bull_day21 - p_bear_day21) * current_probability))
    if current_regime == "Bull" and transition_risk <= thresholds.hold_bull_max_transition_risk:
        return _with_earnings_adjustment("Hold", "medium", medium_strength, int(round(expected_duration)), "Bull regime remains intact but short-term persistence has weakened.")
    if current_regime == "Neutral":
        return _with_earnings_adjustment("Hold", "medium", medium_strength, int(round(expected_duration)), "Neutral regime has no decisive directional edge in the forward curve.")
    return _with_earnings_adjustment("Hold", "medium", medium_strength, int(round(expected_duration)), "Forward probabilities are mixed despite the current regime.")


def compute_technicals(price_series: pd.Series, volume_series: pd.Series, high_series: pd.Series | None = None, low_series: pd.Series | None = None) -> pd.DataFrame:
    prices = price_series.astype(float)
    volume = volume_series.astype(float)
    high = high_series.astype(float) if high_series is not None else prices
    low = low_series.astype(float) if low_series is not None else prices

    delta = prices.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)
    avg_gain = gain.ewm(alpha=1.0 / 14, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1.0 / 14, adjust=False).mean().replace(0.0, np.nan)
    rs = avg_gain / avg_loss
    rsi_14 = 100 - (100 / (1 + rs))

    ema12 = prices.ewm(span=12, adjust=False).mean()
    ema26 = prices.ewm(span=26, adjust=False).mean()
    macd_line = ema12 - ema26
    macd_signal = macd_line.ewm(span=9, adjust=False).mean()
    macd_histogram = macd_line - macd_signal

    bb_mid = prices.rolling(20).mean()
    bb_std = prices.rolling(20).std()
    bb_upper = bb_mid + 2 * bb_std
    bb_lower = bb_mid - 2 * bb_std
    bb_width = bb_upper - bb_lower
    bb_pct = (prices - bb_lower) / bb_width.replace(0.0, np.nan)

    prev_close = prices.shift(1)
    true_range = pd.concat(
        [
            high - low,
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    atr_14 = true_range.rolling(14).mean()

    obv = (np.sign(delta.fillna(0.0)) * volume).cumsum()
    return pd.DataFrame(
        {
            "rsi_14": rsi_14,
            "macd_line": macd_line,
            "macd_signal": macd_signal,
            "macd_histogram": macd_histogram,
            "bb_upper": bb_upper,
            "bb_lower": bb_lower,
            "bb_width": bb_width,
            "bb_pct": bb_pct,
            "atr_14": atr_14,
            "obv": obv,
        }
    )


def intra_regime_signal(technicals_df: pd.DataFrame, regime_label: str) -> str:
    latest = technicals_df.dropna().iloc[-1]
    prev = technicals_df.dropna().iloc[-2] if len(technicals_df.dropna()) > 1 else latest
    rsi = float(latest["rsi_14"])
    bb_pct = float(latest["bb_pct"])
    macd_hist = float(latest["macd_histogram"])
    macd_hist_prev = float(prev["macd_histogram"])

    if regime_label == "Bull":
        if rsi < 30 or bb_pct < 0.1:
            return "Buy the dip"
        if rsi > 80 and macd_hist < 0 and macd_hist_prev >= 0:
            return "Take partial profits"
        return "Hold / add on weakness"
    if regime_label == "Bear":
        if rsi > 70 or bb_pct > 0.9:
            return "Sell the rally"
        if rsi < 20 and macd_hist > 0 and macd_hist_prev <= 0:
            return "Cover short / tactical bounce"
        return "Stay defensive"
    if rsi < 30:
        return "Speculative buy for range trade"
    if rsi > 70:
        return "Speculative sell for range trade"
    return "No clear short-term signal"


def build_composite_signal(
    regime_signal: str,
    regime_probability: float,
    forward_signal: SignalResult,
    technical_signal: str,
) -> CompositeSignal:
    directional_map = {
        "Strong Buy": "buy",
        "Buy": "buy",
        "Hold": "hold",
        "Sell": "sell",
        "Strong Sell": "sell",
    }
    technical_direction = "hold"
    if technical_signal in {"Buy the dip", "Speculative buy for range trade"}:
        technical_direction = "buy"
    elif technical_signal in {"Take partial profits", "Sell the rally", "Speculative sell for range trade", "Stay defensive"}:
        technical_direction = "sell"

    composite_action = forward_signal.action
    composite_strength = forward_signal.strength
    short_term_view = f"1-5 day outlook: {forward_signal.rationale}"
    medium_term_view = f"Expected holding period: ~{forward_signal.expected_holding_days} trading days."

    if regime_signal == "Bear" and technical_signal == "Cover short / tactical bounce":
        composite_action = "Hold"
    elif regime_signal == "Bull" and technical_signal == "Take partial profits":
        composite_action = "Hold"
    elif directional_map.get(forward_signal.action, "hold") == technical_direction:
        composite_strength = min(1.0, composite_strength + 0.15)
    elif technical_direction != "hold":
        composite_strength = max(0.0, composite_strength - 0.20)
        medium_term_view += " Conflicting signals between forward regime and technical overlay."

    return CompositeSignal(
        regime_signal=regime_signal,
        regime_probability=regime_probability,
        forward_signal=forward_signal,
        technical_signal=technical_signal,
        composite_action=composite_action,
        composite_strength=composite_strength,
        short_term_view=short_term_view,
        medium_term_view=medium_term_view,
    )


def multi_timeframe_signal(daily_regime: str, weekly_regime: str) -> str:
    if daily_regime == "Bull" and weekly_regime == "Bull":
        return "Strong trend, high confidence"
    if daily_regime == "Bull" and weekly_regime == "Bear":
        return "Counter-trend rally, reduce size"
    if daily_regime == "Bear" and weekly_regime == "Bull":
        return "Pullback in uptrend, potential buy"
    if daily_regime == "Bear" and weekly_regime == "Bear":
        return "Strong downtrend, high confidence sell"
    if daily_regime == weekly_regime:
        return "Aligned"
    return "Mixed trend, moderate confidence"


def compute_price_targets(
    *,
    current_price: float,
    technicals_df: pd.DataFrame,
    composite_signal: CompositeSignal,
    expected_duration: float,
    state_mean_return: float,
) -> PriceTargets:
    latest = technicals_df.dropna().iloc[-1] if not technicals_df.dropna().empty else None
    bb_lower = float(latest["bb_lower"]) if latest is not None and pd.notna(latest.get("bb_lower")) else current_price
    bb_upper = float(latest["bb_upper"]) if latest is not None and pd.notna(latest.get("bb_upper")) else current_price
    atr_value = float(latest["atr_14"]) if latest is not None and pd.notna(latest.get("atr_14")) else None

    confidence = float(composite_signal.composite_strength or 0.0)
    if confidence < 0.4:
        confidence_multiplier = 0.85
    elif confidence > 0.7:
        confidence_multiplier = 1.15
    else:
        confidence_multiplier = 1.0

    timeframe_days = max(1, int(round(expected_duration or composite_signal.forward_signal.expected_holding_days or 1)))
    action = str(composite_signal.composite_action or "Hold")
    entry_price: float | None = None
    exit_price: float | None = None
    stop_price: float | None = None

    if atr_value is not None and atr_value > 0:
        projected_move = atr_value * sqrt(timeframe_days) * confidence_multiplier
    else:
        projected_move = abs(float(state_mean_return or 0.0)) * current_price * timeframe_days * confidence_multiplier
    if timeframe_days > 10:
        reversion_factor = 1.0 - 0.02 * min(timeframe_days - 10, 30)
        projected_move *= max(0.6, reversion_factor)

    if action in {"Buy", "Strong Buy"}:
        entry_price = bb_lower
        exit_price = max(entry_price, current_price + projected_move)
        stop_price = entry_price - (2.0 * atr_value if atr_value is not None else 0.0)
    elif action in {"Sell", "Strong Sell"}:
        entry_price = bb_upper
        exit_price = min(entry_price, current_price - projected_move)
        stop_price = entry_price + (2.0 * atr_value if atr_value is not None else 0.0)
    else:
        stop_distance = 1.5 * atr_value if atr_value is not None else 0.0
        stop_price = current_price - stop_distance

    risk_reward_ratio: float | None = None
    if action in {"Buy", "Strong Buy"} and entry_price is not None and exit_price is not None and stop_price is not None:
        risk = max(entry_price - stop_price, 0.0)
        reward = max(exit_price - entry_price, 0.0)
        risk_reward_ratio = reward / risk if risk > 0 else None
    elif action in {"Sell", "Strong Sell"} and entry_price is not None and exit_price is not None and stop_price is not None:
        risk = max(stop_price - entry_price, 0.0)
        reward = max(entry_price - exit_price, 0.0)
        risk_reward_ratio = reward / risk if risk > 0 else None

    price_position = "Monitoring"
    if stop_price is not None and current_price < stop_price:
        price_position = "Below stop loss — position at risk"
    elif entry_price is not None and current_price < entry_price:
        price_position = "Below entry — approaching buy zone"
    elif exit_price is not None and current_price > exit_price:
        price_position = "Above exit target — consider taking profits"
    elif entry_price is not None and exit_price is not None:
        price_position = "In target range — position active"

    return PriceTargets(
        current_price=float(current_price),
        entry_price=float(entry_price) if entry_price is not None else None,
        exit_price=float(exit_price) if exit_price is not None else None,
        stop_price=float(stop_price) if stop_price is not None else None,
        risk_reward_ratio=float(risk_reward_ratio) if risk_reward_ratio is not None else None,
        timeframe_days=timeframe_days,
        atr_value=float(atr_value) if atr_value is not None else None,
        confidence_multiplier=confidence_multiplier,
        price_position=price_position,
    )


def compute_position_size(
    *,
    regime_probability: float,
    composite_action: str,
    risk_reward_ratio: float | None,
    atr_value: float | None,
    current_price: float,
    portfolio_value: float | None = None,
    max_risk_pct: float = 2.0,
    regime_exposure: dict[str, float] | None = None,
    sector_exposure_pct: float | None = None,
    correlation_penalty: float = 0.0,
) -> PositionSize:
    """Compute suggested position size based on regime confidence and risk parameters."""
    base_pct = float(regime_probability or 0.0) * 100.0
    if composite_action == "Hold":
        base_pct *= 0.25
    elif composite_action in {"Sell", "Strong Sell"}:
        base_pct = 0.0

    kelly_fraction: float | None = None
    if risk_reward_ratio is not None and risk_reward_ratio > 0:
        win_prob = min(0.95, float(regime_probability or 0.0))
        lose_prob = 1.0 - win_prob
        kelly_fraction = max(0.0, (win_prob * risk_reward_ratio - lose_prob) / risk_reward_ratio)
        kelly_fraction *= 0.5
        base_pct = min(base_pct, kelly_fraction * 100.0)

    max_loss_dollars: float | None = None
    if atr_value is not None and atr_value > 0 and portfolio_value is not None and portfolio_value > 0:
        stop_distance = 2.0 * atr_value
        risk_per_share = stop_distance
        max_risk_dollars = portfolio_value * (max_risk_pct / 100.0)
        max_shares = max_risk_dollars / risk_per_share if risk_per_share > 0 else 0.0
        max_position_value = max_shares * current_price
        max_position_pct = (max_position_value / portfolio_value) * 100.0
        base_pct = min(base_pct, max_position_pct)
        max_loss_dollars = max_risk_dollars

    portfolio_adjustment = 1.0
    adjustment_parts: list[str] = []
    if regime_exposure:
        ticker_regime = "Bull" if composite_action in {"Buy", "Strong Buy"} else "Bear" if composite_action in {"Sell", "Strong Sell"} else "Neutral"
        current_exposure = float(regime_exposure.get(ticker_regime) or 0.0)
        if current_exposure <= 1.0:
            current_exposure *= 100.0
        if current_exposure > 70.0:
            exposure_adjustment = max(0.6, 1.0 - (current_exposure - 70.0) / 75.0)
            portfolio_adjustment *= exposure_adjustment
            adjustment_parts.append(f"{ticker_regime} exposure already {current_exposure:.0f}%")
    if sector_exposure_pct is not None and float(sector_exposure_pct) > 30.0:
        sector_adjustment = max(0.7, 1.0 - (float(sector_exposure_pct) - 30.0) / 100.0)
        portfolio_adjustment *= sector_adjustment
        adjustment_parts.append(f"sector exposure already {float(sector_exposure_pct):.0f}%")
    if correlation_penalty > 0:
        portfolio_adjustment *= max(0.0, 1.0 - float(correlation_penalty))
        adjustment_parts.append(f"correlation penalty {float(correlation_penalty):.0%}")
    base_pct *= portfolio_adjustment

    suggested_pct = round(max(0.0, min(100.0, base_pct)), 1)
    suggested_dollars = round(portfolio_value * (suggested_pct / 100.0), 2) if portfolio_value is not None else None
    rationale_parts = [f"Base sizing: {float(regime_probability or 0.0):.0%} regime confidence"]
    if kelly_fraction is not None:
        rationale_parts.append(f"Half-Kelly: {kelly_fraction:.1%}")
    if max_loss_dollars is not None:
        rationale_parts.append(f"Max risk: ${max_loss_dollars:,.0f} ({max_risk_pct}% of portfolio)")
    rationale = ". ".join(rationale_parts) + "."
    adjustment_rationale = None
    if portfolio_adjustment < 0.999:
        adjustment_rationale = "Position reduced for portfolio concentration"
        if adjustment_parts:
            adjustment_rationale += f": {', '.join(adjustment_parts)}."
    return PositionSize(
        suggested_pct=suggested_pct,
        suggested_dollars=suggested_dollars,
        max_loss_dollars=max_loss_dollars,
        kelly_fraction=kelly_fraction,
        sizing_rationale=rationale,
        portfolio_adjustment=portfolio_adjustment,
        adjustment_rationale=adjustment_rationale,
    )


def concentration_adjusted_strength(
    ticker: str,
    composite_strength: float,
    regime_label: str,
    sector: str,
    portfolio_tickers: list[str],
    correlations: dict[tuple[str, str], float],
    sector_map: dict[str, str],
    regime_map: dict[str, str],
) -> tuple[float, str | None, float]:
    same_regime_same_sector = [
        symbol for symbol in portfolio_tickers
        if symbol != ticker
        and regime_map.get(symbol) == regime_label
        and sector_map.get(symbol) == sector
    ]
    if not same_regime_same_sector:
        return composite_strength, None, 0.0
    corr_values = [float(correlations.get((ticker, symbol), 0.5)) for symbol in same_regime_same_sector]
    avg_corr = sum(corr_values) / len(corr_values) if corr_values else 0.0
    count_factor = min(1.0, len(same_regime_same_sector) / 3.0)
    corr_factor = max(0.0, avg_corr)
    penalty = 0.30 * count_factor * corr_factor
    adjusted = composite_strength * (1.0 - penalty)
    warning = None
    if penalty >= 0.10:
        warning = (
            f"Signal reduced {penalty:.0%} — {len(same_regime_same_sector)} "
            f"{sector} holding(s) in same {regime_label} regime "
            f"(avg correlation: {avg_corr:.2f})"
        )
    return adjusted, warning, penalty


def divergence_severity(
    daily_label: str,
    weekly_label: str,
    regime_history: list[dict[str, Any]],
    ticker: str,
) -> dict[str, Any]:
    if daily_label == weekly_label:
        return {"score": 0.0, "interpretation": "Aligned", "divergence_type": None}
    divergence_type = f"{daily_label}_vs_weekly_{weekly_label}"
    relevant = [
        entry for entry in regime_history
        if str(entry.get("ticker", "")).upper() == str(ticker or "").upper()
    ]
    transitions_to_weekly = [
        entry for entry in relevant
        if str(entry.get("current_label", "")).lower() == str(weekly_label or "").lower()
    ]
    if len(relevant) < 3:
        return {
            "score": 0.5,
            "interpretation": "Divergent — insufficient history",
            "divergence_type": divergence_type,
            "historical_count": len(relevant),
            "transition_rate": None,
            "typical_resolution_days": None,
        }
    transition_rate = len(transitions_to_weekly) / max(1, len(relevant))
    score = min(1.0, transition_rate * 1.5)
    if score >= 0.7:
        interpretation = f"Strong signal — weekly {weekly_label} has historically led daily regime changes"
    elif score >= 0.4:
        interpretation = "Moderate signal — some historical precedent for convergence"
    else:
        interpretation = "Weak signal — divergence often resolves without daily regime change"
    return {
        "score": round(score, 2),
        "interpretation": interpretation,
        "divergence_type": divergence_type,
        "historical_count": len(relevant),
        "transition_rate": round(transition_rate, 2) if transition_rate else None,
        "typical_resolution_days": None,
    }


def earnings_warning(earnings_date: datetime | None) -> str | None:
    if earnings_date is None:
        return None
    now = datetime.now(timezone.utc)
    event = earnings_date if earnings_date.tzinfo is not None else earnings_date.replace(tzinfo=timezone.utc)
    days = int(round((event - now).total_seconds() / 86400.0))
    if days < 0:
        return None
    if days <= 2:
        return f"Earnings imminent ({event.date().isoformat()}) — high binary risk. Regime model does not account for event-driven moves."
    if days <= 7:
        return f"Earnings in ~{days} days — regime signals may be less reliable. Consider reducing position size or hedging."
    return None


def apply_signal_context(
    composite_signal: CompositeSignal,
    *,
    price_targets: PriceTargets | None = None,
    earnings_warning_text: str | None = None,
) -> CompositeSignal:
    composite_signal.earnings_warning = earnings_warning_text
    rr = getattr(price_targets, "risk_reward_ratio", None) if price_targets is not None else None
    if rr is not None:
        if composite_signal.composite_action in {"Buy", "Strong Buy"} and rr < 1.0:
            composite_signal.risk_reward_conflict = True
            composite_signal.risk_reward_warning = (
                f"Signal is {composite_signal.composite_action} but risk/reward is unfavorable ({rr:.2f}). "
                "Consider reducing position size or waiting for a better entry."
            )
        elif composite_signal.composite_action in {"Sell", "Strong Sell"} and rr > 2.0:
            composite_signal.risk_reward_conflict = True
            composite_signal.risk_reward_warning = (
                f"Signal is {composite_signal.composite_action} but risk/reward favors holding ({rr:.2f}). Review stop level."
            )
        else:
            composite_signal.risk_reward_conflict = False
            composite_signal.risk_reward_warning = None
    return composite_signal


def compute_unified_confidence(
    regime_probability: float,
    signal_strength: float,
    calibrator=None,
) -> ConfidenceScore:
    raw_probability = max(0.0, min(1.0, float(regime_probability or 0.0)))
    calibrated = False
    calibrated_probability = raw_probability
    if calibrator is not None:
        try:
            predicted = calibrator.predict([raw_probability])
            if len(predicted):
                calibrated_probability = max(0.0, min(1.0, float(predicted[0])))
                calibrated = True
        except Exception as exc:
            logger.debug("Unable to apply probability calibrator.", exc_info=exc)
    normalized_strength = max(0.0, min(1.0, float(signal_strength or 0.0)))
    value = ((calibrated_probability * 0.7) + (normalized_strength * 0.3)) * 100.0
    if value >= 85:
        label = "Very High"
    elif value >= 70:
        label = "High"
    elif value >= 50:
        label = "Medium"
    else:
        label = "Low"
    return ConfidenceScore(
        value=round(value, 1),
        label=label,
        calibrated=calibrated,
        components={
            "regime_probability": round(raw_probability * 100.0, 1),
            "calibrated_probability": round(calibrated_probability * 100.0, 1),
            "signal_strength": round(normalized_strength * 100.0, 1),
        },
    )


def confidence_trajectory(state_probabilities_series: pd.Series, window: int = 10) -> ConfidenceTrajectory:
    series = state_probabilities_series.dropna().tail(window)
    if len(series) < 2:
        latest = float(series.iloc[-1]) if not series.empty else 0.0
        return ConfidenceTrajectory(
            slope=0.0,
            trend="stable",
            days_declining=0,
            days_rising=0,
            short_ma_latest=latest,
            long_ma_latest=latest,
        )
    x = np.arange(len(series), dtype=float)
    slope = float(np.polyfit(x, series.to_numpy(dtype=float), 1)[0])
    short_ma = series.rolling(3, min_periods=1).mean()
    long_ma = series.rolling(7, min_periods=1).mean()
    short_ma_latest = float(short_ma.iloc[-1])
    long_ma_latest = float(long_ma.iloc[-1])
    if slope > 0.005:
        trend = "rising"
    elif slope < -0.005:
        trend = "declining"
    else:
        trend = "stable"

    days_declining = 0
    days_rising = 0
    if trend == "declining":
        for short_value, long_value in zip(short_ma.iloc[::-1], long_ma.iloc[::-1]):
            if short_value < long_value:
                days_declining += 1
            else:
                break
    elif trend == "rising":
        for short_value, long_value in zip(short_ma.iloc[::-1], long_ma.iloc[::-1]):
            if short_value > long_value:
                days_rising += 1
            else:
                break
    return ConfidenceTrajectory(
        slope=slope,
        trend=trend,
        days_declining=days_declining,
        days_rising=days_rising,
        short_ma_latest=short_ma_latest,
        long_ma_latest=long_ma_latest,
    )


def tax_adjusted_signal(
    composite_signal: CompositeSignal,
    position,
    tax_assumptions: dict[str, float],
    wash_sale_risk: str = "NONE",
) -> TaxAdjustedSignal:
    lot_terms = {str(getattr(lot, "term", "") or "").upper() for lot in getattr(position, "lots", []) if getattr(lot, "term", None)}
    if not lot_terms:
        tax_status = "—"
    elif lot_terms == {"LT"}:
        tax_status = "LT"
    elif lot_terms == {"ST"}:
        tax_status = "ST"
    else:
        tax_status = "Mixed"

    if position.account_type == "IRA":
        return TaxAdjustedSignal(
            account_name=position.account_name,
            account_type=position.account_type,
            original_action=composite_signal.composite_action,
            adjusted_action=composite_signal.composite_action,
            tax_note="IRA — no tax impact on trades.",
            ltcg_threshold_date=None,
            estimated_tax_impact=0.0,
            wash_sale_warning=None,
            tax_status=tax_status,
        )

    ordinary_rate = float(tax_assumptions.get("ordinary_rate", 0.37))
    ltcg_rate = float(tax_assumptions.get("ltcg_rate", 0.20))
    niit_rate = float(tax_assumptions.get("niit_rate", 0.038))

    estimated_tax_impact = 0.0
    for lot in position.lots:
        rate = ltcg_rate if lot.term == "LT" else ordinary_rate
        estimated_tax_impact += lot.unrealized_gain * (rate + niit_rate)

    adjusted_action = composite_signal.composite_action
    tax_note = "No material tax adjustment."
    wash_sale_warning = None
    ltcg_threshold_date = None

    near_ltcg_lots = [
        lot
        for lot in position.lots
        if str(getattr(lot, "term", "") or "").upper() == "ST" and int(getattr(lot, "days_to_ltcg", 9999)) <= 30
    ]
    if composite_signal.composite_action in {"Sell", "Strong Sell"} and near_ltcg_lots:
        adjusted_action = "Hold"
        tax_delta = max(0.0, ordinary_rate - ltcg_rate)
        nearest_lot = min(near_ltcg_lots, key=lambda lot: int(getattr(lot, "days_to_ltcg", 9999)))
        savings_base = sum(max(float(getattr(lot, "unrealized_gain", 0.0) or 0.0), 0.0) for lot in near_ltcg_lots)
        savings_estimate = savings_base * tax_delta
        lot_count = len(near_ltcg_lots)
        lot_noun = "lot" if lot_count == 1 else "lots"
        tax_note = (
            f"{lot_count} short-term {lot_noun} convert to LTCG within 30 days "
            f"(nearest: {nearest_lot.days_to_ltcg} days)"
        )
        if savings_estimate > 0:
            tax_note += f" — holding may avoid about ${savings_estimate:,.2f} in extra tax."
        else:
            tax_note += f" — holding saves ~{tax_delta:.0%} in tax rate."
        ltcg_threshold_date = pd.Timestamp(nearest_lot.acquisition_date) + pd.Timedelta(days=365)
        ltcg_threshold_date = ltcg_threshold_date.date().isoformat()
    elif composite_signal.composite_action in {"Buy", "Strong Buy"}:
        loss_lots = [lot for lot in position.lots if lot.term == "ST" and lot.unrealized_gain < 0]
        if loss_lots:
            total_loss = abs(sum(lot.unrealized_gain for lot in loss_lots))
            tax_note = f"Tax-loss harvesting opportunity: {len(loss_lots)} lots with ${total_loss:,.2f} in short-term losses."

    if composite_signal.composite_action in {"Sell", "Strong Sell"} and position.unrealized_gain < 0 and wash_sale_risk == "DEFINITE":
        wash_sale_warning = "Wash sale risk — loss would be disallowed. Consider waiting 31 days or selling in IRA."

    return TaxAdjustedSignal(
        account_name=position.account_name,
        account_type=position.account_type,
        original_action=composite_signal.composite_action,
        adjusted_action=adjusted_action,
        tax_note=tax_note,
        ltcg_threshold_date=ltcg_threshold_date,
        estimated_tax_impact=estimated_tax_impact,
        wash_sale_warning=wash_sale_warning,
        tax_status=tax_status,
    )


def tax_adjusted_signals(
    composite_signal: CompositeSignal,
    positions: list,
    tax_assumptions: dict[str, float],
    wash_sale_risk: str = "NONE",
) -> list[TaxAdjustedSignal]:
    return [
        tax_adjusted_signal(
            composite_signal,
            position,
            tax_assumptions,
            wash_sale_risk=wash_sale_risk if position.account_type == "TAXABLE" else "NONE",
        )
        for position in positions
    ]


def sentiment_momentum(
    ticker: str,
    regime_label: str,
    days: int = 30,
    short_window: int = 5,
    long_window: int = 10,
) -> tuple[SentimentMomentum, pd.DataFrame]:
    history = pd.DataFrame(get_sentiment_history(ticker, days=days))
    if history.empty:
        empty = SentimentMomentum(0.0, 0.0, "stable", False, None)
        return empty, pd.DataFrame(columns=["recorded_at", "score", "short_ma", "long_ma"])

    history["score"] = history["score"].astype(float)
    history["short_ma"] = history["score"].rolling(short_window, min_periods=1).mean()
    history["long_ma"] = history["score"].rolling(long_window, min_periods=1).mean()
    short_ma = float(history["short_ma"].iloc[-1])
    long_ma = float(history["long_ma"].iloc[-1])
    if short_ma > long_ma + 0.25:
        trend = "improving"
    elif short_ma < long_ma - 0.25:
        trend = "deteriorating"
    else:
        trend = "stable"

    divergence = False
    warning = None
    trailing = history.tail(5)
    if regime_label == "Bull" and trend == "deteriorating" and (trailing["short_ma"] < trailing["long_ma"]).sum() >= 5:
        divergence = True
        warning = "Sentiment deteriorating despite Bull regime — potential leading indicator."
    elif regime_label == "Bear" and trend == "improving" and (trailing["short_ma"] > trailing["long_ma"]).sum() >= 5:
        divergence = True
        warning = "Sentiment improving despite Bear regime — watch for regime turn."

    return SentimentMomentum(short_ma, long_ma, trend, divergence, warning), history
