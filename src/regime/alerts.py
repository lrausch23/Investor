from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any

from .data import download_market_frame
from .digest import generate_weekly_digest
from .hmm_engine import fit_regime_model
from .investor_adapter import get_tax_assumptions, get_wash_sale_risk, positions_by_ticker_and_account
from .persistence import get_alerts, get_recent_regime_changes, get_signal_effectiveness, save_alert, save_regime_event
from .signals import (
    build_composite_signal,
    compute_price_targets,
    compute_technicals,
    forward_regime_curve,
    intra_regime_signal,
    signal_from_forward_curve,
    tax_adjusted_signals,
)


@dataclass(frozen=True)
class RegimeAlert:
    ticker: str
    previous_label: str | None
    new_label: str
    transition_risk: float
    composite_action: str
    price_targets: dict[str, Any] | None
    timestamp: str


@dataclass(frozen=True)
class RiskAlert:
    ticker: str
    transition_risk: float
    threshold: float
    timestamp: str


@dataclass(frozen=True)
class SignalAlert:
    ticker: str
    previous_action: str | None
    new_action: str
    timestamp: str


@dataclass(frozen=True)
class StopAlert:
    ticker: str
    current_price: float
    stop_price: float
    distance_pct: float
    timestamp: str


def _now_text() -> str:
    return datetime.now(timezone.utc).isoformat()


def check_regime_changes(tickers: list[str], db_path: str) -> list[RegimeAlert]:
    positions = positions_by_ticker_and_account([])
    tax_assumptions = get_tax_assumptions(db_path)
    alerts: list[RegimeAlert] = []
    for ticker in tickers:
        market_frame = download_market_frame(ticker=ticker, period="3y", interval="1d").frame
        regime = fit_regime_model(ticker=ticker, market_frame=market_frame, refit_step=21)
        persistence = save_regime_event(ticker, regime.latest_label, int(regime.latest_state_id))
        if persistence.get("previous_label") == regime.latest_label:
            continue
        forward_curve = forward_regime_curve(regime.transition_matrix, regime.latest_state_vector, horizon=21)
        technicals = compute_technicals(
            market_frame["price"],
            market_frame["volume"],
            market_frame["high"] if "high" in market_frame.columns else None,
            market_frame["low"] if "low" in market_frame.columns else None,
        )
        forward_signal = signal_from_forward_curve(
            forward_curve,
            regime.latest_label,
            regime.transition_risk,
            regime.expected_regime_duration,
            regime.latest_probability,
        )
        technical_signal = intra_regime_signal(technicals, regime.latest_label)
        composite = build_composite_signal(regime.latest_label, regime.latest_probability, forward_signal, technical_signal)
        price_targets = compute_price_targets(
            current_price=float(getattr(regime, "latest_price", 0.0) or 0.0),
            technicals_df=technicals,
            composite_signal=composite,
            expected_duration=float(regime.expected_regime_duration),
            state_mean_return=float(getattr(regime, "recent_state_mean_return", 0.0) or 0.0),
        )
        account_positions = positions.get(ticker.upper(), [])
        signals = tax_adjusted_signals(
            composite,
            account_positions,
            tax_assumptions,
            wash_sale_risk=get_wash_sale_risk(db_path, ticker),
        ) if account_positions else []
        composite_action = signals[0].adjusted_action if signals else composite.composite_action
        alerts.append(
            RegimeAlert(
                ticker=ticker.upper(),
                previous_label=persistence.get("previous_label"),
                new_label=str(regime.latest_label),
                transition_risk=float(regime.transition_risk),
                composite_action=str(composite_action),
                price_targets=asdict(price_targets) if price_targets is not None else None,
                timestamp=_now_text(),
            )
        )
    return alerts


def check_transition_risk_spikes(tickers: list[str], threshold: float = 0.20) -> list[RiskAlert]:
    alerts: list[RiskAlert] = []
    for ticker in tickers:
        market_frame = download_market_frame(ticker=ticker, period="3y", interval="1d").frame
        regime = fit_regime_model(ticker=ticker, market_frame=market_frame, refit_step=21)
        if float(regime.transition_risk) <= float(threshold):
            continue
        alerts.append(
            RiskAlert(
                ticker=ticker.upper(),
                transition_risk=float(regime.transition_risk),
                threshold=float(threshold),
                timestamp=_now_text(),
            )
        )
    return alerts


def check_signal_changes(tickers: list[str]) -> list[SignalAlert]:
    effectiveness = get_signal_effectiveness()
    prior_rows = effectiveness.get("rows") or []
    prior_by_ticker = {str(row.get("ticker") or "").upper(): row for row in prior_rows if row.get("ticker")}
    alerts: list[SignalAlert] = []
    for ticker in tickers:
        market_frame = download_market_frame(ticker=ticker, period="3y", interval="1d").frame
        regime = fit_regime_model(ticker=ticker, market_frame=market_frame, refit_step=21)
        technicals = compute_technicals(
            market_frame["price"],
            market_frame["volume"],
            market_frame["high"] if "high" in market_frame.columns else None,
            market_frame["low"] if "low" in market_frame.columns else None,
        )
        forward_curve = forward_regime_curve(regime.transition_matrix, regime.latest_state_vector, horizon=21)
        forward_signal = signal_from_forward_curve(
            forward_curve,
            regime.latest_label,
            regime.transition_risk,
            regime.expected_regime_duration,
            regime.latest_probability,
        )
        technical_signal = intra_regime_signal(technicals, regime.latest_label)
        composite = build_composite_signal(regime.latest_label, regime.latest_probability, forward_signal, technical_signal)
        previous = prior_by_ticker.get(ticker.upper(), {})
        previous_action = previous.get("action")
        if previous_action == composite.composite_action:
            continue
        alerts.append(
            SignalAlert(
                ticker=ticker.upper(),
                previous_action=previous_action,
                new_action=str(composite.composite_action),
                timestamp=_now_text(),
            )
        )
    return alerts


def check_stop_proximity(tickers: list[str], db_path: str, threshold_pct: float = 0.05) -> list[StopAlert]:
    alerts: list[StopAlert] = []
    for ticker in tickers:
        market_frame = download_market_frame(ticker=ticker, period="3y", interval="1d").frame
        regime = fit_regime_model(ticker=ticker, market_frame=market_frame, refit_step=21)
        technicals = compute_technicals(
            market_frame["price"],
            market_frame["volume"],
            market_frame["high"] if "high" in market_frame.columns else None,
            market_frame["low"] if "low" in market_frame.columns else None,
        )
        forward_curve = forward_regime_curve(regime.transition_matrix, regime.latest_state_vector, horizon=21)
        forward_signal = signal_from_forward_curve(
            forward_curve,
            regime.latest_label,
            regime.transition_risk,
            regime.expected_regime_duration,
            regime.latest_probability,
        )
        technical_signal = intra_regime_signal(technicals, regime.latest_label)
        composite = build_composite_signal(regime.latest_label, regime.latest_probability, forward_signal, technical_signal)
        targets = compute_price_targets(
            current_price=float(getattr(regime, "latest_price", 0.0) or 0.0),
            technicals_df=technicals,
            composite_signal=composite,
            expected_duration=float(regime.expected_regime_duration),
            state_mean_return=float(getattr(regime, "recent_state_mean_return", 0.0) or 0.0),
        )
        stop_price = getattr(targets, "stop_price", None)
        current_price = float(getattr(targets, "current_price", 0.0) or 0.0)
        if stop_price is None or current_price <= 0:
            continue
        distance_pct = abs(current_price - float(stop_price)) / max(abs(float(stop_price)), 1e-9)
        if distance_pct <= float(threshold_pct):
            alerts.append(
                StopAlert(
                    ticker=ticker.upper(),
                    current_price=current_price,
                    stop_price=float(stop_price),
                    distance_pct=float(distance_pct),
                    timestamp=_now_text(),
                )
            )
    return alerts


def format_alert_summary(alerts: list[Any]) -> str:
    if not alerts:
        return "No new regime, transition-risk, or signal alerts."
    lines = []
    for alert in alerts:
        if isinstance(alert, RegimeAlert):
            lines.append(
                f"{alert.ticker}: {alert.previous_label or 'Unknown'} -> {alert.new_label}, "
                f"transition risk {alert.transition_risk:.1%}, action {alert.composite_action}"
            )
        elif isinstance(alert, RiskAlert):
            lines.append(
                f"{alert.ticker}: transition risk spike {alert.transition_risk:.1%} "
                f"(threshold {alert.threshold:.0%})"
            )
        elif isinstance(alert, SignalAlert):
            lines.append(
                f"{alert.ticker}: signal changed from {alert.previous_action or 'Unknown'} "
                f"to {alert.new_action}"
            )
        elif isinstance(alert, StopAlert):
            lines.append(
                f"{alert.ticker}: price {alert.current_price:.2f} is within {alert.distance_pct:.1%} of stop {alert.stop_price:.2f}"
            )
    return "\n".join(lines)


def check_loss_breach(portfolio_id: int, daily_pnl: float, daily_loss_limit: float) -> dict[str, Any] | None:
    if daily_pnl >= 0 or abs(float(daily_pnl)) <= float(daily_loss_limit):
        return None
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    existing = get_alerts(alert_type="daily_loss_breach", since=today, limit=50)
    if any(int(item.get("portfolio_id") or 0) == int(portfolio_id) for item in existing):
        return None
    return save_alert(
        "daily_loss_breach",
        f"Daily loss limit breached: ${daily_pnl:,.2f}",
        severity="critical",
        portfolio_id=portfolio_id,
        message=f"Daily P&L ${daily_pnl:,.2f} exceeded loss limit ${daily_loss_limit:,.2f}.",
        data={"daily_pnl": float(daily_pnl), "limit": float(daily_loss_limit)},
    )
