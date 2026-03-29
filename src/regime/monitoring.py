from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from .attribution import compute_ml_accuracy
from .paper_trading import compute_paper_performance
from .persistence import (
    get_alerts,
    get_daily_capital_ceiling_pct,
    get_daily_capital_deployed,
    get_paper_portfolio_summary,
    get_paper_positions,
    save_alert,
)


def _today_text() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _already_alerted(alert_type: str, *, portfolio_id: int | None = None) -> bool:
    rows = get_alerts(alert_type=alert_type, since=_today_text(), limit=50)
    if portfolio_id is None:
        return bool(rows)
    return any(int(row.get("portfolio_id") or 0) == int(portfolio_id) for row in rows)


def detect_drawdown_alert(portfolio_id: int) -> dict[str, Any] | None:
    performance = compute_paper_performance(portfolio_id)
    drawdown_pct = float(performance.get("max_drawdown_pct") or 0.0)
    if drawdown_pct <= 10.0 or _already_alerted("drawdown_breach", portfolio_id=portfolio_id):
        return None
    return save_alert(
        "drawdown_breach",
        f"Portfolio drawdown at {drawdown_pct:.1f}%",
        severity="warning",
        portfolio_id=portfolio_id,
        message="Portfolio drawdown exceeded 10%.",
        data={"drawdown_pct": drawdown_pct},
    )


def detect_concentration_alert(portfolio_id: int) -> dict[str, Any] | None:
    summary = get_paper_portfolio_summary(portfolio_id)
    total_equity = float(summary.get("total_equity") or summary.get("current_value") or 0.0)
    if total_equity <= 0:
        return None
    max_pct = 0.0
    max_ticker = ""
    for row in get_paper_positions(portfolio_id, status="Open"):
        value = float(row.get("market_value") or 0.0)
        if value <= 0:
            value = float(row.get("quantity") or 0.0) * float(row.get("current_price") or row.get("entry_price") or 0.0)
        pct = (value / total_equity) if total_equity else 0.0
        if pct > max_pct:
            max_pct = pct
            max_ticker = str(row.get("ticker") or "").upper()
    if max_pct <= 0.30 or _already_alerted("concentration_breach", portfolio_id=portfolio_id):
        return None
    return save_alert(
        "concentration_breach",
        f"Position concentration high: {max_ticker} at {max_pct:.0%}",
        severity="warning",
        portfolio_id=portfolio_id,
        ticker=max_ticker,
        message="Single-position concentration exceeded 30% of equity.",
        data={"ticker": max_ticker, "concentration_pct": max_pct},
    )


def detect_ml_accuracy_drift(portfolio_id: int) -> dict[str, Any] | None:
    ml = compute_ml_accuracy(portfolio_id)
    accuracy = ml.get("overall_accuracy")
    trades = int(ml.get("total_trades_with_ml") or 0)
    if accuracy is None or trades <= 0 or float(accuracy) >= 0.50 or _already_alerted("ml_accuracy_drift", portfolio_id=portfolio_id):
        return None
    return save_alert(
        "ml_accuracy_drift",
        f"ML accuracy drift: {float(accuracy):.0%}",
        severity="warning",
        portfolio_id=portfolio_id,
        message="Meta-labeler realized accuracy dropped below 50%.",
        data={"accuracy": float(accuracy), "trades": trades},
    )


def detect_capital_ceiling_breach(portfolio_id: int) -> dict[str, Any] | None:
    summary = get_paper_portfolio_summary(portfolio_id)
    total_equity = float(summary.get("total_equity") or summary.get("current_value") or 0.0)
    ceiling_pct = float(get_daily_capital_ceiling_pct() or 0.0)
    max_capital = total_equity * ceiling_pct
    deployed = float(get_daily_capital_deployed(portfolio_id) or 0.0)
    usage = (deployed / max_capital) if max_capital > 0 else 0.0
    if usage <= 0.90 or _already_alerted("capital_ceiling_breach", portfolio_id=portfolio_id):
        return None
    return save_alert(
        "capital_ceiling_breach",
        f"Daily capital ceiling at {usage:.0%}",
        severity="warning",
        portfolio_id=portfolio_id,
        message="Daily capital deployment exceeded 90% of the ceiling.",
        data={"usage_pct": usage, "deployed": deployed, "ceiling": max_capital},
    )


def sweep_monitoring_alerts(portfolio_id: int) -> list[dict[str, Any]]:
    alerts: list[dict[str, Any]] = []
    for detector in (
        detect_drawdown_alert,
        detect_concentration_alert,
        detect_ml_accuracy_drift,
        detect_capital_ceiling_breach,
    ):
        alert = detector(portfolio_id)
        if alert:
            alerts.append(alert)
    return alerts
