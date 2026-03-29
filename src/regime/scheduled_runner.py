from __future__ import annotations

from typing import Any

from .alerts import (
    check_regime_changes,
    check_signal_changes,
    check_stop_proximity,
    check_transition_risk_spikes,
    format_alert_summary,
)
from .discovery import check_entry_signals, expire_stale_candidates, run_full_discovery
from .investor_adapter import get_investor_db_path, get_portfolio_tickers_filtered, get_latest_prices
from .attribution import compute_ml_accuracy, compute_theme_attribution
from .paper_trading import auto_approve_plans, auto_execute_approved, compute_daily_snapshot, expire_stale_plans, generate_daily_plans, record_trade_outcome
from .persistence import (
    get_operating_mode,
    get_daily_snapshots,
    get_pending_transition_outcomes,
    get_paper_positions,
    list_paper_portfolios,
    save_daily_snapshot,
    update_transition_outcome,
)
from .config import DEFAULT_IBKR_CONFIG
from .broker_adapter import PaperBrokerAdapter
from .config import DEFAULT_RISK_GUARDRAILS
from .ib_connection import get_ib_backend, get_mock_ib_backend
from .ibkr_adapter import IBKRBrokerAdapter, poll_pending_orders
from src.app.routes.regime_cache import load_payload


def run_scheduled_regime_checks(tickers: list[str] | None = None) -> dict[str, Any]:
    db_path = get_investor_db_path()
    if not db_path:
        return {"alerts": [], "summary": "Investor database unavailable."}
    selected = tickers or get_portfolio_tickers_filtered(db_path)
    regime_alerts = check_regime_changes(selected, db_path)
    risk_alerts = check_transition_risk_spikes(selected)
    signal_alerts = check_signal_changes(selected)
    stop_alerts = check_stop_proximity(selected, db_path)
    pending = get_pending_transition_outcomes()
    pending_tickers = sorted({str(row.get("ticker") or "").upper() for row in pending if row.get("ticker")})
    latest_prices = get_latest_prices(db_path, pending_tickers)
    for row in pending:
        base_price = row.get("price_at_change")
        current_price = latest_prices.get(str(row.get("ticker") or "").upper())
        if not base_price or current_price is None:
            continue
        base = float(base_price)
        if base <= 0:
            continue
        realized = (float(current_price) - base) / base
        update_transition_outcome(
            int(row["id"]),
            return_5d=realized,
            return_10d=realized,
            return_21d=realized,
        )
    all_alerts = [*regime_alerts, *risk_alerts, *signal_alerts, *stop_alerts]
    return {"alerts": all_alerts, "summary": format_alert_summary(all_alerts)}


def run_scheduled_discovery(
    *,
    frontier_enabled: bool = True,
    frontier_provider: str = "auto",
) -> dict[str, Any]:
    expired = expire_stale_candidates(max_age_days=90)
    discovery = run_full_discovery(
        frontier_enabled=frontier_enabled,
        frontier_provider=frontier_provider,
    )
    entry_signals = check_entry_signals()
    return {
        "expired": expired,
        "discovery": discovery,
        "entry_signals": entry_signals,
    }


def run_scheduled_paper_plans() -> dict[str, Any]:
    cached_payload = load_payload() or {}
    cached_rows = cached_payload.get("rows") if isinstance(cached_payload, dict) else []
    cached_regime = {
        str(row.get("ticker") or "").upper(): (str(row.get("regime") or ""), float(row.get("probability") or 0.0))
        for row in (cached_rows or [])
        if isinstance(row, dict) and str(row.get("ticker") or "").strip()
    }
    results: list[dict[str, Any]] = []
    for portfolio in list_paper_portfolios(include_closed=False):
        if str(portfolio.get("status") or "") != "Active":
            continue
        portfolio_id = int(portfolio["id"])
        expired = expire_stale_plans(portfolio_id)
        generated = generate_daily_plans(portfolio_id, cached_regime=cached_regime, cached_payload=cached_payload)
        auto_result = auto_approve_plans(portfolio_id)
        exec_result = None
        polled = 0
        if str(portfolio.get("broker_type") or "paper").lower() == "ibkr":
            backend = get_ib_backend(
                portfolio_id,
                live=bool(DEFAULT_IBKR_CONFIG.live_backend),
                account_id=str(DEFAULT_IBKR_CONFIG.account_id),
                starting_cash=float(portfolio.get("current_cash") or portfolio.get("starting_budget") or 100000.0),
            )
            adapter = IBKRBrokerAdapter(
                backend,
                portfolio_id,
                host=str(DEFAULT_IBKR_CONFIG.host),
                port=int(DEFAULT_IBKR_CONFIG.port),
                client_id=int(getattr(backend, "_client_id", DEFAULT_IBKR_CONFIG.client_id)),
            )
            try:
                polled = len(poll_pending_orders(adapter, portfolio_id))
            except Exception:
                polled = 0
        if auto_result.get("approved", 0) > 0 and get_operating_mode() == "autonomous":
            exec_adapter = PaperBrokerAdapter(portfolio_id)
            exec_result = auto_execute_approved(portfolio_id, exec_adapter, DEFAULT_RISK_GUARDRAILS, actor="scheduler")
        results.append(
            {
                "portfolio_id": portfolio_id,
                "portfolio_name": portfolio.get("name"),
                "broker_type": portfolio.get("broker_type", "paper"),
                "buy_count": len(generated.get("buy_plans") or []),
                "exit_count": len(generated.get("exit_plans") or []),
                "expired_count": expired,
                "polled_orders": polled,
                "auto_approval": auto_result,
                "auto_execution": exec_result,
            }
        )
    return {"portfolios": results, "cached_regime_count": len(cached_regime)}


def run_end_of_day_processing() -> dict[str, Any]:
    snapshots: list[dict[str, Any]] = []
    outcomes: list[dict[str, Any]] = []
    for portfolio in list_paper_portfolios(include_closed=False):
        portfolio_id = int(portfolio["id"])
        snapshot = compute_daily_snapshot(portfolio_id)
        if snapshot:
            saved = save_daily_snapshot(
                portfolio_id,
                snapshot["snapshot_date"],
                equity=snapshot["equity"],
                cash=snapshot["cash"],
                market_value=snapshot["market_value"],
                realized_pnl=snapshot["realized_pnl"],
                unrealized_pnl=snapshot["unrealized_pnl"],
                position_count=snapshot["position_count"],
                trades_today=snapshot["trades_today"],
                drawdown_pct=snapshot.get("drawdown_pct"),
                regime_exposure_json=snapshot.get("regime_exposure_json"),
            )
            snapshots.append(saved)
        if str(portfolio.get("broker_type") or "paper").lower() == "ibkr":
            backend = get_ib_backend(
                portfolio_id,
                live=bool(DEFAULT_IBKR_CONFIG.live_backend),
                account_id=str(DEFAULT_IBKR_CONFIG.account_id),
                starting_cash=float(portfolio.get("current_cash") or portfolio.get("starting_budget") or 100000.0),
            )
            adapter = IBKRBrokerAdapter(
                backend,
                portfolio_id,
                host=str(DEFAULT_IBKR_CONFIG.host),
                port=int(DEFAULT_IBKR_CONFIG.port),
                client_id=int(getattr(backend, "_client_id", DEFAULT_IBKR_CONFIG.client_id)),
            )
            try:
                poll_pending_orders(adapter, portfolio_id)
            except Exception:
                pass
        for position in get_paper_positions(portfolio_id, status="Closed"):
            if position.get("exit_date"):
                outcomes.append(record_trade_outcome(portfolio_id, position, float(position.get("exit_price") or 0.0)))
    performance = run_performance_snapshot()
    return {
        "snapshots": snapshots,
        "snapshot_count": len(snapshots),
        "outcomes": outcomes,
        "performance": performance,
        "history_counts": {str(row["portfolio_id"]): len(get_daily_snapshots(int(row["portfolio_id"]))) for row in snapshots if row.get("portfolio_id") is not None},
    }


def run_performance_snapshot() -> dict[str, Any]:
    portfolios: list[dict[str, Any]] = []
    for portfolio in list_paper_portfolios(include_closed=False):
        if str(portfolio.get("status") or "") != "Active":
            continue
        portfolio_id = int(portfolio["id"])
        snapshot = compute_daily_snapshot(portfolio_id)
        theme = compute_theme_attribution(portfolio_id)
        ml = compute_ml_accuracy(portfolio_id)
        portfolios.append(
            {
                "portfolio_id": portfolio_id,
                "snapshot_date": snapshot.get("snapshot_date"),
                "drawdown_pct": snapshot.get("drawdown_pct"),
                "theme_count": int(theme.get("theme_count") or 0),
                "ml_trades": int(ml.get("total_trades_with_ml") or 0),
            }
        )
    return {"portfolios": portfolios, "count": len(portfolios)}
