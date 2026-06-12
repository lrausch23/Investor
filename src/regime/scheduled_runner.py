from __future__ import annotations

import asyncio
import datetime as dt
from typing import Any

from .alerts import (
    check_regime_changes,
    check_loss_breach,
    check_signal_changes,
    check_stop_proximity,
    check_transition_risk_spikes,
    format_alert_summary,
)
from .notifications import dispatch_notification_sync, flush_digest
from .monitoring import sweep_monitoring_alerts
from .data_validator import check_database_health, run_pre_trade_validation
from .discovery import check_entry_signals, expire_stale_candidates, run_full_discovery
from .investor_adapter import get_investor_db_path, get_portfolio_tickers_filtered, get_latest_prices
from .attribution import compute_ml_accuracy, compute_theme_attribution
from .paper_trading import (
    auto_approve_plans,
    auto_execute_approved,
    cancel_submitted_orders_by_policy,
    compute_daily_snapshot,
    expire_stale_plans,
    generate_daily_plans,
    record_trade_outcome,
)
from .persistence import (
    get_operating_mode,
    get_daily_snapshots,
    is_live_trading_unlocked,
    get_pending_transition_outcomes,
    get_paper_portfolio_summary,
    get_paper_positions,
    get_setting,
    list_paper_portfolios,
    set_setting,
    save_daily_snapshot,
    update_transition_outcome,
)
from .config import (
    DEFAULT_IBKR_CONFIG,
    ibkr_backend_account_id,
    ibkr_execution_mode,
    should_use_ibkr_paper_backend,
    should_use_real_ibkr_backend,
)
from .vix_freeze import check_vix_freeze
from .broker_adapter import PaperBrokerAdapter
from .config import DEFAULT_RISK_GUARDRAILS
from .ib_connection import get_ib_backend, get_mock_ib_backend
from .ibkr_adapter import IBKRBrokerAdapter, poll_pending_orders
from .ib_types import ET, MarketHoursStatus, get_market_hours_status, next_market_open
from src.app.routes.regime_cache import load_payload
from .persistence import save_alert
from .beta_agents import parse_portfolio_ids
from .thesis_monitor import run_hbm_thesis_monitor


def _parse_preferred_run_window(raw: str | None) -> tuple[dt.time, dt.time]:
    text = str(raw or "10:05-15:30 America/New_York").strip()
    window = text.split()[0] if text else "10:05-15:30"
    try:
        start_text, end_text = window.split("-", maxsplit=1)
        start_hour, start_minute = start_text.split(":", maxsplit=1)
        end_hour, end_minute = end_text.split(":", maxsplit=1)
        return dt.time(int(start_hour), int(start_minute)), dt.time(int(end_hour), int(end_minute))
    except Exception:
        return dt.time(10, 5), dt.time(15, 30)


def _preferred_market_window_status(now: dt.datetime | None = None) -> dict[str, Any]:
    start, end = _parse_preferred_run_window(get_setting("regime_beta_preferred_run_window"))
    current = (now or dt.datetime.now(dt.timezone.utc)).astimezone(ET)
    status = get_market_hours_status(current)
    wall = current.timetz().replace(tzinfo=None)
    schedule_enabled = str(get_setting("regime_beta_schedule_enabled") or "").strip().lower() in {"1", "true", "yes", "on"}
    in_window = status == MarketHoursStatus.REGULAR and start <= wall <= end
    reason = "inside_preferred_market_window"
    if status != MarketHoursStatus.REGULAR:
        reason = f"market_{status.value}"
    elif wall < start:
        reason = "before_preferred_window"
    elif wall > end:
        reason = "after_preferred_window"
    if not schedule_enabled:
        in_window = True
        reason = "schedule_window_not_enforced"
    return {
        "current_et": current.isoformat(),
        "trade_date": current.date().isoformat(),
        "market_status": status.value,
        "window_start": start.strftime("%H:%M"),
        "window_end": end.strftime("%H:%M"),
        "timezone": "America/New_York",
        "schedule_enabled": schedule_enabled,
        "in_window": in_window,
        "reason": reason,
        "next_market_open": next_market_open(current).isoformat() if not in_window else None,
    }


def _setting_enabled(key: str, default: bool = True) -> bool:
    raw = get_setting(key)
    if raw in (None, ""):
        return bool(default)
    return str(raw).strip().lower() not in {"0", "false", "no", "off"}


def _ibkr_adapter_for_portfolio(portfolio_id: int, portfolio: dict[str, Any]) -> tuple[IBKRBrokerAdapter | None, dict[str, Any]]:
    config = DEFAULT_IBKR_CONFIG
    execution_mode = ibkr_execution_mode(config)
    real_backend = should_use_real_ibkr_backend(config)
    paper_backend_ready = should_use_ibkr_paper_backend(config)
    if execution_mode == "ibkr_paper_misconfigured":
        return None, {
            "execution_mode": execution_mode,
            "real_backend": False,
            "paper_backend_ready": False,
            "account_id": str(config.account_id),
            "reason": "IBKR paper backend is enabled, but account/host/port do not describe a local paper account.",
        }
    account_id = ibkr_backend_account_id(config) if real_backend else str(config.account_id)
    backend = get_ib_backend(
        portfolio_id,
        live=real_backend,
        account_id=account_id,
        starting_cash=float(portfolio.get("current_cash") or portfolio.get("starting_budget") or 100000.0),
        config=config,
        client_id_offset=int(getattr(config, "execution_client_id_offset", 0) or 0),
    )
    adapter = IBKRBrokerAdapter(
        backend,
        portfolio_id,
        host=str(config.host),
        port=int(config.port),
        client_id=int(getattr(backend, "_client_id", config.client_id)),
    )
    return adapter, {
        "execution_mode": execution_mode,
        "real_backend": real_backend,
        "paper_backend_ready": paper_backend_ready,
        "account_id": account_id,
    }


def run_daily_backup() -> dict[str, Any]:
    from .backup import create_backup, cleanup_old_backups

    backup = create_backup(label="daily")
    cleanup = cleanup_old_backups()
    return {"backup": backup, "cleanup": cleanup}


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
    for alert in all_alerts:
        payload = None
        if hasattr(alert, "ticker") and hasattr(alert, "new_label"):
            payload = save_alert(
                "regime_change",
                f"{alert.ticker}: {alert.previous_label or 'Unknown'} → {alert.new_label}",
                severity="warning",
                ticker=alert.ticker,
                message=f"Regime transition, action: {alert.composite_action}",
                data={"previous": alert.previous_label, "new": alert.new_label, "risk": alert.transition_risk},
            )
        elif hasattr(alert, "threshold"):
            payload = save_alert(
                "risk_spike",
                f"{alert.ticker}: transition risk {alert.transition_risk:.1%}",
                severity="warning",
                ticker=alert.ticker,
                message=f"Transition risk exceeded threshold {alert.threshold:.0%}.",
                data={"risk": alert.transition_risk, "threshold": alert.threshold},
            )
        elif hasattr(alert, "previous_action"):
            payload = save_alert(
                "signal_change",
                f"{alert.ticker}: {alert.previous_action or 'Unknown'} → {alert.new_action}",
                severity="info",
                ticker=alert.ticker,
                message="Composite signal changed.",
                data={"previous": alert.previous_action, "new": alert.new_action},
            )
        elif hasattr(alert, "stop_price"):
            severity = "critical" if float(alert.distance_pct) <= 0.02 else "warning"
            payload = save_alert(
                "stop_proximity",
                f"{alert.ticker}: {alert.distance_pct:.1%} from stop",
                severity=severity,
                ticker=alert.ticker,
                message=f"Price {alert.current_price:.2f} is near stop {alert.stop_price:.2f}.",
                data={"distance_pct": alert.distance_pct, "stop_price": alert.stop_price},
            )
        if payload and payload.get("severity") in {"warning", "critical"}:
            dispatch_notification_sync(str(payload.get("alert_type")), str(payload.get("title")), str(payload.get("message") or ""), str(payload.get("severity") or "info"))
    set_setting("last_regime_check_at", dt.datetime.now(dt.timezone.utc).isoformat())
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


def run_scheduled_thesis_monitors() -> dict[str, Any]:
    now = dt.datetime.now(dt.timezone.utc)
    if not _setting_enabled("thesis_monitor_hbm_enabled", True):
        return {"enabled": False, "runs": [], "checked_at": now.isoformat()}
    try:
        result = run_hbm_thesis_monitor(save=True, dispatch=True)
    except Exception as exc:
        logger_message = str(exc)
        save_alert(
            "execution_error",
            "HBM thesis monitor failed",
            severity="warning",
            ticker="MU",
            message=logger_message,
            data={"monitor_key": "hbm_mu"},
        )
        return {"enabled": True, "runs": [], "error": logger_message, "checked_at": now.isoformat()}
    set_setting("last_hbm_thesis_monitor_at", now.isoformat())
    return {"enabled": True, "runs": [result], "checked_at": now.isoformat()}


def run_scheduled_paper_plans() -> dict[str, Any]:
    now = dt.datetime.now(dt.timezone.utc)
    set_setting("watchdog_heartbeat", now.isoformat())
    set_setting("heartbeat_epoch", str(now.timestamp()))
    vix_status = check_vix_freeze()
    cached_payload = load_payload() or {}
    cached_rows = cached_payload.get("rows") if isinstance(cached_payload, dict) else []
    # Pass full payload rows through (not (regime, probability) tuples) so exit
    # logic can see composite_signal, signal timestamps, and forward-curve fields.
    # _cached_regime_map handles both shapes, but the tuple form silently drops
    # every key except regime/probability, disabling composite-signal exits and
    # the Neutral partial-reduce in the autonomous scheduler path.
    cached_regime = {
        str(row.get("ticker") or "").upper(): row
        for row in (cached_rows or [])
        if isinstance(row, dict) and str(row.get("ticker") or "").strip()
    }
    active_tickers = sorted(cached_regime.keys())
    validation = run_pre_trade_validation(active_tickers, vix=vix_status.get("vix"))
    window_status = _preferred_market_window_status(now)
    if not validation["valid"]:
        save_alert(
            "data_validation_failed",
            f"Pre-trade data validation issues: {len(validation['issues'])}",
            severity="warning",
            message="; ".join(validation["issues"][:5]),
            data=validation,
        )
    results: list[dict[str, Any]] = []
    enabled_portfolio_ids = set(parse_portfolio_ids(get_setting("autonomous_portfolio_ids")))
    for portfolio in list_paper_portfolios(include_closed=False):
        if str(portfolio.get("status") or "") != "Active":
            continue
        portfolio_id = int(portfolio["id"])
        if enabled_portfolio_ids and portfolio_id not in enabled_portfolio_ids:
            continue
        monitoring_alerts = sweep_monitoring_alerts(portfolio_id)
        expired = expire_stale_plans(portfolio_id)
        exec_result: dict[str, Any] | None = None
        generated: dict[str, Any]
        auto_result: dict[str, Any]
        polled = 0
        policy_cancel = {"cancelled": [], "failed": [], "checked": 0}
        broker_status: dict[str, Any] = {}
        ibkr_adapter: IBKRBrokerAdapter | None = None
        if str(portfolio.get("broker_type") or "paper").lower() == "ibkr":
            ibkr_adapter, broker_status = _ibkr_adapter_for_portfolio(portfolio_id, portfolio)
            try:
                polled = len(poll_pending_orders(ibkr_adapter, portfolio_id)) if ibkr_adapter is not None else 0
            except Exception:
                polled = 0
            if ibkr_adapter is not None:
                policy_cancel = cancel_submitted_orders_by_policy(portfolio_id, ibkr_adapter)
        else:
            policy_cancel = cancel_submitted_orders_by_policy(portfolio_id, PaperBrokerAdapter(portfolio_id))
        if not bool(window_status.get("in_window")):
            generated = {"buy_plans": [], "exit_plans": [], "skipped": True, "reason": window_status.get("reason")}
            auto_result = {"approved": 0, "skipped": 0, "blocked": 0, "details": [], "skipped_reason": window_status.get("reason")}
            exec_result = {"skipped": True, "reason": window_status.get("reason"), "window": window_status}
        else:
            generated = generate_daily_plans(portfolio_id, cached_regime=cached_regime, cached_payload=cached_payload)
            auto_result = auto_approve_plans(portfolio_id)
        approved_count = int(auto_result.get("approved") or 0)
        if approved_count > 0 and get_operating_mode() == "autonomous" and bool(window_status.get("in_window")):
            broker_type = str(portfolio.get("broker_type") or "paper").lower()
            if broker_type == "ibkr":
                execution_mode = str(broker_status.get("execution_mode") or "simulated")
                if ibkr_adapter is None:
                    exec_result = {"skipped": True, "reason": broker_status.get("reason") or "IBKR adapter unavailable", "broker": broker_status}
                elif is_live_trading_unlocked():
                    exec_result = {"skipped": True, "reason": "Live trading is unlocked; autonomous IBKR execution is paused.", "broker": broker_status}
                elif execution_mode == "ibkr_paper":
                    exec_result = auto_execute_approved(portfolio_id, ibkr_adapter, DEFAULT_RISK_GUARDRAILS, actor="scheduler")
                elif execution_mode == "ibkr_live":
                    exec_result = {"skipped": True, "reason": "Live account — manual execution required", "broker": broker_status}
                else:
                    exec_result = {
                        "skipped": True,
                        "reason": "IBKR paper backend is disabled; no internal simulator fallback for IBKR portfolios.",
                        "broker": broker_status,
                    }
            else:
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
                "alert_count": len(monitoring_alerts),
                "polled_orders": polled,
                "policy_cancel": policy_cancel,
                "broker_status": broker_status,
                "auto_approval": auto_result,
                "auto_execution": exec_result,
                "market_window": window_status,
            }
        )
    set_setting("last_paper_plans_at", dt.datetime.now(dt.timezone.utc).isoformat())
    return {"portfolios": results, "cached_regime_count": len(cached_regime), "vix_status": vix_status, "validation": validation}


def run_end_of_day_processing() -> dict[str, Any]:
    from .persistence import save_execution_quality_snapshot
    from .slippage import backfill_execution_benchmarks, compute_execution_quality

    now = dt.datetime.now(dt.timezone.utc)
    set_setting("watchdog_heartbeat", now.isoformat())
    set_setting("heartbeat_epoch", str(now.timestamp()))
    snapshots: list[dict[str, Any]] = []
    outcomes: list[dict[str, Any]] = []
    execution_quality: list[dict[str, Any]] = []
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
            adapter, _broker_status = _ibkr_adapter_for_portfolio(portfolio_id, portfolio)
            try:
                if adapter is not None:
                    poll_pending_orders(adapter, portfolio_id)
            except Exception:
                pass
        for position in get_paper_positions(portfolio_id, status="Closed"):
            if position.get("exit_date"):
                outcomes.append(record_trade_outcome(portfolio_id, position, float(position.get("exit_price") or 0.0)))
        benchmark_backfill = backfill_execution_benchmarks(portfolio_id)
        report = compute_execution_quality(portfolio_id)
        snapshot_id = save_execution_quality_snapshot(report)
        execution_quality.append(
            {
                "portfolio_id": portfolio_id,
                "backfill": benchmark_backfill,
                "snapshot_id": snapshot_id,
                "total_trades": report.total_trades,
                "overall_avg_impl_shortfall_bps": report.overall_avg_impl_shortfall_bps,
            }
        )
    performance = run_performance_snapshot()
    for snapshot in snapshots:
        daily_pnl = float(snapshot.get("unrealized_pnl") or 0.0) + float(snapshot.get("realized_pnl") or 0.0)
        daily_loss_limit = float(DEFAULT_RISK_GUARDRAILS.daily_loss_limit)
        daily_loss_limit_pct = float(getattr(DEFAULT_RISK_GUARDRAILS, "daily_loss_limit_pct", 0.0) or 0.0)
        summary = get_paper_portfolio_summary(int(snapshot["portfolio_id"]))
        equity = float(summary.get("current_cash") or 0.0) + float(summary.get("total_market_value") or 0.0)
        if daily_loss_limit_pct > 0 and equity > 0:
            daily_loss_limit = min(daily_loss_limit, equity * daily_loss_limit_pct)
        check_loss_breach(int(snapshot["portfolio_id"]), daily_pnl, daily_loss_limit)
    health = check_database_health()
    if not health["healthy"]:
        save_alert(
            "data_validation_failed",
            f"Database health issues: {len(health['issues'])}",
            severity="critical",
            message="; ".join(health["issues"][:5]),
            data=health,
        )
    thesis_monitors = run_scheduled_thesis_monitors()
    backup = run_daily_backup()
    try:
        digest_sent = asyncio.run(flush_digest())
    except Exception:
        digest_sent = False
    return {
        "snapshots": snapshots,
        "snapshot_count": len(snapshots),
        "outcomes": outcomes,
        "performance": performance,
        "execution_quality": execution_quality,
        "thesis_monitors": thesis_monitors,
        "backup": backup,
        "digest_sent": digest_sent,
        "health": health,
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
