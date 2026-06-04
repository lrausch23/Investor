#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


DEFAULT_PORTFOLIO_NAME = "Regime Agent Beta - IBKR Paper"
DEFAULT_BUDGET = 25_000.0
DEFAULT_BROKER_TYPE = "ibkr"


def _env_file_path() -> Path:
    return ROOT / ".env"


def _write_env_updates(updates: dict[str, str]) -> dict[str, Any]:
    env_path = _env_file_path()
    existing_lines: list[str] = []
    existing_keys: set[str] = set()
    if env_path.exists():
        for line in env_path.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if stripped and not stripped.startswith("#") and "=" in stripped:
                key = stripped.split("=", 1)[0].strip()
                if key in updates:
                    existing_lines.append(f"{key}={updates[key]}")
                    existing_keys.add(key)
                    continue
            existing_lines.append(line)
    for key, value in updates.items():
        if key not in existing_keys:
            existing_lines.append(f"{key}={value}")
    env_path.write_text("\n".join(existing_lines) + "\n", encoding="utf-8")
    for key, value in updates.items():
        os.environ[key] = str(value)
    return {"path": str(env_path), "updated": sorted(updates)}


def _ensure_ibkr_paper_backend_env() -> dict[str, Any]:
    return _write_env_updates(
        {
            "IBKR_HOST": os.environ.get("IBKR_HOST", "127.0.0.1") or "127.0.0.1",
            "IBKR_PORT": os.environ.get("IBKR_PORT", "7497") or "7497",
            "IBKR_CLIENT_ID": os.environ.get("IBKR_CLIENT_ID", "1") or "1",
            "IBKR_ACCOUNT_ID": os.environ.get("IBKR_ACCOUNT_ID", "DUP579027") or "DUP579027",
            "IBKR_PAPER_BACKEND": "true",
            "IBKR_LIVE_BACKEND": "false",
            "IBKR_EXECUTION_CLIENT_ID_OFFSET": os.environ.get("IBKR_EXECUTION_CLIENT_ID_OFFSET", "20") or "20",
            "IBKR_TIMEOUT": os.environ.get("IBKR_TIMEOUT", "10") or "10",
        }
    )


def _settings_payload(*, include_deployed_at: bool = True, broker_type: str = DEFAULT_BROKER_TYPE) -> dict[str, str]:
    market_data_config = {
        "benchmark_provider_order": ["cache", "ibkr", "stooq", "yahoo"],
        "benchmark_enabled": {"cache": True, "ibkr": True, "stooq": True, "yahoo": False},
        "momentum_provider_order": ["ibkr", "stooq", "finnhub"],
        "momentum_enabled": {"ibkr": True, "stooq": True, "finnhub": True},
        "regime_provider_order": ["ibkr", "yfinance"],
        "regime_enabled": {"ibkr": True, "yfinance": True},
    }
    payload = {
        "beta_target_monthly_return": "0.02",
        "beta_target_rolling_months": "6",
        "beta_target_benchmarks": "SPY,QQQ,SOXX",
        "fundamental_gate_enabled": "true",
        "fundamental_pass_on_insufficient": "true",
        "hurdle_enabled": "true",
        "duration_gate_enabled": "true",
        "hurdle_min_net_return_pct": "3.0",
        "min_regime_duration_days": "7",
        "anti_churn_enabled": "true",
        "anti_churn_max_round_trips_30d": "2",
        "anti_churn_cooldown_days": "30",
        "ltcg_override_enabled": "true",
        "sizing_method": "risk_budget",
        "sizing_base_risk_fraction": "0.02",
        "sizing_atr_multiplier": "2.0",
        "routing_algo_enabled": "true",
        "routing_algo_adv_pct_threshold": "0.01",
        "routing_algo_max_volume_rate": "0.20",
        "agent_competition_enabled": "true",
        "agent_diversification_enabled": "true",
        "agent_diversification_enforce_orders": "true",
        "agent_max_active_portfolios_per_ticker": "1",
        "agent_mandate_diversification_enabled": "true",
        "agent_submitted_order_cancel_enabled": "true",
        "agent_stale_order_max_age_minutes": "45",
        "agent_stale_order_price_deviation_pct": "0.01",
        "agent_cancel_before_close_minutes": "15",
        "agent_drawdown_pause_enabled": "true",
        "agent_max_drawdown_pause_pct": "0.05",
        "agent_beta_max_drawdown_pause_pct": "0.07",
        "agent_guardrail_cooldown_enabled": "true",
        "agent_guardrail_cooldown_event_limit": "5",
        "earnings_blackout_enabled": "true",
        "earnings_blackout_days": "2",
        "frontier_provider": "auto",
        "market_data_provider_config": json.dumps(market_data_config),
        "regime_beta_broker_type": str(broker_type or DEFAULT_BROKER_TYPE).strip().lower(),
        "ibkr_paper_execution_enabled": "true" if str(broker_type or DEFAULT_BROKER_TYPE).strip().lower() == "ibkr" else "false",
        "live_trading_unlocked": "false",
        "regime_beta_status": "active",
    }
    if include_deployed_at:
        payload["regime_beta_deployed_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
    return payload


def _ensure_agent_topology() -> dict[str, Any]:
    from src.app.routes.regime import _load_hmm_runtime
    from src.regime.agents import get_agent_registry
    from src.regime.agents.execution_agent import ExecutionAgent
    from src.regime.agents.fundamental_agent import FundamentalAgent
    from src.regime.agents.orchestrator import AgentOrchestrator, OrchestratorConfig
    from src.regime.agents.portfolio_agent import PortfolioTaxAgent
    from src.regime.agents.quant_agent import QuantAgent
    from src.regime.event_bus import get_event_bus, register_default_subscribers

    bus = get_event_bus()
    register_default_subscribers(bus)
    registry = get_agent_registry()
    runtime_loader = lambda: _load_hmm_runtime()
    registry.register(QuantAgent(bus, runtime_loader=runtime_loader))
    registry.register(FundamentalAgent(bus, runtime_loader=runtime_loader))
    registry.register(PortfolioTaxAgent(bus, runtime_loader=runtime_loader))
    registry.register(ExecutionAgent(bus, runtime_loader=runtime_loader))
    registry.register(
        AgentOrchestrator(
            bus,
            config=OrchestratorConfig(
                fundamental_timeout_seconds=30.0,
                portfolio_timeout_seconds=10.0,
                skip_fundamental_on_timeout=True,
                fundamental_veto_respected=True,
            ),
        )
    )
    return {
        "agent_count": len(registry.all_agents()),
        "agents": registry.status(),
        "subscriber_count": bus.subscriber_count(),
    }


def _find_portfolio(name: str) -> dict[str, Any] | None:
    from src.regime.persistence import list_paper_portfolios

    for portfolio in list_paper_portfolios(include_closed=True):
        if str(portfolio.get("name") or "") == name:
            return portfolio
    return None


def _portfolio_has_activity(portfolio_id: int) -> bool:
    from src.regime.persistence import get_paper_positions, get_trade_plans

    return bool(get_paper_positions(portfolio_id, status="all") or get_trade_plans(portfolio_id, status="all"))


def _ensure_portfolio(name: str, budget: float, broker_type: str = DEFAULT_BROKER_TYPE) -> dict[str, Any]:
    from src.regime.persistence import create_paper_portfolio, set_setting, update_paper_portfolio

    broker = str(broker_type or DEFAULT_BROKER_TYPE).strip().lower()
    if broker not in {"paper", "ibkr"}:
        raise ValueError("broker_type must be 'paper' or 'ibkr'.")
    if broker == "ibkr":
        _ensure_ibkr_paper_backend_env()
    portfolio = _find_portfolio(name)
    if portfolio is None:
        portfolio = create_paper_portfolio(name, budget, broker_type=broker)
    else:
        fields: dict[str, Any] = {"status": "Active", "broker_type": broker}
        if not _portfolio_has_activity(int(portfolio["id"])):
            fields["starting_budget"] = float(budget)
            fields["current_cash"] = float(budget)
        portfolio = update_paper_portfolio(int(portfolio["id"]), **fields) or portfolio
    return portfolio


def _ensure_agent_portfolios(budget: float, broker_type: str = DEFAULT_BROKER_TYPE) -> list[dict[str, Any]]:
    from src.regime.beta_agents import BETA_AGENT_PORTFOLIOS
    from src.regime.persistence import set_setting

    portfolios = [
        _ensure_portfolio(str(agent["name"]), budget, broker_type=broker_type)
        for agent in BETA_AGENT_PORTFOLIOS
    ]
    portfolio_ids = [str(int(portfolio["id"])) for portfolio in portfolios]
    if portfolio_ids:
        set_setting("regime_beta_portfolio_id", portfolio_ids[0])
        set_setting("regime_beta_portfolio_ids", ",".join(portfolio_ids))
        set_setting("autonomous_portfolio_ids", ",".join(portfolio_ids))
        set_setting("regime_beta_agent_count", str(len(portfolio_ids)))
        set_setting("regime_beta_total_budget", str(float(budget) * len(portfolio_ids)))
    return portfolios


def _apply_settings(*, include_deployed_at: bool = True, broker_type: str = DEFAULT_BROKER_TYPE) -> dict[str, Any]:
    from src.regime.persistence import (
        get_auto_approve_threshold,
        get_daily_capital_ceiling_pct,
        get_operating_mode,
        set_auto_approve_threshold,
        set_daily_capital_ceiling_pct,
        set_operating_mode,
        set_setting,
    )

    for key, value in _settings_payload(include_deployed_at=include_deployed_at, broker_type=broker_type).items():
        set_setting(key, value)
    set_operating_mode("autonomous")
    set_auto_approve_threshold(0.72)
    set_daily_capital_ceiling_pct(0.10)
    return {
        "operating_mode": get_operating_mode(),
        "auto_approve_threshold": get_auto_approve_threshold(),
        "daily_capital_ceiling_pct": get_daily_capital_ceiling_pct(),
    }


def _save_initial_snapshot(portfolio_id: int) -> dict[str, Any]:
    from src.regime.paper_trading import compute_daily_snapshot
    from src.regime.persistence import save_daily_snapshot

    snapshot = compute_daily_snapshot(portfolio_id)
    if not snapshot:
        return {}
    return save_daily_snapshot(
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


def _run_beta_paper_cycle(portfolio_id: int) -> dict[str, Any]:
    from src.app.routes.regime_cache import load_payload
    from src.regime.broker_adapter import PaperBrokerAdapter
    from src.regime.config import DEFAULT_RISK_GUARDRAILS
    from src.regime.monitoring import sweep_monitoring_alerts
    from src.regime.paper_trading import (
        auto_approve_plans,
        auto_execute_approved,
        cancel_submitted_orders_by_policy,
        expire_stale_plans,
        generate_daily_plans,
    )
    from src.regime.persistence import get_operating_mode, get_paper_portfolio, is_live_trading_unlocked, set_setting
    from src.regime.scheduled_runner import _ibkr_adapter_for_portfolio
    from src.regime.vix_freeze import check_vix_freeze

    now = dt.datetime.now(dt.timezone.utc)
    set_setting("watchdog_heartbeat", now.isoformat())
    set_setting("heartbeat_epoch", str(now.timestamp()))
    vix_status = check_vix_freeze()
    cached_payload = load_payload() or {}
    cached_rows = cached_payload.get("rows") if isinstance(cached_payload, dict) else []
    cached_regime = {
        str(row.get("ticker") or "").upper(): (str(row.get("regime") or ""), float(row.get("probability") or 0.0))
        for row in (cached_rows or [])
        if isinstance(row, dict) and str(row.get("ticker") or "").strip()
    }
    monitoring_alerts = sweep_monitoring_alerts(portfolio_id)
    expired = expire_stale_plans(portfolio_id)
    execution = None
    broker_status: dict[str, Any] = {}
    policy_cancel = {"cancelled": [], "failed": [], "checked": 0}
    adapter = None
    portfolio = get_paper_portfolio(portfolio_id) or {}
    if str(portfolio.get("broker_type") or "paper").lower() == "ibkr":
        adapter, broker_status = _ibkr_adapter_for_portfolio(portfolio_id, portfolio)
        if adapter is not None:
            policy_cancel = cancel_submitted_orders_by_policy(portfolio_id, adapter)
    else:
        adapter = PaperBrokerAdapter(portfolio_id)
        policy_cancel = cancel_submitted_orders_by_policy(portfolio_id, adapter)
    generated = generate_daily_plans(portfolio_id, cached_regime=cached_regime, cached_payload=cached_payload)
    auto_result = auto_approve_plans(portfolio_id)
    if auto_result.get("approved", 0) > 0 and get_operating_mode() == "autonomous":
        if str(portfolio.get("broker_type") or "paper").lower() == "ibkr":
            execution_mode = str(broker_status.get("execution_mode") or "simulated")
            if is_live_trading_unlocked():
                execution = {
                    "skipped": True,
                    "reason": "Live trading is unlocked; autonomous IBKR execution is paused.",
                    "broker": broker_status,
                }
            elif adapter is not None and execution_mode == "ibkr_paper":
                execution = auto_execute_approved(
                    portfolio_id,
                    adapter,
                    DEFAULT_RISK_GUARDRAILS,
                    actor="scheduler",
                )
            else:
                execution = {
                    "skipped": True,
                    "reason": broker_status.get("reason") or "IBKR paper backend is not ready.",
                    "broker": broker_status,
                }
        else:
            execution = auto_execute_approved(
                portfolio_id,
                PaperBrokerAdapter(portfolio_id),
                DEFAULT_RISK_GUARDRAILS,
                actor="scheduler",
            )
    set_setting("last_paper_plans_at", dt.datetime.now(dt.timezone.utc).isoformat())
    return {
        "portfolio_id": portfolio_id,
        "cached_regime_count": len(cached_regime),
        "buy_count": len(generated.get("buy_plans") or []),
        "holdings_count": len(generated.get("holdings_plans") or []),
        "exit_count": len(generated.get("exit_plans") or []),
        "created_count": generated.get("created_count", 0),
        "expired_count": expired,
        "alert_count": len(monitoring_alerts),
        "auto_approval": auto_result,
        "auto_execution": execution,
        "policy_cancel": policy_cancel,
        "broker_status": broker_status,
        "vix_status": vix_status,
    }


def deploy(*, name: str, budget: float, run_scheduler: bool, broker_type: str = DEFAULT_BROKER_TYPE) -> dict[str, Any]:
    from src.regime.config import IBKRConfig, validate_ibkr_readiness
    from src.regime.paper_trading import compute_beta_target_progress, compute_paper_performance

    env_update = _ensure_ibkr_paper_backend_env() if str(broker_type).strip().lower() == "ibkr" else None
    agents = _ensure_agent_topology()
    portfolios = _ensure_agent_portfolios(budget, broker_type=broker_type)
    portfolio = portfolios[0] if portfolios else _ensure_portfolio(name, budget, broker_type=broker_type)
    settings = _apply_settings(broker_type=broker_type)
    snapshots = [_save_initial_snapshot(int(item["id"])) for item in portfolios]
    scheduled = []
    if run_scheduler:
        scheduled = [_run_beta_paper_cycle(int(item["id"])) for item in portfolios]
    performance = compute_paper_performance(int(portfolio["id"]))
    ibkr_config = IBKRConfig()
    return {
        "portfolio": portfolio,
        "portfolios": portfolios,
        "agent_portfolio_count": len(portfolios),
        "per_agent_budget": float(budget),
        "total_agent_budget": float(budget) * len(portfolios),
        "settings": settings,
        "agents": agents,
        "environment": env_update,
        "snapshot": snapshots[0] if snapshots else {},
        "snapshots": snapshots,
        "scheduled_run": scheduled,
        "target": compute_beta_target_progress(int(portfolio["id"])),
        "performance_summary": {
            "total_equity": performance.get("total_equity"),
            "total_return_pct": performance.get("total_return_pct"),
            "target": performance.get("target"),
            "benchmarks": performance.get("benchmarks"),
        },
        "ibkr": {
            "host": ibkr_config.host,
            "port": ibkr_config.port,
            "account_id": ibkr_config.account_id,
            "paper_backend": ibkr_config.paper_backend,
            "live_backend": ibkr_config.live_backend,
            "readiness": validate_ibkr_readiness(),
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Deploy the Regime Agent paper beta.")
    parser.add_argument("--name", default=DEFAULT_PORTFOLIO_NAME)
    parser.add_argument("--budget", type=float, default=DEFAULT_BUDGET)
    parser.add_argument("--broker-type", choices=("ibkr", "paper"), default=DEFAULT_BROKER_TYPE)
    parser.add_argument("--no-scheduler-run", action="store_true", help="Configure the beta without running today's paper plan cycle.")
    args = parser.parse_args()
    result = deploy(name=args.name, budget=float(args.budget), run_scheduler=not args.no_scheduler_run, broker_type=str(args.broker_type))
    print(json.dumps(result, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
