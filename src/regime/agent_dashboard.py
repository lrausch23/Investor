from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timezone
from typing import Any

from .broker_adapter import OrderRequest, PaperBrokerAdapter, validate_guardrails
from .config import DEFAULT_IBKR_CONFIG, DEFAULT_RISK_GUARDRAILS, ibkr_execution_mode
from .paper_trading import compute_beta_target_progress, estimate_after_tax_performance
from .persistence import (
    get_auto_approve_threshold,
    get_daily_audit_summary,
    get_daily_capital_ceiling_pct,
    get_daily_snapshots,
    get_operating_mode,
    get_paper_portfolio,
    get_paper_portfolio_summary,
    get_llm_attribution_summary,
    get_setting,
    list_paper_portfolios,
    get_trade_plans,
    get_audit_trail,
)
from .agent_competition import (
    configured_beta_portfolio_ids as _shared_configured_beta_portfolio_ids,
    cross_agent_overlap_summary,
)
from .agent_frontier import agent_frontier_settings_rows
from .agent_policy import buy_pause_status, current_portfolio_drawdown_pct, recent_policy_event_count
from .beta_agents import BETA_AGENT_PORTFOLIOS
from .llm_layer import configured_frontier_model


EXPECTED_AGENTS: tuple[dict[str, str], ...] = (
    {
        "name": "quant",
        "label": "Quant",
        "role": "HMM regime, technical, ensemble, and ML signal generation",
    },
    {
        "name": "fundamental",
        "label": "Fundamental",
        "role": "Quality gate, moat review, catalyst review, and veto path",
    },
    {
        "name": "portfolio_tax",
        "label": "Portfolio / Tax",
        "role": "Sizing, anti-churn, hurdle-rate, wash-sale, and LTCG checks",
    },
    {
        "name": "execution",
        "label": "Execution",
        "role": "Guarded paper-order routing and fill tracking",
    },
    {
        "name": "orchestrator",
        "label": "Orchestrator",
        "role": "Sequencing across signal, review, sizing, and execution agents",
    },
)


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        parsed = float(value)
        return parsed if parsed == parsed else default
    except Exception:
        return default


def _as_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "on", "pass", "passed"}:
        return True
    if normalized in {"0", "false", "no", "off", "fail", "failed", "blocked"}:
        return False
    return None


def _setting_bool(key: str, default: bool = False) -> bool:
    raw = get_setting(key)
    if raw in (None, ""):
        return bool(default)
    parsed = _as_bool(raw)
    return bool(default) if parsed is None else parsed


def _metric(label: str, value: Any, *, display: str | None = None, level: str = "neutral") -> dict[str, Any]:
    return {
        "label": label,
        "value": value,
        "display": display if display is not None else str(value),
        "level": level,
    }


def _format_pct(value: float | None, digits: int = 1) -> str:
    if value is None:
        return "-"
    return f"{float(value):.{digits}f}%"


def _format_currency(value: float | None, digits: int = 0) -> str:
    if value is None:
        return "-"
    return f"${float(value):,.{digits}f}"


def _guardrail_check_row(check: Any) -> dict[str, Any]:
    if isinstance(check, dict):
        return dict(check)
    return {
        "name": getattr(check, "name", ""),
        "passed": getattr(check, "passed", None),
        "message": getattr(check, "message", ""),
        "actual": getattr(check, "actual", None),
        "limit": getattr(check, "limit", None),
    }


def _open_plan_readiness(
    portfolio_id: int,
    *,
    portfolio_name: str | None = None,
    plans: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    local_plans = [
        plan
        for plan in (plans if plans is not None else get_trade_plans(portfolio_id, status="all"))
        if str(plan.get("status") or "") in {"Pending", "Approved"}
    ]
    adapter = PaperBrokerAdapter(portfolio_id)
    rows: list[dict[str, Any]] = []
    counts: Counter[str] = Counter()
    for plan in local_plans:
        plan_id = int(plan.get("id") or 0)
        ticker = str(plan.get("ticker") or "").upper()
        action = str(plan.get("action") or "")
        try:
            order = OrderRequest(
                portfolio_id=portfolio_id,
                ticker=ticker,
                action=action,
                quantity=_as_float(plan.get("quantity")),
                order_type=str(plan.get("order_type") or "limit"),
                limit_price=_as_float(plan.get("proposed_price")) or None,
                time_in_force=str(plan.get("time_in_force") or "DAY"),
                routing_strategy=str(plan.get("routing_strategy") or ""),
                algo_strategy=str(plan.get("algo_strategy") or ""),
                source=str(plan.get("source") or "plan"),
                notes=str(plan.get("rationale") or ""),
            )
            result = validate_guardrails(order, adapter, DEFAULT_RISK_GUARDRAILS)
            checks = [_guardrail_check_row(check) for check in list(getattr(result, "checks", None) or [])]
            failures = [str(row.get("message") or row.get("name")) for row in checks if row.get("passed") is False]
            ready = bool(getattr(result, "allowed", False))
            counts["ready" if ready else "blocked"] += 1
            rows.append(
                {
                    "portfolio_id": int(portfolio_id),
                    "portfolio_name": portfolio_name or f"Portfolio {portfolio_id}",
                    "plan_id": plan_id,
                    "ticker": ticker,
                    "action": action,
                    "status": str(plan.get("status") or ""),
                    "ready": ready,
                    "reason": "; ".join(failures) if failures else "Guardrails currently pass.",
                    "guardrail_checks": checks,
                    "updated_at": plan.get("updated_at") or plan.get("created_at"),
                }
            )
        except Exception as exc:
            counts["error"] += 1
            rows.append(
                {
                    "portfolio_id": int(portfolio_id),
                    "portfolio_name": portfolio_name or f"Portfolio {portfolio_id}",
                    "plan_id": plan_id,
                    "ticker": ticker,
                    "action": action,
                    "status": str(plan.get("status") or ""),
                    "ready": False,
                    "reason": f"Readiness check failed: {exc}",
                    "guardrail_checks": [],
                    "updated_at": plan.get("updated_at") or plan.get("created_at"),
                }
            )
    counts["total"] = len(local_plans)
    return {"counts": dict(counts), "rows": rows}


def _latest_timestamp(rows: list[dict[str, Any]], *fields: str) -> str | None:
    values: list[str] = []
    for row in rows:
        for field in fields:
            raw = row.get(field)
            if raw not in (None, ""):
                values.append(str(raw))
                break
    return max(values) if values else None


def _status(enabled: bool, activity_count: int, attention_count: int = 0) -> tuple[str, str]:
    if not enabled:
        return "disabled", "Disabled"
    if attention_count > 0:
        return "attention", "Attention"
    if activity_count > 0:
        return "active", "Active"
    return "standby", "Standby"


def _count_by_status(plans: list[dict[str, Any]]) -> dict[str, int]:
    counts = Counter(str(plan.get("status") or "Unknown") for plan in plans)
    return {key: int(value) for key, value in sorted(counts.items())}


def _boolish(value: Any) -> bool:
    parsed = _as_bool(value)
    return bool(parsed) if parsed is not None else False


def _llm_model_key(plan: dict[str, Any]) -> str:
    display = str(plan.get("llm_model_display") or "").strip()
    if display:
        return display
    provider = str(plan.get("llm_provider") or "").strip() or "unknown"
    model = str(plan.get("llm_model") or "").strip() or "default"
    return f"{provider}: {model}"


def _llm_attribution_rows(plans: list[dict[str, Any]], positions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    current_by_ticker: dict[str, float] = {}
    for position in positions:
        ticker = str(position.get("ticker") or "").upper()
        if not ticker:
            continue
        current_by_ticker[ticker] = _as_float(position.get("current_price") or position.get("entry_price"))

    buckets: dict[tuple[str, str, str], dict[str, Any]] = {}
    for plan in plans:
        if not (_boolish(plan.get("llm_used")) or str(plan.get("llm_provider") or "").strip()):
            continue
        agent_key = str(plan.get("agent_key") or "").strip() or "unassigned"
        model_key = _llm_model_key(plan)
        influence = str(plan.get("llm_influence") or "reviewed").strip() or "reviewed"
        bucket_key = (agent_key, model_key, influence)
        bucket = buckets.setdefault(
            bucket_key,
            {
                "agent_key": agent_key,
                "model": model_key,
                "provider": str(plan.get("llm_provider") or ""),
                "influence": influence,
                "plans": 0,
                "influenced": 0,
                "executed": 0,
                "wins": 0,
                "pnl_samples": 0,
                "estimated_pnl": 0.0,
            },
        )
        bucket["plans"] += 1
        if _boolish(plan.get("llm_influenced")):
            bucket["influenced"] += 1
        status = str(plan.get("status") or "")
        if status in {"Executed", "Submitted", "Partially Filled"}:
            bucket["executed"] += 1
        ticker = str(plan.get("ticker") or "").upper()
        current_price = current_by_ticker.get(ticker)
        fill_price = _as_float(plan.get("execution_price") or plan.get("proposed_price"))
        quantity = _as_float(plan.get("filled_quantity") or plan.get("quantity"))
        if current_price is not None and fill_price > 0 and quantity > 0 and str(plan.get("action") or "") == "Buy":
            pnl = (current_price - fill_price) * quantity
            bucket["estimated_pnl"] += pnl
            bucket["pnl_samples"] += 1
            if pnl > 0:
                bucket["wins"] += 1

    rows = list(buckets.values())
    for row in rows:
        samples = int(row.get("pnl_samples") or 0)
        row["win_rate"] = (float(row.get("wins") or 0) / samples * 100.0) if samples else None
        row["estimated_pnl_display"] = _format_currency(_as_float(row.get("estimated_pnl")), 0)
        row["win_rate_display"] = _format_pct(row["win_rate"], 0) if row["win_rate"] is not None else "-"
    rows.sort(key=lambda item: (_as_float(item.get("estimated_pnl")), int(item.get("influenced") or 0), int(item.get("plans") or 0)), reverse=True)
    return rows


def _combine_llm_attribution(dashboards: list[dict[str, Any]]) -> list[dict[str, Any]]:
    buckets: dict[tuple[str, str, str], dict[str, Any]] = {}
    for dashboard in dashboards:
        for row in list(dashboard.get("llm_attribution") or []):
            key = (str(row.get("agent_key") or ""), str(row.get("model") or ""), str(row.get("influence") or ""))
            bucket = buckets.setdefault(
                key,
                {
                    "agent_key": key[0],
                    "model": key[1],
                    "provider": row.get("provider"),
                    "influence": key[2],
                    "plans": 0,
                    "influenced": 0,
                    "executed": 0,
                    "wins": 0,
                    "pnl_samples": 0,
                    "estimated_pnl": 0.0,
                },
            )
            for field in ("plans", "influenced", "executed", "wins", "pnl_samples"):
                bucket[field] += int(row.get(field) or 0)
            bucket["estimated_pnl"] += _as_float(row.get("estimated_pnl"))
    rows = list(buckets.values())
    for row in rows:
        samples = int(row.get("pnl_samples") or 0)
        row["win_rate"] = (float(row.get("wins") or 0) / samples * 100.0) if samples else None
        row["estimated_pnl_display"] = _format_currency(_as_float(row.get("estimated_pnl")), 0)
        row["win_rate_display"] = _format_pct(row["win_rate"], 0) if row["win_rate"] is not None else "-"
    rows.sort(key=lambda item: (_as_float(item.get("estimated_pnl")), int(item.get("influenced") or 0), int(item.get("plans") or 0)), reverse=True)
    return rows


def _parse_schedule_status() -> Any:
    raw = get_setting("regime_beta_market_session_last_status")
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return raw


def _parse_market_data_config() -> dict[str, Any] | None:
    raw = get_setting("market_data_provider_config")
    if not raw:
        return None
    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, dict) else None
    except Exception:
        return None


def _daily_pnl(equity: float, portfolio_id: int) -> float | None:
    snapshots = get_daily_snapshots(portfolio_id)
    if not snapshots:
        return None
    today = datetime.now(timezone.utc).date().isoformat()
    prior = [row for row in snapshots if str(row.get("snapshot_date") or "") < today]
    anchor = prior[-1] if prior else snapshots[-1]
    anchor_equity = _as_float(anchor.get("equity"), 0.0)
    if anchor_equity <= 0:
        return None
    return equity - anchor_equity


def _agent_enabled_map(agents_status: list[dict[str, Any]] | None) -> dict[str, dict[str, Any]]:
    return {
        str(row.get("name") or ""): row
        for row in (agents_status or [])
        if isinstance(row, dict) and str(row.get("name") or "")
    }


def _agent_trace_mentions(plan: dict[str, Any], agent_name: str) -> bool:
    trace = str(plan.get("agent_trace") or "").lower()
    if agent_name == "portfolio_tax":
        return "portfolio:" in trace or "portfolio_tax" in trace
    return f"{agent_name}:" in trace or agent_name in trace


def compute_agent_portfolio_dashboard(
    portfolio_id: int,
    *,
    agents_status: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Build a shared beta-portfolio dashboard with per-agent operating status."""

    portfolio = get_paper_portfolio(portfolio_id)
    if portfolio is None:
        return {"status": "not_deployed", "portfolio": None, "agents": []}

    broker_type = str(portfolio.get("broker_type") or "paper").strip().lower() or "paper"
    execution_mode = ibkr_execution_mode(DEFAULT_IBKR_CONFIG)
    if broker_type == "paper":
        broker_label = "Internal paper simulator"
    elif execution_mode == "ibkr_paper":
        broker_label = "IBKR paper account (TWS/Gateway)"
    elif execution_mode == "ibkr_paper_misconfigured":
        broker_label = "IBKR paper backend misconfigured"
    elif execution_mode == "ibkr_live":
        broker_label = "IBKR live backend"
    else:
        broker_label = "IBKR simulated backend"
    portfolio_scope = (
        "Internal simulated paper portfolio (SQLite)"
        if broker_type == "paper"
        else "IBKR paper account with local beta ledger"
    )
    is_internal_simulated = broker_type == "paper" or (broker_type == "ibkr" and execution_mode == "simulated")
    summary = get_paper_portfolio_summary(portfolio_id)
    after_tax = estimate_after_tax_performance(portfolio_id, summary=summary)
    summary = {**summary, **after_tax}
    positions = list(summary.get("positions") or [])
    plans = get_trade_plans(portfolio_id, status="all")
    llm_attribution = _llm_attribution_rows(plans, positions)
    llm_outcome_attribution = get_llm_attribution_summary(days=30)
    audit = get_audit_trail(portfolio_id=portfolio_id, days=7, limit=200)
    daily_audit = get_daily_audit_summary(portfolio_id)
    agent_map = _agent_enabled_map(agents_status)

    cash = _as_float(summary.get("current_cash"))
    market_value = _as_float(summary.get("total_market_value"))
    equity = cash + market_value
    starting_budget = _as_float(portfolio.get("starting_budget"))
    return_pct = ((equity - starting_budget) / starting_budget * 100.0) if starting_budget > 0 else 0.0
    exposure_pct = (market_value / equity * 100.0) if equity > 0 else 0.0
    daily_pnl = _daily_pnl(equity, portfolio_id)
    target = compute_beta_target_progress(portfolio_id, summary=summary)

    plan_counts = _count_by_status(plans)
    pending_like = sum(plan_counts.get(status, 0) for status in ("Pending", "Approved", "Submitted", "Partially Filled"))
    audit_counts = Counter(str(row.get("event_type") or "") for row in audit)
    traced_plans = [plan for plan in plans if str(plan.get("agent_trace") or "").strip()]
    quant_plans = [
        plan
        for plan in plans
        if str(plan.get("source") or "").lower() in {"discovery", "holdings", "rebalance", "agent"}
        or _agent_trace_mentions(plan, "quant")
    ]
    scores = [_as_float(plan.get("meta_labeler_score")) for plan in quant_plans if plan.get("meta_labeler_score") not in (None, "")]
    avg_score = sum(scores) / len(scores) if scores else None

    fundamental_plans = [plan for plan in plans if _agent_trace_mentions(plan, "fundamental")]
    fundamental_vetoes = sum(
        1
        for plan in plans
        if "fundamental:vetoed=true" in str(plan.get("agent_trace") or "").lower()
        or "fundamental_veto" in str(plan.get("rationale") or "").lower()
    )
    fundamental_reviews = len(fundamental_plans)
    portfolio_checks = [
        plan
        for plan in plans
        if plan.get("anti_churn_passed") is not None
        or plan.get("hurdle_passed") is not None
        or plan.get("duration_gate_passed") is not None
        or plan.get("ltcg_override_active") is not None
        or _agent_trace_mentions(plan, "portfolio_tax")
    ]
    anti_churn_blocks = sum(1 for plan in plans if _as_bool(plan.get("anti_churn_passed")) is False)
    hurdle_blocks = sum(1 for plan in plans if _as_bool(plan.get("hurdle_passed")) is False)
    duration_blocks = sum(1 for plan in plans if _as_bool(plan.get("duration_gate_passed")) is False)
    ltcg_overrides = sum(1 for plan in plans if _as_bool(plan.get("ltcg_override_active")) is True)
    tax_savings = sum(_as_float(plan.get("ltcg_tax_savings")) for plan in plans)
    submitted = int(plan_counts.get("Submitted", 0) + plan_counts.get("Partially Filled", 0))
    filled = int(audit_counts.get("filled", 0) + audit_counts.get("partially_filled", 0))
    guardrail_blocks = int(audit_counts.get("guardrail_blocked", 0))
    auto_approved = int(audit_counts.get("auto_approved", 0))

    def enabled_for(agent_name: str) -> tuple[bool, bool, list[str]]:
        row = agent_map.get(agent_name)
        registered = row is not None
        if row is None:
            return True, False, []
        return bool(row.get("enabled", True)), True, list(row.get("subscriptions") or [])

    latest_plan_at = _latest_timestamp(plans, "updated_at", "created_at")
    latest_audit_at = _latest_timestamp(audit, "created_at")
    latest_any = max([value for value in (latest_plan_at, latest_audit_at) if value], default=None)

    agent_rows: list[dict[str, Any]] = []
    for agent in EXPECTED_AGENTS:
        name = agent["name"]
        enabled, registered, subscriptions = enabled_for(name)
        metrics: list[dict[str, Any]]
        activity_count = 0
        attention_count = 0
        latest_activity = latest_any
        if name == "quant":
            activity_count = len(quant_plans)
            metrics = [
                _metric("Signals", len(quant_plans)),
                _metric("Pending", pending_like),
                _metric(
                    "Avg ML",
                    avg_score,
                    display=_format_pct(avg_score * 100.0, 0) if avg_score is not None else "-",
                    level="safe" if avg_score is not None and avg_score >= 0.65 else "neutral",
                ),
            ]
            latest_activity = _latest_timestamp(quant_plans, "updated_at", "created_at") or latest_any
        elif name == "fundamental":
            gate_enabled = _setting_bool("fundamental_gate_enabled", True)
            activity_count = fundamental_reviews
            attention_count = fundamental_vetoes
            metrics = [
                _metric("Gate", "On" if gate_enabled else "Off", level="safe" if gate_enabled else "warn"),
                _metric("Reviews", fundamental_reviews),
                _metric("Vetoes", fundamental_vetoes, level="warn" if fundamental_vetoes else "neutral"),
            ]
            latest_activity = _latest_timestamp(fundamental_plans, "updated_at", "created_at")
        elif name == "portfolio_tax":
            activity_count = len(portfolio_checks)
            attention_count = anti_churn_blocks + hurdle_blocks + duration_blocks
            metrics = [
                _metric("Checks", len(portfolio_checks)),
                _metric("Blocks", attention_count, level="warn" if attention_count else "neutral"),
                _metric("LTCG", ltcg_overrides),
                _metric("Tax Save", tax_savings, display=_format_currency(tax_savings, 0)),
            ]
            latest_activity = _latest_timestamp(portfolio_checks, "updated_at", "created_at") or latest_any
        elif name == "execution":
            activity_count = submitted + filled + auto_approved
            attention_count = guardrail_blocks
            metrics = [
                _metric("Submitted", submitted),
                _metric("Fills", filled, level="safe" if filled else "neutral"),
                _metric("Auto", auto_approved),
                _metric("Blocks", guardrail_blocks, level="warn" if guardrail_blocks else "neutral"),
            ]
            latest_activity = latest_audit_at
        else:
            missing_agents = max(0, len(EXPECTED_AGENTS) - len(agent_map)) if agents_status is not None else 0
            activity_count = len(traced_plans) + len(quant_plans)
            attention_count = missing_agents
            metrics = [
                _metric("Trace", len(traced_plans)),
                _metric("Agents", len(agent_map) or len(EXPECTED_AGENTS)),
                _metric("Missing", missing_agents, level="warn" if missing_agents else "neutral"),
            ]

        status, status_label = _status(enabled, activity_count, attention_count)
        agent_rows.append(
            {
                **agent,
                "enabled": enabled,
                "registered": registered,
                "subscriptions": subscriptions,
                "status": status,
                "status_label": status_label,
                "portfolio_id": int(portfolio_id),
                "portfolio_name": portfolio.get("name"),
                "portfolio_scope": portfolio_scope,
                "activity_count": activity_count,
                "attention_count": attention_count,
                "latest_activity_at": latest_activity,
                "metrics": metrics,
            }
        )

    daily_capital_ceiling_pct = get_daily_capital_ceiling_pct()
    daily_loss_pct = _as_float(getattr(DEFAULT_RISK_GUARDRAILS, "daily_loss_limit_pct", 0.0), 0.0)
    static_daily_loss = _as_float(getattr(DEFAULT_RISK_GUARDRAILS, "daily_loss_limit", 0.0), 0.0)
    scaled_daily_loss = equity * daily_loss_pct if equity > 0 and daily_loss_pct > 0 else static_daily_loss
    daily_loss_limit = min(static_daily_loss, scaled_daily_loss) if static_daily_loss > 0 and scaled_daily_loss > 0 else max(static_daily_loss, scaled_daily_loss)

    return {
        "status": get_setting("regime_beta_status") or "active",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "portfolio": portfolio,
        "broker": {
            "type": broker_type,
            "label": broker_label,
            "execution_mode": execution_mode,
            "account_id": DEFAULT_IBKR_CONFIG.account_id if broker_type == "ibkr" else None,
            "paper_backend": bool(DEFAULT_IBKR_CONFIG.paper_backend),
            "live_backend": bool(DEFAULT_IBKR_CONFIG.live_backend),
            "is_internal_simulated": is_internal_simulated,
        },
        "portfolio_summary": {
            "portfolio_id": int(portfolio_id),
            "portfolio_name": portfolio.get("name"),
            "portfolio_scope": portfolio_scope,
            "starting_budget": starting_budget,
            "cash": cash,
            "market_value": market_value,
            "equity": equity,
            "after_tax_equity": _as_float(summary.get("after_tax_equity"), equity),
            "after_tax_profit": _as_float(summary.get("after_tax_profit"), equity - starting_budget),
            "after_tax_return_pct": _as_float(summary.get("after_tax_return_pct"), return_pct),
            "estimated_tax_drag": _as_float(summary.get("estimated_tax_drag")),
            "estimated_realized_tax": _as_float(summary.get("estimated_realized_tax")),
            "estimated_unrealized_tax": _as_float(summary.get("estimated_unrealized_tax")),
            "estimated_realized_loss_tax_value": _as_float(summary.get("estimated_realized_loss_tax_value")),
            "estimated_unrealized_loss_tax_value": _as_float(summary.get("estimated_unrealized_loss_tax_value")),
            "tax_model": summary.get("tax_model"),
            "total_return_pct": return_pct,
            "exposure_pct": exposure_pct,
            "daily_pnl": daily_pnl,
            "realized_pnl": _as_float(summary.get("realized_pnl")),
            "unrealized_pnl": _as_float(summary.get("unrealized_pnl")),
            "positions_open": int(summary.get("positions_open") or len(positions)),
            "positions_closed": int(summary.get("positions_closed") or 0),
            "positions": positions,
        },
        "target": target,
        "agents": agent_rows,
        "llm_attribution": llm_attribution,
        "llm_outcome_attribution": llm_outcome_attribution,
        "plan_counts": plan_counts,
        "pending_action_count": int(pending_like),
        "open_plan_readiness": _open_plan_readiness(
            int(portfolio_id),
            portfolio_name=str(portfolio.get("name") or f"Portfolio {portfolio_id}"),
            plans=plans,
        ),
        "audit_summary": daily_audit,
        "recent_activity": audit[:12],
        "guardrails": {
            "operating_mode": get_operating_mode(),
            "auto_approve_threshold": get_auto_approve_threshold(),
            "daily_capital_ceiling_pct": daily_capital_ceiling_pct,
            "daily_capital_ceiling_value": equity * float(daily_capital_ceiling_pct or 0.0),
            "daily_loss_limit": daily_loss_limit,
            "daily_loss_limit_pct": daily_loss_pct,
            "live_trading_unlocked": str(get_setting("live_trading_unlocked") or "false").lower() == "true",
        },
        "schedule": {
            "enabled": get_setting("regime_beta_schedule_enabled") == "true",
            "label": get_setting("regime_beta_schedule_label"),
            "preferred_window": get_setting("regime_beta_preferred_run_window"),
            "last_cycle_date": get_setting("regime_beta_last_market_session_cycle_date"),
            "last_cycle_at": get_setting("regime_beta_last_market_session_cycle_at"),
            "last_checked_at": get_setting("regime_beta_market_session_last_checked_at"),
            "last_status": _parse_schedule_status(),
        },
        "data": {
            "market_data_provider_config": _parse_market_data_config(),
            "last_paper_plans_at": get_setting("last_paper_plans_at"),
        },
    }


def _configured_beta_portfolio_ids() -> list[int]:
    return _shared_configured_beta_portfolio_ids()


def _combine_plan_counts(dashboards: list[dict[str, Any]]) -> dict[str, int]:
    counter: Counter[str] = Counter()
    for dashboard in dashboards:
        counter.update({str(key): int(value) for key, value in dict(dashboard.get("plan_counts") or {}).items()})
    return {key: int(value) for key, value in sorted(counter.items())}


def _combine_open_plan_readiness(dashboards: list[dict[str, Any]]) -> dict[str, Any]:
    counter: Counter[str] = Counter()
    rows: list[dict[str, Any]] = []
    for dashboard in dashboards:
        readiness = dict(dashboard.get("open_plan_readiness") or {})
        counter.update({str(key): int(value) for key, value in dict(readiness.get("counts") or {}).items()})
        rows.extend(list(readiness.get("rows") or []))
    counter["total"] = len(rows)
    return {"counts": {key: int(value) for key, value in sorted(counter.items())}, "rows": rows}


def _agent_portfolio_row(agent: dict[str, str], dashboard: dict[str, Any]) -> dict[str, Any]:
    summary = dict(dashboard.get("portfolio_summary") or {})
    target = dict(dashboard.get("target") or {})
    plan_counts = dict(dashboard.get("plan_counts") or {})
    starting_budget = _as_float(summary.get("starting_budget"))
    equity = _as_float(summary.get("equity"))
    profit = equity - starting_budget
    return_pct = _as_float(summary.get("total_return_pct"))
    after_tax_equity = _as_float(summary.get("after_tax_equity"), equity)
    after_tax_profit = _as_float(summary.get("after_tax_profit"), after_tax_equity - starting_budget)
    after_tax_return_pct = _as_float(summary.get("after_tax_return_pct"), return_pct)
    estimated_tax_drag = _as_float(summary.get("estimated_tax_drag"))
    submitted = int(plan_counts.get("Submitted", 0) + plan_counts.get("Partially Filled", 0))
    executed = int(plan_counts.get("Executed", 0))
    pending = int(sum(plan_counts.get(status, 0) for status in ("Pending", "Approved", "Submitted", "Partially Filled")))
    activity = list(dashboard.get("recent_activity") or [])
    latest_activity = _latest_timestamp(activity, "created_at") or None
    if latest_activity is None:
        latest_activity = _latest_timestamp(get_trade_plans(int(summary.get("portfolio_id") or 0), status="all"), "updated_at", "created_at")
    activity_count = pending + int(plan_counts.get("Executed", 0)) + int(plan_counts.get("Cancelled", 0)) + int(plan_counts.get("Rejected", 0))
    status, status_label = _status(True, activity_count)
    return {
        "name": str(agent["key"]),
        "label": str(agent["label"]),
        "role": str(agent["role"]),
        "enabled": True,
        "registered": True,
        "subscriptions": [],
        "status": status,
        "status_label": status_label,
        "portfolio_id": int(summary.get("portfolio_id") or 0),
        "portfolio_name": summary.get("portfolio_name"),
        "portfolio_scope": summary.get("portfolio_scope"),
        "activity_count": activity_count,
        "attention_count": int(plan_counts.get("Rejected", 0)),
        "latest_activity_at": latest_activity,
        "metrics": [
            _metric("Equity", equity, display=_format_currency(equity, 0)),
            _metric("After Tax", after_tax_profit, display=_format_currency(after_tax_profit, 0), level="safe" if after_tax_profit >= 0 else "bad"),
            _metric("Pending", pending),
            _metric("Submitted", submitted),
            _metric("Executed", executed),
            _metric("Return", after_tax_return_pct, display=_format_pct(after_tax_return_pct, 2)),
        ],
        "profit": profit,
        "return_pct": return_pct,
        "after_tax_equity": after_tax_equity,
        "after_tax_profit": after_tax_profit,
        "after_tax_return_pct": after_tax_return_pct,
        "estimated_tax_drag": estimated_tax_drag,
        "summary": summary,
        "target": target,
        "plan_counts": plan_counts,
    }


def _competition_payload(agent_rows: list[dict[str, Any]], portfolio_ids: list[int]) -> dict[str, Any]:
    enabled = _setting_bool("agent_competition_enabled", True)
    leaderboard: list[dict[str, Any]] = []
    for row in agent_rows:
        summary = dict(row.get("summary") or {})
        starting_budget = _as_float(summary.get("starting_budget"))
        equity = _as_float(summary.get("equity"))
        profit = equity - starting_budget
        return_pct = _as_float(summary.get("total_return_pct"))
        after_tax_equity = _as_float(summary.get("after_tax_equity"), equity)
        after_tax_profit = _as_float(summary.get("after_tax_profit"), after_tax_equity - starting_budget)
        after_tax_return_pct = _as_float(summary.get("after_tax_return_pct"), return_pct)
        estimated_tax_drag = _as_float(summary.get("estimated_tax_drag"))
        pause = buy_pause_status(int(row.get("portfolio_id") or 0))
        risk_warnings = [
            str(reason.get("code") or reason.get("message") or "policy_warning")
            for reason in list(pause.get("reasons") or [])
            if isinstance(reason, dict)
        ]
        policy_events = recent_policy_event_count(int(row.get("portfolio_id") or 0))
        if policy_events:
            risk_warnings.append(f"policy_events_{policy_events}")
        model_setting = dict(row.get("llm_model_setting") or {})
        leaderboard.append(
            {
                "rank": 0,
                "agent": row.get("name"),
                "label": row.get("label"),
                "portfolio_id": int(row.get("portfolio_id") or 0),
                "portfolio_name": row.get("portfolio_name"),
                "profit": profit,
                "profit_display": _format_currency(profit, 0),
                "after_tax_profit": after_tax_profit,
                "after_tax_profit_display": _format_currency(after_tax_profit, 0),
                "return_pct": return_pct,
                "return_display": _format_pct(return_pct, 2),
                "after_tax_return_pct": after_tax_return_pct,
                "after_tax_return_display": _format_pct(after_tax_return_pct, 2),
                "equity": equity,
                "equity_display": _format_currency(equity, 0),
                "after_tax_equity": after_tax_equity,
                "after_tax_equity_display": _format_currency(after_tax_equity, 0),
                "estimated_tax_drag": estimated_tax_drag,
                "estimated_tax_drag_display": _format_currency(estimated_tax_drag, 0),
                "starting_budget": starting_budget,
                "positions_open": int(summary.get("positions_open") or 0),
                "submitted_count": int((row.get("plan_counts") or {}).get("Submitted", 0))
                + int((row.get("plan_counts") or {}).get("Partially Filled", 0)),
                "executed_count": int((row.get("plan_counts") or {}).get("Executed", 0)),
                "drawdown_pct": current_portfolio_drawdown_pct(int(row.get("portfolio_id") or 0)) * 100.0,
                "policy_event_count": policy_events,
                "risk_warnings": risk_warnings,
                "risk_status": "paused" if not pause.get("allowed", True) else ("warning" if risk_warnings else "ok"),
                "llm_provider": model_setting.get("provider"),
                "llm_model": model_setting.get("model"),
                "llm_model_display": model_setting.get("configured_model"),
            }
        )
    leaderboard.sort(
        key=lambda item: (
            float(item.get("after_tax_profit") or 0.0),
            float(item.get("after_tax_return_pct") or 0.0),
            float(item.get("profit") or 0.0),
            -int(item.get("portfolio_id") or 0),
        ),
        reverse=True,
    )
    row_by_portfolio = {int(row.get("portfolio_id") or 0): row for row in agent_rows}
    for index, item in enumerate(leaderboard, start=1):
        item["rank"] = index
        item["status"] = "winner" if index == 1 else "competing"
        portfolio_row = row_by_portfolio.get(int(item.get("portfolio_id") or 0))
        if portfolio_row is not None:
            portfolio_row["rank"] = index
            portfolio_row["competition_status"] = item["status"]
            portfolio_row["competition_profit"] = item["after_tax_profit"]
            portfolio_row["competition_return_pct"] = item["after_tax_return_pct"]
    overlap = cross_agent_overlap_summary(portfolio_ids)
    item_by_portfolio = {int(item.get("portfolio_id") or 0): item for item in leaderboard}
    for overlap_row in list(overlap.get("risk_tickers") or []):
        ticker = str(overlap_row.get("ticker") or "")
        for owner in list(overlap_row.get("owners") or []):
            leader_item = item_by_portfolio.get(int(owner.get("portfolio_id") or 0))
            if leader_item is None:
                continue
            warning = f"overlap_{ticker}"
            if warning not in leader_item["risk_warnings"]:
                leader_item["risk_warnings"].append(warning)
            if leader_item.get("risk_status") == "ok":
                leader_item["risk_status"] = "warning"
    return {
        "enabled": enabled,
        "basis": "estimated_after_tax_profit",
        "basis_label": "Estimated after-tax equity minus starting budget",
        "winner": leaderboard[0] if enabled and leaderboard else None,
        "leaderboard": leaderboard,
        "overlap": overlap,
    }


def compute_beta_agent_dashboard(
    *,
    agents_status: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    portfolio_ids = _configured_beta_portfolio_ids()
    if not portfolio_ids:
        return {"status": "not_deployed", "portfolio": None, "agents": [], "agent_portfolios": []}
    dashboards = [
        compute_agent_portfolio_dashboard(portfolio_id, agents_status=agents_status)
        for portfolio_id in portfolio_ids
    ]
    dashboards = [dashboard for dashboard in dashboards if dashboard.get("portfolio")]
    if not dashboards:
        return {"status": "not_deployed", "portfolio": None, "agents": [], "agent_portfolios": []}
    model_settings = agent_frontier_settings_rows(
        portfolio_ids=portfolio_ids,
        configured_model_fn=configured_frontier_model,
    )
    model_by_agent = {str(row.get("agent_key") or ""): row for row in model_settings}
    if len(dashboards) == 1:
        single = dict(dashboards[0])
        single["agent_model_settings"] = model_settings
        single["agent_portfolios"] = [
            {
                "agent": BETA_AGENT_PORTFOLIOS[0],
                "dashboard": dashboards[0],
                "summary": dashboards[0].get("portfolio_summary") or {},
                "plan_counts": dashboards[0].get("plan_counts") or {},
            }
        ]
        return single

    agent_portfolios: list[dict[str, Any]] = []
    agent_rows: list[dict[str, Any]] = []
    for index, dashboard in enumerate(dashboards):
        agent = BETA_AGENT_PORTFOLIOS[index] if index < len(BETA_AGENT_PORTFOLIOS) else {
            "key": f"agent_{index + 1}",
            "label": f"Agent {index + 1}",
            "name": str((dashboard.get("portfolio") or {}).get("name") or f"Agent {index + 1}"),
            "role": "Independent beta agent portfolio.",
        }
        row = _agent_portfolio_row(agent, dashboard)
        row["llm_model_setting"] = model_by_agent.get(str(agent.get("key") or ""))
        agent_rows.append(row)
        agent_portfolios.append(
            {
                "agent": agent,
                "dashboard": dashboard,
                "summary": dashboard.get("portfolio_summary") or {},
                "target": dashboard.get("target") or {},
                "plan_counts": dashboard.get("plan_counts") or {},
                "recent_activity": list(dashboard.get("recent_activity") or [])[:6],
                "llm_model_setting": model_by_agent.get(str(agent.get("key") or "")),
            }
        )

    summaries = [dict(dashboard.get("portfolio_summary") or {}) for dashboard in dashboards]
    total_starting = sum(_as_float(row.get("starting_budget")) for row in summaries)
    total_cash = sum(_as_float(row.get("cash")) for row in summaries)
    total_market_value = sum(_as_float(row.get("market_value")) for row in summaries)
    total_equity = total_cash + total_market_value
    total_after_tax_equity = sum(_as_float(row.get("after_tax_equity"), _as_float(row.get("equity"))) for row in summaries)
    total_tax_drag = sum(_as_float(row.get("estimated_tax_drag")) for row in summaries)
    total_return_pct = ((total_equity - total_starting) / total_starting * 100.0) if total_starting > 0 else 0.0
    total_after_tax_return_pct = ((total_after_tax_equity - total_starting) / total_starting * 100.0) if total_starting > 0 else 0.0
    plan_counts = _combine_plan_counts(dashboards)
    recent_activity = sorted(
        [row for dashboard in dashboards for row in list(dashboard.get("recent_activity") or [])],
        key=lambda row: str(row.get("created_at") or ""),
        reverse=True,
    )[:12]
    first = dict(dashboards[0])
    first["portfolio_summary"] = {
        "portfolio_id": int(summaries[0].get("portfolio_id") or portfolio_ids[0]),
        "portfolio_name": "Regime Agent Beta - 4 Agent Portfolios",
        "portfolio_scope": "Four IBKR paper agent ledgers ($25,000 each)",
        "starting_budget": total_starting,
        "cash": total_cash,
        "market_value": total_market_value,
        "equity": total_equity,
        "after_tax_equity": total_after_tax_equity,
        "after_tax_profit": total_after_tax_equity - total_starting,
        "after_tax_return_pct": total_after_tax_return_pct,
        "estimated_tax_drag": total_tax_drag,
        "estimated_realized_tax": sum(_as_float((dashboard.get("portfolio_summary") or {}).get("estimated_realized_tax")) for dashboard in dashboards),
        "estimated_unrealized_tax": sum(_as_float((dashboard.get("portfolio_summary") or {}).get("estimated_unrealized_tax")) for dashboard in dashboards),
        "estimated_realized_loss_tax_value": sum(_as_float((dashboard.get("portfolio_summary") or {}).get("estimated_realized_loss_tax_value")) for dashboard in dashboards),
        "estimated_unrealized_loss_tax_value": sum(_as_float((dashboard.get("portfolio_summary") or {}).get("estimated_unrealized_loss_tax_value")) for dashboard in dashboards),
        "tax_model": "estimated_gain_reserve",
        "total_return_pct": total_return_pct,
        "exposure_pct": (total_market_value / total_equity * 100.0) if total_equity > 0 else 0.0,
        "daily_pnl": sum(_as_float((dashboard.get("portfolio_summary") or {}).get("daily_pnl")) for dashboard in dashboards),
        "realized_pnl": sum(_as_float((dashboard.get("portfolio_summary") or {}).get("realized_pnl")) for dashboard in dashboards),
        "unrealized_pnl": sum(_as_float((dashboard.get("portfolio_summary") or {}).get("unrealized_pnl")) for dashboard in dashboards),
        "positions_open": sum(int((dashboard.get("portfolio_summary") or {}).get("positions_open") or 0) for dashboard in dashboards),
        "positions_closed": sum(int((dashboard.get("portfolio_summary") or {}).get("positions_closed") or 0) for dashboard in dashboards),
        "positions": [position for dashboard in dashboards for position in list((dashboard.get("portfolio_summary") or {}).get("positions") or [])],
    }
    aggregate_target = dict(first.get("target") or {})
    target_return = _as_float(aggregate_target.get("target_return"))
    current_total_return = total_after_tax_return_pct / 100.0
    target_equity = total_starting * (1.0 + target_return) if total_starting > 0 else total_equity
    aggregate_target.update(
        {
            "starting_budget": total_starting,
            "basis": "after_tax",
            "basis_label": "Estimated after-tax equity",
            "current_equity": total_after_tax_equity,
            "pretax_equity": total_equity,
            "after_tax_equity": total_after_tax_equity,
            "estimated_tax_drag": total_tax_drag,
            "current_total_return": current_total_return,
            "current_total_return_pct": total_after_tax_return_pct,
            "target_equity": target_equity,
            "gap_to_target": total_after_tax_equity - target_equity,
            "gap_to_target_return": current_total_return - target_return,
            "gap_to_target_return_pct": (current_total_return - target_return) * 100.0,
        }
    )
    first["target"] = aggregate_target
    first["agents"] = agent_rows
    first["agent_portfolios"] = agent_portfolios
    first["agent_model_settings"] = model_settings
    first["llm_attribution"] = _combine_llm_attribution(dashboards)
    first["llm_outcome_attribution"] = get_llm_attribution_summary(days=30)
    first["competition"] = _competition_payload(agent_rows, portfolio_ids)
    first["plan_counts"] = plan_counts
    first["pending_action_count"] = int(sum(plan_counts.get(status, 0) for status in ("Pending", "Approved", "Submitted", "Partially Filled")))
    first["open_plan_readiness"] = _combine_open_plan_readiness(dashboards)
    first["recent_activity"] = recent_activity
    first["agent_portfolio_count"] = len(agent_portfolios)
    first["per_agent_budget"] = total_starting / len(agent_portfolios) if agent_portfolios else 0.0
    return first
