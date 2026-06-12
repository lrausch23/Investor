from __future__ import annotations

from typing import Any

from .agent_policy import agent_candidate_policy
from .anti_churn import check_anti_churn, get_anti_churn_settings
from .hurdle_rate import check_duration_gate, check_hurdle_rate, get_hurdle_settings
from .order_routing import decide_routing
from .paper_trading import (
    _batch_current_prices,
    _lookup_atr,
    _lookup_beta,
    _open_position_index,
    _pending_plan_index,
    _risk_adjusted_quantity,
    allocate_budget,
    get_sizing_settings,
)
from .persistence import get_latest_signal_snapshot, get_trade_plans, get_watchlist
from .signal_quality import evaluate_signal_quality
from .slippage import estimate_execution_cost


def _latest_plan_by_ticker(portfolio_id: int) -> dict[str, dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}
    for plan in get_trade_plans(portfolio_id, status="all"):
        ticker = str(plan.get("ticker") or "").upper()
        if not ticker or str(plan.get("action") or "") != "Buy":
            continue
        current = latest.get(ticker)
        current_time = str(current.get("created_at") or "") if current else ""
        plan_time = str(plan.get("created_at") or "")
        if current is None or plan_time >= current_time:
            latest[ticker] = plan
    return latest


def _plan_summary(plan: dict[str, Any] | None) -> dict[str, Any] | None:
    if not plan:
        return None
    return {
        "id": plan.get("id"),
        "status": plan.get("status"),
        "source": plan.get("source"),
        "created_at": plan.get("created_at"),
        "updated_at": plan.get("updated_at"),
        "quantity": plan.get("quantity"),
        "proposed_price": plan.get("proposed_price"),
        "broker_status": plan.get("broker_status"),
    }


def _signal_row(candidate: dict[str, Any], snapshot: dict[str, Any], entry_price: float) -> dict[str, Any]:
    row = {**snapshot, **candidate}
    if "price_targets" not in row:
        row["price_targets"] = {
            "entry_price": entry_price,
            "exit_price": candidate.get("suggested_exit_price") or snapshot.get("exit_price"),
            "stop_price": candidate.get("suggested_stop_price") or snapshot.get("stop_price"),
            "risk_reward_ratio": snapshot.get("risk_reward_ratio"),
        }
    return row


def _quality_payload(quality: Any) -> dict[str, Any]:
    return {
        "actionable": bool(quality.actionable),
        "score": quality.score,
        "grade": quality.grade,
        "blockers": list(quality.blockers),
        "warnings": list(quality.warnings),
        "reasons": list(quality.reasons),
        "source_age_hours": None if quality.source_age_minutes is None else round(quality.source_age_minutes / 60.0, 1),
        "price_distance_pct": quality.price_distance_pct,
    }


def _optional_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def compute_agent_candidate_intake(portfolio_id: int, *, limit: int = 20) -> dict[str, Any]:
    """Explain whether discovery candidates are eligible for agent buy plans.

    This is intentionally read-only. It mirrors the buy-plan gates closely enough
    to make the agent intake visible without creating or mutating trade plans.
    """

    candidates = [
        item
        for item in get_watchlist(status=["Entry Signal", "Added"])
        if isinstance(item, dict) and str(item.get("ticker") or "").strip()
    ]
    prices = _batch_current_prices([str(item.get("ticker") or "") for item in candidates])
    allocation = allocate_budget(portfolio_id)
    theme_budgets = {int(item["theme_id"]): item for item in allocation.get("themes", [])}
    pending_buys = _pending_plan_index(portfolio_id, "Buy")
    open_positions = _open_position_index(portfolio_id)
    latest_plans = _latest_plan_by_ticker(portfolio_id)
    sizing_settings = get_sizing_settings()
    hurdle_settings = get_hurdle_settings()
    anti_churn_settings = get_anti_churn_settings()

    rows: list[dict[str, Any]] = []
    for item in candidates:
        ticker = str(item.get("ticker") or "").upper()
        theme_id = int(item.get("theme_id") or 0)
        role = str(item.get("suggested_role") or "Critical-Path")
        entry_price = float(item.get("suggested_entry_price") or 0.0)
        latest_plan = latest_plans.get(ticker)
        row: dict[str, Any] = {
            "ticker": ticker,
            "company_name": item.get("company_name"),
            "status": item.get("status"),
            "theme_id": theme_id,
            "theme_name": item.get("theme_name"),
            "role": role,
            "entry_signal_at": item.get("entry_signal_at"),
            "last_scanned_at": item.get("last_scanned_at"),
            "entry_price": entry_price or None,
            "regime_label": item.get("regime_label"),
            "regime_probability": item.get("regime_probability"),
            "latest_plan": _plan_summary(latest_plan),
        }

        if ticker in pending_buys:
            row["decision"] = "pending_buy_exists"
            row["reason"] = "A pending buy plan already exists for this ticker."
            rows.append(row)
            continue
        if ticker in open_positions:
            row["decision"] = "already_held"
            row["reason"] = "The agent portfolio already has an open position."
            rows.append(row)
            continue

        mandate = agent_candidate_policy(portfolio_id, ticker, source="discovery", candidate=item)
        row["mandate"] = mandate
        if not mandate.get("allowed", True):
            row["decision"] = "blocked_agent_mandate"
            row["reason"] = str(mandate.get("reason") or "Agent mandate blocked this ticker.")
            rows.append(row)
            continue

        theme_budget = theme_budgets.get(theme_id)
        row["theme_budget_exists"] = bool(theme_budget)
        if not theme_budget:
            row["decision"] = "blocked_no_theme_budget"
            row["reason"] = "No allocatable theme budget is available."
            rows.append(row)
            continue
        role_budget = float((theme_budget.get("by_role") or {}).get(role) or 0.0)
        row["role_budget"] = role_budget

        snapshot = get_latest_signal_snapshot(ticker, max_age_days=7) or {}
        current_price = float(prices.get(ticker) or 0.0) or float(snapshot.get("current_price") or 0.0) or entry_price
        row["current_price"] = current_price or None
        row["signal_snapshot_found"] = bool(snapshot)
        row["signal_snapshot_updated_at"] = snapshot.get("updated_at")
        if role_budget <= 0 or entry_price <= 0 or current_price <= 0:
            row["decision"] = "blocked_budget_or_price"
            row["reason"] = "Role budget, entry price, or executable price is unavailable."
            rows.append(row)
            continue

        signal_row = _signal_row(item, snapshot, entry_price)
        quality = evaluate_signal_quality(
            signal_row,
            action="Buy",
            source="discovery",
            current_price=current_price,
            reference_price=entry_price,
        )
        row["signal_quality"] = _quality_payload(quality)
        if not quality.actionable:
            row["decision"] = "blocked_signal_quality"
            row["reason"] = quality.summary()
            rows.append(row)
            continue

        if bool(anti_churn_settings.get("anti_churn_enabled", True)):
            anti_churn = check_anti_churn(portfolio_id, ticker)
            row["anti_churn"] = {
                "passed": anti_churn.passed,
                "reason": anti_churn.reason,
                "round_trip_count": anti_churn.round_trip_count,
                "max_round_trips": anti_churn.max_round_trips,
            }
            if not anti_churn.passed:
                row["decision"] = "blocked_anti_churn"
                row["reason"] = anti_churn.reason
                rows.append(row)
                continue

        atr_14 = _lookup_atr(ticker)
        beta = _lookup_beta(ticker)
        row["atr_14"] = atr_14
        row["beta"] = beta
        if str(sizing_settings.get("sizing_method") or "") == "risk_budget":
            quantity = _risk_adjusted_quantity(role_budget, current_price, atr_14, beta)
        else:
            quantity = int(role_budget // current_price)
        row["quantity"] = quantity
        if quantity <= 0:
            row["decision"] = "blocked_zero_quantity"
            row["reason"] = "Risk sizing produced zero shares."
            rows.append(row)
            continue

        routing = decide_routing(ticker=ticker, action="Buy", quantity=quantity, last_price=current_price, urgency="patient")
        exec_cost = estimate_execution_cost(
            ticker=ticker,
            routing_strategy=routing.strategy_name,
            algo_strategy=routing.algo_strategy,
            portfolio_id=portfolio_id,
        )
        exit_price = item.get("suggested_exit_price") or snapshot.get("exit_price")
        if bool(hurdle_settings.get("hurdle_enabled", True)):
            hurdle = check_hurdle_rate(
                ticker,
                current_price,
                _optional_float(exit_price),
                estimated_execution_cost_pct=exec_cost,
            )
            row["hurdle"] = {
                "passed": hurdle.passed,
                "reason": hurdle.reason,
                "gross_return_pct": hurdle.gross_return_pct,
                "net_return_pct": hurdle.net_return_pct,
            }
            if not hurdle.passed:
                row["decision"] = "blocked_hurdle"
                row["reason"] = hurdle.reason
                rows.append(row)
                continue

        if bool(hurdle_settings.get("duration_gate_enabled", True)):
            expected_duration = snapshot.get("expected_regime_duration") or snapshot.get("timeframe_days")
            duration = check_duration_gate(
                ticker,
                _optional_float(expected_duration),
                str(item.get("regime_label") or snapshot.get("regime_label") or ""),
            )
            row["duration"] = {
                "passed": duration.passed,
                "reason": duration.reason,
                "expected_regime_duration": duration.expected_regime_duration,
            }
            if not duration.passed:
                row["decision"] = "blocked_duration"
                row["reason"] = duration.reason
                rows.append(row)
                continue

        row["decision"] = "would_create_buy_plan"
        row["reason"] = "Candidate passes current buy-plan intake gates."
        rows.append(row)

    priority = {
        "would_create_buy_plan": 0,
        "pending_buy_exists": 1,
        "already_held": 2,
        "blocked_signal_quality": 3,
    }
    rows.sort(key=lambda item: (priority.get(str(item.get("decision") or ""), 9), str(item.get("ticker") or "")))
    limited = rows[: max(0, int(limit))]
    counts: dict[str, int] = {}
    for row in rows:
        key = str(row.get("decision") or "unknown")
        counts[key] = counts.get(key, 0) + 1
    return {
        "portfolio_id": portfolio_id,
        "total_candidates": len(rows),
        "returned_candidates": len(limited),
        "counts": counts,
        "settings": {
            "sizing_method": sizing_settings.get("sizing_method"),
            "hurdle_enabled": hurdle_settings.get("hurdle_enabled"),
            "duration_gate_enabled": hurdle_settings.get("duration_gate_enabled"),
            "anti_churn_enabled": anti_churn_settings.get("anti_churn_enabled"),
        },
        "candidates": limited,
    }
