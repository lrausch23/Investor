from __future__ import annotations

import re
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

from .paper_trading import _batch_current_prices
from .persistence import (
    get_latest_regime_label,
    get_paper_portfolio,
    get_paper_positions,
    get_trade_plans,
    get_training_history,
    list_themes,
)

_ML_CONFIDENCE_RE = re.compile(r"ML confidence:\s*(\d+)%", re.IGNORECASE)
_ML_BANDS: list[tuple[str, float, float]] = [
    ("0-30%", 0.0, 0.30),
    ("30-50%", 0.30, 0.50),
    ("50-70%", 0.50, 0.70),
    ("70-90%", 0.70, 0.90),
    ("90-100%", 0.90, 1.01),
]


def _theme_name_map() -> dict[int, str]:
    return {
        int(theme["id"]): str(theme.get("name") or f"Theme {theme['id']}")
        for theme in list_themes(include_closed=True)
        if theme.get("id") is not None
    }


def _open_price_map(rows: list[dict[str, Any]]) -> dict[str, float]:
    prices = _batch_current_prices([str(row.get("ticker") or "") for row in rows])
    return {str(ticker): float(price) for ticker, price in prices.items()}


def _holding_days(row: dict[str, Any]) -> int | None:
    entry_raw = row.get("entry_date")
    exit_raw = row.get("exit_date")
    if not entry_raw or not exit_raw:
        return None
    try:
        entry = datetime.fromisoformat(str(entry_raw))
        exit_dt = datetime.fromisoformat(str(exit_raw))
    except ValueError:
        return None
    return max(0, (exit_dt - entry).days)


def _match_plan_to_position(plan: dict[str, Any], positions: list[dict[str, Any]]) -> dict[str, Any] | None:
    ticker = str(plan.get("ticker") or "").upper()
    executed_at = str(plan.get("executed_at") or "")
    if not ticker:
        return None
    candidates = [row for row in positions if str(row.get("ticker") or "").upper() == ticker]
    if not candidates:
        return None
    if executed_at:
        plan_day = executed_at[:10]
        same_day = [row for row in candidates if str(row.get("entry_date") or "")[:10] == plan_day]
        if same_day:
            return same_day[0]
    if executed_at:
        try:
            plan_dt = datetime.fromisoformat(executed_at)
        except ValueError:
            plan_dt = None
        if plan_dt is not None:
            ranked = sorted(
                candidates,
                key=lambda row: abs(
                    (
                        datetime.fromisoformat(str(row.get("entry_date") or executed_at)) - plan_dt
                    ).total_seconds()
                ),
            )
            return ranked[0] if ranked else None
    return candidates[0]


def compute_theme_attribution(portfolio_id: int) -> dict[str, Any]:
    portfolio = get_paper_portfolio(portfolio_id)
    if portfolio is None:
        return {"themes": [], "total_realized_pnl": 0.0, "total_unrealized_pnl": 0.0, "total_pnl": 0.0, "theme_count": 0}
    open_positions = get_paper_positions(portfolio_id, status="Open")
    closed_positions = get_paper_positions(portfolio_id, status="Closed")
    prices = _open_price_map(open_positions)
    theme_names = _theme_name_map()
    buckets: dict[int | None, dict[str, Any]] = {}

    def bucket(theme_id: int | None) -> dict[str, Any]:
        if theme_id not in buckets:
            buckets[theme_id] = {
                "theme_id": theme_id,
                "theme_name": theme_names.get(theme_id, "Unassigned") if theme_id is not None else "Unassigned",
                "total_realized_pnl": 0.0,
                "total_unrealized_pnl": 0.0,
                "position_count": 0,
                "closed_count": 0,
                "win_count": 0,
                "loss_count": 0,
                "holding_days": [],
                "best_trade": None,
                "worst_trade": None,
            }
        return buckets[theme_id]

    for row in open_positions:
        theme_id = int(row["theme_id"]) if row.get("theme_id") is not None else None
        item = bucket(theme_id)
        quantity = float(row.get("quantity") or 0.0)
        entry_price = float(row.get("entry_price") or 0.0)
        current_price = float(prices.get(str(row.get("ticker") or "").upper(), entry_price))
        unrealized = (current_price - entry_price) * quantity
        item["total_unrealized_pnl"] += unrealized
        item["position_count"] += 1

    for row in closed_positions:
        theme_id = int(row["theme_id"]) if row.get("theme_id") is not None else None
        item = bucket(theme_id)
        pnl = float(row.get("realized_pnl") or 0.0)
        item["total_realized_pnl"] += pnl
        item["position_count"] += 1
        item["closed_count"] += 1
        if pnl > 0:
            item["win_count"] += 1
        else:
            item["loss_count"] += 1
        days = _holding_days(row)
        if days is not None:
            item["holding_days"].append(days)
        trade = {"ticker": str(row.get("ticker") or "").upper(), "pnl": pnl}
        if item["best_trade"] is None or pnl > float(item["best_trade"]["pnl"]):
            item["best_trade"] = trade
        if item["worst_trade"] is None or pnl < float(item["worst_trade"]["pnl"]):
            item["worst_trade"] = trade

    themes: list[dict[str, Any]] = []
    total_realized = 0.0
    total_unrealized = 0.0
    for item in buckets.values():
        total_realized += float(item["total_realized_pnl"])
        total_unrealized += float(item["total_unrealized_pnl"])
        closed_count = int(item["closed_count"])
        item["win_rate"] = (float(item["win_count"]) / closed_count) if closed_count else None
        holding_days = item.pop("holding_days")
        item["avg_holding_days"] = (sum(holding_days) / len(holding_days)) if holding_days else None
        item["total_pnl"] = float(item["total_realized_pnl"]) + float(item["total_unrealized_pnl"])
        themes.append(item)
    themes.sort(key=lambda row: float(row.get("total_pnl") or 0.0), reverse=True)
    return {
        "themes": themes,
        "total_realized_pnl": total_realized,
        "total_unrealized_pnl": total_unrealized,
        "total_pnl": total_realized + total_unrealized,
        "theme_count": len(themes),
    }


def compute_source_attribution(portfolio_id: int) -> dict[str, Any]:
    plans = [
        row for row in get_trade_plans(portfolio_id, status="all")
        if str(row.get("status") or "") == "Executed"
    ]
    open_positions = get_paper_positions(portfolio_id, status="Open")
    closed_positions = get_paper_positions(portfolio_id, status="Closed")
    all_positions = open_positions + closed_positions
    prices = _open_price_map(open_positions)
    matched_position_ids: set[int] = set()
    grouped: dict[str, dict[str, Any]] = {}

    def bucket(source: str) -> dict[str, Any]:
        if source not in grouped:
            grouped[source] = {
                "source": source,
                "plan_count": 0,
                "total_realized_pnl": 0.0,
                "total_unrealized_pnl": 0.0,
                "win_count": 0,
                "loss_count": 0,
                "slippages": [],
            }
        return grouped[source]

    for plan in plans:
        source = str(plan.get("source") or "manual")
        item = bucket(source)
        item["plan_count"] += 1
        proposed = float(plan.get("proposed_price") or 0.0)
        execution = float(plan.get("execution_price") or 0.0)
        if proposed > 0 and execution > 0:
            item["slippages"].append(((execution - proposed) / proposed) * 100.0)
        position = _match_plan_to_position(plan, all_positions)
        if position is None:
            continue
        if position.get("id") is not None:
            matched_position_ids.add(int(position["id"]))
        if str(position.get("status") or "") == "Closed":
            pnl = float(position.get("realized_pnl") or 0.0)
            item["total_realized_pnl"] += pnl
            if pnl > 0:
                item["win_count"] += 1
            else:
                item["loss_count"] += 1
        else:
            quantity = float(position.get("quantity") or 0.0)
            entry_price = float(position.get("entry_price") or 0.0)
            current_price = float(prices.get(str(position.get("ticker") or "").upper(), entry_price))
            item["total_unrealized_pnl"] += (current_price - entry_price) * quantity

    sources: list[dict[str, Any]] = []
    for item in grouped.values():
        plan_count = int(item["plan_count"])
        closed_count = int(item["win_count"]) + int(item["loss_count"])
        item["win_rate"] = (float(item["win_count"]) / closed_count) if closed_count else None
        item["avg_slippage_pct"] = (sum(item["slippages"]) / len(item["slippages"])) if item["slippages"] else None
        item.pop("slippages", None)
        sources.append(item)
    sources.sort(key=lambda row: float(row.get("total_realized_pnl", 0.0)) + float(row.get("total_unrealized_pnl", 0.0)), reverse=True)
    unmatched_positions = len([row for row in all_positions if row.get("id") is not None and int(row["id"]) not in matched_position_ids])
    return {
        "sources": sources,
        "total_plans_executed": len(plans),
        "unmatched_positions": unmatched_positions,
    }


def compute_regime_attribution(portfolio_id: int) -> dict[str, Any]:
    open_positions = get_paper_positions(portfolio_id, status="Open")
    closed_positions = get_paper_positions(portfolio_id, status="Closed")
    all_positions = open_positions + closed_positions
    prices = _open_price_map(open_positions)
    plans = [row for row in get_trade_plans(portfolio_id, status="all") if str(row.get("action") or "") == "Buy"]
    grouped: dict[str, dict[str, Any]] = {}

    def bucket(regime: str) -> dict[str, Any]:
        key = regime or "Unknown"
        if key not in grouped:
            grouped[key] = {
                "regime": key,
                "position_count": 0,
                "closed_count": 0,
                "total_realized_pnl": 0.0,
                "total_unrealized_pnl": 0.0,
                "win_count": 0,
                "loss_count": 0,
                "returns": [],
            }
        return grouped[key]

    for row in all_positions:
        matched = _match_plan_to_position({"ticker": row.get("ticker"), "executed_at": row.get("entry_date")}, plans)
        regime = None
        if matched and matched.get("regime_label"):
            regime = str(matched.get("regime_label"))
        if not regime:
            regime = get_latest_regime_label(str(row.get("ticker") or ""), str(row.get("entry_date") or "")) or "Unknown"
        item = bucket(regime)
        item["position_count"] += 1
        quantity = float(row.get("quantity") or 0.0)
        entry_price = float(row.get("entry_price") or 0.0)
        if str(row.get("status") or "") == "Closed":
            pnl = float(row.get("realized_pnl") or 0.0)
            item["closed_count"] += 1
            item["total_realized_pnl"] += pnl
            item["returns"].append((pnl / (entry_price * quantity)) * 100.0 if entry_price > 0 and quantity > 0 else 0.0)
            if pnl > 0:
                item["win_count"] += 1
            else:
                item["loss_count"] += 1
        else:
            current_price = float(prices.get(str(row.get("ticker") or "").upper(), entry_price))
            unrealized = (current_price - entry_price) * quantity
            item["total_unrealized_pnl"] += unrealized
            item["returns"].append(((current_price - entry_price) / entry_price) * 100.0 if entry_price > 0 else 0.0)

    regimes: list[dict[str, Any]] = []
    order = {"Bull": 0, "Neutral": 1, "Bear": 2, "Unknown": 3}
    for item in grouped.values():
        item["total_pnl"] = float(item["total_realized_pnl"]) + float(item["total_unrealized_pnl"])
        closed_count = int(item["closed_count"])
        item["win_rate"] = (float(item["win_count"]) / closed_count) if closed_count else None
        returns = item.pop("returns")
        item["avg_return_pct"] = (sum(returns) / len(returns)) if returns else None
        regimes.append(item)
    regimes.sort(key=lambda row: order.get(str(row.get("regime")), 99))
    return {"regimes": regimes}


def _parse_ml_confidence(text: Any) -> float | None:
    match = _ML_CONFIDENCE_RE.search(str(text or ""))
    if not match:
        return None
    try:
        return max(0.0, min(1.0, int(match.group(1)) / 100.0))
    except Exception:
        return None


def _band_for(probability: float) -> str:
    for label, lower, upper in _ML_BANDS:
        if lower <= probability < upper:
            return label
    return "90-100%"


def compute_ml_accuracy(portfolio_id: int) -> dict[str, Any]:
    closed_positions = get_paper_positions(portfolio_id, status="Closed")
    plans = [row for row in get_trade_plans(portfolio_id, status="all") if str(row.get("action") or "") == "Buy"]
    calibration_buckets: dict[str, list[tuple[float, float]]] = defaultdict(list)
    total_with_ml = 0
    total_without_ml = 0
    correct = 0
    total_classified = 0

    for position in closed_positions:
        plan = _match_plan_to_position({"ticker": position.get("ticker"), "executed_at": position.get("entry_date")}, plans)
        confidence = _parse_ml_confidence(plan.get("rationale") if plan else "")
        if confidence is None:
            total_without_ml += 1
            continue
        total_with_ml += 1
        actual = 1.0 if float(position.get("realized_pnl") or 0.0) > 0 else 0.0
        calibration_buckets[_band_for(confidence)].append((confidence, actual))
        predicted_success = confidence >= 0.5
        if predicted_success == (actual > 0):
            correct += 1
        total_classified += 1

    calibration: list[dict[str, Any]] = []
    for label, _lower, _upper in _ML_BANDS:
        rows = calibration_buckets.get(label, [])
        if not rows:
            continue
        predicted_avg = sum(prob for prob, _actual in rows) / len(rows)
        actual_success = sum(actual for _prob, actual in rows) / len(rows)
        calibration.append(
            {
                "band": label,
                "predicted_avg": predicted_avg,
                "actual_success_rate": actual_success,
                "count": len(rows),
                "calibration_gap": actual_success - predicted_avg,
            }
        )

    history_rows = get_training_history(limit=20)
    model_history = [
        {
            "version": int(row.get("version") or 0),
            "accuracy": row.get("accuracy"),
            "f1": row.get("f1"),
            "trained_at": row.get("trained_at"),
            "ticker": row.get("ticker"),
            "status": row.get("status"),
        }
        for row in history_rows
    ]
    return {
        "calibration": calibration,
        "total_trades_with_ml": total_with_ml,
        "total_trades_without_ml": total_without_ml,
        "overall_accuracy": (correct / total_classified) if total_classified else None,
        "model_history": model_history,
    }


def compute_attribution_summary(portfolio_id: int, performance: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "portfolio": get_paper_portfolio(portfolio_id),
        "performance": performance or {},
        "theme_attribution": compute_theme_attribution(portfolio_id),
        "source_attribution": compute_source_attribution(portfolio_id),
        "regime_attribution": compute_regime_attribution(portfolio_id),
        "ml_accuracy": compute_ml_accuracy(portfolio_id),
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
