from __future__ import annotations

from typing import Any, Iterable

from .paper_trading import _batch_current_prices
from .persistence import get_latest_signal_snapshot
from .signal_quality import evaluate_signal_quality


def _quality_payload(quality: Any) -> dict[str, Any]:
    source_age_hours = None
    if quality.source_age_minutes is not None:
        source_age_hours = round(float(quality.source_age_minutes) / 60.0, 1)
    stale = source_age_hours is not None and source_age_hours >= 72.0
    reason = quality.summary()
    return {
        "action": quality.action,
        "actionable": bool(quality.actionable),
        "grade": quality.grade,
        "score": quality.score,
        "label": "Ready" if quality.actionable else "Stale" if stale else "Blocked" if quality.blockers else "Watch",
        "reason": reason,
        "blockers": list(quality.blockers),
        "warnings": list(quality.warnings),
        "reasons": list(quality.reasons),
        "source_age_hours": source_age_hours,
        "is_stale": stale,
        "current_price": quality.current_price,
        "reference_price": quality.reference_price,
        "price_distance_pct": quality.price_distance_pct,
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


def annotate_watchlist_signal_health(rows: Iterable[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Attach read-only freshness/actionability diagnostics to discovery rows."""

    items = [dict(row) for row in rows if isinstance(row, dict)]
    signal_items = [
        item
        for item in items
        if str(item.get("status") or "") in {"Entry Signal", "Added"}
        and str(item.get("ticker") or "").strip()
    ]
    signal_ids = {id(item) for item in signal_items}
    prices = _batch_current_prices([str(item.get("ticker") or "") for item in signal_items]) if signal_items else {}
    counts: dict[str, int] = {"ready": 0, "watch": 0, "blocked": 0, "stale": 0, "unscored": 0}

    for item in items:
        if id(item) not in signal_ids:
            item["signal_health"] = None
            continue
        ticker = str(item.get("ticker") or "").upper()
        entry_price = float(item.get("suggested_entry_price") or 0.0)
        snapshot = get_latest_signal_snapshot(ticker, max_age_days=7) or {}
        current_price = float(prices.get(ticker) or 0.0) or float(snapshot.get("current_price") or 0.0) or entry_price
        if entry_price <= 0 or current_price <= 0:
            counts["unscored"] += 1
            item["signal_health"] = {
                "action": "Buy",
                "actionable": False,
                "grade": "blocked",
                "score": 0.0,
                "label": "Unscored",
                "reason": "Entry price or current price is unavailable.",
                "blockers": ["Entry price or current price is unavailable."],
                "warnings": [],
                "reasons": [],
                "source_age_hours": None,
                "is_stale": False,
                "current_price": current_price or None,
                "reference_price": entry_price or None,
                "price_distance_pct": None,
            }
            continue
        quality = evaluate_signal_quality(
            _signal_row(item, snapshot, entry_price),
            action="Buy",
            source="discovery",
            current_price=current_price,
            reference_price=entry_price,
        )
        payload = _quality_payload(quality)
        item["signal_health"] = payload
        if payload["actionable"]:
            counts["ready"] += 1
        elif payload["grade"] == "watch":
            counts["watch"] += 1
        else:
            counts["blocked"] += 1
        if payload["is_stale"]:
            counts["stale"] += 1

    counts["scored"] = sum(counts[key] for key in ("ready", "watch", "blocked"))
    counts["total_signal_rows"] = len(signal_items)
    return items, counts
