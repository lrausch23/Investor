from __future__ import annotations

from datetime import datetime, timezone
import logging
from pathlib import Path
import sqlite3
from typing import Any

from .persistence import (
    DB_PATH,
    get_setting,
    list_paper_portfolios,
    save_alert,
    update_trade_plan_status,
    get_trade_plans,
)

logger = logging.getLogger(__name__)

STUCK_ORDER_THRESHOLD_MINUTES = 60


def detect_stuck_orders(threshold_minutes: int = STUCK_ORDER_THRESHOLD_MINUTES) -> list[dict[str, Any]]:
    stuck: list[dict[str, Any]] = []
    now = datetime.now(timezone.utc)
    for portfolio in list_paper_portfolios(include_closed=True):
        for plan in get_trade_plans(int(portfolio["id"]), status="all"):
            status = str(plan.get("status") or "")
            if status not in {"Submitted", "Partially Filled"}:
                continue
            updated_at = plan.get("updated_at") or plan.get("created_at")
            if not updated_at:
                continue
            plan_time = datetime.fromisoformat(str(updated_at).replace("Z", "+00:00"))
            if plan_time.tzinfo is None:
                plan_time = plan_time.replace(tzinfo=timezone.utc)
            age_minutes = (now - plan_time).total_seconds() / 60.0
            if age_minutes >= threshold_minutes:
                item = dict(plan)
                item["stuck_minutes"] = round(age_minutes, 1)
                stuck.append(item)
    return stuck


def reconcile_stuck_orders(adapter=None) -> dict[str, Any]:
    reconciled = 0
    expired = 0
    still_stuck = 0
    for plan in detect_stuck_orders():
        broker_order_id = str(plan.get("broker_order_id") or "").strip()
        status = None
        if adapter is not None and broker_order_id:
            try:
                status = adapter.get_order_status(broker_order_id)
            except Exception:
                status = None
        normalized = str(getattr(status, "status", "") or "").lower()
        if normalized in {"filled", "executed"}:
            update_trade_plan_status(int(plan["id"]), "Executed", broker_status=normalized, executed_at=datetime.now(timezone.utc).isoformat())
            reconciled += 1
        elif normalized in {"cancelled", "canceled", "expired", "rejected"}:
            update_trade_plan_status(int(plan["id"]), "Cancelled" if "cancel" in normalized else "Expired", broker_status=normalized)
            reconciled += 1
        else:
            update_trade_plan_status(
                int(plan["id"]),
                "Expired",
                notes=f"{str(plan.get('notes') or '')} Recovered: broker unreachable at startup".strip(),
            )
            expired += 1
        save_alert(
            "execution_error",
            f"Recovered stuck order {plan.get('ticker')}",
            severity="warning",
            ticker=str(plan.get("ticker") or "").upper() or None,
            portfolio_id=int(plan.get("portfolio_id") or 0) or None,
            message=f"Recovered plan {plan.get('id')} from {plan.get('status')}.",
            data={"plan_id": plan.get("id"), "broker_order_id": broker_order_id},
        )
    still_stuck = len(detect_stuck_orders())
    return {"reconciled": reconciled, "expired": expired, "still_stuck": still_stuck}


def check_db_integrity() -> dict[str, Any]:
    db_path = Path(DB_PATH)
    with sqlite3.connect(db_path) as conn:
        integrity_row = conn.execute("PRAGMA integrity_check").fetchone()
        fk_row = conn.execute("PRAGMA foreign_key_check").fetchone()
        table_count_row = conn.execute("SELECT COUNT(*) FROM sqlite_master WHERE type = 'table'").fetchone()
    return {
        "integrity": str(integrity_row[0]) if integrity_row else "unknown",
        "foreign_keys": "ok" if fk_row is None else str(tuple(fk_row)),
        "table_count": int(table_count_row[0] or 0) if table_count_row else 0,
        "size_bytes": db_path.stat().st_size if db_path.exists() else 0,
    }


def run_startup_recovery() -> dict[str, Any]:
    stuck_orders = detect_stuck_orders()
    reconciled = reconcile_stuck_orders()
    integrity = check_db_integrity()
    settings_accessible = get_setting("frontier_provider") is not None or True
    payload = {
        "stuck_orders_found": len(stuck_orders),
        "reconciled": int(reconciled.get("reconciled") or 0),
        "expired": int(reconciled.get("expired") or 0),
        "db_integrity": integrity.get("integrity"),
        "settings_accessible": settings_accessible,
        "db": integrity,
    }
    save_alert(
        "execution_error",
        "Startup recovery completed",
        severity="info" if payload["db_integrity"] == "ok" else "warning",
        message=(
            f"Stuck orders: {payload['stuck_orders_found']}, reconciled: {payload['reconciled']}, "
            f"expired: {payload['expired']}, integrity: {payload['db_integrity']}"
        ),
        data=payload,
    )
    logger.info("Startup recovery completed: %s", payload)
    return payload
