# mypy: ignore-errors
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from dataclasses import asdict, is_dataclass

from ..exceptions import DataValidationError, DuplicateThemeError, PersistenceError
from . import core

logger = core.logger
DEFAULT_OPERATING_MODE = core.DEFAULT_OPERATING_MODE
DEFAULT_AUTO_APPROVE_THRESHOLD = core.DEFAULT_AUTO_APPROVE_THRESHOLD
DEFAULT_DAILY_CAPITAL_CEILING_PCT = core.DEFAULT_DAILY_CAPITAL_CEILING_PCT
LOT_SELECTION_METHODS = core.LOT_SELECTION_METHODS
DEFAULT_LOT_SELECTION_METHOD = core.DEFAULT_LOT_SELECTION_METHOD
DEFAULT_LTCG_DEFER_WINDOW_DAYS = core.DEFAULT_LTCG_DEFER_WINDOW_DAYS
OPERATING_MODES = core.OPERATING_MODES
ALERT_TYPES = core.ALERT_TYPES
NOTIFICATION_CHANNELS = core.NOTIFICATION_CHANNELS
_NOTIFICATION_DEFAULT_MATRIX = core._NOTIFICATION_DEFAULT_MATRIX
_SECTOR_CACHE_TTL_DAYS = core._SECTOR_CACHE_TTL_DAYS
_EARNINGS_CACHE_TTL_HOURS = core._EARNINGS_CACHE_TTL_HOURS


def _connect():
    return core._connect()

from .schema import _create_notification_preferences_table

def save_stress_test_result(
    scenario_id: str,
    config_json: str,
    result_json: str,
    status: str = "completed",
) -> int:
    normalized_status = str(status or "completed").strip().lower()
    if normalized_status not in {"running", "completed", "failed"}:
        raise DataValidationError("Invalid stress test status.")
    with _connect() as conn:
        cursor = conn.execute(
            """
            INSERT INTO stress_test_result (scenario_id, config_json, result_json, status)
            VALUES (?, ?, ?, ?)
            """,
            (
                str(scenario_id or ""),
                str(config_json or "{}"),
                str(result_json or "{}"),
                normalized_status,
            ),
        )
        return int(cursor.lastrowid)


def get_stress_test_results(scenario_id: str | None = None, limit: int = 20) -> list[dict[str, Any]]:
    query = "SELECT * FROM stress_test_result"
    params: list[Any] = []
    if scenario_id:
        query += " WHERE scenario_id = ?"
        params.append(str(scenario_id))
    query += " ORDER BY id DESC LIMIT ?"
    params.append(max(1, int(limit)))
    with _connect() as conn:
        rows = conn.execute(query, params).fetchall()
    return [dict(row) for row in rows]


def get_stress_test_result_by_id(result_id: int) -> dict[str, Any] | None:
    with _connect() as conn:
        row = conn.execute("SELECT * FROM stress_test_result WHERE id = ?", (int(result_id),)).fetchone()
    return dict(row) if row else None


def mark_stress_test_status(
    result_id: int,
    status: str,
    *,
    result_json: str | None = None,
    config_json: str | None = None,
) -> None:
    normalized_status = str(status or "").strip().lower()
    if normalized_status not in {"running", "completed", "failed"}:
        raise DataValidationError("Invalid stress test status.")
    updates = ["status = ?"]
    params: list[Any] = [normalized_status]
    if result_json is not None:
        updates.append("result_json = ?")
        params.append(str(result_json))
    if config_json is not None:
        updates.append("config_json = ?")
        params.append(str(config_json))
    params.append(int(result_id))
    with _connect() as conn:
        conn.execute(
            f"UPDATE stress_test_result SET {', '.join(updates)} WHERE id = ?",
            params,
        )


def _seed_notification_preferences(conn: sqlite3.Connection) -> None:
    existing = int(conn.execute("SELECT COUNT(*) FROM notification_preferences").fetchone()[0] or 0)
    if existing:
        return
    rows: list[tuple[str, str, int]] = []
    for alert_type in ALERT_TYPES:
        enabled_channels = set(_NOTIFICATION_DEFAULT_MATRIX.get(alert_type, ("in_app",)))
        for channel in NOTIFICATION_CHANNELS:
            rows.append((alert_type, channel, 1 if channel in enabled_channels else 0))
    conn.executemany(
        """
        INSERT INTO notification_preferences (alert_type, channel, enabled)
        VALUES (?, ?, ?)
        """,
        rows,
    )


def get_notification_preferences() -> list[dict[str, Any]]:
    with _connect() as conn:
        _create_notification_preferences_table(conn)
        _seed_notification_preferences(conn)
        rows = conn.execute(
            """
            SELECT alert_type, channel, enabled
            FROM notification_preferences
            ORDER BY alert_type ASC, channel ASC
            """
        ).fetchall()
    return [
        {
            "alert_type": str(row["alert_type"]),
            "channel": str(row["channel"]),
            "enabled": bool(int(row["enabled"] or 0)),
        }
        for row in rows
    ]


def set_notification_preference(alert_type: str, channel: str, enabled: bool) -> None:
    normalized_type = str(alert_type or "").strip()
    normalized_channel = str(channel or "").strip()
    if normalized_type not in ALERT_TYPES:
        raise ValueError(f"Unknown alert_type: {normalized_type}")
    if normalized_channel not in NOTIFICATION_CHANNELS:
        raise ValueError(f"Unknown channel: {normalized_channel}")
    with _connect() as conn:
        _create_notification_preferences_table(conn)
        _seed_notification_preferences(conn)
        conn.execute(
            """
            INSERT INTO notification_preferences (alert_type, channel, enabled)
            VALUES (?, ?, ?)
            ON CONFLICT(alert_type, channel) DO UPDATE SET
                enabled = excluded.enabled
            """,
            (normalized_type, normalized_channel, 1 if enabled else 0),
        )


def get_channels_for_alert(alert_type: str) -> list[str]:
    normalized_type = str(alert_type or "").strip()
    if normalized_type not in ALERT_TYPES:
        return ["in_app"]
    rows = [
        row
        for row in get_notification_preferences()
        if str(row["alert_type"]) == normalized_type and bool(row["enabled"])
    ]
    channels = [str(row["channel"]) for row in rows]
    if "in_app" not in channels:
        channels.insert(0, "in_app")
    return channels


def save_alert(
    alert_type: str,
    title: str,
    *,
    severity: str = "info",
    ticker: str | None = None,
    portfolio_id: int | None = None,
    message: str = "",
    data: dict[str, Any] | None = None,
) -> dict[str, Any]:
    created_at = datetime.now(timezone.utc).isoformat()
    data_json = json.dumps(data or {}, default=str) if data is not None else None
    with _connect() as conn:
        cursor = conn.execute(
            """
            INSERT INTO alert_log (
                alert_type, severity, ticker, portfolio_id, title, message,
                data_json, acknowledged, acknowledged_at, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, 0, NULL, ?)
            """,
            (
                str(alert_type),
                str(severity or "info"),
                str(ticker or "").upper() or None,
                int(portfolio_id) if portfolio_id is not None else None,
                str(title or ""),
                str(message or ""),
                data_json,
                created_at,
            ),
        )
        alert_id = int(cursor.lastrowid)
    row = get_alerts(limit=1, since=created_at)
    if row and int(row[0].get("id") or 0) == alert_id:
        return row[0]
    return {
        "id": alert_id,
        "alert_type": str(alert_type),
        "severity": str(severity or "info"),
        "ticker": str(ticker or "").upper() or None,
        "portfolio_id": int(portfolio_id) if portfolio_id is not None else None,
        "title": str(title or ""),
        "message": str(message or ""),
        "data": data or {},
        "acknowledged": 0,
        "acknowledged_at": None,
        "created_at": created_at,
    }


def get_alerts(
    *,
    portfolio_id: int | None = None,
    unacknowledged_only: bool = False,
    alert_type: str | None = None,
    limit: int = 50,
    since: str | None = None,
) -> list[dict[str, Any]]:
    clauses: list[str] = ["1 = 1"]
    params: list[Any] = []
    if portfolio_id is not None:
        clauses.append("portfolio_id = ?")
        params.append(int(portfolio_id))
    if unacknowledged_only:
        clauses.append("acknowledged = 0")
    if alert_type:
        clauses.append("alert_type = ?")
        params.append(str(alert_type))
    if since:
        clauses.append("created_at >= ?")
        params.append(str(since))
    params.append(max(1, int(limit)))
    with _connect() as conn:
        rows = conn.execute(
            f"""
            SELECT *
            FROM alert_log
            WHERE {' AND '.join(clauses)}
            ORDER BY created_at DESC, id DESC
            LIMIT ?
            """,
            params,
        ).fetchall()
    payload: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        raw = item.pop("data_json", None)
        if raw:
            try:
                item["data"] = json.loads(str(raw))
            except json.JSONDecodeError:
                item["data"] = {"raw": str(raw)}
        else:
            item["data"] = {}
        payload.append(item)
    return payload


def _decode_json_list(raw: Any) -> list[Any]:
    if raw in (None, ""):
        return []
    try:
        value = json.loads(str(raw))
    except json.JSONDecodeError:
        return []
    return value if isinstance(value, list) else []


def _thesis_monitor_run_from_row(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    item = dict(row)
    item["evidence"] = _decode_json_list(item.pop("evidence_json", "[]"))
    item["tickers_scanned"] = _decode_json_list(item.pop("tickers_scanned_json", "[]"))
    return item


def save_thesis_monitor_run(
    *,
    monitor_key: str,
    primary_ticker: str,
    status: str,
    severity: str,
    risk_score: float,
    thesis: str,
    evidence: list[dict[str, Any]] | None = None,
    tickers_scanned: list[str] | None = None,
    alert_id: int | None = None,
    created_at: str | None = None,
) -> dict[str, Any]:
    normalized_status = str(status or "intact").strip().lower()
    if normalized_status not in {"intact", "watch", "reunderwrite"}:
        raise DataValidationError("Invalid thesis monitor status.")
    normalized_severity = str(severity or "info").strip().lower()
    if normalized_severity not in {"info", "warning", "critical"}:
        raise DataValidationError("Invalid thesis monitor severity.")
    now = created_at or datetime.now(timezone.utc).isoformat()
    with _connect() as conn:
        cursor = conn.execute(
            """
            INSERT INTO thesis_monitor_run (
                monitor_key, primary_ticker, status, severity, risk_score, thesis,
                evidence_json, tickers_scanned_json, alert_id, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(monitor_key or ""),
                str(primary_ticker or "").upper(),
                normalized_status,
                normalized_severity,
                float(risk_score or 0.0),
                str(thesis or ""),
                json.dumps(evidence or [], default=str),
                json.dumps(tickers_scanned or [], default=str),
                int(alert_id) if alert_id is not None else None,
                now,
            ),
        )
        row = conn.execute("SELECT * FROM thesis_monitor_run WHERE id = ?", (int(cursor.lastrowid),)).fetchone()
    saved = _thesis_monitor_run_from_row(row)
    return saved or {
        "id": int(cursor.lastrowid),
        "monitor_key": str(monitor_key or ""),
        "primary_ticker": str(primary_ticker or "").upper(),
        "status": normalized_status,
        "severity": normalized_severity,
        "risk_score": float(risk_score or 0.0),
        "thesis": str(thesis or ""),
        "evidence": evidence or [],
        "tickers_scanned": tickers_scanned or [],
        "alert_id": int(alert_id) if alert_id is not None else None,
        "created_at": now,
    }


def get_latest_thesis_monitor_run(monitor_key: str = "hbm_mu") -> dict[str, Any] | None:
    with _connect() as conn:
        row = conn.execute(
            """
            SELECT *
            FROM thesis_monitor_run
            WHERE monitor_key = ?
            ORDER BY created_at DESC, id DESC
            LIMIT 1
            """,
            (str(monitor_key or ""),),
        ).fetchone()
    return _thesis_monitor_run_from_row(row)


def get_thesis_monitor_runs(monitor_key: str = "hbm_mu", limit: int = 20) -> list[dict[str, Any]]:
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM thesis_monitor_run
            WHERE monitor_key = ?
            ORDER BY created_at DESC, id DESC
            LIMIT ?
            """,
            (str(monitor_key or ""), max(1, int(limit))),
        ).fetchall()
    return [row for row in (_thesis_monitor_run_from_row(row) for row in rows) if row is not None]


def acknowledge_alert(alert_id: int) -> bool:
    acknowledged_at = datetime.now(timezone.utc).isoformat()
    with _connect() as conn:
        cursor = conn.execute(
            """
            UPDATE alert_log
            SET acknowledged = 1,
                acknowledged_at = ?
            WHERE id = ?
            """,
            (acknowledged_at, int(alert_id)),
        )
        return bool(cursor.rowcount)


def acknowledge_all_alerts() -> int:
    acknowledged_at = datetime.now(timezone.utc).isoformat()
    with _connect() as conn:
        cursor = conn.execute(
            """
            UPDATE alert_log
            SET acknowledged = 1,
                acknowledged_at = ?
            WHERE acknowledged = 0
            """,
            (acknowledged_at,),
        )
        return int(cursor.rowcount or 0)


def log_training_run(
    *,
    version: int,
    ticker: str,
    model_path: str,
    metrics: dict[str, Any],
    config: dict[str, Any] | None = None,
    notes: str = "",
) -> dict[str, Any]:
    now = datetime.now(timezone.utc).isoformat()
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO meta_labeler_training_log
                (version, ticker, model_path, accuracy, precision_score, recall, f1,
                 train_samples, test_samples, positive_rate_train, positive_rate_test,
                 avg_probability_test, feature_importances, config_json, trained_at, status, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?)
            """,
            (
                int(version),
                str(ticker).upper(),
                str(model_path),
                metrics.get("accuracy"),
                metrics.get("precision"),
                metrics.get("recall"),
                metrics.get("f1"),
                metrics.get("train_samples"),
                metrics.get("test_samples"),
                metrics.get("positive_rate_train"),
                metrics.get("positive_rate_test"),
                metrics.get("avg_probability_test"),
                json.dumps(metrics.get("feature_importances", {})),
                json.dumps(config or {}),
                now,
                str(notes or ""),
            ),
        )
    return {"version": int(version), "logged_at": now}


def get_training_history(*, limit: int = 20) -> list[dict[str, Any]]:
    with _connect() as conn:
        rows = conn.execute(
            "SELECT * FROM meta_labeler_training_log ORDER BY id DESC LIMIT ?",
            (int(limit),),
        ).fetchall()
    return [dict(row) for row in rows]


def get_training_run(version: int) -> dict[str, Any] | None:
    with _connect() as conn:
        row = conn.execute(
            "SELECT * FROM meta_labeler_training_log WHERE version = ? ORDER BY id DESC LIMIT 1",
            (int(version),),
        ).fetchone()
    return dict(row) if row else None


def update_training_status(version: int, status: str) -> bool:
    with _connect() as conn:
        cursor = conn.execute(
            "UPDATE meta_labeler_training_log SET status = ? WHERE version = ?",
            (str(status), int(version)),
        )
    return bool(cursor.rowcount)


def log_barrier_override(
    portfolio_id: int,
    ticker: str,
    *,
    lot_id: int | None = None,
    override_type: str = "ltcg_preservation",
    original_stop: float | None = None,
    overridden_stop: float | None = None,
    days_to_ltcg: int | None = None,
    tax_savings_estimate: float | None = None,
    additional_risk: float | None = None,
    status: str = "active",
    expires_at: str | None = None,
) -> dict[str, Any]:
    now = datetime.now(timezone.utc).isoformat()
    expiry_value = expires_at or now
    with _connect() as conn:
        cursor = conn.execute(
            """
            INSERT INTO barrier_override_log (
                portfolio_id, ticker, lot_id, override_type, original_stop, overridden_stop,
                days_to_ltcg, tax_savings_estimate, additional_risk, status, created_at, expires_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(portfolio_id),
                str(ticker or "").upper(),
                int(lot_id) if lot_id is not None else None,
                str(override_type or "ltcg_preservation"),
                float(original_stop) if original_stop is not None else None,
                float(overridden_stop) if overridden_stop is not None else None,
                int(days_to_ltcg) if days_to_ltcg is not None else None,
                float(tax_savings_estimate) if tax_savings_estimate is not None else None,
                float(additional_risk) if additional_risk is not None else None,
                str(status or "active"),
                now,
                str(expiry_value),
            ),
        )
        row_id = int(cursor.lastrowid)
        row = conn.execute("SELECT * FROM barrier_override_log WHERE id = ?", (row_id,)).fetchone()
    return dict(row) if row else {}


def log_audit_event(
    *,
    order_id: str,
    portfolio_id: int,
    event_type: str,
    ticker: str,
    action: str | None = None,
    quantity: float | None = None,
    price: float | None = None,
    actor: str = "user",
    details: str = "",
    guardrail_result: Any | None = None,
    created_at: str | None = None,
) -> dict[str, Any]:
    timestamp = created_at or datetime.now(timezone.utc).isoformat()
    guardrail_payload = None
    if guardrail_result is not None:
        if is_dataclass(guardrail_result):
            guardrail_payload = json.dumps(asdict(guardrail_result), default=str)
        elif hasattr(guardrail_result, "__dict__"):
            guardrail_payload = json.dumps(guardrail_result.__dict__, default=str)
        else:
            guardrail_payload = json.dumps(guardrail_result, default=str)
    with _connect() as conn:
        cursor = conn.execute(
            """
            INSERT INTO order_audit_trail (
                order_id, portfolio_id, event_type, ticker, action, quantity, price,
                actor, details, guardrail_result, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(order_id),
                int(portfolio_id),
                str(event_type),
                str(ticker or "").upper(),
                action,
                float(quantity) if quantity is not None else None,
                float(price) if price is not None else None,
                str(actor or "user"),
                str(details or ""),
                guardrail_payload,
                timestamp,
            ),
        )
        audit_id = int(cursor.lastrowid)
    return {"id": audit_id, "order_id": str(order_id), "event_type": str(event_type), "created_at": timestamp}


def get_audit_trail(
    portfolio_id: int | None = None,
    order_id: str | None = None,
    ticker: str | None = None,
    event_type: str | None = None,
    days: int = 30,
    limit: int = 200,
) -> list[dict[str, Any]]:
    clauses = ["created_at >= ?"]
    params: list[Any] = [(datetime.now(timezone.utc) - timedelta(days=max(1, int(days)))).isoformat()]
    if portfolio_id is not None:
        clauses.append("portfolio_id = ?")
        params.append(int(portfolio_id))
    if order_id:
        clauses.append("order_id = ?")
        params.append(str(order_id))
    if ticker:
        clauses.append("ticker = ?")
        params.append(str(ticker).upper())
    if event_type:
        clauses.append("event_type = ?")
        params.append(str(event_type))
    params.append(max(1, int(limit)))
    query = f"""
        SELECT *
        FROM order_audit_trail
        WHERE {' AND '.join(clauses)}
        ORDER BY created_at DESC, id DESC
        LIMIT ?
    """
    with _connect() as conn:
        rows = conn.execute(query, params).fetchall()
    payload: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        if item.get("guardrail_result"):
            try:
                item["guardrail_result"] = json.loads(str(item["guardrail_result"]))
            except json.JSONDecodeError:
                pass
        payload.append(item)
    return payload


def get_llm_attribution_summary(days: int = 30) -> list[dict[str, Any]]:
    events = get_audit_trail(event_type="llm_attribution", days=max(1, int(days)), limit=5000)
    buckets: dict[str, dict[str, Any]] = {}
    for event in events:
        details: dict[str, Any] = {}
        raw = event.get("details")
        if raw:
            try:
                parsed = json.loads(str(raw))
                if isinstance(parsed, dict):
                    details = parsed
            except json.JSONDecodeError:
                details = {}
        verdict = str(details.get("verdict") or "unknown").strip() or "unknown"
        bucket = buckets.setdefault(
            verdict,
            {
                "verdict": verdict,
                "trade_count": 0,
                "wins": 0,
                "realized_net_pnl": 0.0,
                "confidence_sum": 0.0,
                "confidence_count": 0,
            },
        )
        pnl = float(details.get("realized_net_pnl") or 0.0)
        bucket["trade_count"] += 1
        bucket["realized_net_pnl"] += pnl
        if pnl > 0:
            bucket["wins"] += 1
        confidence = details.get("confidence")
        if confidence not in (None, ""):
            try:
                bucket["confidence_sum"] += float(confidence)
                bucket["confidence_count"] += 1
            except (TypeError, ValueError):
                pass
    rows = list(buckets.values())
    for row in rows:
        trade_count = int(row.get("trade_count") or 0)
        confidence_count = int(row.get("confidence_count") or 0)
        row["win_rate"] = (float(row.get("wins") or 0) / trade_count * 100.0) if trade_count else None
        row["avg_net_pnl"] = (float(row.get("realized_net_pnl") or 0.0) / trade_count) if trade_count else 0.0
        row["avg_confidence"] = (float(row.get("confidence_sum") or 0.0) / confidence_count) if confidence_count else None
        row.pop("confidence_sum", None)
        row.pop("confidence_count", None)
    rows.sort(key=lambda item: (float(item.get("realized_net_pnl") or 0.0), int(item.get("trade_count") or 0)), reverse=True)
    return rows


def count_todays_trades(portfolio_id: int) -> int:
    start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
    with _connect() as conn:
        row = conn.execute(
            """
            SELECT COUNT(*)
            FROM order_audit_trail
            WHERE portfolio_id = ?
              AND event_type IN ('filled', 'partially_filled')
              AND created_at >= ?
            """,
            (int(portfolio_id), start),
        ).fetchone()
    return int(row[0] or 0) if row else 0


def get_daily_capital_deployed(portfolio_id: int) -> float:
    start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
    with _connect() as conn:
        row = conn.execute(
            """
            SELECT COALESCE(SUM(quantity * price), 0.0) AS deployed
            FROM order_audit_trail
            WHERE portfolio_id = ?
              AND event_type IN ('filled', 'partially_filled')
              AND action = 'Buy'
              AND created_at >= ?
            """,
            (int(portfolio_id), start),
        ).fetchone()
    return float(row["deployed"] or 0.0) if row else 0.0


def get_daily_audit_summary(portfolio_id: int) -> dict[str, Any]:
    start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT event_type, COUNT(*) AS count
            FROM order_audit_trail
            WHERE portfolio_id = ?
              AND created_at >= ?
            GROUP BY event_type
            """,
            (int(portfolio_id), start),
        ).fetchall()
    counts = {str(row["event_type"]): int(row["count"] or 0) for row in rows}
    last_trade_at = None
    trail = get_audit_trail(portfolio_id=portfolio_id, days=1, limit=1)
    if trail:
        last_trade_at = trail[0].get("created_at")
    return {
        "portfolio_id": int(portfolio_id),
        "date": start[:10],
        "counts": counts,
        "trades_today": int(counts.get("filled", 0) + counts.get("partially_filled", 0)),
        "orders_submitted": int(counts.get("submitted", 0)),
        "guardrail_blocks": int(counts.get("guardrail_blocked", 0)),
        "last_trade_at": last_trade_at,
        "filled_count": int(counts.get("filled", 0) + counts.get("partially_filled", 0)),
        "blocked_count": int(counts.get("guardrail_blocked", 0)),
        "rejected_count": int(counts.get("rejected", 0)),
    }
