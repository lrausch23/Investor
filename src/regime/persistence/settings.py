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


def get_setting(key: str) -> str | None:
    with _connect() as conn:
        row = conn.execute("SELECT value FROM regime_settings WHERE key = ?", (str(key),)).fetchone()
        if row is None:
            return None
        return str(row["value"])


def get_operating_mode() -> str:
    mode = get_setting("operating_mode")
    return mode if mode in OPERATING_MODES else DEFAULT_OPERATING_MODE


def set_operating_mode(mode: str) -> None:
    normalized = str(mode or "").strip().lower()
    if normalized not in OPERATING_MODES:
        raise DataValidationError(f"Invalid mode: {mode}. Must be one of {OPERATING_MODES}")
    set_setting("operating_mode", normalized)


def get_auto_approve_threshold() -> float:
    raw = get_setting("auto_approve_threshold")
    if raw is None:
        return DEFAULT_AUTO_APPROVE_THRESHOLD
    try:
        return max(0.0, min(1.0, float(raw)))
    except (ValueError, TypeError):
        return DEFAULT_AUTO_APPROVE_THRESHOLD


def set_auto_approve_threshold(threshold: float) -> None:
    set_setting("auto_approve_threshold", str(max(0.0, min(1.0, float(threshold)))))


def get_daily_capital_ceiling_pct() -> float:
    raw = get_setting("daily_capital_ceiling_pct")
    if raw is None:
        return DEFAULT_DAILY_CAPITAL_CEILING_PCT
    try:
        return max(0.0, min(1.0, float(raw)))
    except (ValueError, TypeError):
        return DEFAULT_DAILY_CAPITAL_CEILING_PCT


def set_daily_capital_ceiling_pct(pct: float) -> None:
    set_setting("daily_capital_ceiling_pct", str(max(0.0, min(1.0, float(pct)))))


def get_lot_selection_method() -> str:
    method = str(get_setting("lot_selection_method") or DEFAULT_LOT_SELECTION_METHOD).strip().upper()
    return method if method in LOT_SELECTION_METHODS else DEFAULT_LOT_SELECTION_METHOD


def set_lot_selection_method(method: str) -> str:
    normalized = str(method or "").strip().upper()
    if normalized not in LOT_SELECTION_METHODS:
        raise DataValidationError(f"Invalid lot selection method: {method}")
    set_setting("lot_selection_method", normalized)
    return normalized


def get_ltcg_defer_window_days() -> int:
    raw = get_setting("ltcg_defer_window_days")
    if raw is None:
        return DEFAULT_LTCG_DEFER_WINDOW_DAYS
    try:
        return max(0, int(raw))
    except (ValueError, TypeError):
        return DEFAULT_LTCG_DEFER_WINDOW_DAYS


def set_ltcg_defer_window_days(days: int) -> int:
    normalized = max(0, int(days))
    set_setting("ltcg_defer_window_days", str(normalized))
    return normalized


def is_live_trading_unlocked() -> bool:
    return get_setting("live_trading_unlocked") == "true"


def set_live_trading_unlocked(unlocked: bool) -> None:
    set_setting("live_trading_unlocked", "true" if unlocked else "false")


def set_setting(key: str, value: str) -> None:
    now = datetime.now(timezone.utc).isoformat()
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO regime_settings (key, value, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
                value = excluded.value,
                updated_at = excluded.updated_at
            """,
            (str(key), str(value), now),
        )


def get_all_settings(prefix: str = "") -> dict[str, str]:
    clause = ""
    params: tuple[Any, ...] = ()
    if prefix:
        clause = "WHERE key LIKE ?"
        params = (f"{prefix}%",)
    with _connect() as conn:
        rows = conn.execute(
            f"SELECT key, value FROM regime_settings {clause} ORDER BY key ASC",
            params,
        ).fetchall()
    return {str(row["key"]): str(row["value"]) for row in rows}


def delete_setting(key: str) -> bool:
    with _connect() as conn:
        cursor = conn.execute("DELETE FROM regime_settings WHERE key = ?", (str(key),))
        return bool(cursor.rowcount)
