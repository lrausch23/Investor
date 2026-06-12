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


def create_tax_lot(
    portfolio_id: int,
    position_id: int | None,
    ticker: str,
    quantity: float,
    cost_basis_per_share: float,
    acquisition_date: str,
    notes: str = "",
) -> dict[str, Any]:
    now = datetime.now(timezone.utc).isoformat()
    with _connect() as conn:
        cursor = conn.execute(
            """
            INSERT INTO paper_tax_lot (
                portfolio_id, position_id, ticker, quantity, remaining_quantity,
                cost_basis_per_share, acquisition_date, notes, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(portfolio_id),
                int(position_id) if position_id is not None else None,
                str(ticker or "").upper(),
                float(quantity),
                float(quantity),
                float(cost_basis_per_share),
                acquisition_date,
                notes,
                now,
                now,
            ),
        )
        lot_id = int(cursor.lastrowid)
    return get_tax_lot(lot_id) or {}


def _compute_lot_holding_fields(row: dict[str, Any]) -> dict[str, Any]:
    acquisition_text = str(row.get("acquisition_date") or "")
    now = datetime.now(timezone.utc)
    acquisition_dt: datetime | None = None
    try:
        acquisition_dt = datetime.fromisoformat(acquisition_text.replace("Z", "+00:00"))
    except Exception:
        acquisition_dt = None
    if acquisition_dt is None:
        days_held = 0
    else:
        if acquisition_dt.tzinfo is None:
            acquisition_dt = acquisition_dt.replace(tzinfo=timezone.utc)
        days_held = max(0, (now - acquisition_dt.astimezone(timezone.utc)).days)
    term = "LT" if days_held >= 366 else "ST"
    days_to_ltcg = 0 if term == "LT" else max(0, 366 - days_held)
    return {
        **row,
        "cost_basis_total": float(row.get("quantity") or 0.0) * float(row.get("cost_basis_per_share") or 0.0),
        "days_held": days_held,
        "term": term,
        "days_to_ltcg": 0 if term == "LT" else days_to_ltcg,
    }


def get_tax_lot(lot_id: int) -> dict[str, Any] | None:
    with _connect() as conn:
        row = conn.execute("SELECT * FROM paper_tax_lot WHERE id = ?", (int(lot_id),)).fetchone()
    return _compute_lot_holding_fields(dict(row)) if row else None


def get_tax_lots(portfolio_id: int, ticker: str | None = None, status: str = "open") -> list[dict[str, Any]]:
    query = "SELECT * FROM paper_tax_lot WHERE portfolio_id = ?"
    params: list[Any] = [int(portfolio_id)]
    if ticker:
        query += " AND ticker = ?"
        params.append(str(ticker).upper())
    if status and status.lower() != "all":
        query += " AND status = ?"
        params.append(status.lower())
    query += " ORDER BY ticker ASC, acquisition_date ASC, id ASC"
    with _connect() as conn:
        rows = conn.execute(query, params).fetchall()
    return [_compute_lot_holding_fields(dict(row)) for row in rows]


def close_tax_lot(
    lot_id: int,
    quantity_to_close: float,
    exit_price: float,
    exit_date: str,
) -> dict[str, Any] | None:
    lot = get_tax_lot(lot_id)
    if lot is None:
        return None
    remaining_quantity = float(lot.get("remaining_quantity") or 0.0)
    close_qty = float(quantity_to_close or 0.0)
    if close_qty <= 0 or close_qty > remaining_quantity + 1e-9:
        raise DataValidationError("Invalid quantity_to_close for tax lot.")
    cost_basis = float(lot.get("cost_basis_per_share") or 0.0)
    realized_pnl = (float(exit_price) - cost_basis) * close_qty
    new_remaining = max(0.0, remaining_quantity - close_qty)
    status = "closed" if new_remaining <= 1e-9 else "partial"
    days_held = int(lot.get("days_held") or 0)
    is_long_term = days_held >= 366
    gain_loss_term = ("LT" if is_long_term else "ST") + ("_GAIN" if realized_pnl >= 0 else "_LOSS")
    with _connect() as conn:
        conn.execute(
            """
            UPDATE paper_tax_lot
            SET remaining_quantity = ?, closed_date = ?, exit_price = ?, realized_pnl = ?,
                gain_loss_term = ?, status = ?, updated_at = ?
            WHERE id = ?
            """,
            (
                new_remaining,
                exit_date if status == "closed" else None,
                float(exit_price),
                realized_pnl,
                gain_loss_term,
                status,
                datetime.now(timezone.utc).isoformat(),
                int(lot_id),
            ),
        )
    updated = get_tax_lot(lot_id) or {}
    updated["closed_quantity"] = close_qty
    updated["realized_pnl"] = realized_pnl
    updated["gain_loss_term"] = gain_loss_term
    return updated


def add_wash_sale_restriction(
    portfolio_id: int,
    ticker: str,
    loss_sale_date: str,
    loss_amount: float,
    lot_id: int | None = None,
) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    try:
        loss_dt = datetime.fromisoformat(str(loss_sale_date).replace("Z", "+00:00"))
    except Exception:
        loss_dt = now
    if loss_dt.tzinfo is None:
        loss_dt = loss_dt.replace(tzinfo=timezone.utc)
    expires = (loss_dt + timedelta(days=31)).isoformat()
    with _connect() as conn:
        cursor = conn.execute(
            """
            INSERT INTO wash_sale_restricted (
                portfolio_id, ticker, loss_sale_date, restriction_expires, loss_amount,
                source_lot_id, active, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?)
            """,
            (
                int(portfolio_id),
                str(ticker or "").upper(),
                loss_dt.isoformat(),
                expires,
                float(loss_amount),
                int(lot_id) if lot_id is not None else None,
                now.isoformat(),
                now.isoformat(),
            ),
        )
        restriction_id = int(cursor.lastrowid)
    return get_wash_sale_restriction(restriction_id) or {}


def get_wash_sale_restriction(restriction_id: int) -> dict[str, Any] | None:
    with _connect() as conn:
        row = conn.execute("SELECT * FROM wash_sale_restricted WHERE id = ?", (int(restriction_id),)).fetchone()
    return dict(row) if row else None


def get_wash_sale_restrictions(
    portfolio_id: int,
    ticker: str | None = None,
    active_only: bool = False,
) -> list[dict[str, Any]]:
    query = "SELECT * FROM wash_sale_restricted WHERE portfolio_id = ?"
    params: list[Any] = [int(portfolio_id)]
    if ticker:
        query += " AND ticker = ?"
        params.append(str(ticker).upper())
    if active_only:
        now = datetime.now(timezone.utc).isoformat()
        query += " AND active = 1 AND restriction_expires > ?"
        params.append(now)
    query += " ORDER BY restriction_expires ASC, id ASC"
    with _connect() as conn:
        rows = conn.execute(query, params).fetchall()
    payload: list[dict[str, Any]] = []
    current = datetime.now(timezone.utc)
    for row in rows:
        item = dict(row)
        try:
            expires = datetime.fromisoformat(str(item.get("restriction_expires") or "").replace("Z", "+00:00"))
            if expires.tzinfo is None:
                expires = expires.replace(tzinfo=timezone.utc)
            item["days_remaining"] = max(0, (expires - current).days)
        except Exception:
            item["days_remaining"] = 0
        payload.append(item)
    return payload


def is_wash_sale_restricted(portfolio_id: int, ticker: str) -> bool:
    restrictions = get_wash_sale_restrictions(portfolio_id, ticker=ticker, active_only=True)
    return bool(restrictions)


def open_paper_position(
    portfolio_id: int,
    ticker: str,
    quantity: float,
    entry_price: float,
    entry_date: str,
    theme_id: int | None = None,
    role: str = "Critical-Path",
    stop_price: float | None = None,
    target_price: float | None = None,
    side: str = "long",
) -> dict[str, Any]:
    now = datetime.now(timezone.utc).isoformat()
    with _connect() as conn:
        cursor = conn.execute(
            """
            INSERT INTO paper_position (
                portfolio_id, theme_id, ticker, side, quantity, entry_price, entry_date,
                stop_price, target_price, role, status, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'Open', ?, ?)
            """,
            (
                int(portfolio_id),
                int(theme_id) if theme_id is not None else None,
                ticker.upper(),
                side,
                float(quantity),
                float(entry_price),
                entry_date,
                float(stop_price) if stop_price is not None else None,
                float(target_price) if target_price is not None else None,
                role,
                now,
                now,
            ),
        )
        position_id = int(cursor.lastrowid)
    return get_paper_position(position_id) or {}


def get_paper_position(position_id: int) -> dict[str, Any] | None:
    with _connect() as conn:
        row = conn.execute("SELECT * FROM paper_position WHERE id = ?", (int(position_id),)).fetchone()
    return dict(row) if row else None


def get_paper_positions(portfolio_id: int, status: str = "Open") -> list[dict[str, Any]]:
    query = "SELECT * FROM paper_position WHERE portfolio_id = ?"
    params: list[Any] = [int(portfolio_id)]
    if status and status.lower() != "all":
        query += " AND status = ?"
        params.append(status)
    query += " ORDER BY entry_date DESC, ticker ASC"
    with _connect() as conn:
        rows = conn.execute(query, params).fetchall()
    return [dict(row) for row in rows]


def update_paper_position_quantity(position_id: int, quantity: float) -> dict[str, Any] | None:
    with _connect() as conn:
        cursor = conn.execute(
            "UPDATE paper_position SET quantity = ?, updated_at = ? WHERE id = ?",
            (float(quantity), datetime.now(timezone.utc).isoformat(), int(position_id)),
        )
        if not cursor.rowcount:
            return None
    return get_paper_position(position_id)


def update_paper_position_risk(
    position_id: int,
    *,
    stop_price: float | None = None,
    target_price: float | None = None,
) -> dict[str, Any] | None:
    updates: list[str] = []
    params: list[Any] = []
    if stop_price is not None:
        updates.append("stop_price = ?")
        params.append(float(stop_price))
    if target_price is not None:
        updates.append("target_price = ?")
        params.append(float(target_price))
    if not updates:
        return get_paper_position(position_id)
    updates.append("updated_at = ?")
    params.append(datetime.now(timezone.utc).isoformat())
    params.append(int(position_id))
    with _connect() as conn:
        cursor = conn.execute(f"UPDATE paper_position SET {', '.join(updates)} WHERE id = ?", params)
        if not cursor.rowcount:
            return None
    return get_paper_position(position_id)


def close_paper_position(position_id: int, exit_price: float, exit_date: str, exit_reason: str = "manual") -> dict[str, Any] | None:
    position = get_paper_position(position_id)
    if position is None:
        return None
    qty = float(position.get("quantity") or 0.0)
    entry_price = float(position.get("entry_price") or 0.0)
    side = str(position.get("side") or "long")
    realized_pnl = (float(exit_price) - entry_price) * qty if side == "long" else (entry_price - float(exit_price)) * qty
    with _connect() as conn:
        conn.execute(
            """
            UPDATE paper_position
            SET exit_price = ?, exit_date = ?, exit_reason = ?, status = 'Closed', realized_pnl = ?, updated_at = ?
            WHERE id = ?
            """,
            (float(exit_price), exit_date, exit_reason, realized_pnl, datetime.now(timezone.utc).isoformat(), int(position_id)),
        )
    return get_paper_position(position_id)
