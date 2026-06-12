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

from .positions import get_paper_positions

def create_paper_portfolio(name: str, starting_budget: float = 100000.0, broker_type: str = "paper") -> dict[str, Any]:
    now = datetime.now(timezone.utc).isoformat()
    budget = float(starting_budget)
    broker = str(broker_type or "paper").strip().lower() or "paper"
    with _connect() as conn:
        cursor = conn.execute(
            """
            INSERT INTO paper_portfolio (name, starting_budget, current_cash, broker_type, status, created_at, updated_at)
            VALUES (?, ?, ?, ?, 'Active', ?, ?)
            """,
            (name.strip(), budget, budget, broker, now, now),
        )
        portfolio_id = int(cursor.lastrowid)
    return get_paper_portfolio(portfolio_id) or {}


def get_paper_portfolio(portfolio_id: int) -> dict[str, Any] | None:
    with _connect() as conn:
        row = conn.execute(
            """
            SELECT id, name, starting_budget, current_cash, broker_type, status, created_at, updated_at
            FROM paper_portfolio
            WHERE id = ?
            """,
            (int(portfolio_id),),
        ).fetchone()
    return dict(row) if row else None


def list_paper_portfolios(include_closed: bool = False) -> list[dict[str, Any]]:
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT id, name, starting_budget, current_cash, broker_type, status, created_at, updated_at
            FROM paper_portfolio
            WHERE (? = 1 OR status != 'Closed')
            ORDER BY name ASC
            """,
            (1 if include_closed else 0,),
        ).fetchall()
    return [dict(row) for row in rows]


def update_paper_portfolio(portfolio_id: int, **fields: Any) -> dict[str, Any] | None:
    updates: list[str] = []
    params: list[Any] = []
    for key in ("name", "starting_budget", "current_cash", "status", "broker_type"):
        if key in fields and fields[key] is not None:
            updates.append(f"{key} = ?")
            params.append(fields[key])
    if not updates:
        return get_paper_portfolio(portfolio_id)
    updates.append("updated_at = ?")
    params.append(datetime.now(timezone.utc).isoformat())
    params.append(int(portfolio_id))
    with _connect() as conn:
        cursor = conn.execute(f"UPDATE paper_portfolio SET {', '.join(updates)} WHERE id = ?", params)
        if not cursor.rowcount:
            return None
    return get_paper_portfolio(portfolio_id)


def delete_paper_portfolio(portfolio_id: int) -> bool:
    with _connect() as conn:
        cursor = conn.execute("DELETE FROM paper_portfolio WHERE id = ?", (int(portfolio_id),))
        return bool(cursor.rowcount)


def get_paper_portfolio_summary(portfolio_id: int) -> dict[str, Any]:
    portfolio = get_paper_portfolio(portfolio_id)
    if portfolio is None:
        return {}
    open_positions = get_paper_positions(portfolio_id, status="Open")
    closed_positions = get_paper_positions(portfolio_id, status="Closed")
    prices: dict[str, float] = {}
    if open_positions:
        try:
            from ..paper_trading import _batch_current_prices

            prices = _batch_current_prices([str(row.get("ticker") or "") for row in open_positions])
        except Exception as exc:
            logger.debug("Unable to mark paper portfolio %s to market.", portfolio_id, exc_info=exc)
            prices = {}
    marked_positions: list[dict[str, Any]] = []
    total_market_value = 0.0
    unrealized_pnl = 0.0
    for row in open_positions:
        ticker = str(row.get("ticker") or "").upper()
        quantity = float(row.get("quantity") or 0.0)
        entry_price = float(row.get("entry_price") or 0.0)
        current_price = float(prices.get(ticker, entry_price if entry_price > 0 else 0.0) or 0.0)
        current_value = quantity * current_price
        position_unrealized = (current_price - entry_price) * quantity
        total_market_value += current_value
        unrealized_pnl += position_unrealized
        marked_positions.append(
            {
                **row,
                "current_price": current_price,
                "current_value": current_value,
                "market_value": current_value,
                "unrealized_pnl": position_unrealized,
            }
        )
    realized_pnl = sum(float(row.get("realized_pnl") or 0.0) for row in closed_positions)
    total_equity = float(portfolio.get("current_cash") or 0.0) + total_market_value
    starting_budget = float(portfolio.get("starting_budget") or 0.0)
    total_return_pct = ((total_equity - starting_budget) / starting_budget * 100.0) if starting_budget > 0 else 0.0
    return {
        **portfolio,
        "current_cash": float(portfolio.get("current_cash") or 0.0),
        "total_market_value": total_market_value,
        "unrealized_pnl": unrealized_pnl,
        "realized_pnl": realized_pnl,
        "total_return_pct": total_return_pct,
        "position_count": len(open_positions),
        "positions_open": len(open_positions),
        "positions_closed": len(closed_positions),
        "positions": marked_positions,
    }
