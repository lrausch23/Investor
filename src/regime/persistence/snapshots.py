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


def save_daily_snapshot(
    portfolio_id: int,
    snapshot_date: str,
    *,
    equity: float,
    cash: float,
    market_value: float,
    realized_pnl: float = 0.0,
    unrealized_pnl: float = 0.0,
    position_count: int = 0,
    trades_today: int = 0,
    drawdown_pct: float | None = None,
    regime_exposure_json: str | None = None,
) -> dict[str, Any]:
    now = datetime.now(timezone.utc).isoformat()
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO daily_snapshot (
                portfolio_id, snapshot_date, equity, cash, market_value,
                realized_pnl, unrealized_pnl, position_count, trades_today, drawdown_pct, regime_exposure_json, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(portfolio_id, snapshot_date) DO UPDATE SET
                equity = excluded.equity,
                cash = excluded.cash,
                market_value = excluded.market_value,
                realized_pnl = excluded.realized_pnl,
                unrealized_pnl = excluded.unrealized_pnl,
                position_count = excluded.position_count,
                trades_today = excluded.trades_today,
                drawdown_pct = excluded.drawdown_pct,
                regime_exposure_json = excluded.regime_exposure_json,
                created_at = excluded.created_at
            """,
            (
                int(portfolio_id),
                snapshot_date,
                float(equity),
                float(cash),
                float(market_value),
                float(realized_pnl),
                float(unrealized_pnl),
                int(position_count),
                int(trades_today),
                float(drawdown_pct) if drawdown_pct is not None else None,
                str(regime_exposure_json) if regime_exposure_json is not None else None,
                now,
            ),
        )
        row = conn.execute(
            "SELECT * FROM daily_snapshot WHERE portfolio_id = ? AND snapshot_date = ?",
            (int(portfolio_id), snapshot_date),
        ).fetchone()
    return dict(row) if row else {}


def get_daily_snapshots(
    portfolio_id: int,
    start_date: str | None = None,
    end_date: str | None = None,
) -> list[dict[str, Any]]:
    query = "SELECT * FROM daily_snapshot WHERE portfolio_id = ?"
    params: list[Any] = [int(portfolio_id)]
    if start_date:
        query += " AND snapshot_date >= ?"
        params.append(start_date)
    if end_date:
        query += " AND snapshot_date <= ?"
        params.append(end_date)
    query += " ORDER BY snapshot_date ASC"
    with _connect() as conn:
        rows = conn.execute(query, params).fetchall()
    return [dict(row) for row in rows]


def get_performance_timeseries(portfolio_id: int, days: int = 90) -> list[dict[str, Any]]:
    end_date = datetime.now(timezone.utc).date()
    start_date = end_date - timedelta(days=max(1, int(days)) - 1)
    snapshots = get_daily_snapshots(int(portfolio_id), start_date=start_date.isoformat(), end_date=end_date.isoformat())
    if not snapshots:
        return []
    first_equity = float(snapshots[0].get("equity") or 0.0)
    previous_equity = None
    rows: list[dict[str, Any]] = []
    for snapshot in snapshots:
        equity = float(snapshot.get("equity") or 0.0)
        daily_return_pct = None
        if previous_equity and previous_equity > 0:
            daily_return_pct = ((equity - previous_equity) / previous_equity) * 100.0
        cumulative_return_pct = ((equity - first_equity) / first_equity * 100.0) if first_equity > 0 else None
        item = dict(snapshot)
        item["daily_return_pct"] = daily_return_pct
        item["cumulative_return_pct"] = cumulative_return_pct
        exposure_raw = item.get("regime_exposure_json")
        if isinstance(exposure_raw, str) and exposure_raw.strip():
            try:
                item["regime_exposure"] = json.loads(exposure_raw)
            except json.JSONDecodeError:
                item["regime_exposure"] = None
        else:
            item["regime_exposure"] = None
        rows.append(item)
        previous_equity = equity
    return rows


def save_signal_snapshot(
    *,
    ticker: str,
    snapshot_date: str,
    action: str,
    regime_label: str,
    regime_probability: float,
    composite_strength: float,
    benchmark: str | None,
    current_price: float,
    entry_price: float | None,
    exit_price: float | None,
    stop_price: float | None,
    risk_reward_ratio: float | None,
    timeframe_days: int,
    expected_regime_duration: float | None = None,
) -> None:
    now = datetime.now(timezone.utc).isoformat()
    logger.debug("Saving signal snapshot for %s at %s", ticker, snapshot_date)
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO signal_snapshots (
                ticker, snapshot_date, action, regime_label, regime_probability, composite_strength, benchmark,
                current_price, entry_price, exit_price, stop_price, risk_reward_ratio, timeframe_days, expected_regime_duration, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(ticker, snapshot_date) DO UPDATE SET
                action = excluded.action,
                regime_label = excluded.regime_label,
                regime_probability = excluded.regime_probability,
                composite_strength = excluded.composite_strength,
                benchmark = excluded.benchmark,
                current_price = excluded.current_price,
                entry_price = excluded.entry_price,
                exit_price = excluded.exit_price,
                stop_price = excluded.stop_price,
                risk_reward_ratio = excluded.risk_reward_ratio,
                timeframe_days = excluded.timeframe_days,
                expected_regime_duration = excluded.expected_regime_duration,
                updated_at = excluded.updated_at
            """,
            (
                ticker.upper(),
                snapshot_date,
                action,
                regime_label,
                float(regime_probability),
                float(composite_strength),
                benchmark,
                float(current_price),
                float(entry_price) if entry_price is not None else None,
                float(exit_price) if exit_price is not None else None,
                float(stop_price) if stop_price is not None else None,
                float(risk_reward_ratio) if risk_reward_ratio is not None else None,
                int(timeframe_days),
                float(expected_regime_duration) if expected_regime_duration is not None else None,
                now,
            ),
        )
    try:
        from ..event_bus import get_event_bus
        from ..events import SignalSnapshotEvent

        bus = get_event_bus()
        bus.publish_sync(
            SignalSnapshotEvent(
                ticker=ticker.upper(),
                snapshot_date=snapshot_date,
                action=action,
                regime_label=regime_label,
                regime_probability=float(regime_probability),
                composite_strength=float(composite_strength),
                current_price=float(current_price),
            )
        )
    except Exception:
        logger.debug("Event bus publish failed for signal_snapshot %s — non-fatal", ticker, exc_info=True)


def save_execution_quality_snapshot(report: Any) -> int:
    now = datetime.now(timezone.utc).isoformat()
    payload = asdict(report) if is_dataclass(report) else dict(report)
    with _connect() as conn:
        cursor = conn.execute(
            """
            INSERT INTO execution_quality_snapshot (
                portfolio_id, analysis_date, total_trades, overall_avg_impl_shortfall_bps, overall_avg_vs_vwap_bps,
                by_strategy_json, by_algo_json, by_time_of_day_json, by_theme_json, by_adv_bucket_json,
                patterns_json, best_strategy, worst_strategy, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(payload.get("portfolio_id") or 0),
                str(payload.get("analysis_date") or now[:10]),
                int(payload.get("total_trades") or 0),
                payload.get("overall_avg_impl_shortfall_bps"),
                payload.get("overall_avg_vs_vwap_bps"),
                json.dumps(payload.get("by_strategy") or []),
                json.dumps(payload.get("by_algo") or []),
                json.dumps(payload.get("by_time_of_day") or []),
                json.dumps(payload.get("by_theme") or []),
                json.dumps(payload.get("by_adv_bucket") or []),
                json.dumps(payload.get("patterns") or []),
                str(payload.get("best_strategy") or "") or None,
                str(payload.get("worst_strategy") or "") or None,
                now,
            ),
        )
        return int(cursor.lastrowid)


def get_execution_quality_snapshot(
    portfolio_id: int,
    date: str | None = None,
) -> dict[str, Any] | None:
    query = """
        SELECT *
        FROM execution_quality_snapshot
        WHERE portfolio_id = ?
    """
    params: list[Any] = [int(portfolio_id)]
    if date:
        query += " AND analysis_date = ?"
        params.append(str(date))
    query += " ORDER BY analysis_date DESC, id DESC LIMIT 1"
    with _connect() as conn:
        row = conn.execute(query, params).fetchone()
    if not row:
        return None
    payload = dict(row)
    for key in (
        "by_strategy_json",
        "by_algo_json",
        "by_time_of_day_json",
        "by_theme_json",
        "by_adv_bucket_json",
        "patterns_json",
    ):
        try:
            payload[key[:-5]] = json.loads(str(payload.get(key) or "[]"))
        except Exception:
            payload[key[:-5]] = []
        payload.pop(key, None)
    return payload


def get_execution_quality_history(
    portfolio_id: int,
    limit: int = 30,
) -> list[dict[str, Any]]:
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT analysis_date, total_trades, overall_avg_impl_shortfall_bps, overall_avg_vs_vwap_bps
            FROM execution_quality_snapshot
            WHERE portfolio_id = ?
            ORDER BY analysis_date DESC, id DESC
            LIMIT ?
            """,
            (int(portfolio_id), max(1, int(limit))),
        ).fetchall()
    return [dict(row) for row in rows]


def get_pending_outcomes(as_of: str | None = None) -> list[dict[str, int | str | float | None]]:
    today = datetime.fromisoformat(as_of).date() if as_of else datetime.now(timezone.utc).date()
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM signal_snapshots
            ORDER BY snapshot_date ASC, ticker ASC
            """
        ).fetchall()
    pending: list[dict[str, int | str | float | None]] = []
    for row in rows:
        snapshot = dict(row)
        snapshot_date = datetime.fromisoformat(str(snapshot["snapshot_date"])).date()
        due_1w = snapshot_date.fromordinal(snapshot_date.toordinal() + 7)
        due_1m = snapshot_date.fromordinal(snapshot_date.toordinal() + 30)
        due_3m = snapshot_date.fromordinal(snapshot_date.toordinal() + 90)
        if snapshot.get("return_1w") is None and due_1w <= today:
            pending.append({**snapshot, "interval": "1w"})
        if snapshot.get("return_1m") is None and due_1m <= today:
            pending.append({**snapshot, "interval": "1m"})
        if snapshot.get("return_3m") is None and due_3m <= today:
            pending.append({**snapshot, "interval": "3m"})
    return pending


def get_latest_signal_snapshot(ticker: str, max_age_days: int = 7) -> dict[str, Any] | None:
    with _connect() as conn:
        row = conn.execute(
            """
            SELECT *
            FROM signal_snapshots
            WHERE ticker = ?
              AND snapshot_date >= date('now', ?)
            ORDER BY snapshot_date DESC, updated_at DESC, id DESC
            LIMIT 1
            """,
            (str(ticker or "").upper(), f"-{int(max_age_days)} day"),
        ).fetchone()
    return dict(row) if row else None


def _signal_hit(action: str, raw_return: float) -> int:
    normalized = str(action or "").lower()
    if "sell" in normalized:
        return int(raw_return < 0)
    if "hold" in normalized:
        return int(abs(raw_return) <= 0.03)
    return int(raw_return > 0)


def update_signal_outcome(snapshot_id: int, interval: str, current_price: float) -> None:
    column_map = {
        "1w": ("return_1w", "hit_1w"),
        "1m": ("return_1m", "hit_1m"),
        "3m": ("return_3m", "hit_3m"),
    }
    if interval not in column_map:
        raise PersistenceError(f"Unsupported interval: {interval}")
    return_col, hit_col = column_map[interval]
    with _connect() as conn:
        row = conn.execute("SELECT id, action, current_price FROM signal_snapshots WHERE id = ?", (int(snapshot_id),)).fetchone()
        if row is None:
            return
        base_price = float(row["current_price"] or 0.0)
        if base_price <= 0:
            return
        raw_return = (float(current_price) - base_price) / base_price
        conn.execute(
            f"UPDATE signal_snapshots SET {return_col} = ?, {hit_col} = ?, updated_at = ? WHERE id = ?",
            (raw_return, _signal_hit(str(row["action"]), raw_return), datetime.now(timezone.utc).isoformat(), int(snapshot_id)),
        )


def get_signal_effectiveness() -> dict[str, object]:
    with _connect() as conn:
        rows = [dict(row) for row in conn.execute("SELECT * FROM signal_snapshots ORDER BY snapshot_date DESC, ticker ASC").fetchall()]
    intervals = [("1w", "return_1w", "hit_1w"), ("1m", "return_1m", "hit_1m"), ("3m", "return_3m", "hit_3m")]
    summary: dict[str, dict[str, float | int | None]] = {}
    by_action: dict[str, dict[str, dict[str, float | int | None]]] = {}
    for key, return_col, hit_col in intervals:
        interval_rows = [row for row in rows if row.get(return_col) is not None]
        returns = [float(row[return_col]) for row in interval_rows]
        hits = [int(row[hit_col]) for row in interval_rows if row.get(hit_col) is not None]
        summary[key] = {
            "count": len(interval_rows),
            "hit_rate": (sum(hits) / len(hits)) if hits else None,
            "avg_return": (sum(returns) / len(returns)) if returns else None,
        }
        action_summary: dict[str, dict[str, float | int | None]] = {}
        for action in sorted({str(row["action"]) for row in interval_rows}):
            action_rows = [row for row in interval_rows if str(row["action"]) == action]
            action_returns = [float(row[return_col]) for row in action_rows]
            action_hits = [int(row[hit_col]) for row in action_rows if row.get(hit_col) is not None]
            action_summary[action] = {
                "count": len(action_rows),
                "hit_rate": (sum(action_hits) / len(action_hits)) if action_hits else None,
                "avg_return": (sum(action_returns) / len(action_returns)) if action_returns else None,
            }
        by_action[key] = action_summary
    return {"summary": summary, "by_action": by_action, "rows": rows}


def get_calibration_data(lookback_days: int = 365) -> list[dict[str, Any]]:
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM signal_snapshots
            WHERE snapshot_date >= date('now', ?)
              AND (return_1w IS NOT NULL OR return_1m IS NOT NULL OR return_3m IS NOT NULL)
            ORDER BY snapshot_date DESC, ticker ASC
            """,
            (f"-{int(lookback_days)} day",),
        ).fetchall()
    return [dict(row) for row in rows]
