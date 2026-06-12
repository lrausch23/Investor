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


def create_trade_plan(
    portfolio_id: int,
    ticker: str,
    action: str,
    quantity: float,
    rationale: str,
    theme_id: int | None = None,
    proposed_price: float | None = None,
    arrival_price: float | None = None,
    vwap_benchmark: float | None = None,
    close_price: float | None = None,
    regime_label: str | None = None,
    regime_probability: float | None = None,
    crowd_score: int | None = None,
    source: str = "discovery",
    order_type: str = "limit",
    routing_strategy: str = "",
    algo_strategy: str = "",
    meta_labeler_score: float | None = None,
    stop_price: float | None = None,
    target_price: float | None = None,
    risk_reward_ratio: float | None = None,
    timeframe_days: int | None = None,
    trade_geometry_source: str = "",
    sizing_method: str = "equal_dollar",
    agent_trace: str = "",
    hurdle_gross_return_pct: float | None = None,
    hurdle_net_return_pct: float | None = None,
    hurdle_passed: bool | None = None,
    duration_gate_passed: bool | None = None,
    expected_regime_duration: float | None = None,
    anti_churn_passed: bool | None = None,
    ltcg_override_active: bool | None = None,
    ltcg_protected_quantity: float | None = None,
    ltcg_tax_savings: float | None = None,
    signal_quality_score: float | None = None,
    signal_quality_grade: str = "",
    signal_quality_reasons: list[str] | tuple[str, ...] | str | None = None,
    agent_key: str = "",
    llm_used: bool = False,
    llm_influenced: bool = False,
    llm_influence: str = "",
    llm_source: str = "",
    llm_provider: str = "",
    llm_model: str = "",
    llm_model_display: str = "",
    llm_verdict: str = "",
    llm_confidence: float | None = None,
    decision_constants_version: str = "",
) -> dict[str, Any]:
    now = datetime.now(timezone.utc).isoformat()
    if not str(decision_constants_version or "").strip():
        from ..decision_constants import decision_constants_version as _decision_constants_version

        decision_constants_version = _decision_constants_version()
    if isinstance(signal_quality_reasons, str):
        signal_quality_reasons_json = signal_quality_reasons
    else:
        signal_quality_reasons_json = json.dumps(list(signal_quality_reasons or []))
    with _connect() as conn:
        cursor = conn.execute(
            """
            INSERT INTO paper_trade_plan (
                portfolio_id, theme_id, ticker, action, quantity, proposed_price, rationale,
                regime_label, regime_probability, crowd_score, source, status, order_type, routing_strategy, algo_strategy,
                arrival_price, vwap_benchmark, close_price, meta_labeler_score,
                stop_price, target_price, risk_reward_ratio, timeframe_days, trade_geometry_source,
                sizing_method, agent_trace,
                hurdle_gross_return_pct, hurdle_net_return_pct, hurdle_passed, duration_gate_passed, expected_regime_duration,
                anti_churn_passed, ltcg_override_active, ltcg_protected_quantity, ltcg_tax_savings,
                signal_quality_score, signal_quality_grade, signal_quality_reasons,
                agent_key, llm_used, llm_influenced, llm_influence, llm_source, llm_provider, llm_model,
                llm_model_display, llm_verdict, llm_confidence, decision_constants_version,
                created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'Pending', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(portfolio_id),
                int(theme_id) if theme_id is not None else None,
                ticker.upper(),
                action,
                float(quantity),
                float(proposed_price) if proposed_price is not None else None,
                rationale,
                regime_label,
                float(regime_probability) if regime_probability is not None else None,
                int(crowd_score) if crowd_score is not None else None,
                source,
                str(order_type or "limit"),
                str(routing_strategy or ""),
                str(algo_strategy or ""),
                float(arrival_price) if arrival_price is not None else None,
                float(vwap_benchmark) if vwap_benchmark is not None else None,
                float(close_price) if close_price is not None else None,
                float(meta_labeler_score) if meta_labeler_score is not None else None,
                float(stop_price) if stop_price is not None else None,
                float(target_price) if target_price is not None else None,
                float(risk_reward_ratio) if risk_reward_ratio is not None else None,
                int(timeframe_days) if timeframe_days is not None else None,
                str(trade_geometry_source or ""),
                str(sizing_method or "equal_dollar"),
                str(agent_trace or ""),
                float(hurdle_gross_return_pct) if hurdle_gross_return_pct is not None else None,
                float(hurdle_net_return_pct) if hurdle_net_return_pct is not None else None,
                None if hurdle_passed is None else (1 if hurdle_passed else 0),
                None if duration_gate_passed is None else (1 if duration_gate_passed else 0),
                float(expected_regime_duration) if expected_regime_duration is not None else None,
                None if anti_churn_passed is None else (1 if anti_churn_passed else 0),
                None if ltcg_override_active is None else (1 if ltcg_override_active else 0),
                float(ltcg_protected_quantity) if ltcg_protected_quantity is not None else None,
                float(ltcg_tax_savings) if ltcg_tax_savings is not None else None,
                float(signal_quality_score) if signal_quality_score is not None else None,
                str(signal_quality_grade or ""),
                signal_quality_reasons_json,
                str(agent_key or ""),
                1 if llm_used else 0,
                1 if llm_influenced else 0,
                str(llm_influence or ""),
                str(llm_source or ""),
                str(llm_provider or ""),
                str(llm_model or ""),
                str(llm_model_display or ""),
                str(llm_verdict or ""),
                float(llm_confidence) if llm_confidence is not None else None,
                str(decision_constants_version or ""),
                now,
                now,
            ),
        )
        plan_id = int(cursor.lastrowid)
    try:
        from ..event_bus import get_event_bus
        from ..events import TradeIntentEvent

        bus = get_event_bus()
        bus.publish_sync(
            TradeIntentEvent(
                ticker=ticker.upper(),
                portfolio_id=int(portfolio_id),
                action=str(action),
                source=str(source),
                plan_id=plan_id,
                meta_labeler_score=float(meta_labeler_score) if meta_labeler_score is not None else None,
                regime_label=str(regime_label or ""),
                quantity=float(quantity),
                proposed_price=float(proposed_price) if proposed_price is not None else None,
                rationale=str(rationale or ""),
            )
        )
    except Exception:
        logger.debug("Event bus publish failed for trade_intent %s — non-fatal", ticker, exc_info=True)
    return get_trade_plan(plan_id) or {}


def get_trade_plan(plan_id: int) -> dict[str, Any] | None:
    with _connect() as conn:
        row = conn.execute("SELECT * FROM paper_trade_plan WHERE id = ?", (int(plan_id),)).fetchone()
    return dict(row) if row else None


def get_trade_plans(portfolio_id: int, status: str = "Pending") -> list[dict[str, Any]]:
    query = "SELECT * FROM paper_trade_plan WHERE portfolio_id = ?"
    params: list[Any] = [int(portfolio_id)]
    if status and status.lower() != "all":
        query += " AND status = ?"
        params.append(status)
    query += " ORDER BY created_at DESC, ticker ASC"
    with _connect() as conn:
        rows = conn.execute(query, params).fetchall()
    return [dict(row) for row in rows]


def update_trade_plan_benchmarks(
    plan_id: int,
    *,
    vwap_benchmark: float | None = None,
    close_price: float | None = None,
) -> bool:
    updates: list[str] = []
    params: list[Any] = []
    if vwap_benchmark is not None:
        updates.append("vwap_benchmark = ?")
        params.append(float(vwap_benchmark))
    if close_price is not None:
        updates.append("close_price = ?")
        params.append(float(close_price))
    if not updates:
        return False
    updates.append("updated_at = ?")
    params.append(datetime.now(timezone.utc).isoformat())
    params.append(int(plan_id))
    with _connect() as conn:
        cursor = conn.execute(
            f"UPDATE paper_trade_plan SET {', '.join(updates)} WHERE id = ?",
            params,
        )
        return bool(cursor.rowcount)


def count_executed_sell_plans(portfolio_id: int, ticker: str, days: int = 30) -> int:
    with _connect() as conn:
        row = conn.execute(
            """
            SELECT COUNT(*) AS count
            FROM paper_trade_plan
            WHERE portfolio_id = ? AND ticker = ? AND action = 'Sell'
              AND status = 'Executed'
              AND executed_at >= ?
            """,
            (
                int(portfolio_id),
                str(ticker or "").upper(),
                (datetime.now(timezone.utc) - timedelta(days=max(1, int(days)))).isoformat(),
            ),
        ).fetchone()
    return int(row["count"] or 0) if row else 0


def get_oldest_executed_sell_at(portfolio_id: int, ticker: str, days: int = 30) -> str | None:
    with _connect() as conn:
        row = conn.execute(
            """
            SELECT executed_at
            FROM paper_trade_plan
            WHERE portfolio_id = ? AND ticker = ? AND action = 'Sell'
              AND status = 'Executed'
              AND executed_at >= ?
            ORDER BY executed_at ASC
            LIMIT 1
            """,
            (
                int(portfolio_id),
                str(ticker or "").upper(),
                (datetime.now(timezone.utc) - timedelta(days=max(1, int(days)))).isoformat(),
            ),
        ).fetchone()
    return str(row["executed_at"]) if row and row["executed_at"] else None


def update_trade_plan_status(plan_id: int, status: str, **kwargs: Any) -> dict[str, Any] | None:
    updates = ["status = ?"]
    params: list[Any] = [status]
    for key in ("reviewed_at", "executed_at", "execution_price", "notes", "quantity", "proposed_price"):
        if key in kwargs and kwargs[key] is not None:
            updates.append(f"{key} = ?")
            params.append(kwargs[key])
    for key in ("broker_order_id", "broker_status", "filled_quantity"):
        if key in kwargs and kwargs[key] is not None:
            updates.append(f"{key} = ?")
            params.append(kwargs[key])
    updates.append("updated_at = ?")
    params.append(datetime.now(timezone.utc).isoformat())
    params.append(int(plan_id))
    with _connect() as conn:
        cursor = conn.execute(f"UPDATE paper_trade_plan SET {', '.join(updates)} WHERE id = ?", params)
        if not cursor.rowcount:
            return None
    return get_trade_plan(plan_id)
