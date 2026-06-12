# mypy: ignore-errors
from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime, timedelta, timezone
import logging
from pathlib import Path
from dataclasses import asdict, is_dataclass
from typing import Any

from ..exceptions import DataValidationError, DuplicateThemeError, PersistenceError
from ..logging_config import setup_regime_logging

setup_regime_logging()
logger = logging.getLogger(__name__)

def _default_data_dir() -> Path:
    configured = os.getenv("HMM_DATA_DIR")
    if configured:
        return Path(configured)
    project_data = Path(__file__).resolve().parents[2] / "data" / "regime"
    return project_data


DB_PATH = _default_data_dir() / "regime_watch.db"

_TRANSITION_COLUMNS: dict[str, str] = {
    "price_at_change": "REAL",
    "return_5d": "REAL",
    "return_10d": "REAL",
    "return_21d": "REAL",
    "outcome_updated_at": "TEXT",
}

_PAPER_PORTFOLIO_COLUMNS: dict[str, str] = {
    "broker_type": "TEXT NOT NULL DEFAULT 'paper'",
}

_PAPER_TRADE_PLAN_COLUMNS: dict[str, str] = {
    "broker_order_id": "TEXT",
    "broker_status": "TEXT",
    "filled_quantity": "REAL NOT NULL DEFAULT 0",
    "order_type": "TEXT NOT NULL DEFAULT 'limit'",
    "routing_strategy": "TEXT NOT NULL DEFAULT ''",
    "algo_strategy": "TEXT NOT NULL DEFAULT ''",
    "arrival_price": "REAL",
    "vwap_benchmark": "REAL",
    "close_price": "REAL",
    "meta_labeler_score": "REAL",
    "stop_price": "REAL",
    "target_price": "REAL",
    "risk_reward_ratio": "REAL",
    "timeframe_days": "INTEGER",
    "trade_geometry_source": "TEXT NOT NULL DEFAULT ''",
    "agent_trace": "TEXT NOT NULL DEFAULT ''",
    "hurdle_gross_return_pct": "REAL",
    "hurdle_net_return_pct": "REAL",
    "hurdle_passed": "INTEGER",
    "duration_gate_passed": "INTEGER",
    "expected_regime_duration": "REAL",
    "anti_churn_passed": "INTEGER",
    "ltcg_override_active": "INTEGER",
    "ltcg_protected_quantity": "REAL",
    "ltcg_tax_savings": "REAL",
    "signal_quality_score": "REAL",
    "signal_quality_grade": "TEXT NOT NULL DEFAULT ''",
    "signal_quality_reasons": "TEXT NOT NULL DEFAULT '[]'",
    "agent_key": "TEXT NOT NULL DEFAULT ''",
    "llm_used": "INTEGER NOT NULL DEFAULT 0",
    "llm_influenced": "INTEGER NOT NULL DEFAULT 0",
    "llm_influence": "TEXT NOT NULL DEFAULT ''",
    "llm_source": "TEXT NOT NULL DEFAULT ''",
    "llm_provider": "TEXT NOT NULL DEFAULT ''",
    "llm_model": "TEXT NOT NULL DEFAULT ''",
    "llm_model_display": "TEXT NOT NULL DEFAULT ''",
    "llm_verdict": "TEXT NOT NULL DEFAULT ''",
    "llm_confidence": "REAL",
    "decision_constants_version": "TEXT NOT NULL DEFAULT ''",
}

_SIGNAL_SNAPSHOT_COLUMNS: dict[str, str] = {
    "expected_regime_duration": "REAL",
}

LOT_SELECTION_METHODS = ("HIFO", "HIFO_LTCG", "FIFO", "LIFO")
DEFAULT_LOT_SELECTION_METHOD = "HIFO_LTCG"
DEFAULT_LTCG_DEFER_WINDOW_DAYS = 30

_SECTOR_CACHE_TTL_DAYS = 30
_EARNINGS_CACHE_TTL_HOURS = 24

OPERATING_MODES = ("manual", "semi_auto", "autonomous")
DEFAULT_OPERATING_MODE = "manual"
DEFAULT_AUTO_APPROVE_THRESHOLD = 0.65
DEFAULT_DAILY_CAPITAL_CEILING_PCT = 0.25

ALERT_TYPES = (
    "regime_change",
    "risk_spike",
    "signal_change",
    "stop_proximity",
    "daily_loss_breach",
    "meta_labeler_veto",
    "vix_freeze",
    "vix_resume",
    "execution_error",
    "connection_lost",
    "connection_restored",
    "drawdown_breach",
    "concentration_breach",
    "ml_accuracy_drift",
    "capital_ceiling_breach",
    "wash_sale_block",
    "data_validation_failed",
    "bus_event",
    "analysis_request",
    "fundamental_assessment",
    "trade_decision",
    "order_execution",
    "thesis_monitor",
    "decision_health",
    "test",
)

NOTIFICATION_CHANNELS = ("in_app", "email", "slack")
_NOTIFICATION_DEFAULT_MATRIX: dict[str, tuple[str, ...]] = {
    "regime_change": ("in_app", "email", "slack"),
    "risk_spike": ("in_app", "email", "slack"),
    "daily_loss_breach": ("in_app", "email", "slack"),
    "vix_freeze": ("in_app", "email", "slack"),
    "vix_resume": ("in_app", "email", "slack"),
    "execution_error": ("in_app", "email"),
    "connection_lost": ("in_app", "email", "slack"),
    "connection_restored": ("in_app",),
    "meta_labeler_veto": ("in_app",),
    "signal_change": ("in_app",),
    "stop_proximity": ("in_app",),
    "drawdown_breach": ("in_app", "email", "slack"),
    "concentration_breach": ("in_app", "email"),
    "ml_accuracy_drift": ("in_app", "email"),
    "capital_ceiling_breach": ("in_app", "email"),
    "wash_sale_block": ("in_app",),
    "data_validation_failed": ("in_app", "email"),
    "bus_event": ("in_app",),
    "analysis_request": ("in_app",),
    "fundamental_assessment": ("in_app",),
    "trade_decision": ("in_app",),
    "order_execution": ("in_app",),
    "thesis_monitor": ("in_app", "email"),
    "decision_health": ("in_app",),
    "test": ("in_app", "email", "slack"),
}

_ORDER_AUDIT_EVENT_TYPES = (
    "created",
    "guardrail_check",
    "guardrail_blocked",
    "submitted",
    "filled",
    "partially_filled",
    "rejected",
    "cancelled",
    "expired",
    "error",
    "auto_approved",
    "llm_attribution",
)


def _connect() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    try:
        conn = sqlite3.connect(DB_PATH)
    except sqlite3.Error as exc:
        logger.warning("Unable to open persistence database at %s", DB_PATH, exc_info=exc)
        raise PersistenceError(f"Unable to open persistence database at {DB_PATH}") from exc
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    from . import schema

    schema.initialize_schema(conn)
    return conn
