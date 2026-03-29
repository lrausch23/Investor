from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime, timedelta, timezone
import logging
from pathlib import Path
from dataclasses import asdict, is_dataclass
from typing import Any

from .exceptions import DuplicateThemeError, PersistenceError
from .logging_config import setup_regime_logging

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
    "meta_labeler_score": "REAL",
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


def _ensure_column(conn: sqlite3.Connection, table: str, column: str, ddl: str) -> None:
    columns = {
        str(row["name"])
        for row in conn.execute(f"PRAGMA table_info({table})").fetchall()
    }
    if column not in columns:
        logger.info("Adding missing persistence column %s.%s", table, column)
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")


def _ensure_transition_schema(conn: sqlite3.Connection) -> None:
    for column, ddl in _TRANSITION_COLUMNS.items():
        _ensure_column(conn, "regime_change_history", column, ddl)


def _ensure_paper_schema(conn: sqlite3.Connection) -> None:
    for column, ddl in _PAPER_PORTFOLIO_COLUMNS.items():
        _ensure_column(conn, "paper_portfolio", column, ddl)
    for column, ddl in _PAPER_TRADE_PLAN_COLUMNS.items():
        _ensure_column(conn, "paper_trade_plan", column, ddl)
    try:
        conn.execute("ALTER TABLE daily_snapshot ADD COLUMN drawdown_pct REAL")
    except Exception:
        pass
    try:
        conn.execute("ALTER TABLE daily_snapshot ADD COLUMN regime_exposure_json TEXT")
    except Exception:
        pass


def _create_alert_log_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS alert_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            alert_type TEXT NOT NULL CHECK (
                alert_type IN (
                    'regime_change', 'risk_spike', 'signal_change', 'stop_proximity',
                    'daily_loss_breach', 'meta_labeler_veto', 'vix_freeze', 'vix_resume',
                    'execution_error', 'connection_lost', 'connection_restored',
                    'drawdown_breach', 'concentration_breach', 'ml_accuracy_drift',
                    'capital_ceiling_breach', 'wash_sale_block', 'data_validation_failed', 'test'
                )
            ),
            severity TEXT NOT NULL DEFAULT 'info' CHECK (severity IN ('info', 'warning', 'critical')),
            ticker TEXT,
            portfolio_id INTEGER,
            title TEXT NOT NULL,
            message TEXT NOT NULL DEFAULT '',
            data_json TEXT,
            acknowledged INTEGER NOT NULL DEFAULT 0,
            acknowledged_at TEXT,
            created_at TEXT NOT NULL
        )
        """
    )


def _create_paper_trade_plan_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS paper_trade_plan (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            portfolio_id INTEGER NOT NULL REFERENCES paper_portfolio(id) ON DELETE CASCADE,
            theme_id INTEGER REFERENCES investment_theme(id) ON DELETE SET NULL,
            ticker TEXT NOT NULL,
            action TEXT NOT NULL CHECK (action IN ('Buy', 'Sell')),
            quantity REAL NOT NULL,
            proposed_price REAL,
            rationale TEXT NOT NULL DEFAULT '',
            regime_label TEXT,
            regime_probability REAL,
            crowd_score INTEGER,
            source TEXT NOT NULL DEFAULT 'discovery'
                CHECK (source IN ('discovery', 'exit_signal', 'manual', 'rebalance', 'holdings')),
            status TEXT NOT NULL DEFAULT 'Pending'
                CHECK (status IN ('Pending', 'Approved', 'Rejected', 'Modified', 'Submitted', 'Partially Filled', 'Executed', 'Cancelled', 'Expired')),
            reviewed_at TEXT,
            executed_at TEXT,
            execution_price REAL,
            broker_order_id TEXT,
            broker_status TEXT,
            filled_quantity REAL NOT NULL DEFAULT 0,
            meta_labeler_score REAL,
            notes TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )


def _create_paper_tax_lot_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS paper_tax_lot (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            portfolio_id INTEGER NOT NULL REFERENCES paper_portfolio(id) ON DELETE CASCADE,
            position_id INTEGER REFERENCES paper_position(id) ON DELETE SET NULL,
            ticker TEXT NOT NULL,
            quantity REAL NOT NULL,
            remaining_quantity REAL NOT NULL,
            cost_basis_per_share REAL NOT NULL,
            acquisition_date TEXT NOT NULL,
            closed_date TEXT,
            exit_price REAL,
            realized_pnl REAL,
            gain_loss_term TEXT CHECK (gain_loss_term IN ('ST_GAIN', 'ST_LOSS', 'LT_GAIN', 'LT_LOSS')),
            status TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'partial', 'closed')),
            notes TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_paper_tax_lot_portfolio_ticker
        ON paper_tax_lot(portfolio_id, ticker)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_paper_tax_lot_position
        ON paper_tax_lot(position_id)
        """
    )


def _create_wash_sale_restricted_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS wash_sale_restricted (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            portfolio_id INTEGER NOT NULL REFERENCES paper_portfolio(id) ON DELETE CASCADE,
            ticker TEXT NOT NULL,
            loss_sale_date TEXT NOT NULL,
            restriction_expires TEXT NOT NULL,
            loss_amount REAL NOT NULL DEFAULT 0,
            source_lot_id INTEGER REFERENCES paper_tax_lot(id) ON DELETE SET NULL,
            active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_wash_sale_restricted_active
        ON wash_sale_restricted(portfolio_id, ticker, active, restriction_expires)
        """
    )


def _create_order_audit_trail_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS order_audit_trail (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id TEXT NOT NULL,
            portfolio_id INTEGER NOT NULL,
            event_type TEXT NOT NULL CHECK (
                event_type IN (
                    'created', 'guardrail_check', 'guardrail_blocked', 'submitted', 'filled',
                    'partially_filled', 'rejected', 'cancelled', 'expired', 'error', 'auto_approved'
                )
            ),
            ticker TEXT NOT NULL,
            action TEXT,
            quantity REAL,
            price REAL,
            actor TEXT NOT NULL DEFAULT 'user' CHECK (actor IN ('user', 'system', 'scheduler')),
            details TEXT NOT NULL DEFAULT '',
            guardrail_result TEXT,
            created_at TEXT NOT NULL
        )
        """
    )


def _migrate_audit_event_type_check(conn: sqlite3.Connection) -> None:
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'order_audit_trail'"
    ).fetchone()
    create_sql = str(row["sql"] or "") if row else ""
    if "'auto_approved'" in create_sql:
        return
    if not _table_exists(conn, "order_audit_trail"):
        return
    logger.info("Migrating order_audit_trail event_type CHECK to include 'auto_approved'")
    conn.execute("ALTER TABLE order_audit_trail RENAME TO _order_audit_trail_old")
    _create_order_audit_trail_table(conn)
    old_columns = [str(col["name"]) for col in conn.execute("PRAGMA table_info(_order_audit_trail_old)").fetchall()]
    new_columns = [str(col["name"]) for col in conn.execute("PRAGMA table_info(order_audit_trail)").fetchall()]
    common_columns = [column for column in new_columns if column in old_columns]
    columns_sql = ", ".join(common_columns)
    conn.execute(
        f"INSERT INTO order_audit_trail ({columns_sql}) SELECT {columns_sql} FROM _order_audit_trail_old"
    )
    conn.execute("DROP TABLE _order_audit_trail_old")


def _migrate_alert_log_type_check(conn: sqlite3.Connection) -> None:
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'alert_log'"
    ).fetchone()
    create_sql = str(row["sql"] or "") if row else ""
    if "'wash_sale_block'" in create_sql and "'data_validation_failed'" in create_sql:
        return
    if not _table_exists(conn, "alert_log"):
        return
    logger.info("Migrating alert_log alert_type CHECK to include 'wash_sale_block'")
    conn.execute("ALTER TABLE alert_log RENAME TO _alert_log_old")
    _create_alert_log_table(conn)
    old_columns = [str(col["name"]) for col in conn.execute("PRAGMA table_info(_alert_log_old)").fetchall()]
    new_columns = [str(col["name"]) for col in conn.execute("PRAGMA table_info(alert_log)").fetchall()]
    common_columns = [column for column in new_columns if column in old_columns]
    columns_sql = ", ".join(common_columns)
    conn.execute(
        f"INSERT INTO alert_log ({columns_sql}) SELECT {columns_sql} FROM _alert_log_old"
    )
    conn.execute("DROP TABLE _alert_log_old")


def _migrate_trade_plan_source_check(conn: sqlite3.Connection) -> None:
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'paper_trade_plan'"
    ).fetchone()
    create_sql = str(row["sql"] or "") if row else ""
    if "'holdings'" in create_sql:
        return
    if not _table_exists(conn, "paper_trade_plan"):
        return
    logger.info("Migrating paper_trade_plan source CHECK constraint to include 'holdings'")
    conn.execute("ALTER TABLE paper_trade_plan RENAME TO _paper_trade_plan_old")
    _create_paper_trade_plan_table(conn)
    old_columns = [str(col["name"]) for col in conn.execute("PRAGMA table_info(_paper_trade_plan_old)").fetchall()]
    new_columns = [str(col["name"]) for col in conn.execute("PRAGMA table_info(paper_trade_plan)").fetchall()]
    common_columns = [column for column in new_columns if column in old_columns]
    columns_sql = ", ".join(common_columns)
    conn.execute(
        f"INSERT INTO paper_trade_plan ({columns_sql}) SELECT {columns_sql} FROM _paper_trade_plan_old"
    )
    conn.execute("DROP TABLE _paper_trade_plan_old")


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table,),
    ).fetchone()
    return row is not None


def _migrate_legacy_theses(conn: sqlite3.Connection) -> None:
    if not _table_exists(conn, "ticker_thesis"):
        return
    legacy_rows = conn.execute("SELECT ticker, thesis FROM ticker_thesis ORDER BY ticker ASC").fetchall()
    if not legacy_rows:
        return
    theme_count = int(conn.execute("SELECT COUNT(*) FROM investment_theme").fetchone()[0] or 0)
    now = datetime.now(timezone.utc).isoformat()
    if theme_count == 0:
        conn.execute(
            """
            INSERT INTO investment_theme (name, narrative, conviction, status, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            ("General", "Migrated from legacy per-ticker theses", 3, "Active", now, now),
        )
    theme_row = conn.execute("SELECT id FROM investment_theme WHERE name = ?", ("General",)).fetchone()
    if theme_row is None:
        return
    theme_id = int(theme_row["id"])
    for row in legacy_rows:
        conn.execute(
            """
            INSERT INTO theme_ticker (
                theme_id, ticker, role, rationale, time_horizon, added_at, updated_at
            )
            VALUES (?, ?, 'Core', ?, 'strategic', ?, ?)
            ON CONFLICT(theme_id, ticker) DO UPDATE SET
                rationale = excluded.rationale,
                updated_at = excluded.updated_at
            """,
            (theme_id, str(row["ticker"]).upper(), str(row["thesis"] or ""), now, now),
        )
    conn.execute("DROP TABLE IF EXISTS ticker_thesis")


def _connect() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    try:
        conn = sqlite3.connect(DB_PATH)
    except sqlite3.Error as exc:
        logger.warning("Unable to open persistence database at %s", DB_PATH, exc_info=exc)
        raise PersistenceError(f"Unable to open persistence database at {DB_PATH}") from exc
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ticker_thesis (
            ticker TEXT PRIMARY KEY,
            thesis TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS investment_theme (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            narrative TEXT NOT NULL DEFAULT '',
            sector_hint TEXT NOT NULL DEFAULT '',
            conviction INTEGER NOT NULL DEFAULT 3 CHECK (conviction BETWEEN 1 AND 5),
            status TEXT NOT NULL DEFAULT 'Active' CHECK (status IN ('Active', 'Monitoring', 'Closed')),
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    try:
        conn.execute(
            "ALTER TABLE investment_theme ADD COLUMN sector_hint TEXT NOT NULL DEFAULT ''"
        )
    except Exception:
        pass
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS theme_ticker (
            theme_id INTEGER NOT NULL REFERENCES investment_theme(id) ON DELETE CASCADE,
            ticker TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT 'Core' CHECK (role IN ('Core', 'Critical-Path', 'Speculative')),
            rationale TEXT NOT NULL DEFAULT '',
            entry_price REAL,
            target_price REAL,
            stop_price REAL,
            time_horizon TEXT NOT NULL DEFAULT 'strategic' CHECK (time_horizon IN ('trade', 'tactical', 'strategic')),
            added_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (theme_id, ticker)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS regime_events (
            ticker TEXT PRIMARY KEY,
            current_label TEXT NOT NULL,
            current_state_id INTEGER NOT NULL,
            changed_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS regime_change_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            previous_label TEXT,
            current_label TEXT NOT NULL,
            current_state_id INTEGER NOT NULL,
            changed_at TEXT NOT NULL,
            price_at_change REAL,
            return_5d REAL,
            return_10d REAL,
            return_21d REAL,
            outcome_updated_at TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_regime_change_history_ticker_date
        ON regime_change_history(ticker, changed_at)
        """
    )
    _ensure_transition_schema(conn)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sentiment_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            score INTEGER NOT NULL,
            sentiment TEXT NOT NULL,
            catalyst_count INTEGER NOT NULL,
            recorded_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_sentiment_ticker_date
        ON sentiment_history(ticker, recorded_at)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS signal_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            snapshot_date TEXT NOT NULL,
            action TEXT NOT NULL,
            regime_label TEXT NOT NULL,
            regime_probability REAL NOT NULL,
            composite_strength REAL NOT NULL,
            benchmark TEXT,
            current_price REAL NOT NULL,
            entry_price REAL,
            exit_price REAL,
            stop_price REAL,
            risk_reward_ratio REAL,
            timeframe_days INTEGER NOT NULL,
            return_1w REAL,
            return_1m REAL,
            return_3m REAL,
            hit_1w INTEGER,
            hit_1m INTEGER,
            hit_3m INTEGER,
            updated_at TEXT NOT NULL,
            UNIQUE(ticker, snapshot_date)
        )
        """
    )
    _create_order_audit_trail_table(conn)
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_audit_trail_order_id
        ON order_audit_trail(order_id)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_audit_trail_portfolio_date
        ON order_audit_trail(portfolio_id, created_at)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_audit_trail_ticker
        ON order_audit_trail(ticker)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_signal_snapshots_ticker_date
        ON signal_snapshots(ticker, snapshot_date)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sector_cache (
            ticker TEXT PRIMARY KEY,
            sector TEXT NOT NULL,
            cached_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS earnings_cache (
            ticker TEXT PRIMARY KEY,
            earnings_date TEXT,
            cached_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS regime_settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL DEFAULT '',
            updated_at TEXT NOT NULL
        )
        """
    )
    _create_alert_log_table(conn)
    _create_paper_tax_lot_table(conn)
    _create_wash_sale_restricted_table(conn)
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_alert_log_created
        ON alert_log(created_at)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_alert_log_unacknowledged
        ON alert_log(acknowledged, created_at)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS meta_labeler_training_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            version INTEGER NOT NULL,
            ticker TEXT NOT NULL,
            model_path TEXT NOT NULL,
            accuracy REAL,
            precision_score REAL,
            recall REAL,
            f1 REAL,
            train_samples INTEGER,
            test_samples INTEGER,
            positive_rate_train REAL,
            positive_rate_test REAL,
            avg_probability_test REAL,
            feature_importances TEXT NOT NULL DEFAULT '{}',
            config_json TEXT NOT NULL DEFAULT '{}',
            trained_at TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'superseded', 'rolled_back')),
            notes TEXT NOT NULL DEFAULT ''
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_training_log_version
        ON meta_labeler_training_log (version)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_training_log_status
        ON meta_labeler_training_log (status)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS theme_supply_chain (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            theme_id INTEGER NOT NULL REFERENCES investment_theme(id) ON DELETE CASCADE,
            layer TEXT NOT NULL,
            description TEXT NOT NULL DEFAULT '',
            example_companies TEXT NOT NULL DEFAULT '',
            generated_at TEXT NOT NULL,
            UNIQUE(theme_id, layer)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS discovery_watchlist (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            theme_id INTEGER NOT NULL REFERENCES investment_theme(id) ON DELETE CASCADE,
            ticker TEXT NOT NULL,
            company_name TEXT NOT NULL DEFAULT '',
            supply_chain_layer TEXT NOT NULL DEFAULT '',
            discovery_rationale TEXT NOT NULL DEFAULT '',
            suggested_role TEXT NOT NULL DEFAULT 'Critical-Path'
                CHECK (suggested_role IN ('Core', 'Critical-Path', 'Speculative')),
            suggested_entry_price REAL,
            suggested_stop_price REAL,
            crowd_score INTEGER DEFAULT 50 CHECK (crowd_score BETWEEN 0 AND 100),
            crowd_details TEXT NOT NULL DEFAULT '',
            regime_label TEXT,
            regime_probability REAL,
            status TEXT NOT NULL DEFAULT 'Watching'
                CHECK (status IN ('Watching', 'Entry Signal', 'Added', 'Passed', 'Expired')),
            discovered_at TEXT NOT NULL,
            last_scanned_at TEXT NOT NULL,
            entry_signal_at TEXT,
            notes TEXT NOT NULL DEFAULT '',
            UNIQUE(theme_id, ticker)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS paper_portfolio (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            starting_budget REAL NOT NULL DEFAULT 100000.0,
            current_cash REAL NOT NULL DEFAULT 100000.0,
            broker_type TEXT NOT NULL DEFAULT 'paper',
            status TEXT NOT NULL DEFAULT 'Active'
                CHECK (status IN ('Active', 'Paused', 'Closed')),
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS paper_position (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            portfolio_id INTEGER NOT NULL REFERENCES paper_portfolio(id) ON DELETE CASCADE,
            theme_id INTEGER REFERENCES investment_theme(id) ON DELETE SET NULL,
            ticker TEXT NOT NULL,
            side TEXT NOT NULL DEFAULT 'long' CHECK (side IN ('long', 'short')),
            quantity REAL NOT NULL,
            entry_price REAL NOT NULL,
            entry_date TEXT NOT NULL,
            exit_price REAL,
            exit_date TEXT,
            exit_reason TEXT,
            stop_price REAL,
            target_price REAL,
            role TEXT NOT NULL DEFAULT 'Critical-Path'
                CHECK (role IN ('Core', 'Critical-Path', 'Speculative')),
            status TEXT NOT NULL DEFAULT 'Open'
                CHECK (status IN ('Open', 'Closed')),
            realized_pnl REAL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(portfolio_id, ticker, entry_date)
        )
        """
    )
    _create_paper_trade_plan_table(conn)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS daily_snapshot (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            portfolio_id INTEGER NOT NULL,
            snapshot_date TEXT NOT NULL,
            equity REAL NOT NULL,
            cash REAL NOT NULL,
            market_value REAL NOT NULL,
            realized_pnl REAL NOT NULL DEFAULT 0,
            unrealized_pnl REAL NOT NULL DEFAULT 0,
            position_count INTEGER NOT NULL DEFAULT 0,
            trades_today INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            UNIQUE(portfolio_id, snapshot_date)
        )
        """
    )
    _ensure_paper_schema(conn)
    _migrate_trade_plan_source_check(conn)
    _migrate_audit_event_type_check(conn)
    _migrate_alert_log_type_check(conn)
    _migrate_legacy_theses(conn)
    conn.execute(
        """
        UPDATE investment_theme
        SET narrative = ?,
            sector_hint = ?,
            updated_at = ?
        WHERE name = 'Generative AI'
          AND (narrative IS NULL OR narrative = '')
        """,
        (
            "Companies building, deploying, or enabling generative AI models and applications — "
            "foundation model providers, inference infrastructure, AI-native software, "
            "enterprise AI platforms, and the semiconductor and cloud compute supply chain "
            "that powers large-scale model training and inference.",
            "Artificial Intelligence / Machine Learning",
            datetime.now(timezone.utc).isoformat(),
        ),
    )
    return conn


def upsert_thesis(ticker: str, thesis: str | None) -> str | None:
    with _connect() as conn:
        theme = conn.execute("SELECT id FROM investment_theme WHERE name = ?", ("General",)).fetchone()
        now = datetime.now(timezone.utc).isoformat()
        if theme is None:
            conn.execute(
                """
                INSERT INTO investment_theme (name, narrative, conviction, status, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("General", "Migrated from legacy per-ticker theses", 3, "Active", now, now),
            )
            theme = conn.execute("SELECT id FROM investment_theme WHERE name = ?", ("General",)).fetchone()
        theme_id = int(theme["id"])
        row = conn.execute(
            "SELECT rationale FROM theme_ticker WHERE theme_id = ? AND ticker = ?",
            (theme_id, ticker.upper()),
        ).fetchone()
        existing = str(row["rationale"]) if row and row["rationale"] else None
        if thesis is None:
            return existing
        conn.execute(
            """
            INSERT INTO theme_ticker (theme_id, ticker, role, rationale, time_horizon, added_at, updated_at)
            VALUES (?, ?, 'Core', ?, 'strategic', ?, ?)
            ON CONFLICT(theme_id, ticker) DO UPDATE SET rationale = excluded.rationale, updated_at = excluded.updated_at
            """,
            (theme_id, ticker.upper(), thesis.strip(), now, now),
        )
        return thesis.strip()


def delete_thesis(ticker: str) -> bool:
    with _connect() as conn:
        theme = conn.execute("SELECT id FROM investment_theme WHERE name = ?", ("General",)).fetchone()
        if theme is None:
            return False
        cursor = conn.execute("DELETE FROM theme_ticker WHERE theme_id = ? AND ticker = ?", (int(theme["id"]), ticker.upper()))
        return bool(cursor.rowcount)


def list_theses() -> list[dict[str, str]]:
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT ticker, rationale AS thesis, updated_at
            FROM theme_ticker
            WHERE theme_id = (SELECT id FROM investment_theme WHERE name = 'General')
            ORDER BY ticker ASC
            """
        ).fetchall()
        return [dict(row) for row in rows]


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
        raise ValueError(f"Invalid mode: {mode}. Must be one of {OPERATING_MODES}")
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
        raise ValueError(f"Invalid lot selection method: {method}")
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


def create_theme(name: str, narrative: str = "", conviction: int = 3, status: str = "Active", sector_hint: str = "") -> dict[str, Any]:
    now = datetime.now(timezone.utc).isoformat()
    with _connect() as conn:
        try:
            cursor = conn.execute(
                """
                INSERT INTO investment_theme (name, narrative, sector_hint, conviction, status, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (name.strip(), narrative.strip(), sector_hint.strip(), int(conviction), status, now, now),
            )
        except sqlite3.IntegrityError as exc:
            if "UNIQUE constraint" in str(exc):
                raise DuplicateThemeError(f"A theme named '{name.strip()}' already exists.") from exc
            raise
        theme_id = int(cursor.lastrowid)
    return get_theme(theme_id) or {}


def update_theme(
    theme_id: int,
    *,
    name: str | None = None,
    narrative: str | None = None,
    sector_hint: str | None = None,
    conviction: int | None = None,
    status: str | None = None,
) -> dict[str, Any] | None:
    updates: list[str] = []
    params: list[Any] = []
    if name is not None:
        updates.append("name = ?")
        params.append(name.strip())
    if narrative is not None:
        updates.append("narrative = ?")
        params.append(narrative.strip())
    if sector_hint is not None:
        updates.append("sector_hint = ?")
        params.append(sector_hint.strip())
    if conviction is not None:
        updates.append("conviction = ?")
        params.append(int(conviction))
    if status is not None:
        updates.append("status = ?")
        params.append(status)
    if not updates:
        return get_theme(theme_id)
    updates.append("updated_at = ?")
    params.append(datetime.now(timezone.utc).isoformat())
    params.append(int(theme_id))
    with _connect() as conn:
        try:
            cursor = conn.execute(f"UPDATE investment_theme SET {', '.join(updates)} WHERE id = ?", params)
        except sqlite3.IntegrityError as exc:
            if "UNIQUE constraint" in str(exc):
                raise DuplicateThemeError("A theme with that name already exists.") from exc
            raise
        if not cursor.rowcount:
            return None
    return get_theme(theme_id)


def delete_theme(theme_id: int) -> bool:
    with _connect() as conn:
        cursor = conn.execute("DELETE FROM investment_theme WHERE id = ?", (int(theme_id),))
        return bool(cursor.rowcount)


def get_theme_tickers(theme_id: int) -> list[dict[str, Any]]:
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT theme_id, ticker, role, rationale, entry_price, target_price, stop_price, time_horizon, added_at, updated_at
            FROM theme_ticker
            WHERE theme_id = ?
            ORDER BY ticker ASC
            """,
            (int(theme_id),),
        ).fetchall()
    return [dict(row) for row in rows]


def get_theme(theme_id: int) -> dict[str, Any] | None:
    with _connect() as conn:
        row = conn.execute(
            """
            SELECT id, name, narrative, sector_hint, conviction, status, created_at, updated_at
            FROM investment_theme
            WHERE id = ?
            """,
            (int(theme_id),),
        ).fetchone()
    if row is None:
        return None
    theme = dict(row)
    theme["tickers"] = get_theme_tickers(int(theme_id))
    return theme


def list_themes(include_closed: bool = False) -> list[dict[str, Any]]:
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT id, name, narrative, sector_hint, conviction, status, created_at, updated_at
            FROM investment_theme
            WHERE (? = 1 OR status != 'Closed')
            ORDER BY name ASC
            """,
            (1 if include_closed else 0,),
        ).fetchall()
    themes = [dict(row) for row in rows]
    for theme in themes:
        theme["tickers"] = get_theme_tickers(int(theme["id"]))
    return themes


def add_ticker_to_theme(
    theme_id: int,
    ticker: str,
    *,
    role: str = "Core",
    rationale: str = "",
    entry_price: float | None = None,
    target_price: float | None = None,
    stop_price: float | None = None,
    time_horizon: str = "strategic",
) -> dict[str, Any]:
    now = datetime.now(timezone.utc).isoformat()
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO theme_ticker (
                theme_id, ticker, role, rationale, entry_price, target_price, stop_price, time_horizon, added_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(theme_id, ticker) DO UPDATE SET
                role = excluded.role,
                rationale = excluded.rationale,
                entry_price = excluded.entry_price,
                target_price = excluded.target_price,
                stop_price = excluded.stop_price,
                time_horizon = excluded.time_horizon,
                updated_at = excluded.updated_at
            """,
            (
                int(theme_id),
                ticker.upper(),
                role,
                rationale.strip(),
                float(entry_price) if entry_price is not None else None,
                float(target_price) if target_price is not None else None,
                float(stop_price) if stop_price is not None else None,
                time_horizon,
                now,
                now,
            ),
        )
    return next((item for item in get_theme_tickers(theme_id) if str(item["ticker"]).upper() == ticker.upper()), {})


def remove_ticker_from_theme(theme_id: int, ticker: str) -> bool:
    with _connect() as conn:
        cursor = conn.execute("DELETE FROM theme_ticker WHERE theme_id = ? AND ticker = ?", (int(theme_id), ticker.upper()))
        return bool(cursor.rowcount)


def update_ticker_in_theme(theme_id: int, ticker: str, **fields: Any) -> dict[str, Any] | None:
    updates: list[str] = []
    params: list[Any] = []
    for key in ("role", "rationale", "entry_price", "target_price", "stop_price", "time_horizon"):
        if key in fields:
            value = fields[key]
            if key in {"entry_price", "target_price", "stop_price"} and value is not None:
                value = float(value)
            elif key == "rationale" and value is not None:
                value = str(value).strip()
            updates.append(f"{key} = ?")
            params.append(value)
    if not updates:
        return next((item for item in get_theme_tickers(theme_id) if str(item["ticker"]).upper() == ticker.upper()), None)
    updates.append("updated_at = ?")
    params.append(datetime.now(timezone.utc).isoformat())
    params.extend([int(theme_id), ticker.upper()])
    with _connect() as conn:
        cursor = conn.execute(f"UPDATE theme_ticker SET {', '.join(updates)} WHERE theme_id = ? AND ticker = ?", params)
        if not cursor.rowcount:
            return None
    return next((item for item in get_theme_tickers(theme_id) if str(item["ticker"]).upper() == ticker.upper()), None)


def get_ticker_themes(ticker: str) -> list[dict[str, Any]]:
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT
                tt.theme_id,
                it.name AS theme_name,
                it.narrative,
                it.conviction,
                it.status,
                tt.ticker,
                tt.role,
                tt.rationale,
                tt.entry_price,
                tt.target_price,
                tt.stop_price,
                tt.time_horizon,
                tt.added_at,
                tt.updated_at
            FROM theme_ticker tt
            JOIN investment_theme it ON it.id = tt.theme_id
            WHERE tt.ticker = ?
            ORDER BY it.name ASC
            """,
            (ticker.upper(),),
        ).fetchall()
    return [dict(row) for row in rows]


def get_theme_health_data(theme_id: int) -> dict[str, Any]:
    theme = get_theme(theme_id)
    if theme is None:
        return {}
    return {"theme": theme, "tickers": theme.get("tickers", [])}


def save_supply_chain_layers(theme_id: int, layers: list[dict]) -> list[dict]:
    now = datetime.now(timezone.utc).isoformat()
    normalized_layers: list[tuple[str, str, str]] = []
    seen_layers: set[str] = set()
    for layer in layers:
        name = str((layer or {}).get("layer") or "").strip()
        if not name or name in seen_layers:
            continue
        seen_layers.add(name)
        normalized_layers.append(
            (
                name[:100],
                str((layer or {}).get("description") or "").strip()[:1000],
                str((layer or {}).get("example_companies") or "").strip()[:500],
            )
        )
    with _connect() as conn:
        conn.execute("DELETE FROM theme_supply_chain WHERE theme_id = ?", (int(theme_id),))
        for layer_name, description, companies in normalized_layers:
            conn.execute(
                """
                INSERT INTO theme_supply_chain (theme_id, layer, description, example_companies, generated_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (int(theme_id), layer_name, description, companies, now),
            )
    return get_supply_chain(theme_id)


def get_supply_chain(theme_id: int) -> list[dict[str, Any]]:
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT id, theme_id, layer, description, example_companies, generated_at
            FROM theme_supply_chain
            WHERE theme_id = ?
            ORDER BY layer ASC
            """,
            (int(theme_id),),
        ).fetchall()
    return [dict(row) for row in rows]


def delete_supply_chain(theme_id: int) -> int:
    with _connect() as conn:
        cursor = conn.execute("DELETE FROM theme_supply_chain WHERE theme_id = ?", (int(theme_id),))
        return int(cursor.rowcount or 0)


def upsert_watchlist_candidate(
    theme_id: int,
    ticker: str,
    *,
    company_name: str = "",
    supply_chain_layer: str = "",
    discovery_rationale: str = "",
    suggested_role: str = "Critical-Path",
    suggested_entry_price: float | None = None,
    suggested_stop_price: float | None = None,
    crowd_score: int = 50,
    crowd_details: str = "",
    regime_label: str | None = None,
    regime_probability: float | None = None,
    status: str = "Watching",
    notes: str = "",
) -> dict[str, Any]:
    now = datetime.now(timezone.utc).isoformat()
    ticker_key = ticker.upper()
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO discovery_watchlist (
                theme_id, ticker, company_name, supply_chain_layer, discovery_rationale,
                suggested_role, suggested_entry_price, suggested_stop_price, crowd_score,
                crowd_details, regime_label, regime_probability, status, discovered_at,
                last_scanned_at, notes
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(theme_id, ticker) DO UPDATE SET
                company_name = excluded.company_name,
                supply_chain_layer = excluded.supply_chain_layer,
                discovery_rationale = excluded.discovery_rationale,
                suggested_role = excluded.suggested_role,
                suggested_entry_price = excluded.suggested_entry_price,
                suggested_stop_price = excluded.suggested_stop_price,
                crowd_score = excluded.crowd_score,
                crowd_details = excluded.crowd_details,
                regime_label = excluded.regime_label,
                regime_probability = excluded.regime_probability,
                status = CASE
                    WHEN discovery_watchlist.status IN ('Added', 'Passed') THEN discovery_watchlist.status
                    ELSE excluded.status
                END,
                last_scanned_at = excluded.last_scanned_at,
                notes = CASE
                    WHEN discovery_watchlist.notes != '' THEN discovery_watchlist.notes
                    ELSE excluded.notes
                END
            """,
            (
                int(theme_id),
                ticker_key,
                company_name.strip(),
                supply_chain_layer.strip(),
                discovery_rationale.strip(),
                suggested_role,
                float(suggested_entry_price) if suggested_entry_price is not None else None,
                float(suggested_stop_price) if suggested_stop_price is not None else None,
                max(0, min(100, int(crowd_score))),
                crowd_details,
                regime_label,
                float(regime_probability) if regime_probability is not None else None,
                status,
                now,
                now,
                notes.strip(),
            ),
        )
    row = get_watchlist_by_ticker(ticker_key)
    return next((item for item in row if int(item["theme_id"]) == int(theme_id)), {})


def get_watchlist(
    theme_id: int | None = None,
    status: str | list[str] | None = None,
    max_crowd_score: int | None = None,
) -> list[dict[str, Any]]:
    query = [
        """
        SELECT
            dw.*,
            it.name AS theme_name,
            it.conviction AS theme_conviction,
            it.status AS theme_status
        FROM discovery_watchlist dw
        JOIN investment_theme it ON it.id = dw.theme_id
        WHERE 1 = 1
        """
    ]
    params: list[Any] = []
    if theme_id is not None:
        query.append("AND dw.theme_id = ?")
        params.append(int(theme_id))
    if isinstance(status, str) and status:
        query.append("AND dw.status = ?")
        params.append(status)
    elif isinstance(status, list) and status:
        placeholders = ", ".join("?" for _ in status)
        query.append(f"AND dw.status IN ({placeholders})")
        params.extend(status)
    else:
        query.append("AND dw.status NOT IN ('Expired', 'Passed')")
    if max_crowd_score is not None:
        query.append("AND dw.crowd_score <= ?")
        params.append(int(max_crowd_score))
    query.append("ORDER BY CASE dw.status WHEN 'Entry Signal' THEN 0 WHEN 'Watching' THEN 1 WHEN 'Added' THEN 2 WHEN 'Passed' THEN 3 ELSE 4 END, dw.crowd_score ASC, dw.ticker ASC")
    with _connect() as conn:
        rows = conn.execute("\n".join(query), params).fetchall()
    return [dict(row) for row in rows]


def get_watchlist_entry(watchlist_id: int) -> dict[str, Any] | None:
    with _connect() as conn:
        row = conn.execute(
            """
            SELECT
                dw.*,
                it.name AS theme_name,
                it.conviction AS theme_conviction,
                it.status AS theme_status
            FROM discovery_watchlist dw
            JOIN investment_theme it ON it.id = dw.theme_id
            WHERE dw.id = ?
            """,
            (int(watchlist_id),),
        ).fetchone()
    return dict(row) if row else None


def update_watchlist_status(watchlist_id: int, status: str, **kwargs: Any) -> dict[str, Any] | None:
    updates = ["status = ?"]
    params: list[Any] = [status]
    for key in ("notes", "regime_label", "regime_probability", "crowd_score", "crowd_details", "suggested_entry_price", "suggested_stop_price", "last_scanned_at"):
        if key in kwargs:
            updates.append(f"{key} = ?")
            params.append(kwargs[key])
    if status == "Entry Signal":
        updates.append("entry_signal_at = ?")
        params.append(kwargs.get("entry_signal_at") or datetime.now(timezone.utc).isoformat())
    params.append(int(watchlist_id))
    with _connect() as conn:
        cursor = conn.execute(
            f"UPDATE discovery_watchlist SET {', '.join(updates)} WHERE id = ?",
            params,
        )
        if not cursor.rowcount:
            return None
    return get_watchlist_entry(watchlist_id)


def get_watchlist_by_ticker(ticker: str) -> list[dict[str, Any]]:
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT
                dw.*,
                it.name AS theme_name,
                it.conviction AS theme_conviction,
                it.status AS theme_status
            FROM discovery_watchlist dw
            JOIN investment_theme it ON it.id = dw.theme_id
            WHERE dw.ticker = ?
            ORDER BY it.name ASC
            """,
            (ticker.upper(),),
        ).fetchall()
    return [dict(row) for row in rows]


def delete_watchlist_entry(watchlist_id: int) -> bool:
    with _connect() as conn:
        cursor = conn.execute("DELETE FROM discovery_watchlist WHERE id = ?", (int(watchlist_id),))
        return bool(cursor.rowcount)


def get_watchlist_stats() -> dict[str, Any]:
    with _connect() as conn:
        status_rows = conn.execute(
            """
            SELECT status, COUNT(*) AS count
            FROM discovery_watchlist
            GROUP BY status
            ORDER BY status ASC
            """
        ).fetchall()
        theme_rows = conn.execute(
            """
            SELECT it.name AS theme_name, COUNT(*) AS count, AVG(dw.crowd_score) AS avg_crowd_score
            FROM discovery_watchlist dw
            JOIN investment_theme it ON it.id = dw.theme_id
            GROUP BY dw.theme_id, it.name
            ORDER BY count DESC, it.name ASC
            """
        ).fetchall()
        summary = conn.execute(
            """
            SELECT COUNT(*) AS total, AVG(crowd_score) AS avg_crowd_score
            FROM discovery_watchlist
            """
        ).fetchone()
    return {
        "total": int(summary["total"] or 0) if summary else 0,
        "avg_crowd_score": float(summary["avg_crowd_score"]) if summary and summary["avg_crowd_score"] is not None else None,
        "by_status": {str(row["status"]): int(row["count"] or 0) for row in status_rows},
        "by_theme": [dict(row) for row in theme_rows],
    }


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
        raise ValueError("Invalid quantity_to_close for tax lot.")
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


def create_trade_plan(
    portfolio_id: int,
    ticker: str,
    action: str,
    quantity: float,
    rationale: str,
    theme_id: int | None = None,
    proposed_price: float | None = None,
    regime_label: str | None = None,
    regime_probability: float | None = None,
    crowd_score: int | None = None,
    source: str = "discovery",
    meta_labeler_score: float | None = None,
) -> dict[str, Any]:
    now = datetime.now(timezone.utc).isoformat()
    with _connect() as conn:
        cursor = conn.execute(
            """
            INSERT INTO paper_trade_plan (
                portfolio_id, theme_id, ticker, action, quantity, proposed_price, rationale,
                regime_label, regime_probability, crowd_score, source, status, meta_labeler_score, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'Pending', ?, ?, ?)
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
                float(meta_labeler_score) if meta_labeler_score is not None else None,
                now,
                now,
            ),
        )
        plan_id = int(cursor.lastrowid)
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


def get_paper_portfolio_summary(portfolio_id: int) -> dict[str, Any]:
    portfolio = get_paper_portfolio(portfolio_id)
    if portfolio is None:
        return {}
    open_positions = get_paper_positions(portfolio_id, status="Open")
    closed_positions = get_paper_positions(portfolio_id, status="Closed")
    prices: dict[str, float] = {}
    if open_positions:
        try:
            from .paper_trading import _batch_current_prices

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


def get_latest_regime_label(ticker: str, as_of: str) -> str | None:
    with _connect() as conn:
        row = conn.execute(
            """
            SELECT current_label
            FROM regime_change_history
            WHERE ticker = ? AND changed_at <= ?
            ORDER BY changed_at DESC
            LIMIT 1
            """,
            (str(ticker or "").upper(), str(as_of)),
        ).fetchone()
    return str(row["current_label"]) if row and row["current_label"] else None


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


def save_regime_event(ticker: str, label: str, state_id: int) -> dict[str, int | str | None]:
    now = datetime.now(timezone.utc)
    now_text = now.isoformat()
    with _connect() as conn:
        row = conn.execute(
            "SELECT current_label, current_state_id, changed_at FROM regime_events WHERE ticker = ?",
            (ticker.upper(),),
        ).fetchone()
        previous_label = row["current_label"] if row else None
        changed_at = row["changed_at"] if row else now_text

        if row and row["current_state_id"] == state_id:
            conn.execute(
                "UPDATE regime_events SET updated_at = ? WHERE ticker = ?",
                (now_text, ticker.upper()),
            )
        else:
            changed_at = now_text
            conn.execute(
                """
                INSERT INTO regime_events (ticker, current_label, current_state_id, changed_at, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(ticker) DO UPDATE SET
                    current_label = excluded.current_label,
                    current_state_id = excluded.current_state_id,
                    changed_at = excluded.changed_at,
                    updated_at = excluded.updated_at
                """,
                (ticker.upper(), label, state_id, changed_at, now_text),
            )
            if row:
                conn.execute(
                    """
                    INSERT INTO regime_change_history (ticker, previous_label, current_label, current_state_id, changed_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (ticker.upper(), previous_label, label, state_id, changed_at),
                )

    changed_dt = datetime.fromisoformat(changed_at)
    # Backwards-compatible persistence metadata only; regime_days from the HMM engine
    # is the authoritative regime age shown in the dashboard and CLI.
    days_in_regime = max(0, (now - changed_dt).days)
    return {"previous_label": previous_label, "days_in_regime": days_in_regime}


def save_regime_change_with_price(
    ticker: str,
    previous_label: str | None,
    current_label: str,
    state_id: int,
    price: float | None,
) -> int:
    now_text = datetime.now(timezone.utc).isoformat()
    with _connect() as conn:
        cursor = conn.execute(
            """
            INSERT INTO regime_change_history (
                ticker,
                previous_label,
                current_label,
                current_state_id,
                changed_at,
                price_at_change
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (ticker.upper(), previous_label, current_label, int(state_id), now_text, price),
        )
        return int(cursor.lastrowid)


def save_sentiment(ticker: str, score: int, sentiment: str, catalyst_count: int) -> None:
    now = datetime.now(timezone.utc).isoformat()
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO sentiment_history (ticker, score, sentiment, catalyst_count, recorded_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (ticker.upper(), int(score), sentiment, int(catalyst_count), now),
        )


def get_sentiment_history(ticker: str, days: int = 30) -> list[dict[str, int | str]]:
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT ticker, score, sentiment, catalyst_count, recorded_at
            FROM sentiment_history
            WHERE ticker = ?
              AND recorded_at >= datetime('now', ?)
            ORDER BY recorded_at ASC
            """,
            (ticker.upper(), f"-{int(days)} day"),
        ).fetchall()
    return [dict(row) for row in rows]


def get_recent_regime_changes(ticker: str, days: int = 7) -> list[dict[str, int | str | None]]:
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT ticker, previous_label, current_label, current_state_id, changed_at
            FROM regime_change_history
            WHERE ticker = ?
              AND changed_at >= datetime('now', ?)
            ORDER BY changed_at DESC
            """,
            (ticker.upper(), f"-{int(days)} day"),
        ).fetchall()
    return [dict(row) for row in rows]


def get_pending_transition_outcomes(
    *,
    lookback_days: int = 90,
    as_of: str | None = None,
) -> list[dict[str, Any]]:
    now_text = as_of or datetime.now(timezone.utc).isoformat()
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT id, ticker, changed_at, price_at_change
            FROM regime_change_history
            WHERE changed_at >= datetime(?, ?)
              AND (
                (return_5d IS NULL AND changed_at <= datetime(?, '-5 day'))
                OR (return_10d IS NULL AND changed_at <= datetime(?, '-10 day'))
                OR (return_21d IS NULL AND changed_at <= datetime(?, '-21 day'))
              )
            ORDER BY changed_at ASC
            """,
            (now_text, f"-{int(lookback_days)} day", now_text, now_text, now_text),
        ).fetchall()
    return [dict(row) for row in rows]


def update_transition_outcome(
    change_id: int,
    *,
    return_5d: float | None = None,
    return_10d: float | None = None,
    return_21d: float | None = None,
) -> None:
    with _connect() as conn:
        conn.execute(
            """
            UPDATE regime_change_history
            SET return_5d = COALESCE(?, return_5d),
                return_10d = COALESCE(?, return_10d),
                return_21d = COALESCE(?, return_21d),
                outcome_updated_at = ?
            WHERE id = ?
            """,
            (
                return_5d,
                return_10d,
                return_21d,
                datetime.now(timezone.utc).isoformat(),
                int(change_id),
            ),
        )


def get_transition_journal(ticker: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
    with _connect() as conn:
        if ticker:
            rows = conn.execute(
                """
                SELECT *
                FROM regime_change_history
                WHERE ticker = ?
                ORDER BY changed_at DESC
                LIMIT ?
                """,
                (ticker.upper(), int(limit)),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT *
                FROM regime_change_history
                ORDER BY changed_at DESC
                LIMIT ?
                """,
                (int(limit),),
            ).fetchall()
    return [dict(row) for row in rows]


def get_transition_statistics() -> dict[str, Any]:
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT
                COALESCE(previous_label, 'Unknown') AS previous_label,
                current_label,
                AVG(return_5d) AS avg_return_5d,
                AVG(return_10d) AS avg_return_10d,
                AVG(return_21d) AS avg_return_21d,
                AVG(CASE WHEN return_5d > 0 THEN 1.0 WHEN return_5d IS NULL THEN NULL ELSE 0.0 END) AS hit_rate_5d,
                AVG(CASE WHEN return_10d > 0 THEN 1.0 WHEN return_10d IS NULL THEN NULL ELSE 0.0 END) AS hit_rate_10d,
                AVG(CASE WHEN return_21d > 0 THEN 1.0 WHEN return_21d IS NULL THEN NULL ELSE 0.0 END) AS hit_rate_21d,
                COUNT(*) AS count
            FROM regime_change_history
            GROUP BY COALESCE(previous_label, 'Unknown'), current_label
            ORDER BY count DESC, previous_label ASC, current_label ASC
            """
        ).fetchall()
    pairs = []
    for row in rows:
        item = dict(row)
        item["transition"] = f"{item['previous_label']}→{item['current_label']}"
        pairs.append(item)
    return {"rows": pairs}


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
) -> None:
    now = datetime.now(timezone.utc).isoformat()
    logger.debug("Saving signal snapshot for %s at %s", ticker, snapshot_date)
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO signal_snapshots (
                ticker, snapshot_date, action, regime_label, regime_probability, composite_strength, benchmark,
                current_price, entry_price, exit_price, stop_price, risk_reward_ratio, timeframe_days, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                now,
            ),
        )


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


def get_cached_sector(ticker: str, *, max_age_days: int = _SECTOR_CACHE_TTL_DAYS) -> str | None:
    with _connect() as conn:
        row = conn.execute(
            """
            SELECT sector
            FROM sector_cache
            WHERE ticker = ?
              AND cached_at >= datetime('now', ?)
            """,
            (ticker.upper(), f"-{int(max_age_days)} day"),
        ).fetchone()
    return str(row["sector"]) if row and row["sector"] else None


def save_sector_cache(ticker: str, sector: str) -> None:
    now = datetime.now(timezone.utc).isoformat()
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO sector_cache (ticker, sector, cached_at)
            VALUES (?, ?, ?)
            ON CONFLICT(ticker) DO UPDATE SET
                sector = excluded.sector,
                cached_at = excluded.cached_at
            """,
            (ticker.upper(), sector, now),
        )


def get_cached_earnings_date(ticker: str, *, max_age_hours: int = _EARNINGS_CACHE_TTL_HOURS) -> str | None:
    with _connect() as conn:
        row = conn.execute(
            """
            SELECT earnings_date
            FROM earnings_cache
            WHERE ticker = ?
              AND cached_at >= datetime('now', ?)
            """,
            (ticker.upper(), f"-{int(max_age_hours)} hour"),
        ).fetchone()
    if row is None:
        return None
    return str(row["earnings_date"]) if row["earnings_date"] else None


def save_earnings_cache(ticker: str, earnings_date: str | None) -> None:
    now = datetime.now(timezone.utc).isoformat()
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO earnings_cache (ticker, earnings_date, cached_at)
            VALUES (?, ?, ?)
            ON CONFLICT(ticker) DO UPDATE SET
                earnings_date = excluded.earnings_date,
                cached_at = excluded.cached_at
            """,
            (ticker.upper(), earnings_date, now),
        )


def get_historical_regime_durations(ticker: str | None = None) -> dict[str, Any]:
    with _connect() as conn:
        if ticker:
            rows = conn.execute(
                """
                SELECT ticker, current_label, changed_at
                FROM regime_change_history
                WHERE ticker = ?
                ORDER BY changed_at ASC
                """,
                (ticker.upper(),),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT ticker, current_label, changed_at
                FROM regime_change_history
                ORDER BY ticker ASC, changed_at ASC
                """
            ).fetchall()

    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        item = dict(row)
        grouped.setdefault(str(item["ticker"]).upper(), []).append(item)

    result: dict[str, Any] = {}
    for ticker_key, ticker_rows in grouped.items():
        per_label: dict[str, list[float]] = {}
        for current, nxt in zip(ticker_rows, ticker_rows[1:]):
            label = str(current.get("current_label") or "")
            if not label:
                continue
            start = datetime.fromisoformat(str(current["changed_at"]))
            end = datetime.fromisoformat(str(nxt["changed_at"]))
            duration_days = max(0.0, (end - start).total_seconds() / 86400.0)
            per_label.setdefault(label, []).append(duration_days)
        label_stats: dict[str, Any] = {}
        for label, values in per_label.items():
            ordered = sorted(values)
            if not ordered:
                continue
            count = len(ordered)
            mid = count // 2
            median = ordered[mid] if count % 2 else (ordered[mid - 1] + ordered[mid]) / 2.0
            label_stats[label] = {
                "count": count,
                "avg": sum(ordered) / count,
                "median": median,
                "min": ordered[0],
                "max": ordered[-1],
            }
        result[ticker_key] = label_stats
    return result if ticker is None else result.get(ticker.upper(), {})
