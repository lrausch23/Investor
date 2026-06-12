# mypy: ignore-errors
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

from . import core

logger = core.logger
_TRANSITION_COLUMNS = core._TRANSITION_COLUMNS
_PAPER_PORTFOLIO_COLUMNS = core._PAPER_PORTFOLIO_COLUMNS
_PAPER_TRADE_PLAN_COLUMNS = core._PAPER_TRADE_PLAN_COLUMNS
_SIGNAL_SNAPSHOT_COLUMNS = core._SIGNAL_SNAPSHOT_COLUMNS
ALERT_TYPES = core.ALERT_TYPES
_ORDER_AUDIT_EVENT_TYPES = core._ORDER_AUDIT_EVENT_TYPES

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


def _ensure_signal_snapshot_schema(conn: sqlite3.Connection) -> None:
    for column, ddl in _SIGNAL_SNAPSHOT_COLUMNS.items():
        _ensure_column(conn, "signal_snapshots", column, ddl)


def _alert_type_check_values() -> str:
    return ", ".join(f"'{alert_type}'" for alert_type in ALERT_TYPES)


def _audit_event_type_check_values() -> str:
    return ", ".join(f"'{event_type}'" for event_type in _ORDER_AUDIT_EVENT_TYPES)


def _create_alert_log_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS alert_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            alert_type TEXT NOT NULL CHECK (
                alert_type IN ({_alert_type_check_values()})
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


def _create_thesis_monitor_run_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS thesis_monitor_run (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            monitor_key TEXT NOT NULL,
            primary_ticker TEXT NOT NULL,
            status TEXT NOT NULL CHECK (status IN ('intact', 'watch', 'reunderwrite')),
            severity TEXT NOT NULL DEFAULT 'info' CHECK (severity IN ('info', 'warning', 'critical')),
            risk_score REAL NOT NULL DEFAULT 0,
            thesis TEXT NOT NULL DEFAULT '',
            evidence_json TEXT NOT NULL DEFAULT '[]',
            tickers_scanned_json TEXT NOT NULL DEFAULT '[]',
            alert_id INTEGER REFERENCES alert_log(id) ON DELETE SET NULL,
            created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_thesis_monitor_run_key_created
        ON thesis_monitor_run(monitor_key, created_at DESC)
        """
    )


def _create_notification_preferences_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS notification_preferences (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            alert_type TEXT NOT NULL,
            channel TEXT NOT NULL CHECK (channel IN ('in_app', 'email', 'slack')),
            enabled INTEGER NOT NULL DEFAULT 1,
            UNIQUE(alert_type, channel)
        )
        """
    )


def _create_barrier_override_log_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS barrier_override_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            portfolio_id INTEGER NOT NULL,
            ticker TEXT NOT NULL,
            lot_id INTEGER,
            override_type TEXT NOT NULL DEFAULT 'ltcg_preservation',
            original_stop REAL,
            overridden_stop REAL,
            days_to_ltcg INTEGER,
            tax_savings_estimate REAL,
            additional_risk REAL,
            status TEXT NOT NULL DEFAULT 'active'
                CHECK (status IN ('active', 'expired', 'cancelled')),
            created_at TEXT NOT NULL,
            expires_at TEXT NOT NULL
        )
        """
    )


def _create_stress_test_result_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS stress_test_result (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scenario_id TEXT NOT NULL,
            config_json TEXT NOT NULL,
            result_json TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            status TEXT NOT NULL DEFAULT 'completed'
                CHECK(status IN ('running', 'completed', 'failed'))
        )
        """
    )


def _create_execution_quality_snapshot_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS execution_quality_snapshot (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            portfolio_id INTEGER NOT NULL REFERENCES paper_portfolio(id) ON DELETE CASCADE,
            analysis_date TEXT NOT NULL,
            total_trades INTEGER NOT NULL DEFAULT 0,
            overall_avg_impl_shortfall_bps REAL,
            overall_avg_vs_vwap_bps REAL,
            by_strategy_json TEXT NOT NULL DEFAULT '[]',
            by_algo_json TEXT NOT NULL DEFAULT '[]',
            by_time_of_day_json TEXT NOT NULL DEFAULT '[]',
            by_theme_json TEXT NOT NULL DEFAULT '[]',
            by_adv_bucket_json TEXT NOT NULL DEFAULT '[]',
            patterns_json TEXT NOT NULL DEFAULT '[]',
            best_strategy TEXT,
            worst_strategy TEXT,
            created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_execution_quality_snapshot_portfolio_date
        ON execution_quality_snapshot(portfolio_id, analysis_date DESC)
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
            order_type TEXT NOT NULL DEFAULT 'limit',
            routing_strategy TEXT NOT NULL DEFAULT '',
            algo_strategy TEXT NOT NULL DEFAULT '',
            arrival_price REAL,
            vwap_benchmark REAL,
            close_price REAL,
            meta_labeler_score REAL,
            stop_price REAL,
            target_price REAL,
            risk_reward_ratio REAL,
            timeframe_days INTEGER,
            trade_geometry_source TEXT NOT NULL DEFAULT '',
            sizing_method TEXT NOT NULL DEFAULT 'equal_dollar',
            agent_trace TEXT NOT NULL DEFAULT '',
            hurdle_gross_return_pct REAL,
            hurdle_net_return_pct REAL,
            hurdle_passed INTEGER,
            duration_gate_passed INTEGER,
            expected_regime_duration REAL,
            anti_churn_passed INTEGER,
            ltcg_override_active INTEGER,
            ltcg_protected_quantity REAL,
            ltcg_tax_savings REAL,
            signal_quality_score REAL,
            signal_quality_grade TEXT NOT NULL DEFAULT '',
            signal_quality_reasons TEXT NOT NULL DEFAULT '[]',
            agent_key TEXT NOT NULL DEFAULT '',
            llm_used INTEGER NOT NULL DEFAULT 0,
            llm_influenced INTEGER NOT NULL DEFAULT 0,
            llm_influence TEXT NOT NULL DEFAULT '',
            llm_source TEXT NOT NULL DEFAULT '',
            llm_provider TEXT NOT NULL DEFAULT '',
            llm_model TEXT NOT NULL DEFAULT '',
            llm_model_display TEXT NOT NULL DEFAULT '',
            llm_verdict TEXT NOT NULL DEFAULT '',
            llm_confidence REAL,
            decision_constants_version TEXT NOT NULL DEFAULT '',
            notes TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )


def _migrate_paper_trade_plan_agent_trace(conn: sqlite3.Connection) -> None:
    """Add agent_trace column for orchestrated decision audit."""
    try:
        conn.execute("ALTER TABLE paper_trade_plan ADD COLUMN agent_trace TEXT NOT NULL DEFAULT ''")
    except Exception:
        pass


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
        f"""
        CREATE TABLE IF NOT EXISTS order_audit_trail (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id TEXT NOT NULL,
            portfolio_id INTEGER NOT NULL,
            event_type TEXT NOT NULL CHECK (
                event_type IN ({_audit_event_type_check_values()})
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
    missing_types = [event_type for event_type in _ORDER_AUDIT_EVENT_TYPES if f"'{event_type}'" not in create_sql]
    if not missing_types:
        return
    if not _table_exists(conn, "order_audit_trail"):
        return
    logger.info("Migrating order_audit_trail event_type CHECK to include %s", ", ".join(missing_types))
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
    missing_types = [alert_type for alert_type in ALERT_TYPES if f"'{alert_type}'" not in create_sql]
    if not missing_types:
        return
    if not _table_exists(conn, "alert_log"):
        return
    logger.info("Migrating alert_log alert_type CHECK to include %s", ", ".join(missing_types))
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


def _migrate_discovery_watchlist_fundamental_gate(conn: sqlite3.Connection) -> None:
    """Add fundamental gate columns to discovery_watchlist."""
    columns = (
        ("fundamental_gate_passed", "INTEGER"),
        ("piotroski_score", "INTEGER"),
        ("roic_pct", "REAL"),
        ("fundamental_details", "TEXT NOT NULL DEFAULT ''"),
        ("altman_z_score", "REAL"),
        ("altman_z_interpretation", "TEXT NOT NULL DEFAULT ''"),
    )
    for column, ddl in columns:
        try:
            conn.execute(f"ALTER TABLE discovery_watchlist ADD COLUMN {column} {ddl}")
        except Exception:
            pass


def _migrate_discovery_watchlist_cross_sectional(conn: sqlite3.Connection) -> None:
    columns = (
        ("beta", "REAL"),
        ("beta_adjusted_return", "REAL"),
        ("vol_z_score", "REAL"),
        ("vol_z_interpretation", "TEXT NOT NULL DEFAULT ''"),
        ("normalized_crowd_score", "INTEGER"),
        ("peer_percentile_json", "TEXT NOT NULL DEFAULT ''"),
    )
    for column, ddl in columns:
        try:
            conn.execute(f"ALTER TABLE discovery_watchlist ADD COLUMN {column} {ddl}")
        except Exception:
            pass


def _migrate_paper_trade_plan_sizing_method(conn: sqlite3.Connection) -> None:
    try:
        conn.execute("ALTER TABLE paper_trade_plan ADD COLUMN sizing_method TEXT NOT NULL DEFAULT 'equal_dollar'")
    except Exception:
        pass


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


def initialize_schema(conn: sqlite3.Connection) -> None:
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
            expected_regime_duration REAL,
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
    _create_thesis_monitor_run_table(conn)
    _create_notification_preferences_table(conn)
    _create_paper_tax_lot_table(conn)
    _create_wash_sale_restricted_table(conn)
    _create_stress_test_result_table(conn)
    _create_execution_quality_snapshot_table(conn)
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
            fundamental_gate_passed INTEGER,
            piotroski_score INTEGER,
            roic_pct REAL,
            altman_z_score REAL,
            altman_z_interpretation TEXT NOT NULL DEFAULT '',
            beta REAL,
            beta_adjusted_return REAL,
            vol_z_score REAL,
            vol_z_interpretation TEXT NOT NULL DEFAULT '',
            normalized_crowd_score INTEGER,
            peer_percentile_json TEXT NOT NULL DEFAULT '',
            fundamental_details TEXT NOT NULL DEFAULT '',
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
    _create_barrier_override_log_table(conn)
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
    _ensure_signal_snapshot_schema(conn)
    _migrate_paper_trade_plan_agent_trace(conn)
    _migrate_trade_plan_source_check(conn)
    _migrate_paper_trade_plan_sizing_method(conn)
    _migrate_discovery_watchlist_fundamental_gate(conn)
    _migrate_discovery_watchlist_cross_sectional(conn)
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
