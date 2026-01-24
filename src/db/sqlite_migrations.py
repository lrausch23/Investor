from __future__ import annotations

import re

from sqlalchemy import text
from sqlalchemy.engine import Engine


def _table_columns(engine: Engine, table: str) -> set[str]:
    with engine.connect() as conn:
        rows = conn.execute(text(f"PRAGMA table_info({table})")).fetchall()
    # row: (cid, name, type, notnull, dflt_value, pk)
    cols: set[str] = set()
    for r in rows:
        try:
            cols.add(str(r._mapping["name"]))
        except Exception:
            cols.add(str(r[1]))
    return cols


_COL_NAME_RE = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\b")


def _add_column(engine: Engine, table: str, column_ddl: str) -> None:
    m = _COL_NAME_RE.match(column_ddl)
    col_name = m.group(1) if m else None
    if col_name:
        try:
            if col_name in _table_columns(engine, table):
                return
        except Exception:
            pass
    try:
        with engine.begin() as conn:
            conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {column_ddl}"))
    except Exception as e:
        # SQLite raises OperationalError("duplicate column name: X") for ADD COLUMN on existing columns.
        if "duplicate column name" in str(e).lower():
            return
        raise


def ensure_sqlite_schema(engine: Engine) -> None:
    """
    Minimal SQLite "migrations" for MVP (no Alembic).
    Safe to call on every startup: only adds missing columns/tables.
    """
    if engine.url.get_backend_name() != "sqlite":
        return

    # If table doesn't exist yet, SQLAlchemy create_all will handle it.
    existing_tables = set()
    with engine.connect() as conn:
        existing_tables = {r[0] for r in conn.execute(text("SELECT name FROM sqlite_master WHERE type='table'")).fetchall()}

    if "external_connections" in existing_tables:
        cols = _table_columns(engine, "external_connections")
        if "connector" not in cols:
            _add_column(engine, "external_connections", "connector VARCHAR(50)")
        if "last_successful_sync_at" not in cols:
            _add_column(engine, "external_connections", "last_successful_sync_at DATETIME")
        if "last_successful_txn_end" not in cols:
            _add_column(engine, "external_connections", "last_successful_txn_end DATE")
        if "holdings_last_asof" not in cols:
            _add_column(engine, "external_connections", "holdings_last_asof DATETIME")
        if "txn_earliest_available" not in cols:
            _add_column(engine, "external_connections", "txn_earliest_available DATE")
        if "last_full_sync_at" not in cols:
            _add_column(engine, "external_connections", "last_full_sync_at DATETIME")
        if "coverage_status" not in cols:
            _add_column(engine, "external_connections", "coverage_status VARCHAR(20)")
        if "last_error_json" not in cols:
            _add_column(engine, "external_connections", "last_error_json TEXT")

    if "sync_runs" in existing_tables:
        cols = _table_columns(engine, "sync_runs")
        for name, ddl in [
            ("mode", "mode VARCHAR(20)"),
            ("requested_start_date", "requested_start_date DATE"),
            ("requested_end_date", "requested_end_date DATE"),
            ("effective_start_date", "effective_start_date DATE"),
            ("effective_end_date", "effective_end_date DATE"),
            ("store_payloads", "store_payloads BOOLEAN NOT NULL DEFAULT 0"),
            ("finished_at", "finished_at DATETIME"),
            ("coverage_json", "coverage_json TEXT"),
        ]:
            if name not in cols:
                _add_column(engine, "sync_runs", ddl)
                cols.add(name)
        for name, ddl in [
            ("pages_fetched", "pages_fetched INTEGER NOT NULL DEFAULT 0"),
            ("txn_count", "txn_count INTEGER NOT NULL DEFAULT 0"),
            ("new_count", "new_count INTEGER NOT NULL DEFAULT 0"),
            ("dupes_count", "dupes_count INTEGER NOT NULL DEFAULT 0"),
            ("parse_fail_count", "parse_fail_count INTEGER NOT NULL DEFAULT 0"),
            ("missing_symbol_count", "missing_symbol_count INTEGER NOT NULL DEFAULT 0"),
            ("error_json", "error_json TEXT"),
        ]:
            if name not in cols:
                _add_column(engine, "sync_runs", ddl)
                cols.add(name)

    if "broker_wash_sale_events" in existing_tables:
        cols = _table_columns(engine, "broker_wash_sale_events")
        # Added for broker wash linking / tax summary.
        for name, ddl in [
            ("linked_closure_id", "linked_closure_id INTEGER"),
            ("link_confidence", "link_confidence INTEGER"),
            ("basis_effective", "basis_effective NUMERIC(20,2)"),
            ("proceeds_effective", "proceeds_effective NUMERIC(20,2)"),
            ("disallowed_loss", "disallowed_loss NUMERIC(20,2)"),
            ("realized_pl_effective", "realized_pl_effective NUMERIC(20,2)"),
            ("reason_notes", "reason_notes TEXT"),
        ]:
            if name not in cols:
                _add_column(engine, "broker_wash_sale_events", ddl)

    if "broker_lot_closures" in existing_tables:
        cols = _table_columns(engine, "broker_lot_closures")
        if "taxpayer_entity_id" not in cols:
            _add_column(engine, "broker_lot_closures", "taxpayer_entity_id INTEGER")

    if "expense_transactions" in existing_tables:
        cols = _table_columns(engine, "expense_transactions")
        if "category_hint" not in cols:
            _add_column(engine, "expense_transactions", "category_hint VARCHAR(100)")
        if "account_last4_masked" not in cols:
            _add_column(engine, "expense_transactions", "account_last4_masked VARCHAR(8)")
        if "cardholder_name" not in cols:
            _add_column(engine, "expense_transactions", "cardholder_name VARCHAR(200)")

    if "expense_accounts" in existing_tables:
        cols = _table_columns(engine, "expense_accounts")
        if "provider_account_id" not in cols:
            _add_column(engine, "expense_accounts", "provider_account_id VARCHAR(200)")
        if "scope" not in cols:
            _add_column(engine, "expense_accounts", "scope VARCHAR(20) NOT NULL DEFAULT 'PERSONAL'")

    if "bullion_holdings" in existing_tables:
        cols = _table_columns(engine, "bullion_holdings")
        if "cost_basis_total" not in cols:
            _add_column(engine, "bullion_holdings", "cost_basis_total NUMERIC(20,2)")

    if "external_file_ingests" in existing_tables:
        cols = _table_columns(engine, "external_file_ingests")
        for name, ddl in [
            ("stored_path", "stored_path TEXT"),
            ("start_date_hint", "start_date_hint DATE"),
            ("end_date_hint", "end_date_hint DATE"),
            ("metadata_json", "metadata_json TEXT"),
        ]:
            if name not in cols:
                _add_column(engine, "external_file_ingests", ddl)

    if "external_liability_snapshots" not in existing_tables:
        try:
            with engine.begin() as conn:
                conn.execute(
                    text(
                        """
                        CREATE TABLE external_liability_snapshots (
                            id INTEGER PRIMARY KEY,
                            connection_id INTEGER NOT NULL,
                            as_of DATETIME NOT NULL,
                            payload_json JSON NOT NULL,
                            created_at DATETIME NOT NULL,
                            FOREIGN KEY(connection_id) REFERENCES external_connections(id)
                        )
                        """
                    )
                )
                conn.execute(
                    text(
                        "CREATE INDEX ix_external_liability_snapshots_conn ON external_liability_snapshots(connection_id)"
                    )
                )
        except Exception:
            pass

    if "external_card_statements" not in existing_tables:
        try:
            with engine.begin() as conn:
                conn.execute(
                    text(
                        """
                        CREATE TABLE external_card_statements (
                            id INTEGER PRIMARY KEY,
                            connection_id INTEGER NOT NULL,
                            expense_account_id INTEGER,
                            last4 VARCHAR(8),
                            statement_period_start DATE,
                            statement_period_end DATE,
                            payment_due_date DATE,
                            statement_balance NUMERIC(20,2),
                            interest_saving_balance NUMERIC(20,2),
                            minimum_payment_due NUMERIC(20,2),
                            pay_over_time_json JSON,
                            source_file TEXT,
                            file_hash VARCHAR(64) NOT NULL,
                            created_at DATETIME NOT NULL,
                            FOREIGN KEY(connection_id) REFERENCES external_connections(id),
                            FOREIGN KEY(expense_account_id) REFERENCES expense_accounts(id),
                            UNIQUE(connection_id, file_hash)
                        )
                        """
                    )
                )
                conn.execute(
                    text(
                        "CREATE INDEX ix_external_card_statements_conn_last4 ON external_card_statements(connection_id, last4)"
                    )
                )
        except Exception:
            pass
    else:
        cols = _table_columns(engine, "external_card_statements")
        if "pay_over_time_json" not in cols:
            _add_column(engine, "external_card_statements", "pay_over_time_json TEXT")

    if "expense_account_balances" not in existing_tables:
        try:
            with engine.begin() as conn:
                conn.execute(
                    text(
                        """
                        CREATE TABLE expense_account_balances (
                            id INTEGER PRIMARY KEY,
                            expense_account_id INTEGER NOT NULL,
                            as_of_date DATETIME NOT NULL,
                            balance_current NUMERIC(20,2),
                            balance_available NUMERIC(20,2),
                            currency VARCHAR(8) NOT NULL DEFAULT 'USD',
                            source VARCHAR(50) NOT NULL DEFAULT 'PLAID',
                            created_at DATETIME NOT NULL,
                            FOREIGN KEY(expense_account_id) REFERENCES expense_accounts(id)
                        )
                        """
                    )
                )
                conn.execute(
                    text(
                        "CREATE INDEX ix_expense_account_balance_account ON expense_account_balances(expense_account_id)"
                    )
                )
        except Exception:
            pass

    if "recurring_bill" not in existing_tables:
        try:
            with engine.begin() as conn:
                conn.execute(
                    text(
                        """
                        CREATE TABLE recurring_bill (
                            id INTEGER PRIMARY KEY,
                            scope VARCHAR(20) NOT NULL DEFAULT 'PERSONAL',
                            name VARCHAR(200) NOT NULL,
                            source_account_id INTEGER,
                            cadence VARCHAR(20) NOT NULL DEFAULT 'MONTHLY',
                            amount_mode VARCHAR(20) NOT NULL DEFAULT 'VARIABLE',
                            amount_expected NUMERIC(20,2),
                            amount_min NUMERIC(20,2),
                            amount_max NUMERIC(20,2),
                            due_day_of_month INTEGER,
                            is_active BOOLEAN NOT NULL DEFAULT 1,
                            is_user_confirmed BOOLEAN NOT NULL DEFAULT 0,
                            autodetect_confidence NUMERIC(6,3),
                            created_at DATETIME NOT NULL,
                            updated_at DATETIME NOT NULL,
                            FOREIGN KEY(source_account_id) REFERENCES expense_accounts(id)
                        )
                        """
                    )
                )
                conn.execute(text("CREATE INDEX ix_recurring_bill_scope ON recurring_bill(scope)"))
        except Exception:
            pass

    if "recurring_bill_rule" not in existing_tables:
        try:
            with engine.begin() as conn:
                conn.execute(
                    text(
                        """
                        CREATE TABLE recurring_bill_rule (
                            id INTEGER PRIMARY KEY,
                            recurring_bill_id INTEGER NOT NULL,
                            rule_type VARCHAR(40) NOT NULL,
                            rule_value VARCHAR(200) NOT NULL,
                            priority INTEGER NOT NULL DEFAULT 0,
                            FOREIGN KEY(recurring_bill_id) REFERENCES recurring_bill(id)
                        )
                        """
                    )
                )
                conn.execute(text("CREATE INDEX ix_recurring_bill_rule_bill ON recurring_bill_rule(recurring_bill_id)"))
        except Exception:
            pass

    if "recurring_bill_ignore" not in existing_tables:
        try:
            with engine.begin() as conn:
                conn.execute(
                    text(
                        """
                        CREATE TABLE recurring_bill_ignore (
                            id INTEGER PRIMARY KEY,
                            scope VARCHAR(20) NOT NULL,
                            rule_type VARCHAR(40) NOT NULL,
                            rule_value VARCHAR(200) NOT NULL,
                            created_at DATETIME NOT NULL
                        )
                        """
                    )
                )
                conn.execute(
                    text(
                        "CREATE UNIQUE INDEX ux_recurring_bill_ignore_scope_rule ON recurring_bill_ignore(scope, rule_type, rule_value)"
                    )
                )
        except Exception:
            pass

    if "recurring_card_charge" not in existing_tables:
        try:
            with engine.begin() as conn:
                conn.execute(
                    text(
                        """
                        CREATE TABLE recurring_card_charge (
                            id INTEGER PRIMARY KEY,
                            scope VARCHAR(20) NOT NULL DEFAULT 'PERSONAL',
                            name VARCHAR(200) NOT NULL,
                            source_account_id INTEGER,
                            cadence VARCHAR(20) NOT NULL DEFAULT 'MONTHLY',
                            amount_mode VARCHAR(20) NOT NULL DEFAULT 'VARIABLE',
                            amount_expected NUMERIC(20,2),
                            amount_min NUMERIC(20,2),
                            amount_max NUMERIC(20,2),
                            due_day_of_month INTEGER,
                            is_active BOOLEAN NOT NULL DEFAULT 1,
                            is_user_confirmed BOOLEAN NOT NULL DEFAULT 0,
                            autodetect_confidence NUMERIC(6,3),
                            created_at DATETIME NOT NULL,
                            updated_at DATETIME NOT NULL,
                            FOREIGN KEY(source_account_id) REFERENCES expense_accounts(id)
                        )
                        """
                    )
                )
                conn.execute(text("CREATE INDEX ix_recurring_card_charge_scope ON recurring_card_charge(scope)"))
        except Exception:
            pass

    if "recurring_card_charge_rule" not in existing_tables:
        try:
            with engine.begin() as conn:
                conn.execute(
                    text(
                        """
                        CREATE TABLE recurring_card_charge_rule (
                            id INTEGER PRIMARY KEY,
                            recurring_card_charge_id INTEGER NOT NULL,
                            rule_type VARCHAR(40) NOT NULL,
                            rule_value VARCHAR(200) NOT NULL,
                            priority INTEGER NOT NULL DEFAULT 0,
                            FOREIGN KEY(recurring_card_charge_id) REFERENCES recurring_card_charge(id)
                        )
                        """
                    )
                )
                conn.execute(
                    text(
                        "CREATE INDEX ix_recurring_card_charge_rule_charge ON recurring_card_charge_rule(recurring_card_charge_id)"
                    )
                )
        except Exception:
            pass

    if "recurring_card_charge_ignore" not in existing_tables:
        try:
            with engine.begin() as conn:
                conn.execute(
                    text(
                        """
                        CREATE TABLE recurring_card_charge_ignore (
                            id INTEGER PRIMARY KEY,
                            scope VARCHAR(20) NOT NULL,
                            rule_type VARCHAR(40) NOT NULL,
                            rule_value VARCHAR(200) NOT NULL,
                            created_at DATETIME NOT NULL
                        )
                        """
                    )
                )
                conn.execute(
                    text(
                        "CREATE UNIQUE INDEX ux_recurring_card_charge_ignore_scope_rule ON recurring_card_charge_ignore(scope, rule_type, rule_value)"
                    )
                )
        except Exception:
            pass
