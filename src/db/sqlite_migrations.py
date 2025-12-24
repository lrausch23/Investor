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
