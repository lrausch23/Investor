#!/usr/bin/env python3
"""
Verify that the Investor SQLite DB schema matches what hmm_tool/investor_adapter.py expects.

Usage:
    python scripts/verify_investor_schema.py [/path/to/investor.db]

If no path is given, checks INVESTOR_DB_PATH env var, then the default location.
"""

from __future__ import annotations

import json
import os
import sqlite3
import sys
from pathlib import Path

DEFAULT_DB = Path("/Volumes/Projects/Dev/Investor/data/investor.db")

# ── What investor_adapter.py expects ──────────────────────────────────────────
# Each entry: table_name -> { column_name: description_of_use }
EXPECTED_SCHEMA: dict[str, dict[str, str]] = {
    "accounts": {
        "id": "PK, join key for position_lots / tax_lots / transactions",
        "name": "account_name in PortfolioPosition",
        "account_type": "TAXABLE / IRA / OTHER — drives tax adjustment logic",
        "taxpayer_entity_id": "FK to taxpayer_entities for taxpayer_type lookup",
    },
    "taxpayer_entities": {
        "id": "PK, join target for accounts.taxpayer_entity_id",
        "type": "TRUST / PERSONAL — surfaced in PortfolioPosition.taxpayer_type",
    },
    "securities": {
        "id": "PK, join target for tax_lots.security_id",
        "ticker": "ticker symbol — join key for position_lots, dedup, and display",
        "asset_class": "surfaced in PortfolioPosition.asset_class",
        "metadata_json": "JSON column; adapter reads key 'last_price' for current price",
        "substitute_group_id": "FK to substitute_groups; used in wash sale risk check",
    },
    "position_lots": {
        "id": "PK → TaxLotInfo.lot_id",
        "account_id": "FK to accounts",
        "ticker": "ticker symbol (direct column, no FK to securities required)",
        "acquisition_date": "Date → TaxLotInfo.acquisition_date, days_held, term",
        "qty": "Numeric → TaxLotInfo.qty, PortfolioPosition.qty",
        "basis_total": "Numeric → TaxLotInfo.basis_total",
        "adjusted_basis_total": "Numeric (nullable) → preferred over basis_total if present",
    },
    "tax_lots": {
        "id": "PK → TaxLotInfo.lot_id",
        "account_id": "FK to accounts",
        "security_id": "FK to securities (adapter joins to get ticker)",
        "acquired_date": "Date → TaxLotInfo.acquisition_date (NOTE: column is 'acquired_date' not 'acquisition_date')",
        "quantity_open": "Numeric → TaxLotInfo.qty (filtered: quantity_open > 0)",
        "basis_open": "Numeric (nullable) → TaxLotInfo.basis_total",
        "source": "String → adapter filters source = 'RECONSTRUCTED'",
    },
    "transactions": {
        "id": "PK",
        "type": "TxnType enum → adapter filters type = 'BUY' for wash sale check",
        "ticker": "String (nullable) → matched against target ticker",
        "date": "Date → used in 30-day wash sale window check",
    },
    "tax_assumptions": {
        "name": "String → adapter queries WHERE name = 'Default'",
        "json_definition": "JSON → adapter reads ordinary_rate, ltcg_rate, state_rate, niit_rate",
    },
    "substitute_groups": {
        "id": "PK, join target for securities.substitute_group_id (used in wash sale POSSIBLE check)",
    },
}


def resolve_db_path(argv_path: str | None) -> Path:
    if argv_path:
        return Path(argv_path)
    env = os.getenv("INVESTOR_DB_PATH")
    if env:
        return Path(env)
    return DEFAULT_DB


def get_actual_schema(conn: sqlite3.Connection) -> dict[str, list[dict]]:
    """Return {table_name: [{name, type, notnull, pk, dflt_value}, ...]}."""
    tables = [
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
    ]
    schema: dict[str, list[dict]] = {}
    for table in tables:
        cols = conn.execute(f"PRAGMA table_info(\"{table}\")").fetchall()
        schema[table] = [
            {
                "name": col[1],
                "type": col[2],
                "notnull": bool(col[3]),
                "default": col[4],
                "pk": bool(col[5]),
            }
            for col in cols
        ]
    return schema


def check_table(
    table: str,
    expected_cols: dict[str, str],
    actual_schema: dict[str, list[dict]],
) -> list[str]:
    issues: list[str] = []
    if table not in actual_schema:
        issues.append(f"MISSING TABLE: '{table}' does not exist in the database")
        return issues

    actual_col_names = {col["name"] for col in actual_schema[table]}
    for col_name, usage in expected_cols.items():
        if col_name not in actual_col_names:
            issues.append(
                f"MISSING COLUMN: '{table}.{col_name}' — adapter uses it for: {usage}"
            )
    return issues


def check_metadata_json_has_last_price(conn: sqlite3.Connection) -> list[str]:
    """Spot-check that securities.metadata_json actually contains 'last_price'."""
    issues: list[str] = []
    try:
        rows = conn.execute(
            "SELECT ticker, metadata_json FROM securities LIMIT 20"
        ).fetchall()
    except sqlite3.OperationalError as e:
        issues.append(f"Cannot read securities table: {e}")
        return issues

    if not rows:
        issues.append("WARNING: securities table is empty — no tickers to analyze")
        return issues

    missing_price = []
    has_price = []
    for ticker, meta_raw in rows:
        if not meta_raw:
            missing_price.append(ticker)
            continue
        try:
            meta = json.loads(meta_raw) if isinstance(meta_raw, str) else meta_raw
        except (json.JSONDecodeError, TypeError):
            missing_price.append(ticker)
            continue
        if "last_price" in meta and meta["last_price"]:
            has_price.append(ticker)
        else:
            missing_price.append(ticker)

    if missing_price and not has_price:
        issues.append(
            f"MISSING DATA: No securities have 'last_price' in metadata_json. "
            f"Checked: {', '.join(missing_price[:10])}"
        )
    elif missing_price:
        issues.append(
            f"WARNING: {len(missing_price)} of {len(rows)} sampled securities lack 'last_price' in metadata_json: "
            f"{', '.join(missing_price[:10])}"
        )
    return issues


def check_tax_assumptions_default(conn: sqlite3.Connection) -> list[str]:
    """Verify the 'Default' tax assumptions row exists and has expected keys."""
    issues: list[str] = []
    try:
        row = conn.execute(
            "SELECT json_definition FROM tax_assumptions WHERE name = 'Default' LIMIT 1"
        ).fetchone()
    except sqlite3.OperationalError as e:
        issues.append(f"Cannot query tax_assumptions: {e}")
        return issues

    if not row:
        issues.append(
            "WARNING: No row with name='Default' in tax_assumptions — adapter will use hardcoded fallback rates"
        )
        return issues

    raw = row[0]
    try:
        parsed = json.loads(raw) if isinstance(raw, str) else raw
    except (json.JSONDecodeError, TypeError):
        issues.append(f"BAD DATA: tax_assumptions.json_definition is not valid JSON: {raw!r}")
        return issues

    expected_keys = {"ordinary_rate", "ltcg_rate", "state_rate", "niit_rate"}
    missing = expected_keys - set(parsed.keys())
    if missing:
        issues.append(
            f"WARNING: tax_assumptions 'Default' json_definition missing keys: {', '.join(sorted(missing))}. "
            f"Adapter will use hardcoded fallback for those. Present keys: {', '.join(sorted(parsed.keys()))}"
        )
    else:
        print(f"  ✓ tax_assumptions 'Default' has all expected keys: {sorted(parsed.keys())}")
        for k in sorted(expected_keys):
            print(f"      {k} = {parsed[k]}")

    return issues


def check_row_counts(conn: sqlite3.Connection) -> None:
    """Print row counts for key tables to give a sense of data volume."""
    tables = [
        "accounts", "taxpayer_entities", "securities", "position_lots",
        "tax_lots", "transactions", "tax_assumptions", "substitute_groups",
    ]
    print("\n── Row counts ──")
    for table in tables:
        try:
            count = conn.execute(f"SELECT COUNT(*) FROM \"{table}\"").fetchone()[0]
            print(f"  {table}: {count:,} rows")
        except sqlite3.OperationalError:
            print(f"  {table}: TABLE NOT FOUND")


def check_portfolio_tickers(conn: sqlite3.Connection) -> None:
    """Run the same query the adapter would use and show results."""
    print("\n── Portfolio tickers (what HMM would analyze) ──")
    try:
        rows = conn.execute(
            """
            SELECT ticker FROM (
                SELECT DISTINCT ticker
                FROM position_lots
                WHERE CAST(qty AS REAL) > 0
                UNION
                SELECT DISTINCT s.ticker
                FROM tax_lots tl
                JOIN securities s ON s.id = tl.security_id
                WHERE tl.source = 'RECONSTRUCTED' AND CAST(tl.quantity_open AS REAL) > 0
            )
            ORDER BY ticker
            """
        ).fetchall()
        tickers = sorted({row[0].upper() for row in rows if row[0]})
        if tickers:
            print(f"  Found {len(tickers)} tickers: {', '.join(tickers)}")
        else:
            print("  WARNING: Query returned 0 tickers — adapter would fall back to DEFAULT_TICKERS")
    except sqlite3.OperationalError as e:
        print(f"  ERROR running ticker query: {e}")


def check_sample_position(conn: sqlite3.Connection) -> None:
    """Run the adapter's tax_lots join and show a sample to verify data shape."""
    print("\n── Sample tax lot position (first 3 rows from adapter query) ──")
    try:
        rows = conn.execute(
            """
            SELECT
                tl.id AS lot_id,
                a.name AS account_name,
                a.account_type,
                te.type AS taxpayer_type,
                s.ticker,
                s.asset_class,
                s.metadata_json,
                tl.acquired_date,
                CAST(tl.quantity_open AS REAL) AS qty,
                CAST(COALESCE(tl.basis_open, 0) AS REAL) AS basis_total
            FROM tax_lots tl
            JOIN accounts a ON a.id = tl.account_id
            JOIN taxpayer_entities te ON te.id = a.taxpayer_entity_id
            JOIN securities s ON s.id = tl.security_id
            WHERE tl.source = 'RECONSTRUCTED' AND CAST(tl.quantity_open AS REAL) > 0
            ORDER BY a.id, s.ticker, tl.acquired_date
            LIMIT 3
            """
        ).fetchall()
        if not rows:
            print("  No RECONSTRUCTED tax lots with quantity_open > 0 found.")
            print("  Checking position_lots fallback...")
            rows = conn.execute(
                """
                SELECT
                    pl.id AS lot_id,
                    a.name AS account_name,
                    a.account_type,
                    te.type AS taxpayer_type,
                    pl.ticker,
                    COALESCE(s.asset_class, 'UNKNOWN') AS asset_class,
                    s.metadata_json,
                    pl.acquisition_date,
                    CAST(pl.qty AS REAL) AS qty,
                    CAST(COALESCE(pl.adjusted_basis_total, pl.basis_total, 0) AS REAL) AS basis_total
                FROM position_lots pl
                JOIN accounts a ON a.id = pl.account_id
                JOIN taxpayer_entities te ON te.id = a.taxpayer_entity_id
                LEFT JOIN securities s ON s.ticker = pl.ticker
                WHERE CAST(pl.qty AS REAL) > 0
                ORDER BY a.id, pl.ticker, pl.acquisition_date
                LIMIT 3
                """
            ).fetchall()
        if rows:
            col_names = [desc[0] for desc in rows[0].cursor.description] if hasattr(rows[0], 'cursor') else [
                "lot_id", "account_name", "account_type", "taxpayer_type",
                "ticker", "asset_class", "metadata_json", "date", "qty", "basis_total"
            ]
            for row in rows:
                print(f"  {dict(zip(col_names, row))}")
        else:
            print("  WARNING: No open positions found in either tax_lots or position_lots")
    except sqlite3.OperationalError as e:
        print(f"  ERROR: {e}")


def check_wash_sale_query(conn: sqlite3.Connection) -> None:
    """Verify the wash sale query structure works against the actual schema."""
    print("\n── Wash sale query check ──")
    try:
        # Just verify the query parses and runs (using a dummy ticker)
        conn.execute(
            """
            SELECT 1
            FROM transactions
            WHERE type = 'BUY'
              AND ticker = ?
              AND date BETWEEN date('now', '-30 day') AND date('now', '+30 day')
            LIMIT 1
            """,
            ("__TEST__",),
        ).fetchone()
        print("  ✓ transactions wash sale query runs successfully")
    except sqlite3.OperationalError as e:
        print(f"  FAIL: {e}")

    try:
        conn.execute(
            """
            SELECT 1
            FROM transactions t
            JOIN securities s ON s.ticker = t.ticker
            WHERE t.type = 'BUY'
              AND s.substitute_group_id = ?
              AND date BETWEEN date('now', '-30 day') AND date('now', '+30 day')
            LIMIT 1
            """,
            (999,),
        ).fetchone()
        print("  ✓ substitute group wash sale query runs successfully")
    except sqlite3.OperationalError as e:
        print(f"  FAIL: {e}")


def main() -> None:
    db_path = resolve_db_path(sys.argv[1] if len(sys.argv) > 1 else None)

    print(f"Investor DB schema verification")
    print(f"Database: {db_path}")
    print(f"{'=' * 70}\n")

    if not db_path.exists():
        print(f"ERROR: Database file not found at {db_path}")
        print("Set INVESTOR_DB_PATH or pass the path as an argument.")
        sys.exit(1)

    conn = sqlite3.connect(str(db_path))
    actual_schema = get_actual_schema(conn)

    print(f"Found {len(actual_schema)} tables in database.\n")

    # ── Structure checks ──
    all_issues: list[str] = []
    print("── Table & column checks ──")
    for table, expected_cols in EXPECTED_SCHEMA.items():
        issues = check_table(table, expected_cols, actual_schema)
        if issues:
            for issue in issues:
                print(f"  ✗ {issue}")
            all_issues.extend(issues)
        else:
            actual_cols = [c["name"] for c in actual_schema[table]]
            expected_names = list(expected_cols.keys())
            extra = sorted(set(actual_cols) - set(expected_names))
            print(f"  ✓ {table}: all {len(expected_names)} expected columns present" +
                  (f" (+{len(extra)} extra: {', '.join(extra)})" if extra else ""))

    # ── Data checks ──
    print("\n── Data quality checks ──")
    all_issues.extend(check_metadata_json_has_last_price(conn))
    all_issues.extend(check_tax_assumptions_default(conn))

    check_row_counts(conn)
    check_portfolio_tickers(conn)
    check_sample_position(conn)
    check_wash_sale_query(conn)

    # ── Summary ──
    print(f"\n{'=' * 70}")
    critical = [i for i in all_issues if i.startswith("MISSING")]
    warnings = [i for i in all_issues if i.startswith("WARNING") or i.startswith("BAD")]

    if not all_issues:
        print("✓ ALL CHECKS PASSED — Investor DB schema is fully compatible with HMM adapter")
    else:
        if critical:
            print(f"\n✗ {len(critical)} CRITICAL issue(s):")
            for issue in critical:
                print(f"    {issue}")
        if warnings:
            print(f"\n⚠ {len(warnings)} warning(s):")
            for issue in warnings:
                print(f"    {issue}")
        if critical:
            print("\nThe HMM adapter WILL FAIL with this database. Fix critical issues first.")
        else:
            print("\nThe HMM adapter should work but may use fallback values for missing data.")

    conn.close()
    sys.exit(1 if critical else 0)


if __name__ == "__main__":
    main()
