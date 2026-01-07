from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path


def main() -> int:
    p = argparse.ArgumentParser(description="Disable CHASE_YODLEE connections in the local SQLite DB.")
    p.add_argument("--db", default="data/investor.db", help="Path to SQLite DB (default: data/investor.db)")
    p.add_argument("--dry-run", action="store_true", help="Print affected connections but do not modify the DB")
    args = p.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        raise SystemExit(f"DB not found: {db_path}")

    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    try:
        rows = con.execute(
            "select id, name, status, provider, broker, connector "
            "from external_connections "
            "where upper(connector) = 'CHASE_YODLEE' "
            "order by id"
        ).fetchall()
        if not rows:
            print("No CHASE_YODLEE connections found.")
            return 0

        print("CHASE_YODLEE connections:")
        for r in rows:
            print(f"- id={r['id']} name={r['name']!r} status={r['status']} ({r['provider']}/{r['broker']}/{r['connector']})")

        if args.dry_run:
            print("Dry run; no changes made.")
            return 0

        cur = con.execute(
            "update external_connections set status='DISABLED' where upper(connector)='CHASE_YODLEE' and status!='DISABLED'"
        )
        con.commit()
        print(f"Disabled {cur.rowcount} connection(s).")
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())

