from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import sqlite3


def main(argv: list[str]) -> int:
    if len(argv) != 4:
        print("Usage: python scripts/set_connection_data_dir.py <db_path> <connection_id> <data_dir>")
        return 2
    db_path = argv[1]
    connection_id = int(argv[2])
    data_dir = str(Path(os.path.expanduser(argv[3])).resolve())

    con = sqlite3.connect(db_path)
    try:
        cur = con.cursor()
        cur.execute("select metadata_json from external_connections where id = ?", (connection_id,))
        row = cur.fetchone()
        if row is None:
            print(f"Connection not found: id={connection_id}")
            return 1
        raw = row[0]
        meta = json.loads(raw) if isinstance(raw, str) and raw.strip() else {}
        if not isinstance(meta, dict):
            meta = {}
        old = dict(meta)
        meta["data_dir"] = data_dir
        cur.execute(
            "update external_connections set metadata_json = ? where id = ?",
            (json.dumps(meta), connection_id),
        )
        con.commit()
    finally:
        con.close()

    Path(data_dir).mkdir(parents=True, exist_ok=True)
    print(f"Updated connection {connection_id} data_dir:")
    print(f"  old: {old.get('data_dir')}")
    print(f"  new: {data_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))

