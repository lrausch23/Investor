from __future__ import annotations

import os
from pathlib import Path
import sys
import shutil
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.db.init_db import init_db
from src.db import session as session_module


def _sqlite_sidecar_paths(path: Path) -> list[Path]:
    return [
        path.with_name(f"{path.name}-wal"),
        path.with_name(f"{path.name}-shm"),
        path.with_name(f"{path.name}-journal"),
    ]


def _remove_sqlite_files(path: Path) -> None:
    for candidate in [path, *_sqlite_sidecar_paths(path)]:
        if candidate.exists():
            candidate.unlink()


def main() -> None:
    db_path = os.environ.get("DATABASE_URL", "sqlite:///./data/investor.db").replace(
        "sqlite:///", ""
    )
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        archive = path.with_name(f"{path.stem}_backup_{timestamp}{path.suffix}")
        shutil.copy2(path, archive)
        print(f"Archived DB at {archive}")
    if getattr(session_module, "_ENGINE", None) is not None:
        try:
            session_module._ENGINE.dispose()
        except Exception:
            pass
        session_module._ENGINE = None
    _remove_sqlite_files(path)
    init_db()
    print(f"Reset DB at {path}")


if __name__ == "__main__":
    main()
