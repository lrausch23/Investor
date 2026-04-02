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
        path.unlink()
    if getattr(session_module, "_ENGINE", None) is not None:
        try:
            session_module._ENGINE.dispose()
        except Exception:
            pass
        session_module._ENGINE = None
    init_db()
    print(f"Reset DB at {path}")


if __name__ == "__main__":
    main()
