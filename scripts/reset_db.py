from __future__ import annotations

import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.db.session import get_engine
from src.db.models import Base


def main() -> None:
    engine = get_engine()
    db_path = os.environ.get("DATABASE_URL", "sqlite:///./data/investor.db").replace(
        "sqlite:///", ""
    )
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        path.unlink()
    Base.metadata.create_all(bind=engine)
    print(f"Reset DB at {path}")


if __name__ == "__main__":
    main()
