from __future__ import annotations

import os

from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker


def get_database_url() -> str:
    return os.environ.get("DATABASE_URL", "sqlite:///./data/investor.db")


_ENGINE: Engine | None = None


def get_engine() -> Engine:
    global _ENGINE
    if _ENGINE is not None:
        return _ENGINE
    url = get_database_url()
    connect_args = {}
    if url.startswith("sqlite"):
        # SQLite is used for local/dev. Configure a busy timeout and WAL to reduce
        # "database is locked" errors under concurrent reads/writes (e.g., during sync + UI browsing).
        try:
            timeout_s = float(os.environ.get("SQLITE_TIMEOUT_S", "30") or "30")
        except Exception:
            timeout_s = 30.0
        connect_args = {"check_same_thread": False, "timeout": timeout_s}

    _ENGINE = create_engine(url, future=True, connect_args=connect_args)

    if url.startswith("sqlite"):
        @event.listens_for(_ENGINE, "connect")
        def _set_sqlite_pragmas(dbapi_connection, _connection_record) -> None:  # type: ignore[no-untyped-def]
            try:
                cur = dbapi_connection.cursor()
                # Wait for locks rather than failing immediately.
                try:
                    timeout_s = float(os.environ.get("SQLITE_TIMEOUT_S", "30") or "30")
                except Exception:
                    timeout_s = 30.0
                try:
                    timeout_ms = int(float(os.environ.get("SQLITE_BUSY_TIMEOUT_MS", str(int(timeout_s * 1000))) or "0"))
                except Exception:
                    timeout_ms = int(timeout_s * 1000)
                cur.execute(f"PRAGMA busy_timeout={timeout_ms}")
                cur.execute("PRAGMA journal_mode=WAL")
                cur.execute("PRAGMA synchronous=NORMAL")
                cur.execute("PRAGMA foreign_keys=ON")
                cur.close()
            except Exception:
                # Best-effort; do not fail app startup.
                return
    return _ENGINE


SessionLocal = sessionmaker(bind=get_engine(), class_=Session, autoflush=False, autocommit=False)


def get_session() -> Session:
    return SessionLocal()
