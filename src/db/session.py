from __future__ import annotations

import os

from sqlalchemy import create_engine
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
    connect_args = {"check_same_thread": False} if url.startswith("sqlite") else {}
    _ENGINE = create_engine(url, future=True, connect_args=connect_args)
    return _ENGINE


SessionLocal = sessionmaker(bind=get_engine(), class_=Session, autoflush=False, autocommit=False)


def get_session() -> Session:
    return SessionLocal()
