from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from src.db.models import Base
from src.investor.expenses.categories import ensure_category, list_categories
from src.investor.expenses.config import CategorizationConfig


def test_ensure_category_is_case_insensitive() -> None:
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    session = Session(engine)
    try:
        a = ensure_category(session, name="Books")
        b = ensure_category(session, name="  books  ")
        assert a == b
        session.commit()
        cats = list_categories(session, config=CategorizationConfig())
        assert any(c.lower() == "books" for c in cats)
    finally:
        session.close()

