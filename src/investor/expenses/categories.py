from __future__ import annotations

from sqlalchemy.orm import Session

from src.db.models import ExpenseCategory
from src.investor.expenses.config import CategorizationConfig


def normalize_category_name(name: str) -> str:
    return " ".join((name or "").strip().split())


def ensure_category(session: Session, *, name: str) -> str:
    n = normalize_category_name(name)
    if not n:
        raise ValueError("Category name is required")
    # Case-insensitive "get or create" without extra DB dependencies:
    existing = session.query(ExpenseCategory).filter(ExpenseCategory.name.ilike(n)).one_or_none()
    if existing is not None:
        return existing.name
    row = ExpenseCategory(name=n)
    session.add(row)
    session.flush()
    return row.name


def list_categories(session: Session, *, config: CategorizationConfig) -> list[str]:
    base = [normalize_category_name(c) for c in (config.categories or []) if normalize_category_name(c)]
    db_rows = session.query(ExpenseCategory.name).order_by(ExpenseCategory.name.asc()).all()
    extra = [normalize_category_name(r[0]) for r in db_rows if normalize_category_name(r[0])]
    merged: list[str] = []
    seen = set()
    for c in base + extra:
        k = c.lower()
        if k in seen:
            continue
        seen.add(k)
        merged.append(c)
    return merged

