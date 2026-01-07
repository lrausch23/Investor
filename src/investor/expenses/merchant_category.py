from __future__ import annotations

import hashlib

from sqlalchemy import func
from sqlalchemy.orm import Session

from src.db.models import ExpenseRule, ExpenseTransaction


def _rule_name_for_merchant(merchant: str) -> str:
    m = (merchant or "").strip().casefold()
    h = hashlib.sha1(m.encode("utf-8")).hexdigest()[:12]
    prefix = "merchant_category:exact:"
    head = m[: (200 - len(prefix) - 1 - len(h))].strip() if m else ""
    if head:
        return f"{prefix}{head}:{h}"
    return f"{prefix}{h}"


def set_merchant_category(
    *,
    session: Session,
    merchant: str,
    category: str,
    priority: int = 900,
) -> int:
    """
    Persist a merchant->category mapping (DB rule) and apply it to existing charge transactions
    (amount < 0) that don't have a user override.

    Returns number of transactions updated.
    """
    m = (merchant or "").strip()
    c = (category or "").strip()
    if not m:
        raise ValueError("Merchant is required")
    if not c:
        raise ValueError("Category is required")

    name = _rule_name_for_merchant(m)
    definition = {
        "name": f"Merchant category: {m}",
        "priority": int(priority),
        "category": c,
        "match": {"merchant_exact": m},
    }
    existing = session.query(ExpenseRule).filter(ExpenseRule.name == name).one_or_none()
    if existing is None:
        session.add(
            ExpenseRule(
                name=name,
                priority=int(priority),
                enabled=True,
                json_definition=definition,
            )
        )
    else:
        existing.priority = int(priority)
        existing.enabled = True
        existing.json_definition = definition

    updated = (
        session.query(ExpenseTransaction)
        .filter(
            func.lower(func.trim(func.coalesce(ExpenseTransaction.merchant_norm, ""))) == m.lower(),
            ExpenseTransaction.category_user.is_(None),
            ExpenseTransaction.amount < 0,
        )
        .update({ExpenseTransaction.category_system: c}, synchronize_session=False)
    )
    return int(updated or 0)
