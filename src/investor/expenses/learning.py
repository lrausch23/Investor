from __future__ import annotations

import hashlib
import re
from typing import Optional

from sqlalchemy import func, or_
from sqlalchemy.orm import Session

from src.db.models import ExpenseRule, ExpenseTransaction


_NON_TOKEN_RE = re.compile(r"[^a-z0-9]+")
_STOP_TOKENS = {
    "tst",
    "pp",
    "sq",
    "pos",
    "purchase",
    "payment",
    "online",
    "transfer",
    "debit",
    "credit",
    "card",
    "amex",
    "visa",
    "mastercard",
    "discover",
    "inc",
    "llc",
    "co",
    "corp",
    "company",
    "the",
    "and",
    "of",
    "to",
    "fl",
    "ny",
    "ca",
    "tx",
    "nj",
    "ga",
}


def _tokenize_for_learning(value: str) -> list[str]:
    s = (value or "").strip().lower()
    s = _NON_TOKEN_RE.sub(" ", s)
    parts = [p for p in s.split() if p]
    tokens: list[str] = []
    for p in parts:
        # Keep a few short-but-meaningful brand prefixes.
        if len(p) < 3 and p not in {"mc"}:
            continue
        if p in _STOP_TOKENS:
            continue
        # Light stemming to unify common plurals/possessives across exports:
        # "mcdonalds" vs "mcdonald's", "pounds" vs "pound", etc.
        if len(p) >= 5 and p.endswith("s") and not p.endswith("ss"):
            p = p[:-1]
        # Handle common concatenated prefixes like "mcdonalds" that may appear as
        # "mc donalds" in other exports. Splitting improves learned matching.
        if p.startswith("mc") and len(p) > 4:
            tokens.append("mc")
            rest = p[2:]
            if len(rest) >= 5 and rest.endswith("s") and not rest.endswith("ss"):
                rest = rest[:-1]
            tokens.append(rest)
            continue
        tokens.append(p)
    return tokens


def _learned_rule_name_for_merchant(merchant_norm: str) -> str:
    m = (merchant_norm or "").strip()
    h = hashlib.sha1(m.encode("utf-8")).hexdigest()[:12]
    prefix = "learned:merchant_exact:"
    # Keep name deterministic and within the DB column length (200).
    head = m[: (200 - len(prefix) - 1 - len(h))].strip() if m else ""
    if head:
        return f"{prefix}{head}:{h}"
    return f"{prefix}{h}"


def _learned_rule_name_for_tokens(tokens: list[str]) -> str:
    key = " ".join(tokens).strip()
    h = hashlib.sha1(key.encode("utf-8")).hexdigest()[:12]
    prefix = "learned:merchant_tokens:"
    head = key[: (200 - len(prefix) - 1 - len(h))].strip() if key else ""
    if head:
        return f"{prefix}{head}:{h}"
    return f"{prefix}{h}"


def _merchant_rule_definition(*, merchant_norm: str, category: str, priority: int) -> tuple[str, dict]:
    merchant = (merchant_norm or "").strip()
    tokens = _tokenize_for_learning(merchant)
    if len(tokens) >= 2:
        chosen = tokens[:2]
        name = _learned_rule_name_for_tokens(chosen)
        # Lookahead-based match is robust to punctuation/spacing differences and token order.
        pattern = "".join(f"(?=.*{re.escape(t)})" for t in chosen) + r".*"
        return (
            name,
            {
                "name": f"Learned merchant tokens: {' '.join(chosen)}",
                "priority": int(priority),
                "category": category,
                "match": {"merchant_regex": pattern},
            },
        )
    if len(tokens) == 1:
        tok = tokens[0]
        # Single-token merchants often have minor variations (e.g. MCDONALD'S vs MCDONALDS vs MCDONALDS #1234).
        # Use a word-prefix regex for reasonably-specific tokens.
        if len(tok) >= 4:
            name = _learned_rule_name_for_tokens([tok])
            pattern = r"\b" + re.escape(tok) + r"[A-Za-z0-9]*\b"
            return (
                name,
                {
                    "name": f"Learned merchant token: {tok}",
                    "priority": int(priority),
                    "category": category,
                    "match": {"merchant_regex": pattern},
                },
            )
    # No usable tokens: fall back to exact match.
    name = _learned_rule_name_for_merchant(merchant)
    return (
        name,
        {
            "name": f"Learned merchant: {merchant}",
            "priority": int(priority),
            "category": category,
            "match": {"merchant_exact": merchant},
        },
    )


def learn_unknown_merchant_category(
    *,
    session: Session,
    merchant_norm: str,
    category: str,
    from_category: str = "Unknown",
    priority: int = 1000,
) -> int:
    """
    When a user reclassifies a transaction, apply the same category to other transactions
    for the same merchant (charges only) that don't have a user override,
    and persist a deterministic DB rule so future categorization runs pick it up.

    Returns: number of transactions updated (system category).
    """
    merchant = (merchant_norm or "").strip()
    if not merchant or merchant.lower() == "unknown":
        return 0

    cat = (category or "").strip()
    if not cat:
        return 0

    name, definition = _merchant_rule_definition(merchant_norm=merchant, category=cat, priority=int(priority))

    existing: Optional[ExpenseRule] = session.query(ExpenseRule).filter(ExpenseRule.name == name).one_or_none()
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

    # Apply to other charge transactions for this merchant where the user hasn't explicitly set
    # a category. This is deterministic and ensures newly-learned rules can fix past imports that
    # were categorized by hints (e.g., "Shopping") before the user corrected the merchant.
    user_trim = func.trim(func.coalesce(ExpenseTransaction.category_user, ""))
    q = session.query(ExpenseTransaction).filter(ExpenseTransaction.amount < 0)
    match = (definition.get("match") or {}) if isinstance(definition, dict) else {}
    if isinstance(match, dict) and match.get("merchant_exact"):
        q = q.filter(ExpenseTransaction.merchant_norm == merchant)
    else:
        tokens = _tokenize_for_learning(merchant)[:2]
        for tok in tokens:
            q = q.filter(func.lower(ExpenseTransaction.merchant_norm).like(f"%{tok}%"))

    user_is_empty = func.length(user_trim) == 0
    user_is_unknown = func.lower(user_trim) == "unknown"

    updated = 0
    # 1) Truly unclassified (no user override): update system category.
    updated += int(
        q.filter(or_(ExpenseTransaction.category_user.is_(None), user_is_empty)).update(
            {ExpenseTransaction.category_system: cat},
            synchronize_session=False,
        )
        or 0
    )
    # 2) If user override was explicitly set to "Unknown", treat it as "no override" and clear it,
    #    otherwise the UI will keep showing Unknown even after updating category_system.
    updated += int(
        q.filter(user_is_unknown).update(
            {ExpenseTransaction.category_user: None, ExpenseTransaction.category_system: cat},
            synchronize_session=False,
        )
        or 0
    )
    return int(updated)
