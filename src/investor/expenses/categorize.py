from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import yaml
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from src.db.models import ExpenseRule, ExpenseTransaction
from src.investor.expenses.config import CategorizationConfig


class RuleMatch(BaseModel):
    merchant_exact: Optional[str] = None
    merchant_regex: Optional[str] = None
    description_regex: Optional[str] = None
    category_hint_exact: Optional[str] = None


class CategoryRule(BaseModel):
    name: str
    priority: int = 0
    category: str
    match: RuleMatch = Field(default_factory=RuleMatch)


class RulesFile(BaseModel):
    version: int = 1
    categories: list[str] = Field(default_factory=list)
    transfer_keywords: list[str] = Field(default_factory=list)
    income_keywords: list[str] = Field(default_factory=list)
    rules: list[CategoryRule] = Field(default_factory=list)


@dataclass(frozen=True)
class CompiledRule:
    name: str
    priority: int
    category: str
    merchant_exact: Optional[str]
    merchant_regex: Optional[re.Pattern[str]]
    description_regex: Optional[re.Pattern[str]]
    category_hint_exact: Optional[str]


@dataclass(frozen=True)
class CompiledRules:
    categories: list[str]
    transfer_keywords: list[str]
    income_keywords: list[str]
    rules: list[CompiledRule]


def _compile_rule(r: CategoryRule) -> CompiledRule:
    return CompiledRule(
        name=r.name,
        priority=int(r.priority),
        category=r.category,
        merchant_exact=(r.match.merchant_exact.strip() if r.match.merchant_exact else None),
        merchant_regex=(re.compile(r.match.merchant_regex, re.IGNORECASE) if r.match.merchant_regex else None),
        description_regex=(re.compile(r.match.description_regex, re.IGNORECASE) if r.match.description_regex else None),
        category_hint_exact=(r.match.category_hint_exact.strip() if r.match.category_hint_exact else None),
    )


def load_rules(path: Path, *, defaults: CategorizationConfig) -> CompiledRules:
    data = yaml.safe_load(path.read_text()) or {}
    rf = RulesFile.model_validate(data)
    categories = rf.categories or defaults.categories
    transfer_keywords = rf.transfer_keywords or defaults.transfer_keywords
    income_keywords = rf.income_keywords or defaults.income_keywords
    compiled: list[CompiledRule] = []
    for r in rf.rules:
        compiled.append(_compile_rule(r))
    compiled.sort(key=lambda x: (-x.priority, x.name))
    return CompiledRules(
        categories=categories,
        transfer_keywords=[k.lower() for k in transfer_keywords],
        income_keywords=[k.lower() for k in income_keywords],
        rules=compiled,
    )


def load_db_rules(*, session: Session) -> list[CompiledRule]:
    """
    Optional DB-stored categorization rules. These are merged with YAML rules
    during `apply_rules_to_db`, giving a deterministic "learned rules" path
    for the Web UI without mutating the YAML file.
    """
    out: list[CompiledRule] = []
    q = (
        session.query(ExpenseRule)
        .filter(ExpenseRule.enabled.is_(True))
        .order_by(ExpenseRule.priority.desc(), ExpenseRule.name.asc())
    )
    for row in q:
        try:
            r = CategoryRule.model_validate(row.json_definition or {})
        except Exception:
            continue
        out.append(_compile_rule(r))
    out.sort(key=lambda x: (-x.priority, x.name))
    return out


def compile_rules(
    *,
    session: Session,
    rules_path: Path,
    defaults: CategorizationConfig,
) -> CompiledRules:
    """
    Returns compiled rules even if the YAML rules file doesn't exist yet.
    This enables deterministic categorization (keyword + hint + DB learned rules)
    without requiring a YAML file for the MVP web UI.
    """
    if rules_path.exists():
        yaml_rules = load_rules(rules_path, defaults=defaults)
    else:
        yaml_rules = CompiledRules(
            categories=defaults.categories,
            transfer_keywords=[k.lower() for k in defaults.transfer_keywords],
            income_keywords=[k.lower() for k in defaults.income_keywords],
            rules=[],
        )
    db_rules = load_db_rules(session=session)
    return CompiledRules(
        categories=yaml_rules.categories,
        transfer_keywords=yaml_rules.transfer_keywords,
        income_keywords=yaml_rules.income_keywords,
        rules=sorted(db_rules + yaml_rules.rules, key=lambda x: (-x.priority, x.name)),
    )


def _keyword_category(desc: str, *, rules: CompiledRules, amount: float) -> Optional[str]:
    s = (desc or "").lower()
    if amount < 0 and "monthly installment" in s:
        return "Apple Installment Payment"
    if amount < 0 and "aplpay" in s:
        return "Apple Pay"
    # Bank-side credit card payments often show up as "payment ... card ending in ####".
    # Treat them as Payments so they don't inflate spend when multiple accounts are combined.
    if amount < 0 and "payment" in s and "card" in s:
        if any(k in s for k in ["ending in", "card ending", "gsbank", "applecard", "credit card", "chase card", "amex", "american express"]):
            return "Payments"
    # GS Apple Card exports can label card payments as "transfer" in the description.
    # Treat common "transfer from account ending" patterns as Payments when the amount is positive.
    if amount > 0 and "transfer" in s:
        if any(p in s for p in ["transfer from account ending", "internet transfer from account ending", "ach deposit internet transfer"]):
            return "Payments"
    # Credit-card payments / bill-pay are transfers, not spend (avoids double-counting when both
    # bank and card statements are imported).
    if "autopay" in s and "payment" in s:
        return "Payments"
    if amount < 0 and "payment" in s:
        cc_markers = ["credit card", "card payment", "cc payment", "amex", "american express", "visa", "mastercard", "discover"]
        if any(m in s for m in cc_markers):
            return "Payments"
    if any(k in s for k in rules.transfer_keywords):
        return "Transfers"
    if amount > 0 and any(k in s for k in rules.income_keywords):
        return "Income"
    return None


def categorize_one(
    *,
    merchant_norm: str,
    description_norm: str,
    amount: float,
    category_hint: Optional[str],
    rules: CompiledRules,
) -> tuple[str, Optional[str]]:
    kw = _keyword_category(description_norm, rules=rules, amount=amount)
    if kw:
        return kw, "keyword"
    for r in rules.rules:
        if r.merchant_exact and merchant_norm.strip().lower() != r.merchant_exact.strip().lower():
            continue
        if r.merchant_regex and not r.merchant_regex.search(merchant_norm):
            continue
        if r.description_regex and not r.description_regex.search(description_norm):
            continue
        if r.category_hint_exact and (category_hint or "").strip().lower() != r.category_hint_exact.strip().lower():
            continue
        return r.category, r.name
    if category_hint:
        h = category_hint.lower()
        # If hint matches a known category name, trust it (deterministic).
        for c in rules.categories:
            if c and h.strip() == c.lower():
                return c, "hint"
        if "payment" in h:
            return "Payments", "hint"
        if "credit" in h or "refund" in h or "return" in h:
            return "Merchant Credits", "hint"
        if "grocery" in h:
            return "Groceries", "hint"
        if "restaurant" in h or "food" in h or "dining" in h:
            return "Dining", "hint"
        if "gas" in h or "fuel" in h:
            return "Fuel", "hint"
        if "travel" in h or "air" in h or "hotel" in h:
            return "Travel", "hint"
        if "shop" in h or "merch" in h:
            return "Shopping", "hint"
        if "bill" in h or "utilit" in h:
            return "Utilities", "hint"
        if "health" in h or "medical" in h:
            return "Health", "hint"
        if "insur" in h:
            return "Insurance", "hint"
        if "stream" in h or "subscription" in h:
            return "Subscriptions", "hint"
        if "fee" in h or "interest" in h:
            return "Fees", "hint"
        if "tax" in h:
            return "Taxes", "hint"
    # Heuristic: rows that look like credits/refunds (may be positive after sign normalization).
    s = (description_norm or "").lower()
    if any(k in s for k in ["merchant credit", "chargeback", "refund", "return", "reversal"]):
        return "Merchant Credits", "keyword"
    return "Unknown", None


def apply_rules_to_db(
    *,
    session: Session,
    rules_path: Path,
    config: CategorizationConfig,
    rebuild: bool,
) -> tuple[int, int]:
    rules = compile_rules(session=session, rules_path=rules_path, defaults=config)
    updated = 0
    skipped_user = 0
    q = session.query(ExpenseTransaction).order_by(ExpenseTransaction.posted_date.asc(), ExpenseTransaction.id.asc())
    for t in q:
        if t.category_user:
            skipped_user += 1
            continue
        if (t.category_system or "").strip() and not rebuild:
            continue
        category, _rule = categorize_one(
            merchant_norm=t.merchant_norm or "Unknown",
            description_norm=t.description_norm or "",
            amount=float(t.amount),
            category_hint=t.category_hint,
            rules=rules,
        )
        t.category_system = category
        updated += 1
    session.commit()
    return updated, skipped_user


def write_starter_rules(path: Path, *, config: CategorizationConfig, force: bool = False) -> None:
    if path.exists() and not force:
        raise FileExistsError(f"Rules file already exists: {path}")
    starter = {
        "version": 1,
        "categories": config.categories,
        "transfer_keywords": config.transfer_keywords,
        "income_keywords": config.income_keywords,
        "rules": [
            {
                "name": "Amazon shopping",
                "priority": 100,
                "category": "Shopping",
                "match": {"merchant_exact": "Amazon"},
            },
            {
                "name": "Starbucks dining",
                "priority": 90,
                "category": "Dining",
                "match": {"merchant_exact": "Starbucks"},
            },
            {
                "name": "Grocery stores (heuristic)",
                "priority": 10,
                "category": "Groceries",
                "match": {"description_regex": r"\\b(whole foods|trader joe|kroger|safeway)\\b"},
            },
        ],
    }
    path.write_text(yaml.safe_dump(starter, sort_keys=False))
