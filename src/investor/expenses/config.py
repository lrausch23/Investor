from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

import yaml
from pydantic import BaseModel, Field


class RedactionConfig(BaseModel):
    enabled: bool = True
    patterns: list[str] = Field(
        default_factory=lambda: [
            r"(?<!\d)\d{8,}(?!\d)",  # long digit runs (account numbers, reference ids)
        ]
    )


class CategorizationConfig(BaseModel):
    rules_path: str = "expenses_rules.yaml"
    categories: list[str] = Field(
        default_factory=lambda: [
            "Housing",
            "Utilities",
            "Groceries",
            "Dining",
            "Fuel",
            "Travel",
            "Insurance",
            "Health",
            "Subscriptions",
            "Shopping",
            "Apple Pay",
            "Apple Installment Payment",
            "Transfers",
            "Payments",
            "Merchant Credits",
            "Income",
            "Fees",
            "Taxes",
            "Unknown",
        ]
    )
    transfer_keywords: list[str] = Field(
        default_factory=lambda: ["transfer", "zelle", "venmo", "paypal", "cash app", "ach", "wire"]
    )
    income_keywords: list[str] = Field(default_factory=lambda: ["payroll", "salary", "paycheck", "interest", "dividend"])
    budgets_monthly: dict[str, float] = Field(default_factory=dict)


class OptionalLLMAssistConfig(BaseModel):
    enabled: bool = False
    provider: Optional[str] = None
    api_key_env: str = "LLM_API_KEY"


class ExpensesConfig(BaseModel):
    db_path: Optional[str] = None  # defaults to DATABASE_URL / investor.db
    imports_dir: str = "statements"
    default_currency: str = "USD"
    provider_formats: list[str] = Field(
        default_factory=lambda: ["chase_card_csv", "chase_bank_csv", "amex_csv", "apple_card_csv", "generic_bank_csv"]
    )
    categorization: CategorizationConfig = Field(default_factory=CategorizationConfig)
    redaction: RedactionConfig = Field(default_factory=RedactionConfig)
    optional_llm_assist: OptionalLLMAssistConfig = Field(default_factory=OptionalLLMAssistConfig)


def _candidate_paths() -> list[Path]:
    paths = [Path("expenses.yaml")]
    home = Path(os.path.expanduser("~"))
    paths.append(home / ".bucketmgr" / "expenses.yaml")
    return paths


def load_expenses_config() -> tuple[ExpensesConfig, Optional[str]]:
    for p in _candidate_paths():
        if p.exists():
            data = yaml.safe_load(p.read_text()) or {}
            return ExpensesConfig.model_validate(data.get("expenses") or data), str(p)
    return ExpensesConfig(), None
