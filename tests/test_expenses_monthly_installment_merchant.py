from __future__ import annotations

from src.investor.expenses.normalize import normalize_merchant


def test_monthly_installments_normalizes_to_apple() -> None:
    assert normalize_merchant("Monthly Installments (12 Of 24)") == "Apple"

