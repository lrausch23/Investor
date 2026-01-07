from __future__ import annotations

from decimal import Decimal
from pathlib import Path

from src.investor.expenses.importers import default_importers
from src.investor.expenses.importers.base import read_csv_rows


def test_detect_and_parse_apple_card_tsv() -> None:
    content = Path("tests/fixtures/expenses/apple_card_sample.tsv").read_text()
    headers, rows = read_csv_rows(content)
    imp = next(i for i in default_importers() if i.format_name == "apple_card_csv")
    assert imp.detect(headers)
    txns = imp.parse_rows(rows=rows, default_currency="USD")
    assert len(txns) == 3

    # Payment should be positive in canonical schema.
    assert "Payment" in (txns[0].category_hint or "")
    assert txns[0].amount == Decimal("312.39")
    assert txns[0].cardholder_name == "Laszlo Rausch"

    # Purchase should be negative.
    assert txns[1].amount == Decimal("-5.67")
    assert "Starbucks" in txns[1].description

    # Credit should be positive.
    assert txns[2].amount == Decimal("10.00")
    assert txns[2].cardholder_name == "Milana Kulynych"

