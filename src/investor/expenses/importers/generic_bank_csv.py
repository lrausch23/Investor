from __future__ import annotations

from decimal import Decimal
from typing import Iterable

from src.investor.expenses.importers.base import StatementImporter
from src.investor.expenses.models import RawTxn
from src.investor.expenses.normalize import parse_date, parse_decimal


class GenericBankCSVImporter(StatementImporter):
    format_name = "generic_bank_csv"

    def detect(self, headers: Iterable[str]) -> bool:
        hs = {h.strip().lower() for h in headers}
        if not {"date", "description", "amount"}.issubset(hs):
            return False
        # Avoid ambiguous matches with card exports that include richer identifiers.
        if hs.intersection({"card member", "account #", "reference", "debit", "credit"}):
            return False
        return True

    def parse_rows(self, *, rows: list[dict[str, str]], default_currency: str) -> list[RawTxn]:
        out: list[RawTxn] = []
        for r in rows:
            posted = parse_date(r.get("Date", "") or r.get("date", ""))
            desc = (r.get("Description", "") or r.get("description", "")).strip()
            amt = parse_decimal(r.get("Amount", "") or r.get("amount", ""))
            currency = (r.get("Currency", "") or r.get("currency", "")).strip().upper() or default_currency
            out.append(
                RawTxn(
                    posted_date=posted,
                    transaction_date=None,
                    description=desc,
                    amount=Decimal(amt),
                    currency=currency,
                    category_hint=None,
                    external_id=None,
                    account_last4=None,
                    cardholder_name=None,
                    raw=r,
                )
            )
        return out
