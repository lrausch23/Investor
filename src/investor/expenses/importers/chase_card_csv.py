from __future__ import annotations

from decimal import Decimal
from typing import Iterable

from src.investor.expenses.importers.base import StatementImporter
from src.investor.expenses.models import RawTxn
from src.investor.expenses.normalize import parse_date, parse_decimal


class ChaseCardCSVImporter(StatementImporter):
    format_name = "chase_card_csv"

    def detect(self, headers: Iterable[str]) -> bool:
        hs = {h.strip().lower() for h in headers}
        required = {"transaction date", "post date", "description", "amount"}
        return required.issubset(hs)

    def parse_rows(self, *, rows: list[dict[str, str]], default_currency: str) -> list[RawTxn]:
        out: list[RawTxn] = []
        for r in rows:
            posted = parse_date(r.get("Post Date", "") or r.get("post date", ""))
            txn_date_s = r.get("Transaction Date", "") or r.get("transaction date", "")
            txn_date = parse_date(txn_date_s) if txn_date_s.strip() else None
            desc = (r.get("Description", "") or r.get("description", "")).strip()
            amt = parse_decimal(r.get("Amount", "") or r.get("amount", ""))
            typ = (r.get("Type", "") or r.get("type", "")).strip().lower()
            category_hint = (r.get("Category", "") or r.get("category", "")).strip() or None
            # Canonical: debit negative, credit positive.
            if typ:
                if any(k in typ for k in ["sale", "purchase", "charge"]):
                    amt = -abs(amt)
                elif any(k in typ for k in ["fee", "interest"]):
                    amt = -abs(amt)
                elif any(k in typ for k in ["payment", "credit", "return", "refund"]):
                    amt = abs(amt)
            else:
                # Some Chase exports omit Type; use stable heuristics.
                s = desc.lower()
                h = (category_hint or "").lower()
                if "payment" in s or "payment" in h or "credit" in h:
                    amt = abs(amt)
                else:
                    amt = -abs(amt)
            currency = (r.get("Currency", "") or r.get("currency", "")).strip().upper() or default_currency
            out.append(
                RawTxn(
                    posted_date=posted,
                    transaction_date=txn_date,
                    description=desc,
                    amount=Decimal(amt),
                    currency=currency,
                    category_hint=category_hint,
                    external_id=None,
                    account_last4=None,
                    cardholder_name=None,
                    raw=r,
                )
            )
        return out
