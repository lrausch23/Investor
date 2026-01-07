from __future__ import annotations

import re
from decimal import Decimal
from typing import Iterable

from src.investor.expenses.importers.base import StatementImporter
from src.investor.expenses.models import RawTxn
from src.investor.expenses.normalize import extract_last4_digits, parse_date, parse_decimal


_ELLIPSIS_LAST4_RE = re.compile(r"\.\.\.\s*(\d{3,4})\b")


class ChaseBankCSVImporter(StatementImporter):
    """
    Chase checking/savings export (CSV/TSV) variant.

    Sample headers:
      Details, Posting Date, Description, Amount, Type, Balance, Check or Slip #
    """

    format_name = "chase_bank_csv"

    def detect(self, headers: Iterable[str]) -> bool:
        hs = {h.strip().lower() for h in headers}
        required = {"details", "posting date", "description", "amount"}
        if not required.issubset(hs):
            return False
        # Distinguish from Chase credit card exports.
        if hs.issuperset({"transaction date", "post date"}):
            return False
        return True

    def parse_rows(self, *, rows: list[dict[str, str]], default_currency: str) -> list[RawTxn]:
        out: list[RawTxn] = []
        for r in rows:
            posted = parse_date(r.get("Posting Date", "") or r.get("posting date", ""))
            desc = (r.get("Description", "") or r.get("description", "")).strip()
            amt = parse_decimal(r.get("Amount", "") or r.get("amount", ""))
            details = (r.get("Details", "") or r.get("details", "")).strip().lower()
            typ = (r.get("Type", "") or r.get("type", "")).strip().lower()

            # Canonical: money out negative, money in positive.
            if details in {"debit", "withdrawal"}:
                amt = -abs(amt)
            elif details in {"credit", "deposit"}:
                amt = abs(amt)
            else:
                # Fallback heuristic.
                if any(k in typ for k in ["dep", "credit"]):
                    amt = abs(amt)
                elif any(k in typ for k in ["debit", "wd", "withdraw", "xfer"]):
                    amt = -abs(amt)

            # Try to extract masked last4 from strings like "...0787".
            last4 = None
            m = _ELLIPSIS_LAST4_RE.search(desc)
            if m:
                last4 = extract_last4_digits(m.group(1))

            out.append(
                RawTxn(
                    posted_date=posted,
                    transaction_date=None,
                    description=desc,
                    amount=Decimal(amt),
                    currency=default_currency,
                    category_hint=None,
                    external_id=None,
                    account_last4=last4,
                    cardholder_name=None,
                    raw=r,
                )
            )
        return out

