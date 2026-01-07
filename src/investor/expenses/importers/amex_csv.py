from __future__ import annotations

from decimal import Decimal
from typing import Iterable

from src.investor.expenses.importers.base import StatementImporter
from src.investor.expenses.models import RawTxn
from src.investor.expenses.normalize import extract_last4_digits, parse_date, parse_decimal


class AmexCSVImporter(StatementImporter):
    format_name = "amex_csv"

    def detect(self, headers: Iterable[str]) -> bool:
        hs = {h.strip().lower() for h in headers}
        if not {"date", "description"}.issubset(hs):
            return False
        # Prefer distinguishing headers to avoid ambiguity with generic bank exports.
        amex_markers = {"card member", "account #", "reference", "debit", "credit"}
        if hs.intersection(amex_markers):
            return True
        return False

    def parse_rows(self, *, rows: list[dict[str, str]], default_currency: str) -> list[RawTxn]:
        out: list[RawTxn] = []
        for r in rows:
            posted = parse_date(r.get("Date", "") or r.get("date", ""))
            desc = (r.get("Description", "") or r.get("description", "")).strip()
            currency = (r.get("Currency", "") or r.get("currency", "")).strip().upper() or default_currency
            category_hint = (r.get("Category", "") or r.get("category", "")).strip() or None
            external_id = (r.get("Reference", "") or r.get("reference", "")).strip() or None
            cardholder = (r.get("Card Member", "") or r.get("card member", "")).strip() or None
            acct_last4 = extract_last4_digits(r.get("Account #", "") or r.get("account #", "") or "")
            if (r.get("Debit") or r.get("debit")) and (r.get("Credit") or r.get("credit")):
                debit = r.get("Debit", "") or r.get("debit", "")
                credit = r.get("Credit", "") or r.get("credit", "")
                if debit.strip():
                    amt = -abs(parse_decimal(debit))
                else:
                    amt = abs(parse_decimal(credit))
            else:
                amt = parse_decimal(r.get("Amount", "") or r.get("amount", ""))
                # Canonical sign: charges/debits negative; payments/credits positive.
                desc_l = desc.lower()
                hint_l = (category_hint or "").lower()
                is_payment = ("payment" in desc_l) or ("autopay" in desc_l) or ("payment" in hint_l)
                is_credit = any(k in desc_l for k in ["credit", "refund", "return", "reversal", "chargeback"]) or any(
                    k in hint_l for k in ["credit", "refund", "return"]
                )
                if is_payment or is_credit:
                    amt = abs(amt)
                else:
                    amt = -abs(amt)
            out.append(
                RawTxn(
                    posted_date=posted,
                    transaction_date=None,
                    description=desc,
                    amount=Decimal(amt),
                    currency=currency,
                    category_hint=category_hint,
                    external_id=external_id,
                    account_last4=acct_last4,
                    cardholder_name=cardholder,
                    raw=r,
                )
            )
        return out
