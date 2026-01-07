from __future__ import annotations

from decimal import Decimal
from typing import Iterable

from src.investor.expenses.importers.base import StatementImporter
from src.investor.expenses.models import RawTxn
from src.investor.expenses.normalize import parse_date, parse_decimal


class AppleCardCSVImporter(StatementImporter):
    format_name = "apple_card_csv"

    def detect(self, headers: Iterable[str]) -> bool:
        hs = {h.strip().lower() for h in headers}
        required = {"transaction date", "clearing date", "description", "merchant", "amount (usd)"}
        if not required.issubset(hs):
            return False
        # Distinguish from generic bank files.
        return "purchased by" in hs or "amount (usd)" in hs

    def parse_rows(self, *, rows: list[dict[str, str]], default_currency: str) -> list[RawTxn]:
        out: list[RawTxn] = []
        for r in rows:
            posted = parse_date(r.get("Clearing Date", "") or r.get("clearing date", ""))
            txn_date_s = r.get("Transaction Date", "") or r.get("transaction date", "")
            txn_date = parse_date(txn_date_s) if txn_date_s.strip() else None

            desc = (r.get("Description", "") or r.get("description", "")).strip()
            merch = (r.get("Merchant", "") or r.get("merchant", "")).strip()
            description = f"{merch} - {desc}" if merch else desc

            category_hint = (r.get("Category", "") or r.get("category", "")).strip() or None
            typ = (r.get("Type", "") or r.get("type", "")).strip()
            purchaser = (r.get("Purchased By", "") or r.get("purchased by", "")).strip() or None

            amt_raw = r.get("Amount (USD)", "") or r.get("amount (usd)", "") or r.get("amount", "")
            amt = parse_decimal(amt_raw)

            # Canonical sign: charges negative; payments/credits positive.
            typ_l = (typ or "").lower()
            hint_l = (category_hint or "").lower()
            desc_l = (desc or "").lower()
            is_payment = (
                ("payment" in typ_l)
                or ("payment" in hint_l)
                or ("payment" in desc_l)
                or ("autopay" in desc_l)
                or ("transfer from account ending" in desc_l)
                or ("internet transfer from account ending" in desc_l)
                or ("ach deposit internet transfer" in desc_l)
                or (hint_l == "transfer" and typ_l == "payment")
            )
            is_credit = any(k in typ_l for k in ["credit", "refund", "return"]) or any(k in hint_l for k in ["credit", "refund", "return"])
            if is_payment or is_credit:
                amt = abs(amt)
            else:
                amt = -abs(amt)

            # Help downstream categorization: treat "Transfer" category rows that are payments as Payments.
            if is_payment and hint_l == "transfer":
                category_hint = "Payment"

            out.append(
                RawTxn(
                    posted_date=posted,
                    transaction_date=txn_date,
                    description=description,
                    amount=Decimal(amt),
                    currency=default_currency,
                    category_hint=category_hint,
                    external_id=None,
                    raw=r,
                    account_last4=None,
                    cardholder_name=purchaser,
                )
            )
        return out
