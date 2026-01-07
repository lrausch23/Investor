from __future__ import annotations

import datetime as dt
import json
import re
from dataclasses import dataclass
from decimal import Decimal
from hashlib import sha256
from typing import Optional

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from src.db.models import Account, ExpenseImportBatch, ExpenseTransaction, Transaction
from src.investor.expenses.db import get_or_create_account
from src.investor.expenses.normalize import (
    money_2dp,
    normalize_description,
    normalize_merchant,
    redact_value,
    stable_txn_id,
)


@dataclass(frozen=True)
class RJCashOutImportResult:
    expense_account_id: int
    rj_account_id: int
    inserted: int
    duplicates_skipped: int
    row_count: int
    file_hash: str


def _sha256_text(s: str) -> str:
    return sha256(s.encode("utf-8")).hexdigest()


_WIRE_TO_RE = re.compile(r"\bWIRE\s+TO\s+(?P<payee>.+)$", re.IGNORECASE)
_TRSF_TO_RE = re.compile(r"\bTRSF\s+TO\s+(?P<payee>.+)$", re.IGNORECASE)
_CHECK_TO_RE = re.compile(r"\bCHECK\s+TO\s+(?P<payee>.+)$", re.IGNORECASE)
_REC_PREFIX_RE = re.compile(r"^\s*REC(\s+FR|\s+TRSF)?\b", re.IGNORECASE)
_AMOUNT_SUFFIX_RE = re.compile(r"\s+[\u00a3\u20ac$]?\(?[\d,]+(?:\.\d{2})?\)?\s*$")


def _pick_text(links: dict, *keys: str) -> str:
    for k in keys:
        v = links.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""


def _looks_internal_transfer(detail: str) -> bool:
    s = (detail or "").strip().upper()
    if not s:
        return False
    if "TRSF TO SHADO ACCT" in s:
        return True
    if "SIS FX SETTLEMENT" in s:
        return True
    return False


def _extract_payee(detail: str) -> str:
    s = (detail or "").strip()
    if not s:
        return ""
    s = s.lstrip("*").strip()
    m = _WIRE_TO_RE.search(s) or _CHECK_TO_RE.search(s) or _TRSF_TO_RE.search(s)
    if m:
        payee = (m.group("payee") or "").strip()
        payee = _AMOUNT_SUFFIX_RE.sub("", payee).strip()
        return payee
    return _AMOUNT_SUFFIX_RE.sub("", s).strip()


def import_rj_cash_outs(
    *,
    session: Session,
    rj_account_id: int,
    expense_account_name: str = "Kolozsi Trust",
    institution: str = "RJ",
    start_date: Optional[dt.date] = None,
    end_date: Optional[dt.date] = None,
    redaction_enabled: bool = True,
    redaction_patterns: Optional[list[re.Pattern[str]]] = None,
) -> RJCashOutImportResult:
    """
    Imports RJ "cash out" (withdrawal/distribution) rows from the core `transactions` table
    into the expenses canonical schema.

    Assumptions (MVP):
    - We treat RJ "cash out" as `Transaction.type == TRANSFER` and `amount < 0`.
    - We use `lot_links_json` fields (description/additional_detail/source_file/source_row) to populate
      expense transaction description + merchant so users can categorize correctly.
    - We skip internal FX-settlement legs (e.g. "TRSF TO SHADO ACCT FOR FX TRAD") to avoid noise.
    - Expense sign convention: money out is negative (spend); we store amount as negative.
    """
    rj = session.query(Account).filter(Account.id == int(rj_account_id)).one_or_none()
    if rj is None:
        raise ValueError(f"RJ account not found: {rj_account_id}")
    if str(getattr(rj, "broker", "")).upper() != "RJ":
        raise ValueError("Selected account is not an RJ brokerage account")

    q = session.query(Transaction).filter(Transaction.account_id == rj.id, Transaction.type == "TRANSFER")
    if start_date:
        q = q.filter(Transaction.date >= start_date)
    if end_date:
        q = q.filter(Transaction.date <= end_date)
    q = q.order_by(Transaction.date.asc(), Transaction.id.asc())

    selected: list[Transaction] = []
    seen_keys: set[str] = set()
    for t in q:
        amt = Decimal(str(t.amount))
        if amt >= 0:
            continue
        links = t.lot_links_json or {}
        detail = _pick_text(links, "additional_detail", "details", "memo")
        if _REC_PREFIX_RE.search(detail):
            continue
        if _looks_internal_transfer(detail):
            continue
        provider_txn_id = _pick_text(links, "provider_txn_id")
        if provider_txn_id:
            k = f"pid:{provider_txn_id}"
        else:
            k = f"fallback:{t.date.isoformat()}|{money_2dp(amt)}|{detail.strip().upper()}"
        if k in seen_keys:
            continue
        seen_keys.add(k)
        selected.append(t)

    # Create/get the expense account.
    acct = get_or_create_account(
        session,
        institution=institution,
        name=expense_account_name,
        last4_masked=None,
        account_type="BROKERAGE",
    )

    # Stable batch hash: derived from the selected RJ rows.
    fp = [
        {
            "rj_account_id": int(rj.id),
            "txn_id": int(t.id),
            "date": t.date.isoformat(),
            "amount": str(money_2dp(Decimal(str(t.amount)))),
            "provider_txn_id": (t.lot_links_json or {}).get("provider_txn_id"),
        }
        for t in selected
    ]
    file_hash = _sha256_text(json.dumps(fp, sort_keys=True, separators=(",", ":")))

    batch = ExpenseImportBatch(
        source="RJ",
        file_name=f"RJ cash outs: {rj.name}",
        file_hash=file_hash,
        row_count=len(selected),
        duplicates_skipped=0,
        metadata_json={
            "format": "rj_cash_out",
            "institution": institution,
            "account_name": expense_account_name,
            "rj_account_id": int(rj.id),
        },
    )
    session.add(batch)
    session.flush()

    inserted = 0
    dupes = 0
    redaction_patterns = redaction_patterns or []
    for t in selected:
        amt = Decimal(str(t.amount))
        outflow = money_2dp(-abs(amt))
        links = t.lot_links_json or {}
        base_desc = _pick_text(links, "description") or "Cash"
        detail = _pick_text(links, "additional_detail", "details", "memo")
        if redaction_enabled and redaction_patterns:
            base_desc = redact_value(base_desc, redaction_patterns)
            detail = redact_value(detail, redaction_patterns)
        raw_desc = " ".join(x for x in [detail, base_desc] if x).strip() or base_desc
        payee = _extract_payee(detail) or raw_desc
        desc_norm = normalize_description(raw_desc)
        merchant_norm = normalize_merchant(normalize_description(payee))
        provider_txn_id = _pick_text(links, "provider_txn_id")
        external_id = provider_txn_id or f"RJ:TX:{t.id}"

        cat = "Unknown"
        hint = None
        if _looks_internal_transfer(detail):
            cat = "Transfers"
            hint = "Transfer"
        txn_id = stable_txn_id(
            institution=institution,
            account_name=expense_account_name,
            posted_date=t.date,
            amount=outflow,
            description_norm=desc_norm,
            currency="USD",
            external_id=external_id,
        )
        tx = ExpenseTransaction(
            txn_id=txn_id,
            expense_account_id=acct.id,
            institution=institution,
            account_name=expense_account_name,
            posted_date=t.date,
            transaction_date=None,
            description_raw=raw_desc,
            description_norm=desc_norm,
            merchant_norm=merchant_norm,
            amount=float(outflow),
            currency="USD",
            account_last4_masked=None,
            cardholder_name=None,
            category_hint=hint,
            category_user=None,
            category_system=cat,
            tags_json=[],
            notes=None,
            import_batch_id=batch.id,
            original_row_json={
                "source": "RJ",
                "transaction_id": int(t.id),
                "provider_txn_id": provider_txn_id or None,
                "description": base_desc or None,
                "additional_detail": detail or None,
                "source_file": _pick_text(links, "source_file") or None,
                "source_row": links.get("source_row"),
            },
        )
        try:
            with session.begin_nested():
                session.add(tx)
                session.flush()
            inserted += 1
        except IntegrityError:
            dupes += 1
            continue

    batch.duplicates_skipped = dupes
    session.commit()

    return RJCashOutImportResult(
        expense_account_id=int(acct.id),
        rj_account_id=int(rj.id),
        inserted=int(inserted),
        duplicates_skipped=int(dupes),
        row_count=len(selected),
        file_hash=file_hash,
    )
