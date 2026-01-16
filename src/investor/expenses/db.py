from __future__ import annotations

import datetime as dt
import hashlib
import re
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from src.db.init_db import init_db
from src.db.models import ExpenseAccount, ExpenseImportBatch, ExpenseTransaction
from src.investor.expenses.config import ExpensesConfig
from src.investor.expenses.importers import default_importers
from src.investor.expenses.importers.base import read_csv_rows
from src.investor.expenses.models import ImportFileResult, RawTxn
from src.investor.expenses.normalize import (
    extract_last4_digits,
    money_2dp,
    normalize_description,
    normalize_merchant,
    normalize_bank_merchant,
    redact_row,
    redact_value,
    stable_txn_id,
)


def ensure_db() -> None:
    # Ensure tables exist (create missing) and baseline bootstrapping is applied.
    init_db()


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def sha256_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _detect_importer(*, headers: list[str], format_override: Optional[str], enabled: list[str]):
    if format_override:
        for imp in default_importers():
            if imp.format_name == format_override:
                return imp
        raise ValueError(f"Unknown format: {format_override}")
    matches = []
    for imp in default_importers():
        if imp.format_name not in enabled:
            continue
        if imp.detect(headers):
            matches.append(imp)
    if not matches:
        raise ValueError(f"Could not detect statement format from headers: {headers[:8]}")
    if len(matches) > 1:
        names = ", ".join(i.format_name for i in matches)
        raise ValueError(f"Ambiguous statement format; matches: {names}. Use --format to override.")
    return matches[0]


def get_or_create_account(
    session: Session,
    *,
    institution: str,
    name: str,
    last4_masked: Optional[str],
    account_type: str,
    scope: Optional[str] = None,
) -> ExpenseAccount:
    q = session.query(ExpenseAccount).filter(
        ExpenseAccount.institution == institution, ExpenseAccount.name == name
    )
    if last4_masked:
        q = q.filter(ExpenseAccount.last4_masked == last4_masked)
    acct = q.one_or_none()
    if acct is not None:
        return acct
    acct = ExpenseAccount(
        institution=institution,
        name=name,
        last4_masked=last4_masked,
        type=account_type,
        scope=(scope or "PERSONAL"),
    )
    session.add(acct)
    session.flush()
    return acct


@dataclass(frozen=True)
class ImportOptions:
    format_name: Optional[str] = None
    fuzzy_dedupe: bool = True
    store_original_rows: bool = False
    allow_duplicate_file_hash: bool = False


def _existing_txn_ids(session: Session, txn_ids: list[str]) -> set[str]:
    if not txn_ids:
        return set()
    out: set[str] = set()
    chunk = 900
    for i in range(0, len(txn_ids), chunk):
        part = txn_ids[i : i + chunk]
        rows = session.execute(select(ExpenseTransaction.txn_id).where(ExpenseTransaction.txn_id.in_(part))).all()
        out.update(r[0] for r in rows)
    return out


def _backfill_duplicate_fields(
    session: Session,
    *,
    txn_id: str,
    account_last4_masked: str | None,
    cardholder_name: str | None,
) -> bool:
    if not account_last4_masked and not cardholder_name:
        return False
    row = session.query(ExpenseTransaction).filter(ExpenseTransaction.txn_id == txn_id).one_or_none()
    if row is None:
        return False
    changed = False
    if account_last4_masked and not (row.account_last4_masked or "").strip():
        row.account_last4_masked = account_last4_masked
        changed = True
    if cardholder_name and not (row.cardholder_name or "").strip():
        row.cardholder_name = cardholder_name
        changed = True
    if changed:
        session.flush()
    return changed


def _fuzzy_duplicate_exists(
    session: Session,
    *,
    expense_account_id: int,
    posted_date: dt.date,
    amount_2dp: Decimal,
    merchant_norm: str,
    description_norm: str,
) -> bool:
    start = posted_date - dt.timedelta(days=1)
    end = posted_date + dt.timedelta(days=1)
    amt = float(money_2dp(amount_2dp))
    alt = -amt if amt != 0 else amt
    candidates = (
        session.query(
            ExpenseTransaction.merchant_norm,
            ExpenseTransaction.description_norm,
            ExpenseTransaction.amount,
        )
        .filter(
            ExpenseTransaction.expense_account_id == expense_account_id,
            ExpenseTransaction.posted_date >= start,
            ExpenseTransaction.posted_date <= end,
            ExpenseTransaction.amount.in_([amt, alt]),
        )
        .limit(50)
        .all()
    )

    def _sig(s: str) -> str:
        # Normalize for dedupe matching across minor importer/normalization changes.
        # Keep only letters/numbers to ignore punctuation like apostrophes/dashes.
        x = (s or "").strip().lower()
        x = re.sub(r"[^a-z0-9]+", "", x)
        # Drop digits to be resilient to store IDs and order suffixes.
        x = re.sub(r"\d+", "", x)
        return x[:120]

    m0 = _sig(merchant_norm)
    d0 = _sig(description_norm)
    for m1_raw, d1_raw, _amt1 in candidates:
        m1 = _sig(str(m1_raw or ""))
        d1 = _sig(str(d1_raw or ""))
        if not m1 and not d1:
            continue
        if m0 and m1 and m0 == m1:
            return True
        if d0 and d1 and d0 == d1:
            return True
        # Containment fallback helps when one normalization is "shorter".
        if m0 and m1 and (m0 in m1 or m1 in m0):
            return True
        if d0 and d1 and (d0 in d1 or d1 in d0):
            return True
    return False


def import_csv_statement(
    *,
    session: Session,
    cfg: ExpensesConfig,
    file_path: Path,
    institution: str,
    account_name: str,
    account_type: str = "UNKNOWN",
    account_last4: Optional[str] = None,
    default_cardholder_name: Optional[str] = None,
    options: Optional[ImportOptions] = None,
) -> ImportFileResult:
    content_b = file_path.read_bytes()
    file_hash = sha256_bytes(content_b)
    content = content_b.decode("utf-8-sig", errors="ignore")
    return import_csv_statement_text(
        session=session,
        cfg=cfg,
        content=content,
        file_name=file_path.name,
        file_hash=file_hash,
        institution=institution,
        account_name=account_name,
        account_type=account_type,
        account_last4=account_last4,
        default_cardholder_name=default_cardholder_name,
        options=options,
    )


def import_csv_statement_text(
    *,
    session: Session,
    cfg: ExpensesConfig,
    content: str,
    file_name: str,
    file_hash: str,
    institution: str,
    account_name: str,
    account_type: str = "UNKNOWN",
    account_last4: Optional[str] = None,
    default_cardholder_name: Optional[str] = None,
    options: Optional[ImportOptions] = None,
) -> ImportFileResult:
    options = options or ImportOptions()
    headers, rows = read_csv_rows(content)
    importer = _detect_importer(headers=headers, format_override=options.format_name, enabled=cfg.provider_formats)

    # Prevent importing the exact same file content into multiple accounts; this is a common source
    # of cross-account duplicates when users forget to label accounts consistently.
    existing_batch = (
        session.query(ExpenseImportBatch)
        .filter(ExpenseImportBatch.file_hash == file_hash)
        .order_by(ExpenseImportBatch.imported_at.desc(), ExpenseImportBatch.id.desc())
        .first()
    )
    if existing_batch is not None and not options.allow_duplicate_file_hash:
        meta = existing_batch.metadata_json or {}
        meta_inst = str(meta.get("institution") or "").strip()
        meta_acct = str(meta.get("account_name") or "").strip()
        same_inst = meta_inst.lower() == institution.strip().lower() if meta_inst else True
        same_acct = meta_acct.lower() == account_name.strip().lower() if meta_acct else True
        if not (same_inst and same_acct):
            raise ValueError(
                f"File already imported for {meta_inst or 'Unknown'} — {meta_acct or 'Unknown'}; "
                f"refusing to import into {institution} — {account_name} to prevent duplicates. "
                f"Use /expenses/purge to delete and re-import."
            )

    raw_txns: list[RawTxn] = []
    warnings: list[str] = []
    parse_fails = 0
    for idx, r in enumerate(rows, start=2):
        try:
            raw_txns.extend(importer.parse_rows(rows=[r], default_currency=cfg.default_currency))
        except Exception as e:
            parse_fails += 1
            if parse_fails <= 5:
                warnings.append(f"Row {idx}: parse failed: {type(e).__name__}: {e}")

    acct_last4 = extract_last4_digits(account_last4 or "") if account_last4 else None
    acct = get_or_create_account(
        session,
        institution=institution,
        name=account_name,
        last4_masked=acct_last4,
        account_type=account_type,
    )

    batch = ExpenseImportBatch(
        source="CSV",
        file_name=file_name,
        file_hash=file_hash,
        row_count=len(raw_txns),
        duplicates_skipped=0,
        metadata_json={
            "format": importer.format_name,
            "institution": institution,
            "account_name": account_name,
        },
    )
    session.add(batch)
    session.flush()

    canonical: list[dict[str, Any]] = []
    redaction_pats = [re.compile(p) for p in cfg.redaction.patterns] if cfg.redaction.enabled else []
    default_ch = (default_cardholder_name or "").strip() or None
    for t in raw_txns:
        desc_raw = redact_value(t.description, redaction_pats) if redaction_pats else t.description
        desc_norm = normalize_description(desc_raw)
        if importer.format_name == "chase_bank_csv":
            merchant_norm = normalize_bank_merchant(desc_raw)
        else:
            merchant_norm = normalize_merchant(desc_norm)
        txn_last4 = extract_last4_digits(t.account_last4 or "") if t.account_last4 else acct_last4
        cardholder = (t.cardholder_name.strip() if t.cardholder_name else None) or default_ch
        txn_id = stable_txn_id(
            institution=institution,
            account_name=account_name,
            posted_date=t.posted_date,
            amount=t.amount,
            description_norm=desc_norm,
            currency=t.currency,
            external_id=t.external_id,
        )
        canonical.append(
            {
                "txn_id": txn_id,
                "posted_date": t.posted_date,
                "transaction_date": t.transaction_date,
                "description_raw": desc_raw,
                "description_norm": desc_norm,
                "merchant_norm": merchant_norm,
                "amount": money_2dp(t.amount),
                "currency": t.currency,
                "category_hint": t.category_hint,
                "account_last4_masked": txn_last4,
                "cardholder_name": cardholder,
                "raw_row": t.raw,
            }
        )

    existing = _existing_txn_ids(session, [c["txn_id"] for c in canonical])
    inserted = 0
    dupes = 0
    fuzzy_dupes = 0
    backfilled = 0

    for c in canonical:
        if c["txn_id"] in existing:
            dupes += 1
            if _backfill_duplicate_fields(
                session,
                txn_id=c["txn_id"],
                account_last4_masked=c.get("account_last4_masked"),
                cardholder_name=c.get("cardholder_name"),
            ):
                backfilled += 1
            continue
        if options.fuzzy_dedupe and _fuzzy_duplicate_exists(
            session,
            expense_account_id=acct.id,
            posted_date=c["posted_date"],
            amount_2dp=c["amount"],
            merchant_norm=c["merchant_norm"],
            description_norm=c["description_norm"],
        ):
            fuzzy_dupes += 1
            continue
        original_row_json = None
        if options.store_original_rows:
            if cfg.redaction.enabled:
                original_row_json = redact_row(c["raw_row"], cfg.redaction.patterns)
            else:
                original_row_json = c["raw_row"]
        tx = ExpenseTransaction(
            txn_id=c["txn_id"],
            expense_account_id=acct.id,
            institution=institution,
            account_name=account_name,
            posted_date=c["posted_date"],
            transaction_date=c["transaction_date"],
            description_raw=c["description_raw"],
            description_norm=c["description_norm"],
            merchant_norm=c["merchant_norm"],
            amount=float(c["amount"]),
            currency=c["currency"],
            account_last4_masked=c["account_last4_masked"],
            cardholder_name=c["cardholder_name"],
            category_hint=c["category_hint"],
            category_system=None,
            tags_json=[],
            notes=None,
            import_batch_id=batch.id,
            original_row_json=original_row_json,
        )
        try:
            with session.begin_nested():
                session.add(tx)
                session.flush()
            inserted += 1
        except IntegrityError:
            dupes += 1
            continue

    batch.row_count = len(raw_txns)
    batch.duplicates_skipped = dupes + fuzzy_dupes
    batch.metadata_json = {**(batch.metadata_json or {}), "backfilled_existing": backfilled}
    session.commit()

    return ImportFileResult(
        file_name=file_name,
        file_hash=file_hash,
        format_name=importer.format_name,
        institution=institution,
        account_name=account_name,
        row_count=len(raw_txns),
        inserted=inserted,
        duplicates_skipped=dupes,
        fuzzy_duplicates_skipped=fuzzy_dupes,
        parse_fail_count=parse_fails,
        warnings=warnings,
    )
