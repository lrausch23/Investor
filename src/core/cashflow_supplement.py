from __future__ import annotations

import csv
import datetime as dt
import hashlib
import io
import re
from typing import Any

from sqlalchemy import func, or_
from sqlalchemy.orm import Session

from src.db.models import (
    Account,
    ExternalAccountMap,
    ExternalConnection,
    ExternalFileIngest,
    ExternalTransactionMap,
    Transaction,
)


_DATE_KEYS = (
    "post date",
    "settlement date",
    "trade date",
    "transaction date",
    "date",
    "posted",
)
_TYPE_KEYS = (
    "type",
    "tran code",
    "tran code description",
    "transaction type",
    "txn type",
)
_FLOW_KEYS = (
    "flow",
    "direction",
)
_AMOUNT_KEYS = (
    "amount usd",
    "amount",
    "amount local",
    "amt",
    "value",
    "net",
)
_QTY_KEYS = (
    "quantity",
    "qty",
    "units",
)
_TAX_KEYS = (
    "tax withheld",
    "withheld tax",
    "tax",
    "withholding",
)
_DESC_KEYS = (
    "description",
    "details",
    "memo",
    "name",
    "tran code description",
)
_ACCOUNT_KEYS = (
    "account name",
    "account",
    "account label",
    "acct",
    "account number",
)
_LAST4_KEYS = (
    "account number",
    "last4",
    "accountlast4",
    "acct last4",
    "account last 4",
)


def _normalize_header(s: str) -> str:
    return (s or "").strip().lower()


def _normalize_text(s: str) -> str:
    return re.sub(r"[^A-Z0-9]+", "", (s or "").strip().upper())


def _extract_last4(s: str) -> str:
    digits = re.findall(r"\d{4}", s or "")
    if not digits:
        return ""
    return digits[-1]


def _pick(row: dict[str, str], keys: tuple[str, ...] | list[str] | set[str]) -> str:
    for k in keys:
        if k in row and row[k].strip():
            return row[k].strip()
    return ""


def _parse_amount(raw: str) -> float | None:
    s = (raw or "").strip()
    if not s:
        return None
    neg = False
    if s.startswith("(") and s.endswith(")"):
        neg = True
        s = s[1:-1]
    s = s.replace(",", "")
    try:
        val = float(s)
    except Exception:
        return None
    return -val if neg else val


def _parse_date(raw: str) -> dt.date:
    s = (raw or "").strip()
    if not s:
        raise ValueError("empty date")
    s = s.split()[0]
    try:
        return dt.date.fromisoformat(s[:10])
    except Exception:
        pass
    for fmt in ("%m/%d/%Y", "%m/%d/%y", "%Y/%m/%d", "%m-%d-%Y", "%m-%d-%y"):
        try:
            return dt.datetime.strptime(s, fmt).date()
        except Exception:
            continue
    raise ValueError(f"invalid date: {raw!r}")


def _map_cashflow_type(type_raw: str, flow_raw: str) -> tuple[str, str]:
    t = (type_raw or "").strip().upper()
    f = (flow_raw or "").strip().upper()
    label = f"{t} {f}".strip()

    if any(tok in label for tok in ("DEPOSIT", "CREDIT")):
        return "TRANSFER", "IN"
    if any(tok in label for tok in ("WITHDRAW", "DEBIT", "CASH OUT")):
        return "TRANSFER", "OUT"
    if any(tok in label for tok in ("WITHHOLD", "TAX")):
        return "WITHHOLDING", ""
    if "FEE" in label or "COMMISSION" in label:
        return "FEE", ""
    if "TRANSFER" in label or t in {"BNK", "BANK", "ACH"}:
        return "TRANSFER", ""
    return "OTHER", ""


def _signed_amount(tx_type: str, direction: str, amount: float) -> float:
    t = (tx_type or "").upper()
    if t == "WITHHOLDING":
        return abs(amount)
    if t in {"FEE", "OTHER"}:
        return -abs(amount)
    if t == "TRANSFER":
        if direction == "IN":
            return abs(amount)
        if direction == "OUT":
            return -abs(amount)
        return -abs(amount) if amount < 0 else abs(amount)
    return amount


def import_supplemental_cashflows(
    session: Session,
    *,
    connection: ExternalConnection,
    file_name: str,
    file_bytes: bytes,
    stored_path: str | None = None,
    actor: str | None = None,
    purge_manual_overrides: bool = True,
) -> dict[str, Any]:
    if not file_bytes:
        raise ValueError("Empty CSV file.")

    file_hash = hashlib.sha256(file_bytes).hexdigest()
    existing = (
        session.query(ExternalFileIngest)
        .filter(
            ExternalFileIngest.connection_id == connection.id,
            ExternalFileIngest.kind == "SUPPLEMENTAL_CASHFLOWS",
            ExternalFileIngest.file_hash == file_hash,
        )
        .one_or_none()
    )
    already_imported = existing is not None
    source_file_name = existing.file_name if existing and existing.file_name else file_name

    text = file_bytes.decode("utf-8-sig", errors="replace")
    lines = text.splitlines()
    header_line = lines[0] if lines else ""
    if "\t" in header_line and header_line.count("\t") >= header_line.count(","):
        dialect = csv.excel_tab
    else:
        sample = text[:4096]
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=",\t;|")
        except Exception:
            dialect = csv.excel
    reader = csv.DictReader(io.StringIO(text), dialect=dialect)
    if not reader.fieldnames:
        raise ValueError("CSV is missing a header row.")
    header_fields = [h or "" for h in (reader.fieldnames or [])]

    maps = (
        session.query(ExternalAccountMap, Account)
        .join(Account, Account.id == ExternalAccountMap.account_id)
        .filter(ExternalAccountMap.connection_id == connection.id)
        .all()
    )
    candidates: list[dict[str, Any]] = []
    candidate_ids: set[int] = set()
    for _m, acct in maps:
        if acct.id in candidate_ids:
            continue
        acct_name = str(acct.name or "")
        candidates.append(
            {
                "account": acct,
                "name_norm": _normalize_text(acct_name),
                "last4": _extract_last4(acct_name),
                "mapped": True,
            }
        )
        candidate_ids.add(acct.id)
    # Add broker accounts not mapped to this connection (fallback).
    fallback = session.query(Account).all()
    for acct in fallback:
        if acct.id in candidate_ids:
            continue
        acct_name = str(acct.name or "")
        candidates.append(
            {
                "account": acct,
                "name_norm": _normalize_text(acct_name),
                "last4": _extract_last4(acct_name),
                "mapped": False,
            }
        )
    global_by_last4: dict[str, list[dict[str, Any]]] = {}
    for c in candidates:
        if c["last4"]:
            existing_list = global_by_last4.setdefault(c["last4"], [])
            if all(x["account"].id != c["account"].id for x in existing_list):
                existing_list.append(c)
    all_by_last4: dict[str, list[Account]] = {}
    for c in candidates:
        if c["last4"]:
            all_list = all_by_last4.setdefault(c["last4"], [])
            if all(x.id != c["account"].id for x in all_list):
                all_list.append(c["account"])
    ira_candidates = [c for c in candidates if (c["account"].account_type or "").upper() == "IRA"]

    stats = {
        "file_hash": file_hash,
        "already_imported": already_imported,
        "rows": 0,
        "inserted": 0,
        "duplicates": 0,
        "invalid": 0,
        "unmatched": 0,
        "ambiguous": 0,
        "ignored": 0,
        "purged_manual": 0,
        "unmatched_sample": [],
    }
    start_date: dt.date | None = None
    end_date: dt.date | None = None

    if already_imported and existing and existing.file_name:
        src_name = existing.file_name
        source_u = func.upper(func.coalesce(func.json_extract(Transaction.lot_links_json, "$.source"), ""))
        file_u = func.coalesce(func.json_extract(Transaction.lot_links_json, "$.source_file"), "")
        txn_ids = [
            t_id
            for (t_id,) in session.query(Transaction.id)
            .filter(source_u == "CSV_SUPPLEMENTAL", file_u == src_name)
            .all()
        ]
        if txn_ids:
            session.query(ExternalTransactionMap).filter(ExternalTransactionMap.transaction_id.in_(txn_ids)).delete(
                synchronize_session=False
            )
            session.query(Transaction).filter(Transaction.id.in_(txn_ids)).delete(synchronize_session=False)

    if purge_manual_overrides and candidates:
        acct_ids = [c["account"].id for c in candidates]
        manual_flag = func.coalesce(func.json_extract(Transaction.lot_links_json, "$.manual_override"), 0)
        desc_u = func.upper(func.coalesce(func.json_extract(Transaction.lot_links_json, "$.description"), ""))
        source_u = func.upper(func.coalesce(func.json_extract(Transaction.lot_links_json, "$.source"), ""))
        is_manual = or_(
            manual_flag == 1,
            func.instr(desc_u, "MANUAL CASHFLOW") > 0,
            func.instr(source_u, "MANUAL") > 0,
        )
        txn_ids = [
            t_id
            for (t_id,) in session.query(Transaction.id)
            .filter(Transaction.account_id.in_(acct_ids), is_manual)
            .all()
        ]
        if txn_ids:
            session.query(ExternalTransactionMap).filter(ExternalTransactionMap.transaction_id.in_(txn_ids)).delete(
                synchronize_session=False
            )
            stats["purged_manual"] = int(
                session.query(Transaction).filter(Transaction.id.in_(txn_ids)).delete(synchronize_session=False) or 0
            )

    # If already imported, we still allow re-import for improved parsing.
    # We will skip recording a duplicate ExternalFileIngest entry below.

    def _match_account(name_raw: str, last4_raw: str) -> Account | None:
        last4 = _extract_last4(last4_raw or "")
        name_norm = _normalize_text(name_raw or "")
        if last4:
            matches = [c for c in candidates if c["last4"] == last4]
            uniq: dict[int, dict[str, Any]] = {c["account"].id: c for c in matches}
            matches = list(uniq.values())
            mapped_matches = [c for c in matches if c.get("mapped")]
            if mapped_matches:
                matches = mapped_matches
            if len(matches) == 1:
                return matches[0]["account"]
            if len(matches) > 1:
                # Disambiguate with name if provided.
                if name_norm:
                    narrowed = [c for c in matches if name_norm in c["name_norm"] or c["name_norm"] in name_norm]
                    uniq_n: dict[int, dict[str, Any]] = {c["account"].id: c for c in narrowed}
                    narrowed = list(uniq_n.values())
                    if len(narrowed) == 1:
                        return narrowed[0]["account"]
                    if len(narrowed) > 1:
                        stats["ambiguous"] += 1
                        return None
                stats["ambiguous"] += 1
                return None
        if name_norm:
            matches = [c for c in candidates if name_norm in c["name_norm"] or c["name_norm"] in name_norm]
            uniq_n = {c["account"].id: c for c in matches}
            matches = list(uniq_n.values())
            mapped_matches = [c for c in matches if c.get("mapped")]
            if mapped_matches:
                matches = mapped_matches
            if len(matches) == 1:
                return matches[0]["account"]
            if len(matches) > 1:
                stats["ambiguous"] += 1
                return None
        # IRA fallback: if CSV name hints IRA and there's exactly one IRA account.
        if name_norm and "IRA" in name_norm and len(ira_candidates) == 1:
            return ira_candidates[0]["account"]
        if last4 and last4 in global_by_last4 and len(global_by_last4[last4]) == 1:
            return global_by_last4[last4][0]["account"]
        if last4 and last4 in all_by_last4 and len(all_by_last4[last4]) == 1:
            return all_by_last4[last4][0]
        # Fallback: if only one candidate exists, assume it.
        if len(candidates) == 1:
            return candidates[0]["account"]
        return None

    for raw in reader:
        stats["rows"] += 1
        row = {_normalize_header(k): (str(v or "").strip()) for k, v in raw.items() if k}
        date_raw = _pick(row, _DATE_KEYS)
        amt_raw = _pick(row, _AMOUNT_KEYS)
        qty_raw = _pick(row, _QTY_KEYS)
        tax_raw = _pick(row, _TAX_KEYS)
        tax_amt = _parse_amount(tax_raw) if tax_raw else None
        if tax_amt == 0:
            tax_amt = None
        if not date_raw or not amt_raw:
            if tax_amt is not None:
                amt_raw = tax_raw
            else:
                stats["invalid"] += 1
                continue
        try:
            d = _parse_date(date_raw)
        except Exception:
            stats["invalid"] += 1
            continue
        amt = _parse_amount(amt_raw)
        if amt is None:
            stats["invalid"] += 1
            continue

        type_raw = _pick(row, _TYPE_KEYS)
        flow_raw = _pick(row, _FLOW_KEYS) or _pick(row, {"tran code description", "tran code"})
        tx_type, direction = _map_cashflow_type(type_raw, flow_raw)
        desc = _pick(row, _DESC_KEYS)
        type_u = (type_raw or "").strip().upper()
        flow_u = (flow_raw or "").strip().upper()
        desc_u = (desc or "").strip().upper()
        label_u = f"{type_u} {flow_u} {desc_u}".strip()

        is_div = any(tok in label_u for tok in ("DIVIDEND", "CASH DIV", "DIV ON"))
        is_int = any(tok in label_u for tok in ("INTEREST", "INT "))
        is_adr_fee = ("ADR" in label_u) and ("FEE" in label_u)
        is_transfer_hint = any(tok in label_u for tok in ("BANKLINK", "ACH", "WIRE", "TRANSFER", "WITHDRAWAL", "PUSH"))
        is_banklink = is_transfer_hint or type_u in {"BNK", "BANK", "ACH"} or flow_u in {"BNK", "BANK", "ACH"}
        is_tax_code = (
            type_u in {"TAX", "WITHHOLDING"}
            or flow_u in {"TAX", "WITHHOLDING"}
            or (not is_div and not is_int and not is_adr_fee and not is_transfer_hint and ("WITHHOLD" in desc_u or "TAX WITHHELD" in desc_u))
        )

        if is_adr_fee:
            tx_type = "DIV"
            direction = "IN"
        elif is_div:
            tx_type = "DIV"
            direction = "IN"
        elif is_int:
            tx_type = "INT"
            direction = "IN"
        elif is_transfer_hint:
            tx_type = "TRANSFER"
        elif is_tax_code:
            tx_type = "WITHHOLDING"
        if is_banklink:
            tx_type = "TRANSFER"
        if tx_type == "OTHER" and any(tok in label_u for tok in ("BANKLINK", "ACH", "WIRE", "TRANSFER")):
            tx_type = "TRANSFER"
        if tx_type == "TRANSFER" and not direction:
            direction = "OUT" if amt < 0 else "IN"
        if "DEPOSIT SWEEP" in label_u and "INTRA-DAY" in label_u:
            stats["ignored"] += 1
            continue
        if any(tok in label_u for tok in ("BUY", "SELL", "REINVEST")) and not any(
            tok in label_u for tok in ("TRANSFER", "WITHDRAW", "DEPOSIT", "WITHHOLD", "TAX", "FEE")
        ):
            stats["ignored"] += 1
            continue
        qty_val = _parse_amount(qty_raw) if qty_raw else None
        if qty_val and abs(qty_val) > 0 and tx_type not in {"TRANSFER", "FEE", "WITHHOLDING"}:
            stats["ignored"] += 1
            continue
        if tax_amt is not None and tx_type in {"OTHER", "TRANSFER"}:
            tx_type = "WITHHOLDING"
            signed = _signed_amount(tx_type, direction, tax_amt)
        else:
            signed = _signed_amount(tx_type, direction, amt)
        if signed == 0:
            stats["invalid"] += 1
            continue

        acct_name = _pick(row, _ACCOUNT_KEYS)
        last4_raw = _pick(row, _LAST4_KEYS)
        acct = _match_account(acct_name, last4_raw)
        if acct is None:
            stats["unmatched"] += 1
            if len(stats["unmatched_sample"]) < 5:
                stats["unmatched_sample"].append(
                    {
                        "date": d.isoformat(),
                        "account_name": acct_name,
                        "account_number": last4_raw,
                        "last4": _extract_last4(last4_raw or ""),
                        "type_raw": type_raw,
                        "flow_raw": flow_raw,
                        "description": desc,
                        "amount": amt,
                        "candidates_for_last4": len(global_by_last4.get(_extract_last4(last4_raw or ""), [])),
                    }
                )
            continue

        if tax_amt is not None and tx_type in {"OTHER", "TRANSFER"} and not is_banklink:
            tx_type = "WITHHOLDING"
            signed = _signed_amount(tx_type, direction, amt)

        if start_date is None or d < start_date:
            start_date = d
        if end_date is None or d > end_date:
            end_date = d

        desc_norm = " ".join(desc.split())
        sig = f"{acct.id}|{d.isoformat()}|{tx_type}|{signed:.2f}|{desc_norm.upper()}"
        digest = hashlib.sha256(sig.encode("utf-8")).hexdigest()[:24]
        provider_txn_id = f"CSV_SUPP:{digest}"

        existing_map = (
            session.query(ExternalTransactionMap)
            .filter(
                ExternalTransactionMap.connection_id == connection.id,
                ExternalTransactionMap.provider_txn_id == provider_txn_id,
            )
            .one_or_none()
        )
        if existing_map is not None:
            stats["duplicates"] += 1
            continue

        dup_by_shape = (
            session.query(Transaction.id)
            .filter(
                Transaction.account_id == acct.id,
                Transaction.date == d,
                Transaction.type == tx_type,
                Transaction.amount == signed,
                func.coalesce(func.json_extract(Transaction.lot_links_json, "$.description"), "") == desc_norm,
            )
            .first()
        )
        if dup_by_shape is not None:
            stats["duplicates"] += 1
            continue

        links = {
            "source": "CSV_SUPPLEMENTAL",
            "source_file": source_file_name,
            "description": desc_norm,
            "raw_type": type_raw,
            "raw_flow": flow_raw,
            "account_hint": acct_name,
            "last4_hint": last4_raw,
            "entered_by": actor or "system",
        }
        txn = Transaction(
            account_id=acct.id,
            date=d,
            type=tx_type,
            ticker="UNKNOWN",
            qty=None,
            amount=signed,
            lot_links_json=links,
        )
        session.add(txn)
        session.flush()
        session.add(
            ExternalTransactionMap(
                connection_id=connection.id,
                provider_txn_id=provider_txn_id,
                transaction_id=txn.id,
            )
        )
        stats["inserted"] += 1

    meta_payload = {
        "rows": stats["rows"],
        "inserted": stats["inserted"],
        "duplicates": stats["duplicates"],
        "invalid": stats["invalid"],
        "ignored": stats["ignored"],
        "unmatched": stats["unmatched"],
        "ambiguous": stats["ambiguous"],
        "purged_manual": stats["purged_manual"],
        "unmatched_sample": stats["unmatched_sample"],
        "candidate_accounts": [
            {
                "name": c["account"].name,
                "last4": c["last4"],
                "type": c["account"].account_type,
                "mapped": c.get("mapped"),
            }
            for c in candidates[:10]
        ],
        "header_fields": header_fields,
        "delimiter": getattr(dialect, "delimiter", None),
    }
    if not already_imported:
        session.add(
            ExternalFileIngest(
                connection_id=connection.id,
                kind="SUPPLEMENTAL_CASHFLOWS",
                file_name=file_name,
                file_hash=file_hash,
                file_bytes=len(file_bytes),
                stored_path=stored_path,
                start_date_hint=start_date,
                end_date_hint=end_date,
                metadata_json=meta_payload,
            )
        )
    elif existing is not None:
        existing.metadata_json = dict(existing.metadata_json or {})
        existing.metadata_json.update(meta_payload)
        existing.imported_at = dt.datetime.utcnow()
        existing.file_name = file_name
        existing.stored_path = stored_path
    session.commit()
    return stats
