from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import math
import shutil
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Optional

from sqlalchemy.exc import IntegrityError
from sqlalchemy import func
from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import flag_modified

from src.core.credential_store import get_credential
from src.core.sync_coverage import compute_coverage_status
from src.core.broker_tax import link_broker_wash_sales
from src.adapters.ib_flex_offline.adapter import IBFlexOfflineAdapter
from src.adapters.ib_flex_web.adapter import IBFlexWebAdapter
from src.adapters.chase_offline.adapter import ChaseOfflineAdapter
from src.adapters.plaid_chase.adapter import PlaidChaseAdapter
from src.adapters.plaid_amex.adapter import PlaidAmexAdapter
from src.adapters.rj_offline.adapter import RJOfflineAdapter
from src.adapters.yodlee_chase.adapter import YodleeChaseAdapter
from src.adapters.plaid_chase.client import PlaidApiError, PlaidClient
from src.db.audit import log_change
from src.db.models import (
    Account,
    BrokerLotClosure,
    BrokerWashSaleEvent,
    CashBalance,
    ExternalAccountMap,
    ExternalConnection,
    ExternalCredential,
    ExternalFileIngest,
    ExternalHoldingSnapshot,
    ExternalLiabilitySnapshot,
    ExternalPayloadSnapshot,
    ExternalTransactionMap,
    ExpenseAccount,
    ExpenseImportBatch,
    ExpenseTransaction,
    PositionLot,
    Security,
    SyncRun,
    Transaction,
)
from src.importers.adapters import BrokerAdapter, ProviderError, RangeTooLargeError, YodleeIBFixtureAdapter
from src.investor.expenses.normalize import (
    normalize_bank_merchant,
    normalize_description,
    normalize_merchant,
    stable_txn_id,
)
from src.utils.time import utcfromtimestamp, utcnow


class SyncConfigError(Exception):
    pass


def _dedupe_plaid_expense_transactions(session: Session, expense_account_ids: list[int]) -> int:
    if not expense_account_ids:
        return 0
    dup_groups = (
        session.query(
            ExpenseTransaction.expense_account_id,
            ExpenseTransaction.posted_date,
            ExpenseTransaction.amount,
            ExpenseTransaction.description_norm,
            ExpenseTransaction.merchant_norm,
            ExpenseTransaction.currency,
            func.max(ExpenseTransaction.id).label("keep_id"),
            func.count(ExpenseTransaction.id).label("cnt"),
        )
        .join(ExpenseImportBatch, ExpenseTransaction.import_batch_id == ExpenseImportBatch.id)
        .filter(ExpenseImportBatch.source == "PLAID")
        .filter(ExpenseTransaction.expense_account_id.in_(expense_account_ids))
        .group_by(
            ExpenseTransaction.expense_account_id,
            ExpenseTransaction.posted_date,
            ExpenseTransaction.amount,
            ExpenseTransaction.description_norm,
            ExpenseTransaction.merchant_norm,
            ExpenseTransaction.currency,
        )
        .having(func.count(ExpenseTransaction.id) > 1)
        .all()
    )
    removed = 0
    for g in dup_groups:
        removed += (
            session.query(ExpenseTransaction)
            .filter(
                ExpenseTransaction.expense_account_id == g.expense_account_id,
                ExpenseTransaction.posted_date == g.posted_date,
                ExpenseTransaction.amount == g.amount,
                ExpenseTransaction.description_norm == g.description_norm,
                ExpenseTransaction.merchant_norm == g.merchant_norm,
                ExpenseTransaction.currency == g.currency,
                ExpenseTransaction.id != g.keep_id,
            )
            .delete(synchronize_session=False)
        )
    return removed


def _derive_holdings_snapshot_from_transactions(
    *,
    base_holdings: dict[str, Any],
    txns: list[Transaction],
    as_of: dt.datetime,
    source_label: str,
) -> dict[str, Any]:
    """
    Best-effort "roll forward" holdings snapshot using a baseline holdings snapshot plus
    subsequent transactions. This is planning-grade (not authoritative), but makes the
    UI reflect recent activity when a fresh holdings file hasn't been uploaded yet.
    """
    base_items = base_holdings.get("items") or []
    provider_acct = "RJ:TAXABLE"
    # Track positions by symbol (excluding cash).
    positions: dict[str, dict[str, float | None]] = {}
    # Track last known unit price per symbol (market_value/qty).
    unit_price: dict[str, float] = {}
    cash = 0.0
    for it in base_items if isinstance(base_items, list) else []:
        if not isinstance(it, dict):
            continue
        pa = str(it.get("provider_account_id") or "").strip()
        if pa:
            provider_acct = pa
        sym = str(it.get("symbol") or it.get("ticker") or "").strip().upper()
        if not sym:
            continue
        if bool(it.get("is_total")):
            continue
        try:
            q_raw = it.get("qty")
            if q_raw is None or str(q_raw).strip() == "":
                q_raw = it.get("quantity")
            qty = float(q_raw) if q_raw not in (None, "") else 0.0
        except Exception:
            qty = 0.0
        try:
            mv_raw = it.get("market_value")
            mv = float(mv_raw) if mv_raw not in (None, "") else None
        except Exception:
            mv = None
        if sym.startswith("CASH:"):
            try:
                cash = float(mv if mv is not None else qty)
            except Exception:
                cash = 0.0
            continue
        if qty <= 0:
            continue
        positions[sym] = {"qty": qty, "market_value": mv}
        if mv is not None and abs(qty) > 1e-9:
            unit_price[sym] = float(mv) / float(qty)

    def _infer_trade_delta(tx: Transaction) -> float | None:
        if tx.qty is None:
            return None
        try:
            q = float(tx.qty)
        except Exception:
            return None
        if abs(q) <= 1e-12:
            return None
        t = str(tx.type or "").strip().upper()
        if t == "BUY":
            return abs(q)
        if t == "SELL":
            return -abs(q)
        # Fallback for misclassified rows: infer by cashflow sign.
        try:
            a = float(tx.amount or 0.0)
        except Exception:
            a = 0.0
        return abs(q) if a < 0 else -abs(q)

    def _dedupe_key(tx: Transaction) -> tuple[str, str, str, float, float, str]:
        """
        Best-effort de-dupe for derived snapshots.

        RJ imports can legitimately contain the same economic event twice when classification rules change
        (e.g., SELL vs OTHER), which would otherwise double-count cash.
        """
        d = tx.date.isoformat() if getattr(tx, "date", None) is not None else ""
        sym = str(tx.ticker or "UNKNOWN").strip().upper() or "UNKNOWN"
        try:
            amt = float(tx.amount or 0.0)
        except Exception:
            amt = 0.0
        direction = "IN" if amt > 0 else ("OUT" if amt < 0 else "ZERO")
        try:
            q = float(tx.qty) if tx.qty is not None else 0.0
        except Exception:
            q = 0.0
        q_abs = round(abs(q), 6)
        a_abs = round(abs(amt), 2)
        links = getattr(tx, "lot_links_json", None) or {}
        desc = str(links.get("description") or links.get("raw_description") or "").strip().upper()
        desc = " ".join(desc.split())
        # Keep key stable but not too specific (avoid file row ids etc).
        if len(desc) > 80:
            desc = desc[:80]
        return (d, sym, direction, q_abs, a_abs, desc)

    seen_keys: set[tuple[str, str, str, float, float, str]] = set()
    txns_dedup: list[Transaction] = []
    for tx in txns:
        try:
            k = _dedupe_key(tx)
        except Exception:
            txns_dedup.append(tx)
            continue
        if k in seen_keys:
            continue
        seen_keys.add(k)
        txns_dedup.append(tx)

    for tx in txns_dedup:
        try:
            cash += float(tx.amount or 0.0)
        except Exception:
            pass
        sym = str(tx.ticker or "").strip().upper()
        if not sym or sym == "UNKNOWN":
            continue
        delta = _infer_trade_delta(tx)
        if delta is None:
            continue
        cur_qty = float(positions.get(sym, {}).get("qty") or 0.0)
        new_qty = cur_qty + float(delta)
        if new_qty <= 1e-9:
            positions.pop(sym, None)
            unit_price.pop(sym, None)
            continue
        # Update unit price when we have a trade price (best-effort).
        if sym not in unit_price or unit_price.get(sym, 0.0) <= 0:
            try:
                q = abs(float(tx.qty or 0.0))
                a = abs(float(tx.amount or 0.0))
                if q > 1e-9 and a > 1e-9:
                    unit_price[sym] = a / q
            except Exception:
                pass
        mv = None
        if sym in unit_price:
            mv = float(unit_price[sym]) * float(new_qty)
        positions[sym] = {"qty": float(new_qty), "market_value": mv}

    items_out: list[dict[str, Any]] = []
    # Keep ordering stable for display.
    for sym in sorted(positions.keys()):
        row = positions[sym]
        items_out.append(
            {
                "provider_account_id": provider_acct,
                "symbol": sym,
                "qty": float(row.get("qty") or 0.0),
                "market_value": float(row["market_value"]) if row.get("market_value") is not None else None,
                "source_file": source_label,
            }
        )
    # Cash is modeled as a position item as well as a CashBalance row.
    items_out.append(
        {
            "provider_account_id": provider_acct,
            "symbol": "CASH:USD",
            "qty": float(cash),
            "market_value": float(cash),
            "asset_type": "CASH",
            "source_file": source_label,
        }
    )
    out: dict[str, Any] = {
        "as_of": as_of.isoformat(),
        "items": items_out,
        "source_file": source_label,
        "derived_from_transactions": True,
        "derived_from_holdings_as_of": base_holdings.get("as_of"),
    }
    out["cash_balances"] = [
        {
            "provider_account_id": provider_acct,
            "currency": "USD",
            "amount": float(cash),
            "as_of_date": as_of.date().isoformat(),
            "source_file": source_label,
        }
    ]
    return out


def _store_plaid_liabilities_snapshot(
    session: Session,
    *,
    connection: ExternalConnection,
    run: SyncRun,
    access_token: str,
    store_payloads: bool,
    coverage: dict[str, Any],
    warnings: list[str],
) -> None:
    recent = (
        session.query(ExternalLiabilitySnapshot)
        .filter(ExternalLiabilitySnapshot.connection_id == connection.id)
        .order_by(ExternalLiabilitySnapshot.as_of.desc(), ExternalLiabilitySnapshot.id.desc())
        .first()
    )
    if not access_token:
        warnings.append(f"Liabilities snapshot skipped for {connection.name}: missing access token.")
        return
    env = (connection.metadata_json or {}).get("plaid_env") or None
    client = PlaidClient(env=env)
    attempted_at = utcnow()
    coverage["liability_snapshot_attempted_at"] = attempted_at.isoformat()
    try:
        payload = client.liabilities_get(access_token=access_token)
    except PlaidApiError as e:
        code = (e.info.error_code or "").upper()
        status = 0
        if code.startswith("HTTP_"):
            suffix = code.split("_", 1)[-1]
            if suffix.isdigit():
                status = int(suffix)
        retryable = code in {"HTTP_429", "RATE_LIMIT_EXCEEDED", "CREDITS_EXHAUSTED"} or status >= 500
        if retryable:
            if recent is not None:
                coverage["liability_snapshot_last_asof"] = recent.as_of.isoformat()
                coverage["liability_snapshot_used_stale"] = int(
                    coverage.get("liability_snapshot_used_stale") or 0
                ) + 1
            warnings.append(f"Liabilities snapshot deferred for {connection.name}: {e.info.error_code}")
        else:
            warnings.append(f"Liabilities snapshot failed for {connection.name}: {e.info.error_code}")
        return
    except Exception as e:
        warnings.append(f"Liabilities snapshot failed for {connection.name}: {type(e).__name__}")
        return
    as_of = utcnow()
    session.add(ExternalLiabilitySnapshot(connection_id=connection.id, as_of=as_of, payload_json=payload))
    coverage["liability_snapshots_imported"] = int(coverage.get("liability_snapshots_imported") or 0) + 1
    coverage["liability_snapshot_last_asof"] = as_of.isoformat()
    if store_payloads:
        session.add(
            ExternalPayloadSnapshot(sync_run_id=run.id, kind="liabilities", cursor=None, payload_json=payload)
        )
        coverage["report_payloads_recorded"] = int(coverage.get("report_payloads_recorded") or 0) + 1


@dataclass(frozen=True)
class AdapterConnectionContext:
    connection: ExternalConnection
    credentials: dict[str, str | None]
    run_settings: dict[str, Any]

    @property
    def metadata_json(self) -> dict[str, Any]:
        return self.connection.metadata_json or {}

    @property
    def id(self) -> int:
        return self.connection.id

    @property
    def provider(self) -> str:
        return self.connection.provider

    @property
    def broker(self) -> str:
        return self.connection.broker

    @property
    def taxpayer_entity_id(self) -> int:
        return self.connection.taxpayer_entity_id


def _adapter_for(connection: ExternalConnection) -> BrokerAdapter:
    provider = (connection.provider or "").upper()
    broker = (connection.broker or "").upper()
    connector = (connection.connector or "").upper()
    meta = connection.metadata_json or {}
    if provider == "YODLEE" and broker == "IB" and (meta.get("fixture_dir") or meta.get("fixture_accounts")):
        return YodleeIBFixtureAdapter()
    if provider == "YODLEE" and broker == "CHASE" and connector == "CHASE_YODLEE":
        return YodleeChaseAdapter()
    if provider == "PLAID" and broker == "CHASE" and connector == "CHASE_PLAID":
        return PlaidChaseAdapter()
    if provider == "PLAID" and broker == "AMEX" and connector == "AMEX_PLAID":
        return PlaidAmexAdapter()
    if provider == "YODLEE" and broker == "IB":
        raise SyncConfigError("Yodlee live sync is not implemented in MVP (network is not used). Use fixtures or IB Flex Offline.")
    if provider == "IB" and connector == "IB_FLEX_OFFLINE":
        return IBFlexOfflineAdapter()
    if provider == "IB" and connector == "IB_FLEX_WEB":
        return IBFlexWebAdapter()
    if provider == "CHASE" and connector == "CHASE_OFFLINE":
        return ChaseOfflineAdapter()
    if provider == "RJ" and connector == "RJ_OFFLINE":
        return RJOfflineAdapter()
    raise SyncConfigError(
        f"No adapter configured for provider={provider} broker={broker} connector={connector}."
    )


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _offline_data_dir(connection: ExternalConnection) -> Path:
    meta = connection.metadata_json or {}
    data_dir = meta.get("data_dir")
    if data_dir:
        return Path(os.path.expanduser(str(data_dir)))
    return Path("data") / "external" / f"conn_{connection.id}"


def _archive_raw_file(*, connection_id: int, file_hash: str, src_path: Path) -> str | None:
    """
    Copy an ingested offline file into an append-only archive directory.

    This is a safety/audit mechanism: users can re-run imports and still have the exact bytes that were processed.
    """
    try:
        base = Path("data") / "external" / "raw_archive" / f"conn_{int(connection_id)}"
        base.mkdir(parents=True, exist_ok=True)
        ext = src_path.suffix.lower() if src_path.suffix else ".bin"
        dest = base / f"{file_hash}{ext}"
        if dest.exists():
            return str(dest)
        shutil.copy2(src_path, dest)
        return str(dest)
    except Exception:
        return None


def _select_offline_files(
    session: Session,
    *,
    connection: ExternalConnection,
    mode: str,
    reprocess_files: bool,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    data_dir = _offline_data_dir(connection)
    connector = (connection.connector or "").upper()
    if not data_dir.exists() or not data_dir.is_dir():
        return [], {
            "data_dir": str(data_dir),
            "file_total_all": 0,
            "file_total": 0,  # supported files
            "file_unsupported_total": 0,
            "file_selected": 0,
            "file_skipped_seen": 0,
        }

    supported_exts = {".csv", ".tsv", ".txt", ".xml"}
    if connector == "RJ_OFFLINE":
        supported_exts.update({".qfx", ".ofx"})
    known_unsupported_exts = {".xlsx", ".xls", ".pdf"}

    all_paths = [p for p in sorted(data_dir.glob("**/*")) if p.is_file()]
    supported_files = [p for p in all_paths if p.suffix.lower() in supported_exts]
    unsupported_files = [p for p in all_paths if p.suffix.lower() in known_unsupported_exts]
    all_files = supported_files

    seen_hashes: set[str] = set()
    if not reprocess_files and mode == "INCREMENTAL":
        # ExternalFileIngest is unique by (connection_id, file_hash), so treat any previously ingested file as "seen"
        # regardless of its kind (transactions/holdings/etc). This enables QFX files (containing both) to be handled
        # idempotently without reprocessing on every run.
        rows = session.query(ExternalFileIngest.file_hash).filter(ExternalFileIngest.connection_id == connection.id).all()
        seen_hashes = {r[0] for r in rows}

    selected: list[dict[str, Any]] = []
    skipped_seen = 0
    for p in all_files:
        name = p.name.lower()
        kind = "HOLDINGS" if ("position" in name or "positions" in name or "holding" in name or "openpositions" in name or "portfolio" in name) else "TRANSACTIONS"
        # RJ exports sometimes include transaction activity inside files named like "portfolio_*".
        # Prefer content-based detection over filename heuristics to avoid silently skipping transactions.
        if connector == "RJ_OFFLINE":
            try:
                from src.adapters.rj_offline.adapter import _looks_like_holdings, _looks_like_realized_pl, _looks_like_transactions

                head = p.read_text(encoding="utf-8-sig", errors="ignore")[:50000]
                if _looks_like_realized_pl(head) or (_looks_like_transactions(head) and not _looks_like_holdings(head)):
                    kind = "TRANSACTIONS"
                elif _looks_like_holdings(head):
                    kind = "HOLDINGS"
            except Exception:
                # Fall back to filename heuristic.
                pass
        if kind != "TRANSACTIONS":
            continue
        h = _sha256_file(p)
        if not reprocess_files and mode == "INCREMENTAL" and h in seen_hashes:
            skipped_seen += 1
            continue
        st = p.stat()
        selected.append(
            {
                "path": str(p),
                "file_hash": h,
                "kind": kind,
                "file_name": p.name,
                "file_bytes": int(st.st_size),
                "file_mtime_iso": utcfromtimestamp(st.st_mtime).isoformat(),
            }
        )
    return selected, {
        "data_dir": str(data_dir),
        "file_total_all": len(all_paths),
        "file_total": len(all_files),  # supported files
        "file_unsupported_total": len(unsupported_files),
        "file_selected": len(selected),
        "file_skipped_seen": skipped_seen,
    }


def _select_offline_holdings_files(
    session: Session,
    *,
    connection: ExternalConnection,
    mode: str,
    reprocess_files: bool,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    data_dir = _offline_data_dir(connection)
    if not data_dir.exists() or not data_dir.is_dir():
        return [], {"data_dir": str(data_dir), "holdings_file_total": 0, "holdings_file_selected": 0, "holdings_file_skipped_seen": 0}

    connector = (connection.connector or "").upper()
    # Holdings snapshots can come from PDFs (e.g., monthly broker statements) as well as CSV-like exports.
    # PDF parsing is supported for RJ and Chase (via pdftotext).
    supported_exts = {".csv", ".tsv", ".txt", ".xml"}
    if connector in {"RJ_OFFLINE", "CHASE_OFFLINE", "CHASE_PLAID"}:
        supported_exts.add(".pdf")
    if connector == "RJ_OFFLINE":
        supported_exts.update({".qfx", ".ofx"})
    all_files = [p for p in sorted(data_dir.glob("**/*")) if p.is_file() and p.suffix.lower() in supported_exts]

    seen_hashes: set[str] = set()
    if not reprocess_files and mode == "INCREMENTAL":
        rows = session.query(ExternalFileIngest.file_hash).filter(ExternalFileIngest.connection_id == connection.id).all()
        seen_hashes = {r[0] for r in rows}

    selected: list[dict[str, Any]] = []
    skipped_seen = 0
    holdings_total = 0
    for p in all_files:
        name = p.name.lower()
        if p.suffix.lower() in {".pdf", ".qfx", ".ofx"}:
            kind = "HOLDINGS"
        else:
            kind = "HOLDINGS" if ("position" in name or "positions" in name or "holding" in name or "openpositions" in name or "portfolio" in name) else "TRANSACTIONS"
            # RJ exports sometimes include transaction activity inside files named like "portfolio_*".
            # Prefer content-based detection over filename heuristics to avoid misclassifying activity as holdings.
            if connector == "RJ_OFFLINE":
                try:
                    from src.adapters.rj_offline.adapter import _looks_like_holdings, _looks_like_realized_pl, _looks_like_transactions

                    head = p.read_text(encoding="utf-8-sig", errors="ignore")[:50000]
                    if _looks_like_holdings(head):
                        kind = "HOLDINGS"
                    elif _looks_like_realized_pl(head) or _looks_like_transactions(head):
                        kind = "TRANSACTIONS"
                except Exception:
                    pass
            # IB statement exports (e.g., MTM Summary / Activity Statement with Net Asset Value) are
            # baseline valuation points for performance reporting.
            if kind != "HOLDINGS" and connector in {"IB_FLEX_WEB", "IB_FLEX_OFFLINE"} and p.suffix.lower() in {".csv", ".tsv", ".txt"}:
                try:
                    head = p.read_text(encoding="utf-8-sig", errors="ignore")[:20000].lower()
                    # IB exports are sectioned CSV/TSV with rows like:
                    #   Statement,Data,Title,Activity Statement
                    #   Statement,Data,Period,"January 1, 2025 - January 31, 2025"
                    #   Net Asset Value,Header,...
                    # We treat any statement that includes Net Asset Value as holdings-like (it contains a total).
                    looks_like_ib_statement = ("statement" in head) and ("statement" in head and "period" in head)
                    has_nav_section = ("net asset value" in head) and ("net asset value,header" in head or "net asset value\theader" in head)
                    if looks_like_ib_statement and has_nav_section:
                        kind = "HOLDINGS"
                except Exception:
                    pass
            # Chase performance report exports contain period-ending market values (valuation points) but are not holdings-by-ticker.
            if kind != "HOLDINGS" and connector in {"CHASE_OFFLINE", "CHASE_PLAID"} and p.suffix.lower() in {".csv", ".tsv", ".txt"}:
                try:
                    head = p.read_text(encoding="utf-8-sig", errors="ignore")[:20000].lower()
                    if ("ending market value" in head) and (("wealth generated" in head) or ("beginning market value" in head)):
                        kind = "HOLDINGS"
                except Exception:
                    pass
        if kind != "HOLDINGS":
            continue
        holdings_total += 1
        h = _sha256_file(p)
        if not reprocess_files and mode == "INCREMENTAL" and h in seen_hashes:
            skipped_seen += 1
            continue
        st = p.stat()
        selected.append(
            {
                "path": str(p),
                "file_hash": h,
                "kind": "HOLDINGS",
                "file_name": p.name,
                "file_bytes": int(st.st_size),
                "file_mtime_iso": utcfromtimestamp(st.st_mtime).isoformat(),
            }
        )

    return selected, {
        "data_dir": str(data_dir),
        "holdings_file_total": int(holdings_total),
        "holdings_file_selected": len(selected),
        "holdings_file_skipped_seen": skipped_seen,
    }


def _map_txn_type(raw: str) -> str:
    v = (raw or "").strip().upper()
    m = {
        "BUY": "BUY",
        "SELL": "SELL",
        "DIV": "DIV",
        "DIVIDEND": "DIV",
        "INT": "INT",
        "INTEREST": "INT",
        "FEE": "FEE",
        "WITHHOLDING": "WITHHOLDING",
        "TRANSFER": "TRANSFER",
        "DEPOSIT": "TRANSFER",
        "WITHDRAWAL": "TRANSFER",
    }
    return m.get(v, "OTHER")


def _float_or_none(v: Any) -> float | None:
    try:
        if v is None:
            return None
        if isinstance(v, (int, float)):
            out = float(v)
        else:
            s = str(v).strip()
            if not s:
                return None
            out = float(s.replace(",", ""))
        if math.isnan(out):
            return None
        return out
    except Exception:
        return None


def _parse_ib_date(value: Any) -> dt.date | None:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    if ";" in s:
        s = s.split(";", 1)[0].strip()
    if len(s) >= 8 and s[:8].isdigit():
        try:
            return dt.datetime.strptime(s[:8], "%Y%m%d").date()
        except Exception:
            pass
    try:
        return dt.date.fromisoformat(s[:10])
    except Exception:
        return None


def _stable_provider_txn_id(item: dict[str, Any]) -> str:
    if item.get("provider_transaction_id"):
        return str(item["provider_transaction_id"])
    parts = [
        str(item.get("date") or ""),
        str(item.get("amount") or ""),
        str(item.get("type") or ""),
        str(item.get("ticker") or item.get("symbol") or ""),
        str(item.get("description") or ""),
        str(item.get("provider_account_id") or item.get("account_id") or ""),
        str(item.get("qty") or ""),
    ]
    h = hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()
    return f"HASH:{h}"


def _ensure_security(session: Session, *, ticker: str, meta: dict[str, Any]) -> None:
    existing = session.query(Security).filter(Security.ticker == ticker).one_or_none()
    if existing is not None:
        return
    session.add(
        Security(
            ticker=ticker,
            name=ticker,
            asset_class="UNKNOWN",
            expense_ratio=0.0,
            substitute_group_id=None,
            metadata_json=meta or {},
        )
    )


def _upsert_account_map(
    session: Session,
    *,
    connection: ExternalConnection,
    accounts: list[dict[str, Any]],
    actor: str,
) -> tuple[dict[str, int], list[str]]:
    warnings: list[str] = []
    existing = (
        session.query(ExternalAccountMap)
        .filter(ExternalAccountMap.connection_id == connection.id)
        .all()
    )
    account_map = {r.provider_account_id: r.account_id for r in existing}
    placeholder_names = {"IB FLEX WEB", "IB FLEX (WEB)", "IB FLEX OFFLINE", "IB FLEX (OFFLINE)"}

    for a in accounts:
        provider_account_id = str(a.get("provider_account_id") or a.get("id") or a.get("account_id") or "")
        name = str(a.get("name") or "").strip()
        if not provider_account_id or not name:
            warnings.append("Provider returned an account missing provider_account_id or name; skipping.")
            continue
        if name.strip().upper() in placeholder_names:
            warnings.append(f"Skipped placeholder account name '{name}'.")
            continue
        if provider_account_id in account_map:
            continue
        existing = session.query(Account).filter(Account.name == name).one_or_none()
        if existing is None:
            acct = Account(
                name=name,
                broker=connection.broker,
                account_type=str(a.get("account_type") or "TAXABLE").upper(),
                taxpayer_entity_id=connection.taxpayer_entity_id,
            )
            session.add(acct)
            session.flush()
            log_change(
                session,
                actor=actor,
                action="CREATE",
                entity="Account",
                entity_id=str(acct.id),
                old=None,
                new={"name": acct.name, "broker": acct.broker, "account_type": acct.account_type},
                note=f"Created from external sync connection={connection.id}",
            )
            account_map[provider_account_id] = acct.id
        else:
            account_map[provider_account_id] = existing.id
        session.add(
            ExternalAccountMap(
                connection_id=connection.id, provider_account_id=provider_account_id, account_id=account_map[provider_account_id]
            )
        )

    return account_map, warnings


def _plaid_is_investment_account(a: dict[str, Any]) -> bool:
    try:
        raw_type = str(a.get("raw_type") or "").strip().lower()
    except Exception:
        raw_type = ""
    if raw_type == "investment":
        return True
    # Some Plaid responses may omit `type` in the adapter mapping; treat it as non-investment by default.
    return False


def _plaid_account_type_for_investment(a: dict[str, Any]) -> str:
    """
    Map Plaid investment account subtype -> Investor Account.account_type.

    Investor only models: TAXABLE | IRA | OTHER.
    """
    try:
        subtype = str(a.get("raw_subtype") or "").strip().lower()
    except Exception:
        subtype = ""
    if any(x in subtype for x in ("ira", "roth", "401k", "403b", "sep", "simple", "pension")):
        return "IRA"
    return "TAXABLE"


def _unique_plaid_account_name(*, base: str, provider_account_id: str) -> str:
    """
    Accounts.name is globally unique, so ensure a stable unique name for Plaid-created investment accounts.
    """
    base_s = (base or "").strip() or "Chase Investment"
    # Reserve room for suffix.
    base_s = base_s[:180]
    short = hashlib.sha256(provider_account_id.encode("utf-8")).hexdigest()[:6].upper()
    return f"{base_s} ({short})"


def _last4_from_account_name(name: str) -> str | None:
    """
    Best-effort last4 extractor from an Account.name that may contain:
      - ".... ****8839"
      - "(8839)"
      - trailing digits
    """
    try:
        import re

        s = str(name or "")
        m = re.search(r"(\d{4})\b", s[::-1])
        if m:
            # Because we searched reversed string, reverse the match too.
            return m.group(1)[::-1]
    except Exception:
        pass
    return None


def _pick_existing_chase_account_for_plaid(
    session: Session,
    *,
    taxpayer_entity_id: int,
    account_type: str,
    label: str,
    mask_last4: str | None,
) -> int | None:
    """
    Try to link a Plaid investment account to an existing Investor Account (avoid duplicate portfolios).

    This is heuristic: Account has no explicit last4 field, so we match based on:
      - broker=CHASE
      - taxpayer_entity_id
      - account_type (IRA/TAXABLE)
      - optional last4 in account name
      - optional label tokens (e.g., "IRA")
    """
    try:
        q = session.query(Account).filter(
            Account.broker == "CHASE",
            Account.taxpayer_entity_id == int(taxpayer_entity_id),
            Account.account_type == str(account_type).upper(),
        )
        candidates = q.all()
    except Exception:
        candidates = []
    if not candidates:
        return None

    label_u = " ".join(str(label or "").strip().upper().split())
    want_ira = "IRA" in label_u or str(account_type).upper() == "IRA"

    best_id: int | None = None
    best_score = -1
    for a in candidates:
        nm = str(getattr(a, "name", "") or "")
        nm_u = " ".join(nm.strip().upper().split())
        score = 0
        if want_ira and "IRA" in nm_u:
            score += 3
        if mask_last4:
            a_last4 = _last4_from_account_name(nm_u)
            if a_last4 == mask_last4:
                score += 6
            elif mask_last4 in nm_u:
                score += 4
        # Token overlap on words (very light fuzzy).
        if label_u:
            for tok in label_u.split():
                if tok and tok in nm_u and tok not in {"CHASE", "—"}:
                    score += 1
        if score > best_score:
            best_score = score
            best_id = int(getattr(a, "id"))
    # Require at least a minimal signal to avoid false positives.
    if best_score < 4:
        return None
    return best_id


def _upsert_plaid_investment_account_map(
    session: Session,
    *,
    connection: ExternalConnection,
    accounts: list[dict[str, Any]],
    actor: str,
) -> tuple[dict[str, int], list[str]]:
    """
    For CHASE_PLAID: create Account + ExternalAccountMap ONLY for Plaid investment accounts.

    This keeps bank/credit Plaid accounts in the Expenses system, while still enabling investment holdings
    snapshots to appear on the Holdings page and in Performance reports.
    """
    warnings: list[str] = []
    existing_maps = (
        session.query(ExternalAccountMap)
        .filter(ExternalAccountMap.connection_id == connection.id)
        .all()
    )
    account_map: dict[str, int] = {r.provider_account_id: r.account_id for r in existing_maps}

    for a in accounts:
        if not _plaid_is_investment_account(a):
            continue
        provider_account_id = str(a.get("provider_account_id") or "").strip()
        name = str(a.get("name") or "").strip()
        mask = str(a.get("mask") or "").strip()
        if not provider_account_id:
            continue
        label = name or "Chase Investment"
        base_name = f"Chase — {label}"
        if mask:
            base_name = f"{base_name} ****{mask}"
        # Ensure global uniqueness.
        acct_name = base_name[:200]
        acct_type = _plaid_account_type_for_investment(a)

        # Prefer linking to an existing CHASE account when possible (avoid duplicates like "Chase IRA" vs "Chase IRA ****8839").
        existing_account_id = _pick_existing_chase_account_for_plaid(
            session,
            taxpayer_entity_id=int(connection.taxpayer_entity_id),
            account_type=str(acct_type),
            label=label,
            mask_last4=mask or None,
        )

        # If this provider_account_id already maps somewhere, allow a safe re-link to the existing account:
        # only if the old account appears unused (no txns/lots/cash/taxlots).
        if provider_account_id in account_map and existing_account_id and int(account_map[provider_account_id]) != int(existing_account_id):
            old_id = int(account_map[provider_account_id])
            try:
                has_txn = session.query(Transaction.id).filter(Transaction.account_id == old_id).limit(1).first() is not None
                has_lots = session.query(PositionLot.id).filter(PositionLot.account_id == old_id).limit(1).first() is not None
                has_cash = session.query(CashBalance.id).filter(CashBalance.account_id == old_id).limit(1).first() is not None
                has_taxlots = session.query(TaxLot.id).filter(TaxLot.account_id == old_id).limit(1).first() is not None
                has_any = bool(has_txn or has_lots or has_cash or has_taxlots)
            except Exception:
                has_any = True
            if not has_any:
                try:
                    mrow = (
                        session.query(ExternalAccountMap)
                        .filter(
                            ExternalAccountMap.connection_id == connection.id,
                            ExternalAccountMap.provider_account_id == provider_account_id,
                        )
                        .one_or_none()
                    )
                    if mrow is not None:
                        mrow.account_id = int(existing_account_id)
                        session.flush()
                        account_map[provider_account_id] = int(existing_account_id)
                        log_change(
                            session,
                            actor=actor,
                            action="UPDATE",
                            entity="ExternalAccountMap",
                            entity_id=str(mrow.id),
                            old={"account_id": old_id},
                            new={"account_id": int(existing_account_id)},
                            note="Re-linked Plaid investment account to existing Chase account (dedupe)",
                        )
                except Exception:
                    pass
            continue

        if provider_account_id in account_map:
            continue

        if existing_account_id is not None:
            account_map[provider_account_id] = int(existing_account_id)
            session.add(
                ExternalAccountMap(
                    connection_id=connection.id,
                    provider_account_id=provider_account_id,
                    account_id=int(existing_account_id),
                )
            )
            log_change(
                session,
                actor=actor,
                action="CREATE",
                entity="ExternalAccountMap",
                entity_id=str(connection.id),
                old=None,
                new={"provider_account_id": provider_account_id, "account_id": int(existing_account_id)},
                note="Linked Plaid investment account to existing Chase account",
            )
            continue

        # Create a new Account (no suitable existing match).
        if session.query(Account).filter(Account.name == acct_name).one_or_none() is not None:
            acct_name = _unique_plaid_account_name(base=base_name, provider_account_id=provider_account_id)[:200]
        acct = Account(
            name=acct_name,
            broker="CHASE",
            account_type=acct_type,
            taxpayer_entity_id=connection.taxpayer_entity_id,
        )
        session.add(acct)
        session.flush()
        log_change(
            session,
            actor=actor,
            action="CREATE",
            entity="Account",
            entity_id=str(acct.id),
            old=None,
            new={"name": acct.name, "broker": acct.broker, "account_type": acct.account_type},
            note=f"Created from Plaid investment account (connection={connection.id})",
        )
        account_map[provider_account_id] = int(acct.id)
        session.add(
            ExternalAccountMap(connection_id=connection.id, provider_account_id=provider_account_id, account_id=acct.id)
        )

    return account_map, warnings


def _compute_incremental_range(
    *,
    now: dt.date,
    last_successful_sync_at: Optional[dt.datetime],
    last_successful_txn_end: Optional[dt.date] = None,
    overlap_days: int,
) -> tuple[dt.date, dt.date]:
    overlap_days = max(0, min(30, int(overlap_days)))
    if last_successful_txn_end is not None:
        start = last_successful_txn_end - dt.timedelta(days=overlap_days)
    elif last_successful_sync_at is not None:
        start = last_successful_sync_at.date() - dt.timedelta(days=overlap_days)
    else:
        start = now - dt.timedelta(days=90)
    end = now
    return start, end


def _fallback_spans() -> list[dt.timedelta]:
    return [
        dt.timedelta(days=365 * 10),
        dt.timedelta(days=365 * 5),
        dt.timedelta(days=365 * 3),
        dt.timedelta(days=365 * 2),
        dt.timedelta(days=365),
        dt.timedelta(days=180),
        dt.timedelta(days=90),
    ]


def _negotiate_full_range(
    *,
    adapter: BrokerAdapter,
    connection: Any,
    requested_start: dt.date,
    requested_end: dt.date,
) -> tuple[dt.date, dt.date]:
    # Try requested range first, then progressively shrink to end-fixed windows.
    try:
        adapter.fetch_transactions(connection, requested_start, requested_end, cursor=None)
        return requested_start, requested_end
    except RangeTooLargeError:
        pass

    for span in _fallback_spans():
        start = max(requested_start, requested_end - span)
        try:
            adapter.fetch_transactions(connection, start, requested_end, cursor=None)
            return start, requested_end
        except RangeTooLargeError:
            continue
    # If still failing, surface last error as config/provider issue.
    raise RangeTooLargeError("Provider rejected all fallback ranges as too large.")


@dataclass(frozen=True)
class SyncResult:
    status: str
    coverage: dict[str, Any]
    warnings: list[str]


def run_sync(
    session: Session,
    *,
    connection_id: int,
    mode: str,
    start_date: dt.date | None = None,
    end_date: dt.date | None = None,
    overlap_days: int = 7,
    store_payloads: bool | None = None,
    actor: str = "sync",
    pull_holdings: bool = True,
    reprocess_files: bool = False,
) -> SyncRun:
    conn = session.query(ExternalConnection).filter(ExternalConnection.id == connection_id).one()
    if (conn.status or "").upper() != "ACTIVE":
        raise SyncConfigError("Connection is disabled.")
    adapter = _adapter_for(conn)

    now_dt = utcnow()
    today = now_dt.date()
    mode_u = (mode or "").upper()
    if mode_u not in {"FULL", "INCREMENTAL"}:
        raise ValueError("mode must be FULL or INCREMENTAL")

    run_settings: dict[str, Any] = {}
    offline_metrics: dict[str, Any] | None = None
    offline_holdings_metrics: dict[str, Any] | None = None
    connector = (conn.connector or "").upper()
    is_offline_files = connector in {"IB_FLEX_OFFLINE", "CHASE_OFFLINE", "RJ_OFFLINE"}
    if connector in {"CHASE_PLAID", "AMEX_PLAID"}:
        meta = conn.metadata_json or {}
        if connector == "AMEX_PLAID" and bool(meta.get("plaid_force_transactions_get")):
            run_settings["plaid_force_transactions_get"] = True
    # Allow importing offline holdings snapshots (e.g., baseline statement valuations) even for live connectors.
    # This is especially useful for IB Flex Web when historical holdings snapshots cannot be fetched via the API.
    holdings_files: list[dict[str, Any]] = []
    if is_offline_files:
        files, offline_metrics = _select_offline_files(
            session, connection=conn, mode=mode_u, reprocess_files=bool(reprocess_files)
        )
        holdings_files, offline_holdings_metrics = _select_offline_holdings_files(
            session, connection=conn, mode=mode_u, reprocess_files=bool(reprocess_files)
        )
        run_settings["selected_files"] = files
        run_settings["holdings_files"] = holdings_files
        run_settings["offline_metrics"] = offline_metrics
        run_settings["offline_holdings_metrics"] = offline_holdings_metrics
    else:
        # For live connectors, only select holdings-like files; transactions should still come from the adapter.
        holdings_files, offline_holdings_metrics = _select_offline_holdings_files(
            session, connection=conn, mode=mode_u, reprocess_files=bool(reprocess_files)
        )
        if holdings_files:
            run_settings["holdings_files"] = holdings_files
            run_settings["offline_holdings_metrics"] = offline_holdings_metrics

    ctx = AdapterConnectionContext(
        connection=conn,
        credentials={
            "IB_YODLEE_TOKEN": get_credential(session, connection_id=conn.id, key="IB_YODLEE_TOKEN"),
            "IB_YODLEE_QUERY_ID": get_credential(session, connection_id=conn.id, key="IB_YODLEE_QUERY_ID"),
            "IB_FLEX_TOKEN": get_credential(session, connection_id=conn.id, key="IB_FLEX_TOKEN"),
            "IB_FLEX_QUERY_ID": get_credential(session, connection_id=conn.id, key="IB_FLEX_QUERY_ID"),
            "YODLEE_ACCESS_TOKEN": get_credential(session, connection_id=conn.id, key="YODLEE_ACCESS_TOKEN"),
            "YODLEE_REFRESH_TOKEN": get_credential(session, connection_id=conn.id, key="YODLEE_REFRESH_TOKEN"),
            "PLAID_ACCESS_TOKEN": get_credential(session, connection_id=conn.id, key="PLAID_ACCESS_TOKEN"),
            "PLAID_ITEM_ID": get_credential(session, connection_id=conn.id, key="PLAID_ITEM_ID"),
        },
        run_settings=run_settings,
    )

    if mode_u == "INCREMENTAL":
        eff_start, eff_end = _compute_incremental_range(
            now=today,
            last_successful_sync_at=conn.last_successful_sync_at,
            last_successful_txn_end=conn.last_successful_txn_end,
            overlap_days=overlap_days,
        )
        # Plaid can backfill up to ~24 months of history. Make the *first* Plaid run do a full backfill
        # automatically so users don't have to remember to run FULL.
        #
        # This also ensures `/investments/transactions/get` (which is date-range based) pulls the full history.
        if (conn.connector or "").upper() in {"CHASE_PLAID", "AMEX_PLAID"} and start_date is None:
            meta = conn.metadata_json or {}
            backfill_done = bool(meta.get("plaid_initial_backfill_done") is True)
            # If Investments was enabled after initial sync, force a one-time backfill to hydrate investment txns.
            inv_enabled = bool(meta.get("plaid_enable_investments") is True)
            inv_backfill_done = bool(meta.get("plaid_investments_backfill_done") is True)
            # Bank/credit history is cursor-driven; the effective start date does not change what Plaid returns.
            # Keep a "backfill" marker so we can avoid declaring it complete until Plaid reports historical complete.
            if not backfill_done:
                run_settings["plaid_backfill_24m"] = True

            # Investment transactions are date-range based, so we must explicitly request the full ~24 months once.
            if inv_enabled and not inv_backfill_done:
                eff_start = today - dt.timedelta(days=730)
                run_settings["plaid_investments_backfill_24m"] = True
        if start_date is not None:
            eff_start = start_date
        eff_end = today
        # If we auto-backfill, record the requested start for auditability.
        requested_start = eff_start if run_settings.get("plaid_backfill_24m") and start_date is None else start_date
        requested_end = end_date
        store_payloads = False if store_payloads is None else bool(store_payloads)
    else:
        requested_start = start_date or (today - dt.timedelta(days=365 * 10))
        requested_end = end_date or today
        # FULL defaults: store payloads ON.
        store_payloads = True if store_payloads is None else bool(store_payloads)
        # Set initial effective range; may be negotiated down below (after a SyncRun record exists).
        eff_start, eff_end = requested_start, requested_end

    # Provide effective range and settings to adapters (useful for live connectors and shared caching).
    run_settings["effective_start_date"] = eff_start.isoformat()
    run_settings["effective_end_date"] = eff_end.isoformat()
    run_settings["store_payloads"] = bool(store_payloads)

    run = SyncRun(
        connection_id=conn.id,
        status="ERROR",
        mode=mode_u,
        requested_start_date=requested_start,
        requested_end_date=requested_end,
        effective_start_date=eff_start,
        effective_end_date=eff_end,
        store_payloads=bool(store_payloads),
        pages_fetched=0,
        txn_count=0,
        new_count=0,
        dupes_count=0,
        parse_fail_count=0,
        missing_symbol_count=0,
        error_json=None,
        coverage_json={},
    )
    session.add(run)
    session.flush()

    log_change(
        session,
        actor=actor,
        action="SYNC_RUN_STARTED",
        entity="SyncRun",
        entity_id=str(run.id),
        old=None,
        new={
            "connection_id": conn.id,
            "mode": mode_u,
            "effective_start_date": eff_start.isoformat(),
            "effective_end_date": eff_end.isoformat(),
            "store_payloads": bool(store_payloads),
        },
        note=f"Sync run started for connection={conn.id}",
    )
    # Important for SQLite: commit the "run started" record immediately so:
    #  - other UI requests can see there's an in-flight run (finished_at IS NULL)
    #  - we don't hold a write transaction open while doing network I/O
    # This reduces "database is locked" errors from concurrent writes/double-submits.
    session.commit()

    warnings: list[str] = []
    coverage: dict[str, Any] = {
        "earliest_txn_date": None,
        "latest_txn_date": None,
        # txn_count = number of provider items processed (including dupes and parse fails).
        "txn_count": 0,
        "txn_type_counts": {},
        "pages_fetched": 0,
        "missing_symbol_count": 0,
        "parse_fail_count": 0,
        "new_inserted": 0,
        "duplicates_skipped": 0,
        "updated_existing": 0,
        "holdings_asof": None,
        "holdings_items_imported": 0,
        "holdings_snapshots_imported": 0,
        "accounts_fetched": 0,
        "file_count": 0,  # offline: files processed this run
        "file_new_recorded": 0,  # offline: new file hashes added
        "file_selected": 0,
        "file_skipped_seen": 0,
        "holdings_file_selected": 0,
        "holdings_file_skipped_seen": 0,
        "data_dir": None,
        # Live connectors: report-level payload idempotency tracking.
        "report_payloads_recorded": 0,
        "report_payloads_skipped": 0,
    }
    if offline_metrics:
        coverage["data_dir"] = offline_metrics.get("data_dir")
        coverage["file_total"] = int(offline_metrics.get("file_total") or 0)
        coverage["file_total_all"] = int(offline_metrics.get("file_total_all") or 0)
        coverage["file_unsupported_total"] = int(offline_metrics.get("file_unsupported_total") or 0)
        coverage["file_selected"] = int(offline_metrics.get("file_selected") or 0)
        coverage["file_skipped_seen"] = int(offline_metrics.get("file_skipped_seen") or 0)
    if offline_holdings_metrics:
        coverage["holdings_file_total"] = int(offline_holdings_metrics.get("holdings_file_total") or 0)
        coverage["holdings_file_selected"] = int(offline_holdings_metrics.get("holdings_file_selected") or 0)
        coverage["holdings_file_skipped_seen"] = int(offline_holdings_metrics.get("holdings_file_skipped_seen") or 0)

    try:
        # FULL mode negotiation happens after the SyncRun record exists so network/provider failures do not 500 the UI.
        if mode_u == "FULL":
            try:
                eff_start, eff_end = _negotiate_full_range(
                    adapter=adapter, connection=ctx, requested_start=requested_start or eff_start, requested_end=requested_end or eff_end
                )
                run.effective_start_date = eff_start
                run.effective_end_date = eff_end
                run_settings["effective_start_date"] = eff_start.isoformat()
                run_settings["effective_end_date"] = eff_end.isoformat()
                session.flush()
            except ProviderError as e:
                warnings.append(f"FULL range negotiation failed: {type(e).__name__}: {e}")
                coverage["parse_fail_count"] = int(coverage.get("parse_fail_count") or 0)
                run.status = "ERROR"
                run.finished_at = utcnow()
                run.error_json = json.dumps({"error": f"{type(e).__name__}: {e}"})
                run.coverage_json = coverage | {"warnings": warnings, "error": f"{type(e).__name__}: {e}"}
                conn.last_error_json = json.dumps(
                    {"at": utcnow().isoformat(), "run_id": run.id, "error": f"{type(e).__name__}: {e}"}
                )
                conn.coverage_status = compute_coverage_status(conn, latest_run=run)
                session.flush()
                log_change(
                    session,
                    actor=actor,
                    action="SYNC_RUN_FINISHED",
                    entity="SyncRun",
                    entity_id=str(run.id),
                    old=None,
                    new={"status": run.status, "error": f"{type(e).__name__}: {e}"},
                    note="Sync run error (range negotiation)",
                )
                session.commit()
                return run

        accounts = adapter.fetch_accounts(ctx)
        if store_payloads:
            session.add(
                ExternalPayloadSnapshot(sync_run_id=run.id, kind="accounts", cursor=None, payload_json={"items": accounts})
            )
        coverage["accounts_fetched"] = len(accounts)
        if not accounts:
            run.status = "ERROR"
            run.finished_at = utcnow()
            run.coverage_json = coverage
            session.flush()
            log_change(
                session,
                actor=actor,
                action="SYNC_RUN_FINISHED",
                entity="SyncRun",
                entity_id=str(run.id),
                old=None,
                new={"status": run.status, "coverage": coverage},
                note="ERROR: 0 accounts fetched",
            )
            session.commit()
            return run

        connector_u = (conn.connector or "").upper()
        is_plaid_connector = connector_u in {"CHASE_PLAID", "AMEX_PLAID"}
        if is_plaid_connector:
            _store_plaid_liabilities_snapshot(
                session,
                connection=conn,
                run=run,
                access_token=str(ctx.credentials.get("PLAID_ACCESS_TOKEN") or ""),
                store_payloads=bool(store_payloads),
                coverage=coverage,
                warnings=warnings,
            )

        is_plaid_chase = connector_u == "CHASE_PLAID"
        is_expenses_connector = is_plaid_connector

        account_map: dict[str, int] = {}
        expense_account_id_by_provider: dict[str, int] = {}
        if not is_plaid_connector:
            account_map, acct_warnings = _upsert_account_map(session, connection=conn, accounts=accounts, actor=actor)
            warnings.extend(acct_warnings)
        else:
            # For expenses connectors, do not create investment Accounts. Map provider accounts to ExpenseAccount.
            # However, if Plaid exposes investment accounts, we *do* create Account + ExternalAccountMap for those
            # so Holdings snapshots can be displayed and used in Performance reporting.
            if is_plaid_chase:
                account_map, acct_warnings = _upsert_plaid_investment_account_map(
                    session, connection=conn, accounts=accounts, actor=actor
                )
                warnings.extend(acct_warnings)
            for a in accounts:
                # Skip Plaid investment accounts from Expenses; those are handled as Accounts + holdings/transactions.
                if _plaid_is_investment_account(a):
                    continue
                provider_account_id = str(a.get("provider_account_id") or "").strip()
                if not provider_account_id:
                    continue
                institution = str(conn.broker or "Chase").strip() or "Chase"
                name = str(a.get("name") or provider_account_id).strip()
                last4 = str(a.get("mask") or "").strip() or None
                acct_type = str(a.get("account_type") or "UNKNOWN").strip().upper()
                # Create or reuse ExpenseAccount.
                q = session.query(ExpenseAccount).filter(
                    ExpenseAccount.institution == institution,
                    ExpenseAccount.name == name,
                )
                if last4:
                    q = q.filter(ExpenseAccount.last4_masked == last4)
                row = q.one_or_none()
                if row is None:
                    row = ExpenseAccount(
                        institution=institution,
                        name=name,
                        last4_masked=last4,
                        type=acct_type,
                        provider_account_id=provider_account_id,
                    )
                    session.add(row)
                    session.flush()
                elif not (row.provider_account_id or "").strip():
                    row.provider_account_id = provider_account_id
                    session.flush()
                expense_account_id_by_provider[provider_account_id] = int(row.id)

            # Update current balances for expense accounts (if provided by Plaid).
            try:
                from src.db.models import ExpenseAccountBalance
            except Exception:
                ExpenseAccountBalance = None  # type: ignore
            if ExpenseAccountBalance is not None:
                now_ts = utcnow()
                for a in accounts:
                    if _plaid_is_investment_account(a):
                        continue
                    provider_account_id = str(a.get("provider_account_id") or "").strip()
                    if not provider_account_id:
                        continue
                    exp_id = expense_account_id_by_provider.get(provider_account_id)
                    if exp_id is None:
                        continue
                    bal_current = a.get("balance_current")
                    bal_available = a.get("balance_available")
                    currency = str(a.get("balance_currency") or "USD").strip().upper() or "USD"
                    if bal_current is None and bal_available is None:
                        continue
                    existing = (
                        session.query(ExpenseAccountBalance)
                        .filter(ExpenseAccountBalance.expense_account_id == exp_id)
                        .one_or_none()
                    )
                    if existing is None:
                        session.add(
                            ExpenseAccountBalance(
                                expense_account_id=exp_id,
                                as_of_date=now_ts,
                                balance_current=bal_current,
                                balance_available=bal_available,
                                currency=currency,
                                source="PLAID",
                            )
                        )
                    else:
                        existing.as_of_date = now_ts
                        existing.balance_current = bal_current
                        existing.balance_available = bal_available
                        existing.currency = currency
                        existing.source = "PLAID"
                    coverage["expense_balances_updated"] = int(coverage.get("expense_balances_updated") or 0) + 1

        # Transactions: paginate until exhausted.
        cursor: str | None = None
        exhausted = False
        earliest: dt.date | None = None
        latest: dt.date | None = None
        is_offline_flex = (conn.provider or "").upper() == "IB" and (conn.connector or "").upper() == "IB_FLEX_OFFLINE"
        expense_batch_id: int | None = None

        if is_offline_files and not ((ctx.run_settings or {}).get("selected_files") or []):
            exhausted = True
        while not exhausted:
            try:
                items, next_cursor = adapter.fetch_transactions(ctx, eff_start, eff_end, cursor=cursor)
            except ProviderError as e:
                # For cursor-based live connectors (e.g., Plaid), provider errors are generally fatal
                # (token revoked, ITEM_LOGIN_REQUIRED, etc.). Surface as ERROR so the UI clearly prompts re-linking.
                if (conn.connector or "").upper() in {"CHASE_PLAID", "AMEX_PLAID"}:
                    raise
                warnings.append(f"Provider error during pagination: {type(e).__name__}: {e}")
                break

            coverage["pages_fetched"] += 1
            # Live connectors can emit a report-level payload marker for idempotency. If we've already imported
            # this exact payload hash for this connection, skip processing this page to avoid duplicating
            # holdings snapshots/cash balances/etc.
            skip_page = False
            for it in items:
                if str(it.get("record_kind") or "").strip().upper() != "REPORT_PAYLOAD":
                    continue
                payload_hash = str(it.get("payload_hash") or "").strip()
                if not payload_hash:
                    continue
                existing_payload = (
                    session.query(ExternalFileIngest)
                    .filter(
                        ExternalFileIngest.connection_id == conn.id,
                        ExternalFileIngest.kind == "REPORT_PAYLOAD",
                        ExternalFileIngest.file_hash == payload_hash,
                    )
                    .one_or_none()
                )
                if existing_payload is not None and not reprocess_files:
                    coverage["report_payloads_skipped"] = int(coverage.get("report_payloads_skipped") or 0) + 1
                    skip_page = True
                    skipped = (ctx.run_settings or {}).setdefault("skipped_payload_hashes", [])
                    if isinstance(skipped, list) and payload_hash not in skipped:
                        skipped.append(payload_hash)
                    continue
                if existing_payload is None:
                    try:
                        with session.begin_nested():
                            session.add(
                                ExternalFileIngest(
                                    connection_id=conn.id,
                                    kind="REPORT_PAYLOAD",
                                    file_name=str(it.get("source") or "REPORT"),
                                    file_hash=payload_hash,
                                    file_bytes=int(it.get("bytes") or 0) or None,
                                    file_mtime=None,
                                )
                            )
                        coverage["report_payloads_recorded"] = int(coverage.get("report_payloads_recorded") or 0) + 1
                        newly = (ctx.run_settings or {}).setdefault("new_payload_hashes", [])
                        if isinstance(newly, list) and payload_hash not in newly:
                            newly.append(payload_hash)
                    except IntegrityError:
                        coverage["report_payloads_skipped"] = int(coverage.get("report_payloads_skipped") or 0) + 1
                        skip_page = True
                        skipped = (ctx.run_settings or {}).setdefault("skipped_payload_hashes", [])
                        if isinstance(skipped, list) and payload_hash not in skipped:
                            skipped.append(payload_hash)
            if skip_page:
                warnings.append("Report payload already imported; skipped processing this page (idempotent).")
                cursor = next_cursor
                exhausted = next_cursor is None
                continue
            if is_offline_files:
                try:
                    idx = int(cursor) if cursor is not None else 0
                    selected_files = (ctx.run_settings or {}).get("selected_files") or []
                    if 0 <= idx < len(selected_files):
                        f = selected_files[idx]
                        coverage["file_count"] += 1
                        pth = Path(str(f.get("path") or ""))
                        ext = pth.suffix.lower()
                        ingest_kind = "TRANSACTIONS"
                        meta_json: dict[str, Any] | None = None
                        start_hint: dt.date | None = None
                        end_hint: dt.date | None = None
                        stored_path = None
                        if ext in {".qfx", ".ofx"} and (conn.connector or "").upper() == "RJ_OFFLINE":
                            ingest_kind = "QFX"
                            try:
                                from src.adapters.rj_offline.qfx_parser import extract_qfx_header_meta

                                txt = pth.read_text(encoding="utf-8-sig", errors="ignore")
                                hdr = extract_qfx_header_meta(txt)
                                start_hint = hdr.dt_start
                                end_hint = hdr.dt_end or hdr.dt_asof
                                meta_json = {
                                    "broker_id": hdr.broker_id,
                                    "acct_id": hdr.acct_id,
                                    "dt_start": hdr.dt_start.isoformat() if hdr.dt_start else None,
                                    "dt_end": hdr.dt_end.isoformat() if hdr.dt_end else None,
                                    "dt_asof": hdr.dt_asof.isoformat() if hdr.dt_asof else None,
                                    "org": hdr.org,
                                    "fid": hdr.fid,
                                    "trnuid": hdr.intuid,
                                }
                            except Exception:
                                meta_json = None
                        stored_path = _archive_raw_file(connection_id=conn.id, file_hash=str(f.get("file_hash") or ""), src_path=pth)
                        with session.begin_nested():
                            session.add(
                                ExternalFileIngest(
                                    connection_id=conn.id,
                                    kind=ingest_kind,
                                    file_name=str(f.get("file_name") or pth.name),
                                    file_hash=str(f.get("file_hash")),
                                    file_bytes=int(f.get("file_bytes") or 0) or None,
                                    file_mtime=dt.datetime.fromisoformat(str(f.get("file_mtime_iso")))
                                    if f.get("file_mtime_iso")
                                    else None,
                                    stored_path=stored_path,
                                    start_date_hint=start_hint,
                                    end_date_hint=end_hint,
                                    metadata_json=meta_json,
                                )
                            )
                        coverage["file_new_recorded"] += 1
                except IntegrityError:
                    pass
                except Exception:
                    pass
            if store_payloads:
                session.add(
                    ExternalPayloadSnapshot(
                        sync_run_id=run.id,
                        kind="transactions_page",
                        cursor=cursor,
                        payload_json={"items": items, "next_cursor": next_cursor},
                    )
                )

            for it in items:
                record_kind = str(it.get("record_kind") or "TRANSACTION").strip().upper()
                try:
                    if record_kind == "REPORT_PAYLOAD":
                        continue
                    if record_kind == "SYNC_CURSOR":
                        # Adapter-supplied cursor updates (persisted after SUCCESS).
                        kind = str(it.get("cursor_kind") or "").strip().upper()
                        cur = str(it.get("cursor") or "").strip()
                        if kind == "PLAID_TRANSACTIONS" and cur:
                            ctx.run_settings["plaid_transactions_cursor"] = cur
                            coverage["plaid_transactions_cursor_updated"] = True
                            st = str(it.get("transactions_update_status") or "").strip()
                            if st:
                                ctx.run_settings["plaid_transactions_update_status"] = st
                                coverage["plaid_transactions_update_status"] = st
                            if "historical_complete" in it:
                                try:
                                    hc = bool(it.get("historical_complete"))
                                    ctx.run_settings["plaid_transactions_historical_complete"] = hc
                                    coverage["plaid_transactions_historical_complete"] = hc
                                except Exception:
                                    pass
                        continue
                    if record_kind == "ADAPTER_WARNING":
                        msg = str(it.get("message") or "").strip()
                        if msg:
                            warnings.append(msg)
                        continue
                    if record_kind == "EXPENSE_TXN":
                        # Expense transactions are stored in `expense_transactions` (not the investment `transactions` table).
                        provider_account_id = str(it.get("provider_account_id") or it.get("account_id") or "").strip()
                        exp_acct_id = expense_account_id_by_provider.get(provider_account_id)
                        if exp_acct_id is None:
                            if expense_account_id_by_provider:
                                exp_acct_id = next(iter(expense_account_id_by_provider.values()))
                                warnings.append(
                                    f"Missing expense account mapping for provider_account_id={provider_account_id}; used expense_account_id={exp_acct_id}."
                                )
                            else:
                                raise ValueError("No expense accounts available for this connection.")

                        d_s = str(it.get("date") or "").strip()
                        posted = dt.date.fromisoformat(d_s[:10])
                        amount = Decimal(str(it.get("amount") or "0")).quantize(Decimal("0.01"))
                        currency = str(it.get("currency") or "USD").strip().upper() or "USD"
                        desc_raw = str(it.get("description") or "").strip() or "Unknown"

                        # Use bank vs card merchant normalization based on ExpenseAccount.type.
                        exp_acct = session.query(ExpenseAccount).filter(ExpenseAccount.id == exp_acct_id).one()
                        desc_norm = normalize_description(desc_raw)
                        if (exp_acct.type or "").upper() == "CREDIT":
                            merchant_norm = normalize_merchant(desc_raw)
                        else:
                            merchant_norm = normalize_bank_merchant(desc_raw)

                        provider_txn_id = str(it.get("provider_transaction_id") or "").strip() or None
                        # Plaid provides a stable transaction_id; use it as the authoritative idempotency key
                        # to avoid duplicating when Plaid modifies a transaction (pending→posted, date tweaks, etc.).
                        if provider_txn_id:
                            txn_id = hashlib.sha256(f"PLAID:{provider_txn_id}".encode("utf-8")).hexdigest()
                        else:
                            txn_id = stable_txn_id(
                                institution=str(exp_acct.institution),
                                account_name=str(exp_acct.name),
                                posted_date=posted,
                                amount=amount,
                                description_norm=desc_norm,
                                currency=currency,
                                external_id=None,
                            )

                        if expense_batch_id is None:
                            h = hashlib.sha256(f"PLAID:{conn.id}:{run.id}".encode("utf-8")).hexdigest()
                            batch = ExpenseImportBatch(
                                source="PLAID",
                                file_name=f"plaid:sync_run:{run.id}",
                                file_hash=h,
                                row_count=0,
                                duplicates_skipped=0,
                                metadata_json={"connection_id": int(conn.id), "connector": str(conn.connector or "")},
                            )
                            session.add(batch)
                            session.flush()
                            expense_batch_id = int(batch.id)

                        category_hint = str(it.get("category_hint") or "").strip() or None
                        raw_payload = it.get("raw") if isinstance(it.get("raw"), dict) else None
                        raw_cardholder = None
                        if raw_payload:
                            raw_cardholder = (
                                str(
                                    raw_payload.get("authorized_user")
                                    or raw_payload.get("authorized_user_name")
                                    or raw_payload.get("account_owner")
                                    or ""
                                )
                                .strip()
                                or None
                            )
                        inserted = False
                        try:
                            with session.begin_nested():
                                session.add(
                                    ExpenseTransaction(
                                        txn_id=txn_id,
                                        expense_account_id=exp_acct_id,
                                        institution=str(exp_acct.institution),
                                        account_name=str(exp_acct.name),
                                        posted_date=posted,
                                        transaction_date=None,
                                        description_raw=desc_raw,
                                        description_norm=desc_norm,
                                        merchant_norm=merchant_norm,
                                        amount=float(amount),
                                        currency=currency,
                                        account_last4_masked=str(exp_acct.last4_masked or "")[:8] or None,
                                        cardholder_name=raw_cardholder,
                                        category_hint=category_hint,
                                        category_user=None,
                                        category_system=None,
                                        tags_json=[],
                                        notes=None,
                                        import_batch_id=expense_batch_id,
                                        original_row_json=raw_payload,
                                    )
                                )
                                inserted = True
                        except IntegrityError:
                            inserted = False

                        if inserted:
                            if expense_batch_id is not None:
                                try:
                                    batch = session.query(ExpenseImportBatch).filter(ExpenseImportBatch.id == expense_batch_id).one()
                                    batch.row_count = int(batch.row_count or 0) + 1
                                    session.flush()
                                except Exception:
                                    pass
                        else:
                            # Upsert: update key fields when Plaid reports the txn as modified.
                            existing = session.query(ExpenseTransaction).filter(ExpenseTransaction.txn_id == txn_id).one_or_none()
                            if existing is not None:
                                existing.posted_date = posted
                                existing.description_raw = desc_raw
                                existing.description_norm = desc_norm
                                existing.merchant_norm = merchant_norm
                                existing.amount = float(amount)
                                existing.currency = currency
                                if category_hint and not (existing.category_hint or "").strip():
                                    existing.category_hint = category_hint
                                if raw_cardholder and not (existing.cardholder_name or "").strip():
                                    existing.cardholder_name = raw_cardholder
                                if raw_payload and not (existing.original_row_json or {}):
                                    existing.original_row_json = raw_payload
                                session.flush()
                                coverage["updated_existing"] = int(coverage.get("updated_existing") or 0) + 1
                            else:
                                coverage["duplicates_skipped"] += 1
                                if expense_batch_id is not None:
                                    try:
                                        batch = session.query(ExpenseImportBatch).filter(ExpenseImportBatch.id == expense_batch_id).one()
                                        batch.duplicates_skipped = int(batch.duplicates_skipped or 0) + 1
                                        session.flush()
                                    except Exception:
                                        pass
                                continue

                        if inserted:
                            coverage["new_inserted"] += 1
                            coverage["txn_count"] += 1
                            coverage["expenses_txns_imported"] = int(coverage.get("expenses_txns_imported") or 0) + 1
                        coverage["expenses_txns_processed"] = int(coverage.get("expenses_txns_processed") or 0) + 1
                        if earliest is None or posted < earliest:
                            earliest = posted
                        if latest is None or posted > latest:
                            latest = posted
                        continue
                    if record_kind == "CASH_BALANCE":
                        provider_account_id = str(it.get("provider_account_id") or it.get("account_id") or "")
                        account_id = account_map.get(provider_account_id)
                        if account_id is None:
                            account_id = next(iter(account_map.values()))
                            warnings.append(f"Missing provider_account_id mapping for cash balance; used account_id={account_id}.")
                        ccy = str(it.get("currency") or "USD").strip().upper()
                        if ccy != "USD":
                            warnings.append(f"Ignored non-USD cash balance for {provider_account_id}: {ccy}")
                            continue
                        as_of_date_s = str(it.get("as_of_date") or it.get("date") or "")
                        try:
                            as_of_date = dt.date.fromisoformat(as_of_date_s[:10])
                        except Exception:
                            as_of_date = today
                        amount = float(it.get("amount") or 0.0)
                        existing_cb = (
                            session.query(CashBalance)
                            .filter(CashBalance.account_id == account_id, CashBalance.as_of_date == as_of_date)
                            .order_by(CashBalance.id.desc())
                            .first()
                        )
                        with session.begin_nested():
                            if existing_cb is not None:
                                existing_cb.amount = amount
                                coverage["cash_balances_updated"] = int(coverage.get("cash_balances_updated") or 0) + 1
                            else:
                                session.add(CashBalance(account_id=account_id, as_of_date=as_of_date, amount=amount))
                                coverage["cash_balances_imported"] = int(coverage.get("cash_balances_imported") or 0) + 1
                        continue
                    if record_kind == "BROKER_SYMBOL_SUMMARY":
                        coverage["symbol_summary_rows_seen"] = int(coverage.get("symbol_summary_rows_seen") or 0) + 1
                        continue
                    if record_kind in {"BROKER_CLOSED_LOT", "BROKER_WASH_SALE"}:
                        symbol = str(it.get("symbol") or it.get("ticker") or "").strip().upper()
                        if not symbol:
                            coverage["parse_fail_count"] += 1
                            continue
                        provider_account_id = str(it.get("provider_account_id") or "")
                        trade_date = dt.date.fromisoformat(str(it.get("date")))
                        qty = _float_or_none(it.get("qty"))
                        if qty is None:
                            coverage["parse_fail_count"] += 1
                            continue
                        qty = abs(qty)
                        cost_basis = _float_or_none(it.get("cost_basis"))
                        realized = _float_or_none(it.get("realized_pl_fifo"))
                        proceeds = _float_or_none(it.get("proceeds_derived"))
                        if proceeds is None and cost_basis is not None and realized is not None:
                            proceeds = cost_basis + realized
                        if proceeds is None:
                            warns = coverage.get("warnings") or []
                            warns.append(f"Broker {record_kind} missing proceeds derivation for {symbol} on {trade_date}.")
                            coverage["warnings"] = warns

                        source_file_hash = str(it.get("source_file_hash") or "")
                        if not source_file_hash:
                            source_file_hash = "UNKNOWN"

                        if record_kind == "BROKER_CLOSED_LOT":
                            row = BrokerLotClosure(
                                connection_id=conn.id,
                                taxpayer_entity_id=conn.taxpayer_entity_id,
                                provider_account_id=provider_account_id or "IBFLEX-1",
                                symbol=symbol,
                                conid=str(it.get("conid")) if it.get("conid") not in (None, "") else None,
                                trade_date=trade_date,
                                datetime_raw=str(it.get("datetime_raw")) if it.get("datetime_raw") not in (None, "") else None,
                                open_datetime_raw=str(it.get("open_datetime_raw")) if it.get("open_datetime_raw") not in (None, "") else None,
                                quantity_closed=qty,
                                cost_basis=cost_basis,
                                realized_pl_fifo=realized,
                                proceeds_derived=proceeds,
                                currency=str(it.get("currency")) if it.get("currency") not in (None, "") else None,
                                fx_rate_to_base=_float_or_none(it.get("fx_rate_to_base")),
                                ib_transaction_id=str(it.get("ib_transaction_id")) if it.get("ib_transaction_id") not in (None, "") else None,
                                ib_trade_id=str(it.get("ib_trade_id")) if it.get("ib_trade_id") not in (None, "") else None,
                                source_file_hash=source_file_hash,
                                raw_json={"row": it.get("raw_row") or {}, "source_file": it.get("source_file"), "source_row": it.get("source_row")},
                            )
                            try:
                                with session.begin_nested():
                                    session.add(row)
                                    session.flush()
                                coverage["closed_lot_rows_imported"] = int(coverage.get("closed_lot_rows_imported") or 0) + 1
                            except IntegrityError:
                                coverage["closed_lot_rows_dupes"] = int(coverage.get("closed_lot_rows_dupes") or 0) + 1

                            # YTD gain summary (planning-grade) for the connection detail view.
                            if trade_date.year == today.year and realized is not None:
                                open_d = _parse_ib_date(it.get("open_datetime_raw"))
                                if open_d is None:
                                    coverage["broker_gains_ytd_unknown"] = float(coverage.get("broker_gains_ytd_unknown") or 0.0) + float(realized)
                                else:
                                    term = "LT" if (trade_date - open_d).days >= 365 else "ST"
                                    k = "broker_gains_ytd_lt" if term == "LT" else "broker_gains_ytd_st"
                                    coverage[k] = float(coverage.get(k) or 0.0) + float(realized)
                        else:
                            row = BrokerWashSaleEvent(
                                connection_id=conn.id,
                                provider_account_id=provider_account_id or "IBFLEX-1",
                                symbol=symbol,
                                trade_date=trade_date,
                                holding_period_datetime_raw=str(it.get("holding_period_datetime_raw")) if it.get("holding_period_datetime_raw") not in (None, "") else None,
                                when_realized_raw=str(it.get("when_realized_raw")) if it.get("when_realized_raw") not in (None, "") else None,
                                when_reopened_raw=str(it.get("when_reopened_raw")) if it.get("when_reopened_raw") not in (None, "") else None,
                                quantity=qty,
                                realized_pl_fifo=realized,
                                cost_basis=cost_basis,
                                proceeds_derived=proceeds,
                                ib_transaction_id=str(it.get("ib_transaction_id")) if it.get("ib_transaction_id") not in (None, "") else None,
                                ib_trade_id=str(it.get("ib_trade_id")) if it.get("ib_trade_id") not in (None, "") else None,
                                source_file_hash=source_file_hash,
                                raw_json={"row": it.get("raw_row") or {}, "source_file": it.get("source_file"), "source_row": it.get("source_row")},
                            )
                            try:
                                with session.begin_nested():
                                    session.add(row)
                                    session.flush()
                                coverage["wash_sale_rows_imported"] = int(coverage.get("wash_sale_rows_imported") or 0) + 1
                            except IntegrityError:
                                coverage["wash_sale_rows_dupes"] = int(coverage.get("wash_sale_rows_dupes") or 0) + 1
                        continue

                    coverage["txn_count"] += 1
                    tx_date = dt.date.fromisoformat(str(it.get("date")))
                    amount = float(it.get("amount"))
                    tx_type = _map_txn_type(str(it.get("type") or "OTHER"))
                    # Normalize sign conventions for cashflows:
                    # - WITHHOLDING stored as positive credit
                    # - FEE stored as negative cash outflow
                    if tx_type == "WITHHOLDING":
                        amount = abs(amount)
                    elif tx_type == "FEE":
                        amount = -abs(amount)
                    # Per-run type counts (count provider items processed).
                    tcounts = coverage.get("txn_type_counts") or {}
                    tcounts[tx_type] = int(tcounts.get(tx_type) or 0) + 1
                    coverage["txn_type_counts"] = tcounts
                    description = str(it.get("description") or "")
                    qty = it.get("qty")
                    qty_f = float(qty) if qty not in (None, "") else None
                    # Some providers encode SELL quantities as negative numbers (e.g., RJ offline exports).
                    # Normalize BUY/SELL quantities to positive values for downstream lot reconstruction.
                    if tx_type in {"BUY", "SELL"} and qty_f is not None:
                        qty_f = abs(float(qty_f))
                    symbol = it.get("ticker") or it.get("symbol")
                    provider_account_id = str(it.get("provider_account_id") or it.get("account_id") or "")

                    account_id = account_map.get(provider_account_id)
                    if account_id is None:
                        if (conn.connector or "").upper() == "CHASE_PLAID":
                            warnings.append(
                                f"Missing provider_account_id mapping for Plaid investment txn; skipped provider_account_id={provider_account_id}."
                            )
                            continue
                        # fallback to first mapped account
                        account_id = next(iter(account_map.values()))
                        warnings.append(f"Missing provider_account_id mapping for txn; used account_id={account_id}.")

                    ticker = None
                    if symbol and str(symbol).strip():
                        ticker = str(symbol).strip().upper()
                        _ensure_security(session, ticker=ticker, meta={"source": "sync", "provider_symbol": ticker})
                    else:
                        coverage["missing_symbol_count"] += 1
                        ticker = "UNKNOWN"
                        _ensure_security(
                            session,
                            ticker="UNKNOWN",
                            meta={"source": "sync", "note": "Placeholder for missing symbols"},
                        )

                    provider_txn_id = _stable_provider_txn_id(it)
                    exists = (
                        session.query(ExternalTransactionMap)
                        .filter(
                            ExternalTransactionMap.connection_id == conn.id,
                            ExternalTransactionMap.provider_txn_id == provider_txn_id,
                        )
                        .one_or_none()
                    )
                    if exists is not None:
                        # Allow safe reclassification for Plaid investment txns when upstream mapping improves.
                        # This avoids cashflow/report distortion from earlier misclassification (e.g., cash dividends as fees).
                        try:
                            if (conn.connector or "").upper() == "CHASE_PLAID" and str(provider_txn_id).startswith("PLAID_INV:"):
                                existing_txn = session.query(Transaction).filter(Transaction.id == exists.transaction_id).one_or_none()
                                if existing_txn is not None:
                                    changed = False
                                    if existing_txn.type != tx_type:
                                        existing_txn.type = tx_type
                                        changed = True
                                    if existing_txn.ticker != ticker:
                                        existing_txn.ticker = ticker
                                        changed = True
                                    if qty_f is not None and existing_txn.qty != qty_f:
                                        existing_txn.qty = qty_f
                                        changed = True
                                    if existing_txn.amount != amount:
                                        existing_txn.amount = amount
                                        changed = True
                                    if changed:
                                        links = existing_txn.lot_links_json or {}
                                        links["reclassified_by_sync"] = True
                                        links["reclassified_at"] = utcnow().isoformat()
                                        links["raw_type"] = tx_type
                                        existing_txn.lot_links_json = links
                                        coverage["updated_existing"] = int(coverage.get("updated_existing") or 0) + 1
                        except Exception:
                            pass
                        # Idempotency: skip duplicates; if reprocessing offline files, allow upgrading previously imported
                        # rows (e.g., backfilling better classification) without changing transaction IDs.
                        if is_offline_flex and reprocess_files:
                            existing_txn = session.query(Transaction).filter(Transaction.id == exists.transaction_id).one_or_none()
                            if existing_txn is not None:
                                changed = False
                                if existing_txn.type != tx_type:
                                    existing_txn.type = tx_type
                                    changed = True
                                # Keep ticker/qty/amount consistent with the latest normalization.
                                if existing_txn.ticker != ticker:
                                    existing_txn.ticker = ticker
                                    changed = True
                                if qty_f is not None and existing_txn.qty != qty_f:
                                    existing_txn.qty = qty_f
                                    changed = True
                                if existing_txn.amount != amount:
                                    existing_txn.amount = amount
                                    changed = True
                                if changed:
                                    links = existing_txn.lot_links_json or {}
                                    links["reclassified_by_sync"] = True
                                    links["reclassified_at"] = utcnow().isoformat()
                                    existing_txn.lot_links_json = links
                                    coverage["updated_existing"] += 1
                        coverage["duplicates_skipped"] += 1
                        continue

                    # Plaid Chase transactions can "churn" IDs (pending vs posted/corrected). Prevent duplicates by
                    # linking a new provider_txn_id to an existing imported transaction with the same signature.
                    if (conn.connector or "").upper() == "CHASE_PLAID" and str(provider_txn_id).startswith(
                        ("PLAID_INV:", "PLAID_TXN:")
                    ):
                        try:
                            qsig = (
                                session.query(Transaction, ExternalTransactionMap)
                                .join(ExternalTransactionMap, ExternalTransactionMap.transaction_id == Transaction.id)
                                .filter(
                                    ExternalTransactionMap.connection_id == conn.id,
                                    Transaction.account_id == account_id,
                                    Transaction.date == tx_date,
                                    Transaction.type == tx_type,
                                    Transaction.ticker == ticker,
                                    Transaction.amount == amount,
                                )
                            )
                            if qty_f is None:
                                qsig = qsig.filter(Transaction.qty.is_(None))
                            else:
                                qsig = qsig.filter(Transaction.qty == qty_f)
                            hit = qsig.order_by(Transaction.id.asc()).first()
                            if hit is not None:
                                existing_txn = hit[0]
                                with session.begin_nested():
                                    session.add(
                                        ExternalTransactionMap(
                                            connection_id=conn.id,
                                            provider_txn_id=provider_txn_id,
                                            transaction_id=existing_txn.id,
                                        )
                                    )
                                    session.flush()
                                coverage["duplicates_skipped"] += 1
                                coverage["linked_existing"] = int(coverage.get("linked_existing") or 0) + 1
                                continue
                        except IntegrityError:
                            coverage["duplicates_skipped"] += 1
                            continue
                        except Exception:
                            pass

                    # Insert txn + external map atomically (savepoint). Never rollback the whole run on one dup/error.
                    try:
                        with session.begin_nested():
                            txn = Transaction(
                                account_id=account_id,
                                date=tx_date,
                                type=tx_type,
                                ticker=ticker,
                                qty=qty_f,
                                amount=amount,
                                lot_links_json={
                                    "provider_txn_id": provider_txn_id,
                                    "provider_account_id": provider_account_id,
                                    "raw_type": it.get("type"),
                                    "description": description,
                                    "additional_detail": it.get("additional_detail"),
                                    "currency": it.get("currency"),
                                    "cashflow_kind": it.get("cashflow_kind"),
                                    "source_file": it.get("source_file"),
                                    "source_row": it.get("source_row"),
                                },
                            )
                            session.add(txn)
                            session.flush()
                            session.add(
                                ExternalTransactionMap(
                                    connection_id=conn.id, provider_txn_id=provider_txn_id, transaction_id=txn.id
                                )
                            )
                            session.flush()
                    except IntegrityError:
                        coverage["duplicates_skipped"] += 1
                        continue

                    coverage["new_inserted"] += 1

                    if earliest is None or tx_date < earliest:
                        earliest = tx_date
                    if latest is None or tx_date > latest:
                        latest = tx_date

                    # Minimal BUY-lot creation (MVP): create lot if qty and amount exist and account is taxable.
                    acct = session.query(Account).filter(Account.id == account_id).one()
                    if acct.account_type == "TAXABLE" and tx_type == "BUY" and qty_f and qty_f > 0:
                        try:
                            basis_total = abs(amount)
                            # Idempotency: PositionLot has no natural unique key, so do a best-effort match to
                            # avoid duplicating lots when the same BUY appears across re-runs/imports.
                            existing_lots = (
                                session.query(PositionLot)
                                .filter(
                                    PositionLot.account_id == account_id,
                                    PositionLot.ticker == ticker,
                                    PositionLot.acquisition_date == tx_date,
                                )
                                .all()
                            )
                            lot_exists = False
                            for l in existing_lots:
                                try:
                                    if abs(float(l.qty) - float(qty_f)) <= 1e-6 and abs(float(l.basis_total) - float(basis_total)) <= 0.01:
                                        lot_exists = True
                                        break
                                except Exception:
                                    continue
                            if not lot_exists:
                                with session.begin_nested():
                                    session.add(
                                        PositionLot(
                                            account_id=account_id,
                                            ticker=ticker,
                                            acquisition_date=tx_date,
                                            qty=qty_f,
                                            basis_total=basis_total,
                                            adjusted_basis_total=None,
                                        )
                                    )
                                    session.flush()
                        except Exception as e:
                            warnings.append(f"Best-effort lot creation failed for BUY txn_id={tx.id}: {type(e).__name__}")
                except Exception as e:
                    coverage["parse_fail_count"] += 1
                    # Include a small, redacted hint to aid debugging; never include secrets.
                    warnings.append(
                        f"Parse fail kind={record_kind} file={it.get('source_file') or ''} row={it.get('source_row') or ''} err={type(e).__name__}"
                    )
                    continue

            cursor = next_cursor
            if not next_cursor:
                exhausted = True
                break
            # If a next_cursor is provided, keep paginating regardless of count.
            # Some providers return short pages even when more data exists.
            #
            # Offline file connectors can legitimately produce empty pages (e.g., a holdings-only file in the
            # transactions iterator). Do not treat that as exhaustion when a next_cursor exists.
            if len(items) == 0 and not is_offline_files:
                exhausted = True
                break

        coverage["earliest_txn_date"] = earliest.isoformat() if earliest else None
        coverage["latest_txn_date"] = latest.isoformat() if latest else None

        if (conn.connector or "").upper() in {"CHASE_PLAID", "AMEX_PLAID"} and expense_account_id_by_provider:
            try:
                deduped = _dedupe_plaid_expense_transactions(session, list(set(expense_account_id_by_provider.values())))
                if deduped:
                    coverage["expenses_duplicates_removed"] = int(deduped)
                    warnings.append(f"Removed {deduped} duplicate Plaid expense transactions.")
            except Exception as e:
                warnings.append(f"Expense dedupe skipped: {type(e).__name__}")

        if pull_holdings:
            try:
                def _import_holdings_payload(holdings: dict[str, Any], *, source_file: dict[str, Any] | None = None) -> None:
                    as_of_str = holdings.get("as_of") or now_dt.isoformat()
                    try:
                        as_of_dt = dt.datetime.fromisoformat(str(as_of_str).replace("Z", "+00:00"))
                    except Exception:
                        as_of_dt = now_dt

                    # Track min/max as-of (useful for debugging performance coverage).
                    prev_min = str(coverage.get("holdings_asof_min") or "").strip()
                    prev_max = str(coverage.get("holdings_asof_max") or "").strip()
                    try:
                        prev_min_dt = dt.datetime.fromisoformat(prev_min.replace("Z", "+00:00")) if prev_min else None
                    except Exception:
                        prev_min_dt = None
                    try:
                        prev_max_dt = dt.datetime.fromisoformat(prev_max.replace("Z", "+00:00")) if prev_max else None
                    except Exception:
                        prev_max_dt = None
                    if prev_min_dt is None or as_of_dt < prev_min_dt:
                        coverage["holdings_asof_min"] = as_of_dt.isoformat()
                    if prev_max_dt is None or as_of_dt > prev_max_dt:
                        coverage["holdings_asof_max"] = as_of_dt.isoformat()

                    existing_snap = (
                        session.query(ExternalHoldingSnapshot)
                        .filter(ExternalHoldingSnapshot.connection_id == conn.id, ExternalHoldingSnapshot.as_of == as_of_dt)
                        .order_by(ExternalHoldingSnapshot.id.desc())
                        .first()
                    )
                    if existing_snap is not None:
                        existing_snap.payload_json = holdings
                    else:
                        session.add(ExternalHoldingSnapshot(connection_id=conn.id, as_of=as_of_dt, payload_json=holdings))

                    try:
                        items_list = list(holdings.get("items") or [])
                        items_n = len(items_list)
                    except Exception:
                        items_list = []
                        items_n = 0
                    coverage["holdings_snapshots_imported"] = int(coverage.get("holdings_snapshots_imported") or 0) + 1
                    coverage["holdings_items_imported"] = int(coverage.get("holdings_items_imported") or 0) + int(items_n)

                    prev_asof = str(coverage.get("holdings_asof") or "").strip()
                    try:
                        prev_dt = dt.datetime.fromisoformat(prev_asof.replace("Z", "+00:00")) if prev_asof else None
                    except Exception:
                        prev_dt = None
                    best_dt = as_of_dt if (prev_dt is None or as_of_dt > prev_dt) else prev_dt
                    coverage["holdings_asof"] = best_dt.isoformat()

                    # Small debug rollup: which valuation points did we ingest this run?
                    # Keep it small to avoid bloating coverage_json.
                    try:
                        dbg = coverage.setdefault("holdings_snapshot_debug", [])
                        if isinstance(dbg, list) and len(dbg) < 30:
                            total_value = holdings.get("statement_total_value")
                            if total_value is None and items_list:
                                try:
                                    for it in items_list:
                                        if isinstance(it, dict) and bool(it.get("is_total")):
                                            total_value = it.get("market_value")
                                            break
                                except Exception:
                                    pass
                            src_name = None
                            try:
                                src_name = str(holdings.get("source_file") or "").strip() or None
                            except Exception:
                                src_name = None
                            if source_file and not src_name:
                                try:
                                    src_name = str(source_file.get("file_name") or "").strip() or None
                                except Exception:
                                    src_name = None
                            dbg.append(
                                {
                                    "as_of": as_of_dt.isoformat(),
                                    "source_file": src_name,
                                    "items": int(items_n),
                                    "statement_period_start": holdings.get("statement_period_start"),
                                    "statement_period_end": holdings.get("statement_period_end"),
                                    "statement_total_value": float(total_value) if total_value is not None else None,
                                }
                            )
                    except Exception:
                        pass

                    # Record file ingestion for holdings files (idempotent).
                    if source_file:
                        try:
                            with session.begin_nested():
                                session.add(
                                    ExternalFileIngest(
                                        connection_id=conn.id,
                                        kind="HOLDINGS",
                                        file_name=str(source_file.get("file_name") or Path(str(source_file.get("path"))).name),
                                        file_hash=str(source_file.get("file_hash")),
                                        file_bytes=int(source_file.get("file_bytes") or 0) or None,
                                        file_mtime=dt.datetime.fromisoformat(str(source_file.get("file_mtime_iso")))
                                        if source_file.get("file_mtime_iso")
                                        else None,
                                    )
                                )
                        except IntegrityError:
                            pass
                        except Exception:
                            pass

                    # Optional: import cash balances when provided by the adapter.
                    # MVP assumes USD cash; non-USD is ignored with a warning (no FX in DB schema).
                    if isinstance(holdings.get("cash_balances"), list) and account_map:
                        cash_by_account_id: dict[int, float] = {}
                        cash_date_by_account_id: dict[int, dt.date] = {}
                        ignored_non_usd = 0
                        for r in holdings.get("cash_balances") or []:
                            try:
                                ccy = str((r or {}).get("currency") or "USD").strip().upper()
                                if ccy and ccy != "USD":
                                    ignored_non_usd += 1
                                    continue
                                provider_account_id = str((r or {}).get("provider_account_id") or "")
                                acct_id = account_map.get(provider_account_id) if provider_account_id else None
                                if acct_id is None:
                                    acct_id = next(iter(account_map.values()))
                                amt = float((r or {}).get("amount"))
                                cash_by_account_id[acct_id] = float(cash_by_account_id.get(acct_id) or 0.0) + amt
                                d_s = (r or {}).get("as_of_date")
                                if isinstance(d_s, str) and d_s.strip():
                                    try:
                                        cash_date_by_account_id[acct_id] = dt.date.fromisoformat(d_s.strip())
                                    except Exception:
                                        pass
                            except Exception:
                                continue

                        for acct_id, amt in cash_by_account_id.items():
                            cb_date = cash_date_by_account_id.get(acct_id) or as_of_dt.date()
                            existing_cb = session.query(CashBalance).filter(
                                CashBalance.account_id == acct_id, CashBalance.as_of_date == cb_date
                            ).one_or_none()
                            if existing_cb is not None:
                                existing_cb.amount = float(amt)
                            else:
                                session.add(CashBalance(account_id=acct_id, as_of_date=cb_date, amount=float(amt)))
                        if ignored_non_usd:
                            warnings.append(f"Ignored {ignored_non_usd} non-USD cash row(s); MVP does not model FX cash.")
                        coverage["cash_balances_imported"] = int(coverage.get("cash_balances_imported") or 0) + int(len(cash_by_account_id))

                if is_offline_files:
                    holdings_files = (ctx.run_settings or {}).get("holdings_files") or []
                    if isinstance(holdings_files, list) and holdings_files:
                        for f in holdings_files:
                            try:
                                (ctx.run_settings or {})["holdings_file_path"] = str(f.get("path") or "")
                                holdings = adapter.fetch_holdings(ctx, as_of=None)
                                if isinstance(holdings, dict):
                                    if store_payloads:
                                        session.add(
                                            ExternalPayloadSnapshot(
                                                sync_run_id=run.id,
                                                kind="holdings",
                                                cursor=str(f.get("file_name") or f.get("path") or ""),
                                                payload_json=holdings,
                                            )
                                        )
                                    snaps = holdings.get("snapshots")
                                    if isinstance(snaps, list) and snaps:
                                        first = True
                                        for s in snaps:
                                            if not isinstance(s, dict):
                                                continue
                                            _import_holdings_payload(s, source_file=f if first else None)
                                            first = False
                                    else:
                                        _import_holdings_payload(holdings, source_file=f)
                            except ProviderError as e:
                                if str(e) != "Holdings skipped":
                                    warnings.append(f"Provider error fetching holdings: {type(e).__name__}: {e}")
                            except Exception as e:
                                warnings.append(f"Failed importing holdings file {Path(str(f.get('path') or '')).name}: {type(e).__name__}")
                            finally:
                                try:
                                    (ctx.run_settings or {}).pop("holdings_file_path", None)
                                except Exception:
                                    pass
                    else:
                        # Offline file connectors: do not stamp an old holdings file with "now" just because a
                        # transactions-only incremental sync ran. For RJ, roll forward the last holdings file using
                        # the newly-imported transactions so the UI reflects recent activity.
                        if connector == "RJ_OFFLINE" and account_map:
                            try:
                                base_holdings = adapter.fetch_holdings(ctx, as_of=None)
                            except ProviderError:
                                base_holdings = {}
                            if isinstance(base_holdings, dict) and list(base_holdings.get("items") or []):
                                # Apply transactions for this connection within the effective sync window.
                                # We intentionally do not trust the holdings file's mtime-based as-of, since it can
                                # be misleading when users copy old files into the directory.
                                start_roll = eff_start
                                end_roll = eff_end
                                acct_ids = list({int(x) for x in account_map.values() if x})
                                tx_rows = (
                                    session.query(Transaction)
                                    .join(ExternalTransactionMap, ExternalTransactionMap.transaction_id == Transaction.id)
                                    .filter(
                                        ExternalTransactionMap.connection_id == conn.id,
                                        Transaction.account_id.in_(acct_ids),
                                        Transaction.date >= start_roll,
                                        Transaction.date <= end_roll,
                                    )
                                    .order_by(Transaction.date.asc(), Transaction.id.asc())
                                    .all()
                                )
                                if tx_rows:
                                    derived = _derive_holdings_snapshot_from_transactions(
                                        base_holdings=base_holdings,
                                        txns=tx_rows,
                                        as_of=now_dt,
                                        source_label=f"DERIVED_FROM_TXNS:{base_holdings.get('source_file') or 'RJ'}",
                                    )
                                    _import_holdings_payload(derived, source_file=None)
                                    coverage["holdings_derived_txn_count"] = int(len(tx_rows))
                                else:
                                    coverage["holdings_skipped"] = int(coverage.get("holdings_skipped") or 0) + 1
                                    warnings.append("Holdings snapshot not updated: no new holdings file selected and no post-snapshot transactions found.")
                            else:
                                coverage["holdings_skipped"] = int(coverage.get("holdings_skipped") or 0) + 1
                                warnings.append("Holdings snapshot not updated: no holdings/positions file found to use as a baseline.")
                        else:
                            # Preserve existing behavior for other offline connectors (e.g. Chase): adapters may be
                            # able to infer positions from activity-only exports when no positions file exists.
                            holdings = adapter.fetch_holdings(ctx, as_of=now_dt)
                            if isinstance(holdings, dict):
                                if store_payloads:
                                    session.add(
                                        ExternalPayloadSnapshot(
                                            sync_run_id=run.id, kind="holdings", cursor=None, payload_json=holdings
                                        )
                                    )
                                _import_holdings_payload(holdings, source_file=None)
                else:
                    # Import any uploaded holdings statement files first (baseline valuation points), then pull live holdings.
                    holdings_files = (ctx.run_settings or {}).get("holdings_files") or []
                    if isinstance(holdings_files, list) and holdings_files:
                        if connector == "CHASE_PLAID":
                            maps = (
                                session.query(ExternalAccountMap, Account)
                                .join(Account, Account.id == ExternalAccountMap.account_id)
                                .filter(ExternalAccountMap.connection_id == conn.id)
                                .order_by(ExternalAccountMap.created_at.asc())
                                .all()
                            )
                            acct_map = []
                            ira_ids: list[tuple[dt.datetime, str]] = []
                            for m, a in maps:
                                last4 = _last4_from_account_name(str(a.name or "").upper())
                                acct_map.append(
                                    {
                                        "provider_account_id": str(m.provider_account_id),
                                        "last4": last4 or "",
                                        "account_type": str(a.account_type or ""),
                                        "created_at": m.created_at.isoformat() if getattr(m, "created_at", None) else "",
                                    }
                                )
                                if str(a.account_type or "").upper() == "IRA":
                                    ira_ids.append((getattr(m, "created_at", utcnow()), str(m.provider_account_id)))
                            if acct_map:
                                ctx.run_settings["plaid_account_map"] = acct_map
                            if ira_ids:
                                ira_ids.sort(key=lambda t: t[0])
                                ctx.run_settings["plaid_default_provider_account_id"] = ira_ids[-1][1]
                        for f in holdings_files:
                            try:
                                (ctx.run_settings or {})["holdings_file_path"] = str(f.get("path") or "")
                                holdings0 = adapter.fetch_holdings(ctx, as_of=None)
                                if isinstance(holdings0, dict):
                                    if store_payloads:
                                        session.add(
                                            ExternalPayloadSnapshot(
                                                sync_run_id=run.id,
                                                kind="holdings",
                                                cursor=str(f.get("file_name") or f.get("path") or ""),
                                                payload_json=holdings0,
                                            )
                                        )
                                    snaps = holdings0.get("snapshots")
                                    if isinstance(snaps, list) and snaps:
                                        first = True
                                        for s in snaps:
                                            if not isinstance(s, dict):
                                                continue
                                            _import_holdings_payload(s, source_file=f if first else None)
                                            first = False
                                    else:
                                        _import_holdings_payload(holdings0, source_file=f)
                            except ProviderError as e:
                                if str(e) != "Holdings skipped":
                                    warnings.append(f"Provider error importing holdings file: {type(e).__name__}: {e}")
                            except Exception:
                                warnings.append(f"Failed importing holdings file {Path(str(f.get('path') or '')).name}")
                            finally:
                                try:
                                    (ctx.run_settings or {}).pop("holdings_file_path", None)
                                except Exception:
                                    pass
                    holdings = adapter.fetch_holdings(ctx, as_of=now_dt)
                    if store_payloads:
                        session.add(
                            ExternalPayloadSnapshot(
                                sync_run_id=run.id, kind="holdings", cursor=None, payload_json=holdings
                            )
                        )
                    # Surface adapter warnings (e.g., Plaid investments not enabled) without failing the run.
                    try:
                        hw = holdings.get("warnings") if isinstance(holdings, dict) else None
                        if isinstance(hw, list):
                            for w in hw:
                                s = str(w).strip()
                                if s:
                                    warnings.append(f"Holdings: {s}")
                    except Exception:
                        pass
                    skipped_hashes = (ctx.run_settings or {}).get("skipped_payload_hashes")
                    if isinstance(skipped_hashes, list) and skipped_hashes and not reprocess_files:
                        # If we skipped all transaction pages due to report-level idempotency, adapters may return empty
                        # holdings (no parsing happened). Avoid writing an empty snapshot in that case.
                        if isinstance(holdings, dict) and not list(holdings.get("items") or []) and not list(holdings.get("snapshots") or []):
                            coverage["holdings_skipped"] = int(coverage.get("holdings_skipped") or 0) + 1
                            warnings.append("Holdings snapshot skipped: this run only re-fetched already-imported report payload(s).")
                            raise ProviderError("Holdings skipped")
                    # Live connectors may return holdings derived from one or more report payload hashes. If we've already
                    # ingested those payloads and we're not explicitly reprocessing, skip inserting another identical
                    # snapshot (prevents duplicate snapshots when IB returns the same report content).
                    if isinstance(holdings, dict) and not reprocess_files:
                        ph = holdings.get("payload_hashes")
                        if isinstance(ph, list) and ph:
                            seen = (
                                session.query(ExternalFileIngest.file_hash)
                                .filter(ExternalFileIngest.connection_id == conn.id, ExternalFileIngest.kind == "REPORT_PAYLOAD")
                                .all()
                            )
                            seen_set = {r[0] for r in seen}
                            new_hashes = set((ctx.run_settings or {}).get("new_payload_hashes") or [])
                            if all((str(h) in seen_set) and (str(h) not in new_hashes) for h in ph):
                                coverage["holdings_skipped"] = int(coverage.get("holdings_skipped") or 0) + 1
                                warnings.append("Holdings snapshot skipped: derived from an already-imported report payload (idempotent).")
                                raise ProviderError("Holdings skipped")
                    if isinstance(holdings, dict):
                        if (conn.connector or "").upper() in {"CHASE_PLAID", "AMEX_PLAID"}:
                            try:
                                items_n = len(list(holdings.get("items") or []))
                            except Exception:
                                items_n = 0
                            if items_n <= 0:
                                coverage["holdings_skipped"] = int(coverage.get("holdings_skipped") or 0) + 1
                                warnings.append(
                                    "Holdings snapshot not updated: Plaid returned no holdings. "
                                    "If this connection includes investments, enable the investments product and re-link via Credentials."
                                )
                            else:
                                _import_holdings_payload(holdings, source_file=None)
                        else:
                            _import_holdings_payload(holdings, source_file=None)
            except ProviderError as e:
                # A deliberate "skip" uses ProviderError; keep it quiet.
                if str(e) != "Holdings skipped":
                    warnings.append(f"Provider error fetching holdings: {type(e).__name__}: {e}")

        # Post-process broker rows: link WASH_SALE rows to CLOSED_LOT rows and compute disallowed losses.
        try:
            stats = link_broker_wash_sales(session, connection_id=conn.id, start_date=eff_start, end_date=eff_end)
            coverage["broker_wash_rows"] = int(stats.get("wash_rows") or 0)
            coverage["broker_wash_linked"] = int(stats.get("linked") or 0)
            coverage["broker_wash_updated"] = int(stats.get("updated") or 0)
            coverage["broker_wash_with_basis"] = int(stats.get("with_basis") or 0)
            coverage["broker_wash_with_proceeds"] = int(stats.get("with_proceeds") or 0)
            coverage["broker_wash_with_disallowed"] = int(stats.get("with_disallowed") or 0)
        except Exception as e:
            warnings.append(f"Broker wash link step failed: {type(e).__name__}")

        # Determine status.
        # Include adapter-level warnings (e.g., skipped Flex queries due to missing endpoints) in the run warnings list.
        try:
            aw = (ctx.run_settings or {}).get("adapter_warnings")
            if isinstance(aw, list):
                for w in aw:
                    if w and str(w) not in warnings:
                        warnings.append(str(w))
        except Exception:
            pass
        # Persist safe, per-query adapter audit metrics (no secrets) into coverage_json.
        try:
            qa = (ctx.run_settings or {}).get("_ib_flex_web_query_audit")
            if isinstance(qa, list) and qa:
                coverage["ib_flex_web_query_audit"] = qa[:50]
        except Exception:
            pass
        # Persist safe Yodlee metrics (no secrets).
        try:
            hits = (ctx.run_settings or {}).get("yodlee_rate_limit_hits")
            if hits is not None:
                coverage["yodlee_rate_limit_hits"] = int(hits)
        except Exception:
            pass

        offline_imported_any = (
            int(coverage.get("txn_count") or 0) > 0
            or int(coverage.get("closed_lot_rows_imported") or 0) > 0
            or int(coverage.get("wash_sale_rows_imported") or 0) > 0
            or int(coverage.get("cash_balances_imported") or 0) > 0
            or int(coverage.get("holdings_items_imported") or 0) > 0
        )
        if is_offline_files and int(coverage.get("file_total") or 0) == 0 and not offline_imported_any:
            run.status = "PARTIAL"
            unsupported = int(coverage.get("file_unsupported_total") or 0)
            if unsupported > 0:
                if (conn.connector or "").upper() == "RJ_OFFLINE":
                    warnings.append(
                        "No supported offline transaction files were found. "
                        "CSV is required for transaction history; PDF statements can be used for holdings/performance if `pdftotext` is installed."
                    )
                else:
                    warnings.append(
                        "No supported offline files were found. "
                        "It looks like you uploaded Excel/PDF exports; re-export as .csv/.tsv/.txt/.xml and re-run sync."
                    )
            else:
                dd = str(coverage.get("data_dir") or "").strip()
                loc = f" in {dd}" if dd else ""
                warnings.append(
                    "No offline files were found for this connection"
                    + loc
                    + ". Upload exports on the connection detail page, or set the Data directory to where the files live."
                )
        elif is_offline_files and coverage.get("file_count", 0) and not offline_imported_any:
            run.status = "PARTIAL"
            warnings.append(
                "Offline files were processed but no records were imported; check the file format/headers and date range."
            )
        elif not exhausted:
            run.status = "PARTIAL"
            warnings.append("Pagination did not exhaust; run marked PARTIAL.")
        elif coverage["parse_fail_count"] > 0:
            run.status = "PARTIAL"
            warnings.append("Parse failures occurred; run marked PARTIAL.")
        else:
            run.status = "SUCCESS"

        # Web connector quality gate: avoid reporting SUCCESS when no importable items were returned.
        if (conn.provider or "").upper() == "IB" and (conn.connector or "").upper() == "IB_FLEX_WEB":
            imported_any = (
                int(coverage.get("txn_count") or 0) > 0
                or int(coverage.get("closed_lot_rows_imported") or 0) > 0
                or int(coverage.get("wash_sale_rows_imported") or 0) > 0
                or int(coverage.get("cash_balances_imported") or 0) > 0
                or int(coverage.get("holdings_items_imported") or 0) > 0
            )
            report_skipped = int(coverage.get("report_payloads_skipped") or 0)
            # If the run only re-fetched already-imported report payload(s), it's still a clean SUCCESS.
            if not imported_any and int(coverage.get("pages_fetched") or 0) > 0 and report_skipped == 0:
                run.status = "PARTIAL"
                warnings.append(
                    "No data was imported from IB Flex Web for this run. "
                    "Verify your Flex Query IDs (numeric) and that the query template returns data for the selected date range."
                )

        run.finished_at = utcnow()
        run.pages_fetched = int(coverage.get("pages_fetched") or 0)
        run.txn_count = int(coverage.get("txn_count") or 0)
        run.new_count = int(coverage.get("new_inserted") or 0)
        run.dupes_count = int(coverage.get("duplicates_skipped") or 0)
        run.parse_fail_count = int(coverage.get("parse_fail_count") or 0)
        run.missing_symbol_count = int(coverage.get("missing_symbol_count") or 0)
        run.coverage_json = coverage | {"warnings": warnings}

        # Update connection pointers on SUCCESS only.
        if run.status == "SUCCESS":
            conn.last_successful_sync_at = utcnow()
            conn.last_successful_txn_end = eff_end
            conn.last_error_json = None
            # Persist adapter-provided cursors for incremental syncs (e.g., Plaid /transactions/sync).
            plaid_cur = str((ctx.run_settings or {}).get("plaid_transactions_cursor") or "").strip()
            meta_before = conn.metadata_json or {}
            # Copy to ensure SQLAlchemy observes JSON changes (and to avoid mutating the original dict in-place).
            meta = dict(meta_before)
            if plaid_cur:
                meta["plaid_transactions_cursor"] = plaid_cur
                st = str((ctx.run_settings or {}).get("plaid_transactions_update_status") or "").strip()
                if st:
                    meta["plaid_transactions_update_status"] = st

            # Mark backfills as done (so future runs don't repeatedly rehydrate huge ranges).
            if run_settings.get("plaid_backfill_24m"):
                # Only declare the Plaid "initial backfill" complete once Plaid reports historical complete.
                # If Plaid is still hydrating history, the cursor will advance over subsequent incrementals.
                historical_ok = bool((ctx.run_settings or {}).get("plaid_transactions_historical_complete", True))
                if historical_ok:
                    meta["plaid_initial_backfill_done"] = True

            if run_settings.get("plaid_investments_backfill_24m"):
                # Only mark done if the investments transaction fetch succeeded (even if it returned 0 rows).
                inv_ok = bool((ctx.run_settings or {}).get("plaid_investments_txns_fetch_ok", False))
                if inv_ok:
                    meta["plaid_investments_backfill_done"] = True

            # One-time AMEX backfill via /transactions/get should clear its force flag after success.
            if run_settings.get("plaid_force_transactions_get"):
                meta.pop("plaid_force_transactions_get", None)

            if meta != meta_before:
                conn.metadata_json = meta
                try:
                    flag_modified(conn, "metadata_json")
                except Exception:
                    pass
            if mode_u == "FULL":
                conn.last_full_sync_at = utcnow()
                # Earliest available is the earliest imported transaction date across this connection.
                # For expenses-only connectors (e.g., Plaid Chase when investments are disabled), we don't populate
                # the investment `transactions` table.
                conn_meta = conn.metadata_json or {}
                plaid_investments_enabled = bool(conn_meta.get("plaid_enable_investments") is True)
                if not ((conn.connector or "").upper() in {"CHASE_PLAID", "AMEX_PLAID"} and not plaid_investments_enabled):
                    min_date = (
                        session.query(Transaction.date)
                        .join(ExternalTransactionMap, ExternalTransactionMap.transaction_id == Transaction.id)
                        .filter(ExternalTransactionMap.connection_id == conn.id)
                        .order_by(Transaction.date.asc())
                        .limit(1)
                        .scalar()
                    )
                    conn.txn_earliest_available = min_date or earliest or eff_start
            if coverage.get("holdings_asof"):
                conn.holdings_last_asof = dt.datetime.fromisoformat(str(coverage["holdings_asof"]))
        else:
            # Store a sanitized error/partial summary (no secrets).
            conn.last_error_json = json.dumps(
                {
                    "at": utcnow().isoformat(),
                    "run_id": run.id,
                    "status": run.status,
                    "warnings": warnings[:50],
                    "parse_fail_count": int(coverage.get("parse_fail_count") or 0),
                    "data_dir": coverage.get("data_dir"),
                    "file_total_all": int(coverage.get("file_total_all") or 0),
                    "file_total": int(coverage.get("file_total") or 0),
                    "file_selected": int(coverage.get("file_selected") or 0),
                    "file_unsupported_total": int(coverage.get("file_unsupported_total") or 0),
                }
            )

        # Update coverage status after every run.
        conn.coverage_status = compute_coverage_status(conn, latest_run=run)

        session.flush()
        log_change(
            session,
            actor=actor,
            action="SYNC_RUN_FINISHED",
            entity="SyncRun",
            entity_id=str(run.id),
            old=None,
            new={"status": run.status, "coverage": coverage, "warnings": warnings[:50]},
            note=f"Sync run finished for connection={conn.id}",
        )
        session.commit()
        return run

    except Exception as e:
        run.status = "ERROR"
        run.finished_at = utcnow()
        run.error_json = json.dumps({"error": f"{type(e).__name__}: {e}"})
        run.coverage_json = coverage | {"warnings": warnings, "error": f"{type(e).__name__}: {e}"}
        conn.last_error_json = json.dumps({"at": utcnow().isoformat(), "run_id": run.id, "error": f"{type(e).__name__}: {e}"})
        conn.coverage_status = compute_coverage_status(conn, latest_run=run)
        session.flush()
        log_change(
            session,
            actor=actor,
            action="SYNC_RUN_FINISHED",
            entity="SyncRun",
            entity_id=str(run.id),
            old=None,
            new={"status": run.status, "error": f"{type(e).__name__}: {e}"},
            note="Sync run error",
        )
        session.commit()
        return run
