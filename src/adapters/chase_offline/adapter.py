from __future__ import annotations

import csv
import datetime as dt
import hashlib
import io
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from src.importers.adapters import BrokerAdapter, ProviderError
from src.utils.time import utcfromtimestamp, utcnow


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _as_float(v: Any) -> float | None:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        try:
            return float(v)
        except Exception:
            return None
    s = str(v).strip()
    if not s:
        return None
    # Currency formatting + parentheses negatives
    neg = False
    if s.startswith("(") and s.endswith(")"):
        neg = True
        s = s[1:-1]
    s = s.replace("$", "").replace(",", "").strip()
    if not s:
        return None
    try:
        out = float(s)
    except Exception:
        return None
    return -out if neg else out


def _parse_date(v: Any) -> dt.date | None:
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    # ISO
    try:
        return dt.date.fromisoformat(s[:10])
    except Exception:
        pass
    # MM/DD/YYYY
    for fmt in ("%m/%d/%Y", "%m/%d/%y"):
        try:
            return dt.datetime.strptime(s.split()[0], fmt).date()
        except Exception:
            continue
    return None


def _lower_keys(row: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k, v in (row or {}).items():
        kk = str(k or "").strip().lower().replace(" ", "_")
        out[kk] = v
    return out


def _detect_header(rows: list[list[str]], required: set[str]) -> int | None:
    for i, r in enumerate(rows[:50]):
        cols = {str(c).strip().lower().replace(" ", "_") for c in r if str(c).strip()}
        if required.issubset(cols):
            return i
    return None


def _find_header_line_index(lines: list[str], required: set[str]) -> int | None:
    return _find_header_line_index_with_delim(lines, required, delimiter=",")


def _find_header_line_index_with_delim(lines: list[str], required: set[str], delimiter: str) -> int | None:
    # Tolerant header detection for delimited files that may include a preamble.
    for i, line in enumerate(lines[:120]):
        if not line.strip():
            continue
        parts = [p.strip().strip('"').strip("'").lower().replace(" ", "_") for p in line.split(delimiter)]
        cols = {p for p in parts if p}
        if required.issubset(cols):
            return i
    return None


def _sniff_delimiter(text: str) -> str:
    lines = (text or "").splitlines()
    sample = "\n".join(lines[:30])
    if not sample:
        return ","
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",\t;")
        delim = getattr(dialect, "delimiter", ",") or ","
        return delim
    except Exception:
        # Heuristic fallback: pick the delimiter that appears most frequently in the first N lines.
        # This handles TSV exports that include commas in numeric fields (e.g. "174,582") which can
        # confuse csv.Sniffer into choosing "," incorrectly.
        counts = {",": 0, "\t": 0, ";": 0}
        for line in lines[:50]:
            if not line:
                continue
            for d in counts.keys():
                counts[d] += line.count(d)
        # Prefer tab if present at all and at least as frequent as commas.
        if counts["\t"] > 0 and counts["\t"] >= counts[","]:
            return "\t"
        # Otherwise choose the most frequent delimiter.
        best = max(counts.items(), key=lambda kv: kv[1])[0]
        return best or ","


def _read_csv_rows(text: str) -> tuple[list[str], list[dict[str, Any]]]:
    """
    Return (header, rows) using tolerant header detection.

    IMPORTANT: uses original CSV lines (no re-serialization), so quoted commas remain intact.
    """
    if not text:
        return [], []
    lines = text.splitlines()
    if not lines:
        return [], []
    delimiter = _sniff_delimiter(text)
    # Heuristic: prefer positions-like header if present, else transactions-like.
    pos_required = {"symbol", "quantity"}
    tx_required = {"date"}
    header_idx = _find_header_line_index_with_delim(lines, pos_required, delimiter)
    if header_idx is None:
        header_idx = _find_header_line_index_with_delim(lines, tx_required, delimiter)
    if header_idx is None:
        # Chase statements use Trade Date.
        header_idx = _find_header_line_index_with_delim(lines, {"trade_date"}, delimiter)
    if header_idx is None:
        header_idx = 0
    data = "\n".join(lines[header_idx:])
    reader = csv.DictReader(io.StringIO(data), delimiter=delimiter)
    out: list[dict[str, Any]] = []
    for r in reader:
        if not any((str(v).strip() for v in (r or {}).values())):
            continue
        out.append(r)
    return [str(h) for h in (reader.fieldnames or [])], out


def _looks_like_holdings(text: str) -> bool:
    lines = (text or "").splitlines()
    delimiter = _sniff_delimiter(text)
    # Holdings snapshots should include a value column; activity/trade exports often contain Symbol+Quantity too.
    idx = _find_header_line_index_with_delim(lines, {"symbol"}, delimiter)
    if idx is None:
        idx = _find_header_line_index_with_delim(lines, {"ticker"}, delimiter)
    if idx is None:
        return False
    header = lines[idx]
    cols = {p.strip().strip('"').strip("'").lower().replace(" ", "_") for p in header.split(delimiter) if p.strip()}
    has_qty = ("quantity" in cols) or ("qty" in cols) or ("shares" in cols) or ("position" in cols)
    has_value = ("market_value" in cols) or ("marketvalue" in cols) or ("market" in cols) or ("value" in cols)
    # If the header looks like an activity/trade export, treat as not-holdings.
    looks_like_activity = ("date" in cols) and (("amount" in cols) or ("type" in cols) or ("transaction_type" in cols))
    return bool(has_qty and has_value and not looks_like_activity)


def _looks_like_transactions(text: str) -> bool:
    lines = (text or "").splitlines()
    delimiter = _sniff_delimiter(text)
    idx = _find_header_line_index_with_delim(lines, {"date"}, delimiter)
    if idx is None:
        idx = _find_header_line_index_with_delim(lines, {"trade_date"}, delimiter)
    if idx is None:
        return False
    header = lines[idx]
    cols = {p.strip().strip('"').strip("'").lower().replace(" ", "_") for p in header.split(delimiter) if p.strip()}
    return (("date" in cols) or ("trade_date" in cols)) and (
        ("amount" in cols)
        or ("amount_usd" in cols)
        or ("type" in cols)
        or ("transaction_type" in cols)
        or ("tran_code" in cols)
    )


def _classify_txn(row: dict[str, Any]) -> str:
    t = str(row.get("type") or row.get("transaction_type") or row.get("activity") or row.get("action") or "").strip().upper()
    desc = str(row.get("description") or row.get("details") or row.get("memo") or "").strip().upper()
    side = str(row.get("buy_sell") or row.get("side") or row.get("buy/sell") or "").strip().upper()
    tran_code = str(row.get("tran_code") or "").strip().upper()
    tran_desc = str(row.get("tran_code_description") or "").strip().upper()
    joined = " ".join([t, desc, side, tran_code, tran_desc]).strip()
    # Reinvest rows are typically internal sweep mechanics; do not count as interest income.
    if "REINVEST" in joined:
        return "OTHER"
    # Chase IRA sweep activity (internal cash ↔ sweep): do NOT treat as external contribution/withdrawal.
    # These rows include "DEPOSIT SWEEP" and are commonly tagged with DBS/WDL codes.
    code = tran_code or t
    if ("DEPOSIT SWEEP" in joined or "INTRA-DAY DEPOSIT" in joined or "INTRA-DAY WITHDRWAL" in joined) and code in {"DBS", "WDL"}:
        return "OTHER"
    if any(x in joined for x in ("BUY", "BOUGHT", "PURCHASE")) or side in {"BUY", "B"}:
        return "BUY"
    if any(x in joined for x in ("SELL", "SOLD")) or side in {"SELL", "S"}:
        return "SELL"
    if "DIV" in joined:
        return "DIV"
    if "INTEREST" in joined or joined.startswith("INT"):
        return "INT"
    if "WITHHOLD" in joined or "TAX" in joined:
        return "WITHHOLDING"
    if "FEE" in joined or "COMMISSION" in joined:
        return "FEE"
    if "CONTRIBUT" in joined or "DEPOSIT" in joined or "ACH PUSH" in joined:
        return "TRANSFER"
    if "DISTRIBUT" in joined or "WITHDRAW" in joined or "DISBURSE" in joined or "ACH PULL" in joined:
        return "TRANSFER"
    # Some Chase exports use "Deposits/Withdrawals"
    if "DEPOSITS/WITHDRAWALS" in joined or "DEPOSIT" in joined or "WITHDRAWAL" in joined:
        return "TRANSFER"
    return "OTHER"


def _stable_txid(provider_account_id: str, date: dt.date, tx_type: str, symbol: str | None, qty: float | None, amount: float | None, desc: str) -> str:
    key = f"{provider_account_id}|{date.isoformat()}|{tx_type}|{symbol or ''}|{qty or ''}|{amount or ''}|{desc}"
    return "CHASE:HASH:" + _sha256_bytes(key.encode("utf-8"))


def _list_files(data_dir: Path) -> list[Path]:
    if not data_dir.exists() or not data_dir.is_dir():
        return []
    out: list[Path] = []
    for p in sorted(data_dir.glob("**/*")):
        if p.is_file() and p.suffix.lower() in {".csv", ".tsv", ".txt"}:
            out.append(p)
    return out


def _is_holdings_filename(name: str) -> bool:
    n = name.lower()
    return any(k in n for k in ("positions", "holdings", "position", "holding"))


def _is_transactions_filename(name: str) -> bool:
    n = name.lower()
    return any(k in n for k in ("activity", "transactions", "trades", "trade", "history", "statement", "cash"))


@dataclass(frozen=True)
class OfflineFile:
    path: Path
    file_hash: str
    kind: str  # TRANSACTIONS|HOLDINGS


class ChaseOfflineAdapter(BrokerAdapter):
    """
    Offline Chase CSV adapter for IRA accounts.

    Connection metadata_json:
      - data_dir: directory containing Chase CSV exports

    Notes:
      - Chase exports vary; this adapter uses tolerant header detection and keyword-based classification.
      - This connector does not require credentials.
    """

    @property
    def page_size(self) -> int:
        # Paginate by file (1 CSV per page) via sync_runner cursor.
        return 1

    def _data_dir(self, connection: Any) -> Path:
        meta = getattr(connection, "metadata_json", {}) or {}
        dd = meta.get("data_dir")
        if dd:
            return Path(os.path.expanduser(str(dd)))
        return Path("data") / "external" / f"conn_{getattr(connection, 'id', 'unknown')}"

    def _selected_files(self, connection: Any) -> list[OfflineFile]:
        run_settings = getattr(connection, "run_settings", None) or {}
        selected = run_settings.get("selected_files")
        out: list[OfflineFile] = []
        if isinstance(selected, list):
            for it in selected:
                try:
                    out.append(
                        OfflineFile(
                            path=Path(str(it["path"])),
                            file_hash=str(it["file_hash"]),
                            kind=str(it.get("kind") or "TRANSACTIONS"),
                        )
                    )
                except Exception:
                    continue
            return out
        data_dir = self._data_dir(connection)
        for p in _list_files(data_dir):
            kind = "HOLDINGS" if _is_holdings_filename(p.name) else "TRANSACTIONS"
            out.append(OfflineFile(path=p, file_hash=_sha256_bytes(p.read_bytes()), kind=kind))
        return out

    def test_connection(self, connection: Any) -> dict[str, Any]:
        data_dir = self._data_dir(connection)
        files = _list_files(data_dir)
        if not files:
            return {"ok": False, "message": f"No .csv files found in {data_dir}."}
        return {"ok": True, "message": f"OK (Chase offline): {len(files)} CSV file(s) found.", "data_dir": str(data_dir)}

    def fetch_accounts(self, connection: Any) -> list[dict[str, Any]]:
        # Use a stable mapping to the internal account name.
        return [{"provider_account_id": "CHASE:IRA", "name": "Chase IRA", "account_type": "IRA"}]

    def fetch_holdings(self, connection: Any, as_of: dt.datetime | None = None) -> dict[str, Any]:
        data_dir = self._data_dir(connection)
        files = _list_files(data_dir)
        holdings_files: list[Path] = []
        # Prefer header-based classification (filenames vary in Chase exports).
        for p in files:
            try:
                txt = p.read_text(encoding="utf-8-sig")
            except Exception:
                continue
            if _looks_like_holdings(txt):
                holdings_files.append(p)
        # Do not treat a file as holdings based on name alone; Chase activity statements often include Quantity.
        if not holdings_files:
            holdings_files = []
        # Fall back to any CSV that looks like a positions file by header.
        if not holdings_files:
            for p in files:
                if not _is_transactions_filename(p.name):
                    holdings_files.append(p)
        if not holdings_files:
            # Fallback: infer open positions from transaction history so holdings views are not all-zero.
            run_settings = getattr(connection, "run_settings", None) or {}
            warnings = run_settings.setdefault("adapter_warnings", [])
            if isinstance(warnings, list):
                warnings.append(
                    "No Chase positions/holdings CSV found; holdings snapshot estimated from BUY/SELL history (market value uses last trade price)."
                )
            return self._infer_holdings_from_transactions(connection, as_of=as_of)

        # Choose newest file by mtime.
        holdings_files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        p = holdings_files[0]
        file_mtime_asof = utcfromtimestamp(p.stat().st_mtime)
        items: list[dict[str, Any]] = []
        cash_balances: list[dict[str, Any]] = []
        cash_total: float = 0.0
        inferred_asof: dt.datetime | None = None

        try:
            text = p.read_text(encoding="utf-8-sig")
            _hdr, rows = _read_csv_rows(text)
            for r in rows:
                row = _lower_keys(r)
                sym = str(row.get("symbol") or row.get("ticker") or row.get("security") or row.get("description") or "").strip().upper()
                qty = _as_float(row.get("quantity") or row.get("qty") or row.get("shares") or row.get("position"))
                mv = _as_float(row.get("market_value") or row.get("marketvalue") or row.get("value") or row.get("market"))
                cb = _as_float(
                    row.get("cost")
                    or row.get("orig_cost_(base)")
                    or row.get("orig_cost_base")
                    or row.get("orig_cost")
                    or row.get("amount_invested")
                    or row.get("amountinvested")
                )

                # Prefer "As of" / "Pricing Date" over file mtime when present.
                asof_s = row.get("as_of") or row.get("pricing_date")
                d = _parse_date(asof_s)
                if d is not None:
                    candidate = dt.datetime.combine(d, dt.time.min, tzinfo=dt.timezone.utc)
                    if inferred_asof is None or candidate > inferred_asof:
                        inferred_asof = candidate

                if not sym or qty is None:
                    continue

                asset_class = str(row.get("asset_class") or "").strip().upper()
                desc_u = str(row.get("description") or "").strip().upper()
                is_cash_like = False
                # Chase positions export often represents cash as:
                # - a sweep vehicle (e.g. QCERQ "JPMORGAN IRA DEPOSIT SWEEP ...")
                # - a "US DOLLAR" line (can be negative during settlement)
                if sym in {"CASH", "USD", "US DOLLAR", "SWEEP", "CASH_BALANCE"}:
                    is_cash_like = True
                if "DEPOSIT SWEEP" in desc_u or "SWEEP" in desc_u:
                    is_cash_like = True
                if asset_class and "CASH" in asset_class:
                    is_cash_like = True
                if asset_class.startswith("FIXED INCOME") and ("CASH" in asset_class or "SHORT TERM" in str(row.get("asset_strategy") or "").upper()):
                    is_cash_like = True

                if mv is None:
                    mv = qty
                if is_cash_like:
                    cash_total += float(mv or 0.0)
                    continue

                items.append(
                    {
                        "provider_account_id": "CHASE:IRA",
                        "symbol": sym,
                        "qty": float(qty),
                        "market_value": float(mv) if mv is not None else None,
                        "cost_basis_total": float(cb) if cb is not None else None,
                        "source_file": p.name,
                    }
                )
        except Exception as e:
            raise ProviderError(f"Failed to parse Chase holdings file {p.name}: {type(e).__name__}: {e}")

        if not items:
            run_settings = getattr(connection, "run_settings", None) or {}
            warnings = run_settings.setdefault("adapter_warnings", [])
            if isinstance(warnings, list):
                warnings.append(
                    f"Holdings snapshot from {p.name} contained 0 position rows; falling back to estimated holdings from BUY/SELL history."
                )
            return self._infer_holdings_from_transactions(connection, as_of=as_of)

        as_of_dt = inferred_asof or file_mtime_asof
        if abs(float(cash_total)) > 1e-9:
            cash_balances.append(
                {
                    "provider_account_id": "CHASE:IRA",
                    "currency": "USD",
                    "amount": float(cash_total),
                    "as_of_date": as_of_dt.date().isoformat(),
                    "source_file": p.name,
                }
            )
            # Also include cash as an item for fallback display if CashBalance isn't present.
            items.append(
                {
                    "provider_account_id": "CHASE:IRA",
                    "symbol": "CASH:USD",
                    "qty": float(cash_total),
                    "market_value": float(cash_total),
                    "asset_type": "CASH",
                    "source_file": p.name,
                }
            )

        out: dict[str, Any] = {"as_of": as_of_dt.isoformat(), "items": items, "source_file": p.name}
        if cash_balances:
            out["cash_balances"] = cash_balances
        return out

    def _infer_holdings_from_transactions(self, connection: Any, as_of: dt.datetime | None) -> dict[str, Any]:
        """
        Planning-grade holdings snapshot inferred from BUY/SELL activity.
        Intended only as a fallback when no positions export is available.
        """
        run_settings = getattr(connection, "run_settings", None) or {}
        eff_start_s = str(run_settings.get("effective_start_date") or "")
        eff_end_s = str(run_settings.get("effective_end_date") or "")
        try:
            eff_start = dt.date.fromisoformat(eff_start_s[:10]) if eff_start_s else dt.date(1900, 1, 1)
        except Exception:
            eff_start = dt.date(1900, 1, 1)
        try:
            eff_end = dt.date.fromisoformat(eff_end_s[:10]) if eff_end_s else dt.date.today()
        except Exception:
            eff_end = dt.date.today()

        data_dir = self._data_dir(connection)
        files = _list_files(data_dir)
        tx_files = []
        for p in files:
            try:
                txt = p.read_text(encoding="utf-8-sig")
            except Exception:
                continue
            # Transactions file usually includes a Date column; exclude holdings-like files.
            if _looks_like_transactions(txt) and not _looks_like_holdings(txt):
                tx_files.append(p)

        # Use newest file mtime as as_of.
        if tx_files:
            tx_files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
            as_of_dt = utcfromtimestamp(tx_files[0].stat().st_mtime)
        else:
            as_of_dt = as_of or utcnow()

        net_qty: dict[str, float] = {}
        last_trade_price: dict[str, float] = {}
        last_trade_date: dict[str, dt.date] = {}
        cash_units: float = 0.0

        for p in tx_files:
            try:
                text = p.read_text(encoding="utf-8-sig")
                _hdr, rows = _read_csv_rows(text)
            except Exception:
                continue
            for r in rows:
                row = _lower_keys(r)
                d = _parse_date(row.get("date") or row.get("trade_date") or row.get("posted_date") or row.get("transaction_date"))
                if d is None or d < eff_start or d > eff_end:
                    continue
                sec_type = str(row.get("security_type") or "").strip().upper()
                tx_type = _classify_txn(row)
                symbol = str(row.get("symbol") or row.get("ticker") or "").strip().upper()
                qty = _as_float(row.get("quantity") or row.get("qty") or row.get("shares"))
                amount = _as_float(
                    row.get("amount")
                    or row.get("amount_usd")
                    or row.get("amount_local")
                    or row.get("net_amount")
                    or row.get("value")
                    or row.get("total")
                )

                # Treat money market sweep shares as cash-like holdings when present.
                if sec_type and "MONEY MARKET" in sec_type and qty is not None:
                    cash_units += float(qty)
                    continue

                if tx_type not in {"BUY", "SELL"}:
                    continue
                if not symbol or qty is None:
                    continue

                # Normalize qty direction for position aggregation.
                q = abs(float(qty))
                if tx_type == "BUY":
                    net_qty[symbol] = float(net_qty.get(symbol) or 0.0) + q
                else:
                    net_qty[symbol] = float(net_qty.get(symbol) or 0.0) - q

                # Estimate last price from cash amount if present.
                if amount is not None and abs(float(amount)) > 1e-9 and q > 1e-9:
                    px = abs(float(amount)) / q
                    last_trade_price[symbol] = float(px)
                    last_trade_date[symbol] = d

        items: list[dict[str, Any]] = []
        if abs(cash_units) > 1e-6:
            items.append(
                {
                    "provider_account_id": "CHASE:IRA",
                    "symbol": "CASH:USD",
                    "qty": float(cash_units),
                    "market_value": float(cash_units),
                    "estimated": True,
                    "method": "money_market_sweep_qty",
                }
            )
        for sym, q in sorted(net_qty.items()):
            if abs(q) < 1e-9:
                continue
            mv = None
            px = last_trade_price.get(sym)
            if px is not None:
                mv = float(px) * float(q)
            items.append(
                {
                    "provider_account_id": "CHASE:IRA",
                    "symbol": sym,
                    "qty": float(q),
                    "market_value": float(mv) if mv is not None else None,
                    "estimated": True,
                    "last_trade_date": last_trade_date.get(sym).isoformat() if last_trade_date.get(sym) else None,
                    "last_trade_price": float(px) if px is not None else None,
                }
            )

        return {"as_of": as_of_dt.isoformat(), "items": items, "estimated": True, "method": "from_transactions"}

    def fetch_transactions(
        self,
        connection: Any,
        start_date: dt.date,
        end_date: dt.date,
        cursor: str | None = None,
    ) -> tuple[list[dict[str, Any]], str | None]:
        files = [f for f in self._selected_files(connection) if f.kind == "TRANSACTIONS"]
        idx = int(cursor) if cursor is not None else 0
        if idx >= len(files):
            return [], None

        f = files[idx]
        p = f.path
        items: list[dict[str, Any]] = []

        # Skip obvious holdings files even if mis-classified.
        if _is_holdings_filename(p.name):
            next_cursor = str(idx + 1) if (idx + 1) < len(files) else None
            return [], next_cursor

        try:
            text = p.read_text(encoding="utf-8-sig")
            _hdr, rows = _read_csv_rows(text)
            for r in rows:
                row = _lower_keys(r)
                d = _parse_date(row.get("date") or row.get("trade_date") or row.get("posted_date") or row.get("transaction_date"))
                if d is None:
                    continue
                if d < start_date or d > end_date:
                    continue
                symbol = str(row.get("symbol") or row.get("ticker") or "").strip().upper() or None
                desc = str(row.get("description") or row.get("details") or row.get("memo") or row.get("security") or "").strip()
                tx_type = _classify_txn(row)
                qty = _as_float(row.get("quantity") or row.get("qty") or row.get("shares"))
                amount = _as_float(
                    row.get("amount")
                    or row.get("amount_usd")
                    or row.get("amount_local")
                    or row.get("net_amount")
                    or row.get("value")
                    or row.get("total")
                    or row.get("income_usd")
                    or row.get("income_local")
                )
                security_type = str(row.get("security_type") or "").strip().upper()
                tran_code = str(row.get("tran_code") or "").strip().upper()
                desc_u = str(desc or "").upper()
                code = tran_code or str(row.get("type") or "").strip().upper()
                # Hard exclude internal sweep mechanics from TRANSFER classification even if description includes
                # words like "deposit/withdrawal". These are cash↔money-market movements, not external flows.
                if (symbol or "").upper() == "QCERQ" or "DEPOSIT SWEEP" in desc_u:
                    if code in {"DBS", "WDL"} or "INTRA-DAY" in desc_u or "DEPOSIT SWEEP" in desc_u:
                        tx_type = "OTHER"
                if security_type and "MONEY MARKET" in security_type and ("DEPOSIT SWEEP" in desc_u or (symbol or "").upper() == "QCERQ"):
                    tx_type = "OTHER"
                # If amount missing, try compute from qty*price.
                if amount is None:
                    price = _as_float(row.get("price") or row.get("price_usd") or row.get("price_local"))
                    if qty is not None and price is not None and tx_type in {"BUY", "SELL"}:
                        amount = float(qty) * float(price)

                # Normalize signs/conventions.
                if tx_type == "BUY":
                    if amount is not None:
                        amount = -abs(float(amount))
                    if qty is not None:
                        qty = abs(float(qty))
                elif tx_type == "SELL":
                    if amount is not None:
                        amount = abs(float(amount))
                    if qty is not None:
                        qty = abs(float(qty))
                elif tx_type == "FEE":
                    if amount is not None:
                        amount = -abs(float(amount))
                elif tx_type == "WITHHOLDING":
                    if amount is not None:
                        amount = abs(float(amount))
                elif tx_type == "TRANSFER":
                    # For contributions/withdrawals, preserve sign if present; otherwise infer from keywords.
                    if amount is None:
                        amount = 0.0
                    up = (desc or "").upper()
                    if abs(float(amount)) <= 1e-9:
                        if any(k in up for k in ("DISTRIBUT", "WITHDRAW", "DISBURSE", "ACH PULL")):
                            amount = -abs(float(amount))
                        elif any(k in up for k in ("CONTRIBUT", "DEPOSIT", "ACH PUSH")):
                            amount = abs(float(amount))

                provider_txn_id = _stable_txid("CHASE:IRA", d, tx_type, symbol, qty, amount, desc)
                items.append(
                    {
                        "date": d.isoformat(),
                        "type": tx_type,
                        "ticker": symbol,
                        "qty": float(qty) if qty is not None else None,
                        "amount": float(amount or 0.0),
                        "description": desc,
                        "provider_transaction_id": provider_txn_id,
                        "provider_account_id": "CHASE:IRA",
                        "source_file": p.name,
                        "source_row": None,
                        "source_file_hash": f.file_hash,
                        "currency": "USD",
                    }
                )

                # Chase statements sometimes include tax withheld on dividend rows (in addition to separate TAX rows).
                tax_w = _as_float(row.get("tax_withheld") or row.get("tax_withheld_usd"))
                if tax_w is not None and abs(float(tax_w)) > 1e-9:
                    w_amt = abs(float(tax_w))
                    w_desc = (desc + " (Tax withheld)") if desc else "Tax withheld"
                    w_id = _stable_txid("CHASE:IRA", d, "WITHHOLDING", symbol, None, w_amt, w_desc)
                    items.append(
                        {
                            "date": d.isoformat(),
                            "type": "WITHHOLDING",
                            "ticker": symbol,
                            "qty": None,
                            "amount": float(w_amt),
                            "description": w_desc,
                            "provider_transaction_id": w_id,
                            "provider_account_id": "CHASE:IRA",
                            "source_file": p.name,
                            "source_row": None,
                            "source_file_hash": f.file_hash,
                            "currency": "USD",
                        }
                    )
        except Exception as e:
            raise ProviderError(f"Failed to parse Chase transactions file {p.name}: {type(e).__name__}: {e}")

        next_cursor = str(idx + 1) if (idx + 1) < len(files) else None
        return items, next_cursor
