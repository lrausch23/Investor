from __future__ import annotations

import csv
import datetime as dt
import hashlib
import io
import math
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional
from xml.etree import ElementTree

from src.importers.adapters import BrokerAdapter, ProviderError
from src.utils.time import date_from_filename, end_of_day_utc, utcnow
from src.utils.time import utcfromtimestamp


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _parse_date(value: str) -> dt.date:
    v = (value or "").strip()
    if not v:
        raise ValueError("missing date")
    # Common IB format: YYYYMMDD;HHMMSS
    if ";" in v:
        v = v.split(";", 1)[0].strip()
    # If the value starts with 8 digits, treat as YYYYMMDD.
    if len(v) >= 8 and v[:8].isdigit():
        try:
            return dt.datetime.strptime(v[:8], "%Y%m%d").date()
        except Exception:
            pass
    # Common formats: YYYY-MM-DD, YYYYMMDD, MM/DD/YYYY
    try:
        return dt.date.fromisoformat(v[:10])
    except Exception:
        pass
    if len(v) == 8 and v.isdigit():
        return dt.datetime.strptime(v, "%Y%m%d").date()
    for fmt in ("%m/%d/%Y", "%Y/%m/%d", "%d/%m/%Y"):
        try:
            return dt.datetime.strptime(v, fmt).date()
        except Exception:
            continue
    raise ValueError(f"unrecognized date format: {value}")


def _as_float(value: Any) -> float:
    if value is None:
        raise ValueError("missing number")
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value).strip()
    if s == "":
        raise ValueError("missing number")
    # Allow commas and parentheses.
    s = s.replace(",", "")
    neg = False
    if s.startswith("(") and s.endswith(")"):
        neg = True
        s = s[1:-1]
    out = float(s)
    return -out if neg else out


def _as_float_or_none(value: Any) -> float | None:
    try:
        v = _as_float(value)
    except Exception:
        return None
    try:
        if math.isnan(v):
            return None
    except Exception:
        pass
    return v


def _norm_key(s: str) -> str:
    return "".join(ch for ch in (s or "").strip().lower() if ch.isalnum() or ch == "_")


def _get_any(row: dict[str, Any], keys: list[str]) -> Optional[str]:
    norm = {_norm_key(k): k for k in row.keys()}
    for k in keys:
        kk = _norm_key(k)
        if kk in norm:
            v = row.get(norm[kk])
            if v is None:
                continue
            sv = str(v).strip()
            if sv != "":
                return sv
    return None


def _map_tx_type(raw: str) -> str:
    v = (raw or "").strip().upper()
    if v in {"BUY", "BOT"}:
        return "BUY"
    if v in {"SELL", "SLD"}:
        return "SELL"
    if "DIV" in v:
        return "DIV"
    if v in {"INT", "INTEREST"} or "INTEREST" in v:
        return "INT"
    if "WITHHOLD" in v or "TAX" in v:
        return "WITHHOLDING"
    if "FEE" in v or "COMMISSION" in v:
        return "FEE"
    if "TRANSFER" in v or "WIRE" in v or "DEPOSIT" in v or "WITHDRAW" in v:
        return "TRANSFER"
    return "OTHER"


def _contains_any(haystack: str, needles: list[str]) -> bool:
    h = (haystack or "").upper()
    return any(n.upper() in h for n in needles)


def _classify_activity_row(
    row: dict[str, Any],
    *,
    qty: float | None,
    cash: float | None,
    description: str,
) -> str:
    """
    Normalize IB Activity rows into our Transaction.type values.

    Output types: BUY/SELL/DIV/INT/FEE/WITHHOLDING/TRANSFER/OTHER
    """
    # 1) Explicit side field wins for trades.
    side = _get_any(row, ["buy/sell", "buysell", "side", "action"])
    if side:
        s = side.strip().upper()
        if s in {"BUY", "B", "BOT"}:
            return "BUY"
        if s in {"SELL", "S", "SLD"}:
            return "SELL"

    # 2) Keywords (description-like fields).
    # Prefer explicit withholding/fee keywords over dividend keywords: many IB rows include "CASH DIVIDEND ... - US TAX"
    # for the withholding line, and we'd otherwise misclassify it as DIV.
    desc_u = (description or "").upper()
    if _contains_any(desc_u, ["WITHHOLD", "WHT"]):
        return "WITHHOLDING"
    if _contains_any(desc_u, ["COMMISSION", "REG FEE", "SEC FEE", "STAMP", "FEE"]):
        return "FEE"
    if _contains_any(desc_u, ["INTEREST", " INT"]):
        return "INT"
    if _contains_any(desc_u, ["DIVIDEND", "CASH DIV", "DIV "]):
        return "DIV"
    if _contains_any(desc_u, ["TRANSFER", "CONTRIBUTION", "WITHDRAWAL", "DEPOSIT", "WIRE"]):
        return "TRANSFER"

    txn_type = _get_any(row, ["transactiontype", "type", "activitytype", "activity"]) or ""
    # TransactionType/Type is more reliable than description for distinguishing dividend vs withholding lines.
    if _contains_any(txn_type, ["WITHHOLD"]):
        return "WITHHOLDING"
    if _contains_any(txn_type, ["FEE", "COMMISSION"]):
        return "FEE"
    if _contains_any(txn_type, ["INTEREST", "INT"]):
        return "INT"
    if _contains_any(txn_type, ["DIV"]):
        return "DIV"
    if _contains_any(txn_type, ["TRANSFER", "WIRE", "WITHDRAW", "DEPOSIT"]):
        return "TRANSFER"

    # 3) Infer BUY/SELL from qty/cash sign if present.
    if qty is not None and qty != 0 and cash is not None and cash != 0:
        # Typical IB: BUY => qty>0, NetCash<0; SELL => qty<0, NetCash>0
        if cash < 0:
            return "BUY"
        if cash > 0:
            return "SELL"

    return "OTHER"


def _extract_cash_amount(row: dict[str, Any], *, is_trade: bool) -> float | None:
    """
    Best-effort cash amount extraction for IB CSV rows.

    - For trades, prefer NetCash/Proceeds/TradeMoney.
    - For cashflows, prefer Amount/NetAmount/NetCash, or Debit/Credit columns.
    """
    # Debit/Credit style exports
    credit_s = _get_any(row, ["credit", "creditamount", "credit amount", "cr"])
    debit_s = _get_any(row, ["debit", "debitamount", "debit amount", "dr"])
    credit = _as_float_or_none(credit_s) if credit_s is not None else None
    debit = _as_float_or_none(debit_s) if debit_s is not None else None
    if credit is not None or debit is not None:
        c = float(credit or 0.0)
        d = float(debit or 0.0)
        if abs(c) > 1e-12 or abs(d) > 1e-12:
            return c - d

    # Column name variants
    if is_trade:
        keys = [
            "netcash",
            "net cash",
            "proceeds",
            "trademoney",
            "trade money",
            "amount",
            "netamount",
            "net amount",
        ]
    else:
        keys = [
            "amount",
            "netamount",
            "net amount",
            "netcash",
            "net cash",
            "grossamount",
            "gross amount",
            "total",
            "cash",
            "cashamount",
            "cash amount",
        ]
    s = _get_any(row, keys)
    if s is None:
        # As a last resort for fee-only rows.
        s = _get_any(row, ["fee", "fees", "commission", "ibcommission", "ib commission"])
    return _as_float_or_none(s) if s is not None else None


def _extract_balance(row: dict[str, Any]) -> float | None:
    s = _get_any(row, ["balance", "endingbalance", "ending balance", "endingcash", "ending cash", "cashbalance", "cash balance"])
    return _as_float_or_none(s) if s is not None else None


def _extract_currency(row: dict[str, Any]) -> str | None:
    ccy = _get_any(row, ["currency", "currencyprimary", "ccy", "basecurrency", "base currency"])
    if not ccy:
        return None
    c = str(ccy).strip().upper()
    return c if _is_ccy_code(c) else None


def _normalize_level_of_detail(value: str | None) -> str:
    """
    Normalize IB Flex Trades "LevelOfDetail" to stable tokens:
    EXECUTION, CLOSED_LOT, WASH_SALE, SYMBOL_SUMMARY.

    Real-world exports vary: "WASH SALE" vs "WASH_SALE", "ClosedLot", etc.
    """
    if not value:
        return ""
    s = str(value).strip()
    if not s:
        return ""
    k = _norm_key(s)  # e.g. "wash_sale", "closedlot", "symbolsummary"
    if k in {"execution"}:
        return "EXECUTION"
    if k in {"closed_lot", "closedlot"}:
        return "CLOSED_LOT"
    if k in {"wash_sale", "washsale"}:
        return "WASH_SALE"
    if k in {"symbol_summary", "symbolsummary"}:
        return "SYMBOL_SUMMARY"
    return s.strip().upper()


def _classify_file(path: Path) -> str:
    name = path.name.lower()
    if "position" in name or "holding" in name or "openpositions" in name:
        return "HOLDINGS"
    if name.endswith(".xml"):
        return "TRANSACTIONS"
    return "TRANSACTIONS"


def _list_files(data_dir: Path) -> list[Path]:
    if not data_dir.exists() or not data_dir.is_dir():
        return []
    out: list[Path] = []
    for p in sorted(data_dir.glob("**/*")):
        if not p.is_file():
            continue
        if p.suffix.lower() not in {".csv", ".xml"}:
            continue
        out.append(p)
    return out


def _iter_ib_activity_rows(fp):
    """
    IB "Activity" exports can contain a short preamble before the actual header row.
    This helper finds a plausible header row (one that includes DateTime/TradeDate)
    and returns dict rows for the remainder.
    """
    reader = csv.reader(fp)
    header: list[str] | None = None
    for row in reader:
        if not row:
            continue
        # Strip surrounding whitespace/quotes.
        row = [str(c).strip().strip('"') for c in row]
        low = {c.strip().lower() for c in row if c is not None}
        has_date = ("datetime" in low) or ("tradedate" in low) or ("trade date" in low) or ("date" in low) or ("activitydate" in low) or ("date/time" in low) or ("date time" in low)
        has_symbol = ("symbol" in low) or ("ticker" in low) or ("underlyingsymbol" in low)
        has_type = ("type" in low) or ("transactiontype" in low) or ("transaction type" in low) or ("activitytype" in low) or ("activity code" in low) or ("activitycode" in low) or ("buy/sell" in low) or ("buysell" in low)
        has_desc = ("description" in low) or ("details" in low) or ("memo" in low) or ("notes/codes" in low) or ("notes" in low)
        has_amount = ("netcash" in low) or ("net cash" in low) or ("amount" in low) or ("netamount" in low) or ("net amount" in low) or ("grossamount" in low) or ("gross amount" in low) or ("total" in low) or ("credit" in low) or ("debit" in low)
        is_header = (
            # Trade-like exports.
            ("datetime" in low)
            or ("tradedate" in low)
            or ("trade date" in low)
            or ((has_date and has_type and has_symbol))
            # Cashflow-like exports (Statement of Funds / Cash Transactions).
            or ((has_date and has_amount and (has_desc or has_type)))
        )
        if header is None:
            if is_header:
                header = row
            continue
        # Allow section header repeats to reset header.
        if is_header:
            header = row
            continue
        d = {header[i]: (row[i] if i < len(row) else "") for i in range(len(header))}
        yield d


def _iter_positions_rows(fp):
    reader = csv.reader(fp)
    header: list[str] | None = None
    for row in reader:
        if not row:
            continue
        row = [str(c).strip().strip('"') for c in row]
        low = {c.strip().lower() for c in row if c is not None}
        has_symbol = any(k in low for k in {"symbol", "ticker", "underlyingsymbol"})
        has_qty = any(k in low for k in {"quantity", "position", "pos", "qty"})
        has_value = any(k in low for k in {"marketvalue", "positionvalue", "value", "mktvalue"})
        has_price = any(k in low for k in {"price", "markprice", "closeprice", "mark price"})
        is_header = has_symbol and (has_qty or has_value or has_price)
        if header is None:
            if is_header:
                header = row
            continue
        if is_header:
            header = row
            continue
        d = {header[i]: (row[i] if i < len(row) else "") for i in range(len(header))}
        yield d


def _iter_cash_rows(fp):
    """
    Best-effort parser for "cash report" sections sometimes included in IB position exports.
    Detects a header row with a currency-like column + a cash amount column, then yields dict rows.
    """
    reader = csv.reader(fp)
    header: list[str] | None = None
    for row in reader:
        if not row:
            continue
        row = [str(c).strip().strip('"') for c in row]
        low = {c.strip().lower() for c in row if c is not None}
        has_currency = any(k in low for k in {"currency", "ccy", "currencyprimary", "basecurrency"})
        has_cash = any(
            k in low
            for k in {
                "cash",
                "endingcash",
                "ending cash",
                "totalcashbalance",
                "total cash balance",
                "cashbalance",
                "cash balance",
                "settledcash",
                "settled cash",
                "endingsettledcash",
                "ending settled cash",
            }
        )
        is_header = has_currency and has_cash
        if header is None:
            if is_header:
                header = row
            continue
        if is_header:
            header = row
            continue
        d = {header[i]: (row[i] if i < len(row) else "") for i in range(len(header))}
        yield d


_CCY_RE = re.compile(r"^[A-Z]{3}$")


def _is_ccy_code(v: str | None) -> bool:
    if not v:
        return False
    return bool(_CCY_RE.match(v.strip().upper()))


def _extract_preamble_account(text: str) -> str | None:
    """
    Some IB exports start with:
      "ClientAccountID"
      "U12345"
    Capture that value as a fallback account id for cash/positions rows that omit it.
    """
    try:
        r = csv.reader(io.StringIO(text))
        first = next(r, None)
        second = next(r, None)
        if first and str(first[0]).strip().strip('"').lower() == "clientaccountid":
            if second and str(second[0]).strip().strip('"'):
                return str(second[0]).strip().strip('"')
    except Exception:
        return None
    return None


@dataclass(frozen=True)
class OfflineFile:
    path: Path
    file_hash: str
    kind: str


class IBFlexOfflineAdapter(BrokerAdapter):
    """
    Offline adapter that ingests local IB Flex / statement-like exports (CSV or XML).

    Connection metadata_json:
      - data_dir: directory containing exported files (csv/xml)
      - default_account_name (optional): used if account column not present
      - default_account_type (optional): TAXABLE|IRA|OTHER (default TAXABLE)

    Runtime settings (connection.run_settings, provided by sync runner):
      - selected_files: list[dict] with keys: path, file_hash, kind
    """

    @property
    def page_size(self) -> int:
        # For offline we paginate by file (1 file = 1 page).
        return 1

    def _data_dir(self, connection: Any) -> Path:
        meta = getattr(connection, "metadata_json", {}) or {}
        data_dir = meta.get("data_dir") or meta.get("fixture_dir")
        if not data_dir:
            return Path("data") / "external" / f"conn_{getattr(connection, 'id', 'unknown')}"
        return Path(os.path.expanduser(str(data_dir)))

    def _selected_files(self, connection: Any) -> list[OfflineFile]:
        run_settings = getattr(connection, "run_settings", None) or {}
        selected = run_settings.get("selected_files")
        out: list[OfflineFile] = []
        # If the sync runner provided an explicit selection (including empty), treat it as authoritative.
        if isinstance(selected, list):
            for it in selected:
                try:
                    out.append(
                        OfflineFile(
                            path=Path(str(it["path"])),
                            file_hash=str(it["file_hash"]),
                            kind=str(it.get("kind") or _classify_file(Path(str(it["path"])))),
                        )
                    )
                except Exception:
                    continue
            return out

        data_dir = self._data_dir(connection)
        for p in _list_files(data_dir):
            out.append(OfflineFile(path=p, file_hash=_sha256_file(p), kind=_classify_file(p)))
        return out

    def test_connection(self, connection: Any) -> dict[str, Any]:
        data_dir = self._data_dir(connection)
        files = _list_files(data_dir)
        if not files:
            return {"ok": False, "message": f"No .csv/.xml files found in {data_dir}."}
        return {"ok": True, "message": f"OK (offline files): {len(files)} file(s) found.", "data_dir": str(data_dir)}

    def fetch_accounts(self, connection: Any) -> list[dict[str, Any]]:
        meta = getattr(connection, "metadata_json", {}) or {}
        default_name = str(meta.get("default_account_name") or "IB Flex")
        default_type = str(meta.get("default_account_type") or "TAXABLE").upper()

        files = self._selected_files(connection)
        data_dir = self._data_dir(connection)
        if not files and data_dir.exists():
            files = [OfflineFile(path=p, file_hash=_sha256_file(p), kind=_classify_file(p)) for p in _list_files(data_dir)]

        names: set[str] = set()
        for f in files:
            if f.kind != "TRANSACTIONS" or f.path.suffix.lower() != ".csv":
                continue
            try:
                with f.path.open("r", encoding="utf-8-sig", newline="") as fp:
                    # Handle preamble style: first lines may contain just ClientAccountID then the account value.
                    r = csv.reader(fp)
                    raw = []
                    for _ in range(2):
                        try:
                            raw.append(next(r))
                        except StopIteration:
                            break
                    # If first row is "ClientAccountID" and second row is an account id, capture it.
                    if len(raw) >= 2 and raw[0] and str(raw[0][0]).strip().strip('"').lower() == "clientaccountid":
                        if raw[1] and str(raw[1][0]).strip().strip('"'):
                            names.add(str(raw[1][0]).strip().strip('"'))
                    # Rewind and parse rows properly for account ids.
                    fp.seek(0)
                    for i, row in enumerate(_iter_ib_activity_rows(fp)):
                        acct = _get_any(row, ["account", "account_name", "accountid", "clientaccountid", "accountnumber"])
                        if acct:
                            names.add(acct)
                        if i >= 200:
                            break
            except Exception:
                continue

        if not names:
            return [{"provider_account_id": "IBFLEX-1", "name": default_name, "account_type": default_type}]

        out: list[dict[str, Any]] = []
        for name in sorted(names):
            out.append({"provider_account_id": f"IBFLEX:{name}", "name": name, "account_type": default_type})
        return out

    def fetch_holdings(self, connection: Any, as_of: dt.datetime | None = None) -> dict[str, Any]:
        run_settings = getattr(connection, "run_settings", None) or {}
        forced_path = str(run_settings.get("holdings_file_path") or "").strip()
        forced: Path | None = None
        if forced_path:
            try:
                p0 = Path(os.path.expanduser(forced_path))
                if p0.exists() and p0.is_file():
                    forced = p0
            except Exception:
                forced = None

        files = self._selected_files(connection)
        holdings_files = [f for f in files if f.kind == "HOLDINGS"]
        if not holdings_files:
            # Also scan directory for holdings-like files in case selected_files were txn-only.
            data_dir = self._data_dir(connection)
            for p in _list_files(data_dir):
                if _classify_file(p) == "HOLDINGS":
                    holdings_files.append(OfflineFile(path=p, file_hash=_sha256_file(p), kind="HOLDINGS"))

        if not holdings_files:
            return {"as_of": (as_of or utcnow()).isoformat(), "items": []}

        if forced is not None:
            p = forced
        else:
            # Choose the newest holdings file.
            holdings_files.sort(key=lambda f: f.path.stat().st_mtime, reverse=True)
            p = holdings_files[0].path
        items: list[dict[str, Any]] = []
        cash_balances: list[dict[str, Any]] = []
        try:
            file_mtime_asof = utcfromtimestamp(p.stat().st_mtime)
            name_date = date_from_filename(p.name)
            file_name_asof = end_of_day_utc(name_date) if name_date else None
            inferred_report_date: dt.date | None = None
            if p.suffix.lower() == ".csv":
                text = p.read_text(encoding="utf-8-sig")
                fallback_acct = _extract_preamble_account(text)
                # Positions section.
                for idx, row in enumerate(_iter_positions_rows(io.StringIO(text))):
                        symbol = _get_any(row, ["symbol", "ticker", "underlyingsymbol", "financialinstrument"])
                        qty_s = _get_any(row, ["qty", "quantity", "position", "pos"])
                        mv_s = _get_any(row, ["marketvalue", "market_value", "positionvalue", "value", "mktvalue"])
                        cb_s = _get_any(row, ["costbasismoney", "cost basis money", "costbasis", "cost basis"])
                        px_s = _get_any(row, ["price", "markprice", "closeprice", "mark price"])
                        acct = _get_any(row, ["clientaccountid", "account", "accountid", "accountnumber"])
                        report_date_s = _get_any(row, ["reportdate", "todate", "date"])
                        if report_date_s:
                            try:
                                rd = _parse_date(report_date_s)
                                if inferred_report_date is None or rd > inferred_report_date:
                                    inferred_report_date = rd
                            except Exception:
                                pass
                        if not symbol:
                            continue
                        qty = _as_float(qty_s) if qty_s is not None else None
                        mv = _as_float(mv_s) if mv_s is not None else None
                        cb = _as_float(cb_s) if cb_s is not None else None
                        if mv is None and qty is not None and px_s is not None:
                            try:
                                mv = float(qty) * _as_float(px_s)
                            except Exception:
                                mv = None
                        items.append(
                            {
                                "provider_account_id": f"IBFLEX:{acct}" if acct else (f"IBFLEX:{fallback_acct}" if fallback_acct else "IBFLEX-1"),
                                "symbol": symbol,
                                "qty": qty,
                                "market_value": mv,
                                "cost_basis_total": cb,
                                "source_file": p.name,
                                "row": idx + 1,
                            }
                        )
                # Cash section (optional).
                cash_candidates: dict[tuple[str, str], dict[str, Any]] = {}
                for idx, row in enumerate(_iter_cash_rows(io.StringIO(text))):
                    ccy = (_get_any(row, ["currency", "ccy", "currencyprimary"]) or "").strip().upper()
                    if not _is_ccy_code(ccy):
                        continue
                    amt_s = _get_any(
                        row,
                        [
                            "endingsettledcash",
                            "ending settled cash",
                            "endingcash",
                            "ending cash",
                            "cash",
                            "totalcashbalance",
                            "total cash balance",
                            "cashbalance",
                            "cash balance",
                            "settledcash",
                            "settled cash",
                        ],
                    )
                    if amt_s is None:
                        continue
                    try:
                        amt = _as_float(amt_s)
                    except Exception:
                        continue

                    acct = _get_any(row, ["clientaccountid", "account", "accountid", "accountnumber"]) or fallback_acct
                    provider_account_id = f"IBFLEX:{acct}" if acct else "IBFLEX-1"

                    # Prefer the latest report date within the file when present.
                    report_date_s = _get_any(row, ["reportdate", "todate", "date"])
                    report_date: dt.date | None = None
                    if report_date_s:
                        try:
                            report_date = _parse_date(report_date_s)
                        except Exception:
                            report_date = None
                    if report_date is not None and (inferred_report_date is None or report_date > inferred_report_date):
                        inferred_report_date = report_date

                    key = (provider_account_id, ccy)
                    existing = cash_candidates.get(key)
                    if existing is None:
                        cash_candidates[key] = {
                            "provider_account_id": provider_account_id,
                            "currency": ccy,
                            "amount": amt,
                            "as_of_date": report_date.isoformat() if report_date else None,
                            "source_file": p.name,
                        }
                    else:
                        ex_d = existing.get("as_of_date")
                        ex_date = dt.date.fromisoformat(ex_d) if ex_d else None
                        if report_date and (ex_date is None or report_date > ex_date):
                            existing["amount"] = amt
                            existing["as_of_date"] = report_date.isoformat()

                cash_balances = list(cash_candidates.values())
                # Also surface cash in the holdings item list so the connection detail page shows it.
                for c in cash_balances:
                    amt = float(c.get("amount") or 0.0)
                    ccy = str(c.get("currency") or "USD").strip().upper()
                    provider_account_id = str(c.get("provider_account_id") or "IBFLEX-1")
                    if abs(amt) <= 1e-9:
                        continue
                    items.append(
                        {
                            "provider_account_id": provider_account_id,
                            "symbol": f"CASH:{ccy}",
                            "qty": amt,
                            "market_value": amt,
                            "asset_type": "CASH",
                            "source_file": p.name,
                            "row": "cash",
                        }
                    )
            else:
                # Minimal XML support: look for <Position symbol="..." position="..." marketValue="..."/>
                root = ElementTree.fromstring(p.read_text(encoding="utf-8"))
                for idx, pos in enumerate(root.findall(".//Position")):
                    symbol = pos.attrib.get("symbol") or pos.attrib.get("ticker")
                    if not symbol:
                        continue
                    qty = pos.attrib.get("position") or pos.attrib.get("qty")
                    mv = pos.attrib.get("marketValue") or pos.attrib.get("value")
                    items.append(
                        {
                            "provider_account_id": "IBFLEX-1",
                            "symbol": symbol,
                            "qty": _as_float(qty) if qty is not None else None,
                            "market_value": _as_float(mv) if mv is not None else None,
                            "source_file": p.name,
                            "row": idx + 1,
                        }
                    )
        except Exception as e:
            raise ProviderError(f"Failed to parse holdings file {p.name}: {type(e).__name__}: {e}")

        inferred_asof_dt = end_of_day_utc(inferred_report_date) if inferred_report_date else None
        as_of_dt = as_of or inferred_asof_dt or file_name_asof or file_mtime_asof
        out: dict[str, Any] = {"as_of": as_of_dt.isoformat(), "items": items, "source_file": p.name}
        if cash_balances:
            out["cash_balances"] = cash_balances
        return out

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
        try:
            if p.suffix.lower() == ".csv":
                cash_balance_last: dict[tuple[str, str], dict[str, Any]] = {}
                with p.open("r", encoding="utf-8-sig", newline="") as fp:
                    for row_idx, row in enumerate(_iter_ib_activity_rows(fp)):
                        level = _normalize_level_of_detail(_get_any(row, ["levelofdetail", "level_of_detail", "level of detail"]))
                        # Many IB cashflow exports include both SUMMARY and DETAIL rows for the same event.
                        # Import DETAIL rows only to avoid double counting.
                        if level == "SUMMARY":
                            continue
                        date_s = _get_any(row, ["date", "activitydate", "trade_date", "tradedate", "settledate", "datetime", "date/time", "date time"])
                        # Some multi-detail rows (notably WASH_SALE) may not populate TradeDate/DateTime; fall back
                        # to the broker timestamps for "when realized" etc so rows can still be imported and filtered by year.
                        if not date_s and level == "WASH_SALE":
                            date_s = _get_any(row, ["whenrealized", "when realized", "holdingperioddatetime", "holding period date time", "whenreopened", "when reopened"])
                        if not date_s:
                            continue
                        try:
                            d = _parse_date(date_s)
                        except Exception:
                            # Some IB exports include non-data rows where the date column contains a token like "MULTI".
                            # Skip these rows rather than failing the entire file/run.
                            continue

                        acct = _get_any(row, ["account", "account_name", "accountid", "clientaccountid", "accountnumber"])
                        provider_account_id = f"IBFLEX:{acct}" if acct else "IBFLEX-1"

                        symbol = _get_any(row, ["symbol", "ticker", "underlyingsymbol"])
                        qty_s = _get_any(row, ["qty", "quantity", "shares", "units"])
                        raw_type = _get_any(
                            row,
                            [
                                "transactiontype",
                                "transaction type",
                                "type",
                                "activitytype",
                                "activity",
                                "activity code",
                                "activitycode",
                                "notes/codes",
                                "notes",
                            ],
                        ) or ""
                        desc = _get_any(row, ["description", "memo", "details"]) or ""
                        if not desc:
                            desc = str(raw_type or "")
                        txid = _get_any(
                            row,
                            [
                                "provider_transaction_id",
                                "transactionid",
                                "id",
                                "tradeid",
                                "ibexecid",
                                "cashtransactionid",
                                "cash transaction id",
                            ],
                        )

                        qty = _as_float_or_none(qty_s) if qty_s is not None else None
                        has_side = bool(_get_any(row, ["buy/sell", "buysell", "side", "action"]))
                        is_trade = bool(level in {"EXECUTION"} or (symbol and qty is not None and abs(float(qty)) > 1e-12 and has_side))
                        amount = _extract_cash_amount(row, is_trade=is_trade)

                        ccy_txn = _extract_currency(row) or (_get_any(row, ["currency", "currencyprimary", "ccy"]) or "").strip().upper() or "USD"
                        bal = _extract_balance(row)
                        if bal is not None and _is_ccy_code(ccy_txn):
                            cash_balance_last[(provider_account_id, ccy_txn)] = {
                                "record_kind": "CASH_BALANCE",
                                "provider_account_id": provider_account_id,
                                "currency": ccy_txn,
                                "as_of_date": d.isoformat(),
                                "amount": float(bal),
                                "source_file": p.name,
                                "source_row": row_idx + 1,
                                "source_file_hash": f.file_hash,
                            }

                        # Multi-detail Trades export:
                        # - EXECUTION -> transactions (existing path)
                        # - CLOSED_LOT/WASH_SALE -> broker-based tax records (imported by sync runner)
                        if level in {"CLOSED_LOT", "WASH_SALE"}:
                            cost_basis = _as_float_or_none(_get_any(row, ["costbasis", "costbasismoney", "cost basis", "cost basis money"]))
                            fifo_realized = _as_float_or_none(_get_any(row, ["fifopnlrealized", "fifo pnl realized", "fifopnlrealizedpl"]))
                            proceeds = (cost_basis + fifo_realized) if (cost_basis is not None and fifo_realized is not None) else None
                            conid = _get_any(row, ["conid"])
                            ccy = _get_any(row, ["currency", "currencyprimary", "ccy"])
                            fx = _as_float_or_none(_get_any(row, ["fxratetobase", "fx rate to base"]))
                            trade_id = _get_any(row, ["tradeid", "ib_trade_id"])
                            txn_id = _get_any(row, ["transactionid", "ib_transaction_id"])
                            dt_raw = _get_any(row, ["datetime", "date/time", "date time"]) or None
                            open_dt_raw = _get_any(row, ["opendatetime", "open date time", "open_date_time"]) or None
                            holding_dt_raw = _get_any(row, ["holdingperioddatetime", "holding period date time"]) or None
                            when_realized_raw = _get_any(row, ["whenrealized", "when realized"]) or None
                            when_reopened_raw = _get_any(row, ["whenreopened", "when reopened"]) or None

                            if not txid:
                                key = f"{provider_account_id}|{d.isoformat()}|{level}|{symbol or ''}|{qty or ''}|{cost_basis or ''}|{fifo_realized or ''}|{open_dt_raw or ''}"
                                txid = f"FILE:{f.file_hash}:{_sha256_bytes(key.encode('utf-8'))}"

                            items.append(
                                {
                                    "record_kind": "BROKER_CLOSED_LOT" if level == "CLOSED_LOT" else "BROKER_WASH_SALE",
                                    "provider_account_id": provider_account_id,
                                    "date": d.isoformat(),
                                    "symbol": symbol,
                                    "qty": abs(qty) if qty is not None else None,
                                    "cost_basis": cost_basis,
                                    "realized_pl_fifo": fifo_realized,
                                    "proceeds_derived": proceeds,
                                    "currency": ccy,
                                    "fx_rate_to_base": fx,
                                    "conid": conid,
                                    "ib_transaction_id": txn_id,
                                    "ib_trade_id": trade_id,
                                    "datetime_raw": dt_raw,
                                    "open_datetime_raw": open_dt_raw,
                                    "holding_period_datetime_raw": holding_dt_raw,
                                    "when_realized_raw": when_realized_raw,
                                    "when_reopened_raw": when_reopened_raw,
                                    "source_file": p.name,
                                    "source_row": row_idx + 1,
                                    "source_file_hash": f.file_hash,
                                    "raw_row": row,
                                }
                            )
                            continue
                        if level == "SYMBOL_SUMMARY":
                            items.append({"record_kind": "BROKER_SYMBOL_SUMMARY", "source_file": p.name, "source_row": row_idx + 1, "source_file_hash": f.file_hash})
                            continue

                        tx_type = _classify_activity_row(row, qty=qty, cash=amount, description=f"{desc} {raw_type}".strip())
                        if amount is None:
                            # For non-trade cashflows, skip if there is no parsable cash amount.
                            if tx_type not in {"BUY", "SELL"}:
                                continue
                            amount_f = 0.0
                        else:
                            amount_f = float(amount)

                        qty_abs = abs(qty) if qty is not None else None
                        if tx_type == "BUY":
                            if qty_abs is not None:
                                qty = qty_abs
                            amount_f = -abs(amount_f) if amount_f != 0 else amount_f
                        elif tx_type == "SELL":
                            if qty_abs is not None:
                                qty = qty_abs
                            amount_f = abs(amount_f) if amount_f != 0 else amount_f
                        elif tx_type == "WITHHOLDING":
                            # App convention: store withholding as a positive credit.
                            if amount_f != 0:
                                amount_f = abs(amount_f)
                        elif tx_type == "FEE":
                            if amount_f != 0:
                                amount_f = -abs(amount_f)

                        if not desc:
                            desc = f"{tx_type} {symbol or ''}".strip()

                        if not txid:
                            # Deterministic per-row fallback, namespaced to file hash.
                            key = f"{provider_account_id}|{d.isoformat()}|{tx_type}|{symbol or ''}|{qty or ''}|{amount_f}|{desc}"
                            txid = f"FILE:{f.file_hash}:{_sha256_bytes(key.encode('utf-8'))}"
                        cashflow_kind = None
                        if tx_type == "TRANSFER":
                            if amount_f > 0:
                                cashflow_kind = "DEPOSIT"
                            elif amount_f < 0:
                                cashflow_kind = "WITHDRAWAL"
                        items.append(
                            {
                                "record_kind": "TRANSACTION",
                                "provider_transaction_id": txid,
                                "provider_account_id": provider_account_id,
                                "date": d.isoformat(),
                                "type": tx_type,
                                "symbol": symbol,
                                "qty": qty,
                                "amount": amount_f,
                                "description": desc,
                                "currency": ccy_txn,
                                "cashflow_kind": cashflow_kind,
                                "source_file": p.name,
                                "source_row": row_idx + 1,
                                "source_file_hash": f.file_hash,
                            }
                        )
                for _k, rec in cash_balance_last.items():
                    items.append(rec)
            else:
                # Minimal Flex XML support.
                root = ElementTree.fromstring(p.read_text(encoding="utf-8"))
                # Trade-like nodes
                for row_idx, node in enumerate(root.findall(".//Trade")):
                    d_s = node.attrib.get("tradeDate") or node.attrib.get("dateTime") or ""
                    d = _parse_date(d_s)
                    symbol = node.attrib.get("symbol") or node.attrib.get("ticker")
                    qty = node.attrib.get("quantity")
                    amt = node.attrib.get("netCash") or node.attrib.get("proceeds") or node.attrib.get("amount")
                    side = node.attrib.get("buySell") or node.attrib.get("action") or ""
                    tx_type = _map_tx_type(side)
                    desc = node.attrib.get("description") or f"{tx_type} {symbol or ''}".strip()
                    txid = node.attrib.get("transactionID") or node.attrib.get("tradeID")
                    if not txid:
                        key = f"{d.isoformat()}|{tx_type}|{symbol or ''}|{qty or ''}|{amt or ''}|{desc}"
                        txid = f"FILE:{f.file_hash}:{_sha256_bytes(key.encode('utf-8'))}"
                    items.append(
                        {
                            "provider_transaction_id": txid,
                            "provider_account_id": "IBFLEX-1",
                            "date": d.isoformat(),
                            "type": tx_type,
                            "symbol": symbol,
                            "qty": _as_float(qty) if qty is not None else None,
                            "amount": _as_float(amt) if amt is not None else 0.0,
                            "description": desc,
                            "source_file": p.name,
                            "source_row": row_idx + 1,
                        }
                    )
                # CashTransaction-like nodes
                for row_idx, node in enumerate(root.findall(".//CashTransaction")):
                    d_s = node.attrib.get("date") or node.attrib.get("dateTime") or ""
                    d = _parse_date(d_s)
                    symbol = node.attrib.get("symbol") or node.attrib.get("ticker")
                    amt = node.attrib.get("amount") or node.attrib.get("netCash")
                    raw_type = node.attrib.get("type") or node.attrib.get("description") or "OTHER"
                    tx_type = _map_tx_type(raw_type)
                    desc = node.attrib.get("description") or str(raw_type)
                    txid = node.attrib.get("transactionID")
                    if not txid:
                        key = f"{d.isoformat()}|{tx_type}|{symbol or ''}|{amt or ''}|{desc}"
                        txid = f"FILE:{f.file_hash}:{_sha256_bytes(key.encode('utf-8'))}"
                    items.append(
                        {
                            "provider_transaction_id": txid,
                            "provider_account_id": "IBFLEX-1",
                            "date": d.isoformat(),
                            "type": tx_type,
                            "symbol": symbol,
                            "qty": None,
                            "amount": _as_float(amt) if amt is not None else 0.0,
                            "description": desc,
                            "source_file": p.name,
                            "source_row": row_idx + 1,
                        }
                    )
        except Exception as e:
            raise ProviderError(f"Failed to parse transactions file {p.name}: {type(e).__name__}: {e}")

        next_cursor = str(idx + 1) if (idx + 1) < len(files) else None
        return items, next_cursor
