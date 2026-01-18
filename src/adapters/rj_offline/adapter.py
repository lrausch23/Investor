from __future__ import annotations

import csv
import datetime as dt
import hashlib
import io
import os
import re
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from src.importers.adapters import BrokerAdapter, ProviderError
from src.utils.time import date_from_filename, end_of_day_utc, utcfromtimestamp, utcnow


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _norm_key(k: Any) -> str:
    raw = str(k or "").strip()
    if not raw:
        return ""
    suffix = ""
    # Preserve meaning for headers that differ only by "$" vs "%".
    if "$" in raw:
        suffix = "_usd"
    elif "%" in raw:
        suffix = "_pct"

    s = raw.lower()
    # Replace non-alphanumeric chars (e.g. "Symbol/CUSIP") with underscores.
    s = s.replace("$", "").replace("%", "")
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    if suffix and s:
        return f"{s}{suffix}"
    return s


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
    neg = False
    if s.startswith("(") and s.endswith(")"):
        neg = True
        s = s[1:-1]
    # Common adornments in brokerage exports: "$1.00*", "123†", etc.
    s = s.replace("$", "").replace(",", "").replace("*", "").replace("†", "").strip()
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
    # MM/DD/YYYY, MM-DD-YYYY
    for fmt in ("%m/%d/%Y", "%m/%d/%y", "%m-%d-%Y", "%m-%d-%y", "%m.%d.%Y", "%m.%d.%y"):
        try:
            return dt.datetime.strptime(s.split()[0], fmt).date()
        except Exception:
            continue
    return None


def _pdf_to_text(path: Path) -> str:
    exe = shutil.which("pdftotext")
    if not exe:
        raise ProviderError(
            "RJ PDF statements require the `pdftotext` utility (Poppler). "
            "Install it (e.g., `brew install poppler`) or export holdings as CSV instead."
        )
    with tempfile.NamedTemporaryFile(prefix="rj_stmt_", suffix=".txt", delete=False) as tmp:
        out_path = Path(tmp.name)
    try:
        p = subprocess.run(
            [exe, "-layout", str(path), str(out_path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        if p.returncode != 0:
            err = (p.stderr or p.stdout or "").strip()
            hint = f": {err[:200]}" if err else ""
            raise ProviderError(f"Failed to extract text from PDF {path.name} via pdftotext{hint}")
        return out_path.read_text(encoding="utf-8", errors="replace")
    finally:
        try:
            out_path.unlink(missing_ok=True)
        except Exception:
            pass


def _infer_statement_asof_from_text(text: str) -> dt.date | None:
    s = text or ""
    dates: list[dt.date] = []

    # 12/31/2025, 12/31/25, or 12-31-2025
    for m in re.finditer(r"(?<!\d)(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})(?!\d)", s):
        try:
            mo = int(m.group(1))
            da = int(m.group(2))
            y = int(m.group(3))
            if y < 100:
                y = 2000 + y
            dates.append(dt.date(y, mo, da))
        except Exception:
            continue

    # YYYY-MM-DD
    for m in re.finditer(r"(?<!\d)(20\d{2})-(\d{1,2})-(\d{1,2})(?!\d)", s):
        try:
            y = int(m.group(1))
            mo = int(m.group(2))
            da = int(m.group(3))
            dates.append(dt.date(y, mo, da))
        except Exception:
            continue

    return max(dates) if dates else None


def _extract_statement_period(text: str) -> tuple[dt.date | None, dt.date | None]:
    """
    Extract (period_start, period_end) from statement text when possible.

    Common patterns include:
      - "Statement Period: 12/01/2025 - 12/31/2025"
      - "For the period 12/01/2025 through 12/31/2025"
    """
    s = " ".join((text or "").split())
    if not s:
        return None, None
    date_pat = r"(?:\d{1,2}[./-]\d{1,2}[./-]\d{2,4})"
    m = re.search(
        rf"(?i)(statement\s+period|for\s+the\s+period)\s*[:\-]?\s*({date_pat})\s*(?:-|to|through|thru|–|—)\s*({date_pat})",
        s,
    )
    if m:
        try:
            start_s = str(m.group(2)).strip()
            end_s = str(m.group(3)).strip()
            start_d = _parse_date(start_s)
            end_d = _parse_date(end_s)
            return start_d, end_d
        except Exception:
            return None, None

    # Alternate RJ PDF header patterns (month-name dates), e.g.:
    # - "December 31, 2024 to January 31, 2025"
    # - "January 31 to February 28, 2025" (year omitted on start date)
    m2 = re.search(
        r"(?i)\b([A-Za-z]{3,9}\s+\d{1,2}(?:,\s+\d{4})?)\s+to\s+([A-Za-z]{3,9}\s+\d{1,2}(?:,\s+\d{4})?)\b",
        s,
    )
    if not m2:
        return None, None
    start_s = str(m2.group(1)).strip()
    end_s = str(m2.group(2)).strip()
    start_has_year = bool(re.search(r"\b\d{4}\b", start_s))
    end_has_year = bool(re.search(r"\b\d{4}\b", end_s))
    fmts_full = ("%B %d, %Y", "%b %d, %Y")
    fmts_no_year = ("%B %d", "%b %d")

    # Parse end first to infer year when start omits it.
    end_d = None
    if end_has_year:
        for fmt in fmts_full:
            try:
                end_d = dt.datetime.strptime(end_s, fmt).date()
                break
            except Exception:
                continue
    if end_d is None and not end_has_year:
        # Extremely rare; attempt no-year parse with the current year as a fallback.
        for fmt in fmts_no_year:
            try:
                end_d = dt.datetime.strptime(end_s, fmt).date().replace(year=dt.date.today().year)
                break
            except Exception:
                continue
    if end_d is None:
        return None, None

    start_d = None
    if start_has_year:
        for fmt in fmts_full:
            try:
                start_d = dt.datetime.strptime(start_s, fmt).date()
                break
            except Exception:
                continue
    else:
        for fmt in fmts_no_year:
            try:
                start_d = dt.datetime.strptime(start_s, fmt).date().replace(year=end_d.year)
                break
            except Exception:
                continue
    if start_d is None:
        return None, None
    return start_d, end_d


def _extract_statement_begin_end_balances(text: str) -> tuple[float | None, float | None]:
    """
    Best-effort extraction of Beginning Balance and Ending Balance from RJ PDF statement text.
    """
    lines = (text or "").splitlines()
    money_re = re.compile(r"(?P<amt>\(?\$?\d[\d,]*\.?\d*\)?\*?)")

    def _find_amount_for_key(key: str) -> float | None:
        key_u = key.upper()
        best: float | None = None
        for i, raw in enumerate(lines):
            line = str(raw or "").strip()
            if not line:
                continue
            u = line.upper()
            if key_u not in u:
                continue
            # Prefer amounts on the same line; if absent, scan a couple lines below but stop
            # once we hit another balance label.
            candidates: list[str] = [m.group("amt") for m in money_re.finditer(line)]
            if not candidates:
                stop_re = re.compile(r"(?i)\\b(beginning\\s+balance|ending\\s+balance)\\b")
                for j in range(1, 4):
                    if i + j >= len(lines):
                        break
                    nxt = str(lines[i + j] or "").strip()
                    if not nxt:
                        continue
                    if stop_re.search(nxt):
                        break
                    for m in money_re.finditer(nxt):
                        candidates.append(m.group("amt"))
            for c in candidates:
                v = _as_float(c)
                if v is not None and v > 0:
                    if best is None or v > best:
                        best = float(v)
        return best

    begin_v = _find_amount_for_key("Beginning Balance")
    end_v = _find_amount_for_key("Ending Balance")
    return begin_v, end_v


def _extract_statement_total_value(text: str) -> float | None:
    lines = (text or "").splitlines()
    keys = (
        "TOTAL ACCOUNT VALUE",
        "TOTAL PORTFOLIO VALUE",
        "TOTAL MARKET VALUE",
        "NET ASSET VALUE",
        "TOTAL ASSETS",
        "TOTAL VALUE",
        "PORTFOLIO TOTAL",
        "ENDING BALANCE",
        "VALUE THIS STATEMENT",
    )
    noise = ("CHANGE", "GAIN", "LOSS", "RETURN", "YIELD", "PERCENT", "%")
    money_re = re.compile(r"(?P<amt>\(?\$?\d[\d,]*\.?\d*\)?\*?)")

    best: tuple[int, float] | None = None  # (score, value)

    def _score_line(u: str) -> int:
        score = 0
        if "TOTAL ACCOUNT VALUE" in u:
            score += 100
        if "NET ASSET VALUE" in u:
            score += 90
        if "TOTAL PORTFOLIO VALUE" in u:
            score += 80
        if "TOTAL MARKET VALUE" in u:
            score += 70
        if "TOTAL ASSETS" in u:
            score += 60
        if "TOTAL VALUE" in u:
            score += 40
        if any(n in u for n in noise):
            score -= 25
        return score

    def _consider(u: str, raw_amt: str) -> None:
        nonlocal best
        v = _as_float(raw_amt)
        if v is None or v <= 0:
            return
        score = _score_line(u)
        s = str(raw_amt or "")
        if "," in s or v >= 1000:
            score += 10
        if v < 100:
            score -= 10
        if best is None or score > best[0] or (score == best[0] and v > best[1]):
            best = (int(score), float(v))

    # Pass 1: line/near-line search around explicit keys.
    for i, raw in enumerate(lines):
        line = str(raw or "").strip()
        if not line:
            continue
        u = line.upper()
        if not any(k in u for k in keys):
            continue
        # RJ statements often place the dollar amount a few lines below the label in a table.
        lookahead = [line]
        for j in range(1, 8):
            if i + j < len(lines):
                nxt = str(lines[i + j] or "").strip()
                if nxt:
                    lookahead.append(nxt)
        for block_line in lookahead:
            for m in money_re.finditer(block_line):
                _consider(u, m.group("amt"))

    # Pass 2 (fallback): search flattened text for key + nearby amount.
    if best is None:
        flat = "\n".join(lines)
        for k in keys:
            for m in re.finditer(re.escape(k), flat.upper()):
                start = m.start()
                window = flat[start : min(len(flat), start + 600)]
                for mm in money_re.finditer(window):
                    _consider(k, mm.group("amt"))

    return float(best[1]) if best is not None else None


def _split_combined_statement_text(text: str) -> list[str]:
    """
    Split a combined PDF (multiple statements in one file) into statement-sized chunks.
    This is a best-effort heuristic driven by repeated "Statement Period" markers.
    """
    t = text or ""
    if not t.strip():
        return []
    markers = [m.start() for m in re.finditer(r"(?i)statement\s+period", t)]
    if len(markers) <= 1:
        return [t]
    out: list[str] = []
    for i, start in enumerate(markers):
        end = markers[i + 1] if (i + 1) < len(markers) else len(t)
        chunk = t[start:end]
        if chunk.strip():
            out.append(chunk)
    return out


def _split_rj_pdf_text_into_statements(text: str) -> list[str]:
    """
    RJ combined PDFs contain multiple statements, each spanning multiple pages.
    `pdftotext` inserts form-feed separators between pages. We group pages into statements by
    detecting "Page 1 of" markers which reliably indicate statement starts.
    """
    t = text or ""
    if not t.strip():
        return []
    pages = t.split("\f")
    if len(pages) <= 1:
        return _split_combined_statement_text(t)

    start_re = re.compile(r"(?im)\bPage\s+1\s+of\s+\d+\b")
    statement_pages: list[list[str]] = []
    cur: list[str] = []
    for p in pages:
        if not p.strip():
            continue
        if cur and start_re.search(p):
            statement_pages.append(cur)
            cur = [p]
        else:
            cur.append(p)
    if cur:
        statement_pages.append(cur)

    # If we failed to split (e.g., page markers missing), fall back to the older heuristic.
    if len(statement_pages) <= 1:
        return _split_combined_statement_text(t)
    return ["\f".join(ps) for ps in statement_pages]


def _lower_keys(row: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k, v in (row or {}).items():
        kk = _norm_key(k)
        out[kk] = v
    return out


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
        counts = {",": 0, "\t": 0, ";": 0}
        for line in lines[:50]:
            if not line:
                continue
            for d in counts.keys():
                counts[d] += line.count(d)
        if counts["\t"] > 0 and counts["\t"] >= counts[","]:
            return "\t"
        best = max(counts.items(), key=lambda kv: kv[1])[0]
        return best or ","


def _find_header_line_index(lines: list[str], required: set[str], *, delimiter: str) -> int | None:
    for i, line in enumerate(lines[:120]):
        if not line.strip():
            continue
        parts = [_norm_key(p.strip().strip('"').strip("'")) for p in line.split(delimiter)]
        cols = {p for p in parts if p}
        if required.issubset(cols):
            return i
    return None


def _read_csv_rows(text: str) -> tuple[list[str], list[dict[str, Any]]]:
    if not text:
        return [], []
    lines = text.splitlines()
    if not lines:
        return [], []
    delimiter = _sniff_delimiter(text)

    # Prefer holdings-like header if present, else transactions-like.
    header_idx = _find_header_line_index(lines, {"symbol"}, delimiter=delimiter)
    if header_idx is None:
        header_idx = _find_header_line_index(lines, {"symbol_cusip"}, delimiter=delimiter)
    if header_idx is None:
        header_idx = _find_header_line_index(lines, {"ticker"}, delimiter=delimiter)
    if header_idx is None:
        # Realized P&L exports use Opening/Closing date headers.
        header_idx = _find_header_line_index(lines, {"opening_date", "closing_date"}, delimiter=delimiter)
    if header_idx is None:
        header_idx = _find_header_line_index(lines, {"date"}, delimiter=delimiter)
    if header_idx is None:
        header_idx = _find_header_line_index(lines, {"trade_date"}, delimiter=delimiter)
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
    idx = _find_header_line_index(lines, {"symbol"}, delimiter=delimiter)
    if idx is None:
        idx = _find_header_line_index(lines, {"symbol_cusip"}, delimiter=delimiter)
    if idx is None:
        idx = _find_header_line_index(lines, {"ticker"}, delimiter=delimiter)
    if idx is None:
        return False
    header = lines[idx]
    cols = {_norm_key(p.strip().strip('"').strip("'")) for p in header.split(delimiter) if p.strip()}
    has_qty = ("quantity" in cols) or ("qty" in cols) or ("shares" in cols) or ("units" in cols)
    # RJ exports commonly use "Current Value" for holdings snapshots.
    has_value = (
        ("market_value" in cols)
        or ("marketvalue" in cols)
        or ("value" in cols)
        or ("current_value" in cols)
        or ("market_value_usd" in cols)
    )
    looks_like_activity = ("date" in cols or "trade_date" in cols) and (("amount" in cols) or ("description" in cols))
    return bool(has_qty and has_value and not looks_like_activity)


def _looks_like_transactions(text: str) -> bool:
    lines = (text or "").splitlines()
    delimiter = _sniff_delimiter(text)
    idx = _find_header_line_index(lines, {"date"}, delimiter=delimiter)
    if idx is None:
        idx = _find_header_line_index(lines, {"trade_date"}, delimiter=delimiter)
    if idx is None:
        return False
    header = lines[idx]
    cols = {_norm_key(p.strip().strip('"').strip("'")) for p in header.split(delimiter) if p.strip()}
    return (("date" in cols) or ("trade_date" in cols)) and (
        ("amount" in cols)
        or ("net_amount" in cols)
        or ("proceeds" in cols)
        or ("description" in cols)
        or ("transaction_type" in cols)
        or ("type" in cols)
    )

def _looks_like_realized_pl(text: str) -> bool:
    lines = (text or "").splitlines()
    delimiter = _sniff_delimiter(text)
    # Key columns from RJ realized P&L worksheet:
    # - Opening/Closing dates & amounts
    idx = _find_header_line_index(lines, {"opening_date", "closing_date"}, delimiter=delimiter)
    if idx is None:
        return False
    header = lines[idx]
    cols = {_norm_key(p.strip().strip('"').strip("'")) for p in header.split(delimiter) if p.strip()}
    has_open = "opening_amount" in cols or "opening_value" in cols
    has_close = "closing_amount" in cols or "closing_value" in cols
    has_gain = ("realized_gain_loss_usd" in cols) or ("realized_gain_loss" in cols)
    # Some exports encode the first column as "Description (Symbol/CUSIP)".
    has_desc = "description_symbol_cusip" in cols or "description" in cols
    has_qty = "quantity" in cols
    return bool(has_open and has_close and has_gain and has_qty and has_desc and ("closing_date" in cols))


_DESC_SYMBOL_RE = re.compile(r"\((?P<sym>[A-Z0-9.\-]{1,16})\)\s*$")


def _symbol_from_desc(desc: str) -> str | None:
    s = (desc or "").strip()
    if not s:
        return None
    m = _DESC_SYMBOL_RE.search(s.upper())
    if m:
        sym = str(m.group("sym") or "").strip().upper()
        return sym or None
    return None


def _classify_txn(row: dict[str, Any]) -> str:
    t = str(row.get("type") or row.get("transaction_type") or row.get("activity") or row.get("action") or "").strip().upper()
    desc = str(row.get("description") or row.get("details") or row.get("memo") or "").strip().upper()
    joined = " ".join([t, desc]).strip()
    if any(x in joined for x in ("BUY", "BOUGHT", "PURCHASE")):
        return "BUY"
    if any(x in joined for x in ("SELL", "SOLD", "SALE", "REDEMPTION")):
        return "SELL"
    if "DIV" in joined or "DIVIDEND" in joined:
        return "DIV"
    if "INTEREST" in joined or joined.startswith("INT"):
        return "INT"
    if "WITHHOLD" in joined or "TAX" in joined:
        return "WITHHOLDING"
    if "FEE" in joined or "COMMISSION" in joined:
        return "FEE"
    if "CONTRIBUT" in joined or "DEPOSIT" in joined or "TRANSFER" in joined or "ACH" in joined:
        return "TRANSFER"
    if "WITHDRAW" in joined or "DISTRIBUT" in joined:
        return "TRANSFER"
    return "OTHER"


def _stable_txid(provider_account_id: str, date: dt.date, tx_type: str, symbol: str | None, qty: float | None, amount: float | None, desc: str) -> str:
    key = f"{provider_account_id}|{date.isoformat()}|{tx_type}|{symbol or ''}|{qty or ''}|{amount or ''}|{desc}"
    return "RJ:HASH:" + _sha256_bytes(key.encode("utf-8"))


def _list_files(data_dir: Path) -> list[Path]:
    if not data_dir.exists() or not data_dir.is_dir():
        return []
    out: list[Path] = []
    for p in sorted(data_dir.glob("**/*")):
        if p.is_file() and p.suffix.lower() in {".csv", ".tsv", ".txt", ".xml", ".qfx", ".ofx"}:
            out.append(p)
    return out


@dataclass(frozen=True)
class OfflineFile:
    path: Path
    file_hash: str
    kind: str  # TRANSACTIONS|HOLDINGS


class RJOfflineAdapter(BrokerAdapter):
    """
    Offline Raymond James CSV adapter.

    Connection metadata_json:
      - data_dir: directory containing RJ exports

    Notes:
      - RJ export formats vary; this adapter uses tolerant header detection and keyword-based classification.
      - This connector does not require credentials.
      - For MVP, all rows map to a single provider_account_id ("RJ:TAXABLE") which is intended to map to the
        default internal account name "RJ Taxable".
    """

    @property
    def page_size(self) -> int:
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
            if out:
                return out
        data_dir = self._data_dir(connection)
        for p in _list_files(data_dir):
            if p.suffix.lower() in {".qfx", ".ofx"}:
                out.append(OfflineFile(path=p, file_hash=_sha256_bytes(p.read_bytes()), kind="TRANSACTIONS"))
                continue
            try:
                txt = p.read_text(encoding="utf-8-sig")
            except Exception:
                txt = ""
            kind = "HOLDINGS" if _looks_like_holdings(txt) else "TRANSACTIONS"
            out.append(OfflineFile(path=p, file_hash=_sha256_bytes(p.read_bytes()), kind=kind))
        return out

    def test_connection(self, connection: Any) -> dict[str, Any]:
        data_dir = self._data_dir(connection)
        files = _list_files(data_dir)
        if not files:
            return {
                "ok": False,
                "message": f"No supported files found in {data_dir} (.csv/.tsv/.txt/.xml/.qfx/.ofx).",
            }
        return {"ok": True, "message": f"OK (RJ offline): {len(files)} file(s) found.", "data_dir": str(data_dir)}

    def fetch_accounts(self, connection: Any) -> list[dict[str, Any]]:
        return [{"provider_account_id": "RJ:TAXABLE", "name": "RJ Taxable", "account_type": "TAXABLE"}]

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
        next_cursor = str(idx + 1) if (idx + 1) < len(files) else None

        if p.suffix.lower() in {".qfx", ".ofx"}:
            from src.adapters.rj_offline.qfx_parser import (
                extract_qfx_header_meta,
                parse_security_list,
                parse_transactions,
                placeholder_ticker_from_security,
                stable_txn_id_from_qfx,
            )

            try:
                text = p.read_text(encoding="utf-8-sig", errors="ignore")
            except Exception as e:
                raise ProviderError(f"Failed to read RJ QFX file {p.name}: {type(e).__name__}: {e}")

            try:
                sec_map = parse_security_list(text)
            except Exception:
                sec_map = {}

            try:
                hdr = extract_qfx_header_meta(text)
            except Exception:
                hdr = None

            txs = parse_transactions(text)
            provider_account_id = "RJ:TAXABLE"
            out: list[dict[str, Any]] = []
            for i, tx in enumerate(txs):
                d = tx.dt_trade or tx.dt_posted
                if d is None or d < start_date or d > end_date:
                    continue
                raw = str(tx.raw_type or "").upper()
                if raw.startswith("BUY"):
                    ttype = "BUY"
                elif raw.startswith("SELL"):
                    ttype = "SELL"
                elif raw == "INCOME":
                    # Use memo to separate DIV/INT when possible.
                    joined = " ".join([str(tx.name or ""), str(tx.memo or "")]).upper()
                    ttype = "INT" if "INTEREST" in joined else "DIV"
                elif raw in {"REINVEST"}:
                    ttype = "DIV"
                elif raw in {"FEES", "FEE", "INVEXPENSE"}:
                    ttype = "FEE"
                elif raw.startswith("BANKTRN"):
                    joined = " ".join([raw, str(tx.name or ""), str(tx.memo or "")]).upper()
                    if "WITHHOLD" in joined or "TAX" in joined:
                        ttype = "WITHHOLDING"
                    elif "INTEREST" in joined or raw.endswith("_INT"):
                        ttype = "INT"
                    elif "FEE" in joined:
                        ttype = "FEE"
                    else:
                        ttype = "TRANSFER"
                else:
                    joined = " ".join([raw, str(tx.name or ""), str(tx.memo or "")]).upper()
                    if "DIV" in joined:
                        ttype = "DIV"
                    elif "INTEREST" in joined or joined.startswith("INT"):
                        ttype = "INT"
                    elif "FEE" in joined:
                        ttype = "FEE"
                    elif "WITHHOLD" in joined or "TAX" in joined:
                        ttype = "WITHHOLDING"
                    elif "TRANSFER" in joined or "WIRE" in joined or "ACH" in joined or "DEPOSIT" in joined or "WITHDRAW" in joined:
                        ttype = "TRANSFER"
                    else:
                        ttype = "OTHER"

                uid = (tx.unique_id or "").upper() if tx.unique_id else None
                sec = sec_map.get(uid) if uid else None
                ticker = placeholder_ticker_from_security(sec, unique_id=uid) if (uid or sec) else None

                amt = float(tx.amount or 0.0)
                if ttype in {"BUY", "FEE", "WITHHOLDING"}:
                    amt = -abs(amt)
                elif ttype in {"SELL", "DIV", "INT"}:
                    amt = abs(amt)
                # TRANSFER keeps source sign.

                qty = float(tx.units) if tx.units is not None else None
                desc = (tx.memo or tx.name or (sec.name if sec else None) or "").strip()
                additional_detail = ""
                if hdr is not None:
                    additional_detail = " ".join([x for x in [hdr.acct_id, hdr.broker_id] if x]).strip()

                out.append(
                    {
                        "provider_account_id": provider_account_id,
                        "provider_transaction_id": stable_txn_id_from_qfx(provider_account_id=provider_account_id, tx=tx),
                        "date": d.isoformat(),
                        "type": ttype,
                        "ticker": ticker,
                        "qty": qty,
                        "amount": float(amt),
                        "description": desc,
                        "additional_detail": additional_detail,
                        "source_file": p.name,
                        "source_row": i + 1,
                        "source_file_hash": f.file_hash,
                        "qfx_raw_type": raw,
                        "qfx_cusip": uid,
                    }
                )
            return out, next_cursor

        try:
            text = p.read_text(encoding="utf-8-sig")
        except Exception as e:
            raise ProviderError(f"Failed to read RJ file {p.name}: {type(e).__name__}: {e}")

        if _looks_like_realized_pl(text):
            _hdr, rows = _read_csv_rows(text)
            out: list[dict[str, Any]] = []
            for i, r in enumerate(rows):
                row = _lower_keys(r)
                close_d = _parse_date(row.get("closing_date"))
                open_d = _parse_date(row.get("opening_date"))
                if close_d is None or close_d < start_date or close_d > end_date:
                    continue
                desc = str(row.get("description_symbol_cusip") or row.get("description") or "").strip()
                symbol = _symbol_from_desc(desc) or str(row.get("symbol") or row.get("symbol_cusip") or "").strip().upper() or None
                if not symbol:
                    continue
                qty = _as_float(row.get("quantity"))
                if qty is None:
                    continue
                opening_amt = _as_float(row.get("opening_amount") or row.get("opening_value"))
                closing_amt = _as_float(row.get("closing_amount") or row.get("closing_value"))
                realized_amt = _as_float(row.get("realized_gain_loss") or row.get("realized_gain_loss_") or row.get("realized_gain_loss_d") or row.get("realized_gain_loss_dollars") or row.get("realized_gain_loss"))
                if realized_amt is None:
                    realized_amt = _as_float(row.get("realized_gain_loss_usd"))
                # Fallback: derive realized from amounts when not present.
                if realized_amt is None and opening_amt is not None and closing_amt is not None:
                    realized_amt = float(closing_amt) - float(opening_amt)

                # Build a stable id for idempotency (stored in ib_trade_id field by sync_runner).
                key = "|".join(
                    [
                        "RJ_REALIZED",
                        symbol,
                        str(open_d.isoformat() if open_d else ""),
                        str(close_d.isoformat()),
                        str(abs(float(qty))),
                        str(opening_amt if opening_amt is not None else ""),
                        str(closing_amt if closing_amt is not None else ""),
                        str(realized_amt if realized_amt is not None else ""),
                    ]
                )
                stable_id = "RJ:" + _sha256_bytes(key.encode("utf-8"))[:48]

                out.append(
                    {
                        "record_kind": "BROKER_CLOSED_LOT",
                        "provider_account_id": "RJ:TAXABLE",
                        "symbol": symbol,
                        "date": close_d.isoformat(),
                        "datetime_raw": close_d.isoformat(),
                        "open_datetime_raw": open_d.isoformat() if open_d else None,
                        "qty": abs(float(qty)),
                        "cost_basis": float(opening_amt) if opening_amt is not None else None,
                        "proceeds_derived": float(closing_amt) if closing_amt is not None else None,
                        "realized_pl_fifo": float(realized_amt) if realized_amt is not None else None,
                        "currency": "USD",
                        "fx_rate_to_base": 1.0,
                        "ib_trade_id": stable_id,
                        "source_file_hash": f.file_hash,
                        "source_file": p.name,
                        "source_row": i + 1,
                        "raw_row": r,
                    }
                )
            return out, next_cursor

        if not _looks_like_transactions(text) or _looks_like_holdings(text):
            return [], next_cursor

        _hdr, rows = _read_csv_rows(text)
        out: list[dict[str, Any]] = []
        for i, r in enumerate(rows):
            row = _lower_keys(r)
            d = _parse_date(row.get("date") or row.get("trade_date") or row.get("posted_date") or row.get("transaction_date"))
            if d is None or d < start_date or d > end_date:
                continue

            symbol = str(row.get("symbol") or row.get("symbol_cusip") or row.get("ticker") or row.get("security") or "").strip().upper() or None
            qty = _as_float(row.get("quantity") or row.get("qty") or row.get("shares") or row.get("units"))
            amount = _as_float(
                row.get("amount")
                or row.get("net_amount")
                or row.get("cash_amount")
                or row.get("proceeds")
                or row.get("net")
            )
            desc = str(
                row.get("description")
                or row.get("details")
                or row.get("memo")
                or row.get("security_description")
                or ""
            ).strip()
            additional_detail = str(row.get("additional_detail") or row.get("additionaldetail") or "").strip()
            tx_type = _classify_txn(row)

            provider_account_id = "RJ:TAXABLE"
            # Use both description + additional detail for stable id (prevents collisions when RJ leaves Description blank).
            desc_for_id = " | ".join([x for x in [desc, additional_detail] if x])
            txid = _stable_txid(provider_account_id, d, tx_type, symbol, qty, amount, desc_for_id)
            out.append(
                {
                    "provider_account_id": provider_account_id,
                    "provider_transaction_id": txid,
                    "date": d.isoformat(),
                    "type": tx_type,
                    "ticker": symbol,
                    "qty": float(qty) if qty is not None else None,
                    "amount": float(amount or 0.0),
                    "description": desc or additional_detail,
                    "additional_detail": additional_detail if additional_detail != desc else "",
                    "source_file": p.name,
                    "source_row": i + 1,
                }
            )
        return out, next_cursor

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

        data_dir = self._data_dir(connection)
        holdings_files: list[Path] = []
        if forced is not None:
            holdings_files = [forced]
        else:
            files = _list_files(data_dir)
            for p in files:
                try:
                    txt = p.read_text(encoding="utf-8-sig", errors="ignore")
                except Exception:
                    continue
                if p.suffix.lower() in {".qfx", ".ofx"}:
                    holdings_files.append(p)
                elif _looks_like_holdings(txt):
                    holdings_files.append(p)
            if not holdings_files:
                return {"as_of": (as_of or utcnow()).isoformat(), "items": []}
            holdings_files.sort(key=lambda p: p.stat().st_mtime, reverse=True)

        p = holdings_files[0]
        if p.suffix.lower() in {".qfx", ".ofx"}:
            from src.adapters.rj_offline.qfx_parser import (
                extract_qfx_header_meta,
                parse_positions,
                parse_security_list,
                placeholder_ticker_from_security,
            )

            text = p.read_text(encoding="utf-8-sig", errors="ignore")
            try:
                sec_map = parse_security_list(text)
            except Exception:
                sec_map = {}
            asof_date, pos, meta = parse_positions(text, securities=sec_map)
            try:
                hdr = extract_qfx_header_meta(text)
            except Exception:
                hdr = None

            asof_dt = end_of_day_utc(asof_date) if asof_date else (as_of or utcnow())
            provider_account_id = "RJ:TAXABLE"
            items: list[dict[str, Any]] = []
            total_value = 0.0
            for it in pos:
                if it.qty is None or abs(float(it.qty)) <= 1e-9:
                    continue
                sec = sec_map.get((it.unique_id or "").upper()) if it.unique_id else None
                ticker = placeholder_ticker_from_security(sec, unique_id=it.unique_id)
                mv = float(it.market_value) if it.market_value is not None else None
                if mv is None and it.unit_price is not None and it.qty is not None:
                    mv = float(it.unit_price) * float(it.qty)
                if mv is not None:
                    total_value += float(mv)
                items.append(
                    {
                        "provider_account_id": provider_account_id,
                        "symbol": ticker,
                        "qty": float(it.qty),
                        "market_value": float(mv) if mv is not None else None,
                        "cost_basis_total": float(it.cost_basis) if it.cost_basis is not None else None,
                        "source_file": p.name,
                        "metadata": {"cusip": it.unique_id, "name": it.name},
                    }
                )

            # Add cash balance when present (USD-only in MVP).
            cash_balances: list[dict[str, Any]] = []
            try:
                avail = float(meta.get("avail_cash") or 0.0) if isinstance(meta, dict) else 0.0
            except Exception:
                avail = 0.0
            if abs(avail) > 1e-9:
                cash_balances.append(
                    {
                        "provider_account_id": provider_account_id,
                        "currency": "USD",
                        "amount": float(avail),
                        "as_of_date": asof_dt.date().isoformat(),
                        "source_file": p.name,
                    }
                )
                items.append(
                    {
                        "provider_account_id": provider_account_id,
                        "symbol": "CASH:USD",
                        "qty": float(avail),
                        "market_value": float(avail),
                        "asset_type": "CASH",
                        "source_file": p.name,
                    }
                )
                total_value += float(avail)

            # Total row for valuation points.
            items.append(
                {
                    "provider_account_id": provider_account_id,
                    "symbol": "STATEMENT:TOTAL",
                    "market_value": float(total_value) if total_value else None,
                    "is_total": True,
                    "source_file": p.name,
                }
            )

            out: dict[str, Any] = {
                "as_of": asof_dt.isoformat(),
                "items": items,
                "source_file": p.name,
                "statement_period_start": hdr.dt_start.isoformat() if hdr and hdr.dt_start else None,
                "statement_period_end": hdr.dt_end.isoformat() if hdr and hdr.dt_end else None,
                "statement_total_value": float(total_value) if total_value else None,
                "qfx_meta": {
                    "broker_id": hdr.broker_id if hdr else None,
                    "acct_id": hdr.acct_id if hdr else None,
                    "dt_start": hdr.dt_start.isoformat() if hdr and hdr.dt_start else None,
                    "dt_end": hdr.dt_end.isoformat() if hdr and hdr.dt_end else None,
                    "dt_asof": hdr.dt_asof.isoformat() if hdr and hdr.dt_asof else None,
                },
            }
            if cash_balances:
                out["cash_balances"] = cash_balances
            return out
        file_mtime_asof = utcfromtimestamp(p.stat().st_mtime)
        name_date = date_from_filename(p.name)
        file_name_asof = end_of_day_utc(name_date) if name_date else None
        items: list[dict[str, Any]] = []
        cash_total = 0.0

        if p.suffix.lower() == ".pdf":
            text = _pdf_to_text(p)
            chunks = _split_rj_pdf_text_into_statements(text)
            snapshots: list[dict[str, Any]] = []
            for chunk in chunks:
                period_start, period_end = _extract_statement_period(chunk)
                begin_bal, end_bal = _extract_statement_begin_end_balances(chunk)
                period_asof = end_of_day_utc(period_end) if period_end else None
                text_date = _infer_statement_asof_from_text(chunk)
                text_asof = end_of_day_utc(text_date) if text_date else None
                total = _extract_statement_total_value(chunk)
                # Prefer explicit Ending Balance when available; fall back to total-value heuristics.
                end_total = end_bal if end_bal is not None else total
                if end_total is None and begin_bal is None:
                    continue

                # Emit a baseline point at the statement period start using the beginning balance.
                if begin_bal is not None and period_start is not None:
                    as_of_begin = as_of or end_of_day_utc(period_start)
                    snapshots.append(
                        {
                            "as_of": as_of_begin.isoformat(),
                            "statement_period_start": period_start.isoformat() if period_start else None,
                            "statement_period_end": period_end.isoformat() if period_end else None,
                            "statement_total_value": float(begin_bal),
                            "balance_kind": "BEGIN",
                            "items": [
                                {
                                    "provider_account_id": "RJ:TAXABLE",
                                    "symbol": "STATEMENT:TOTAL",
                                    "market_value": float(begin_bal),
                                    "is_total": True,
                                    "source_file": p.name,
                                }
                            ],
                            "source_file": p.name,
                        }
                    )

                # Emit the statement as-of at the statement period end (or best-effort inferred as-of).
                if end_total is not None:
                    as_of_dt = as_of or period_asof or text_asof or file_name_asof or file_mtime_asof
                    snapshots.append(
                        {
                            "as_of": as_of_dt.isoformat(),
                            "statement_period_start": period_start.isoformat() if period_start else None,
                            "statement_period_end": period_end.isoformat() if period_end else None,
                            "statement_total_value": float(end_total),
                            "balance_kind": "END",
                            "items": [
                                {
                                    "provider_account_id": "RJ:TAXABLE",
                                    "symbol": "STATEMENT:TOTAL",
                                    "market_value": float(end_total),
                                    "is_total": True,
                                    "source_file": p.name,
                                }
                            ],
                            "source_file": p.name,
                        }
                    )

            if not snapshots:
                raise ProviderError(
                    f"RJ PDF statement {p.name} was read but no statement total values could be extracted; "
                    "export a CSV holdings/positions file for best results."
                )

            # If multiple statements are present in one PDF, return them all so the sync runner can create
            # multiple valuation points (monthly performance).
            if len(snapshots) > 1:
                # Deduplicate by as-of date (keep largest total for that date).
                by_day: dict[str, dict[str, Any]] = {}
                for s in snapshots:
                    d = str(s.get("as_of") or "")
                    prev = by_day.get(d)
                    if prev is None:
                        by_day[d] = s
                        continue
                    try:
                        if float(s.get("statement_total_value") or 0.0) > float(prev.get("statement_total_value") or 0.0):
                            by_day[d] = s
                    except Exception:
                        continue
                snaps = list(by_day.values())
                snaps.sort(key=lambda x: str(x.get("as_of") or ""))
                return {"snapshots": snaps, "source_file": p.name}

            return snapshots[0]
        try:
            text = p.read_text(encoding="utf-8-sig")
            _hdr, rows = _read_csv_rows(text)
            for r in rows:
                row = _lower_keys(r)
                sym_raw = str(
                    row.get("symbol")
                    or row.get("symbol_cusip")
                    or row.get("ticker")
                    or row.get("security")
                    or ""
                ).strip()
                sym = sym_raw.upper() if sym_raw else ""
                if not sym:
                    # Some RJ positions rows are cash-like and have no symbol.
                    sym = ""
                qty = _as_float(row.get("quantity") or row.get("qty") or row.get("shares") or row.get("units"))
                mv = _as_float(
                    row.get("current_value")
                    or row.get("market_value")
                    or row.get("marketvalue")
                    or row.get("value")
                    or row.get("market_value_usd")
                )
                cb = _as_float(
                    row.get("amount_invested")
                    or row.get("amount_invested_unit")
                    or row.get("amount_invested_unit")
                    or row.get("cost_basis")
                    or row.get("cost_basis_total")
                    or row.get("cost")
                    or row.get("basis")
                )
                desc_u = str(row.get("description") or "").strip().upper()
                product_u = str(row.get("product_type") or "").strip().upper()

                # Cash-like handling:
                # - Many RJ "Bank Deposit Program" rows have no symbol and should be treated as cash.
                # - Product Type often contains "Cash & Cash Alternatives".
                is_cash_like = (
                    (not sym and ("CASH" in product_u or "DEPOSIT" in desc_u))
                    or sym in {"CASH", "CASH_BALANCE", "CASH_AND_CASH_INVESTMENTS"}
                    or sym.startswith("CASH")
                    or ("CASH" in product_u)
                )
                if is_cash_like:
                    cash_total += float(mv or 0.0)
                    continue
                if qty is None or not sym:
                    continue
                items.append(
                    {
                        "provider_account_id": "RJ:TAXABLE",
                        "symbol": sym,
                        "qty": float(qty),
                        "market_value": float(mv) if mv is not None else None,
                        "cost_basis_total": float(cb) if cb is not None else None,
                        "source_file": p.name,
                    }
                )
        except Exception as e:
            raise ProviderError(f"Failed to parse RJ holdings file {p.name}: {type(e).__name__}: {e}")

        as_of_dt = as_of or file_name_asof or file_mtime_asof
        cash_balances: list[dict[str, Any]] = []
        if abs(float(cash_total)) > 1e-9:
            cash_balances.append(
                {
                    "provider_account_id": "RJ:TAXABLE",
                    "currency": "USD",
                    "amount": float(cash_total),
                    "as_of_date": as_of_dt.date().isoformat(),
                    "source_file": p.name,
                }
            )
            items.append(
                {
                    "provider_account_id": "RJ:TAXABLE",
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
