from __future__ import annotations

import datetime as dt
import re
from pathlib import Path
from typing import Any

from src.adapters.chase_offline.adapter import _extract_statement_period, _pdf_to_text
from src.importers.adapters import ProviderError


_MONEY_RE = re.compile(r"(?P<amt>\(?\$?\d{1,3}(?:,\d{3})*(?:\.\d{2})?\)?)")
_DATE_SLASH_RE = re.compile(r"\b(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})\b")
_DATE_WORD_RE = re.compile(r"\b([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})\b")
_PAY_OVER_TIME_ROW_RE = re.compile(
    r"^(?P<desc>.+?)\s+"
    r"(?P<date>\d{1,2}/\d{1,2}/\d{2,4})\s+"
    r"(?P<orig>\$?[\d,]+\.\d{2})\s+"
    r"(?P<total_payments>\d+)\s+"
    r"(?P<remaining_principal>\$?[\d,]+\.\d{2})\s+"
    r"(?P<remaining_payments>\d+)\s+"
    r"(?P<monthly_principal>\$?[\d,]+\.\d{2})\s+"
    r"(?P<monthly_fee>\$?[\d,]+\.\d{2})\s+"
    r"(?P<payment_due>\$?[\d,]+\.\d{2})"
)


def _parse_money(value: str) -> float | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    neg = raw.startswith("(") and raw.endswith(")")
    cleaned = raw.replace("$", "").replace(",", "").strip("() ")
    if not cleaned:
        return None
    try:
        amt = float(cleaned)
    except Exception:
        return None
    return -amt if neg else amt


def _parse_date(value: str) -> dt.date | None:
    s = str(value or "").strip()
    if not s:
        return None
    for fmt in ("%m/%d/%Y", "%m/%d/%y", "%b %d, %Y", "%B %d, %Y", "%b %d %Y", "%B %d %Y"):
        try:
            return dt.datetime.strptime(s, fmt).date()
        except Exception:
            continue
    return None


def _extract_amount_after_label(lines: list[str], labels: list[str]) -> float | None:
    for idx, line in enumerate(lines):
        u = line.upper()
        if not any(label in u for label in labels):
            continue
        candidates: list[float] = []
        candidates_with_dollar: list[float] = []
        for j in range(idx, min(idx + 5, len(lines))):
            line_j = lines[j]
            for m in _MONEY_RE.finditer(line_j):
                amt_raw = m.group("amt")
                parsed = _parse_money(amt_raw)
                if parsed is None:
                    continue
                candidates.append(parsed)
                if "$" in amt_raw:
                    candidates_with_dollar.append(parsed)
        if candidates_with_dollar:
            return max(candidates_with_dollar)
        if candidates:
            return max(candidates)
    return None


def _extract_date_after_label(lines: list[str], labels: list[str]) -> dt.date | None:
    for idx, line in enumerate(lines):
        u = line.upper()
        if not any(label in u for label in labels):
            continue
        for j in range(idx, min(idx + 4, len(lines))):
            for regex in (_DATE_WORD_RE, _DATE_SLASH_RE):
                m = regex.search(lines[j])
                if m:
                    parsed = _parse_date(m.group(1))
                    if parsed is not None:
                        return parsed
    return None


def _last4_from_filename(name: str) -> str | None:
    if not name:
        return None
    hits = re.findall(r"(?<!\d)(\d{4})(?!\d)", name)
    for h in reversed(hits):
        if h.startswith("20"):
            continue
        return h
    return None


def _extract_last4(text: str, filename: str) -> str | None:
    patterns = [
        r"(?i)(?:account|card)\s*(?:number|no\.?|#)?[^0-9]{0,8}(?:[x*•]{2,}|\d{4}\s*)*(\d{4})",
        r"(?i)(?:\*{2,}|x{2,}|•{2,})\s*(\d{4})",
    ]
    for pat in patterns:
        hits = re.findall(pat, text or "")
        if hits:
            return hits[-1]
    return _last4_from_filename(filename)


def _extract_pay_over_time(lines: list[str]) -> dict[str, Any] | None:
    start = None
    for idx, line in enumerate(lines):
        u = line.upper()
        if "PAY OVER TIME" in u and "PLANS SET UP AFTER PURCHASE" in u:
            start = idx
            break
    if start is None:
        return None

    end = None
    for idx in range(start + 1, len(lines)):
        if "PAYMENT DUE FOR PLANS SET UP AFTER PURCHASE" in lines[idx].upper():
            end = idx
            break
    if end is None:
        end = min(len(lines), start + 80)

    rows: list[dict[str, Any]] = []
    totals: dict[str, float] | None = None
    for line in lines[start + 1 : end]:
        s = line.strip()
        if not s:
            continue
        if "PLAN TOTALS" in s.upper():
            amounts: list[float] = []
            for m in _MONEY_RE.finditer(s):
                parsed = _parse_money(m.group("amt"))
                if parsed is not None:
                    amounts.append(parsed)
            if len(amounts) >= 5:
                totals = {
                    "original_principal": amounts[0],
                    "remaining_principal": amounts[1],
                    "monthly_principal": amounts[2],
                    "monthly_fee": amounts[3],
                    "payment_due": amounts[4],
                }
            continue
        m = _PAY_OVER_TIME_ROW_RE.match(s)
        if not m:
            continue
        start_date = _parse_date(m.group("date"))
        rows.append(
            {
                "description": m.group("desc").strip(),
                "plan_start_date": start_date.isoformat() if start_date else None,
                "original_principal": _parse_money(m.group("orig")),
                "total_payments": int(m.group("total_payments")),
                "remaining_principal": _parse_money(m.group("remaining_principal")),
                "remaining_payments": int(m.group("remaining_payments")),
                "monthly_principal": _parse_money(m.group("monthly_principal")),
                "monthly_fee": _parse_money(m.group("monthly_fee")),
                "payment_due": _parse_money(m.group("payment_due")),
            }
        )

    payment_due_total = None
    if end is not None and end < len(lines):
        for m in _MONEY_RE.finditer(lines[end]):
            parsed = _parse_money(m.group("amt"))
            if parsed is not None:
                payment_due_total = parsed
    elif end is None:
        for idx in range(start + 1, min(len(lines), start + 160)):
            if "PAYMENT DUE FOR PLANS SET UP AFTER PURCHASE" not in lines[idx].upper():
                continue
            for m in _MONEY_RE.finditer(lines[idx]):
                parsed = _parse_money(m.group("amt"))
                if parsed is not None:
                    payment_due_total = parsed
            break

    if not rows and totals is None and payment_due_total is None:
        return None

    return {
        "rows": rows,
        "totals": totals,
        "payment_due_total": payment_due_total,
    }


def parse_chase_card_statement_pdf(path: Path) -> dict[str, Any]:
    """
    Parse Chase credit card statement PDFs for interest-free balance and related fields.
    """
    text = _pdf_to_text(path)
    if not text:
        raise ProviderError(f"Chase statement '{path.name}' returned empty text from pdftotext.")
    lines = [ln.strip() for ln in (text or "").splitlines() if ln.strip()]

    last4 = _extract_last4(text, path.name)
    if not last4:
        raise ProviderError(f"Chase statement '{path.name}' is missing a masked account number (last-4).")

    interest_saving_balance = _extract_amount_after_label(
        lines,
        ["INTEREST SAVING BALANCE", "INTEREST-SAVING BALANCE"],
    )
    if interest_saving_balance is None:
        raise ProviderError(f"Chase statement '{path.name}' is missing an Interest-free balance value.")

    statement_balance = _extract_amount_after_label(lines, ["STATEMENT BALANCE", "NEW BALANCE"])
    minimum_payment_due = _extract_amount_after_label(lines, ["MINIMUM PAYMENT DUE"])
    payment_due_date = _extract_date_after_label(lines, ["PAYMENT DUE DATE", "PAYMENT IS DUE ON", "DUE DATE"])
    period_start, period_end = _extract_statement_period(text)
    pay_over_time = _extract_pay_over_time(lines)

    return {
        "last4": last4,
        "statement_period_start": period_start,
        "statement_period_end": period_end,
        "payment_due_date": payment_due_date,
        "statement_balance": statement_balance,
        "interest_saving_balance": interest_saving_balance,
        "minimum_payment_due": minimum_payment_due,
        "pay_over_time": pay_over_time,
        "source_file": path.name,
    }
