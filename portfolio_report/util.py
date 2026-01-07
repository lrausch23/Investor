from __future__ import annotations

import csv
import datetime as dt
import re
from typing import Any, Iterable


_MONEY_RE = re.compile(r"[-+]?\d[\d,]*\.?\d*")


def sniff_delimiter(text: str) -> str:
    sample = "\n".join((text or "").splitlines()[:30])
    if not sample:
        return ","
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",\t;")
        return getattr(dialect, "delimiter", ",") or ","
    except Exception:
        return ","


def parse_date(value: Any) -> dt.date | None:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    # Common ISO-like.
    try:
        return dt.date.fromisoformat(s[:10])
    except Exception:
        pass
    # Common broker formats.
    for fmt in ("%m/%d/%Y", "%m/%d/%y", "%d-%b-%y", "%d-%b-%Y", "%d/%m/%Y", "%Y/%m/%d"):
        try:
            return dt.datetime.strptime(s.split()[0], fmt).date()
        except Exception:
            continue
    return None


def parse_money(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        try:
            return float(value)
        except Exception:
            return None
    s = str(value).strip()
    if not s:
        return None
    neg = False
    # Formats like "(123.45)".
    if s.startswith("(") and s.endswith(")"):
        neg = True
        s = s[1:-1]
    # Formats like "Loss of -$18,000.00".
    m = _MONEY_RE.search(s.replace("$", "").replace("*", "").replace(" ", ""))
    if not m:
        return None
    try:
        x = float(m.group(0).replace(",", ""))
    except Exception:
        return None
    if "-" in s and not neg:
        # When embedded "-" exists (e.g., "Loss of -$18,000.00"), treat as negative.
        if re.search(r"[-]\$?\d", s):
            neg = True
    return -x if neg else x


def safe_div(n: float, d: float) -> float | None:
    if d == 0:
        return None
    return n / d


def last_day_of_month(d: dt.date) -> dt.date:
    # Move to first day of next month and subtract a day.
    if d.month == 12:
        return dt.date(d.year, 12, 31)
    first_next = dt.date(d.year, d.month + 1, 1)
    return first_next - dt.timedelta(days=1)


def month_ends(start: dt.date, end: dt.date) -> list[dt.date]:
    out: list[dt.date] = []
    cur = dt.date(start.year, start.month, 1)
    while cur <= end:
        me = last_day_of_month(cur)
        if start <= me <= end:
            out.append(me)
        # advance 1 month
        if cur.month == 12:
            cur = dt.date(cur.year + 1, 1, 1)
        else:
            cur = dt.date(cur.year, cur.month + 1, 1)
    return out


def uniq_sorted(xs: Iterable[str]) -> list[str]:
    return sorted({str(x).strip() for x in xs if str(x).strip()})

