from __future__ import annotations

import datetime as dt
import os
import re
from functools import lru_cache
from typing import Any
from zoneinfo import ZoneInfo

UTC = dt.timezone.utc


def utcnow() -> dt.datetime:
    return dt.datetime.now(UTC)


def utcfromtimestamp(ts: float) -> dt.datetime:
    return dt.datetime.fromtimestamp(ts, UTC)


def end_of_day_utc(d: dt.date) -> dt.datetime:
    # Use end-of-day UTC so the local UI date doesn't shift backward (e.g., 00:00Z -> prior day in US timezones).
    return dt.datetime(d.year, d.month, d.day, 23, 59, 59, tzinfo=UTC)


def date_from_filename(name: str) -> dt.date | None:
    """
    Best-effort date inference from a filename.

    Supports patterns like:
      - YYYY-MM-DD, YYYY_MM_DD, YYYY.MM.DD
      - MM-DD-YYYY, MM_DD_YYYY, MM.DD.YYYY
      - YYYYMMDD
    """
    s = str(name or "").strip()
    if not s:
        return None

    patterns: list[tuple[re.Pattern[str], str]] = [
        (re.compile(r"(?<!\d)(?P<y>20\d{2})[-_.](?P<m>\d{1,2})[-_.](?P<d>\d{1,2})(?!\d)"), "ymd"),
        (re.compile(r"(?<!\d)(?P<m>\d{1,2})[-_.](?P<d>\d{1,2})[-_.](?P<y>20\d{2})(?!\d)"), "mdy"),
        (re.compile(r"(?<!\d)(?P<y>20\d{2})(?P<m>\d{2})(?P<d>\d{2})(?!\d)"), "compact"),
    ]

    best: dt.date | None = None
    best_pos = -1
    for pat, _kind in patterns:
        for m in pat.finditer(s):
            try:
                y = int(m.group("y"))
                mo = int(m.group("m"))
                da = int(m.group("d"))
                d = dt.date(y, mo, da)
            except Exception:
                continue
            if m.start() >= best_pos:
                best_pos = m.start()
                best = d
    return best


@lru_cache(maxsize=32)
def _zone(name: str) -> ZoneInfo:
    return ZoneInfo(name)


def ui_timezone_name() -> str:
    return os.environ.get("UI_TIMEZONE", "America/New_York").strip() or "America/New_York"


def ensure_utc(value: dt.datetime) -> dt.datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def parse_datetime(value: Any) -> dt.datetime | None:
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        return value
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        # Support "Z" suffix.
        s = s.replace("Z", "+00:00")
        try:
            return dt.datetime.fromisoformat(s)
        except Exception:
            return None
    return None


def to_local(value: Any, *, tz_name: str | None = None) -> dt.datetime | None:
    d = parse_datetime(value)
    if d is None:
        return None
    tz = _zone(tz_name or ui_timezone_name())
    return ensure_utc(d).astimezone(tz)


def format_local(value: Any, fmt: str = "%Y-%m-%d %H:%M:%S %Z", tz_name: str | None = None) -> str:
    if value is None:
        return "—"
    if isinstance(value, dt.date) and not isinstance(value, dt.datetime):
        return value.isoformat()
    d = to_local(value, tz_name=tz_name)
    if d is None:
        return str(value)
    return d.strftime(fmt)


def format_local_date(value: Any, tz_name: str | None = None) -> str:
    if value is None:
        return "—"
    if isinstance(value, dt.date) and not isinstance(value, dt.datetime):
        return value.isoformat()
    d = to_local(value, tz_name=tz_name)
    if d is None:
        return str(value)
    return d.date().isoformat()
