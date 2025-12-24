from __future__ import annotations

import datetime as dt
import os
from functools import lru_cache
from typing import Any
from zoneinfo import ZoneInfo

UTC = dt.timezone.utc


def utcnow() -> dt.datetime:
    return dt.datetime.now(UTC)


def utcfromtimestamp(ts: float) -> dt.datetime:
    return dt.datetime.fromtimestamp(ts, UTC)


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
