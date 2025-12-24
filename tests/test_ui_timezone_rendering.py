from __future__ import annotations

import datetime as dt

from src.utils.time import UTC, format_local, format_local_date, to_local


def test_format_local_converts_utc_to_newyork():
    # 2025-01-01 05:00 UTC == 2025-01-01 00:00 in America/New_York (EST)
    d = dt.datetime(2025, 1, 1, 5, 0, 0, tzinfo=UTC)
    s = format_local(d, tz_name="America/New_York")
    assert "2025-01-01 00:00:00" in s
    assert s.endswith("EST")


def test_local_date_uses_local_calendar_day():
    # 2025-01-01 04:30 UTC == 2024-12-31 23:30 in America/New_York (EST)
    d = dt.datetime(2025, 1, 1, 4, 30, 0, tzinfo=UTC)
    assert format_local_date(d, tz_name="America/New_York") == "2024-12-31"


def test_to_local_returns_tzaware():
    d = dt.datetime(2025, 1, 1, 5, 0, 0, tzinfo=UTC)
    out = to_local(d, tz_name="America/New_York")
    assert out is not None
    assert out.tzinfo is not None

