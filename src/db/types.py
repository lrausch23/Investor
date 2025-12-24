from __future__ import annotations

import datetime as dt

from sqlalchemy.types import DateTime as _DateTime
from sqlalchemy.types import TypeDecorator

from src.utils.time import UTC


class UTCDateTime(TypeDecorator):
    """
    Store datetimes as UTC and always return tz-aware UTC datetimes.

    SQLite doesn't have a native timezone-aware datetime type. This decorator treats
    naive datetimes as UTC and attaches tzinfo on read.
    """

    impl = _DateTime
    cache_ok = True

    def process_bind_param(self, value: dt.datetime | None, dialect):
        if value is None:
            return None
        v = value
        if v.tzinfo is None:
            v = v.replace(tzinfo=UTC)
        v = v.astimezone(UTC)
        # Store as naive UTC for broad DB compatibility.
        return v.replace(tzinfo=None)

    def process_result_value(self, value: dt.datetime | None, dialect):
        if value is None:
            return None
        v = value
        if v.tzinfo is None:
            return v.replace(tzinfo=UTC)
        return v.astimezone(UTC)

