from __future__ import annotations

import datetime as dt

import pytest

from src.app.routes.sync import _parse_form_date


def test_parse_form_date_accepts_iso_and_us_formats():
    assert _parse_form_date("2024-12-31") == dt.date(2024, 12, 31)
    assert _parse_form_date("12/31/2024") == dt.date(2024, 12, 31)
    assert _parse_form_date("12-31-2024") == dt.date(2024, 12, 31)


def test_parse_form_date_blank_is_none():
    assert _parse_form_date("") is None
    assert _parse_form_date("   ") is None


def test_parse_form_date_invalid_raises():
    with pytest.raises(ValueError):
        _parse_form_date("31/12/2024")

