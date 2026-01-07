from __future__ import annotations

import datetime as dt

from src.adapters.rj_offline.adapter import (
    _extract_statement_begin_end_balances,
    _extract_statement_period,
    _extract_statement_total_value,
    _parse_date,
    _split_rj_pdf_text_into_statements,
)


def test_rj_parse_date_supports_dot_format():
    assert _parse_date("12.31.2025") == dt.date(2025, 12, 31)
    assert _parse_date("12.31.25") == dt.date(2025, 12, 31)


def test_rj_extract_statement_period_dot_format():
    text = "Statement Period: 12.01.2025 - 12.31.2025"
    start_d, end_d = _extract_statement_period(text)
    assert start_d == dt.date(2025, 12, 1)
    assert end_d == dt.date(2025, 12, 31)


def test_rj_extract_statement_total_value_prefers_large_total():
    text = "\n".join(
        [
            "Some Header",
            "TOTAL ACCOUNT VALUE",
            "($6.54) Change",
            "$961,441.23",
            "Other Lines",
        ]
    )
    assert _extract_statement_total_value(text) == 961_441.23


def test_rj_extract_statement_period_month_name_format():
    text = "December 31, 2024 to January 31, 2025"
    start_d, end_d = _extract_statement_period(text)
    assert start_d == dt.date(2024, 12, 31)
    assert end_d == dt.date(2025, 1, 31)

def test_rj_extract_statement_period_month_name_start_missing_year():
    text = "January 31 to February 28, 2025"
    start_d, end_d = _extract_statement_period(text)
    assert start_d == dt.date(2025, 1, 31)
    assert end_d == dt.date(2025, 2, 28)


def test_rj_extract_begin_end_balances_from_summary():
    text = "\n".join(
        [
            "Beginning Balance                         $896,580.21",
            "Ending Balance                            $930,723.69",
        ]
    )
    begin_v, end_v = _extract_statement_begin_end_balances(text)
    assert begin_v == 896_580.21
    assert end_v == 930_723.69


def test_rj_split_pdf_text_into_statements_uses_page1_markers():
    # Simulate 2 statements, each with 2 pages.
    p1 = "December 31, 2024 to January 31, 2025\nPage 1 of 6\nEnding Balance $1,000.00"
    p2 = "December 31, 2024 to January 31, 2025\nPage 2 of 6"
    p3 = "January 31, 2025 to February 28, 2025\nPage 1 of 6\nEnding Balance $2,000.00"
    p4 = "January 31, 2025 to February 28, 2025\nPage 2 of 6"
    text = "\f".join([p1, p2, p3, p4])
    chunks = _split_rj_pdf_text_into_statements(text)
    assert len(chunks) == 2
    assert "Ending Balance $1,000.00" in chunks[0]
    assert "Ending Balance $2,000.00" in chunks[1]
