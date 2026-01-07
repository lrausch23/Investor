from __future__ import annotations

import datetime as dt


def test_chase_pdf_text_parses_period_and_total_value():
    from src.adapters.chase_offline.adapter import _extract_statement_period, _extract_statement_total_value

    text = """
    Performance Summary
    From Jan 1, 2025 to Jan 31, 2025
    Ending Market Value $702,740.73
    """
    start_d, end_d = _extract_statement_period(text)
    assert start_d == dt.date(2025, 1, 1)
    assert end_d == dt.date(2025, 1, 31)
    total = _extract_statement_total_value(text)
    assert float(total or 0.0) == 702740.73

