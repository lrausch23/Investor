from __future__ import annotations

import datetime as dt
from types import SimpleNamespace

import pytest


def test_ib_mtm_summary_statement_parses_nav_total(tmp_path):
    from src.adapters.ib_flex_web.adapter import IBFlexWebAdapter

    text = (
        "Statement\tData\tTitle\tMTM Summary\n"
        "Statement\tData\tPeriod\tDecember 1, 2024 - December 31, 2024\n"
        "Account Information\tData\tAccount\tU5891158\n"
        "Net Asset Value\tHeader\tAsset Class\tPrior Total\tCurrent Long\tCurrent Short\tCurrent Total\tChange\n"
        "Net Asset Value\tData\tCash\t0\t0\t0\t202346.598971019\t0\n"
        "Net Asset Value\tData\tTotal\t0\t0\t0\t462796.398971019\t0\n"
    )
    p = tmp_path / "ib_mtm_summary.tsv"
    p.write_text(text, encoding="utf-8")

    adapter = IBFlexWebAdapter()
    conn = SimpleNamespace(run_settings={"holdings_file_path": str(p)})
    out = adapter.fetch_holdings(conn, as_of=None)

    assert str(out.get("as_of") or "").startswith("2024-12-31T23:59:59")
    items = list(out.get("items") or [])
    assert any(bool(it.get("is_total")) and float(it.get("market_value") or 0.0) == pytest.approx(462796.398971019) for it in items)
    assert out.get("statement_period_end") == dt.date(2024, 12, 31).isoformat()

