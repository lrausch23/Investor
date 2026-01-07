from __future__ import annotations

import datetime as dt
from types import SimpleNamespace


def test_chase_performance_csv_parses_multiple_snapshots(tmp_path):
    from src.adapters.chase_offline.adapter import ChaseOfflineAdapter

    text = "\n".join(
        [
            "Date\tBeginning market value\tContributions\tWithdrawals\tWealth generated\tIncome\tChange in investment value\tEnding market value",
            "Jan 1, 2025 -\t$710,577.98\t$0.00\t$0.00\t$0.00\t$0.00\t$0.00\t$710,577.98",
            "From Jan 1, 2025 to Jan 1, 2025\t\t\t\t\t\t\t",
            "Jan 1, 2025 -\t$692,577.98\t$0.00\t($18,000.00)\t$10,162.75\t$13,056.51\t($2,893.76)\t$702,740.73",
            "From Jan 1, 2025 to Jan 31, 2025\t\t\t\t\t\t\t",
            "Jan 1, 2025 -\t$485,077.98\t$0.00\t($225,500.00)\t$79,092.70\t$40,900.35\t$38,192.35\t$564,170.68",
            "From Jan 1, 2025 to Dec 31, 2025\t\t\t\t\t\t\t",
        ]
    )
    p = tmp_path / "chase_performance_2025.tsv"
    p.write_text(text, encoding="utf-8")

    adapter = ChaseOfflineAdapter()
    conn = SimpleNamespace(run_settings={"holdings_file_path": str(p)}, metadata_json={"data_dir": str(tmp_path)})
    out = adapter.fetch_holdings(conn, as_of=None)
    snaps = list(out.get("snapshots") or [])
    assert len(snaps) == 3

    # First point: Jan 1, 2025
    assert snaps[0]["statement_period_end"] == dt.date(2025, 1, 1).isoformat()
    assert str(snaps[0]["as_of"]).startswith("2025-01-01T23:59:59")
    items0 = list(snaps[0].get("items") or [])
    assert any(bool(it.get("is_total")) and float(it.get("market_value") or 0.0) == 710577.98 for it in items0)

    # Last point: Dec 31, 2025
    assert snaps[-1]["statement_period_end"] == dt.date(2025, 12, 31).isoformat()
    assert str(snaps[-1]["as_of"]).startswith("2025-12-31T23:59:59")
    itemsN = list(snaps[-1].get("items") or [])
    assert any(bool(it.get("is_total")) and float(it.get("market_value") or 0.0) == 564170.68 for it in itemsN)


