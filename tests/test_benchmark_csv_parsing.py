from __future__ import annotations

import datetime as dt
from pathlib import Path

from src.core.performance import load_price_series


def test_load_price_series_prefers_adj_close(tmp_path: Path):
    p = tmp_path / "voo.csv"
    p.write_text(
        "\n".join(
            [
                "Date,Close,Adj. Close",
                "2025-07-01,567.77,564.568",
                "2025-07-02,568.00,565.000",
            ]
        ),
        encoding="utf-8",
    )
    series = load_price_series(p)
    assert series == [(dt.date(2025, 7, 1), 564.568), (dt.date(2025, 7, 2), 565.0)]

