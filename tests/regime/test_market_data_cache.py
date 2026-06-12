from __future__ import annotations

import pandas as pd

from src.regime import data


def _history(start, end) -> pd.DataFrame:
    dates = pd.bdate_range(pd.Timestamp(start), pd.Timestamp(end))
    return pd.DataFrame(
        {
            "Open": [10.0] * len(dates),
            "High": [11.0] * len(dates),
            "Low": [9.0] * len(dates),
            "Close": [10.0] * len(dates),
            "Volume": [2_000_000.0] * len(dates),
        },
        index=dates,
    )


def test_download_market_frame_uses_cache_for_same_date_range(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("HMM_DATA_DIR", str(tmp_path))
    calls: list[tuple[object, object]] = []

    def fake_price_history(ticker, period, interval, *, start=None, end=None):
        del ticker, period, interval
        calls.append((start, end))
        return "TEST", _history(start, end)

    monkeypatch.setattr(data, "_download_price_history", fake_price_history)
    monkeypatch.setattr(
        data,
        "_download_macro_inputs",
        lambda index, period, interval, start=None, end=None: pd.DataFrame({"vix": [20.0] * len(index), "yield_10y": [4.0] * len(index)}, index=index),
    )

    first = data.download_market_frame("TEST", start="2024-01-02", end="2024-01-12", cache=True).frame
    second = data.download_market_frame("TEST", start="2024-01-02", end="2024-01-12", cache=True).frame

    assert len(calls) == 1
    assert len(first) == len(second)
    assert (tmp_path / "price_cache" / "TEST_1d.csv").exists()


def test_download_market_frame_refreshes_missing_trailing_dates(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("HMM_DATA_DIR", str(tmp_path))
    calls: list[tuple[object, object]] = []

    def fake_price_history(ticker, period, interval, *, start=None, end=None):
        del ticker, period, interval
        calls.append((start, end))
        return "TEST", _history(start, end)

    monkeypatch.setattr(data, "_download_price_history", fake_price_history)
    monkeypatch.setattr(
        data,
        "_download_macro_inputs",
        lambda index, period, interval, start=None, end=None: pd.DataFrame({"vix": [20.0] * len(index), "yield_10y": [4.0] * len(index)}, index=index),
    )

    data.download_market_frame("TEST", start="2024-01-02", end="2024-01-05", cache=True)
    expanded = data.download_market_frame("TEST", start="2024-01-02", end="2024-01-12", cache=True).frame

    assert len(calls) == 2
    assert pd.Timestamp(calls[-1][0]) > pd.Timestamp("2024-01-05")
    assert expanded.index.max() >= pd.Timestamp("2024-01-12")
