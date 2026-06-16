from __future__ import annotations

import json

import pandas as pd

from src.regime import portfolio_campaign3 as campaign3


def _frame(start: str, prices: list[float]) -> pd.DataFrame:
    dates = pd.bdate_range(start, periods=len(prices))
    return pd.DataFrame({"open": prices, "high": prices, "low": prices, "price": prices, "volume": 1_000_000}, index=dates)


def test_campaign3_runs_l1_sweep_without_production_default_changes(tmp_path) -> None:
    basket_path = tmp_path / "basket.json"
    basket_path.write_text(
        json.dumps(
            {
                "tickers": ["AAA", "BBB"],
                "basket_size": 2,
                "selected": [
                    {"ticker": "AAA", "sector": "Technology"},
                    {"ticker": "BBB", "sector": "Industrials"},
                ],
            }
        ),
        encoding="utf-8",
    )
    frames = {
        "AAA": _frame("2006-01-02", [10 + index * 0.03 for index in range(260)]),
        "BBB": _frame("2006-02-01", [20 + index * 0.02 for index in range(238)]),
        "SPY": _frame("2006-01-02", [100 + index * 0.04 for index in range(260)]),
    }

    summary = campaign3.run_campaign3(
        basket_path=basket_path,
        campaign_dir=tmp_path / "campaign",
        report_dir=tmp_path / "report",
        start="2006-01-01",
        end="2006-12-31",
        frame_loader=lambda ticker, start, end: frames[str(ticker).upper()],
        regime_enricher=lambda payload: payload,
    )

    assert summary["schema"] == "regime_portfolio_campaign3.v1"
    assert summary["production_defaults_changed"] is False
    assert summary["availability_mode"] == "panel"
    arms = {row["arm"] for row in summary["rows"]}
    assert {"L0", "L1", "L1_spy200", "L2", "L3", "C1_spy_buy_hold", "C2_spy_200dma"} <= arms
    assert summary["verdict"]["recommended_production_default_changes"] == []
    assert (tmp_path / "campaign" / "summary.json").exists()
    report_path = tmp_path / "report" / "management_report.html"
    assert report_path.exists()
    report_html = report_path.read_text(encoding="utf-8")
    assert "Candidate Ranking" in report_html
    assert "Strategy Used By Arm" in report_html
    assert "Performance By Year" in report_html
    assert (tmp_path / "report" / "assets" / "campaign3_total_return.png").exists()


def test_market_timing_signal_is_copied_to_basket_frames() -> None:
    frames = {"AAA": _frame("2006-01-02", [10.0] * 230)}
    spy = _frame("2006-01-02", [100.0] * 205 + [120.0] * 25)

    enriched = campaign3.with_market_timing_signal(frames, spy)

    assert "market_timing_confirmed" in enriched["AAA"]
    assert enriched["AAA"]["market_timing_confirmed"].any()
