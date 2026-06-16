from __future__ import annotations

import json

import pandas as pd

from src.regime import portfolio_historical_campaign as historical


def _frame(start: str, prices: list[float]) -> pd.DataFrame:
    dates = pd.bdate_range(start, periods=len(prices))
    return pd.DataFrame({"open": prices, "high": prices, "low": prices, "price": prices, "volume": 1_000_000}, index=dates)


def test_historical_campaign_runs_with_availability_panel_and_report(tmp_path) -> None:
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
        "AAA": _frame("1996-01-02", [10 + index * 0.05 for index in range(260)]),
        "BBB": _frame("1996-03-01", [20 + index * 0.03 for index in range(220)]),
        "SPY": _frame("1996-01-02", [100 + index * 0.04 for index in range(260)]),
    }

    summary = historical.run_historical_campaign(
        basket_path=basket_path,
        campaign_dir=tmp_path / "campaign",
        report_dir=tmp_path / "report",
        start="1996-01-01",
        end="1996-12-31",
        include_campaign1_baseline=False,
        frame_loader=lambda ticker, start, end: frames[str(ticker).upper()],
        regime_enricher=lambda payload: payload,
    )

    assert summary["schema"] == "regime_portfolio_historical_campaign.v1"
    assert summary["availability_mode"] == "panel"
    assert summary["production_defaults_changed"] is False
    assert {row["arm"] for row in summary["rows"]} >= {"L0", "L1", "L2", "L3", "C1_spy_buy_hold", "C2_spy_200dma"}
    coverage = {row["ticker"]: row for row in summary["coverage"]["tickers"]}
    assert coverage["AAA"]["starts_after_target"] is False
    assert coverage["BBB"]["starts_after_target"] is True
    assert (tmp_path / "campaign" / "summary.json").exists()
    report_path = tmp_path / "report" / "management_report.html"
    assert report_path.exists()
    report_html = report_path.read_text(encoding="utf-8")
    assert "Strategy Used By Arm" in report_html
    assert "Volatility-target overlay" in report_html
    assert "SPY 200dma" in report_html
    assert "Performance By Year" in report_html
    assert (tmp_path / "report" / "assets" / "historical_total_return.png").exists()
    assert (tmp_path / "report" / "assets" / "historical_yearly_returns.png").exists()
