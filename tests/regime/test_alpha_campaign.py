from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from src.regime.alpha_campaign import (
    aggregate_result_payloads,
    evidence_floor,
    render_report,
    robustness_verdict,
    run_phase0,
    select_basket,
)
from src.regime.pipeline_backtest import PipelineBacktestResult


def _frame(*, price: float = 20.0, volume: float = 1_000_000.0, days: int = 2600) -> pd.DataFrame:
    index = pd.bdate_range("2015-01-01", periods=days)
    return pd.DataFrame(
        {
            "price": [price] * days,
            "open": [price] * days,
            "high": [price * 1.01] * days,
            "low": [price * 0.99] * days,
            "volume": [volume] * days,
        },
        index=index,
    )


def _payload(ticker: str, *, ret: float, sharpe: float, drawdown: float, trades: int) -> dict:
    return {
        "ticker": ticker,
        "metrics": {"total_return": ret, "trade_count": trades},
        "out_of_sample": {
            "total_return": ret,
            "sharpe_ratio": sharpe,
            "max_drawdown": drawdown,
            "trade_count": trades,
        },
        "stress_windows": [
            {
                "key": "vol_shock_aug_2024",
                "label": "Aug-2024 Vol Shock",
                "strategy_total_return": -0.01,
                "benchmark_total_return": -0.03,
                "strategy_max_drawdown": -0.02,
                "benchmark_max_drawdown": -0.05,
                "days_to_bear_flag": 3,
                "trade_count": 1,
            }
        ],
    }


def _result(ticker: str, trades: int = 4) -> PipelineBacktestResult:
    payload = _payload(ticker, ret=0.10, sharpe=1.0, drawdown=-0.08, trades=trades)
    return PipelineBacktestResult(
        ticker=ticker,
        config={},
        metrics=payload["metrics"],
        in_sample={"total_return": 0.05, "trade_count": 1},
        out_of_sample=payload["out_of_sample"],
        trades=[],
        equity_curve=[],
        exit_type_counts={},
        gate_counts={},
        stress_windows=payload["stress_windows"],
    )


def test_select_basket_is_mechanical_by_sector_and_dollar_adv(tmp_path: Path) -> None:
    sectors = {
        "AAA": "Information Technology",
        "BBB": "Information Technology",
        "CCC": "Information Technology",
        "DDD": "Health Care",
        "EEE": "Health Care",
        "FFF": "Health Care",
    }
    prices = {"AAA": 20, "BBB": 30, "CCC": 10, "DDD": 40, "EEE": 20, "FFF": 8}
    volumes = {"AAA": 1_000_000, "BBB": 3_000_000, "CCC": 2_000_000, "DDD": 1_000_000, "EEE": 4_000_000, "FFF": 10_000_000}

    payload = select_basket(
        output_path=tmp_path / "basket.json",
        candidates=list(sectors),
        sector_lookup=lambda tickers: {ticker: sectors[ticker] for ticker in tickers},
        market_frame_loader=lambda ticker: _frame(price=prices[ticker], volume=volumes[ticker]),
        names_per_sector=2,
    )

    assert payload["screen_stats"]["selected_count"] == 4
    assert payload["screen_stats"]["sector_status"]["Information Technology"]["selected"] == ["BBB", "AAA"]
    assert payload["screen_stats"]["sector_status"]["Health Care"]["selected"] == ["EEE", "FFF"]
    assert (tmp_path / "basket.json").exists()


def test_select_basket_dedupes_dual_class_listings_by_issuer(tmp_path: Path) -> None:
    sectors = {
        "GOOGL": "Communication Services",
        "GOOG": "Communication Services",
        "META": "Communication Services",
        "NFLX": "Communication Services",
    }
    prices = {"GOOGL": 180, "GOOG": 181, "META": 500, "NFLX": 700}
    volumes = {"GOOGL": 30_000_000, "GOOG": 25_000_000, "META": 15_000_000, "NFLX": 4_000_000}

    payload = select_basket(
        output_path=tmp_path / "basket.json",
        candidates=list(sectors),
        sector_lookup=lambda tickers: {ticker: sectors[ticker] for ticker in tickers},
        market_frame_loader=lambda ticker: _frame(price=prices[ticker], volume=volumes[ticker]),
        names_per_sector=3,
    )

    status = payload["screen_stats"]["sector_status"]["Communication Services"]
    # By dollar ADV: META (7.5B) > GOOGL (5.4B) > GOOG (4.5B, duplicate Alphabet
    # issuer -> skipped) > NFLX (2.8B, promoted into the third slot).
    assert status["selected"] == ["META", "GOOGL", "NFLX"]
    assert status["skipped_duplicate_issuers"] == ["GOOG"]
    assert "GOOG" not in payload["tickers"]
    assert "one listing per issuer" in payload["selection_rule"]


def test_phase0_resume_skips_completed_backtest_units(tmp_path: Path) -> None:
    basket = select_basket(
        output_path=tmp_path / "basket.json",
        candidates=["AAA"],
        sector_lookup=lambda tickers: {"AAA": "Information Technology"},
        market_frame_loader=lambda ticker: _frame(),
        names_per_sector=1,
    )
    assert basket["tickers"] == ["AAA"]

    calls = {"runner": 0}

    def runner(ticker, market_frame, config, benchmark_frame):
        calls["runner"] += 1
        return _result(ticker)

    run_phase0(
        basket_path=tmp_path / "basket.json",
        campaign_dir=tmp_path / "campaign",
        frame_loader=lambda ticker: _frame(),
        backtest_runner=runner,
    )
    run_phase0(
        basket_path=tmp_path / "basket.json",
        campaign_dir=tmp_path / "campaign",
        resume=True,
        frame_loader=lambda ticker: _frame(),
        backtest_runner=runner,
    )

    assert calls["runner"] == 1
    summary = tmp_path / "campaign" / "phase0" / "summary.json"
    assert summary.exists()


def test_evidence_floor_and_robustness_verdict() -> None:
    insufficient = evidence_floor({"oos_trade_count": 99, "traded_ticker_count": 30})
    assert insufficient["status"] == "insufficient_sample"

    baseline = {f"T{i}": _payload(f"T{i}", ret=0.01, sharpe=0.2, drawdown=-0.10, trades=5) for i in range(30)}
    candidate = {f"T{i}": _payload(f"T{i}", ret=0.03 if i < 20 else 0.0, sharpe=0.5, drawdown=-0.11, trades=5) for i in range(30)}

    verdict = robustness_verdict(baseline, candidate)
    assert verdict["verdict"] == "recommended"
    assert verdict["individual_improvement_rate"] >= 0.60

    sparse = {f"T{i}": _payload(f"T{i}", ret=0.10, sharpe=1.0, drawdown=-0.05, trades=3) for i in range(10)}
    assert robustness_verdict(baseline, sparse)["verdict"] == "insufficient_sample"


def test_report_renderer_includes_required_sections(tmp_path: Path) -> None:
    basket_path = tmp_path / "basket.json"
    select_basket(
        output_path=basket_path,
        candidates=["AAA"],
        sector_lookup=lambda tickers: {"AAA": "Information Technology"},
        market_frame_loader=lambda ticker: _frame(),
        names_per_sector=1,
    )
    campaign_dir = tmp_path / "campaign"
    phase0_dir = campaign_dir / "phase0"
    phase0_dir.mkdir(parents=True)
    aggregate = aggregate_result_payloads([_payload("AAA", ret=0.10, sharpe=1.2, drawdown=-0.08, trades=5)])
    (phase0_dir / "summary.json").write_text(
        json.dumps({"phase": 0, "config_id": "baseline", "config_count": 1, "aggregate": aggregate}) + "\n",
        encoding="utf-8",
    )

    report = render_report(campaign_dir=campaign_dir, basket_path=basket_path, output_path=tmp_path / "report.md")

    assert "# Alpha Campaign Report" in report
    assert "## Q1 Baseline Versus Buy-And-Hold" in report
    assert "## Q3 Meta-Labeler Verdict" in report
    assert "## Recommended Default Changes" in report
