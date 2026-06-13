from __future__ import annotations

import json

import pandas as pd

from src.regime import portfolio_campaign as campaign2
from src.regime.strategy import StrategySpec


def _payload(
    *,
    oos_return: float,
    oos_sharpe: float,
    oos_calmar: float,
    max_dd: float = -0.1,
    stress_dd: float = -0.1,
) -> dict:
    return {
        "strategy_hash": "hash",
        "metrics": {
            "total_return": oos_return,
            "annualized_return": oos_return,
            "annualized_volatility": 0.15,
            "sharpe_ratio": oos_sharpe,
            "calmar_ratio": oos_calmar,
            "max_drawdown": max_dd,
            "annualized_turnover": 1.0,
            "total_costs_paid": 10.0,
            "exposure_pct": 0.8,
        },
        "out_of_sample": {
            "total_return": oos_return,
            "annualized_return": oos_return,
            "sharpe_ratio": oos_sharpe,
            "calmar_ratio": oos_calmar,
            "max_drawdown": max_dd,
        },
        "stress_windows": [
            {"key": "covid_crash", "strategy_max_drawdown": stress_dd, "strategy_total_return": -0.02, "days_to_derisk": 3, "exposure_mean": 0.5},
            {"key": "bear_2022", "strategy_max_drawdown": stress_dd, "strategy_total_return": -0.03, "days_to_derisk": 5, "exposure_mean": 0.6},
        ],
    }


def _frame(prices: list[float]) -> pd.DataFrame:
    dates = pd.bdate_range("2024-01-01", periods=len(prices))
    return pd.DataFrame({"open": prices, "high": prices, "low": prices, "price": prices, "volume": 1_000_000}, index=dates)


def test_campaign2_verdict_supports_layers_and_exact_control_sentence() -> None:
    results = {
        "L0": _payload(oos_return=0.10, oos_sharpe=0.5, oos_calmar=1.0, stress_dd=-0.30),
        "L1": _payload(oos_return=0.09, oos_sharpe=0.7, oos_calmar=1.2, stress_dd=-0.28),
        "L2": _payload(oos_return=0.085, oos_sharpe=0.8, oos_calmar=1.4, stress_dd=-0.18),
        "L3": _payload(oos_return=0.08, oos_sharpe=0.9, oos_calmar=1.5, stress_dd=-0.20),
        "C2_spy_200dma": _payload(oos_return=0.12, oos_sharpe=1.1, oos_calmar=2.0),
    }
    verdict = campaign2.campaign2_verdict(results, cost_fragility_result=_payload(oos_return=0.02, oos_sharpe=0.1, oos_calmar=0.2))
    assert verdict["best_supported_arm"] == "L3"
    assert verdict["control_verdict"] == campaign2.COMPLEXITY_VERDICT
    assert verdict["cost_fragility"] == "cost-fragile"
    assert verdict["stress_preservation"]["passed"]


def test_campaign2_runner_and_report_are_offline_with_patched_specs(tmp_path, monkeypatch) -> None:
    basket_path = tmp_path / "basket.json"
    basket_path.write_text(json.dumps({"tickers": ["AAA", "BBB"], "basket_size": 2}), encoding="utf-8")
    frames = {
        "AAA": _frame([10, 10.5, 11, 11.5, 12, 12.5, 13]),
        "BBB": _frame([20, 20, 19.5, 19, 19.5, 20, 20.5]),
        "SPY": _frame([100, 101, 102, 103, 104, 105, 106]),
    }

    monkeypatch.setattr(
        campaign2,
        "campaign2_headline_specs",
        lambda: {
            "L0": StrategySpec(name="L0", signal_provider="price_history"),
            "C2_spy_200dma": StrategySpec(name="C2_spy_200dma", signal_provider="price_history", exposure_policy="moving_average_timing", exposure_params={"ticker": "SPY"}),
        },
    )
    monkeypatch.setattr(campaign2, "campaign2_sensitivity_specs", lambda: {})
    summary = campaign2.run_campaign2(
        basket_path=basket_path,
        campaign_dir=tmp_path / "campaign2",
        frame_loader=lambda ticker: frames[str(ticker).upper()],
    )
    assert summary["configurations_evaluated"] == 3
    assert (tmp_path / "campaign2" / "summary.json").exists()
    report = campaign2.render_campaign2_report(campaign_dir=tmp_path / "campaign2", output_path=tmp_path / "report.md")
    assert "Alpha Campaign 2 Report" in report
    assert "Configurations evaluated" in report
