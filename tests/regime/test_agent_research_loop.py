from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.regime.agent_research_ledger import append_trial, verify_trial_ledger
from src.regime.agent_research_loop import (
    DEFAULT_WALK_FORWARD_FOLDS,
    append_h001_walk_forward_ledger_entry,
    apply_momentum_risk_overlay,
    run_dev_walk_forward_evaluation,
    score_walk_forward_fold_distribution,
    seed_basket_study_ledger,
)
from src.regime.basket_study import BasketStudyConfig, _buy_hold_curve
from src.regime.alpha_campaign import _write_json


def _summary(path: Path, start: str, end: str) -> None:
    arms = [
        "C0b_static_pit",
        "A1_pure_momentum",
        "A2_quality_momentum",
        "A3_momentum_valuation_cap",
        "A4_quality_momentum_valuation",
    ]
    rows = [
        {
            "arm": arm,
            "after_tax_terminal_wealth": 100_000 + idx * 10_000,
            "annualized_return": 0.01 * idx,
            "max_drawdown": -0.1,
            "ulcer_index": 0.05,
        }
        for idx, arm in enumerate(arms, start=1)
    ]
    verdicts = {
        arm: {
            "status": "kill_switch_fail",
            "oos_total_return": 0.01,
            "oos_calmar_ratio": 0.2,
            "oos_ulcer_index": 0.1,
        }
        for arm in arms
    }
    _write_json(
        path,
        {
            "start": start,
            "end": end,
            "data_readiness": "survivorship_free",
            "gate_status": "certifiable",
            "rows": rows,
            "verdict": {"arm_verdicts": verdicts},
        },
    )


def test_seed_basket_study_ledger_appends_five_and_is_idempotent(tmp_path) -> None:
    summary_a = tmp_path / "summary_a.json"
    summary_b = tmp_path / "summary_b.json"
    _summary(summary_a, "2006-01-01", "2025-12-31")
    _summary(summary_b, "1998-01-01", "2015-12-31")
    ledger = tmp_path / "arl_trials.jsonl"

    first = seed_basket_study_ledger(
        ledger,
        data_snapshot_hash="snapshot-a",
        campaign_2006_2025=summary_a,
        campaign_1998_2015=summary_b,
    )
    second = seed_basket_study_ledger(
        ledger,
        data_snapshot_hash="snapshot-a",
        campaign_2006_2025=summary_a,
        campaign_1998_2015=summary_b,
    )

    status = verify_trial_ledger(ledger)
    assert status.valid is True
    assert status.trial_count == 5
    assert first["appended_count"] == 5
    assert second["appended_count"] == 0
    assert len(second["skipped_existing"]) == 5


def test_momentum_risk_overlay_reduces_exposure_without_underlying_sales() -> None:
    dates = pd.date_range("2020-01-02", periods=90, freq="B")
    equity = 100_000.0
    curve = []
    for idx, date in enumerate(dates):
        if idx:
            ret = -0.04 if idx % 5 == 0 else 0.025
            equity *= 1.0 + ret
        curve.append({"date": date.date().isoformat(), "equity": equity})
    benchmark_prices = pd.DataFrame(
        {
            "open": [100 + idx * 0.1 for idx in range(len(dates))],
            "close": [100 + idx * 0.1 for idx in range(len(dates))],
            "closeadj": [100 + idx * 0.1 for idx in range(len(dates))],
            "volume": [1_000_000 for _ in dates],
        },
        index=dates,
    )

    payload = apply_momentum_risk_overlay(
        {"after_tax_equity_curve": curve},
        BasketStudyConfig(starting_cash=100_000.0, oos_start="2020-03-01"),
        benchmark_curve=_buy_hold_curve(benchmark_prices, starting_cash=100_000.0),
        oos_start="2020-03-01",
    )

    exposures = [float(row["exposure"]) for row in payload["after_tax_equity_curve"]]
    assert min(exposures) < 1.0
    assert payload["strategy_spec"]["underlying_tax_lot_sales_by_overlay"] is False
    assert payload["strategy_spec"]["significant_gains_held_long_term"] is True
    assert all(row["ticker"] == "A1_RISK_OVERLAY" for row in payload["trades"])
    assert payload["production_defaults_changed"] is False


def test_walkforward_multiple_folds() -> None:
    strategy = _curve_payload("2000-01-03", "2023-12-29", daily_return=0.00035)
    benchmark = _curve_payload("2000-01-03", "2023-12-29", daily_return=0.00020)
    base = _curve_payload("2000-01-03", "2023-12-29", daily_return=0.00025)

    result = run_dev_walk_forward_evaluation(
        strategy,
        benchmark,
        base_payload=base,
        folds=DEFAULT_WALK_FORWARD_FOLDS,
        min_oos_folds=4,
        min_major_crashes=3,
    )

    aggregate = result["aggregate"]
    assert result["oos_evaluation_mode"] == "walk_forward_stress_folds"
    assert aggregate["included_fold_count"] >= 4
    assert aggregate["major_crash_fold_count"] >= 3
    assert {row["stress_label"] for row in result["folds"] if row["status"] == "included"} >= {
        "dotcom_bust",
        "global_financial_crisis",
        "inflation_rates_bear",
    }
    assert result["holdout_window"]["accessed"] is False


def test_single_fold_luck_not_promising() -> None:
    rows = [
        _fold_result("lucky_2022", True, ret=0.30, calmar=1.00, ulcer=-0.10, full=True),
        _fold_result("dotcom", True, ret=-0.05, calmar=-0.30, ulcer=0.04, full=False),
        _fold_result("gfc", True, ret=-0.08, calmar=-0.40, ulcer=0.06, full=False),
        _fold_result("covid", True, ret=-0.03, calmar=-0.10, ulcer=0.02, full=False),
    ]

    aggregate = score_walk_forward_fold_distribution(rows, min_oos_folds=4, min_major_crashes=3)

    assert aggregate["verdict"] in {"inconclusive", "killed"}
    assert aggregate["verdict"] != "promising"
    assert aggregate["single_fold_concentration_flag"] is True


def test_holdout_still_untouched() -> None:
    strategy = _curve_payload("2022-01-03", "2024-12-31", daily_return=0.00030)
    benchmark = _curve_payload("2022-01-03", "2024-12-31", daily_return=0.00020)
    folds = [
        {
            "fold_id": "dev_2022",
            "train_through": "2021-12-31",
            "oos_start": "2022-01-01",
            "oos_end": "2022-12-31",
            "stress_label": "inflation_bear",
            "major_crash": True,
        },
        {
            "fold_id": "holdout_2024",
            "train_through": "2023-12-31",
            "oos_start": "2024-01-01",
            "oos_end": "2024-12-31",
            "stress_label": "locked_holdout",
            "major_crash": False,
        },
    ]

    result = run_dev_walk_forward_evaluation(
        strategy,
        benchmark,
        folds=folds,
        dev_start="2022-01-01",
        dev_end="2024-12-31",
        holdout_start="2024-01-01",
        min_oos_folds=1,
        min_major_crashes=1,
    )

    included = [row for row in result["folds"] if row["status"] == "included"]
    excluded = [row for row in result["folds"] if row["status"] != "included"]
    assert [row["fold_id"] for row in included] == ["dev_2022"]
    assert [row["fold_id"] for row in excluded] == ["holdout_2024"]
    assert result["holdout_window"]["accessed"] is False
    assert all(pd.Timestamp(row["oos_end"]) < pd.Timestamp("2024-01-01") for row in included)


def test_verdict_uses_distribution() -> None:
    single_window_win = [
        _fold_result("one_big_win", True, ret=1.50, calmar=4.0, ulcer=-0.20, full=True),
        _fold_result("loss_1", True, ret=-0.05, calmar=-0.1, ulcer=0.02, full=False),
        _fold_result("loss_2", True, ret=-0.04, calmar=-0.1, ulcer=0.02, full=False),
        _fold_result("loss_3", False, ret=-0.03, calmar=-0.1, ulcer=0.02, full=False),
        _fold_result("loss_4", False, ret=-0.02, calmar=-0.1, ulcer=0.02, full=False),
    ]
    robust_distribution = [
        _fold_result("win_1", True, ret=0.03, calmar=0.2, ulcer=-0.01, full=True),
        _fold_result("win_2", True, ret=0.04, calmar=0.3, ulcer=-0.02, full=True),
        _fold_result("win_3", True, ret=0.02, calmar=0.1, ulcer=-0.01, full=True),
        _fold_result("win_4", False, ret=0.05, calmar=0.4, ulcer=-0.03, full=True),
        _fold_result("flat_5", False, ret=-0.01, calmar=-0.1, ulcer=0.01, full=False),
    ]

    lucky = score_walk_forward_fold_distribution(single_window_win, min_oos_folds=4, min_major_crashes=3)
    robust = score_walk_forward_fold_distribution(robust_distribution, min_oos_folds=4, min_major_crashes=3)

    assert lucky["verdict"] != "promising"
    assert lucky["single_fold_concentration_flag"] is True
    assert robust["verdict"] == "promising"
    assert robust["single_fold_concentration_flag"] is False


def test_h001_walkforward_corrective_entry_appends_without_rewriting_trial_6(tmp_path) -> None:
    ledger = tmp_path / "arl_trials.jsonl"
    original = {
        "trial_id": "H001_A1_momentum_risk_overlay",
        "verdict": "inconclusive",
        "oos_evaluation_mode": "single_cut_2021_2023",
        "production_defaults_changed": False,
    }
    append_trial(ledger, original, data_snapshot_hash="snapshot-a")
    original_line = ledger.read_text(encoding="utf-8").splitlines()[0]
    corrective = {
        "trial_id": "H001R_A1_momentum_risk_overlay_walk_forward",
        "verdict": "killed",
        "oos_evaluation_mode": "walk_forward_stress_folds",
        "holdout_accessed": False,
        "production_defaults_changed": False,
    }

    out = append_h001_walk_forward_ledger_entry(ledger, corrective, data_snapshot_hash="snapshot-a")

    lines = ledger.read_text(encoding="utf-8").splitlines()
    status = verify_trial_ledger(ledger)
    assert out["ledger_append_status"] == "appended"
    assert len(lines) == 2
    assert lines[0] == original_line
    assert status.valid is True
    assert status.trial_count == 2


def _curve_payload(start: str, end: str, *, daily_return: float) -> dict:
    equity = 100_000.0
    rows = []
    for idx, date in enumerate(pd.date_range(start, end, freq="B")):
        if idx:
            equity *= 1.0 + daily_return
        rows.append({"date": date.date().isoformat(), "equity": equity})
    return {"after_tax_equity_curve": rows, "trades": [], "production_defaults_changed": False}


def _fold_result(
    fold_id: str,
    major_crash: bool,
    *,
    ret: float,
    calmar: float,
    ulcer: float,
    full: bool,
) -> dict:
    return {
        "fold_id": fold_id,
        "status": "included",
        "major_crash": major_crash,
        "total_return_delta": ret,
        "calmar_delta": calmar,
        "ulcer_delta": ulcer,
        "beats_index_metric_count": 3 if full else (1 if ret > 0 else 0),
        "clears_full_metric_set": full,
        "crash_risk_improved_vs_bare_a1": major_crash and full,
    }
