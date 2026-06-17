from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from src.regime.agent_research_ledger import append_trial, verify_trial_ledger
from src.regime.agent_research_loop import (
    DEFAULT_WALK_FORWARD_FOLDS,
    append_h001_walk_forward_ledger_entry,
    apply_momentum_risk_overlay,
    agent_research_loop_status,
    request_agent_research_loop_pause,
    run_dev_walk_forward_evaluation,
    run_agent_research_loop,
    score_walk_forward_fold_distribution,
    seed_basket_study_ledger,
)
from src.regime.basket_study import BasketStudyConfig, SelectionRow, _buy_hold_curve, _candidate_feature_start_date, _score_selection_rows
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


def test_h002_quality_value_scores_quality_minus_expensiveness() -> None:
    rows = [
        SelectionRow(1, "CHEAPQ", 0.0, momentum=0.01, quality=0.30, valuation=8.0, marketcap=1_000_000_000, dollar_adv=20_000_000),
        SelectionRow(2, "EXPQ", 0.0, momentum=0.20, quality=0.30, valuation=30.0, marketcap=1_000_000_000, dollar_adv=20_000_000),
        SelectionRow(3, "CHEAPL", 0.0, momentum=0.30, quality=0.05, valuation=8.0, marketcap=1_000_000_000, dollar_adv=20_000_000),
        SelectionRow(4, "MISS", 0.0, momentum=0.40, quality=None, valuation=5.0, marketcap=1_000_000_000, dollar_adv=20_000_000),
    ]

    scored = _score_selection_rows(rows, "H002_quality_value_defensive", BasketStudyConfig())

    assert [row.ticker for row in sorted(scored, key=lambda row: row.score, reverse=True)] == ["CHEAPQ", "CHEAPL", "EXPQ"]
    assert all(row.ticker != "MISS" for row in scored)


def test_six_month_candidate_window_covers_listing_floor() -> None:
    as_of = pd.Timestamp("2020-01-02")
    start = pd.Timestamp(_candidate_feature_start_date(as_of, BasketStudyConfig(formation="6_1")))

    assert (as_of - start).days >= BasketStudyConfig().min_listing_days


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


def test_kill_midtrial_no_partial_ledger(tmp_path) -> None:
    ledger = tmp_path / "arl_trials.jsonl"
    research_dir = tmp_path / "research"
    calls: list[str] = []

    def killing_runner(*, spec, scratch_dir, research_dir):
        calls.append(str(spec["trial_id"]))
        Path(scratch_dir, "partial.json").write_text("partial", encoding="utf-8")
        raise RuntimeError("simulated kill")

    with pytest.raises(RuntimeError, match="simulated kill"):
        run_agent_research_loop(
            ledger,
            data_snapshot_hash="snapshot-a",
            max_trials=1,
            stop_after_no_promising=99,
            research_dir=research_dir,
            hypotheses=[_hypothesis("H900_kill")],
            readiness_checker=_ready,
            trial_runner=killing_runner,
        )

    status_after_kill = verify_trial_ledger(ledger)
    assert status_after_kill.valid is True
    assert _committed_trial_ids(ledger) == {
        "seed_C0b_static_pit",
        "seed_A1_pure_momentum",
        "seed_A2_quality_momentum",
        "seed_A3_momentum_valuation_cap",
        "seed_A4_quality_momentum_valuation",
    }
    assert calls == ["H900_kill"]

    payload = run_agent_research_loop(
        ledger,
        data_snapshot_hash="snapshot-a",
        mode="resume",
        max_trials=1,
        stop_after_no_promising=99,
        research_dir=research_dir,
        hypotheses=[_hypothesis("H900_kill")],
        readiness_checker=_ready,
        trial_runner=_runner(verdicts={"H900_kill": "killed"}),
    )

    assert payload["ledger_status"]["valid"] is True
    assert payload["budget"]["trials_appended_this_run"] == 1
    assert verify_trial_ledger(ledger).trial_count == status_after_kill.trial_count + 1
    assert "H900_kill" in _committed_trial_ids(ledger)


def test_pause_resume_preserves_cumulative_count(tmp_path) -> None:
    ledger = tmp_path / "arl_trials.jsonl"
    research_dir = tmp_path / "research"
    hypotheses = [_hypothesis("H901_pause"), _hypothesis("H902_resume")]

    def pausing_runner(*, spec, scratch_dir, research_dir):
        request_agent_research_loop_pause(research_dir=research_dir, ledger_path=ledger)
        return _trial(str(spec["trial_id"]), verdict="killed")

    first = run_agent_research_loop(
        ledger,
        data_snapshot_hash="snapshot-a",
        max_trials=5,
        stop_after_no_promising=99,
        research_dir=research_dir,
        hypotheses=hypotheses,
        readiness_checker=_ready,
        trial_runner=pausing_runner,
    )
    count_after_pause = verify_trial_ledger(ledger).trial_count
    status_after_pause = agent_research_loop_status(
        research_dir=research_dir,
        ledger_path=ledger,
        data_snapshot_hash="snapshot-a",
    )

    second = run_agent_research_loop(
        ledger,
        data_snapshot_hash="snapshot-a",
        mode="resume",
        max_trials=5,
        stop_after_no_promising=99,
        research_dir=research_dir,
        hypotheses=hypotheses,
        readiness_checker=_ready,
        trial_runner=_runner(verdicts={"H902_resume": "killed"}),
    )

    assert first["stop_reason"] == "paused"
    assert first["budget"]["trials_appended_this_run"] == 1
    assert status_after_pause["trials_committed"] == count_after_pause
    assert second["skipped_existing"] == ["H901_pause"]
    assert second["budget"]["trials_appended_this_run"] == 1
    assert verify_trial_ledger(ledger).trial_count == count_after_pause + 1
    assert {"H901_pause", "H902_resume"} <= _committed_trial_ids(ledger)


def test_budgets_stop_gracefully(tmp_path) -> None:
    hypotheses = [_hypothesis("H910_budget_a"), _hypothesis("H911_budget_b"), _hypothesis("H912_budget_c")]

    max_trials_payload = run_agent_research_loop(
        tmp_path / "max_trials.jsonl",
        data_snapshot_hash="snapshot-a",
        max_trials=2,
        stop_after_no_promising=99,
        research_dir=tmp_path / "max_trials",
        hypotheses=hypotheses,
        readiness_checker=_ready,
        trial_runner=_runner(),
    )
    assert max_trials_payload["stop_reason"] == "max_trials"
    assert max_trials_payload["budget"]["trials_appended_this_run"] == 2

    stop_after_payload = run_agent_research_loop(
        tmp_path / "stop_after.jsonl",
        data_snapshot_hash="snapshot-a",
        max_trials=3,
        stop_after_no_promising=6,
        research_dir=tmp_path / "stop_after",
        hypotheses=hypotheses,
        readiness_checker=_ready,
        trial_runner=_runner(),
    )
    assert stop_after_payload["stop_reason"] == "stop_after_no_promising"
    assert stop_after_payload["budget"]["trials_appended_this_run"] == 1

    wall_clock_payload = run_agent_research_loop(
        tmp_path / "wall_clock.jsonl",
        data_snapshot_hash="snapshot-a",
        max_trials=3,
        max_wall_clock=0.001,
        stop_after_no_promising=99,
        research_dir=tmp_path / "wall_clock",
        hypotheses=hypotheses,
        readiness_checker=_ready,
        trial_runner=_runner(sleep_seconds=0.01),
    )
    assert wall_clock_payload["stop_reason"] == "max_wall_clock"
    assert wall_clock_payload["budget"]["trials_appended_this_run"] == 1


def test_resume_refuses_broken_chain_or_changed_snapshot(tmp_path) -> None:
    broken_ledger = tmp_path / "broken.jsonl"
    append_trial(broken_ledger, _trial("H920_original"), data_snapshot_hash="snapshot-a")
    broken_ledger.write_text(
        broken_ledger.read_text(encoding="utf-8").replace("H920_original", "H920_tampered"),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="broken ledger chain"):
        run_agent_research_loop(
            broken_ledger,
            data_snapshot_hash="snapshot-a",
            mode="resume",
            max_trials=1,
            stop_after_no_promising=99,
            research_dir=tmp_path / "broken_research",
            hypotheses=[_hypothesis("H921_new")],
            readiness_checker=_ready,
            trial_runner=_runner(),
        )

    changed_ledger = tmp_path / "changed.jsonl"
    append_trial(changed_ledger, _trial("H922_original"), data_snapshot_hash="snapshot-a")

    with pytest.raises(ValueError, match="snapshot change"):
        run_agent_research_loop(
            changed_ledger,
            data_snapshot_hash="snapshot-b",
            mode="resume",
            max_trials=1,
            stop_after_no_promising=99,
            research_dir=tmp_path / "changed_research",
            hypotheses=[_hypothesis("H923_new")],
            readiness_checker=_ready,
            trial_runner=_runner(),
        )


def test_resume_idempotent_skips_committed(tmp_path) -> None:
    ledger = tmp_path / "arl_trials.jsonl"
    append_trial(ledger, _trial("H002_marker"), data_snapshot_hash="snapshot-a")
    calls: list[str] = []

    def recording_runner(*, spec, scratch_dir, research_dir):
        calls.append(str(spec["trial_id"]))
        return _trial(str(spec["trial_id"]), verdict="killed")

    payload = run_agent_research_loop(
        ledger,
        data_snapshot_hash="snapshot-a",
        mode="resume",
        max_trials=2,
        stop_after_no_promising=99,
        research_dir=tmp_path / "research",
        hypotheses=[_hypothesis("H002_marker"), _hypothesis("H003_next")],
        readiness_checker=_ready,
        trial_runner=recording_runner,
    )

    assert payload["skipped_existing"] == ["H002_marker"]
    assert calls == ["H003_next"]
    assert {"H002_marker", "H003_next"} <= _committed_trial_ids(ledger)


def test_holdout_untouched_across_pause_resume(tmp_path) -> None:
    ledger = tmp_path / "arl_trials.jsonl"
    research_dir = tmp_path / "research"
    hypotheses = [_hypothesis("H930_holdout_a"), _hypothesis("H931_holdout_b")]

    def pausing_runner(*, spec, scratch_dir, research_dir):
        request_agent_research_loop_pause(research_dir=research_dir, ledger_path=ledger)
        return _trial(str(spec["trial_id"]), verdict="killed")

    first = run_agent_research_loop(
        ledger,
        data_snapshot_hash="snapshot-a",
        max_trials=5,
        stop_after_no_promising=99,
        research_dir=research_dir,
        hypotheses=hypotheses,
        readiness_checker=_ready,
        trial_runner=pausing_runner,
    )
    second = run_agent_research_loop(
        ledger,
        data_snapshot_hash="snapshot-a",
        mode="resume",
        max_trials=5,
        stop_after_no_promising=99,
        research_dir=research_dir,
        hypotheses=hypotheses,
        readiness_checker=_ready,
        trial_runner=_runner(),
    )

    assert first["holdout_window"]["accessed"] is False
    assert second["holdout_window"]["accessed"] is False
    assert all(record["trial"].get("holdout_accessed") is False for record in _ledger_records(ledger))


def test_promising_halts_run(tmp_path) -> None:
    ledger = tmp_path / "arl_trials.jsonl"
    hypotheses = [_hypothesis("H940_promising"), _hypothesis("H941_should_not_run")]

    payload = run_agent_research_loop(
        ledger,
        data_snapshot_hash="snapshot-a",
        max_trials=5,
        stop_after_no_promising=99,
        research_dir=tmp_path / "research",
        hypotheses=hypotheses,
        readiness_checker=_ready,
        trial_runner=_runner(verdicts={"H940_promising": "promising"}),
    )

    assert payload["stop_reason"] == "promising_candidate"
    assert payload["candidate_pool"][0]["trial_id"] == "H940_promising"
    assert payload["budget"]["trials_appended_this_run"] == 1
    assert "H941_should_not_run" not in _committed_trial_ids(ledger)


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


def _ready(**kwargs) -> dict:
    return {
        "verdict": "HARNESS READY",
        "production_defaults_changed": False,
        "snapshot": kwargs.get("expected_snapshot_hash"),
    }


def _hypothesis(trial_id: str) -> dict:
    return {
        "trial_id": trial_id,
        "hypothesis": f"{trial_id} test hypothesis",
        "economic_rationale": "deterministic runner fixture",
        "arm": "A1_pure_momentum",
    }


def _trial(trial_id: str, *, verdict: str = "killed") -> dict:
    return {
        "trial_id": trial_id,
        "verdict": verdict,
        "verdict_rationale": f"{trial_id} {verdict}",
        "walk_forward": {
            "holdout_window": {"start": "2024-01-01", "end": "2025-12-31", "accessed": False},
            "aggregate": {"verdict": verdict, "single_fold_concentration_flag": False},
        },
        "holdout_accessed": False,
        "production_defaults_changed": False,
    }


def _runner(*, verdicts: dict[str, str] | None = None, sleep_seconds: float = 0.0):
    import time

    verdicts = dict(verdicts or {})

    def run(*, spec, scratch_dir, research_dir):
        if sleep_seconds:
            time.sleep(sleep_seconds)
        return _trial(str(spec["trial_id"]), verdict=verdicts.get(str(spec["trial_id"]), "killed"))

    return run


def _committed_trial_ids(ledger: Path) -> set[str]:
    return {str(record["trial"]["trial_id"]) for record in _ledger_records(ledger)}


def _ledger_records(ledger: Path) -> list[dict]:
    import json

    if not ledger.exists():
        return []
    return [json.loads(line) for line in ledger.read_text(encoding="utf-8").splitlines() if line.strip()]
