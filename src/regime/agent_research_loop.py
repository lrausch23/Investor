from __future__ import annotations

import datetime as dt
import json
import math
import os
from pathlib import Path
from typing import Any, Sequence

import pandas as pd

from .agent_research_ledger import append_trial, verify_trial_ledger
from .alpha_campaign import DEFAULT_BASKET_PATH, _git_sha, _json_safe, _read_json, _write_json, load_basket
from .basket_study import (
    BasketStudyConfig,
    _buy_hold_curve,
    _segment_metrics,
    run_basket_arm,
)
from .ccel_campaign import buy_hold_taxable_payload, _metrics, _stable_hash
from .portfolio_historical_campaign import _period_returns, _stress_results_for_curve, historical_stress_windows_for_range
from .sharadar import DEFAULT_SHARADAR_DIR, SharadarStore

DEFAULT_AGENT_RESEARCH_DIR = Path("data") / "agent_research"
DEFAULT_AGENT_RESEARCH_LEDGER = DEFAULT_AGENT_RESEARCH_DIR / "arl_trials.jsonl"
DEFAULT_AGENT_RESEARCH_DEV_START = "1998-01-01"
DEFAULT_AGENT_RESEARCH_DEV_END = "2023-12-31"
DEFAULT_AGENT_RESEARCH_DEV_OOS_START = "2021-01-01"
DEFAULT_AGENT_RESEARCH_HOLDOUT_START = "2024-01-01"
DEFAULT_AGENT_RESEARCH_HOLDOUT_END = "2025-12-31"
DEFAULT_WALK_FORWARD_MIN_FOLDS = 4
DEFAULT_WALK_FORWARD_MIN_MAJOR_CRASHES = 3

DEFAULT_WALK_FORWARD_FOLDS: tuple[dict[str, Any], ...] = (
    {
        "fold_id": "wf_2000_2002_dotcom",
        "train_through": "1999-12-31",
        "oos_start": "2000-01-01",
        "oos_end": "2002-12-31",
        "stress_label": "dotcom_bust",
        "major_crash": True,
    },
    {
        "fold_id": "wf_2008_2009_gfc",
        "train_through": "2007-12-31",
        "oos_start": "2008-01-01",
        "oos_end": "2009-12-31",
        "stress_label": "global_financial_crisis",
        "major_crash": True,
    },
    {
        "fold_id": "wf_2011_2012_macro",
        "train_through": "2010-12-31",
        "oos_start": "2011-01-01",
        "oos_end": "2012-12-31",
        "stress_label": "euro_debt_ceiling_macro",
        "major_crash": False,
    },
    {
        "fold_id": "wf_2015_2016_growth_scare",
        "train_through": "2014-12-31",
        "oos_start": "2015-01-01",
        "oos_end": "2016-12-31",
        "stress_label": "china_oil_growth_scare",
        "major_crash": False,
    },
    {
        "fold_id": "wf_2018_q4",
        "train_through": "2017-12-31",
        "oos_start": "2018-01-01",
        "oos_end": "2018-12-31",
        "stress_label": "q4_2018_drawdown",
        "major_crash": True,
    },
    {
        "fold_id": "wf_2020_covid",
        "train_through": "2019-12-31",
        "oos_start": "2020-01-01",
        "oos_end": "2020-12-31",
        "stress_label": "covid_crash_rebound",
        "major_crash": True,
    },
    {
        "fold_id": "wf_2022_inflation_bear",
        "train_through": "2021-12-31",
        "oos_start": "2022-01-01",
        "oos_end": "2022-12-31",
        "stress_label": "inflation_rates_bear",
        "major_crash": True,
    },
    {
        "fold_id": "wf_2023_recovery",
        "train_through": "2022-12-31",
        "oos_start": "2023-01-01",
        "oos_end": "2023-12-31",
        "stress_label": "post_bear_recovery",
        "major_crash": False,
    },
)

SNAPSHOT_D2CC = "d2ccfd9ea42e4db663003dcfacfa6a3ce69e4e91ea5c059de82b356f3a17f527"

BASKET_SEED_TRIALS = (
    {
        "trial_id": "seed_C0b_static_pit",
        "hypothesis": "Liquidity-screened PIT basket construction has robust edge over the passive index.",
        "arm": "C0b_static_pit",
        "verdict": "killed",
        "summary": "Beats index in 2006-2025 but loses 1998-2015; no robust cross-window edge.",
    },
    {
        "trial_id": "seed_A1_pure_momentum",
        "hypothesis": "Pure momentum PIT selection has robust after-tax risk-adjusted edge.",
        "arm": "A1_pure_momentum",
        "verdict": "killed",
        "summary": "Near-tie in 2006-2025 but loses 1998-2015; no robust risk-adjusted cross-window edge.",
    },
    {
        "trial_id": "seed_A2_quality_momentum",
        "hypothesis": "Quality plus momentum PIT selection has robust after-tax risk-adjusted edge.",
        "arm": "A2_quality_momentum",
        "verdict": "killed",
        "summary": "Loses both certified windows.",
    },
    {
        "trial_id": "seed_A4_quality_momentum_valuation",
        "hypothesis": "Quality, momentum, and valuation PIT selection has robust after-tax risk-adjusted edge.",
        "arm": "A4_quality_momentum_valuation",
        "verdict": "killed",
        "summary": "Loses both certified windows.",
    },
    {
        "trial_id": "seed_A3_momentum_valuation_cap",
        "hypothesis": "Momentum with valuation cap PIT selection has robust after-tax risk-adjusted edge.",
        "arm": "A3_momentum_valuation_cap",
        "verdict": "killed",
        "summary": "Beats raw return in 2006-2025 at higher risk but loses 1998-2015.",
    },
)


def run_stage2_go_live(
    *,
    expected_snapshot_hash: str = SNAPSHOT_D2CC,
    research_dir: str | Path = DEFAULT_AGENT_RESEARCH_DIR,
    ledger_path: str | Path | None = None,
    store_dir: str | Path = DEFAULT_SHARADAR_DIR,
    basket_path: str | Path = DEFAULT_BASKET_PATH,
    run_hypothesis: bool = True,
) -> dict[str, Any]:
    """Confirm Stage-1 readiness, seed the ledger, and run one DEV-only ARL smoke test."""

    root = Path(research_dir)
    root.mkdir(parents=True, exist_ok=True)
    ledger = Path(ledger_path) if ledger_path is not None else root / "arl_trials.jsonl"
    readiness = confirm_harness_ready(expected_snapshot_hash=expected_snapshot_hash, store_dir=store_dir, ledger_path=ledger)
    _write_json(root / "harness_readiness.json", readiness)
    if readiness.get("verdict") != "HARNESS READY":
        return {
            "schema": "regime_agent_research_go_live.v1",
            "stage": "stage_1_readiness",
            "verdict": "NOT READY",
            "blocking_items": readiness.get("blocking_items") or [],
            "production_defaults_changed": False,
        }
    seeded = seed_basket_study_ledger(ledger, data_snapshot_hash=expected_snapshot_hash)
    first_iteration = None
    if run_hypothesis:
        first_iteration = run_first_momentum_risk_overlay_iteration(
            ledger,
            data_snapshot_hash=expected_snapshot_hash,
            research_dir=root,
            store_dir=store_dir,
            basket_path=basket_path,
        )
    ledger_status = verify_trial_ledger(ledger).to_dict()
    payload = {
        "schema": "regime_agent_research_go_live.v1",
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "git_sha": _git_sha(),
        "snapshot": expected_snapshot_hash,
        "readiness": readiness,
        "ledger_path": str(ledger),
        "ledger_seeded": seeded,
        "first_iteration": first_iteration,
        "ledger_status": ledger_status,
        "holdout_window": {
            "start": DEFAULT_AGENT_RESEARCH_HOLDOUT_START,
            "end": DEFAULT_AGENT_RESEARCH_HOLDOUT_END,
            "accessed": False,
        },
        "pause_for_human_review": bool(first_iteration),
        "production_defaults_changed": False,
    }
    _write_json(root / "go_live_summary.json", payload)
    _write_iteration_markdown(root / "iteration_001_summary.md", payload)
    return payload


def run_h001_walk_forward_rescore(
    ledger_path: str | Path = DEFAULT_AGENT_RESEARCH_LEDGER,
    *,
    data_snapshot_hash: str,
    research_dir: str | Path = DEFAULT_AGENT_RESEARCH_DIR,
    store_dir: str | Path = DEFAULT_SHARADAR_DIR,
    basket_path: str | Path = DEFAULT_BASKET_PATH,
    start: str = DEFAULT_AGENT_RESEARCH_DEV_START,
    end: str = DEFAULT_AGENT_RESEARCH_DEV_END,
) -> dict[str, Any]:
    """Append a corrective walk-forward re-score of H001 without editing trial 6."""

    ledger = Path(ledger_path)
    existing = _ledger_trial_ids(ledger)
    trial_id = "H001R_A1_momentum_risk_overlay_walk_forward"
    if trial_id in existing:
        existing_result = _safe_json(Path(research_dir) / "iteration_001_walk_forward_result.json")
        return {
            "trial_id": trial_id,
            "status": "already_recorded",
            "result": existing_result,
            "ledger_status": verify_trial_ledger(ledger).to_dict(),
            "production_defaults_changed": False,
        }
    if "H001_A1_momentum_risk_overlay" not in existing:
        raise ValueError("Original H001 trial is not present in the ledger; seed/run Stage-2 go-live first.")
    store = SharadarStore(store_dir)
    if str(store.data_snapshot_hash) != str(data_snapshot_hash):
        raise ValueError("Snapshot changed; Stage 1 readiness must be rerun before Stage 2.")
    cfg = BasketStudyConfig(oos_start=DEFAULT_WALK_FORWARD_FOLDS[0]["oos_start"])
    synth_sp500 = store.synth_sp500_total_return(start, end)
    if synth_sp500.empty:
        raise ValueError("Synthesized S&P 500 benchmark unavailable for DEV window.")
    benchmark_curve = _buy_hold_curve(synth_sp500, starting_cash=cfg.starting_cash)
    stress_windows = historical_stress_windows_for_range(start, end)
    basket = load_basket(basket_path) if Path(basket_path).exists() else {"tickers": []}
    base = run_basket_arm(
        store,
        "A1_pure_momentum",
        cfg,
        start=start,
        end=end,
        basket=basket,
        benchmark_curve=benchmark_curve,
        windows=stress_windows,
    )
    overlay = apply_momentum_risk_overlay(
        base,
        cfg,
        benchmark_curve=benchmark_curve,
        oos_start=DEFAULT_WALK_FORWARD_FOLDS[0]["oos_start"],
        windows=stress_windows,
    )
    benchmark = buy_hold_taxable_payload(
        "SYNTH_SP500",
        synth_sp500,
        oos_start=DEFAULT_WALK_FORWARD_FOLDS[0]["oos_start"],
        benchmark_curve=benchmark_curve,
        windows=stress_windows,
    )
    walk_forward = run_dev_walk_forward_evaluation(
        overlay,
        benchmark,
        base_payload=base,
        folds=DEFAULT_WALK_FORWARD_FOLDS,
        dev_start=start,
        dev_end=end,
        holdout_start=DEFAULT_AGENT_RESEARCH_HOLDOUT_START,
        min_major_crashes=DEFAULT_WALK_FORWARD_MIN_MAJOR_CRASHES,
        min_oos_folds=DEFAULT_WALK_FORWARD_MIN_FOLDS,
    )
    result = {
        "schema": "regime_agent_research_iteration.v2",
        "trial_id": trial_id,
        "supersedes_for_verdict": "H001_A1_momentum_risk_overlay",
        "supersedes_reason": "Original trial used one OOS slice; this corrective entry uses multi-fold walk-forward stress OOS.",
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "data_snapshot_hash": data_snapshot_hash,
        "git_sha": _git_sha(),
        "hypothesis": "A1 pure-momentum selection plus fixed L1-style volatility-target/drawdown-brake overlay has robust OOS risk-adjusted after-tax edge.",
        "economic_rationale": "A1's single-slice improvement may have been one-crash timing luck; a robust overlay should improve risk metrics across multiple independent crash regimes.",
        "pre_registered_success_criterion": (
            "Walk-forward DEV OOS: majority of included folds beat synthesized S&P 500 on all pre-registered metrics; "
            "median total-return and Calmar deltas are positive, median Ulcer delta is negative; no single-fold concentration; "
            "major crash folds show drawdown and Ulcer improvement versus bare A1."
        ),
        "oos_evaluation_mode": "walk_forward_stress_folds",
        "dev_window": {"start": start, "end": end},
        "locked_holdout": {
            "start": DEFAULT_AGENT_RESEARCH_HOLDOUT_START,
            "end": DEFAULT_AGENT_RESEARCH_HOLDOUT_END,
            "accessed": False,
        },
        "strategy_spec": overlay.get("strategy_spec"),
        "walk_forward": walk_forward,
        "verdict": walk_forward.get("verdict"),
        "verdict_rationale": walk_forward.get("verdict_rationale"),
        "base_payload_path": str(Path(research_dir) / "iteration_001_walk_forward_A1_base_dev.json"),
        "overlay_payload_path": str(Path(research_dir) / "iteration_001_walk_forward_A1_risk_overlay_dev.json"),
        "benchmark_payload_path": str(Path(research_dir) / "iteration_001_walk_forward_synth_sp500_dev.json"),
        "holdout_accessed": False,
        "production_defaults_changed": False,
    }
    root = Path(research_dir)
    root.mkdir(parents=True, exist_ok=True)
    _write_json(root / "iteration_001_walk_forward_A1_base_dev.json", base)
    _write_json(root / "iteration_001_walk_forward_A1_risk_overlay_dev.json", overlay)
    _write_json(root / "iteration_001_walk_forward_synth_sp500_dev.json", benchmark)
    _write_json(root / "iteration_001_walk_forward_result.json", result)
    pd.DataFrame(walk_forward.get("folds") or []).to_csv(root / "iteration_001_walk_forward_folds.csv", index=False)
    _write_walk_forward_markdown(root / "iteration_001_walk_forward_summary.md", result)
    result = append_h001_walk_forward_ledger_entry(ledger, result, data_snapshot_hash=data_snapshot_hash)
    _write_json(root / "iteration_001_walk_forward_result.json", result)
    _write_walk_forward_markdown(root / "iteration_001_walk_forward_summary.md", result)
    summary = _safe_json(root / "stage2_walk_forward_summary.json")
    summary.update(
        {
            "schema": "regime_agent_research_walk_forward_summary.v1",
            "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            "data_snapshot_hash": data_snapshot_hash,
            "latest_walk_forward_trial": trial_id,
            "latest_verdict": result["verdict"],
            "holdout_accessed": False,
            "ledger_status": result["ledger_status"],
            "pause_for_human_review": True,
            "production_defaults_changed": False,
        }
    )
    _write_json(root / "stage2_walk_forward_summary.json", summary)
    return result


def append_h001_walk_forward_ledger_entry(
    ledger_path: str | Path,
    result: dict[str, Any],
    *,
    data_snapshot_hash: str,
) -> dict[str, Any]:
    """Append the corrective H001 walk-forward record without mutating prior trials."""

    ledger = Path(ledger_path)
    trial_id = str(result.get("trial_id") or "H001R_A1_momentum_risk_overlay_walk_forward")
    existing = _ledger_trial_ids(ledger)
    if trial_id in existing:
        out = dict(result)
        out["ledger_status"] = verify_trial_ledger(ledger).to_dict()
        out["ledger_append_status"] = "already_recorded"
        return out
    if "H001_A1_momentum_risk_overlay" not in existing:
        raise ValueError("Original H001 trial is not present in the ledger; corrective re-score must append after it.")
    record = append_trial(ledger, result, data_snapshot_hash=data_snapshot_hash)
    out = dict(result)
    out["ledger_record_hash"] = record.get("record_hash")
    out["ledger_sequence"] = record.get("sequence")
    out["ledger_status"] = verify_trial_ledger(ledger).to_dict()
    out["ledger_append_status"] = "appended"
    return out


def confirm_harness_ready(
    *,
    expected_snapshot_hash: str,
    store_dir: str | Path = DEFAULT_SHARADAR_DIR,
    readiness_path: str | Path = "HARNESS_READINESS.md",
    ledger_path: str | Path = DEFAULT_AGENT_RESEARCH_LEDGER,
) -> dict[str, Any]:
    store = SharadarStore(store_dir)
    manifest = _safe_json(Path(store_dir) / "manifest.json")
    edgar = _safe_json(Path(store_dir) / "edgar_validation.json")
    checklist = Path(readiness_path).read_text(encoding="utf-8") if Path(readiness_path).exists() else ""
    unchecked = [line.strip() for line in checklist.splitlines() if line.strip().startswith("- [ ]")]
    required_tables = ("ACTIONS", "DAILY", "SEP", "SF1", "SP500", "TICKERS")
    table_rows = {
        table: int(((manifest.get("tables") or {}).get(table) or {}).get("rows") or ((manifest.get("tables") or {}).get(table) or {}).get("row_count") or 0)
        for table in required_tables
    }
    summaries = _certified_summary_checks(expected_snapshot_hash)
    valuation = _valuation_spot_check()
    benchmark = _benchmark_spot_check(store)
    ledger_status = verify_trial_ledger(ledger_path).to_dict()
    blocking: list[str] = []
    if not bool(os.environ.get("NASDAQ_DATA_LINK_API_KEY")):
        blocking.append("NASDAQ_DATA_LINK_API_KEY_not_exported")
    if str(manifest.get("data_snapshot_hash") or "") != expected_snapshot_hash:
        blocking.append("manifest_snapshot_mismatch")
    if any(count <= 0 for count in table_rows.values()):
        blocking.append("missing_or_empty_sharadar_table")
    if str(edgar.get("status") or "").upper() != "PASS" or str(edgar.get("data_snapshot_hash") or "") != expected_snapshot_hash:
        blocking.append("edgar_not_pass_bound_to_snapshot")
    if unchecked:
        blocking.append("harness_readiness_unchecked_boxes")
    for item in summaries:
        if not item.get("ready"):
            blocking.append(f"summary_not_ready:{item.get('name')}")
    if not valuation.get("ok"):
        blocking.append("valuation_spot_check_failed")
    if not benchmark.get("ok"):
        blocking.append("benchmark_spot_check_failed")
    if not bool(ledger_status.get("valid")):
        blocking.append("ledger_invalid")
    return {
        "schema": "regime_harness_readiness_reconfirm.v1",
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "snapshot": expected_snapshot_hash,
        "environment": {
            "api_key_present": bool(os.environ.get("NASDAQ_DATA_LINK_API_KEY")),
            "manifest_snapshot": manifest.get("data_snapshot_hash"),
            "table_rows": table_rows,
            "edgar_status": edgar.get("status"),
            "edgar_snapshot": edgar.get("data_snapshot_hash"),
        },
        "checklist": {
            "readiness_path": str(readiness_path),
            "unchecked_count": len(unchecked),
            "unchecked": unchecked,
        },
        "certified_summaries": summaries,
        "manual_spot_checks": {
            "valuation": valuation,
            "benchmark": benchmark,
        },
        "ledger_status": ledger_status,
        "blocking_items": blocking,
        "verdict": "HARNESS READY" if not blocking else "NOT READY",
        "production_defaults_changed": False,
    }


def seed_basket_study_ledger(
    ledger_path: str | Path = DEFAULT_AGENT_RESEARCH_LEDGER,
    *,
    data_snapshot_hash: str,
    campaign_2006_2025: str | Path = "data/campaign/basket_construction_study_2006_2025/summary.json",
    campaign_1998_2015: str | Path = "data/campaign/basket_construction_study_1998_2015/summary.json",
) -> dict[str, Any]:
    ledger = Path(ledger_path)
    existing = _ledger_trial_ids(ledger)
    summaries = {
        "2006_2025": _safe_json(campaign_2006_2025),
        "1998_2015": _safe_json(campaign_1998_2015),
    }
    appended: list[dict[str, Any]] = []
    skipped: list[str] = []
    for seed in BASKET_SEED_TRIALS:
        trial_id = str(seed["trial_id"])
        if trial_id in existing:
            skipped.append(trial_id)
            continue
        arm = str(seed["arm"])
        trial = {
            "trial_id": trial_id,
            "source": "certified_basket_study_seed",
            "already_tested": True,
            "hypothesis": seed["hypothesis"],
            "arm": arm,
            "pre_registered": True,
            "verdict": seed["verdict"],
            "rationale": seed["summary"],
            "windows": {
                "2006_2025": _arm_snapshot(summaries["2006_2025"], arm),
                "1998_2015": _arm_snapshot(summaries["1998_2015"], arm),
            },
            "holdout_accessed": False,
            "production_defaults_changed": False,
        }
        appended.append(append_trial(ledger, trial, data_snapshot_hash=data_snapshot_hash))
        existing.add(trial_id)
    return {
        "ledger_path": str(ledger),
        "appended_count": len(appended),
        "skipped_existing": skipped,
        "trial_count": verify_trial_ledger(ledger).trial_count,
        "appended_trial_ids": [record["trial"]["trial_id"] for record in appended],
        "production_defaults_changed": False,
    }


def run_first_momentum_risk_overlay_iteration(
    ledger_path: str | Path = DEFAULT_AGENT_RESEARCH_LEDGER,
    *,
    data_snapshot_hash: str,
    research_dir: str | Path = DEFAULT_AGENT_RESEARCH_DIR,
    store_dir: str | Path = DEFAULT_SHARADAR_DIR,
    basket_path: str | Path = DEFAULT_BASKET_PATH,
    start: str = DEFAULT_AGENT_RESEARCH_DEV_START,
    end: str = DEFAULT_AGENT_RESEARCH_DEV_END,
    oos_start: str = DEFAULT_AGENT_RESEARCH_DEV_OOS_START,
) -> dict[str, Any]:
    ledger = Path(ledger_path)
    trial_id = "H001_A1_momentum_risk_overlay"
    existing = _ledger_trial_ids(ledger)
    if trial_id in existing:
        return {
            "trial_id": trial_id,
            "status": "already_recorded",
            "ledger_status": verify_trial_ledger(ledger).to_dict(),
            "production_defaults_changed": False,
        }
    store = SharadarStore(store_dir)
    if str(store.data_snapshot_hash) != str(data_snapshot_hash):
        raise ValueError("Snapshot changed; Stage 1 readiness must be rerun before Stage 2.")
    cfg = BasketStudyConfig(oos_start=oos_start)
    synth_sp500 = store.synth_sp500_total_return(start, end)
    if synth_sp500.empty:
        raise ValueError("Synthesized S&P 500 benchmark unavailable for DEV window.")
    benchmark_curve = _buy_hold_curve(synth_sp500, starting_cash=cfg.starting_cash)
    stress_windows = historical_stress_windows_for_range(start, end)
    basket = load_basket(basket_path) if Path(basket_path).exists() else {"tickers": []}
    base = run_basket_arm(
        store,
        "A1_pure_momentum",
        cfg,
        start=start,
        end=end,
        basket=basket,
        benchmark_curve=benchmark_curve,
        windows=stress_windows,
    )
    overlay = apply_momentum_risk_overlay(
        base,
        cfg,
        benchmark_curve=benchmark_curve,
        oos_start=oos_start,
        windows=stress_windows,
    )
    benchmark = buy_hold_taxable_payload(
        "SYNTH_SP500",
        synth_sp500,
        oos_start=oos_start,
        benchmark_curve=benchmark_curve,
        windows=stress_windows,
    )
    verdict = _first_hypothesis_verdict(overlay, benchmark)
    result = {
        "schema": "regime_agent_research_iteration.v1",
        "trial_id": trial_id,
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "data_snapshot_hash": data_snapshot_hash,
        "git_sha": _git_sha(),
        "hypothesis": "Adding an L1-style volatility-target and drawdown-brake overlay to A1 pure-momentum selection improves OOS risk-adjusted after-tax results.",
        "economic_rationale": "A1 was closest to passing in the 2006-2025 certified study but failed on drawdown pain; a risk overlay targets that failure without changing selection.",
        "pre_registered_success_criterion": "DEV OOS only: beat synthesized S&P 500 on after-tax total return, Calmar, and Ulcer. Significant gains are not sold by the overlay.",
        "dev_window": {"start": start, "end": end, "oos_start": oos_start},
        "locked_holdout": {
            "start": DEFAULT_AGENT_RESEARCH_HOLDOUT_START,
            "end": DEFAULT_AGENT_RESEARCH_HOLDOUT_END,
            "accessed": False,
        },
        "strategy_spec": overlay.get("strategy_spec"),
        "base_a1_metrics": _summary_metrics(base),
        "overlay_metrics": _summary_metrics(overlay),
        "benchmark_metrics": _summary_metrics(benchmark),
        "criterion_checks": verdict["criterion_checks"],
        "verdict": verdict["verdict"],
        "verdict_rationale": verdict["rationale"],
        "base_payload_path": str(Path(research_dir) / "iteration_001_A1_base_dev.json"),
        "overlay_payload_path": str(Path(research_dir) / "iteration_001_A1_risk_overlay_dev.json"),
        "benchmark_payload_path": str(Path(research_dir) / "iteration_001_synth_sp500_dev.json"),
        "production_defaults_changed": False,
    }
    root = Path(research_dir)
    root.mkdir(parents=True, exist_ok=True)
    _write_json(root / "iteration_001_A1_base_dev.json", base)
    _write_json(root / "iteration_001_A1_risk_overlay_dev.json", overlay)
    _write_json(root / "iteration_001_synth_sp500_dev.json", benchmark)
    _write_json(root / "iteration_001_result.json", result)
    ledger_record = append_trial(ledger, result, data_snapshot_hash=data_snapshot_hash)
    result["ledger_record_hash"] = ledger_record.get("record_hash")
    result["ledger_sequence"] = ledger_record.get("sequence")
    result["ledger_status"] = verify_trial_ledger(ledger).to_dict()
    _write_json(root / "iteration_001_result.json", result)
    return result


def apply_momentum_risk_overlay(
    base_payload: dict[str, Any],
    cfg: BasketStudyConfig,
    *,
    benchmark_curve: pd.DataFrame,
    oos_start: str,
    windows: Sequence[Any] | None = None,
    target_vol: float = 0.15,
    vol_window_days: int = 63,
    drawdown_brake_1: float = -0.10,
    drawdown_brake_1_exposure: float = 0.50,
    drawdown_brake_2: float = -0.20,
    drawdown_brake_2_exposure: float = 0.25,
    overlay_cost_bps: float = 2.0,
) -> dict[str, Any]:
    base_curve = pd.DataFrame(base_payload.get("after_tax_equity_curve") or [])
    if base_curve.empty:
        raise ValueError("A1 base payload has no after-tax equity curve.")
    base_curve = base_curve.copy()
    base_curve["date"] = pd.to_datetime(base_curve["date"])
    base_curve = base_curve.sort_values("date")
    base_curve["equity"] = pd.to_numeric(base_curve["equity"], errors="coerce")
    returns = base_curve["equity"].pct_change().fillna(0.0)
    realized_vol = returns.rolling(vol_window_days).std().fillna(returns.expanding().std()).fillna(0.0) * math.sqrt(252)
    raw_exposure = (target_vol / realized_vol.replace(0.0, float("nan"))).clip(lower=0.0, upper=1.0).fillna(1.0)
    equity = float(cfg.starting_cash)
    peak = equity
    previous_exposure = 1.0
    curve: list[dict[str, Any]] = []
    trades: list[dict[str, Any]] = []
    total_costs = 0.0
    total_turnover = 0.0
    for idx, row in base_curve.reset_index(drop=True).iterrows():
        date = pd.Timestamp(row["date"])
        if idx > 0:
            equity *= 1.0 + previous_exposure * float(returns.iloc[idx])
        peak = max(peak, equity)
        drawdown = equity / peak - 1.0 if peak > 0 else 0.0
        cap = 1.0
        if drawdown <= drawdown_brake_2:
            cap = drawdown_brake_2_exposure
        elif drawdown <= drawdown_brake_1:
            cap = drawdown_brake_1_exposure
        next_exposure = float(min(float(raw_exposure.iloc[idx]), cap))
        delta = next_exposure - previous_exposure
        cost = abs(delta) * equity * overlay_cost_bps / 10_000.0
        if cost > 0:
            equity -= cost
            total_costs += cost
            total_turnover += abs(delta) * equity / cfg.starting_cash
            trades.append(
                {
                    "date": date.date().isoformat(),
                    "ticker": "A1_RISK_OVERLAY",
                    "side": "OverlayIncrease" if delta > 0 else "OverlayDecrease",
                    "quantity": abs(delta),
                    "price": equity,
                    "notional": abs(delta) * equity,
                    "costs_paid": cost,
                    "net_pnl": -cost,
                    "exit_type": "vol_target_drawdown_brake",
                }
            )
        curve.append(
            {
                "date": date.date().isoformat(),
                "equity": equity,
                "cash": equity * (1.0 - next_exposure),
                "position_value": equity * next_exposure,
                "exposure": next_exposure,
                "base_a1_return": float(returns.iloc[idx]),
                "realized_vol": float(realized_vol.iloc[idx] or 0.0),
                "drawdown": drawdown,
                "costs_paid": cost,
                "turnover": abs(delta) * equity / cfg.starting_cash,
            }
        )
        previous_exposure = next_exposure
    metrics = _metrics(curve, trades, benchmark_curve=benchmark_curve)
    metrics["after_tax_terminal_wealth"] = curve[-1]["equity"] if curve else None
    metrics["pre_tax_terminal_wealth"] = curve[-1]["equity"] if curve else None
    metrics["total_costs_paid"] = total_costs
    metrics["annualized_turnover"] = total_turnover / max(len(curve) / 252.0, 1e-9)
    metrics["total_turnover"] = total_turnover
    oos_ts = pd.Timestamp(oos_start)
    payload = {
        "schema": "regime_agent_research_overlay_payload.v1",
        "arm": "H001_A1_momentum_risk_overlay",
        "strategy_spec": {
            "selection": "A1_pure_momentum",
            "overlay": "vol_target_drawdown_brake",
            "target_vol": target_vol,
            "vol_window_days": vol_window_days,
            "drawdown_brake_1": drawdown_brake_1,
            "drawdown_brake_1_exposure": drawdown_brake_1_exposure,
            "drawdown_brake_2": drawdown_brake_2,
            "drawdown_brake_2_exposure": drawdown_brake_2_exposure,
            "overlay_cost_bps": overlay_cost_bps,
            "underlying_tax_lot_sales_by_overlay": False,
            "significant_gains_held_long_term": True,
        },
        "config": cfg.to_dict(),
        "strategy_hash": _stable_hash({"arm": "H001_A1_momentum_risk_overlay", "config": cfg.to_dict(), "target_vol": target_vol}),
        "metrics": _json_safe(metrics),
        "pre_tax_metrics": _json_safe(metrics),
        "in_sample": _json_safe(_segment_metrics(curve, trades, benchmark_curve, None, oos_ts)),
        "out_of_sample": _json_safe(_segment_metrics(curve, trades, benchmark_curve, oos_ts, None)),
        "after_tax_equity_curve": _json_safe(curve),
        "equity_curve": _json_safe(curve),
        "trades": _json_safe(trades),
        "monthly_returns": _period_returns(curve, "M"),
        "yearly_returns": _period_returns(curve, "Y"),
        "stress_windows": _json_safe(_stress_results_for_curve(pd.DataFrame(curve), trades, benchmark_curve, list(windows or []))),
        "production_defaults_changed": False,
    }
    return payload


def run_dev_walk_forward_evaluation(
    strategy_payload: dict[str, Any],
    benchmark_payload: dict[str, Any],
    *,
    base_payload: dict[str, Any] | None = None,
    folds: Sequence[dict[str, Any]] = DEFAULT_WALK_FORWARD_FOLDS,
    dev_start: str = DEFAULT_AGENT_RESEARCH_DEV_START,
    dev_end: str = DEFAULT_AGENT_RESEARCH_DEV_END,
    holdout_start: str = DEFAULT_AGENT_RESEARCH_HOLDOUT_START,
    min_major_crashes: int = DEFAULT_WALK_FORWARD_MIN_MAJOR_CRASHES,
    min_oos_folds: int = DEFAULT_WALK_FORWARD_MIN_FOLDS,
) -> dict[str, Any]:
    """Score one hypothesis across fixed DEV-only OOS folds."""

    dev_start_ts = pd.Timestamp(dev_start)
    dev_end_ts = pd.Timestamp(dev_end)
    holdout_ts = pd.Timestamp(holdout_start)
    strategy_curve = _payload_curve(strategy_payload)
    benchmark_curve = _payload_curve(benchmark_payload)
    base_curve = _payload_curve(base_payload or {}) if base_payload is not None else []
    strategy_trades = [dict(row) for row in strategy_payload.get("trades") or [] if isinstance(row, dict)]
    benchmark_trades = [dict(row) for row in benchmark_payload.get("trades") or [] if isinstance(row, dict)]
    base_trades = [dict(row) for row in (base_payload or {}).get("trades") or [] if isinstance(row, dict)]
    fold_rows: list[dict[str, Any]] = []
    for fold in folds:
        start_ts = pd.Timestamp(fold["oos_start"])
        end_inclusive = pd.Timestamp(fold["oos_end"])
        end_exclusive = end_inclusive + pd.Timedelta(days=1)
        row: dict[str, Any] = {
            "fold_id": str(fold.get("fold_id") or ""),
            "train_through": str(fold.get("train_through") or ""),
            "oos_start": start_ts.date().isoformat(),
            "oos_end": end_inclusive.date().isoformat(),
            "stress_label": str(fold.get("stress_label") or ""),
            "major_crash": bool(fold.get("major_crash", False)),
            "holdout_accessed": False,
        }
        if start_ts < dev_start_ts or end_inclusive > dev_end_ts or start_ts >= holdout_ts or end_inclusive >= holdout_ts:
            row.update({"status": "excluded_outside_dev_or_holdout_overlap"})
            fold_rows.append(row)
            continue
        strategy_count = _segment_row_count(strategy_curve, start_ts, end_exclusive)
        benchmark_count = _segment_row_count(benchmark_curve, start_ts, end_exclusive)
        if strategy_count < 2 or benchmark_count < 2:
            row.update(
                {
                    "status": "insufficient_history",
                    "strategy_rows": strategy_count,
                    "benchmark_rows": benchmark_count,
                    "effective_strategy_start": _effective_segment_start(strategy_curve, start_ts, end_exclusive),
                    "effective_benchmark_start": _effective_segment_start(benchmark_curve, start_ts, end_exclusive),
                }
            )
            fold_rows.append(row)
            continue
        strategy_metrics = _segment_metrics(strategy_curve, strategy_trades, None, start_ts, end_exclusive)
        benchmark_metrics = _segment_metrics(benchmark_curve, benchmark_trades, None, start_ts, end_exclusive)
        base_metrics = _segment_metrics(base_curve, base_trades, None, start_ts, end_exclusive) if base_curve else {}
        total_return_delta = _metric_delta(strategy_metrics, benchmark_metrics, "total_return")
        calmar_delta = _metric_delta(strategy_metrics, benchmark_metrics, "calmar_ratio")
        ulcer_delta = _metric_delta(strategy_metrics, benchmark_metrics, "ulcer_index")
        max_drawdown_delta_vs_base = _metric_delta(strategy_metrics, base_metrics, "max_drawdown") if base_metrics else None
        ulcer_delta_vs_base = _metric_delta(strategy_metrics, base_metrics, "ulcer_index") if base_metrics else None
        beats_return = total_return_delta is not None and total_return_delta > 0
        beats_calmar = calmar_delta is not None and calmar_delta > 0
        beats_ulcer = ulcer_delta is not None and ulcer_delta < 0
        crash_risk_improved = (
            bool(fold.get("major_crash", False))
            and max_drawdown_delta_vs_base is not None
            and ulcer_delta_vs_base is not None
            and max_drawdown_delta_vs_base > 0
            and ulcer_delta_vs_base < 0
        )
        row.update(
            {
                "status": "included",
                "strategy_rows": strategy_count,
                "benchmark_rows": benchmark_count,
                "strategy_total_return": strategy_metrics.get("total_return"),
                "benchmark_total_return": benchmark_metrics.get("total_return"),
                "total_return_delta": total_return_delta,
                "strategy_calmar_ratio": strategy_metrics.get("calmar_ratio"),
                "benchmark_calmar_ratio": benchmark_metrics.get("calmar_ratio"),
                "calmar_delta": calmar_delta,
                "strategy_ulcer_index": strategy_metrics.get("ulcer_index"),
                "benchmark_ulcer_index": benchmark_metrics.get("ulcer_index"),
                "ulcer_delta": ulcer_delta,
                "strategy_max_drawdown": strategy_metrics.get("max_drawdown"),
                "benchmark_max_drawdown": benchmark_metrics.get("max_drawdown"),
                "base_a1_max_drawdown": base_metrics.get("max_drawdown"),
                "base_a1_ulcer_index": base_metrics.get("ulcer_index"),
                "max_drawdown_delta_vs_base_a1": max_drawdown_delta_vs_base,
                "ulcer_delta_vs_base_a1": ulcer_delta_vs_base,
                "beats_index_total_return": beats_return,
                "beats_index_calmar": beats_calmar,
                "beats_index_ulcer": beats_ulcer,
                "beats_index_metric_count": sum(1 for value in (beats_return, beats_calmar, beats_ulcer) if value),
                "clears_full_metric_set": bool(beats_return and beats_calmar and beats_ulcer),
                "crash_risk_improved_vs_bare_a1": crash_risk_improved,
            }
        )
        fold_rows.append(row)
    aggregate = score_walk_forward_fold_distribution(
        fold_rows,
        min_major_crashes=min_major_crashes,
        min_oos_folds=min_oos_folds,
    )
    return {
        "schema": "regime_agent_research_walk_forward_oos.v1",
        "oos_evaluation_mode": "walk_forward_stress_folds",
        "dev_window": {"start": dev_start, "end": dev_end},
        "holdout_window": {
            "start": holdout_start,
            "accessed": False,
        },
        "folds": _json_safe(fold_rows),
        "aggregate": aggregate,
        "verdict": aggregate["verdict"],
        "verdict_rationale": aggregate["verdict_rationale"],
        "production_defaults_changed": False,
    }


def score_walk_forward_fold_distribution(
    fold_rows: Sequence[dict[str, Any]],
    *,
    min_major_crashes: int = DEFAULT_WALK_FORWARD_MIN_MAJOR_CRASHES,
    min_oos_folds: int = DEFAULT_WALK_FORWARD_MIN_FOLDS,
) -> dict[str, Any]:
    included = [dict(row) for row in fold_rows if str(row.get("status") or "") == "included"]
    included_count = len(included)
    full_pass_count = sum(1 for row in included if bool(row.get("clears_full_metric_set")))
    two_of_three_count = sum(1 for row in included if int(row.get("beats_index_metric_count") or 0) >= 2)
    major_crash_rows = [row for row in included if bool(row.get("major_crash"))]
    major_crash_count = len(major_crash_rows)
    crash_risk_improved_count = sum(1 for row in major_crash_rows if bool(row.get("crash_risk_improved_vs_bare_a1")))
    full_pass_fraction = full_pass_count / included_count if included_count else 0.0
    two_of_three_fraction = two_of_three_count / included_count if included_count else 0.0
    median_total_return_delta = _median_metric(included, "total_return_delta")
    median_calmar_delta = _median_metric(included, "calmar_delta")
    median_ulcer_delta = _median_metric(included, "ulcer_delta")
    median_clears = (
        median_total_return_delta is not None
        and median_calmar_delta is not None
        and median_ulcer_delta is not None
        and median_total_return_delta > 0
        and median_calmar_delta > 0
        and median_ulcer_delta < 0
    )
    crash_risk_pass = (
        major_crash_count >= min_major_crashes
        and crash_risk_improved_count / major_crash_count > 0.5
    )
    prelim_promising = (
        included_count >= min_oos_folds
        and major_crash_count >= min_major_crashes
        and full_pass_fraction > 0.5
        and median_clears
        and crash_risk_pass
    )
    best_fold_id, drop_best_flips = _drop_best_fold_sensitivity(included, prelim_promising, min_major_crashes=min_major_crashes, min_oos_folds=min_oos_folds)
    single_fold_luck = included_count > 1 and full_pass_count == 1 and any((_float(row.get("total_return_delta"), 0.0) or 0.0) > 0 for row in included)
    concentration_flag = bool(drop_best_flips or single_fold_luck)
    reasons: list[str] = []
    if included_count < min_oos_folds:
        reasons.append("insufficient_fold_count")
    if major_crash_count < min_major_crashes:
        reasons.append("insufficient_crash_coverage")
    if concentration_flag:
        reasons.append("single_fold_concentration")
    if prelim_promising and not concentration_flag:
        verdict = "promising"
        reasons.append("majority_folds_and_medians_clear_bar")
    elif included_count < min_oos_folds or major_crash_count < min_major_crashes:
        verdict = "inconclusive"
    elif full_pass_fraction <= 0.5:
        verdict = "killed"
        reasons.append("fails_full_metric_set_in_majority_of_folds")
    else:
        verdict = "inconclusive"
        reasons.append("mixed_fold_distribution")
    return {
        "included_fold_count": included_count,
        "configured_fold_count": len(list(fold_rows)),
        "min_oos_folds": min_oos_folds,
        "major_crash_fold_count": major_crash_count,
        "min_major_crashes": min_major_crashes,
        "folds_clearing_full_metric_set": full_pass_count,
        "full_metric_set_pass_fraction": full_pass_fraction,
        "folds_beating_two_of_three": two_of_three_count,
        "two_of_three_fraction": two_of_three_fraction,
        "median_total_return_delta": median_total_return_delta,
        "median_calmar_delta": median_calmar_delta,
        "median_ulcer_delta": median_ulcer_delta,
        "median_clears_full_bar": median_clears,
        "major_crash_risk_improved_count": crash_risk_improved_count,
        "crash_risk_pass": crash_risk_pass,
        "single_fold_concentration_flag": concentration_flag,
        "drop_best_fold_flips_verdict": drop_best_flips,
        "best_fold_id": best_fold_id,
        "verdict": verdict,
        "verdict_reasons": reasons,
        "verdict_rationale": "; ".join(reasons) if reasons else "No verdict reason recorded.",
    }


def _payload_curve(payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows = payload.get("after_tax_equity_curve") or payload.get("equity_curve") or []
    out: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        if row.get("date") is None or row.get("equity") is None:
            continue
        try:
            ts = pd.Timestamp(row["date"])
            equity = float(row["equity"])
        except Exception:
            continue
        if not math.isfinite(equity):
            continue
        item = dict(row)
        item["date"] = ts.date().isoformat()
        item["equity"] = equity
        out.append(item)
    out.sort(key=lambda item: str(item["date"]))
    return out


def _segment_row_count(curve: Sequence[dict[str, Any]], start: pd.Timestamp, end: pd.Timestamp) -> int:
    count = 0
    for row in curve:
        try:
            ts = pd.Timestamp(row["date"])
        except Exception:
            continue
        if start <= ts < end:
            count += 1
    return count


def _effective_segment_start(curve: Sequence[dict[str, Any]], start: pd.Timestamp, end: pd.Timestamp) -> str | None:
    dates: list[pd.Timestamp] = []
    for row in curve:
        try:
            ts = pd.Timestamp(row["date"])
        except Exception:
            continue
        if start <= ts < end:
            dates.append(ts)
    return min(dates).date().isoformat() if dates else None


def _metric_delta(left: dict[str, Any], right: dict[str, Any], key: str) -> float | None:
    left_value = _float_or_none(left.get(key))
    right_value = _float_or_none(right.get(key))
    if left_value is None or right_value is None:
        return None
    return left_value - right_value


def _median_metric(rows: Sequence[dict[str, Any]], key: str) -> float | None:
    values = [_float_or_none(row.get(key)) for row in rows]
    parsed = sorted(value for value in values if value is not None)
    if not parsed:
        return None
    mid = len(parsed) // 2
    if len(parsed) % 2:
        return float(parsed[mid])
    return float((parsed[mid - 1] + parsed[mid]) / 2.0)


def _float_or_none(value: Any) -> float | None:
    try:
        parsed = float(value)
    except Exception:
        return None
    return parsed if math.isfinite(parsed) else None


def _drop_best_fold_sensitivity(
    included: Sequence[dict[str, Any]],
    prelim_promising: bool,
    *,
    min_major_crashes: int,
    min_oos_folds: int,
) -> tuple[str | None, bool]:
    if not included:
        return None, False
    best = max(included, key=_best_fold_key)
    best_fold_id = str(best.get("fold_id") or "")
    if not prelim_promising:
        return best_fold_id, False
    remaining = [row for row in included if row is not best]
    still_promising = _fold_distribution_prelim_promising(
        remaining,
        min_major_crashes=min_major_crashes,
        min_oos_folds=min_oos_folds,
    )
    return best_fold_id, not still_promising


def _best_fold_key(row: dict[str, Any]) -> tuple[int, float, float, float]:
    return (
        int(row.get("beats_index_metric_count") or 0),
        _float(row.get("total_return_delta"), -math.inf),
        _float(row.get("calmar_delta"), -math.inf),
        -_float(row.get("ulcer_delta"), math.inf),
    )


def _fold_distribution_prelim_promising(
    rows: Sequence[dict[str, Any]],
    *,
    min_major_crashes: int,
    min_oos_folds: int,
) -> bool:
    included = [row for row in rows if str(row.get("status") or "included") == "included"]
    if len(included) < min_oos_folds:
        return False
    major = [row for row in included if bool(row.get("major_crash"))]
    if len(major) < min_major_crashes:
        return False
    full_pass_count = sum(1 for row in included if bool(row.get("clears_full_metric_set")))
    if full_pass_count / len(included) <= 0.5:
        return False
    median_total = _median_metric(included, "total_return_delta")
    median_calmar = _median_metric(included, "calmar_delta")
    median_ulcer = _median_metric(included, "ulcer_delta")
    if median_total is None or median_calmar is None or median_ulcer is None:
        return False
    if not (median_total > 0 and median_calmar > 0 and median_ulcer < 0):
        return False
    crash_improved = sum(1 for row in major if bool(row.get("crash_risk_improved_vs_bare_a1")))
    return crash_improved / len(major) > 0.5


def _write_walk_forward_markdown(path: Path, result: dict[str, Any]) -> None:
    walk = result.get("walk_forward") or {}
    aggregate = walk.get("aggregate") or {}
    folds = [row for row in walk.get("folds") or [] if isinstance(row, dict)]
    included = [row for row in folds if str(row.get("status") or "") == "included"]
    lines = [
        "# Agent Research Loop - H001 Walk-Forward Re-score",
        "",
        f"Trial: `{result.get('trial_id')}`",
        f"Snapshot: `{result.get('data_snapshot_hash')}`",
        f"Evaluation mode: `{result.get('oos_evaluation_mode')}`",
        f"Verdict: `{result.get('verdict')}`",
        f"Rationale: {result.get('verdict_rationale')}",
        f"Holdout accessed: `{result.get('holdout_accessed')}`",
        f"Production defaults changed: `{result.get('production_defaults_changed')}`",
        "",
        "## Aggregate",
        "",
        f"- Included folds: {aggregate.get('included_fold_count')} / {aggregate.get('configured_fold_count')}",
        f"- Major crash folds: {aggregate.get('major_crash_fold_count')} / {aggregate.get('min_major_crashes')}",
        f"- Full metric pass fraction: {aggregate.get('full_metric_set_pass_fraction')}",
        f"- Median total-return delta: {aggregate.get('median_total_return_delta')}",
        f"- Median Calmar delta: {aggregate.get('median_calmar_delta')}",
        f"- Median Ulcer delta: {aggregate.get('median_ulcer_delta')}",
        f"- Single-fold concentration flag: {aggregate.get('single_fold_concentration_flag')}",
        f"- Drop-best-fold flips verdict: {aggregate.get('drop_best_fold_flips_verdict')} ({aggregate.get('best_fold_id')})",
        "",
        "## Per-Fold Results",
        "",
        "| Fold | Stress | Full pass | Return delta | Calmar delta | Ulcer delta | Crash risk improved |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for row in included:
        lines.append(
            "| {fold} | {stress} | {full} | {ret} | {calmar} | {ulcer} | {risk} |".format(
                fold=row.get("fold_id"),
                stress=row.get("stress_label"),
                full=row.get("clears_full_metric_set"),
                ret=row.get("total_return_delta"),
                calmar=row.get("calmar_delta"),
                ulcer=row.get("ulcer_delta"),
                risk=row.get("crash_risk_improved_vs_bare_a1"),
            )
        )
    lines.extend(
        [
            "",
            "## Pause",
            "",
            "The loop remains paused for human review. This corrective trial appends to the ledger and does not edit the original single-window trial.",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def _certified_summary_checks(expected_snapshot_hash: str) -> list[dict[str, Any]]:
    paths = [
        ("basket_construction_study_2006_2025", Path("data/campaign/basket_construction_study_2006_2025/summary.json")),
        ("basket_construction_study_1998_2015", Path("data/campaign/basket_construction_study_1998_2015/summary.json")),
    ]
    out: list[dict[str, Any]] = []
    for name, path in paths:
        data = _safe_json(path)
        ready = (
            str(data.get("data_snapshot_hash") or "") == expected_snapshot_hash
            and str(data.get("data_readiness") or "") == "survivorship_free"
            and str(data.get("gate_status") or "") == "certifiable"
            and str(((data.get("edgar_validation") or {}).get("status") or "")).upper() == "PASS"
            and not bool(data.get("production_defaults_changed"))
        )
        out.append(
            {
                "name": name,
                "path": str(path),
                "ready": ready,
                "snapshot": data.get("data_snapshot_hash"),
                "data_readiness": data.get("data_readiness"),
                "gate_status": data.get("gate_status"),
                "edgar_status": (data.get("edgar_validation") or {}).get("status"),
                "production_defaults_changed": data.get("production_defaults_changed"),
            }
        )
    return out


def _valuation_spot_check() -> dict[str, Any]:
    paths = [
        Path("data/campaign/basket_construction_study_2006_2025/results/A1_pure_momentum.json"),
        Path("data/campaign/basket_construction_study_2006_2025/results/C0b_static_pit.json"),
        Path("data/campaign/basket_construction_study_1998_2015/results/A3_momentum_valuation_cap.json"),
    ]
    rows: list[dict[str, Any]] = []
    ok = True
    for path in paths:
        payload = _safe_json(path)
        diag = dict(payload.get("valuation_diagnostics") or payload.get("metrics") or {})
        unresolved = int(diag.get("unresolved_mark_count") or 0)
        zero = int(diag.get("zero_mark_count") or 0)
        rows.append({"path": str(path), "unresolved_mark_count": unresolved, "zero_mark_count": zero})
        if unresolved != 0 or zero != 0:
            ok = False
    return {"ok": ok, "rows": rows}


def _benchmark_spot_check(store: SharadarStore) -> dict[str, Any]:
    frame = store.synth_sp500_total_return("2020-01-01", "2023-12-31")
    if frame.empty:
        return {"ok": False, "reason": "empty_synth_sp500"}
    normalized = frame.sort_index()
    start = float(normalized["price"].iloc[0])
    end = float(normalized["price"].iloc[-1])
    total_return = end / start - 1.0 if start > 0 else None
    return {
        "ok": start > 0 and end > 0 and total_return is not None and -0.8 < total_return < 3.0,
        "start": normalized.index[0].date().isoformat(),
        "end": normalized.index[-1].date().isoformat(),
        "total_return": total_return,
    }


def _ledger_trial_ids(path: Path) -> set[str]:
    if not path.exists():
        return set()
    ids: set[str] = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        trial = record.get("trial")
        if isinstance(trial, dict) and trial.get("trial_id"):
            ids.add(str(trial["trial_id"]))
    return ids


def _arm_snapshot(summary: dict[str, Any], arm: str) -> dict[str, Any]:
    row = next((dict(item) for item in summary.get("rows") or [] if str(item.get("arm") or "") == arm), {})
    verdict = ((summary.get("verdict") or {}).get("arm_verdicts") or {}).get(arm) or {}
    return {
        "window": f"{summary.get('start')}:{summary.get('end')}",
        "data_readiness": summary.get("data_readiness"),
        "gate_status": summary.get("gate_status"),
        "after_tax_terminal_wealth": row.get("after_tax_terminal_wealth"),
        "annualized_return": row.get("annualized_return"),
        "max_drawdown": row.get("max_drawdown"),
        "ulcer_index": row.get("ulcer_index"),
        "oos_total_return": verdict.get("oos_total_return"),
        "oos_calmar_ratio": verdict.get("oos_calmar_ratio"),
        "oos_ulcer_index": verdict.get("oos_ulcer_index"),
        "arm_status": verdict.get("status"),
    }


def _first_hypothesis_verdict(overlay: dict[str, Any], benchmark: dict[str, Any]) -> dict[str, Any]:
    left = dict(overlay.get("out_of_sample") or {})
    right = dict(benchmark.get("out_of_sample") or {})
    checks = {
        "after_tax_oos_total_return_beats_index": _float(left.get("total_return"), -math.inf) > _float(right.get("total_return"), math.inf),
        "after_tax_oos_calmar_beats_index": _float(left.get("calmar_ratio"), -math.inf) > _float(right.get("calmar_ratio"), math.inf),
        "after_tax_oos_ulcer_beats_index": _float(left.get("ulcer_index"), math.inf) < _float(right.get("ulcer_index"), -math.inf),
    }
    passed = sum(1 for value in checks.values() if value)
    if passed == 3:
        verdict = "promising"
        rationale = "DEV OOS criterion passed on all three pre-registered metrics; stop for human review before any holdout access."
    elif passed == 2:
        verdict = "inconclusive"
        rationale = "DEV OOS criterion partially passed but did not clear all three pre-registered metrics."
    else:
        verdict = "killed"
        rationale = "DEV OOS criterion failed on two or more pre-registered metrics."
    return {"verdict": verdict, "criterion_checks": checks, "rationale": rationale}


def _summary_metrics(payload: dict[str, Any]) -> dict[str, Any]:
    metrics = dict(payload.get("metrics") or {})
    oos = dict(payload.get("out_of_sample") or {})
    return {
        "after_tax_terminal_wealth": metrics.get("after_tax_terminal_wealth"),
        "annualized_return": metrics.get("annualized_return"),
        "max_drawdown": metrics.get("max_drawdown"),
        "calmar_ratio": metrics.get("calmar_ratio"),
        "ulcer_index": metrics.get("ulcer_index"),
        "annualized_turnover": metrics.get("annualized_turnover"),
        "total_costs_paid": metrics.get("total_costs_paid"),
        "trade_count": metrics.get("trade_count"),
        "oos_total_return": oos.get("total_return"),
        "oos_calmar_ratio": oos.get("calmar_ratio"),
        "oos_ulcer_index": oos.get("ulcer_index"),
    }


def _write_iteration_markdown(path: Path, payload: dict[str, Any]) -> None:
    readiness = payload.get("readiness") or {}
    iteration = payload.get("first_iteration") or {}
    lines = [
        "# Agent Research Loop - Supervised Iteration 1",
        "",
        f"Snapshot: `{payload.get('snapshot')}`",
        f"Gate: `{readiness.get('verdict')}`",
        f"Ledger: `{payload.get('ledger_path')}`",
        f"Trial count: `{(payload.get('ledger_status') or {}).get('trial_count')}`",
        f"Holdout accessed: `{(payload.get('holdout_window') or {}).get('accessed')}`",
        f"Production defaults changed: `{payload.get('production_defaults_changed')}`",
        "",
        "## Hypothesis",
        "",
        str(iteration.get("hypothesis") or "Not run."),
        "",
        "## Verdict",
        "",
        f"`{iteration.get('verdict')}` - {iteration.get('verdict_rationale')}",
        "",
        "## Pause",
        "",
        "Loop is paused for human review. No certified winner is declared.",
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def _safe_json(path: str | Path) -> dict[str, Any]:
    try:
        target = Path(path)
        if not target.exists():
            return {}
        payload = _read_json(target)
        return dict(payload) if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _float(value: Any, default: float) -> float:
    try:
        parsed = float(value)
        return parsed if math.isfinite(parsed) else default
    except Exception:
        return default
