from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from typing import Any

import pandas as pd

from .alpha_campaign import CAMPAIGN_OOS_START, DEFAULT_BASKET_PATH, DEFAULT_CAMPAIGN_DIR, _git_sha, _json_safe, _load_frame, _read_json, _write_json, load_basket
from .portfolio_backtest import PortfolioBacktestConfig, PortfolioBacktestResult, control_specs, run_portfolio_backtest
from .strategy import StrategySpec
from .strategy.layers import RegimeHMMSignalProvider

DEFAULT_CAMPAIGN2_DIR = DEFAULT_CAMPAIGN_DIR / "portfolio_campaign2"
DEFAULT_CAMPAIGN2_REPORT_PATH = Path("ALPHA_CAMPAIGN_2_REPORT.md")
COMPLEXITY_VERDICT = "the HMM brake does not yet pay for its complexity"


def campaign2_headline_specs() -> dict[str, StrategySpec]:
    return {
        "L0": StrategySpec(
            name="L0",
            signal_provider="precomputed_regime",
            exposure_policy="always_full",
            allocation_policy="equal_weight",
            rebalance_policy="monthly_bands",
            description="Equal-weight basket, fully invested.",
        ),
        "L1": StrategySpec(
            name="L1",
            signal_provider="precomputed_regime",
            exposure_policy="vol_target",
            exposure_params={"target_vol": 0.15, "min_exposure": 0.25},
            allocation_policy="equal_weight",
            rebalance_policy="monthly_bands",
            description="L0 plus portfolio-level volatility target.",
        ),
        "L2": StrategySpec(
            name="L2",
            signal_provider="precomputed_regime",
            exposure_policy="vol_target",
            exposure_params={"target_vol": 0.15, "min_exposure": 0.25},
            override_policy="regime_brake",
            override_params={"breadth_trigger": 0.5, "breadth_cap": 0.5, "aux_dd_trigger": 0.08, "aux_cap": 0.5, "reentry_days": 3},
            allocation_policy="equal_weight",
            rebalance_policy="monthly_bands",
            description="L1 plus HMM regime brake.",
        ),
        "L3": StrategySpec(
            name="L3",
            signal_provider="precomputed_regime",
            exposure_policy="vol_target",
            exposure_params={"target_vol": 0.15, "min_exposure": 0.25},
            override_policy="regime_brake",
            override_params={"breadth_trigger": 0.5, "breadth_cap": 0.5, "aux_dd_trigger": 0.08, "aux_cap": 0.5, "reentry_days": 3},
            allocation_policy="momentum_tilt",
            allocation_params={"top_fraction": 0.5},
            rebalance_policy="monthly_bands",
            description="L2 plus 12-1 cross-sectional momentum tilt.",
        ),
        **control_specs(),
    }


def campaign2_sensitivity_specs() -> dict[str, StrategySpec]:
    specs: dict[str, StrategySpec] = {}
    for target_vol in (0.12, 0.15, 0.18):
        specs[f"S_vol_{target_vol:.2f}"] = StrategySpec(
            name=f"S_vol_{target_vol:.2f}",
            signal_provider="precomputed_regime",
            exposure_policy="vol_target",
            exposure_params={"target_vol": target_vol, "min_exposure": 0.25},
            allocation_policy="equal_weight",
            rebalance_policy="monthly_bands",
        )
    for aux_dd in (0.06, 0.08, 0.10):
        for reentry_days in (3, 5):
            specs[f"S_brake_dd_{int(aux_dd * 100)}_reentry_{reentry_days}"] = StrategySpec(
                name=f"S_brake_dd_{int(aux_dd * 100)}_reentry_{reentry_days}",
                signal_provider="precomputed_regime",
                exposure_policy="vol_target",
                exposure_params={"target_vol": 0.15, "min_exposure": 0.25},
                override_policy="regime_brake",
                override_params={
                    "breadth_trigger": 0.5,
                    "breadth_cap": 0.5,
                    "aux_dd_trigger": aux_dd,
                    "aux_cap": 0.5,
                    "reentry_days": reentry_days,
                },
                allocation_policy="equal_weight",
                rebalance_policy="monthly_bands",
            )
    for top_fraction in (0.33, 0.5):
        specs[f"S_momentum_top_{top_fraction:.2f}"] = StrategySpec(
            name=f"S_momentum_top_{top_fraction:.2f}",
            signal_provider="precomputed_regime",
            exposure_policy="vol_target",
            exposure_params={"target_vol": 0.15, "min_exposure": 0.25},
            override_policy="regime_brake",
            override_params={"breadth_trigger": 0.5, "breadth_cap": 0.5, "aux_dd_trigger": 0.08, "aux_cap": 0.5, "reentry_days": 3},
            allocation_policy="momentum_tilt",
            allocation_params={"top_fraction": top_fraction},
            rebalance_policy="monthly_bands",
        )
    return specs


def run_campaign2(
    *,
    basket_path: str | Path = DEFAULT_BASKET_PATH,
    campaign_dir: str | Path = DEFAULT_CAMPAIGN2_DIR,
    resume: bool = False,
    frame_loader: Any | None = None,
) -> dict[str, Any]:
    started = dt.datetime.now(dt.timezone.utc)
    root = Path(campaign_dir)
    result_dir = root / "results"
    result_dir.mkdir(parents=True, exist_ok=True)
    basket = load_basket(basket_path)
    tickers = [str(ticker).upper() for ticker in basket.get("tickers") or []]
    if not tickers:
        raise ValueError("Campaign 2 requires a pinned basket with tickers.")
    loader = frame_loader or _load_frame
    raw_frames = {ticker: loader(ticker) for ticker in tickers}
    frames = _enrich_regime_frames(raw_frames)
    spy_frame = loader("SPY")
    _write_json(
        root / "cache_manifest.json",
        {
            "schema": "regime_portfolio_campaign2_cache.v1",
            "generated_at": started.isoformat(),
            "git_sha": _git_sha(),
            "tickers": tickers,
            "benchmark": "SPY",
            "oos_start": CAMPAIGN_OOS_START,
            "frozen_cache_note": "Market frames loaded once by ticker for the campaign run.",
        },
    )
    config = PortfolioBacktestConfig(oos_start=CAMPAIGN_OOS_START)
    headline_specs = campaign2_headline_specs()
    sensitivity_specs = campaign2_sensitivity_specs()
    results: dict[str, dict[str, Any]] = {}
    for arm, spec in {**headline_specs, **sensitivity_specs}.items():
        output = result_dir / f"{_safe_name(arm)}.json"
        if resume and output.exists():
            results[arm] = dict(_read_json(output))
            continue
        arm_frames = {"SPY": spy_frame} if arm.startswith("C") else frames
        result = run_portfolio_backtest(arm_frames, spec, config)
        payload = result.to_dict()
        payload["campaign"] = {"campaign": 2, "arm": arm, "git_sha": _git_sha(), "generated_at": dt.datetime.now(dt.timezone.utc).isoformat()}
        _write_json(output, payload)
        results[arm] = payload
    verdict = campaign2_verdict(results)
    winning_arm = str(verdict.get("best_supported_arm") or "L0")
    cost_output = result_dir / f"{_safe_name(winning_arm)}__cost_2x.json"
    if resume and cost_output.exists():
        cost_result = dict(_read_json(cost_output))
    else:
        winning_spec = headline_specs.get(winning_arm) or sensitivity_specs.get(winning_arm) or headline_specs["L0"]
        arm_frames = {"SPY": spy_frame} if winning_arm.startswith("C") else frames
        doubled = PortfolioBacktestConfig(oos_start=CAMPAIGN_OOS_START, entry_cost_bps=10.0, exit_cost_bps=10.0)
        cost_result_obj = run_portfolio_backtest(arm_frames, winning_spec, doubled)
        cost_result = cost_result_obj.to_dict()
        cost_result["campaign"] = {"campaign": 2, "arm": f"{winning_arm}_cost_2x", "git_sha": _git_sha(), "generated_at": dt.datetime.now(dt.timezone.utc).isoformat()}
        _write_json(cost_output, cost_result)
    verdict = campaign2_verdict(results, cost_fragility_result=cost_result)
    finished = dt.datetime.now(dt.timezone.utc)
    summary = {
        "schema": "regime_portfolio_campaign2_summary.v1",
        "generated_at": finished.isoformat(),
        "git_sha": _git_sha(),
        "wall_clock_seconds": (finished - started).total_seconds(),
        "basket_path": str(basket_path),
        "basket_size": len(tickers),
        "tickers": tickers,
        "oos_start": CAMPAIGN_OOS_START,
        "headline_arms": list(headline_specs),
        "sensitivity_arms": list(sensitivity_specs),
        "configurations_evaluated": len(results) + 1,
        "rows": [_campaign_row(arm, payload) for arm, payload in results.items()],
        "verdict": verdict,
        "cost_fragility_result": _campaign_row(f"{winning_arm}_cost_2x", cost_result),
    }
    _write_json(root / "summary.json", summary)
    _append_run_log(root, f"Campaign 2 completed in {summary['wall_clock_seconds']:.1f}s with {summary['configurations_evaluated']} configurations.")
    return summary


def campaign2_verdict(results: dict[str, dict[str, Any]], cost_fragility_result: dict[str, Any] | None = None) -> dict[str, Any]:
    rows = {arm: _campaign_row(arm, payload) for arm, payload in results.items()}
    layer_pairs = [("L1", "L0"), ("L2", "L1"), ("L3", "L2")]
    support: dict[str, dict[str, Any]] = {}
    best_supported = "L0"
    for layer, previous in layer_pairs:
        supported = _layer_supported(rows.get(layer, {}), rows.get(previous, {}))
        support[layer] = supported
        if supported["supported"] and best_supported == previous:
            best_supported = layer
    c2 = rows.get("C2_spy_200dma", {})
    best = rows.get(best_supported, {})
    best_sharpe = _float(best.get("oos_sharpe_ratio"))
    c2_sharpe = _float(c2.get("oos_sharpe_ratio"))
    control_verdict = "best supported stack beats spy_200dma"
    if best_sharpe is None or c2_sharpe is None or best_sharpe <= c2_sharpe:
        control_verdict = COMPLEXITY_VERDICT
    stress = _stress_preservation(rows, best_supported)
    cost_fragility = "not_run"
    if cost_fragility_result is not None:
        cost_row = _campaign_row(f"{best_supported}_cost_2x", cost_fragility_result)
        supported_at_2x = _layer_supported(cost_row, rows.get("L0", {}))["supported"]
        cost_fragility = "cost-fragile" if not supported_at_2x and best_supported != "L0" else "not_cost_fragile"
    return {
        "layer_support": support,
        "best_supported_arm": best_supported,
        "control_verdict": control_verdict,
        "stress_preservation": stress,
        "cost_fragility": cost_fragility,
        "recommended_default_changes": [],
    }


def render_campaign2_report(
    *,
    campaign_dir: str | Path = DEFAULT_CAMPAIGN2_DIR,
    output_path: str | Path = DEFAULT_CAMPAIGN2_REPORT_PATH,
) -> str:
    summary_path = Path(campaign_dir) / "summary.json"
    if not summary_path.exists():
        raise FileNotFoundError(f"Campaign 2 summary not found: {summary_path}")
    summary = dict(_read_json(summary_path))
    verdict = dict(summary.get("verdict") or {})
    rows = list(summary.get("rows") or [])
    best_return_arm = _best_return_arm(rows)
    best_supported = verdict.get("best_supported_arm") or "none"
    if best_return_arm == "L0":
        honesty = f"L0 wins raw OOS return; `{best_supported}` is the best supported arm under the pre-registered promotion rules."
    else:
        honesty = "Layer ablation completed under pre-registered rules."
    lines = [
        "# Alpha Campaign 2 Report",
        "",
        honesty,
        "",
        f"Generated: {summary.get('generated_at')}",
        f"Git SHA: `{summary.get('git_sha')}`",
        f"OOS boundary: `{summary.get('oos_start')}`",
        f"Configurations evaluated: `{summary.get('configurations_evaluated')}`",
        f"Wall clock seconds: `{summary.get('wall_clock_seconds')}`",
        "",
        "## Executive Answers",
        "",
        f"- Raw OOS return winner: `{best_return_arm}`.",
        f"- Best supported arm: `{best_supported}`.",
        f"- Dumb-control verdict: {verdict.get('control_verdict')}.",
        f"- Cost fragility: `{verdict.get('cost_fragility')}`.",
        "- No production defaults or agent behavior were changed.",
        "",
        "## Per-Arm Results",
        "",
        _markdown_table(
            ["Arm", "CAGR", "Vol", "Sharpe", "Calmar", "Max DD", "OOS Return", "OOS Sharpe", "Turnover", "Costs", "Exposure"],
            [
                [
                    row.get("arm"),
                    _fmt_pct(row.get("annualized_return")),
                    _fmt_pct(row.get("annualized_volatility")),
                    _fmt_num(row.get("sharpe_ratio")),
                    _fmt_num(row.get("calmar_ratio")),
                    _fmt_pct(row.get("max_drawdown")),
                    _fmt_pct(row.get("oos_total_return")),
                    _fmt_num(row.get("oos_sharpe_ratio")),
                    _fmt_num(row.get("annualized_turnover")),
                    _fmt_num(row.get("total_costs_paid")),
                    _fmt_pct(row.get("exposure_pct")),
                ]
                for row in rows
            ],
        ),
        "",
        "## Stress Windows",
        "",
        _stress_markdown(rows),
        "",
        "## Promotion Rules",
        "",
        _markdown_table(
            ["Rule", "Result"],
            [
                ["Layer support", json.dumps(verdict.get("layer_support") or {}, sort_keys=True)],
                ["Control hurdle", verdict.get("control_verdict")],
                ["Stress preservation", json.dumps(verdict.get("stress_preservation") or {}, sort_keys=True)],
                ["Cost fragility", verdict.get("cost_fragility")],
            ],
        ),
        "",
        "## Sensitivity Grids",
        "",
        f"Sensitivity arms evaluated: `{len(summary.get('sensitivity_arms') or [])}`. Headline comparisons use defaults; grids are for robustness commentary only.",
        "",
        "## Recommended Next Steps",
        "",
        "No default changes are recommended without human sign-off. Mapping winning specs onto the four agents remains a future task.",
        "",
    ]
    report = "\n".join(lines)
    Path(output_path).write_text(report, encoding="utf-8")
    return report


def campaign2_status(campaign_dir: str | Path = DEFAULT_CAMPAIGN2_DIR) -> dict[str, Any]:
    root = Path(campaign_dir)
    return {
        "campaign_dir": str(root),
        "summary_exists": (root / "summary.json").exists(),
        "cache_manifest_exists": (root / "cache_manifest.json").exists(),
        "result_count": len(list((root / "results").glob("*.json"))) if (root / "results").exists() else 0,
    }


def _enrich_regime_frames(frames: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    provider = RegimeHMMSignalProvider()
    enriched: dict[str, pd.DataFrame] = {}
    for ticker, frame in frames.items():
        provider.prepare(ticker, frame)
        cached = provider.prepared_frame(ticker)
        enriched[str(ticker).upper()] = cached if not cached.empty else frame
    return enriched


def _campaign_row(arm: str, payload: dict[str, Any]) -> dict[str, Any]:
    metrics = dict(payload.get("metrics") or {})
    oos = dict(payload.get("out_of_sample") or {})
    return {
        "arm": arm,
        "strategy_hash": payload.get("strategy_hash"),
        "total_return": metrics.get("total_return"),
        "annualized_return": metrics.get("annualized_return"),
        "annualized_volatility": metrics.get("annualized_volatility"),
        "sharpe_ratio": metrics.get("sharpe_ratio"),
        "calmar_ratio": metrics.get("calmar_ratio"),
        "max_drawdown": metrics.get("max_drawdown"),
        "annualized_turnover": metrics.get("annualized_turnover"),
        "total_costs_paid": metrics.get("total_costs_paid"),
        "exposure_pct": metrics.get("exposure_pct"),
        "oos_total_return": oos.get("total_return"),
        "oos_sharpe_ratio": oos.get("sharpe_ratio"),
        "oos_calmar_ratio": oos.get("calmar_ratio") or _calmar_from_row(oos),
        "oos_max_drawdown": oos.get("max_drawdown"),
        "stress_windows": payload.get("stress_windows") or [],
    }


def _layer_supported(candidate: dict[str, Any], previous: dict[str, Any]) -> dict[str, Any]:
    candidate_sharpe = _float(candidate.get("oos_sharpe_ratio"))
    previous_sharpe = _float(previous.get("oos_sharpe_ratio"))
    candidate_calmar = _float(candidate.get("oos_calmar_ratio"))
    previous_calmar = _float(previous.get("oos_calmar_ratio"))
    candidate_return = _float(candidate.get("oos_total_return"))
    previous_return = _float(previous.get("oos_total_return"))
    sharpe_ok = candidate_sharpe is not None and previous_sharpe is not None and candidate_sharpe > previous_sharpe
    calmar_ok = candidate_calmar is not None and previous_calmar is not None and candidate_calmar > previous_calmar
    if candidate_return is None or previous_return is None:
        return_ok = False
    else:
        return_ok = candidate_return >= previous_return - abs(previous_return) * 0.15
    return {"supported": bool(sharpe_ok and calmar_ok and return_ok), "sharpe_ok": sharpe_ok, "calmar_ok": calmar_ok, "return_ok": return_ok}


def _stress_preservation(rows: dict[str, dict[str, Any]], final_arm: str) -> dict[str, Any]:
    required = ("covid_crash", "bear_2022")
    result: dict[str, Any] = {}
    for key in required:
        l0_dd = abs(_stress_metric(rows.get("L0", {}), key, "strategy_max_drawdown") or 0.0)
        l2_dd = abs(_stress_metric(rows.get("L2", {}), key, "strategy_max_drawdown") or 0.0)
        final_dd = abs(_stress_metric(rows.get(final_arm, {}), key, "strategy_max_drawdown") or 0.0)
        l2_advantage = max(0.0, l0_dd - l2_dd)
        final_advantage = max(0.0, l0_dd - final_dd)
        passed = True if l2_advantage <= 0 else final_advantage >= 0.5 * l2_advantage
        result[key] = {"passed": passed, "l2_advantage": l2_advantage, "final_advantage": final_advantage}
    result["passed"] = all(bool(row.get("passed")) for row in result.values() if isinstance(row, dict))
    return result


def _stress_metric(row: dict[str, Any], key: str, metric: str) -> float | None:
    for item in row.get("stress_windows") or []:
        if isinstance(item, dict) and item.get("key") == key:
            return _float(item.get(metric) or (item.get("metrics") or {}).get(metric))
    return None


def _stress_markdown(rows: list[dict[str, Any]]) -> str:
    table_rows: list[list[Any]] = []
    for row in rows:
        for item in row.get("stress_windows") or []:
            if not isinstance(item, dict):
                continue
            table_rows.append(
                [
                    row.get("arm"),
                    item.get("key"),
                    _fmt_pct(item.get("strategy_total_return")),
                    _fmt_pct(item.get("strategy_max_drawdown")),
                    _fmt_num(item.get("days_to_derisk")),
                    _fmt_pct(item.get("exposure_mean")),
                ]
            )
    return _markdown_table(["Arm", "Window", "Return", "Max DD", "Days To Derisk", "Exposure"], table_rows) if table_rows else "No stress-window results are available."


def _best_return_arm(rows: list[dict[str, Any]]) -> str | None:
    if not rows:
        return None
    return str(max(rows, key=lambda row: _float(row.get("oos_total_return")) or -999.0).get("arm"))


def _calmar_from_row(row: dict[str, Any]) -> float | None:
    annualized = _float(row.get("annualized_return"))
    drawdown = abs(_float(row.get("max_drawdown")) or 0.0)
    if annualized is None or drawdown <= 0:
        return None
    return annualized / drawdown


def _append_run_log(root: Path, message: str) -> None:
    root.mkdir(parents=True, exist_ok=True)
    with (root / "run_log.md").open("a", encoding="utf-8") as handle:
        handle.write(f"- {dt.datetime.now(dt.timezone.utc).isoformat()} {message}\n")


def _safe_name(value: str) -> str:
    return "".join(char if char.isalnum() or char in {"-", "_"} else "_" for char in str(value)).strip("_") or "arm"


def _markdown_table(headers: list[str], rows: list[list[Any]]) -> str:
    rendered = ["| " + " | ".join(headers) + " |", "| " + " | ".join("---" for _ in headers) + " |"]
    for row in rows:
        rendered.append("| " + " | ".join(str(value if value is not None else "") for value in row) + " |")
    return "\n".join(rendered)


def _fmt_num(value: Any) -> str:
    parsed = _float(value)
    return "" if parsed is None else f"{parsed:.3f}"


def _fmt_pct(value: Any) -> str:
    parsed = _float(value)
    return "" if parsed is None else f"{parsed:.2%}"


def _float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        parsed = float(value)
    except Exception:
        return None
    return parsed if parsed == parsed and parsed not in (float("inf"), float("-inf")) else None
