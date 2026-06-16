from __future__ import annotations

import datetime as dt
import html
import math
from pathlib import Path
from typing import Any

import pandas as pd

from .alpha_campaign import DEFAULT_BASKET_PATH, DEFAULT_CAMPAIGN_DIR, _git_sha, _json_safe, _read_json, _write_json, load_basket
from .portfolio_backtest import PortfolioBacktestConfig, control_specs, run_portfolio_backtest
from .portfolio_campaign import _campaign_row, _enrich_regime_frames as _campaign2_enrich_regime_frames
from .portfolio_campaign import _float, _fmt_num, _fmt_pct, _safe_name, campaign2_headline_specs
from .portfolio_historical_campaign import (
    FrameLoader,
    RegimeEnricher,
    _benchmark_relative_rows,
    _buy_hold_curve,
    _historical_row,
    _html_table,
    _limitations,
    _load_historical_frame,
    _period_returns,
    _slice_frame,
    _stress_results_for_curve,
    build_availability_report,
    historical_stress_windows_for_range,
)
from .stress_windows import StressWindow
from .strategy import StrategySpec

DEFAULT_CAMPAIGN3_START = "2006-01-01"
DEFAULT_CAMPAIGN3_END = "2025-12-31"
DEFAULT_CAMPAIGN3_DIR = DEFAULT_CAMPAIGN_DIR / "portfolio_campaign3_2006_2025"
DEFAULT_CAMPAIGN3_REPORT_DIR = Path("output") / "campaign3_2006_2025_report"
DEFAULT_CAMPAIGN3_REPORT_PATH = DEFAULT_CAMPAIGN3_REPORT_DIR / "management_report.html"

TARGET_VOLS = (0.15, 0.18, 0.20)
MIN_EXPOSURES = (0.25, 0.40, 0.60)
REBALANCE_BANDS = (0.25, 0.40)
AGENT_RETURN_HAIRCUT_LIMIT = 0.25
AGENT_TURNOVER_LIMIT = 3.0


def campaign3_specs() -> dict[str, StrategySpec]:
    specs: dict[str, StrategySpec] = {
        "L0": StrategySpec(
            name="L0",
            signal_provider="precomputed_regime",
            exposure_policy="always_full",
            allocation_policy="equal_weight",
            rebalance_policy="monthly_bands",
            rebalance_params={"band": 0.25},
            description="Full-invested equal-weight basket control.",
        )
    }
    for target_vol in TARGET_VOLS:
        for min_exposure in MIN_EXPOSURES:
            for band in REBALANCE_BANDS:
                for spy_brake in (False, True):
                    arm = _l1_arm_name(target_vol, min_exposure, band, spy_brake)
                    if arm in specs:
                        continue
                    specs[arm] = StrategySpec(
                        name=arm,
                        signal_provider="precomputed_regime",
                        exposure_policy="vol_target",
                        exposure_params={"target_vol": target_vol, "min_exposure": min_exposure},
                        override_policy="market_timing_brake" if spy_brake else None,
                        override_params={"cap": 0.0, "signal_column": "market_timing_confirmed"} if spy_brake else {},
                        allocation_policy="equal_weight",
                        rebalance_policy="monthly_bands",
                        rebalance_params={"band": band},
                        description=_l1_description(target_vol, min_exposure, band, spy_brake),
                    )
    headline = campaign2_headline_specs()
    specs["L2"] = headline["L2"]
    specs["L3"] = headline["L3"]
    controls = control_specs()
    specs["C1_spy_buy_hold"] = controls["C1_spy_buy_hold"]
    specs["C2_spy_200dma"] = controls["C2_spy_200dma"]
    return specs


def run_campaign3(
    *,
    basket_path: str | Path = DEFAULT_BASKET_PATH,
    campaign_dir: str | Path = DEFAULT_CAMPAIGN3_DIR,
    report_dir: str | Path = DEFAULT_CAMPAIGN3_REPORT_DIR,
    start: str = DEFAULT_CAMPAIGN3_START,
    end: str = DEFAULT_CAMPAIGN3_END,
    resume: bool = False,
    frame_loader: FrameLoader | None = None,
    regime_enricher: RegimeEnricher | None = None,
    render_report: bool = True,
) -> dict[str, Any]:
    started = dt.datetime.now(dt.timezone.utc)
    root = Path(campaign_dir)
    result_dir = root / "results"
    result_dir.mkdir(parents=True, exist_ok=True)
    basket = load_basket(basket_path)
    tickers = [str(ticker).upper() for ticker in basket.get("tickers") or []]
    if not tickers:
        raise ValueError("Campaign 3 requires a pinned basket with tickers.")

    loader = frame_loader or _load_historical_frame
    raw_frames: dict[str, pd.DataFrame] = {}
    load_errors: dict[str, str] = {}
    for ticker in tickers:
        try:
            frame = _slice_frame(loader(ticker, start, end), start, end)
        except Exception as exc:
            load_errors[ticker] = str(exc)
            continue
        if frame.empty:
            load_errors[ticker] = "empty adjusted-price frame for requested window"
            continue
        raw_frames[ticker] = frame
    if not raw_frames:
        raise ValueError("No basket constituents have usable history in the requested window.")

    spy_frame = _slice_frame(loader("SPY", start, end), start, end)
    if spy_frame.empty:
        raise ValueError("SPY benchmark history is required for Campaign 3.")

    enricher = regime_enricher or _campaign2_enrich_regime_frames
    frames = enricher(raw_frames)
    market_timing_frames = with_market_timing_signal(frames, spy_frame)
    benchmark_curve = _buy_hold_curve(spy_frame, starting_cash=100_000.0)
    config = PortfolioBacktestConfig(oos_start=start, availability_mode="panel")
    stress_windows = historical_stress_windows_for_range(start, end)
    specs = campaign3_specs()
    results: dict[str, dict[str, Any]] = {}

    for arm, spec in specs.items():
        output = result_dir / f"{_safe_name(arm)}.json"
        if resume and output.exists():
            payload = _refresh_campaign3_payload(
                dict(_read_json(output)),
                arm=arm,
                start=start,
                end=end,
                windows=stress_windows,
                benchmark_curve=benchmark_curve,
            )
            _write_json(output, payload)
            results[arm] = payload
            continue
        arm_frames = _frames_for_arm(arm, spec, frames, market_timing_frames, spy_frame)
        result = run_portfolio_backtest(arm_frames, spec, config, benchmark_curve=benchmark_curve, windows=stress_windows)
        payload = result.to_dict()
        payload["campaign"] = _campaign_metadata(arm=arm, start=start, end=end, spec=spec)
        _write_json(output, payload)
        results[arm] = payload

    preliminary_verdict = campaign3_verdict(results)
    candidate_arm = str(preliminary_verdict.get("best_l1_candidate_arm") or "L1")
    cost_output = result_dir / f"{_safe_name(candidate_arm)}__cost_2x.json"
    if resume and cost_output.exists():
        cost_result = _refresh_campaign3_payload(
            dict(_read_json(cost_output)),
            arm=f"{candidate_arm}_cost_2x",
            start=start,
            end=end,
            windows=stress_windows,
            benchmark_curve=benchmark_curve,
        )
        _write_json(cost_output, cost_result)
    else:
        candidate_spec = specs[candidate_arm]
        doubled = PortfolioBacktestConfig(oos_start=start, availability_mode="panel", entry_cost_bps=10.0, exit_cost_bps=10.0)
        cost_obj = run_portfolio_backtest(
            _frames_for_arm(candidate_arm, candidate_spec, frames, market_timing_frames, spy_frame),
            candidate_spec,
            doubled,
            benchmark_curve=benchmark_curve,
            windows=stress_windows,
        )
        cost_result = cost_obj.to_dict()
        cost_result["campaign"] = _campaign_metadata(arm=f"{candidate_arm}_cost_2x", start=start, end=end, spec=candidate_spec)
        _write_json(cost_output, cost_result)

    verdict = campaign3_verdict(results, cost_fragility_result=cost_result, cost_fragility_arm=candidate_arm)
    rows = [_campaign3_row(arm, payload, result_dir / f"{_safe_name(arm)}.json", specs.get(arm)) for arm, payload in results.items()]
    rows = sorted(rows, key=_row_order)
    cost_row = _campaign3_row(f"{candidate_arm}_cost_2x", cost_result, cost_output, specs.get(candidate_arm))
    monthly_returns = {arm: _period_returns(payload.get("equity_curve") or [], "M") for arm, payload in results.items()}
    yearly_returns = {arm: _period_returns(payload.get("equity_curve") or [], "Y") for arm, payload in results.items()}
    finished = dt.datetime.now(dt.timezone.utc)
    coverage = build_availability_report(raw_frames, start=start, end=end, load_errors=load_errors, basket=basket)
    summary = {
        "schema": "regime_portfolio_campaign3.v1",
        "generated_at": finished.isoformat(),
        "git_sha": _git_sha(),
        "wall_clock_seconds": (finished - started).total_seconds(),
        "basket_path": str(basket_path),
        "basket_size": len(tickers),
        "available_ticker_count": len(raw_frames),
        "tickers": tickers,
        "available_tickers": sorted(raw_frames),
        "unavailable_tickers": sorted(load_errors),
        "start": start,
        "end": end,
        "availability_mode": "panel",
        "adjusted_price_note": "Adjusted daily OHLC is used where available; no live trading defaults were changed.",
        "target_vol_grid": list(TARGET_VOLS),
        "min_exposure_grid": list(MIN_EXPOSURES),
        "rebalance_band_grid": list(REBALANCE_BANDS),
        "headline_arms": ["L0", "L1", "L2", "L3", "C1_spy_buy_hold", "C2_spy_200dma"],
        "configurations_evaluated": len(results) + 1,
        "rows": rows,
        "cost_fragility_result": cost_row,
        "benchmark_relative": _benchmark_relative_rows(rows),
        "monthly_returns": monthly_returns,
        "yearly_returns": yearly_returns,
        "coverage": coverage,
        "stress_windows": [window.to_dict() for window in stress_windows],
        "verdict": verdict,
        "single_command": (
            "python -m src.regime.cli portfolio-campaign3 run "
            f"--start {start} --end {end} --campaign-dir {root} --report-dir {Path(report_dir)} --resume"
        ),
        "production_defaults_changed": False,
        "limitations": _limitations(start=start, end=end, load_errors=load_errors),
    }
    _write_json(root / "summary.json", summary)
    _write_json(
        root / "cache_manifest.json",
        {
            "schema": "regime_portfolio_campaign3_cache.v1",
            "generated_at": started.isoformat(),
            "git_sha": _git_sha(),
            "start": start,
            "end": end,
            "tickers": tickers,
            "available_tickers": sorted(raw_frames),
            "benchmark": "SPY",
            "availability_mode": "panel",
            "frozen_cache_note": "Frames are loaded once at campaign start and sliced to the requested historical window.",
        },
    )
    _append_run_log(root, f"Campaign 3 completed in {summary['wall_clock_seconds']:.1f}s with {summary['configurations_evaluated']} configurations.")
    if render_report:
        report_path = render_campaign3_report(campaign_dir=root, output_dir=report_dir)
        summary["report_path"] = str(report_path)
        _write_json(root / "summary.json", summary)
    return summary


def render_campaign3_report(
    *,
    campaign_dir: str | Path = DEFAULT_CAMPAIGN3_DIR,
    output_dir: str | Path = DEFAULT_CAMPAIGN3_REPORT_DIR,
    output_path: str | Path | None = None,
) -> Path:
    root = Path(campaign_dir)
    summary_path = root / "summary.json"
    if not summary_path.exists():
        raise FileNotFoundError(f"Campaign 3 summary not found: {summary_path}")
    summary = dict(_read_json(summary_path))
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    assets_dir = out_dir / "assets"
    assets_dir.mkdir(parents=True, exist_ok=True)
    results = _load_result_payloads(summary)
    charts = _build_campaign3_charts(summary, results, assets_dir)
    report_path = Path(output_path) if output_path is not None else out_dir / "management_report.html"
    report_path.write_text(_campaign3_report_html(summary, charts, results), encoding="utf-8")
    return report_path


def campaign3_status(campaign_dir: str | Path = DEFAULT_CAMPAIGN3_DIR) -> dict[str, Any]:
    root = Path(campaign_dir)
    return {
        "campaign_dir": str(root),
        "summary_exists": (root / "summary.json").exists(),
        "cache_manifest_exists": (root / "cache_manifest.json").exists(),
        "result_count": len(list((root / "results").glob("*.json"))) if (root / "results").exists() else 0,
    }


def with_market_timing_signal(frames: dict[str, pd.DataFrame], spy_frame: pd.DataFrame) -> dict[str, pd.DataFrame]:
    timing = _spy_timing_series(spy_frame)
    output: dict[str, pd.DataFrame] = {}
    for ticker, frame in frames.items():
        copied = frame.copy()
        copied["market_timing_confirmed"] = timing.reindex(pd.to_datetime(copied.index)).ffill().fillna(False).astype(bool)
        output[str(ticker).upper()] = copied
    return output


def campaign3_verdict(
    results: dict[str, dict[str, Any]],
    *,
    cost_fragility_result: dict[str, Any] | None = None,
    cost_fragility_arm: str | None = None,
) -> dict[str, Any]:
    rows = {arm: _campaign3_row(arm, payload, Path(""), campaign3_specs().get(arm)) for arm, payload in results.items()}
    l0 = rows.get("L0", {})
    spy_200dma = rows.get("C2_spy_200dma", {})
    l1_candidates = [row for arm, row in rows.items() if _is_l1_candidate_arm(arm)]
    candidate_rankings = sorted(
        [_candidate_score(row, l0, spy_200dma) for row in l1_candidates],
        key=lambda row: (
            not bool(row.get("readiness_passed")),
            -(_float(row.get("sharpe_ratio")) or -999.0),
            -(_float(row.get("annualized_return")) or -999.0),
            abs(_float(row.get("max_drawdown")) or 999.0),
        ),
    )
    best_candidate = candidate_rankings[0] if candidate_rankings else {}
    candidate_arm = str(best_candidate.get("arm") or "")
    cost_row = _campaign_row(f"{cost_fragility_arm or candidate_arm}_cost_2x", cost_fragility_result or {}) if cost_fragility_result else {}
    cost_ok = True
    if cost_fragility_result is not None:
        original = rows.get(str(cost_fragility_arm or candidate_arm), {})
        cost_ok = _cost_fragility_passed(original, cost_row)
    readiness_passed = bool(best_candidate.get("readiness_passed")) and cost_ok
    l2_promotion = _research_promotion(rows.get("L2", {}), rows.get(candidate_arm) or rows.get("L1", {}))
    l3_promotion = _research_promotion(rows.get("L3", {}), rows.get(candidate_arm) or rows.get("L1", {}))
    return {
        "best_l1_candidate_arm": candidate_arm or None,
        "recommended_paper_validation_arm": candidate_arm if readiness_passed else None,
        "l1_remains_paper_validation_candidate": readiness_passed,
        "l1_readiness": best_candidate,
        "l1_cost_fragility": "passed" if cost_ok else "failed",
        "l2_promoted": bool(l2_promotion.get("promoted")),
        "l2_promotion": l2_promotion,
        "l3_promoted": bool(l3_promotion.get("promoted")),
        "l3_promotion": l3_promotion,
        "research_arms": ["L2", "L3"],
        "recommended_production_default_changes": [],
        "recommended_default_changes": [],
        "candidate_rankings": candidate_rankings[:10],
        "decision_rule": (
            "Paper-validation candidate must improve drawdown versus L0, beat L0 and SPY 200dma on Sharpe, "
            "retain at least 75% of L0 CAGR, keep annualized turnover under 3.0x, and pass the 2x cost-fragility run."
        ),
    }


def _refresh_campaign3_payload(
    payload: dict[str, Any],
    *,
    arm: str,
    start: str,
    end: str,
    windows: list[StressWindow],
    benchmark_curve: pd.DataFrame,
) -> dict[str, Any]:
    refreshed = dict(payload)
    campaign = dict(refreshed.get("campaign") or {})
    campaign.update({"campaign": "portfolio_campaign3", "arm": arm, "git_sha": campaign.get("git_sha") or _git_sha(), "start": start, "end": end})
    refreshed["campaign"] = campaign
    curve = pd.DataFrame(refreshed.get("equity_curve") or [])
    if not curve.empty:
        trades = [dict(row) for row in refreshed.get("trades") or [] if isinstance(row, dict)]
        refreshed["stress_windows"] = _json_safe(_stress_results_for_curve(curve, trades, benchmark_curve, windows))
    return refreshed


def _frames_for_arm(
    arm: str,
    spec: StrategySpec,
    frames: dict[str, pd.DataFrame],
    market_timing_frames: dict[str, pd.DataFrame],
    spy_frame: pd.DataFrame,
) -> dict[str, pd.DataFrame]:
    if arm.startswith("C"):
        return {"SPY": spy_frame}
    if spec.override_policy == "market_timing_brake":
        return market_timing_frames
    return frames


def _campaign_metadata(*, arm: str, start: str, end: str, spec: StrategySpec) -> dict[str, Any]:
    return {
        "campaign": "portfolio_campaign3",
        "arm": arm,
        "git_sha": _git_sha(),
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "start": start,
        "end": end,
        "availability_mode": "panel",
        "strategy_metadata": _spec_metadata(arm, spec),
    }


def _campaign3_row(arm: str, payload: dict[str, Any], result_path: Path, spec: StrategySpec | None) -> dict[str, Any]:
    row = _historical_row(arm, payload, result_path)
    row.update(_spec_metadata(arm, spec))
    return row


def _spec_metadata(arm: str, spec: StrategySpec | None) -> dict[str, Any]:
    exposure_params = dict(spec.exposure_params) if spec else {}
    rebalance_params = dict(spec.rebalance_params) if spec else {}
    is_control = str(arm).startswith("C")
    return {
        "role": _arm_role(arm),
        "target_vol": exposure_params.get("target_vol"),
        "min_exposure": exposure_params.get("min_exposure"),
        "rebalance_band": rebalance_params.get("band", 0.25 if spec and spec.rebalance_policy == "monthly_bands" else None),
        "spy_200dma_brake": bool(spec and spec.override_policy == "market_timing_brake"),
        "hmm_brake": bool(spec and spec.override_policy == "regime_brake"),
        "momentum_tilt": bool(spec and spec.allocation_policy == "momentum_tilt"),
        "control": is_control,
        "description": (spec.description if spec else ""),
        "strategy_stack": _strategy_stack_text(spec),
    }


def _candidate_score(row: dict[str, Any], l0: dict[str, Any], spy_200dma: dict[str, Any]) -> dict[str, Any]:
    candidate_cagr = _float(row.get("annualized_return"))
    l0_cagr = _float(l0.get("annualized_return"))
    candidate_sharpe = _float(row.get("sharpe_ratio"))
    l0_sharpe = _float(l0.get("sharpe_ratio"))
    spy_sharpe = _float(spy_200dma.get("sharpe_ratio"))
    candidate_dd = abs(_float(row.get("max_drawdown")) or 0.0)
    l0_dd = abs(_float(l0.get("max_drawdown")) or 0.0)
    turnover = _float(row.get("annualized_turnover")) or 0.0
    if l0_cagr is None or candidate_cagr is None:
        return_ok = False
        return_haircut = None
    elif l0_cagr <= 0:
        return_ok = candidate_cagr > l0_cagr
        return_haircut = None
    else:
        return_haircut = max(0.0, 1.0 - candidate_cagr / l0_cagr)
        return_ok = return_haircut <= AGENT_RETURN_HAIRCUT_LIMIT
    drawdown_ok = l0_dd > 0 and candidate_dd < l0_dd
    sharpe_ok = candidate_sharpe is not None and l0_sharpe is not None and spy_sharpe is not None and candidate_sharpe > l0_sharpe and candidate_sharpe > spy_sharpe
    turnover_ok = turnover <= AGENT_TURNOVER_LIMIT
    stress_ok = _stress_passes(row, l0)
    passed = bool(drawdown_ok and sharpe_ok and return_ok and turnover_ok and stress_ok)
    output = dict(row)
    output.update(
        {
            "readiness_passed": passed,
            "drawdown_ok": drawdown_ok,
            "sharpe_ok": sharpe_ok,
            "return_ok": return_ok,
            "turnover_ok": turnover_ok,
            "stress_ok": stress_ok,
            "return_haircut": return_haircut,
        }
    )
    return output


def _research_promotion(candidate: dict[str, Any], comparator: dict[str, Any]) -> dict[str, Any]:
    candidate_sharpe = _float(candidate.get("sharpe_ratio"))
    comparator_sharpe = _float(comparator.get("sharpe_ratio"))
    candidate_cagr = _float(candidate.get("annualized_return"))
    comparator_cagr = _float(comparator.get("annualized_return"))
    candidate_dd = abs(_float(candidate.get("max_drawdown")) or 0.0)
    comparator_dd = abs(_float(comparator.get("max_drawdown")) or 0.0)
    sharpe_ok = candidate_sharpe is not None and comparator_sharpe is not None and candidate_sharpe > comparator_sharpe
    drawdown_ok = comparator_dd > 0 and candidate_dd <= comparator_dd
    return_ok = (
        candidate_cagr is not None
        and comparator_cagr is not None
        and (candidate_cagr >= comparator_cagr * 0.85 if comparator_cagr > 0 else candidate_cagr > comparator_cagr)
    )
    return {"promoted": bool(sharpe_ok and drawdown_ok and return_ok), "sharpe_ok": sharpe_ok, "drawdown_ok": drawdown_ok, "return_ok": return_ok}


def _cost_fragility_passed(original: dict[str, Any], cost_row: dict[str, Any]) -> bool:
    original_sharpe = _float(original.get("sharpe_ratio"))
    cost_sharpe = _float(cost_row.get("sharpe_ratio"))
    original_cagr = _float(original.get("annualized_return"))
    cost_cagr = _float(cost_row.get("annualized_return"))
    if original_sharpe is None or cost_sharpe is None or original_cagr is None or cost_cagr is None:
        return False
    sharpe_ok = cost_sharpe >= 0.85 * original_sharpe
    cagr_ok = cost_cagr >= original_cagr - 0.01
    return bool(sharpe_ok and cagr_ok)


def _stress_passes(candidate: dict[str, Any], l0: dict[str, Any]) -> bool:
    required = ("gfc_2007_2009", "covid_crash", "bear_2022")
    for key in required:
        candidate_dd = abs(_stress_metric(candidate, key, "strategy_max_drawdown") or 0.0)
        l0_dd = abs(_stress_metric(l0, key, "strategy_max_drawdown") or 0.0)
        if l0_dd > 0 and candidate_dd > l0_dd:
            return False
    return True


def _stress_metric(row: dict[str, Any], key: str, metric: str) -> float | None:
    for item in row.get("stress_windows") or []:
        if isinstance(item, dict) and item.get("key") == key:
            return _float(item.get(metric) or (item.get("metrics") or {}).get(metric))
    return None


def _build_campaign3_charts(summary: dict[str, Any], results: dict[str, dict[str, Any]], assets_dir: Path) -> dict[str, str]:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    charts: dict[str, str] = {}

    def save(fig, name: str) -> None:
        path = assets_dir / f"{name}.png"
        fig.tight_layout()
        fig.savefig(path, dpi=160, bbox_inches="tight")
        plt.close(fig)
        charts[name] = str(path)

    rows = _comparison_rows(summary)
    labels = [_short_label(str(row.get("arm"))) for row in rows]
    if rows:
        fig, ax = plt.subplots(figsize=(10.5, 4.4))
        ax.bar(labels, [float(row.get("total_return") or 0.0) * 100 for row in rows], color="#2563eb")
        ax.set_ylabel("Total return (%)")
        ax.set_title("Campaign 3 total return")
        ax.tick_params(axis="x", rotation=25)
        save(fig, "campaign3_total_return")

        fig, ax = plt.subplots(figsize=(10.5, 4.4))
        ax.bar(labels, [float(row.get("sharpe_ratio") or 0.0) for row in rows], color="#059669")
        ax.set_ylabel("Sharpe")
        ax.set_title("Campaign 3 Sharpe")
        ax.tick_params(axis="x", rotation=25)
        save(fig, "campaign3_sharpe")

        fig, ax = plt.subplots(figsize=(10.5, 4.4))
        ax.bar(labels, [float(row.get("max_drawdown") or 0.0) * 100 for row in rows], color="#dc2626")
        ax.set_ylabel("Max drawdown (%)")
        ax.set_title("Campaign 3 drawdown")
        ax.tick_params(axis="x", rotation=25)
        save(fig, "campaign3_drawdown")

        fig, ax1 = plt.subplots(figsize=(10.5, 4.4))
        ax1.bar(labels, [float(row.get("annualized_turnover") or 0.0) for row in rows], color="#7c3aed", alpha=0.75)
        ax1.set_ylabel("Annualized turnover")
        ax2 = ax1.twinx()
        ax2.plot(labels, [float(row.get("total_costs_paid") or 0.0) for row in rows], color="#f97316", marker="o")
        ax2.set_ylabel("Total costs paid ($)")
        ax1.set_title("Turnover and costs")
        ax1.tick_params(axis="x", rotation=25)
        save(fig, "campaign3_turnover_costs")

    comparison_arms = [str(row.get("arm")) for row in rows]
    if comparison_arms:
        fig, ax = plt.subplots(figsize=(11.0, 5.2))
        for arm in comparison_arms:
            payload = results.get(arm)
            if not payload:
                continue
            curve = pd.DataFrame(payload.get("equity_curve") or [])
            if curve.empty:
                continue
            curve.index = pd.to_datetime(curve["date"])
            equity = pd.to_numeric(curve["equity"], errors="coerce").dropna()
            if equity.empty:
                continue
            ax.plot(equity.index, equity / float(equity.iloc[0]), label=_short_label(arm), linewidth=1.5)
        ax.set_title("Growth of $1")
        ax.set_ylabel("Multiple")
        ax.legend(loc="best", fontsize=8, ncols=2)
        save(fig, "campaign3_equity_curves")

    yearly_points: list[dict[str, Any]] = []
    selected = set(comparison_arms)
    for arm in selected:
        for row in (summary.get("yearly_returns") or {}).get(arm) or []:
            if not isinstance(row, dict):
                continue
            value = _float(row.get("return"))
            if value is None:
                continue
            yearly_points.append({"year": pd.Timestamp(row.get("period")).year, "arm": _short_label(arm), "return": value * 100.0})
    if yearly_points:
        yearly = pd.DataFrame(yearly_points)
        pivot = yearly.pivot_table(index="year", columns="arm", values="return", aggfunc="last").sort_index()
        fig, ax = plt.subplots(figsize=(11.0, 5.2))
        for label in pivot.columns:
            ax.plot(pivot.index, pivot[label], marker="o", linewidth=1.4, markersize=3, label=label)
        ax.axhline(0, color="#6b7280", linewidth=0.8)
        ax.set_title("Performance by year")
        ax.set_ylabel("Annual return (%)")
        ax.legend(loc="best", fontsize=8, ncols=2)
        save(fig, "campaign3_yearly_returns")
    return charts


def _campaign3_report_html(summary: dict[str, Any], charts: dict[str, str], results: dict[str, dict[str, Any]]) -> str:
    del results
    verdict = dict(summary.get("verdict") or {})
    rows = [row for row in summary.get("rows") or [] if isinstance(row, dict)]
    candidate = verdict.get("recommended_paper_validation_arm") or verdict.get("best_l1_candidate_arm") or "none"
    title = f"Campaign 3 L1 Deep Validation {pd.Timestamp(str(summary.get('start'))).year}-{pd.Timestamp(str(summary.get('end'))).year}"
    chart_tags = "\n".join(
        f'<figure><img src="assets/{html.escape(Path(path).name)}" alt="{html.escape(name)}"><figcaption>{html.escape(name.replace("_", " ").title())}</figcaption></figure>'
        for name, path in charts.items()
    )
    comparison_rows = _comparison_rows(summary)
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>{html.escape(title)}</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; color: #111827; margin: 28px; line-height: 1.45; }}
    h1, h2, h3 {{ margin-bottom: 8px; }}
    .summary {{ display: grid; grid-template-columns: repeat(4, minmax(180px, 1fr)); gap: 12px; margin: 16px 0 22px; max-width: 1180px; }}
    .card {{ border: 1px solid #d1d5db; border-radius: 8px; padding: 12px 14px; background: #fff; }}
    .label {{ color: #6b7280; font-size: 12px; text-transform: uppercase; letter-spacing: 0; }}
    .value {{ font-size: 22px; font-weight: 700; }}
    .pass {{ color: #047857; }}
    .fail {{ color: #b91c1c; }}
    table {{ border-collapse: collapse; width: 100%; max-width: 1260px; margin: 12px 0 24px; font-size: 13px; }}
    th, td {{ border: 1px solid #e5e7eb; padding: 7px 9px; text-align: right; vertical-align: top; }}
    th:first-child, td:first-child, td.text, th.text {{ text-align: left; }}
    th {{ background: #f3f4f6; }}
    figure {{ margin: 18px 0; max-width: 1120px; }}
    img {{ max-width: 100%; border: 1px solid #e5e7eb; border-radius: 6px; }}
    figcaption {{ color: #4b5563; font-size: 12px; }}
    .note {{ max-width: 1180px; background: #f9fafb; border: 1px solid #e5e7eb; border-radius: 8px; padding: 12px 14px; }}
  </style>
</head>
<body>
  <h1>{html.escape(title)}</h1>
  <p>Generated {html.escape(str(summary.get("generated_at")))} at git SHA <code>{html.escape(str(summary.get("git_sha")))}</code>. Window: <code>{html.escape(str(summary.get("start")))}</code> to <code>{html.escape(str(summary.get("end")))}</code>. No production trading defaults or live execution paths were changed.</p>
  <section class="summary">
    <div class="card"><div class="label">Paper-validation candidate</div><div class="value {'pass' if verdict.get('l1_remains_paper_validation_candidate') else 'fail'}">{html.escape(str(candidate))}</div></div>
    <div class="card"><div class="label">L2 HMM brake promoted</div><div class="value {'pass' if verdict.get('l2_promoted') else 'fail'}">{'Yes' if verdict.get('l2_promoted') else 'No'}</div></div>
    <div class="card"><div class="label">L3 momentum promoted</div><div class="value {'pass' if verdict.get('l3_promoted') else 'fail'}">{'Yes' if verdict.get('l3_promoted') else 'No'}</div></div>
    <div class="card"><div class="label">Configurations</div><div class="value">{int(summary.get("configurations_evaluated") or 0)}</div></div>
  </section>
  <div class="note">
    <strong>Executive readout.</strong>
    Campaign 3 tests only research arms. L1 variants are eligible for paper validation, while L2 HMM brake and L3 momentum tilt remain research-only unless the promotion flags above are green.
    The active rule is: {html.escape(str(verdict.get("decision_rule") or ""))}
  </div>

  <h2>Candidate Ranking</h2>
  {_html_table(["Arm", "Ready", "Target vol", "Min exposure", "Band", "SPY brake", "CAGR", "Sharpe", "Max DD", "Turnover", "Return haircut"], _candidate_table_rows(verdict), text_columns={0, 1})}

  <h2>Focused Comparison</h2>
  {_html_table(["Arm", "Role", "Total Return", "CAGR", "Vol", "Sharpe", "Sortino", "Max DD", "Calmar", "Turnover", "Costs", "Exposure", "Trades"], _result_table_rows(comparison_rows), text_columns={0, 1})}

  <h2>Charts</h2>
  {chart_tags}

  <h2>Strategy Used By Arm</h2>
  {_html_table(["Arm", "Role", "Target vol", "Min exposure", "Band", "SPY brake", "HMM brake", "Momentum", "Description", "Stack"], _strategy_rows(rows), text_columns={0, 1, 8, 9})}

  <h2>Full Results</h2>
  {_html_table(["Arm", "Role", "Total Return", "CAGR", "Vol", "Sharpe", "Sortino", "Max DD", "Calmar", "Turnover", "Costs", "Exposure", "Trades"], _result_table_rows(rows), text_columns={0, 1})}

  <h2>Performance By Year</h2>
  {_html_table(_yearly_headers(summary), _yearly_rows(summary), text_columns={0})}

  <h2>Benchmark Relative Performance</h2>
  {_html_table(["Arm", "Benchmark", "Return delta", "CAGR delta", "Sharpe delta", "Max DD delta"], _relative_rows(summary), text_columns={0, 1})}

  <h2>Stress Windows</h2>
  {_html_table(["Arm", "Window", "Return", "Max DD", "Exposure", "Trades"], _stress_rows(comparison_rows), text_columns={0, 1})}

  <h2>Coverage And Limitations</h2>
  {_html_table(["Ticker", "Sector", "First date", "Last date", "Rows", "Starts after target"], _coverage_ticker_rows(summary), text_columns={0, 1})}
  <ul>{''.join(f'<li>{html.escape(str(item))}</li>' for item in summary.get("limitations") or [])}</ul>
  <p>Single documented command: <code>{html.escape(str(summary.get("single_command") or ""))}</code></p>
</body>
</html>
"""


def _load_result_payloads(summary: dict[str, Any]) -> dict[str, dict[str, Any]]:
    payloads: dict[str, dict[str, Any]] = {}
    for row in summary.get("rows") or []:
        if not isinstance(row, dict):
            continue
        arm = str(row.get("arm") or "")
        path = Path(str(row.get("result_path") or ""))
        if arm and path.exists():
            payloads[arm] = dict(_read_json(path))
    return payloads


def _comparison_rows(summary: dict[str, Any]) -> list[dict[str, Any]]:
    rows = [row for row in summary.get("rows") or [] if isinstance(row, dict)]
    by_arm = {str(row.get("arm")): row for row in rows}
    verdict = dict(summary.get("verdict") or {})
    candidates = [str(verdict.get("best_l1_candidate_arm") or ""), str(verdict.get("recommended_paper_validation_arm") or "")]
    wanted = ["L0", "L1", *candidates, "L2", "L3", "C1_spy_buy_hold", "C2_spy_200dma"]
    output: list[dict[str, Any]] = []
    seen: set[str] = set()
    for arm in wanted:
        if arm and arm in by_arm and arm not in seen:
            output.append(by_arm[arm])
            seen.add(arm)
    if len(output) < 8:
        ranked = sorted([row for row in rows if _is_l1_candidate_arm(str(row.get("arm")))], key=lambda row: -(_float(row.get("sharpe_ratio")) or -999.0))
        for row in ranked[: 8 - len(output)]:
            arm = str(row.get("arm"))
            if arm not in seen:
                output.append(row)
                seen.add(arm)
    return output


def _result_table_rows(rows: list[dict[str, Any]]) -> list[list[Any]]:
    return [
        [
            _short_label(str(row.get("arm") or "")),
            row.get("role"),
            _fmt_pct(row.get("total_return")),
            _fmt_pct(row.get("annualized_return")),
            _fmt_pct(row.get("annualized_volatility")),
            _fmt_num(row.get("sharpe_ratio")),
            _fmt_num(row.get("sortino_ratio")),
            _fmt_pct(row.get("max_drawdown")),
            _fmt_num(row.get("calmar_ratio")),
            _fmt_num(row.get("annualized_turnover")),
            _fmt_num(row.get("total_costs_paid")),
            _fmt_pct(row.get("exposure_pct")),
            _fmt_num(row.get("trade_count")),
        ]
        for row in rows
    ]


def _candidate_table_rows(verdict: dict[str, Any]) -> list[list[Any]]:
    rows = []
    for row in verdict.get("candidate_rankings") or []:
        if not isinstance(row, dict):
            continue
        rows.append(
            [
                _short_label(str(row.get("arm") or "")),
                "yes" if row.get("readiness_passed") else "no",
                _fmt_pct(row.get("target_vol")),
                _fmt_pct(row.get("min_exposure")),
                _fmt_pct(row.get("rebalance_band")),
                "yes" if row.get("spy_200dma_brake") else "no",
                _fmt_pct(row.get("annualized_return")),
                _fmt_num(row.get("sharpe_ratio")),
                _fmt_pct(row.get("max_drawdown")),
                _fmt_num(row.get("annualized_turnover")),
                _fmt_pct(row.get("return_haircut")),
            ]
        )
    return rows


def _strategy_rows(rows: list[dict[str, Any]]) -> list[list[Any]]:
    return [
        [
            _short_label(str(row.get("arm") or "")),
            row.get("role"),
            _fmt_pct(row.get("target_vol")),
            _fmt_pct(row.get("min_exposure")),
            _fmt_pct(row.get("rebalance_band")),
            "yes" if row.get("spy_200dma_brake") else "no",
            "yes" if row.get("hmm_brake") else "no",
            "yes" if row.get("momentum_tilt") else "no",
            row.get("description"),
            row.get("strategy_stack"),
        ]
        for row in rows
    ]


def _relative_rows(summary: dict[str, Any]) -> list[list[Any]]:
    return [
        [
            _short_label(str(row.get("arm") or "")),
            _short_label(str(row.get("benchmark") or "")),
            _fmt_pct(row.get("total_return_delta")),
            _fmt_pct(row.get("cagr_delta")),
            _fmt_num(row.get("sharpe_delta")),
            _fmt_pct(row.get("max_drawdown_delta")),
        ]
        for row in summary.get("benchmark_relative") or []
        if isinstance(row, dict)
    ]


def _stress_rows(rows: list[dict[str, Any]]) -> list[list[Any]]:
    output: list[list[Any]] = []
    for row in rows:
        for item in row.get("stress_windows") or []:
            if not isinstance(item, dict):
                continue
            output.append(
                [
                    _short_label(str(row.get("arm") or "")),
                    item.get("label") or item.get("key"),
                    _fmt_pct(item.get("strategy_total_return")),
                    _fmt_pct(item.get("strategy_max_drawdown")),
                    _fmt_pct(item.get("exposure_mean")),
                    _fmt_num(item.get("trade_count")),
                ]
            )
    return output


def _coverage_ticker_rows(summary: dict[str, Any]) -> list[list[Any]]:
    return [
        [
            row.get("ticker"),
            row.get("sector"),
            row.get("first_date"),
            row.get("last_date"),
            row.get("row_count"),
            "yes" if row.get("starts_after_target") else "no",
        ]
        for row in ((summary.get("coverage") or {}).get("tickers") or [])
        if isinstance(row, dict)
    ]


def _yearly_headers(summary: dict[str, Any]) -> list[str]:
    arms = [str(row.get("arm")) for row in _comparison_rows(summary)]
    return ["Year", *[_short_label(arm) for arm in arms]]


def _yearly_rows(summary: dict[str, Any]) -> list[list[Any]]:
    arms = [str(row.get("arm")) for row in _comparison_rows(summary)]
    by_year: dict[int, dict[str, Any]] = {}
    for arm in arms:
        for row in (summary.get("yearly_returns") or {}).get(arm) or []:
            if not isinstance(row, dict):
                continue
            try:
                year = int(pd.Timestamp(row.get("period")).year)
            except Exception:
                continue
            by_year.setdefault(year, {})[arm] = row.get("return")
    return [[year, *[_fmt_pct(values.get(arm)) if arm in values else "" for arm in arms]] for year, values in sorted(by_year.items())]


def _spy_timing_series(spy_frame: pd.DataFrame) -> pd.Series:
    price = _price_series(spy_frame)
    if price.empty:
        return pd.Series(dtype=bool)
    ma_200 = price.rolling(200, min_periods=1).mean()
    above = price > ma_200
    return (above.rolling(5, min_periods=5).sum().fillna(0.0) >= 5.0).astype(bool)


def _price_series(frame: pd.DataFrame) -> pd.Series:
    if frame.empty:
        return pd.Series(dtype=float)
    copied = frame.copy()
    if not isinstance(copied.index, pd.DatetimeIndex):
        copied.index = pd.to_datetime(copied.index)
    for column in ("price", "Close", "Adj Close", "close"):
        if column in copied.columns:
            return pd.to_numeric(copied[column], errors="coerce").dropna().sort_index()
    return pd.Series(dtype=float)


def _l1_arm_name(target_vol: float, min_exposure: float, band: float, spy_brake: bool) -> str:
    if _is_close(target_vol, 0.15) and _is_close(min_exposure, 0.25) and _is_close(band, 0.25) and not spy_brake:
        return "L1"
    if _is_close(target_vol, 0.15) and _is_close(min_exposure, 0.25) and _is_close(band, 0.25) and spy_brake:
        return "L1_spy200"
    base = f"L1_tv{int(round(target_vol * 100))}_min{int(round(min_exposure * 100))}_band{int(round(band * 100))}"
    return f"{base}_spy200" if spy_brake else base


def _l1_description(target_vol: float, min_exposure: float, band: float, spy_brake: bool) -> str:
    brake = " with a portfolio-level SPY 200dma brake" if spy_brake else ""
    return f"L1 volatility-target overlay at {target_vol:.0%} target vol, {min_exposure:.0%} minimum exposure, {band:.0%} rebalance band{brake}."


def _arm_role(arm: str) -> str:
    if arm == "L0":
        return "full-basket control"
    if _is_l1_candidate_arm(arm):
        return "L1 validation candidate"
    if arm == "L2":
        return "HMM brake research"
    if arm == "L3":
        return "momentum research"
    if arm.startswith("C"):
        return "benchmark control"
    return "research"


def _is_l1_candidate_arm(arm: str) -> bool:
    return str(arm) == "L1" or str(arm).startswith("L1_")


def _row_order(row: dict[str, Any]) -> tuple[int, str]:
    arm = str(row.get("arm") or "")
    if arm == "L0":
        return (0, arm)
    if _is_l1_candidate_arm(arm):
        return (1, arm)
    if arm in {"L2", "L3"}:
        return (2, arm)
    if arm.startswith("C"):
        return (3, arm)
    return (4, arm)


def _short_label(arm: str) -> str:
    labels = {"C1_spy_buy_hold": "SPY buy-hold", "C2_spy_200dma": "SPY 200dma"}
    return labels.get(arm, arm)


def _strategy_stack_text(spec: StrategySpec | None) -> str:
    if spec is None:
        return ""
    parts = [
        f"signal={spec.signal_provider}{_params_text(spec.signal_params)}",
        f"exposure={spec.exposure_policy}{_params_text(spec.exposure_params)}",
    ]
    if spec.override_policy:
        parts.append(f"override={spec.override_policy}{_params_text(spec.override_params)}")
    parts.extend(
        [
            f"allocation={spec.allocation_policy}{_params_text(spec.allocation_params)}",
            f"rebalance={spec.rebalance_policy}{_params_text(spec.rebalance_params)}",
        ]
    )
    return "; ".join(parts)


def _params_text(params: dict[str, Any]) -> str:
    if not params:
        return ""
    return "(" + ", ".join(f"{key}={params[key]}" for key in sorted(params)) + ")"


def _is_close(left: float, right: float) -> bool:
    return abs(float(left) - float(right)) < 1e-9


def _append_run_log(root: Path, message: str) -> None:
    root.mkdir(parents=True, exist_ok=True)
    with (root / "run_log.md").open("a", encoding="utf-8") as handle:
        handle.write(f"- {dt.datetime.now(dt.timezone.utc).isoformat()} {message}\n")
