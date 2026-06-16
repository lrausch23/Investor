from __future__ import annotations

import datetime as dt
import html
import json
import math
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from .alpha_campaign import DEFAULT_BASKET_PATH, DEFAULT_CAMPAIGN_DIR, _git_sha, _json_safe, _read_json, _write_json, load_basket
from .data import download_market_frame
from .pipeline_backtest import PipelineBacktestConfig, compute_equity_metrics, run_pipeline_backtest
from .portfolio_backtest import PortfolioBacktestConfig, run_portfolio_backtest
from .portfolio_campaign import COMPLEXITY_VERDICT, _campaign_row, _fmt_num, _fmt_pct, _safe_name, campaign2_headline_specs, campaign2_verdict
from .portfolio_campaign import _enrich_regime_frames as _campaign2_enrich_regime_frames
from .stress_windows import StressWindow
from .strategy import StrategySpec

DEFAULT_HISTORICAL_CAMPAIGN_DIR = DEFAULT_CAMPAIGN_DIR / "portfolio_campaign_1996_2006"
DEFAULT_HISTORICAL_REPORT_DIR = Path("output") / "campaign_1996_2006_report"
DEFAULT_HISTORICAL_REPORT_PATH = DEFAULT_HISTORICAL_REPORT_DIR / "management_report.html"
DEFAULT_HISTORICAL_START = "1996-01-01"
DEFAULT_HISTORICAL_END = "2006-12-31"
HISTORICAL_REPORT_ARM_ORDER = ("L0", "L1", "L2", "L3", "C1_spy_buy_hold", "C2_spy_200dma", "Campaign1_per_name_HMM")
HISTORICAL_REPORT_ARM_LABELS = {
    "L0": "L0",
    "L1": "L1",
    "L2": "L2",
    "L3": "L3",
    "C1_spy_buy_hold": "SPY buy-hold",
    "C2_spy_200dma": "SPY 200dma",
    "Campaign1_per_name_HMM": "Campaign 1 HMM",
}

HISTORICAL_STRESS_WINDOWS: tuple[StressWindow, ...] = (
    StressWindow("ltcm_1998", "1998 LTCM / Risk Shock", "1998-08-17", "1998-10-30"),
    StressWindow("dotcom_2000_2002", "2000-2002 Dot-Com Drawdown", "2000-03-10", "2002-10-09"),
    StressWindow("sep11_2001", "2001-09-11 Shock", "2001-09-10", "2001-09-21"),
    StressWindow("recovery_2003", "2003 Recovery", "2003-03-12", "2003-12-31"),
    StressWindow("normalization_2004_2006", "2004-2006 Normalization", "2004-01-01", "2006-12-31"),
    StressWindow("gfc_2007_2009", "2007-2009 Global Financial Crisis", "2007-10-09", "2009-03-09"),
    StressWindow("lehman_2008", "2008 Lehman Shock", "2008-09-15", "2008-11-20"),
    StressWindow("euro_us_debt_2011", "2011 Eurozone / US Debt Shock", "2011-07-22", "2011-10-04"),
    StressWindow("q4_2018", "2018 Q4 Risk-Off", "2018-10-03", "2018-12-24"),
    StressWindow("covid_crash", "2020 COVID Crash", "2020-02-19", "2020-03-23"),
    StressWindow("bear_2022", "2022 Inflation / Rate Bear", "2022-01-03", "2022-10-12"),
    StressWindow("recovery_2023", "2023 Recovery", "2023-01-03", "2023-12-29"),
    StressWindow("tariff_vol_2025", "2025 Tariff / Volatility Shock", "2025-04-02", "2025-04-30"),
)

FrameLoader = Callable[[str, str, str], pd.DataFrame]
RegimeEnricher = Callable[[dict[str, pd.DataFrame]], dict[str, pd.DataFrame]]


def historical_stress_windows_for_range(start: str, end: str) -> list[StressWindow]:
    start_date = pd.Timestamp(start)
    end_date = pd.Timestamp(end)
    windows: list[StressWindow] = []
    for window in HISTORICAL_STRESS_WINDOWS:
        window_start = pd.Timestamp(window.start)
        window_end = pd.Timestamp(window.end)
        if window_start <= end_date and window_end >= start_date:
            windows.append(window)
    return windows


def _campaign_key(start: str, end: str) -> str:
    return f"portfolio_{pd.Timestamp(start).year}_{pd.Timestamp(end).year}"


def _period_label(start: str, end: str) -> str:
    start_ts = pd.Timestamp(start)
    end_ts = pd.Timestamp(end)
    if start_ts.year == end_ts.year:
        return str(start_ts.year)
    return f"{start_ts.year}-{end_ts.year}"


def _refresh_resumed_payload(
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
    campaign.update(
        {
            "campaign": _campaign_key(start, end),
            "arm": arm,
            "git_sha": campaign.get("git_sha") or _git_sha(),
            "availability_mode": "panel",
        }
    )
    refreshed["campaign"] = campaign
    curve = pd.DataFrame(refreshed.get("equity_curve") or [])
    if not curve.empty:
        trades = [dict(row) for row in refreshed.get("trades") or [] if isinstance(row, dict)]
        refreshed["stress_windows"] = _json_safe(_stress_results_for_curve(curve, trades, benchmark_curve, windows))
    return refreshed


def historical_campaign_specs() -> dict[str, StrategySpec]:
    specs = campaign2_headline_specs()
    return {
        name: specs[name]
        for name in ("L0", "L1", "L2", "L3", "C1_spy_buy_hold", "C2_spy_200dma")
        if name in specs
    }


def _try_fast_resume_historical_campaign(
    *,
    basket_path: str | Path,
    basket: dict[str, Any],
    campaign_dir: Path,
    report_dir: str | Path,
    start: str,
    end: str,
    include_campaign1_baseline: bool,
    render_report: bool,
    started: dt.datetime,
) -> dict[str, Any] | None:
    summary_path = campaign_dir / "summary.json"
    result_dir = campaign_dir / "results"
    if not summary_path.exists() or not result_dir.exists():
        return None
    specs = historical_campaign_specs()
    expected = {arm: result_dir / f"{_safe_name(arm)}.json" for arm in specs}
    expected["L1_cost_2x"] = result_dir / "L1__cost_2x.json"
    if include_campaign1_baseline:
        expected["Campaign1_per_name_HMM"] = result_dir / "Campaign1_per_name_HMM.json"
    if any(not path.exists() for path in expected.values()):
        return None

    previous = dict(_read_json(summary_path))
    stress_windows = historical_stress_windows_for_range(start, end)
    benchmark_curve = pd.DataFrame()
    results: dict[str, dict[str, Any]] = {}
    for arm in specs:
        path = expected[arm]
        payload = _refresh_resumed_payload(
            dict(_read_json(path)),
            arm=arm,
            start=start,
            end=end,
            windows=stress_windows,
            benchmark_curve=benchmark_curve,
        )
        _write_json(path, payload)
        results[arm] = payload

    l1_cost_result = _refresh_resumed_payload(
        dict(_read_json(expected["L1_cost_2x"])),
        arm="L1_cost_2x",
        start=start,
        end=end,
        windows=stress_windows,
        benchmark_curve=benchmark_curve,
    )
    _write_json(expected["L1_cost_2x"], l1_cost_result)

    campaign1_baseline_status: dict[str, Any] = {"included": False, "reason": "not requested"}
    if include_campaign1_baseline:
        baseline_payload = _refresh_resumed_payload(
            dict(_read_json(expected["Campaign1_per_name_HMM"])),
            arm="Campaign1_per_name_HMM",
            start=start,
            end=end,
            windows=stress_windows,
            benchmark_curve=benchmark_curve,
        )
        _write_json(expected["Campaign1_per_name_HMM"], baseline_payload)
        results["Campaign1_per_name_HMM"] = baseline_payload
        campaign1_baseline_status = dict(baseline_payload.get("campaign1_baseline_status") or {"included": True})

    verdict = historical_campaign_verdict(results, l1_cost_result)
    rows = [_historical_row(arm, payload, result_dir / f"{_safe_name(arm)}.json") for arm, payload in results.items()]
    cost_row = _historical_row("L1_cost_2x", l1_cost_result, expected["L1_cost_2x"])
    monthly_returns = {arm: _period_returns(payload.get("equity_curve") or [], "M") for arm, payload in results.items()}
    yearly_returns = {arm: _period_returns(payload.get("equity_curve") or [], "Y") for arm, payload in results.items()}
    benchmark_relative = _benchmark_relative_rows(rows)
    finished = dt.datetime.now(dt.timezone.utc)
    coverage = dict(previous.get("coverage") or {})
    load_errors = dict(coverage.get("load_errors") or {})
    tickers = [str(ticker).upper() for ticker in basket.get("tickers") or previous.get("tickers") or []]
    available_tickers = sorted(
        str(row.get("ticker") or "").upper()
        for row in coverage.get("tickers") or []
        if isinstance(row, dict) and row.get("ticker")
    ) or list(previous.get("available_tickers") or [])
    summary = {
        "schema": "regime_portfolio_historical_campaign.v1",
        "generated_at": finished.isoformat(),
        "git_sha": _git_sha(),
        "wall_clock_seconds": (finished - started).total_seconds(),
        "basket_path": str(basket_path),
        "basket_size": len(tickers) or int(previous.get("basket_size") or 0),
        "available_ticker_count": len(available_tickers) or int(previous.get("available_ticker_count") or 0),
        "tickers": tickers or list(previous.get("tickers") or []),
        "available_tickers": available_tickers or list(previous.get("available_tickers") or []),
        "unavailable_tickers": sorted(load_errors) or list(previous.get("unavailable_tickers") or []),
        "start": start,
        "end": end,
        "availability_mode": "panel",
        "adjusted_price_note": previous.get("adjusted_price_note") or "Adjusted daily OHLC is used where available.",
        "headline_arms": list(specs),
        "configurations_evaluated": len(results) + 1,
        "rows": rows,
        "cost_fragility_result": cost_row,
        "benchmark_relative": benchmark_relative,
        "monthly_returns": monthly_returns,
        "yearly_returns": yearly_returns,
        "coverage": coverage,
        "stress_windows": [window.to_dict() for window in stress_windows],
        "verdict": verdict,
        "campaign1_baseline_status": campaign1_baseline_status,
        "single_command": (
            "python -m src.regime.cli portfolio-history-campaign run "
            f"--start {start} --end {end} --campaign-dir {campaign_dir} --report-dir {Path(report_dir)} --resume"
        ),
        "production_defaults_changed": False,
        "limitations": _limitations(start=start, end=end, load_errors=load_errors),
    }
    _write_json(summary_path, summary)
    _append_run_log(campaign_dir, f"Historical campaign metadata refreshed in {summary['wall_clock_seconds']:.1f}s with {summary['configurations_evaluated']} configurations.")
    if render_report:
        report_path = render_historical_campaign_report(campaign_dir=campaign_dir, output_dir=report_dir)
        summary["report_path"] = str(report_path)
        _write_json(summary_path, summary)
    return summary


def run_historical_campaign(
    *,
    basket_path: str | Path = DEFAULT_BASKET_PATH,
    campaign_dir: str | Path = DEFAULT_HISTORICAL_CAMPAIGN_DIR,
    report_dir: str | Path = DEFAULT_HISTORICAL_REPORT_DIR,
    start: str = DEFAULT_HISTORICAL_START,
    end: str = DEFAULT_HISTORICAL_END,
    resume: bool = False,
    include_campaign1_baseline: bool = True,
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
        raise ValueError("Historical portfolio campaign requires a pinned basket with tickers.")
    if resume:
        resumed = _try_fast_resume_historical_campaign(
            basket_path=basket_path,
            basket=basket,
            campaign_dir=root,
            report_dir=report_dir,
            start=start,
            end=end,
            include_campaign1_baseline=include_campaign1_baseline,
            render_report=render_report,
            started=started,
        )
        if resumed is not None:
            return resumed

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
        raise ValueError("SPY benchmark history is required for the historical campaign.")

    enricher = regime_enricher or _campaign2_enrich_regime_frames
    frames = enricher(raw_frames)
    coverage = build_availability_report(raw_frames, start=start, end=end, load_errors=load_errors, basket=basket)
    benchmark_curve = _buy_hold_curve(spy_frame, starting_cash=100_000.0)
    config = PortfolioBacktestConfig(oos_start=start, availability_mode="panel")
    stress_windows = historical_stress_windows_for_range(start, end)
    specs = historical_campaign_specs()
    results: dict[str, dict[str, Any]] = {}
    for arm, spec in specs.items():
        output = result_dir / f"{_safe_name(arm)}.json"
        if resume and output.exists():
            payload = _refresh_resumed_payload(
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
        arm_frames = {"SPY": spy_frame} if arm.startswith("C") else frames
        result = run_portfolio_backtest(arm_frames, spec, config, benchmark_curve=benchmark_curve, windows=stress_windows)
        payload = result.to_dict()
        payload["campaign"] = {
            "campaign": _campaign_key(start, end),
            "arm": arm,
            "git_sha": _git_sha(),
            "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            "availability_mode": "panel",
        }
        _write_json(output, payload)
        results[arm] = payload

    l1_cost_output = result_dir / "L1__cost_2x.json"
    if resume and l1_cost_output.exists():
        l1_cost_result = _refresh_resumed_payload(
            dict(_read_json(l1_cost_output)),
            arm="L1_cost_2x",
            start=start,
            end=end,
            windows=stress_windows,
            benchmark_curve=benchmark_curve,
        )
        _write_json(l1_cost_output, l1_cost_result)
    else:
        l1_spec = specs.get("L1") or specs["L0"]
        doubled = PortfolioBacktestConfig(oos_start=start, availability_mode="panel", entry_cost_bps=10.0, exit_cost_bps=10.0)
        l1_cost_obj = run_portfolio_backtest(frames, l1_spec, doubled, benchmark_curve=benchmark_curve, windows=stress_windows)
        l1_cost_result = l1_cost_obj.to_dict()
        l1_cost_result["campaign"] = {
            "campaign": _campaign_key(start, end),
            "arm": "L1_cost_2x",
            "git_sha": _git_sha(),
            "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            "availability_mode": "panel",
        }
        _write_json(l1_cost_output, l1_cost_result)

    campaign1_baseline_status: dict[str, Any] = {"included": False, "reason": "not requested"}
    if include_campaign1_baseline:
        baseline_output = result_dir / "Campaign1_per_name_HMM.json"
        if resume and baseline_output.exists():
            baseline_payload = _refresh_resumed_payload(
                dict(_read_json(baseline_output)),
                arm="Campaign1_per_name_HMM",
                start=start,
                end=end,
                windows=stress_windows,
                benchmark_curve=benchmark_curve,
            )
            _write_json(baseline_output, baseline_payload)
        else:
            baseline_payload = _run_campaign1_baseline(raw_frames, spy_frame, start=start, windows=stress_windows)
            _write_json(baseline_output, baseline_payload)
        results["Campaign1_per_name_HMM"] = baseline_payload
        campaign1_baseline_status = dict(baseline_payload.get("campaign1_baseline_status") or {"included": True})

    verdict = historical_campaign_verdict(results, l1_cost_result)
    rows = [_historical_row(arm, payload, result_dir / f"{_safe_name(arm)}.json") for arm, payload in results.items()]
    cost_row = _historical_row("L1_cost_2x", l1_cost_result, l1_cost_output)
    monthly_returns = {arm: _period_returns(payload.get("equity_curve") or [], "M") for arm, payload in results.items()}
    yearly_returns = {arm: _period_returns(payload.get("equity_curve") or [], "Y") for arm, payload in results.items()}
    benchmark_relative = _benchmark_relative_rows(rows)
    finished = dt.datetime.now(dt.timezone.utc)
    summary = {
        "schema": "regime_portfolio_historical_campaign.v1",
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
        "adjusted_price_note": "Yahoo/IBKR normalized adjusted daily OHLC is used where available; yfinance calls use auto_adjust=True.",
        "headline_arms": list(specs),
        "configurations_evaluated": len(results) + 1,
        "rows": rows,
        "cost_fragility_result": cost_row,
        "benchmark_relative": benchmark_relative,
        "monthly_returns": monthly_returns,
        "yearly_returns": yearly_returns,
        "coverage": coverage,
        "stress_windows": [window.to_dict() for window in stress_windows],
        "verdict": verdict,
        "campaign1_baseline_status": campaign1_baseline_status,
        "single_command": (
            "python -m src.regime.cli portfolio-history-campaign run "
            f"--start {start} --end {end} --campaign-dir {root} --report-dir {Path(report_dir)} --resume"
        ),
        "production_defaults_changed": False,
        "limitations": _limitations(start=start, end=end, load_errors=load_errors),
    }
    _write_json(root / "summary.json", summary)
    _write_json(
        root / "cache_manifest.json",
        {
            "schema": "regime_portfolio_historical_campaign_cache.v1",
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
    _append_run_log(root, f"Historical campaign completed in {summary['wall_clock_seconds']:.1f}s with {summary['configurations_evaluated']} configurations.")
    if render_report:
        report_path = render_historical_campaign_report(campaign_dir=root, output_dir=report_dir)
        summary["report_path"] = str(report_path)
        _write_json(root / "summary.json", summary)
    return summary


def historical_campaign_verdict(results: dict[str, dict[str, Any]], l1_cost_result: dict[str, Any] | None = None) -> dict[str, Any]:
    campaign2_like = {arm: payload for arm, payload in results.items() if arm in {"L0", "L1", "L2", "L3", "C2_spy_200dma"}}
    verdict = campaign2_verdict(campaign2_like, cost_fragility_result=l1_cost_result)
    rows = {arm: _campaign_row(arm, payload) for arm, payload in results.items()}
    l1 = rows.get("L1", {})
    l1_cost = _campaign_row("L1_cost_2x", l1_cost_result or {}) if l1_cost_result else {}
    c2 = rows.get("C2_spy_200dma", {})
    l1_support = (verdict.get("layer_support") or {}).get("L1") or {}
    l2_support = (verdict.get("layer_support") or {}).get("L2") or {}
    l3_support = (verdict.get("layer_support") or {}).get("L3") or {}
    l1_oos_sharpe = _float(l1.get("oos_sharpe_ratio"))
    c2_oos_sharpe = _float(c2.get("oos_sharpe_ratio"))
    l1_cost_oos_sharpe = _float(l1_cost.get("oos_sharpe_ratio"))
    l1_beats_200dma = l1_oos_sharpe is not None and c2_oos_sharpe is not None and l1_oos_sharpe > c2_oos_sharpe
    l1_cost_ok = l1_cost_oos_sharpe is not None and l1_oos_sharpe is not None and l1_cost_oos_sharpe >= 0.85 * l1_oos_sharpe
    l1_candidate = bool(l1_support.get("supported") and l1_beats_200dma and l1_cost_ok)
    verdict.update(
        {
            "l1_deeper_validation_candidate": l1_candidate,
            "l1_cost_fragility": "passed" if l1_cost_ok else "failed",
            "l1_beats_spy_200dma_sharpe": bool(l1_beats_200dma),
            "l2_promoted": bool(l2_support.get("supported")),
            "l3_promoted": bool(l3_support.get("supported")),
            "recommended_production_default_changes": [],
            "recommended_paper_validation_arm": "L1" if l1_candidate else None,
            "research_arms": ["L2", "L3"],
        }
    )
    if verdict.get("control_verdict") == COMPLEXITY_VERDICT:
        verdict["complexity_note"] = COMPLEXITY_VERDICT
    return verdict


def render_historical_campaign_report(
    *,
    campaign_dir: str | Path = DEFAULT_HISTORICAL_CAMPAIGN_DIR,
    output_dir: str | Path = DEFAULT_HISTORICAL_REPORT_DIR,
    output_path: str | Path | None = None,
) -> Path:
    root = Path(campaign_dir)
    summary_path = root / "summary.json"
    if not summary_path.exists():
        raise FileNotFoundError(f"Historical campaign summary not found: {summary_path}")
    summary = dict(_read_json(summary_path))
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    assets_dir = out_dir / "assets"
    assets_dir.mkdir(parents=True, exist_ok=True)
    results = _load_result_payloads(summary)
    charts = _build_historical_charts(summary, results, assets_dir)
    report_path = Path(output_path) if output_path is not None else out_dir / "management_report.html"
    report_path.write_text(_historical_report_html(summary, charts, results), encoding="utf-8")
    return report_path


def historical_campaign_status(campaign_dir: str | Path = DEFAULT_HISTORICAL_CAMPAIGN_DIR) -> dict[str, Any]:
    root = Path(campaign_dir)
    return {
        "campaign_dir": str(root),
        "summary_exists": (root / "summary.json").exists(),
        "cache_manifest_exists": (root / "cache_manifest.json").exists(),
        "result_count": len(list((root / "results").glob("*.json"))) if (root / "results").exists() else 0,
    }


def build_availability_report(
    frames: dict[str, pd.DataFrame],
    *,
    start: str,
    end: str,
    load_errors: dict[str, str] | None = None,
    basket: dict[str, Any] | None = None,
) -> dict[str, Any]:
    target_start = pd.Timestamp(start)
    target_end = pd.Timestamp(end)
    sector_map = {
        str(item.get("ticker") or "").upper(): str(item.get("sector") or "Unknown")
        for item in (basket or {}).get("selected") or []
        if isinstance(item, dict)
    }
    union_dates: set[pd.Timestamp] = set()
    for frame in frames.values():
        union_dates.update(pd.Timestamp(value).normalize() for value in _slice_frame(frame, start, end).index)
    panel_start = min(union_dates) if union_dates else target_start.normalize()
    by_ticker: list[dict[str, Any]] = []
    for ticker, frame in sorted(frames.items()):
        normalized = _slice_frame(frame, start, end)
        if normalized.empty:
            continue
        dates = pd.to_datetime(normalized.index)
        first_date = dates.min().normalize()
        by_ticker.append(
            {
                "ticker": ticker,
                "sector": sector_map.get(ticker, "Unknown"),
                "first_date": dates.min().date().isoformat(),
                "last_date": dates.max().date().isoformat(),
                "row_count": int(len(normalized)),
                "starts_after_target": bool(first_date > panel_start),
                "ends_before_target": bool(dates.max() < target_end - pd.Timedelta(days=7)),
                "coverage_pct_of_spy_window": None,
            }
        )
    by_year: list[dict[str, Any]] = []
    for year in range(target_start.year, target_end.year + 1):
        active = [
            ticker
            for ticker, frame in sorted(frames.items())
            if not _slice_frame(frame, f"{year}-01-01", f"{year}-12-31").empty
        ]
        by_year.append({"year": year, "active_ticker_count": len(active), "active_tickers": active})
    return {
        "target_start": start,
        "target_end": end,
        "availability_mode": "panel",
        "tickers": by_ticker,
        "years": by_year,
        "load_errors": dict(load_errors or {}),
        "panel_trading_days": len(union_dates),
    }


def _load_historical_frame(ticker: str, start: str, end: str) -> pd.DataFrame:
    end_exclusive = (pd.Timestamp(end) + pd.Timedelta(days=1)).date().isoformat()
    return download_market_frame(ticker=ticker, period="max", interval="1d", start=start, end=end_exclusive, cache=True).frame


def _slice_frame(frame: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    sliced = frame.copy()
    if not isinstance(sliced.index, pd.DatetimeIndex):
        sliced.index = pd.to_datetime(sliced.index)
    sliced = sliced.sort_index()
    return sliced.loc[(sliced.index >= pd.Timestamp(start)) & (sliced.index <= pd.Timestamp(end))].copy()


def _buy_hold_curve(frame: pd.DataFrame, starting_cash: float) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame()
    normalized = _slice_frame(frame, str(pd.Timestamp(frame.index.min()).date()), str(pd.Timestamp(frame.index.max()).date()))
    price = pd.to_numeric(normalized["price"], errors="coerce").dropna()
    if price.empty or float(price.iloc[0]) <= 0:
        return pd.DataFrame()
    shares = float(starting_cash) / float(price.iloc[0])
    return pd.DataFrame({"date": [pd.Timestamp(index).date().isoformat() for index in price.index], "equity": [shares * float(value) for value in price]})


def _run_campaign1_baseline(
    frames: dict[str, pd.DataFrame],
    spy_frame: pd.DataFrame,
    *,
    start: str,
    windows: list[StressWindow],
) -> dict[str, Any]:
    sleeve_cash = 100_000.0 / max(1, len(frames))
    cfg = PipelineBacktestConfig(
        starting_cash=sleeve_cash,
        entry_cost_bps=5.0,
        exit_cost_bps=5.0,
        oos_start=start,
        enforce_universe_screen=False,
    )
    per_ticker: dict[str, Any] = {}
    failures: dict[str, str] = {}
    equity_series: dict[str, pd.Series] = {}
    trades: list[dict[str, Any]] = []
    for ticker, frame in sorted(frames.items()):
        try:
            result = run_pipeline_backtest(ticker, frame, config=cfg, benchmark_frame=spy_frame)
        except Exception as exc:
            failures[ticker] = str(exc)
            continue
        payload = result.to_dict()
        per_ticker[ticker] = {
            "metrics": payload.get("metrics"),
            "out_of_sample": payload.get("out_of_sample"),
            "trade_count": len(payload.get("trades") or []),
        }
        curve = pd.DataFrame(payload.get("equity_curve") or [])
        if not curve.empty:
            curve.index = pd.to_datetime(curve["date"])
            equity_series[ticker] = pd.to_numeric(curve["equity"], errors="coerce")
        for trade in payload.get("trades") or []:
            row = dict(trade)
            row["ticker"] = ticker
            trades.append(row)
    if not equity_series:
        return {
            "campaign1_baseline_status": {"included": False, "reason": "all per-name pipeline runs failed", "failures": failures},
            "metrics": {},
            "out_of_sample": {},
            "equity_curve": [],
            "trades": [],
            "stress_windows": [],
        }
    all_dates = pd.DatetimeIndex(sorted(set().union(*(set(series.index) for series in equity_series.values()))))
    aggregate = pd.Series(0.0, index=all_dates)
    for series in equity_series.values():
        aggregate += series.reindex(all_dates).ffill().fillna(sleeve_cash)
    equity_curve = [{"date": pd.Timestamp(date).date().isoformat(), "equity": float(value), "cash": 0.0, "position_value": float(value), "exposure": 1.0} for date, value in aggregate.items()]
    equity_df = pd.DataFrame(equity_curve)
    benchmark_curve = _buy_hold_curve(spy_frame, starting_cash=100_000.0)
    metrics = compute_equity_metrics(equity_df, trades, benchmark_curve=benchmark_curve)
    metrics["calmar_ratio"] = _calmar(metrics)
    out_of_sample = compute_equity_metrics(equity_df, trades, benchmark_curve=benchmark_curve)
    stress = _stress_results_for_curve(equity_df, trades, benchmark_curve, windows)
    return {
        "strategy_hash": "campaign1_per_name_hmm",
        "config": cfg.__dict__,
        "metrics": _json_safe(metrics),
        "in_sample": {},
        "out_of_sample": _json_safe(out_of_sample),
        "equity_curve": _json_safe(equity_curve),
        "trades": _json_safe(trades),
        "stress_windows": _json_safe(stress),
        "campaign1_baseline_status": {
            "included": True,
            "ticker_count": len(equity_series),
            "failed_tickers": failures,
            "note": "Aggregate of the existing per-name HMM pipeline path with equal starting sleeves.",
        },
    }


def _stress_results_for_curve(
    equity_df: pd.DataFrame,
    trades: list[dict[str, Any]],
    benchmark_curve: pd.DataFrame,
    windows: list[StressWindow],
) -> list[dict[str, Any]]:
    if equity_df.empty:
        return []
    dates = pd.to_datetime(equity_df["date"])
    rows: list[dict[str, Any]] = []
    for window in windows:
        start = pd.Timestamp(window.start)
        end_exclusive = pd.Timestamp(window.end) + pd.Timedelta(days=1)
        segment = equity_df.loc[(dates >= start) & (dates < end_exclusive)].copy()
        if segment.empty:
            continue
        segment_trades = [
            row for row in trades
            if row.get("exit_date") and start <= pd.Timestamp(row["exit_date"]) < end_exclusive
        ]
        metrics = compute_equity_metrics(segment, segment_trades, benchmark_curve=benchmark_curve)
        rows.append(
            {
                "key": window.key,
                "label": window.label,
                "start": window.start,
                "end": window.end,
                "metrics": metrics,
                "strategy_total_return": metrics.get("total_return"),
                "strategy_max_drawdown": metrics.get("max_drawdown"),
                "exposure_mean": metrics.get("exposure_pct"),
                "days_to_derisk": None,
                "trade_count": len(segment_trades),
            }
        )
    return rows


def _historical_row(arm: str, payload: dict[str, Any], result_path: Path) -> dict[str, Any]:
    row = _campaign_row(arm, payload)
    metrics = dict(payload.get("metrics") or {})
    row.update(
        {
            "sortino_ratio": metrics.get("sortino_ratio"),
            "trade_count": metrics.get("trade_count"),
            "result_path": str(result_path),
        }
    )
    return row


def _benchmark_relative_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_arm = {str(row.get("arm")): row for row in rows}
    benchmarks = {"SPY buy-hold": by_arm.get("C1_spy_buy_hold"), "SPY 200dma": by_arm.get("C2_spy_200dma")}
    output: list[dict[str, Any]] = []
    for row in rows:
        arm = str(row.get("arm"))
        for label, benchmark in benchmarks.items():
            if not benchmark or arm == str(benchmark.get("arm")):
                continue
            output.append(
                {
                    "arm": arm,
                    "benchmark": label,
                    "total_return_delta": _delta(row.get("total_return"), benchmark.get("total_return")),
                    "cagr_delta": _delta(row.get("annualized_return"), benchmark.get("annualized_return")),
                    "sharpe_delta": _delta(row.get("sharpe_ratio"), benchmark.get("sharpe_ratio")),
                    "max_drawdown_delta": _delta(row.get("max_drawdown"), benchmark.get("max_drawdown")),
                }
            )
    return output


def _period_returns(equity_curve: list[dict[str, Any]], freq: str) -> list[dict[str, Any]]:
    if not equity_curve:
        return []
    frame = pd.DataFrame(equity_curve)
    if frame.empty or "date" not in frame or "equity" not in frame:
        return []
    frame.index = pd.to_datetime(frame["date"])
    equity = pd.to_numeric(frame["equity"], errors="coerce").dropna()
    if equity.empty:
        return []
    rule = {"M": "ME", "Y": "YE"}.get(str(freq).upper(), freq)
    last = equity.resample(rule).last().dropna()
    if last.empty:
        return []
    returns = last.pct_change()
    returns.iloc[0] = (float(last.iloc[0]) / float(equity.iloc[0]) - 1.0) if float(equity.iloc[0]) else 0.0
    return [{"period": pd.Timestamp(index).date().isoformat(), "return": float(value)} for index, value in returns.items() if math.isfinite(float(value))]


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


def _build_historical_charts(summary: dict[str, Any], results: dict[str, dict[str, Any]], assets_dir: Path) -> dict[str, str]:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    rows = [row for row in summary.get("rows") or [] if isinstance(row, dict)]
    charts: dict[str, str] = {}

    def save(fig, name: str) -> None:
        path = assets_dir / f"{name}.png"
        fig.tight_layout()
        fig.savefig(path, dpi=160, bbox_inches="tight")
        plt.close(fig)
        charts[name] = str(path)

    ordered = [row for row in rows if str(row.get("arm")) in HISTORICAL_REPORT_ARM_ORDER]
    labels = [_arm_label(str(row.get("arm"))) for row in ordered]
    if ordered:
        fig, ax = plt.subplots(figsize=(9.5, 4.2))
        ax.bar(labels, [float(row.get("total_return") or 0.0) * 100 for row in ordered], color="#3b82f6")
        ax.set_ylabel("Total return (%)")
        ax.set_title("Historical total return by arm")
        ax.tick_params(axis="x", rotation=25)
        save(fig, "historical_total_return")

        fig, ax = plt.subplots(figsize=(9.5, 4.2))
        ax.bar(labels, [float(row.get("sharpe_ratio") or 0.0) for row in ordered], color="#059669")
        ax.set_ylabel("Sharpe")
        ax.set_title("Risk-adjusted return")
        ax.tick_params(axis="x", rotation=25)
        save(fig, "historical_sharpe")

        fig, ax = plt.subplots(figsize=(9.5, 4.2))
        ax.bar(labels, [float(row.get("max_drawdown") or 0.0) * 100 for row in ordered], color="#dc2626")
        ax.set_ylabel("Max drawdown (%)")
        ax.set_title("Drawdown by arm")
        ax.tick_params(axis="x", rotation=25)
        save(fig, "historical_drawdown")

        fig, ax1 = plt.subplots(figsize=(9.5, 4.2))
        ax1.bar(labels, [float(row.get("annualized_turnover") or 0.0) for row in ordered], color="#7c3aed", alpha=0.75, label="Turnover")
        ax1.set_ylabel("Annualized turnover")
        ax2 = ax1.twinx()
        ax2.plot(labels, [float(row.get("total_costs_paid") or 0.0) for row in ordered], color="#f97316", marker="o", label="Costs")
        ax2.set_ylabel("Total costs paid ($)")
        ax1.set_title("Turnover and costs")
        ax1.tick_params(axis="x", rotation=25)
        save(fig, "historical_turnover_costs")

    if results:
        fig, ax = plt.subplots(figsize=(10.5, 5.0))
        for arm in HISTORICAL_REPORT_ARM_ORDER:
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
            ax.plot(equity.index, equity / float(equity.iloc[0]), label=_arm_label(arm))
        ax.set_title("Growth of $1")
        ax.set_ylabel("Multiple")
        ax.legend(loc="best", fontsize=8)
        save(fig, "historical_equity_curves")

    yearly_points: list[dict[str, Any]] = []
    for arm in HISTORICAL_REPORT_ARM_ORDER:
        for row in (summary.get("yearly_returns") or {}).get(arm) or []:
            if not isinstance(row, dict):
                continue
            value = _float(row.get("return"))
            if value is None:
                continue
            yearly_points.append({"year": pd.Timestamp(row.get("period")).year, "arm": _arm_label(arm), "return": value * 100.0})
    if yearly_points:
        yearly = pd.DataFrame(yearly_points)
        pivot = yearly.pivot_table(index="year", columns="arm", values="return", aggfunc="last").sort_index()
        fig, ax = plt.subplots(figsize=(11.0, 5.2))
        for label in [_arm_label(arm) for arm in HISTORICAL_REPORT_ARM_ORDER if _arm_label(arm) in pivot.columns]:
            ax.plot(pivot.index, pivot[label], marker="o", linewidth=1.5, markersize=3.5, label=label)
        ax.axhline(0, color="#6b7280", linewidth=0.8)
        ax.set_title("Performance by year")
        ax.set_ylabel("Annual return (%)")
        ax.set_xlabel("Year")
        ax.legend(loc="best", fontsize=8, ncols=2)
        save(fig, "historical_yearly_returns")
    return charts


def _historical_report_html(summary: dict[str, Any], charts: dict[str, str], results: dict[str, dict[str, Any]]) -> str:
    rows = [row for row in summary.get("rows") or [] if isinstance(row, dict)]
    verdict = dict(summary.get("verdict") or {})
    l1_candidate = bool(verdict.get("l1_deeper_validation_candidate"))
    l2_promoted = bool(verdict.get("l2_promoted"))
    l3_promoted = bool(verdict.get("l3_promoted"))
    title = f"Historical Portfolio Campaign {_period_label(str(summary.get('start')), str(summary.get('end')))}"
    chart_tags = "\n".join(
        f'<figure><img src="assets/{html.escape(Path(path).name)}" alt="{html.escape(name)}"><figcaption>{html.escape(name.replace("_", " ").title())}</figcaption></figure>'
        for name, path in charts.items()
    )
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>{html.escape(title)}</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; color: #111827; margin: 28px; line-height: 1.45; }}
    h1, h2, h3 {{ margin-bottom: 8px; }}
    .summary {{ display: grid; grid-template-columns: repeat(4, minmax(180px, 1fr)); gap: 12px; margin: 16px 0 22px; max-width: 1120px; }}
    .card {{ border: 1px solid #d1d5db; border-radius: 8px; padding: 12px 14px; background: #fff; }}
    .label {{ color: #6b7280; font-size: 12px; text-transform: uppercase; letter-spacing: 0; }}
    .value {{ font-size: 22px; font-weight: 700; }}
    .pass {{ color: #047857; }}
    .fail {{ color: #b91c1c; }}
    table {{ border-collapse: collapse; width: 100%; max-width: 1180px; margin: 12px 0 24px; font-size: 13px; }}
    th, td {{ border: 1px solid #e5e7eb; padding: 7px 9px; text-align: right; vertical-align: top; }}
    th:first-child, td:first-child, td.text, th.text {{ text-align: left; }}
    th {{ background: #f3f4f6; }}
    figure {{ margin: 18px 0; max-width: 1080px; }}
    img {{ max-width: 100%; border: 1px solid #e5e7eb; border-radius: 6px; }}
    figcaption {{ color: #4b5563; font-size: 12px; }}
    .note {{ max-width: 1120px; background: #f9fafb; border: 1px solid #e5e7eb; border-radius: 8px; padding: 12px 14px; }}
    details {{ max-width: 1180px; margin: 10px 0; }}
    summary {{ cursor: pointer; font-weight: 600; }}
  </style>
</head>
<body>
  <h1>{html.escape(title)}</h1>
  <p>Generated {html.escape(str(summary.get("generated_at")))} at git SHA <code>{html.escape(str(summary.get("git_sha")))}</code>. Window: <code>{html.escape(str(summary.get("start")))}</code> to <code>{html.escape(str(summary.get("end")))}</code>. No live trading defaults or production execution paths were changed.</p>
  <section class="summary">
    <div class="card"><div class="label">L1 paper validation</div><div class="value {'pass' if l1_candidate else 'fail'}">{'Yes' if l1_candidate else 'No'}</div></div>
    <div class="card"><div class="label">L2 HMM brake promoted</div><div class="value {'pass' if l2_promoted else 'fail'}">{'Yes' if l2_promoted else 'No'}</div></div>
    <div class="card"><div class="label">L3 momentum promoted</div><div class="value {'pass' if l3_promoted else 'fail'}">{'Yes' if l3_promoted else 'No'}</div></div>
    <div class="card"><div class="label">Available basket names</div><div class="value">{int(summary.get("available_ticker_count") or 0)} / {int(summary.get("basket_size") or 0)}</div></div>
  </section>
  <div class="note">
    <strong>Executive readout.</strong>
    L1 remains a deeper paper-validation candidate only if it beats L0 on the promotion rules, clears SPY 200dma on Sharpe, and survives the 2x cost check.
    L2 and L3 remain research arms unless their layer-support flags are true in this expanded window.
  </div>
  <h2>Strategy Used By Arm</h2>
  <p>Each arm is a serialized strategy stack. The table below explains the investment rule in plain English and shows the actual layer configuration used by the portfolio engine.</p>
  {_html_table(["Arm", "Role", "Strategy Used", "Risk / Reweighting Rule", "Layer Stack"], _strategy_explanation_rows(results), text_columns={0, 1, 2, 3, 4})}
  <h2>Arm Results</h2>
  {_html_table(["Arm", "Total Return", "CAGR", "Vol", "Sharpe", "Sortino", "Max DD", "Calmar", "Turnover", "Costs", "Trades", "Exposure"], _arm_table_rows(rows))}
  <h2>Performance By Year</h2>
  <p>Annual returns use the same availability-aware panel as the campaign. Blank cells mean that arm did not have a valid return for that year.</p>
  {_html_table(["Year", *[_arm_label(arm) for arm in HISTORICAL_REPORT_ARM_ORDER]], _yearly_performance_rows(summary))}
  <h2>Charts</h2>
  {chart_tags}
  <h2>Benchmark Relative</h2>
  {_html_table(["Arm", "Benchmark", "Return Delta", "CAGR Delta", "Sharpe Delta", "Max DD Delta"], _relative_table_rows(summary.get("benchmark_relative") or []))}
  <h2>Stress Windows</h2>
  {_html_table(["Arm", "Window", "Return", "Max DD", "Exposure", "Trades"], _stress_table_rows(rows))}
  <h2>Coverage</h2>
  <p>{html.escape(str(summary.get("adjusted_price_note") or ""))}</p>
  {_html_table(["Ticker", "Sector", "First Date", "Last Date", "Rows", "Starts Late"], _coverage_ticker_rows(summary))}
  <details open><summary>Coverage By Year</summary>{_html_table(["Year", "Active Names", "Tickers"], _coverage_year_rows(summary))}</details>
  <details><summary>Yearly Returns - Long Format</summary>{_html_table(["Arm", "Year", "Return"], _return_table_rows(summary.get("yearly_returns") or {}))}</details>
  <details><summary>Monthly Returns</summary>{_html_table(["Arm", "Month", "Return"], _return_table_rows(summary.get("monthly_returns") or {}))}</details>
  <h2>Limitations</h2>
  <ul>
    {''.join(f"<li>{html.escape(str(item))}</li>" for item in summary.get("limitations") or [])}
  </ul>
  <h2>Reproduction</h2>
  <pre>{html.escape(str(summary.get("single_command") or ""))}</pre>
</body>
</html>
"""


def _arm_table_rows(rows: list[dict[str, Any]]) -> list[list[Any]]:
    return [
        [
            _arm_label(str(row.get("arm") or "")),
            _fmt_pct(row.get("total_return")),
            _fmt_pct(row.get("annualized_return")),
            _fmt_pct(row.get("annualized_volatility")),
            _fmt_num(row.get("sharpe_ratio")),
            _fmt_num(row.get("sortino_ratio")),
            _fmt_pct(row.get("max_drawdown")),
            _fmt_num(row.get("calmar_ratio")),
            _fmt_num(row.get("annualized_turnover")),
            _fmt_num(row.get("total_costs_paid")),
            _fmt_num(row.get("trade_count")),
            _fmt_pct(row.get("exposure_pct")),
        ]
        for row in rows
    ]


def _strategy_explanation_rows(results: dict[str, dict[str, Any]]) -> list[list[Any]]:
    rows: list[list[Any]] = []
    for arm in HISTORICAL_REPORT_ARM_ORDER:
        if arm not in results and arm != "Campaign1_per_name_HMM":
            continue
        rows.append([_arm_label(arm), *_strategy_explanation_cells(arm, results.get(arm) or {})])
    return rows


def _strategy_explanation_cells(arm: str, payload: dict[str, Any]) -> list[str]:
    spec_payload = payload.get("strategy_spec") if isinstance(payload, dict) else None
    spec = StrategySpec.from_dict(spec_payload) if isinstance(spec_payload, dict) and spec_payload else None
    if arm == "L0":
        return [
            "Portfolio baseline",
            "Owns the available Campaign 2 basket at equal weights with target exposure at 100%. The precomputed regime-enriched price history is loaded, but no regime brake or momentum selection is applied.",
            "No explicit de-risking overlay. Risk is only affected by availability-aware membership, integer-share sizing, transaction costs, and monthly or drift-triggered rebalancing.",
            _strategy_stack_text(spec),
        ]
    if arm == "L1":
        return [
            "Volatility-target overlay",
            "Starts from L0's equal-weight basket, then scales total portfolio exposure toward a 15% annualized volatility target using recent portfolio returns.",
            "Exposure is clipped between 25% and 100%, so high-volatility periods raise cash instead of changing the stock ranking. No HMM brake or momentum tilt is active.",
            _strategy_stack_text(spec),
        ]
    if arm == "L2":
        return [
            "HMM brake research arm",
            "Starts from L1, then uses the cached HMM regime labels and day-5 forward probabilities to remove names in Bear regimes and control gross exposure in broad stress.",
            "A Bear label excludes a name until it has 3 consecutive reentry signals. If at least 50% of names are Bear, or drawdown is at least 8% while at least half the book is non-Bull, exposure is capped at 50%.",
            _strategy_stack_text(spec),
        ]
    if arm == "L3":
        return [
            "Momentum tilt research arm",
            "Starts from L2, then allocates only to the top half of eligible names ranked by 12-1 momentum instead of equal-weighting every eligible name.",
            "Uses the same HMM brake and exposure caps as L2, then concentrates capital in higher-momentum survivors. This raises selection pressure and turnover versus L1/L2.",
            _strategy_stack_text(spec),
        ]
    if arm == "C1_spy_buy_hold":
        return [
            "Benchmark control",
            "Runs a single-instrument SPY buy-and-hold control through the same portfolio engine.",
            "No timing, brake, or volatility overlay. It stays fully invested except for normal execution frictions from the shared engine.",
            _strategy_stack_text(spec),
        ]
    if arm == "C2_spy_200dma":
        return [
            "Timing control",
            "Owns SPY only when SPY has closed above its 200-day moving average for 5 consecutive trading days; otherwise the strategy moves to cash.",
            "This is a simple benchmark risk switch. It tests whether the more complex portfolio arms earn their complexity versus a transparent 200dma rule.",
            _strategy_stack_text(spec),
        ]
    if arm == "Campaign1_per_name_HMM":
        status = payload.get("campaign1_baseline_status") if isinstance(payload, dict) else None
        note = ""
        if isinstance(status, dict) and status.get("note"):
            note = f" {status.get('note')}"
        return [
            "Legacy baseline",
            "Aggregates the existing Campaign 1 per-name HMM pipeline with equal starting sleeves across the basket.",
            "This path is included for continuity against the earlier per-ticker HMM experiment, not as a promoted portfolio-engine arm." + note,
            "legacy_pipeline=per_name_HMM; sleeve=equal_starting_capital; engine=Campaign 1 pipeline path",
        ]
    return ["Research arm", str((spec.description if spec else "") or "No description available."), "", _strategy_stack_text(spec)]


def _strategy_stack_text(spec: StrategySpec | None) -> str:
    if spec is None:
        return "No serialized StrategySpec available."
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


def _relative_table_rows(rows: list[dict[str, Any]]) -> list[list[Any]]:
    return [
        [
            _arm_label(str(row.get("arm") or "")),
            _arm_label(str(row.get("benchmark") or "")),
            _fmt_pct(row.get("total_return_delta")),
            _fmt_pct(row.get("cagr_delta")),
            _fmt_num(row.get("sharpe_delta")),
            _fmt_pct(row.get("max_drawdown_delta")),
        ]
        for row in rows
    ]


def _stress_table_rows(rows: list[dict[str, Any]]) -> list[list[Any]]:
    output: list[list[Any]] = []
    for row in rows:
        for item in row.get("stress_windows") or []:
            if not isinstance(item, dict):
                continue
            output.append(
                [
                    _arm_label(str(row.get("arm") or "")),
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
    ]


def _coverage_year_rows(summary: dict[str, Any]) -> list[list[Any]]:
    return [
        [row.get("year"), row.get("active_ticker_count"), ", ".join(row.get("active_tickers") or [])]
        for row in ((summary.get("coverage") or {}).get("years") or [])
    ]


def _yearly_performance_rows(summary: dict[str, Any]) -> list[list[Any]]:
    by_year: dict[int, dict[str, Any]] = {}
    for arm in HISTORICAL_REPORT_ARM_ORDER:
        for row in (summary.get("yearly_returns") or {}).get(arm) or []:
            if not isinstance(row, dict):
                continue
            try:
                year = int(pd.Timestamp(row.get("period")).year)
            except Exception:
                continue
            by_year.setdefault(year, {})[arm] = row.get("return")
    return [
        [year, *[_fmt_pct(values.get(arm)) if arm in values else "" for arm in HISTORICAL_REPORT_ARM_ORDER]]
        for year, values in sorted(by_year.items())
    ]


def _return_table_rows(periods_by_arm: dict[str, Any]) -> list[list[Any]]:
    rows: list[list[Any]] = []
    for arm, periods in sorted(periods_by_arm.items()):
        for row in periods or []:
            if isinstance(row, dict):
                rows.append([_arm_label(arm), row.get("period"), _fmt_pct(row.get("return"))])
    return rows


def _arm_label(arm: str) -> str:
    return HISTORICAL_REPORT_ARM_LABELS.get(str(arm), str(arm))


def _html_table(headers: list[str], rows: list[list[Any]], *, text_columns: set[int] | None = None) -> str:
    text_columns = text_columns or {0}
    head = "".join(f"<th class=\"{'text' if idx in text_columns else ''}\">{html.escape(header)}</th>" for idx, header in enumerate(headers))
    body_rows = []
    for row in rows:
        cells = []
        for idx, value in enumerate(row):
            cls = " class=\"text\"" if idx in text_columns else ""
            cells.append(f"<td{cls}>{html.escape(str(value if value is not None else ''))}</td>")
        body_rows.append("<tr>" + "".join(cells) + "</tr>")
    return "<table><thead><tr>" + head + "</tr></thead><tbody>" + "".join(body_rows) + "</tbody></table>"


def _limitations(*, start: str, end: str, load_errors: dict[str, str]) -> list[str]:
    period = _period_label(start, end)
    limitations = [
        f"The basket is the current Campaign 2 30-name basket, so the {period} test is survivorship-biased and does not include delisted names or period-correct sector leaders.",
        "The panel is availability-aware: a ticker can only enter after valid adjusted-price history exists; no replacement ticker is substituted for missing early history.",
        "Years before every basket constituent has valid history may have fewer active names, so equal-weight basket concentration can be higher than in full-coverage years.",
        "Adjusted prices include split/dividend effects where the upstream provider supplies them; provider revisions can change historical adjusted series.",
        "The campaign is research-only and intentionally does not change live trading defaults, broker routing, or agent production policy.",
    ]
    if load_errors:
        limitations.append(f"{len(load_errors)} basket tickers had unavailable or unusable history and were excluded from the historical panel.")
    return limitations


def _append_run_log(root: Path, message: str) -> None:
    root.mkdir(parents=True, exist_ok=True)
    with (root / "run_log.md").open("a", encoding="utf-8") as handle:
        handle.write(f"- {dt.datetime.now(dt.timezone.utc).isoformat()} {message}\n")


def _calmar(metrics: dict[str, Any]) -> float | None:
    annualized = _float(metrics.get("annualized_return"))
    drawdown = abs(_float(metrics.get("max_drawdown")) or 0.0)
    if annualized is None or drawdown <= 0:
        return None
    return annualized / drawdown


def _delta(left: Any, right: Any) -> float | None:
    parsed_left = _float(left)
    parsed_right = _float(right)
    if parsed_left is None or parsed_right is None:
        return None
    return parsed_left - parsed_right


def _float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        parsed = float(value)
    except Exception:
        return None
    return parsed if math.isfinite(parsed) else None
