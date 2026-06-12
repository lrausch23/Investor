from __future__ import annotations

import argparse
import json
from dataclasses import replace
from pathlib import Path
from typing import Any

from .config import DEFAULT_TICKERS
from .data import download_market_frame
from .digest import digest_to_dict, digest_to_text, generate_weekly_digest
from .hmm_engine import fit_regime_model, fit_regime_model_weekly
from .investor_adapter import (
    get_investor_db_path,
    get_portfolio_positions,
    get_portfolio_tickers_filtered,
    get_portfolio_tickers,
    get_tax_assumptions,
    get_wash_sale_risk,
    positions_by_ticker_and_account,
    positions_by_ticker,
)
from .llm_layer import build_qualitative_assessment
from .persistence import get_calibration_data, get_setting, save_regime_event, save_sentiment, upsert_thesis
from .diagnostics import calibration_payload
from .backtest import compare_to_benchmark, run_backtest
from .meta_labeler import (
    MetaLabelerEngine,
    auto_load_active_model,
    extract_meta_features,
    list_saved_versions,
    meta_labeler_result_can_influence,
    normalize_meta_labeler_veto_mode,
)
from .pipeline_backtest import PipelineBacktestConfig, PipelineSignal, _ProductionSignalProvider, run_pipeline_backtest, run_pipeline_backtest_for_ticker
from .reporting import TickerReport, summarize_relative_strength
from .signals import (
    build_composite_signal,
    compute_technicals,
    confidence_trajectory,
    forward_regime_curve,
    intra_regime_signal,
    multi_timeframe_signal,
    sentiment_momentum,
    signal_from_forward_curve,
    tax_adjusted_signals,
)
from .visualization import save_regime_chart
from .threshold_sweep import load_threshold_grid, run_threshold_sweep, write_sweep_rows
from .alpha_campaign import (
    DEFAULT_BASKET_PATH,
    DEFAULT_CAMPAIGN_DIR,
    DEFAULT_REPORT_PATH,
    campaign_status,
    render_report,
    run_campaign_phase,
    select_basket,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Market regime detection using a 3-state Hidden Markov Model.")
    subparsers = parser.add_subparsers(dest="command")
    pipeline_parser = subparsers.add_parser("pipeline-backtest", help="Run the event-driven production-pipeline backtest.")
    pipeline_parser.add_argument("ticker", nargs="?", help="Ticker to backtest.")
    pipeline_parser.add_argument("--tickers", nargs="+", help="Tickers to backtest in a basket A/B run.")
    pipeline_parser.add_argument("--period", default="10y", help="Market data period. Default: 10y")
    pipeline_parser.add_argument("--start", default=None, help="Explicit market data start date, e.g. 2019-01-01.")
    pipeline_parser.add_argument("--end", default=None, help="Explicit market data end date, e.g. 2025-12-31.")
    pipeline_parser.add_argument("--cache", action="store_true", help="Use the HMM_DATA_DIR price cache.")
    pipeline_parser.add_argument("--stress-report", action="store_true", help="Print the stress-window summary table.")
    pipeline_parser.add_argument("--oos-start", default=None, help="Out-of-sample start date, e.g. 2025-01-01.")
    pipeline_parser.add_argument("--benchmark", default="SPY", help="Benchmark ticker. Default: SPY")
    pipeline_parser.add_argument("--json", dest="json_path", default=None, help="Write JSON report to this path.")
    pipeline_parser.add_argument("--meta-labeler-ab", action="store_true", help="Run baseline vs meta-labeler-veto pipeline comparison.")
    pipeline_parser.add_argument("--veto-mode", choices=["gate", "size_only"], default="gate", help="Meta-labeler A/B mode. Default: gate")
    sweep_parser = subparsers.add_parser("threshold-sweep", help="Run a grid sweep through the production-pipeline backtest.")
    sweep_parser.add_argument("--tickers", nargs="+", required=True, help="Tickers to sweep.")
    sweep_parser.add_argument("--period", default="10y", help="Market data period. Default: 10y")
    sweep_parser.add_argument("--start", default=None, help="Explicit market data start date, e.g. 2019-01-01.")
    sweep_parser.add_argument("--end", default=None, help="Explicit market data end date, e.g. 2025-12-31.")
    sweep_parser.add_argument("--cache", action="store_true", help="Use the HMM_DATA_DIR price cache.")
    sweep_parser.add_argument("--stress-report", action="store_true", help="Include stress-window aggregate columns.")
    sweep_parser.add_argument("--oos-start", default=None, help="Out-of-sample start date, e.g. 2025-01-01.")
    sweep_parser.add_argument("--benchmark", default="SPY", help="Benchmark ticker. Default: SPY")
    sweep_parser.add_argument("--grid-json", default=None, help="Optional JSON parameter grid.")
    sweep_parser.add_argument("--output-json", default=None, help="Write sweep rows to JSON.")
    sweep_parser.add_argument("--output-csv", default=None, help="Write sweep rows to CSV.")
    sweep_parser.add_argument("--lookback-window", type=int, default=20)
    sweep_parser.add_argument("--training-window", type=int, default=504)
    sweep_parser.add_argument("--refit-step", type=int, default=21)
    sweep_parser.add_argument("--macro-weighting", action="store_true")
    sweep_parser.add_argument("--macro-weight", type=float, default=1.5)
    sweep_parser.add_argument("--hmm-covariance", choices=["diag", "full", "spherical", "tied"], default="diag")
    sweep_parser.add_argument("--hmm-n-seeds", type=int, default=1)
    sweep_parser.add_argument("--seed-agreement-min", type=float, default=0.8)
    campaign_parser = subparsers.add_parser("alpha-campaign", help="Run the pre-registered regime alpha campaign.")
    campaign_subparsers = campaign_parser.add_subparsers(dest="campaign_command")
    select_parser = campaign_subparsers.add_parser("select-basket", help="Select and pin the campaign basket.")
    select_parser.add_argument("--output", default=str(DEFAULT_BASKET_PATH), help="Basket JSON output path.")
    select_parser.add_argument("--names-per-sector", type=int, default=3)
    select_parser.add_argument("--candidates", nargs="+", default=None, help="Optional candidate tickers for the screen.")
    run_parser = campaign_subparsers.add_parser("run", help="Run one campaign phase.")
    run_parser.add_argument("--phase", type=int, choices=[0, 1, 2, 3], required=True)
    run_parser.add_argument("--resume", action="store_true")
    run_parser.add_argument("--basket", default=str(DEFAULT_BASKET_PATH))
    run_parser.add_argument("--campaign-dir", default=str(DEFAULT_CAMPAIGN_DIR))
    report_parser = campaign_subparsers.add_parser("report", help="Render ALPHA_CAMPAIGN_REPORT.md from campaign artifacts.")
    report_parser.add_argument("--basket", default=str(DEFAULT_BASKET_PATH))
    report_parser.add_argument("--campaign-dir", default=str(DEFAULT_CAMPAIGN_DIR))
    report_parser.add_argument("--output", default=str(DEFAULT_REPORT_PATH))
    status_parser = campaign_subparsers.add_parser("status", help="Show campaign artifact status.")
    status_parser.add_argument("--campaign-dir", default=str(DEFAULT_CAMPAIGN_DIR))
    parser.add_argument("--tickers", nargs="+", help="Tickers to analyze.")
    parser.add_argument("--benchmark", default="SOXX", help="Benchmark ticker. Default: SOXX")
    parser.add_argument("--period", default="3y", help="yfinance period string. Default: 3y")
    parser.add_argument("--interval", default="1d", help="Price interval. Default: 1d")
    parser.add_argument("--lookback-window", type=int, default=20, help="Feature lookback window in trading days.")
    parser.add_argument("--training-window", type=int, default=504, help="Walk-forward training window in trading days.")
    parser.add_argument("--refit-step", type=int, default=21, help="Refit frequency for the walk-forward HMM in trading days.")
    parser.add_argument("--barrier-vol-multiplier", type=float, default=1.0, help="Triple-barrier width multiplier.")
    parser.add_argument("--macro-weighting", action="store_true", help="Boost ^VIX and ^TNX influence in the HMM features.")
    parser.add_argument("--frontier-on", action="store_true", help="Enable live OpenAI/Gemini calls.")
    parser.add_argument("--frontier-provider", default="auto", choices=["auto", "openai", "gemini", "claude", "ollama", "best"])
    parser.add_argument("--chart-dir", default=str(Path(__file__).resolve().parents[1] / "charts"))
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--weekly-digest", action="store_true")
    parser.add_argument("--digest-format", choices=["json", "text"], default="json")
    parser.add_argument("--backtest", action="store_true")
    parser.add_argument("--backtest-period", default="10y")
    return parser.parse_args()


def _load_meta_labeler_for_ab() -> MetaLabelerEngine:
    engine = MetaLabelerEngine()
    active_version = get_setting("meta_labeler_active_version")
    if active_version:
        loaded = auto_load_active_model(engine, active_version)
        if loaded.get("loaded") and engine.is_ready():
            return engine
    versions = list_saved_versions()
    if versions:
        engine.load_model(str(versions[-1]["path"]))
        if engine.is_ready():
            return engine
    raise RuntimeError("No trained meta-labeler model is available for --meta-labeler-ab.")


def _features_from_pipeline_context(signal: PipelineSignal, history) -> dict[str, float]:
    row = history.iloc[-1].copy()
    price = history["price"].astype(float)
    returns = price.pct_change()
    volume = history["volume"].astype(float) if "volume" in history else None
    if volume is not None and len(volume) >= 20:
        vol_mean = float(volume.tail(20).mean())
        vol_std = float(volume.tail(20).std(ddof=0) or 0.0)
        row["volume_zscore"] = (float(volume.iloc[-1]) - vol_mean) / vol_std if vol_std > 0 else 0.0
    row["canonical_state"] = {"Bull": 0, "Neutral": 1, "Bear": 2}.get(str(signal.regime), 1)
    row["return"] = float(returns.iloc[-1]) if len(returns) and returns.notna().iloc[-1] else 0.0
    row["volatility"] = float(returns.tail(20).std(ddof=0) or 0.0) if len(returns) >= 2 else 0.0
    row["vix_change"] = float(history["vix"].diff().iloc[-1]) if "vix" in history and len(history) >= 2 else 0.0
    row["yield_10y_change"] = float(history["yield_10y"].diff().iloc[-1]) if "yield_10y" in history and len(history) >= 2 else 0.0
    row["current_price"] = float(price.iloc[-1])
    row["composite_strength"] = float(signal.composite_strength or 0.0)
    row["transition_risk"] = float(signal.transition_risk or 0.0)
    row["regime_days"] = int(signal.regime_days or 1)
    row["p_bull_day5"] = signal.p_bull_day5
    row["p_bear_day5"] = signal.p_bear_day5
    row["p_neutral_day5"] = signal.p_neutral_day5
    row["price_targets"] = dict(signal.price_targets or {})
    row["atr_14"] = signal.atr_14 or (signal.price_targets or {}).get("atr_value")
    row["risk_reward_ratio"] = (signal.price_targets or {}).get("risk_reward_ratio")
    row["entry_price"] = (signal.price_targets or {}).get("entry_price")
    row["target_price"] = (signal.price_targets or {}).get("target_price") or (signal.price_targets or {}).get("exit_price")
    row["stop_price"] = (signal.price_targets or {}).get("stop_price")
    try:
        technicals = compute_technicals(history["price"], history["volume"], history.get("high"), history.get("low"))
        latest = technicals.dropna().iloc[-1]
        row["rsi_14"] = latest.get("rsi_14")
        row["macd_histogram"] = latest.get("macd_histogram")
    except Exception:
        pass
    return extract_meta_features(row)


class _MetaLabelerVetoProvider:
    def __init__(self, engine: MetaLabelerEngine, veto_mode: str = "gate") -> None:
        self._base = _ProductionSignalProvider()
        self._engine = engine
        self._veto_mode = normalize_meta_labeler_veto_mode(veto_mode)
        self._probabilities: list[float] = []
        self._analyzed_count = 0
        self._passthrough_count = 0

    def __call__(self, ticker, date, history, config, previous_regime):
        signal = self._base(ticker, date, history, config, previous_regime)
        if signal is None or signal.composite_action not in {"Buy", "Strong Buy"}:
            return signal
        result = self._engine.analyze(ticker, _features_from_pipeline_context(signal, history), None)
        self._analyzed_count += 1
        can_influence = meta_labeler_result_can_influence(result)
        if can_influence:
            self._probabilities.append(float(result.confidence))
        else:
            self._passthrough_count += 1
        if self._veto_mode == "gate" and can_influence and result.signal == "veto":
            return replace(signal, composite_action="Hold")
        if self._veto_mode == "size_only" and can_influence:
            # Never blocks; the calibrated probability flows into entry sizing
            # (0.5-1.0x in _build_entry_order). Without this, size_only is
            # indistinguishable from disabling the labeler entirely.
            return replace(signal, meta_labeler_probability=float(result.confidence))
        return signal

    def evidence_summary(self) -> dict[str, object]:
        return _meta_labeler_evidence_summary(self._engine, self._probabilities, self._analyzed_count, self._passthrough_count)


def _finite_number(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        parsed = float(value)
    except Exception:
        return None
    return parsed if np_isfinite(parsed) else None


def np_isfinite(value: float) -> bool:
    try:
        return bool(value == value and value not in (float("inf"), float("-inf")))
    except Exception:
        return False


def _probability_dispersion(probabilities: list[float]) -> dict[str, object]:
    if not probabilities:
        return {"probability_count": 0, "probability_std": None, "probability_iqr": None}
    ordered = sorted(float(value) for value in probabilities)
    count = len(ordered)
    mean = sum(ordered) / count
    variance = sum((value - mean) ** 2 for value in ordered) / count

    def percentile(p: float) -> float:
        if count == 1:
            return ordered[0]
        rank = (count - 1) * p
        lower = int(rank)
        upper = min(lower + 1, count - 1)
        weight = rank - lower
        return ordered[lower] * (1.0 - weight) + ordered[upper] * weight

    return {
        "probability_count": count,
        "probability_std": variance ** 0.5,
        "probability_iqr": percentile(0.75) - percentile(0.25),
    }


def _meta_labeler_evidence_summary(
    engine: MetaLabelerEngine,
    probabilities: list[float],
    analyzed_count: int = 0,
    passthrough_count: int = 0,
) -> dict[str, object]:
    metrics = dict(getattr(engine, "_training_metrics", {}) or {})
    positive_rate = _finite_number(metrics.get("positive_rate_train") or metrics.get("positive_rate"))
    base_rate_brier = positive_rate * (1.0 - positive_rate) if positive_rate is not None else None
    calibrated_brier = _finite_number(metrics.get("brier_score_calibrated"))
    if calibrated_brier is None:
        calibrated_brier = _finite_number(metrics.get("brier_score"))
    dispersion = _probability_dispersion(probabilities)
    return {
        "oof_roc_auc": _finite_number(metrics.get("roc_auc")),
        "positive_rate_train": positive_rate,
        "base_rate_brier": base_rate_brier,
        "calibrated_brier": calibrated_brier,
        "calibration_lift_vs_base_rate": (
            base_rate_brier - calibrated_brier
            if base_rate_brier is not None and calibrated_brier is not None
            else None
        ),
        "analyzed_signals": int(analyzed_count),
        "passthrough_signals": int(passthrough_count),
        **dispersion,
    }


def _backtest_summary(result) -> dict[str, object]:
    metrics = dict(result.metrics or {})
    return {
        "total_return": metrics.get("total_return"),
        "sharpe_ratio": metrics.get("sharpe_ratio"),
        "max_drawdown": metrics.get("max_drawdown"),
        "trade_count": metrics.get("trade_count"),
        "exit_type_counts": dict(metrics.get("exit_type_counts") or result.exit_type_counts or {}),
    }


def _diff_numeric(after: dict[str, object], before: dict[str, object]) -> dict[str, object]:
    diff: dict[str, object] = {}
    for key in ("total_return", "sharpe_ratio", "max_drawdown", "trade_count"):
        left = after.get(key)
        right = before.get(key)
        diff[key] = (float(left) - float(right)) if left is not None and right is not None else None
    raw_before_counts = before.get("exit_type_counts")
    raw_after_counts = after.get("exit_type_counts")
    before_counts: dict[str, Any] = dict(raw_before_counts) if isinstance(raw_before_counts, dict) else {}
    after_counts: dict[str, Any] = dict(raw_after_counts) if isinstance(raw_after_counts, dict) else {}
    diff["exit_type_counts"] = {
        key: int(after_counts.get(key, 0)) - int(before_counts.get(key, 0))
        for key in sorted(set(before_counts) | set(after_counts))
    }
    return diff


def _format_summary_fields(summary: dict[str, object]) -> list[str]:
    evidence_fields = [
        "oof_roc_auc",
        "base_rate_brier",
        "calibration_lift_vs_base_rate",
        "probability_std",
        "probability_iqr",
    ]
    trade_count = summary.get("trade_count")
    trade_count_text = str(int(trade_count)) if isinstance(trade_count, (int, float)) else ""
    return [
        "" if summary.get("total_return") is None else f"{float(summary['total_return']):.6f}",
        "" if summary.get("sharpe_ratio") is None else f"{float(summary['sharpe_ratio']):.6f}",
        "" if summary.get("max_drawdown") is None else f"{float(summary['max_drawdown']):.6f}",
        trade_count_text,
        json.dumps(summary.get("exit_type_counts") or {}, sort_keys=True),
        *[
            "" if summary.get(field) is None else f"{float(summary[field]):.6f}"
            for field in evidence_fields
        ],
    ]


def _ab_segment_rows(item: dict[str, object], mode_label: str) -> list[tuple[str, str, dict[str, object]]]:
    baseline = item.get("baseline")
    meta = item.get("meta_veto")
    rows: list[tuple[str, str, dict[str, object]]] = [
        ("full", "baseline", baseline if isinstance(baseline, dict) else {}),
        ("full", mode_label, meta if isinstance(meta, dict) else {}),
    ]
    if isinstance(baseline, dict) and isinstance(meta, dict):
        rows.append(("full", "diff", item.get("diff") if isinstance(item.get("diff"), dict) else _diff_numeric(meta, baseline)))

    for segment in ("in_sample", "out_of_sample"):
        segment_payload = item.get(segment)
        if not isinstance(segment_payload, dict):
            continue
        segment_baseline = segment_payload.get("baseline")
        segment_meta = segment_payload.get(mode_label)
        if not isinstance(segment_baseline, dict) or not isinstance(segment_meta, dict):
            continue
        rows.extend(
            [
                (segment, "baseline", segment_baseline),
                (segment, mode_label, segment_meta),
                (segment, "diff", _diff_numeric(segment_meta, segment_baseline)),
            ]
        )
    return rows


def _format_meta_labeler_ab(payload: dict[str, object]) -> str:
    mode_label = str(payload.get("mode_label") or "meta_veto")
    results = payload.get("results")
    item = results[0] if isinstance(results, list) and results and isinstance(results[0], dict) else payload
    lines = [
        "segment,run,total_return,sharpe_ratio,max_drawdown,trade_count,exit_type_counts,"
        "oof_roc_auc,base_rate_brier,calibration_lift_vs_base_rate,probability_std,probability_iqr"
    ]
    for segment, name, summary in _ab_segment_rows(item, mode_label):
        lines.append(",".join([segment, name, *_format_summary_fields(summary)]))
    return "\n".join(lines)


def _format_meta_labeler_ab_basket(payload: dict[str, object]) -> str:
    rows = payload.get("results")
    if not isinstance(rows, list):
        return _format_meta_labeler_ab(payload)
    lines = [
        "ticker,segment,run,total_return,sharpe_ratio,max_drawdown,trade_count,exit_type_counts,"
        "oof_roc_auc,base_rate_brier,calibration_lift_vs_base_rate,probability_std,probability_iqr"
    ]
    for item in rows:
        if not isinstance(item, dict):
            continue
        ticker = str(item.get("ticker") or "")
        mode_label = str(item.get("mode_label") or "meta_veto")
        for segment, name, summary in _ab_segment_rows(item, mode_label):
            lines.append(",".join([ticker, segment, name, *_format_summary_fields(summary)]))
    return "\n".join(lines)


def _format_stress_report(windows: list[dict[str, object]]) -> str:
    lines = [
        "key,label,start,end,strategy_total_return,benchmark_total_return,"
        "strategy_max_drawdown,benchmark_max_drawdown,exposure_pct,trade_count,days_to_bear_flag,exit_type_counts"
    ]
    for row in windows:
        if not isinstance(row, dict):
            continue
        lines.append(
            ",".join(
                [
                    str(row.get("key") or ""),
                    str(row.get("label") or ""),
                    str(row.get("start") or ""),
                    str(row.get("end") or ""),
                    _csv_float(row.get("strategy_total_return")),
                    _csv_float(row.get("benchmark_total_return")),
                    _csv_float(row.get("strategy_max_drawdown")),
                    _csv_float(row.get("benchmark_max_drawdown")),
                    _csv_float(row.get("exposure_pct")),
                    str(_csv_int(row.get("trade_count"))),
                    "" if row.get("days_to_bear_flag") is None else str(_csv_int(row.get("days_to_bear_flag"))),
                    json.dumps(row.get("exit_type_counts") or {}, sort_keys=True),
                ]
            )
        )
    return "\n".join(lines)


def _csv_float(value: object) -> str:
    try:
        return "" if value is None else f"{float(value):.6f}"
    except Exception:
        return ""


def _csv_int(value: object) -> int:
    try:
        return int(float(value)) if value is not None else 0
    except Exception:
        return 0


def _resolve_pipeline_tickers(args: Any) -> list[str]:
    raw_tickers = getattr(args, "tickers", None)
    tokens: list[str] = []
    if raw_tickers:
        for item in raw_tickers:
            tokens.extend(str(item or "").replace(";", ",").split(","))
    elif getattr(args, "ticker", None):
        tokens.append(str(args.ticker))
    result: list[str] = []
    seen: set[str] = set()
    for token in tokens:
        ticker = str(token or "").strip().upper()
        if ticker and ticker not in seen:
            seen.add(ticker)
            result.append(ticker)
    return result


def _build_report(
    ticker: str,
    period: str,
    interval: str,
    chart_dir: str,
    lookback_window: int,
    training_window: int,
    refit_step: int,
    barrier_vol_multiplier: float,
    macro_weighting: bool,
    frontier_on: bool,
    frontier_provider: str,
    benchmark: str,
    benchmark_state: str | None = None,
) -> tuple[TickerReport, str, object]:
    market_series = download_market_frame(ticker=ticker, period=period, interval=interval)
    regime_result = fit_regime_model(
        ticker=ticker,
        market_frame=market_series.frame,
        lookback_window=lookback_window,
        training_window=training_window,
        refit_step=refit_step,
        macro_weighting=macro_weighting,
    )
    prior_event = save_regime_event(ticker, regime_result.latest_label, regime_result.latest_state_id)
    qualitative = build_qualitative_assessment(
        ticker=ticker,
        regime_signal=regime_result.regime_signal,
        state_name=regime_result.latest_label,
        latest_probability=regime_result.latest_probability,
        context_symbols=[benchmark, "SPY", "^TNX"],
        frontier_enabled=frontier_on,
        frontier_provider=frontier_provider,
        initial_thesis=upsert_thesis(ticker, None),
        previous_label=prior_event["previous_label"],
        benchmark_state=benchmark_state or "Neutral",
    )
    save_sentiment(ticker, qualitative.sentiment_score, qualitative.catalyst_sentiment, len(qualitative.catalysts))
    chart_path = str(save_regime_chart(regime_result, chart_dir))
    return TickerReport(regime=regime_result, qualitative=qualitative, regime_started_days_ago=prior_event["days_in_regime"]), chart_path, market_series.frame


def main() -> None:
    args = parse_args()
    if getattr(args, "command", None) == "alpha-campaign":
        campaign_command = getattr(args, "campaign_command", None)
        if campaign_command == "select-basket":
            payload = select_basket(
                output_path=getattr(args, "output", str(DEFAULT_BASKET_PATH)),
                candidates=getattr(args, "candidates", None),
                names_per_sector=int(getattr(args, "names_per_sector", 3)),
            )
            print(json.dumps(payload, indent=2))
            return
        if campaign_command == "run":
            payload = run_campaign_phase(
                int(getattr(args, "phase")),
                basket_path=getattr(args, "basket", str(DEFAULT_BASKET_PATH)),
                campaign_dir=getattr(args, "campaign_dir", str(DEFAULT_CAMPAIGN_DIR)),
                resume=bool(getattr(args, "resume", False)),
            )
            print(json.dumps(payload, indent=2))
            return
        if campaign_command == "report":
            campaign_report = render_report(
                basket_path=getattr(args, "basket", str(DEFAULT_BASKET_PATH)),
                campaign_dir=getattr(args, "campaign_dir", str(DEFAULT_CAMPAIGN_DIR)),
                output_path=getattr(args, "output", str(DEFAULT_REPORT_PATH)),
            )
            print(getattr(args, "output", str(DEFAULT_REPORT_PATH)))
            if not campaign_report.strip():
                raise SystemExit("empty campaign report")
            return
        if campaign_command == "status":
            print(json.dumps(campaign_status(getattr(args, "campaign_dir", str(DEFAULT_CAMPAIGN_DIR))), indent=2))
            return
        raise SystemExit("alpha-campaign requires one of: select-basket, run, report, status")
    if getattr(args, "command", None) == "threshold-sweep":
        tickers = [str(ticker).strip().upper() for ticker in getattr(args, "tickers", []) if str(ticker).strip()]
        if not tickers:
            raise SystemExit("threshold-sweep requires --tickers.")
        benchmark_ticker = getattr(args, "benchmark", "SPY")
        benchmark = download_market_frame(
            ticker=benchmark_ticker,
            period=getattr(args, "period", "10y"),
            interval="1d",
            start=getattr(args, "start", None),
            end=getattr(args, "end", None),
            cache=bool(getattr(args, "cache", False)),
        ).frame if benchmark_ticker else None
        frames = {
            ticker: download_market_frame(
                ticker=ticker,
                period=getattr(args, "period", "10y"),
                interval="1d",
                start=getattr(args, "start", None),
                end=getattr(args, "end", None),
                cache=bool(getattr(args, "cache", False)),
            ).frame
            for ticker in tickers
        }
        config = PipelineBacktestConfig(
            oos_start=getattr(args, "oos_start", None),
            lookback_window=int(getattr(args, "lookback_window", 20)),
            training_window=int(getattr(args, "training_window", 504)),
            refit_step=int(getattr(args, "refit_step", 21)),
            macro_weighting=bool(getattr(args, "macro_weighting", False)),
            macro_weight=float(getattr(args, "macro_weight", 1.5)),
            hmm_covariance_type=str(getattr(args, "hmm_covariance", "diag") or "diag"),
            hmm_n_seeds=max(1, int(getattr(args, "hmm_n_seeds", 1))),
            seed_agreement_min=max(0.0, min(1.0, float(getattr(args, "seed_agreement_min", 0.8)))),
        )
        rows = run_threshold_sweep(
            tickers=tickers,
            market_frames=frames,
            benchmark_frame=benchmark,
            grid=load_threshold_grid(getattr(args, "grid_json", None)),
            base_config=config,
            include_stress_windows=bool(getattr(args, "stress_report", False)),
        )
        write_sweep_rows(rows, json_path=getattr(args, "output_json", None), csv_path=getattr(args, "output_csv", None))
        if not getattr(args, "output_json", None) and not getattr(args, "output_csv", None):
            print(json.dumps(rows, indent=2))
        else:
            print(json.dumps({"rows": len(rows), "json": getattr(args, "output_json", None), "csv": getattr(args, "output_csv", None)}))
        return
    if getattr(args, "command", None) == "pipeline-backtest":
        config = PipelineBacktestConfig(oos_start=getattr(args, "oos_start", None))
        tickers = _resolve_pipeline_tickers(args)
        if not tickers:
            raise SystemExit("pipeline-backtest requires a ticker or --tickers.")
        if getattr(args, "meta_labeler_ab", False):
            veto_mode = normalize_meta_labeler_veto_mode(getattr(args, "veto_mode", "gate"))
            mode_label = "meta_veto" if veto_mode == "gate" else "meta_size_only"
            benchmark_ticker = getattr(args, "benchmark", "SPY")
            benchmark = download_market_frame(
                ticker=benchmark_ticker,
                period=getattr(args, "period", "10y"),
                interval="1d",
                start=getattr(args, "start", None),
                end=getattr(args, "end", None),
                cache=bool(getattr(args, "cache", False)),
            ).frame if benchmark_ticker else None
            engine = _load_meta_labeler_for_ab()
            results = []
            for ticker in tickers:
                market = download_market_frame(
                    ticker=ticker,
                    period=getattr(args, "period", "10y"),
                    interval="1d",
                    start=getattr(args, "start", None),
                    end=getattr(args, "end", None),
                    cache=bool(getattr(args, "cache", False)),
                ).frame
                baseline = run_pipeline_backtest(ticker, market, config=config, benchmark_frame=benchmark)
                provider = _MetaLabelerVetoProvider(engine, veto_mode=veto_mode)
                meta_veto = run_pipeline_backtest(
                    ticker,
                    market,
                    config=config,
                    benchmark_frame=benchmark,
                    signal_provider=provider,
                )
                baseline_summary = _backtest_summary(baseline)
                meta_summary = _backtest_summary(meta_veto)
                evidence = provider.evidence_summary()
                meta_summary.update(evidence)
                results.append(
                    {
                        "ticker": str(ticker).upper(),
                        "mode": veto_mode,
                        "mode_label": mode_label,
                        "baseline": baseline_summary,
                        "meta_veto": meta_summary,
                        "diff": _diff_numeric(meta_summary, baseline_summary),
                        "meta_labeler_evidence": evidence,
                        "in_sample": {
                            "baseline": getattr(baseline, "in_sample", None),
                            mode_label: getattr(meta_veto, "in_sample", None),
                        },
                        "out_of_sample": {
                            "baseline": getattr(baseline, "out_of_sample", None),
                            mode_label: getattr(meta_veto, "out_of_sample", None),
                        },
                    }
                )
            payload = {
                "tickers": tickers,
                "ticker": tickers[0] if len(tickers) == 1 else None,
                "period": getattr(args, "period", "10y"),
                "oos_start": getattr(args, "oos_start", None),
                "benchmark": benchmark_ticker,
                "veto_mode": veto_mode,
                "mode_label": mode_label,
                "results": results,
            }
            if len(results) == 1:
                payload.update(
                    {
                        "ticker": results[0]["ticker"],
                        "baseline": results[0]["baseline"],
                        "meta_veto": results[0]["meta_veto"],
                        "diff": results[0]["diff"],
                    }
                )
            json_path = getattr(args, "json_path", None)
            if json_path:
                Path(json_path).write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
                print(json_path)
            else:
                print(_format_meta_labeler_ab(payload) if len(results) == 1 else _format_meta_labeler_ab_basket(payload))
            return
        result = run_pipeline_backtest_for_ticker(
            ticker=tickers[0],
            period=getattr(args, "period", "10y"),
            oos_start=getattr(args, "oos_start", None),
            benchmark_ticker=getattr(args, "benchmark", "SPY"),
            start=getattr(args, "start", None),
            end=getattr(args, "end", None),
            cache=bool(getattr(args, "cache", False)),
            config=config,
        )
        json_path = getattr(args, "json_path", None)
        if json_path:
            result.to_json(json_path)
            print(json_path)
        else:
            payload = result.to_dict()
            if bool(getattr(args, "stress_report", False)):
                print(_format_stress_report(payload.get("stress_windows") or []))
            else:
                print(json.dumps(payload, indent=2))
        return
    investor_db_path = get_investor_db_path()
    tickers_arg = getattr(args, "tickers", None)
    filtered_tickers = [] if tickers_arg else get_portfolio_tickers_filtered(investor_db_path)
    resolved_tickers = tickers_arg or filtered_tickers or DEFAULT_TICKERS
    if getattr(args, "weekly_digest", False):
        digest_tickers = tickers_arg or get_portfolio_tickers_filtered(investor_db_path) or DEFAULT_TICKERS
        digest = generate_weekly_digest(
            tickers=digest_tickers,
            benchmark=args.benchmark,
            investor_db_path=investor_db_path,
        )
        if getattr(args, "digest_format", "json") == "text" and not getattr(args, "json", False):
            print(digest_to_text(digest))
        else:
            print(json.dumps(digest_to_dict(digest), indent=2))
        return
    if getattr(args, "backtest", False):
        backtest_period = getattr(args, "backtest_period", "5y")
        results = []
        for ticker in resolved_tickers:
            backtest_result = run_backtest(ticker=ticker, period=backtest_period, refit_step=args.refit_step)
            results.append(
                {
                    "ticker": ticker,
                    "backtest": backtest_result.__dict__,
                    "benchmark_compare": compare_to_benchmark(backtest_result, benchmark_ticker="SPY", period=backtest_period),
                }
            )
        print(json.dumps(results, indent=2) if getattr(args, "json", False) else "\n".join(
            f"{item['ticker']}: total_return={item['backtest']['total_return']:.1%} sharpe={item['backtest']['sharpe_ratio'] if item['backtest']['sharpe_ratio'] is not None else 'n/a'}"
            for item in results
        ))
        return

    relevant_tickers = sorted({*resolved_tickers, args.benchmark})
    investor_position_list = get_portfolio_positions(investor_db_path, relevant_tickers)
    investor_positions = positions_by_ticker(investor_position_list)
    investor_positions_by_account = positions_by_ticker_and_account(investor_position_list)
    tax_assumptions = get_tax_assumptions(investor_db_path)
    benchmark_report, benchmark_chart, benchmark_market = _build_report(
        args.benchmark,
        args.period,
        args.interval,
        args.chart_dir,
        args.lookback_window,
        args.training_window,
        args.refit_step,
        args.barrier_vol_multiplier,
        args.macro_weighting,
        args.frontier_on,
        args.frontier_provider,
        args.benchmark,
        None,
    )

    reports: list[TickerReport] = []
    analyses = []
    chart_paths = {benchmark_report.regime.ticker: benchmark_chart}
    for ticker in resolved_tickers:
        report, chart_path, market_frame = _build_report(
            ticker,
            args.period,
            args.interval,
            args.chart_dir,
            args.lookback_window,
            args.training_window,
            args.refit_step,
            args.barrier_vol_multiplier,
            args.macro_weighting,
            args.frontier_on,
            args.frontier_provider,
            args.benchmark,
            benchmark_report.regime.latest_label,
        )
        reports.append(report)
        analyses.append({"report": report, "market_frame": market_frame})
        chart_paths[ticker] = chart_path

    benchmark_forward_curve = forward_regime_curve(
        benchmark_report.regime.transition_matrix,
        benchmark_report.regime.latest_state_vector,
        horizon=21,
    )
    benchmark_forward_signal = signal_from_forward_curve(
        benchmark_forward_curve,
        benchmark_report.regime.latest_label,
        benchmark_report.regime.transition_risk,
        benchmark_report.regime.expected_regime_duration,
        benchmark_report.regime.latest_probability,
    )
    benchmark_technicals = compute_technicals(
        benchmark_market["price"],
        benchmark_market["volume"],
        benchmark_market["high"] if "high" in benchmark_market.columns else None,
        benchmark_market["low"] if "low" in benchmark_market.columns else None,
    )
    benchmark_technical_signal = intra_regime_signal(benchmark_technicals, benchmark_report.regime.latest_label)
    benchmark_composite_signal = build_composite_signal(
        benchmark_report.regime.latest_label,
        benchmark_report.regime.latest_probability,
        benchmark_forward_signal,
        benchmark_technical_signal,
    )
    benchmark_trajectory = confidence_trajectory(benchmark_report.regime.price_frame["state_probability"], window=10)

    payload = {
        "benchmark": {
            "ticker": benchmark_report.regime.ticker,
            "regime": benchmark_report.regime.latest_label,
            "state_id": benchmark_report.regime.latest_state_id,
            "probability": benchmark_report.regime.latest_probability,
            "transition_matrix": benchmark_report.regime.transition_matrix.tolist(),
            "expected_regime_duration": benchmark_report.regime.expected_regime_duration,
            "transition_risk": benchmark_report.regime.transition_risk,
            "forward_curve": benchmark_forward_curve.to_dict(orient="records"),
            "forward_signal": benchmark_forward_signal.__dict__,
            "technical_signal": benchmark_technical_signal,
            "confidence_trajectory": benchmark_trajectory.__dict__,
            "composite_signal": {
                "regime_signal": benchmark_composite_signal.regime_signal,
                "regime_probability": benchmark_composite_signal.regime_probability,
                "forward_signal": benchmark_composite_signal.forward_signal.__dict__,
                "technical_signal": benchmark_composite_signal.technical_signal,
                "composite_action": benchmark_composite_signal.composite_action,
                "composite_strength": benchmark_composite_signal.composite_strength,
                "short_term_view": benchmark_composite_signal.short_term_view,
                "medium_term_view": benchmark_composite_signal.medium_term_view,
            },
            "chart": chart_paths[benchmark_report.regime.ticker],
        },
        "tickers": [
            _build_ticker_payload(
                analysis["report"],
                analysis["market_frame"],
                chart_paths,
                investor_positions.get(analysis["report"].regime.ticker.upper()),
                investor_positions_by_account.get(analysis["report"].regime.ticker.upper(), []),
                tax_assumptions,
                investor_db_path,
            )
            for analysis in analyses
        ],
        "relative_strength": [report.regime.ticker for report in summarize_relative_strength(reports, benchmark_report.regime.latest_label)],
        "model_diagnostics": calibration_payload(get_calibration_data(lookback_days=365)),
    }
    if getattr(args, "json", False):
        print(json.dumps(payload, indent=2))
    else:
        print(_payload_to_text(payload))


def _build_ticker_payload(
    report: TickerReport,
    market_frame,
    chart_paths: dict[str, str],
    position,
    account_positions: list,
    tax_assumptions: dict[str, float],
    investor_db_path: str | None,
) -> dict:
    forward_curve = forward_regime_curve(
        report.regime.transition_matrix,
        report.regime.latest_state_vector,
        horizon=21,
    )
    forward_signal = signal_from_forward_curve(
        forward_curve,
        report.regime.latest_label,
        report.regime.transition_risk,
        report.regime.expected_regime_duration,
        report.regime.latest_probability,
    )
    technicals = compute_technicals(
        market_frame["price"],
        market_frame["volume"],
        market_frame["high"] if "high" in market_frame.columns else None,
        market_frame["low"] if "low" in market_frame.columns else None,
    )
    technical_signal = intra_regime_signal(technicals, report.regime.latest_label)
    composite_signal = build_composite_signal(
        report.regime.latest_label,
        report.regime.latest_probability,
        forward_signal,
        technical_signal,
    )
    weekly_regime = fit_regime_model_weekly(report.regime.ticker, market_frame)
    composite_signal.weekly_regime = weekly_regime.latest_label
    composite_signal.multi_timeframe_note = multi_timeframe_signal(report.regime.latest_label, weekly_regime.latest_label)
    trajectory = confidence_trajectory(report.regime.price_frame["state_probability"], window=10)
    sentiment_info, _ = sentiment_momentum(report.regime.ticker, report.regime.latest_label)
    tax_signal = None
    account_tax_signals = []
    wash_sale_risk = get_wash_sale_risk(investor_db_path, report.regime.ticker)
    if account_positions:
        account_tax_signals = tax_adjusted_signals(
            composite_signal,
            account_positions,
            tax_assumptions,
            wash_sale_risk=wash_sale_risk,
        )
        taxable_account_signals = [signal for signal in account_tax_signals if signal.account_type == "TAXABLE"]
        tax_signal = taxable_account_signals[0] if taxable_account_signals else account_tax_signals[0]
    return {
        "ticker": report.regime.ticker,
        "state_id": report.regime.latest_state_id,
        "regime": report.regime.latest_label,
        "probability": report.regime.latest_probability,
        "price": report.regime.latest_price,
        "regime_signal": report.regime.regime_signal,
        "recent_state_mean_return": report.regime.recent_state_mean_return,
        "regime_inconsistency_warning": report.regime.regime_inconsistency_warning,
        "transition_matrix": report.regime.transition_matrix.tolist(),
        "expected_regime_duration": report.regime.expected_regime_duration,
        "transition_risk": report.regime.transition_risk,
        "forward_curve": forward_curve.to_dict(orient="records"),
        "forward_signal": forward_signal.__dict__,
        "technical_signal": technical_signal,
        "confidence_trajectory": trajectory.__dict__,
        "sentiment_momentum": sentiment_info.__dict__,
        "composite_signal": {
            "regime_signal": composite_signal.regime_signal,
            "regime_probability": composite_signal.regime_probability,
            "forward_signal": composite_signal.forward_signal.__dict__,
            "technical_signal": composite_signal.technical_signal,
            "composite_action": composite_signal.composite_action,
            "composite_strength": composite_signal.composite_strength,
            "short_term_view": composite_signal.short_term_view,
            "medium_term_view": composite_signal.medium_term_view,
            "weekly_regime": composite_signal.weekly_regime,
            "multi_timeframe_note": composite_signal.multi_timeframe_note,
        },
        "tax_adjusted_signals": [signal.__dict__ for signal in account_tax_signals],
        "sentiment": report.qualitative.catalyst_sentiment,
        "llm": report.qualitative.llm_response,
        "thesis_check": report.qualitative.thesis_check_response,
        "days_in_regime": report.regime.regime_days,
        "chart": chart_paths[report.regime.ticker],
    }


def _payload_to_text(payload: dict) -> str:
    lines = [
        f"Benchmark: {payload['benchmark']['ticker']} | regime={payload['benchmark']['regime']} | probability={payload['benchmark']['probability']:.1%}",
        "",
    ]
    if payload.get("relative_strength"):
        lines.append("Relative strength: " + ", ".join(payload["relative_strength"]))
        lines.append("")
    for item in payload["tickers"]:
        composite = item["composite_signal"]
        lines.append(
            f"{item['ticker']}: regime={item['regime']} | probability={item['probability']:.1%} | "
            f"signal={composite['composite_action']} | days_in_regime={item['days_in_regime']}"
        )
        lines.append(f"  Forward: {composite['forward_signal']['action']} | Technical: {item['technical_signal']}")
        if item.get("tax_adjusted_signals"):
            for signal in item["tax_adjusted_signals"]:
                lines.append(
                    f"  Tax [{signal['account_name'] or 'Unknown'} / {signal['account_type'] or 'Unknown'}]: "
                    f"{signal['adjusted_action']} | {signal['tax_note']}"
                )
        elif item.get("tax_adjusted_signal"):
            signal = item["tax_adjusted_signal"]
            lines.append(f"  Tax: {signal['adjusted_action']} | {signal['tax_note']}")
        lines.append(f"  Chart: {item['chart']}")
        lines.append("")
    return "\n".join(lines).rstrip()


if __name__ == "__main__":
    main()
