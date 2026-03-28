from __future__ import annotations

import argparse
import json
from pathlib import Path

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
from .persistence import get_calibration_data, save_regime_event, save_sentiment, upsert_thesis
from .diagnostics import calibration_payload
from .backtest import compare_to_benchmark, run_backtest
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Market regime detection using a 3-state Hidden Markov Model.")
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
    parser.add_argument("--backtest-period", default="5y")
    return parser.parse_args()


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
