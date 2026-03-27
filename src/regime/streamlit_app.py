from __future__ import annotations

from typing import Any

import pandas as pd
import streamlit as st

try:
    from .config import DEFAULT_TICKERS
    from .charts import build_confidence_timeline, build_regime_price_chart, build_transition_heatmap
    from .data import download_market_frame
    from .digest import generate_weekly_digest
    from .hmm_engine import STATE_META, fit_regime_model
    from .investor_adapter import (
        get_investor_db_path,
        get_portfolio_positions,
        get_portfolio_tickers,
        get_tax_assumptions,
        get_wash_sale_risk,
        positions_by_ticker_and_account,
        positions_by_ticker,
    )
    from .llm_layer import build_qualitative_assessment, configured_frontier_model
    from .portfolio import portfolio_risk_summary_dict
    from .persistence import get_calibration_data, save_regime_event, save_sentiment, upsert_thesis
    from .reporting import TickerReport, summarize_relative_strength
    from .diagnostics import calibration_payload
    from .signals import (
        build_composite_signal,
        compute_position_size,
        compute_price_targets,
        compute_technicals,
        confidence_trajectory,
        forward_regime_curve,
        intra_regime_signal,
        multi_timeframe_signal,
        sentiment_momentum,
        signal_from_forward_curve,
        tax_adjusted_signals,
    )
except ImportError:
    from src.regime.config import DEFAULT_TICKERS
    from src.regime.charts import build_confidence_timeline, build_regime_price_chart, build_transition_heatmap
    from src.regime.data import download_market_frame
    from src.regime.digest import generate_weekly_digest
    from src.regime.hmm_engine import STATE_META, fit_regime_model
    from src.regime.investor_adapter import (
        get_investor_db_path,
        get_portfolio_positions,
        get_portfolio_tickers,
        get_tax_assumptions,
        get_wash_sale_risk,
        positions_by_ticker_and_account,
        positions_by_ticker,
    )
    from src.regime.llm_layer import build_qualitative_assessment, configured_frontier_model
    from src.regime.portfolio import portfolio_risk_summary_dict
    from src.regime.persistence import get_calibration_data, save_regime_event, save_sentiment, upsert_thesis
    from src.regime.reporting import TickerReport, summarize_relative_strength
    from src.regime.diagnostics import calibration_payload
    from src.regime.signals import (
        build_composite_signal,
        compute_position_size,
        compute_price_targets,
        compute_technicals,
        confidence_trajectory,
        forward_regime_curve,
        intra_regime_signal,
        multi_timeframe_signal,
        sentiment_momentum,
        signal_from_forward_curve,
        tax_adjusted_signals,
    )


def _plotly_figure(spec: dict[str, Any]):
    try:
        import plotly.graph_objects as go
    except ImportError:
        return None
    return go.Figure(spec)


def _confidence(report: TickerReport) -> int:
    if report.qualitative.llm_response and report.qualitative.llm_response.get("confidence") is not None:
        return int(report.qualitative.llm_response["confidence"])
    return report.qualitative.fallback_confidence


def _confidence_gauge(report: TickerReport) -> int:
    institutional = (report.qualitative.llm_response or {}).get("institutional_report", {})
    if institutional.get("confidence_score") is not None:
        return int(institutional["confidence_score"])
    if report.qualitative.llm_response and report.qualitative.llm_response.get("confidence_gauge") is not None:
        return int(report.qualitative.llm_response["confidence_gauge"])
    return max(1, min(10, round(_confidence(report) / 10)))


def _status_icon(label: str) -> str:
    return {"Bull": "🟢", "Neutral": "🟡", "Bear": "🔴"}[label]


def _relative_strength_text(label: str, benchmark_label: str) -> str:
    if label == "Bull" and benchmark_label in {"Neutral", "Bear"}:
        return "↑ Outperforming"
    if label == "Bear" and benchmark_label in {"Bull", "Neutral"}:
        return "↓ Lagging"
    return "→ In-line"


@st.cache_data(ttl=3600, show_spinner=False)
def _analyze_ticker(
    ticker: str,
    period: str,
    lookback_window: int,
    training_window: int,
    refit_step: int,
    barrier_vol_multiplier: float,
    macro_weighting: bool,
    benchmark: str,
    frontier_enabled: bool,
    frontier_provider: str,
    benchmark_state: str,
    min_regime_days: int,
    run_nonce: int,
) -> dict[str, Any]:
    market_series = download_market_frame(ticker=ticker, period=period, interval="1d")
    regime = fit_regime_model(
        ticker=ticker,
        market_frame=market_series.frame,
        lookback_window=lookback_window,
        training_window=training_window,
        refit_step=refit_step,
        macro_weighting=macro_weighting,
    )
    persistence = save_regime_event(ticker, regime.latest_label, regime.latest_state_id)
    qualitative = build_qualitative_assessment(
        ticker=ticker,
        regime_signal=regime.regime_signal,
        state_name=regime.latest_label,
        latest_probability=regime.latest_probability,
        context_symbols=[benchmark, "SPY", "^TNX"],
        frontier_enabled=frontier_enabled,
        frontier_provider=frontier_provider,
        initial_thesis=upsert_thesis(ticker, None),
        previous_label=persistence["previous_label"],
        benchmark_state=benchmark_state,
    )
    save_sentiment(ticker, qualitative.sentiment_score, qualitative.catalyst_sentiment, len(qualitative.catalysts))
    return {
        "report": TickerReport(regime=regime, qualitative=qualitative, regime_started_days_ago=persistence["days_in_regime"]),
        "min_regime_days": min_regime_days,
        "market_frame": market_series.frame,
    }


def _verdict_display(
    report: TickerReport,
    min_regime_days: int,
    min_signal_probability: float,
) -> tuple[str, bool]:
    conditions: list[str] = []
    if report.regime.regime_days < min_regime_days:
        conditions.append(
            f"Regime too new ({report.regime.regime_days}d < {min_regime_days}d minimum)"
        )
    if report.regime.latest_probability < min_signal_probability:
        conditions.append(
            f"Low confidence ({report.regime.latest_probability:.0%} < {min_signal_probability:.0%} threshold)"
        )
    if conditions:
        return "Hold — " + " | ".join(conditions), True

    institutional = (report.qualitative.llm_response or {}).get("institutional_report", {})
    return institutional.get("verdict", "Hold"), False


def _sizing_guidance(report: TickerReport, signal_suppressed: bool) -> tuple[str, str]:
    probability = report.regime.latest_probability
    label = report.regime.latest_label
    if label == "Bull":
        if probability >= 0.90:
            text = "Suggested sizing: Full position (90%+ confidence)"
            color = "#1b5e20"
        elif probability >= 0.80:
            text = "Suggested sizing: 75% position (80-90% confidence)"
            color = "#1b5e20"
        elif probability >= 0.70:
            text = "Suggested sizing: 50% position (70-80% confidence)"
            color = "#b26a00"
        elif probability >= 0.60:
            text = "Suggested sizing: 25% position (60-70% confidence)"
            color = "#b26a00"
        else:
            text = "Suggested sizing: No new position (sub-60% confidence)"
            color = "#8b1e1e"
    elif label == "Neutral":
        text = "Suggested sizing: Hold / reduce to 25%"
        color = "#b26a00"
    else:
        text = "Suggested sizing: Exit or short (if strategy permits)"
        color = "#8b1e1e"

    if signal_suppressed:
        text += " - signal suppressed by filter"
    return text, color


def _signal_badge_color(action: str) -> str:
    return {
        "Strong Buy": "#0b7a28",
        "Buy": "#5aa469",
        "Hold": "#b26a00",
        "Sell": "#d07a00",
        "Strong Sell": "#b42318",
    }.get(action, "#455a64")


def _render_status_card(report: TickerReport, benchmark_label: str) -> None:
    badge = STATE_META[report.regime.latest_label]["badge"]
    st.markdown(
        f"""
        <div style="border:1px solid #d9dfe7;border-radius:14px;padding:14px 16px;background:#ffffff;min-height:154px;">
          <div style="font-size:1.05rem;font-weight:700;">{report.regime.ticker}</div>
          <div style="font-size:1.4rem;font-weight:800;padding-top:4px;">${report.regime.latest_price:,.2f}</div>
          <div style="padding-top:8px;">
            <span style="background:{STATE_META[report.regime.latest_label]['color']};padding:6px 10px;border-radius:999px;font-weight:700;">
              {_status_icon(report.regime.latest_label)} {badge} (State {report.regime.latest_state_id})
            </span>
          </div>
          <div style="padding-top:10px;color:#425466;font-weight:600;">{_relative_strength_text(report.regime.latest_label, benchmark_label)}</div>
          <div style="padding-top:6px;color:#425466;">In current regime for {report.regime.regime_days} day(s)</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _heatmap_df(reports: list[TickerReport], benchmark_label: str) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "Ticker": report.regime.ticker,
                "Current Price": round(report.regime.latest_price, 2),
                "State": report.regime.latest_state_id,
                "Regime": STATE_META[report.regime.latest_label]["badge"],
                "Probability %": round(report.regime.latest_probability * 100, 1),
                "Relative Strength": _relative_strength_text(report.regime.latest_label, benchmark_label),
            }
            for report in reports
        ]
    ).set_index("Ticker")


def _styled_heatmap(df: pd.DataFrame):
    def style_regime(value: str) -> str:
        if "Bullish" in value:
            return "background-color: #d9f2df; color: #1b5e20;"
        if "Neutral" in value:
            return "background-color: #e3e5e8; color: #37474f;"
        return "background-color: #f8d7da; color: #8b1e1e;"

    def style_strength(value: str) -> str:
        if "Outperforming" in value:
            return "color: #1b5e20; font-weight: 700;"
        if "Lagging" in value:
            return "color: #8b1e1e; font-weight: 700;"
        return "color: #455a64;"

    return (
        df.style
        .map(style_regime, subset=["Regime"])
        .map(style_strength, subset=["Relative Strength"])
        .background_gradient(subset=["Probability %"], cmap="YlGn")
    )


def main() -> None:
    st.set_page_config(page_title="Regime-Watch Dashboard", layout="wide")
    st.title("Regime-Watch Dashboard")
    st.caption("Portfolio regime monitoring with Viterbi decoding, catalyst analysis, and thesis validation.")
    investor_db_path = get_investor_db_path()
    portfolio_tickers = get_portfolio_tickers(investor_db_path) if investor_db_path else []
    available_tickers = portfolio_tickers or DEFAULT_TICKERS
    default_selection = portfolio_tickers if portfolio_tickers else DEFAULT_TICKERS[:4]
    if "analyze_nonce" not in st.session_state:
        st.session_state["analyze_nonce"] = 0

    with st.sidebar:
        st.header("Controls")
        selected_tickers = st.multiselect("Ticker Selector", available_tickers, default=default_selection)
        if portfolio_tickers:
            st.caption(f"Portfolio mode: {len(portfolio_tickers)} tickers from Investor")
        else:
            st.caption("Manual mode: using default tickers.")
        benchmark = st.radio("Benchmark Toggle", options=["SOXX", "SPY"], index=0, horizontal=True)
        lookback_window = st.slider("Model Responsiveness", min_value=10, max_value=90, value=20, step=5)
        with st.expander("Advanced Engine Settings"):
            training_window = st.slider("Lookback Window", min_value=252, max_value=756, value=504, step=21)
            refit_step = st.slider("Refit Frequency (days)", min_value=1, max_value=63, value=21, step=1)
            barrier_vol_multiplier = st.slider("Volatility Multiplier", min_value=0.5, max_value=3.0, value=1.0, step=0.1)
            macro_weighting = st.toggle("Macro Weighting", value=False, help="Boost the influence of ^VIX and ^TNX in the HMM feature space.")
            min_regime_days = st.slider("Min Regime Days", min_value=1, max_value=10, value=3, step=1)
            min_signal_probability = st.slider(
                "Min Signal Probability",
                min_value=0.50,
                max_value=0.95,
                value=0.70,
                step=0.05,
                format="%.2f",
            )
        frontier_enabled = st.toggle("Frontier API Toggle", value=False)
        frontier_provider = st.selectbox("Frontier Provider", options=["auto", "openai", "gemini", "claude", "best"], index=0, format_func=lambda value: "Best Available" if value == "auto" else value.title() if value != "best" else "Best Available")
        if frontier_enabled:
            st.caption(f"Frontier model configured: {configured_frontier_model(frontier_provider)}")
        thesis_ticker = st.selectbox("Thesis Ticker", options=selected_tickers or available_tickers)
        saved_thesis = upsert_thesis(thesis_ticker, None) or ""
        thesis_text = st.text_area(
            'Initial Investment Thesis',
            value=saved_thesis,
            help='Example: "Buying AVGO for custom AI silicon growth."',
            height=110,
        )
        if st.button("Save Thesis", width="stretch"):
            upsert_thesis(thesis_ticker, thesis_text)
            st.success(f"Saved 2026 investment thesis for {thesis_ticker}.")
        analyze_clicked = st.button("Analyze", type="primary", width="stretch")
        if analyze_clicked:
            st.session_state["analyze_nonce"] += 1

    if not analyze_clicked:
        st.info("Configure the portfolio in the sidebar, then click Analyze.")
        return
    if not selected_tickers:
        st.error("Select at least one ticker.")
        return

    investor_position_list = get_portfolio_positions(investor_db_path)
    investor_positions_by_account = positions_by_ticker_and_account(investor_position_list)
    tax_assumptions = get_tax_assumptions(investor_db_path)
    benchmark_report = _analyze_ticker(
        benchmark,
        "3y",
        lookback_window,
        training_window,
        refit_step,
        barrier_vol_multiplier,
        macro_weighting,
        benchmark,
        frontier_enabled,
        frontier_provider,
        "Neutral",
        min_regime_days,
        st.session_state["analyze_nonce"],
    )["report"]
    analyses = []
    run_started = pd.Timestamp.utcnow()
    progress_bar = st.progress(0, text="Preparing analysis...")
    status = st.status("Regime Analysis", expanded=True)
    for idx, ticker in enumerate(selected_tickers, start=1):
        status.update(label=f"Analyzing {ticker} ({idx}/{len(selected_tickers)})")
        status.write(f"⏳ Downloading market data for {ticker}...")
        started = pd.Timestamp.utcnow()
        analysis = _analyze_ticker(
            ticker,
            "3y",
            lookback_window,
            training_window,
            refit_step,
            barrier_vol_multiplier,
            macro_weighting,
            benchmark,
            frontier_enabled,
            frontier_provider,
            benchmark_report.regime.latest_label,
            min_regime_days,
            st.session_state["analyze_nonce"],
        )
        elapsed = (pd.Timestamp.utcnow() - started).total_seconds()
        if elapsed < 0.05:
            status.write(f"✓ {ticker} (cached)")
        else:
            status.write(f"⏳ Computing signals for {ticker}...")
            status.write(f"✓ {ticker} complete")
        analyses.append(analysis)
        progress_bar.progress(idx / len(selected_tickers), text=f"{ticker} complete")
    total_elapsed = (pd.Timestamp.utcnow() - run_started).total_seconds()
    status.update(label="Analysis complete", state="complete")
    st.caption(f"Analyzed {len(selected_tickers)} ticker(s) in {total_elapsed:.1f}s")
    reports = [analysis["report"] for analysis in analyses]
    signal_payloads = []
    for analysis in analyses:
        report = analysis["report"]
        market_frame = analysis["market_frame"]
        forward_curve = forward_regime_curve(report.regime.transition_matrix, report.regime.latest_state_vector, horizon=21)
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
        trajectory = confidence_trajectory(report.regime.price_frame["state_probability"], window=10)
        sentiment_info, sentiment_history = sentiment_momentum(report.regime.ticker, report.regime.latest_label)
        account_tax_signals = []
        account_positions = investor_positions_by_account.get(report.regime.ticker.upper(), [])
        if account_positions:
            account_tax_signals = tax_adjusted_signals(
                composite_signal,
                account_positions,
                tax_assumptions,
                wash_sale_risk=get_wash_sale_risk(investor_db_path, report.regime.ticker),
            )
        signal_payloads.append(
            {
                "ticker": report.regime.ticker,
                "forward_curve": forward_curve,
                "forward_signal": forward_signal,
                "technical_signal": technical_signal,
                "composite_signal": composite_signal,
                "confidence_trajectory": trajectory,
                "sentiment_momentum": sentiment_info,
                "sentiment_history": sentiment_history,
                "tax_adjusted_signals": account_tax_signals,
                "technicals": technicals,
            }
        )
    signal_by_ticker = {payload["ticker"]: payload for payload in signal_payloads}
    dashboard_tab, digest_tab = st.tabs(["Dashboard", "Weekly Digest"])

    with dashboard_tab:
        regime_results = {
            report.regime.ticker: {
                "label": report.regime.latest_label,
                "transition_risk": report.regime.transition_risk,
                "composite_action": signal_by_ticker[report.regime.ticker]["composite_signal"].composite_action,
            }
            for report in reports
        }
        with st.expander("Portfolio Summary", expanded=True):
            portfolio_summary = portfolio_risk_summary_dict(investor_position_list, regime_results)
            st.write(
                f"Portfolio signal: {portfolio_summary['portfolio_composite_signal']} | "
                f"Transition risk: {portfolio_summary['aggregate_transition_risk']:.1%} | "
                f"Diversification: {portfolio_summary['diversification_score']:.2f}"
            )
            sector_rows = portfolio_summary.get("sector_concentration") or []
            if sector_rows:
                st.dataframe(pd.DataFrame(sector_rows), width="stretch")
        with st.expander("Model Health", expanded=False):
            st.json(calibration_payload(get_calibration_data(lookback_days=365)))
        st.subheader("Regime Heatmap")
        card_columns = st.columns(len(reports))
        for col, report in zip(card_columns, reports):
            with col:
                _render_status_card(report, benchmark_report.regime.latest_label)
                if report.regime.regime_inconsistency_warning:
                    st.warning("Regime Inconsistency", icon="⚠️")

        st.dataframe(_styled_heatmap(_heatmap_df(reports, benchmark_report.regime.latest_label)), width="stretch")

        st.subheader("Viterbi Vision")
        for report in reports:
            with st.container(border=True):
                st.markdown(
                    f"**{report.regime.ticker}**: {_status_icon(report.regime.latest_label)} "
                    f"{STATE_META[report.regime.latest_label]['badge']} | "
                    f"State {report.regime.latest_state_id} | "
                    f"{_relative_strength_text(report.regime.latest_label, benchmark_report.regime.latest_label)}"
                )
                price_fig = _plotly_figure(build_regime_price_chart(report.regime.price_frame, report.regime.ticker))
                confidence_fig = _plotly_figure(build_confidence_timeline(report.regime.price_frame))
                transition_fig = _plotly_figure(build_transition_heatmap(report.regime.transition_matrix.tolist()))
                if price_fig is not None:
                    st.plotly_chart(price_fig, use_container_width=True)
                if confidence_fig is not None:
                    st.plotly_chart(confidence_fig, use_container_width=True)
                if transition_fig is not None:
                    st.plotly_chart(transition_fig, use_container_width=True)

        st.subheader("Signal Dashboard")
        for report in reports:
            signal_payload = signal_by_ticker[report.regime.ticker]
            composite_signal = signal_payload["composite_signal"]
            forward_curve = signal_payload["forward_curve"]
            trajectory = signal_payload["confidence_trajectory"]
            sentiment_info = signal_payload["sentiment_momentum"]
            sentiment_history = signal_payload["sentiment_history"]
            account_tax_signals = signal_payload["tax_adjusted_signals"]
            technicals = signal_payload["technicals"]
            with st.container(border=True):
                st.markdown(
                    f'<div style="background:{_signal_badge_color(composite_signal.composite_action)};'
                    f'color:#ffffff;font-weight:800;padding:10px 14px;border-radius:10px;display:inline-block;">'
                    f'{composite_signal.composite_action.upper()}</div>',
                    unsafe_allow_html=True,
                )
                if account_tax_signals:
                    st.markdown("**Per-account tax signals**")
                    for account_signal in account_tax_signals:
                        tax_color = "#b26a00" if account_signal.adjusted_action != account_signal.original_action else "#455a64"
                        st.markdown(
                            f'<div style="color:{tax_color};font-weight:700;padding-top:8px;">'
                            f'{account_signal.account_name or "Unknown"} / {account_signal.account_type or "Unknown"}: '
                            f'{account_signal.adjusted_action} | {account_signal.tax_note}</div>',
                            unsafe_allow_html=True,
                        )
                        if account_signal.ltcg_threshold_date:
                            st.caption(f"LTCG threshold date: {account_signal.ltcg_threshold_date}")
                        st.caption(f"Estimated tax impact: ${account_signal.estimated_tax_impact:,.2f}")
                        if account_signal.wash_sale_warning:
                            st.markdown(
                                f'<div style="color:#b42318;font-weight:700;">{account_signal.wash_sale_warning}</div>',
                                unsafe_allow_html=True,
                            )
                st.caption(f"Sentiment trend: {sentiment_info.trend}")
                if not sentiment_history.empty:
                    sentiment_chart = sentiment_history.tail(10)[["score"]].reset_index(drop=True)
                    st.line_chart(sentiment_chart, width="stretch")
                if sentiment_info.divergence_vs_regime and sentiment_info.warning:
                    st.warning(sentiment_info.warning)

                left_sig, right_sig = st.columns(2)
                with left_sig:
                    st.write(composite_signal.short_term_view)
                with right_sig:
                    st.write(composite_signal.medium_term_view)

                st.line_chart(
                    forward_curve.set_index("day")[["p_bull", "p_neutral", "p_bear"]],
                    width="stretch",
                )
                if report.regime.expected_regime_duration >= 999:
                    st.caption("Regime is highly persistent (999+ day expected duration)")
                else:
                    st.caption(
                        f"{STATE_META[report.regime.latest_label]['badge']} expected to persist ~{report.regime.expected_regime_duration:.1f} more trading days."
                    )
                risk_color = "#1b5e20" if report.regime.transition_risk < 0.05 else "#b26a00" if report.regime.transition_risk <= 0.15 else "#b42318"
                st.markdown(
                    f'<div style="color:{risk_color};font-weight:700;">Daily transition risk: {report.regime.transition_risk:.1%}</div>',
                    unsafe_allow_html=True,
                )
                st.write(f"Intra-regime timing: {signal_payload['technical_signal']}")
                sparkline_df = report.regime.price_frame["state_probability"].tail(10).reset_index(drop=True).to_frame(name="state_probability")
                st.line_chart(sparkline_df, width="stretch")
                st.caption(
                    f"Confidence trajectory slope: {trajectory.slope:.4f} ({trajectory.trend}) | "
                    f"short MA: {trajectory.short_ma_latest:.2%}, long MA: {trajectory.long_ma_latest:.2%}"
                )
                if report.regime.latest_label == "Bull" and (
                    (trajectory.trend == "declining" and trajectory.days_declining >= 5) or trajectory.slope < -0.02
                ):
                    st.warning("Bull confidence eroding - consider tightening stops")
                if report.regime.latest_label == "Bear" and trajectory.trend == "rising" and trajectory.days_rising >= 5:
                    st.warning("Bear conviction strengthening - avoid bottom-fishing")
                if report.regime.transition_risk > 0.20:
                    st.warning(f"Elevated regime transition risk ({report.regime.transition_risk:.1%}) - reduce position size")

                atr_value = None
                technicals_nonnull = technicals.dropna(how="all") if isinstance(technicals, pd.DataFrame) else pd.DataFrame()
                if not technicals_nonnull.empty and "atr_14" in technicals_nonnull.columns:
                    latest_atr = technicals_nonnull["atr_14"].iloc[-1]
                    if pd.notna(latest_atr):
                        atr_value = float(latest_atr)
                price_targets = compute_price_targets(
                    current_price=float(report.regime.latest_price),
                    technicals_df=technicals,
                    composite_signal=composite_signal,
                    expected_duration=float(report.regime.expected_regime_duration),
                    state_mean_return=float(report.regime.recent_state_mean_return or 0.0),
                )
                portfolio_value = None
                ticker_positions = investor_positions_by_account.get(report.regime.ticker.upper(), [])
                if ticker_positions:
                    portfolio_value = float(
                        sum(float(getattr(position, "market_value", 0.0) or 0.0) for position in ticker_positions)
                    ) or None
                position_size = compute_position_size(
                    regime_probability=float(report.regime.latest_probability),
                    composite_action=str(composite_signal.composite_action),
                    risk_reward_ratio=price_targets.risk_reward_ratio,
                    atr_value=atr_value,
                    current_price=float(report.regime.latest_price),
                    portfolio_value=portfolio_value,
                )
                st.markdown("**Position sizing**")
                sizing_line = f"Suggested allocation: {position_size.suggested_pct:.1f}%"
                if position_size.suggested_dollars is not None:
                    sizing_line += f" (${position_size.suggested_dollars:,.2f})"
                st.write(sizing_line)
                if position_size.max_loss_dollars is not None:
                    st.caption(f"Max risk: ${position_size.max_loss_dollars:,.2f}")
                if position_size.kelly_fraction is not None:
                    st.caption(f"Half-Kelly: {position_size.kelly_fraction:.1%}")
                st.caption(position_size.sizing_rationale)

        st.subheader("Quant-Mental Intelligence Feed")
        relative_strength = summarize_relative_strength(reports, benchmark_report.regime.latest_label)
        if relative_strength:
            st.success("Relative strength: " + ", ".join(report.regime.ticker for report in relative_strength))

        for report in reports:
            left, right = st.columns([1, 1.15])
            with left:
                st.markdown(f"### {report.regime.ticker} | The Math")
                if report.regime.regime_inconsistency_warning:
                    st.warning(report.regime.regime_inconsistency_warning)
                current_row = report.regime.state_statistics.loc[report.regime.state_statistics["state_id"] == report.regime.latest_state_id]
                st.dataframe(current_row, width="stretch")
                st.write(
                    f"Mean Return in current state: {current_row['mean_return'].iloc[0]:.4f} | "
                    f"Expected Volatility: {current_row['expected_volatility'].iloc[0]:.4f} | "
                    f"Volume Z-Score: {current_row['volume_zscore'].iloc[0]:.2f}"
                )
                if report.regime.recent_state_mean_return is not None:
                    st.caption(f"Recent 20-observation mean return for current state: {report.regime.recent_state_mean_return:.4f}")
                st.caption(
                    f"{report.regime.ticker} entered {STATE_META[report.regime.latest_label]['badge']} "
                    f"{report.regime.regime_days} day(s) ago."
                )
                st.caption(f"Current regime streak: {report.regime.regime_days} day(s)")

            with right:
                st.markdown(f"### {report.regime.ticker} | The Frontier Analysis")
                thesis_check = report.qualitative.thesis_check_response
                if thesis_check:
                    st.warning(f"Thesis Check: {thesis_check.get('answer', 'No thesis-check answer returned.')}")
                    if thesis_check.get("rationale"):
                        st.caption(thesis_check["rationale"])
                else:
                    st.info("No new regime change detected, or no saved thesis exists for this ticker.")

                st.markdown("**Catalyst Summary**")
                for item in report.qualitative.catalysts[:5]:
                    st.write(f"- {item.get('title', 'Untitled')}")

                llm = report.qualitative.llm_response or {}
                institutional = llm.get("institutional_report", {})
                if institutional:
                    st.markdown("**Regime Validation**")
                    st.write(institutional.get("regime_validation", "Unavailable"))
                    st.markdown("**Divergence Check**")
                    st.write(institutional.get("divergence_check", "None"))
                    st.markdown("**Actionable Verdict**")
                    displayed_verdict, is_override = _verdict_display(report, min_regime_days, min_signal_probability)
                    if is_override:
                        st.markdown(
                            f'<div style="color:#b26a00;font-weight:700;">{displayed_verdict}</div>',
                            unsafe_allow_html=True,
                        )
                    else:
                        st.write(displayed_verdict)
                    st.markdown("**Risk Trigger**")
                    st.write(institutional.get("risk_trigger", "Unavailable"))
                st.markdown("**Entry/Exit Confidence**")
                st.progress(_confidence(report) / 100)
                sizing_text, sizing_color = _sizing_guidance(report, is_override if institutional else False)
                st.markdown(
                    f'<div style="color:{sizing_color};font-weight:700;">{sizing_text}</div>',
                    unsafe_allow_html=True,
                )
                st.write(f"Confidence Score: {_confidence(report)}/100")
                if institutional.get("thesis_alignment"):
                    st.caption(institutional["thesis_alignment"])
                elif llm.get("rationale"):
                    st.caption(llm["rationale"])

    with digest_tab:
        digest = generate_weekly_digest(selected_tickers, benchmark, investor_db_path=investor_db_path, persist=False)
        color_map = {"ACTION REQUIRED": "#b42318", "WATCH": "#b26a00", "NO CHANGE": "#1b5e20"}
        for entry in digest.entries:
            st.markdown(
                f'<div style="border:1px solid #d9dfe7;border-radius:12px;padding:12px 14px;margin-bottom:10px;">'
                f'<div style="color:{color_map.get(entry.priority, "#455a64")};font-weight:800;">{entry.priority}</div>'
                f'<div style="font-weight:700;">{entry.ticker} | {entry.current_regime} | {entry.composite_action}</div>'
                f'<div>Sentiment: {entry.sentiment_trend}</div>'
                f'<div>{entry.tax_note or "No tax note."}</div>'
                f'</div>',
                unsafe_allow_html=True,
            )
        if digest.regime_changes:
            st.markdown("**Regime Changes**")
            for item in digest.regime_changes:
                st.write(f"- {item}")
        if digest.sentiment_divergences:
            st.markdown("**Sentiment Divergences**")
            for item in digest.sentiment_divergences:
                st.write(f"- {item}")
        if digest.tax_alerts:
            st.markdown("**Tax Alerts**")
            for item in digest.tax_alerts:
                st.write(f"- {item}")
        if digest.action_items:
            st.markdown("**Action Items**")
            for item in digest.action_items:
                st.write(f"- {item}")


if __name__ == "__main__":
    main()
