from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import logging

from .config import DEFAULT_TICKERS
from .data import download_market_frame
from .hmm_engine import fit_regime_model
from .exceptions import InsufficientDataError
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
from .persistence import get_recent_regime_changes, save_regime_event, save_sentiment, upsert_thesis
from .signals import (
    build_composite_signal,
    compute_technicals,
    sentiment_momentum,
    signal_from_forward_curve,
    tax_adjusted_signal,
    tax_adjusted_signals,
    forward_regime_curve,
    intra_regime_signal,
)

logger = logging.getLogger(__name__)


@dataclass
class TickerDigestEntry:
    ticker: str
    current_regime: str
    regime_changed_this_week: bool
    composite_action: str
    sentiment_trend: str
    tax_note: str | None
    priority: str


@dataclass
class WeeklyDigest:
    generated_at: str
    benchmark_regime: str
    entries: list[TickerDigestEntry]
    regime_changes: list[str]
    sentiment_divergences: list[str]
    tax_alerts: list[str]
    action_items: list[str]


def generate_weekly_digest(
    tickers: list[str] | None,
    benchmark: str,
    investor_db_path: str | None = None,
    persist: bool = True,
) -> WeeklyDigest:
    investor_db_path = investor_db_path or get_investor_db_path()
    tickers = tickers or get_portfolio_tickers_filtered(investor_db_path) or DEFAULT_TICKERS
    relevant_tickers = sorted({*(tickers or []), benchmark})
    position_list = get_portfolio_positions(investor_db_path, relevant_tickers)
    positions = positions_by_ticker(position_list)
    positions_by_account = positions_by_ticker_and_account(position_list)
    tax_assumptions = get_tax_assumptions(investor_db_path)

    benchmark_market = download_market_frame(benchmark, period="3y", interval="1d").frame
    benchmark_regime = _adaptive_fit_regime_model(benchmark, benchmark_market)

    entries: list[TickerDigestEntry] = []
    regime_changes: list[str] = []
    sentiment_divergences: list[str] = []
    tax_alerts: list[str] = []
    action_items: list[str] = []

    for ticker in tickers:
        try:
            market_frame = download_market_frame(ticker, period="3y", interval="1d").frame
            regime = _adaptive_fit_regime_model(ticker, market_frame)
        except (ValueError, InsufficientDataError) as exc:
            logger.warning("Skipping %s in weekly digest due to insufficient history.", ticker)
            logger.debug("Weekly digest regime build failed for %s.", ticker, exc_info=exc)
            continue
        persistence = save_regime_event(ticker, regime.latest_label, regime.latest_state_id) if persist else {"previous_label": None, "days_in_regime": regime.regime_days}
        qualitative = build_qualitative_assessment(
            ticker=ticker,
            regime_signal=regime.regime_signal,
            state_name=regime.latest_label,
            latest_probability=regime.latest_probability,
            context_symbols=[benchmark, "SPY", "^TNX"],
            initial_thesis=upsert_thesis(ticker, None),
            previous_label=persistence["previous_label"],
            benchmark_state=benchmark_regime.latest_label,
        )
        if persist:
            save_sentiment(ticker, qualitative.sentiment_score, qualitative.catalyst_sentiment, len(qualitative.catalysts))

        forward_curve = forward_regime_curve(regime.transition_matrix, regime.latest_state_vector, horizon=21)
        forward_signal = signal_from_forward_curve(
            forward_curve,
            regime.latest_label,
            regime.transition_risk,
            regime.expected_regime_duration,
            regime.latest_probability,
        )
        technicals = compute_technicals(
            market_frame["price"],
            market_frame["volume"],
            market_frame["high"] if "high" in market_frame.columns else None,
            market_frame["low"] if "low" in market_frame.columns else None,
        )
        technical_signal = intra_regime_signal(technicals, regime.latest_label)
        composite = build_composite_signal(regime.latest_label, regime.latest_probability, forward_signal, technical_signal)

        tax_signal = None
        account_tax_signals = []
        wash_sale_risk = get_wash_sale_risk(investor_db_path, ticker)
        if ticker.upper() in positions_by_account:
            account_tax_signals = tax_adjusted_signals(
                composite,
                positions_by_account[ticker.upper()],
                tax_assumptions,
                wash_sale_risk=wash_sale_risk,
            )
            taxable_signals = [signal for signal in account_tax_signals if signal.account_type == "TAXABLE"]
            tax_signal = taxable_signals[0] if taxable_signals else account_tax_signals[0]
        elif ticker.upper() in positions:
            tax_signal = tax_adjusted_signal(
                composite,
                positions[ticker.upper()],
                tax_assumptions,
                wash_sale_risk=wash_sale_risk,
            )
            account_tax_signals = [tax_signal]

        sentiment_info, _ = sentiment_momentum(ticker, regime.latest_label)
        changed_rows = get_recent_regime_changes(ticker, days=7)
        changed_this_week = bool(changed_rows)
        if changed_rows:
            latest_change = changed_rows[0]
            regime_changes.append(
                f"{ticker}: {latest_change.get('previous_label') or 'Unknown'} → {latest_change['current_label']} on {str(latest_change['changed_at'])[:10]}"
            )
        if sentiment_info.divergence_vs_regime and sentiment_info.warning:
            sentiment_divergences.append(f"{ticker}: {sentiment_info.warning}")
        for account_signal in account_tax_signals:
            if account_signal.ltcg_threshold_date:
                tax_alerts.append(
                    f"{ticker} ({account_signal.account_name}) lot converts to LTCG on {account_signal.ltcg_threshold_date}"
                )
            if account_signal.wash_sale_warning:
                tax_alerts.append(f"{ticker} ({account_signal.account_name}): {account_signal.wash_sale_warning}")

        priority = "NO CHANGE"
        if changed_this_week or composite.composite_action in {"Strong Buy", "Strong Sell"} or (tax_signal and tax_signal.ltcg_threshold_date):
            priority = "ACTION REQUIRED"
        elif sentiment_info.divergence_vs_regime or regime.transition_risk > 0.15 or composite.composite_action in {"Buy", "Sell"}:
            priority = "WATCH"

        tax_note = None
        if account_tax_signals:
            tax_note = " | ".join(
                f"{signal.account_name}: {signal.adjusted_action} ({signal.tax_note})"
                for signal in account_tax_signals
            )
        entries.append(
            TickerDigestEntry(
                ticker=ticker,
                current_regime=regime.latest_label,
                regime_changed_this_week=changed_this_week,
                composite_action=composite.composite_action,
                sentiment_trend=sentiment_info.trend,
                tax_note=tax_note,
                priority=priority,
            )
        )
        if priority != "NO CHANGE":
            action_items.append(f"{ticker}: {priority} — {composite.composite_action} in {regime.latest_label} regime")

    return WeeklyDigest(
        generated_at=datetime.now(timezone.utc).isoformat(),
        benchmark_regime=benchmark_regime.latest_label,
        entries=entries,
        regime_changes=regime_changes,
        sentiment_divergences=sentiment_divergences,
        tax_alerts=tax_alerts,
        action_items=action_items,
    )


def _adaptive_fit_regime_model(ticker: str, market_frame):
    try:
        return fit_regime_model(ticker, market_frame)
    except InsufficientDataError:
        adaptive_window = max(63, min(504, max(63, len(market_frame) - 21)))
        if adaptive_window >= 504:
            raise
        return fit_regime_model(ticker, market_frame, training_window=adaptive_window)


def digest_to_dict(digest: WeeklyDigest) -> dict:
    return {
        "generated_at": digest.generated_at,
        "benchmark_regime": digest.benchmark_regime,
        "entries": [asdict(entry) for entry in digest.entries],
        "regime_changes": digest.regime_changes,
        "sentiment_divergences": digest.sentiment_divergences,
        "tax_alerts": digest.tax_alerts,
        "action_items": digest.action_items,
    }


def digest_to_text(digest: WeeklyDigest) -> str:
    lines = [
        f"Weekly Digest ({digest.generated_at[:10]})",
        f"Benchmark regime: {digest.benchmark_regime}",
        "",
    ]
    for entry in digest.entries:
        lines.append(
            f"{entry.priority}: {entry.ticker} | regime={entry.current_regime} | action={entry.composite_action} | sentiment={entry.sentiment_trend}"
        )
        if entry.tax_note:
            lines.append(f"  Tax: {entry.tax_note}")
    if digest.regime_changes:
        lines.append("")
        lines.append("Regime changes:")
        lines.extend(f"- {item}" for item in digest.regime_changes)
    if digest.sentiment_divergences:
        lines.append("")
        lines.append("Sentiment divergences:")
        lines.extend(f"- {item}" for item in digest.sentiment_divergences)
    if digest.tax_alerts:
        lines.append("")
        lines.append("Tax alerts:")
        lines.extend(f"- {item}" for item in digest.tax_alerts)
    if digest.action_items:
        lines.append("")
        lines.append("Action items:")
        lines.extend(f"- {item}" for item in digest.action_items)
    return "\n".join(lines)
