from __future__ import annotations

from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd

from src.regime.data import get_next_earnings_date
from src.regime.diagnostics import duration_accuracy
from src.regime.investor_adapter import get_sector_map
from src.regime.llm_layer import configured_frontier_model
from src.regime.portfolio import compute_correlation_risk
from src.regime.signals import (
    CompositeSignal,
    PriceTargets,
    SignalResult,
    apply_signal_context,
    compute_price_targets,
    earnings_warning,
)


def _composite(action: str = "Buy", strength: float = 0.65) -> CompositeSignal:
    return CompositeSignal(
        regime_signal="Bull",
        regime_probability=0.82,
        forward_signal=SignalResult(action=action, timeframe="short", strength=strength, expected_holding_days=12, rationale="test"),
        technical_signal="Buy the dip",
        composite_action=action,
        composite_strength=strength,
        short_term_view="short",
        medium_term_view="medium",
    )


def test_price_targets_include_current_price_and_position() -> None:
    technicals = pd.DataFrame(
        {
            "bb_lower": [95.0, 98.0],
            "bb_upper": [110.0, 112.0],
            "atr_14": [3.0, 4.0],
        }
    )
    targets = compute_price_targets(
        current_price=105.0,
        technicals_df=technicals,
        composite_signal=_composite("Buy", 0.7),
        expected_duration=10.0,
        state_mean_return=0.01,
    )
    assert targets.current_price == 105.0
    assert targets.price_position == "In target range — position active"


def test_apply_signal_context_flags_risk_reward_conflict() -> None:
    composite = _composite("Buy", 0.6)
    targets = PriceTargets(
        current_price=100.0,
        entry_price=100.0,
        exit_price=104.0,
        stop_price=95.5,
        risk_reward_ratio=0.89,
        timeframe_days=10,
        atr_value=2.0,
        confidence_multiplier=1.0,
        price_position="In target range — position active",
    )
    enriched = apply_signal_context(composite, price_targets=targets, earnings_warning_text=None)
    assert enriched.risk_reward_conflict is True
    assert "unfavorable" in str(enriched.risk_reward_warning)


def test_configured_frontier_model_supports_best_and_claude(monkeypatch) -> None:
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.setenv("ANTHROPIC_API_KEY", "x")
    assert configured_frontier_model("claude").startswith("Claude:")
    assert configured_frontier_model("best").endswith("(best)")


def test_get_sector_map_uses_yfinance_fallback(monkeypatch) -> None:
    monkeypatch.setattr("src.regime.investor_adapter.get_ticker_info", lambda ticker: {"sector": "Semiconductors"})
    monkeypatch.setattr("src.regime.investor_adapter.get_cached_sector", lambda ticker: None)
    saved: list[tuple[str, str]] = []
    monkeypatch.setattr("src.regime.investor_adapter.save_sector_cache", lambda ticker, sector: saved.append((ticker, sector)))
    sector_map = get_sector_map(None, ["MU"])
    assert sector_map["MU"] == "Semiconductors"
    assert saved == [("MU", "Semiconductors")]


def test_earnings_warning_and_cache_lookup(monkeypatch) -> None:
    upcoming = datetime.now(timezone.utc) + timedelta(days=2)
    monkeypatch.setattr("src.regime.data.get_cached_earnings_date", lambda ticker: None)
    monkeypatch.setattr("src.regime.data.save_earnings_cache", lambda ticker, earnings_date: None)

    monkeypatch.setattr("src.regime.data.get_earnings_date", lambda ticker: upcoming)
    earnings_date = get_next_earnings_date("MU")
    assert earnings_date is not None
    assert "Earnings imminent" in str(earnings_warning(earnings_date))


def test_duration_accuracy_and_correlation_clusters() -> None:
    duration = duration_accuracy(8.0, {"Bull": {"avg": 12.0, "median": 11.5, "min": 6.0, "max": 20.0}}, "Bull")
    assert duration["historical_avg"] == 12.0
    assert "underestimating" in str(duration["accuracy_note"])

    positions = [
        type("Pos", (), {"ticker": "MU", "market_value": 40000.0})(),
        type("Pos", (), {"ticker": "MRVL", "market_value": 35000.0})(),
        type("Pos", (), {"ticker": "GLW", "market_value": 15000.0})(),
    ]
    risk = compute_correlation_risk(
        positions,
        {
            "MU": {"label": "Bull", "sector": "Semiconductors"},
            "MRVL": {"label": "Bull", "sector": "Semiconductors"},
            "GLW": {"label": "Bull", "sector": "Semiconductors"},
        },
    )
    assert risk["clusters"]
    assert risk["clusters"][0]["pct_of_portfolio"] == 1.0
    assert risk["cluster_warnings"]
