from __future__ import annotations

import sys
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace

import pandas as pd


INVESTOR_ROOT = Path("/Volumes/T9/Projects/Dev/Investor")
if str(INVESTOR_ROOT) not in sys.path:
    sys.path.insert(0, str(INVESTOR_ROOT))

from src.app.routes import regime as regime_route
from _fixtures import FakeRegime


def _runtime(*, regime_days: int = 6, probability: float = 0.91) -> dict:
    qualitative = SimpleNamespace(
        ticker="NVDA",
        catalyst_sentiment="Positive",
        sentiment_score=4,
        catalysts=[{"title": "Datacenter demand holds", "summary": "Demand remains firm", "link": "https://example.com/nvda", "source_symbol": "NVDA"}],
        decision_prompt="prompt",
        llm_response={
            "confidence": 82,
            "institutional_report": {
                "regime_validation": "Fundamental Pivot",
                "divergence_check": "Alpha from execution",
                "verdict": "Entry",
                "risk_trigger": "Rates spike",
                "confidence_score": 8,
                "thesis_alignment": "Execution remains aligned.",
            },
        },
        fallback_confidence=78,
        thesis_check_prompt=None,
        thesis_check_response={"answer": "No. Thesis remains valid.", "rationale": "Demand still supports the setup."},
    )

    return {
        "DEFAULT_TICKERS": ["NVDA"],
        "download_market_frame": lambda **kwargs: type("MarketSeries", (), {"frame": pd.DataFrame({"price": [100.0, 101.0], "volume": [1_000_000, 1_050_000], "high": [101.0, 102.0], "low": [99.0, 100.0]})})(),
        "generate_weekly_digest": lambda **kwargs: type("Digest", (), {"action_items": [], "entries": [], "regime_changes": [], "sentiment_divergences": [], "tax_alerts": [], "generated_at": "2026-03-23T12:00:00+00:00"})(),
        "fit_regime_model": lambda ticker, market_frame: FakeRegime(
            ticker,
            "Bear" if ticker == "SOXX" else "Bull",
            latest_probability=probability,
            regime_days=regime_days,
            recent_state_mean_return=0.012,
            price_frame=pd.DataFrame(
                {"state_probability": [0.80, 0.84, 0.88, probability]},
                index=pd.to_datetime(["2026-03-18", "2026-03-19", "2026-03-20", "2026-03-21"]),
            ),
        ),
        "build_qualitative_assessment": lambda **kwargs: deepcopy(qualitative),
        "configured_frontier_model": lambda provider="auto": "Gemini: gemini-2.5-flash" if provider == "gemini" else "OpenAI: gpt-4o-mini",
        "get_current_holding_tickers": lambda db_path: ["NVDA"],
        "get_current_holding_tickers_grouped": lambda db_path: {"Personal": ["NVDA"]},
        "get_investor_db_path": lambda: "/tmp/investor.db",
        "get_portfolio_positions": lambda db_path, tickers=None, account_id=None: [],
        "get_portfolio_tickers": lambda db_path: ["NVDA"],
        "get_portfolio_tickers_filtered": lambda db_path: ["NVDA"],
        "get_tax_assumptions": lambda db_path: {},
        "get_wash_sale_risk": lambda db_path, ticker: "NONE",
        "positions_by_ticker_and_account": lambda positions: {"NVDA": []},
        "build_composite_signal": lambda *args, **kwargs: type("Composite", (), {"composite_action": "Buy"})(),
        "compute_technicals": lambda *args, **kwargs: pd.DataFrame({"rsi_14": [45, 50], "bb_pct": [0.4, 0.5], "macd_histogram": [0.1, 0.2]}),
        "confidence_trajectory": lambda *args, **kwargs: type("Trajectory", (), {"trend": "rising"})(),
        "forward_regime_curve": lambda *args, **kwargs: pd.DataFrame({"day": [1, 2], "p_bull": [0.7, 0.72], "p_neutral": [0.2, 0.18], "p_bear": [0.1, 0.1]}),
        "intra_regime_signal": lambda *args, **kwargs: "Buy the dip",
        "sentiment_momentum": lambda *args, **kwargs: (
            type("Sentiment", (), {"trend": "improving"})(),
            pd.DataFrame({"recorded_at": ["2026-03-21", "2026-03-22"], "score": [1, 2]}),
        ),
        "signal_from_forward_curve": lambda *args, **kwargs: type("Signal", (), {"action": "Buy"})(),
        "tax_adjusted_signals": lambda *args, **kwargs: [],
    }


def test_payload_structure_includes_math_and_frontier(monkeypatch) -> None:
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (_runtime(), None))
    payload = regime_route._build_regime_dashboard_payload(benchmark="SOXX", period="3y", tickers=["NVDA"], frontier_enabled=True, force_refresh=True)
    row = payload["rows"][0]
    assert row["math"]["state_statistics"]
    assert row["math"]["regime_entry_date"] == "2026-03-18"
    assert row["frontier"]["display_verdict"] == "Entry"
    assert row["qualitative"]["catalyst_sentiment"] == "Positive"


def test_frontier_toggle_skips_pipeline(monkeypatch) -> None:
    calls: list[str] = []
    runtime = _runtime()
    runtime["build_qualitative_assessment"] = lambda **kwargs: calls.append("called")
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (runtime, None))
    payload = regime_route._build_regime_dashboard_payload(benchmark="SOXX", period="3y", tickers=["NVDA"], frontier_enabled=False, force_refresh=True)
    row = payload["rows"][0]
    assert calls == []
    assert row["qualitative"] is None
    assert row["frontier"] is None


def test_catalyst_links_preserved(monkeypatch) -> None:
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (_runtime(), None))
    payload = regime_route._build_regime_dashboard_payload(benchmark="SOXX", period="3y", tickers=["NVDA"], frontier_enabled=True, force_refresh=True)
    row = payload["rows"][0]
    assert row["frontier"]["catalysts"][0]["link"] == "https://example.com/nvda"


def test_fallback_heuristics_payload_survives(monkeypatch) -> None:
    runtime = _runtime()
    runtime["build_qualitative_assessment"] = lambda **kwargs: SimpleNamespace(
        ticker="NVDA",
        catalyst_sentiment="Neutral",
        sentiment_score=0,
        catalysts=[],
        decision_prompt="prompt",
        llm_response={
            "confidence": 64,
            "institutional_report": {
                "regime_validation": "Technical Glitch",
                "divergence_check": "None",
                "verdict": "Hold",
                "risk_trigger": "Benchmark weakness deepens",
                "confidence_score": 6,
                "thesis_alignment": "Fallback note.",
            },
        },
        fallback_confidence=64,
        thesis_check_prompt=None,
        thesis_check_response=None,
    )
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (runtime, None))
    payload = regime_route._build_regime_dashboard_payload(benchmark="SOXX", period="3y", tickers=["NVDA"], frontier_enabled=True, force_refresh=True)
    frontier = payload["rows"][0]["frontier"]
    assert frontier["institutional_report"]["regime_validation"] == "Technical Glitch"
    assert frontier["confidence_pct"] == 64
    assert frontier["confidence_score"] == 6


def test_verdict_override_logic(monkeypatch) -> None:
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (_runtime(regime_days=2, probability=0.55), None))
    payload = regime_route._build_regime_dashboard_payload(benchmark="SOXX", period="3y", tickers=["NVDA"], frontier_enabled=True, force_refresh=True)
    frontier = payload["rows"][0]["frontier"]
    assert frontier["verdict_overridden"] is True
    assert frontier["display_verdict"].startswith("Hold — Regime too new")
