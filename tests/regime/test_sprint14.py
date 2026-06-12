from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

HMM_ROOT = Path("/Volumes/T9/Projects/Dev/HMM")
if str(HMM_ROOT) not in sys.path:
    sys.path.insert(0, str(HMM_ROOT))

from src.regime.investor_adapter import PortfolioPosition, TaxLotInfo
from src.regime.signals import CompositeSignal, SignalResult, tax_adjusted_signal

INVESTOR_ROOT = Path("/Volumes/T9/Projects/Dev/Investor")
if str(INVESTOR_ROOT) not in sys.path:
    sys.path.insert(0, str(INVESTOR_ROOT))

from src.app.routes import regime as regime_route
from src.app.routes import regime_cache
from _fixtures import FakeRegime


def _composite(action: str = "Sell") -> CompositeSignal:
    return CompositeSignal(
        regime_signal="Bull",
        regime_probability=0.91,
        forward_signal=SignalResult(action=action, timeframe="short", strength=0.8, expected_holding_days=12, rationale="test"),
        technical_signal="Hold / add on weakness",
        composite_action=action,
        composite_strength=0.8,
        short_term_view="short",
        medium_term_view="medium",
    )


def _position() -> PortfolioPosition:
    return PortfolioPosition(
        ticker="NVDA",
        account_name="RJ-Taxable",
        account_type="TAXABLE",
        taxpayer_type="PERSONAL",
        qty=30.0,
        market_value=3600.0,
        current_price=120.0,
        cost_basis=2700.0,
        unrealized_gain=900.0,
        asset_class="EQUITY",
        lots=[
            TaxLotInfo(
                lot_id=1,
                acquisition_date="2025-04-02",
                qty=10.0,
                basis_total=900.0,
                days_held=355,
                term="ST",
                unrealized_gain=220.0,
                days_to_ltcg=10,
            ),
            TaxLotInfo(
                lot_id=2,
                acquisition_date="2025-04-12",
                qty=8.0,
                basis_total=720.0,
                days_held=345,
                term="ST",
                unrealized_gain=140.0,
                days_to_ltcg=20,
            ),
            TaxLotInfo(
                lot_id=3,
                acquisition_date="2024-02-01",
                qty=12.0,
                basis_total=1080.0,
                days_held=700,
                term="LT",
                unrealized_gain=540.0,
                days_to_ltcg=0,
            ),
        ],
    )


def _runtime(*, qualitative_calls=None, save_sentiment_calls=None, tax_signals=None) -> dict:
    qualitative_payload = {
        "sentiment_score": 4,
        "catalyst_sentiment": "Positive",
        "catalysts": [{"title": "Headline", "link": "https://example.com/nvda"}],
        "llm_response": {"confidence": 81, "institutional_report": {"verdict": "Entry", "confidence_score": 8}},
        "thesis_check_response": {"answer": "Still intact"},
    }

    def build_qualitative_assessment(**kwargs):
        if qualitative_calls is not None:
            qualitative_calls.append(kwargs)
        return qualitative_payload

    position = _position()
    tax_signal = SimpleNamespace(
        account_name="RJ-Taxable",
        account_type="TAXABLE",
        adjusted_action="Hold",
        original_action="Sell",
        tax_note="2 short-term lots convert to LTCG within 30 days.",
        ltcg_threshold_date="2026-04-02",
        estimated_tax_impact=180.0,
        wash_sale_warning=None,
        tax_status="Mixed",
    )
    if tax_signals is None:
        tax_signals = [tax_signal]

    return {
        "DEFAULT_TICKERS": ["NVDA"],
        "download_market_frame": lambda **kwargs: type("MarketSeries", (), {"frame": pd.DataFrame({"price": [100.0, 101.0], "volume": [1_000_000, 1_050_000], "high": [101.0, 102.0], "low": [99.0, 100.0]})})(),
        "generate_weekly_digest": lambda **kwargs: type("Digest", (), {"action_items": [], "entries": [], "regime_changes": [], "sentiment_divergences": [], "tax_alerts": [], "generated_at": "2026-03-23T12:00:00+00:00"})(),
        "fit_regime_model": lambda ticker, market_frame: FakeRegime(ticker, "Bear" if ticker == "SOXX" else "Bull"),
        "build_qualitative_assessment": build_qualitative_assessment,
        "configured_frontier_model": lambda provider="auto": "OpenAI: gpt-4o-mini",
        "get_investor_db_path": lambda: "/tmp/investor.db",
        "get_portfolio_positions": lambda db_path, tickers=None, account_id=None: [position],
        "get_portfolio_tickers": lambda db_path: ["NVDA"],
        "get_tax_assumptions": lambda db_path: {},
        "get_wash_sale_risk": lambda db_path, ticker: "NONE",
        "positions_by_ticker_and_account": lambda positions: {"NVDA": [position]},
        "save_regime_event": lambda ticker, label, state_id: {"previous_label": "Neutral", "days_in_regime": 2},
        "save_sentiment": (lambda *args: save_sentiment_calls.append(args)) if save_sentiment_calls is not None else None,
        "build_composite_signal": lambda *args, **kwargs: type("Composite", (), {"composite_action": "Sell"})(),
        "compute_technicals": lambda *args, **kwargs: pd.DataFrame({"rsi_14": [45, 50], "bb_pct": [0.4, 0.5], "macd_histogram": [0.1, 0.2]}),
        "confidence_trajectory": lambda *args, **kwargs: type("Trajectory", (), {"trend": "rising"})(),
        "forward_regime_curve": lambda *args, **kwargs: pd.DataFrame({"day": [1, 2], "p_bull": [0.7, 0.72], "p_neutral": [0.2, 0.18], "p_bear": [0.1, 0.1]}),
        "intra_regime_signal": lambda *args, **kwargs: "Take partial profits",
        "sentiment_momentum": lambda *args, **kwargs: (
            type("Sentiment", (), {"trend": "improving"})(),
            pd.DataFrame({"recorded_at": ["2026-03-21", "2026-03-22"], "score": [1, 2]}),
        ),
        "signal_from_forward_curve": lambda *args, **kwargs: type("Signal", (), {"action": "Sell"})(),
        "tax_adjusted_signals": lambda *args, **kwargs: tax_signals,
        "list_theses": lambda: [],
        "upsert_thesis": lambda ticker, thesis=None: None,
    }


def test_tax_adjusted_signal_flags_all_short_term_lots_near_ltcg() -> None:
    adjusted = tax_adjusted_signal(
        _composite("Sell"),
        _position(),
        {"ordinary_rate": 0.37, "ltcg_rate": 0.20, "niit_rate": 0.0},
    )
    assert adjusted.adjusted_action == "Hold"
    assert "2 short-term lots convert to LTCG within 30 days" in adjusted.tax_note
    assert adjusted.ltcg_threshold_date == "2026-04-02"
    assert adjusted.tax_status == "Mixed"


def test_regime_payload_includes_action_tax_status_and_lot_details(monkeypatch) -> None:
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (_runtime(), None))
    monkeypatch.setattr(regime_route, "get_current_tickers_by_scope", lambda session, scope, account_id=None: ["NVDA"])
    monkeypatch.setattr(
        regime_route,
        "get_lot_details_by_scope",
        lambda session, **kwargs: {
            "NVDA": [
                {
                    "ticker": "NVDA",
                    "account_name": "RJ-Taxable",
                    "account_type": "TAXABLE",
                    "acquisition_date": "2025-04-02",
                    "qty": 10.0,
                    "basis_total": 900.0,
                    "cost_basis": 900.0,
                    "term": "ST",
                    "days_to_ltcg": 10,
                    "unrealized_gain": 220.0,
                    "market_value": 1200.0,
                    "near_ltcg": True,
                },
                {
                    "ticker": "NVDA",
                    "account_name": "RJ-Taxable",
                    "account_type": "TAXABLE",
                    "acquisition_date": "2025-04-12",
                    "qty": 8.0,
                    "basis_total": 720.0,
                    "cost_basis": 720.0,
                    "term": "ST",
                    "days_to_ltcg": 20,
                    "unrealized_gain": 140.0,
                    "market_value": 960.0,
                    "near_ltcg": True,
                },
                {
                    "ticker": "NVDA",
                    "account_name": "RJ-Taxable",
                    "account_type": "TAXABLE",
                    "acquisition_date": "2024-02-01",
                    "qty": 12.0,
                    "basis_total": 1080.0,
                    "cost_basis": 1080.0,
                    "term": "LT",
                    "days_to_ltcg": 0,
                    "unrealized_gain": 540.0,
                    "market_value": 1440.0,
                    "near_ltcg": False,
                },
            ]
        },
    )
    payload = regime_route._build_regime_dashboard_payload(session=object(), benchmark="SOXX", period="3y", tickers=["NVDA"])
    row = payload["rows"][0]
    assert row["action"] == "Hold"
    assert row["tax_status"] == "2 ST · 1 LT"
    assert row["lot_count_st"] == 2
    assert row["lot_count_lt"] == 1
    assert len(row["lot_details"]) == 3
    assert row["lot_details"][0]["near_ltcg"] is True


def test_sentiment_persists_on_fresh_qualitative_compute(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(regime_cache, "_CACHE_ROOT", tmp_path / "regime_cache")
    saved: list[tuple] = []
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (_runtime(save_sentiment_calls=saved), None))
    regime_route._build_regime_dashboard_payload(
        benchmark="SOXX",
        period="3y",
        tickers=["NVDA"],
        frontier_enabled=True,
        force_refresh=True,
    )
    assert saved == [("NVDA", 4, "Positive", 1)]


def test_sentiment_not_persisted_on_qualitative_cache_hit(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(regime_cache, "_CACHE_ROOT", tmp_path / "regime_cache")
    regime_cache.save_qualitative_cache(
        "NVDA",
        provider="auto",
        data={"sentiment_score": 4, "catalyst_sentiment": "Positive", "catalysts": [{"title": "Headline"}], "llm_response": {}},
    )
    llm_calls: list[dict] = []
    saved: list[tuple] = []
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (_runtime(qualitative_calls=llm_calls, save_sentiment_calls=saved), None))
    regime_route._build_regime_dashboard_payload(
        benchmark="SOXX",
        period="3y",
        tickers=["NVDA"],
        frontier_enabled=True,
        force_refresh=False,
    )
    assert llm_calls == []
    assert saved == []


def test_regime_history_payload_preserved_for_rendering(monkeypatch) -> None:
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (_runtime(), None))
    monkeypatch.setattr(
        regime_route,
        "_fetch_regime_change_history",
        lambda tickers, days=90: [{"ticker": "NVDA", "previous_label": "Neutral", "current_label": "Bull", "changed_at": "2026-03-20T12:00:00+00:00"}],
    )
    payload = regime_route._build_regime_dashboard_payload(benchmark="SOXX", period="3y", tickers=["NVDA"])
    assert payload["regime_history"][0]["previous_label"] == "Neutral"
    assert payload["regime_history"][0]["current_label"] == "Bull"
    row = payload["rows"][0]
    assert row["previous_regime"] == "Neutral"
    assert row["p_bull_day5"] == 0.72
    assert row["p_neutral_day5"] == 0.18
    assert row["p_bear_day5"] == 0.1
    assert row["forward_probabilities"]["p_bull_day5"] == 0.72
    assert row["signal_diagnostics"]["p_bull_day5"] == 0.72


def test_forward_probability_context_uses_explicit_day_rows() -> None:
    context = regime_route._forward_curve_probability_context(
        pd.DataFrame(
            {
                "day": [1, 5, 21],
                "p_bull": [0.62, 0.48, 0.41],
                "p_neutral": [0.28, 0.39, 0.37],
                "p_bear": [0.10, 0.13, 0.22],
            }
        )
    )
    assert context["p_bull_day5"] == 0.48
    assert context["p_neutral_day5"] == 0.39
    assert context["p_bear_day5"] == 0.13
    assert context["p_bull_day21"] == 0.41


def test_non_material_tax_signal_still_exposes_lot_details(monkeypatch) -> None:
    non_material = SimpleNamespace(
        account_name="RJ-Taxable",
        account_type="TAXABLE",
        adjusted_action="Sell",
        original_action="Sell",
        tax_note="No material tax adjustment.",
        ltcg_threshold_date=None,
        estimated_tax_impact=0.0,
        wash_sale_warning=None,
        tax_status="Mixed",
    )
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (_runtime(tax_signals=[non_material]), None))
    monkeypatch.setattr(regime_route, "get_current_tickers_by_scope", lambda session, scope, account_id=None: ["NVDA"])
    monkeypatch.setattr(
        regime_route,
        "get_lot_details_by_scope",
        lambda session, **kwargs: {
            "NVDA": [
                {"ticker": "NVDA", "account_name": "RJ-Taxable", "account_type": "TAXABLE", "acquisition_date": "2025-04-02", "qty": 10.0, "basis_total": 900.0, "cost_basis": 900.0, "term": "ST", "days_to_ltcg": 10, "unrealized_gain": 220.0, "market_value": 1200.0, "near_ltcg": True},
                {"ticker": "NVDA", "account_name": "RJ-Taxable", "account_type": "TAXABLE", "acquisition_date": "2025-04-12", "qty": 8.0, "basis_total": 720.0, "cost_basis": 720.0, "term": "ST", "days_to_ltcg": 20, "unrealized_gain": 140.0, "market_value": 960.0, "near_ltcg": True},
                {"ticker": "NVDA", "account_name": "RJ-Taxable", "account_type": "TAXABLE", "acquisition_date": "2024-02-01", "qty": 12.0, "basis_total": 1080.0, "cost_basis": 1080.0, "term": "LT", "days_to_ltcg": 0, "unrealized_gain": 540.0, "market_value": 1440.0, "near_ltcg": False},
            ]
        },
    )
    payload = regime_route._build_regime_dashboard_payload(session=object(), benchmark="SOXX", period="3y", tickers=["NVDA"])
    row = payload["rows"][0]
    assert row["account_tax_signals"] == []
    assert row["action"] == "—"
    assert row["tax_status"] == "2 ST · 1 LT"
    assert len(row["lot_details"]) == 3
