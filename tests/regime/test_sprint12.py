from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
from fastapi import FastAPI
from fastapi.testclient import TestClient


INVESTOR_ROOT = Path("/Volumes/T9/Projects/Dev/Investor")
if str(INVESTOR_ROOT) not in sys.path:
    sys.path.insert(0, str(INVESTOR_ROOT))

from src.app.routes import regime as regime_route
from _fixtures import FakeRegime


def _runtime(*, qualitative_callback=None) -> dict:
    class FakeDigest:
        def __init__(self):
            self.generated_at = "2026-03-23T12:00:00+00:00"
            self.benchmark_regime = "Bear"
            self.entries = []
            self.regime_changes = []
            self.sentiment_divergences = []
            self.tax_alerts = []
            self.action_items = []

    def build_qualitative_assessment(**kwargs):
        if qualitative_callback is not None:
            qualitative_callback(kwargs)
        return {
            "catalysts": [{"title": "Headline", "link": "https://example.com/item"}],
            "llm_response": {
                "confidence": 82,
                "institutional_report": {
                    "regime_validation": "Aligned",
                    "divergence_check": "None",
                    "verdict": "Entry",
                    "risk_trigger": "Rates spike",
                    "confidence_score": 8,
                    "thesis_alignment": "Aligned",
                },
            },
            "thesis_check_response": {"answer": "No", "rationale": "Still aligned."},
        }

    return {
        "DEFAULT_TICKERS": ["NVDA"],
        "download_market_frame": lambda **kwargs: type("MarketSeries", (), {"frame": pd.DataFrame({"price": [100.0, 101.0], "volume": [1_000_000, 1_050_000], "high": [101.0, 102.0], "low": [99.0, 100.0]})})(),
        "generate_weekly_digest": lambda **kwargs: FakeDigest(),
        "fit_regime_model": lambda ticker, market_frame: FakeRegime(ticker, "Bear" if ticker == "SOXX" else "Bull"),
        "build_qualitative_assessment": build_qualitative_assessment,
        "configured_frontier_model": lambda provider="auto": "Gemini: gemini-2.5-flash" if provider == "gemini" else "OpenAI: gpt-4o-mini",
        "get_investor_db_path": lambda: "/tmp/investor.db",
        "get_portfolio_positions": lambda db_path, tickers=None, account_id=None: [],
        "get_portfolio_tickers": lambda db_path: ["NVDA", "AVGO", "HIST"],
        "get_tax_assumptions": lambda db_path: {},
        "get_wash_sale_risk": lambda db_path, ticker: "NONE",
        "positions_by_ticker_and_account": lambda positions: {"NVDA": [], "AVGO": []},
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


def _client(monkeypatch) -> TestClient:
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (_runtime(), None))
    monkeypatch.setattr(
        regime_route,
        "get_current_tickers_by_scope",
        lambda session, scope, account_id=None: (
            ["NVDA"]
            if account_id == 101
            else ["AVGO"]
            if account_id == 202
            else ["NVDA", "MSFT"]
            if scope == "personal"
            else ["AVGO"]
            if scope == "trust"
            else ["NVDA", "AVGO"]
        ),
    )
    monkeypatch.setattr(
        regime_route,
        "get_available_portfolio_scopes",
        lambda session: [
            {"value": "household", "label": "All Portfolios", "ticker_count": 2, "accounts": [{"id": 101, "name": "RJ-Taxable", "ticker_count": 1, "has_holdings": True}, {"id": 202, "name": "Chase-2138", "ticker_count": 1, "has_holdings": True}]},
            {"value": "personal", "label": "Personal", "ticker_count": 2, "accounts": [{"id": 101, "name": "RJ-Taxable", "ticker_count": 2, "has_holdings": True}]},
            {"value": "trust", "label": "Trust", "ticker_count": 1, "accounts": [{"id": 202, "name": "Chase-2138", "ticker_count": 1, "has_holdings": True}]},
        ],
    )
    app = FastAPI()
    app.include_router(regime_route.router)
    app.dependency_overrides[regime_route.require_actor] = lambda: "tester"
    app.dependency_overrides[regime_route.db_session] = lambda: iter([object()])
    return TestClient(app)


def test_frontier_provider_passthrough(monkeypatch) -> None:
    calls: list[dict] = []
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (_runtime(qualitative_callback=calls.append), None))
    payload = regime_route._build_regime_dashboard_payload(
        benchmark="SOXX",
        period="3y",
        tickers=["NVDA"],
        frontier_enabled=True,
        frontier_provider="gemini",
    )
    assert calls
    assert calls[0]["frontier_provider"] == "gemini"
    assert payload["frontier_model"] == "Gemini: gemini-2.5-flash"
    assert payload["rows"][0]["frontier"]["model_name"] == "Gemini: gemini-2.5-flash"


def test_portfolios_endpoint_returns_scopes(monkeypatch) -> None:
    client = _client(monkeypatch)
    response = client.get("/regime/portfolios")
    assert response.status_code == 200
    assert response.json()["scopes"][1]["value"] == "personal"
    assert response.json()["scopes"][1]["ticker_count"] == 2
    assert response.json()["scopes"][0]["accounts"][0]["name"] == "RJ-Taxable"


def test_holdings_endpoint_respects_portfolio_scope_and_account(monkeypatch) -> None:
    client = _client(monkeypatch)
    response = client.get("/regime/holdings?portfolio_scope=trust&account_id=202")
    assert response.status_code == 200
    payload = response.json()
    assert payload["portfolio_scope"] == "trust"
    assert payload["account_id"] == 202
    assert payload["tickers"] == ["AVGO"]
    assert payload["groups"] == {"Current Holdings": ["AVGO"]}


def test_portfolio_tickers_do_not_fallback_to_historical_when_current_empty(monkeypatch) -> None:
    monkeypatch.setattr(regime_route, "get_current_tickers_by_scope", lambda session, scope, account_id=None: [])
    runtime = _runtime()
    investor_db_path, tickers = regime_route._portfolio_tickers(runtime, object(), False, "household")
    assert investor_db_path == "/tmp/investor.db"
    assert tickers == []


def test_show_all_explicitly_uses_historical_holdings(monkeypatch) -> None:
    monkeypatch.setattr(regime_route, "get_current_tickers_by_scope", lambda session, scope, account_id=None: [])
    runtime = _runtime()
    _investor_db_path, tickers = regime_route._portfolio_tickers(runtime, object(), True, "household")
    assert tickers == ["NVDA", "AVGO", "HIST"]
