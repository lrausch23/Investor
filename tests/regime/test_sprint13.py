from __future__ import annotations

import datetime as dt
import json
import sys
from pathlib import Path

import pandas as pd
from fastapi import FastAPI
from fastapi.testclient import TestClient


INVESTOR_ROOT = Path("/Volumes/T9/Projects/Dev/Investor")
if str(INVESTOR_ROOT) not in sys.path:
    sys.path.insert(0, str(INVESTOR_ROOT))

from src.app.routes import regime as regime_route
from src.app.routes import regime_cache
from _fixtures import FakeRegime


def _runtime(*, qualitative_callback=None) -> tuple[dict, dict]:
    theses = {"NVDA": "AI demand remains durable."}

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
            "thesis_check_response": {"answer": "Review thesis", "rationale": "Regime changed."},
        }

    runtime = {
        "DEFAULT_TICKERS": ["NVDA"],
        "build_qualitative_assessment": build_qualitative_assessment,
        "configured_frontier_model": lambda provider="auto": "OpenAI: gpt-4o-mini",
        "delete_thesis": lambda ticker: theses.pop(ticker.upper(), None) is not None,
        "download_market_frame": lambda **kwargs: type("MarketSeries", (), {"frame": pd.DataFrame({"price": [100.0, 101.0], "volume": [1_000_000, 1_050_000], "high": [101.0, 102.0], "low": [99.0, 100.0]})})(),
        "fit_regime_model": lambda ticker, market_frame: FakeRegime(ticker, "Bear" if ticker == "SOXX" else "Bull"),
        "generate_weekly_digest": lambda **kwargs: FakeDigest(),
        "get_investor_db_path": lambda: "/tmp/investor.db",
        "get_portfolio_positions": lambda db_path, tickers=None, account_id=None: [],
        "get_portfolio_tickers": lambda db_path: ["NVDA"],
        "get_tax_assumptions": lambda db_path: {},
        "get_wash_sale_risk": lambda db_path, ticker: "NONE",
        "intra_regime_signal": lambda *args, **kwargs: "Buy the dip",
        "list_theses": lambda: [{"ticker": ticker, "thesis": thesis, "updated_at": "2026-03-23T12:00:00+00:00"} for ticker, thesis in sorted(theses.items())],
        "positions_by_ticker_and_account": lambda positions: {"NVDA": []},
        "save_regime_event": lambda ticker, label, state_id: {"previous_label": "Neutral", "days_in_regime": 1},
        "sentiment_momentum": lambda *args, **kwargs: (type("Sentiment", (), {"trend": "improving"})(), pd.DataFrame({"recorded_at": ["2026-03-21"], "score": [2]})),
        "signal_from_forward_curve": lambda *args, **kwargs: type("Signal", (), {"action": "Buy"})(),
        "tax_adjusted_signals": lambda *args, **kwargs: [],
        "upsert_thesis": lambda ticker, thesis=None: theses.get(ticker.upper()) if thesis is None else theses.__setitem__(ticker.upper(), thesis) or thesis,
        "build_composite_signal": lambda *args, **kwargs: type("Composite", (), {"composite_action": "Buy"})(),
        "compute_technicals": lambda *args, **kwargs: pd.DataFrame({"rsi_14": [45, 50], "bb_pct": [0.4, 0.5], "macd_histogram": [0.1, 0.2]}),
        "confidence_trajectory": lambda *args, **kwargs: type("Trajectory", (), {"trend": "rising"})(),
        "forward_regime_curve": lambda *args, **kwargs: pd.DataFrame({"day": [1, 2], "p_bull": [0.7, 0.72], "p_neutral": [0.2, 0.18], "p_bear": [0.1, 0.1]}),
    }
    return runtime, theses


def _client(monkeypatch, runtime: dict) -> TestClient:
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (runtime, None))
    monkeypatch.setattr(regime_route, "get_current_tickers_by_scope", lambda session, scope, account_id=None: ["NVDA"])
    monkeypatch.setattr(regime_route, "get_available_portfolio_scopes", lambda session: [{"value": "household", "label": "All Portfolios", "ticker_count": 1, "accounts": [{"id": 101, "name": "RJ-Taxable", "ticker_count": 1, "has_holdings": True}]}])
    app = FastAPI()
    app.include_router(regime_route.router)
    app.dependency_overrides[regime_route.require_actor] = lambda: "tester"
    app.dependency_overrides[regime_route.db_session] = lambda: iter([object()])
    return TestClient(app)


def test_thesis_crud_endpoints(monkeypatch) -> None:
    runtime, theses = _runtime()
    client = _client(monkeypatch, runtime)
    assert client.get("/regime/theses").status_code == 200
    assert client.get("/regime/thesis/NVDA").json()["thesis"] == theses["NVDA"]
    saved = client.post("/regime/thesis/AVGO", data={"thesis": "Broadcom networking cashflow stays resilient."})
    assert saved.status_code == 200
    listing = client.get("/regime/theses").json()["theses"]
    assert any(item["ticker"] == "AVGO" for item in listing)
    deleted = client.delete("/regime/thesis/AVGO")
    assert deleted.status_code == 200
    assert deleted.json()["deleted"] is True
    too_long = client.post("/regime/thesis/NVDA", data={"thesis": "x" * 2001})
    assert too_long.status_code == 422


def test_thesis_passed_to_llm(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(regime_cache, "_CACHE_ROOT", tmp_path / "regime_cache")
    calls: list[dict] = []
    runtime, _ = _runtime(qualitative_callback=calls.append)
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (runtime, None))
    payload = regime_route._build_regime_dashboard_payload(benchmark="SOXX", period="3y", tickers=["NVDA"], frontier_enabled=True)
    assert payload["rows"]
    assert calls[0]["initial_thesis"] == "AI demand remains durable."


def test_thesis_check_wires_previous_label_on_regime_change(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(regime_cache, "_CACHE_ROOT", tmp_path / "regime_cache")
    calls: list[dict] = []
    runtime, _ = _runtime(qualitative_callback=calls.append)
    runtime["save_regime_event"] = lambda ticker, label, state_id: {"previous_label": "Bear", "days_in_regime": 1}
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (runtime, None))
    regime_route._build_regime_dashboard_payload(benchmark="SOXX", period="3y", tickers=["NVDA"], frontier_enabled=True)
    assert calls[0]["previous_label"] == "Bear"


def test_qualitative_cache_hit_skips_llm(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(regime_cache, "_CACHE_ROOT", tmp_path / "regime_cache")
    regime_cache.save_qualitative_cache("NVDA", provider="auto", data={"catalysts": [], "llm_response": {}, "thesis_check_response": None})
    calls: list[dict] = []
    runtime, _ = _runtime(qualitative_callback=calls.append)
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (runtime, None))
    payload = regime_route._build_regime_dashboard_payload(benchmark="SOXX", period="3y", tickers=["NVDA"], frontier_enabled=True)
    assert calls == []
    assert payload["rows"][0]["qualitative"]["catalysts"] == []


def test_qualitative_cache_expiry_triggers_llm(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(regime_cache, "_CACHE_ROOT", tmp_path / "regime_cache")
    path = regime_cache.save_qualitative_cache("NVDA", provider="auto", data={"catalysts": []})
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["cached_at"] = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=5)).isoformat()
    path.write_text(json.dumps(payload), encoding="utf-8")
    calls: list[dict] = []
    runtime, _ = _runtime(qualitative_callback=calls.append)
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (runtime, None))
    regime_route._build_regime_dashboard_payload(benchmark="SOXX", period="3y", tickers=["NVDA"], frontier_enabled=True)
    assert len(calls) == 1


def test_qualitative_cache_provider_change_triggers_llm(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(regime_cache, "_CACHE_ROOT", tmp_path / "regime_cache")
    regime_cache.save_qualitative_cache("NVDA", provider="auto", data={"catalysts": []})
    calls: list[dict] = []
    runtime, _ = _runtime(qualitative_callback=calls.append)
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (runtime, None))
    regime_route._build_regime_dashboard_payload(benchmark="SOXX", period="3y", tickers=["NVDA"], frontier_enabled=True, frontier_provider="gemini")
    assert len(calls) == 1
    assert calls[0]["frontier_provider"] == "gemini"


def test_dead_preset_functions_removed() -> None:
    assert not hasattr(regime_cache, "load_presets")
    assert not hasattr(regime_cache, "save_presets")
    assert not hasattr(regime_cache, "upsert_preset")
    assert not hasattr(regime_cache, "delete_preset")
