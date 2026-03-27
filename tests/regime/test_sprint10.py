from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pandas as pd
from fastapi import FastAPI
from fastapi.testclient import TestClient

HMM_ROOT = Path("/Volumes/T9/Projects/Dev/HMM")
if str(HMM_ROOT) not in sys.path:
    sys.path.insert(0, str(HMM_ROOT))

from src.regime import investor_adapter


INVESTOR_ROOT = Path("/Volumes/T9/Projects/Dev/Investor")
if str(INVESTOR_ROOT) not in sys.path:
    sys.path.insert(0, str(INVESTOR_ROOT))

from src.app.routes import regime as regime_route
from _fixtures import FakeRegime


def _base_runtime() -> dict:
    class FakeDigest:
        def __init__(self):
            self.generated_at = "2026-03-23T12:00:00+00:00"
            self.benchmark_regime = "Bear"
            self.entries = []
            self.regime_changes = []
            self.sentiment_divergences = []
            self.tax_alerts = []
            self.action_items = []

    class FakeTaxSignal:
        def __init__(self, *, adjusted_action: str = "Hold", original_action: str = "Hold", tax_note: str = "Wait", estimated_tax_impact: float = 0.0, ltcg_threshold_date: str | None = None, wash_sale_warning: str | None = None):
            self.account_name = "Brokerage"
            self.account_type = "TAXABLE"
            self.adjusted_action = adjusted_action
            self.original_action = original_action
            self.tax_note = tax_note
            self.ltcg_threshold_date = ltcg_threshold_date
            self.estimated_tax_impact = estimated_tax_impact
            self.wash_sale_warning = wash_sale_warning

    return {
        "DEFAULT_TICKERS": ["NVDA"],
        "download_market_frame": lambda **kwargs: type("MarketSeries", (), {"frame": pd.DataFrame({"price": [100.0, 101.0], "volume": [1_000_000, 1_050_000], "high": [101.0, 102.0], "low": [99.0, 100.0]})})(),
        "generate_weekly_digest": lambda **kwargs: FakeDigest(),
        "fit_regime_model": lambda ticker, market_frame: FakeRegime(ticker, "Bear" if ticker == "SOXX" else "Bull"),
        "configured_frontier_model": lambda provider="auto": f"OpenAI: {provider}",
        "get_current_holding_tickers": lambda db_path: ["NVDA", "AVGO"],
        "get_current_holding_tickers_grouped": lambda db_path: {"Personal": ["NVDA"], "Trust": ["AVGO"]},
        "get_investor_db_path": lambda: "/tmp/investor.db",
        "get_portfolio_positions": lambda db_path, tickers=None, account_id=None: [],
        "get_portfolio_tickers": lambda db_path: ["NVDA", "AVGO", "TSM"],
        "get_tax_assumptions": lambda db_path: {},
        "get_wash_sale_risk": lambda db_path, ticker: "NONE",
        "positions_by_ticker_and_account": lambda positions: {"NVDA": [], "AVGO": []},
        "build_composite_signal": lambda *args, **kwargs: type("Composite", (), {"composite_action": "Buy"})(),
        "compute_technicals": lambda *args, **kwargs: pd.DataFrame({"rsi_14": [45, 50], "bb_pct": [0.4, 0.5], "macd_histogram": [0.1, 0.2]}),
        "confidence_trajectory": lambda *args, **kwargs: type("Trajectory", (), {"trend": "rising"})(),
        "forward_regime_curve": lambda *args, **kwargs: pd.DataFrame({"day": [1, 2], "p_bull": [0.7, 0.72], "p_neutral": [0.2, 0.18], "p_bear": [0.1, 0.1]}),
        "intra_regime_signal": lambda *args, **kwargs: "Buy the dip",
        "sentiment_momentum": lambda *args, **kwargs: (
            type("Sentiment", (), {"trend": "flat"})(),
            pd.DataFrame({"recorded_at": [], "score": []}),
        ),
        "signal_from_forward_curve": lambda *args, **kwargs: type("Signal", (), {"action": "Buy"})(),
        "tax_adjusted_signals": lambda *args, **kwargs: [FakeTaxSignal()],
    }


def _client(monkeypatch, runtime: dict | None = None) -> TestClient:
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: ((runtime or _base_runtime()), None))
    monkeypatch.setattr(regime_route, "get_current_tickers_by_scope", lambda session, scope, account_id=None: ["NVDA", "AVGO"] if scope == "household" and account_id is None else ["NVDA"] if scope == "personal" or account_id == 101 else ["AVGO"])
    monkeypatch.setattr(
        regime_route,
        "get_available_portfolio_scopes",
        lambda session: [
            {"value": "household", "label": "All Portfolios", "ticker_count": 2, "accounts": [{"id": 101, "name": "RJ-Taxable", "ticker_count": 1, "has_holdings": True}, {"id": 202, "name": "Chase-2138", "ticker_count": 1, "has_holdings": True}]},
            {"value": "personal", "label": "Personal", "ticker_count": 1, "accounts": [{"id": 101, "name": "RJ-Taxable", "ticker_count": 1, "has_holdings": True}]},
            {"value": "trust", "label": "Trust", "ticker_count": 1, "accounts": [{"id": 202, "name": "Chase-2138", "ticker_count": 1, "has_holdings": True}]},
        ],
    )
    regime_route._JOBS.clear()
    app = FastAPI()
    app.include_router(regime_route.router)
    app.dependency_overrides[regime_route.require_actor] = lambda: "tester"
    app.dependency_overrides[regime_route.db_session] = lambda: iter([None])
    return TestClient(app)


def test_adapter_fix_uses_external_holding_snapshot_source(tmp_path) -> None:
    db_path = tmp_path / "investor.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute("CREATE TABLE external_connections (id INTEGER PRIMARY KEY, status TEXT)")
        conn.execute("CREATE TABLE external_holding_snapshots (id INTEGER PRIMARY KEY, connection_id INTEGER, as_of TEXT, payload_json TEXT)")
        conn.execute("CREATE TABLE external_account_maps (id INTEGER PRIMARY KEY, connection_id INTEGER, provider_account_id TEXT, account_id INTEGER)")
        conn.execute("CREATE TABLE accounts (id INTEGER PRIMARY KEY, taxpayer_entity_id INTEGER)")
        conn.execute("CREATE TABLE taxpayer_entities (id INTEGER PRIMARY KEY, type TEXT)")
        conn.execute("CREATE TABLE position_lots (id INTEGER PRIMARY KEY, ticker TEXT, qty REAL)")
        conn.execute("INSERT INTO external_connections (id, status) VALUES (1, 'ACTIVE')")
        conn.execute("INSERT INTO taxpayer_entities (id, type) VALUES (10, 'PERSONAL'), (20, 'TRUST')")
        conn.execute("INSERT INTO accounts (id, taxpayer_entity_id) VALUES (100, 10), (200, 20)")
        conn.execute(
            "INSERT INTO external_account_maps (connection_id, provider_account_id, account_id) VALUES (1, 'acct-personal', 100), (1, 'acct-trust', 200)"
        )
        conn.execute(
            "INSERT INTO external_holding_snapshots (connection_id, as_of, payload_json) VALUES (?, ?, ?)",
            (
                1,
                "2026-03-23T12:00:00+00:00",
                '{"items":[{"symbol":"NVDA","provider_account_id":"acct-personal"},{"symbol":"AVGO","provider_account_id":"acct-trust"},{"symbol":"CASH:USD","provider_account_id":"acct-personal"},{"symbol":"OLD","provider_account_id":"acct-personal","is_total":true}]}',
            ),
        )
        conn.execute("INSERT INTO position_lots (ticker, qty) VALUES ('HIST', 5.0)")
    grouped = investor_adapter.get_current_holding_tickers_grouped(str(db_path))
    assert grouped == {"Personal": ["NVDA"], "Trust": ["AVGO"]}
    assert investor_adapter.get_current_holding_tickers(str(db_path)) == ["AVGO", "NVDA"]
    assert "HIST" not in investor_adapter.get_current_holding_tickers(str(db_path))


def test_grouped_holdings_endpoint(monkeypatch) -> None:
    client = _client(monkeypatch)
    response = client.get("/regime/holdings")
    assert response.status_code == 200
    payload = response.json()
    assert payload["tickers"] == ["NVDA", "AVGO"]
    assert payload["groups"] == {"Current Holdings": ["NVDA", "AVGO"]}


def test_grouped_holdings_fallback_behavior(monkeypatch) -> None:
    client = _client(monkeypatch)
    response = client.get("/regime/holdings")
    assert response.status_code == 200
    payload = response.json()
    assert payload["groups"] == {"Current Holdings": ["NVDA", "AVGO"]}


def test_exposure_kpi_structure() -> None:
    exposure, total_market_value = regime_route._compute_regime_exposure(
        [
            {"ticker": "NVDA", "regime": "Bull", "market_value": 75_000.0},
            {"ticker": "AVGO", "regime": "Neutral", "market_value": 25_000.0},
        ]
    )
    assert total_market_value == 100_000.0
    assert set(exposure) == {"Bull", "Neutral", "Bear"}
    assert exposure["Bull"] == 0.75
    assert exposure["Neutral"] == 0.25
    assert exposure["Bear"] == 0.0


def test_empty_sentiment_payload_and_non_material_tax(monkeypatch) -> None:
    runtime = _base_runtime()

    class TaxSignal:
        account_name = "Brokerage"
        account_type = "TAXABLE"
        adjusted_action = "Hold"
        original_action = "Hold"
        tax_note = "No material tax adjustment"
        ltcg_threshold_date = None
        estimated_tax_impact = 0.0
        wash_sale_warning = None

    runtime["tax_adjusted_signals"] = lambda *args, **kwargs: [TaxSignal()]
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (runtime, None))
    payload = regime_route._build_regime_dashboard_payload(benchmark="SOXX", period="3y", tickers=["NVDA"])
    row = payload["rows"][0]
    assert row["sentiment_history_json"] == "[]"
    assert row["action"] == "—"
    assert row["account_tax_signals"] == []
