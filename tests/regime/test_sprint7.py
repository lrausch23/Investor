from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from fastapi import FastAPI
from fastapi.testclient import TestClient
from starlette.requests import Request


INVESTOR_ROOT = Path("/Volumes/T9/Projects/Dev/Investor")
if str(INVESTOR_ROOT) not in sys.path:
    sys.path.insert(0, str(INVESTOR_ROOT))

from src.app.routes import regime as regime_route
from _fixtures import FakeRegime


def _request() -> Request:
    return Request({"type": "http", "method": "GET", "path": "/regime", "headers": []})


def _fake_runtime() -> dict:
    class FakeTaxSignal:
        def __init__(self, account_name: str, account_type: str, adjusted_action: str, tax_note: str):
            self.account_name = account_name
            self.account_type = account_type
            self.adjusted_action = adjusted_action
            self.original_action = adjusted_action
            self.tax_note = tax_note
            self.ltcg_threshold_date = None
            self.estimated_tax_impact = 0.0
            self.wash_sale_warning = None

    class FakeDigest:
        def __init__(self):
            self.generated_at = "2026-03-22T12:00:00+00:00"
            self.benchmark_regime = "Bear"
            self.entries = [
                {
                    "ticker": "NVDA",
                    "current_regime": "Bull",
                    "regime_changed_this_week": False,
                    "composite_action": "Buy",
                    "sentiment_trend": "improving",
                    "tax_note": "Brokerage: Hold (Wait for LTCG.)",
                    "priority": "WATCH",
                }
            ]
            self.regime_changes = ["NVDA: Bear → Bull on 2026-03-20"]
            self.sentiment_divergences = []
            self.tax_alerts = []
            self.action_items = ["NVDA: WATCH — Buy in Bull regime"]

    def download_market_frame(*, ticker: str, period: str, interval: str):
        return type(
            "MarketSeries",
            (),
            {
                "frame": pd.DataFrame(
                    {
                        "price": [100.0, 101.0, 102.0, 103.0],
                        "volume": [1_000_000, 1_050_000, 1_060_000, 1_070_000],
                        "high": [101.0, 102.0, 103.0, 104.0],
                        "low": [99.0, 100.0, 101.0, 102.0],
                    }
                )
            },
        )()

    return {
        "DEFAULT_TICKERS": ["NVDA"],
        "download_market_frame": download_market_frame,
        "generate_weekly_digest": lambda **kwargs: FakeDigest(),
        "fit_regime_model": lambda ticker, market_frame: FakeRegime(ticker, "Bear" if ticker == "SOXX" else "Bull"),
        "get_investor_db_path": lambda: "/tmp/investor.db",
        "get_portfolio_positions": lambda db_path, tickers=None, account_id=None: [],
        "get_portfolio_tickers": lambda db_path: ["NVDA"],
        "get_portfolio_tickers_filtered": lambda db_path: ["NVDA"],
        "get_tax_assumptions": lambda db_path: {},
        "get_wash_sale_risk": lambda db_path, ticker: "NONE",
        "positions_by_ticker_and_account": lambda positions: {"NVDA": [type("Pos", (), {"account_name": "Brokerage", "account_type": "TAXABLE"})()]},
        "build_composite_signal": lambda *args, **kwargs: type("Composite", (), {"composite_action": "Buy"})(),
        "compute_technicals": lambda *args, **kwargs: pd.DataFrame({"rsi_14": [45, 50], "bb_pct": [0.4, 0.5], "macd_histogram": [0.1, 0.2]}),
        "confidence_trajectory": lambda *args, **kwargs: type("Trajectory", (), {"trend": "rising"})(),
        "forward_regime_curve": lambda *args, **kwargs: pd.DataFrame(
            {"day": [1, 2], "p_bull": [0.7, 0.72], "p_neutral": [0.2, 0.18], "p_bear": [0.1, 0.10]}
        ),
        "intra_regime_signal": lambda *args, **kwargs: "Buy the dip",
        "sentiment_momentum": lambda *args, **kwargs: (
            type("Sentiment", (), {"trend": "improving"})(),
            pd.DataFrame({"recorded_at": ["2026-03-21", "2026-03-22"], "score": [2, 3]}),
        ),
        "signal_from_forward_curve": lambda *args, **kwargs: type("Signal", (), {"action": "Buy"})(),
        "tax_adjusted_signals": lambda *args, **kwargs: [FakeTaxSignal("Brokerage", "TAXABLE", "Hold", "Wait for LTCG.")],
    }


def test_regime_route_import() -> None:
    assert regime_route.router.prefix == "/regime"


def test_regime_route_no_hmm_graceful(monkeypatch) -> None:
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (None, "missing hmm"))
    context = regime_route.build_regime_page_context(_request(), session=None, actor="tester")
    assert context["hmm_available"] is False
    assert "missing hmm" in context["warnings"][0]


def test_forward_curve_json_serialization() -> None:
    curve = pd.DataFrame(
        {
            "day": np.array([1, 2], dtype=np.int64),
            "p_bull": np.array([0.7, 0.8], dtype=np.float64),
            "p_neutral": np.array([0.2, 0.1], dtype=np.float64),
            "p_bear": np.array([0.1, 0.1], dtype=np.float64),
        }
    )
    payload = regime_route._json_ready(curve)
    rendered = json.dumps(payload)
    assert '"day": 1' in rendered
    assert '"p_bull": 0.7' in rendered


def test_regime_payload_structure(monkeypatch) -> None:
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (_fake_runtime(), None))
    context = regime_route.build_regime_page_context(_request(), session=None, actor="tester")
    assert context["benchmark"] == "SOXX"
    assert context["period"] == "3y"
    assert context["hmm_available"] is True
    assert "regime_config_json" in context


def test_digest_endpoint_json_format(monkeypatch) -> None:
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (_fake_runtime(), None))
    app = FastAPI()
    app.include_router(regime_route.router)
    app.dependency_overrides[regime_route.require_actor] = lambda: "tester"
    app.dependency_overrides[regime_route.db_session] = lambda: iter([None])
    client = TestClient(app)
    response = client.get("/regime/digest?format=json")
    assert response.status_code == 200
    payload = response.json()
    assert "entries" in payload
    assert "action_items" in payload
