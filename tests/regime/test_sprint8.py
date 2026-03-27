from __future__ import annotations

import datetime as dt
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


def _fake_runtime() -> dict:
    class FakeTaxSignal:
        def __init__(self):
            self.account_name = "Brokerage"
            self.account_type = "TAXABLE"
            self.adjusted_action = "Hold"
            self.original_action = "Hold"
            self.tax_note = "Wait for LTCG."
            self.ltcg_threshold_date = None
            self.estimated_tax_impact = 0.0
            self.wash_sale_warning = None

    class FakeDigest:
        def __init__(self):
            self.generated_at = "2026-03-22T12:00:00+00:00"
            self.benchmark_regime = "Bear"
            self.entries = []
            self.regime_changes = []
            self.sentiment_divergences = []
            self.tax_alerts = []
            self.action_items = []

    return {
        "DEFAULT_TICKERS": ["NVDA"],
        "download_market_frame": lambda **kwargs: type("MarketSeries", (), {"frame": pd.DataFrame({"price": [100.0, 101.0], "volume": [1_000_000, 1_050_000], "high": [101.0, 102.0], "low": [99.0, 100.0]})})(),
        "generate_weekly_digest": lambda **kwargs: FakeDigest(),
        "fit_regime_model": lambda ticker, market_frame: FakeRegime(ticker, "Bear" if ticker == "SOXX" else "Bull"),
        "configured_frontier_model": lambda provider="auto": f"OpenAI: {provider}",
        "get_investor_db_path": lambda: "/tmp/investor.db",
        "get_portfolio_positions": lambda db_path, tickers=None, account_id=None: [],
        "get_portfolio_tickers": lambda db_path: ["NVDA", "AVGO", "TSM"],
        "get_tax_assumptions": lambda db_path: {},
        "get_wash_sale_risk": lambda db_path, ticker: "NONE",
        "positions_by_ticker_and_account": lambda positions: {"NVDA": [object()], "AVGO": [object()]},
        "build_composite_signal": lambda *args, **kwargs: type("Composite", (), {"composite_action": "Buy"})(),
        "compute_technicals": lambda *args, **kwargs: pd.DataFrame({"rsi_14": [45, 50], "bb_pct": [0.4, 0.5], "macd_histogram": [0.1, 0.2]}),
        "confidence_trajectory": lambda *args, **kwargs: type("Trajectory", (), {"trend": "rising"})(),
        "forward_regime_curve": lambda *args, **kwargs: pd.DataFrame({"day": [1, 2], "p_bull": [0.7, 0.72], "p_neutral": [0.2, 0.18], "p_bear": [0.1, 0.1]}),
        "intra_regime_signal": lambda *args, **kwargs: "Buy the dip",
        "sentiment_momentum": lambda *args, **kwargs: (type("Sentiment", (), {"trend": "improving"})(), pd.DataFrame({"recorded_at": ["2026-03-21"], "score": [2]})),
        "signal_from_forward_curve": lambda *args, **kwargs: type("Signal", (), {"action": "Buy"})(),
        "tax_adjusted_signals": lambda *args, **kwargs: [FakeTaxSignal()],
    }


class _ImmediateExecutor:
    def submit(self, fn, *args, **kwargs):
        fn(*args, **kwargs)
        return type("DoneFuture", (), {"result": lambda self: None})()


def _client(monkeypatch) -> TestClient:
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (_fake_runtime(), None))
    monkeypatch.setattr(regime_route, "_EXECUTOR", _ImmediateExecutor())
    monkeypatch.setattr(regime_route, "get_current_tickers_by_scope", lambda session, scope, account_id=None: ["NVDA", "AVGO"])
    monkeypatch.setattr(regime_route, "get_available_portfolio_scopes", lambda session: [{"value": "household", "label": "All Portfolios", "ticker_count": 2, "accounts": [{"id": 101, "name": "RJ-Taxable", "ticker_count": 2, "has_holdings": True}]}])
    regime_route._JOBS.clear()
    app = FastAPI()
    app.include_router(regime_route.router)
    app.dependency_overrides[regime_route.require_actor] = lambda: "tester"
    app.dependency_overrides[regime_route.db_session] = lambda: iter([None])
    return TestClient(app)


def test_max_ticker_validation(monkeypatch) -> None:
    client = _client(monkeypatch)
    response = client.post("/regime/run", data={"tickers": ",".join(f"T{i}" for i in range(1, 52))})
    assert response.status_code == 422


def test_job_lifecycle(monkeypatch) -> None:
    client = _client(monkeypatch)
    response = client.post("/regime/run", data={"tickers": "NVDA,AVGO"})
    assert response.status_code == 200
    job_id = response.json()["job_id"]

    status = client.get(f"/regime/status/{job_id}")
    assert status.status_code == 200
    payload = status.json()
    assert payload["status"] == "done"
    assert payload["progress"] == 2
    assert payload["payload"]["selected_tickers"] == ["NVDA", "AVGO"]


def test_empty_ticker_rejection(monkeypatch) -> None:
    client = _client(monkeypatch)
    response = client.post("/regime/run", data={"tickers": ""})
    assert response.status_code == 422


def test_holdings_endpoint(monkeypatch) -> None:
    client = _client(monkeypatch)
    response = client.get("/regime/holdings")
    assert response.status_code == 200
    assert response.json()["tickers"] == ["NVDA", "AVGO"]
    assert response.json()["groups"]["Current Holdings"] == ["NVDA", "AVGO"]


def test_job_ttl_pruning() -> None:
    regime_route._JOBS.clear()
    stale_job = regime_route.RegimeJob(
        job_id="stale",
        status="done",
        tickers=["NVDA"],
        benchmark="SOXX",
        period="3y",
        progress=1,
        total=1,
        payload={},
        error=None,
        created_at=dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=regime_route._JOB_TTL_SECONDS + 1),
    )
    fresh_job = regime_route.RegimeJob(
        job_id="fresh",
        status="done",
        tickers=["AVGO"],
        benchmark="SOXX",
        period="3y",
        progress=1,
        total=1,
        payload={},
        error=None,
        created_at=dt.datetime.now(dt.timezone.utc),
    )
    regime_route._JOBS["stale"] = stale_job
    regime_route._JOBS["fresh"] = fresh_job

    regime_route._prune_jobs(dt.datetime.now(dt.timezone.utc))

    assert "stale" not in regime_route._JOBS
    assert "fresh" in regime_route._JOBS
