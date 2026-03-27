from __future__ import annotations

import datetime as dt
import sys
from pathlib import Path

import pandas as pd
from fastapi import FastAPI
from fastapi.testclient import TestClient

HMM_ROOT = Path("/Volumes/T9/Projects/Dev/HMM")
if str(HMM_ROOT) not in sys.path:
    sys.path.insert(0, str(HMM_ROOT))

from src.regime import persistence
from src.regime.signals import CompositeSignal, SignalResult, compute_price_targets

INVESTOR_ROOT = Path("/Volumes/T9/Projects/Dev/Investor")
if str(INVESTOR_ROOT) not in sys.path:
    sys.path.insert(0, str(INVESTOR_ROOT))

from src.app.routes import regime as regime_route
from _fixtures import FakeRegime


def _composite(action: str, strength: float = 0.8) -> CompositeSignal:
    return CompositeSignal(
        regime_signal="Bull" if "Buy" in action else "Bear" if "Sell" in action else "Neutral",
        regime_probability=0.9,
        forward_signal=SignalResult(action=action, timeframe="short", strength=strength, expected_holding_days=10, rationale="test"),
        technical_signal="Buy the dip",
        composite_action=action,
        composite_strength=strength,
        short_term_view="short",
        medium_term_view="medium",
    )


def _technicals() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "bb_lower": [95.0, 96.0],
            "bb_upper": [110.0, 111.0],
            "atr_14": [2.0, 2.5],
        }
    )


def _runtime(*, pending_calls=None, updated=None) -> dict:
    return {
        "DEFAULT_TICKERS": ["NVDA"],
        "download_market_frame": lambda **kwargs: type("MarketSeries", (), {"frame": pd.DataFrame({"price": [100.0, 101.0], "volume": [1_000_000, 1_050_000], "high": [101.0, 102.0], "low": [99.0, 100.0]})})(),
        "generate_weekly_digest": lambda **kwargs: type("Digest", (), {"action_items": [], "entries": [], "regime_changes": [], "sentiment_divergences": [], "tax_alerts": [], "generated_at": "2026-03-24T12:00:00+00:00"})(),
        "fit_regime_model": lambda ticker, market_frame: FakeRegime(ticker, "Bear" if ticker == "SOXX" else "Bull", latest_price=123.0, recent_state_mean_return=0.01),
        "configured_frontier_model": lambda provider="auto": "OpenAI: gpt-4o-mini",
        "get_investor_db_path": lambda: "/tmp/investor.db",
        "get_latest_prices": lambda db_path, tickers: {"NVDA": 130.0},
        "get_pending_outcomes": (lambda: pending_calls) if pending_calls is not None else lambda: [],
        "get_signal_effectiveness": lambda: {"summary": {"1w": {"count": 1, "hit_rate": 1.0, "avg_return": 0.05}, "1m": {"count": 0, "hit_rate": None, "avg_return": None}, "3m": {"count": 0, "hit_rate": None, "avg_return": None}}, "by_action": {"1w": {"Buy": {"count": 1, "hit_rate": 1.0, "avg_return": 0.05}}, "1m": {}, "3m": {}}, "rows": []},
        "get_portfolio_positions": lambda db_path, tickers=None, account_id=None: [],
        "get_portfolio_tickers": lambda db_path: ["NVDA"],
        "get_tax_assumptions": lambda db_path: {},
        "get_wash_sale_risk": lambda db_path, ticker: "NONE",
        "positions_by_ticker_and_account": lambda positions: {"NVDA": []},
        "save_regime_event": lambda ticker, label, state_id: {"previous_label": "Neutral", "days_in_regime": 2},
        "save_signal_snapshot": lambda **kwargs: None,
        "update_signal_outcome": (lambda snapshot_id, interval, current_price: updated.append((snapshot_id, interval, current_price))) if updated is not None else (lambda snapshot_id, interval, current_price: None),
        "build_composite_signal": lambda *args, **kwargs: _composite("Buy", 0.82),
        "compute_price_targets": compute_price_targets,
        "compute_technicals": lambda *args, **kwargs: pd.DataFrame(
            {
                "rsi_14": [45, 50],
                "bb_pct": [0.4, 0.5],
                "macd_histogram": [0.1, 0.2],
                "bb_lower": [95.0, 96.0],
                "bb_upper": [110.0, 111.0],
                "atr_14": [2.0, 2.5],
            }
        ),
        "confidence_trajectory": lambda *args, **kwargs: type("Trajectory", (), {"trend": "rising"})(),
        "forward_regime_curve": lambda *args, **kwargs: pd.DataFrame({"day": [1, 2], "p_bull": [0.7, 0.72], "p_neutral": [0.2, 0.18], "p_bear": [0.1, 0.1]}),
        "intra_regime_signal": lambda *args, **kwargs: "Buy the dip",
        "sentiment_momentum": lambda *args, **kwargs: (type("Sentiment", (), {"trend": "improving"})(), pd.DataFrame({"recorded_at": ["2026-03-23"], "score": [1]})),
        "signal_from_forward_curve": lambda *args, **kwargs: type("Signal", (), {"action": "Buy"})(),
        "tax_adjusted_signals": lambda *args, **kwargs: [],
        "list_theses": lambda: [],
        "upsert_thesis": lambda ticker, thesis=None: None,
    }


def test_compute_price_targets_buy_signal() -> None:
    targets = compute_price_targets(
        current_price=100.0,
        technicals_df=_technicals(),
        composite_signal=_composite("Buy", 0.8),
        expected_duration=10,
        state_mean_return=0.01,
    )
    assert targets.entry_price == 96.0
    assert targets.stop_price == 91.0
    assert targets.exit_price > targets.entry_price
    assert targets.risk_reward_ratio is not None


def test_compute_price_targets_sell_signal() -> None:
    targets = compute_price_targets(
        current_price=100.0,
        technicals_df=_technicals(),
        composite_signal=_composite("Sell", 0.75),
        expected_duration=12,
        state_mean_return=-0.012,
    )
    assert targets.entry_price == 111.0
    assert targets.stop_price == 116.0
    assert targets.exit_price < targets.entry_price


def test_compute_price_targets_hold_signal() -> None:
    targets = compute_price_targets(
        current_price=100.0,
        technicals_df=_technicals(),
        composite_signal=_composite("Hold", 0.35),
        expected_duration=8,
        state_mean_return=0.0,
    )
    assert targets.entry_price is None
    assert targets.exit_price is None
    assert targets.stop_price == 96.25


def test_signal_snapshot_persistence_and_pending_outcomes(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(persistence, "DB_PATH", tmp_path / "regime_watch.db")
    persistence.save_signal_snapshot(
        ticker="NVDA",
        snapshot_date=(dt.date.today() - dt.timedelta(days=40)).isoformat(),
        action="Buy",
        regime_label="Bull",
        regime_probability=0.9,
        composite_strength=0.8,
        benchmark="SOXX",
        current_price=100.0,
        entry_price=96.0,
        exit_price=110.0,
        stop_price=91.0,
        risk_reward_ratio=2.8,
        timeframe_days=10,
    )
    pending = persistence.get_pending_outcomes(as_of=dt.datetime.now(dt.timezone.utc).isoformat())
    assert {row["interval"] for row in pending} >= {"1w", "1m"}


def test_signal_outcome_update_and_effectiveness(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(persistence, "DB_PATH", tmp_path / "regime_watch.db")
    snapshot_date = (dt.date.today() - dt.timedelta(days=100)).isoformat()
    persistence.save_signal_snapshot(
        ticker="NVDA",
        snapshot_date=snapshot_date,
        action="Buy",
        regime_label="Bull",
        regime_probability=0.9,
        composite_strength=0.8,
        benchmark="SOXX",
        current_price=100.0,
        entry_price=96.0,
        exit_price=110.0,
        stop_price=91.0,
        risk_reward_ratio=2.8,
        timeframe_days=10,
    )
    pending = persistence.get_pending_outcomes(as_of=dt.datetime.now(dt.timezone.utc).isoformat())
    for row in pending:
        persistence.update_signal_outcome(int(row["id"]), str(row["interval"]), 110.0)
    effectiveness = persistence.get_signal_effectiveness()
    assert effectiveness["summary"]["1w"]["hit_rate"] == 1.0
    assert effectiveness["summary"]["1w"]["avg_return"] == 0.1


def test_dashboard_payload_includes_price_targets_and_effectiveness(monkeypatch) -> None:
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (_runtime(), None))
    payload = regime_route._build_regime_dashboard_payload(benchmark="SOXX", period="3y", tickers=["NVDA"])
    row = payload["rows"][0]
    assert row["price_targets"]["entry_price"] == 96.0
    assert payload["signal_effectiveness"]["summary"]["1w"]["count"] == 1


def test_dashboard_build_updates_pending_outcomes(monkeypatch) -> None:
    updated: list[tuple[int, str, float]] = []
    pending = [{"id": 5, "ticker": "NVDA", "interval": "1w"}]
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (_runtime(pending_calls=pending, updated=updated), None))
    regime_route._build_regime_dashboard_payload(benchmark="SOXX", period="3y", tickers=["NVDA"])
    assert updated == [(5, "1w", 130.0)]


def test_effectiveness_endpoint_returns_summary(monkeypatch) -> None:
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (_runtime(), None))
    app = FastAPI()
    app.include_router(regime_route.router)
    app.dependency_overrides[regime_route.require_actor] = lambda: "tester"
    app.dependency_overrides[regime_route.db_session] = lambda: iter([None])
    client = TestClient(app)
    response = client.get("/regime/effectiveness")
    assert response.status_code == 200
    assert response.json()["summary"]["1w"]["count"] == 1
