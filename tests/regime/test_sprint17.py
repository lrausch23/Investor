from __future__ import annotations

import datetime as dt
import sys
from pathlib import Path
from types import SimpleNamespace

HMM_ROOT = Path("/Volumes/T9/Projects/Dev/HMM")
if str(HMM_ROOT) not in sys.path:
    sys.path.insert(0, str(HMM_ROOT))

import pandas as pd

from src.regime import persistence
from src.regime.alerts import RegimeAlert, SignalAlert, format_alert_summary
from src.regime.portfolio import compute_correlation_risk, portfolio_risk_summary_dict
from src.regime.scheduled_runner import run_scheduled_regime_checks


def test_transition_outcome_columns_and_journal(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(persistence, "DB_PATH", tmp_path / "regime_watch.db")
    change_id = persistence.save_regime_change_with_price("NVDA", "Neutral", "Bull", 0, 100.0)
    persistence.update_transition_outcome(change_id, return_5d=0.05, return_10d=0.08, return_21d=0.1)
    rows = persistence.get_transition_journal("NVDA", limit=5)
    assert rows[0]["price_at_change"] == 100.0
    assert rows[0]["return_21d"] == 0.1


def test_pending_transition_outcomes_returns_mature_rows(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(persistence, "DB_PATH", tmp_path / "regime_watch.db")
    old_changed_at = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=30)).isoformat()
    with persistence._connect() as conn:  # type: ignore[attr-defined]
        conn.execute(
            """
            INSERT INTO regime_change_history (
                ticker, previous_label, current_label, current_state_id, changed_at, price_at_change
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            ("AVGO", "Bull", "Bear", 2, old_changed_at, 200.0),
        )
    pending = persistence.get_pending_transition_outcomes()
    assert pending
    assert pending[0]["ticker"] == "AVGO"


def test_transition_statistics_aggregate_rows(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(persistence, "DB_PATH", tmp_path / "regime_watch.db")
    change_id = persistence.save_regime_change_with_price("TSM", "Bear", "Bull", 0, 90.0)
    persistence.update_transition_outcome(change_id, return_5d=0.04, return_10d=0.06, return_21d=0.09)
    stats = persistence.get_transition_statistics()
    assert stats["rows"][0]["transition"] == "Bear→Bull"


def test_portfolio_summary_reports_sector_and_diversification() -> None:
    positions = [
        SimpleNamespace(ticker="NVDA", market_value=75_000.0),
        SimpleNamespace(ticker="AVGO", market_value=25_000.0),
    ]
    results = {
        "NVDA": {"label": "Bull", "transition_risk": 0.04, "composite_action": "Buy", "sector": "Semiconductors"},
        "AVGO": {"label": "Bear", "transition_risk": 0.12, "composite_action": "Hold", "sector": "Semiconductors"},
    }
    summary = portfolio_risk_summary_dict(positions, results)
    assert summary["regime_exposure"]["bull_pct"] == 0.75
    assert summary["sector_concentration"][0]["sector"] == "Semiconductors"
    assert 0 <= summary["diversification_score"] <= 1


def test_correlation_risk_flags_concentration() -> None:
    risk = compute_correlation_risk(
        {
            "A": {"label": "Bull"},
            "B": {"label": "Bull"},
            "C": {"label": "Bull"},
            "D": {"label": "Neutral"},
        }
    )
    assert risk["dominant_regime"] == "Bull"
    assert risk["warning"] == "High regime correlation"


def test_scheduled_runner_formats_alert_summary(monkeypatch) -> None:
    monkeypatch.setattr("src.regime.scheduled_runner.get_investor_db_path", lambda: "/tmp/investor.db")
    monkeypatch.setattr("src.regime.scheduled_runner.get_portfolio_tickers_filtered", lambda db_path: ["NVDA"])
    monkeypatch.setattr(
        "src.regime.scheduled_runner.check_regime_changes",
        lambda tickers, db_path: [RegimeAlert("NVDA", "Neutral", "Bull", 0.08, "Buy", None, "2026-03-24T12:00:00+00:00")],
    )
    monkeypatch.setattr("src.regime.scheduled_runner.check_transition_risk_spikes", lambda tickers: [])
    monkeypatch.setattr(
        "src.regime.scheduled_runner.check_signal_changes",
        lambda tickers: [SignalAlert("NVDA", "Hold", "Buy", "2026-03-24T12:00:00+00:00")],
    )
    monkeypatch.setattr("src.regime.scheduled_runner.check_stop_proximity", lambda tickers, db_path: [])
    monkeypatch.setattr("src.regime.scheduled_runner.get_pending_transition_outcomes", lambda: [])
    monkeypatch.setattr("src.regime.scheduled_runner.get_latest_prices", lambda db_path, tickers: {})
    result = run_scheduled_regime_checks()
    assert "NVDA" in result["summary"]


def test_alert_summary_formats_multiple_alert_types() -> None:
    summary = format_alert_summary(
        [
            RegimeAlert("NVDA", "Neutral", "Bull", 0.08, "Buy", None, "2026-03-24T12:00:00+00:00"),
            SignalAlert("NVDA", "Hold", "Buy", "2026-03-24T12:00:00+00:00"),
        ]
    )
    assert "Neutral -> Bull" in summary
    assert "signal changed" in summary
