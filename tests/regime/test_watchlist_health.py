from __future__ import annotations

import importlib
from pathlib import Path

import pytest


@pytest.fixture
def health_modules(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("HMM_DATA_DIR", str(tmp_path))
    import src.regime.persistence as store
    import src.regime.watchlist_health as health

    store = importlib.reload(store)
    store.DB_PATH = tmp_path / "regime_watch.db"
    health = importlib.reload(health)
    return store, health


def test_watchlist_health_marks_stale_entry_signal_blocked(health_modules, monkeypatch: pytest.MonkeyPatch) -> None:
    store, health = health_modules
    theme = store.create_theme("AI Enablers", conviction=3)
    candidate = store.upsert_watchlist_candidate(
        theme["id"],
        "ARQQ",
        company_name="Arista Networks",
        suggested_entry_price=10.0,
        suggested_stop_price=9.0,
        regime_label="Bull",
        regime_probability=0.95,
        status="Entry Signal",
    )
    old_ts = "2026-01-01T14:30:00+00:00"
    with store._connect() as conn:
        conn.execute(
            "UPDATE discovery_watchlist SET last_scanned_at = ?, entry_signal_at = ? WHERE id = ?",
            (old_ts, old_ts, int(candidate["id"])),
        )

    monkeypatch.setattr(health, "_batch_current_prices", lambda tickers: {"ARQQ": 11.5})

    rows, stats = health.annotate_watchlist_signal_health(store.get_watchlist())
    row = next(item for item in rows if item["ticker"] == "ARQQ")

    assert stats["blocked"] == 1
    assert stats["stale"] == 1
    assert row["signal_health"]["actionable"] is False
    assert row["signal_health"]["is_stale"] is True
    assert "Signal is stale" in row["signal_health"]["reason"]


def test_watchlist_health_marks_fresh_entry_signal_ready(health_modules, monkeypatch: pytest.MonkeyPatch) -> None:
    store, health = health_modules
    theme = store.create_theme("AI Enablers", conviction=3)
    store.upsert_watchlist_candidate(
        theme["id"],
        "AVT",
        company_name="Avnet",
        suggested_entry_price=100.0,
        suggested_stop_price=90.0,
        regime_label="Bull",
        regime_probability=0.95,
        status="Entry Signal",
    )

    monkeypatch.setattr(health, "_batch_current_prices", lambda tickers: {"AVT": 101.0})

    rows, stats = health.annotate_watchlist_signal_health(store.get_watchlist())
    row = next(item for item in rows if item["ticker"] == "AVT")

    assert stats["ready"] == 1
    assert row["signal_health"]["actionable"] is True
    assert row["signal_health"]["grade"] == "actionable"
