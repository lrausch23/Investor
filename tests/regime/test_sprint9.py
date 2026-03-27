from __future__ import annotations

import json
import os
import sqlite3
import sys
from pathlib import Path

from starlette.requests import Request


INVESTOR_ROOT = Path("/Volumes/T9/Projects/Dev/Investor")
if str(INVESTOR_ROOT) not in sys.path:
    sys.path.insert(0, str(INVESTOR_ROOT))

from src.app.routes import regime as regime_route
from src.app.routes import regime_cache


def test_compute_regime_exposure_weighted() -> None:
    exposure, total_market_value = regime_route._compute_regime_exposure(
        [
            {"ticker": "NVDA", "regime": "Bull", "market_value": 75_000.0},
            {"ticker": "AVGO", "regime": "Bear", "market_value": 25_000.0},
        ]
    )
    assert total_market_value == 100_000.0
    assert exposure["Bull"] == 0.75
    assert exposure["Bear"] == 0.25


def test_fetch_regime_change_history_reads_sqlite(tmp_path, monkeypatch) -> None:
    data_dir = tmp_path / "hmm_data"
    data_dir.mkdir()
    db_path = data_dir / "regime_watch.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE regime_change_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                previous_label TEXT,
                current_label TEXT NOT NULL,
                current_state_id INTEGER NOT NULL,
                changed_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            INSERT INTO regime_change_history (ticker, previous_label, current_label, current_state_id, changed_at)
            VALUES ('NVDA', 'Neutral', 'Bull', 0, '2026-03-20T12:00:00+00:00')
            """
        )
    monkeypatch.setenv("HMM_DATA_DIR", str(data_dir))
    history = regime_route._fetch_regime_change_history(["NVDA"], days=90)
    assert history[0]["ticker"] == "NVDA"
    assert history[0]["current_label"] == "Bull"


def test_regime_cache_round_trip(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(regime_cache, "_CACHE_ROOT", tmp_path / "regime_cache")
    payload = {"rows": [{"ticker": "NVDA", "regime": "Bull"}], "last_run_display": "2026-03-23 09:00:00 EDT"}
    regime_cache.save_payload(payload)
    loaded = regime_cache.load_payload()
    assert loaded == {**payload, "cache_version": regime_cache._CACHE_VERSION}


def test_regime_cache_rejects_unversioned_payload(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(regime_cache, "_CACHE_ROOT", tmp_path / "regime_cache")
    path = regime_cache.last_run_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text('{"rows":[{"ticker":"NVDA"}]}', encoding="utf-8")
    assert regime_cache.load_payload() is None


def test_regime_qualitative_cache_round_trip(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(regime_cache, "_CACHE_ROOT", tmp_path / "regime_cache")
    regime_cache.save_qualitative_cache("NVDA", provider="auto", data={"ticker": "NVDA", "sentiment": "Positive"})
    cached = regime_cache.load_qualitative_cache("NVDA", provider="auto")
    assert cached == {"ticker": "NVDA", "sentiment": "Positive"}


def test_shell_context_uses_cached_payload_and_portfolios(monkeypatch) -> None:
    monkeypatch.setattr(regime_route, "load_payload", lambda: {"rows": [{"ticker": "NVDA", "regime": "Bull"}], "last_run_display": "2026-03-23 09:00:00 EDT", "warnings": []})
    monkeypatch.setattr(regime_route, "_load_hmm_runtime", lambda: (None, "missing hmm"))
    monkeypatch.setattr(regime_route, "get_available_portfolio_scopes", lambda session: [{"value": "household", "label": "All Portfolios", "ticker_count": 2}])
    request = Request({"type": "http", "method": "GET", "path": "/regime", "headers": []})
    context = regime_route.build_regime_page_context(request, session=object(), actor="tester")
    assert "regime_config_json" in context
    assert "Showing cached results from 2026-03-23 09:00:00 EDT." in context["regime_config_json"]
    assert "All Portfolios" in context["regime_config_json"]
