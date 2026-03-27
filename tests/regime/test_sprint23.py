from __future__ import annotations

import datetime as dt

from src.app.routes import regime as regime_route
from src.app.routes import regime_cache


def test_extract_ai_verdict_prefers_display_verdict() -> None:
    frontier = {"display_verdict": "Hold — low confidence", "institutional_report": {"verdict": "Buy"}}
    assert regime_route._extract_ai_verdict(frontier) == "Hold — low confidence"


def test_extract_ai_verdict_falls_back_to_institutional_report() -> None:
    frontier = {"institutional_report": {"verdict": "Buy"}}
    assert regime_route._extract_ai_verdict(frontier) == "Buy"


def test_stop_proximity_warning_band() -> None:
    row = {"current_price": 103.0, "price_targets": {"stop_price": 100.0, "current_price": 103.0}}
    result = regime_route._stop_proximity(row)
    assert result is not None
    assert result["level"] == "critical"


def test_stop_proximity_safe_band() -> None:
    row = {"current_price": 120.0, "price_targets": {"stop_price": 100.0, "current_price": 120.0}}
    result = regime_route._stop_proximity(row)
    assert result is not None
    assert result["level"] == "safe"


def test_compute_run_diff_detects_regime_signal_and_stop_changes() -> None:
    previous = {
        "rows": [
            {
                "ticker": "NVDA",
                "regime": "Bull",
                "composite_signal": "Buy",
                "stop_proximity": {"level": "safe"},
                "frontier": {"display_verdict": "Buy"},
            }
        ]
    }
    current = {
        "rows": [
            {
                "ticker": "NVDA",
                "regime": "Bear",
                "composite_signal": "Sell",
                "stop_proximity": {"level": "critical"},
                "frontier": {"display_verdict": "Hold"},
            }
        ]
    }
    diff = regime_route._compute_run_diff(current, previous)
    assert diff["has_previous"] is True
    assert len(diff["changes"]) == 4


def test_compute_run_diff_handles_missing_previous() -> None:
    diff = regime_route._compute_run_diff({"rows": []}, None)
    assert diff["has_previous"] is False


def test_archive_and_load_previous_payload(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(regime_cache, "_CACHE_ROOT", tmp_path)
    regime_cache.save_payload({"rows": [{"ticker": "NVDA"}]})
    archived = regime_cache.archive_previous_payload()
    assert archived is not None and archived.exists()
    loaded = regime_cache.load_previous_payload()
    assert loaded is not None
    assert loaded["rows"][0]["ticker"] == "NVDA"


def test_archive_previous_payload_ignores_invalid_cache(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(regime_cache, "_CACHE_ROOT", tmp_path)
    regime_cache.last_run_path().write_text("{not json", encoding="utf-8")
    assert regime_cache.archive_previous_payload() is None
    assert regime_cache.load_previous_payload() is None


def test_run_analysis_archives_before_save(monkeypatch) -> None:
    order: list[str] = []
    job = regime_route.RegimeJob(
        job_id="job-1",
        status="pending",
        tickers=["NVDA"],
        benchmark="SOXX",
        period="3y",
        progress=0,
        total=1,
        payload=None,
        error=None,
        created_at=dt.datetime.now(dt.timezone.utc),
    )
    regime_route._JOBS["job-1"] = job
    monkeypatch.setattr(regime_route, "get_session", lambda: None)
    monkeypatch.setattr(
        regime_route,
        "_build_regime_dashboard_payload",
        lambda **kwargs: {"rows": [{"ticker": "NVDA"}], "selected_tickers": ["NVDA"]},
    )
    monkeypatch.setattr(regime_route, "archive_previous_payload", lambda: order.append("archive"))
    monkeypatch.setattr(regime_route, "save_payload", lambda payload: order.append("save"))
    regime_route._run_analysis("job-1")
    assert order == ["archive", "save"]

