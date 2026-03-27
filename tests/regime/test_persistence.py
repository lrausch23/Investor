from __future__ import annotations

from datetime import datetime, timedelta, timezone
import importlib
import sqlite3

import pytest

from src.regime import persistence


@pytest.fixture()
def temp_persistence(tmp_path, monkeypatch):
    monkeypatch.setenv("HMM_DATA_DIR", str(tmp_path))
    module = importlib.reload(persistence)
    monkeypatch.setattr(module, "DB_PATH", tmp_path / "regime_watch.db")
    return module


def test_connect_creates_database_file(temp_persistence) -> None:
    conn = temp_persistence._connect()
    conn.close()
    assert temp_persistence.DB_PATH.exists()


def test_connect_creates_core_tables(temp_persistence) -> None:
    with temp_persistence._connect() as conn:
        tables = {
            row["name"]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }
    assert "ticker_thesis" in tables
    assert "regime_events" in tables
    assert "regime_change_history" in tables
    assert "sentiment_history" in tables
    assert "signal_snapshots" in tables
    assert "sector_cache" in tables
    assert "earnings_cache" in tables


def test_transition_columns_exist(temp_persistence) -> None:
    with temp_persistence._connect() as conn:
        columns = {row["name"] for row in conn.execute("PRAGMA table_info(regime_change_history)").fetchall()}
    assert {"price_at_change", "return_5d", "return_10d", "return_21d", "outcome_updated_at"} <= columns


def test_ensure_column_is_idempotent(temp_persistence) -> None:
    with temp_persistence._connect() as conn:
        conn.execute("CREATE TABLE IF NOT EXISTS scratch (id INTEGER PRIMARY KEY)")
        temp_persistence._ensure_column(conn, "scratch", "flag", "TEXT")
        temp_persistence._ensure_column(conn, "scratch", "flag", "TEXT")
        columns = [row["name"] for row in conn.execute("PRAGMA table_info(scratch)").fetchall()]
    assert columns.count("flag") == 1


def test_upsert_thesis_returns_existing_on_none(temp_persistence) -> None:
    assert temp_persistence.upsert_thesis("nvda", "AI thesis") == "AI thesis"
    assert temp_persistence.upsert_thesis("nvda", None) == "AI thesis"


def test_list_theses_returns_saved_rows(temp_persistence) -> None:
    temp_persistence.upsert_thesis("NVDA", "AI thesis")
    temp_persistence.upsert_thesis("AVGO", "Networking thesis")
    rows = temp_persistence.list_theses()
    assert [row["ticker"] for row in rows] == ["AVGO", "NVDA"]


def test_delete_thesis_returns_true_when_row_deleted(temp_persistence) -> None:
    temp_persistence.upsert_thesis("NVDA", "AI thesis")
    assert temp_persistence.delete_thesis("NVDA") is True


def test_delete_thesis_returns_false_when_missing(temp_persistence) -> None:
    assert temp_persistence.delete_thesis("MISSING") is False


def test_save_regime_event_initial_has_no_previous_label(temp_persistence) -> None:
    result = temp_persistence.save_regime_event("NVDA", "Bull", 0)
    assert result["previous_label"] is None


def test_save_regime_event_update_tracks_previous_label(temp_persistence) -> None:
    temp_persistence.save_regime_event("NVDA", "Bull", 0)
    result = temp_persistence.save_regime_event("NVDA", "Bear", 2)
    assert result["previous_label"] == "Bull"


def test_save_regime_event_same_state_does_not_duplicate_history(temp_persistence) -> None:
    temp_persistence.save_regime_event("NVDA", "Bull", 0)
    temp_persistence.save_regime_event("NVDA", "Bull", 0)
    with temp_persistence._connect() as conn:
        count = conn.execute("SELECT COUNT(*) AS n FROM regime_change_history").fetchone()["n"]
    assert count == 0


def test_save_regime_change_with_price_persists_row(temp_persistence) -> None:
    change_id = temp_persistence.save_regime_change_with_price("NVDA", "Bull", "Bear", 2, 123.45)
    rows = temp_persistence.get_transition_journal("NVDA")
    assert change_id >= 1
    assert rows[0]["price_at_change"] == pytest.approx(123.45)


def test_update_transition_outcome_sets_returns(temp_persistence) -> None:
    change_id = temp_persistence.save_regime_change_with_price("NVDA", "Bull", "Bear", 2, 123.45)
    temp_persistence.update_transition_outcome(change_id, return_5d=-0.02, return_10d=-0.03)
    row = temp_persistence.get_transition_journal("NVDA")[0]
    assert row["return_5d"] == pytest.approx(-0.02)
    assert row["return_10d"] == pytest.approx(-0.03)


def test_get_transition_statistics_returns_transition_label(temp_persistence) -> None:
    change_id = temp_persistence.save_regime_change_with_price("NVDA", "Bull", "Bear", 2, 123.45)
    temp_persistence.update_transition_outcome(change_id, return_5d=-0.02, return_10d=-0.03, return_21d=-0.05)
    stats = temp_persistence.get_transition_statistics()
    assert stats["rows"][0]["transition"] == "Bull→Bear"


def test_save_sentiment_and_history(temp_persistence) -> None:
    temp_persistence.save_sentiment("NVDA", 4, "Positive", 3)
    history = temp_persistence.get_sentiment_history("NVDA")
    assert history[0]["score"] == 4


def test_save_signal_snapshot_and_pending_outcomes(temp_persistence) -> None:
    snapshot_date = (datetime.now(timezone.utc) - timedelta(days=31)).date().isoformat()
    temp_persistence.save_signal_snapshot(
        ticker="NVDA",
        snapshot_date=snapshot_date,
        action="Buy",
        regime_label="Bull",
        regime_probability=0.82,
        composite_strength=0.77,
        benchmark="SOXX",
        current_price=100.0,
        entry_price=95.0,
        exit_price=115.0,
        stop_price=90.0,
        risk_reward_ratio=2.0,
        timeframe_days=21,
    )
    pending = temp_persistence.get_pending_outcomes()
    assert {row["interval"] for row in pending} >= {"1w", "1m"}


def test_update_signal_outcome_computes_return_and_hit(temp_persistence) -> None:
    snapshot_date = (datetime.now(timezone.utc) - timedelta(days=31)).date().isoformat()
    temp_persistence.save_signal_snapshot(
        ticker="NVDA",
        snapshot_date=snapshot_date,
        action="Buy",
        regime_label="Bull",
        regime_probability=0.82,
        composite_strength=0.77,
        benchmark="SOXX",
        current_price=100.0,
        entry_price=95.0,
        exit_price=115.0,
        stop_price=90.0,
        risk_reward_ratio=2.0,
        timeframe_days=21,
    )
    pending = temp_persistence.get_pending_outcomes()
    row_1m = next(row for row in pending if row["interval"] == "1m")
    temp_persistence.update_signal_outcome(int(row_1m["id"]), "1m", 110.0)
    effectiveness = temp_persistence.get_signal_effectiveness()
    assert effectiveness["summary"]["1m"]["count"] == 1
    assert effectiveness["summary"]["1m"]["hit_rate"] == pytest.approx(1.0)


def test_update_signal_outcome_rejects_invalid_interval(temp_persistence) -> None:
    with pytest.raises(temp_persistence.PersistenceError):
        temp_persistence.update_signal_outcome(1, "bad", 100.0)


def test_get_calibration_data_filters_completed_rows(temp_persistence) -> None:
    snapshot_date = (datetime.now(timezone.utc) - timedelta(days=31)).date().isoformat()
    temp_persistence.save_signal_snapshot(
        ticker="NVDA",
        snapshot_date=snapshot_date,
        action="Buy",
        regime_label="Bull",
        regime_probability=0.82,
        composite_strength=0.77,
        benchmark="SOXX",
        current_price=100.0,
        entry_price=95.0,
        exit_price=115.0,
        stop_price=90.0,
        risk_reward_ratio=2.0,
        timeframe_days=21,
    )
    row_1m = next(row for row in temp_persistence.get_pending_outcomes() if row["interval"] == "1m")
    temp_persistence.update_signal_outcome(int(row_1m["id"]), "1m", 110.0)
    rows = temp_persistence.get_calibration_data()
    assert rows and rows[0]["return_1m"] is not None


def test_sector_cache_round_trip(temp_persistence) -> None:
    temp_persistence.save_sector_cache("NVDA", "Semiconductors")
    assert temp_persistence.get_cached_sector("NVDA") == "Semiconductors"


def test_sector_cache_respects_ttl(temp_persistence) -> None:
    temp_persistence.save_sector_cache("NVDA", "Semiconductors")
    with temp_persistence._connect() as conn:
        conn.execute("UPDATE sector_cache SET cached_at = ?", ("2000-01-01T00:00:00+00:00",))
    assert temp_persistence.get_cached_sector("NVDA", max_age_days=1) is None


def test_earnings_cache_round_trip(temp_persistence) -> None:
    temp_persistence.save_earnings_cache("NVDA", "2026-04-30T00:00:00+00:00")
    assert temp_persistence.get_cached_earnings_date("NVDA") == "2026-04-30T00:00:00+00:00"


def test_earnings_cache_respects_ttl(temp_persistence) -> None:
    temp_persistence.save_earnings_cache("NVDA", "2026-04-30T00:00:00+00:00")
    with temp_persistence._connect() as conn:
        conn.execute("UPDATE earnings_cache SET cached_at = ?", ("2000-01-01T00:00:00+00:00",))
    assert temp_persistence.get_cached_earnings_date("NVDA", max_age_hours=1) is None


def test_get_historical_regime_durations_returns_stats(temp_persistence) -> None:
    with temp_persistence._connect() as conn:
        conn.execute(
            "INSERT INTO regime_change_history (ticker, previous_label, current_label, current_state_id, changed_at) VALUES (?, ?, ?, ?, ?)",
            ("NVDA", "Neutral", "Bull", 0, "2026-01-01T00:00:00+00:00"),
        )
        conn.execute(
            "INSERT INTO regime_change_history (ticker, previous_label, current_label, current_state_id, changed_at) VALUES (?, ?, ?, ?, ?)",
            ("NVDA", "Bull", "Bear", 2, "2026-01-11T00:00:00+00:00"),
        )
    durations = temp_persistence.get_historical_regime_durations("NVDA")
    assert durations["Bull"]["avg"] == pytest.approx(10.0)


def test_connect_raises_persistence_error_on_sqlite_failure(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("HMM_DATA_DIR", str(tmp_path))
    module = importlib.reload(persistence)
    monkeypatch.setattr(module.sqlite3, "connect", lambda *_args, **_kwargs: (_ for _ in ()).throw(sqlite3.Error("boom")))
    with pytest.raises(module.PersistenceError):
        module._connect()
