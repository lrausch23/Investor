from __future__ import annotations

import hashlib
import sqlite3
from pathlib import Path

import pytest

import scripts.uat_reset_db as uat_reset_db


def _write_sqlite(path: Path, *, table: str = "sample", value: str = "x") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    try:
        conn.execute(f"CREATE TABLE {table} (value TEXT)")
        conn.execute(f"INSERT INTO {table} (value) VALUES (?)", (value,))
        conn.commit()
    finally:
        conn.close()


def _checksum(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _targets(tmp_path: Path) -> list[uat_reset_db.DatabaseTarget]:
    return [
        uat_reset_db.DatabaseTarget("investor.db", tmp_path / "data" / "investor.db"),
        uat_reset_db.DatabaseTarget("regime_watch.db", tmp_path / "src" / "data" / "regime" / "regime_watch.db"),
    ]


def test_archive_creates_timestamped_copy(tmp_path: Path) -> None:
    db = tmp_path / "investor.db"
    _write_sqlite(db)
    archive = uat_reset_db.archive_database(db, "20260402_143000")
    assert archive is not None
    assert archive.name == "investor_pre_uat_20260402_143000.db"
    assert db.exists()
    assert archive.exists()
    assert _checksum(db) == _checksum(archive)


def test_archive_skips_missing_database(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    archive = uat_reset_db.archive_database(tmp_path / "missing.db", "20260402_143000")
    captured = capsys.readouterr()
    assert archive is None
    assert "does not exist; skipping archive" in captured.out


def test_reset_recreates_investor_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    targets = _targets(tmp_path)
    investor = targets[0].path
    regime = targets[1].path
    _write_sqlite(investor, table="legacy", value="old")
    _write_sqlite(regime, table="legacy", value="old")
    monkeypatch.setattr(uat_reset_db, "get_targets", lambda: targets)
    monkeypatch.setattr(uat_reset_db, "get_investor_db_path", lambda: investor)
    monkeypatch.setattr(uat_reset_db, "get_regime_db_path", lambda: regime)
    monkeypatch.setattr(uat_reset_db, "app_running", lambda port=None: False)
    uat_reset_db.reset_databases(yes=True)
    archives = list(investor.parent.glob("investor_pre_uat_*.db"))
    assert investor.exists()
    assert archives
    conn = sqlite3.connect(investor)
    try:
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    finally:
        conn.close()
    assert "household_entities" in tables or "tax_assumptions_sets" in tables


def test_reset_recreates_regime_watch_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    targets = _targets(tmp_path)
    investor = targets[0].path
    regime = targets[1].path
    _write_sqlite(investor, table="legacy", value="old")
    _write_sqlite(regime, table="legacy", value="old")
    monkeypatch.setattr(uat_reset_db, "get_targets", lambda: targets)
    monkeypatch.setattr(uat_reset_db, "get_investor_db_path", lambda: investor)
    monkeypatch.setattr(uat_reset_db, "get_regime_db_path", lambda: regime)
    monkeypatch.setattr(uat_reset_db, "app_running", lambda port=None: False)
    uat_reset_db.reset_databases(yes=True)
    archives = list(regime.parent.glob("regime_watch_pre_uat_*.db"))
    assert not regime.exists()
    assert archives


def test_list_archives_finds_files(tmp_path: Path) -> None:
    investor_dir = tmp_path / "data"
    regime_dir = tmp_path / "src" / "data" / "regime"
    investor_dir.mkdir(parents=True)
    regime_dir.mkdir(parents=True)
    newest = investor_dir / "investor_pre_uat_20260402_143000.db"
    older = investor_dir / "investor_pre_uat_20260401_090000.db"
    regime = regime_dir / "regime_watch_pre_uat_20260402_143000.db"
    newest.write_text("a")
    older.write_text("b")
    regime.write_text("c")
    archives = uat_reset_db.list_archives(
        [
            uat_reset_db.DatabaseTarget("investor.db", investor_dir / "investor.db"),
            uat_reset_db.DatabaseTarget("regime_watch.db", regime_dir / "regime_watch.db"),
        ]
    )
    assert archives[0].name == "regime_watch_pre_uat_20260402_143000.db" or archives[0].name == "investor_pre_uat_20260402_143000.db"
    assert older in archives
    assert newest in archives
    assert regime in archives


def test_restore_replaces_current_with_archive(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    targets = _targets(tmp_path)
    investor = targets[0].path
    regime = targets[1].path
    investor.parent.mkdir(parents=True, exist_ok=True)
    regime.parent.mkdir(parents=True, exist_ok=True)
    investor.write_text("current-investor")
    regime.write_text("current-regime")
    investor_archive = investor.parent / "investor_pre_uat_20260402_143000.db"
    regime_archive = regime.parent / "regime_watch_pre_uat_20260402_143000.db"
    investor_archive.write_text("archive-investor")
    regime_archive.write_text("archive-regime")
    monkeypatch.setattr(uat_reset_db, "get_targets", lambda: targets)
    monkeypatch.setattr(uat_reset_db, "utc_timestamp", lambda: "20260403_010000")
    restored = uat_reset_db.restore_databases("20260402_143000")
    assert restored["investor.db"] == investor
    assert investor.read_text() == "archive-investor"
    assert regime.read_text() == "archive-regime"
    assert (investor.parent / "investor_pre_uat_20260403_010000.db").exists()
    assert (regime.parent / "regime_watch_pre_uat_20260403_010000.db").exists()


def test_restore_missing_timestamp_errors(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(uat_reset_db, "get_targets", lambda: _targets(tmp_path))
    with pytest.raises(RuntimeError, match="No UAT archives found"):
        uat_reset_db.restore_databases("19990101_000000")


def test_reset_aborted_if_app_running(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    targets = _targets(tmp_path)
    investor = targets[0].path
    regime = targets[1].path
    _write_sqlite(investor)
    _write_sqlite(regime)
    monkeypatch.setattr(uat_reset_db, "get_targets", lambda: targets)
    monkeypatch.setattr(uat_reset_db, "app_running", lambda port=None: True)
    with pytest.raises(RuntimeError, match="Application appears to be running"):
        uat_reset_db.reset_databases(yes=True)
    assert investor.exists()
    assert regime.exists()
    assert not list(investor.parent.glob("*_pre_uat_*.db"))


def test_atomic_archive_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    targets = _targets(tmp_path)
    investor = targets[0].path
    regime = targets[1].path
    _write_sqlite(investor)
    _write_sqlite(regime)
    monkeypatch.setattr(uat_reset_db, "get_targets", lambda: targets)
    monkeypatch.setattr(uat_reset_db, "get_investor_db_path", lambda: investor)
    monkeypatch.setattr(uat_reset_db, "get_regime_db_path", lambda: regime)
    monkeypatch.setattr(uat_reset_db, "app_running", lambda port=None: False)
    original_archive = uat_reset_db.archive_database

    def failing_archive(path: Path, timestamp: str, *, label: str = "pre_uat") -> Path | None:
        if path == regime:
            raise PermissionError("boom")
        return original_archive(path, timestamp, label=label)

    monkeypatch.setattr(uat_reset_db, "archive_database", failing_archive)
    with pytest.raises(RuntimeError, match="Unable to archive"):
        uat_reset_db.reset_databases(yes=True)
    assert investor.exists()
    assert regime.exists()
    assert len(list(investor.parent.glob("investor_pre_uat_*.db"))) == 1


def test_yes_flag_skips_confirmation(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    targets = _targets(tmp_path)
    investor = targets[0].path
    regime = targets[1].path
    _write_sqlite(investor)
    _write_sqlite(regime)
    monkeypatch.setattr(uat_reset_db, "get_targets", lambda: targets)
    monkeypatch.setattr(uat_reset_db, "get_investor_db_path", lambda: investor)
    monkeypatch.setattr(uat_reset_db, "get_regime_db_path", lambda: regime)
    monkeypatch.setattr(uat_reset_db, "app_running", lambda port=None: False)
    monkeypatch.setattr("builtins.input", lambda prompt="": (_ for _ in ()).throw(AssertionError("input should not be called")))
    uat_reset_db.reset_databases(yes=True)
    assert investor.exists()
