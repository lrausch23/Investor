from __future__ import annotations

import argparse
import os
import shutil
import socket
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@dataclass(frozen=True)
class DatabaseTarget:
    name: str
    path: Path


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def _resolve_sqlite_path(database_url: str) -> Path:
    if database_url.startswith("sqlite:///"):
        return Path(database_url.replace("sqlite:///", "", 1))
    return Path(database_url)


def get_investor_db_path() -> Path:
    database_url = os.environ.get("DATABASE_URL", "sqlite:///./data/investor.db")
    path = _resolve_sqlite_path(database_url)
    return path if path.is_absolute() else (ROOT / path).resolve()


def get_regime_db_path() -> Path:
    configured = os.environ.get("HMM_DATA_DIR")
    if configured:
        return (Path(configured).expanduser().resolve() / "regime_watch.db").resolve()
    return (ROOT / "src" / "data" / "regime" / "regime_watch.db").resolve()


def get_targets() -> list[DatabaseTarget]:
    return [
        DatabaseTarget("investor.db", get_investor_db_path()),
        DatabaseTarget("regime_watch.db", get_regime_db_path()),
    ]


def format_size(num_bytes: int) -> str:
    size = float(num_bytes)
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1024.0 or unit == "GB":
            precision = 0 if unit == "B" else 1
            return f"{size:.{precision}f} {unit}"
        size /= 1024.0
    return f"{num_bytes} B"


def archive_name_for(path: Path, timestamp: str, *, label: str = "pre_uat") -> Path:
    return path.with_name(f"{path.stem}_{label}_{timestamp}{path.suffix}")


def archive_database(path: Path, timestamp: str, *, label: str = "pre_uat") -> Path | None:
    if not path.exists():
        print(f"Warning: {path} does not exist; skipping archive.")
        return None
    archive_path = archive_name_for(path, timestamp, label=label)
    shutil.copy2(path, archive_path)
    print(f"Archived {path} -> {archive_path}")
    return archive_path


def list_archives(targets: Iterable[DatabaseTarget] | None = None) -> list[Path]:
    active_targets = list(targets or get_targets())
    archives: list[Path] = []
    for target in active_targets:
        if target.path.parent.exists():
            archives.extend(target.path.parent.glob("*_pre_uat_*.db"))
    return sorted(archives, key=lambda path: path.name, reverse=True)


def print_archives(targets: Iterable[DatabaseTarget] | None = None) -> list[Path]:
    archives = list_archives(targets)
    print("UAT Archives:")
    for archive in archives:
        size = archive.stat().st_size if archive.exists() else 0
        rel = archive.relative_to(ROOT) if archive.is_relative_to(ROOT) else archive
        print(f"  {rel} ({format_size(size)})")
    return archives


def app_running(port: int | None = None) -> bool:
    candidate_port = int(port or int(os.environ.get("APP_PORT", "8000") or "8000"))
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.settimeout(0.5)
        return sock.connect_ex(("localhost", candidate_port)) == 0
    finally:
        sock.close()


def _set_database_url(path: Path) -> None:
    os.environ["DATABASE_URL"] = f"sqlite:///{path}"


def _reset_sqlalchemy_engine() -> None:
    from src.db import session as session_module

    engine = getattr(session_module, "_ENGINE", None)
    if engine is not None:
        try:
            engine.dispose()
        except Exception:
            pass
    session_module._ENGINE = None


def _sqlite_sidecar_paths(path: Path) -> list[Path]:
    return [
        path.with_name(f"{path.name}-wal"),
        path.with_name(f"{path.name}-shm"),
        path.with_name(f"{path.name}-journal"),
    ]


def _remove_sqlite_files(path: Path) -> None:
    for candidate in [path, *_sqlite_sidecar_paths(path)]:
        if candidate.exists():
            candidate.unlink()


def recreate_investor_db(path: Path) -> None:
    from src.db.init_db import init_db

    path.parent.mkdir(parents=True, exist_ok=True)
    _set_database_url(path)
    _reset_sqlalchemy_engine()
    _remove_sqlite_files(path)
    init_db()


def delete_regime_db(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    _remove_sqlite_files(path)


def summarize_reset(targets: Iterable[DatabaseTarget], timestamp: str) -> list[tuple[DatabaseTarget, Path | None, int | None]]:
    summary: list[tuple[DatabaseTarget, Path | None, int | None]] = []
    for target in targets:
        archive_path = archive_name_for(target.path, timestamp)
        size = target.path.stat().st_size if target.path.exists() else None
        summary.append((target, archive_path if target.path.exists() else None, size))
    return summary


def confirm_reset(summary: list[tuple[DatabaseTarget, Path | None, int | None]]) -> bool:
    print("The following databases will be archived and reset:")
    for target, archive_path, size in summary:
        size_label = format_size(size) if size is not None else "missing"
        if archive_path is None:
            print(f"  {target.name:<16} -> missing ({size_label})")
        else:
            rel_archive = archive_path.relative_to(ROOT) if archive_path.is_relative_to(ROOT) else archive_path
            print(f"  {target.name:<16} -> {rel_archive} ({size_label})")
    response = input("\nType 'yes' to proceed: ").strip().lower()
    return response == "yes"


def archive_targets(targets: Iterable[DatabaseTarget], timestamp: str) -> dict[str, Path | None]:
    archives: dict[str, Path | None] = {}
    for target in targets:
        try:
            archives[target.name] = archive_database(target.path, timestamp)
        except Exception as exc:
            raise RuntimeError(f"Unable to archive {target.path}: {exc}") from exc
    return archives


def reset_databases(*, yes: bool = False) -> dict[str, Path | None]:
    port = int(os.environ.get("APP_PORT", "8000") or "8000")
    if app_running(port):
        raise RuntimeError(
            f"Error: Application appears to be running on port {port}. Stop the server before resetting databases."
        )
    targets = get_targets()
    timestamp = utc_timestamp()
    summary = summarize_reset(targets, timestamp)
    if not yes and not confirm_reset(summary):
        raise RuntimeError("Reset cancelled.")
    archives = archive_targets(targets, timestamp)
    investor_path = get_investor_db_path()
    regime_path = get_regime_db_path()
    recreate_investor_db(investor_path)
    delete_regime_db(regime_path)
    print("Reset complete. investor.db and regime_watch.db recreated.")
    return archives


def find_restore_archives(timestamp: str, targets: Iterable[DatabaseTarget] | None = None) -> dict[str, Path]:
    matches: dict[str, Path] = {}
    for target in list(targets or get_targets()):
        candidate = archive_name_for(target.path, timestamp)
        if candidate.exists():
            matches[target.name] = candidate
    return matches


def restore_databases(timestamp: str) -> dict[str, Path]:
    targets = get_targets()
    matches = find_restore_archives(timestamp, targets)
    if not matches:
        raise RuntimeError(f"No UAT archives found for timestamp {timestamp}.")
    safety_timestamp = utc_timestamp()
    for target in targets:
        if target.path.exists():
            archive_database(target.path, safety_timestamp)
    restored: dict[str, Path] = {}
    for target in targets:
        archive = matches.get(target.name)
        if archive is None:
            print(f"Warning: No archive found for {target.name} at {timestamp}; skipping restore.")
            continue
        target.path.parent.mkdir(parents=True, exist_ok=True)
        if target.name == "investor.db":
            _set_database_url(target.path)
            _reset_sqlalchemy_engine()
        _remove_sqlite_files(target.path)
        shutil.copy2(archive, target.path)
        print(f"Restored {target.path} from {archive}")
        restored[target.name] = target.path
    return restored


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Archive and reset Investor databases for UAT.")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--list", action="store_true", dest="list_archives", help="List available UAT archives.")
    group.add_argument("--restore", metavar="STAMP", help="Restore databases from a matching UAT archive timestamp.")
    parser.add_argument("--yes", action="store_true", help="Skip confirmation prompt for reset.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.list_archives:
            print_archives()
            return 0
        if args.restore:
            restore_databases(str(args.restore))
            return 0
        reset_databases(yes=bool(args.yes))
        return 0
    except RuntimeError as exc:
        print(str(exc))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
