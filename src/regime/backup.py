from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
import logging
from pathlib import Path
from typing import Any

from .persistence import DB_PATH, get_setting, set_setting

logger = logging.getLogger(__name__)

DEFAULT_BACKUP_DIR = "backups"
DEFAULT_RETENTION_DAYS = 30
DEFAULT_MAX_BACKUPS = 30


def get_backup_dir() -> str:
    return str(get_setting("backup_dir") or DEFAULT_BACKUP_DIR)


def _backup_root() -> Path:
    return Path(DB_PATH).parent / get_backup_dir()


def create_backup(label: str | None = None) -> dict[str, Any]:
    db_path = Path(DB_PATH)
    backup_dir = _backup_root()
    backup_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    suffix = f"_{str(label).strip()}" if label else ""
    backup_path = backup_dir / f"regime_watch_{timestamp}{suffix}.db"
    source = sqlite3.connect(str(db_path))
    dest = sqlite3.connect(str(backup_path))
    try:
        source.backup(dest)
    finally:
        dest.close()
        source.close()
    created_at = datetime.now(timezone.utc).isoformat()
    size = backup_path.stat().st_size if backup_path.exists() else 0
    set_setting("last_backup_at", created_at)
    set_setting("last_backup_path", str(backup_path))
    logger.info("Database backup created: %s (%d bytes)", backup_path.name, size)
    return {"path": str(backup_path), "size_bytes": size, "created_at": created_at}


def list_backups() -> list[dict[str, Any]]:
    backup_dir = _backup_root()
    if not backup_dir.exists():
        return []
    rows: list[dict[str, Any]] = []
    for path in backup_dir.glob("regime_watch_*.db"):
        stat = path.stat()
        rows.append(
            {
                "filename": path.name,
                "path": str(path),
                "size_bytes": stat.st_size,
                "created_at": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
            }
        )
    rows.sort(key=lambda row: str(row.get("created_at") or ""), reverse=True)
    return rows


def cleanup_old_backups() -> dict[str, Any]:
    backups = list_backups()
    retention_days = max(1, int(get_setting("backup_retention_days") or DEFAULT_RETENTION_DAYS))
    max_backups = max(1, int(get_setting("backup_max_count") or DEFAULT_MAX_BACKUPS))
    cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
    removed = 0
    for row in backups[max_backups:]:
        path = Path(str(row["path"]))
        if path.exists():
            path.unlink()
            removed += 1
    for row in backups[:max_backups]:
        created = datetime.fromisoformat(str(row.get("created_at") or "").replace("Z", "+00:00"))
        if created < cutoff:
            path = Path(str(row["path"]))
            if path.exists():
                path.unlink()
                removed += 1
    remaining = len(list_backups())
    return {"removed": removed, "remaining": remaining}


def get_backup_status() -> dict[str, Any]:
    backups = list_backups()
    return {
        "last_backup_at": get_setting("last_backup_at"),
        "last_backup_path": get_setting("last_backup_path"),
        "backup_count": len(backups),
        "backup_dir": str(_backup_root()),
        "retention_days": max(1, int(get_setting("backup_retention_days") or DEFAULT_RETENTION_DAYS)),
        "max_backups": max(1, int(get_setting("backup_max_count") or DEFAULT_MAX_BACKUPS)),
        "total_size_bytes": sum(int(row.get("size_bytes") or 0) for row in backups),
    }
