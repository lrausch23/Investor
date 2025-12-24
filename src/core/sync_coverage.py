from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from src.db.models import ExternalConnection, SyncRun


CoverageStatus = str  # UNKNOWN|PARTIAL|COMPLETE


def compute_coverage_status(conn: ExternalConnection, latest_run: Optional[SyncRun]) -> CoverageStatus:
    if conn.last_full_sync_at is None:
        return "UNKNOWN"
    if conn.last_error_json:
        return "PARTIAL"
    if latest_run is None:
        return "PARTIAL"
    if latest_run.status != "SUCCESS":
        return "PARTIAL"
    if int(getattr(latest_run, "parse_fail_count", 0) or 0) > 0:
        return "PARTIAL"
    return "COMPLETE"

