from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

GENESIS_HASH = "0" * 64


@dataclass(frozen=True)
class TrialLedgerStatus:
    valid: bool
    trial_count: int
    last_hash: str
    issues: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "valid": self.valid,
            "trial_count": self.trial_count,
            "last_hash": self.last_hash,
            "issues": list(self.issues),
        }


def append_trial(
    ledger_path: str | Path,
    trial: dict[str, Any],
    *,
    data_snapshot_hash: str,
) -> dict[str, Any]:
    """Append one ARL trial record to a tamper-evident JSONL ledger."""

    path = Path(ledger_path)
    status = verify_trial_ledger(path)
    if not status.valid:
        raise ValueError(f"Cannot append to invalid trial ledger: {', '.join(status.issues)}")
    path.parent.mkdir(parents=True, exist_ok=True)
    sequence = status.trial_count + 1
    record = {
        "sequence": sequence,
        "recorded_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "previous_hash": status.last_hash,
        "data_snapshot_hash": str(data_snapshot_hash),
        "trial": dict(trial),
    }
    record["record_hash"] = _record_hash(record)
    existing = path.read_text(encoding="utf-8") if path.exists() else ""
    payload = existing
    if payload and not payload.endswith("\n"):
        payload += "\n"
    payload += json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n"
    _atomic_write_text(path, payload)
    _write_state(path, sequence, str(record["record_hash"]))
    return record


def verify_trial_ledger(ledger_path: str | Path) -> TrialLedgerStatus:
    path = Path(ledger_path)
    state = _read_state(path)
    if not path.exists():
        if int(state.get("trial_count") or 0) > 0:
            return TrialLedgerStatus(False, 0, GENESIS_HASH, ("ledger_missing_after_prior_trials",))
        return TrialLedgerStatus(True, 0, GENESIS_HASH, ())

    issues: list[str] = []
    count = 0
    previous_hash = GENESIS_HASH
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            text = line.strip()
            if not text:
                continue
            try:
                record = json.loads(text)
            except json.JSONDecodeError:
                issues.append(f"line_{line_number}_invalid_json")
                continue
            count += 1
            if int(record.get("sequence") or -1) != count:
                issues.append(f"line_{line_number}_sequence_mismatch")
            if str(record.get("previous_hash") or "") != previous_hash:
                issues.append(f"line_{line_number}_previous_hash_mismatch")
            expected_hash = _record_hash(record)
            if str(record.get("record_hash") or "") != expected_hash:
                issues.append(f"line_{line_number}_record_hash_mismatch")
            previous_hash = expected_hash

    state_count = state.get("trial_count")
    state_hash = state.get("last_hash")
    if state_count is not None and int(state_count) != count:
        issues.append("state_trial_count_mismatch")
    if state_hash is not None and str(state_hash) != previous_hash:
        issues.append("state_last_hash_mismatch")
    return TrialLedgerStatus(not issues, count, previous_hash, tuple(issues))


def _record_hash(record: dict[str, Any]) -> str:
    payload = dict(record)
    payload.pop("record_hash", None)
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _state_path(path: Path) -> Path:
    return path.with_suffix(path.suffix + ".state.json")


def _read_state(path: Path) -> dict[str, Any]:
    state_path = _state_path(path)
    if not state_path.exists():
        return {}
    try:
        payload = json.loads(state_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {"state_invalid_json": True}
    return payload if isinstance(payload, dict) else {}


def _write_state(path: Path, trial_count: int, last_hash: str) -> None:
    state_path = _state_path(path)
    _atomic_write_text(
        state_path,
        json.dumps(
            {
                "trial_count": int(trial_count),
                "last_hash": str(last_hash),
            },
            sort_keys=True,
            indent=2,
        )
        + "\n",
    )


def _atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with tmp.open("w", encoding="utf-8") as handle:
        handle.write(text)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp, path)
