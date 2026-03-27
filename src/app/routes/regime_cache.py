from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from typing import Any


_CACHE_ROOT = Path("data") / "regime_cache"
_CACHE_VERSION = 2
_QUALITATIVE_TTL_SECONDS = 4 * 60 * 60
_BACKTEST_TTL_SECONDS = 24 * 60 * 60


def _ensure_root() -> Path:
    _CACHE_ROOT.mkdir(parents=True, exist_ok=True)
    return _CACHE_ROOT


def last_run_path() -> Path:
    _ensure_root()
    return _CACHE_ROOT / "last_run.json"


def previous_run_path() -> Path:
    _ensure_root()
    return _CACHE_ROOT / "previous_run.json"


def qualitative_cache_dir() -> Path:
    root = _ensure_root() / "qualitative"
    root.mkdir(parents=True, exist_ok=True)
    return root


def qualitative_cache_path(ticker: str) -> Path:
    return qualitative_cache_dir() / f"{str(ticker or '').strip().upper()}.json"


def save_payload(payload: dict[str, Any]) -> Path:
    path = last_run_path()
    cache_payload = dict(payload)
    cache_payload["cache_version"] = _CACHE_VERSION
    path.write_text(json.dumps(cache_payload, indent=2, sort_keys=True), encoding="utf-8")
    return path


def archive_previous_payload() -> Path | None:
    current = last_run_path()
    if not current.exists():
        return None
    previous = previous_run_path()
    try:
        payload = json.loads(current.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    payload["cache_version"] = _CACHE_VERSION
    previous.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return previous


def load_payload() -> dict[str, Any] | None:
    path = last_run_path()
    return _load_versioned_payload(path)


def load_previous_payload() -> dict[str, Any] | None:
    return _load_versioned_payload(previous_run_path())


def _load_versioned_payload(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    if int(payload.get("cache_version") or 0) != _CACHE_VERSION:
        return None
    return payload


def load_qualitative_cache(
    ticker: str,
    *,
    provider: str,
    ttl_seconds: int = _QUALITATIVE_TTL_SECONDS,
) -> dict[str, Any] | None:
    path = qualitative_cache_path(ticker)
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    cached_provider = str(payload.get("provider") or "").strip().lower()
    if cached_provider != str(provider or "").strip().lower():
        return None
    cached_at_raw = str(payload.get("cached_at") or "").strip()
    if not cached_at_raw:
        return None
    try:
        cached_at = dt.datetime.fromisoformat(cached_at_raw)
    except Exception:
        return None
    age = dt.datetime.now(dt.timezone.utc) - cached_at.astimezone(dt.timezone.utc)
    if age.total_seconds() > int(ttl_seconds):
        return None
    data = payload.get("data")
    return data if isinstance(data, dict) else None


def save_qualitative_cache(
    ticker: str,
    *,
    provider: str,
    data: dict[str, Any],
) -> Path:
    path = qualitative_cache_path(ticker)
    payload = {
        "ticker": str(ticker or "").strip().upper(),
        "provider": str(provider or "").strip().lower(),
        "cached_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "data": data,
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return path


def backtest_cache_dir() -> Path:
    root = _ensure_root() / "backtest"
    root.mkdir(parents=True, exist_ok=True)
    return root


def backtest_cache_path(ticker: str, period: str) -> Path:
    key = f"{str(ticker or '').strip().upper()}_{str(period or '').strip().lower() or '5y'}"
    return backtest_cache_dir() / f"{key}.json"


def load_backtest_cache(ticker: str, period: str, ttl_seconds: int = _BACKTEST_TTL_SECONDS) -> dict[str, Any] | None:
    path = backtest_cache_path(ticker, period)
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        cached_at = dt.datetime.fromisoformat(str(payload.get("cached_at") or ""))
    except Exception:
        return None
    age = dt.datetime.now(dt.timezone.utc) - cached_at.astimezone(dt.timezone.utc)
    if age.total_seconds() > int(ttl_seconds):
        return None
    data = payload.get("data")
    return data if isinstance(data, dict) else None


def save_backtest_cache(ticker: str, period: str, data: dict[str, Any]) -> Path:
    path = backtest_cache_path(ticker, period)
    payload = {
        "ticker": str(ticker or "").strip().upper(),
        "period": str(period or "").strip().lower() or "5y",
        "cached_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "data": data,
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return path
