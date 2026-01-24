from __future__ import annotations

import os
import secrets
import threading
import time
from typing import Optional

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials


security = HTTPBasic(auto_error=False)
_AUTH_LOCK = threading.Lock()
_AUTH_FAILS: dict[str, dict[str, float]] = {}


def _expected_password() -> Optional[str]:
    pw = os.environ.get("APP_PASSWORD")
    if pw is not None and pw.strip() == "":
        return None
    return pw


def _env_int(name: str, default: int) -> int:
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except Exception:
        return default


def _auth_limits() -> tuple[int, int, int] | None:
    max_attempts = _env_int("APP_AUTH_MAX_ATTEMPTS", 5)
    window = _env_int("APP_AUTH_WINDOW_SECONDS", 300)
    lockout = _env_int("APP_AUTH_LOCKOUT_SECONDS", 900)
    if max_attempts <= 0 or window <= 0 or lockout <= 0:
        return None
    return max_attempts, window, lockout


def _trust_proxy() -> bool:
    raw = (os.environ.get("APP_AUTH_TRUST_PROXY") or "").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _client_id(request: Request | None) -> str:
    if request is None:
        return "unknown"
    if _trust_proxy():
        xff = request.headers.get("X-Forwarded-For")
        if xff:
            return xff.split(",")[0].strip()
    if request.client and request.client.host:
        return request.client.host
    return "unknown"


def _auth_key(username: str, request: Request | None) -> str:
    return f"{_client_id(request)}:{username or 'unknown'}"


def _record_success(key: str) -> None:
    if not key:
        return
    with _AUTH_LOCK:
        _AUTH_FAILS.pop(key, None)


def _check_lockout(key: str) -> None:
    limits = _auth_limits()
    if not limits or not key:
        return
    now = time.monotonic()
    with _AUTH_LOCK:
        state = _AUTH_FAILS.get(key)
        if not state:
            return
        locked_until = float(state.get("locked_until") or 0.0)
        if locked_until <= now:
            return
    retry = max(1, int(locked_until - now))
    raise HTTPException(
        status_code=status.HTTP_429_TOO_MANY_REQUESTS,
        detail="Too many failed login attempts. Try again later.",
        headers={"Retry-After": str(retry)},
    )


def _record_failure(key: str) -> None:
    limits = _auth_limits()
    if not limits or not key:
        return
    max_attempts, window, lockout = limits
    now = time.monotonic()
    with _AUTH_LOCK:
        state = _AUTH_FAILS.get(key)
        if state is None or now - float(state.get("window_start") or 0.0) > window:
            state = {"count": 0.0, "window_start": now, "locked_until": 0.0}
            _AUTH_FAILS[key] = state
        locked_until = float(state.get("locked_until") or 0.0)
        if locked_until > now:
            return
        state["count"] = float(state.get("count") or 0.0) + 1.0
        if state["count"] >= float(max_attempts):
            state["locked_until"] = now + lockout


def auth_enabled() -> bool:
    return _expected_password() is not None


def get_actor_from_request(request: Request) -> str:
    return request.headers.get("X-Actor") or os.environ.get("APP_ACTOR_DEFAULT", "local")


def require_actor(credentials: Optional[HTTPBasicCredentials] = Depends(security), request: Request = None) -> str:  # type: ignore[assignment]
    expected = _expected_password()
    if expected is None:
        return get_actor_from_request(request) if request is not None else "local"

    if credentials is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required",
            headers={"WWW-Authenticate": "Basic"},
        )
    username = credentials.username or ""
    key = _auth_key(username, request)
    _check_lockout(key)
    if not secrets.compare_digest(credentials.password or "", expected):
        _record_failure(key)
        _check_lockout(key)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required",
            headers={"WWW-Authenticate": "Basic"},
        )
    _record_success(key)
    return username or "user"


def auth_status_label() -> Optional[str]:
    return "Basic" if auth_enabled() else None


def auth_banner_message() -> Optional[str]:
    if auth_enabled():
        return None
    return "WARNING: APP_PASSWORD is not set. This UI is unauthenticated; run locally only."
