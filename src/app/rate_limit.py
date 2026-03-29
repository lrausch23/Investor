"""In-memory sliding-window rate limiter for trading endpoints."""
from __future__ import annotations

import os
import threading
import time
from collections import defaultdict

from fastapi import HTTPException, Request, status

_LOCK = threading.Lock()
_BUCKETS: dict[str, list[float]] = defaultdict(list)


def _env_int(name: str, default: int) -> int:
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except Exception:
        return default


def _client_id(request: Request) -> str:
    trust_proxy = os.environ.get("APP_AUTH_TRUST_PROXY", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    if trust_proxy:
        xff = request.headers.get("X-Forwarded-For")
        if xff:
            return xff.split(",")[0].strip()
    if request.client and request.client.host:
        return request.client.host
    return "unknown"


def check_rate_limit(
    request: Request,
    *,
    action: str,
    max_requests: int | None = None,
    window_seconds: int | None = None,
) -> None:
    """Check rate limit for an action and client."""
    if max_requests is None:
        max_requests = _env_int("APP_RATE_LIMIT_MAX", 10)
    if window_seconds is None:
        window_seconds = _env_int("APP_RATE_LIMIT_WINDOW", 60)
    if max_requests <= 0 or window_seconds <= 0:
        return

    client = _client_id(request)
    key = f"{action}:{client}"
    now = time.monotonic()
    cutoff = now - window_seconds

    with _LOCK:
        timestamps = [ts for ts in _BUCKETS[key] if ts > cutoff]
        _BUCKETS[key] = timestamps
        if len(timestamps) >= max_requests:
            retry_after = int(timestamps[0] - cutoff) + 1
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail=f"Rate limit exceeded for {action}. Try again in {retry_after}s.",
                headers={"Retry-After": str(retry_after)},
            )
        timestamps.append(now)
