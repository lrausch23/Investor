from __future__ import annotations

import logging
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass


log = logging.getLogger(__name__)


def mask_secret(value: str | None) -> str:
    s = (value or "").strip()
    if not s:
        return "****"
    return "****" + s[-4:]


@dataclass
class _TokenState:
    lock: threading.Lock
    last_send_request_at: float
    last_poll_at: float


_GLOBAL_LOCK = threading.Lock()
_STATE_BY_TOKEN: dict[str, _TokenState] = {}


def _state_for_token(token: str) -> _TokenState:
    with _GLOBAL_LOCK:
        st = _STATE_BY_TOKEN.get(token)
        if st is None:
            st = _TokenState(lock=threading.Lock(), last_send_request_at=0.0, last_poll_at=0.0)
            _STATE_BY_TOKEN[token] = st
        return st


@contextmanager
def token_serial_lock(token: str):
    """
    Serialize Flex execution per token (SendRequest + subsequent polling).
    """
    st = _state_for_token(token)
    st.lock.acquire()
    try:
        yield
    finally:
        st.lock.release()


def rate_limit_sleep(*, token: str, action: str, min_interval_s: float) -> float:
    """
    In-process, per-token spacing using monotonic time.

    Returns:
      sleep_seconds (0 if no sleep occurred)
    """
    st = _state_for_token(token)
    now = time.monotonic()
    if action == "send_request":
        last = st.last_send_request_at
    else:
        last = st.last_poll_at

    wait = (last + float(min_interval_s)) - now
    if wait > 0:
        # No secrets in logs; token masked.
        log.debug("Flex rate limit sleep: %.2fs (token %s, action=%s)", wait, mask_secret(token), action)
        time.sleep(wait)
        slept = wait
    else:
        slept = 0.0

    after = time.monotonic()
    if action == "send_request":
        st.last_send_request_at = after
    else:
        st.last_poll_at = after
    return float(slept)

