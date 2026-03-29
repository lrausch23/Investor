from __future__ import annotations

from datetime import datetime, timezone
import logging
import threading
import time
from typing import Any, Callable

from .persistence import set_setting, save_alert

logger = logging.getLogger(__name__)

DEFAULT_CHECK_INTERVAL_SECONDS = 60
DEFAULT_MAX_RECONNECT_ATTEMPTS = 5
DEFAULT_RECONNECT_BACKOFF_BASE = 2.0


class ConnectionWatchdog:
    def __init__(self, health_fn: Callable[[], dict[str, Any]], reconnect_fn: Callable[[], bool], check_interval: int = DEFAULT_CHECK_INTERVAL_SECONDS):
        self._health_fn = health_fn
        self._reconnect_fn = reconnect_fn
        self._check_interval = int(check_interval)
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._consecutive_failures = 0
        self._last_connected = True
        self._started_at: str | None = None
        self._last_check: str | None = None

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, daemon=True, name="ib-watchdog")
        self._thread.start()
        self._started_at = datetime.now(timezone.utc).isoformat()
        logger.info("Connection watchdog started (interval=%ds)", self._check_interval)

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=10)
        logger.info("Connection watchdog stopped")

    @property
    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def _run(self) -> None:
        while not self._stop_event.is_set():
            try:
                self._check_connection()
            except Exception as exc:
                logger.warning("Watchdog check failed: %s", exc)
            self._stop_event.wait(timeout=self._check_interval)

    def _check_connection(self) -> None:
        self._last_check = datetime.now(timezone.utc).isoformat()
        set_setting("last_watchdog_check", self._last_check)
        health = self._health_fn() or {}
        connected = bool(health.get("connected"))
        if connected:
            if not self._last_connected:
                save_alert("connection_restored", "IBKR connection restored", severity="warning", message="Watchdog observed reconnection.", data=health)
            self._consecutive_failures = 0
            self._last_connected = True
            return
        if self._last_connected:
            save_alert("connection_lost", "IBKR connection lost", severity="critical", message="Watchdog detected disconnect.", data=health)
        self._last_connected = False
        self._consecutive_failures += 1
        for attempt in range(1, DEFAULT_MAX_RECONNECT_ATTEMPTS + 1):
            if self._stop_event.is_set():
                return
            if self._reconnect_fn():
                save_alert("connection_restored", "IBKR connection restored", severity="warning", message="Watchdog reconnected successfully.", data={"attempt": attempt})
                self._consecutive_failures = 0
                self._last_connected = True
                return
            time.sleep(DEFAULT_RECONNECT_BACKOFF_BASE ** (attempt - 1))

    def get_status(self) -> dict[str, Any]:
        return {
            "running": self.is_running,
            "started_at": self._started_at,
            "last_check": self._last_check,
            "connected": self._last_connected,
            "consecutive_failures": self._consecutive_failures,
            "check_interval": self._check_interval,
        }


_watchdog: ConnectionWatchdog | None = None


def get_watchdog() -> ConnectionWatchdog | None:
    return _watchdog


def start_watchdog(health_fn, reconnect_fn, interval: int = DEFAULT_CHECK_INTERVAL_SECONDS) -> ConnectionWatchdog:
    global _watchdog
    if _watchdog is not None and _watchdog.is_running:
        return _watchdog
    _watchdog = ConnectionWatchdog(health_fn, reconnect_fn, interval)
    _watchdog.start()
    return _watchdog


def stop_watchdog() -> None:
    global _watchdog
    if _watchdog is not None:
        _watchdog.stop()
        _watchdog = None
