from __future__ import annotations

import asyncio
import inspect
import logging
import threading
from concurrent.futures import Future, TimeoutError as FutureTimeoutError
from typing import Any, Callable, TypeVar

from .exceptions import BrokerConnectionError

logger = logging.getLogger(__name__)

T = TypeVar("T")


class IBThread:
    """Singleton daemon thread owning the asyncio event loop for ib_insync."""

    def __init__(self) -> None:
        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._ready = threading.Event()
        self._started = False

    def start(self) -> None:
        if self._started and self._thread is not None and self._thread.is_alive():
            return
        self._ready.clear()
        self._thread = threading.Thread(target=self._run_loop, daemon=True, name="ib-thread")
        self._thread.start()
        if not self._ready.wait(timeout=5.0):
            raise BrokerConnectionError("IB thread failed to start within 5 seconds")
        self._started = True
        logger.info("IB thread started (thread=%s, loop=%s)", self._thread.ident, id(self._loop))

    def _run_loop(self) -> None:
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._ready.set()
        self._loop.run_forever()

    def run(self, fn: Callable[..., T], *args: Any, timeout: float = 30.0) -> T:
        if self._loop is None or not self._started:
            raise BrokerConnectionError("IB thread not started. Call start() first.")
        future: Future[T] = Future()

        def _task() -> None:
            try:
                result = fn(*args)
                if inspect.isawaitable(result):
                    task = self._loop.create_task(result)

                    def _copy_result(completed: asyncio.Future[Any]) -> None:
                        try:
                            future.set_result(completed.result())
                        except Exception as exc:
                            future.set_exception(exc)

                    task.add_done_callback(_copy_result)
                else:
                    future.set_result(result)
            except Exception as exc:
                future.set_exception(exc)

        self._loop.call_soon_threadsafe(_task)
        try:
            return future.result(timeout=timeout)
        except FutureTimeoutError as exc:
            raise TimeoutError(f"IB thread task exceeded {timeout:.1f}s timeout") from exc

    @property
    def loop(self) -> asyncio.AbstractEventLoop | None:
        return self._loop

    @property
    def is_alive(self) -> bool:
        return bool(self._started and self._thread is not None and self._thread.is_alive())


_ib_thread = IBThread()


def get_ib_thread() -> IBThread:
    if not _ib_thread.is_alive:
        _ib_thread.start()
    return _ib_thread
