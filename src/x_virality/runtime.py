from __future__ import annotations

import asyncio
import signal
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from types import FrameType
from typing import Any


def configure_event_loop_policy() -> None:
    if sys.platform != "win32":
        return
    selector_policy = getattr(asyncio, "WindowsSelectorEventLoopPolicy", None)
    if selector_policy is not None:
        asyncio.set_event_loop_policy(selector_policy())


@contextmanager
def shutdown_signal_handlers(stop: asyncio.Event) -> Iterator[None]:
    """Set a stop event on SIGINT/SIGTERM and restore prior handlers afterward."""

    loop = asyncio.get_running_loop()
    loop_handlers: list[signal.Signals] = []
    fallback_handlers: dict[signal.Signals, Any] = {}

    def request_stop(_signum: int | None = None, _frame: FrameType | None = None) -> None:
        loop.call_soon_threadsafe(stop.set)

    for signum in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(signum, stop.set)
        except (NotImplementedError, RuntimeError):
            try:
                fallback_handlers[signum] = signal.getsignal(signum)
                signal.signal(signum, request_stop)
            except (OSError, RuntimeError, ValueError):
                continue
        else:
            loop_handlers.append(signum)

    try:
        yield
    finally:
        for signum in loop_handlers:
            loop.remove_signal_handler(signum)
        for signum, previous in fallback_handlers.items():
            signal.signal(signum, previous)
