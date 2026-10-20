"""Process-local FIFO filesystem leases. External writers still need RootLock.

Admission happens in the filesystem thread, not in the coroutine awaiting it.
Cancellation removes a waiting admission; an admitted operation finishes with
its lease intact. Futures are tracked independently of their cancelled callers.
"""

from __future__ import annotations

import asyncio
import threading
from collections import deque
from contextlib import contextmanager
from pathlib import Path
from typing import Callable, Iterator, TypeVar

T = TypeVar("T")


class AdmissionCancelled(Exception):
    pass


class Coordinator:
    def __init__(self) -> None:
        self.condition = threading.Condition()
        self.queue: deque[tuple[object, bool]] = deque()
        self.readers = 0
        self.writer = False
        self.service: object | None = None

    @contextmanager
    def lease(self, *, exclusive: bool = False, cancel: threading.Event | None = None) -> Iterator[None]:
        token = object()
        with self.condition:
            self.queue.append((token, exclusive))
            try:
                while True:
                    if cancel is not None and cancel.is_set():
                        raise AdmissionCancelled()
                    preceding_writer = False
                    for queued, write in self.queue:
                        if queued is token:
                            break
                        preceding_writer |= write
                    if not self.writer and not preceding_writer and (
                        not exclusive or (self.queue[0][0] is token and not self.readers)
                    ):
                        self.queue.remove((token, exclusive))
                        if exclusive:
                            self.writer = True
                        else:
                            self.readers += 1
                        self.condition.notify_all()
                        break
                    self.condition.wait(0.05 if cancel is not None else None)
            except BaseException:
                self.queue.remove((token, exclusive))
                self.condition.notify_all()
                raise
        try:
            yield
        finally:
            with self.condition:
                if exclusive:
                    self.writer = False
                else:
                    self.readers -= 1
                self.condition.notify_all()


_registry_lock = threading.Lock()
_registry: dict[Path, Coordinator] = {}


def coordinator_for(path: Path | str) -> Coordinator:
    """Call in an I/O thread: canonical aliases share one admission queue."""
    canonical = Path(path).expanduser().resolve()
    with _registry_lock:
        return _registry.setdefault(canonical, Coordinator())


# Accessed only on the event-loop thread. Keep the underlying futures alive even
# when their awaiting request/drainer has been cancelled.
_pending: dict[asyncio.AbstractEventLoop, set[asyncio.Future]] = {}


async def filesystem(path: Path, operation: Callable[[], T], *, exclusive: bool = False) -> T:
    loop = asyncio.get_running_loop()
    cancel = threading.Event()

    def run() -> T:
        with coordinator_for(path).lease(exclusive=exclusive, cancel=cancel):
            return operation()

    future = asyncio.create_task(asyncio.to_thread(run))
    pending = _pending.setdefault(loop, set())
    pending.add(future)

    def finished(done: asyncio.Future) -> None:
        pending.discard(done)
        # Retrieve abandoned exceptions, without changing the awaiter's result.
        if not done.cancelled():
            done.exception()
        if not pending:
            _pending.pop(loop, None)

    future.add_done_callback(finished)
    try:
        return await asyncio.shield(future)
    except asyncio.CancelledError:
        cancel.set()
        raise


async def wait_filesystem() -> None:
    """After drainers stop submitting, wait for queued AND running real I/O."""
    loop = asyncio.get_running_loop()
    while pending := tuple(_pending.get(loop, ())):
        await asyncio.gather(*(asyncio.shield(f) for f in pending), return_exceptions=True)