"""Process-wide single-writer lock for one validated durable state root."""

from __future__ import annotations

import fcntl
import os
from pathlib import Path

_LOCK_FILENAME = ".context-intelligence-server.lock"


class InstanceLockUnavailable(RuntimeError):
    """Raised when another server already owns this durable queue volume."""


class ServerInstanceLock:
    """Hold a nonblocking exclusive lock for the server process lifecycle."""

    def __init__(self, state_root: str | Path) -> None:
        self.path = Path(state_root) / _LOCK_FILENAME
        self._fd: int | None = None

    def acquire(self) -> None:
        """Acquire the validated state root's exclusive writer lock."""
        if self._fd is not None:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            os.close(fd)
            raise InstanceLockUnavailable(
                "context-intelligence-server refuses to start because another "
                f"server holds the shared state lock at {self.path}. Run exactly "
                "one replica and one worker against this state volume."
            ) from exc
        except BaseException:
            os.close(fd)
            raise
        self._fd = fd

    def release(self) -> None:
        """Release the lock after the server and all of its workers have stopped."""
        if self._fd is None:
            return
        fd, self._fd = self._fd, None
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)

    def __enter__(self) -> ServerInstanceLock:
        self.acquire()
        return self

    def __exit__(self, *_: object) -> None:
        self.release()
