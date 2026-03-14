"""Session registry — per-session worker management."""

import asyncio
from dataclasses import dataclass, field


@dataclass
class SessionWorker:
    session_id: str
    workspace: str
    queue: asyncio.Queue = field(default_factory=asyncio.Queue)
    task: asyncio.Task | None = None


class SessionRegistry:
    def __init__(self) -> None:
        self._workers: dict[str, SessionWorker] = {}

    def get_or_create(self, session_id: str, workspace: str) -> SessionWorker:
        if session_id not in self._workers:
            self._workers[session_id] = SessionWorker(
                session_id=session_id,
                workspace=workspace,
            )
        return self._workers[session_id]

    def remove(self, session_id: str) -> None:
        self._workers.pop(session_id, None)

    def active_count(self) -> int:
        return len(self._workers)

    def active_sessions(self) -> list[str]:
        return sorted(self._workers.keys())
