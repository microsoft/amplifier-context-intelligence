"""Session manager Protocol and in-memory stub implementation.

Defines the SessionManager protocol that any concrete session backend must
satisfy, plus a StubSessionManager for use in tests and local development.
"""

import uuid
from typing import Protocol, runtime_checkable


@runtime_checkable
class SessionManager(Protocol):
    """Protocol describing the session lifecycle operations."""

    @property
    def active_count(self) -> int:
        """Return the number of currently active sessions."""
        ...

    async def create_session(self) -> str:
        """Create a new session and return its ID."""
        ...

    async def destroy_session(self, session_id: str) -> None:
        """Destroy the session with *session_id*.  No-op if not found."""
        ...

    async def reset_session(self, session_id: str) -> str:
        """Destroy *session_id* and create a replacement session.

        Returns the new session ID.  The active_count remains unchanged.
        """
        ...

    async def get_session(self, session_id: str) -> dict[str, str] | None:
        """Return session metadata for *session_id*, or None if not found."""
        ...


class StubSessionManager:
    """In-memory session manager suitable for tests and local development.

    Stores sessions as ``dict[str, dict[str, str]]`` keyed by session ID.
    Each session record contains at minimum ``session_id`` and ``status``.
    """

    def __init__(self) -> None:
        self._sessions: dict[str, dict[str, str]] = {}

    # ------------------------------------------------------------------
    # Protocol implementation
    # ------------------------------------------------------------------

    @property
    def active_count(self) -> int:
        """Return the number of currently active sessions."""
        return len(self._sessions)

    async def create_session(self) -> str:
        """Create a new session and return its unique ID."""
        session_id = str(uuid.uuid4())
        self._sessions[session_id] = {
            "session_id": session_id,
            "status": "active",
        }
        return session_id

    async def destroy_session(self, session_id: str) -> None:
        """Remove *session_id* from the store.  Silently ignores unknown IDs."""
        self._sessions.pop(session_id, None)

    async def reset_session(self, session_id: str) -> str:
        """Replace *session_id* with a fresh session and return the new ID."""
        await self.destroy_session(session_id)
        return await self.create_session()

    async def get_session(self, session_id: str) -> dict[str, str] | None:
        """Return the session record for *session_id*, or None if not found."""
        return self._sessions.get(session_id)
