"""AmplifierSessionManager — Amplifier-backed session manager.

Implements the SessionManager protocol and adds an execute() method that
dispatches prompts to real Amplifier sessions.
"""

from __future__ import annotations

import inspect
import uuid
from typing import Any

from intelligence_service.a2ui_bridge import extract_a2ui_from_response

class AmplifierSessionManager:
    """Session manager that delegates to a live Amplifier PreparedBundle.

    Each session is backed by a real Amplifier session object returned by
    ``amplifier_app.prepared.create_session()``.  The internal registry maps
    session IDs (str) to those session objects.
    """

    def __init__(
        self,
        *,
        amplifier_app: Any,
        workspace: str,
        amplifier_home: str,
    ) -> None:
        self._amplifier_app = amplifier_app
        self._workspace = workspace
        self._amplifier_home = amplifier_home
        self._sessions: dict[str, Any] = {}

    # ------------------------------------------------------------------
    # SessionManager protocol
    # ------------------------------------------------------------------

    @property
    def active_count(self) -> int:
        """Return the number of currently active sessions."""
        return len(self._sessions)

    async def create_session(self) -> str:
        """Create a new Amplifier session and return its ID."""
        session_id = str(uuid.uuid4())
        session = await self._amplifier_app.prepared.create_session(
            session_id=session_id,
            session_cwd=f"{self._amplifier_home}/{self._workspace}",
        )
        self._sessions[session_id] = session
        return session_id

    async def destroy_session(self, session_id: str) -> None:
        """Remove the session with *session_id*.  No-op if not found."""
        session = self._sessions.pop(session_id, None)
        if session is not None and inspect.iscoroutinefunction(
            getattr(session, "close", None)
        ):
            await session.close()

    async def reset_session(self, session_id: str) -> str:
        """Destroy *session_id* and create a replacement; return new ID."""
        await self.destroy_session(session_id)
        return await self.create_session()

    async def get_session(self, session_id: str) -> dict[str, str] | None:
        """Return metadata for *session_id*, or None if not found."""
        if session_id not in self._sessions:
            return None
        return {"session_id": session_id, "status": "active"}

    # ------------------------------------------------------------------
    # Extended API
    # ------------------------------------------------------------------

    async def execute(self, session_id: str, prompt: str) -> dict[str, Any]:
        """Execute *prompt* in the given session and return the result.

        Returns a dict with keys:
          - ``text``: the raw response string from the session
          - ``a2ui``: list of A2UI message dicts extracted from the response

        Raises ``KeyError`` if *session_id* is not found.
        """
        session = self._sessions[session_id]
        response = await session.execute(prompt)
        a2ui_messages = extract_a2ui_from_response(response)
        return {"text": response, "a2ui": a2ui_messages}

    def close_all(self) -> None:
        """Clear all sessions from the registry."""
        self._sessions.clear()
