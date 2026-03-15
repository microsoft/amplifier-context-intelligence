"""Graceful-shutdown drain manager.

DrainManager tracks active sessions and coordinates a clean shutdown by:

1. Refusing new connections once ``start_drain()`` is called.
2. Waiting for all in-flight sessions to call ``unregister()`` before
   signalling completion.
3. Returning ``False`` if the wait exceeds the caller-supplied timeout.
"""

import asyncio


class DrainManager:
    """Coordinate graceful shutdown by draining active sessions.

    Usage::

        dm = DrainManager()

        # When a new session starts:
        if dm.accepting:
            dm.register(session_id)

        # When a session ends:
        dm.unregister(session_id)

        # On SIGTERM:
        clean = await dm.start_drain(timeout=30)
    """

    def __init__(self) -> None:
        self._accepting: bool = True
        self._active: set[str] = set()
        # Event is *set* when there are no active sessions (drained state).
        # It starts set because active_count == 0 at construction time.
        self._drained: asyncio.Event = asyncio.Event()
        self._drained.set()

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def accepting(self) -> bool:
        """True if the manager is still accepting new registrations."""
        return self._accepting

    @property
    def active_count(self) -> int:
        """Number of currently registered (in-flight) sessions."""
        return len(self._active)

    # ------------------------------------------------------------------
    # Session lifecycle
    # ------------------------------------------------------------------

    def register(self, session_id: str) -> None:
        """Register *session_id* as active.

        Clears the drained event so that ``start_drain()`` will wait.
        """
        self._active.add(session_id)
        self._drained.clear()

    def unregister(self, session_id: str) -> None:
        """Remove *session_id* from the active set.

        No-op if *session_id* is not registered.  Sets the drained event
        when the active set becomes empty.
        """
        self._active.discard(session_id)
        if not self._active:
            self._drained.set()

    # ------------------------------------------------------------------
    # Drain
    # ------------------------------------------------------------------

    async def start_drain(self, *, timeout: float) -> bool:
        """Stop accepting new connections and wait for active sessions to finish.

        Args:
            timeout: Maximum seconds to wait for all sessions to unregister.

        Returns:
            ``True`` if all sessions drained within *timeout*,
            ``False`` if the timeout expired with sessions still active.
        """
        self._accepting = False

        if not self._active:
            return True

        try:
            await asyncio.wait_for(self._drained.wait(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            return False
