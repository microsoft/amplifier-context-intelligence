"""Durable, atomic ownership claims for scoped ingestion sessions."""

from __future__ import annotations

import asyncio
import sqlite3
import threading
from pathlib import Path


class SessionClaimStore:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self._lock = threading.Lock()
        self._initialized = False

    def _initialize_locked(self) -> None:
        if self._initialized:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.path, timeout=30) as db:
            db.execute(
                """CREATE TABLE IF NOT EXISTS session_claims (
                    session_id TEXT PRIMARY KEY, contributor_id TEXT NOT NULL,
                    workspace TEXT NOT NULL
                )"""
            )
        self._initialized = True

    async def claim(self, session_id: str, contributor_id: str, workspace: str) -> bool:
        """Create a claim or verify the existing claim matches exactly."""

        def _claim() -> bool:
            with self._lock:
                self._initialize_locked()
                with sqlite3.connect(self.path, timeout=30) as db:
                    db.execute("BEGIN IMMEDIATE")
                    row = db.execute(
                        "SELECT contributor_id, workspace FROM session_claims WHERE session_id=?",
                        (session_id,),
                    ).fetchone()
                    if row is None:
                        db.execute(
                            "INSERT INTO session_claims VALUES (?, ?, ?)",
                            (session_id, contributor_id, workspace),
                        )
                        return True
                    return row == (contributor_id, workspace)

        return await asyncio.to_thread(_claim)

    async def get_workspace(self, session_id: str) -> str | None:
        """Resolve a claimed workspace without revealing the owner."""

        def _get() -> str | None:
            with self._lock:
                self._initialize_locked()
                with sqlite3.connect(self.path, timeout=30) as db:
                    row = db.execute(
                        "SELECT workspace FROM session_claims WHERE session_id=?",
                        (session_id,),
                    ).fetchone()
                    return row[0] if row else None

        return await asyncio.to_thread(_get)
