"""Durable, atomic ownership claims for scoped ingestion sessions."""

from __future__ import annotations

import asyncio
import sqlite3
import threading
from pathlib import Path
from typing import Literal


RecoveryReservationResult = Literal[
    "reserved", "existing_reservation", "committed", "conflict"
]


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
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                """CREATE TABLE IF NOT EXISTS session_claims (
                    session_id TEXT PRIMARY KEY, contributor_id TEXT NOT NULL,
                    workspace TEXT NOT NULL, reservation_token TEXT
                )"""
            )
            columns = {
                row[1] for row in db.execute("PRAGMA table_info(session_claims)")
            }
            if "reservation_token" not in columns:
                db.execute(
                    "ALTER TABLE session_claims ADD COLUMN reservation_token TEXT"
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
                        """SELECT contributor_id, workspace, reservation_token
                           FROM session_claims WHERE session_id=?""",
                        (session_id,),
                    ).fetchone()
                    if row is None:
                        db.execute(
                            """INSERT INTO session_claims(
                                session_id, contributor_id, workspace, reservation_token
                            ) VALUES (?, ?, ?, NULL)""",
                            (session_id, contributor_id, workspace),
                        )
                        return True
                    return row[2] is None and row[:2] == (contributor_id, workspace)

        return await asyncio.to_thread(_claim)

    async def reserve_recovery(
        self,
        session_id: str,
        contributor_id: str,
        workspace: str,
        reservation_token: str,
    ) -> RecoveryReservationResult:
        """Create or inspect the exact provisional claim for a recovery origin."""

        def _reserve() -> RecoveryReservationResult:
            with self._lock:
                self._initialize_locked()
                with sqlite3.connect(self.path, timeout=30) as db:
                    db.execute("BEGIN IMMEDIATE")
                    row = db.execute(
                        """SELECT contributor_id, workspace, reservation_token
                           FROM session_claims WHERE session_id=?""",
                        (session_id,),
                    ).fetchone()
                    if row is None:
                        db.execute(
                            """INSERT INTO session_claims(
                                session_id, contributor_id, workspace, reservation_token
                            ) VALUES (?, ?, ?, ?)""",
                            (session_id, contributor_id, workspace, reservation_token),
                        )
                        return "reserved"
                    if row[:2] != (contributor_id, workspace):
                        return "conflict"
                    if row[2] is None:
                        return "committed"
                    if row[2] == reservation_token:
                        return "existing_reservation"
                    return "conflict"

        return await asyncio.to_thread(_reserve)

    async def finalize_recovery(
        self,
        session_id: str,
        contributor_id: str,
        workspace: str,
        reservation_token: str,
    ) -> bool:
        """Commit the exact recovery reservation without replacing another claim."""

        def _finalize() -> bool:
            with self._lock:
                self._initialize_locked()
                with sqlite3.connect(self.path, timeout=30) as db:
                    db.execute("BEGIN IMMEDIATE")
                    row = db.execute(
                        """SELECT contributor_id, workspace, reservation_token
                           FROM session_claims WHERE session_id=?""",
                        (session_id,),
                    ).fetchone()
                    if row is None or row[:2] != (contributor_id, workspace):
                        return False
                    if row[2] is None:
                        return True
                    if row[2] != reservation_token:
                        return False
                    db.execute(
                        """UPDATE session_claims SET reservation_token=NULL
                           WHERE session_id=? AND contributor_id=? AND workspace=?
                           AND reservation_token=?""",
                        (session_id, contributor_id, workspace, reservation_token),
                    )
                    return True

        return await asyncio.to_thread(_finalize)

    async def release_recovery(
        self,
        session_id: str,
        contributor_id: str,
        workspace: str,
        reservation_token: str,
    ) -> None:
        """Release only the exact provisional recovery reservation."""

        def _release() -> None:
            with self._lock:
                self._initialize_locked()
                with sqlite3.connect(self.path, timeout=30) as db:
                    db.execute("BEGIN IMMEDIATE")
                    db.execute(
                        """DELETE FROM session_claims
                           WHERE session_id=? AND contributor_id=? AND workspace=?
                           AND reservation_token=?""",
                        (session_id, contributor_id, workspace, reservation_token),
                    )

        await asyncio.to_thread(_release)

    async def get_workspace(self, session_id: str) -> str | None:
        """Resolve a claimed workspace without revealing the owner."""

        def _get() -> str | None:
            with self._lock:
                self._initialize_locked()
                with sqlite3.connect(self.path, timeout=30) as db:
                    row = db.execute(
                        """SELECT workspace FROM session_claims
                           WHERE session_id=? AND reservation_token IS NULL""",
                        (session_id,),
                    ).fetchone()
                    return row[0] if row else None

        return await asyncio.to_thread(_get)
