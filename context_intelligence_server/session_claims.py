"""Durable, atomic ownership claims for scoped ingestion sessions."""

from __future__ import annotations

import asyncio
import sqlite3
import threading
from pathlib import Path
from typing import Literal

from context_intelligence_server.blob_store import AsyncDiskBlobStore
from context_intelligence_server.queue_manager import fold_worker_key


def _storage_components(session_id: str) -> list[tuple[str, str]]:
    """Use the storage implementations' authoritative, pure name derivations."""
    components = [("queue", fold_worker_key(session_id))]
    try:
        components.append(("blob", AsyncDiskBlobStore._fold_session_id(session_id)))
    except ValueError:
        # Historical claims may predate blob component validation. Retain the
        # claim and protect its queue alias without poisoning unrelated backfill.
        pass
    return components


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
            bindings_exist = db.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='storage_bindings'"
            ).fetchone()
            db.execute(
                """CREATE TABLE IF NOT EXISTS storage_bindings (
                    kind TEXT NOT NULL, component TEXT NOT NULL, session_id TEXT,
                    PRIMARY KEY (kind, component)
                )"""
            )
            if not bindings_exist:
                # One additive initialization transaction, bounded Python memory.
                # NULL is a sticky ambiguity marker, never a first-row winner.
                rows = db.execute("SELECT session_id FROM session_claims")
                while batch := rows.fetchmany(256):
                    for (session_id,) in batch:
                        for kind, component in _storage_components(session_id):
                            db.execute(
                                """INSERT INTO storage_bindings VALUES (?, ?, ?)
                                   ON CONFLICT(kind, component) DO UPDATE SET
                                   session_id=CASE
                                     WHEN storage_bindings.session_id=excluded.session_id
                                     THEN excluded.session_id ELSE NULL END""",
                                (kind, component, session_id),
                            )
        self._initialized = True

    @staticmethod
    def _bindings_match(db: sqlite3.Connection, session_id: str) -> bool:
        components = _storage_components(session_id)
        return len(components) == 2 and all(
            db.execute(
                "SELECT session_id FROM storage_bindings WHERE kind=? AND component=?",
                (kind, component),
            ).fetchone() == (session_id,)
            for kind, component in components
        )

    @staticmethod
    def _bind(db: sqlite3.Connection, session_id: str) -> bool:
        components = _storage_components(session_id)
        if len(components) != 2:
            return False
        # Check every identity before inserting any, even across store instances
        # or processes: the caller holds SQLite's IMMEDIATE transaction.
        for kind, component in components:
            row = db.execute(
                "SELECT session_id FROM storage_bindings WHERE kind=? AND component=?",
                (kind, component),
            ).fetchone()
            if row is not None and row != (session_id,):
                return False
        db.executemany(
            "INSERT OR IGNORE INTO storage_bindings VALUES (?, ?, ?)",
            [(kind, component, session_id) for kind, component in components],
        )
        return True

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
                    if row is not None and (
                        row[2] is not None or row[:2] != (contributor_id, workspace)
                    ):
                        return False
                    if not self._bind(db, session_id):
                        return False
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
                    if row is not None and (
                        row[:2] != (contributor_id, workspace)
                        or row[2] not in (None, reservation_token)
                    ):
                        return "conflict"
                    if not self._bind(db, session_id):
                        return "conflict"
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
                    if (
                        row is None
                        or row[:2] != (contributor_id, workspace)
                        or not self._bindings_match(db, session_id)
                    ):
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
                    removed = db.execute(
                        """DELETE FROM session_claims
                           WHERE session_id=? AND contributor_id=? AND workspace=?
                           AND reservation_token=?""",
                        (session_id, contributor_id, workspace, reservation_token),
                    )
                    if removed.rowcount:
                        db.execute(
                            "DELETE FROM storage_bindings WHERE session_id=?",
                            (session_id,),
                        )

        await asyncio.to_thread(_release)

    async def storage_identity_matches(self, session_id: str) -> bool:
        """Whether a committed original unambiguously owns both storage names."""
        def _get() -> bool:
            with self._lock:
                self._initialize_locked()
                with sqlite3.connect(self.path, timeout=30) as db:
                    db.execute("BEGIN")
                    return bool(db.execute(
                        """SELECT 1 FROM session_claims
                           WHERE session_id=? AND reservation_token IS NULL""",
                        (session_id,),
                    ).fetchone()) and self._bindings_match(db, session_id)

        return await asyncio.to_thread(_get)

    async def get_blob_workspace(self, session_id: str) -> str | None:
        """Resolve original/canonical blob spelling through its committed binding."""
        try:
            component = AsyncDiskBlobStore._fold_session_id(session_id)
        except ValueError:
            return None

        def _get() -> str | None:
            with self._lock:
                self._initialize_locked()
                with sqlite3.connect(self.path, timeout=30) as db:
                    row = db.execute(
                        """SELECT c.workspace FROM storage_bindings AS b
                           JOIN session_claims AS c ON c.session_id=b.session_id
                           WHERE b.kind='blob' AND b.component=?
                             AND c.reservation_token IS NULL""",
                        (component,),
                    ).fetchone()
                    return row[0] if row else None

        return await asyncio.to_thread(_get)

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
