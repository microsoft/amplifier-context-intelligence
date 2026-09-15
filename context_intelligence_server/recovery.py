"""Native recovery receipt and lease primitives."""

from __future__ import annotations

import asyncio
import hashlib
import json
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Literal

ReceiptResult = Literal["accepted", "duplicate", "conflict", "busy"]
ReceiptState = Literal[
    "pending_enqueue", "enqueued", "committed_pending", "written", "quarantined"
]
_PENDING = ("pending_enqueue", "enqueued", "committed_pending")


class RecoveryReceiptStore:
    """SQLite receipt ledger/outbox. All request I/O runs in a worker thread."""

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
                """CREATE TABLE IF NOT EXISTS recovery_receipts (
                    source_session_id TEXT NOT NULL,
                    source_stream_sha256 TEXT NOT NULL,
                    ordinal INTEGER NOT NULL,
                    source_line_sha256 TEXT NOT NULL,
                    actor TEXT NOT NULL, workspace TEXT NOT NULL,
                    payload_sha256 TEXT NOT NULL, payload TEXT NOT NULL,
                    state TEXT NOT NULL,
                    PRIMARY KEY(source_session_id, source_stream_sha256, ordinal)
                )"""
            )
            # A UNIQUE table constraint is backed by an un-droppable SQLite
            # autoindex, so migrate the table in one transaction instead of
            # attempting DROP INDEX. Lines may repeat at new ordinals.
            schema = db.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' "
                "AND name='recovery_receipts'"
            ).fetchone()[0]
            normalized_schema = "".join(schema.split())
            if (
                "UNIQUE(source_session_id,source_stream_sha256,source_line_sha256)"
                in normalized_schema
            ):
                db.execute(
                    """CREATE TABLE recovery_receipts_migrating (
                        source_session_id TEXT NOT NULL,
                        source_stream_sha256 TEXT NOT NULL,
                        ordinal INTEGER NOT NULL,
                        source_line_sha256 TEXT NOT NULL,
                        actor TEXT NOT NULL, workspace TEXT NOT NULL,
                        payload_sha256 TEXT NOT NULL, payload TEXT NOT NULL,
                        state TEXT NOT NULL,
                        PRIMARY KEY(source_session_id, source_stream_sha256, ordinal)
                    )"""
                )
                db.execute(
                    """INSERT INTO recovery_receipts_migrating
                       SELECT source_session_id, source_stream_sha256, ordinal,
                       source_line_sha256, actor, workspace, payload_sha256,
                       payload, state FROM recovery_receipts"""
                )
                db.execute("DROP TABLE recovery_receipts")
                db.execute(
                    "ALTER TABLE recovery_receipts_migrating RENAME TO recovery_receipts"
                )
            db.execute(
                "CREATE INDEX IF NOT EXISTS recovery_receipts_state_idx "
                "ON recovery_receipts(state)"
            )
            db.execute(
                """CREATE TABLE IF NOT EXISTS recovery_lease_consumptions (
                    lease_id TEXT PRIMARY KEY
                )"""
            )
            # The preliminary state name meant "the record was admitted and
            # should be in the spool"; return it to the outbox so a spool
            # scan can safely establish whether it must be appended.
            db.execute(
                "UPDATE recovery_receipts SET state='pending_enqueue' WHERE state='queued'"
            )
        self._initialized = True

    @staticmethod
    def _identity(origin: dict[str, Any]) -> tuple[str, str, int]:
        return (
            origin["session_id"],
            origin["source_stream_sha256"],
            origin["ordinal"],
        )

    async def admit(
        self,
        origin: dict[str, Any],
        actor: str,
        workspace: str,
        payload: dict[str, Any],
        lease_path: str | Path,
    ) -> ReceiptResult:
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        digest = hashlib.sha256(encoded.encode()).hexdigest()
        identity = self._identity(origin)

        def _admit() -> ReceiptResult:
            with self._lock:
                self._initialize_locked()
                with sqlite3.connect(self.path, timeout=30) as db:
                    db.execute("BEGIN IMMEDIATE")
                    row = db.execute(
                        """SELECT source_line_sha256, actor, workspace, payload_sha256
                           FROM recovery_receipts WHERE source_session_id=?
                           AND source_stream_sha256=? AND ordinal=?""",
                        identity,
                    ).fetchone()
                    if row is not None:
                        return (
                            "duplicate"
                            if row
                            == (origin["source_line_sha256"], actor, workspace, digest)
                            else "conflict"
                        )
                    if db.execute(
                        "SELECT 1 FROM recovery_receipts "
                        "WHERE state IN ('pending_enqueue','enqueued','committed_pending') "
                        "LIMIT 1"
                    ).fetchone():
                        return "busy"
                    lease_id = _allowed_lease_id(lease_path)
                    if lease_id is None:
                        return "busy"
                    try:
                        db.execute(
                            "INSERT INTO recovery_lease_consumptions(lease_id) VALUES (?)",
                            (lease_id,),
                        )
                    except sqlite3.IntegrityError:
                        return "busy"
                    db.execute(
                        "INSERT INTO recovery_receipts VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            *identity,
                            origin["source_line_sha256"],
                            actor,
                            workspace,
                            digest,
                            encoded,
                            "pending_enqueue",
                        ),
                    )
                    return "accepted"

        return await asyncio.to_thread(_admit)

    async def mark(self, origin: dict[str, Any], state: ReceiptState) -> None:
        def _mark() -> None:
            with self._lock:
                self._initialize_locked()
                with sqlite3.connect(self.path, timeout=30) as db:
                    # ``enqueued`` is only a forward transition. A drainer can
                    # finish a just-appended record before its enqueuer resumes;
                    # never let that delayed acknowledgement overwrite
                    # ``committed_pending`` or ``written``.
                    db.execute(
                        """UPDATE recovery_receipts
                           SET state=?, payload=CASE WHEN ?='written' THEN '' ELSE payload END
                           WHERE source_session_id=? AND source_stream_sha256=? AND ordinal=?
                           AND (state='pending_enqueue' OR ? != 'enqueued')""",
                        (state, state, *self._identity(origin), state),
                    )

        await asyncio.to_thread(_mark)

    async def pending_outbox(self) -> list[dict[str, Any]]:
        def _read() -> list[dict[str, Any]]:
            with self._lock:
                self._initialize_locked()
                with sqlite3.connect(self.path, timeout=30) as db:
                    rows = db.execute(
                        """SELECT source_session_id, source_stream_sha256, ordinal,
                           source_line_sha256, actor, workspace, payload, state
                           FROM recovery_receipts
                           WHERE state IN ('pending_enqueue','enqueued','committed_pending')
                           ORDER BY rowid"""
                    ).fetchall()
            return [
                {
                    "origin": {
                        "session_id": row[0],
                        "source_stream_sha256": row[1],
                        "ordinal": row[2],
                        "source_line_sha256": row[3],
                    },
                    "actor": row[4],
                    "workspace": row[5],
                    "payload": json.loads(row[6]),
                    "state": row[7],
                }
                for row in rows
            ]

        return await asyncio.to_thread(_read)

    async def status(self) -> dict[str, int]:
        def _read() -> dict[str, int]:
            with self._lock:
                self._initialize_locked()
                with sqlite3.connect(self.path, timeout=30) as db:
                    return {
                        state: count
                        for state, count in db.execute(
                            "SELECT state, count(*) FROM recovery_receipts GROUP BY state"
                        )
                    }

        return await asyncio.to_thread(_read)

    async def retry_quarantined(self) -> bool:
        def _retry() -> bool:
            with self._lock:
                self._initialize_locked()
                with sqlite3.connect(self.path, timeout=30) as db:
                    db.execute("BEGIN IMMEDIATE")
                    if db.execute(
                        "SELECT 1 FROM recovery_receipts "
                        "WHERE state IN ('pending_enqueue','enqueued','committed_pending') "
                        "LIMIT 1"
                    ).fetchone():
                        return False
                    row = db.execute(
                        "SELECT rowid FROM recovery_receipts WHERE state='quarantined' "
                        "ORDER BY rowid LIMIT 1"
                    ).fetchone()
                    if row is None:
                        return False
                    db.execute(
                        "UPDATE recovery_receipts SET state='pending_enqueue' WHERE rowid=?",
                        row,
                    )
                    return True

        return await asyncio.to_thread(_retry)


def _allowed_lease_id(path: str | Path) -> str | None:
    """Return the allowed, unexpired lease's opaque identifier, if any."""
    try:
        lease = json.loads(Path(path).read_text("utf-8"))
        if (
            isinstance(lease, dict)
            and lease.get("allow") is True
            and isinstance(lease.get("expires_at"), (int, float))
            and lease["expires_at"] > time.time()
            and isinstance(lease.get("lease_id"), str)
            and lease["lease_id"]
        ):
            return lease["lease_id"]
    except (OSError, ValueError, TypeError):
        pass
    return None


def lease_allows(path: str | Path) -> bool:
    """A lease is valid only when explicitly allowed, identified, and unexpired."""
    return _allowed_lease_id(path) is not None
