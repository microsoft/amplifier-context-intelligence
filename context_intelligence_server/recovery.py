"""Native recovery source capabilities, receipts, permits, and lease primitives."""

from __future__ import annotations

import asyncio
import base64
import binascii
import hashlib
import json
import os
import re
import secrets
import sqlite3
import threading
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from context_intelligence_server.queue_manager import is_safe_session_id

ReceiptResult = Literal["accepted", "duplicate", "conflict", "busy"]
PermitResult = Literal[
    "accepted",
    "duplicate",
    "conflict",
    "existing",
    "unavailable",
    "invalid",
    "mismatch",
]
ReceiptClassification = Literal["new", "duplicate", "conflict", "foreign"]
ReceiptState = Literal[
    "pending_enqueue", "enqueued", "committed_pending", "written", "quarantined"
]
_PENDING = ("pending_enqueue", "enqueued", "committed_pending")
_SOURCE_CAPABILITY_BYTES = 32
_SOURCE_CAPABILITY_LENGTH = 43
_SOURCE_HANDLE_DOMAIN = b"context-intelligence/recovery/source-handle/v1\x00"
_DESCRIPTOR_DOMAIN = b"context-intelligence/recovery/source-descriptor/v1\x00"
_LEGACY_HANDLE_DOMAIN = b"context-intelligence/recovery/legacy-source/v1\x00"
_EVENT_IDENTITY_DOMAIN = b"context-intelligence/recovery/event-identity/v1\x00"
_QUARANTINED_SOURCE_SESSION = "quarantined-invalid-source-v1"
_SHA256_HEX = re.compile(r"[0-9a-f]{64}\Z")


def has_only_unicode_scalars(value: Any) -> bool:
    """Return whether a JSON value contains no lone UTF-16 surrogate code points.

    JSON decoders accept escaped lone surrogates even though they cannot be
    encoded as UTF-8.  Inspect both object keys and values recursively before a
    recovery request reaches any canonical JSON encoding or durable state.
    """
    if isinstance(value, str):
        return not any(0xD800 <= ord(character) <= 0xDFFF for character in value)
    if isinstance(value, dict):
        return all(
            has_only_unicode_scalars(key) and has_only_unicode_scalars(child)
            for key, child in value.items()
        )
    if isinstance(value, list):
        return all(has_only_unicode_scalars(child) for child in value)
    return True


def canonical_payload_digest(payload: dict[str, Any]) -> str:
    """Return the SHA-256 of the API's canonical recovery event payload."""
    encoded = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def sanitize_recovery_payload(value: Any) -> Any:
    """Return a JSON-compatible value with local working-directory keys removed."""
    if isinstance(value, dict):
        return {
            key: sanitize_recovery_payload(child)
            for key, child in value.items()
            if key != "working_dir"
        }
    if isinstance(value, list):
        return [sanitize_recovery_payload(child) for child in value]
    return value


def source_handle_from_capability(capability: str | None) -> str | None:
    """Validate the raw, unpadded base64url capability and return its handle.

    Raw source capabilities deliberately never leave this function.  The
    domain-separated handle is safe for durable receipt and queue identity.
    """
    if (
        not isinstance(capability, str)
        or len(capability) != _SOURCE_CAPABILITY_LENGTH
        or "=" in capability
        or any(
            char
            not in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
            for char in capability
        )
    ):
        return None
    try:
        raw = base64.b64decode(capability + "=", altchars=b"-_", validate=True)
    except (binascii.Error, ValueError):
        return None
    if len(raw) != _SOURCE_CAPABILITY_BYTES:
        return None
    return hashlib.sha256(_SOURCE_HANDLE_DOMAIN + raw).hexdigest()


def recovery_event_identity(source_handle: str, ordinal: int) -> str:
    """Return one opaque, replay-stable identity for a stored source record.

    ``source_handle`` is already a server-held, capability-derived digest. The
    raw capability and capture path therefore never enter event or blob IDs.
    """
    if (
        _SHA256_HEX.fullmatch(source_handle) is None
        or not isinstance(ordinal, int)
        or ordinal < 0
    ):
        raise ValueError("invalid recovery event identity inputs")
    encoded = json.dumps(
        {"ordinal": ordinal, "source_handle": source_handle},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return f"recovery-{hashlib.sha256(_EVENT_IDENTITY_DOMAIN + encoded).hexdigest()}"


def source_descriptor_digest(descriptor: dict[str, Any]) -> str:
    """Return the domain-separated digest of an immutable source descriptor."""
    encoded = json.dumps(descriptor, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(_DESCRIPTOR_DOMAIN + encoded).hexdigest()


def legacy_source_descriptor(origin: dict[str, Any]) -> dict[str, Any]:
    """Build the explicit lease-compatibility descriptor from the former origin."""
    return {
        "protocol": "legacy-recovery-lease-v1",
        "session_id": origin.get("session_id", "legacy-unknown"),
        "source_stream_sha256": origin.get("source_stream_sha256", "0" * 64),
        "source_sha256": origin.get("source_stream_sha256", "0" * 64),
        "record_count": 0,
    }


def legacy_source_handle(origin: dict[str, Any]) -> str:
    """Return a namespaced handle for an explicitly enabled legacy lease client."""
    descriptor = legacy_source_descriptor(origin)
    encoded = json.dumps(descriptor, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(_LEGACY_HANDLE_DOMAIN + encoded).hexdigest()


def _quarantined_source_descriptor() -> dict[str, Any]:
    """A path-free descriptor for a legacy source that must never be replayed."""
    return {
        "protocol": "native-recovery-source-v1",
        "session_id": _QUARANTINED_SOURCE_SESSION,
        "source_stream_sha256": "0" * 64,
        "source_sha256": "0" * 64,
        "record_count": 0,
    }


def _valid_source_descriptor(value: Any) -> bool:
    """Strictly recognize only the path-free v1 descriptor persisted by this server."""
    return (
        isinstance(value, dict)
        and set(value)
        == {
            "protocol",
            "session_id",
            "source_stream_sha256",
            "source_sha256",
            "record_count",
        }
        and value["protocol"]
        in {"native-recovery-source-v1", "legacy-recovery-lease-v1"}
        and is_safe_session_id(value["session_id"])
        and isinstance(value["source_stream_sha256"], str)
        and isinstance(value["source_sha256"], str)
        and _SHA256_HEX.fullmatch(value["source_stream_sha256"]) is not None
        and _SHA256_HEX.fullmatch(value["source_sha256"]) is not None
        and isinstance(value["record_count"], int)
        and value["record_count"] >= 0
    )


def _sanitize_stored_payload(value: Any) -> tuple[dict[str, Any], bool]:
    """Scrub paths and identify payloads that cannot safely enter a queue."""
    sanitized = sanitize_recovery_payload(value)
    invalid = sanitized != value
    if not isinstance(sanitized, dict):
        return {}, True
    data = sanitized.get("data")
    if not isinstance(data, dict) or not is_safe_session_id(data.get("session_id")):
        invalid = True
        sanitized = dict(sanitized)
        if isinstance(data, dict):
            data = dict(data)
            data.pop("session_id", None)
            sanitized["data"] = data
    return sanitized, invalid


@dataclass(frozen=True)
class _PermitBinding:
    actor: str
    workspace: str
    source_handle: str
    descriptor_sha256: str
    ordinal: int
    source_line_sha256: str
    payload_sha256: str


class RecoveryAdmissionGate:
    """Runtime-only, live-first permit authority for one server generation."""

    def __init__(self, ttl_seconds: int) -> None:
        self._ttl_seconds = ttl_seconds
        self._lock = asyncio.Lock()
        self._permits: dict[str, tuple[_PermitBinding, float]] = {}

    @staticmethod
    def binding(
        source_handle: str,
        descriptor_sha256: str,
        origin: dict[str, Any],
        actor: str,
        workspace: str,
        payload_sha256: str,
    ) -> _PermitBinding:
        """Create the complete source-capability binding for one event."""
        return _PermitBinding(
            actor=actor,
            workspace=workspace,
            source_handle=source_handle,
            descriptor_sha256=descriptor_sha256,
            ordinal=origin["ordinal"],
            source_line_sha256=origin["source_line_sha256"],
            payload_sha256=payload_sha256,
        )

    def _discard_expired(self) -> None:
        now = time.monotonic()
        self._permits = {
            token: stored for token, stored in self._permits.items() if stored[1] > now
        }

    async def issue(
        self,
        binding: _PermitBinding,
        available: Callable[[], Awaitable[bool]],
        receipt_state: Callable[[], Awaitable[ReceiptClassification]] | None = None,
    ) -> tuple[str, float] | None:
        """Issue one permit only while this source-local event is available."""
        async with self._lock:
            self._discard_expired()
            receipt = await receipt_state() if receipt_state is not None else "new"
            live_available = await available()
            if self._permits or receipt != "new" or not live_available:
                return None
            token = secrets.token_urlsafe(32)
            self._permits[token] = (
                binding,
                time.monotonic() + self._ttl_seconds,
            )
            return token, time.time() + self._ttl_seconds

    @asynccontextmanager
    async def consume(
        self,
        token: str | None,
        binding: _PermitBinding,
        available: Callable[[], Awaitable[bool]],
        receipt_state: Callable[[], Awaitable[ReceiptClassification]] | None = None,
    ) -> AsyncIterator[PermitResult]:
        """Consume a matching permit while retaining the live enqueue gate."""
        async with self._lock:
            self._discard_expired()
            state = await receipt_state() if receipt_state is not None else "new"
            live_available = await available()
            if state == "duplicate":
                yield "duplicate"
                return
            if state == "conflict":
                yield "conflict"
                return
            if state != "new":
                yield "existing"
                return
            stored = self._permits.get(token or "")
            if stored is None:
                yield "invalid"
                return
            if stored[0] != binding:
                yield "mismatch"
                return
            if not live_available:
                yield "unavailable"
                return
            del self._permits[token or ""]
            yield "accepted"

    @asynccontextmanager
    async def live_enqueue(self) -> AsyncIterator[None]:
        """Serialize a normal durable live append with recovery admission."""
        async with self._lock:
            yield


class RecoveryReceiptStore:
    """SQLite receipt ledger/outbox keyed only by source handle and ordinal."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self._lock = threading.Lock()
        self._initialized = False

    @staticmethod
    def _legacy_context(
        origin: dict[str, Any],
    ) -> tuple[str, dict[str, Any], dict[str, Any]]:
        descriptor = legacy_source_descriptor(origin)
        return (
            legacy_source_handle(origin),
            descriptor,
            {
                "ordinal": origin["ordinal"],
                "source_line_sha256": origin["source_line_sha256"],
            },
        )

    def _initialize_locked(self) -> None:
        if self._initialized:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        columns: set[str] = set()
        with sqlite3.connect(self.path, timeout=30) as db:
            columns = {
                row[1] for row in db.execute("PRAGMA table_info(recovery_receipts)")
            }
        if columns and "source_handle" not in columns:
            self._rebuild_legacy_database_locked(columns)
        elif columns and self._current_database_needs_sanitization_locked():
            self._rebuild_current_database_locked()
        with sqlite3.connect(self.path, timeout=30) as db:
            self._create_current_schema_locked(db)
        self._initialized = True

    @staticmethod
    def _create_current_schema_locked(db: sqlite3.Connection) -> None:
        """Create the complete current schema without retaining old table pages."""
        db.execute(
            """CREATE TABLE IF NOT EXISTS recovery_sources (
                source_handle TEXT PRIMARY KEY,
                descriptor_sha256 TEXT NOT NULL,
                descriptor TEXT NOT NULL
            )"""
        )
        db.execute(
            """CREATE TABLE IF NOT EXISTS recovery_receipts (
                source_handle TEXT NOT NULL,
                ordinal INTEGER NOT NULL,
                source_line_sha256 TEXT NOT NULL,
                actor TEXT NOT NULL, workspace TEXT NOT NULL,
                payload_sha256 TEXT NOT NULL, payload TEXT NOT NULL,
                state TEXT NOT NULL,
                PRIMARY KEY(source_handle, ordinal),
                FOREIGN KEY(source_handle) REFERENCES recovery_sources(source_handle)
            )"""
        )
        db.execute(
            """CREATE TABLE IF NOT EXISTS recovery_source_conflicts (
                source_handle TEXT NOT NULL,
                conflict_kind TEXT NOT NULL,
                ordinal INTEGER NOT NULL,
                stored_descriptor_sha256 TEXT,
                received_descriptor_sha256 TEXT,
                stored_line_sha256 TEXT,
                received_line_sha256 TEXT,
                stored_payload_sha256 TEXT,
                received_payload_sha256 TEXT,
                PRIMARY KEY(source_handle, conflict_kind, ordinal)
            )"""
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
        db.execute(
            "UPDATE recovery_receipts SET state='pending_enqueue' WHERE state='queued'"
        )

    @staticmethod
    def _fsync_path(path: Path) -> None:
        """Durably flush a file or directory before an atomic migration swap."""
        fd = os.open(path, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    @staticmethod
    def _remove_sqlite_sidecars(path: Path) -> None:
        """Remove SQLite artifacts only after WAL has been safely checkpointed."""
        for suffix in ("-wal", "-shm", "-journal"):
            try:
                path.with_name(f"{path.name}{suffix}").unlink()
            except FileNotFoundError:
                pass

    def _rebuild_legacy_database_locked(self, columns: set[str]) -> None:
        """Replace an old receipt DB with a sanitized, current-schema database.

        A raw old receipt cannot be associated with a newly introduced source
        capability.  It therefore remains reachable only through the explicit
        lease compatibility namespace. Unlike a table-copy migration, this
        rebuild never leaves old payload bytes in free pages or a WAL belonging
        to the final database. The old database is left untouched unless the
        complete replacement is fsynced and ready for one atomic ``replace``.
        """
        required = {
            "source_session_id",
            "source_stream_sha256",
            "ordinal",
            "source_line_sha256",
            "actor",
            "workspace",
            "payload_sha256",
            "payload",
            "state",
        }
        if not required.issubset(columns):
            raise RuntimeError("unsupported recovery receipt schema")
        temporary = self.path.with_name(
            f".{self.path.name}.source-v1-{secrets.token_hex(16)}.tmp"
        )
        if temporary.exists():
            raise RuntimeError("recovery receipt migration temporary path exists")

        source = sqlite3.connect(self.path, timeout=30)
        replaced = False
        try:
            source.execute("PRAGMA busy_timeout=30000")
            checkpoint = source.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
            if checkpoint is None or checkpoint[0] != 0:
                raise RuntimeError("recovery receipt WAL checkpoint unavailable")
            # Keep the legacy database in WAL mode while it is read. Switching
            # journal modes requires an exclusive lock that an otherwise-idle
            # legacy reader may still hold, so it needlessly prevents a
            # restart-safe migration. BEGIN EXCLUSIVE still blocks writers
            # while the point-in-time copy is made.
            source.execute("BEGIN EXCLUSIVE")

            source_tables = {
                row[0]
                for row in source.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }
            if source_tables - {"recovery_receipts", "recovery_lease_consumptions"}:
                raise RuntimeError("unsupported recovery receipt database tables")
            has_lease_consumptions = "recovery_lease_consumptions" in source_tables
            if has_lease_consumptions:
                lease_columns = {
                    row[1]
                    for row in source.execute(
                        "PRAGMA table_info(recovery_lease_consumptions)"
                    )
                }
                if lease_columns != {"lease_id"}:
                    raise RuntimeError("unsupported recovery lease consumption schema")

            receipt_rows = source.execute(
                """SELECT source_session_id, source_stream_sha256, ordinal,
                          source_line_sha256, actor, workspace, payload_sha256,
                          payload, state FROM recovery_receipts"""
            )
            with sqlite3.connect(temporary, timeout=30) as destination:
                destination.execute("PRAGMA journal_mode=DELETE")
                destination.execute("PRAGMA synchronous=FULL")
                destination.execute("PRAGMA secure_delete=ON")
                destination.execute("PRAGMA foreign_keys=ON")
                self._create_current_schema_locked(destination)
                destination.commit()
                destination.execute("BEGIN IMMEDIATE")
                for row in receipt_rows:
                    if (
                        not isinstance(row[0], str)
                        or not isinstance(row[1], str)
                        or not isinstance(row[2], int)
                        or not isinstance(row[3], str)
                        or not isinstance(row[4], str)
                        or not isinstance(row[5], str)
                        or not isinstance(row[6], str)
                        or not isinstance(row[7], str)
                        or row[8] not in (*_PENDING, "written", "quarantined", "queued")
                    ):
                        raise RuntimeError("invalid legacy recovery receipt")
                    origin = {
                        "session_id": row[0],
                        "source_stream_sha256": row[1],
                        "ordinal": row[2],
                        "source_line_sha256": row[3],
                    }
                    handle, descriptor, event_origin = self._legacy_context(origin)
                    invalid_source = not is_safe_session_id(row[0])
                    if invalid_source:
                        descriptor = _quarantined_source_descriptor()
                    descriptor_json = json.dumps(
                        descriptor, sort_keys=True, separators=(",", ":")
                    )
                    descriptor_hash = source_descriptor_digest(descriptor)
                    destination.execute(
                        "INSERT OR IGNORE INTO recovery_sources VALUES (?, ?, ?)",
                        (handle, descriptor_hash, descriptor_json),
                    )
                    payload = row[7]
                    state = "pending_enqueue" if row[8] == "queued" else row[8]
                    invalid_payload = False
                    if payload:
                        try:
                            decoded = json.loads(payload)
                        except (TypeError, ValueError) as exc:
                            raise RuntimeError(
                                "malformed legacy recovery receipt payload"
                            ) from exc
                        if not isinstance(decoded, dict):
                            raise RuntimeError(
                                "malformed legacy recovery receipt payload"
                            )
                        decoded, invalid_payload = _sanitize_stored_payload(decoded)
                        payload = json.dumps(
                            decoded, sort_keys=True, separators=(",", ":")
                        )
                        payload_hash = canonical_payload_digest(decoded)
                    elif state == "written":
                        payload_hash = row[6]
                    else:
                        raise RuntimeError(
                            "legacy pending recovery receipt lacks payload"
                        )
                    if state in _PENDING and (invalid_source or invalid_payload):
                        state = "quarantined"
                    destination.execute(
                        "INSERT INTO recovery_receipts VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            handle,
                            event_origin["ordinal"],
                            event_origin["source_line_sha256"],
                            row[4],
                            row[5],
                            payload_hash,
                            payload,
                            state,
                        ),
                    )
                if has_lease_consumptions:
                    for (lease_id,) in source.execute(
                        "SELECT lease_id FROM recovery_lease_consumptions"
                    ):
                        if not isinstance(lease_id, str) or not lease_id:
                            raise RuntimeError("invalid recovery lease consumption")
                        destination.execute(
                            "INSERT INTO recovery_lease_consumptions VALUES (?)",
                            (lease_id,),
                        )
                destination.commit()
            self._fsync_path(temporary)
            # Do not leave a WAL sidecar containing legacy payloads beside the
            # replacement. Closing first prevents this connection from
            # re-creating it after cleanup.
            source.close()
            source = None
            self._remove_sqlite_sidecars(self.path)
            self._fsync_path(self.path.parent)
            os.replace(temporary, self.path)
            replaced = True
            self._fsync_path(self.path.parent)
        finally:
            if source is not None:
                source.close()
            if not replaced:
                self._remove_sqlite_sidecars(temporary)
                try:
                    temporary.unlink()
                except FileNotFoundError:
                    pass

    def _current_database_needs_sanitization_locked(self) -> bool:
        """Detect persisted values which cannot safely be used as queue keys."""
        try:
            with sqlite3.connect(self.path, timeout=30) as db:
                source_rows = db.execute(
                    "SELECT descriptor FROM recovery_sources"
                ).fetchall()
                for (encoded,) in source_rows:
                    try:
                        if not _valid_source_descriptor(json.loads(encoded)):
                            return True
                    except (TypeError, ValueError):
                        return True
                receipt_rows = db.execute(
                    "SELECT payload, state FROM recovery_receipts"
                ).fetchall()
                for payload, state in receipt_rows:
                    if not payload:
                        if state in _PENDING:
                            return True
                        continue
                    try:
                        _payload, invalid = _sanitize_stored_payload(
                            json.loads(payload)
                        )
                    except (TypeError, ValueError):
                        return True
                    if invalid:
                        return True
        except sqlite3.DatabaseError as exc:
            raise RuntimeError("invalid recovery receipt database") from exc
        return False

    def _rebuild_current_database_locked(self) -> None:
        """Atomically scrub unsafe current-schema rows into a fresh database.

        A terminal unsafe receipt must not reach QueueManager, and an in-place
        UPDATE would leave its path-like bytes in free pages or a WAL. Copying
        only validated/sanitized values to a new DB gives both properties.
        """
        temporary = self.path.with_name(
            f".{self.path.name}.source-v1-scrub-{secrets.token_hex(16)}.tmp"
        )
        if temporary.exists():
            raise RuntimeError("recovery receipt scrub temporary path exists")
        source: sqlite3.Connection | None = sqlite3.connect(self.path, timeout=30)
        replaced = False
        try:
            source.execute("PRAGMA busy_timeout=30000")
            checkpoint = source.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
            if checkpoint is None or checkpoint[0] != 0:
                raise RuntimeError("recovery receipt WAL checkpoint unavailable")
            source.execute("BEGIN EXCLUSIVE")
            tables = {
                row[0]
                for row in source.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }
            expected = {
                "recovery_sources",
                "recovery_receipts",
                "recovery_source_conflicts",
                "recovery_lease_consumptions",
            }
            if not {"recovery_sources", "recovery_receipts"}.issubset(tables) or (
                tables - expected
            ):
                raise RuntimeError("unsupported recovery receipt database tables")
            source_rows = source.execute(
                "SELECT source_handle, descriptor FROM recovery_sources"
            ).fetchall()
            receipt_rows = source.execute(
                """SELECT source_handle, ordinal, source_line_sha256, actor, workspace,
                          payload_sha256, payload, state FROM recovery_receipts"""
            ).fetchall()
            conflict_rows = (
                source.execute(
                    """SELECT source_handle, conflict_kind, ordinal,
                              stored_descriptor_sha256, received_descriptor_sha256,
                              stored_line_sha256, received_line_sha256,
                              stored_payload_sha256, received_payload_sha256
                       FROM recovery_source_conflicts"""
                ).fetchall()
                if "recovery_source_conflicts" in tables
                else []
            )
            lease_rows = (
                source.execute(
                    "SELECT lease_id FROM recovery_lease_consumptions"
                ).fetchall()
                if "recovery_lease_consumptions" in tables
                else []
            )
            with sqlite3.connect(temporary, timeout=30) as destination:
                destination.execute("PRAGMA journal_mode=DELETE")
                destination.execute("PRAGMA synchronous=FULL")
                destination.execute("PRAGMA foreign_keys=ON")
                self._create_current_schema_locked(destination)
                destination.commit()
                destination.execute("BEGIN IMMEDIATE")
                source_invalid: dict[str, bool] = {}
                for source_handle, encoded in source_rows:
                    if (
                        not isinstance(source_handle, str)
                        or _SHA256_HEX.fullmatch(source_handle) is None
                    ):
                        raise RuntimeError("invalid recovery source handle")
                    try:
                        descriptor = json.loads(encoded)
                    except (TypeError, ValueError):
                        descriptor = None
                    invalid = not _valid_source_descriptor(descriptor)
                    if invalid:
                        descriptor = _quarantined_source_descriptor()
                    # _valid_source_descriptor first requires a dict, so a
                    # non-quarantined descriptor is safe to serialize.
                    assert isinstance(descriptor, dict)
                    source_invalid[source_handle] = invalid
                    destination.execute(
                        "INSERT INTO recovery_sources VALUES (?, ?, ?)",
                        (
                            source_handle,
                            source_descriptor_digest(descriptor),
                            json.dumps(
                                descriptor, sort_keys=True, separators=(",", ":")
                            ),
                        ),
                    )
                for row in receipt_rows:
                    (
                        source_handle,
                        ordinal,
                        line_sha256,
                        actor,
                        workspace,
                        payload_sha256,
                        encoded,
                        state,
                    ) = row
                    if (
                        source_handle not in source_invalid
                        or not isinstance(ordinal, int)
                        or ordinal < 0
                        or not isinstance(line_sha256, str)
                        or _SHA256_HEX.fullmatch(line_sha256) is None
                        or not isinstance(actor, str)
                        or not isinstance(workspace, str)
                        or not isinstance(payload_sha256, str)
                        or _SHA256_HEX.fullmatch(payload_sha256) is None
                        or state not in (*_PENDING, "written", "quarantined", "queued")
                    ):
                        raise RuntimeError("invalid recovery receipt")
                    invalid_payload = False
                    if encoded:
                        try:
                            payload, invalid_payload = _sanitize_stored_payload(
                                json.loads(encoded)
                            )
                        except (TypeError, ValueError):
                            payload, invalid_payload = {}, True
                        encoded = json.dumps(
                            payload, sort_keys=True, separators=(",", ":")
                        )
                        payload_sha256 = canonical_payload_digest(payload)
                    elif state != "written":
                        invalid_payload = True
                    state = "pending_enqueue" if state == "queued" else state
                    if state in _PENDING and (
                        source_invalid[source_handle] or invalid_payload
                    ):
                        state = "quarantined"
                    destination.execute(
                        "INSERT INTO recovery_receipts VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            source_handle,
                            ordinal,
                            line_sha256,
                            actor,
                            workspace,
                            payload_sha256,
                            encoded,
                            state,
                        ),
                    )
                for row in conflict_rows:
                    if (
                        not isinstance(row[0], str)
                        or _SHA256_HEX.fullmatch(row[0]) is None
                        or not isinstance(row[1], str)
                        or not isinstance(row[2], int)
                        or row[2] < -1
                        or any(
                            value is not None
                            and (
                                not isinstance(value, str)
                                or _SHA256_HEX.fullmatch(value) is None
                            )
                            for value in row[3:]
                        )
                    ):
                        raise RuntimeError("invalid recovery conflict metadata")
                    destination.execute(
                        "INSERT INTO recovery_source_conflicts VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        row,
                    )
                for (lease_id,) in lease_rows:
                    if not isinstance(lease_id, str) or not lease_id:
                        raise RuntimeError("invalid recovery lease consumption")
                    destination.execute(
                        "INSERT INTO recovery_lease_consumptions VALUES (?)",
                        (lease_id,),
                    )
                destination.commit()
            self._fsync_path(temporary)
            source.close()
            source = None
            self._remove_sqlite_sidecars(self.path)
            self._fsync_path(self.path.parent)
            os.replace(temporary, self.path)
            replaced = True
            self._fsync_path(self.path.parent)
        finally:
            if source is not None:
                source.close()
            if not replaced:
                self._remove_sqlite_sidecars(temporary)
                try:
                    temporary.unlink()
                except FileNotFoundError:
                    pass

    @staticmethod
    def _record_conflict(
        db: sqlite3.Connection,
        source_handle: str,
        kind: str,
        ordinal: int,
        *,
        stored_descriptor: str | None = None,
        received_descriptor: str | None = None,
        stored_line: str | None = None,
        received_line: str | None = None,
        stored_payload: str | None = None,
        received_payload: str | None = None,
    ) -> None:
        db.execute(
            """INSERT INTO recovery_source_conflicts VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(source_handle, conflict_kind, ordinal) DO UPDATE SET
                 received_descriptor_sha256=COALESCE(
                   excluded.received_descriptor_sha256,
                   recovery_source_conflicts.received_descriptor_sha256
                 ),
                 received_line_sha256=COALESCE(
                   excluded.received_line_sha256,
                   recovery_source_conflicts.received_line_sha256
                 ),
                 received_payload_sha256=COALESCE(
                   excluded.received_payload_sha256,
                   recovery_source_conflicts.received_payload_sha256
                 )""",
            (
                source_handle,
                kind,
                ordinal,
                stored_descriptor,
                received_descriptor,
                stored_line,
                received_line,
                stored_payload,
                received_payload,
            ),
        )

    @staticmethod
    def _source_state(
        db: sqlite3.Connection, source_handle: str, descriptor: dict[str, Any]
    ) -> tuple[ReceiptClassification, str]:
        descriptor_hash = source_descriptor_digest(descriptor)
        row = db.execute(
            "SELECT descriptor_sha256 FROM recovery_sources WHERE source_handle=?",
            (source_handle,),
        ).fetchone()
        if row is not None and row[0] != descriptor_hash:
            RecoveryReceiptStore._record_conflict(
                db,
                source_handle,
                "descriptor",
                -1,
                stored_descriptor=row[0],
                received_descriptor=descriptor_hash,
            )
            return "conflict", descriptor_hash
        return "new", descriptor_hash

    async def classify_source(
        self,
        source_handle: str,
        descriptor: dict[str, Any],
        origin: dict[str, Any],
        actor: str,
        workspace: str,
        payload_sha256: str,
    ) -> ReceiptClassification:
        """Classify only within a capability-derived source namespace."""

        def _classify() -> ReceiptClassification:
            with self._lock:
                self._initialize_locked()
                with sqlite3.connect(self.path, timeout=30) as db:
                    db.execute("BEGIN IMMEDIATE")
                    source_state, _ = self._source_state(db, source_handle, descriptor)
                    if source_state == "conflict":
                        return "conflict"
                    row = db.execute(
                        """SELECT source_line_sha256, actor, workspace, payload_sha256
                           FROM recovery_receipts WHERE source_handle=? AND ordinal=?""",
                        (source_handle, origin["ordinal"]),
                    ).fetchone()
                    if row is None:
                        return "new"
                    if (
                        row[0] == origin["source_line_sha256"]
                        and row[3] == payload_sha256
                    ):
                        # An exact record belongs only to its admitting actor
                        # and workspace.  Do not turn another holder of the
                        # source capability into a receipt-status oracle.
                        return (
                            "duplicate" if row[1:3] == (actor, workspace) else "foreign"
                        )
                    self._record_conflict(
                        db,
                        source_handle,
                        "ordinal",
                        origin["ordinal"],
                        stored_line=row[0],
                        received_line=origin["source_line_sha256"],
                        stored_payload=row[3],
                        received_payload=payload_sha256,
                    )
                    return "conflict"

        return await asyncio.to_thread(_classify)

    async def admit_source(
        self,
        source_handle: str,
        descriptor: dict[str, Any],
        origin: dict[str, Any],
        actor: str,
        workspace: str,
        payload: dict[str, Any],
        lease_path: str | Path | None = None,
        *,
        require_lease: bool = True,
    ) -> ReceiptResult:
        """Atomically lazily register a source and admit its exact event."""
        payload = sanitize_recovery_payload(payload)
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        digest = canonical_payload_digest(payload)
        descriptor_json = json.dumps(descriptor, sort_keys=True, separators=(",", ":"))

        def _admit() -> ReceiptResult:
            with self._lock:
                self._initialize_locked()
                with sqlite3.connect(self.path, timeout=30) as db:
                    db.execute("BEGIN IMMEDIATE")
                    source_state, descriptor_hash = self._source_state(
                        db, source_handle, descriptor
                    )
                    if source_state == "conflict":
                        return "conflict"
                    row = db.execute(
                        """SELECT source_line_sha256, actor, workspace, payload_sha256
                           FROM recovery_receipts WHERE source_handle=? AND ordinal=?""",
                        (source_handle, origin["ordinal"]),
                    ).fetchone()
                    if row is not None:
                        if row[0] == origin["source_line_sha256"] and row[3] == digest:
                            return (
                                "duplicate"
                                if row[1:3] == (actor, workspace)
                                else "busy"
                            )
                        self._record_conflict(
                            db,
                            source_handle,
                            "ordinal",
                            origin["ordinal"],
                            stored_line=row[0],
                            received_line=origin["source_line_sha256"],
                            stored_payload=row[3],
                            received_payload=digest,
                        )
                        return "conflict"
                    if db.execute(
                        "SELECT 1 FROM recovery_receipts WHERE state IN "
                        "('pending_enqueue','enqueued','committed_pending') LIMIT 1"
                    ).fetchone():
                        return "busy"
                    if require_lease:
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
                        "INSERT OR IGNORE INTO recovery_sources VALUES (?, ?, ?)",
                        (source_handle, descriptor_hash, descriptor_json),
                    )
                    db.execute(
                        "INSERT INTO recovery_receipts VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            source_handle,
                            origin["ordinal"],
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

    # The compatibility methods are intentionally only used by explicit
    # lease-mode callers and older unit-level integrations.
    async def admit(
        self,
        origin: dict[str, Any],
        actor: str,
        workspace: str,
        payload: dict[str, Any],
        lease_path: str | Path | None = None,
        *,
        require_lease: bool = True,
    ) -> ReceiptResult:
        handle, descriptor, event_origin = self._legacy_context(origin)
        return await self.admit_source(
            handle,
            descriptor,
            event_origin,
            actor,
            workspace,
            payload,
            lease_path,
            require_lease=require_lease,
        )

    async def classify(
        self,
        origin: dict[str, Any],
        actor: str,
        workspace: str,
        payload: dict[str, Any],
    ) -> ReceiptClassification:
        handle, descriptor, event_origin = self._legacy_context(origin)
        return await self.classify_source(
            handle,
            descriptor,
            event_origin,
            actor,
            workspace,
            canonical_payload_digest(sanitize_recovery_payload(payload)),
        )

    async def has_pending(self) -> bool:
        def _has_pending() -> bool:
            with self._lock:
                self._initialize_locked()
                with sqlite3.connect(self.path, timeout=30) as db:
                    return bool(
                        db.execute(
                            "SELECT 1 FROM recovery_receipts WHERE state IN "
                            "('pending_enqueue','enqueued','committed_pending') LIMIT 1"
                        ).fetchone()
                    )

        return await asyncio.to_thread(_has_pending)

    async def mark(
        self,
        origin: dict[str, Any],
        state: ReceiptState,
        *,
        source_handle: str | None = None,
    ) -> None:
        if source_handle is None:
            source_handle, _, origin = self._legacy_context(origin)

        def _mark() -> None:
            with self._lock:
                self._initialize_locked()
                with sqlite3.connect(self.path, timeout=30) as db:
                    exists = db.execute(
                        """SELECT 1 FROM recovery_receipts
                           WHERE source_handle=? AND ordinal=?""",
                        (source_handle, origin["ordinal"]),
                    ).fetchone()
                    if exists is None:
                        raise RuntimeError(
                            "recovery receipt terminal transition has no durable receipt"
                        )
                    db.execute(
                        """UPDATE recovery_receipts
                           SET state=?, payload=CASE WHEN ?='written' THEN '' ELSE payload END
                           WHERE source_handle=? AND ordinal=?
                           AND (state='pending_enqueue' OR ? != 'enqueued')""",
                        (state, state, source_handle, origin["ordinal"], state),
                    )

        await asyncio.to_thread(_mark)

    async def pending_outbox(self) -> list[dict[str, Any]]:
        def _read() -> list[dict[str, Any]]:
            with self._lock:
                self._initialize_locked()
                with sqlite3.connect(self.path, timeout=30) as db:
                    rows = db.execute(
                        """SELECT r.source_handle, r.ordinal, r.source_line_sha256,
                                  s.descriptor, r.actor, r.workspace, r.payload, r.state
                           FROM recovery_receipts AS r JOIN recovery_sources AS s
                           ON s.source_handle=r.source_handle
                           WHERE r.state IN ('pending_enqueue','enqueued','committed_pending')
                           ORDER BY r.rowid"""
                    ).fetchall()
            return [
                {
                    "source_handle": row[0],
                    "source": json.loads(row[3]),
                    "origin": {"ordinal": row[1], "source_line_sha256": row[2]},
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
                        "SELECT 1 FROM recovery_receipts WHERE state IN "
                        "('pending_enqueue','enqueued','committed_pending') LIMIT 1"
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


def _allowed_lease_id(path: str | Path | None) -> str | None:
    """Return the allowed, unexpired lease's opaque identifier, if any."""
    if not isinstance(path, (str, Path)):
        return None
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


def lease_allows(path: str | Path | None) -> bool:
    """A lease is valid only when explicitly allowed, identified, and unexpired."""
    return _allowed_lease_id(path) is not None
