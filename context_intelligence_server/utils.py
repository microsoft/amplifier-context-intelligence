"""Shared utilities for context-intelligence handlers."""

from __future__ import annotations

import hashlib
import logging
from contextvars import ContextVar, Token
from datetime import datetime, timezone
from typing import Any

# Persisted only in server-owned queue envelopes.  It is deliberately absent
# from the EventRequest model: clients do not choose event identity.
SPOOL_EVENT_IDENTITY = "_ci_event_identity"
_event_identity: ContextVar[str | None] = ContextVar(
    "context_intelligence_event_identity", default=None
)

# --- shared byte-length folding -------------------------------------------
#
# Two independent identifier families derive filesystem names with no
# length bound: QueueManager's worker keys (queue_manager.py's
# fold_worker_key) and AsyncDiskBlobStore's session ids / blob keys
# (blob_store.py). Both hit the SAME underlying incident shape -- a
# deeply-nested sub-agent session id makes a derived name exceed the
# filesystem's NAME_MAX (255 bytes) -- so the folding algorithm lives here
# ONCE, parameterised by the caller's own worst-case suffix budget.
_FOLD_DIGEST_CHARS = 16
_FOLD_SEP = "~"


def fold_name(value: str, max_bytes: int) -> str:
    """Fold *value* to fit within *max_bytes* UTF-8 bytes, or return it unchanged.

    A value at or under the budget is returned BYTE-IDENTICAL -- this is
    the back-compat guarantee for every caller: an identifier already in
    use today (queue worker key, blob session id, blob key) is under its
    budget, so every existing on-disk name keeps working and this function
    is a no-op for it.

    A value over budget folds to ``<truncated-prefix>~<16-hex-digest>``,
    sized to land AT *max_bytes* bytes (exactly, for any prefix that
    doesn't need multi-byte truncation -- e.g. every ASCII value; a few
    bytes under when the cut point would otherwise split a codepoint, see
    below):

    - The digest is the first 16 hex characters of
      ``sha256(value.encode("utf-8")).hexdigest()``, computed over the
      WHOLE original value -- so two long values sharing a common prefix
      still fold to different names.
    - The prefix is truncated on a UTF-8 character boundary: a multi-byte
      codepoint straddling the cut point is dropped WHOLE, never split, so
      the result is always valid UTF-8 (this is the only case where the
      folded length lands under, not at, the budget).
    - Any ``/``, ``\\``, or ``\\0`` surviving into the prefix is replaced
      with ``_`` -- the characters every caller here rejects as unsafe --
      so a folded value can never be filesystem-unsafe even when the
      original (long) value was.

    Deterministic and IDEMPOTENT: a folded value is always <= the budget,
    so ``fold_name(fold_name(v, n), n) == fold_name(v, n)`` for every
    ``v``/``n`` -- a respawned drainer, a boot recovery scan, a blob
    write, and a blob read of an already-folded URI all compute the same
    name from the same input.
    """
    encoded = value.encode("utf-8")
    if len(encoded) <= max_bytes:
        return value
    digest = hashlib.sha256(encoded).hexdigest()[:_FOLD_DIGEST_CHARS]
    suffix = _FOLD_SEP + digest
    prefix_budget = max_bytes - len(suffix.encode("utf-8"))
    truncated = encoded[:prefix_budget]
    while truncated:
        try:
            prefix = truncated.decode("utf-8")
            break
        except UnicodeDecodeError:
            truncated = truncated[:-1]
    else:
        prefix = ""
    prefix = prefix.replace("/", "_").replace("\\", "_").replace("\0", "_")
    return prefix + suffix


def set_event_identity(identity: str | None) -> Token[str | None]:
    """Scope a persisted event identity to the current processing task."""
    return _event_identity.set(identity)


def reset_event_identity(token: Token[str | None]) -> None:
    """Restore the event-identity scope established by ``set_event_identity``."""
    _event_identity.reset(token)


def make_node_id(
    session_id: str,
    event_name: str,
    timestamp: str,
    disambiguator: str | None = None,
) -> str:
    """Generate a deterministic, filesystem-safe node ID from event data.

    Pattern: {session_id}__{safe_event}__{timestamp_ms}
    With disambiguator: {session_id}__{safe_event}__{timestamp_ms}__{disambiguator}
    While processing a newly accepted, stamped queue record, a final
    server-owned event-identity suffix is appended.  Direct and historical
    unscoped callers retain the legacy output exactly.

    Colons in *event_name* are replaced with underscores so the ID is safe
    for use as a filename component.  Parses ISO-8601 timestamps (with
    fractional seconds and timezone offsets) and converts to epoch
    milliseconds.

    The optional *disambiguator* (e.g. tool_call_id) is appended as a fourth
    segment when provided.  When omitted, the format is unchanged — full
    backward compatibility.
    """
    safe_event = event_name.replace(":", "_")
    try:
        dt = datetime.fromisoformat(timestamp)
    except ValueError as exc:
        raise ValueError(
            f"make_node_id: invalid/empty timestamp {timestamp!r} for event "
            f"{event_name!r} (session {session_id!r})"
        ) from exc
    epoch_ms = int(dt.astimezone(timezone.utc).timestamp() * 1000)
    node_id = f"{session_id}__{safe_event}__{epoch_ms}"
    if disambiguator is not None:
        node_id = f"{node_id}__{disambiguator}"
    event_identity = _event_identity.get()
    if event_identity is not None:
        node_id = f"{node_id}__{event_identity}"
    return node_id


def make_edge_id(source_id: str, target_id: str, edge_type: str) -> str:
    """Generate a deterministic edge ID from source, target, and type.

    Pattern: {source_id}==[{edge_type}]=={target_id}

    The ``==[`` and ``]==`` separators never appear in node IDs, so edge
    IDs are always unambiguously parseable back into their three components.
    """
    return f"{source_id}==[{edge_type}]=={target_id}"


class EventLogContext:
    """Log context with handler name, session_id, and event name pre-bound as prefix."""

    def __init__(
        self,
        handler_name: str,
        session_id: str,
        event: str,
        logger: logging.Logger,
    ) -> None:
        self._logger = logger
        self._prefix = f"[{handler_name}] [{session_id}] [{event}]"

    def info(self, message: str, *args: object) -> None:
        """Log an info message with the pre-bound prefix."""
        self._logger.info("%s " + message, self._prefix, *args)

    def warning(self, message: str, *args: object) -> None:
        """Log a warning message with the pre-bound prefix."""
        self._logger.warning("%s " + message, self._prefix, *args)

    def error(self, message: str, *args: object) -> None:
        """Log an error message with the pre-bound prefix."""
        self._logger.error("%s " + message, self._prefix, *args)


class HandlerLogger:
    """Structured logging wrapper that binds handler name to every log call."""

    def __init__(self, handler_name: str, logger: logging.Logger) -> None:
        self._handler_name = handler_name
        self._logger = logger

    def with_event(self, event: str, data: dict[str, Any]) -> EventLogContext:
        """Return an EventLogContext with session_id extracted from data."""
        session_id = data.get("session_id", "")
        return EventLogContext(
            handler_name=self._handler_name,
            session_id=session_id,
            event=event,
            logger=self._logger,
        )
