"""BlobStore Protocol \u2014 the backend-neutral seam.

The only identity that crosses the boundary is the ``ci-blob://<session_id>/<key>``
URI, carried by :class:`BlobReference`. No ``Path``, on-disk layout, ``dest_dir``,
or ``os.*`` detail appears here or in any value the Protocol returns \u2014 that is
private to a concrete backend (:class:`~context_intelligence_server.blob_store.filesystem.FileSystemBlobStore`
and, later, an Azure equivalent).

This module is backend-neutral by construction: it imports nothing from
``os``, ``pathlib``, or any filesystem library.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

# ---------------------------------------------------------------------------
# BlobNotFoundError \u2014 backend-neutral missing-blob exception (guard #6)
# ---------------------------------------------------------------------------


class BlobNotFoundError(FileNotFoundError):
    """Raised when a blob addressed by a ``ci-blob://`` URI does not exist.

    Subclasses :class:`FileNotFoundError` so existing ``except
    FileNotFoundError`` callers keep working unchanged (zero caller churn).
    The message carries the URI ONLY \u2014 never an on-disk path, container, or
    account \u2014 so a future Azure backend can raise the same type/message
    shape and no caller (or log line) ever learns which backend is in use.
    """


# ---------------------------------------------------------------------------
# BlobReference \u2014 cheap handle: identity + metadata, NO payload, NO Path
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class BlobReference:
    """Cheap handle \u2014 identity + metadata, NO payload, NO Path.

    This is what ``scan()``/``list()`` return and what everything except a
    payload read passes around. It is what gets serialized on the graph (as
    its ``.uri``).
    """

    uri: str  # ci-blob://<session_id>/<key> \u2014 the ONLY address callers use
    session_id: str
    key: str
    size: int  # content length in bytes
    last_modified: float  # epoch seconds: disk st_mtime || azure Last-Modified


# ---------------------------------------------------------------------------
# BlobStore protocol
# ---------------------------------------------------------------------------


@runtime_checkable
class BlobStore(Protocol):
    """Protocol for a session-scoped, URI-addressable blob store.

    100% backend-neutral: the only identity that crosses the boundary is the
    ``ci-blob://`` URI (carried by :class:`BlobReference`). No ``Path``, no
    on-disk layout, no ``dest_dir``, no ``os.*`` \u2014 ever.
    """

    async def write(
        self, session_id: str, key: str, value: dict[str, Any] | list[Any]
    ) -> BlobReference:
        """Persist *value* as JSON and return a :class:`BlobReference`."""
        ...

    def list(self, session_id: str) -> AsyncIterator[BlobReference]:
        """Stream all blob references for *session_id* (one session)."""
        ...

    def scan(self) -> AsyncIterator[BlobReference]:
        """Stream all blob references across ALL sessions."""
        ...

    async def delete(
        self, uri: str, if_unmodified: BlobReference | None = None
    ) -> bool:
        """Delete the blob addressed by *uri*. Idempotent: returns False if absent.

        Args:
            uri: The ``ci-blob://`` URI to delete.
            if_unmodified: When provided, this is a **fenced (compare-and-delete)**
                delete \u2014 the store re-checks the blob's current metadata against
                *if_unmodified* (disk: mtime + size; Azure: ``If-Match`` ETag) and
                refuses (returns ``False``, does NOT delete) if the blob changed
                since *if_unmodified* was observed (e.g. by a `scan()`). When
                ``None`` (default), this is the unconditional idempotent delete.
        """
        ...

    async def read(self, uri: str) -> dict[str, Any] | list[Any]:
        """Resolve *uri* and return the stored value (the sole payload path).

        Raises:
            ValueError: If *uri* does not match the ``ci-blob://`` scheme.
            BlobNotFoundError: If no blob exists for *uri*. Subclasses
                ``FileNotFoundError`` for back-compat; the message carries the
                URI only \u2014 never an on-disk path, container, or account.
        """
        ...
