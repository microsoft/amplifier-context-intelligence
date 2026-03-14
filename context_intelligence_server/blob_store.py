"""AsyncDiskBlobStore — async, disk-backed blob storage with ci-blob:// URIs.

Disk layout:
    <root>/<session-id>/blobs/<key>.json

URI scheme:
    ci-blob://<session-id>/<key>

All filesystem I/O is wrapped with ``asyncio.to_thread`` to keep the event
loop non-blocking.  ``dump()`` is stubbed for Task 2.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

_SCHEME = "ci-blob://"


# ---------------------------------------------------------------------------
# BlobStore protocol
# ---------------------------------------------------------------------------


@runtime_checkable
class BlobStore(Protocol):
    """Protocol for a session-scoped, URI-addressable blob store."""

    async def write(
        self, session_id: str, key: str, value: dict[str, Any] | list[Any]
    ) -> str:
        """Persist *value* as JSON and return a ``ci-blob://`` URI."""
        ...

    async def read(self, uri: str) -> dict[str, Any] | list[Any]:
        """Resolve *uri* and return the stored value.

        Raises:
            ValueError: If *uri* does not match the ``ci-blob://`` scheme.
            FileNotFoundError: If no blob exists at the resolved path.
        """
        ...

    async def list(self, session_id: str) -> list[str]:
        """Return all blob URIs for *session_id*, sorted lexicographically."""
        ...

    async def dump(self, session_id: str) -> dict[str, Any]:
        """Return all blobs for *session_id* as a dict keyed by URI.

        .. note:: Not yet implemented — reserved for Task 2.
        """
        ...


# ---------------------------------------------------------------------------
# AsyncDiskBlobStore
# ---------------------------------------------------------------------------


class AsyncDiskBlobStore:
    """Async, disk-backed implementation of :class:`BlobStore`.

    Args:
        root: Root directory under which all session blobs are stored.
    """

    def __init__(self, root: Path | str) -> None:
        self._root = Path(root)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _make_uri(self, session_id: str, key: str) -> str:
        """Return the canonical ``ci-blob://`` URI for a session/key pair."""
        return f"{_SCHEME}{session_id}/{key}"

    def _parse_uri(self, uri: str) -> tuple[str, str]:
        """Parse a ``ci-blob://`` URI into ``(session_id, key)``.

        Raises:
            ValueError: If *uri* is not a valid ``ci-blob://`` URI.
        """
        if not uri.startswith(_SCHEME):
            raise ValueError(
                f"Invalid URI scheme — expected '{_SCHEME}...', got: {uri!r}"
            )
        remainder = uri[len(_SCHEME) :]
        # remainder must be "<session_id>/<key>" — both parts non-empty
        if "/" not in remainder:
            raise ValueError(f"URI missing key component: {uri!r}")
        session_id, _, key = remainder.partition("/")
        if not session_id or not key:
            raise ValueError(f"URI has empty session_id or key: {uri!r}")
        return session_id, key

    def _blob_path(self, session_id: str, key: str) -> Path:
        """Return the filesystem path for a given session/key blob."""
        return self._root / session_id / "blobs" / f"{key}.json"

    # ------------------------------------------------------------------
    # Public accessors (mirror of internal helpers for external callers)
    # ------------------------------------------------------------------

    def parse_uri(self, uri: str) -> tuple[str, str]:
        """Public alias for :meth:`_parse_uri`."""
        return self._parse_uri(uri)

    def blob_path(self, session_id: str, key: str) -> Path:
        """Public alias for :meth:`_blob_path`."""
        return self._blob_path(session_id, key)

    # ------------------------------------------------------------------
    # Async API
    # ------------------------------------------------------------------

    async def write(
        self, session_id: str, key: str, value: dict[str, Any] | list[Any]
    ) -> str:
        """Persist *value* as JSON and return a ``ci-blob://`` URI.

        Creates the directory ``<root>/<session_id>/blobs/`` if needed.

        Returns:
            A ``ci-blob://<session_id>/<key>`` URI.
        """
        path = self._blob_path(session_id, key)

        def _write() -> None:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(value), encoding="utf-8")

        await asyncio.to_thread(_write)
        return self._make_uri(session_id, key)

    async def read(self, uri: str) -> dict[str, Any] | list[Any]:
        """Return the blob addressed by *uri*.

        The session_id is resolved from the URI itself — callers do not
        supply it separately (avoids the bundle footgun where the wrong
        session_id is passed).

        Raises:
            ValueError: If *uri* is not a valid ``ci-blob://`` URI.
            FileNotFoundError: If no blob exists at the resolved path.
        """
        session_id, key = self._parse_uri(uri)
        path = self._blob_path(session_id, key)

        def _read() -> dict[str, Any] | list[Any]:
            if not path.exists():
                raise FileNotFoundError(f"Blob not found: {uri!r} (path: {path})")
            return json.loads(path.read_text(encoding="utf-8"))  # type: ignore[return-value]

        return await asyncio.to_thread(_read)

    async def list(self, session_id: str) -> list[str]:
        """Return all blob URIs for *session_id*, sorted lexicographically.

        Returns an empty list if the session directory does not exist.
        """
        blobs_dir = self._root / session_id / "blobs"

        def _list() -> list[str]:
            if not blobs_dir.exists():
                return []
            keys = sorted(p.stem for p in blobs_dir.glob("*.json"))
            return [self._make_uri(session_id, key) for key in keys]

        return await asyncio.to_thread(_list)

    async def dump(self, session_id: str) -> dict[str, Any]:
        """Return all blobs for *session_id* as a dict keyed by URI.

        .. note:: Not yet implemented — reserved for Task 2.

        Raises:
            NotImplementedError: Always.
        """
        raise NotImplementedError("dump() is not yet implemented — see Task 2")
