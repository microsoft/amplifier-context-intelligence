"""AsyncDiskBlobStore — async, disk-backed blob storage with ci-blob:// URIs.

Disk layout:
    <root>/<session-id>/blobs/<key>.json

URI scheme:
    ci-blob://<session-id>/<key>

All filesystem I/O is wrapped with ``asyncio.to_thread`` to keep the event
loop non-blocking.
"""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any, Protocol, cast, runtime_checkable

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

    async def dump(self, uri: str, dest_dir: Path | str | None = None) -> str:
        """Copy the blob file addressed by *uri* to *dest_dir*.

        Args:
            uri: ``ci-blob://`` URI identifying the blob to copy.
            dest_dir: Destination directory.  Defaults to
                ``Path(tempfile.gettempdir()) / 'ci-blobs'``.

        Returns:
            The destination file path as a string.

        Raises:
            ValueError: If *uri* is not a valid ``ci-blob://`` URI.
            FileNotFoundError: If no blob exists at the resolved path.
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
            data = json.dumps(value)
            tmp_fd, tmp_name = tempfile.mkstemp(
                dir=str(path.parent), prefix=f"{key}.", suffix=".tmp"
            )
            try:
                with os.fdopen(tmp_fd, "w", encoding="utf-8") as f:
                    f.write(data)
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(tmp_name, path)
            except BaseException:
                try:
                    os.unlink(tmp_name)
                except FileNotFoundError:
                    pass
                raise

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
            try:
                return cast(
                    dict[str, Any] | list[Any],
                    json.loads(path.read_text(encoding="utf-8")),
                )
            except FileNotFoundError:
                raise FileNotFoundError(f"Blob not found: {uri!r} (path: {path})")

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

    async def dump(self, uri: str, dest_dir: Path | str | None = None) -> str:
        """Copy the blob file addressed by *uri* to *dest_dir*.

        Args:
            uri: ``ci-blob://`` URI identifying the blob to copy.
            dest_dir: Destination directory.  Defaults to
                ``Path(tempfile.gettempdir()) / 'ci-blobs'``.

        Returns:
            The destination file path as a string.

        Raises:
            ValueError: If *uri* is not a valid ``ci-blob://`` URI.
            FileNotFoundError: If no blob exists at the resolved path.
        """
        session_id, key = self._parse_uri(uri)
        src = self._blob_path(session_id, key)

        if dest_dir is None:
            dest_dir_path = Path(tempfile.gettempdir()) / "ci-blobs"
        else:
            dest_dir_path = Path(dest_dir)

        def _copy() -> str:
            if not src.exists():
                raise FileNotFoundError(f"Blob not found: {uri!r}")
            dest_dir_path.mkdir(parents=True, exist_ok=True)
            return str(shutil.copy2(src, dest_dir_path))

        return await asyncio.to_thread(_copy)
