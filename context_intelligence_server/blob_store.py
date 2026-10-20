"""AsyncDiskBlobStore — async, disk-backed blob storage with ci-blob:// URIs.

Disk layout:
    <root>/<session-id>/blobs/<key>.json

URI scheme:
    ci-blob://<session-id>/<key>

All filesystem I/O is wrapped with ``asyncio.to_thread`` to keep the event
loop non-blocking.
"""

from __future__ import annotations

import json
import os
import shutil
import stat
import tempfile
import uuid
from pathlib import Path
from typing import Any, Protocol, cast, runtime_checkable

from context_intelligence_server.blob_retention import (
    BlobExpiredError, RootLock, component, expired, parse_uri,
)
from context_intelligence_server.blob_coordination import filesystem
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

    async def size(self, uri: str) -> int:
        """Return the byte size of the blob addressed by *uri*.

        Idempotent: a missing blob returns 0, not an error (mirrors
        ``delete_session``'s missing-is-zero contract).

        Raises:
            ValueError: If *uri* is not a valid ``ci-blob://`` URI.
        """
        ...

    async def delete_session(self, session_id: str) -> int:
        """Delete all blobs for *session_id* and return the number removed.

        Idempotent: a session with no stored blobs returns 0, not an error.
        """
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
        return parse_uri(uri)

    def _blob_path(self, session_id: str, key: str) -> Path:
        """Return the filesystem path for a given session/key blob."""
        return self._root / component(session_id) / "blobs" / f"{component(key)}.json"

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
        self._blob_path(session_id, key)  # validate before touching storage
        uri = self._make_uri(session_id, key)

        def _write() -> None:
            # The lock lives inside the thread: cancelling the awaiting task
            # cannot release it while an atomic write is still in progress.
            with RootLock(self._root, create_root=True) as lock:
                if expired(lock.root, uri):
                    return  # replay keeps the reference, never rehydrates
                with lock.root.blobs(session_id, create=True) as fd:
                    data = json.dumps(value)
                    tmp_name = f"{key}.{uuid.uuid4().hex}.tmp"
                    tmp_fd = os.open(tmp_name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=fd)
                    try:
                        with os.fdopen(tmp_fd, "w", encoding="utf-8") as f:
                            f.write(data)
                            f.flush()
                            os.fsync(f.fileno())
                        os.replace(tmp_name, f"{key}.json", src_dir_fd=fd, dst_dir_fd=fd)
                    except BaseException:
                        try:
                            os.unlink(tmp_name, dir_fd=fd)
                        except FileNotFoundError:
                            pass
                        raise

        await filesystem(self._root, _write)
        return uri

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
            with RootLock(self._root, create_root=True) as lock:
                try:
                    with lock.root.blobs(session_id) as fd:
                        file_fd = os.open(f"{key}.json", os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=fd)
                        with os.fdopen(file_fd, "r", encoding="utf-8") as f:
                            self._check_payload(f.fileno())
                            return cast(dict[str, Any] | list[Any], json.load(f))
                except FileNotFoundError:
                    if expired(lock.root, uri):
                        raise BlobExpiredError(uri)
                    raise FileNotFoundError(f"Blob not found: {uri!r} (path: {path})")

        return await filesystem(self._root, _read)

    async def list(self, session_id: str) -> list[str]:
        """Return all blob URIs for *session_id*, sorted lexicographically.

        Returns an empty list if the session directory does not exist.
        """
        component(session_id)

        def _list() -> list[str]:
            with RootLock(self._root, create_root=True) as lock:
                try:
                    with lock.root.blobs(session_id) as fd, os.scandir(fd) as entries:
                        keys = sorted(
                            component(e.name[:-5]) for e in entries
                            if e.name.endswith(".json") and e.is_file(follow_symlinks=False)
                        )
                        return [self._make_uri(session_id, key) for key in keys]
                except FileNotFoundError:
                    return []

        return await filesystem(self._root, _list)

    async def size(self, uri: str) -> int:
        """Return the byte size of the blob addressed by *uri*, or 0 if missing."""
        session_id, key = self._parse_uri(uri)
        def _size() -> int:
            with RootLock(self._root, create_root=True) as lock:
                try:
                    with lock.root.blobs(session_id) as fd:
                        st = os.stat(f"{key}.json", dir_fd=fd, follow_symlinks=False)
                        if not stat.S_ISREG(st.st_mode):
                            raise ValueError("Unsafe blob payload")
                        return st.st_size
                except FileNotFoundError:
                    return 0

        return await filesystem(self._root, _size)

    async def delete_session(self, session_id: str) -> int:
        """Delete all blobs for *session_id* and return the number removed.

        Removes ``<root>/<session_id>/``. Idempotent: a session with no
        stored blobs returns 0 and is not an error.
        """
        component(session_id)

        def _delete() -> int:
            with RootLock(self._root, create_root=True) as lock:
                try:
                    with lock.root.blobs(session_id) as fd, os.scandir(fd) as entries:
                        count = sum(1 for e in entries if e.name.endswith(".json") and e.is_file(follow_symlinks=False))
                    shutil.rmtree(session_id, dir_fd=lock.root.fd)
                    return count
                except FileNotFoundError:
                    return 0

        return await filesystem(self._root, _delete, exclusive=True)

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
        self._blob_path(session_id, key)

        if dest_dir is None:
            dest_dir_path = Path(tempfile.gettempdir()) / "ci-blobs"
        else:
            dest_dir_path = Path(dest_dir)

        def _copy() -> str:
            with RootLock(self._root, create_root=True) as lock:
                try:
                    with lock.root.blobs(session_id) as fd:
                        file_fd = os.open(f"{key}.json", os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=fd)
                        with os.fdopen(file_fd, "rb") as src:
                            self._check_payload(src.fileno())
                            st = os.fstat(src.fileno())
                            dest_dir_path.mkdir(parents=True, exist_ok=True)
                            dest = dest_dir_path / f"{key}.json"
                            # Do not truncate the source if destination is its
                            # own directory, a symlink alias, or a hardlink.
                            dest_fd = os.open(dest, os.O_WRONLY | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
                            with os.fdopen(dest_fd, "wb") as out:
                                self._check_payload(out.fileno())
                                dst = os.fstat(out.fileno())
                                if (dst.st_dev, dst.st_ino) == (st.st_dev, st.st_ino):
                                    raise shutil.SameFileError("Blob dump destination is the source payload")
                                os.ftruncate(out.fileno(), 0)
                                shutil.copyfileobj(src, out)
                            os.utime(dest, ns=(st.st_atime_ns, st.st_mtime_ns))
                            return str(dest)
                except FileNotFoundError:
                    if expired(lock.root, uri):
                        raise BlobExpiredError(uri)
                    raise FileNotFoundError(f"Blob not found: {uri!r}")

        return await filesystem(self._root, _copy)

    @staticmethod
    def _check_payload(fd: int) -> None:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise ValueError("Unsafe blob payload: not a regular file")
