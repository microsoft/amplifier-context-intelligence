"""FileSystemBlobStore \u2014 async, disk-backed blob storage with ci-blob:// URIs.

Disk layout:
    <root>/<session-id>/blobs/<key>.json

URI scheme:
    ci-blob://<session-id>/<key>

All filesystem I/O is wrapped with ``asyncio.to_thread`` to keep the event
loop non-blocking.

This is a concrete implementation of the :class:`~context_intelligence_server.blob_store.protocol.BlobStore`
Protocol. No ``Path``, on-disk layout, ``dest_dir``, or ``os.*`` detail
appears in the Protocol or in any value it returns \u2014 those details are
private to this class (and, later, an Azure equivalent).
"""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import tempfile
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any, cast

from .protocol import BlobNotFoundError, BlobReference

_SCHEME = "ci-blob://"


class FileSystemBlobStore:
    """Async, disk-backed implementation of :class:`~.protocol.BlobStore`.

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
                f"Invalid URI scheme \u2014 expected '{_SCHEME}...', got: {uri!r}"
            )
        remainder = uri[len(_SCHEME) :]
        # remainder must be "<session_id>/<key>" \u2014 both parts non-empty
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
    ) -> BlobReference:
        """Persist *value* as JSON and return a :class:`BlobReference`.

        Creates the directory ``<root>/<session_id>/blobs/`` if needed.
        ``last_modified`` is the storage mtime (from the same ``stat`` call
        that produces ``size``) \u2014 never a writer-clock timestamp.
        """
        path = self._blob_path(session_id, key)

        def _write() -> os.stat_result:
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
            return path.stat()

        st = await asyncio.to_thread(_write)
        return BlobReference(
            uri=self._make_uri(session_id, key),
            session_id=session_id,
            key=key,
            size=st.st_size,
            last_modified=st.st_mtime,
        )

    async def read(self, uri: str) -> dict[str, Any] | list[Any]:
        """Return the blob addressed by *uri*.

        The session_id is resolved from the URI itself \u2014 callers do not
        supply it separately (avoids the bundle footgun where the wrong
        session_id is passed).

        Raises:
            ValueError: If *uri* is not a valid ``ci-blob://`` URI.
            BlobNotFoundError: If no blob exists for *uri*. Subclasses
                ``FileNotFoundError`` for back-compat; the message carries
                the URI only \u2014 never the on-disk path.
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
                raise BlobNotFoundError(f"Blob not found: {uri!r}") from None

        return await asyncio.to_thread(_read)

    async def list(self, session_id: str) -> AsyncIterator[BlobReference]:
        """Stream all blob references for *session_id*.

        Yields nothing if the session's blobs directory does not exist.
        The scandir + per-entry stat work is offloaded to a thread in small
        units (per-entry), so the event loop stays responsive and references
        stream out incrementally rather than blocking on the whole walk.
        """
        blobs_dir = self._root / session_id / "blobs"

        def _list_entries() -> list[tuple[str, int, float]]:
            if not blobs_dir.exists():
                return []
            entries: list[tuple[str, int, float]] = []
            with os.scandir(blobs_dir) as it:
                for entry in it:
                    if not entry.name.endswith(".json"):
                        continue
                    st = entry.stat()
                    key = entry.name[: -len(".json")]
                    entries.append((key, st.st_size, st.st_mtime))
            entries.sort(key=lambda e: e[0])
            return entries

        entries = await asyncio.to_thread(_list_entries)
        for key, size, last_modified in entries:
            yield BlobReference(
                uri=self._make_uri(session_id, key),
                session_id=session_id,
                key=key,
                size=size,
                last_modified=last_modified,
            )

    async def scan(self) -> AsyncIterator[BlobReference]:
        """Stream all blob references across ALL sessions.

        Walks ``<root>/*/blobs/*.json`` \u2014 session-dir enumeration and each
        session's blob-dir scan are offloaded to a thread in small units
        (never one giant ``to_thread`` for the whole tree), so references
        stream out as they are discovered instead of materializing the
        entire store in memory before yielding anything.
        """

        def _list_session_dirs() -> list[str]:
            if not self._root.exists():
                return []
            with os.scandir(self._root) as it:
                return sorted(entry.name for entry in it if entry.is_dir())

        session_ids = await asyncio.to_thread(_list_session_dirs)
        for session_id in session_ids:
            async for ref in self.list(session_id):
                yield ref

    async def delete(
        self, uri: str, if_unmodified: BlobReference | None = None
    ) -> bool:
        """Delete the blob addressed by *uri*.

        Idempotent: returns ``False`` (never raises) if the blob is already
        absent, ``True`` if it existed and was removed.

        Args:
            uri: The ``ci-blob://`` URI to delete.
            if_unmodified: When ``None`` (default), unconditional delete \u2014
                unlinks and returns ``True``, or ``False`` if already absent.
                When provided, this is a **fenced compare-and-delete**: the
                blob is re-``stat``'d (inside the same thread hop, right
                before the unlink, to minimise the TOCTOU window) and the
                delete only proceeds if ``st_mtime``/``st_size`` still match
                *if_unmodified* \u2014 i.e. nothing rewrote the blob since it was
                observed (e.g. by ``scan()``). If the blob is missing, or it
                changed, the delete is refused and ``False`` is returned \u2014
                the blob is left untouched on disk.
        """
        session_id, key = self._parse_uri(uri)
        path = self._blob_path(session_id, key)

        def _delete() -> bool:
            if if_unmodified is None:
                try:
                    os.unlink(path)
                    return True
                except FileNotFoundError:
                    return False

            # Fenced compare-and-delete: stat first, unlink only if unchanged.
            try:
                st = path.stat()
            except FileNotFoundError:
                return False
            if (
                st.st_mtime != if_unmodified.last_modified
                or st.st_size != if_unmodified.size
            ):
                # Blob was rewritten since it was observed \u2014 refuse to delete.
                return False
            try:
                os.unlink(path)
                return True
            except FileNotFoundError:
                # Deleted concurrently between our stat and unlink.
                return False

        return await asyncio.to_thread(_delete)

    async def dump(self, uri: str, dest_dir: Path | str | None = None) -> str:
        """Copy the blob file addressed by *uri* to *dest_dir*.

        Disk-only helper \u2014 NOT part of the :class:`~.protocol.BlobStore`
        Protocol (no production caller; kept as a concrete convenience for
        external tooling that needs a local export).

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
