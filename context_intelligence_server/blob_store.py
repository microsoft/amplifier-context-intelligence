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

from context_intelligence_server.utils import fold_name

_SCHEME = "ci-blob://"

# --- session-id / blob-key length budgets ---------------------------------
#
# INCIDENT (production, sibling of the queue's worker-key incident): a blob
# key is ``f"{node_id}__{field_name}"`` (blob_processor.py), and node_id
# embeds the session_id (utils.make_node_id) -- so for a deeply-nested
# sub-agent session, the blob key is LONGER than the queue's worker key was.
# ``write()``'s ``tempfile.mkstemp(prefix=f"{key}.", suffix=".tmp")`` names
# a temp file that is WIDER than the final ``{key}.json`` name -- exactly
# the "the temp file is the real worst case" shape as the queue's
# ``commit()``. Once that temp name crossed NAME_MAX (255 bytes),
# ``write()`` raised OSError, silently replacing the field's data with
# ``{"$blob_error": ...}`` in the graph (2,313 occurrences/24h in
# production) -- never a crash, just silent data loss.
#
# ``mkstemp``'s random component is documented (and pinned by
# ``tempfile._RandomNameSequence``'s own docstring: "Each string is eight
# characters long") to always be exactly 8 characters; verified empirically
# against this interpreter too. So the temp name is
# ``{key}.{8 random chars}.tmp`` = key + 1 ("." after key) + 8 + 4 (".tmp").
_NAME_MAX = 255
_KEY_TMP_SUFFIX_BYTES = len(".") + 8 + len(".tmp")  # mkstemp's own suffix: 13
_MAX_KEY_BYTES = _NAME_MAX - _KEY_TMP_SUFFIX_BYTES  # 242

# session_id is a bare directory component (``<root>/<session_id>/blobs/``):
# nothing ever appends a suffix to the session_id segment itself (mkstemp's
# temp file lives inside the "blobs" subdirectory, widening the KEY, not the
# session_id), so its budget is the plain NAME_MAX.
_MAX_SESSION_ID_BYTES = _NAME_MAX


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

    @staticmethod
    def _fold_session_id(session_id: str) -> str:
        """Bound *session_id* to fit as a bare directory component.

        See the module-level comment on ``_MAX_SESSION_ID_BYTES`` for why
        the plain ``NAME_MAX`` budget is correct here (no suffix is ever
        appended to the session_id segment itself).
        """
        return fold_name(session_id, _MAX_SESSION_ID_BYTES)

    @staticmethod
    def _fold_key(key: str) -> str:
        """Bound *key* to fit under ``write()``'s temp-file suffix budget.

        See the module-level comment on ``_MAX_KEY_BYTES`` for the
        ``tempfile.mkstemp`` suffix this is sized against.
        """
        return fold_name(key, _MAX_KEY_BYTES)

    def _make_uri(self, session_id: str, key: str) -> str:
        """Return the canonical ``ci-blob://`` URI for a session/key pair.

        Both components are folded to their filesystem-safe length BEFORE
        being embedded in the URI: this URI is what gets persisted into
        the graph, and ``_blob_path`` (which resolves it back to disk) also
        folds -- so an unfolded URI here would point at a path no read
        could ever reach. Folding is idempotent, so calling this with an
        already-folded session_id/key (e.g. from ``_parse_uri``) is a
        no-op.
        """
        return f"{_SCHEME}{self._fold_session_id(session_id)}/{self._fold_key(key)}"

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

    def _session_dir(self, session_id: str) -> Path:
        """Return ``<root>/<folded-session_id>`` -- the shared base every
        session-scoped path (blob path, list scan, delete) resolves under."""
        return self._root / self._fold_session_id(session_id)

    def _blob_path(self, session_id: str, key: str) -> Path:
        """Return the filesystem path for a given session/key blob.

        Both *session_id* and *key* are folded here too -- calling this
        with an already-folded pair (as ``read``/``size``/``dump`` do,
        via ``_parse_uri``) is an idempotent no-op, but ``write()`` calls
        this FIRST, before a URI exists, so the fold must happen here
        independently of ``_make_uri``.
        """
        return self._session_dir(session_id) / "blobs" / f"{self._fold_key(key)}.json"

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
        # Fold explicitly and reuse the SAME value for the mkstemp prefix
        # below. ``_blob_path`` already folds ``key`` internally to build
        # ``path``'s stem -- fold_name is deterministic, so re-deriving it
        # here is guaranteed identical -- but the mkstemp prefix must never
        # be built from the RAW key: that key can be far longer than the
        # budget, and the whole point of folding is that the temp file's
        # name (this module's own widest suffix -- see the module-level
        # comment on ``_MAX_KEY_BYTES``) never exceeds NAME_MAX.
        folded_key = self._fold_key(key)

        def _write() -> None:
            path.parent.mkdir(parents=True, exist_ok=True)
            data = json.dumps(value)
            tmp_fd, tmp_name = tempfile.mkstemp(
                dir=str(path.parent), prefix=f"{folded_key}.", suffix=".tmp"
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

        *session_id* is folded via ``_session_dir`` to locate the
        directory; ``p.stem`` recovers an ALREADY-folded key (every file
        on disk is named with its folded key), so the ``_make_uri`` call
        below folds it again as a no-op -- idempotent, per ``fold_name``.
        """
        blobs_dir = self._session_dir(session_id) / "blobs"

        def _list() -> list[str]:
            if not blobs_dir.exists():
                return []
            keys = sorted(p.stem for p in blobs_dir.glob("*.json"))
            return [self._make_uri(session_id, key) for key in keys]

        return await asyncio.to_thread(_list)

    async def size(self, uri: str) -> int:
        """Return the byte size of the blob addressed by *uri*, or 0 if missing."""
        session_id, key = self._parse_uri(uri)
        path = self._blob_path(session_id, key)

        def _size() -> int:
            try:
                return path.stat().st_size
            except FileNotFoundError:
                return 0

        return await asyncio.to_thread(_size)

    async def delete_session(self, session_id: str) -> int:
        """Delete all blobs for *session_id* and return the number removed.

        Removes ``<root>/<folded-session_id>/``. Idempotent: a session
        with no stored blobs returns 0 and is not an error.
        """
        session_dir = self._session_dir(session_id)
        blobs_dir = session_dir / "blobs"

        def _delete() -> int:
            if not blobs_dir.exists():
                return 0
            count = sum(1 for _ in blobs_dir.glob("*.json"))
            shutil.rmtree(session_dir)
            return count

        return await asyncio.to_thread(_delete)

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
