"""AsyncDiskBlobStore — async, disk-backed blob storage with ci-blob:// URIs.

Disk layout:
    <root>/<session-id>/blobs/<name>.json                    ordinary names
    <root>/<session-id>/blobs/~ci-key-v1~<64 hex>.blob       separator/control names

URI scheme:
    ci-blob://<session-id>/<name>
    ci-blob://<session-id>/~ci-key-v1/<64 hex>

The logical-name -> physical-name map lives in ``blob_keys``; this module only
does I/O.  All filesystem I/O is wrapped with ``asyncio.to_thread`` to keep the
event loop non-blocking.
"""

from __future__ import annotations

import json
import os
import secrets
import shutil
import stat
import tempfile
from pathlib import Path
from typing import Any, NoReturn, Protocol, cast, runtime_checkable

from context_intelligence_server.blob_coordination import filesystem
from context_intelligence_server.blob_keys import (
    MAPPED_MARKER,
    MAX_KEY_BYTES,
    MAX_SESSION_BYTES,
    BlobAddress,
    address_from_filename,
    component,
    exact_address,
    legacy_address,
    parse_address,
    read_candidates,
    write_address,
)
from context_intelligence_server.blob_retention import (
    BlobExpiredError,
    Root,
    RootLock,
    expired,
)
from context_intelligence_server.utils import fold_name

_SCHEME = "ci-blob://"

# --- session-id / blob-key length budgets ---------------------------------
#
# INCIDENT (production): a blob key is ``f"{node_id}__{field_name}"``
# (blob_processor.py) and node_id embeds the session_id, so for a deeply
# nested sub-agent session the key can exceed the filesystem's NAME_MAX (255
# bytes) once a temp-file suffix is added.  ``write()`` then raised OSError and
# the field was silently replaced by ``{"$blob_error": ...}`` in the graph.
#
# Temp files are created as ``{physical stem}.{8 random chars}.tmp``: the
# stem plus 13 bytes.  ``blob_keys`` therefore folds an ordinary key to 242
# bytes (242 + 13 = 255) and a mapped name's stem is a fixed 75 bytes.  The
# session id is a bare directory component (nothing is appended), so its
# budget is the plain NAME_MAX.
_MAX_KEY_BYTES = MAX_KEY_BYTES
_MAX_SESSION_ID_BYTES = MAX_SESSION_BYTES


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
        return fold_name(component(session_id), _MAX_SESSION_ID_BYTES)

    def _parse_uri(self, uri: str) -> tuple[str, str]:
        """Parse a ``ci-blob://`` URI into ``(session_id, key)``.

        *key* is the ordinary name, or the canonical handle
        ``~ci-key-v1/<64 hex>`` for a mapped blob.

        Raises:
            ValueError: If *uri* is not a valid ``ci-blob://`` URI.
        """
        address = parse_address(uri)
        handle = f"{MAPPED_MARKER}/{address.name}" if address.mapped else address.name
        return address.session, handle

    def _session_dir(self, session_id: str) -> Path:
        """Return ``<root>/<folded-session_id>`` -- the shared base every
        session-scoped path (blob path, list scan, delete) resolves under."""
        return self._root / self._fold_session_id(session_id)

    def _address(self, session_id: str, key: str) -> BlobAddress:
        """Resolve a ``(session_id, key)`` pair to its physical address.

        A key spelled ``~ci-key-v1/<64 hex>`` is the canonical handle that
        ``parse_uri`` returns: it names the stored file directly and is never
        hashed again.  Every other key is a logical write name.
        """
        if key.startswith(f"{MAPPED_MARKER}/"):
            try:
                return exact_address(
                    f"{_SCHEME}{self._fold_session_id(session_id)}/{key}"
                )
            except ValueError:
                pass
        return write_address(session_id, key)

    def _blob_path(self, session_id: str, key: str) -> Path:
        """Return the filesystem path for a given session/key blob."""
        return (
            self._session_dir(session_id)
            / "blobs"
            / self._address(session_id, key).filename
        )

    @staticmethod
    def _recognised(session: str, entries: Any) -> list[BlobAddress]:
        """Addresses of the regular ``.json`` / ``.blob`` files in *entries*."""
        found: list[BlobAddress] = []
        for entry in entries:
            if not entry.is_file(follow_symlinks=False):
                continue
            try:
                address = address_from_filename(session, entry.name)
            except ValueError:
                continue  # a recognised-but-invalid name never poisons the listing
            if address is not None:
                found.append(address)
        return found

    @staticmethod
    def _open_first(
        dir_fd: int, candidates: tuple[BlobAddress, ...]
    ) -> tuple[int, BlobAddress] | None:
        """Open the first candidate that exists.

        Only a missing file falls through to the next candidate: a symlink,
        permission error or non-regular entry must never be masked by a
        fallback.
        """
        for candidate in candidates:
            try:
                fd = os.open(
                    candidate.filename,
                    os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
                    dir_fd=dir_fd,
                )
            except FileNotFoundError:
                continue
            return fd, candidate
        return None

    @staticmethod
    def _raise_missing(
        root: Root, candidates: tuple[BlobAddress, ...], message: str
    ) -> NoReturn:
        """Every candidate is physically absent: report a recorded expiry, else not-found."""
        for candidate in candidates:
            if expired(root, candidate.uri):
                raise BlobExpiredError(candidate.uri)
        raise FileNotFoundError(message)

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

        *key* is a raw logical name.  Creates ``<root>/<session_id>/blobs/``
        if needed.  A pre-upgrade backslash file is never created, migrated or
        deleted: if its expiry is recorded the write keeps that old URI and
        creates nothing, so replay never rehydrates a pruned payload.

        Returns:
            The canonical URI, or the recorded-expired URI that blocked the write.
        """
        canonical = write_address(session_id, key)  # validates before any I/O
        legacy = legacy_address(session_id, key)
        if legacy == canonical:
            legacy = None

        def _write() -> str:
            # The lock lives inside the thread: cancelling the awaiting task
            # cannot release it while an atomic write is still in progress.
            with RootLock(self._root, create_root=True) as lock:
                if expired(lock.root, canonical.uri):
                    return canonical.uri  # replay keeps the reference, never rehydrates
                if legacy is not None and expired(lock.root, legacy.uri):
                    return legacy.uri
                with lock.root.blobs(canonical.session, create=True) as fd:
                    data = json.dumps(value)
                    for _ in range(100):
                        tmp_name = f"{canonical.stem}.{secrets.token_urlsafe(6)}.tmp"
                        try:
                            tmp_fd = os.open(
                                tmp_name,
                                os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                                0o600,
                                dir_fd=fd,
                            )
                            break
                        except FileExistsError:
                            continue
                    else:
                        raise FileExistsError("Could not allocate blob temporary file")
                    try:
                        with os.fdopen(tmp_fd, "w", encoding="utf-8") as f:
                            f.write(data)
                            f.flush()
                            os.fsync(f.fileno())
                        os.replace(
                            tmp_name, canonical.filename, src_dir_fd=fd, dst_dir_fd=fd
                        )
                    except BaseException:
                        try:
                            os.unlink(tmp_name, dir_fd=fd)
                        except FileNotFoundError:
                            pass
                        raise
                return canonical.uri

        return await filesystem(self._root, _write)

    async def read(self, uri: str) -> dict[str, Any] | list[Any]:
        """Return the blob addressed by *uri*.

        The session_id is resolved from the URI itself — callers do not
        supply it separately (avoids the bundle footgun where the wrong
        session_id is passed).

        Raises:
            ValueError: If *uri* is not a valid ``ci-blob://`` URI.
            FileNotFoundError: If no blob exists at the resolved path.
            BlobExpiredError: If every candidate is absent and one has a
                recorded expiry.
        """
        candidates = read_candidates(uri)
        first = candidates[0]
        path = self._root / first.session / "blobs" / first.filename

        def _read() -> dict[str, Any] | list[Any]:
            with RootLock(self._root, create_root=True) as lock:
                try:
                    with lock.root.blobs(first.session) as fd:
                        opened = self._open_first(fd, candidates)
                        if opened is not None:
                            with os.fdopen(opened[0], "r", encoding="utf-8") as f:
                                self._check_payload(f.fileno())
                                return cast(dict[str, Any] | list[Any], json.load(f))
                except FileNotFoundError:
                    pass  # no session/blobs directory: every candidate is missing
                self._raise_missing(
                    lock.root,
                    candidates,
                    f"Blob not found: {first.uri!r} (path: {path})",
                )

        return await filesystem(self._root, _read)

    async def list(self, session_id: str) -> list[str]:
        """Return all blob URIs for *session_id*, sorted lexicographically.

        Returns an empty list if the session directory does not exist.  Both
        ``.json`` and mapped ``.blob`` files are listed under the URI of their
        physical name; unrecognised entries are ignored.
        """
        session = self._fold_session_id(session_id)

        def _list() -> list[str]:
            with RootLock(self._root, create_root=True) as lock:
                try:
                    with lock.root.blobs(session) as fd, os.scandir(fd) as entries:
                        return sorted(
                            address.uri
                            for address in self._recognised(session, entries)
                        )
                except FileNotFoundError:
                    return []

        return await filesystem(self._root, _list)

    async def size(self, uri: str) -> int:
        """Return the byte size of the blob addressed by *uri*, or 0 if missing."""
        candidates = read_candidates(uri)

        def _size() -> int:
            with RootLock(self._root, create_root=True) as lock:
                try:
                    with lock.root.blobs(candidates[0].session) as fd:
                        for candidate in candidates:
                            try:
                                st = os.stat(
                                    candidate.filename, dir_fd=fd, follow_symlinks=False
                                )
                            except FileNotFoundError:
                                continue
                            if not stat.S_ISREG(st.st_mode):
                                raise ValueError("Unsafe blob payload")
                            return st.st_size
                except FileNotFoundError:
                    pass
                return 0

        return await filesystem(self._root, _size)

    async def delete_session(self, session_id: str) -> int:
        """Delete all blobs for *session_id* and return the number removed.

        Removes ``<root>/<folded-session_id>/``. Idempotent: a session
        with no stored blobs returns 0 and is not an error.
        """
        session = self._fold_session_id(session_id)

        def _delete() -> int:
            with RootLock(self._root, create_root=True) as lock:
                try:
                    with lock.root.blobs(session) as fd, os.scandir(fd) as entries:
                        count = len(self._recognised(session, entries))
                    shutil.rmtree(session, dir_fd=lock.root.fd)
                    return count
                except FileNotFoundError:
                    return 0

        return await filesystem(self._root, _delete, exclusive=True)

    async def dump(self, uri: str, dest_dir: Path | str | None = None) -> str:
        """Copy the blob file addressed by *uri* to *dest_dir*.

        The copy is named after the selected PHYSICAL file (never the URI).

        Args:
            uri: ``ci-blob://`` URI identifying the blob to copy.
            dest_dir: Destination directory.  Defaults to
                ``Path(tempfile.gettempdir()) / 'ci-blobs'``.

        Returns:
            The destination file path as a string.

        Raises:
            ValueError: If *uri* is not a valid ``ci-blob://`` URI.
            FileNotFoundError: If no blob exists at the resolved path.
            BlobExpiredError: If every candidate is absent and one has a
                recorded expiry.
        """
        candidates = read_candidates(uri)
        first = candidates[0]

        if dest_dir is None:
            dest_dir_path = Path(tempfile.gettempdir()) / "ci-blobs"
        else:
            dest_dir_path = Path(dest_dir)

        def _copy() -> str:
            with RootLock(self._root, create_root=True) as lock:
                try:
                    with lock.root.blobs(first.session) as fd:
                        opened = self._open_first(fd, candidates)
                        if opened is not None:
                            file_fd, selected = opened
                            with os.fdopen(file_fd, "rb") as src:
                                self._check_payload(src.fileno())
                                st = os.fstat(src.fileno())
                                dest_dir_path.mkdir(parents=True, exist_ok=True)
                                dest = dest_dir_path / selected.filename
                                # Do not truncate the source if destination is its
                                # own directory, a symlink alias, or a hardlink.
                                dest_fd = os.open(
                                    dest,
                                    os.O_WRONLY
                                    | os.O_CREAT
                                    | os.O_NOFOLLOW
                                    | os.O_NONBLOCK,
                                    0o600,
                                )
                                with os.fdopen(dest_fd, "wb") as out:
                                    self._check_payload(out.fileno())
                                    dst = os.fstat(out.fileno())
                                    if (dst.st_dev, dst.st_ino) == (
                                        st.st_dev,
                                        st.st_ino,
                                    ):
                                        raise shutil.SameFileError(
                                            "Blob dump destination is the source payload"
                                        )
                                    os.ftruncate(out.fileno(), 0)
                                    shutil.copyfileobj(src, out)
                                os.utime(dest, ns=(st.st_atime_ns, st.st_mtime_ns))
                                return str(dest)
                except FileNotFoundError:
                    pass  # no session/blobs directory: every candidate is missing
                self._raise_missing(
                    lock.root, candidates, f"Blob not found: {first.uri!r}"
                )

        return await filesystem(self._root, _copy)

    @staticmethod
    def _check_payload(fd: int) -> None:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise ValueError("Unsafe blob payload: not a regular file")
