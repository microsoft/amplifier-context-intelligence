"""Process-wide single-writer locks for durable server state.

``ServerInstanceLock`` protects one validated state root.  ``IdentityStoreLock``
protects one identity-store file independently of any queue root, so two
servers with unrelated queue roots (or aliased paths) cannot share a mutable
credential store.
"""

from __future__ import annotations

import errno
import fcntl
import os
import secrets
import stat
from pathlib import Path

_LOCK_FILENAME = ".context-intelligence-server.lock"


class InstanceLockUnavailable(RuntimeError):
    """Raised when another server already owns this durable queue volume."""


class ServerInstanceLock:
    """Hold a nonblocking exclusive lock for the server process lifecycle."""

    def __init__(self, state_root: str | Path) -> None:
        self.path = Path(state_root) / _LOCK_FILENAME
        self._fd: int | None = None

    def acquire(self) -> None:
        """Acquire the validated state root's exclusive writer lock."""
        if self._fd is not None:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            os.close(fd)
            raise InstanceLockUnavailable(
                "context-intelligence-server refuses to start because another "
                f"server holds the shared state lock at {self.path}. Run exactly "
                "one replica and one worker against this state volume."
            ) from exc
        except BaseException:
            os.close(fd)
            raise
        self._fd = fd

    def release(self) -> None:
        """Release the lock after the server and all of its workers have stopped."""
        if self._fd is None:
            return
        fd, self._fd = self._fd, None
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)

    def __enter__(self) -> ServerInstanceLock:
        self.acquire()
        return self

    def __exit__(self, *_: object) -> None:
        self.release()


class IdentityStoreOwnershipError(RuntimeError):
    """Raised when exclusive ownership of an identity store is absent or unsafe."""


class IdentityStoreLockUnavailable(IdentityStoreOwnershipError):
    """Raised when another process already owns this identity store."""


class IdentityStoreCommitUncertain(IdentityStoreOwnershipError, OSError):
    """The store file WAS replaced but its durability could not be confirmed.

    Disk may now hold the new content while memory still holds the old; there
    is no rollback.  It is an ownership error so the store latches unavailable
    (nothing is trusted until an orderly stop and re-initialization reload the
    file under the lock), and an ``OSError`` because it is a genuine I/O fault.
    """


class IdentityStoreLock:
    """Exclusive, process-owned lock on one identity-store file.

    The lock is a sidecar ``<store name>.lock`` in the store's *canonical*
    parent directory, so symlinked or otherwise aliased parents contend on the
    same inode.  The parent directory is pinned with a file descriptor and the
    store, its temp files and the sidecar are only touched relative to that
    descriptor.  ``assert_owned`` re-verifies the bindings before every
    protected operation.

    The owner is the acquiring PID.  A forked child inherits the descriptors
    but is never authorized, and its ``release`` only closes its own copies.
    The sidecar is never unlinked.

    Checked by ``assert_owned`` before every protected operation (and by
    ``IdentityStore.healthy`` before every authenticated request):

    * the pinned parent descriptor, the canonical parent path AND the original
      configured (lexical, absolute) parent path all name the same directory;
    * the sidecar is a regular, single-link file bound to the held descriptor;
    * the store entry is a regular single-link file (no symlink, no hardlink).

    Trusted-storage boundary: this is an ADVISORY ``flock``.  It stops two
    cooperating servers sharing a store; it is not an access-control mechanism.
    New sidecars are created owner-only (0600), but an existing sidecar's mode
    and owner are deliberately NOT policed: a mount that reports wide or fixed
    modes (for example fixed-mode SMB) must still be able to start.  Anyone who
    can write the store directory can already replace the store or the sidecar
    and bypass the lock, so directory write access must be trusted.  When this
    server's own binding breaks it stops trusting its cache, but it cannot
    prevent another process from then locking a replacement file.
    """

    def __init__(self, store_path: str | Path) -> None:
        self._requested = Path(store_path).expanduser()
        self.store_name = self._requested.name
        self.lock_name = f"{self.store_name}.lock"
        self.parent: Path | None = None
        # The configured parent as an absolute, NOT symlink-resolved path.  It
        # must keep resolving to the pinned directory: a retargeted alias would
        # otherwise leave us trusting a directory the operator no longer names.
        self._lexical_parent: Path | None = None
        self._parent_fd: int | None = None
        self._fd: int | None = None
        self._pid: int | None = None

    # -- lifecycle ---------------------------------------------------------

    @property
    def path(self) -> Path:
        """Return the sidecar path (for messages); valid after ``acquire``."""
        base = self.parent if self.parent is not None else self._requested.parent
        return base / self.lock_name

    def acquire(self) -> None:
        """Take the exclusive nonblocking lock or raise a clear error."""
        if self._fd is not None:
            if self._pid != os.getpid():
                raise IdentityStoreOwnershipError(
                    "identity store lock is inherited from another process"
                )
            return
        if self.store_name in ("", ".", ".."):
            raise IdentityStoreOwnershipError(
                f"identity store path {self._requested} has no file name"
            )
        parent_fd: int | None = None
        fd: int | None = None
        try:
            try:
                self._requested.parent.mkdir(parents=True, exist_ok=True)
                parent = Path(os.path.realpath(self._requested.parent))
                parent_fd = os.open(
                    parent,
                    os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                )
            except OSError as exc:
                raise IdentityStoreOwnershipError(
                    f"cannot open identity store directory "
                    f"{self._requested.parent}: {exc!r}"
                ) from exc
            self.parent = parent
            self._lexical_parent = Path(os.path.abspath(self._requested.parent))
            self._check_store_entry(parent_fd)
            try:
                fd = os.open(
                    self.lock_name,
                    os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_CLOEXEC,
                    0o600,
                    dir_fd=parent_fd,
                )
            except OSError as exc:
                raise IdentityStoreOwnershipError(
                    f"cannot open identity store lock {self.path}: {exc!r}. "
                    "The store directory must be writable by the server user."
                ) from exc
            self._check_sidecar(parent_fd, fd)
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise IdentityStoreLockUnavailable(
                    "context-intelligence-server refuses to start because "
                    "another server owns the identity store "
                    f"{parent / self.store_name} "
                    f"(lock {self.path}). Run one server per identity store."
                ) from exc
            # The lock is held: confirm the sidecar still names this inode.
            self._check_sidecar(parent_fd, fd)
        except BaseException:
            for handle in (fd, parent_fd):
                if handle is not None:
                    os.close(handle)
            self.parent = None
            self._lexical_parent = None
            raise
        self._parent_fd, self._fd, self._pid = parent_fd, fd, os.getpid()

    def release(self) -> None:
        """Drop ownership.  Never unlinks the sidecar."""
        if self._fd is None:
            return
        fd, parent_fd, pid = self._fd, self._parent_fd, self._pid
        self._fd = self._parent_fd = self._pid = None
        try:
            if pid == os.getpid():
                fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)
            if parent_fd is not None:
                os.close(parent_fd)

    def owned_by_this_process(self) -> bool:
        """Cheap check (no syscalls): the acquiring PID is the current PID."""
        return self._fd is not None and self._pid == os.getpid()

    # -- verification ------------------------------------------------------

    def assert_owned(self) -> int:
        """Re-verify ownership and bindings; return the pinned parent fd."""
        if self._fd is None or self._parent_fd is None or self.parent is None:
            raise IdentityStoreOwnershipError("identity store is not owned")
        if self._pid != os.getpid():
            raise IdentityStoreOwnershipError(
                "identity store is owned by another process (inherited descriptor)"
            )
        try:
            pinned = os.fstat(self._parent_fd)
            current = os.stat(self.parent)
        except OSError as exc:
            raise IdentityStoreOwnershipError(
                f"identity store directory {self.parent} is no longer reachable: "
                f"{exc!r}"
            ) from exc
        if (pinned.st_dev, pinned.st_ino) != (current.st_dev, current.st_ino):
            raise IdentityStoreOwnershipError(
                f"identity store directory {self.parent} was replaced after "
                "ownership was taken"
            )
        configured = self._lexical_parent
        if configured is not None:
            try:
                named = os.stat(configured)
            except OSError as exc:
                raise IdentityStoreOwnershipError(
                    f"configured identity store directory {configured} is no "
                    f"longer reachable: {exc!r}"
                ) from exc
            if (pinned.st_dev, pinned.st_ino) != (named.st_dev, named.st_ino):
                raise IdentityStoreOwnershipError(
                    f"configured identity store directory {configured} now "
                    f"resolves to a different directory than the one owned "
                    f"({self.parent})"
                )
        self._check_sidecar(self._parent_fd, self._fd)
        self._check_store_entry(self._parent_fd)
        return self._parent_fd

    def _check_sidecar(self, parent_fd: int, fd: int) -> None:
        try:
            held = os.fstat(fd)
            named = os.lstat(self.lock_name, dir_fd=parent_fd)
        except OSError as exc:
            raise IdentityStoreOwnershipError(
                f"identity store lock {self.path} is unreadable: {exc!r}"
            ) from exc
        if (
            not stat.S_ISREG(held.st_mode)
            or held.st_nlink != 1
            or (held.st_dev, held.st_ino) != (named.st_dev, named.st_ino)
        ):
            raise IdentityStoreOwnershipError(
                f"identity store lock {self.path} is not a regular single-link "
                "file bound to the held descriptor"
            )

    def _check_store_entry(self, parent_fd: int) -> None:
        """Reject a final-component symlink, non-regular file, or hardlink."""
        try:
            info = os.lstat(self.store_name, dir_fd=parent_fd)
        except FileNotFoundError:
            return
        except OSError as exc:
            raise IdentityStoreOwnershipError(
                f"identity store {self.store_name} cannot be inspected: {exc!r}"
            ) from exc
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            raise IdentityStoreOwnershipError(
                f"identity store {self.store_name} must be a regular, "
                "single-link file (symlinks, hardlinks and special files are "
                "refused)"
            )

    # -- protected store I/O (all anchored to the pinned parent fd) -------

    def store_exists(self) -> bool:
        """Return whether the store file exists (after verifying ownership)."""
        parent_fd = self.assert_owned()
        try:
            os.lstat(self.store_name, dir_fd=parent_fd)
        except FileNotFoundError:
            return False
        return True

    def read_store_bytes(self) -> bytes | None:
        """Return the store bytes, or ``None`` when the file does not exist."""
        parent_fd = self.assert_owned()
        try:
            fd = os.open(
                self.store_name,
                os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC,
                dir_fd=parent_fd,
            )
        except FileNotFoundError:
            return None
        except OSError as exc:
            if exc.errno == errno.ELOOP:
                raise IdentityStoreOwnershipError(
                    f"identity store {self.store_name} is a symlink; refused"
                ) from exc
            raise
        with os.fdopen(fd, "rb") as handle:
            info = os.fstat(handle.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                raise IdentityStoreOwnershipError(
                    f"identity store {self.store_name} is not a single-link "
                    "regular file"
                )
            return handle.read()

    def write_store_atomic(self, payload: bytes) -> None:
        """Atomically replace the store; the sidecar is unaffected."""
        parent_fd = self.assert_owned()
        tmp_name = f".{self.store_name}.{os.getpid()}.{secrets.token_hex(6)}.tmp"
        fd = os.open(
            tmp_name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
            0o600,
            dir_fd=parent_fd,
        )
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(
                tmp_name, self.store_name, src_dir_fd=parent_fd, dst_dir_fd=parent_fd
            )
        except BaseException:
            # Failure BEFORE the rename took effect: nothing changed (certain).
            try:
                os.unlink(tmp_name, dir_fd=parent_fd)
            except OSError:
                pass
            raise
        # From here the new content IS the store file.  If durability cannot be
        # confirmed the outcome is uncertain: never report success or rollback.
        try:
            os.fsync(parent_fd)
        except BaseException as exc:
            raise IdentityStoreCommitUncertain(
                f"identity store {self.store_name} was replaced but the "
                f"directory sync failed ({exc!r}); the stored credentials are "
                "now uncertain and the store is unavailable until it is "
                "reloaded under ownership"
            ) from exc
