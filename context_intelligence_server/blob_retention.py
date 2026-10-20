"""Manual storage-age retention. No graph access and no payload reads.

The flock is cooperative: all namespace mutators must honor it. In particular,
stop old, unpatched writers before apply. Private metadata and no-follow opens
protect against unsafe on-disk entries, not a hostile process with our own uid.
"""

from __future__ import annotations

import fcntl
import ctypes
import os
import re
import sqlite3
import stat
import sys
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Callable, Iterator
from zoneinfo import ZoneInfo

LOCK = ".ci-blob-retention.lock"
METADATA = ".ci-blob-retention"
DATABASE = "expiry.sqlite3"
BATCH_SIZE = 512
_DIR = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
_COLUMNS = ("uri", "dev", "ino", "size", "mtime_ns", "ctime_ns", "expired_ns", "pending")


class RetentionError(RuntimeError):
    """An unsafe namespace, unavailable lock, or invalid ledger."""


class BlobExpiredError(FileNotFoundError):
    """The physical payload is absent and its URI has an expiry record."""

    def __init__(self, uri: str):
        self.uri = uri
        super().__init__(f"Blob expired: {uri}")


def component(value: str) -> str:
    """Accept one unambiguous URI/filesystem component, never a path."""
    if (
        not value or value in (".", "..", LOCK, METADATA)
        or any(c in value for c in "/\\%?#")
        or any(ord(c) < 32 or ord(c) == 127 for c in value)
    ):
        raise ValueError(f"Unsafe blob component: {value!r}")
    return value


def parse_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith("ci-blob://"):
        raise ValueError(f"Invalid blob URI: {uri!r}")
    parts = uri[10:].split("/")
    if len(parts) != 2:
        raise ValueError(f"Invalid blob URI: {uri!r}")
    return component(parts[0]), component(parts[1])


def duration_ns(value: str) -> int:
    match = re.fullmatch(r"([1-9][0-9]*)([smhdw])", value)
    if not match or len(match[1]) > 18:
        raise ValueError("Age must be a positive integer followed by s/m/h/d/w")
    result = int(match[1]) * {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}[match[2]] * 10**9
    if result > 2**63 - 1:
        raise ValueError("Age exceeds supported nanosecond range")
    return result


def cutoff_for(age_ns: int, now_ns: int | None = None) -> int:
    cutoff = (time.time_ns() if now_ns is None else now_ns) - age_ns
    if cutoff < 0 or age_ns <= 0:
        raise ValueError("Age exceeds the supported storage-time range")
    return cutoff


def _identity(st: os.stat_result) -> tuple[int, int]:
    return st.st_dev, st.st_ino


def fingerprint(st: os.stat_result) -> tuple[int, int, int, int, int]:
    return st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns, st.st_ctime_ns


def _regular(st: os.stat_result) -> bool:
    return stat.S_ISREG(st.st_mode) and st.st_nlink == 1


def _open_absolute(path: Path) -> int:
    """Walk every canonical ancestor without following a late symlink."""
    fd = os.open("/", _DIR)
    try:
        for part in path.parts[1:]:
            next_fd = os.open(part, _DIR, dir_fd=fd)
            os.close(fd)
            fd = next_fd
        return fd
    except BaseException:
        os.close(fd)
        raise


class Root:
    def __init__(self, path: Path | str):
        self.path = Path(path).expanduser().resolve(strict=True)
        self.fd = _open_absolute(self.path)
        self._inside_checked_batch = False

    def check(self) -> None:
        if self._inside_checked_batch:
            return
        fd = _open_absolute(self.path)
        try:
            if _identity(os.fstat(fd)) != _identity(os.fstat(self.fd)):
                raise RetentionError("Blob root changed during operation")
        finally:
            os.close(fd)

    @contextmanager
    def checked_batch(self) -> Iterator[None]:
        """Exclusive maintenance checks root ancestry at both batch boundaries.

        Descendant no-follow opens and inode checks remain active per file.
        The cooperative writer lock excludes namespace mutation in the batch.
        """
        if self._inside_checked_batch:
            raise RetentionError("Nested root-maintenance batch")
        self.check()
        self._inside_checked_batch = True
        try:
            yield
        finally:
            self._inside_checked_batch = False
            self.check()

    @contextmanager
    def blobs(self, session: str, *, create: bool = False) -> Iterator[int]:
        component(session)
        self.check()
        if create:
            try:
                os.mkdir(session, dir_fd=self.fd)
            except FileExistsError:
                pass
        session_fd = os.open(session, _DIR, dir_fd=self.fd)
        try:
            if create:
                try:
                    os.mkdir("blobs", dir_fd=session_fd)
                except FileExistsError:
                    pass
            fd = os.open("blobs", _DIR, dir_fd=session_fd)
            try:
                self.check()
                if _identity(os.stat(session, dir_fd=self.fd, follow_symlinks=False)) != _identity(os.fstat(session_fd)):
                    raise RetentionError("Session directory changed")
                if _identity(os.stat("blobs", dir_fd=session_fd, follow_symlinks=False)) != _identity(os.fstat(fd)):
                    raise RetentionError("Blobs directory changed")
                yield fd
                self.check()
                if _identity(os.stat(session, dir_fd=self.fd, follow_symlinks=False)) != _identity(os.fstat(session_fd)):
                    raise RetentionError("Session directory changed")
                if _identity(os.stat("blobs", dir_fd=session_fd, follow_symlinks=False)) != _identity(os.fstat(fd)):
                    raise RetentionError("Blobs directory changed")
            finally:
                os.close(fd)
        finally:
            os.close(session_fd)

    def close(self) -> None:
        os.close(self.fd)

    def __enter__(self) -> Root:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()


class RootLock:
    """Independent open-file description; no process-global lock cache."""

    def __init__(self, path: Path | str, *, exclusive: bool = False, create_root: bool = False):
        self.path = Path(path)
        self.exclusive = exclusive
        self.create_root = create_root
        self.root: Root
        self.fd: int | None = None

    def __enter__(self) -> RootLock:
        if self.create_root:
            self.path.mkdir(parents=True, exist_ok=True)
        self.root = Root(self.path)
        try:
            self.fd = os.open(LOCK, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600, dir_fd=self.root.fd)
            st = os.fstat(self.fd)
            if not _regular(st):
                raise RetentionError("Unsafe blob-retention lock file")
            try:
                fcntl.flock(self.fd, (fcntl.LOCK_EX if self.exclusive else fcntl.LOCK_SH) | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise RetentionError("Blob root is busy: stop writers through their service manager before apply") from exc
            self.root.check()
            if _identity(st) != _identity(os.stat(LOCK, dir_fd=self.root.fd, follow_symlinks=False)):
                raise RetentionError("Blob-retention lock was replaced")
            return self
        except BaseException:
            self.__exit__()
            raise

    def __exit__(self, *args: object) -> None:
        if self.fd is not None:
            os.close(self.fd)  # release only this independent lock
            self.fd = None
        self.root.close()


@dataclass(frozen=True)
class Candidate:
    uri: str
    fp: tuple[int, int, int, int, int]
    allocated: int


@dataclass
class Report:
    root: str
    cutoff_ns: int
    files: int = 0
    logical_bytes: int = 0
    allocated_bytes: int = 0
    retained: int = 0
    skipped: int = 0
    changed: int = 0
    errors: int = 0
    deleted: int = 0
    recovery_required: bool = False
    samples: list[str] = field(default_factory=list)

    def text(self) -> str:
        seconds, nanos = divmod(self.cutoff_ns, 10**9)
        local = datetime.fromtimestamp(seconds, ZoneInfo("America/Los_Angeles"))
        return (
            f"Root: {self.root}\nCutoff: {local:%Y-%m-%d %H:%M:%S}.{nanos:09d} {local:%Z} (America/Los_Angeles)\n"
            f"Eligible files: {self.files}; allocated bytes: {self.allocated_bytes}; logical bytes: {self.logical_bytes}\n"
            f"Retained: {self.retained}; skipped: {self.skipped}; changed: {self.changed}; errors: {self.errors}; deleted: {self.deleted}\n"
            + ("Ledger rollback recovery required after confirmation.\n" if self.recovery_required else "")
            + "".join(f"Sample: {uri}\n" for uri in self.samples)
        ).rstrip()


def candidates(root: Root, cutoff_ns: int, report: Report) -> Iterator[Candidate]:
    """Only root/session/blobs/key.json; memory independent of tree size."""
    with os.scandir(root.fd) as sessions:
        for session in sessions:
            if session.name in (LOCK, METADATA):
                continue
            try:
                component(session.name)
                if not session.is_dir(follow_symlinks=False):
                    report.skipped += 1
                    continue
                with root.blobs(session.name) as fd:
                    with os.scandir(fd) as files:
                        for entry in files:
                            try:
                                if not entry.name.endswith(".json"):
                                    report.skipped += 1
                                    continue
                                key = component(entry.name[:-5])
                                st = os.stat(entry.name, dir_fd=fd, follow_symlinks=False)
                                if not _regular(st):
                                    report.skipped += 1
                                elif st.st_mtime_ns >= cutoff_ns:
                                    report.retained += 1
                                else:
                                    item = Candidate(f"ci-blob://{session.name}/{key}", fingerprint(st), st.st_blocks * 512)
                                    report.files += 1
                                    report.logical_bytes += st.st_size
                                    report.allocated_bytes += item.allocated
                                    if len(report.samples) < 5:
                                        report.samples.append(item.uri)
                                    yield item
                            except ValueError:
                                report.skipped += 1
                            except OSError:
                                report.errors += 1
            except (ValueError, FileNotFoundError, NotADirectoryError):
                report.skipped += 1
            except OSError:
                report.errors += 1


def _preview_root(root: Root, cutoff_ns: int) -> Report:
    try:
        st = os.stat(LOCK, dir_fd=root.fd, follow_symlinks=False)
    except FileNotFoundError:
        pass
    else:
        if not _regular(st):
            raise RetentionError("Unsafe blob-retention lock file")
    report = Report(str(root.path), cutoff_ns)
    for _ in candidates(root, cutoff_ns, report):
        pass
    root.check()
    return report


def preview(path: Path | str, cutoff_ns: int) -> Report:
    with Root(path) as root:
        with Ledger(root):
            pass
        return _preview_root(root, cutoff_ns)


class Ledger:
    """Bounded SQLite tombstones; existing readers never create or migrate."""

    def __init__(self, root: Root, *, writable: bool = False):
        self.root = root
        self.writable = writable
        self.fd: int | None = None
        self.db: sqlite3.Connection | None = None

    def _check(self) -> None:
        self.root.check()
        assert self.fd is not None
        st = os.stat(METADATA, dir_fd=self.root.fd, follow_symlinks=False)
        if _identity(st) != _identity(os.fstat(self.fd)) or st.st_uid != os.geteuid() or st.st_mode & 0o077:
            raise RetentionError("Unsafe or replaced private expiry metadata directory")
        with os.scandir(self.fd) as entries:
            for entry in entries:
                st = os.stat(entry.name, dir_fd=self.fd, follow_symlinks=False)
                if not _regular(st) or st.st_uid != os.geteuid():
                    raise RetentionError("Unsafe entry in expiry metadata")
        if self.db is not None:
            st = os.stat(DATABASE, dir_fd=self.fd, follow_symlinks=False)
            if _identity(st) != self.db_identity:
                raise RetentionError("Expiry ledger was replaced")

    @staticmethod
    def _validate_schema(db: sqlite3.Connection) -> None:
        columns = tuple(row[1] for row in db.execute("PRAGMA table_info(expiry)"))
        if columns != _COLUMNS:
            raise RetentionError("Invalid expiry ledger schema (no automatic migration)")
        db.execute("SELECT uri, dev, ino, size, mtime_ns, ctime_ns, expired_ns, pending FROM expiry LIMIT 1").fetchone()

    def _initialize(self) -> None:
        """Publish only a closed, validated, durable DB; caller holds root EX lock.

        A crash may leave private initialization files, which readers ignore.
        Never adopt them or reinterpret an invalid final ledger as a new one.
        """
        assert self.fd is not None
        name = f"{DATABASE}.init-{uuid.uuid4().hex}"
        fd = os.open(name, os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600, dir_fd=self.fd)
        try:
            self._check()
            path = self.root.path / METADATA / name
            db = sqlite3.connect(path.as_uri() + "?mode=rw", uri=True)
            try:
                if db.execute("PRAGMA journal_mode=DELETE").fetchone()[0] != "delete":
                    raise RetentionError("Expiry ledger must use DELETE journal mode")
                db.execute("PRAGMA synchronous=FULL")
                db.execute("BEGIN IMMEDIATE")
                db.execute(
                    "CREATE TABLE expiry (uri TEXT PRIMARY KEY, dev INTEGER NOT NULL, ino INTEGER NOT NULL, "
                    "size INTEGER NOT NULL, mtime_ns INTEGER NOT NULL, ctime_ns INTEGER NOT NULL, "
                    "expired_ns INTEGER NOT NULL, pending INTEGER NOT NULL) WITHOUT ROWID"
                )
                self._validate_schema(db)
                db.commit()
            finally:
                db.close()
            os.fsync(fd)
            self._check()
            if _identity(os.stat(name, dir_fd=self.fd, follow_symlinks=False)) != _identity(os.fstat(fd)):
                raise RetentionError("Expiry ledger initialization file was replaced")
            os.fsync(self.fd)
            os.rename(name, DATABASE, src_dir_fd=self.fd, dst_dir_fd=self.fd)
            os.fsync(self.fd)
        finally:
            os.close(fd)

    def __enter__(self) -> Ledger:
        try:
            if self.writable:
                try:
                    os.mkdir(METADATA, 0o700, dir_fd=self.root.fd)
                    os.fsync(self.root.fd)
                except FileExistsError:
                    pass
            try:
                self.fd = os.open(METADATA, _DIR, dir_fd=self.root.fd)
            except FileNotFoundError:
                if not self.writable:
                    return self
                raise
            self._check()
            try:
                fd = os.open(DATABASE, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=self.fd)
            except FileNotFoundError:
                if not self.writable:
                    return self
                self._initialize()
                fd = os.open(DATABASE, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=self.fd)
            try:
                st = os.fstat(fd)
                if not _regular(st):
                    raise RetentionError("Unsafe expiry ledger file")
                self.db_identity = _identity(st)
            finally:
                os.close(fd)
            path = self.root.path / METADATA / DATABASE
            self.db = sqlite3.connect(path.as_uri() + ("?mode=rw" if self.writable else "?mode=ro"), uri=True)
            self._check()
            if self.writable:
                if self.db.execute("PRAGMA journal_mode=DELETE").fetchone()[0] != "delete":
                    raise RetentionError("Expiry ledger must use DELETE journal mode")
                self.db.execute("PRAGMA synchronous=FULL")
            self._validate_schema(self.db)
            return self
        except BaseException:
            self.__exit__()
            raise

    def contains(self, uri: str) -> bool:
        parse_uri(uri)
        if self.db is None:
            return False
        self._check()
        return self.db.execute("SELECT 1 FROM expiry WHERE uri=?", (uri,)).fetchone() is not None

    def commit(self) -> None:
        assert self.db is not None and self.fd is not None
        self._check()
        self.db.commit()
        # FULL synchronizes database/journal; also persist DELETE's directory change.
        os.fsync(self.fd)
        self._check()

    def journal(self, items: list[Candidate]) -> None:
        assert self.db is not None
        self.db.executemany(
            "INSERT OR IGNORE INTO expiry VALUES (?, ?, ?, ?, ?, ?, ?, 1)",
            ((item.uri, *item.fp, time.time_ns()) for item in items),
        )
        self.commit()  # MUST complete successfully before the first unlink

    def __exit__(self, *args: object) -> None:
        if self.db is not None:
            self.db.close()
            self.db = None
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None


def expired(root: Root, uri: str) -> bool:
    with Ledger(root) as ledger:
        return ledger.contains(uri)


def _matches(root: Root, item: Candidate) -> bool:
    session, key = parse_uri(item.uri)
    try:
        with root.blobs(session) as fd:
            st = os.stat(key + ".json", dir_fd=fd, follow_symlinks=False)
            return _regular(st) and fingerprint(st) == item.fp
    except FileNotFoundError:
        return False


@contextmanager
def _sync_dirs() -> Iterator[dict[tuple[int, int], int]]:
    """Pin at most one descriptor per batch directory; sync before DB commit."""
    directories: dict[tuple[int, int], int] = {}
    try:
        yield directories
    finally:
        failure: OSError | None = None
        for fd in directories.values():
            try:
                os.fsync(fd)
            except OSError as exc:
                failure = failure or exc
            finally:
                try:
                    os.close(fd)
                except OSError as exc:
                    # A failed close must not abandon later pinned directories.
                    # Do not retry: the descriptor may already have been closed.
                    failure = failure or exc
        if failure is not None:
            raise failure


def _pin_directory(fd: int, directories: dict[tuple[int, int], int]) -> None:
    identity = _identity(os.fstat(fd))
    if identity not in directories:
        directories[identity] = os.dup(fd)


def _unlink(
    root: Root,
    ledger: Ledger,
    item: Candidate,
    report: Report,
    *,
    dirty_dirs: dict[tuple[int, int], int] | None = None,
) -> None:
    """Never delete an updated/restored inode or a URI with a different record."""
    assert ledger.db is not None
    row = ledger.db.execute("SELECT dev, ino, size, mtime_ns, ctime_ns, pending FROM expiry WHERE uri=?", (item.uri,)).fetchone()
    if row is None or tuple(row[:5]) != item.fp or not row[5]:
        report.changed += 1
        return
    session, key = parse_uri(item.uri)
    try:
        ledger._check()
        with root.blobs(session) as fd:
            try:
                st = os.stat(key + ".json", dir_fd=fd, follow_symlinks=False)
            except FileNotFoundError:
                # A previous process may have died after unlink but before
                # syncing this directory. Absence must be durable too.
                if dirty_dirs is not None:
                    _pin_directory(fd, dirty_dirs)
                else:
                    os.fsync(fd)
            else:
                if not _regular(st) or fingerprint(st) != item.fp:
                    report.changed += 1
                    return
                if dirty_dirs is not None:
                    _pin_directory(fd, dirty_dirs)  # failure must precede unlink
                os.unlink(key + ".json", dir_fd=fd)
                report.deleted += 1
                if dirty_dirs is None:
                    os.fsync(fd)
    except FileNotFoundError:
        return  # missing ancestor cannot be synced; conservatively keep pending
    except (OSError, ValueError):
        report.errors += 1
        return
    ledger.db.execute("UPDATE expiry SET pending=0 WHERE uri=?", (item.uri,))


def _recover(root: Root, ledger: Ledger, report: Report) -> None:
    assert ledger.db is not None
    # Keyset pagination, not OFFSET, and never fetch the full ledger into memory.
    # An earlier confirmation does not authorize deletion under today's policy.
    # Exact fingerprint checks in _unlink keep present deletions within the fresh
    # confirmed preview; immutable records outside this cutoff remain pending.
    after = ""
    while True:
        rows = ledger.db.execute(
            "SELECT uri, dev, ino, size, mtime_ns, ctime_ns FROM expiry "
            "WHERE pending=1 AND mtime_ns<? AND uri>? ORDER BY uri LIMIT ?",
            (report.cutoff_ns, after, BATCH_SIZE)
        ).fetchall()
        if not rows:
            break
        with root.checked_batch():
            with _sync_dirs() as directories:
                for row in rows:
                    _unlink(
                        root, ledger, Candidate(row[0], tuple(row[1:]), 0),
                        report, dirty_dirs=directories,
                    )
        ledger.commit()
        after = rows[-1][0]


def apply(
    path: Path | str,
    cutoff_ns: int,
    confirm: Callable[[Report], bool],
) -> Report:
    """Exclusive lock spans fresh preview, confirmation, journal, and unlink.

    Callers must supply an interactive confirmation callback. Library users may
    provide their own explicit human gate; the CLI has no noninteractive bypass.
    """
    with RootLock(path, exclusive=True) as lock:
        root = lock.root
        # Read-only validation before asking; no writable recovery before consent.
        recovery_required = False
        try:
            with Ledger(root):
                pass
        except sqlite3.OperationalError as exc:
            # A valid hot rollback journal requires writable SQLite recovery.
            # Preview alone must remain read-only; confirmed apply may recover
            # it under the exclusive root lock. Do not mask other DB failures.
            if getattr(exc, "sqlite_errorcode", None) != sqlite3.SQLITE_READONLY_ROLLBACK:
                raise
            recovery_required = True
        report = _preview_root(root, cutoff_ns)
        report.recovery_required = recovery_required
        if not confirm(report):
            raise RetentionError("Prune not confirmed; no payloads deleted")
        result = Report(str(root.path), cutoff_ns)
        with Ledger(root, writable=True) as ledger:
            _recover(root, ledger, result)
            batch: list[Candidate] = []

            def flush() -> None:
                with root.checked_batch():
                    ready = []
                    for item in batch:
                        if ledger.contains(item.uri):
                            # Never replace a restored URI's tombstone.
                            result.skipped += 1
                        elif _matches(root, item):
                            ready.append(item)
                        else:
                            result.changed += 1
                    ledger.journal(ready)
                    with _sync_dirs() as directories:
                        for item in ready:
                            _unlink(root, ledger, item, result, dirty_dirs=directories)
                ledger.commit()
                batch.clear()

            for item in candidates(root, cutoff_ns, result):
                batch.append(item)
                if len(batch) == BATCH_SIZE:
                    flush()
            if batch:
                flush()
        return result


@dataclass
class ScanCursor:
    """Only directory cookies and identities; never open fds or a tree-sized list."""

    root_identity: tuple[int, int] | None = None
    root_cookie: int = 0
    session: str | None = None
    session_cookie: int = 0
    blobs_identity: tuple[int, int] | None = None
    done: bool = False


@contextmanager
def _directory_entries(fd: int, cookie: int) -> Iterator[Iterator[tuple[str, int]]]:
    """Linux 64-bit directory cookies, qualified by live-scan tests.

    POSIX telldir cookies are NOT portable across reopened DIR streams (notably
    on macOS). Linux glibc uses the filesystem's offset directly. Refuse other
    ABIs rather than silently skip entries. No DIR or descriptor escapes a window.
    Cookies provide a live estimate under mutations, not snapshot completeness.
    """
    if sys.platform != "linux" or ctypes.sizeof(ctypes.c_void_p) != 8:
        raise RetentionError("Online bounded directory scans require 64-bit Linux; use offline prune")

    class Dirent(ctypes.Structure):
        _fields_ = [
            ("ino", ctypes.c_ulong), ("off", ctypes.c_long),
            ("reclen", ctypes.c_ushort), ("type", ctypes.c_ubyte),
            ("name", ctypes.c_char * 256),
        ]

    libc = ctypes.CDLL(None, use_errno=True)
    libc.fdopendir.argtypes, libc.fdopendir.restype = [ctypes.c_int], ctypes.c_void_p
    libc.readdir.argtypes, libc.readdir.restype = [ctypes.c_void_p], ctypes.POINTER(Dirent)
    libc.seekdir.argtypes, libc.seekdir.restype = [ctypes.c_void_p, ctypes.c_long], None
    libc.telldir.argtypes, libc.telldir.restype = [ctypes.c_void_p], ctypes.c_long
    libc.closedir.argtypes, libc.closedir.restype = [ctypes.c_void_p], ctypes.c_int
    owned = os.open(".", _DIR, dir_fd=fd)
    stream = libc.fdopendir(owned)
    if not stream:
        os.close(owned)
        raise OSError(ctypes.get_errno(), "fdopendir failed")
    try:
        libc.seekdir(stream, cookie)

        def entries() -> Iterator[tuple[str, int]]:
            while True:
                ctypes.set_errno(0)
                entry = libc.readdir(stream)
                if not entry:
                    if error := ctypes.get_errno():
                        raise OSError(error, "readdir failed")
                    return
                name = os.fsdecode(entry.contents.name)
                yield name, libc.telldir(stream)

        yield entries()
    finally:
        if libc.closedir(stream) != 0:
            raise OSError(ctypes.get_errno(), "closedir failed")


def scan_window(
    path: Path, cursor: ScanCursor, report: Report, preview_started_ns: int,
    *, max_entries: int = 512, budget_ns: int = 10_000_000,
    max_candidates: int = 32,
) -> list[Candidate]:
    """Caller holds a process-local shared lease; all resources close on return."""
    items: list[Candidate] = []
    inspected = 0
    started = time.monotonic_ns()

    def full() -> bool:
        return (inspected >= max_entries or len(items) >= max_candidates
                or (inspected > 0 and time.monotonic_ns() - started >= budget_ns))

    with Root(path) as root:
        identity = _identity(os.fstat(root.fd))
        if cursor.root_identity is not None and cursor.root_identity != identity:
            raise RetentionError("Blob root changed between scan windows")
        cursor.root_identity = identity
        while not cursor.done and not full():
            if cursor.session is None:
                with _directory_entries(root.fd, cursor.root_cookie) as entries:
                    for name, cookie in entries:
                        inspected += 1
                        cursor.root_cookie = cookie
                        if name not in (".", "..", LOCK, METADATA):
                            try:
                                component(name)
                                st = os.stat(name, dir_fd=root.fd, follow_symlinks=False)
                                if stat.S_ISDIR(st.st_mode):
                                    cursor.session = name
                                    cursor.session_cookie = 0
                                    cursor.blobs_identity = None
                                    break
                                report.skipped += 1
                            except (ValueError, FileNotFoundError):
                                report.skipped += 1
                        if full():
                            return items
                    else:
                        cursor.done = True
                if cursor.session is None or full():
                    continue
            try:
                with root.blobs(cursor.session) as fd:
                    identity = _identity(os.fstat(fd))
                    if cursor.blobs_identity is not None and identity != cursor.blobs_identity:
                        report.changed += 1
                        cursor.session = None
                        continue
                    cursor.blobs_identity = identity
                    with _directory_entries(fd, cursor.session_cookie) as entries:
                        for name, cookie in entries:
                            inspected += 1
                            cursor.session_cookie = cookie
                            if name not in (".", ".."):
                                try:
                                    if not name.endswith(".json"):
                                        report.skipped += 1
                                    else:
                                        key = component(name[:-5])
                                        st = os.stat(name, dir_fd=fd, follow_symlinks=False)
                                        if not _regular(st):
                                            report.skipped += 1
                                        elif st.st_mtime_ns >= report.cutoff_ns or st.st_ctime_ns > preview_started_ns:
                                            report.retained += 1
                                        else:
                                            item = Candidate(f"ci-blob://{cursor.session}/{key}", fingerprint(st), st.st_blocks * 512)
                                            items.append(item)
                                            report.files += 1
                                            report.logical_bytes += st.st_size
                                            report.allocated_bytes += item.allocated
                                            if len(report.samples) < 5:
                                                report.samples.append(item.uri)
                                except (ValueError, FileNotFoundError):
                                    report.skipped += 1
                                except OSError:
                                    report.errors += 1
                            if full():
                                return items
                        cursor.session = None
            except (FileNotFoundError, NotADirectoryError):
                report.skipped += 1
                cursor.session = None
            except OSError:
                # ELOOP and unsafe directories are skipped, never followed.
                report.errors += 1
                cursor.session = None
    return items


def recovery_page(path: Path, after: str, cutoff_ns: int, preview_started_ns: int) -> tuple[str, list[Candidate], int]:
    """At most 512 PRIMARY KEY rows, including NONpending rows, under SH lease."""
    with Root(path) as root, Ledger(root) as ledger:
        if ledger.db is None:
            return after, [], 0
        rows = ledger.db.execute(
            "SELECT uri, dev, ino, size, mtime_ns, ctime_ns, pending FROM expiry "
            "WHERE uri>? ORDER BY uri LIMIT 512", (after,),
        ).fetchall()
        return (
            rows[-1][0] if rows else after,
            [Candidate(row[0], tuple(row[1:6]), 0) for row in rows
             if row[6] and row[4] < cutoff_ns and row[5] <= preview_started_ns],
            len(rows),
        )


def apply_batch(path: Path, items: list[Candidate], report: Report, *, recovery: bool = False) -> None:
    """Caller holds EX in-process lease, never an upgraded flock."""
    if len(items) > 64:
        raise RetentionError("Online batch exceeds 64 candidates")
    with Root(path) as root, root.checked_batch(), Ledger(root, writable=True) as ledger:
        ready = []
        for item in items:
            if item.fp[3] >= report.cutoff_ns:
                raise RetentionError("Candidate exceeds frozen cutoff")
            if recovery:
                ready.append(item)
            elif ledger.contains(item.uri):
                report.skipped += 1
            elif _matches(root, item):
                ready.append(item)
            else:
                report.changed += 1
        if not recovery:
            ledger.journal(ready)
        with _sync_dirs() as directories:
            for item in ready:
                _unlink(root, ledger, item, report, dirty_dirs=directories)
        ledger.commit()