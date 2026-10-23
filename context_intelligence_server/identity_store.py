"""Durable identity-map store for the Context Intelligence Server.

Each ``IdentityStore`` wraps ONE JSON file and keeps an in-process dict
(``_data``) that IS the live source of truth for the single-replica process.
A second derived dict, ``flat_dict``, exposes ``{key: contributor_id}`` and is
kept in-sync with ``_data`` via in-place mutations so that any object holding a
reference to ``flat_dict`` always sees the latest state without a restart.

**Commit order (ROB F2 — NON-NEGOTIABLE)**

On every mutation (put / delete):

1. Build the new data dict (do NOT touch ``_data`` yet).
2. Serialize and write to a tempfile **in the same directory** as the target file.
3. ``os.replace()`` the tempfile onto the target (atomic rename on POSIX / Azure Files).
4. **ONLY IF the above succeeds**: update ``_data`` and ``flat_dict`` in-place.

If the file write raises for any reason, ``_data`` and ``flat_dict`` are
**unchanged** and the exception propagates to the caller (who returns 5xx).
The file and memory are never out of sync.

**Fail-CLOSED load()**

On ``load()``:

- Missing file → empty dict (normal first boot). No log, no raise.
- Corrupt / torn / partial / invalid-JSON file → **empty dict + a LOUD
  ``logger.error`` / ``logger.critical``**.  The server MUST NOT crash-loop on
  a bad store file.  An empty map means "nobody is bound yet" — every auth
  attempt then fails normally until an admin re-populates via the /admin API.

**Managed (production) ownership contract**

A managed store works only while its ``IdentityStoreLock`` is attached and
verified.  ``healthy()`` re-checks the bindings on every call and gates
authentication.  Two kinds of failure latch the store UNAVAILABLE, clear the
served credential cache, and refuse every further operation until an orderly
``stop()`` and a fresh ``attach()``/``load()`` reload the file under the lock:

* an ownership failure (directory replaced or retargeted, sidecar swapped,
  permissions loosened, inherited descriptor);
* an UNCERTAIN commit: the file was atomically replaced but the directory sync
  failed.  Disk may be ahead of memory and nothing is rolled back; the caller
  gets an error (never success) and the old cache is not served.

A managed store also never converts an I/O failure into credentials: an
ordinary ``OSError`` reading the file, or writing the first-boot seed, clears the
served cache and propagates (corrupt CONTENT stays the loud empty-map fallback).

A failure BEFORE the rename took effect is a certain rollback: memory and disk
are unchanged, the store stays healthy, and the plain ``OSError`` propagates.

File format (both modes share the same abstraction)::

    # api-keys.json
    {
      "<sha256_hex>": {"id": "<contributor_id>"}
    }

    # entra-identities.json
    {
      "<oid>": {"id": "<contributor_id>", "display_name": "<optional>"}
    }
"""

import json
import logging
import os
import tempfile
import threading
from pathlib import Path

from context_intelligence_server.instance_lock import (
    IdentityStoreLock,
    IdentityStoreOwnershipError,
)

logger = logging.getLogger(__name__)


class IdentityStore:
    """Durable, write-through identity map backed by a single JSON file.

    See module docstring for the commit-order contract and fail-closed guarantees.

    Args:
        path: Absolute path to the JSON store file.  The parent directory is
              created automatically on the first write.
        managed: When True (production), every operation requires an attached
              ``IdentityStoreLock`` that is still owned by this process, and
              all file I/O is anchored to the lock's pinned directory.  A
              managed store with no lock attached refuses to operate.
    """

    def __init__(self, path: Path, *, managed: bool = False) -> None:
        self.path = path
        self._managed = managed
        self._lock: IdentityStoreLock | None = None
        self._stopped = managed  # a managed store is inert until attach()
        self._ownership_failed = False
        # Serializes load/seed/put/delete and shutdown against each other.
        self._op_lock = threading.RLock()
        # Rich format: {key: {id: ..., display_name?: ...}}
        self._data: dict[str, dict[str, str]] = {}
        # Flat derived cache: {key: contributor_id}.
        # This dict object is shared with the BearerTokenMiddleware keystore.
        # It is mutated IN-PLACE so existing references always see live data.
        self.flat_dict: dict[str, str] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def attach(self, lock: IdentityStoreLock) -> None:
        """Bind an owned lock and (re)enable operations; call before ``load``."""
        with self._op_lock:
            self._lock = lock
            self._stopped = False
            self._ownership_failed = False

    def stop(self) -> None:
        """Refuse further operations, then clear the in-process map in place.

        Waits for any in-flight operation (via the operation lock), so no
        write can land after ownership is released.
        """
        with self._op_lock:
            self._stopped = True
            self._lock = None
            self._data = {}
            self.flat_dict.clear()

    def healthy(self) -> bool:
        """True only while ownership is intact RIGHT NOW (gate for every auth).

        Re-verifies the pinned bindings with a few ``stat`` calls (parent and
        configured-path identity, sidecar identity/permissions, store entry), so
        a replaced or retargeted directory, a swapped sidecar, or loosened
        permissions disable authentication immediately instead of at the next
        mutation.  A failed check latches the store unavailable.
        """
        if not self._managed:
            return True
        lock = self._lock
        if (
            self._stopped
            or self._ownership_failed
            or lock is None
            or not lock.owned_by_this_process()
        ):
            return False
        try:
            lock.assert_owned()
        except IdentityStoreOwnershipError:
            self._latch()
            return False
        return True

    @property
    def failure_detected(self) -> bool:
        """True once a runtime failure (ownership loss, uncertain commit) latched.

        A plain cached flag set by ``_latch``: reading it does no I/O, so
        ``/status`` can publish a detected failure without probing the store.
        """
        return self._ownership_failed

    def exists(self) -> bool:
        """Whether the store file exists (ownership-verified when managed)."""
        with self._op_lock:
            self._require_ownership()
            if self._lock is not None:
                return self._guarded(self._lock.store_exists)
            return self.path.exists()

    def load(self) -> None:
        """Read the file and populate the in-process map.

        Missing file → empty dict (normal first boot, no log).
        Corrupt / non-dict → empty dict + LOUD error log, never raise.
        Ordinary I/O error (``OSError``): unmanaged → empty dict + loud log; a
        MANAGED store clears its cache and raises, because an unreadable file is
        not an empty credential map.
        """
        with self._op_lock:
            self._load_locked()

    def _load_locked(self) -> None:
        self._require_ownership()
        raw: object
        try:
            text = self._read_text()
            if text is None:
                # Normal first boot — the file hasn't been written yet.
                self._data = {}
                self._rebuild_flat()
                return
            raw = json.loads(text)
        except IdentityStoreOwnershipError:
            raise
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            logger.error(
                "identity_store.load CORRUPT FILE path=%s error=%r — "
                "failing CLOSED to empty map.  Re-populate via /admin API.",
                self.path,
                exc,
            )
            self._data = {}
            self._rebuild_flat()
            return
        except OSError as exc:
            # An I/O error says nothing about the file's content: for a managed
            # store it is NOT "no credentials".  Drop the served cache and
            # propagate so identity init latches unavailable (503) instead of
            # reporting ready with an empty map that an admin write could then
            # persist over the real file.  Unmanaged stores keep the old
            # lenient fallback.
            self._data = {}
            self._rebuild_flat()
            if self._managed:
                logger.error(
                    "identity_store.load READ FAILED path=%s error=%r — "
                    "identity unavailable.",
                    self.path,
                    exc,
                )
                raise
            logger.error(
                "identity_store.load CORRUPT FILE path=%s error=%r — "
                "failing CLOSED to empty map.  Re-populate via /admin API.",
                self.path,
                exc,
            )
            return

        if not isinstance(raw, dict):
            logger.critical(
                "identity_store.load INVALID FORMAT path=%s got=%r — "
                "expected a JSON object at top level.  Failing CLOSED to empty map.",
                self.path,
                type(raw).__name__,
            )
            self._data = {}
            self._rebuild_flat()
            return

        # Accept the data.  Individual entries with missing/bad types are tolerated
        # by the load (they'll just produce no flat_dict entry), since the validator
        # on put() guards inbound writes.
        self._data = {k: v for k, v in raw.items() if isinstance(v, dict)}
        self._rebuild_flat()

    def put(self, key: str, value: dict[str, str]) -> None:
        """Upsert *key* → *value*.

        Commit order (F2): write tempfile → os.replace → update in-process.
        Raises on file-write failure; in-process state is UNCHANGED.
        """
        with self._op_lock:
            self._require_ownership()
            new_data = dict(self._data)
            new_data[key] = value
            self._write_atomic(new_data)
            # File write succeeded — now update in-process state.
            self._data[key] = value
            contributor_id = value.get("id", "")
            if contributor_id:
                self.flat_dict[key] = contributor_id
            else:
                self.flat_dict.pop(key, None)

    def delete(self, key: str) -> None:
        """Remove *key* from the store.

        Commit order (F2): write tempfile → os.replace → update in-process.
        Raises on file-write failure; in-process state is UNCHANGED.
        No-op if *key* is not present.
        """
        with self._op_lock:
            self._require_ownership()
            new_data = {k: v for k, v in self._data.items() if k != key}
            self._write_atomic(new_data)
            # File write succeeded — now update in-process state.
            self._data.pop(key, None)
            self.flat_dict.pop(key, None)

    def seed(self, data: dict[str, dict[str, str]]) -> None:
        """Bulk-seed from config on first boot.

        Unlike ``put()`` (which enforces F2 write-before-memory strictly), this
        method is designed for startup initialization from durable config.  It:

        1. Tries an atomic write of *data* to the store file.
        2. UNMANAGED stores only: whether or not the write succeeds, updates
           ``_data`` and ``flat_dict`` in-place (the data came from config, which
           is itself durable; a restart would re-seed from config again, so
           memory-ahead-of-disk is safe for a store that owns no lock).

        A warning is logged if an unmanaged write fails so an operator knows the
        store file was not written.  A MANAGED store propagates the write error
        and leaves memory untouched, so identity init reports unavailable.
        """
        with self._op_lock:
            self._seed_locked(data)

    def _seed_locked(self, data: dict[str, dict[str, str]]) -> None:
        self._require_ownership()
        try:
            self._write_atomic(data)
        except IdentityStoreOwnershipError:
            # Lost/unsafe ownership must never degrade to "memory only".
            raise
        except Exception as exc:
            if self._managed:
                # Never serve credentials that were not persisted: memory must
                # not get ahead of disk under ownership.  Nothing was updated.
                raise
            logger.warning(
                "identity_store.seed: could not write seed to %s: %r "
                "— in-memory map is live but the file is not yet persisted.  "
                "The next mutation via /admin API will persist the file.",
                self.path,
                exc,
            )
        # Update in-memory regardless — data is from durable config.
        self._data = dict(data)
        self._rebuild_flat()

    def get(self, key: str) -> dict[str, str] | None:
        """Return the value for *key*, or ``None`` if not present."""
        return self._data.get(key)

    def items(self):  # type: ignore[override]
        """Iterate over ``(key, value)`` pairs in the store."""
        return self._data.items()

    def __len__(self) -> int:
        return len(self._data)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _require_ownership(self) -> None:
        if not self._managed:
            return
        if self._stopped:
            raise IdentityStoreOwnershipError("identity store is stopped")
        if self._ownership_failed:
            raise IdentityStoreOwnershipError("identity store ownership was lost")
        if self._lock is None:
            raise IdentityStoreOwnershipError("identity store has no owner attached")

    def _latch(self) -> None:
        """Mark the store unavailable until an orderly stop and re-attach.

        The served credential cache is cleared as well: after an ownership loss
        or an uncertain commit it can no longer be trusted, and it is never
        presented as rolled back.  The lock stays held until ``stop()``.
        """
        self._ownership_failed = True
        self.flat_dict.clear()

    def _guarded(self, fn, *args):  # type: ignore[no-untyped-def]
        """Run a lock-anchored call; an ownership failure disables the store."""
        try:
            return fn(*args)
        except IdentityStoreOwnershipError:
            self._latch()
            raise

    def _read_text(self) -> str | None:
        if self._lock is not None:
            raw = self._guarded(self._lock.read_store_bytes)
            return None if raw is None else raw.decode("utf-8")
        if not self.path.exists():
            return None
        return self.path.read_text(encoding="utf-8")

    def _rebuild_flat(self) -> None:
        """Rebuild ``flat_dict`` IN-PLACE from ``_data``.

        Uses ``dict.clear()`` + ``dict.update()`` so existing references to
        ``flat_dict`` continue pointing at the same dict object.
        """
        self.flat_dict.clear()
        for key, meta in self._data.items():
            contributor_id = meta.get("id", "")
            if contributor_id:
                self.flat_dict[key] = contributor_id

    def _write_atomic(self, data: dict[str, dict[str, str]]) -> None:
        """Write *data* atomically to ``self.path``.

        Steps:
        1. Create parent directory (parents=True, exist_ok=True).
        2. Write to a tempfile in the SAME directory (ensures os.replace is
           a local rename, not a cross-device copy).
        3. fsync the tempfile so data is on stable storage.
        4. ``os.replace()`` onto the target path.
        5. On any failure: delete the tempfile and re-raise.

        Raises the underlying OS/IO exception so the caller (put/delete)
        knows the write failed and leaves in-process state unchanged.
        """
        if self._lock is not None:
            payload = json.dumps(data, indent=2, ensure_ascii=False).encode("utf-8")
            self._guarded(self._lock.write_store_atomic, payload)
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp_fd, tmp_str = tempfile.mkstemp(dir=str(self.path.parent), suffix=".tmp")
        tmp_path = Path(tmp_str)
        try:
            with os.fdopen(tmp_fd, "w", encoding="utf-8") as fh:
                json.dump(data, fh, indent=2, ensure_ascii=False)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(str(tmp_path), str(self.path))
        except Exception:
            # Best-effort cleanup of the tempfile before propagating.
            try:
                tmp_path.unlink(missing_ok=True)
            except Exception:
                pass
            raise
