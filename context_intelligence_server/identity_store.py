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
from pathlib import Path

logger = logging.getLogger(__name__)


class IdentityStore:
    """Durable, write-through identity map backed by a single JSON file.

    See module docstring for the commit-order contract and fail-closed guarantees.

    Args:
        path: Absolute path to the JSON store file.  The parent directory is
              created automatically on the first write.
    """

    def __init__(self, path: Path) -> None:
        self.path = path
        # Rich format: {key: {id: ..., display_name?: ...}}
        self._data: dict[str, dict[str, str]] = {}
        # Flat derived cache: {key: contributor_id}.
        # This dict object is shared with the BearerTokenMiddleware keystore.
        # It is mutated IN-PLACE so existing references always see live data.
        self.flat_dict: dict[str, str] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def load(self) -> None:
        """Read the file and populate the in-process map.

        Missing file → empty dict (normal first boot, no log).
        Corrupt / non-dict → empty dict + LOUD error log, never raise.
        """
        if not self.path.exists():
            # Normal first boot — the file hasn't been written yet.
            self._data = {}
            self._rebuild_flat()
            return

        raw: object
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError, OSError) as exc:
            logger.error(
                "identity_store.load CORRUPT FILE path=%s error=%r — "
                "failing CLOSED to empty map.  Re-populate via /admin API.",
                self.path,
                exc,
            )
            self._data = {}
            self._rebuild_flat()
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
        2. Whether or not the write succeeds, updates ``_data`` and ``flat_dict``
           in-place (the data came from config, which is itself durable; a restart
           would re-seed from config again, so memory-ahead-of-disk is safe here).

        A warning is logged if the write fails so an operator knows the store file
        was not written (e.g., ``/data`` not yet mounted at startup).
        """
        try:
            self._write_atomic(data)
        except Exception as exc:
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
