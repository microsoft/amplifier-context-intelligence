"""Tests for IdentityStore — T2 (runtime-identity-map).

Contract under test (ROB F2, non-negotiable):
  write-file-then-swap-memory on every mutation.
  If the file write raises, the in-process dict is UNCHANGED.
  The store NEVER raises on load() regardless of file state.
"""

import json
import logging
import os
from pathlib import Path
from unittest.mock import patch

import pytest

from context_intelligence_server.identity_store import IdentityStore


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

FAKE_HASH_A = "a" * 64  # 64-char hex string — stands in for a real sha256 digest
FAKE_HASH_B = "b" * 64
FAKE_OID = "11111111-1111-1111-1111-111111111111"


def _alice_entry() -> dict[str, str]:
    return {"id": "alice"}


def _bob_entry() -> dict[str, str]:
    return {"id": "bob", "display_name": "Bob Smith"}


# ---------------------------------------------------------------------------
# T2.1 — Basic put / get / items round-trip
# ---------------------------------------------------------------------------


class TestPutGetRoundtrip:
    def test_put_then_get_returns_value(self, tmp_path: Path) -> None:
        """put(key, value) → get(key) returns that value immediately."""
        store = IdentityStore(path=tmp_path / "store.json")
        store.load()

        store.put(FAKE_HASH_A, _alice_entry())

        result = store.get(FAKE_HASH_A)
        assert result == _alice_entry()

    def test_put_persists_to_file_and_loads_fresh(self, tmp_path: Path) -> None:
        """Round-trip: put → create new store → load → value is present."""
        store_path = tmp_path / "store.json"
        store = IdentityStore(path=store_path)
        store.load()
        store.put(FAKE_HASH_A, _alice_entry())

        # New store instance reads from disk
        store2 = IdentityStore(path=store_path)
        store2.load()
        assert store2.get(FAKE_HASH_A) == _alice_entry()

    def test_get_missing_key_returns_none(self, tmp_path: Path) -> None:
        """get() returns None for a key that was never put."""
        store = IdentityStore(path=tmp_path / "store.json")
        store.load()
        assert store.get(FAKE_HASH_A) is None

    def test_delete_removes_key(self, tmp_path: Path) -> None:
        """delete(key) removes the entry from in-process dict AND file."""
        store_path = tmp_path / "store.json"
        store = IdentityStore(path=store_path)
        store.load()
        store.put(FAKE_HASH_A, _alice_entry())
        store.delete(FAKE_HASH_A)

        assert store.get(FAKE_HASH_A) is None

        # Verify file is also updated
        store2 = IdentityStore(path=store_path)
        store2.load()
        assert store2.get(FAKE_HASH_A) is None

    def test_items_returns_all_entries(self, tmp_path: Path) -> None:
        """items() yields all key-value pairs currently in the store."""
        store = IdentityStore(path=tmp_path / "store.json")
        store.load()
        store.put(FAKE_HASH_A, _alice_entry())
        store.put(FAKE_HASH_B, _bob_entry())

        result = dict(store.items())
        assert result == {FAKE_HASH_A: _alice_entry(), FAKE_HASH_B: _bob_entry()}

    def test_upsert_overwrites_existing(self, tmp_path: Path) -> None:
        """put() on an existing key overwrites the value."""
        store = IdentityStore(path=tmp_path / "store.json")
        store.load()
        store.put(FAKE_HASH_A, _alice_entry())
        store.put(FAKE_HASH_A, {"id": "alice-updated"})

        assert store.get(FAKE_HASH_A) == {"id": "alice-updated"}

    def test_sequential_puts_all_persist(self, tmp_path: Path) -> None:
        """Multiple sequential puts all persist correctly (each write is the full map)."""
        store_path = tmp_path / "store.json"
        store = IdentityStore(path=store_path)
        store.load()
        store.put(FAKE_HASH_A, _alice_entry())
        store.put(FAKE_HASH_B, _bob_entry())

        store2 = IdentityStore(path=store_path)
        store2.load()
        assert store2.get(FAKE_HASH_A) == _alice_entry()
        assert store2.get(FAKE_HASH_B) == _bob_entry()


# ---------------------------------------------------------------------------
# T2.2 — load() fail-closed contract
# ---------------------------------------------------------------------------


class TestLoadFailClosed:
    def test_missing_file_yields_empty_dict(self, tmp_path: Path) -> None:
        """Missing store file → load() yields empty dict (normal first boot), no raise."""
        store = IdentityStore(path=tmp_path / "nonexistent.json")
        store.load()  # must not raise
        assert store.get(FAKE_HASH_A) is None
        assert list(store.items()) == []

    def test_corrupt_json_yields_empty_dict_and_logs_error(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Corrupt JSON file → load() yields empty dict, logs a LOUD error, does NOT raise."""
        store_path = tmp_path / "store.json"
        store_path.write_text("{{{{not valid json at all}}}}", encoding="utf-8")

        store = IdentityStore(path=store_path)
        with caplog.at_level(logging.ERROR):
            store.load()  # must NOT raise

        assert store.get(FAKE_HASH_A) is None
        # A LOUD error (ERROR or CRITICAL) must be logged
        error_records = [r for r in caplog.records if r.levelno >= logging.ERROR]
        assert error_records, (
            "load() on a corrupt file must emit at least one ERROR/CRITICAL log record"
        )

    def test_valid_json_but_not_dict_yields_empty_and_logs(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """JSON array (not a dict) → load() yields empty dict, logs error, does NOT raise."""
        store_path = tmp_path / "store.json"
        store_path.write_text(
            json.dumps([{"id": "alice"}]), encoding="utf-8"
        )  # list, not dict

        store = IdentityStore(path=store_path)
        with caplog.at_level(logging.ERROR):
            store.load()  # must NOT raise

        assert store.get(FAKE_HASH_A) is None
        error_records = [r for r in caplog.records if r.levelno >= logging.ERROR]
        assert error_records

    def test_partial_write_torn_file_loads_empty(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Truncated / partial JSON simulates a torn write → empty + error log, no raise."""
        store_path = tmp_path / "store.json"
        store_path.write_bytes(b'{"aaa": {"id": "al')  # truncated mid-write

        store = IdentityStore(path=store_path)
        with caplog.at_level(logging.ERROR):
            store.load()  # must NOT raise

        assert store.get(FAKE_HASH_A) is None
        error_records = [r for r in caplog.records if r.levelno >= logging.ERROR]
        assert error_records


# ---------------------------------------------------------------------------
# T2.3 — write-file-then-swap-memory (ROB F2)
# ---------------------------------------------------------------------------


class TestWriteFileThenSwapMemory:
    def test_put_write_failure_leaves_dict_unchanged(self, tmp_path: Path) -> None:
        """If os.replace raises, the in-process dict is UNCHANGED (F2 contract)."""
        store = IdentityStore(path=tmp_path / "store.json")
        store.load()
        # Establish an existing entry
        store.put(FAKE_HASH_A, _alice_entry())

        # Simulate a disk-full / permission error on the atomic rename
        with patch("os.replace", side_effect=OSError("simulated disk full")):
            with pytest.raises(OSError, match="simulated disk full"):
                store.put(FAKE_HASH_B, _bob_entry())

        # In-process dict must be unchanged
        assert store.get(FAKE_HASH_B) is None, (
            "FAKE_HASH_B must not appear in the dict after a failed write"
        )
        assert store.get(FAKE_HASH_A) == _alice_entry(), (
            "Existing entry must remain after a failed write"
        )

    def test_delete_write_failure_leaves_dict_unchanged(self, tmp_path: Path) -> None:
        """If delete's file write fails, the in-process dict is UNCHANGED."""
        store = IdentityStore(path=tmp_path / "store.json")
        store.load()
        store.put(FAKE_HASH_A, _alice_entry())

        with patch("os.replace", side_effect=OSError("simulated disk full")):
            with pytest.raises(OSError):
                store.delete(FAKE_HASH_A)

        # Entry must still be present (delete did not take effect)
        assert store.get(FAKE_HASH_A) == _alice_entry()

    def test_failed_write_leaves_no_torn_tempfile(self, tmp_path: Path) -> None:
        """A failed os.replace must clean up the tempfile — no orphaned .tmp files."""
        store = IdentityStore(path=tmp_path / "store.json")
        store.load()

        with patch("os.replace", side_effect=OSError("simulated disk full")):
            with pytest.raises(OSError):
                store.put(FAKE_HASH_A, _alice_entry())

        # No .tmp files should remain
        tmp_files = list(tmp_path.glob("*.tmp"))
        assert tmp_files == [], f"Orphaned temp files: {tmp_files}"

    def test_atomic_write_uses_tempfile_in_same_dir(self, tmp_path: Path) -> None:
        """Writes use a temp file in the same directory (then os.replace)."""
        store_path = tmp_path / "store.json"
        store = IdentityStore(path=store_path)
        store.load()

        replaced_from: list[str] = []
        real_replace = os.replace

        def capturing_replace(src: str, dst: str) -> None:
            replaced_from.append(src)
            real_replace(src, dst)

        with patch("os.replace", side_effect=capturing_replace):
            store.put(FAKE_HASH_A, _alice_entry())

        assert replaced_from, "os.replace was never called"
        tmp_used = Path(replaced_from[0])
        # Tempfile must be in the same directory as the target file
        assert tmp_used.parent == store_path.parent, (
            f"Tempfile {tmp_used} is not in the same dir as {store_path}"
        )


# ---------------------------------------------------------------------------
# T2.4 — flat_dict live reference
# ---------------------------------------------------------------------------


class TestFlatDictLiveReference:
    """flat_dict exposes {key: contributor_id} kept live with _data."""

    def test_flat_dict_empty_on_new_store(self, tmp_path: Path) -> None:
        """flat_dict is empty after load() with no file."""
        store = IdentityStore(path=tmp_path / "store.json")
        store.load()
        assert store.flat_dict == {}

    def test_flat_dict_updated_after_put(self, tmp_path: Path) -> None:
        """flat_dict is updated immediately after put()."""
        store = IdentityStore(path=tmp_path / "store.json")
        store.load()
        store.put(FAKE_HASH_A, {"id": "alice"})

        assert store.flat_dict[FAKE_HASH_A] == "alice"

    def test_flat_dict_updated_after_delete(self, tmp_path: Path) -> None:
        """flat_dict removes key immediately after delete()."""
        store = IdentityStore(path=tmp_path / "store.json")
        store.load()
        store.put(FAKE_HASH_A, {"id": "alice"})
        store.delete(FAKE_HASH_A)

        assert FAKE_HASH_A not in store.flat_dict

    def test_flat_dict_is_same_object_across_puts(self, tmp_path: Path) -> None:
        """flat_dict is the SAME dict object before and after put()
        (so a shared reference to flat_dict stays live)."""
        store = IdentityStore(path=tmp_path / "store.json")
        store.load()
        flat_ref = store.flat_dict  # capture the reference

        store.put(FAKE_HASH_A, {"id": "alice"})

        # The reference must point at the same dict object, now containing the new key
        assert flat_ref is store.flat_dict, (
            "flat_dict must be the same object after put() — mutations must be in-place"
        )
        assert flat_ref[FAKE_HASH_A] == "alice"

    def test_flat_dict_populated_from_file_on_load(self, tmp_path: Path) -> None:
        """After load() from an existing file, flat_dict reflects all entries."""
        store_path = tmp_path / "store.json"
        store_path.write_text(
            json.dumps({FAKE_HASH_A: {"id": "alice"}, FAKE_HASH_B: {"id": "bob"}}),
            encoding="utf-8",
        )
        store = IdentityStore(path=store_path)
        store.load()

        assert store.flat_dict[FAKE_HASH_A] == "alice"
        assert store.flat_dict[FAKE_HASH_B] == "bob"
