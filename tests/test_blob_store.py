"""Tests for AsyncDiskBlobStore — Write, Read, List, Scan, Delete, Dump.

Covers:
1.  write/read roundtrip
2.  BlobReference.uri format
3.  directory structure creation
4.  URI-based session_id resolution
5.  missing blob raises FileNotFoundError
6.  invalid URI raises ValueError
7.  empty list for missing session
8.  correct BlobReference listing (async iterator)
9.  session isolation
10. asyncio.to_thread delegation verification
11. dump() copies blob to specified dest_dir
12. dump() uses default dest_dir (tempdir/ci-blobs)
13. dump() missing blob raises FileNotFoundError
14. dump() delegates copy2 via asyncio.to_thread
15. BlobStore protocol conformance
16. scan() yields BlobReference across multiple sessions
17. delete() is idempotent (True then False) and removes the blob
18. list()/write() BlobReference has correct uri/size/last_modified
"""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from unittest.mock import patch

import pytest
from context_intelligence_server.blob_store import (
    AsyncDiskBlobStore,
    BlobNotFoundError,
    BlobReference,
    BlobStore,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def store(tmp_path: Path) -> AsyncDiskBlobStore:
    """Return a fresh AsyncDiskBlobStore rooted at a temporary directory."""
    return AsyncDiskBlobStore(root=tmp_path)


async def _list_uris(store: AsyncDiskBlobStore, session_id: str) -> list[str]:
    return [ref.uri async for ref in store.list(session_id)]


# ---------------------------------------------------------------------------
# 1. Write/read roundtrip
# ---------------------------------------------------------------------------


async def test_write_read_roundtrip(store: AsyncDiskBlobStore) -> None:
    """Data written can be read back unchanged."""
    payload = {"event": "tool_call", "tool": "bash", "args": ["ls"]}
    ref = await store.write("session-abc", "tool_call_01", payload)
    result = await store.read(ref.uri)
    assert result == payload


# ---------------------------------------------------------------------------
# 2. BlobReference.uri format
# ---------------------------------------------------------------------------


async def test_uri_format(store: AsyncDiskBlobStore) -> None:
    """write() returns a BlobReference whose .uri is ci-blob://<session_id>/<key>."""
    ref = await store.write("session-xyz", "my_key", {"x": 1})
    assert isinstance(ref, BlobReference)
    assert ref.uri == "ci-blob://session-xyz/my_key"
    assert ref.session_id == "session-xyz"
    assert ref.key == "my_key"
    assert ref.size > 0
    assert ref.last_modified > 0


# ---------------------------------------------------------------------------
# 3. Directory structure creation
# ---------------------------------------------------------------------------


async def test_directory_structure_creation(
    store: AsyncDiskBlobStore, tmp_path: Path
) -> None:
    """write() creates <root>/<session_id>/blobs/<key>.json on disk."""
    await store.write("session-123", "blob_key", {"data": "value"})
    expected_path = tmp_path / "session-123" / "blobs" / "blob_key.json"
    assert expected_path.exists(), f"Expected file not found: {expected_path}"
    content = json.loads(expected_path.read_text())
    assert content == {"data": "value"}


# ---------------------------------------------------------------------------
# 4. URI-based session_id resolution
# ---------------------------------------------------------------------------


async def test_uri_based_session_id_resolution(
    store: AsyncDiskBlobStore, tmp_path: Path
) -> None:
    """read() resolves the session_id from the URI, not from a parameter."""
    session_id = "session-uri-resolve"
    key = "my_blob"
    payload = {"resolved": True}
    ref = await store.write(session_id, key, payload)
    # Confirm URI contains session_id
    assert session_id in ref.uri
    # read must successfully resolve session_id from URI
    result = await store.read(ref.uri)
    assert result == payload


# ---------------------------------------------------------------------------
# 5. Missing blob raises FileNotFoundError
# ---------------------------------------------------------------------------


async def test_missing_blob_raises_file_not_found(store: AsyncDiskBlobStore) -> None:
    """read() raises FileNotFoundError for a URI pointing to a non-existent blob."""
    uri = "ci-blob://session-missing/nonexistent_key"
    with pytest.raises(FileNotFoundError):
        await store.read(uri)


# ---------------------------------------------------------------------------
# 6. Invalid URI raises ValueError
# ---------------------------------------------------------------------------


async def test_invalid_uri_raises_value_error(store: AsyncDiskBlobStore) -> None:
    """read() raises ValueError for URIs that don't match the ci-blob:// scheme."""
    with pytest.raises(ValueError):
        await store.read("not-a-ci-blob-uri")

    with pytest.raises(ValueError):
        await store.read("http://example.com/blob")

    with pytest.raises(ValueError):
        await store.read("ci-blob://")  # missing key


# ---------------------------------------------------------------------------
# 7. Empty list for missing session
# ---------------------------------------------------------------------------


async def test_empty_list_for_missing_session(store: AsyncDiskBlobStore) -> None:
    """list() yields nothing when no blobs exist for the session."""
    result = await _list_uris(store, "session-does-not-exist")
    assert result == []


# ---------------------------------------------------------------------------
# 8. Correct BlobReference listing (async iterator)
# ---------------------------------------------------------------------------


async def test_correct_uri_listing(store: AsyncDiskBlobStore) -> None:
    """list() yields all blob references for a session, sorted by key."""
    session_id = "session-list"
    await store.write(session_id, "key_b", {"b": 2})
    await store.write(session_id, "key_a", {"a": 1})
    await store.write(session_id, "key_c", {"c": 3})

    refs = [ref async for ref in store.list(session_id)]
    assert [r.uri for r in refs] == [
        "ci-blob://session-list/key_a",
        "ci-blob://session-list/key_b",
        "ci-blob://session-list/key_c",
    ]
    for r in refs:
        assert isinstance(r, BlobReference)
        assert r.session_id == session_id
        assert r.size > 0
        assert r.last_modified > 0


# ---------------------------------------------------------------------------
# 9. Session isolation
# ---------------------------------------------------------------------------


async def test_session_isolation(store: AsyncDiskBlobStore) -> None:
    """list() only returns references for the requested session, not other sessions."""
    await store.write("session-alpha", "blob_1", {"alpha": True})
    await store.write("session-beta", "blob_2", {"beta": True})
    await store.write("session-alpha", "blob_3", {"alpha2": True})

    alpha_uris = await _list_uris(store, "session-alpha")
    beta_uris = await _list_uris(store, "session-beta")

    assert all("session-alpha" in u for u in alpha_uris)
    assert all("session-beta" in u for u in beta_uris)
    assert len(alpha_uris) == 2
    assert len(beta_uris) == 1


# ---------------------------------------------------------------------------
# 10. asyncio.to_thread delegation
# ---------------------------------------------------------------------------


async def test_asyncio_to_thread_delegation(tmp_path: Path) -> None:
    """All filesystem I/O is delegated to asyncio.to_thread for non-blocking I/O."""
    store = AsyncDiskBlobStore(root=tmp_path)

    to_thread_calls: list[str] = []
    original_to_thread = asyncio.to_thread

    async def tracking_to_thread(func, *args, **kwargs):  # type: ignore[no-untyped-def]
        to_thread_calls.append(getattr(func, "__name__", str(func)))
        return await original_to_thread(func, *args, **kwargs)

    with patch("asyncio.to_thread", side_effect=tracking_to_thread):
        await store.write("sess", "k", {"v": 1})
        await store.read("ci-blob://sess/k")
        async for _ in store.list("sess"):
            pass

    assert len(to_thread_calls) >= 3, (
        f"Expected at least 3 asyncio.to_thread calls (write, read, list), "
        f"got {len(to_thread_calls)}: {to_thread_calls}"
    )


# ---------------------------------------------------------------------------
# 11. dump() copies blob to specified dest_dir
# ---------------------------------------------------------------------------


async def test_dump_copy_to_specified_dest_dir(
    store: AsyncDiskBlobStore, tmp_path: Path
) -> None:
    """dump() copies the blob file to the specified dest_dir and returns the path."""
    session_id = "session-dump-copy"
    key = "blob_to_copy"
    payload = {"copy": "me"}
    ref = await store.write(session_id, key, payload)

    dest_dir = tmp_path / "my_dest"
    result = await store.dump(ref.uri, dest_dir=dest_dir)

    result_path = Path(result)
    assert result_path.exists()
    assert result_path.parent == dest_dir
    assert json.loads(result_path.read_text()) == payload


# ---------------------------------------------------------------------------
# 12. dump() uses default dest_dir (tempdir/ci-blobs)
# ---------------------------------------------------------------------------


async def test_dump_default_dest_dir(store: AsyncDiskBlobStore) -> None:
    """dump() uses Path(tempfile.gettempdir()) / 'ci-blobs' when dest_dir is None."""
    import tempfile

    session_id = "session-dump-default"
    key = "default_blob"
    ref = await store.write(session_id, key, {"default": True})

    result = await store.dump(ref.uri)

    expected_dir = Path(tempfile.gettempdir()) / "ci-blobs"
    result_path = Path(result)
    assert result_path.parent == expected_dir
    assert result_path.exists()


# ---------------------------------------------------------------------------
# 13. dump() missing blob raises FileNotFoundError
# ---------------------------------------------------------------------------


async def test_dump_missing_blob_raises_file_not_found(
    store: AsyncDiskBlobStore,
) -> None:
    """dump() raises FileNotFoundError with 'Blob not found' message for missing blob."""
    uri = "ci-blob://session-nonexistent/missing_blob"
    with pytest.raises(FileNotFoundError, match="Blob not found"):
        await store.dump(uri)


# ---------------------------------------------------------------------------
# 14. dump() delegates shutil.copy2 via asyncio.to_thread
# ---------------------------------------------------------------------------


async def test_dump_uses_asyncio_to_thread_for_copy2(
    store: AsyncDiskBlobStore, tmp_path: Path
) -> None:
    """dump() delegates shutil.copy2 to asyncio.to_thread for non-blocking I/O."""
    session_id = "session-dump-thread"
    key = "thread_blob"
    ref = await store.write(session_id, key, {"thread": True})
    dest_dir = tmp_path / "thread_dest"

    to_thread_calls: list[str] = []
    original_to_thread = asyncio.to_thread

    async def tracking_to_thread(func, *args, **kwargs):  # type: ignore[no-untyped-def]
        to_thread_calls.append(getattr(func, "__name__", str(func)))
        return await original_to_thread(func, *args, **kwargs)

    with patch("asyncio.to_thread", side_effect=tracking_to_thread):
        await store.dump(ref.uri, dest_dir=dest_dir)

    assert len(to_thread_calls) >= 1, (
        f"Expected at least 1 asyncio.to_thread call for dump(), "
        f"got {len(to_thread_calls)}: {to_thread_calls}"
    )


# ---------------------------------------------------------------------------
# BlobStore protocol conformance
# ---------------------------------------------------------------------------


def test_blob_store_protocol_conformance(store: AsyncDiskBlobStore) -> None:
    """AsyncDiskBlobStore conforms to the BlobStore protocol."""
    assert isinstance(store, BlobStore)


# ---------------------------------------------------------------------------
# Atomic / durable write
# ---------------------------------------------------------------------------


async def test_write_is_atomic_no_torn_file_on_failure(
    store: AsyncDiskBlobStore, tmp_path: Path
) -> None:
    """A failure during os.replace leaves no torn final file and no temp siblings."""
    session_id = "sess-atomic"
    key = "k1"

    with (
        patch(
            "context_intelligence_server.blob_store.os.replace",
            side_effect=OSError("simulated replace failure"),
        ),
        pytest.raises(OSError),
    ):
        await store.write(session_id, key, {"v": 1})

    final_path = store.blob_path(session_id, key)
    # No torn file observable at the final path.
    assert not final_path.exists()
    # No leftover *.tmp siblings in the blobs dir.
    blobs_dir = final_path.parent
    if blobs_dir.exists():
        assert list(blobs_dir.glob("*.tmp")) == []


async def test_write_replaces_atomically_on_success(
    store: AsyncDiskBlobStore,
) -> None:
    """On success the final file has the exact JSON, no temp remains, URI is correct."""
    session_id = "sess-atomic"
    key = "k2"

    ref = await store.write(session_id, key, {"v": 1})

    assert ref.uri == "ci-blob://sess-atomic/k2"
    final_path = store.blob_path(session_id, key)
    assert final_path.read_text(encoding="utf-8") == '{"v": 1}'
    # No leftover temp files.
    assert list(final_path.parent.glob("*.tmp")) == []


# ---------------------------------------------------------------------------
# 16. scan() yields BlobReference across multiple sessions
# ---------------------------------------------------------------------------


async def test_scan_yields_references_across_sessions(
    store: AsyncDiskBlobStore,
) -> None:
    """scan() streams a BlobReference for every blob across ALL sessions."""
    await store.write("session-scan-a", "k1", {"a": 1})
    await store.write("session-scan-a", "k2", {"a": 2})
    await store.write("session-scan-b", "k1", {"b": 1})

    refs = [ref async for ref in store.scan()]
    uris = {r.uri for r in refs}
    assert uris == {
        "ci-blob://session-scan-a/k1",
        "ci-blob://session-scan-a/k2",
        "ci-blob://session-scan-b/k1",
    }
    for r in refs:
        assert isinstance(r, BlobReference)
        assert r.size > 0
        assert r.last_modified > 0


async def test_scan_empty_store_yields_nothing(store: AsyncDiskBlobStore) -> None:
    """scan() over an empty store yields no references."""
    refs = [ref async for ref in store.scan()]
    assert refs == []


# ---------------------------------------------------------------------------
# 17. delete() is idempotent and removes the blob
# ---------------------------------------------------------------------------


async def test_delete_idempotent_true_then_false(store: AsyncDiskBlobStore) -> None:
    """delete() returns True the first time (blob existed), False thereafter."""
    ref = await store.write("session-delete", "to_delete", {"gone": "soon"})

    first = await store.delete(ref.uri)
    assert first is True

    # The blob is actually removed from disk.
    with pytest.raises(FileNotFoundError):
        await store.read(ref.uri)

    second = await store.delete(ref.uri)
    assert second is False


async def test_delete_missing_blob_returns_false(store: AsyncDiskBlobStore) -> None:
    """delete() on a never-written blob returns False, never raises."""
    result = await store.delete("ci-blob://never-existed/nope")
    assert result is False


# ---------------------------------------------------------------------------
# BlobNotFoundError — neutral missing-blob error (guard #6)
# ---------------------------------------------------------------------------


async def test_missing_blob_raises_blob_not_found_error_no_path_leak(
    store: AsyncDiskBlobStore, tmp_path: Path
) -> None:
    """read() of a missing uri raises BlobNotFoundError (a FileNotFoundError
    subclass, for back-compat) whose message carries the uri only — never the
    on-disk path/root.
    """
    uri = "ci-blob://session-missing/nonexistent_key"

    with pytest.raises(BlobNotFoundError) as exc_info:
        await store.read(uri)

    # Back-compat: existing `except FileNotFoundError` callers still catch it.
    assert isinstance(exc_info.value, FileNotFoundError)

    message = str(exc_info.value)
    assert uri in message
    # No on-disk path fragment or root leaks into the message.
    assert "path" not in message.lower()
    assert str(tmp_path) not in message


# ---------------------------------------------------------------------------
# Fenced (compare-and-delete) delete — guard #1
# ---------------------------------------------------------------------------


async def test_fenced_delete_succeeds_when_unchanged(
    store: AsyncDiskBlobStore,
) -> None:
    """delete(uri, if_unmodified=ref) removes the blob when it has not
    changed since ref was observed (e.g. by scan()/list())."""
    ref = await store.write("session-fence", "unchanged_key", {"v": 1})

    result = await store.delete(ref.uri, if_unmodified=ref)
    assert result is True

    with pytest.raises(BlobNotFoundError):
        await store.read(ref.uri)


async def test_fenced_delete_refuses_when_rewritten(
    store: AsyncDiskBlobStore,
) -> None:
    """delete(uri, if_unmodified=stale_ref) returns False and leaves the
    (new) blob on disk when the blob was rewritten after stale_ref was
    observed.

    Deterministic (not sleep-based): the rewrite uses a longer JSON payload
    so the size differs regardless of filesystem mtime granularity, and the
    file's mtime is also forced forward via os.utime so both the size AND
    mtime comparisons independently detect the change.
    """
    session_id, key = "session-fence-stale", "rewritten_key"

    stale_ref = await store.write(session_id, key, {"v": 1})

    # Rewrite with a longer payload -> different size, independent of mtime
    # resolution/granularity on the filesystem.
    new_ref = await store.write(session_id, key, {"v": 1, "extra": "x" * 64})
    assert new_ref.size != stale_ref.size

    # Force the mtime to be unambiguously different too (belt-and-suspenders
    # against any filesystem where sizes could coincidentally collide).
    path = store.blob_path(session_id, key)
    new_mtime = stale_ref.last_modified + 100.0
    os.utime(path, (new_mtime, new_mtime))

    result = await store.delete(stale_ref.uri, if_unmodified=stale_ref)
    assert result is False

    # The (new) blob survives on disk, untouched.
    survived = await store.read(stale_ref.uri)
    assert survived == {"v": 1, "extra": "x" * 64}


async def test_fenced_delete_missing_blob_returns_false(
    store: AsyncDiskBlobStore,
) -> None:
    """delete(uri, if_unmodified=ref) on an already-absent blob returns False."""
    ref = await store.write("session-fence-missing", "gone_key", {"v": 1})
    assert await store.delete(ref.uri) is True  # unconditional delete first

    result = await store.delete(ref.uri, if_unmodified=ref)
    assert result is False


async def test_unconditional_delete_still_idempotent(
    store: AsyncDiskBlobStore,
) -> None:
    """Unconditional delete(uri) (if_unmodified=None, the default) is
    unchanged: True then False, idempotent."""
    ref = await store.write("session-fence-uncond", "plain_key", {"v": 1})

    first = await store.delete(ref.uri)
    assert first is True

    second = await store.delete(ref.uri)
    assert second is False
