"""Tests for AsyncDiskBlobStore — Write, Read, List.

11 tests covering:
1.  write/read roundtrip
2.  URI format
3.  directory structure creation
4.  URI-based session_id resolution
5.  missing blob raises FileNotFoundError
6.  invalid URI raises ValueError
7.  empty list for missing session
8.  correct URI listing
9.  session isolation
10. asyncio.to_thread delegation verification
11. dump() raises NotImplementedError
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from unittest.mock import patch

import pytest

from context_intelligence_server.blob_store import AsyncDiskBlobStore, BlobStore


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def store(tmp_path: Path) -> AsyncDiskBlobStore:
    """Return a fresh AsyncDiskBlobStore rooted at a temporary directory."""
    return AsyncDiskBlobStore(root=tmp_path)


# ---------------------------------------------------------------------------
# 1. Write/read roundtrip
# ---------------------------------------------------------------------------


async def test_write_read_roundtrip(store: AsyncDiskBlobStore) -> None:
    """Data written can be read back unchanged."""
    payload = {"event": "tool_call", "tool": "bash", "args": ["ls"]}
    uri = await store.write("session-abc", "tool_call_01", payload)
    result = await store.read(uri)
    assert result == payload


# ---------------------------------------------------------------------------
# 2. URI format
# ---------------------------------------------------------------------------


async def test_uri_format(store: AsyncDiskBlobStore) -> None:
    """write() returns a ci-blob://<session_id>/<key> URI."""
    uri = await store.write("session-xyz", "my_key", {"x": 1})
    assert uri == "ci-blob://session-xyz/my_key"


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
    uri = await store.write(session_id, key, payload)
    # Confirm URI contains session_id
    assert session_id in uri
    # read must successfully resolve session_id from URI
    result = await store.read(uri)
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
    """list() returns an empty list when no blobs exist for the session."""
    result = await store.list("session-does-not-exist")
    assert result == []


# ---------------------------------------------------------------------------
# 8. Correct URI listing
# ---------------------------------------------------------------------------


async def test_correct_uri_listing(store: AsyncDiskBlobStore) -> None:
    """list() returns all blob URIs for a session, sorted."""
    session_id = "session-list"
    await store.write(session_id, "key_b", {"b": 2})
    await store.write(session_id, "key_a", {"a": 1})
    await store.write(session_id, "key_c", {"c": 3})

    uris = await store.list(session_id)
    assert uris == [
        "ci-blob://session-list/key_a",
        "ci-blob://session-list/key_b",
        "ci-blob://session-list/key_c",
    ]


# ---------------------------------------------------------------------------
# 9. Session isolation
# ---------------------------------------------------------------------------


async def test_session_isolation(store: AsyncDiskBlobStore) -> None:
    """list() only returns URIs for the requested session, not other sessions."""
    await store.write("session-alpha", "blob_1", {"alpha": True})
    await store.write("session-beta", "blob_2", {"beta": True})
    await store.write("session-alpha", "blob_3", {"alpha2": True})

    alpha_uris = await store.list("session-alpha")
    beta_uris = await store.list("session-beta")

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
        await store.list("sess")

    assert len(to_thread_calls) >= 3, (
        f"Expected at least 3 asyncio.to_thread calls (write, read, list), "
        f"got {len(to_thread_calls)}: {to_thread_calls}"
    )


# ---------------------------------------------------------------------------
# 11. dump() raises NotImplementedError
# ---------------------------------------------------------------------------


async def test_dump_raises_not_implemented(store: AsyncDiskBlobStore) -> None:
    """dump() is stubbed and raises NotImplementedError (to be implemented in Task 2)."""
    with pytest.raises(NotImplementedError):
        await store.dump("session-any")


# ---------------------------------------------------------------------------
# BlobStore protocol conformance
# ---------------------------------------------------------------------------


def test_blob_store_protocol_conformance(store: AsyncDiskBlobStore) -> None:
    """AsyncDiskBlobStore conforms to the BlobStore protocol."""
    assert isinstance(store, BlobStore)
