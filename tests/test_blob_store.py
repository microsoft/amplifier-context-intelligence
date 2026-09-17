"""Tests for AsyncDiskBlobStore — Write, Read, List, Dump.

15 tests covering:
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
11. dump() copies blob to specified dest_dir
12. dump() uses default dest_dir (tempdir/ci-blobs)
13. dump() missing blob raises FileNotFoundError
14. dump() delegates copy2 via asyncio.to_thread
15. BlobStore protocol conformance
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from unittest.mock import patch

import pytest
from context_intelligence_server.blob_store import (
    _MAX_KEY_BYTES,
    _MAX_SESSION_ID_BYTES,
    AsyncDiskBlobStore,
    BlobStore,
)

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
# 11. dump() copies blob to specified dest_dir
# ---------------------------------------------------------------------------


async def test_dump_copy_to_specified_dest_dir(
    store: AsyncDiskBlobStore, tmp_path: Path
) -> None:
    """dump() copies the blob file to the specified dest_dir and returns the path."""
    session_id = "session-dump-copy"
    key = "blob_to_copy"
    payload = {"copy": "me"}
    uri = await store.write(session_id, key, payload)

    dest_dir = tmp_path / "my_dest"
    result = await store.dump(uri, dest_dir=dest_dir)

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
    uri = await store.write(session_id, key, {"default": True})

    result = await store.dump(uri)

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
    uri = await store.write(session_id, key, {"thread": True})
    dest_dir = tmp_path / "thread_dest"

    to_thread_calls: list[str] = []
    original_to_thread = asyncio.to_thread

    async def tracking_to_thread(func, *args, **kwargs):  # type: ignore[no-untyped-def]
        to_thread_calls.append(getattr(func, "__name__", str(func)))
        return await original_to_thread(func, *args, **kwargs)

    with patch("asyncio.to_thread", side_effect=tracking_to_thread):
        await store.dump(uri, dest_dir=dest_dir)

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

    uri = await store.write(session_id, key, {"v": 1})

    assert uri == "ci-blob://sess-atomic/k2"
    final_path = store.blob_path(session_id, key)
    assert final_path.read_text(encoding="utf-8") == '{"v": 1}'
    # No leftover temp files.
    assert list(final_path.parent.glob("*.tmp")) == []


# ---------------------------------------------------------------------------
# delete_session
# ---------------------------------------------------------------------------


async def test_delete_session_removes_all_blobs(store: AsyncDiskBlobStore) -> None:
    """delete_session removes every blob and returns the count."""
    await store.write("sess-del", "k1", {"v": 1})
    await store.write("sess-del", "k2", {"v": 2})

    removed = await store.delete_session("sess-del")

    assert removed == 2
    assert await store.list("sess-del") == []


async def test_delete_session_missing_is_noop(store: AsyncDiskBlobStore) -> None:
    """Deleting a session with no blobs returns 0 and does not raise."""
    assert await store.delete_session("never-existed") == 0


async def test_delete_session_isolates_other_sessions(
    store: AsyncDiskBlobStore,
) -> None:
    """Deleting one session leaves other sessions' blobs intact."""
    await store.write("sess-a", "k1", {"v": 1})
    await store.write("sess-b", "k1", {"v": 1})

    await store.delete_session("sess-a")

    assert await store.list("sess-a") == []
    assert await store.list("sess-b") == ["ci-blob://sess-b/k1"]


# ---------------------------------------------------------------------------
# size()
# ---------------------------------------------------------------------------


async def test_size_returns_byte_size_of_written_blob(
    store: AsyncDiskBlobStore, tmp_path: Path
) -> None:
    """size() returns the exact on-disk byte size of the JSON file."""
    uri = await store.write("sess-size", "k1", {"v": 1})
    expected = (tmp_path / "sess-size" / "blobs" / "k1.json").stat().st_size
    assert await store.size(uri) == expected
    assert expected > 0


async def test_size_missing_blob_returns_zero(store: AsyncDiskBlobStore) -> None:
    """size() is idempotent-on-missing: returns 0, never raises."""
    assert await store.size("ci-blob://never-existed/missing_key") == 0


# ---------------------------------------------------------------------------
# Length budget -- sibling of the queue worker-key incident. A blob key is
# f"{node_id}__{field_name}" (blob_processor.py) and node_id embeds the
# session_id (utils.make_node_id), so for a deeply-nested sub-agent session
# the blob key is LONGER than the queue's worker key was. write()'s
# tempfile.mkstemp(prefix=f"{key}.", suffix=".tmp") names a temp file WIDER
# than the final "{key}.json" -- exactly the "temp name is the real worst
# case" shape as the queue's commit(). See blob_store.py's module-level
# comment for the budget derivation (_MAX_KEY_BYTES=242, _MAX_SESSION_ID_
# BYTES=255).
# ---------------------------------------------------------------------------


class TestBlobStoreLengthBudget:
    async def test_write_read_round_trip_with_deeply_nested_session_and_long_key(
        self, store: AsyncDiskBlobStore, tmp_path: Path
    ) -> None:
        """A 300-char session_id (deeply-nested sub-agent chain) AND an
        over-budget key must still round-trip through write/read/list/size,
        with every filesystem path component staying under NAME_MAX (255)."""
        session_id = "sub-agent-chain-" + ("segment-" * 40)  # far over 255
        key = "node-id-with-a-long-session__" + ("field-" * 60)  # far over 242
        payload = {"raw": "x" * 5000}

        uri = await store.write(session_id, key, payload)

        # Every path component the write actually touched fits under
        # NAME_MAX. (str(path) round-trips through Path, so encode the
        # NAME (not the whole path) for each component.)
        path = store.blob_path(session_id, key)
        for part in path.parts:
            assert len(part.encode("utf-8")) <= 255, f"component too long: {part!r}"

        result = await store.read(uri)
        assert result == payload

        listed = await store.list(session_id)
        assert listed == [uri]

        size = await store.size(uri)
        assert size > 0

        # _parse_uri resolves the URI produced by write() -- both folded,
        # both discoverable.
        parsed_session_id, parsed_key = store.parse_uri(uri)
        assert store.blob_path(parsed_session_id, parsed_key) == path

    async def test_folded_path_components_are_exactly_at_budget(
        self, store: AsyncDiskBlobStore
    ) -> None:
        session_id = "s" * 400
        key = "k" * 400
        path = store.blob_path(session_id, key)
        folded_session_id = path.parent.parent.name
        folded_key = path.stem
        assert len(folded_session_id.encode("utf-8")) == _MAX_SESSION_ID_BYTES
        assert len(folded_key.encode("utf-8")) == _MAX_KEY_BYTES

    async def test_write_survives_at_every_historical_boundary(
        self, store: AsyncDiskBlobStore
    ) -> None:
        """At and around the two budgets (session_id / key), write+read
        must succeed cleanly -- no OSError, no $blob_error path."""
        for length in (
            _MAX_KEY_BYTES,
            _MAX_KEY_BYTES + 1,
            _MAX_SESSION_ID_BYTES,
            _MAX_SESSION_ID_BYTES + 1,
            9000,
        ):
            uri = await store.write(f"sess-{length}", "k" * length, {"v": length})
            assert await store.read(uri) == {"v": length}

    async def test_idempotent_read_of_already_folded_uri(
        self, store: AsyncDiskBlobStore
    ) -> None:
        """_parse_uri yields an already-folded key; _blob_path folds again
        -- a no-op because fold_name is idempotent. Reading the URI
        write() returned (already folded) must resolve to the same file a
        second parse/fold pass would."""
        long_key = "k" * 1000
        uri = await store.write("sess-idem", long_key, {"a": 1})
        # The URI itself must already contain the folded key/session_id --
        # parsing and re-resolving it is an idempotent no-op.
        session_id, key = store.parse_uri(uri)
        path_from_uri = store.blob_path(session_id, key)
        path_direct = store.blob_path("sess-idem", long_key)
        assert path_from_uri == path_direct
        assert await store.read(uri) == {"a": 1}

    async def test_list_recovers_already_folded_key_and_uri_is_readable(
        self, store: AsyncDiskBlobStore
    ) -> None:
        """list() recovers the key from p.stem (already folded) and
        re-wraps it via _make_uri -- also idempotent. Every URI list()
        returns must be directly readable."""
        long_session = "sess-" + ("z" * 500)
        await store.write(long_session, "k" * 500, {"a": 1})
        await store.write(long_session, "k2" * 500, {"a": 2})

        uris = await store.list(long_session)
        assert len(uris) == 2
        for uri in uris:
            # Must not raise -- list()'s URIs are directly consumable.
            await store.read(uri)

    async def test_under_budget_session_and_key_are_byte_identical_back_compat(
        self, store: AsyncDiskBlobStore, tmp_path: Path
    ) -> None:
        """Every blob on the live share today has an under-budget name --
        the fold must be identity for it, so every existing ci-blob:// URI
        already in the graph keeps resolving with NO path change."""
        session_id = "normal-session-abc123"
        key = "tool_call_01"
        uri = await store.write(session_id, key, {"v": 1})
        assert uri == f"ci-blob://{session_id}/{key}"
        assert store.blob_path(session_id, key) == (
            tmp_path / session_id / "blobs" / f"{key}.json"
        )

    async def test_delete_session_removes_a_folded_deeply_nested_session(
        self, store: AsyncDiskBlobStore
    ) -> None:
        long_session = "sess-" + ("q" * 500)
        uri = await store.write(long_session, "key1", {"v": 1})
        removed = await store.delete_session(long_session)
        assert removed == 1
        assert await store.list(long_session) == []
        with pytest.raises(FileNotFoundError):
            await store.read(uri)
