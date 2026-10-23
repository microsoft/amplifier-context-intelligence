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
import os
from pathlib import Path
from unittest.mock import patch

import pytest
from context_intelligence_server import blob_retention as r
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
        re-wraps it as a URI without folding again. Every URI list()
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


# --- Tilde-prefixed roots (Copilot r4329246656) ---------------------------------


async def test_store_write_read_under_fresh_tilde_root(tmp_path, monkeypatch):
    home, cwd = tmp_path / "home", tmp_path / "cwd"
    home.mkdir()
    cwd.mkdir()
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.chdir(cwd)
    store = AsyncDiskBlobStore("~/fresh-blobs")
    uri = await store.write("session", "key", {"v": 1})
    assert await store.read(uri) == {"v": 1}
    assert (home / "fresh-blobs" / "session" / "blobs" / "key.json").is_file()
    assert list(cwd.iterdir()) == [], "a literal './~' directory must never be created"


# --- Legacy '%', '?', '#' identifiers (PR126 lane A) -----------------------
#
# Session ids and blob keys are opaque literals.  Event data can carry '%',
# '?' and '#' (queue admission accepts them), and every URI is parsed by
# plain scheme-stripping -- never URL-decoded -- so these characters are
# ordinary filename bytes.  Rejecting them made blob_processor swap the
# field for {"$blob_error": ...}: permanent payload loss for legacy ids.

_LEGACY_NAMES = [
    "a%b",
    "a?b",
    "a#b",
    "100%",
    "q?x=1#frag",
    "a%2fb",
    "%2e%2e",
    "a%00b",
    "%",
    "?",
    "#",
]


def _pair(where: str, name: str) -> tuple[str, str]:
    """Place *name* in the session-id slot or the key slot of a pair."""
    return (name, "key") if where == "session" else ("sess", name)


@pytest.mark.parametrize("name", _LEGACY_NAMES)
@pytest.mark.parametrize("where", ["session", "key"])
async def test_legacy_chars_write_read_list_size_round_trip(
    store: AsyncDiskBlobStore, tmp_path: Path, where: str, name: str
) -> None:
    session_id, key = _pair(where, name)
    uri = await store.write(session_id, key, {"v": name})

    assert uri == f"ci-blob://{session_id}/{key}"
    on_disk = tmp_path / session_id / "blobs" / f"{key}.json"
    assert on_disk.is_file()
    assert await store.read(uri) == {"v": name}
    assert await store.list(session_id) == [uri]
    assert await store.size(uri) == on_disk.stat().st_size > 0


@pytest.mark.parametrize("name", _LEGACY_NAMES)
@pytest.mark.parametrize("where", ["session", "key"])
async def test_legacy_chars_dump_copies_payload(
    store: AsyncDiskBlobStore, tmp_path: Path, where: str, name: str
) -> None:
    session_id, key = _pair(where, name)
    uri = await store.write(session_id, key, {"v": name})

    dumped = await store.dump(uri, tmp_path / "dump")

    assert Path(dumped) == tmp_path / "dump" / f"{key}.json"
    assert json.loads(Path(dumped).read_text()) == {"v": name}


@pytest.mark.parametrize("name", _LEGACY_NAMES)
@pytest.mark.parametrize("where", ["session", "key"])
async def test_legacy_chars_delete_session_removes_blob(
    store: AsyncDiskBlobStore, tmp_path: Path, where: str, name: str
) -> None:
    session_id, key = _pair(where, name)
    uri = await store.write(session_id, key, {"v": name})

    assert await store.delete_session(session_id) == 1

    assert not (tmp_path / session_id).exists()
    assert await store.list(session_id) == []
    with pytest.raises(FileNotFoundError):
        await store.read(uri)


async def test_percent_2f_stays_literal_and_never_traverses(
    store: AsyncDiskBlobStore, tmp_path: Path
) -> None:
    """'%2f' is three ordinary bytes: no decoding, so no extra path level."""
    uri = await store.write("a%2fb", "k%2f..%2fescape", {"v": 1})

    assert uri == "ci-blob://a%2fb/k%2f..%2fescape"
    assert [p.relative_to(tmp_path).as_posix() for p in tmp_path.rglob("*.json")] == [
        "a%2fb/blobs/k%2f..%2fescape.json"
    ]
    assert not (tmp_path / "a").exists()
    assert await store.read(uri) == {"v": 1}


_ALWAYS_INVALID = ["", ".", "..", ".ci-blob-retention.lock", ".ci-blob-retention"]
_SEPARATOR_OR_CONTROL = ["a/b", "a\\b", "a\nb", "a\x00b", "a\x7fb"]


@pytest.mark.parametrize("bad", _ALWAYS_INVALID + _SEPARATOR_OR_CONTROL)
async def test_unsafe_session_ids_are_rejected_before_any_io(
    store: AsyncDiskBlobStore, tmp_path: Path, bad: str
) -> None:
    with pytest.raises(ValueError):
        await store.write(bad, "key", {"v": 1})
    with pytest.raises(ValueError):
        await store.write(bad, "a/b", {"v": 1})

    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("bad", _ALWAYS_INVALID)
async def test_empty_dot_and_reserved_keys_are_still_rejected_before_any_io(
    store: AsyncDiskBlobStore, tmp_path: Path, bad: str
) -> None:
    with pytest.raises(ValueError):
        await store.write("sess", bad, {"v": 1})

    assert list(tmp_path.iterdir()) == []


async def test_pre_existing_physical_legacy_files_keep_their_uris(
    store: AsyncDiskBlobStore, tmp_path: Path
) -> None:
    """Blobs already on disk under legacy names are read/dumped/deleted via
    the unchanged literal URIs -- no rewrite, no migration."""
    blobs = tmp_path / "s%1?2#3" / "blobs"
    blobs.mkdir(parents=True)
    (blobs / "k%?#.json").write_text('{"legacy": true}')
    uri = "ci-blob://s%1?2#3/k%?#"

    assert await store.read(uri) == {"legacy": True}
    assert await store.list("s%1?2#3") == [uri]
    assert await store.size(uri) == (blobs / "k%?#.json").stat().st_size
    dumped = await store.dump(uri, tmp_path / "dump")
    assert json.loads(Path(dumped).read_text()) == {"legacy": True}
    assert await store.delete_session("s%1?2#3") == 1
    assert not (tmp_path / "s%1?2#3").exists()


# --- Mapped keys: separators / controls in logical write names (PR126) ------
#
# Digests are hand-derived: sha256(b"ci-blob-event-key:v1\x00" + key.encode())
# computed independently of the code under test.

_ZERO = "0" * 64
_MARK = "~ci-key-v1"
_MAPPED_KEYS = [
    ("a/b", "5998a4c7b7280b4eb36c9f88878ed97c34b2cbeb7f138372a02365515c97bde9"),
    ("a\\b", "f3af24d320ab49ad3ecaddd0d48f69cb7c393eb657ffbf1b80416af26a1346b4"),
    ("a\nb", "97acd576bb4912bc9586a183710a1ee2753dacc4d562fae69f46a9523cc79a93"),
    ("a\x00b", "73d03db243d6111678a74f7b4a9c4378f9d628339e36b2f55a569d0f64860bd7"),
    ("a\x7fb", "1ff7ea36424f80b894b2ddb54b7c3cb6ce34971b3c68afc560dea4d6cbd54f53"),
]
_BACKSLASH_DIGEST = _MAPPED_KEYS[1][1]
_SLASH_DIGEST = _MAPPED_KEYS[0][1]


def _blobs(tmp_path: Path, session: str = "sess") -> Path:
    return tmp_path / session / "blobs"


def _seed(
    tmp_path: Path, filename: str, text: str = '{"seed": true}', session: str = "sess"
) -> Path:
    path = _blobs(tmp_path, session) / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def _tombstone(root: Path, uri: str) -> None:
    """Record an expiry tombstone for *uri* through the public ledger API."""
    with (
        r.RootLock(root, exclusive=True, create_root=True) as lock,
        r.Ledger(lock.root, writable=True) as ledger,
    ):
        ledger.journal([r.Candidate(uri, (1, 2, 3, 4, 5), 0)])


@pytest.mark.parametrize(("key", "digest"), _MAPPED_KEYS)
async def test_separator_and_control_keys_round_trip_through_a_flat_mapped_file(
    store: AsyncDiskBlobStore, tmp_path: Path, key: str, digest: str
) -> None:
    uri = await store.write("sess", key, {"v": key})

    assert uri == f"ci-blob://sess/{_MARK}/{digest}"
    physical = _blobs(tmp_path) / f"{_MARK}~{digest}.blob"
    assert [p.name for p in _blobs(tmp_path).iterdir()] == [physical.name]
    assert [p.name for p in (tmp_path / "sess").iterdir()] == ["blobs"]
    assert await store.read(uri) == {"v": key}
    assert await store.list("sess") == [uri]
    assert await store.size(uri) == physical.stat().st_size > 0
    dumped = await store.dump(uri, tmp_path / "dump")
    assert Path(dumped) == tmp_path / "dump" / physical.name
    assert json.loads(Path(dumped).read_text(encoding="utf-8")) == {"v": key}
    assert await store.delete_session("sess") == 1
    assert not (tmp_path / "sess").exists()


async def test_rewriting_a_mapped_key_replaces_the_same_physical_file(
    store: AsyncDiskBlobStore, tmp_path: Path
) -> None:
    first = await store.write("sess", "a/b", {"v": 1})
    second = await store.write("sess", "a/b", {"v": 2})

    assert first == second
    assert await store.read(first) == {"v": 2}
    assert len(list(_blobs(tmp_path).iterdir())) == 1


async def test_percent_encoded_separators_stay_ordinary_not_mapped(
    store: AsyncDiskBlobStore, tmp_path: Path
) -> None:
    uri = await store.write("sess", "a%5cb", {"v": 1})

    assert uri == "ci-blob://sess/a%5cb"
    assert (_blobs(tmp_path) / "a%5cb.json").is_file()


async def test_a_raw_key_that_spells_a_mapped_handle_is_hashed_not_trusted(
    store: AsyncDiskBlobStore, tmp_path: Path
) -> None:
    canonical = _seed(tmp_path, f"{_MARK}~{_ZERO}.blob", '{"canonical": true}')
    canonical_uri = f"ci-blob://sess/{_MARK}/{_ZERO}"

    uri = await store.write("sess", f"{_MARK}/{_ZERO}", {"new": 1})

    assert (
        uri
        == "ci-blob://sess/~ci-key-v1/4ed35e725865da2e0d3fa8f8369fbc02ab78e80f4a05e1eaaacbb994e9e01ca9"
    )
    assert json.loads(canonical.read_text(encoding="utf-8")) == {"canonical": True}
    assert await store.read(canonical_uri) == {"canonical": True}
    assert await store.read(uri) == {"new": 1}


@pytest.mark.parametrize("mapped_first", [True, False])
async def test_marker_looking_ordinary_json_and_mapped_blob_never_alias(
    store: AsyncDiskBlobStore, tmp_path: Path, mapped_first: bool
) -> None:
    ordinary_key = f"{_MARK}~{_SLASH_DIGEST}"
    ordinary_uri = f"ci-blob://sess/{ordinary_key}"
    if mapped_first:
        mapped_uri = await store.write("sess", "a/b", {"which": "mapped"})
        assert (
            await store.write("sess", ordinary_key, {"which": "ordinary"})
            == ordinary_uri
        )
    else:
        assert (
            await store.write("sess", ordinary_key, {"which": "ordinary"})
            == ordinary_uri
        )
        mapped_uri = await store.write("sess", "a/b", {"which": "mapped"})

    assert mapped_uri == f"ci-blob://sess/{_MARK}/{_SLASH_DIGEST}"
    assert sorted(p.name for p in _blobs(tmp_path).iterdir()) == [
        f"{_MARK}~{_SLASH_DIGEST}.blob",
        f"{_MARK}~{_SLASH_DIGEST}.json",
    ]
    assert await store.read(ordinary_uri) == {"which": "ordinary"}
    assert await store.read(mapped_uri) == {"which": "mapped"}
    assert await store.size(ordinary_uri) != 0 and await store.size(mapped_uri) != 0
    assert await store.list("sess") == sorted([ordinary_uri, mapped_uri])
    assert await store.delete_session("sess") == 2


async def test_temp_names_derive_from_the_physical_stem_and_fit_name_max(
    store: AsyncDiskBlobStore, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    created: list[str] = []
    real_open = os.open

    def spy(path, flags, mode=0o777, *, dir_fd=None):  # type: ignore[no-untyped-def]
        if isinstance(path, str) and path.endswith(".tmp"):
            created.append(path)
        return real_open(path, flags, mode, dir_fd=dir_fd)

    monkeypatch.setattr("context_intelligence_server.blob_store.os.open", spy)

    long_mapped = "\n" + "k" * 300
    mapped_uri = await store.write("sess", long_mapped, {"v": 1})
    ordinary_uri = await store.write("sess", "k" * 242, {"v": 2})
    folded_uri = await store.write("sess-" + "q" * 500, "k" * 300, {"v": 3})

    assert len(created) == 3
    assert all(len(name.encode()) <= 255 for name in created)
    mapped_stem = mapped_uri.rsplit("/", 2)[1:]
    assert created[0].startswith(f"{_MARK}~{mapped_stem[1]}.")
    assert len(created[0].encode()) == 75 + 13
    assert len(created[1].encode()) == 255
    assert await store.read(mapped_uri) == {"v": 1}
    assert await store.read(ordinary_uri) == {"v": 2}
    assert await store.read(folded_uri) == {"v": 3}


async def test_a_failed_mapped_write_leaves_no_temp_or_final_file(
    store: AsyncDiskBlobStore, tmp_path: Path
) -> None:
    with (
        patch(
            "context_intelligence_server.blob_store.os.replace",
            side_effect=OSError("simulated replace failure"),
        ),
        pytest.raises(OSError),
    ):
        await store.write("sess", "a/b", {"v": 1})

    assert list(_blobs(tmp_path).iterdir()) == []
    assert not store.blob_path("sess", "a/b").exists()


async def test_public_parse_uri_and_blob_path_speak_the_canonical_handle(
    store: AsyncDiskBlobStore, tmp_path: Path
) -> None:
    uri = f"ci-blob://sess/{_MARK}/{_SLASH_DIGEST}"
    handle = f"{_MARK}/{_SLASH_DIGEST}"

    assert store.parse_uri(uri) == ("sess", handle)
    # The handle names the stored file directly: it is NOT hashed a second time.
    assert (
        store.blob_path("sess", handle)
        == _blobs(tmp_path) / f"{_MARK}~{_SLASH_DIGEST}.blob"
    )
    # An ordinary logical key with a separator resolves through the write mapping.
    assert (
        store.blob_path("sess", "a/b")
        == _blobs(tmp_path) / f"{_MARK}~{_SLASH_DIGEST}.blob"
    )
    assert store.blob_path("sess", "plain") == _blobs(tmp_path) / "plain.json"


# --- Pre-upgrade POSIX backslash files ---------------------------------------


async def test_pre_upgrade_backslash_files_are_read_sized_dumped_and_listed(
    store: AsyncDiskBlobStore, tmp_path: Path
) -> None:
    legacy = _seed(tmp_path, "a\\b.json", '{"legacy": true}')
    uri = "ci-blob://sess/a\\b"

    assert await store.read(uri) == {"legacy": True}
    assert await store.size(uri) == legacy.stat().st_size
    assert await store.list("sess") == [uri]
    dumped = await store.dump(uri, tmp_path / "dump")
    assert Path(dumped) == tmp_path / "dump" / "a\\b.json"
    assert json.loads(Path(dumped).read_text(encoding="utf-8")) == {"legacy": True}
    assert await store.delete_session("sess") == 1


async def test_a_new_write_never_creates_touches_or_migrates_a_backslash_file(
    store: AsyncDiskBlobStore, tmp_path: Path
) -> None:
    legacy = _seed(tmp_path, "a\\b.json", '{"legacy": true}')
    before = legacy.stat()

    uri = await store.write("sess", "a\\b", {"new": True})

    assert uri == f"ci-blob://sess/{_MARK}/{_BACKSLASH_DIGEST}"
    assert json.loads(legacy.read_text(encoding="utf-8")) == {"legacy": True}
    assert (legacy.stat().st_ino, legacy.stat().st_mtime_ns) == (
        before.st_ino,
        before.st_mtime_ns,
    )
    assert sorted(p.name for p in _blobs(tmp_path).iterdir()) == sorted(
        ["a\\b.json", f"{_MARK}~{_BACKSLASH_DIGEST}.blob"]
    )
    assert await store.list("sess") == sorted([uri, "ci-blob://sess/a\\b"])
    assert await store.delete_session("sess") == 2


async def test_a_fresh_backslash_write_creates_only_the_mapped_file(
    store: AsyncDiskBlobStore, tmp_path: Path
) -> None:
    await store.write("sess", "a\\b", {"v": 1})

    assert [p.name for p in _blobs(tmp_path).iterdir()] == [
        f"{_MARK}~{_BACKSLASH_DIGEST}.blob"
    ]


async def test_a_legacy_uri_falls_back_to_the_canonical_file_only_when_missing(
    store: AsyncDiskBlobStore, tmp_path: Path
) -> None:
    new_uri = await store.write("sess", "a\\b", {"canonical": True})
    legacy_uri = "ci-blob://sess/a\\b"

    assert await store.read(legacy_uri) == {"canonical": True}
    assert await store.size(legacy_uri) == await store.size(new_uri) > 0
    dumped = await store.dump(legacy_uri, tmp_path / "dump")
    assert Path(dumped).name == f"{_MARK}~{_BACKSLASH_DIGEST}.blob"

    _seed(tmp_path, "a\\b.json", '{"legacy": true}')
    assert await store.read(legacy_uri) == {"legacy": True}  # legacy is tried first
    assert (
        await store.size(legacy_uri) == (_blobs(tmp_path) / "a\\b.json").stat().st_size
    )


@pytest.mark.parametrize("kind", ["dangling_symlink", "directory", "invalid_json"])
async def test_legacy_fallback_never_masks_an_unsafe_or_corrupt_legacy_entry(
    store: AsyncDiskBlobStore, tmp_path: Path, kind: str
) -> None:
    await store.write("sess", "a\\b", {"canonical": True})
    legacy = _blobs(tmp_path) / "a\\b.json"
    if kind == "dangling_symlink":
        legacy.symlink_to(tmp_path / "nowhere")
    elif kind == "directory":
        legacy.mkdir()
    else:
        legacy.write_text("{not json", encoding="utf-8")

    with pytest.raises((OSError, ValueError)) as excinfo:
        await store.read("ci-blob://sess/a\\b")

    assert not isinstance(excinfo.value, FileNotFoundError)


# --- Expiry tombstones across the candidate set --------------------------------


async def test_a_legacy_tombstone_blocks_the_write_and_returns_the_old_uri(
    store: AsyncDiskBlobStore, tmp_path: Path
) -> None:
    legacy_uri = "ci-blob://sess/a\\b"
    _tombstone(tmp_path, legacy_uri)

    assert await store.write("sess", "a\\b", {"v": 1}) == legacy_uri

    assert not (tmp_path / "sess").exists()  # nothing was created, not even the dir
    with pytest.raises(r.BlobExpiredError) as exc:
        await store.read(legacy_uri)
    assert exc.value.uri == legacy_uri


async def test_a_canonical_tombstone_blocks_the_write_and_returns_the_canonical_uri(
    store: AsyncDiskBlobStore, tmp_path: Path
) -> None:
    mapped_uri = f"ci-blob://sess/{_MARK}/{_SLASH_DIGEST}"
    _tombstone(tmp_path, mapped_uri)

    assert await store.write("sess", "a/b", {"v": 1}) == mapped_uri

    assert not (tmp_path / "sess").exists()
    for operation in (
        store.read(mapped_uri),
        store.dump(mapped_uri, tmp_path / "dump"),
    ):
        with pytest.raises(r.BlobExpiredError) as exc:
            await operation
        assert exc.value.uri == mapped_uri
    assert await store.size(mapped_uri) == 0  # size never raises expiry


async def test_a_restored_physical_file_wins_over_every_tombstone(
    store: AsyncDiskBlobStore, tmp_path: Path
) -> None:
    legacy_uri = "ci-blob://sess/a\\b"
    mapped_uri = f"ci-blob://sess/{_MARK}/{_BACKSLASH_DIGEST}"
    _tombstone(tmp_path, legacy_uri)
    _tombstone(tmp_path, mapped_uri)
    _seed(tmp_path, f"{_MARK}~{_BACKSLASH_DIGEST}.blob", '{"restored": "canonical"}')

    assert await store.read(legacy_uri) == {"restored": "canonical"}
    assert await store.read(mapped_uri) == {"restored": "canonical"}

    _seed(tmp_path, "a\\b.json", '{"restored": "legacy"}')
    assert await store.read(legacy_uri) == {"restored": "legacy"}


async def test_a_legacy_read_reports_the_recorded_uri_when_everything_is_missing(
    store: AsyncDiskBlobStore, tmp_path: Path
) -> None:
    legacy_uri = "ci-blob://sess/a\\b"
    mapped_uri = f"ci-blob://sess/{_MARK}/{_BACKSLASH_DIGEST}"
    _seed(tmp_path, "unrelated.json")

    with pytest.raises(FileNotFoundError) as plain:
        await store.read(legacy_uri)
    assert not isinstance(plain.value, r.BlobExpiredError)

    _tombstone(tmp_path, mapped_uri)
    with pytest.raises(r.BlobExpiredError) as exc:
        await store.read(legacy_uri)
    assert exc.value.uri == mapped_uri

    _tombstone(tmp_path, legacy_uri)
    with pytest.raises(r.BlobExpiredError) as exc:
        await store.read(legacy_uri)
    assert exc.value.uri == legacy_uri  # candidate order: the legacy record first


async def test_listing_and_deletion_recognise_every_address_kind_and_skip_the_rest(
    store: AsyncDiskBlobStore, tmp_path: Path
) -> None:
    _seed(tmp_path, "plain.json")
    _seed(tmp_path, "a\\b.json")
    _seed(tmp_path, f"{_MARK}~{_SLASH_DIGEST}.blob")
    for junk in ("x.tmp", "foo.blob", ".json", f"{_MARK}~{'A' * 64}.blob", "readme"):
        _seed(tmp_path, junk)
    (_blobs(tmp_path) / "dir.json").mkdir()

    assert await store.list("sess") == [
        "ci-blob://sess/a\\b",
        "ci-blob://sess/plain",
        f"ci-blob://sess/{_MARK}/{_SLASH_DIGEST}",
    ]
    assert await store.delete_session("sess") == 3


async def test_where_a_backslash_is_a_separator_a_legacy_uri_is_refused_and_writes_still_map(
    store: AsyncDiskBlobStore, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Platform predicate: no legacy backslash fallback where "\\" can separate paths."""
    from context_intelligence_server import blob_keys

    monkeypatch.setattr(blob_keys, "_legacy_backslash_supported", lambda: False)

    with pytest.raises(ValueError):
        await store.read("ci-blob://sess/a\\b")
    with pytest.raises(ValueError):
        await store.size("ci-blob://sess/a\\b")
    uri = await store.write("sess", "a\\b", {"v": 1})
    assert uri == f"ci-blob://sess/{_MARK}/{_BACKSLASH_DIGEST}"
    assert await store.read(uri) == {"v": 1}


async def test_a_long_slash_key_round_trips_under_the_pre_mapping_ordinary_fold(
    store: AsyncDiskBlobStore, tmp_path: Path
) -> None:
    from context_intelligence_server.utils import fold_name

    logical = "a/" + "x" * 400
    folded = "a_" + "x" * 223 + "~4d42d46a6b95ff4c"  # derived independently of the code

    uri = await store.write("sess", logical, {"v": "long-slash"})

    assert uri == f"ci-blob://sess/{folded}"  # ordinary, NOT ~ci-key-v1
    assert fold_name(logical, _MAX_KEY_BYTES) == folded  # the old fold, unchanged
    assert [p.name for p in _blobs(tmp_path).iterdir()] == [f"{folded}.json"]
    assert await store.read(uri) == {"v": "long-slash"}
    assert await store.list("sess") == [uri]
