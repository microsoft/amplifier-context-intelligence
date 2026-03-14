# Phase 3: Blob Storage & Cypher Proxy — Implementation Plan

> **Execution:** Use the subagent-driven-development workflow to implement this plan.

**Goal:** Add async blob storage with `ci-blob://` URIs, in-place blob processing in the event pipeline, HTTP blob retrieval endpoints, and a Cypher query proxy to the Context Intelligence Server.

**Architecture:** `AsyncDiskBlobStore` wraps all filesystem I/O in `asyncio.to_thread()` for genuinely non-blocking operation. The pipeline mutates event data dicts in-place (no `deepcopy` — the server owns each deserialized JSON exclusively), replacing large fields with `ci-blob://` URI references before handler dispatch. Three new HTTP endpoints serve blob content, list blob URIs, and proxy Cypher queries to Neo4j via a shared driver managed by the FastAPI lifespan.

**Tech Stack:** Python 3.11, FastAPI, asyncio, Pydantic v2, neo4j async driver, pytest-asyncio

---

## Repo Context

**Root directory:** `/home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence/`

All relative paths below are relative to this root.

**Starting state (after Phases 1 & 2):**

```
context_intelligence_server/
├── __init__.py
├── config.py          # Settings with blob_path="/data/blobs"
├── models.py          # EventRequest, EventResponse, StatusResponse
├── registry.py        # SessionRegistry, SessionWorker (with HookStateService)
├── main.py            # FastAPI app: GET /status, POST /events
├── pipeline.py        # process_event drain loop: ensure_session_node → handler dispatch
├── services.py        # HookStateService, SessionCursors, GraphState
├── neo4j_store.py     # Neo4jGraphStore
├── utils.py           # make_node_id, make_edge_id, EventLogContext
└── handlers/          # 7 event handlers
tests/
├── __init__.py
├── conftest.py
├── test_config.py
├── test_models.py
├── test_registry.py
├── test_main.py
├── test_pipeline.py
└── ...
```

**Files created by Phase 3:**

```
context_intelligence_server/
├── blob_store.py          # AsyncDiskBlobStore
└── blob_processor.py      # In-place blob transform

tests/
├── test_blob_store.py
├── test_blob_processor.py
└── integration/
    ├── __init__.py
    └── test_blob_pipeline.py
```

**Files modified by Phase 3:**

- `context_intelligence_server/registry.py` — wire `AsyncDiskBlobStore` into `SessionWorker`
- `context_intelligence_server/pipeline.py` — add blob processing step before handler dispatch
- `context_intelligence_server/models.py` — add `CypherRequest` model
- `context_intelligence_server/main.py` — add blob endpoints, cypher proxy, lifespan

---

## Task 1: AsyncDiskBlobStore — Write, Read, List

**Files:**
- Create: `context_intelligence_server/blob_store.py`
- Create: `tests/test_blob_store.py`

### Step 1: Write the failing tests

Create file `tests/test_blob_store.py`:

```python
"""Tests for AsyncDiskBlobStore — write, read, list, URI scheme."""

from __future__ import annotations

import asyncio
from unittest.mock import patch

import pytest


@pytest.fixture
def blob_root(tmp_path):
    """Return a temporary directory for blob storage."""
    return tmp_path


@pytest.fixture
def store(blob_root):
    from context_intelligence_server.blob_store import AsyncDiskBlobStore

    return AsyncDiskBlobStore(root=blob_root)


async def test_write_then_read_roundtrip(store):
    """write() stores data and read() retrieves it identically."""
    value = {"key": "value", "nested": [1, 2, 3]}
    uri = await store.write("sess-1", "my_key", value)
    result = await store.read(uri)
    assert result == value


async def test_uri_format(store):
    """write() returns a ci-blob://<session_id>/<key> URI."""
    uri = await store.write("sess-1", "my_key", {"data": 1})
    assert uri == "ci-blob://sess-1/my_key"


async def test_write_creates_directory_structure(store, blob_root):
    """write() creates <root>/<session_id>/blobs/<key>.json on disk."""
    await store.write("sess-1", "my_key", {"data": 1})
    expected_path = blob_root / "sess-1" / "blobs" / "my_key.json"
    assert expected_path.exists()


async def test_read_uses_uri_session_id(blob_root):
    """read() resolves session_id from the URI, not from a separate parameter."""
    from context_intelligence_server.blob_store import AsyncDiskBlobStore

    store = AsyncDiskBlobStore(root=blob_root)
    value = {"important": "data"}
    uri = await store.write("correct-session", "k1", value)

    # read() takes only a URI — session_id comes from the URI itself
    result = await store.read(uri)
    assert result == value

    # Verify the URI embeds the correct session_id
    assert "correct-session" in uri


async def test_read_missing_blob_raises(store):
    """read() raises FileNotFoundError for a nonexistent blob."""
    with pytest.raises(FileNotFoundError):
        await store.read("ci-blob://no-session/no-key")


async def test_read_invalid_uri_raises(store):
    """read() raises ValueError for a non-ci-blob URI."""
    with pytest.raises(ValueError, match="Invalid URI scheme"):
        await store.read("https://example.com/blob")


async def test_list_empty_for_missing_session(store):
    """list() returns empty list when the session directory doesn't exist."""
    result = await store.list("nonexistent-session")
    assert result == []


async def test_list_returns_correct_uris(store):
    """list() returns ci-blob:// URIs for all blobs in a session."""
    await store.write("sess-1", "alpha", {"a": 1})
    await store.write("sess-1", "beta", {"b": 2})

    uris = await store.list("sess-1")
    assert sorted(uris) == [
        "ci-blob://sess-1/alpha",
        "ci-blob://sess-1/beta",
    ]


async def test_list_does_not_include_other_sessions(store):
    """list() only returns blobs for the requested session."""
    await store.write("sess-1", "key-a", {"a": 1})
    await store.write("sess-2", "key-b", {"b": 2})

    uris = await store.list("sess-1")
    assert uris == ["ci-blob://sess-1/key-a"]


async def test_write_uses_asyncio_to_thread(store):
    """write() delegates filesystem operations to asyncio.to_thread."""
    call_log = []
    original = asyncio.to_thread

    async def spy(fn, /, *args, **kwargs):
        name = getattr(fn, "__name__", str(fn))
        call_log.append(name)
        return await original(fn, *args, **kwargs)

    with patch("context_intelligence_server.blob_store.asyncio.to_thread", side_effect=spy):
        await store.write("sess-1", "k1", {"data": 1})

    assert "mkdir" in call_log
    assert "write_text" in call_log
```

### Step 2: Run tests to verify they fail

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence
pytest tests/test_blob_store.py -v
```

Expected: **FAIL** — `ModuleNotFoundError: No module named 'context_intelligence_server.blob_store'`

### Step 3: Write the implementation

Create file `context_intelligence_server/blob_store.py`:

```python
"""AsyncDiskBlobStore — async blob storage with ci-blob:// URI scheme.

Ported from the bundle's DiskBlobStore with two key changes:
1. All filesystem operations wrapped with asyncio.to_thread() for non-blocking I/O.
2. read() uses the session_id embedded in the URI (fixes the bundle's footgun
   where a caller-provided session_id was used for path resolution instead).

Disk layout: <root>/<session-id>/blobs/<key>.json
URI scheme:  ci-blob://<session-id>/<key>
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Protocol, runtime_checkable

_URI_SCHEME = "ci-blob://"


@runtime_checkable
class BlobStore(Protocol):
    """Async protocol for blob storage backends.

    All methods are async. Implementations must wrap blocking I/O
    with asyncio.to_thread() or equivalent.
    """

    async def write(self, session_id: str, key: str, value: dict | list) -> str: ...

    async def read(self, uri: str) -> dict | list: ...

    async def list(self, session_id: str) -> list[str]: ...

    async def dump(self, uri: str, dest_dir: Path | None = None) -> str: ...


class AsyncDiskBlobStore:
    """Async disk-backed blob store.

    All filesystem operations are wrapped with asyncio.to_thread()
    to avoid blocking the event loop.
    """

    def __init__(self, root: Path | str) -> None:
        self._root = Path(root)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _make_uri(self, session_id: str, key: str) -> str:
        """Construct a ci-blob://<session-id>/<key> URI."""
        return f"{_URI_SCHEME}{session_id}/{key}"

    def _parse_uri(self, uri: str) -> tuple[str, str]:
        """Split ci-blob://<session-id>/<key> into (session_id, key)."""
        if not uri.startswith(_URI_SCHEME):
            raise ValueError(f"Invalid URI scheme: {uri!r}")
        rest = uri[len(_URI_SCHEME) :]
        session_id, key = rest.split("/", 1)
        return session_id, key

    def _blob_path(self, session_id: str, key: str) -> Path:
        """Return the canonical path for a blob file."""
        return self._root / session_id / "blobs" / f"{key}.json"

    # ------------------------------------------------------------------
    # Public helpers (used by sibling modules)
    # ------------------------------------------------------------------

    def parse_uri(self, uri: str) -> tuple[str, str]:
        """Split ci-blob://<session_id>/<key> into (session_id, key)."""
        return self._parse_uri(uri)

    def blob_path(self, session_id: str, key: str) -> Path:
        """Return the canonical path for a blob file."""
        return self._blob_path(session_id, key)

    # ------------------------------------------------------------------
    # BlobStore protocol methods
    # ------------------------------------------------------------------

    async def write(self, session_id: str, key: str, value: dict | list) -> str:
        """Write value to disk and return a ci-blob:// URI."""
        path = self._blob_path(session_id, key)
        await asyncio.to_thread(path.parent.mkdir, parents=True, exist_ok=True)
        content = json.dumps(value, default=str)
        await asyncio.to_thread(path.write_text, content)
        return self._make_uri(session_id, key)

    async def read(self, uri: str) -> dict | list:
        """Read and deserialize blob content from a ci-blob:// URI.

        Uses the session_id embedded in the URI for path resolution —
        fixes the bundle's footgun where the caller-provided session_id
        was used instead.
        """
        session_id, key = self._parse_uri(uri)
        path = self._blob_path(session_id, key)
        content = await asyncio.to_thread(path.read_text)
        return json.loads(content)

    async def list(self, session_id: str) -> list[str]:
        """List all blob URIs for the given session."""
        blobs_dir = self._root / session_id / "blobs"
        is_dir = await asyncio.to_thread(blobs_dir.is_dir)
        if not is_dir:
            return []
        files = await asyncio.to_thread(lambda: sorted(blobs_dir.glob("*.json")))
        return [self._make_uri(session_id, p.stem) for p in files]

    async def dump(self, uri: str, dest_dir: Path | None = None) -> str:
        """Copy blob file to dest_dir. Implemented in Task 2."""
        raise NotImplementedError("dump() is implemented in Task 2")
```

### Step 4: Run tests to verify they pass

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence
pytest tests/test_blob_store.py -v
```

Expected: **ALL PASS** (11 tests)

### Step 5: Commit

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence
git add context_intelligence_server/blob_store.py tests/test_blob_store.py
git commit -m "feat: phase 3 — AsyncDiskBlobStore with write, read, list"
```

---

## Task 2: AsyncDiskBlobStore — dump() Method

**Files:**
- Modify: `context_intelligence_server/blob_store.py`
- Modify: `tests/test_blob_store.py`

### Step 1: Write the failing tests

Append to `tests/test_blob_store.py`:

```python
async def test_dump_copies_file_to_dest_dir(store, blob_root, tmp_path):
    """dump() copies the blob file to the specified destination directory."""
    await store.write("sess-1", "my_key", {"data": "payload"})
    dest_dir = tmp_path / "dump-output"

    result_path = await store.dump("ci-blob://sess-1/my_key", dest_dir=dest_dir)

    import json
    from pathlib import Path

    dest = Path(result_path)
    assert dest.exists()
    assert dest.name == "my_key.json"
    assert dest.parent == dest_dir
    assert json.loads(dest.read_text()) == {"data": "payload"}


async def test_dump_uses_default_dest_dir(store, blob_root):
    """dump() uses tempdir/ci-blobs when dest_dir is None."""
    import tempfile
    from pathlib import Path

    await store.write("sess-1", "my_key", {"data": 1})
    result_path = await store.dump("ci-blob://sess-1/my_key")

    default_dir = Path(tempfile.gettempdir()) / "ci-blobs"
    assert Path(result_path).parent == default_dir


async def test_dump_raises_for_missing_blob(store):
    """dump() raises FileNotFoundError when the blob doesn't exist."""
    with pytest.raises(FileNotFoundError, match="Blob not found"):
        await store.dump("ci-blob://no-session/no-key")


async def test_dump_uses_asyncio_to_thread(store, tmp_path):
    """dump() delegates shutil.copy2 to asyncio.to_thread."""
    call_log = []
    original = asyncio.to_thread

    async def spy(fn, /, *args, **kwargs):
        name = getattr(fn, "__name__", str(fn))
        call_log.append(name)
        return await original(fn, *args, **kwargs)

    await store.write("sess-1", "k1", {"data": 1})

    with patch("context_intelligence_server.blob_store.asyncio.to_thread", side_effect=spy):
        await store.dump("ci-blob://sess-1/k1", dest_dir=tmp_path / "out")

    assert "copy2" in call_log
```

### Step 2: Run tests to verify they fail

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence
pytest tests/test_blob_store.py -v -k "dump"
```

Expected: **FAIL** — `NotImplementedError: dump() is implemented in Task 2`

### Step 3: Replace the dump() stub in `context_intelligence_server/blob_store.py`

Add these imports at the top of `blob_store.py` (alongside the existing ones):

```python
import shutil
import tempfile
```

Replace the existing `dump()` method in `AsyncDiskBlobStore` with:

```python
    async def dump(self, uri: str, dest_dir: Path | None = None) -> str:
        """Copy blob file to dest_dir (default: <tempdir>/ci-blobs/).

        Returns the path where the file was copied.
        Raises FileNotFoundError if the blob does not exist.
        """
        session_id, key = self._parse_uri(uri)
        src = self._blob_path(session_id, key)
        exists = await asyncio.to_thread(src.exists)
        if not exists:
            raise FileNotFoundError(f"Blob not found: {uri!r}")
        if dest_dir is None:
            dest_dir = Path(tempfile.gettempdir()) / "ci-blobs"
        await asyncio.to_thread(dest_dir.mkdir, parents=True, exist_ok=True)
        dest = dest_dir / f"{key}.json"
        await asyncio.to_thread(shutil.copy2, src, dest)
        return str(dest)
```

### Step 4: Run tests to verify they pass

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence
pytest tests/test_blob_store.py -v
```

Expected: **ALL PASS** (15 tests)

### Step 5: Commit

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence
git add context_intelligence_server/blob_store.py tests/test_blob_store.py
git commit -m "feat: phase 3 — AsyncDiskBlobStore dump() with configurable dest_dir"
```

---

## Task 3: Port blob_processor.py — In-Place Transform

**Files:**
- Create: `context_intelligence_server/blob_processor.py`
- Create: `tests/test_blob_processor.py`

### Step 1: Write the failing tests

Create file `tests/test_blob_processor.py`:

```python
"""Tests for blob_processor — in-place blob field substitution."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest


async def test_blob_fields_constant():
    """BLOB_FIELDS contains the expected field names."""
    from context_intelligence_server.blob_processor import BLOB_FIELDS

    assert BLOB_FIELDS == frozenset(
        {"raw", "result", "messages", "mount_plan", "context_snapshot", "debug"}
    )


async def test_in_place_mutation():
    """process_event_data mutates the original dict — no deepcopy."""
    from context_intelligence_server.blob_processor import process_event_data

    mock_store = AsyncMock()
    mock_store.write.return_value = "ci-blob://sess-1/node__raw"

    original = {"session_id": "sess-1", "raw": {"big": "payload"}}
    data = original  # Same reference

    await process_event_data(data, mock_store, "sess-1", "node")

    # The original dict is mutated (same object)
    assert original["raw"] == {"$blob_ref": "ci-blob://sess-1/node__raw"}
    assert data is original


async def test_returns_none():
    """process_event_data returns None (in-place mutation, no return value)."""
    from context_intelligence_server.blob_processor import process_event_data

    mock_store = AsyncMock()
    mock_store.write.return_value = "ci-blob://s/k"

    result = await process_event_data({"raw": {"x": 1}}, mock_store, "s", "n")
    assert result is None


async def test_blob_ref_substitution():
    """Blob-eligible fields are replaced with $blob_ref on successful write."""
    from context_intelligence_server.blob_processor import process_event_data

    mock_store = AsyncMock()
    mock_store.write.side_effect = lambda sid, key, val: f"ci-blob://{sid}/{key}"

    data = {
        "session_id": "sess-1",
        "raw": {"llm_response": "big"},
        "result": {"tool_output": "large"},
        "other_field": "untouched",
    }
    await process_event_data(data, mock_store, "sess-1", "node")

    assert data["raw"] == {"$blob_ref": "ci-blob://sess-1/node__raw"}
    assert data["result"] == {"$blob_ref": "ci-blob://sess-1/node__result"}
    assert data["other_field"] == "untouched"  # Non-blob field is NOT touched


async def test_blob_error_on_failed_write():
    """Failed blob writes produce $blob_error — non-blocking."""
    from context_intelligence_server.blob_processor import process_event_data

    mock_store = AsyncMock()
    mock_store.write.side_effect = OSError("disk full")

    data = {"raw": {"data": 1}, "result": {"data": 2}}
    await process_event_data(data, mock_store, "sess-1", "node")

    assert data["raw"] == {"$blob_error": "write failed: disk full"}
    assert data["result"] == {"$blob_error": "write failed: disk full"}


async def test_absent_fields_skipped():
    """Fields not present in data are skipped entirely."""
    from context_intelligence_server.blob_processor import process_event_data

    mock_store = AsyncMock()
    data = {"session_id": "sess-1", "custom_field": "value"}

    await process_event_data(data, mock_store, "sess-1", "node")

    mock_store.write.assert_not_called()
    assert data == {"session_id": "sess-1", "custom_field": "value"}


async def test_none_fields_skipped():
    """Fields with value None are skipped (no blob write)."""
    from context_intelligence_server.blob_processor import process_event_data

    mock_store = AsyncMock()
    data = {"raw": None, "result": None}

    await process_event_data(data, mock_store, "sess-1", "node")

    mock_store.write.assert_not_called()


async def test_blob_key_format():
    """Blob key is {node_id}__{field_name}."""
    from context_intelligence_server.blob_processor import process_event_data

    mock_store = AsyncMock()
    mock_store.write.return_value = "ci-blob://s/k"

    data = {"raw": {"data": 1}}
    await process_event_data(data, mock_store, "sess-1", "my_node_id")

    mock_store.write.assert_called_once_with("sess-1", "my_node_id__raw", {"data": 1})


async def test_lift_raw_fields_promotes_stop_reason():
    """_lift_raw_fields copies raw.stop_reason to top-level when absent."""
    from context_intelligence_server.blob_processor import _lift_raw_fields

    data = {"raw": {"stop_reason": "end_turn"}}
    _lift_raw_fields(data)
    assert data["stop_reason"] == "end_turn"


async def test_lift_raw_fields_preserves_existing_stop_reason():
    """_lift_raw_fields does NOT overwrite an existing top-level stop_reason."""
    from context_intelligence_server.blob_processor import _lift_raw_fields

    data = {"stop_reason": "already_set", "raw": {"stop_reason": "from_raw"}}
    _lift_raw_fields(data)
    assert data["stop_reason"] == "already_set"


async def test_lift_raw_fields_promotes_finish_reason():
    """_lift_raw_fields copies raw.finish_reason to top-level when absent."""
    from context_intelligence_server.blob_processor import _lift_raw_fields

    data = {"raw": {"finish_reason": "stop"}}
    _lift_raw_fields(data)
    assert data["finish_reason"] == "stop"


async def test_lift_raw_fields_merges_usage():
    """_lift_raw_fields merges raw.usage into top-level usage (existing keys win)."""
    from context_intelligence_server.blob_processor import _lift_raw_fields

    data = {
        "usage": {"input_tokens": 100},
        "raw": {"usage": {"input_tokens": 50, "output_tokens": 200}},
    }
    _lift_raw_fields(data)
    # Existing keys win on collision
    assert data["usage"] == {"input_tokens": 100, "output_tokens": 200}


async def test_lift_raw_fields_sets_usage_when_absent():
    """_lift_raw_fields sets usage from raw.usage when top-level usage is absent."""
    from context_intelligence_server.blob_processor import _lift_raw_fields

    data = {"raw": {"usage": {"input_tokens": 50}}}
    _lift_raw_fields(data)
    assert data["usage"] == {"input_tokens": 50}


async def test_lift_raw_fields_no_raw():
    """_lift_raw_fields is a no-op when raw is absent or not a dict."""
    from context_intelligence_server.blob_processor import _lift_raw_fields

    data = {"result": "keep"}
    _lift_raw_fields(data)
    assert data == {"result": "keep"}

    data2 = {"raw": "not-a-dict"}
    _lift_raw_fields(data2)
    assert data2 == {"raw": "not-a-dict"}
```

### Step 2: Run tests to verify they fail

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence
pytest tests/test_blob_processor.py -v
```

Expected: **FAIL** — `ModuleNotFoundError: No module named 'context_intelligence_server.blob_processor'`

### Step 3: Write the implementation

Create file `context_intelligence_server/blob_processor.py`:

```python
"""Blob processor — replaces large event fields with blob store references.

Provides an async function that offloads known-large fields to an
AsyncDiskBlobStore, substituting them with ci-blob:// URIs.

CRITICAL: Unlike the bundle version, data is mutated IN-PLACE.
The server owns the deserialized JSON dict exclusively — no deepcopy.
"""

from __future__ import annotations

from context_intelligence_server.blob_store import AsyncDiskBlobStore

# Fields that contain large payloads and should be offloaded to blob storage.
BLOB_FIELDS: frozenset[str] = frozenset(
    {"raw", "result", "messages", "mount_plan", "context_snapshot", "debug"}
)


def _lift_raw_fields(data: dict) -> None:
    """Lift stop_reason, finish_reason, and usage from raw before offloading.

    Mutates *data* in place.
    """
    raw = data.get("raw")
    if not isinstance(raw, dict):
        return

    # Lift stop_reason (only if not already set at top level)
    if raw.get("stop_reason") is not None and data.get("stop_reason") is None:
        data["stop_reason"] = raw["stop_reason"]

    # Lift finish_reason (only if not already set at top level)
    if raw.get("finish_reason") is not None and data.get("finish_reason") is None:
        data["finish_reason"] = raw["finish_reason"]

    # Merge raw.usage into data.usage (existing keys win on collision)
    raw_usage = raw.get("usage")
    if isinstance(raw_usage, dict):
        existing_usage = data.get("usage")
        if isinstance(existing_usage, dict):
            data["usage"] = {**raw_usage, **existing_usage}
        else:
            data["usage"] = dict(raw_usage)


async def process_event_data(
    data: dict,
    blob_store: AsyncDiskBlobStore,
    session_id: str,
    node_id: str,
) -> None:
    """Replace known-large fields in *data* with blob store references.

    CRITICAL: Mutates *data* in place (no deepcopy). The server owns the
    deserialized JSON dict exclusively — no other handler shares it.

    For each field in BLOB_FIELDS:
    - If absent or None: skipped (no blob write, no change).
    - Otherwise: written to blob store, field replaced with {"$blob_ref": uri}.
    - If write fails: field replaced with {"$blob_error": "write failed: <reason>"}.
      Processing continues for remaining fields (non-blocking).
    """
    _lift_raw_fields(data)

    for field_name in BLOB_FIELDS:
        if field_name not in data or data[field_name] is None:
            continue

        key = f"{node_id}__{field_name}"
        try:
            uri = await blob_store.write(session_id, key, data[field_name])
            data[field_name] = {"$blob_ref": uri}
        except Exception as exc:  # noqa: BLE001
            data[field_name] = {"$blob_error": f"write failed: {exc}"}
```

### Step 4: Run tests to verify they pass

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence
pytest tests/test_blob_processor.py -v
```

Expected: **ALL PASS** (14 tests)

### Step 5: Commit

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence
git add context_intelligence_server/blob_processor.py tests/test_blob_processor.py
git commit -m "feat: phase 3 — blob processor with in-place mutation (no deepcopy)"
```

---

## Task 4: Wire AsyncDiskBlobStore into registry.py

**Files:**
- Modify: `context_intelligence_server/registry.py`
- Modify: `tests/test_registry.py`

### Step 1: Write the failing tests

Append to `tests/test_registry.py`:

```python
async def test_worker_has_blob_store(tmp_path, monkeypatch):
    """SessionWorker created by get_or_create has an AsyncDiskBlobStore."""
    from context_intelligence_server.blob_store import AsyncDiskBlobStore
    from context_intelligence_server.config import get_settings
    from context_intelligence_server.registry import SessionRegistry

    monkeypatch.setattr(get_settings(), "blob_path", str(tmp_path))

    reg = SessionRegistry()
    worker = reg.get_or_create("sess-1", "ws")

    assert isinstance(worker.services.blob_store, AsyncDiskBlobStore)


async def test_worker_blob_store_uses_configured_root(tmp_path, monkeypatch):
    """SessionWorker's blob_store root matches Settings.blob_path."""
    from pathlib import Path

    from context_intelligence_server.config import get_settings
    from context_intelligence_server.registry import SessionRegistry

    monkeypatch.setattr(get_settings(), "blob_path", str(tmp_path))

    reg = SessionRegistry()
    worker = reg.get_or_create("sess-1", "ws")

    assert worker.services.blob_store._root == Path(str(tmp_path))
```

### Step 2: Run tests to verify they fail

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence
pytest tests/test_registry.py -v -k "blob_store"
```

Expected: **FAIL** — `worker.services.blob_store` is `None` (Phase 2 default)

### Step 3: Modify `context_intelligence_server/registry.py`

Add this import at the top of `registry.py` (alongside the existing imports):

```python
from context_intelligence_server.blob_store import AsyncDiskBlobStore
from context_intelligence_server.config import get_settings
```

In the `get_or_create` method, find the line where `HookStateService` is instantiated. Add blob store creation **before** that line, and pass it to `HookStateService`:

```python
    def get_or_create(self, session_id: str, workspace: str) -> SessionWorker:
        """Return existing worker or create a new one for *session_id*."""
        if session_id not in self._workers:
            settings = get_settings()
            blob_store = AsyncDiskBlobStore(root=settings.blob_path)
            # ... existing graph_store creation ...
            services = HookStateService(
                # ... existing kwargs ...
                blob_store=blob_store,
            )
            worker = SessionWorker(
                session_id=session_id,
                workspace=workspace,
                services=services,
            )
            self._workers[session_id] = worker
            self.start_drain(worker)
        return self._workers[session_id]
```

The exact insertion depends on Phase 2's `get_or_create`. The key change: create an `AsyncDiskBlobStore(root=settings.blob_path)` and pass it as `blob_store=blob_store` to the `HookStateService` constructor.

### Step 4: Run tests to verify they pass

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence
pytest tests/test_registry.py -v
```

Expected: **ALL PASS** (existing tests + 2 new tests)

### Step 5: Commit

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence
git add context_intelligence_server/registry.py tests/test_registry.py
git commit -m "feat: phase 3 — wire AsyncDiskBlobStore into SessionWorker"
```

---

## Task 5: Wire Blob Processing into pipeline.py

**Files:**
- Modify: `context_intelligence_server/pipeline.py`
- Modify: `tests/test_pipeline.py`

### Step 1: Write the failing tests

Append to `tests/test_pipeline.py`:

```python
from unittest.mock import AsyncMock, patch


async def test_blob_processing_called_when_timestamp_present(monkeypatch):
    """process_event calls process_event_data when session_id and timestamp exist."""
    from context_intelligence_server.pipeline import process_event
    from context_intelligence_server.registry import SessionRegistry
    from context_intelligence_server.config import get_settings

    monkeypatch.setattr(get_settings(), "blob_path", "/tmp/test-blobs")

    registry = SessionRegistry()
    worker = registry.get_or_create("sess-1", "ws")

    mock_process = AsyncMock()
    with patch("context_intelligence_server.pipeline.process_event_data", mock_process):
        data = {
            "session_id": "sess-1",
            "timestamp": "2026-03-13T14:30:00Z",
            "raw": {"big": "payload"},
        }
        await process_event(worker, "tool:post", data)

    mock_process.assert_awaited_once()
    # Verify correct arguments: data dict, blob_store, session_id, node_id
    call_args = mock_process.call_args
    assert call_args[0][0] is data  # Same dict reference (in-place)
    assert call_args[0][2] == "sess-1"  # session_id


async def test_blob_processing_skipped_without_timestamp(monkeypatch):
    """process_event skips blob processing when timestamp is missing."""
    from context_intelligence_server.pipeline import process_event
    from context_intelligence_server.registry import SessionRegistry
    from context_intelligence_server.config import get_settings

    monkeypatch.setattr(get_settings(), "blob_path", "/tmp/test-blobs")

    registry = SessionRegistry()
    worker = registry.get_or_create("sess-1", "ws")

    mock_process = AsyncMock()
    with patch("context_intelligence_server.pipeline.process_event_data", mock_process):
        data = {"session_id": "sess-1"}  # No timestamp
        await process_event(worker, "tool:post", data)

    mock_process.assert_not_awaited()


async def test_blob_processing_skipped_without_session_id(monkeypatch):
    """process_event skips blob processing when session_id is missing."""
    from context_intelligence_server.pipeline import process_event
    from context_intelligence_server.registry import SessionRegistry
    from context_intelligence_server.config import get_settings

    monkeypatch.setattr(get_settings(), "blob_path", "/tmp/test-blobs")

    registry = SessionRegistry()
    worker = registry.get_or_create("", "ws")

    mock_process = AsyncMock()
    with patch("context_intelligence_server.pipeline.process_event_data", mock_process):
        data = {"timestamp": "2026-03-13T14:30:00Z"}  # No session_id
        await process_event(worker, "tool:post", data)

    mock_process.assert_not_awaited()


async def test_handler_receives_blob_refs(tmp_path, monkeypatch):
    """Handler receives mutated data with $blob_ref instead of raw values."""
    from context_intelligence_server.pipeline import process_event
    from context_intelligence_server.registry import SessionRegistry
    from context_intelligence_server.config import get_settings

    monkeypatch.setattr(get_settings(), "blob_path", str(tmp_path))

    registry = SessionRegistry()
    worker = registry.get_or_create("sess-1", "ws")

    captured_data = {}

    async def fake_handler(event, data, *args, **kwargs):
        captured_data.update(data)

    with patch("context_intelligence_server.pipeline._find_handler", return_value=fake_handler):
        data = {
            "session_id": "sess-1",
            "timestamp": "2026-03-13T14:30:00Z",
            "raw": {"llm_response": "huge payload"},
        }
        await process_event(worker, "tool:post", data)

    # Handler should see $blob_ref, not the original raw value
    assert "$blob_ref" in captured_data.get("raw", {})
    assert captured_data["raw"]["$blob_ref"].startswith("ci-blob://")
```

### Step 2: Run tests to verify they fail

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence
pytest tests/test_pipeline.py -v -k "blob"
```

Expected: **FAIL** — `process_event_data` is not called in the pipeline

### Step 3: Modify `context_intelligence_server/pipeline.py`

Add these imports at the top of `pipeline.py`:

```python
from context_intelligence_server.blob_processor import process_event_data
from context_intelligence_server.utils import make_node_id
```

In the `process_event` function, insert the blob processing block **after** the `ensure_session_node` call and **before** the `_find_handler` call:

```python
async def process_event(worker, event: str, data: dict) -> None:
    try:
        session_id = data.get("session_id", "")
        timestamp = data.get("timestamp", "")

        if session_id:
            await worker.services.ensure_session_node(session_id, data)

        # --- BEGIN Phase 3 addition: blob processing ---
        if session_id and timestamp and worker.services.blob_store:
            node_id = make_node_id(session_id, event, timestamp)
            await process_event_data(
                data, worker.services.blob_store, session_id, node_id
            )
        # --- END Phase 3 addition ---

        handler = _find_handler(event)
        if handler:
            await handler(event, data)
        _check_terminal_flush(event, worker)
    except Exception:
        logger.exception(
            "event_processing_error event=%s session_id=%s",
            event,
            data.get("session_id", ""),
        )
```

The exact function signature and surrounding code depends on Phase 2's implementation. The three new lines to insert (between `ensure_session_node` and `_find_handler`) are:

```python
        if session_id and timestamp and worker.services.blob_store:
            node_id = make_node_id(session_id, event, timestamp)
            await process_event_data(data, worker.services.blob_store, session_id, node_id)
```

### Step 4: Run tests to verify they pass

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence
pytest tests/test_pipeline.py -v
```

Expected: **ALL PASS** (existing tests + 4 new tests)

### Step 5: Commit

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence
git add context_intelligence_server/pipeline.py tests/test_pipeline.py
git commit -m "feat: phase 3 — wire blob processing into pipeline before handler dispatch"
```

---

## Task 6: GET /blobs/{session_id}/{key} Endpoint

**Files:**
- Modify: `context_intelligence_server/main.py`
- Modify: `tests/test_main.py`

### Step 1: Write the failing tests

Append to `tests/test_main.py`:

```python
import json as _json
from pathlib import Path as _Path


async def test_get_blob_returns_200(client, tmp_path, monkeypatch):
    """GET /blobs/{session_id}/{key} returns 200 with blob content."""
    from context_intelligence_server.config import get_settings

    monkeypatch.setattr(get_settings(), "blob_path", str(tmp_path))

    # Write a blob file directly to disk in the expected layout
    blob_dir = tmp_path / "sess-1" / "blobs"
    blob_dir.mkdir(parents=True)
    (blob_dir / "my_key.json").write_text(_json.dumps({"hello": "world"}))

    resp = await client.get("/blobs/sess-1/my_key")
    assert resp.status_code == 200
    assert resp.json() == {"hello": "world"}


async def test_get_blob_returns_404_for_missing(client, tmp_path, monkeypatch):
    """GET /blobs/{session_id}/{key} returns 404 when blob doesn't exist."""
    from context_intelligence_server.config import get_settings

    monkeypatch.setattr(get_settings(), "blob_path", str(tmp_path))

    resp = await client.get("/blobs/no-session/no-key")
    assert resp.status_code == 404
    assert "not found" in resp.json()["detail"].lower()
```

### Step 2: Run tests to verify they fail

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence
pytest tests/test_main.py -v -k "get_blob"
```

Expected: **FAIL** — `404 Not Found` (route does not exist yet)

### Step 3: Add the endpoint to `context_intelligence_server/main.py`

Add these imports at the top of `main.py` (alongside the existing imports):

```python
from fastapi import HTTPException
from fastapi.responses import JSONResponse

from context_intelligence_server.blob_store import AsyncDiskBlobStore
```

Add this route after the existing endpoints:

```python
@app.get("/blobs/{session_id}/{key}")
async def get_blob(session_id: str, key: str):
    """Retrieve a single blob's JSON content by session and key."""
    settings = get_settings()
    blob_store = AsyncDiskBlobStore(root=settings.blob_path)
    uri = f"ci-blob://{session_id}/{key}"
    try:
        content = await blob_store.read(uri)
        return JSONResponse(content=content)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"Blob not found: {uri}")
```

### Step 4: Run tests to verify they pass

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence
pytest tests/test_main.py -v -k "get_blob"
```

Expected: **ALL PASS** (2 new tests)

### Step 5: Commit

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence
git add context_intelligence_server/main.py tests/test_main.py
git commit -m "feat: phase 3 — GET /blobs/{session_id}/{key} endpoint"
```

---

## Task 7: GET /blobs/{session_id} Endpoint

**Files:**
- Modify: `context_intelligence_server/main.py`
- Modify: `tests/test_main.py`

### Step 1: Write the failing tests

Append to `tests/test_main.py`:

```python
async def test_list_blobs_empty_session(client, tmp_path, monkeypatch):
    """GET /blobs/{session_id} returns empty list for session with no blobs."""
    from context_intelligence_server.config import get_settings

    monkeypatch.setattr(get_settings(), "blob_path", str(tmp_path))

    resp = await client.get("/blobs/no-blobs-session")
    assert resp.status_code == 200
    body = resp.json()
    assert body["session_id"] == "no-blobs-session"
    assert body["blobs"] == []


async def test_list_blobs_returns_uris(client, tmp_path, monkeypatch):
    """GET /blobs/{session_id} returns ci-blob:// URIs for existing blobs."""
    from context_intelligence_server.config import get_settings

    monkeypatch.setattr(get_settings(), "blob_path", str(tmp_path))

    # Write blob files directly to disk
    blob_dir = tmp_path / "sess-1" / "blobs"
    blob_dir.mkdir(parents=True)
    (blob_dir / "alpha.json").write_text('{"a": 1}')
    (blob_dir / "beta.json").write_text('{"b": 2}')

    resp = await client.get("/blobs/sess-1")
    assert resp.status_code == 200
    body = resp.json()
    assert body["session_id"] == "sess-1"
    assert sorted(body["blobs"]) == [
        "ci-blob://sess-1/alpha",
        "ci-blob://sess-1/beta",
    ]
```

### Step 2: Run tests to verify they fail

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence
pytest tests/test_main.py -v -k "list_blobs"
```

Expected: **FAIL** — `404 Not Found` or route conflict (route does not exist yet)

### Step 3: Add the endpoint to `context_intelligence_server/main.py`

Add this route **before** the `GET /blobs/{session_id}/{key}` route (to avoid FastAPI matching `{key}` for the list endpoint):

```python
@app.get("/blobs/{session_id}")
async def list_blobs(session_id: str):
    """List all ci-blob:// URIs for a session."""
    settings = get_settings()
    blob_store = AsyncDiskBlobStore(root=settings.blob_path)
    uris = await blob_store.list(session_id)
    return {"session_id": session_id, "blobs": uris}
```

**Route ordering matters:** `GET /blobs/{session_id}` must be declared **before** `GET /blobs/{session_id}/{key}` in `main.py`. FastAPI matches routes in declaration order, and `/blobs/sess-1` should match the list endpoint, not the key endpoint with an empty key.

### Step 4: Run tests to verify they pass

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence
pytest tests/test_main.py -v -k "list_blobs or get_blob"
```

Expected: **ALL PASS** (2 list tests + 2 get tests)

### Step 5: Commit

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence
git add context_intelligence_server/main.py tests/test_main.py
git commit -m "feat: phase 3 — GET /blobs/{session_id} list endpoint"
```

---

## Task 8: POST /cypher — Request Model and Proxy Endpoint

**Files:**
- Modify: `context_intelligence_server/models.py`
- Modify: `context_intelligence_server/main.py`
- Modify: `tests/test_main.py`

### Step 1: Write the failing tests

Append to `tests/test_main.py`:

```python
from unittest.mock import AsyncMock, MagicMock


async def test_cypher_request_model_valid():
    """CypherRequest parses a well-formed payload."""
    from context_intelligence_server.models import CypherRequest

    req = CypherRequest(query="MATCH (n) RETURN n LIMIT 10")
    assert req.query == "MATCH (n) RETURN n LIMIT 10"
    assert req.params == {}
    assert req.workspace is None  # None = cross-workspace


async def test_cypher_request_model_with_workspace():
    """CypherRequest accepts workspace for scoped queries."""
    from context_intelligence_server.models import CypherRequest

    req = CypherRequest(
        query="MATCH (n) WHERE n.workspace = $workspace RETURN n",
        params={"limit": 5},
        workspace="my-project",
    )
    assert req.workspace == "my-project"
    assert req.params == {"limit": 5}


async def test_cypher_proxy_returns_results(client):
    """POST /cypher returns 200 with query results."""
    from context_intelligence_server.main import app

    # Mock the shared Neo4j driver on app.state
    mock_result = MagicMock()
    mock_records = [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]

    async def mock_aiter(self):
        for r in mock_records:
            yield r

    mock_result.__aiter__ = mock_aiter

    mock_session = AsyncMock()
    mock_session.run.return_value = mock_result
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=False)

    mock_driver = MagicMock()
    mock_driver.session.return_value = mock_session

    app.state.neo4j_driver = mock_driver

    resp = await client.post("/cypher", json={"query": "MATCH (n) RETURN n"})
    assert resp.status_code == 200
    body = resp.json()
    assert "results" in body
    assert len(body["results"]) == 2


async def test_cypher_proxy_injects_workspace(client):
    """POST /cypher injects workspace param when workspace is not None."""
    from context_intelligence_server.main import app

    mock_result = MagicMock()

    async def mock_aiter(self):
        return
        yield  # empty async generator

    mock_result.__aiter__ = mock_aiter

    mock_session = AsyncMock()
    mock_session.run.return_value = mock_result
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=False)

    mock_driver = MagicMock()
    mock_driver.session.return_value = mock_session

    app.state.neo4j_driver = mock_driver

    await client.post(
        "/cypher",
        json={
            "query": "MATCH (n) WHERE n.workspace = $workspace RETURN n",
            "workspace": "my-project",
        },
    )

    # Verify workspace was injected into params
    call_args = mock_session.run.call_args
    params = call_args[1] if call_args[1] else call_args[0][1]
    assert params.get("workspace") == "my-project"


async def test_cypher_proxy_neo4j_error_returns_500(client):
    """POST /cypher returns 500 when Neo4j raises an error."""
    from context_intelligence_server.main import app

    mock_session = AsyncMock()
    mock_session.run.side_effect = Exception("Neo4j connection refused")
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=False)

    mock_driver = MagicMock()
    mock_driver.session.return_value = mock_session

    app.state.neo4j_driver = mock_driver

    resp = await client.post("/cypher", json={"query": "INVALID CYPHER"})
    assert resp.status_code == 500
    assert "Neo4j connection refused" in resp.json()["detail"]
```

### Step 2: Run tests to verify they fail

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence
pytest tests/test_main.py -v -k "cypher"
```

Expected: **FAIL** — `ModuleNotFoundError` for `CypherRequest`, `404` for `/cypher` route

### Step 3: Add the CypherRequest model to `context_intelligence_server/models.py`

Add this class to the end of `models.py`:

```python
class CypherRequest(BaseModel):
    """Request body for POST /cypher."""

    query: str
    params: dict[str, Any] = {}
    workspace: str | None = None  # None = cross-workspace query
```

### Step 4: Add the cypher proxy endpoint to `context_intelligence_server/main.py`

Add these imports at the top of `main.py` (alongside existing imports):

```python
import json as _json

from fastapi.responses import Response

from context_intelligence_server.models import CypherRequest
```

Add this route after the blob endpoints:

```python
@app.post("/cypher")
async def cypher_proxy(body: CypherRequest):
    """Proxy a Cypher query to Neo4j and return results as JSON.

    Uses a shared Neo4j driver (not per-session). The driver is created
    and closed in the FastAPI lifespan (Task 9).

    Workspace injection:
    - workspace=None  → cross-workspace query (no $workspace param injected)
    - workspace="*"   → same as None (cross-workspace)
    - workspace="foo" → injects $workspace="foo" into query params
    """
    driver = app.state.neo4j_driver
    workspace = body.workspace if body.workspace is not None else "*"
    params = dict(body.params)
    if workspace != "*":
        params["workspace"] = workspace

    try:
        async with driver.session() as session:
            result = await session.run(body.query, params)
            records = [dict(record) async for record in result]
        # Serialize with str() fallback for Neo4j-specific types
        serialized = _json.dumps({"results": records}, default=str)
        return Response(content=serialized, media_type="application/json")
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
```

### Step 5: Run tests to verify they pass

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence
pytest tests/test_main.py -v -k "cypher"
```

Expected: **ALL PASS** (5 cypher tests)

### Step 6: Commit

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence
git add context_intelligence_server/models.py context_intelligence_server/main.py tests/test_main.py
git commit -m "feat: phase 3 — POST /cypher proxy endpoint with CypherRequest model"
```

---

## Task 9: Wire Shared Neo4j Driver in Lifespan

**Files:**
- Modify: `context_intelligence_server/main.py`
- Modify: `tests/test_main.py`

### Step 1: Write the failing test

Append to `tests/test_main.py`:

```python
async def test_lifespan_creates_and_closes_neo4j_driver():
    """FastAPI lifespan creates a shared Neo4j driver at startup and closes at shutdown."""
    from unittest.mock import patch

    mock_driver = MagicMock()
    mock_driver.close = AsyncMock()

    with patch("context_intelligence_server.main.AsyncGraphDatabase") as mock_agd:
        mock_agd.driver.return_value = mock_driver

        # Import after patching to pick up the mock
        from context_intelligence_server.main import lifespan, app as _app
        from contextlib import asynccontextmanager

        # Manually run the lifespan context manager
        async with lifespan(_app):
            # During lifespan: driver should be created
            mock_agd.driver.assert_called_once()
            assert _app.state.neo4j_driver is mock_driver

        # After lifespan exit: driver should be closed
        mock_driver.close.assert_awaited_once()
```

### Step 2: Run tests to verify it fails

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence
pytest tests/test_main.py::test_lifespan_creates_and_closes_neo4j_driver -v
```

Expected: **FAIL** — `ImportError: cannot import name 'lifespan'` (lifespan function does not exist yet)

### Step 3: Add the lifespan to `context_intelligence_server/main.py`

Add these imports at the top of `main.py`:

```python
from contextlib import asynccontextmanager

from neo4j import AsyncGraphDatabase
```

Add the lifespan context manager **before** the `app = FastAPI(...)` line:

```python
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan: create and close shared Neo4j driver."""
    settings = get_settings()
    app.state.neo4j_driver = AsyncGraphDatabase.driver(
        settings.neo4j_url,
        auth=(settings.neo4j_user, settings.neo4j_password),
    )
    logger.info("shared Neo4j driver created for POST /cypher")
    yield
    await app.state.neo4j_driver.close()
    logger.info("shared Neo4j driver closed")
```

Update the `app = FastAPI(...)` line to use the lifespan:

```python
app = FastAPI(title="Context Intelligence Server", lifespan=lifespan)
```

**Note:** The `AsyncGraphDatabase.driver()` call does not connect to Neo4j at creation time — it only creates a driver object. Actual connections happen when queries are executed. This means the lifespan will succeed even if Neo4j is unreachable at startup, which is correct behavior for tests and for Docker Compose startup ordering.

### Step 4: Run tests to verify they pass

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence
pytest tests/test_main.py -v
```

Expected: **ALL PASS** (all existing tests + lifespan test)

**Important:** Existing tests use the `client` fixture with `ASGITransport`, which does **not** trigger lifespan events. This is correct — existing tests don't need the Neo4j driver. The cypher tests from Task 8 manually set `app.state.neo4j_driver` to a mock. The lifespan test explicitly invokes the lifespan context manager.

### Step 5: Commit

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence
git add context_intelligence_server/main.py tests/test_main.py
git commit -m "feat: phase 3 — shared Neo4j driver in FastAPI lifespan"
```

---

## Task 10: Integration Test — Blob Pipeline End-to-End

**Files:**
- Create: `tests/integration/__init__.py`
- Create: `tests/integration/test_blob_pipeline.py`

### Step 1: Create the integration test directory

Create file `tests/integration/__init__.py`:

```python
```

(Empty file — makes `tests/integration/` a package.)

### Step 2: Write the integration test

Create file `tests/integration/test_blob_pipeline.py`:

```python
"""Integration test: blob processing through the full pipeline.

Verifies end-to-end:
1. POST /events with a blob-eligible field (result)
2. Pipeline drain loop runs blob processing (in-place mutation)
3. GET /blobs/{session_id} lists the stored blob URIs
4. GET /blobs/{session_id}/{key} returns the original field content
"""

from __future__ import annotations

import asyncio

import pytest
from httpx import ASGITransport, AsyncClient


@pytest.fixture
async def integration_env(tmp_path, monkeypatch):
    """Set up isolated environment for integration test.

    - Override blob_path to tmp_path
    - Return (client, tmp_path) tuple
    """
    from context_intelligence_server.config import get_settings

    monkeypatch.setattr(get_settings(), "blob_path", str(tmp_path))

    from context_intelligence_server.main import app

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac, tmp_path


async def test_blob_pipeline_end_to_end(integration_env):
    """POST an event with blob-eligible fields, then retrieve via blob endpoints."""
    client, blob_root = integration_env

    session_id = "integration-test-sess"
    original_result = {"tool_output": "large response data", "status": "success"}

    # 1. POST an event with a blob-eligible 'result' field
    payload = {
        "event": "tool:post",
        "workspace": "test-workspace",
        "data": {
            "session_id": session_id,
            "timestamp": "2026-03-13T14:30:00Z",
            "result": original_result,
            "tool_name": "my_tool",
        },
    }
    resp = await client.post("/events", json=payload)
    assert resp.status_code == 202

    # 2. Wait for the drain loop to process the event
    from context_intelligence_server.main import registry

    worker = registry.get_or_create(session_id, "test-workspace")
    try:
        await asyncio.wait_for(worker.queue.join(), timeout=10.0)
    except asyncio.TimeoutError:
        pytest.fail("Drain loop did not process the event within 10 seconds")

    # 3. GET /blobs/{session_id} — should list at least one blob URI
    resp = await client.get(f"/blobs/{session_id}")
    assert resp.status_code == 200
    body = resp.json()
    assert body["session_id"] == session_id
    assert len(body["blobs"]) >= 1

    # Find the 'result' blob URI (key ends with __result)
    result_uris = [uri for uri in body["blobs"] if "__result" in uri]
    assert len(result_uris) == 1, f"Expected 1 result blob, got: {body['blobs']}"
    result_uri = result_uris[0]

    # 4. Parse session_id and key from the URI
    # URI format: ci-blob://<session_id>/<key>
    assert result_uri.startswith("ci-blob://")
    uri_path = result_uri[len("ci-blob://"):]
    uri_session_id, uri_key = uri_path.split("/", 1)
    assert uri_session_id == session_id

    # 5. GET /blobs/{session_id}/{key} — should return the original content
    resp = await client.get(f"/blobs/{session_id}/{uri_key}")
    assert resp.status_code == 200
    assert resp.json() == original_result
```

### Step 3: Run the integration test

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence
pytest tests/integration/test_blob_pipeline.py -v
```

Expected: **PASS**

### Step 4: Run the full test suite

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence
pytest tests/ -v
```

Expected: **ALL PASS** — all Phase 1, Phase 2, and Phase 3 tests pass.

### Step 5: Commit

```bash
cd /home/dicolomb/amplifier-context-intelligence/amplifier-context-intelligence
git add tests/integration/
git commit -m "feat: phase 3 — blob storage and cypher proxy (complete)"
```

---

## Summary

| Task | What It Builds | Tests Added |
|------|---------------|-------------|
| 1 | `blob_store.py` — Protocol, write, read, list | 11 |
| 2 | `blob_store.py` — dump() method | 4 |
| 3 | `blob_processor.py` — in-place transform, _lift_raw_fields | 14 |
| 4 | Wire `AsyncDiskBlobStore` into `registry.py` | 2 |
| 5 | Wire blob processing into `pipeline.py` | 4 |
| 6 | `GET /blobs/{session_id}/{key}` endpoint | 2 |
| 7 | `GET /blobs/{session_id}` endpoint | 2 |
| 8 | `POST /cypher` model + proxy endpoint | 5 |
| 9 | Shared Neo4j driver in FastAPI lifespan | 1 |
| 10 | Integration test — blob pipeline end-to-end | 1 |
| **Total** | | **~46 tests** |

---

## Key Constraints Checklist

- [ ] `copy.deepcopy(data)` is REMOVED — in-place mutation only in `blob_processor.py`
- [ ] `ci-blob://` URI scheme is kept exactly as-is in storage and Neo4j properties
- [ ] `AsyncDiskBlobStore.read()` uses session_id from the URI (fixes bundle footgun)
- [ ] All blob filesystem operations use `asyncio.to_thread` — never blocking the event loop
- [ ] `POST /cypher` uses a shared driver, not per-session
- [ ] The shared Neo4j driver is created and closed in the FastAPI lifespan
- [ ] `GET /blobs/{session_id}` route is declared before `GET /blobs/{session_id}/{key}` to avoid matching conflicts
- [ ] Blob processing is conditional: requires `session_id`, `timestamp`, and `worker.services.blob_store` to all be truthy
- [ ] `_lift_raw_fields` promotes stop_reason, finish_reason, and usage from raw BEFORE blob offloading
- [ ] `dump()` default dest_dir uses `Path(tempfile.gettempdir()) / "ci-blobs"` instead of hardcoded `/tmp/ci-blobs/`
