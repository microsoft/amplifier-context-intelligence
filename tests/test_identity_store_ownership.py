"""Exclusive, lifetime ownership of the active identity store (PR #126 review).

Review findings covered:

* r4329246423 - an EXISTING identity store was only protected when seeds or an
  admin key were configured, so an unseeded second server could still read it.
* r4329246508 - servers with different queue-root locks could share one mutable
  identity store, so a revocation made through one was never seen by the other.

Startup contract (final user constraint): the server comes up and advertises
identity state; identity initialization runs ONCE in a worker thread and never
blocks the lifespan.  Expected identity failures (OSError, ownership errors, a
selection ValueError) leave the server up with identity UNAVAILABLE: every
authenticated or mutating request answers 503, ``/status`` reports it, and
nothing retries, scans, or heals in the background.

All keys, oids and tokens below are synthetic.  No live service is contacted.
"""

from __future__ import annotations

import asyncio
import errno
import fcntl
import gc
import hashlib
import inspect
import json
import multiprocessing
import os
import stat
import threading
import time
import warnings
from collections.abc import AsyncGenerator
from concurrent.futures import Future as ConcurrentFuture
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

import httpx
import pytest
from fastapi import FastAPI

import context_intelligence_server.main as main_module
from context_intelligence_server.config import Settings
from context_intelligence_server.identity_store import IdentityStore
from context_intelligence_server.instance_lock import (
    IdentityStoreLock,
    IdentityStoreLockUnavailable,
    IdentityStoreOwnershipError,
)

# These tests drive the real lifespan themselves.
pytestmark = pytest.mark.raw_identity_factory

TOKEN = "synthetic-token-one"
DIGEST = hashlib.sha256(TOKEN.encode()).hexdigest()
ADMIN_TOKEN = "synthetic-admin-token"
ADMIN_DIGEST = hashlib.sha256(ADMIN_TOKEN.encode()).hexdigest()
OID = "11111111-2222-3333-4444-555566667777"
CLIENT_ID = "aaaabbbb-1111-2222-3333-ccccddddeeee"
TENANT_ID = "ffffeeee-dddd-cccc-bbbb-aaaa99998888"
STORE_NAMES = {"static": "api-keys.json", "entra": "entra-identities.json"}


class _StubJWKS:
    def get_signing_key_from_jwt(self, token: str) -> Any:
        return SimpleNamespace(key="dummy")

    def fetch_data(self) -> None:
        pass

    def get_jwk_set(self) -> Any:
        return SimpleNamespace(keys=[SimpleNamespace(key="dummy")])


def _settings(
    root: Path, mode: str, store: Path, *, seeded: bool = True, admin: bool = False
) -> Settings:
    """Settings whose queue root is ``root`` and whose store is ``store``."""
    common: dict[str, Any] = {
        "queues_path": str(root / "queues"),
        "blob_path": str(root / "blobs"),
        "allow_unauthenticated": False,
    }
    if mode == "static":
        return Settings(
            auth_mode="static",
            api_keys={DIGEST: {"id": "writer"}} if seeded else None,
            admin_api_key_sha256=ADMIN_DIGEST if admin else None,
            api_keys_store_path=str(store),
            **common,
        )
    return Settings(
        auth_mode="entra",
        azure_client_id=CLIENT_ID,
        azure_tenant_id=TENANT_ID,
        entra_identities={OID: {"id": "writer"}} if seeded else None,
        entra_admin_role="IdentityAdmin" if admin else "",
        entra_identities_store_path=str(store),
        **common,
    )


@asynccontextmanager
async def _no_server_lifespan(_app: FastAPI) -> AsyncGenerator[None, None]:
    yield


@asynccontextmanager
async def _serving() -> AsyncGenerator[None, None]:
    """Enter the REAL lifespan, then await its single identity-init task.

    Used where a test is about AUTH behavior, not boot: the lifespan itself
    never waits for identity initialization, so the test does it explicitly.
    """
    async with main_module.lifespan(main_module.app):
        task = getattr(main_module.app.state, "identity_init_task", None)
        if task is not None:
            await task
        yield


async def _get_json(wrapper: Any, path: str) -> dict[str, Any]:
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=wrapper), base_url="http://test"
    ) as client:
        response = await client.get(path)
        assert response.status_code == 200, response.text
        return response.json()


def _child_start(mode: str, root: str, store: str, seeded: bool, out: Any) -> None:
    """Spawned process: build the app and enter the REAL lifespan once."""
    import asyncio as aio

    import context_intelligence_server.main as m

    m._server_lifespan = _no_server_lifespan  # type: ignore[assignment]
    settings = _settings(Path(root), mode, Path(store), seeded=seeded)

    async def run() -> None:
        m.create_asgi_app(settings=settings, _jwks_client=_StubJWKS())
        try:
            async with m.lifespan(m.app):
                task = getattr(m.app.state, "identity_init_task", None)
                if task is not None:
                    await task
                out.put(("started", getattr(m.app.state, "identity_state", "unknown")))
        except RuntimeError as exc:
            out.put(("refused", str(exc)))

    aio.run(run())


def _spawn_child(mode: str, root: Path, store: Path, seeded: bool) -> tuple[str, str]:
    ctx = multiprocessing.get_context("spawn")
    out = ctx.Queue()
    child = ctx.Process(
        target=_child_start, args=(mode, str(root), str(store), seeded, out)
    )
    child.start()
    child.join(timeout=60)
    assert child.exitcode == 0
    return out.get(timeout=5)


def _make_app(settings: Settings) -> Any:
    return main_module.create_asgi_app(settings=settings, _jwks_client=_StubJWKS())


async def _status(wrapper: Any, path: str, token: str | None = None) -> int:
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=wrapper), base_url="http://test"
    ) as client:
        return (await client.get(path, headers=headers)).status_code


@pytest.fixture(autouse=True)
def _isolate(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(main_module, "_server_lifespan", _no_server_lifespan)
    main_module._asgi_app = None


# ---------------------------------------------------------------------------
# Active-store selection (r4329246423)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("mode", ["static", "entra"])
def test_existing_unseeded_store_is_selected_by_identity_init_not_stateful_root(
    tmp_path: Path, mode: str
) -> None:
    """An existing store is active even with no seed and no admin capability."""
    store = tmp_path / "state" / "identity" / STORE_NAMES[mode]
    unseeded = _settings(tmp_path / "state", mode, store, seeded=False)
    assert unseeded.active_identity_store_path() is None  # absent: inert
    assert not store.parent.exists()  # and never created

    store.parent.mkdir(parents=True)
    store.write_text("{}", encoding="utf-8")
    assert unseeded.active_identity_store_path() == store
    assert unseeded.stateful_root() == (tmp_path / "state").resolve()

    # Cross-root protection is the identity lock's job at identity init, not an
    # early synchronous boot check: an unseeded server elsewhere still boots.
    elsewhere = _settings(tmp_path / "other", mode, store, seeded=False)
    assert elsewhere.stateful_root() == (tmp_path / "other").resolve()


@pytest.mark.parametrize("mode", ["static", "entra"])
def test_stateful_root_performs_no_identity_file_inspection(
    tmp_path: Path, mode: str
) -> None:
    """Synchronous early boot never stats the identity file (only identity init)."""
    blocker = tmp_path / "blocker"
    blocker.write_text("x", encoding="utf-8")
    settings = _settings(
        tmp_path / "state", mode, blocker / STORE_NAMES[mode], seeded=False
    )
    assert settings.stateful_root() == (tmp_path / "state").resolve()  # no raise
    with pytest.raises(ValueError, match="cannot be inspected"):
        settings.active_identity_store_path()  # inspection lives here only


@pytest.mark.parametrize("mode", ["static", "entra"])
def test_seed_or_admin_selects_even_when_the_file_is_absent(
    tmp_path: Path, mode: str
) -> None:
    store = tmp_path / "s" / STORE_NAMES[mode]
    assert _settings(tmp_path, mode, store).active_identity_store_path() == store
    admin_only = _settings(tmp_path, mode, store, seeded=False, admin=True)
    assert admin_only.active_identity_store_path() == store


def test_selector_expands_tilde(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("HOME", str(tmp_path))
    settings = Settings(
        auth_mode="static",
        api_keys={DIGEST: {"id": "w"}},
        api_keys_store_path="~/ids/api-keys.json",
    )
    assert settings.active_identity_store_path() == tmp_path / "ids" / "api-keys.json"


def test_selector_fails_closed_when_the_path_cannot_be_inspected(
    tmp_path: Path,
) -> None:
    """Only genuine absence is 'absent'; a file-as-directory is an error."""
    blocker = tmp_path / "blocker"
    blocker.write_text("x", encoding="utf-8")
    settings = _settings(
        tmp_path / "state", "static", blocker / "api-keys.json", seeded=False
    )
    with pytest.raises(ValueError, match="cannot be inspected"):
        settings.active_identity_store_path()


def test_dangling_symlink_counts_as_present(tmp_path: Path) -> None:
    store = tmp_path / "state" / "api-keys.json"
    store.parent.mkdir(parents=True)
    store.symlink_to(tmp_path / "missing-target")
    settings = _settings(tmp_path / "state", "static", store, seeded=False)
    assert settings.active_identity_store_path() == store


def test_absent_unselected_store_creates_no_state_on_import_or_build(
    tmp_path: Path,
) -> None:
    store = tmp_path / "state" / "identity" / "api-keys.json"
    wrapper = main_module.create_asgi_app(
        settings=_settings(tmp_path / "state", "static", store, seeded=False)
    )
    assert wrapper is not None
    assert not store.parent.exists()


# ---------------------------------------------------------------------------
# IdentityStoreLock primitive (r4329246508)
# ---------------------------------------------------------------------------


def _child_lock(store: str, out: Any) -> None:
    from context_intelligence_server.instance_lock import (
        IdentityStoreLock as Lock,
    )
    from context_intelligence_server.instance_lock import (
        IdentityStoreOwnershipError as Err,
    )

    lock = Lock(store)
    try:
        lock.acquire()
    except Err as exc:
        out.put(("refused", type(exc).__name__))
    else:
        out.put(("acquired", ""))
        lock.release()


def _run_lock_child(store: Path) -> tuple[str, str]:
    ctx = multiprocessing.get_context("spawn")
    out = ctx.Queue()
    child = ctx.Process(target=_child_lock, args=(str(store), out))
    child.start()
    child.join(timeout=60)
    assert child.exitcode == 0
    return out.get(timeout=5)


def test_second_process_is_refused_and_release_allows_a_new_owner(
    tmp_path: Path,
) -> None:
    store = tmp_path / "id" / "api-keys.json"
    first = IdentityStoreLock(store)
    first.acquire()
    try:
        assert _run_lock_child(store) == ("refused", "IdentityStoreLockUnavailable")
    finally:
        first.release()
    assert _run_lock_child(store) == ("acquired", "")


def test_parent_alias_contends_on_the_same_sidecar(tmp_path: Path) -> None:
    real = tmp_path / "real"
    real.mkdir()
    (tmp_path / "alias").symlink_to(real)
    first = IdentityStoreLock(real / "api-keys.json")
    first.acquire()
    try:
        assert _run_lock_child(tmp_path / "alias" / "api-keys.json")[0] == "refused"
    finally:
        first.release()


def test_different_stores_do_not_contend(tmp_path: Path) -> None:
    first = IdentityStoreLock(tmp_path / "a" / "api-keys.json")
    first.acquire()
    try:
        assert _run_lock_child(tmp_path / "b" / "api-keys.json")[0] == "acquired"
        assert _run_lock_child(tmp_path / "a" / "other.json")[0] == "acquired"
    finally:
        first.release()


def test_atomic_replace_keeps_the_lock_and_never_unlinks_the_sidecar(
    tmp_path: Path,
) -> None:
    store_path = tmp_path / "id" / "api-keys.json"
    lock = IdentityStoreLock(store_path)
    lock.acquire()
    store = IdentityStore(store_path, managed=True)
    store.attach(lock)
    try:
        sidecar = lock.path
        before = os.stat(sidecar).st_ino
        store.put(DIGEST, {"id": "writer"})
        store.put("b" * 64, {"id": "second"})
        assert os.stat(sidecar).st_ino == before
        probe = os.open(sidecar, os.O_RDWR)
        try:
            with pytest.raises(BlockingIOError):
                fcntl.flock(probe, fcntl.LOCK_EX | fcntl.LOCK_NB)
        finally:
            os.close(probe)
        assert stat.S_IMODE(os.stat(store_path).st_mode) == 0o600
    finally:
        store.stop()
        lock.release()
    assert sidecar.exists()  # release never unlinks


def test_final_symlink_store_is_rejected(tmp_path: Path) -> None:
    parent = tmp_path / "id"
    parent.mkdir()
    target = tmp_path / "elsewhere.json"
    target.write_text("{}", encoding="utf-8")
    (parent / "api-keys.json").symlink_to(target)
    with pytest.raises(IdentityStoreOwnershipError, match="regular"):
        IdentityStoreLock(parent / "api-keys.json").acquire()


def test_hardlinked_store_is_rejected(tmp_path: Path) -> None:
    parent = tmp_path / "id"
    parent.mkdir()
    original = tmp_path / "elsewhere.json"
    original.write_text("{}", encoding="utf-8")
    os.link(original, parent / "api-keys.json")
    with pytest.raises(IdentityStoreOwnershipError, match="single-link"):
        IdentityStoreLock(parent / "api-keys.json").acquire()


def test_symlink_or_hardlink_sidecar_is_rejected(tmp_path: Path) -> None:
    parent = tmp_path / "id"
    parent.mkdir()
    decoy = tmp_path / "decoy"
    decoy.write_text("", encoding="utf-8")
    (parent / "api-keys.json.lock").symlink_to(decoy)
    with pytest.raises(IdentityStoreOwnershipError):
        IdentityStoreLock(parent / "api-keys.json").acquire()
    (parent / "api-keys.json.lock").unlink()
    os.link(decoy, parent / "api-keys.json.lock")
    with pytest.raises(
        IdentityStoreOwnershipError, match="bound to the held descriptor"
    ):
        IdentityStoreLock(parent / "api-keys.json").acquire()


def test_store_swapped_for_a_symlink_after_acquire_is_refused(tmp_path: Path) -> None:
    store_path = tmp_path / "id" / "api-keys.json"
    lock = IdentityStoreLock(store_path)
    lock.acquire()
    try:
        store_path.write_text("{}", encoding="utf-8")
        store_path.unlink()
        store_path.symlink_to(tmp_path / "attacker.json")
        with pytest.raises(IdentityStoreOwnershipError):
            lock.write_store_atomic(b"{}")
        assert not (tmp_path / "attacker.json").exists()
    finally:
        lock.release()


def test_parent_directory_replacement_is_detected(tmp_path: Path) -> None:
    parent = tmp_path / "id"
    lock = IdentityStoreLock(parent / "api-keys.json")
    lock.acquire()
    try:
        parent.rename(tmp_path / "moved")
        parent.mkdir()
        with pytest.raises(IdentityStoreOwnershipError, match="replaced"):
            lock.write_store_atomic(b"{}")
        assert not (parent / "api-keys.json").exists()
    finally:
        lock.release()


def test_unwritable_directory_fails_with_a_clear_message(tmp_path: Path) -> None:
    if os.geteuid() == 0:
        pytest.skip("root ignores directory permissions")
    parent = tmp_path / "ro"
    parent.mkdir()
    parent.chmod(0o500)
    try:
        with pytest.raises(IdentityStoreOwnershipError, match="writable"):
            IdentityStoreLock(parent / "api-keys.json").acquire()
    finally:
        parent.chmod(0o700)


def test_forked_child_is_not_authorized_and_cannot_release_the_parents_lock(
    tmp_path: Path,
) -> None:
    store = tmp_path / "id" / "api-keys.json"
    lock = IdentityStoreLock(store)
    lock.acquire()
    try:
        read_end, write_end = os.pipe()
        pid = os.fork()
        if pid == 0:  # child: inherited descriptors only
            code = 1
            try:
                try:
                    lock.assert_owned()
                except IdentityStoreOwnershipError:
                    code = 0
                lock.release()  # must NOT drop the parent's flock
            finally:
                os.write(write_end, b"x")
                os._exit(code)
        os.close(write_end)
        os.read(read_end, 1)
        os.close(read_end)
        _, status = os.waitpid(pid, 0)
        assert os.WEXITSTATUS(status) == 0
        assert lock.owned_by_this_process()
        assert _run_lock_child(store)[0] == "refused"  # parent still owns it
    finally:
        lock.release()


# ---------------------------------------------------------------------------
# IdentityStore under ownership
# ---------------------------------------------------------------------------


def test_managed_store_refuses_to_operate_without_an_owner(tmp_path: Path) -> None:
    store = IdentityStore(tmp_path / "api-keys.json", managed=True)
    for call in (store.load, lambda: store.put("k", {"id": "x"}), store.exists):
        with pytest.raises(IdentityStoreOwnershipError):
            call()
    assert not (tmp_path / "api-keys.json").exists()


def test_seed_does_not_swallow_ownership_errors(tmp_path: Path) -> None:
    store_path = tmp_path / "id" / "api-keys.json"
    lock = IdentityStoreLock(store_path)
    lock.acquire()
    store = IdentityStore(store_path, managed=True)
    store.attach(lock)
    try:
        (tmp_path / "id").rename(tmp_path / "moved")
        (tmp_path / "id").mkdir()
        with pytest.raises(IdentityStoreOwnershipError):
            store.seed({DIGEST: {"id": "writer"}})
        assert not store.healthy()  # ownership failure disables the store
    finally:
        store.stop()
        lock.release()


def test_corrupt_store_fails_closed_empty_and_is_never_reseeded(tmp_path: Path) -> None:
    store_path = tmp_path / "id" / "api-keys.json"
    store_path.parent.mkdir()
    store_path.write_text("{not json", encoding="utf-8")
    settings = _settings(tmp_path, "static", store_path)
    _make_app(settings)

    async def run() -> None:
        async with _serving():
            assert main_module.app.state.api_key_store.flat_dict == {}

    asyncio.run(run())
    assert store_path.read_text(encoding="utf-8") == "{not json"


# ---------------------------------------------------------------------------
# Real lifespan: ordering, release, reload
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("mode", ["static", "entra"])
def test_second_server_with_an_unrelated_root_comes_up_with_identity_unavailable(
    tmp_path: Path, mode: str
) -> None:
    """Cross-root: the first owner keeps serving; the second is UP but identity-less.

    The second server's listener must not fail; only its identity subsystem is
    unavailable (so auth and mutation answer 503 there), and it never loads or
    seeds the shared file.
    """
    store = tmp_path / "first" / "identity" / STORE_NAMES[mode]
    first_settings = _settings(tmp_path / "first", mode, store)

    async def run() -> None:
        wrapper = _make_app(first_settings)
        async with _serving():
            before = store.read_bytes()
            result = _spawn_child(mode, tmp_path / "second", store, seeded=False)
            assert result == ("started", "unavailable"), result
            assert store.read_bytes() == before  # second never loaded or seeded
            if mode == "static":
                assert await _status(wrapper, "/whoami", TOKEN) == 200

    asyncio.run(run())


class _FakeSettings:
    """Settings stand-in that records whether seed/config was ever consulted."""

    allow_unauthenticated = False
    api_keys_store_path = ""
    entra_identities_store_path = ""

    def __init__(self, mode: str, root: Path, store: Path, flag: Path) -> None:
        self.auth_mode = mode
        self.blob_path = str(root / "blobs")
        self._root, self._store, self._flag = root, store, flag

    def stateful_root(self) -> Path:
        return self._root

    def active_identity_store_path(self) -> Path:
        return self._store

    def _touch(self) -> dict[str, str]:
        self._flag.write_text("seed-consulted", encoding="utf-8")
        return {}

    build_keystore = _touch
    build_identity_map = _touch
    build_service_identity_map = _touch

    def resolve_admin_api_key_digest(self) -> None:
        return None


def _child_fake(mode: str, root: str, store: str, flag: str, out: Any) -> None:
    import asyncio as aio

    import context_intelligence_server.main as m
    from context_intelligence_server.identity_store import (
        IdentityStore as Store,
    )

    m._server_lifespan = _no_server_lifespan  # type: ignore[assignment]
    app = FastAPI()
    app.state.settings = _FakeSettings(mode, Path(root), Path(store), Path(flag))
    holder = Store(Path(store), managed=True)
    setattr(
        app.state,
        "api_key_store" if mode == "static" else "entra_identity_store",
        holder,
    )

    async def run() -> None:
        try:
            async with m.lifespan(app):
                task = getattr(app.state, "identity_init_task", None)
                if task is not None:
                    await task
                out.put(("started", getattr(app.state, "identity_state", "unknown")))
        except RuntimeError as exc:
            out.put(("refused", str(exc)))

    aio.run(run())


@pytest.mark.parametrize("mode", ["static", "entra"])
def test_identity_lock_is_taken_before_any_load_or_seed(
    tmp_path: Path, mode: str
) -> None:
    """Different queue roots, one shared store: loser never reads or seeds."""
    store = tmp_path / "shared" / STORE_NAMES[mode]
    holder = IdentityStoreLock(store)
    holder.acquire()
    try:
        flag = tmp_path / "flag"
        ctx = multiprocessing.get_context("spawn")
        out = ctx.Queue()
        child = ctx.Process(
            target=_child_fake,
            args=(mode, str(tmp_path / "root2"), str(store), str(flag), out),
        )
        child.start()
        child.join(timeout=60)
        assert child.exitcode == 0
        status, state = out.get(timeout=5)
        assert (status, state) == ("started", "unavailable")  # listener is up
        assert not flag.exists()  # config/seed was never consulted
        assert not store.exists()  # and nothing was written
    finally:
        holder.release()


@pytest.mark.asyncio
async def test_lifespan_loads_seeds_gates_and_releases(tmp_path: Path) -> None:
    store = tmp_path / "state" / "identity" / "api-keys.json"
    wrapper = _make_app(_settings(tmp_path / "state", "static", store))
    assert not store.exists()  # the factory performs no identity file I/O
    assert not store.with_name("api-keys.json.lock").exists()

    # Pre-startup: even a VALID token is refused (503), not authenticated.
    assert await _status(wrapper, "/whoami", TOKEN) == 503

    async with _serving():
        assert store.exists()
        assert await _status(wrapper, "/whoami", TOKEN) == 200
        assert await _status(wrapper, "/whoami", "wrong") == 401

    assert await _status(wrapper, "/whoami", TOKEN) == 503  # post-shutdown
    # The lock was released: another owner can take it immediately.
    other = IdentityStoreLock(store)
    other.acquire()
    other.release()
    assert main_module.app.state.api_key_store.flat_dict == {}  # cache cleared


@pytest.mark.asyncio
async def test_pre_start_admin_key_and_unauthenticated_shortcut_are_gated(
    tmp_path: Path,
) -> None:
    store = tmp_path / "state" / "api-keys.json"
    settings = _settings(tmp_path / "state", "static", store, admin=True).model_copy(
        update={"allow_unauthenticated": True}
    )
    wrapper = _make_app(settings)
    assert await _status(wrapper, "/admin/keys", ADMIN_TOKEN) == 503
    assert await _status(wrapper, "/whoami") == 503  # no pass-through


@pytest.mark.asyncio
async def test_reentering_the_lifespan_reloads_so_revocations_are_seen(
    tmp_path: Path,
) -> None:
    store_path = tmp_path / "state" / "api-keys.json"
    wrapper = _make_app(_settings(tmp_path / "state", "static", store_path))
    async with _serving():
        assert await _status(wrapper, "/whoami", TOKEN) == 200
    store_path.write_text("{}", encoding="utf-8")  # revoked while stopped
    async with _serving():
        assert await _status(wrapper, "/whoami", TOKEN) == 401


@pytest.mark.asyncio
async def test_server_lifespan_failure_releases_identity_ownership(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    @asynccontextmanager
    async def failing(_app: FastAPI) -> AsyncGenerator[None, None]:
        raise RuntimeError("neo4j unavailable")
        yield  # pragma: no cover

    monkeypatch.setattr(main_module, "_server_lifespan", failing)
    store = tmp_path / "state" / "api-keys.json"
    _make_app(_settings(tmp_path / "state", "static", store))
    with pytest.raises(RuntimeError, match="neo4j unavailable"):
        async with _serving():
            pass
    assert main_module.app.state.identity_lifespan_active is False
    other = IdentityStoreLock(store)
    other.acquire()  # not held
    other.release()


@pytest.mark.asyncio
async def test_cancelled_lifespan_releases_identity_ownership(tmp_path: Path) -> None:
    store = tmp_path / "state" / "api-keys.json"
    _make_app(_settings(tmp_path / "state", "static", store))
    entered = asyncio.Event()

    async def serve() -> None:
        async with _serving():
            entered.set()
            await asyncio.sleep(60)

    task = asyncio.create_task(serve())
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    other = IdentityStoreLock(store)
    other.acquire()
    other.release()


@pytest.mark.asyncio
async def test_factory_refuses_replacement_while_lifespan_owns_the_store(
    tmp_path: Path,
) -> None:
    store = tmp_path / "state" / "api-keys.json"
    wrapper = _make_app(_settings(tmp_path / "state", "static", store))
    async with _serving():
        state = main_module.app.state
        token_before = state.identity_config_token
        store_before = state.api_key_store
        with pytest.raises(RuntimeError, match="cannot replace identity wiring"):
            _make_app(_settings(tmp_path / "other", "static", store))
        # Nothing was mutated by the refused call.
        assert state.identity_config_token is token_before
        assert state.api_key_store is store_before
        assert await _status(wrapper, "/whoami", TOKEN) == 200


@pytest.mark.asyncio
async def test_stale_wrapper_from_an_earlier_factory_call_is_refused(
    tmp_path: Path,
) -> None:
    store = tmp_path / "state" / "api-keys.json"
    stale = _make_app(_settings(tmp_path / "state", "static", store))
    fresh = _make_app(_settings(tmp_path / "state", "static", store))
    async with _serving():
        assert await _status(fresh, "/whoami", TOKEN) == 200
        assert await _status(stale, "/whoami", TOKEN) == 503


@pytest.mark.asyncio
async def test_lost_ownership_disables_auth_and_mutation_until_cleanup(
    tmp_path: Path,
) -> None:
    state_dir = tmp_path / "state"
    store_path = state_dir / "api-keys.json"
    wrapper = _make_app(_settings(state_dir, "static", store_path))
    async with _serving():
        store = main_module.app.state.api_key_store
        assert await _status(wrapper, "/whoami", TOKEN) == 200
        state_dir.rename(tmp_path / "moved")  # directory replaced under us
        state_dir.mkdir()
        with pytest.raises(IdentityStoreOwnershipError):
            store.put("c" * 64, {"id": "late"})
        assert await _status(wrapper, "/whoami", TOKEN) == 503  # auth disabled
        with pytest.raises(IdentityStoreOwnershipError):
            store.delete(DIGEST)  # mutations stay refused
        assert not (state_dir / "api-keys.json").exists()
        assert lock_is_held(tmp_path / "moved" / "api-keys.json.lock")
    assert not lock_is_held(tmp_path / "moved" / "api-keys.json.lock")


def lock_is_held(sidecar: Path) -> bool:
    fd = os.open(sidecar, os.O_RDWR)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        return True
    else:
        fcntl.flock(fd, fcntl.LOCK_UN)
        return False
    finally:
        os.close(fd)


def test_lock_unavailable_is_an_ownership_error() -> None:
    assert issubclass(IdentityStoreLockUnavailable, IdentityStoreOwnershipError)


# ---------------------------------------------------------------------------
# Final security review blockers (PR #126).  Every test below drives the REAL
# factory + wrapper + lifespan (no legacy fixture bypass); this module opts out
# of the autouse ownership harness with ``raw_identity_factory``.
# ---------------------------------------------------------------------------


def _directory_fsync_fault() -> Any:
    """Fail ONLY directory fsyncs: i.e. strictly after the atomic replace."""
    real_fsync = os.fsync

    def fault(fd: int) -> None:
        if stat.S_ISDIR(os.fstat(fd).st_mode):
            raise OSError(errno.EIO, "synthetic directory fsync failure")
        real_fsync(fd)

    return patch("context_intelligence_server.instance_lock.os.fsync", fault)


async def _assert_status_unavailable(wrapper: Any) -> None:
    """/status advertises the latched failure and leaks no credentials."""
    status = await _get_json(wrapper, "/status")
    assert status["identity_ready"] is False
    assert status["identity_reason"] == "store_unavailable"
    rendered = json.dumps(status)
    for secret in (TOKEN, DIGEST, ADMIN_TOKEN, ADMIN_DIGEST):
        assert secret not in rendered


def _status_must_not_probe(monkeypatch: pytest.MonkeyPatch) -> None:
    """After a failure is latched, /status may only read cached state."""

    def boom(*_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("/status probed the store instead of the cached flag")

    monkeypatch.setattr(IdentityStore, "healthy", boom)
    monkeypatch.setattr(IdentityStoreLock, "assert_owned", boom)


def _store_for(mode: str) -> IdentityStore:
    state = main_module.app.state
    return state.api_key_store if mode == "static" else state.entra_identity_store


def _probe_credentials(mode: str) -> tuple[str, str]:
    """Return (store key, bearer token) whose use is observable over HTTP."""
    return (DIGEST, TOKEN) if mode == "static" else (OID, "not-a-jwt")


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["static", "entra"])
async def test_post_replace_fsync_failure_latches_the_store_unavailable(
    tmp_path: Path, mode: str
) -> None:
    """Blocker 1: replace succeeded, durability unknown => nothing is trusted."""
    store_path = tmp_path / "state" / STORE_NAMES[mode]
    wrapper = _make_app(_settings(tmp_path / "state", mode, store_path))
    key, token = _probe_credentials(mode)
    async with _serving():
        store = _store_for(mode)
        assert await _status(wrapper, "/whoami", token) != 503
        with _directory_fsync_fault(), pytest.raises(IdentityStoreOwnershipError):
            store.delete(key)  # the call must NOT report success
        on_disk = json.loads(store_path.read_text(encoding="utf-8"))
        assert key not in on_disk  # the revocation really reached disk
        assert store.healthy() is False
        assert store.flat_dict == {}  # the old credential cache is not served
        assert await _status(wrapper, "/whoami", token) == 503
        before = store_path.read_bytes()
        with pytest.raises(IdentityStoreOwnershipError):
            store.put("c" * 64, {"id": "late"})  # no further mutation
        assert store_path.read_bytes() == before
        assert lock_is_held(store_path.with_name(store_path.name + ".lock"))
    assert not lock_is_held(store_path.with_name(store_path.name + ".lock"))
    # Re-entering the lifespan reloads under the lock and sees the revocation.
    async with _serving():
        store = _store_for(mode)
        assert key not in store.flat_dict
        if mode == "static":
            assert await _status(wrapper, "/whoami", TOKEN) == 401


@pytest.mark.asyncio
async def test_post_replace_fsync_failure_over_http_does_not_claim_success(
    tmp_path: Path,
) -> None:
    store_path = tmp_path / "state" / "api-keys.json"
    wrapper = _make_app(_settings(tmp_path / "state", "static", store_path, admin=True))
    async with _serving():
        transport = httpx.ASGITransport(app=wrapper, raise_app_exceptions=False)
        async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
            with _directory_fsync_fault():
                resp = await c.delete(
                    f"/admin/keys/{DIGEST}",
                    headers={"Authorization": f"Bearer {ADMIN_TOKEN}"},
                )
            assert resp.status_code >= 500  # never a 200 "deleted"
            assert DIGEST not in json.loads(store_path.read_text(encoding="utf-8"))
            # /status reports it NOW, with no protected request in between.
            await _assert_status_unavailable(wrapper)
            after = await c.get("/whoami", headers={"Authorization": f"Bearer {TOKEN}"})
            assert after.status_code == 503


@pytest.mark.asyncio
async def test_failure_before_replace_is_a_certain_rollback_and_does_not_latch(
    tmp_path: Path,
) -> None:
    """Only UNCERTAIN post-replace errors latch; a failed rename changed nothing."""
    store_path = tmp_path / "state" / "api-keys.json"
    wrapper = _make_app(_settings(tmp_path / "state", "static", store_path))
    async with _serving():
        store = _store_for("static")
        before = store_path.read_bytes()
        boom = OSError(errno.EIO, "synthetic replace failure")
        with (
            patch(
                "context_intelligence_server.instance_lock.os.replace", side_effect=boom
            ),
            pytest.raises(OSError) as raised,
        ):
            store.delete(DIGEST)
        assert not isinstance(raised.value, IdentityStoreOwnershipError)
        assert store.healthy() is True
        assert DIGEST in store.flat_dict
        assert store_path.read_bytes() == before
        assert await _status(wrapper, "/whoami", TOKEN) == 200


@pytest.mark.asyncio
async def test_inactive_wrapper_is_usable_only_while_it_is_the_live_generation(
    tmp_path: Path,
) -> None:
    """Every wrapper starts closed; init marks the live one ready; stale is 503."""
    store = tmp_path / "state" / "api-keys.json"
    root = tmp_path / "state"
    stale = _make_app(
        _settings(root, "static", store, seeded=False).model_copy(
            update={"allow_unauthenticated": True}
        )
    )
    assert await _status(stale, "/whoami") == 503  # readiness closed initially
    async with _serving():
        assert await _status(stale, "/whoami") == 200  # init marked it ready
    fresh = _make_app(_settings(root, "static", store))  # config B enforces auth
    assert await _status(stale, "/whoami") == 503  # obsolete generation
    async with _serving():
        assert await _status(stale, "/whoami") == 503  # never admits a request
        assert await _status(stale, "/whoami", TOKEN) == 503
        assert await _status(fresh, "/whoami", TOKEN) == 200
        # Status is public (existing exemption) and reports the CURRENT state.
        assert (await _get_json(stale, "/status"))["identity_ready"] is True
    assert await _status(stale, "/whoami") == 503
    assert await _status(fresh, "/whoami", TOKEN) == 503


@pytest.mark.asyncio
async def test_store_appearing_between_factory_and_startup_is_gated_and_ready(
    tmp_path: Path,
) -> None:
    store_path = tmp_path / "state" / "identity" / "api-keys.json"
    wrapper = _make_app(
        _settings(tmp_path / "state", "static", store_path, seeded=False)
    )
    store_path.parent.mkdir(parents=True)
    store_path.write_text(json.dumps({DIGEST: {"id": "writer"}}), encoding="utf-8")
    # No key file is read before init and readiness is closed: not admitted.
    assert await _status(wrapper, "/whoami", TOKEN) == 503
    async with _serving():
        assert await _status(wrapper, "/whoami", TOKEN) == 200  # selection recomputed
        moved = store_path.parent.with_name("moved-identity")
        store_path.parent.rename(moved)
        assert await _status(wrapper, "/whoami", TOKEN) == 503  # gate installed


@pytest.mark.asyncio
async def test_selected_wrapper_reads_no_key_file_before_the_lifespan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store_path = tmp_path / "state" / "api-keys.json"
    wrapper = _make_app(_settings(tmp_path / "state", "static", store_path))

    def forbidden(*_a: Any, **_k: Any) -> None:
        raise AssertionError("identity file touched before the lifespan")

    monkeypatch.setattr(IdentityStore, "load", forbidden)
    monkeypatch.setattr(IdentityStore, "exists", forbidden)
    assert await _status(wrapper, "/whoami", TOKEN) == 503
    assert not store_path.exists()


@pytest.mark.parametrize("bits", [0o666, 0o644, 0o660])
def test_wide_sidecar_mode_is_not_rejected_by_itself(tmp_path: Path, bits: int) -> None:
    """No mount-specific policy: mode alone never refuses ownership.

    The lock is advisory and relies on trusted storage; a mount that reports
    wide modes (fixed-mode SMB) must still be able to start.  The original
    protections remain: regular file, one link, bound inode, no symlink.
    """
    parent = tmp_path / "id"
    parent.mkdir(mode=0o700)
    sidecar = parent / "api-keys.json.lock"
    sidecar.write_bytes(b"")
    sidecar.chmod(bits)
    lock = IdentityStoreLock(parent / "api-keys.json")
    lock.acquire()
    try:
        assert lock.owned_by_this_process()
        assert stat.S_IMODE(sidecar.stat().st_mode) == bits  # reported, untouched
    finally:
        lock.release()


def test_private_sidecar_mode_0600_is_accepted(tmp_path: Path) -> None:
    parent = tmp_path / "id"
    parent.mkdir(mode=0o700)
    sidecar = parent / "api-keys.json.lock"
    sidecar.write_bytes(b"")
    sidecar.chmod(0o600)
    lock = IdentityStoreLock(parent / "api-keys.json")
    lock.acquire()
    lock.release()


def test_new_sidecar_is_private_even_under_a_permissive_umask(tmp_path: Path) -> None:
    previous = os.umask(0)
    try:
        lock = IdentityStoreLock(tmp_path / "id" / "api-keys.json")
        lock.acquire()
    finally:
        os.umask(previous)
    try:
        assert stat.S_IMODE(lock.path.stat().st_mode) == 0o600
    finally:
        lock.release()


@pytest.mark.asyncio
async def test_parent_alias_retarget_disables_auth_and_writes_nothing(
    tmp_path: Path,
) -> None:
    """Blocker 4: the ORIGINAL configured path must still resolve to the pin."""
    root = tmp_path / "state"
    real_one, real_two, alias = root / "real-one", root / "real-two", root / "alias"
    for directory in (real_one, real_two):
        directory.mkdir(parents=True)
    alias.symlink_to(real_one)
    store_path = alias / "api-keys.json"
    wrapper = _make_app(_settings(root, "static", store_path))
    async with _serving():
        assert await _status(wrapper, "/whoami", TOKEN) == 200
        alias.unlink()
        alias.symlink_to(real_two)
        assert await _status(wrapper, "/whoami", TOKEN) == 503  # not stale-200
        assert _store_for("static").healthy() is False
        with pytest.raises(IdentityStoreOwnershipError):
            _store_for("static").put("d" * 64, {"id": "x"})
        assert list(real_two.iterdir()) == []  # nothing written to the new target
        assert "d" * 64 not in (real_one / "api-keys.json").read_text(encoding="utf-8")


def test_lock_rejects_a_retargeted_parent_alias_directly(tmp_path: Path) -> None:
    one, two, alias = tmp_path / "one", tmp_path / "two", tmp_path / "alias"
    one.mkdir()
    two.mkdir()
    alias.symlink_to(one)
    lock = IdentityStoreLock(alias / "api-keys.json")
    lock.acquire()
    try:
        alias.unlink()
        alias.symlink_to(two)
        with pytest.raises(IdentityStoreOwnershipError, match="configured"):
            lock.assert_owned()
        with pytest.raises(IdentityStoreOwnershipError):
            lock.write_store_atomic(b"{}")
        assert list(two.iterdir()) == [] and not (one / "api-keys.json").exists()
    finally:
        lock.release()


@pytest.mark.asyncio
@pytest.mark.parametrize("recreate", [True, False])
async def test_parent_rename_or_replacement_disables_auth_before_any_mutation(
    tmp_path: Path, recreate: bool
) -> None:
    store_path = tmp_path / "state" / "identity" / "api-keys.json"
    wrapper = _make_app(_settings(tmp_path / "state", "static", store_path))
    async with _serving():
        assert await _status(wrapper, "/whoami", TOKEN) == 200
        store_path.parent.rename(tmp_path / "state" / "old-identity")
        if recreate:
            store_path.parent.mkdir()
        assert await _status(wrapper, "/whoami", TOKEN) == 503
        assert _store_for("static").healthy() is False
        if recreate:
            assert list(store_path.parent.iterdir()) == []


@pytest.mark.asyncio
async def test_sidecar_replacement_disables_auth(tmp_path: Path) -> None:
    """Our own binding assertion is honest about a replaced sidecar.

    Replacing the sidecar needs write access to the store directory, which is
    outside the trusted-storage model: a second server COULD then start on the
    new inode.  What this server guarantees is that IT stops trusting its
    cache the moment its own binding no longer holds.
    """
    store_path = tmp_path / "state" / "api-keys.json"
    wrapper = _make_app(_settings(tmp_path / "state", "static", store_path))
    async with _serving():
        assert await _status(wrapper, "/whoami", TOKEN) == 200
        sidecar = store_path.with_name("api-keys.json.lock")
        sidecar.unlink()
        sidecar.write_bytes(b"")
        sidecar.chmod(0o600)
        assert await _status(wrapper, "/whoami", TOKEN) == 503


@pytest.mark.asyncio
async def test_state_root_replacement_fails_closed_and_keeps_other_owners_locks(
    tmp_path: Path,
) -> None:
    root = tmp_path / "state"
    store_path = root / "api-keys.json"
    wrapper = _make_app(_settings(root, "static", store_path))
    other: IdentityStoreLock | None = None
    try:
        async with _serving():
            root.rename(tmp_path / "moved-state")
            root.mkdir()
            assert await _status(wrapper, "/whoami", TOKEN) == 503
            other = IdentityStoreLock(store_path)  # a new owner of the new root
            other.acquire()
        # Our orderly stop must not have released the OTHER owner's flock.
        assert lock_is_held(root / "api-keys.json.lock")
    finally:
        if other is not None:
            other.release()


# ---------------------------------------------------------------------------
# Startup: the server comes up and advertises identity state (final constraint)
# ---------------------------------------------------------------------------


def _count_init_calls(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    """Wrap the one-shot identity init so a test can prove it ran exactly once."""
    calls: list[int] = []
    real = main_module._start_identity_ownership

    def counted(app: Any, s: Any, **kwargs: Any) -> Any:
        calls.append(1)
        return real(app, s, **kwargs)

    monkeypatch.setattr(main_module, "_start_identity_ownership", counted)
    return calls


async def _wait_for_state(state_name: str, seconds: float = 5.0) -> None:
    for _ in range(int(seconds / 0.01)):
        if getattr(main_module.app.state, "identity_state", None) == state_name:
            return
        await asyncio.sleep(0.01)
    raise AssertionError(f"identity never became {state_name!r}")


def test_factory_does_no_identity_selection_or_file_io(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Construction only builds managed dicts/resolvers; selection is thread-only."""

    def forbidden(self: Settings) -> Path:
        raise AssertionError("factory evaluated the identity selection (file I/O)")

    monkeypatch.setattr(Settings, "active_identity_store_path", forbidden)
    store = tmp_path / "state" / "identity" / "api-keys.json"
    wrapper = _make_app(_settings(tmp_path / "state", "static", store))
    assert wrapper is not None
    assert main_module.app.state.identity_state == "initializing"  # closed initially
    assert not store.parent.exists()


@pytest.mark.asyncio
async def test_lifespan_yields_and_status_answers_while_identity_init_is_delayed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = tmp_path / "state" / "api-keys.json"
    wrapper = _make_app(_settings(tmp_path / "state", "static", store))
    release = threading.Event()
    real = main_module._start_identity_ownership

    def delayed(app: Any, s: Any, **kwargs: Any) -> Any:
        assert release.wait(timeout=5), "identity init was never released"
        return real(app, s, **kwargs)

    monkeypatch.setattr(main_module, "_start_identity_ownership", delayed)
    try:
        async with main_module.lifespan(main_module.app):
            status = await _get_json(wrapper, "/status")
            assert status["identity_ready"] is False
            assert status["identity_reason"] == "initializing"
            assert await _status(wrapper, "/whoami", TOKEN) == 503
            release.set()
            await main_module.app.state.identity_init_task
            status = await _get_json(wrapper, "/status")
            assert status["identity_ready"] is True
            assert status["identity_reason"] is None
            assert await _status(wrapper, "/whoami", TOKEN) == 200
    finally:
        release.set()


# Unit I: each regression names a real await/lifetime boundary, not a mock gate.
async def _until(predicate: Any, seconds: float = 1.0) -> None:
    async with asyncio.timeout(seconds):
        while not predicate():
            await asyncio.sleep(0.001)


def _refuse_identity_replacement(tmp_path: Path) -> None:
    state = main_module.app.state
    before = (
        state.identity_config_token,
        state.api_key_store,
        state.identity_resolver,
        state.identity_runtime,
    )
    with pytest.raises(RuntimeError, match="cannot replace identity wiring"):
        _make_app(_settings(tmp_path / "other", "static", tmp_path / "other.json"))
    after = (
        state.identity_config_token,
        state.api_key_store,
        state.identity_resolver,
        state.identity_runtime,
    )
    assert all(a is b for a, b in zip(before, after, strict=True))


async def test_slow_runtime_ownership_probe_does_not_block_exempt_requests(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    wrapper = _make_app(_settings(tmp_path / "state", "static", tmp_path / "keys.json"))
    async with _serving():
        loop_thread = threading.get_ident()
        store = _store_for("static")
        lock = store._lock
        assert lock is not None
        real = lock.assert_owned
        threads: list[int] = []

        def slow_assert() -> int:
            threads.append(threading.get_ident())
            time.sleep(0.08)  # finite even on the broken inline baseline
            return real()

        monkeypatch.setattr(lock, "assert_owned", slow_assert)
        request = asyncio.create_task(_status(wrapper, "/whoami", TOKEN))
        started = asyncio.get_running_loop().time()
        try:
            await asyncio.sleep(0.01)
            assert await _status(wrapper, "/version") == 200
            assert (await _get_json(wrapper, "/status"))["identity_ready"]
            assert asyncio.get_running_loop().time() - started < 0.06
            assert await request == 200
            assert threads and all(t != loop_thread for t in threads)
        finally:
            await asyncio.gather(request, return_exceptions=True)


@pytest.mark.parametrize("mode", ["static", "entra"])
async def test_concurrent_protected_requests_each_verify_and_succeed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
) -> None:
    monkeypatch.setattr(
        "context_intelligence_server.auth.jwt.decode",
        lambda *a, **k: {
            "tid": TENANT_ID,
            "oid": OID,
            "scp": "access_as_user",
            "roles": [],
        },
    )
    wrapper = _make_app(_settings(tmp_path / "state", mode, tmp_path / "keys.json"))
    async with _serving():
        store = _store_for(mode)
        real = store.healthy
        threads: list[int] = []

        def check() -> bool:
            threads.append(threading.get_ident())
            time.sleep(0.002)
            return real()

        monkeypatch.setattr(store, "healthy", check)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=wrapper), base_url="http://test"
        ) as client:
            tasks = [
                asyncio.create_task(
                    client.get(
                        "/whoami",
                        headers={"Authorization": f"Bearer {TOKEN}"},
                    )
                )
                for _ in range(16)
            ]
            try:
                responses = await asyncio.gather(*tasks)
                assert [r.status_code for r in responses] == [200] * 16
                assert [r.json() for r in responses] == [
                    {"contributor_id": "writer"}
                ] * 16
                assert len(threads) == 16
                assert threading.get_ident() not in threads
            finally:
                await asyncio.gather(*tasks, return_exceptions=True)


async def test_runtime_probe_admission_and_executor_work_are_bounded(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    wrapper = _make_app(_settings(tmp_path / "state", "static", tmp_path / "keys.json"))
    monkeypatch.setattr(
        main_module, "_IDENTITY_PROBE_TIMEOUT_SECONDS", 2.5, raising=False
    )
    release = threading.Event()
    tasks: list[asyncio.Task[Any]] = []
    async with _serving():
        store = _store_for("static")
        real = store.healthy
        calls: list[int] = []
        loop_thread = threading.get_ident()

        def check() -> bool:
            calls.append(threading.get_ident())
            if threading.get_ident() != loop_thread:  # baseline must not deadlock
                assert release.wait(3)
            return real()

        monkeypatch.setattr(store, "healthy", check)
        try:
            tasks = [
                asyncio.create_task(_status(wrapper, "/whoami", TOKEN))
                for _ in range(36)
            ]
            await _until(lambda: len(calls) >= 4)
            await asyncio.sleep(0.02)
            assert len(calls) == 4, "more than four jobs submitted or inline probes"
            runtime = main_module.app.state.identity_runtime
            assert runtime.admitted == 36
            assert (
                await asyncio.wait_for(_status(wrapper, "/whoami", TOKEN), 0.3) == 503
            )
            assert len(calls) == 4
            release.set()
            assert await asyncio.gather(*tasks) == [200] * 36
            assert len(calls) == 36
            assert (
                runtime.admitted == 0
                and not runtime.probes
                and runtime.drained.is_set()
            )
        finally:
            release.set()
            await asyncio.gather(*tasks, return_exceptions=True)


@pytest.mark.parametrize("before_first_step", [False, True])
async def test_direct_cancellation_of_init_observer_retains_real_ownership(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    before_first_step: bool,
) -> None:
    store_path = tmp_path / "keys.json"
    wrapper = _make_app(_settings(tmp_path / "state", "static", store_path))
    release, acquired, finished = (
        threading.Event(),
        threading.Event(),
        threading.Event(),
    )
    real = main_module._initialize_identity_store
    recorded: list[tuple[Any, Any]] = []

    def initialize(app: Any, settings: Any, store: Any, **kwargs: Any) -> None:
        recorded.append((store, store._lock))
        acquired.set()
        try:
            assert release.wait(3)
            real(app, settings, store, **kwargs)
        finally:
            finished.set()

    monkeypatch.setattr(main_module, "_initialize_identity_store", initialize)
    cm = (
        main_module._identity_lifespan(main_module.app, main_module.app.state.settings)
        if before_first_step
        else main_module.lifespan(main_module.app)
    )
    shutdown = None
    await cm.__aenter__()
    observer = main_module.app.state.identity_init_task
    try:
        if not before_first_step:
            await _until(acquired.is_set)
            _refuse_identity_replacement(tmp_path)
        observer.cancel()
        observer.cancel()
        with pytest.raises(asyncio.CancelledError):
            await observer
        await _until(acquired.is_set)
        shutdown = asyncio.create_task(cm.__aexit__(None, None, None))
        await asyncio.sleep(0.02)
        assert not shutdown.done(), "observer cancellation forgot the real initializer"
        assert main_module.app.state.identity_lifespan_active
        _refuse_identity_replacement(tmp_path)
        assert lock_is_held(store_path.with_name("keys.json.lock"))
        if not before_first_step:
            assert lock_is_held(
                tmp_path / "state" / ".context-intelligence-server.lock"
            )
        assert await _status(wrapper, "/whoami", TOKEN) == 503
        assert await _status(wrapper, "/version") == 200
        release.set()
        await asyncio.wait_for(shutdown, 3)
        # Product cleanup, BEFORE the emergency-repair finally (I-COV-1).
        repair_needed = any(
            lock is not None and lock.owned_by_this_process() for _, lock in recorded
        )
        assert not repair_needed
        assert not lock_is_held(store_path.with_name("keys.json.lock"))
        assert all(store.flat_dict == {} for store, _ in recorded)
        assert not main_module.app.state.identity_lifespan_active
        if not before_first_step:
            assert not lock_is_held(
                tmp_path / "state" / ".context-intelligence-server.lock"
            )
    finally:
        release.set()
        await _until(finished.is_set, 3)
        if shutdown is None:
            shutdown = asyncio.create_task(cm.__aexit__(None, None, None))
        await asyncio.gather(shutdown, return_exceptions=True)
        # Repair only a leaked baseline owner, after its actual thread finished.
        for store, lock in recorded:
            if lock is not None and lock.owned_by_this_process():
                main_module._release_identity(store, lock)
    assert not lock_is_held(store_path.with_name("keys.json.lock"))
    assert _store_for("static").flat_dict == {}


async def test_concurrent_ingest_is_not_rejected_for_identity_overlap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from context_intelligence_server.queue_manager import QueueManager

    wrapper = _make_app(_settings(tmp_path / "state", "static", tmp_path / "keys.json"))
    qm = QueueManager(queues_dir=tmp_path / "state" / "queues")
    monkeypatch.setattr(main_module.registry, "_queue_manager", qm)
    monkeypatch.setattr(main_module.registry, "start_drain", lambda worker: None)
    async with _serving():
        real = _store_for("static").healthy
        calls: list[int] = []

        def check() -> bool:
            calls.append(threading.get_ident())
            time.sleep(0.002)
            return real()

        monkeypatch.setattr(_store_for("static"), "healthy", check)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=wrapper), base_url="http://test"
        ) as client:
            tasks = [
                asyncio.create_task(
                    client.post(
                        "/events",
                        headers={"Authorization": f"Bearer {TOKEN}"},
                        json={
                            "event": "session:start",
                            "workspace": "/identity-test",
                            "data": {
                                "session_id": f"identity-ingest-{i}",
                                "timestamp": "2026-01-01T00:00:00Z",
                            },
                        },
                    )
                )
                for i in range(16)
            ]
            try:
                responses = await asyncio.gather(*tasks)
                assert [r.status_code for r in responses] == [202] * 16
                assert len(calls) == 16 and threading.get_ident() not in calls
                for i in range(16):
                    batch = await qm.read_batch(f"identity-ingest-{i}", max_items=2)
                    assert len(batch.records) == 1
                    assert json.loads(batch.records[0].raw)["created_by"] == "writer"
            finally:
                await asyncio.gather(*tasks, return_exceptions=True)
                main_module.registry._workers.clear()


async def test_queued_probe_checks_ownership_after_admission(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store_path = tmp_path / "keys.json"
    wrapper = _make_app(_settings(tmp_path / "state", "static", store_path))
    release = threading.Event()
    tasks: list[asyncio.Task[Any]] = []
    async with _serving():
        store = _store_for("static")
        real = store.healthy
        checked: list[bool] = []

        def check() -> bool:
            result = real()
            checked.append(result)
            if len(checked) <= 4:
                assert release.wait(3)  # deliberately OUTSIDE the real store mutex
            return result

        monkeypatch.setattr(store, "healthy", check)
        try:
            tasks = [
                asyncio.create_task(_status(wrapper, "/whoami", TOKEN))
                for _ in range(4)
            ]
            await _until(lambda: len(checked) == 4)
            tasks.append(asyncio.create_task(_status(wrapper, "/whoami", TOKEN)))
            await _until(lambda: main_module.app.state.identity_runtime.admitted == 5)
            sidecar = store_path.with_name("keys.json.lock")
            sidecar.unlink()
            sidecar.write_bytes(b"")
            release.set()
            assert (await asyncio.gather(*tasks))[-1] == 503
            assert len(checked) == 5 and checked[-1] is False
            assert store.flat_dict == {}
            # Cached status must not re-enter even a healthy() wrapper.
            monkeypatch.setattr(store, "healthy", lambda: pytest.fail("status probed"))
            assert not (await _get_json(wrapper, "/status"))["identity_ready"]
        finally:
            release.set()
            await asyncio.gather(*tasks, return_exceptions=True)


async def test_queue_timeout_and_cancel_do_not_submit_later(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    wrapper = _make_app(_settings(tmp_path / "state", "static", tmp_path / "keys.json"))
    monkeypatch.setattr(main_module, "_IDENTITY_PROBE_TIMEOUT_SECONDS", 2.0)
    release = threading.Event()
    tasks: list[asyncio.Task[Any]] = []
    async with _serving():
        real = _store_for("static").healthy
        calls: list[int] = []

        def check() -> bool:
            calls.append(1)
            assert release.wait(3)
            return real()

        monkeypatch.setattr(_store_for("static"), "healthy", check)
        try:
            tasks = [
                asyncio.create_task(_status(wrapper, "/whoami", TOKEN))
                for _ in range(4)
            ]
            await _until(lambda: len(calls) == 4)
            runtime = main_module.app.state.identity_runtime
            monkeypatch.setattr(main_module, "_IDENTITY_PROBE_TIMEOUT_SECONDS", 0.05)
            timed = asyncio.create_task(_status(wrapper, "/whoami", TOKEN))
            cancelled = asyncio.create_task(_status(wrapper, "/whoami", TOKEN))
            tasks.extend([timed, cancelled])
            await _until(lambda: runtime.admitted == 6)
            cancelled.cancel()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(cancelled, 0.2)
            assert await asyncio.wait_for(timed, 0.3) == 503
            assert runtime.admitted == 4 and len(calls) == 4
            release.set()
            assert await asyncio.gather(*tasks[:4]) == [200] * 4
            assert runtime.admitted == 0 and len(calls) == 4
            assert await _status(wrapper, "/whoami", TOKEN) == 200
            assert len(calls) == 5
        finally:
            release.set()
            await asyncio.gather(*tasks, return_exceptions=True)


async def test_probe_cancel_at_slot_handoff_returns_capacity_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    wrapper = _make_app(_settings(tmp_path / "state", "static", tmp_path / "keys.json"))
    async with _serving():
        runtime = main_module.app.state.identity_runtime
        target: list[asyncio.Task[Any]] = []

        class HandoffSemaphore(asyncio.Semaphore):
            def release(self) -> None:
                super().release()
                if target:
                    target.pop().cancel()  # granted, but acquire hasn't resumed

        slots = HandoffSemaphore(4)
        runtime.slots = slots
        for _ in range(4):
            await slots.acquire()
        task = asyncio.create_task(_status(wrapper, "/whoami", TOKEN))
        try:
            await _until(lambda: runtime.admitted == 1)
            target.append(task)
            slots.release()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert runtime.admitted == 0 and not runtime.probes
            # Three held tokens plus the returned fourth: no under/over release.
            for _ in range(3):
                slots.release()
            for _ in range(4):
                await asyncio.wait_for(slots.acquire(), 0.2)
            assert slots.locked()
            for _ in range(4):
                slots.release()
            assert await _status(wrapper, "/whoami", TOKEN) == 200
        finally:
            await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize("outcome", ["healthy", "false", "error"])
async def test_submitted_probe_timeout_and_cancel_retain_real_capacity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    outcome: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    wrapper = _make_app(_settings(tmp_path / "state", "static", tmp_path / "keys.json"))
    monkeypatch.setattr(main_module, "_IDENTITY_PROBE_TIMEOUT_SECONDS", 0.05)
    release = threading.Event()
    tasks: list[asyncio.Task[Any]] = []
    errors: list[Any] = []
    loop = asyncio.get_running_loop()
    old_handler = loop.get_exception_handler()
    loop.set_exception_handler(lambda _loop, context: errors.append(context))
    try:
        async with _serving():
            store = _store_for("static")
            real = store.healthy
            calls: list[int] = []

            def check() -> bool:
                calls.append(1)
                assert release.wait(3)
                if outcome == "error":
                    raise RuntimeError("synthetic probe error")
                return real() if outcome == "healthy" else False

            monkeypatch.setattr(store, "healthy", check)
            try:
                tasks = [
                    asyncio.create_task(_status(wrapper, "/whoami", TOKEN))
                    for _ in range(4)
                ]
                await _until(lambda: len(calls) == 4)
                tasks[0].cancel()
                with pytest.raises(asyncio.CancelledError):
                    await asyncio.wait_for(tasks[0], 0.2)
                assert await asyncio.gather(*tasks[1:]) == [503] * 3
                runtime = main_module.app.state.identity_runtime
                assert len(runtime.probes) == runtime.admitted == 4
                monkeypatch.setattr(main_module, "_IDENTITY_PROBE_TIMEOUT_SECONDS", 0.5)
                later = [
                    asyncio.create_task(_status(wrapper, "/whoami", TOKEN))
                    for _ in range(32)
                ]
                tasks.extend(later)
                await _until(lambda: runtime.admitted == 36)
                assert len(calls) == 4
                assert (
                    await asyncio.wait_for(_status(wrapper, "/whoami", TOKEN), 0.2)
                    == 503
                )
                assert runtime.admitted == 36 and len(calls) == 4
                release.set()
                assert (
                    await asyncio.gather(*later)
                    == [200 if outcome == "healthy" else 503] * 32
                )
                await _until(lambda: runtime.admitted == 0)
                assert not runtime.probes
                if outcome == "healthy":
                    assert len(calls) == 36
                else:
                    assert not (await _get_json(wrapper, "/status"))["identity_ready"]
                gc.collect()
                await asyncio.sleep(0)
                assert errors == []
                assert TOKEN not in caplog.text and DIGEST not in caplog.text
            finally:
                release.set()
                await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        loop.set_exception_handler(old_handler)


@pytest.mark.parametrize(
    "mode,uncertain", [("static", False), ("entra", False), ("static", True)]
)
async def test_revocation_during_awaited_probe_uses_live_map(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
    uncertain: bool,
) -> None:
    monkeypatch.setattr(
        "context_intelligence_server.auth.jwt.decode",
        lambda *a, **k: {
            "tid": TENANT_ID,
            "oid": OID,
            "scp": "access_as_user",
            "roles": [],
        },
    )
    wrapper = _make_app(_settings(tmp_path / "state", mode, tmp_path / "keys.json"))
    release, checked = threading.Event(), threading.Event()
    async with _serving():
        store = _store_for(mode)
        real = store.healthy

        def check() -> bool:
            result = real()
            checked.set()
            assert release.wait(3)
            return result

        monkeypatch.setattr(store, "healthy", check)
        task = asyncio.create_task(_status(wrapper, "/whoami", TOKEN))
        try:
            await _until(checked.is_set)
            if uncertain:
                # Failure after replace, before directory fsync: committed file,
                # untrustworthy cache. Exercise the real mutation's latch.
                real_fsync = os.fsync

                def fail_fsync(fd: int) -> None:
                    if stat.S_ISDIR(os.fstat(fd).st_mode):
                        raise OSError("synthetic uncertain commit")
                    real_fsync(fd)

                monkeypatch.setattr(os, "fsync", fail_fsync)
                with pytest.raises(OSError):
                    await asyncio.to_thread(store.delete, DIGEST)
            else:
                await asyncio.to_thread(
                    store.delete, DIGEST if mode == "static" else OID
                )
            release.set()
            assert await task == (
                503 if uncertain else 401 if mode == "static" else 403
            )
        finally:
            release.set()
            await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize("reenter", [False, True])
@pytest.mark.parametrize("shortcut", ["data", "admin", "dev"])
async def test_old_probe_cannot_authenticate_after_orderly_lifespan_reentry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    reenter: bool,
    shortcut: str,
) -> None:
    settings = _settings(
        tmp_path / "state",
        "static",
        tmp_path / "keys.json",
        seeded=shortcut != "dev",
        admin=shortcut == "admin",
    )
    if shortcut == "dev":
        (tmp_path / "keys.json").write_text("{}", encoding="utf-8")
        settings = settings.model_copy(update={"allow_unauthenticated": True})
    wrapper = _make_app(settings)
    path = "/admin/keys" if shortcut == "admin" else "/whoami"
    bearer = ADMIN_TOKEN if shortcut == "admin" else TOKEN
    real_wait = asyncio.wait
    parked, resume = asyncio.Event(), asyncio.Event()
    target: list[asyncio.Task[Any]] = []

    async def defer_wait(*args: Any, **kwargs: Any) -> Any:
        result = await real_wait(*args, **kwargs)
        if target and asyncio.current_task() is target[0]:
            parked.set()
            await resume.wait()
        return result

    monkeypatch.setattr(asyncio, "wait", defer_wait)
    cm = _serving()
    await cm.__aenter__()
    old_runtime = main_module.app.state.identity_runtime
    task = asyncio.create_task(_status(wrapper, path, bearer))
    target.append(task)
    try:
        await asyncio.wait_for(parked.wait(), 0.5)
        await _until(lambda: old_runtime.admitted == 0)
        await cm.__aexit__(None, None, None)
        if reenter:
            cm = _serving()
            await cm.__aenter__()
            assert main_module.app.state.identity_runtime is not old_runtime
            assert await _status(wrapper, path, bearer) == 200
        resume.set()
        assert await task == 503
    finally:
        resume.set()
        await asyncio.gather(task, return_exceptions=True)
        if main_module.app.state.identity_lifespan_active:
            await cm.__aexit__(None, None, None)


async def test_shutdown_waits_for_all_real_probes_and_queued_admission(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Repeated cancellation cannot skip probe drain or off-thread mutex stop."""
    store_path = tmp_path / "keys.json"
    root = tmp_path / "state"
    wrapper = _make_app(_settings(root, "static", store_path))
    monkeypatch.setattr(main_module, "_IDENTITY_PROBE_TIMEOUT_SECONDS", 2.0)
    probe_release = threading.Event()
    mutex_release = threading.Event()
    mutex_entered = threading.Event()
    stop_entered = threading.Event()
    trace: list[str] = []
    tasks: list[asyncio.Task[Any]] = []
    holder = shutdown = None
    cm = _serving()
    await cm.__aenter__()
    runtime = main_module.app.state.identity_runtime
    store = runtime.store
    real_check, real_stop = store.healthy, store.stop
    real_identity_release = IdentityStoreLock.release
    real_root_release = main_module.ServerInstanceLock.release
    calls: list[int] = []
    stop_threads: list[int] = []

    def check() -> bool:
        result = real_check()
        calls.append(1)
        assert probe_release.wait(3)
        trace.append("probe-finished")
        return result

    def hold_mutex() -> None:
        with store._op_lock:
            mutex_entered.set()
            assert mutex_release.wait(3)

    def stop() -> None:
        stop_threads.append(threading.get_ident())
        trace.append("stop-start")
        stop_entered.set()
        real_stop()
        trace.append("stop-finished")

    def identity_release(lock: Any) -> None:
        trace.append("identity-release")
        real_identity_release(lock)

    def root_release(lock: Any) -> None:
        trace.append("root-release")
        real_root_release(lock)

    monkeypatch.setattr(store, "healthy", check)
    monkeypatch.setattr(store, "stop", stop)
    monkeypatch.setattr(IdentityStoreLock, "release", identity_release)
    monkeypatch.setattr(main_module.ServerInstanceLock, "release", root_release)
    try:
        tasks = [
            asyncio.create_task(_status(wrapper, "/whoami", TOKEN)) for _ in range(4)
        ]
        await _until(lambda: len(calls) == 4)
        _refuse_identity_replacement(tmp_path)  # active probes
        tasks.extend(
            asyncio.create_task(_status(wrapper, "/whoami", TOKEN)) for _ in range(8)
        )
        await _until(lambda: runtime.admitted == 12)
        holder = asyncio.create_task(asyncio.to_thread(hold_mutex))
        await _until(mutex_entered.is_set)
        shutdown = asyncio.create_task(cm.__aexit__(None, None, None))
        await _until(lambda: runtime.closing)
        for _ in range(3):
            shutdown.cancel()
            await asyncio.sleep(0)
            assert not shutdown.done()
            assert main_module.app.state.identity_lifespan_active
            assert lock_is_held(root / ".context-intelligence-server.lock")
            assert lock_is_held(store_path.with_name("keys.json.lock"))
        assert trace == [] and len(calls) == 4
        probe_release.set()
        await _until(stop_entered.is_set)
        assert trace[:4] == ["probe-finished"] * 4
        assert trace[4:] == ["stop-start"]
        assert runtime.admitted == 0 and not runtime.probes
        _refuse_identity_replacement(tmp_path)  # cleanup is still outstanding
        for _ in range(3):
            shutdown.cancel()
            await asyncio.sleep(0)
            assert not shutdown.done()
        started = asyncio.get_running_loop().time()
        await asyncio.sleep(0.01)
        assert await asyncio.wait_for(_status(wrapper, "/version"), 0.3) == 200
        assert not (await asyncio.wait_for(_get_json(wrapper, "/status"), 0.3))[
            "identity_ready"
        ]
        assert asyncio.get_running_loop().time() - started < 0.5
        assert stop_threads == [stop_threads[0]]
        assert stop_threads[0] != threading.get_ident()
        assert lock_is_held(store_path.with_name("keys.json.lock"))
        assert lock_is_held(root / ".context-intelligence-server.lock")
        assert await asyncio.gather(*tasks) == [503] * 12
        mutex_release.set()
        await holder
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(shutdown, 1)
        assert trace == ["probe-finished"] * 4 + [
            "stop-start",
            "stop-finished",
            "identity-release",
            "root-release",
        ]
        assert not main_module.app.state.identity_lifespan_active
        assert not lock_is_held(store_path.with_name("keys.json.lock"))
        assert not lock_is_held(root / ".context-intelligence-server.lock")
        assert store.flat_dict == {}
        fresh = _make_app(_settings(root, "static", store_path))  # now LEGAL
        async with _serving():
            assert await _status(wrapper, "/whoami", TOKEN) == 503
            assert await _status(fresh, "/whoami", TOKEN) == 200
            # Completed old-context publication cannot poison the new generation.
            main_module._set_identity_state(
                main_module.app, "unavailable", runtime=runtime
            )
            main_module._set_identity_state(main_module.app, "ready", runtime=runtime)
            assert await _status(fresh, "/whoami", TOKEN) == 200
    finally:
        probe_release.set()
        mutex_release.set()
        if holder is not None:
            await asyncio.gather(holder, return_exceptions=True)
        if shutdown is None:
            shutdown = asyncio.create_task(cm.__aexit__(None, None, None))
        await asyncio.gather(shutdown, *tasks, return_exceptions=True)


async def test_init_exception_after_observer_cancel_is_observed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    wrapper = _make_app(_settings(tmp_path / "state", "static", tmp_path / "keys.json"))
    entered, release = threading.Event(), threading.Event()
    errors: list[Any] = []
    loop = asyncio.get_running_loop()
    old_handler = loop.get_exception_handler()
    loop.set_exception_handler(lambda _loop, context: errors.append(context))

    def broken(app: Any, s: Any, store: Any, **kwargs: Any) -> None:
        entered.set()
        assert release.wait(3)
        raise RuntimeError("synthetic init programming error")

    monkeypatch.setattr(main_module, "_initialize_identity_store", broken)
    try:
        async with main_module.lifespan(main_module.app):
            await _until(entered.is_set)
            observer = main_module.app.state.identity_init_task
            observer.cancel()
            with pytest.raises(asyncio.CancelledError):
                await observer
            release.set()
            await _until(lambda: main_module.app.state.identity_runtime.init_io.done())
            assert await _status(wrapper, "/whoami", TOKEN) == 503
        gc.collect()
        await asyncio.sleep(0)
        assert not errors
        assert caplog.text.count("identity initialization failed unexpectedly") == 1
    finally:
        release.set()
        loop.set_exception_handler(old_handler)


async def test_identity_init_submission_failure_clears_active(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _make_app(_settings(tmp_path / "state", "static", tmp_path / "keys.json"))
    loop = asyncio.get_running_loop()
    real = loop.run_in_executor
    submitted: list[Any] = []

    def submit(executor: Any, func: Any, *args: Any) -> Any:
        submitted.append(func)
        if len(submitted) == 1:
            raise RuntimeError("synthetic executor submission error")
        return real(executor, func, *args)

    monkeypatch.setattr(loop, "run_in_executor", submit)
    with pytest.raises(RuntimeError, match="synthetic executor submission"):
        async with main_module._identity_lifespan(
            main_module.app, main_module.app.state.settings
        ):
            pytest.fail("failed submission must not yield")
    assert not main_module.app.state.identity_lifespan_active
    assert main_module.app.state.identity_state == "unavailable"
    assert not _store_for("static").flat_dict


async def test_probe_deadline_includes_queue_and_execution(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    wrapper = _make_app(_settings(tmp_path / "state", "static", tmp_path / "keys.json"))
    release_first, release_later = threading.Event(), threading.Event()
    calls: list[int] = []
    tasks: list[asyncio.Task[Any]] = []
    monkeypatch.setattr(main_module, "_IDENTITY_PROBE_TIMEOUT_SECONDS", 2.0)
    async with _serving():
        store = _store_for("static")
        real = store.healthy

        def check() -> bool:
            calls.append(1)
            barrier = release_first if len(calls) <= 4 else release_later
            assert barrier.wait(3)
            return real()

        monkeypatch.setattr(store, "healthy", check)
        try:
            tasks = [
                asyncio.create_task(_status(wrapper, "/whoami", TOKEN))
                for _ in range(4)
            ]
            await _until(lambda: len(calls) == 4)
            monkeypatch.setattr(main_module, "_IDENTITY_PROBE_TIMEOUT_SECONDS", 0.2)
            started = asyncio.get_running_loop().time()
            later = asyncio.create_task(_status(wrapper, "/whoami", TOKEN))
            tasks.append(later)
            await _until(lambda: main_module.app.state.identity_runtime.admitted == 5)
            await asyncio.sleep(0.13)  # most of the single budget is spent queued
            release_first.set()
            await _until(lambda: len(calls) == 5)
            assert await asyncio.wait_for(later, 0.14) == 503
            assert asyncio.get_running_loop().time() - started < 0.3
            assert main_module.app.state.identity_runtime.admitted == 1
            release_later.set()
            await _until(lambda: main_module.app.state.identity_runtime.admitted == 0)
        finally:
            release_first.set()
            release_later.set()
            await asyncio.gather(*tasks, return_exceptions=True)


async def test_probe_and_mutation_share_operation_mutex_but_status_never_waits(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    wrapper = _make_app(_settings(tmp_path / "state", "static", tmp_path / "keys.json"))
    entered, release = threading.Event(), threading.Event()
    checked = threading.Event()
    async with _serving():
        store = _store_for("static")
        assert store._lock is not None
        real = store._lock.assert_owned

        def assert_owned() -> int:
            checked.set()
            return real()

        def mutation() -> None:
            with store._op_lock:
                entered.set()
                assert release.wait(3)
                store.delete(DIGEST)

        monkeypatch.setattr(store._lock, "assert_owned", assert_owned)
        holder = asyncio.create_task(asyncio.to_thread(mutation))
        task = None
        try:
            await _until(entered.is_set)
            task = asyncio.create_task(_status(wrapper, "/whoami", TOKEN))
            await _until(
                lambda: len(main_module.app.state.identity_runtime.probes) == 1
            )
            assert not checked.is_set()
            assert (await _get_json(wrapper, "/status"))["identity_ready"]
            assert not task.done()
            release.set()
            await holder
            assert await task == 401
            assert checked.is_set()
        finally:
            release.set()
            await asyncio.gather(
                holder, *([task] if task is not None else []), return_exceptions=True
            )


@pytest.mark.asyncio
async def test_lock_contention_leaves_the_server_up_with_identity_unavailable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store_path = tmp_path / "state" / "api-keys.json"
    holder = IdentityStoreLock(store_path)
    holder.acquire()
    wrapper = _make_app(_settings(tmp_path / "state", "static", store_path, admin=True))
    calls = _count_init_calls(monkeypatch)
    try:
        async with main_module.lifespan(main_module.app):
            await main_module.app.state.identity_init_task
            status = await _get_json(wrapper, "/status")  # listener is up
            assert status["identity_ready"] is False
            assert status["identity_reason"] == "store_unavailable"
            assert await _status(wrapper, "/whoami", TOKEN) == 503
            transport = httpx.ASGITransport(app=wrapper, raise_app_exceptions=False)
            async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
                admin = await c.delete(
                    f"/admin/keys/{DIGEST}",
                    headers={"Authorization": f"Bearer {ADMIN_TOKEN}"},
                )
            assert admin.status_code == 503  # mutation unavailable, not a crash
            await asyncio.sleep(0.3)  # no retry / heal loop
            assert calls == [1]
            assert not store_path.exists()  # no load, no seed
            assert main_module.app.state.api_key_store.flat_dict == {}
    finally:
        holder.release()
    assert main_module.app.state.identity_lifespan_active is False


@pytest.mark.asyncio
async def test_permission_error_during_init_leaves_identity_unavailable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = tmp_path / "state" / "api-keys.json"
    wrapper = _make_app(_settings(tmp_path / "state", "static", store))

    def denied(self: IdentityStoreLock) -> None:
        raise PermissionError(errno.EACCES, "synthetic permission denied")

    monkeypatch.setattr(IdentityStoreLock, "acquire", denied)
    calls = _count_init_calls(monkeypatch)
    async with main_module.lifespan(main_module.app):
        await main_module.app.state.identity_init_task
        assert (await _get_json(wrapper, "/status"))["identity_reason"] == (
            "store_unavailable"
        )
        assert await _status(wrapper, "/whoami", TOKEN) == 503
        assert calls == [1]


@pytest.mark.asyncio
async def test_selection_value_error_leaves_identity_unavailable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    blocker = tmp_path / "blocker"
    blocker.write_text("x", encoding="utf-8")
    settings = _settings(tmp_path / "state", "static", blocker / "k.json", seeded=False)
    wrapper = _make_app(settings)
    calls = _count_init_calls(monkeypatch)
    async with main_module.lifespan(main_module.app):  # boots; not refused
        await main_module.app.state.identity_init_task
        assert (await _get_json(wrapper, "/status"))["identity_ready"] is False
        assert await _status(wrapper, "/whoami", TOKEN) == 503
        assert calls == [1]


@pytest.mark.asyncio
async def test_unexpected_init_error_is_not_swallowed_and_is_always_observed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A programming error is not blanket-caught, yet fails closed and is observed."""
    store_path = tmp_path / "state" / "api-keys.json"
    wrapper = _make_app(_settings(tmp_path / "state", "static", store_path))

    def programming_error(*_a: Any, **_k: Any) -> None:
        raise KeyError("synthetic programming error")

    monkeypatch.setattr(main_module, "_initialize_identity_store", programming_error)
    loop = asyncio.get_running_loop()
    unobserved: list[dict[str, Any]] = []
    loop.set_exception_handler(lambda _loop, context: unobserved.append(context))
    try:
        async with main_module.lifespan(main_module.app):
            await _wait_for_state("unavailable")  # done-callback recorded it
            assert await _status(wrapper, "/whoami", TOKEN) == 503
            # The partially taken lock was released, not leaked.
            sidecar = store_path.with_name("api-keys.json.lock")
            assert not sidecar.exists() or not lock_is_held(sidecar)
        main_module.app.state.identity_init_task = None
        gc.collect()
        await asyncio.sleep(0)
        assert unobserved == []  # no "Task exception was never retrieved"
    finally:
        loop.set_exception_handler(None)


@pytest.mark.asyncio
async def test_shutdown_waits_for_active_init_io_before_releasing_the_lock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store_path = tmp_path / "state" / "api-keys.json"
    wrapper = _make_app(_settings(tmp_path / "state", "static", store_path))
    entered, release = threading.Event(), threading.Event()
    real = main_module._initialize_identity_store

    def slow(app: Any, s: Any, store: Any, *, owned: bool) -> None:
        entered.set()
        assert release.wait(timeout=5), "init I/O was never released"
        real(app, s, store, owned=owned)

    monkeypatch.setattr(main_module, "_initialize_identity_store", slow)
    sidecar = store_path.with_name("api-keys.json.lock")
    manager = main_module.lifespan(main_module.app)
    await manager.__aenter__()  # must not wait for the blocked init
    for _ in range(500):
        if entered.is_set():
            break
        await asyncio.sleep(0.01)
    assert entered.is_set() and lock_is_held(sidecar)
    leaving = asyncio.create_task(manager.__aexit__(None, None, None))
    await asyncio.sleep(0.3)
    assert not leaving.done()  # settling the worker thread (cannot be killed)
    assert lock_is_held(sidecar)  # NOT released while I/O is active
    assert await _status(wrapper, "/whoami", TOKEN) == 503  # readiness closed first
    release.set()
    await leaving
    assert not lock_is_held(sidecar)  # then identity, then root, are released
    assert main_module.app.state.api_key_store.flat_dict == {}


@pytest.mark.asyncio
async def test_exempt_paths_are_served_before_the_identity_gate_does_any_io(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = tmp_path / "state" / "api-keys.json"
    wrapper = _make_app(_settings(tmp_path / "state", "static", store))
    async with _serving():
        probes: list[int] = []
        monkeypatch.setattr(
            IdentityStore, "healthy", lambda self: probes.append(1) or True
        )
        assert await _status(wrapper, "/version") == 200
        assert (await _get_json(wrapper, "/status"))["identity_ready"] is True
        assert probes == []  # health/status never touch the filesystem
        assert await _status(wrapper, "/whoami", TOKEN) == 200
        assert probes  # a protected request does


@pytest.mark.asyncio
async def test_successful_init_with_an_empty_map_is_ready_with_normal_401(
    tmp_path: Path,
) -> None:
    store = tmp_path / "state" / "api-keys.json"
    wrapper = _make_app(
        _settings(tmp_path / "state", "static", store, seeded=False, admin=True)
    )
    async with _serving():
        status = await _get_json(wrapper, "/status")
        assert status["identity_ready"] is True and status["identity_reason"] is None
        assert await _status(wrapper, "/whoami", "unknown-token") == 401  # onboarding


# ---------------------------------------------------------------------------
# r4332085777 / r4332085805: managed store I/O failures never become an empty
# or memory-only credential map.  Corrupt CONTENT stays a loud fail-closed empty
# map (control); only ordinary I/O errors on the managed path propagate, so the
# one-shot identity init latches "unavailable" (listener and /status stay up).
# ---------------------------------------------------------------------------


def _managed_read_fault(store_name: str) -> Any:
    """Fail ONLY the read-only open of the store file (the managed read)."""
    real_open = os.open

    def fault(path: Any, flags: int, *args: Any, **kwargs: Any) -> int:
        if path == store_name and (flags & os.O_ACCMODE) == os.O_RDONLY:
            raise OSError(errno.EIO, "synthetic managed read failure")
        return real_open(path, flags, *args, **kwargs)

    return patch("context_intelligence_server.instance_lock.os.open", fault)


def _pre_replace_write_fault(kind: str) -> Any:
    """Fail the managed seed write strictly BEFORE the atomic replace."""
    real_open, real_fsync = os.open, os.fsync

    def open_fault(path: Any, flags: int, *args: Any, **kwargs: Any) -> int:
        if kind == "enospc_create" and flags & os.O_CREAT and ".tmp" in str(path):
            raise OSError(errno.ENOSPC, "synthetic no space left on device")
        return real_open(path, flags, *args, **kwargs)

    def fsync_fault(fd: int) -> None:
        if kind == "fsync" and not stat.S_ISDIR(os.fstat(fd).st_mode):
            raise OSError(errno.EIO, "synthetic pre-replace fsync failure")
        real_fsync(fd)

    stack = patch("context_intelligence_server.instance_lock.os.open", open_fault)
    stack2 = patch("context_intelligence_server.instance_lock.os.fsync", fsync_fault)

    @asynccontextmanager
    async def both() -> AsyncGenerator[None, None]:
        with stack, stack2:
            yield

    return both()


async def _assert_protected_503(wrapper: Any, mode: str) -> None:
    """The identity gate runs before auth, so any bearer token answers 503."""
    _key, token = _probe_credentials(mode)
    assert await _status(wrapper, "/whoami", token) == 503
    path = f"/admin/keys/{DIGEST}" if mode == "static" else f"/admin/identities/{OID}"
    transport = httpx.ASGITransport(app=wrapper, raise_app_exceptions=False)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        admin = await c.delete(path, headers={"Authorization": f"Bearer {ADMIN_TOKEN}"})
    assert admin.status_code == 503


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["static", "entra"])
async def test_managed_read_error_leaves_identity_unavailable_and_file_untouched(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mode: str
) -> None:
    """An EIO reading an EXISTING managed store is not 'no credentials'."""
    state = tmp_path / "state"
    state.mkdir()
    store_path = state / STORE_NAMES[mode]
    key, _token = _probe_credentials(mode)
    store_path.write_text(json.dumps({key: {"id": "writer"}}), encoding="utf-8")
    before = store_path.read_bytes()
    wrapper = _make_app(_settings(state, mode, store_path, admin=True))
    calls = _count_init_calls(monkeypatch)
    with _managed_read_fault(store_path.name):
        async with main_module.lifespan(main_module.app):
            await main_module.app.state.identity_init_task
            status = await _get_json(wrapper, "/status")  # listener stays up
            assert status["identity_ready"] is False
            assert status["identity_reason"] == "store_unavailable"
            assert _store_for(mode).flat_dict == {}
            await _assert_protected_503(wrapper, mode)
            await asyncio.sleep(0.2)  # no retry / heal loop
            assert calls == [1]
    assert store_path.read_bytes() == before  # never overwritten from empty


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["static", "entra"])
async def test_corrupt_json_store_is_still_a_ready_loud_empty_map(
    tmp_path: Path, mode: str
) -> None:
    """Control: corrupt CONTENT keeps the documented fail-closed empty map."""
    state = tmp_path / "state"
    state.mkdir()
    store_path = state / STORE_NAMES[mode]
    store_path.write_text("{not json", encoding="utf-8")
    wrapper = _make_app(_settings(state, mode, store_path))
    async with _serving():
        status = await _get_json(wrapper, "/status")
        assert status["identity_ready"] is True
        assert _store_for(mode).flat_dict == {}
    assert store_path.read_text(encoding="utf-8") == "{not json"


def test_managed_load_error_clears_the_cache_and_propagates(tmp_path: Path) -> None:
    store_path = tmp_path / "id" / "api-keys.json"
    lock = IdentityStoreLock(store_path)
    lock.acquire()
    store = IdentityStore(store_path, managed=True)
    store.attach(lock)
    try:
        store.seed({DIGEST: {"id": "writer"}})
        assert store.flat_dict == {DIGEST: "writer"}
        with _managed_read_fault(store_path.name), pytest.raises(OSError):
            store.load()
        assert store.flat_dict == {}  # a stale cache is never served after EIO
    finally:
        store.stop()
        lock.release()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["enospc_create", "fsync"])
@pytest.mark.parametrize("mode", ["static", "entra"])
async def test_managed_seed_write_failure_leaves_identity_unavailable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mode: str, kind: str
) -> None:
    """A seed that cannot be persisted is not served from memory only."""
    state = tmp_path / "state"
    store_path = state / STORE_NAMES[mode]
    wrapper = _make_app(_settings(state, mode, store_path, admin=True))
    calls = _count_init_calls(monkeypatch)
    async with (
        _pre_replace_write_fault(kind),
        main_module.lifespan(main_module.app),
    ):
        await main_module.app.state.identity_init_task
        status = await _get_json(wrapper, "/status")  # listener stays up
        assert status["identity_ready"] is False
        assert status["identity_reason"] == "store_unavailable"
        assert _store_for(mode).flat_dict == {}  # not live in memory
        await _assert_protected_503(wrapper, mode)
        await asyncio.sleep(0.2)  # one write attempt, no retry
        assert calls == [1]
    assert not store_path.exists()  # no credential file was left behind
    assert [p.name for p in state.glob(".*.tmp")] == []  # no torn temp file


# ---------------------------------------------------------------------------
# r4335610828: a failure the store has LATCHED at runtime (ownership loss or an
# uncertain commit) is published to the cached /status readiness.  /status reads
# the already-latched flag; it never probes the store.  Nothing is predicted:
# before any failure is detected, status stays ready.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["static", "entra"])
async def test_ownership_loss_seen_by_the_gate_is_published_to_status(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mode: str
) -> None:
    store_path = tmp_path / "state" / STORE_NAMES[mode]
    wrapper = _make_app(_settings(tmp_path / "state", mode, store_path))
    sidecar = store_path.with_name(store_path.name + ".lock")
    _key, token = _probe_credentials(mode)
    async with _serving():
        status = await _get_json(wrapper, "/status")
        assert status["identity_ready"] is True and status["identity_reason"] is None
        sidecar.rename(sidecar.with_name(sidecar.name + ".moved"))
        sidecar.write_bytes(b"")  # the sidecar is replaced by a different inode
        # Not yet observed: status never probes, so it does not predict a failure.
        assert (await _get_json(wrapper, "/status"))["identity_ready"] is True
        assert await _status(wrapper, "/whoami", token) == 503  # the gate detects it
        with monkeypatch.context() as spy:
            _status_must_not_probe(spy)
            await _assert_status_unavailable(wrapper)


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["static", "entra"])
async def test_uncertain_commit_is_published_to_status_without_a_protected_request(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mode: str
) -> None:
    store_path = tmp_path / "state" / STORE_NAMES[mode]
    wrapper = _make_app(_settings(tmp_path / "state", mode, store_path))
    key, _token = _probe_credentials(mode)
    async with _serving():
        status = await _get_json(wrapper, "/status")
        assert status["identity_ready"] is True and status["identity_reason"] is None
        with _directory_fsync_fault(), pytest.raises(IdentityStoreOwnershipError):
            _store_for(mode).delete(key)  # replaced on disk, durability unknown
        with monkeypatch.context() as spy:  # no protected request happened yet
            _status_must_not_probe(spy)
            await _assert_status_unavailable(wrapper)


# ---------------------------------------------------------------------------
# r4336046216: the identity store need not sit beneath the queue root.
#
# The default queue root is /data while the default identity store lives under
# the user's home, so root containment would reject the supported default layout.
# Ownership is the per-store IdentityStoreLock's job, taken before any load/seed.
# ---------------------------------------------------------------------------

FOREIGN_TOKEN = "foreign-seed-token-0001"  # synthetic
FOREIGN_DIGEST = hashlib.sha256(FOREIGN_TOKEN.encode()).hexdigest()
FOREIGN_OID = "99999999-8888-7777-6666-555555555555"


def _foreign_settings(root: Path, mode: str, store: Path) -> Settings:
    """Settings seeded with a credential the existing store does NOT contain."""
    common: dict[str, Any] = {
        "queues_path": str(root / "queues"),
        "blob_path": str(root / "blobs"),
        "allow_unauthenticated": False,
    }
    if mode == "static":
        return Settings(
            auth_mode="static",
            api_keys={FOREIGN_DIGEST: {"id": "intruder"}},
            api_keys_store_path=str(store),
            **common,
        )
    return Settings(
        auth_mode="entra",
        azure_client_id=CLIENT_ID,
        azure_tenant_id=TENANT_ID,
        entra_identities={FOREIGN_OID: {"id": "intruder"}},
        entra_admin_role="",
        entra_identities_store_path=str(store),
        **common,
    )


def _child_start_foreign_seed(mode: str, root: str, store: str, out: Any) -> None:
    """Spawned process: a SEEDED server on its own root, naming a shared store."""
    import asyncio as aio

    import context_intelligence_server.main as m

    m._server_lifespan = _no_server_lifespan  # type: ignore[assignment]
    settings = _foreign_settings(Path(root), mode, Path(store))

    async def run() -> None:
        m.create_asgi_app(settings=settings, _jwks_client=_StubJWKS())
        try:
            async with m.lifespan(m.app):
                task = getattr(m.app.state, "identity_init_task", None)
                if task is not None:
                    await task
                out.put(("started", getattr(m.app.state, "identity_state", "unknown")))
        except RuntimeError as exc:
            out.put(("refused", str(exc)))

    aio.run(run())


@pytest.mark.parametrize("mode", ["static", "entra"])
def test_seeded_second_server_on_an_unrelated_root_starts_with_identity_unavailable(
    tmp_path: Path, mode: str
) -> None:
    """Previously a SEEDED second server was refused at config time (no /status).

    Now it comes up like an unseeded one: listener and /status are available,
    identity is unavailable, and it neither loads nor seeds the shared store.
    """
    store = tmp_path / "first" / "identity" / STORE_NAMES[mode]
    first_settings = _settings(tmp_path / "first", mode, store)

    async def run() -> None:
        wrapper = _make_app(first_settings)
        async with _serving():
            before = store.read_bytes()
            ctx = multiprocessing.get_context("spawn")
            out = ctx.Queue()
            child = ctx.Process(
                target=_child_start_foreign_seed,
                args=(mode, str(tmp_path / "second"), str(store), out),
            )
            child.start()
            child.join(timeout=60)
            assert child.exitcode == 0
            assert out.get(timeout=5) == ("started", "unavailable")
            assert store.read_bytes() == before  # second never loaded or seeded
            assert FOREIGN_DIGEST.encode() not in before
            assert FOREIGN_OID.encode() not in before
            if mode == "static":
                assert await _status(wrapper, "/whoami", TOKEN) == 200
                assert await _status(wrapper, "/whoami", FOREIGN_TOKEN) == 401

    asyncio.run(run())


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["static", "entra"])
async def test_existing_credentials_outside_the_queue_root_survive_a_different_seed(
    tmp_path: Path, mode: str
) -> None:
    """Upgrade path: the existing store stays where it is and its bytes win.

    The queue root is under tmp ``state``; the store sits in a separate
    home-like directory (the default-layout shape).  Startup must not abort,
    must not relocate or rewrite the file, and a DIFFERENT configured seed must
    not be merged over the credentials that already exist.
    """
    store = tmp_path / "home" / ".local" / "share" / "ci-server" / "identity"
    store = store / STORE_NAMES[mode]
    store.parent.mkdir(parents=True)
    existing_key = DIGEST if mode == "static" else OID
    store.write_text(json.dumps({existing_key: {"id": "legacy"}}), encoding="utf-8")
    before = store.read_bytes()

    settings = _foreign_settings(tmp_path / "state", mode, store)
    assert not store.resolve().is_relative_to((tmp_path / "state").resolve())
    wrapper = _make_app(settings)

    async with _serving():
        assert main_module.app.state.identity_state == "ready"
        assert (await _get_json(wrapper, "/status"))["identity_ready"] is True
        assert _store_for(mode).flat_dict == {existing_key: "legacy"}
        if mode == "static":
            assert await _status(wrapper, "/whoami", TOKEN) == 200  # existing wins
            # The different configured seed was never merged over the existing file.
            assert await _status(wrapper, "/whoami", FOREIGN_TOKEN) == 401

    assert store.read_bytes() == before  # never rewritten
    configured = (
        settings.api_keys_store_path
        if mode == "static"
        else settings.entra_identities_store_path
    )
    assert configured == str(store)  # never relocated


# Dispatch repair: each sync test owns an isolated loop and a real CPython
# executor. asyncio.run settles that test executor after all barriers release.
def _submit_test_loop(
    executors: list[ThreadPoolExecutor] | None = None,
) -> asyncio.AbstractEventLoop:
    loop = asyncio.get_running_loop()
    executor = ThreadPoolExecutor(
        max_workers=8,
        thread_name_prefix="ci-identity-submit",
    )
    if executors is not None:
        executors.append(executor)
    loop.set_default_executor(executor)
    return loop


def _identity_submission_kind(func: Any) -> str | None:
    target = getattr(func, "func", func)
    if getattr(target, "__name__", "") == "_run_identity_call":
        return _identity_submission_kind(func.args[1])
    name = getattr(target, "__name__", "")
    if name == "_start_identity_ownership":
        return "init"
    if name == "_stop_identity_ownership":
        return "cleanup"
    if name in {"healthy", "check"}:
        return "probe"
    return None


async def _executor_holder(
    loop: asyncio.AbstractEventLoop,
) -> tuple[Any, threading.Event]:
    entered, release = threading.Event(), threading.Event()

    def hold() -> None:
        entered.set()
        assert release.wait(3), "test executor holder was not released"

    future = loop.run_in_executor(None, hold)
    await _until(entered.is_set)
    return future, release


async def _repair_test_owner() -> None:
    """Failure hygiene only; acceptance assertions must precede this helper."""
    store = _store_for("static")
    if store._lock is not None:
        await asyncio.to_thread(main_module._release_identity, store, store._lock)
    root = getattr(main_module.app.state, "server_instance_lock", None)
    if root is not None:
        root.release()
    main_module.app.state.identity_lifespan_active = False


def test_identity_init_thread_start_failure_after_enqueue_has_no_late_owner(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def run() -> None:
        loop = _submit_test_loop()
        root, path = tmp_path / "state", tmp_path / "keys.json"
        _make_app(_settings(root, "static", path))
        real_start = threading.Thread.start
        real_init = main_module._start_identity_ownership
        actions: list[int] = []
        failed: list[str] = []

        def counted(*args: Any, **kwargs: Any) -> Any:
            actions.append(1)
            return real_init(*args, **kwargs)

        def start(thread: threading.Thread) -> None:
            if thread.name.startswith("ci-identity-submit") and not failed:
                failed.append(thread.name)  # submit already queued the wrapper
                raise RuntimeError("synthetic queued init start failure")
            real_start(thread)

        monkeypatch.setattr(main_module, "_start_identity_ownership", counted)
        try:
            with monkeypatch.context() as scoped:
                scoped.setattr(threading.Thread, "start", start)
                with pytest.raises(
                    RuntimeError, match="synthetic queued init start failure"
                ):
                    async with main_module.lifespan(main_module.app):
                        pytest.fail("failed dispatch must not yield serving context")
            # A real later worker retires the queued old wrapper.
            await asyncio.wait({loop.run_in_executor(None, lambda: None)})
            assert actions == [], "aborted initializer ran later without an owner"
            runtime = main_module.app.state.identity_runtime
            assert runtime.init_io is not None and runtime.init_io.cancelled()
            assert runtime.cleanup_complete
            assert not main_module.app.state.identity_lifespan_active
            assert runtime.store.flat_dict == {} and runtime.store._lock is None
            assert not path.with_name("keys.json.lock").exists()
            assert not lock_is_held(root / ".context-intelligence-server.lock")
        finally:
            await _repair_test_owner()

    asyncio.run(run())


def test_repeated_runtime_submit_failures_do_not_queue_untracked_checks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def run() -> None:
        loop = _submit_test_loop()
        wrapper = _make_app(
            _settings(tmp_path / "state", "static", tmp_path / "keys.json")
        )
        async with _serving():
            runtime = main_module.app.state.identity_runtime
            real = runtime.store.healthy
            calls: list[int] = []
            starts: list[str] = []
            real_start = threading.Thread.start

            def check() -> bool:
                calls.append(1)
                return real()

            def fail(thread: threading.Thread) -> None:
                if thread.name.startswith("ci-identity-submit"):
                    starts.append(thread.name)
                    raise RuntimeError("synthetic runtime queued start failure")
                real_start(thread)

            monkeypatch.setattr(runtime.store, "healthy", check)
            holder, release = await _executor_holder(loop)
            try:
                with monkeypatch.context() as scoped:
                    scoped.setattr(threading.Thread, "start", fail)
                    assert [
                        await _status(wrapper, "/whoami", TOKEN) for _ in range(6)
                    ] == [503] * 6
                    assert len(starts) == 1, (
                        "failed lifetime kept producing queued wrappers"
                    )
                    assert runtime.closing and calls == []
                release.set()
                await asyncio.wait({holder})
                await asyncio.wait({loop.run_in_executor(None, lambda: None)})
                await _until(lambda: runtime.admitted == 0)
                assert not runtime.probes and calls == []
            finally:
                release.set()
                await asyncio.wait({holder})

    asyncio.run(run())


@pytest.mark.parametrize(
    "phase,late_error",
    [
        pytest.param(phase, error, id=f"{error}-{phase}")
        for error in (False, True)
        for phase in ("init", "probe", "cleanup")
    ]
    + [pytest.param("probe", "false", id="False-result-probe")],
)
def test_identity_dispatch_error_after_claim_retains_real_completion(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    phase: str,
    late_error: bool | str,
) -> None:
    """Actual Thread.start starts a worker/action, then reports failure."""
    executors: list[ThreadPoolExecutor] = []

    async def run() -> None:
        loop = _submit_test_loop(executors)
        root, path = tmp_path / "state", tmp_path / "keys.json"
        wrapper = _make_app(_settings(root, "static", path))
        entered, release = threading.Event(), threading.Event()
        real_init = main_module._initialize_identity_store
        store = _store_for("static")
        real_check, real_stop = store.healthy, store.stop
        actions: list[int] = []
        stop_threads: list[int] = []
        errors: list[Any] = []
        loop.set_exception_handler(lambda _loop, context: errors.append(context))

        def parked() -> None:
            actions.append(1)
            entered.set()
            assert release.wait(3)
            if late_error is True:
                raise RuntimeError("synthetic claimed action error")

        def initialize(*args: Any, **kwargs: Any) -> Any:
            parked()  # _initialize is called AFTER real lock acquisition
            return real_init(*args, **kwargs)

        def check() -> bool:
            parked()
            result = real_check()
            return False if late_error == "false" else result

        def stop() -> None:
            stop_threads.append(threading.get_ident())
            if phase == "cleanup":
                parked()
            real_stop()

        monkeypatch.setattr(store, "stop", stop)
        if phase == "init":
            monkeypatch.setattr(main_module, "_initialize_identity_store", initialize)
        if phase == "probe":
            monkeypatch.setattr(store, "healthy", check)
        cm = main_module.lifespan(main_module.app)
        task = holder = None
        holder_release = threading.Event()
        real_start = threading.Thread.start
        faults: list[str] = []
        real_submit = loop.run_in_executor
        armed = False

        def submit(executor: Any, func: Any, *args: Any) -> Any:
            nonlocal armed
            identity = executor is None and _identity_submission_kind(func) == phase
            if identity and not faults:
                # A fresh actual DEFAULT executor removes idle-worker timing
                # from this Thread.start fault seam, without private internals.
                _submit_test_loop(executors)
            armed = identity
            try:
                return real_submit(executor, func, *args)
            finally:
                armed = False

        def start(thread: threading.Thread) -> None:
            if armed and thread.name.startswith("ci-identity-submit") and not faults:
                faults.append(thread.name)
                real_start(thread)
                assert entered.wait(3), "new worker did not claim the action"
                raise RuntimeError("synthetic after-claim dispatch failure")
            real_start(thread)

        try:
            if phase != "init":
                await cm.__aenter__()
                await main_module.app.state.identity_init_task
                holder, holder_release = await _executor_holder(loop)
            with monkeypatch.context() as scoped:
                scoped.setattr(threading.Thread, "start", start)
                scoped.setattr(loop, "run_in_executor", submit)
                if phase == "init":
                    task = asyncio.create_task(cm.__aenter__())
                elif phase == "probe":
                    task = asyncio.create_task(_status(wrapper, "/whoami", TOKEN))
                else:
                    task = asyncio.create_task(cm.__aexit__(None, None, None))
                await _until(entered.is_set)
                runtime = main_module.app.state.identity_runtime
                assert len(faults) == 1, "the real Thread.start fault was not exercised"
                assert runtime.closing, "ambiguous dispatch did not close this lifetime"
                assert main_module.app.state.identity_lifespan_active
                assert lock_is_held(root / ".context-intelligence-server.lock")
                assert lock_is_held(path.with_name("keys.json.lock"))
                _refuse_identity_replacement(tmp_path)
                assert await _status(wrapper, "/version") == 200
                assert await _status(wrapper, "/whoami", TOKEN) == 503
                if phase == "probe":
                    assert await task == 503
                    assert len(runtime.probes) == runtime.admitted == 1
                    assert not next(iter(runtime.probes)).done()
                else:
                    if phase == "init":
                        assert (
                            runtime.init_io is not None and not runtime.init_io.done()
                        )
                        assert runtime.init_call.completion.running()
                    else:
                        assert len(runtime.cleanup_calls) == 1
                        assert runtime.cleanup_calls[0].completion.running()
                    for _ in range(3):
                        task.cancel()
                        await asyncio.sleep(0)
                        assert not task.done()
                release.set()
                if phase == "probe":
                    await _until(lambda: runtime.admitted == 0)
                    assert not runtime.probes
                    await cm.__aexit__(None, None, None)
                else:
                    result = await asyncio.gather(task, return_exceptions=True)
                    assert isinstance(result[0], (RuntimeError, asyncio.CancelledError))
                if phase == "cleanup" and late_error:
                    assert not runtime.cleanup_complete
                    assert main_module.app.state.identity_lifespan_active
                    assert lock_is_held(root / ".context-intelligence-server.lock")
                    assert lock_is_held(path.with_name("keys.json.lock"))
                    assert (
                        len(runtime.cleanup_calls) == 1
                    )  # no retry of a started error
                else:
                    assert not main_module.app.state.identity_lifespan_active
                    assert not lock_is_held(root / ".context-intelligence-server.lock")
                    assert not lock_is_held(path.with_name("keys.json.lock"))
                    assert store.flat_dict == {}
                assert actions == [1]
                assert stop_threads and threading.get_ident() not in stop_threads
                assert len(stop_threads) == 1
                gc.collect()
                await asyncio.sleep(0)
                assert errors == []
            if not (phase == "cleanup" and late_error):
                monkeypatch.setattr(store, "healthy", real_check)
                monkeypatch.setattr(store, "stop", real_stop)
                monkeypatch.setattr(
                    main_module, "_initialize_identity_store", real_init
                )
                async with _serving():
                    assert main_module.app.state.identity_runtime is not runtime
                    assert await _status(wrapper, "/whoami", TOKEN) == 200
        finally:
            release.set()
            holder_release.set()
            if task is not None:
                await asyncio.gather(task, return_exceptions=True)
            if holder is not None:
                await asyncio.wait({holder})
            monkeypatch.setattr(store, "stop", real_stop)
            if main_module.app.state.identity_lifespan_active:
                # Explicit repair of expected retained-failure or broken baseline.
                await _repair_test_owner()
            elif store._lock is not None:
                await _repair_test_owner()

    try:
        asyncio.run(run())
    finally:
        for executor in executors:
            executor.shutdown(wait=True)  # outside every asyncio loop


@pytest.mark.parametrize("fallback", ["normal", "after-claim", "abort", "construct"])
@pytest.mark.parametrize("preenqueue", [False, True])
def test_cleanup_prestart_abort_uses_one_guarded_thread_fallback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fallback: str,
    preenqueue: bool,
) -> None:
    executors: list[ThreadPoolExecutor] = []

    async def run() -> None:
        loop = _submit_test_loop(executors)
        root, path = tmp_path / "state", tmp_path / "keys.json"
        wrapper = _make_app(_settings(root, "static", path))
        cm = _serving()
        await cm.__aenter__()
        runtime = main_module.app.state.identity_runtime
        store = runtime.store
        real_stop, real_start = store.stop, threading.Thread.start
        entered, release = threading.Event(), threading.Event()
        stops: list[int] = []
        launched: list[Any] = []
        trace: list[str] = []
        real_submit = loop.run_in_executor
        armed = False
        fault_injected = False
        fault_executor: ThreadPoolExecutor | None = None
        real_root_release = main_module.ServerInstanceLock.release
        real_lock_release = IdentityStoreLock.release

        def stop() -> None:
            stops.append(threading.get_ident())
            entered.set()
            assert release.wait(3)
            real_stop()
            trace.append("stop")

        def root_release(lock: Any) -> None:
            assert all(not thread.is_alive() for thread in launched)
            trace.append("root")
            real_root_release(lock)

        def identity_release(lock: Any) -> None:
            trace.append("identity")
            real_lock_release(lock)

        def start(thread: threading.Thread) -> None:
            if armed and thread.name.startswith("ci-identity-submit"):
                raise RuntimeError("synthetic cleanup default start failure")
            if thread.name == "ci-identity-cleanup":
                launched.append(thread)
                if fallback == "abort":
                    raise RuntimeError("synthetic fallback before claim")
                real_start(thread)
                if fallback == "after-claim":
                    assert entered.wait(3)
                    raise RuntimeError("synthetic fallback after claim")
                return
            real_start(thread)

        def submit(executor: Any, func: Any, *args: Any) -> Any:
            nonlocal armed, fault_injected, fault_executor
            identity = executor is None and _identity_submission_kind(func) == "cleanup"
            if identity and not fault_injected:
                _submit_test_loop(executors)
                fault_executor = executors[-1]
                fault_injected = True
            armed = identity
            try:
                if armed and preenqueue:
                    raise RuntimeError("synthetic cleanup preenqueue rejection")
                return real_submit(executor, func, *args)
            finally:
                armed = False

        monkeypatch.setattr(store, "stop", stop)
        monkeypatch.setattr(main_module.ServerInstanceLock, "release", root_release)
        monkeypatch.setattr(IdentityStoreLock, "release", identity_release)
        holder, holder_release = await _executor_holder(loop)
        task = None
        try:
            with monkeypatch.context() as scoped:
                scoped.setattr(threading.Thread, "start", start)
                scoped.setattr(loop, "run_in_executor", submit)
                if fallback == "construct":
                    real_thread = threading.Thread

                    def cannot_construct(*args: Any, **kwargs: Any) -> Any:
                        if kwargs.get("name") == "ci-identity-cleanup":
                            raise RuntimeError(
                                "synthetic fallback construction failure"
                            )
                        return real_thread(*args, **kwargs)

                    scoped.setattr(main_module.threading, "Thread", cannot_construct)
                task = asyncio.create_task(cm.__aexit__(None, None, None))
                if fallback in {"abort", "construct"}:
                    result = await asyncio.gather(task, return_exceptions=True)
                    assert isinstance(result[0], RuntimeError)
                    assert main_module.app.state.identity_lifespan_active
                    assert not runtime.cleanup_complete
                    assert len(runtime.cleanup_calls) == 2
                    assert all(c.completion.cancelled() for c in runtime.cleanup_calls)
                    assert store.flat_dict == {DIGEST: "writer"} and stops == []
                    assert trace == []
                    assert lock_is_held(path.with_name("keys.json.lock"))
                    assert lock_is_held(root / ".context-intelligence-server.lock")
                    assert await _status(wrapper, "/whoami", TOKEN) == 503
                    _refuse_identity_replacement(tmp_path)
                else:
                    await _until(entered.is_set)
                    assert main_module.app.state.identity_lifespan_active
                    assert lock_is_held(path.with_name("keys.json.lock"))
                    assert lock_is_held(root / ".context-intelligence-server.lock")
                    assert len(runtime.cleanup_calls) == 2
                    assert runtime.cleanup_calls[0].completion.cancelled()
                    assert runtime.cleanup_calls[1].completion.running()
                    for _ in range(3):
                        task.cancel()
                        await asyncio.sleep(0)
                        assert not task.done()
                    release.set()
                    result = await asyncio.gather(task, return_exceptions=True)
                    assert isinstance(result[0], (RuntimeError, asyncio.CancelledError))
                    assert runtime.cleanup_complete
                    assert not main_module.app.state.identity_lifespan_active
                    assert len(stops) == 1 and threading.get_ident() not in stops
                    assert trace == ["stop", "identity", "root"]
                    assert store.flat_dict == {} and not store._lock
            # Keep the aborted wrapper's queue dormant until the SAME store
            # owns a legal new lifetime on a different default executor.
            holder_release.set()
            await asyncio.wait({holder})
            if fallback in {"normal", "after-claim"}:
                assert len(stops) == 1
                monkeypatch.setattr(store, "stop", real_stop)
                _submit_test_loop(executors)
                async with _serving():
                    assert await _status(wrapper, "/whoami", TOKEN) == 200
                    assert fault_executor is not None
                    await asyncio.wait(
                        {loop.run_in_executor(fault_executor, lambda: None)}
                    )
                    assert len(stops) == 1
                    assert store.flat_dict == {DIGEST: "writer"}
                    assert await _status(wrapper, "/whoami", TOKEN) == 200
            else:
                assert fault_executor is not None
                await asyncio.wait({loop.run_in_executor(fault_executor, lambda: None)})
        finally:
            release.set()
            holder_release.set()
            if task is not None:
                await asyncio.gather(task, return_exceptions=True)
            await asyncio.wait({holder})
            # Remove stop barrier before explicit retained-resource repair.
            monkeypatch.setattr(store, "stop", real_stop)
            monkeypatch.setattr(
                main_module.ServerInstanceLock, "release", real_root_release
            )
            monkeypatch.setattr(IdentityStoreLock, "release", real_lock_release)
            await _repair_test_owner()

    try:
        asyncio.run(run())
    finally:
        for executor in executors:
            executor.shutdown(wait=True)


@pytest.mark.parametrize("state", ["aborted", "running", "completed"])
def test_identity_dispatch_claim_or_abort_is_atomic(
    monkeypatch: pytest.MonkeyPatch,
    state: str,
) -> None:
    async def run() -> None:
        _submit_test_loop()
        new_call = getattr(main_module, "_new_identity_call", None)
        run_call = getattr(main_module, "_run_identity_call", None)
        dispatch = getattr(main_module, "_dispatch_identity_call", None)
        assert callable(new_call) and callable(run_call) and callable(dispatch)
        loop = asyncio.get_running_loop()
        completion: ConcurrentFuture[int] = ConcurrentFuture()
        call: Any = new_call(loop, completion)
        entered, release = threading.Event(), threading.Event()
        actions: list[int] = []
        thread = None

        def action() -> int:
            actions.append(1)
            entered.set()
            assert release.wait(3)
            return 42

        def ambiguous_submit(executor: Any, func: Any, *args: Any) -> Any:
            nonlocal thread
            if state != "aborted":
                thread = threading.Thread(
                    target=func, args=args, name="ci-claim-control"
                )
                thread.start()
                assert entered.wait(3)
                if state == "completed":
                    release.set()
                    thread.join(3)  # test seam only, completion has no loop dependency
                    assert not thread.is_alive()
            raise RuntimeError("synthetic ambiguous dispatch")

        try:
            with monkeypatch.context() as scoped:
                scoped.setattr(loop, "run_in_executor", ambiguous_submit)
                dispatch(call, action)
            assert isinstance(call.dispatch_error, RuntimeError)
            if state == "aborted":
                assert completion.cancelled()
                run_call(completion, action)  # late executor wrapper must be inert
                assert actions == []
            else:
                assert not completion.cancelled()
                if state == "running":
                    assert completion.running() and not completion.done()
                release.set()
            await asyncio.wait({call.io})
            if state != "aborted":
                assert completion.result() == call.io.result() == 42
                assert actions == [1]
        finally:
            release.set()
            if thread is not None:
                owned_thread = thread
                await _until(lambda: not owned_thread.is_alive(), 3)
                thread.join(0)

    asyncio.run(run())


@pytest.mark.parametrize("timing", ["during-init", "ready-same-turn"])
async def test_cancelled_init_observer_late_success_stays_unavailable_without_shutdown(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    timing: str,
) -> None:
    wrapper = _make_app(
        _settings(
            tmp_path / "state",
            "static",
            tmp_path / "keys.json",
            admin=True,
        )
    )
    entered, release = threading.Event(), threading.Event()
    hold_observer = asyncio.Event()
    real_init, real_observer = (
        main_module._initialize_identity_store,
        main_module._observe_identity_init,
    )
    calls: list[int] = []

    def initialize(*args: Any, **kwargs: Any) -> None:
        calls.append(1)
        entered.set()
        assert release.wait(3)
        real_init(*args, **kwargs)

    async def observer(*args: Any, **kwargs: Any) -> Any:
        result = await real_observer(*args, **kwargs)
        await hold_observer.wait()
        return result

    monkeypatch.setattr(main_module, "_initialize_identity_store", initialize)
    monkeypatch.setattr(main_module, "_observe_identity_init", observer)
    try:
        async with main_module.lifespan(main_module.app):
            await _until(entered.is_set)
            runtime = main_module.app.state.identity_runtime
            task = main_module.app.state.identity_init_task
            if timing == "ready-same-turn":
                release.set()
                await asyncio.wait({runtime.init_io})
                assert main_module._identity_runtime_ready(main_module.app, runtime)
                assert main_module.app.state.identity_state == "ready"
                assert not task.done()
            task.cancel()
            # No intervening await/callback: cancelling()>0 must deny readiness.
            assert not main_module._identity_runtime_ready(main_module.app, runtime)
            assert runtime.closing
            with pytest.raises(asyncio.CancelledError):
                await task
            release.set()
            await asyncio.wait({runtime.init_io})
            owned = runtime.init_io.result()
            assert owned is not None and owned[0].flat_dict == {DIGEST: "writer"}
            assert owned[1] is not None and owned[1].owned_by_this_process()
            assert main_module.app.state.identity_lifespan_active
            assert main_module.app.state.identity_state == "unavailable"
            assert main_module.app.state.identity_ready_token is None
            assert not (await _get_json(wrapper, "/status"))["identity_ready"]
            assert await _status(wrapper, "/version") == 200
            assert await _status(wrapper, "/whoami", TOKEN) == 503
            assert await _status(wrapper, "/admin/keys", ADMIN_TOKEN) == 503
            await asyncio.sleep(0.02)
            assert calls == [1]
        assert owned[0].flat_dict == {}
        assert not owned[1].owned_by_this_process()
        old_runtime = runtime
        hold_observer.set()
        async with _serving():
            assert main_module.app.state.identity_runtime is not old_runtime
            assert await _status(wrapper, "/whoami", TOKEN) == 200
            assert await _status(wrapper, "/admin/keys", ADMIN_TOKEN) == 200
    finally:
        release.set()
        hold_observer.set()


def test_observer_create_task_failure_closes_coroutine_and_settles_init(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def run() -> None:
        _submit_test_loop()
        root, path = tmp_path / "state", tmp_path / "keys.json"
        _make_app(_settings(root, "static", path))
        entered, release = threading.Event(), threading.Event()
        rejected: list[Any] = []
        stops: list[int] = []
        errors: list[Any] = []
        loop = asyncio.get_running_loop()
        loop.set_exception_handler(lambda _loop, context: errors.append(context))
        real_init, real_create = (
            main_module._initialize_identity_store,
            asyncio.create_task,
        )
        store = _store_for("static")
        real_stop = store.stop

        def initialize(*args: Any, **kwargs: Any) -> None:
            entered.set()
            assert release.wait(3)
            real_init(*args, **kwargs)

        def stop() -> None:
            stops.append(threading.get_ident())
            real_stop()

        def create(coro: Any, **kwargs: Any) -> Any:
            if coro.cr_code.co_name == "_observe_identity_init":
                rejected.append(coro)
                raise RuntimeError("synthetic observer scheduling rejection")
            return real_create(coro, **kwargs)

        monkeypatch.setattr(main_module, "_initialize_identity_store", initialize)
        monkeypatch.setattr(store, "stop", stop)
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            with monkeypatch.context() as scoped:
                scoped.setattr(asyncio, "create_task", create)
                task = real_create(main_module.lifespan(main_module.app).__aenter__())
                try:
                    await _until(entered.is_set)
                    for _ in range(3):
                        task.cancel()
                        await asyncio.sleep(0)
                        assert not task.done()
                    assert lock_is_held(path.with_name("keys.json.lock"))
                    assert lock_is_held(root / ".context-intelligence-server.lock")
                    assert inspect.getcoroutinestate(rejected[0]) == inspect.CORO_CLOSED
                finally:
                    release.set()
                    result = await asyncio.gather(task, return_exceptions=True)
            try:
                assert isinstance(result[0], (RuntimeError, asyncio.CancelledError))
                assert store.flat_dict == {} and store._lock is None
                assert stops and len(stops) == 1 and stops[0] != threading.get_ident()
                assert not lock_is_held(path.with_name("keys.json.lock"))
                assert not lock_is_held(root / ".context-intelligence-server.lock")
                del result
                rejected.clear()
                gc.collect()
                await asyncio.sleep(0)
                assert not any("was never awaited" in str(w.message) for w in caught)
                assert errors == []
            finally:
                for coro in rejected:
                    coro.close()  # red-only hygiene, not a passing acceptance assertion
                await _repair_test_owner()

    asyncio.run(run())


async def test_drained_waiter_create_task_failure_closes_coroutine_and_settles_admissions(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    wrapper = _make_app(_settings(tmp_path / "state", "static", tmp_path / "keys.json"))
    cm = _serving()
    await cm.__aenter__()
    runtime = main_module.app.state.identity_runtime
    # Public semaphore behavior leaves one real HTTP task admitted but queued.
    runtime.slots = asyncio.Semaphore(0)
    request = asyncio.create_task(_status(wrapper, "/whoami", TOKEN))
    real_create = asyncio.create_task
    rejected: list[Any] = []
    task = None

    def create(coro: Any, **kwargs: Any) -> Any:
        if coro.cr_code.co_qualname == "Event.wait":
            rejected.append(coro)
            raise RuntimeError("synthetic drained waiter scheduling rejection")
        return real_create(coro, **kwargs)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        try:
            await _until(lambda: runtime.admitted == 1)
            with monkeypatch.context() as scoped:
                scoped.setattr(asyncio, "create_task", create)
                task = real_create(cm.__aexit__(None, None, None))
                await _until(lambda: bool(rejected))
                assert inspect.getcoroutinestate(rejected[0]) == inspect.CORO_CLOSED
                assert main_module.app.state.identity_lifespan_active
                assert lock_is_held(tmp_path / "keys.json.lock")
                assert lock_is_held(
                    tmp_path / "state" / ".context-intelligence-server.lock"
                )
                for _ in range(3):
                    task.cancel()
                    await asyncio.sleep(0)
                    assert not task.done()
                runtime.slots.release()
                assert await request == 503
                result = await asyncio.gather(task, return_exceptions=True)
                assert isinstance(result[0], (RuntimeError, asyncio.CancelledError))
            assert runtime.admitted == 0 and runtime.cleanup_complete
            assert not main_module.app.state.identity_lifespan_active
            assert not lock_is_held(tmp_path / "keys.json.lock")
            assert not lock_is_held(
                tmp_path / "state" / ".context-intelligence-server.lock"
            )
            del result
            rejected.clear()
            gc.collect()
            await asyncio.sleep(0)
            assert not any("was never awaited" in str(w.message) for w in caught)
        finally:
            runtime.slots.release()
            if task is None:
                task = real_create(cm.__aexit__(None, None, None))
            await asyncio.gather(task, request, return_exceptions=True)
            for coro in rejected:
                coro.close()


async def test_release_identity_stop_failure_does_not_release_sidecar(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The partial-owner helper must not unlock if stop failed before clearing."""
    path = tmp_path / "keys.json"
    _make_app(_settings(tmp_path / "state", "static", path))
    async with _serving():
        store = _store_for("static")
        lock = store._lock
        assert lock is not None
        real_stop = store.stop
        threads: list[int] = []

        def stop() -> None:
            threads.append(threading.get_ident())
            raise RuntimeError("synthetic stop before cache clear")

        try:
            monkeypatch.setattr(store, "stop", stop)
            with pytest.raises(RuntimeError, match="stop before cache clear"):
                await asyncio.to_thread(main_module._release_identity, store, lock)
            assert lock_is_held(path.with_name("keys.json.lock"))
            assert lock.owned_by_this_process()
            assert store.flat_dict == {DIGEST: "writer"}
            assert threads and threads[0] != threading.get_ident()
        finally:
            monkeypatch.setattr(store, "stop", real_stop)


@pytest.mark.parametrize("failure", ["shutdown", "load", "seed"])
async def test_started_stop_failure_retains_exact_owner_and_both_locks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    root, path = tmp_path / "state", tmp_path / "keys.json"
    if failure == "load":
        path.write_text(json.dumps({DIGEST: {"id": "writer"}}), encoding="utf-8")
    wrapper = _make_app(_settings(root, "static", path, admin=True))
    store = _store_for("static")
    real_stop = store.stop
    real_load, real_seed = store.load, store.seed
    stop_threads: list[int] = []
    init_threads: list[int] = []
    records: list[tuple[Any, Any]] = []
    real_initialize = main_module._initialize_identity_store
    cm = main_module.lifespan(main_module.app)
    entered = False
    shutdown_attempted = False

    def initialize(*args: Any, **kwargs: Any) -> None:
        init_threads.append(threading.get_ident())
        records.append((store, store._lock))
        runtime = main_module.app.state.identity_runtime
        retained = getattr(runtime, "owned", None)
        assert retained is not None, "owner must be retained BEFORE load/seed"
        assert retained[0] is store and retained[1] is store._lock
        real_initialize(*args, **kwargs)

    def load() -> None:
        real_load()
        raise OSError("synthetic read failure after map publication")

    def seed(*args: Any, **kwargs: Any) -> None:
        real_seed(*args, **kwargs)
        raise OSError("synthetic seed failure after map publication")

    def stop() -> None:
        stop_threads.append(threading.get_ident())
        raise RuntimeError("synthetic started stop before cache clear")

    monkeypatch.setattr(main_module, "_initialize_identity_store", initialize)
    try:
        if failure != "shutdown":
            monkeypatch.setattr(store, "stop", stop)
            monkeypatch.setattr(
                store,
                "load" if failure == "load" else "seed",
                load if failure == "load" else seed,
            )
        await cm.__aenter__()
        entered = True
        observer = main_module.app.state.identity_init_task
        if failure == "shutdown":
            await observer
            assert await _status(wrapper, "/whoami", TOKEN) == 200
            monkeypatch.setattr(store, "stop", stop)
        else:
            with pytest.raises(RuntimeError, match="started stop before cache clear"):
                await observer
        shutdown_attempted = True
        with pytest.raises(RuntimeError, match="root exclusion retained"):
            await cm.__aexit__(None, None, None)
        runtime = main_module.app.state.identity_runtime
        # All acceptance assertions precede the emergency finally.
        assert lock_is_held(path.with_name("keys.json.lock"))
        assert lock_is_held(root / ".context-intelligence-server.lock")
        assert main_module.app.state.identity_lifespan_active
        assert runtime.closing and not runtime.cleanup_complete
        assert store.flat_dict == {DIGEST: "writer"}
        retained = getattr(runtime, "owned", None)
        assert retained is not None, "partial owner survives only in an exception/local"
        assert retained[0] is store and retained[1] is records[0][1]
        assert retained[1].owned_by_this_process()
        assert len(stop_threads) == 1 and threading.get_ident() not in stop_threads
        assert len(init_threads) == 1 and threading.get_ident() not in init_threads
        assert len(runtime.cleanup_calls) == (1 if failure == "shutdown" else 0)
        if runtime.cleanup_calls:
            assert not runtime.cleanup_calls[0].completion.cancelled()
            assert isinstance(
                runtime.cleanup_calls[0].completion.exception(), RuntimeError
            )
        else:
            assert isinstance(runtime.init_call.completion.exception(), RuntimeError)
        assert await _status(wrapper, "/whoami", TOKEN) == 503
        assert await _status(wrapper, "/admin/keys", ADMIN_TOKEN) == 503
        assert not (await _get_json(wrapper, "/status"))["identity_ready"]
        assert await _status(wrapper, "/version") == 200
        _refuse_identity_replacement(tmp_path)
        gc.collect()
        assert retained[1].owned_by_this_process()
        assert len(stop_threads) == 1  # no stop retry or fallback after execution
    finally:
        monkeypatch.setattr(store, "stop", real_stop)
        monkeypatch.setattr(store, "load", real_load)
        monkeypatch.setattr(store, "seed", real_seed)
        if entered and not shutdown_attempted:
            await cm.__aexit__(None, None, None)
        # Expected retained-failure resources are repaired ONLY after assertions.
        await _repair_test_owner()
