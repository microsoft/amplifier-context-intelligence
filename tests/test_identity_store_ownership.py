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
import json
import multiprocessing
import os
import stat
import threading
from collections.abc import AsyncGenerator
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

    def counted(app: Any, s: Any) -> Any:
        calls.append(1)
        return real(app, s)

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

    def delayed(app: Any, s: Any) -> Any:
        assert release.wait(timeout=5), "identity init was never released"
        return real(app, s)

    monkeypatch.setattr(main_module, "_start_identity_ownership", delayed)
    # The lifespan must yield WITHOUT awaiting identity initialization.
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
