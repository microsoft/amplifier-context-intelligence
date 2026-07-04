"""M2 service-auth capability / route-dependency tests (§7 CAP-* test matrix).

Tests the full auth chain end-to-end for service / app-token principals using
httpx → ASGITransport.  Covers all nine §7 CAP rows:
  CAP-HW    — human principal → POST /events → 2xx (is_service=False → write-capable)
  CAP-SC    — service Contributor → POST /events → 2xx
  CAP-SR-w  — service Reader → POST /events → 403 (require_write fails)
  CAP-SR-r  — service Reader → GET /blobs/{sid} → 2xx (require_read passes)
  CAP-SADM-w — service IdentityAdmin only → POST /events → 403 (no Contributor)
  CAP-SADM-a — service IdentityAdmin only → GET /admin/identities → 2xx
  CAP-SC-r  — service Contributor → GET /blobs/{sid} → 2xx (write-capable → read-capable)
  CAP-cypher-read  — service Reader → POST /cypher (read query) → 2xx
  CAP-cypher-soft  — service Reader → POST /cypher (MUTATING query) → 2xx
                     SOFT-M2: soft gap hardens to 403 at M3 via read-only Neo4j DB user.

Pattern: RSA keypair (module-scoped), stub JWKS client, create_asgi_app with
service role names configured.  Token shape follows the discriminator logic in
auth.py EntraResolver.resolve(): human tokens have scp="access_as_user" (user
branch, is_service=False); service tokens omit scp (service branch, is_service=True).
"""

from __future__ import annotations

import json
import time
from collections.abc import AsyncGenerator
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

# ---------------------------------------------------------------------------
# Fake constants
# ---------------------------------------------------------------------------
FAKE_CLIENT_ID = "aaaabbbb-1111-2222-3333-ccccddddeeee"
FAKE_TENANT_ID = "ffffeeee-dddd-cccc-bbbb-aaaa99998888"
FAKE_OID_HUMAN = "11111111-2222-3333-4444-555566667777"
FAKE_OID_SERVICE = "aaaabbbb-9999-9999-9999-ccccddddffff"
FAKE_APPID = "bbbbcccc-2222-3333-4444-eeeeffff0000"
FAKE_CONTRIBUTOR_HUMAN = "colombod"
FAKE_ISSUER = f"https://login.microsoftonline.com/{FAKE_TENANT_ID}/v2.0"


# ---------------------------------------------------------------------------
# Stub JWKS client (no network)
# ---------------------------------------------------------------------------


class _StubSigningKey:
    def __init__(self, key: Any) -> None:
        self.key = key


class _StubJWKSClient:
    def __init__(self, key: Any) -> None:
        self._key = _StubSigningKey(key)

    def fetch_data(self) -> None:
        pass

    def get_signing_key_from_jwt(self, token: str) -> _StubSigningKey:
        return self._key

    def get_jwk_set(self) -> Any:
        _k = self._key

        class _FakeJWKSet:
            keys = [_k]

        return _FakeJWKSet()


# ---------------------------------------------------------------------------
# JWT helpers
# ---------------------------------------------------------------------------


def _sign_jwt(private_key: Any, claims: dict[str, Any]) -> str:
    import jwt as pyjwt  # noqa: PLC0415

    return pyjwt.encode(claims, private_key, algorithm="RS256")


def _human_claims(
    oid: str = FAKE_OID_HUMAN, roles: list[str] | None = None
) -> dict[str, Any]:
    """Claims for a delegated human token (has scp → user branch, is_service=False)."""
    now = int(time.time())
    claims: dict[str, Any] = {
        "oid": oid,
        "tid": FAKE_TENANT_ID,
        "scp": "access_as_user",
        "aud": FAKE_CLIENT_ID,
        "iss": FAKE_ISSUER,
        "exp": now + 3600,
        "iat": now - 10,
    }
    if roles:
        claims["roles"] = roles
    return claims


def _service_claims(roles: list[str], appid: str = FAKE_APPID) -> dict[str, Any]:
    """Claims for a service/app token (no scp → service branch, is_service=True)."""
    now = int(time.time())
    return {
        "oid": FAKE_OID_SERVICE,
        "tid": FAKE_TENANT_ID,
        "aud": FAKE_CLIENT_ID,
        "iss": FAKE_ISSUER,
        "exp": now + 3600,
        "iat": now - 10,
        "appid": appid,
        "roles": roles,
        # Deliberately NO "scp" — service / app-only token
    }


# ---------------------------------------------------------------------------
# Mock infrastructure for route backends
# ---------------------------------------------------------------------------


class _MockBlobStore:
    """Mock for AsyncDiskBlobStore — returns empty list, never touches filesystem."""

    def __init__(self, root: Any) -> None:
        pass

    async def list(self, session_id: str) -> list[str]:
        return []

    async def read(self, uri: str) -> Any:
        raise FileNotFoundError(f"mock blob store: not found: {uri}")


class _MockNeo4jResult:
    """Async-iterable result mock that yields a fixed list of rows."""

    def __init__(self, rows: list[dict[str, Any]] | None = None) -> None:
        self._rows = list(rows or [])
        self._index = 0

    def __aiter__(self) -> "_MockNeo4jResult":
        return self

    async def __anext__(self) -> dict[str, Any]:
        if self._index >= len(self._rows):
            raise StopAsyncIteration
        row = self._rows[self._index]
        self._index += 1
        return row


class _MockNeo4jSession:
    """Async context-manager session mock."""

    def __init__(self, rows: list[dict[str, Any]] | None = None) -> None:
        self._rows = rows

    async def run(self, query: str, params: dict[str, Any]) -> _MockNeo4jResult:
        return _MockNeo4jResult(self._rows)

    async def __aenter__(self) -> "_MockNeo4jSession":
        return self

    async def __aexit__(self, *args: object) -> None:
        pass


class _MockNeo4jDriver:
    """Driver mock; delegates to a single _MockNeo4jSession.

    Accepts (and ignores) ``default_access_mode`` so it stays compatible with
    the two-client split's ``driver.session(default_access_mode=...)`` call
    in ``post_cypher`` (main.py).
    """

    def __init__(self, rows: list[dict[str, Any]] | None = None) -> None:
        self._rows = rows

    def session(self, default_access_mode: str | None = None) -> _MockNeo4jSession:
        return _MockNeo4jSession(self._rows)


# ---------------------------------------------------------------------------
# Module-scoped RSA keypair (generated once, shared across all cap tests)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def rsa_keypair_cap() -> tuple[Any, Any]:
    """Generate a 2048-bit RSA keypair once per module (expensive)."""
    from cryptography.hazmat.primitives.asymmetric import rsa  # noqa: PLC0415

    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    public_key = private_key.public_key()
    return private_key, public_key


# ---------------------------------------------------------------------------
# Fixture: service-configured entra ASGI app
# ---------------------------------------------------------------------------


@pytest.fixture
async def service_asgi(
    rsa_keypair_cap: tuple[Any, Any],
    tmp_path: Any,
) -> AsyncGenerator[tuple[Any, Any], None]:
    """Create a fresh create_asgi_app configured with M2 service roles.

    Returns (private_key, asgi_app) so tests can sign tokens and make requests.
    Roles: service_data_role="Contributor", reader_role="Reader",
           entra_admin_role="IdentityAdmin".
    Identity map: FAKE_OID_HUMAN → FAKE_CONTRIBUTOR_HUMAN.
    """
    from context_intelligence_server.config import Settings  # noqa: PLC0415
    from context_intelligence_server.main import app, create_asgi_app  # noqa: PLC0415
    from context_intelligence_server.routers.skills import SkillRegistry  # noqa: PLC0415

    private_key, public_key = rsa_keypair_cap

    settings = Settings(
        auth_mode="entra",
        azure_client_id=FAKE_CLIENT_ID,
        azure_tenant_id=FAKE_TENANT_ID,
        entra_identities={
            FAKE_OID_HUMAN: {"id": FAKE_CONTRIBUTOR_HUMAN},
        },
        service_data_role="Contributor",
        reader_role="Reader",
        entra_admin_role="IdentityAdmin",
        entra_identities_store_path=str(tmp_path / "entra-identities.json"),
        api_keys_store_path=str(tmp_path / "api-keys.json"),
    )

    if not hasattr(app.state, "skill_registry"):
        app.state.skill_registry = SkillRegistry()

    asgi = create_asgi_app(
        settings=settings,
        _jwks_client=_StubJWKSClient(public_key),
    )
    yield private_key, asgi


# ---------------------------------------------------------------------------
# Helper: create an httpx client for the given ASGI app
# ---------------------------------------------------------------------------


def _make_client(asgi: Any) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=asgi),
        base_url="http://test",
    )


# ---------------------------------------------------------------------------
# §7 capability / route-dep tests
# ---------------------------------------------------------------------------


class TestM2CapabilityDeps:
    """§7 CAP-* route-level capability tests.

    All tests use the service_asgi fixture which creates a fresh ASGI app
    with service roles configured.  Tokens are signed with the module-scoped
    RSA keypair and validated by the stub JWKS client.
    """

    # -- CAP-HW: human principal → POST /events → 2xx ----------------------

    async def test_cap_hw_human_write_capable(
        self,
        service_asgi: tuple[Any, Any],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """CAP-HW: human (is_service=False) → POST /events → 2xx regardless of roles."""
        import context_intelligence_server.main as main_module  # noqa: PLC0415

        private_key, asgi = service_asgi
        token = _sign_jwt(private_key, _human_claims())

        monkeypatch.setattr(
            main_module.registry, "get_or_create", lambda *a, **kw: MagicMock()
        )
        monkeypatch.setattr(main_module.registry.queue_manager, "append", AsyncMock())

        async with _make_client(asgi) as c:
            resp = await c.post(
                "/events",
                json={
                    "event": "tool_use",
                    "workspace": "/ws",
                    "data": {
                        "session_id": "cap-hw-1",
                        "timestamp": "2026-06-16T20:00:00+00:00",
                    },
                },
                headers={"Authorization": f"Bearer {token}"},
            )
        assert resp.status_code == 202, (
            f"CAP-HW: expected 202 for human token, got {resp.status_code}: {resp.text}"
        )

    # -- CAP-SC: service Contributor → POST /events → 2xx ------------------

    async def test_cap_sc_contributor_write_capable(
        self,
        service_asgi: tuple[Any, Any],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """CAP-SC: service Contributor → POST /events → 2xx."""
        import context_intelligence_server.main as main_module  # noqa: PLC0415

        private_key, asgi = service_asgi
        token = _sign_jwt(private_key, _service_claims(roles=["Contributor"]))

        monkeypatch.setattr(
            main_module.registry, "get_or_create", lambda *a, **kw: MagicMock()
        )
        monkeypatch.setattr(main_module.registry.queue_manager, "append", AsyncMock())

        async with _make_client(asgi) as c:
            resp = await c.post(
                "/events",
                json={
                    "event": "tool_use",
                    "workspace": "/ws",
                    "data": {
                        "session_id": "cap-sc-1",
                        "timestamp": "2026-06-16T20:00:00+00:00",
                    },
                },
                headers={"Authorization": f"Bearer {token}"},
            )
        assert resp.status_code == 202, (
            f"CAP-SC: expected 202 for Contributor token, got {resp.status_code}: {resp.text}"
        )

    # -- CAP-SR-w: service Reader → POST /events → 403 ---------------------

    async def test_cap_sr_w_reader_write_blocked(
        self,
        service_asgi: tuple[Any, Any],
    ) -> None:
        """CAP-SR-w: service Reader → POST /events → 403 (require_write fails)."""
        private_key, asgi = service_asgi
        token = _sign_jwt(private_key, _service_claims(roles=["Reader"]))

        async with _make_client(asgi) as c:
            resp = await c.post(
                "/events",
                json={
                    "event": "tool_use",
                    "workspace": "/ws",
                    "data": {
                        "session_id": "cap-sr-w-1",
                        "timestamp": "2026-06-16T20:00:00+00:00",
                    },
                },
                headers={"Authorization": f"Bearer {token}"},
            )
        assert resp.status_code == 403, (
            f"CAP-SR-w: expected 403 for Reader on /events, got {resp.status_code}: {resp.text}"
        )

    # -- CAP-SR-r: service Reader → GET /blobs/{sid} → 2xx ----------------

    async def test_cap_sr_r_reader_read_capable(
        self,
        service_asgi: tuple[Any, Any],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """CAP-SR-r: service Reader → GET /blobs/{sid} → 2xx (require_read passes)."""
        import context_intelligence_server.main as main_module  # noqa: PLC0415

        private_key, asgi = service_asgi
        token = _sign_jwt(private_key, _service_claims(roles=["Reader"]))

        monkeypatch.setattr(main_module, "AsyncDiskBlobStore", _MockBlobStore)

        async with _make_client(asgi) as c:
            resp = await c.get(
                "/blobs/cap-sr-r-session",
                headers={"Authorization": f"Bearer {token}"},
            )
        assert resp.status_code == 200, (
            f"CAP-SR-r: expected 200 for Reader on /blobs, got {resp.status_code}: {resp.text}"
        )

    # -- CAP-SADM-w: service IdentityAdmin only → POST /events → 403 -------

    async def test_cap_sadm_w_admin_only_write_blocked(
        self,
        service_asgi: tuple[Any, Any],
    ) -> None:
        """CAP-SADM-w: service IdentityAdmin (no Contributor) → POST /events → 403."""
        private_key, asgi = service_asgi
        token = _sign_jwt(private_key, _service_claims(roles=["IdentityAdmin"]))

        async with _make_client(asgi) as c:
            resp = await c.post(
                "/events",
                json={
                    "event": "tool_use",
                    "workspace": "/ws",
                    "data": {
                        "session_id": "cap-sadm-w-1",
                        "timestamp": "2026-06-16T20:00:00+00:00",
                    },
                },
                headers={"Authorization": f"Bearer {token}"},
            )
        assert resp.status_code == 403, (
            f"CAP-SADM-w: expected 403 for IdentityAdmin-only on /events, "
            f"got {resp.status_code}: {resp.text}"
        )

    # -- CAP-SADM-a: service IdentityAdmin → /admin/* → 2xx ----------------

    async def test_cap_sadm_a_admin_role_passes_require_admin(
        self,
        service_asgi: tuple[Any, Any],
    ) -> None:
        """CAP-SADM-a: service IdentityAdmin → GET /admin/identities → 2xx.

        require_admin (admin.py) checks 'IdentityAdmin' in roles; the service
        branch sets is_service=True and roles=["IdentityAdmin"] on scope state.
        No change to admin.py needed (Q4 / §1.4).
        """
        private_key, asgi = service_asgi
        token = _sign_jwt(private_key, _service_claims(roles=["IdentityAdmin"]))

        async with _make_client(asgi) as c:
            resp = await c.get(
                "/admin/identities",
                headers={"Authorization": f"Bearer {token}"},
            )
        assert resp.status_code == 200, (
            f"CAP-SADM-a: expected 200 for IdentityAdmin on /admin/identities, "
            f"got {resp.status_code}: {resp.text}"
        )
        data = resp.json()
        assert "identities" in data

    # -- CAP-SC-r: service Contributor → GET /blobs/{sid} → 2xx -----------

    async def test_cap_sc_r_contributor_read_capable(
        self,
        service_asgi: tuple[Any, Any],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """CAP-SC-r: service Contributor → GET /blobs/{sid} → 2xx (write-capable → read-capable)."""
        import context_intelligence_server.main as main_module  # noqa: PLC0415

        private_key, asgi = service_asgi
        token = _sign_jwt(private_key, _service_claims(roles=["Contributor"]))

        monkeypatch.setattr(main_module, "AsyncDiskBlobStore", _MockBlobStore)

        async with _make_client(asgi) as c:
            resp = await c.get(
                "/blobs/cap-sc-r-session",
                headers={"Authorization": f"Bearer {token}"},
            )
        assert resp.status_code == 200, (
            f"CAP-SC-r: expected 200 for Contributor on /blobs, "
            f"got {resp.status_code}: {resp.text}"
        )

    # -- CAP-cypher-read: service Reader → POST /cypher (read query) → 2xx -

    async def test_cap_cypher_read_reader_can_reach_cypher(
        self,
        service_asgi: tuple[Any, Any],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """CAP-cypher-read: service Reader → POST /cypher (read query) → 2xx.

        /cypher is wired with require_read (NOT require_write per spec §5.3).
        Reader is exactly the role meant to reach /cypher + /blobs.
        """
        from context_intelligence_server.main import app  # noqa: PLC0415

        private_key, asgi = service_asgi
        token = _sign_jwt(private_key, _service_claims(roles=["Reader"]))

        # Mock the neo4j QUERY (read-intent) driver on app.state -- /cypher reads
        # app.state.neo4j_query_driver + neo4j_query_access_mode (two-client
        # split, doc 12), not the admin neo4j_driver. (raising=False since
        # lifespan didn't run.)
        mock_driver = _MockNeo4jDriver(rows=[{"n": {"label": "Session"}}])
        monkeypatch.setattr(app.state, "neo4j_query_driver", mock_driver, raising=False)
        monkeypatch.setattr(app.state, "neo4j_query_access_mode", "READ", raising=False)

        async with _make_client(asgi) as c:
            resp = await c.post(
                "/cypher",
                json={"query": "MATCH (n:Session) RETURN n LIMIT 5"},
                headers={"Authorization": f"Bearer {token}"},
            )
        assert resp.status_code == 200, (
            f"CAP-cypher-read: expected 200 for Reader on /cypher, "
            f"got {resp.status_code}: {resp.text}"
        )

    # -- CAP-cypher-soft: service Reader + MUTATING Cypher → 2xx (SOFT-M2) --

    async def test_cap_cypher_soft_reader_can_mutate_soft_m2(
        self,
        service_asgi: tuple[Any, Any],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """CAP-cypher-soft: service Reader + MUTATING Cypher query → 2xx + mutation succeeds.

        SOFT-M2: at M2, /cypher uses require_read not require_write.  A Reader can
        technically issue a mutating Cypher query and it will execute.  This is the
        consciously-accepted soft gap: Neo4j Community is single-user and does not
        support read-only DB users.  This test pins the accepted soft behavior.

        HARDENS TO 403 AT M3 via a read-only Neo4j DB user on the read path.
        Do NOT assert "cannot mutate" — that assertion would be false at M2.
        """
        from context_intelligence_server.main import app  # noqa: PLC0415

        private_key, asgi = service_asgi
        token = _sign_jwt(private_key, _service_claims(roles=["Reader"]))

        # Mock the neo4j QUERY (read-intent) driver — /cypher reads
        # app.state.neo4j_query_driver (two-client split, doc 12). The
        # "mutation" is accepted by the mock (no real DB).
        mock_driver = _MockNeo4jDriver(rows=[])
        monkeypatch.setattr(app.state, "neo4j_query_driver", mock_driver, raising=False)
        monkeypatch.setattr(app.state, "neo4j_query_access_mode", "READ", raising=False)

        # A MUTATING Cypher query — at M2 this is allowed through require_read
        mutating_query = "CREATE (n:M2SoftGapNode {id: 'soft-gap-test'}) RETURN n"

        async with _make_client(asgi) as c:
            resp = await c.post(
                "/cypher",
                json={"query": mutating_query},
                headers={"Authorization": f"Bearer {token}"},
            )

        # SOFT-M2: 2xx — Reader can reach /cypher and mutations are not blocked at M2.
        # This test exists to document and PIN the accepted gap, NOT to prove "can mutate".
        # At M3: this will harden to 403 via a read-only Neo4j DB user on the read path.
        assert resp.status_code == 200, (
            f"CAP-cypher-soft (SOFT-M2): expected 200 for Reader mutating /cypher at M2, "
            f"got {resp.status_code}: {resp.text}"
        )
        # Verify the response is valid JSON (the mock returns empty results)
        data = resp.json()
        assert "results" in data


# ---------------------------------------------------------------------------
# Additional constants for B4 / P3 tests
# ---------------------------------------------------------------------------

# An oid used in both maps to trigger the B4 overlap guard
_OVERLAP_OID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"

# Friendly name assigned to FAKE_OID_SERVICE via service_identities (P3 mapped test)
_FAKE_SERVICE_FRIENDLY_NAME = "my-automation-service"


# ---------------------------------------------------------------------------
# Additional fixture: service app with service_identities mapped for P3
# ---------------------------------------------------------------------------


@pytest.fixture
async def service_asgi_with_map(
    rsa_keypair_cap: tuple[Any, Any],
    tmp_path: Any,
) -> AsyncGenerator[tuple[Any, Any], None]:
    """ASGI app with service_identities: FAKE_OID_SERVICE → _FAKE_SERVICE_FRIENDLY_NAME.

    Used by the P3 mapped-oid vertical slice test.  FAKE_OID_HUMAN stays in
    entra_identities and FAKE_OID_SERVICE goes into service_identities so the
    two maps are disjoint and B4 does not fire.
    """
    from context_intelligence_server.config import Settings  # noqa: PLC0415
    from context_intelligence_server.main import app, create_asgi_app  # noqa: PLC0415
    from context_intelligence_server.routers.skills import SkillRegistry  # noqa: PLC0415

    private_key, public_key = rsa_keypair_cap

    settings = Settings(
        auth_mode="entra",
        azure_client_id=FAKE_CLIENT_ID,
        azure_tenant_id=FAKE_TENANT_ID,
        entra_identities={
            FAKE_OID_HUMAN: {"id": FAKE_CONTRIBUTOR_HUMAN},
        },
        service_identities={
            FAKE_OID_SERVICE: {"id": _FAKE_SERVICE_FRIENDLY_NAME},
        },
        service_data_role="Contributor",
        reader_role="Reader",
        entra_admin_role="IdentityAdmin",
        entra_identities_store_path=str(tmp_path / "entra-identities-map.json"),
        api_keys_store_path=str(tmp_path / "api-keys-map.json"),
    )

    if not hasattr(app.state, "skill_registry"):
        app.state.skill_registry = SkillRegistry()

    asgi = create_asgi_app(settings=settings, _jwks_client=_StubJWKSClient(public_key))
    yield private_key, asgi


# ---------------------------------------------------------------------------
# B4 — boot disjointness invariant
# ---------------------------------------------------------------------------


class TestBootDisjointnessInvariant:
    """B4: oid appearing in both entra_identities and service_identities → RuntimeError."""

    def test_overlapping_oid_raises_runtime_error(
        self,
        rsa_keypair_cap: tuple[Any, Any],
        tmp_path: Any,
    ) -> None:
        """B4: oid in both maps → RuntimeError naming the oid at boot.

        The server must refuse to start rather than silently mis-routing a
        token that matches the wrong identity source.
        """
        from context_intelligence_server.config import Settings  # noqa: PLC0415
        from context_intelligence_server.main import app, create_asgi_app  # noqa: PLC0415
        from context_intelligence_server.routers.skills import SkillRegistry  # noqa: PLC0415

        _, public_key = rsa_keypair_cap

        settings = Settings(
            auth_mode="entra",
            azure_client_id=FAKE_CLIENT_ID,
            azure_tenant_id=FAKE_TENANT_ID,
            entra_identities={_OVERLAP_OID: {"id": "human-user"}},
            service_identities={_OVERLAP_OID: {"id": "service-principal"}},
            entra_identities_store_path=str(tmp_path / "entra-identities-b4.json"),
            api_keys_store_path=str(tmp_path / "api-keys-b4.json"),
        )

        if not hasattr(app.state, "skill_registry"):
            app.state.skill_registry = SkillRegistry()

        with pytest.raises(RuntimeError, match=_OVERLAP_OID):
            create_asgi_app(settings=settings, _jwks_client=_StubJWKSClient(public_key))

    def test_disjoint_config_boots_cleanly(
        self,
        rsa_keypair_cap: tuple[Any, Any],
        tmp_path: Any,
    ) -> None:
        """B4 negative: disjoint entra + service oids → create_asgi_app succeeds."""
        from context_intelligence_server.config import Settings  # noqa: PLC0415
        from context_intelligence_server.main import app, create_asgi_app  # noqa: PLC0415
        from context_intelligence_server.routers.skills import SkillRegistry  # noqa: PLC0415

        _, public_key = rsa_keypair_cap

        settings = Settings(
            auth_mode="entra",
            azure_client_id=FAKE_CLIENT_ID,
            azure_tenant_id=FAKE_TENANT_ID,
            entra_identities={FAKE_OID_HUMAN: {"id": FAKE_CONTRIBUTOR_HUMAN}},
            service_identities={FAKE_OID_SERVICE: {"id": "my-service"}},
            service_data_role="Contributor",
            reader_role="Reader",
            entra_admin_role="IdentityAdmin",
            entra_identities_store_path=str(tmp_path / "entra-ids-disjoint.json"),
            api_keys_store_path=str(tmp_path / "api-keys-disjoint.json"),
        )

        if not hasattr(app.state, "skill_registry"):
            app.state.skill_registry = SkillRegistry()

        # Must not raise — disjoint config is valid
        create_asgi_app(settings=settings, _jwks_client=_StubJWKSClient(public_key))


# ---------------------------------------------------------------------------
# /status additive fields (M2)
# ---------------------------------------------------------------------------


class TestStatusAdditiveFields:
    """/status includes reader_role and service_data_role in entra mode (additive only)."""

    async def test_status_includes_reader_role_and_service_data_role(
        self,
        service_asgi: tuple[Any, Any],
    ) -> None:
        """Additive: /status auth block includes reader_role and service_data_role.

        Existing fields (mode, admin_api_enabled, entra_admin_role) must remain
        untouched — this is an additive-only change.

        /status is exempt from auth (Azure Container Apps liveness/health probe),
        but this request still carries a valid human bearer token to prove /status
        does not REJECT an authenticated principal either (any authenticated
        principal passes; /status has no capability dependency).
        """
        private_key, asgi = service_asgi
        token = _sign_jwt(private_key, _human_claims())

        async with _make_client(asgi) as c:
            resp = await c.get("/status", headers={"Authorization": f"Bearer {token}"})

        assert resp.status_code == 200
        data = resp.json()
        auth = data.get("auth", {})

        # New M2 fields
        assert auth.get("reader_role") == "Reader", (
            f"Expected reader_role='Reader' in /status auth, got {auth!r}"
        )
        assert auth.get("service_data_role") == "Contributor", (
            f"Expected service_data_role='Contributor' in /status auth, got {auth!r}"
        )
        # Additive-only: pre-existing fields must still be present
        assert "mode" in auth, f"/status auth missing 'mode': {auth!r}"
        assert "admin_api_enabled" in auth, (
            f"/status auth missing 'admin_api_enabled': {auth!r}"
        )
        assert "entra_admin_role" in auth, (
            f"/status auth missing 'entra_admin_role': {auth!r}"
        )


# ---------------------------------------------------------------------------
# P3 end — vertical-slice proof (resolver → middleware → require_write → queue)
# ---------------------------------------------------------------------------


class TestP3VerticalSlice:
    """P3: offline end-to-end proof of the full service-auth pipe.

    Uses real RS256 tokens, the stub JWKS client, and httpx ASGITransport.
    Captures queue-append bytes to assert the final created_by value.
    """

    async def test_p3_unmapped_service_contributor_created_by_is_appid(
        self,
        service_asgi: tuple[Any, Any],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """P3 unmapped: service Contributor, oid NOT in any map → 202, created_by == appid.

        service_asgi has no service_identities, so FAKE_OID_SERVICE is unmapped.
        The resolver falls through to appid as the stable created_by.
        """
        import context_intelligence_server.main as main_module  # noqa: PLC0415

        private_key, asgi = service_asgi
        token = _sign_jwt(
            private_key, _service_claims(roles=["Contributor"], appid=FAKE_APPID)
        )

        captured: list[bytes] = []

        async def _fake_append(worker_key: str, raw: bytes) -> None:
            captured.append(raw)

        monkeypatch.setattr(
            main_module.registry, "get_or_create", lambda *a, **kw: MagicMock()
        )
        monkeypatch.setattr(main_module.registry.queue_manager, "append", _fake_append)

        async with _make_client(asgi) as c:
            resp = await c.post(
                "/events",
                json={
                    "event": "tool_use",
                    "workspace": "/ws",
                    "data": {
                        "session_id": "p3-unmapped-1",
                        "timestamp": "2026-06-16T20:00:00+00:00",
                    },
                },
                headers={"Authorization": f"Bearer {token}"},
            )

        assert resp.status_code == 202, (
            f"P3-unmapped: expected 202 for service Contributor, "
            f"got {resp.status_code}: {resp.text}"
        )
        assert len(captured) == 1, (
            f"P3-unmapped: expected 1 queue append, got {len(captured)}"
        )
        body_obj = json.loads(captured[0])
        assert body_obj["created_by"] == FAKE_APPID, (
            f"P3-unmapped: created_by should be appid {FAKE_APPID!r}, "
            f"got {body_obj.get('created_by')!r}"
        )

    async def test_p3_mapped_service_oid_created_by_is_friendly_name(
        self,
        service_asgi_with_map: tuple[Any, Any],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """P3 mapped: service Contributor, oid IN service_identities → 202, created_by == friendly name.

        service_asgi_with_map maps FAKE_OID_SERVICE → _FAKE_SERVICE_FRIENDLY_NAME.
        The resolver picks the mapped friendly name over appid/azp/oid.
        """
        import context_intelligence_server.main as main_module  # noqa: PLC0415

        private_key, asgi = service_asgi_with_map
        token = _sign_jwt(
            private_key, _service_claims(roles=["Contributor"], appid=FAKE_APPID)
        )

        captured: list[bytes] = []

        async def _fake_append(worker_key: str, raw: bytes) -> None:
            captured.append(raw)

        monkeypatch.setattr(
            main_module.registry, "get_or_create", lambda *a, **kw: MagicMock()
        )
        monkeypatch.setattr(main_module.registry.queue_manager, "append", _fake_append)

        async with _make_client(asgi) as c:
            resp = await c.post(
                "/events",
                json={
                    "event": "tool_use",
                    "workspace": "/ws",
                    "data": {
                        "session_id": "p3-mapped-1",
                        "timestamp": "2026-06-16T20:00:00+00:00",
                    },
                },
                headers={"Authorization": f"Bearer {token}"},
            )

        assert resp.status_code == 202, (
            f"P3-mapped: expected 202 for service Contributor, "
            f"got {resp.status_code}: {resp.text}"
        )
        assert len(captured) == 1, (
            f"P3-mapped: expected 1 queue append, got {len(captured)}"
        )
        body_obj = json.loads(captured[0])
        assert body_obj["created_by"] == _FAKE_SERVICE_FRIENDLY_NAME, (
            f"P3-mapped: created_by should be {_FAKE_SERVICE_FRIENDLY_NAME!r}, "
            f"got {body_obj.get('created_by')!r}"
        )


# ---------------------------------------------------------------------------
# Phase 4 — self-enforcing route-enumeration guard (rewritten)
# ---------------------------------------------------------------------------


class TestPhase4RouteGuard:
    """Phase 4: every mutating route must carry a capability/admin dependency.

    Iterates EVERY APIRoute in the app and asserts that any route whose
    methods include a mutating verb (POST/PUT/DELETE/PATCH) is covered by:
      - require_write or require_read in route.dependencies, OR
      - require_admin in route.dependencies (admin-router routes), OR
      - an entry in _EXEMPT_MUTATING (tiny, commented, justified).

    This test FAILS before the queues.py fix (purge/replay ungated) — proving
    the hole — and passes after.  A future unguarded mutating route will trip
    it immediately with a message naming the offending path.
    """

    # Routes with MUTATING verbs that are INTENTIONALLY exempt from capability
    # gating.  This set must remain SMALL and every entry must have an
    # explicit comment.  Silent additions are the failure mode this test exists
    # to prevent.  Currently: empty — all mutating routes must be capability-
    # or admin-gated.
    _EXEMPT_MUTATING: frozenset[tuple[str, str]] = frozenset()

    # Data-exposing GET routes that MUST carry a read-capability gate.
    # (Best-effort, per §2 of the security fix spec.)
    _REQUIRED_READ_GATED: frozenset[tuple[str, str]] = frozenset(
        {
            ("GET", "/queues/dead-letter"),
        }
    )

    def test_all_mutating_routes_have_capability_or_admin_dep(self) -> None:
        """Every POST/PUT/DELETE/PATCH route must have a capability gate.

        For each mutating route:
          - /admin/* routes are covered by require_admin (router-level dep).
          - _EXEMPT_MUTATING entries are explicitly excepted (rationale above).
          - All others: require_write or require_read in route.dependencies.

        FAILS before the queues.py fix (purge/replay are ungated).  Any future
        unguarded mutating route trips this test with a descriptive message.
        """
        from fastapi.routing import APIRoute  # noqa: PLC0415

        from context_intelligence_server.authz import require_read, require_write  # noqa: PLC0415
        from context_intelligence_server.main import app  # noqa: PLC0415
        from context_intelligence_server.routers.admin import require_admin  # noqa: PLC0415

        _MUTATING_VERBS = {"POST", "PUT", "DELETE", "PATCH"}

        unguarded: list[str] = []

        for route in app.routes:
            if not isinstance(route, APIRoute):
                continue
            for method in route.methods or set():
                if method.upper() not in _MUTATING_VERBS:
                    continue

                spec = (method.upper(), route.path)
                if spec in self._EXEMPT_MUTATING:
                    continue  # explicitly excepted — rationale required above

                dep_callables = {dep.dependency for dep in route.dependencies}

                # Admin-gated: require_admin injected by the admin router
                if require_admin in dep_callables:
                    continue

                # Capability-gated: require_write or require_read on this route
                if require_write in dep_callables or require_read in dep_callables:
                    continue

                unguarded.append(
                    f"  {method.upper()} {route.path}"
                    f"  — no require_write / require_read / require_admin"
                )

        assert not unguarded, (
            "SECURITY: the following mutating route(s) have NO capability/admin "
            "dependency.\nA Reader-only service token can call these endpoints:\n"
            + "\n".join(sorted(unguarded))
            + "\n\nFix: add Depends(require_write) (or require_admin for admin routes) "
            "to each route listed above."
        )

    def test_data_read_routes_have_read_gate(self) -> None:
        """Data-exposing GET routes in _REQUIRED_READ_GATED carry a read gate.

        Step 3 (doc 16 W2): dead-letter LIST is open to any authenticated
        principal, so it is gated by require_read (NOT require_admin — the
        destructive purge/replay mutations are the admin-tier routes, and they
        relocated to POST /admin/queues/dead-letter/* where the mutating-route
        guard below covers them). This asserts the TRUE gate on the list route:
        require_read/require_write, deliberately NOT accepting require_admin.
        """
        from fastapi.routing import APIRoute  # noqa: PLC0415

        from context_intelligence_server.authz import require_read, require_write  # noqa: PLC0415
        from context_intelligence_server.main import app  # noqa: PLC0415

        route_map: dict[tuple[str, str], APIRoute] = {}
        for route in app.routes:
            if not isinstance(route, APIRoute):
                continue
            for method in route.methods or set():
                route_map[(method.upper(), route.path)] = route

        unguarded: list[str] = []
        for spec in self._REQUIRED_READ_GATED:
            method, path = spec
            route = route_map.get(spec)
            if route is None:
                unguarded.append(f"  {method} {path}  — route not found in app")
                continue
            dep_callables = {dep.dependency for dep in route.dependencies}
            if require_write not in dep_callables and require_read not in dep_callables:
                unguarded.append(
                    f"  {method} {path}  — missing require_read/require_write"
                )

        assert not unguarded, (
            "Data-exposing read route(s) have no read-capability gate:\n"
            + "\n".join(sorted(unguarded))
        )


# ---------------------------------------------------------------------------
# 403 message-content assertions
# ---------------------------------------------------------------------------


class TestM2MessageContent:
    """403 responses name the missing roles so operators can diagnose quickly."""

    async def test_no_role_403_names_contributor_and_reader_roles(
        self,
        service_asgi: tuple[Any, Any],
    ) -> None:
        """Resolver 403: names required App Roles + rejected appid; NO raw roles echo.

        Security R1: the raw ``roles=[...]`` list must NOT appear in the response
        body (it is an internal claim value, not operator guidance).
        User-advocate: the message must NAME the rejected service principal
        (appid) and the required App Roles so the operator knows what to assign.
        """
        private_key, asgi = service_asgi
        # _service_claims uses FAKE_APPID as default appid
        token = _sign_jwt(private_key, _service_claims(roles=[]))

        async with _make_client(asgi) as c:
            resp = await c.post(
                "/events",
                json={
                    "event": "tool_use",
                    "workspace": "/ws",
                    "data": {
                        "session_id": "msg-no-role-1",
                        "timestamp": "2026-06-16T20:00:00+00:00",
                    },
                },
                headers={"Authorization": f"Bearer {token}"},
            )

        assert resp.status_code == 403
        detail = resp.json().get("detail", "")
        # Required role names must appear so operator knows what to assign.
        assert "Contributor" in detail, (
            f"Resolver 403 must name the Contributor role in detail: {detail!r}"
        )
        assert "Reader" in detail, (
            f"Resolver 403 must name the Reader role in detail: {detail!r}"
        )
        # The rejected service principal (appid) must be named.
        assert FAKE_APPID in detail, (
            f"Resolver 403 must name the rejected appid {FAKE_APPID!r}: {detail!r}"
        )
        # Raw roles list must NOT be echoed in the response body (security R1).
        assert "roles=" not in detail, (
            f"Resolver 403 must NOT echo the raw roles claim: {detail!r}"
        )

    async def test_require_write_403_names_write_role(
        self,
        service_asgi: tuple[Any, Any],
    ) -> None:
        """Service Reader → POST /events → require_write 403 naming 'Contributor'.

        Reader passes the resolver (qualifying role), but require_write rejects
        it because Contributor is absent.  The 403 detail must name Contributor.
        """
        private_key, asgi = service_asgi
        token = _sign_jwt(private_key, _service_claims(roles=["Reader"]))

        async with _make_client(asgi) as c:
            resp = await c.post(
                "/events",
                json={
                    "event": "tool_use",
                    "workspace": "/ws",
                    "data": {
                        "session_id": "msg-rw-1",
                        "timestamp": "2026-06-16T20:00:00+00:00",
                    },
                },
                headers={"Authorization": f"Bearer {token}"},
            )

        assert resp.status_code == 403
        detail = resp.json().get("detail", "")
        assert "Contributor" in detail, (
            f"require_write 403 must name the Contributor (write) role: {detail!r}"
        )

    async def test_require_read_403_names_reader_and_write_roles(
        self,
        service_asgi: tuple[Any, Any],
    ) -> None:
        """Service IdentityAdmin-only → GET /blobs → require_read 403 naming Reader and Contributor.

        IdentityAdmin passes the resolver but is neither Contributor nor Reader,
        so require_read rejects it.  The 403 detail must name both read roles.
        """
        private_key, asgi = service_asgi
        token = _sign_jwt(private_key, _service_claims(roles=["IdentityAdmin"]))

        async with _make_client(asgi) as c:
            resp = await c.get(
                "/blobs/msg-rr-session",
                headers={"Authorization": f"Bearer {token}"},
            )

        assert resp.status_code == 403
        detail = resp.json().get("detail", "")
        assert "Reader" in detail, (
            f"require_read 403 must name the Reader role: {detail!r}"
        )
        assert "Contributor" in detail, (
            f"require_read 403 must name the Contributor role: {detail!r}"
        )


# ---------------------------------------------------------------------------
# is_service-missing — default-False behavior (pins the deliberate design)
# ---------------------------------------------------------------------------


class TestIsServiceMissingDefaultBehavior:
    """Pin the deliberate behavior of _is_write_capable when is_service is absent.

    FINDING (§3 of security fix):
    When ``is_service`` is absent from request scope state,
    ``_is_write_capable`` defaults to False (human-like) → returns True
    (write-capable).  This is the deliberate design for two safe scenarios:

    1. ``allow_unauthenticated=True`` dev mode (no credentials required).
    2. Auth-exempt paths (/status, /version, /skills/*) — none of which carry
       a capability gate, so _is_write_capable is never called for them.

    IN AUTH-ENABLED PRODUCTION MODE: BearerTokenMiddleware ALWAYS sets
    ``is_service`` on scope state before any handler or dependency runs.
    - StaticKeyResolver: sets is_service=False
    - EntraResolver:     sets is_service=True (service) or False (human)

    Therefore: no auth-enabled request can reach a capability-gated handler
    without is_service having been set.  The default-False path is unreachable
    in production.  These tests PIN that assertion so a future middleware change
    that removes the is_service write cannot silently change the security model.
    """

    def test_is_write_capable_no_is_service_defaults_human_write_capable(
        self,
    ) -> None:
        """is_service absent from scope state → defaults False (human-like) → True.

        Deliberate behavior: missing is_service means non-service principal.
        Only reachable in allow_unauthenticated=True dev/test mode.
        """
        from unittest.mock import MagicMock  # noqa: PLC0415

        from fastapi import Request  # noqa: PLC0415

        from context_intelligence_server.authz import _is_write_capable  # noqa: PLC0415

        mock_request = MagicMock(spec=Request)
        # No "is_service" key — models a scope state where middleware was bypassed
        mock_request.scope = {"state": {"contributor_id": "dev-user"}}
        mock_request.app.state.service_data_role = "Contributor"

        result = _is_write_capable(mock_request)
        assert result is True, (
            "_is_write_capable should return True when is_service is absent "
            "(defaults to human-like: write-capable). "
            "This is the deliberate allow_unauthenticated=True dev behavior."
        )

    def test_is_write_capable_is_service_false_returns_true(self) -> None:
        """is_service=False (human or static) → always write-capable (True)."""
        from unittest.mock import MagicMock  # noqa: PLC0415

        from fastapi import Request  # noqa: PLC0415

        from context_intelligence_server.authz import _is_write_capable  # noqa: PLC0415

        mock_request = MagicMock(spec=Request)
        mock_request.scope = {"state": {"is_service": False, "roles": []}}
        mock_request.app.state.service_data_role = "Contributor"

        assert _is_write_capable(mock_request) is True

    def test_is_write_capable_service_no_role_returns_false(self) -> None:
        """is_service=True, roles=[] → not write-capable (False)."""
        from unittest.mock import MagicMock  # noqa: PLC0415

        from fastapi import Request  # noqa: PLC0415

        from context_intelligence_server.authz import _is_write_capable  # noqa: PLC0415

        mock_request = MagicMock(spec=Request)
        mock_request.scope = {"state": {"is_service": True, "roles": []}}
        mock_request.app.state.service_data_role = "Contributor"

        assert _is_write_capable(mock_request) is False

    def test_is_write_capable_service_with_role_returns_true(self) -> None:
        """is_service=True, roles=["Contributor"] → write-capable (True)."""
        from unittest.mock import MagicMock  # noqa: PLC0415

        from fastapi import Request  # noqa: PLC0415

        from context_intelligence_server.authz import _is_write_capable  # noqa: PLC0415

        mock_request = MagicMock(spec=Request)
        mock_request.scope = {"state": {"is_service": True, "roles": ["Contributor"]}}
        mock_request.app.state.service_data_role = "Contributor"

        assert _is_write_capable(mock_request) is True

    def test_static_resolver_always_sets_is_service_false(self) -> None:
        """StaticKeyResolver sets is_service=False — no auth-enabled path skips it.

        Confirms that the StaticKeyResolver (used in static auth mode) always
        returns is_service=False in its 3-tuple.  BearerTokenMiddleware stores
        this on scope state unconditionally.  So in auth-enabled static mode,
        is_service is always set before any route handler runs.
        """
        import hashlib  # noqa: PLC0415

        from context_intelligence_server.auth import StaticKeyResolver  # noqa: PLC0415

        token = "test-token-for-is-service-check"
        digest = hashlib.sha256(token.encode()).hexdigest()
        resolver = StaticKeyResolver({digest: "contributor"})
        result = resolver.resolve(token)
        assert result is not None
        _cid, _roles, is_service = result
        assert is_service is False, (
            "StaticKeyResolver must always return is_service=False. "
            "BearerTokenMiddleware stores this on scope state — so in static "
            "auth mode, is_service is always set before handlers run."
        )
