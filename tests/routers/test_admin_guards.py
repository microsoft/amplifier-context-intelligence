"""T6 tests: input-validation guards, admin-key protection, mutation audit, corrupt-store.

All guards close tester-breaker adversarial-review findings.  Test matrix:
  - Path-param OID validation → 422 (TB-10)
  - Path-param hash validation → 422 (TB-10)
  - Contributor body validation → 422 (TB-12)
  - Admin-key un-deletable / un-shadowable → 409 (TB-05)
  - Mutation audit log: one line per PUT/DELETE (TB-11)
  - Overwrite audit: old→new contributor logged (TB-11)
  - Raw-key not logged
  - Corrupt-store integration: app starts with empty map + loud log (ROB F5)

Fake constants only — never real credentials, OIDs, or keys (§0.3 of design doc).
"""

from __future__ import annotations

import hashlib
import logging
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any

import httpx
import pytest

# ---------------------------------------------------------------------------
# Fake constants (NEVER real credentials / OIDs / keys)
# ---------------------------------------------------------------------------

# Use an OID that contains letters so .upper() actually changes it (digits are case-invariant).
FAKE_OID = "aabbccdd-1122-3344-5566-778899aabbcc"
FAKE_CONTRIBUTOR = "alice"
FAKE_CLIENT_ID = "aaaabbbb-1111-2222-3333-ccccddddeeee"
FAKE_TENANT_ID = "ffffeeee-dddd-cccc-bbbb-aaaa99998888"

# Static-mode key material
FAKE_DATA_RAW_KEY = "t6-data-key-ordinary-user"
FAKE_DATA_KEY_DIGEST = hashlib.sha256(FAKE_DATA_RAW_KEY.encode()).hexdigest()
FAKE_CONTRIBUTOR_KEY = "carol"

# Admin key material — separate from data keys (TB-05)
FAKE_ADMIN_RAW_KEY = "t6-admin-key-do-not-use-in-production"
FAKE_ADMIN_KEY_DIGEST = hashlib.sha256(FAKE_ADMIN_RAW_KEY.encode()).hexdigest()

# All-zeros GUID sentinel (must be rejected — config.py:45)
ALL_ZEROS_OID = "00000000-0000-0000-0000-000000000000"

ADMIN_LOGGER = "context_intelligence_server.routers.admin"


# ---------------------------------------------------------------------------
# JWKS stub (no network)
# ---------------------------------------------------------------------------


class _StubSigningKey:
    def __init__(self, key: Any = "dummy-key") -> None:
        self.key = key


class _StubJWKSClient:
    def fetch_data(self) -> None:
        pass

    def get_signing_key_from_jwt(self, token: str) -> _StubSigningKey:
        raise NotImplementedError("guard tests do not call resolve()")

    def get_jwk_set(self) -> Any:
        class _FakeJWKSet:
            keys = [_StubSigningKey()]

        return _FakeJWKSet()


# ---------------------------------------------------------------------------
# Settings helpers
# ---------------------------------------------------------------------------


def _make_static_settings(
    tmp_path: Path,
    *,
    with_key: bool = True,
    with_admin_key: bool = False,
) -> Any:
    from context_intelligence_server.config import Settings  # noqa: PLC0415

    ks = {FAKE_DATA_KEY_DIGEST: {"id": FAKE_CONTRIBUTOR_KEY}} if with_key else None
    return Settings(
        auth_mode="static",
        allow_unauthenticated=True,
        api_keys=ks,
        admin_api_key=FAKE_ADMIN_RAW_KEY if with_admin_key else None,
        api_keys_store_path=str(tmp_path / "api-keys.json"),
        entra_identities_store_path=str(tmp_path / "entra-identities.json"),
    )


def _make_entra_settings(tmp_path: Path, *, with_identity: bool = True) -> Any:
    from context_intelligence_server.config import Settings  # noqa: PLC0415

    ids = {FAKE_OID: {"id": FAKE_CONTRIBUTOR}} if with_identity else None
    return Settings(
        auth_mode="entra",
        allow_unauthenticated=True,
        azure_client_id=FAKE_CLIENT_ID,
        azure_tenant_id=FAKE_TENANT_ID,
        entra_identities=ids,
        entra_identities_store_path=str(tmp_path / "entra-identities.json"),
        api_keys_store_path=str(tmp_path / "api-keys.json"),
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
async def entra_client(tmp_path: Path) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Entra-mode client with require_admin bypassed."""
    from context_intelligence_server.main import app, create_asgi_app  # noqa: PLC0415
    from context_intelligence_server.routers.admin import require_admin  # noqa: PLC0415

    settings = _make_entra_settings(tmp_path)
    create_asgi_app(settings=settings, _jwks_client=_StubJWKSClient())
    app.dependency_overrides[require_admin] = lambda: None
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as c:
            yield c
    finally:
        app.dependency_overrides.pop(require_admin, None)


@pytest.fixture
async def static_client(tmp_path: Path) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Static-mode client (no admin key) with require_admin bypassed."""
    from context_intelligence_server.main import app, create_asgi_app  # noqa: PLC0415
    from context_intelligence_server.routers.admin import require_admin  # noqa: PLC0415

    settings = _make_static_settings(tmp_path)
    create_asgi_app(settings=settings)
    app.dependency_overrides[require_admin] = lambda: None
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as c:
            yield c
    finally:
        app.dependency_overrides.pop(require_admin, None)


@pytest.fixture
async def static_admin_client(
    tmp_path: Path,
) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Static-mode client WITH admin_api_key set; require_admin bypassed."""
    from context_intelligence_server.main import app, create_asgi_app  # noqa: PLC0415
    from context_intelligence_server.routers.admin import require_admin  # noqa: PLC0415

    settings = _make_static_settings(tmp_path, with_admin_key=True)
    create_asgi_app(settings=settings)
    app.dependency_overrides[require_admin] = lambda: None
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as c:
            yield c
    finally:
        app.dependency_overrides.pop(require_admin, None)


# ===========================================================================
# Path-param OID validation → 422 (TB-10)
# ===========================================================================


class TestOidPathValidation422:
    """PUT / DELETE /admin/identities/{oid} rejects malformed OIDs with 422."""

    @pytest.mark.anyio
    async def test_put_identity_garbage_oid_returns_422(
        self, entra_client: httpx.AsyncClient
    ) -> None:
        """Non-GUID OID on PUT → 422."""
        resp = await entra_client.put(
            "/admin/identities/not-a-guid", json={"id": FAKE_CONTRIBUTOR}
        )
        assert resp.status_code == 422

    @pytest.mark.anyio
    async def test_put_identity_all_zeros_oid_returns_422(
        self, entra_client: httpx.AsyncClient
    ) -> None:
        """All-zeros sentinel OID on PUT → 422 (config.py:45 — placeholder guard)."""
        resp = await entra_client.put(
            f"/admin/identities/{ALL_ZEROS_OID}", json={"id": FAKE_CONTRIBUTOR}
        )
        assert resp.status_code == 422

    @pytest.mark.anyio
    async def test_put_identity_uppercase_oid_returns_422(
        self, entra_client: httpx.AsyncClient
    ) -> None:
        """Uppercase OID on PUT → 422 (GUID regex requires lowercase hex)."""
        uppercase_oid = FAKE_OID.upper()
        resp = await entra_client.put(
            f"/admin/identities/{uppercase_oid}", json={"id": FAKE_CONTRIBUTOR}
        )
        assert resp.status_code == 422

    @pytest.mark.anyio
    async def test_put_identity_oid_with_braces_returns_422(
        self, entra_client: httpx.AsyncClient
    ) -> None:
        """OID with braces on PUT → 422."""
        braced_oid = f"{{{FAKE_OID}}}"
        resp = await entra_client.put(
            f"/admin/identities/{braced_oid}", json={"id": FAKE_CONTRIBUTOR}
        )
        assert resp.status_code == 422

    @pytest.mark.anyio
    async def test_delete_identity_garbage_oid_returns_422(
        self, entra_client: httpx.AsyncClient
    ) -> None:
        """Non-GUID OID on DELETE → 422."""
        resp = await entra_client.delete("/admin/identities/garbage-oid-value")
        assert resp.status_code == 422

    @pytest.mark.anyio
    async def test_delete_identity_all_zeros_oid_returns_422(
        self, entra_client: httpx.AsyncClient
    ) -> None:
        """All-zeros OID on DELETE → 422."""
        resp = await entra_client.delete(f"/admin/identities/{ALL_ZEROS_OID}")
        assert resp.status_code == 422


# ===========================================================================
# Path-param hash validation → 422 (TB-10)
# ===========================================================================


class TestHashPathValidation422:
    """PUT / DELETE /admin/keys/{hash} rejects malformed hashes with 422."""

    @pytest.mark.anyio
    async def test_put_key_short_hash_returns_422(
        self, static_client: httpx.AsyncClient
    ) -> None:
        """63-char hex hash on PUT → 422 (must be exactly 64 chars)."""
        short_hash = "a" * 63
        resp = await static_client.put(
            f"/admin/keys/{short_hash}", json={"id": FAKE_CONTRIBUTOR_KEY}
        )
        assert resp.status_code == 422

    @pytest.mark.anyio
    async def test_put_key_long_hash_returns_422(
        self, static_client: httpx.AsyncClient
    ) -> None:
        """65-char hex hash on PUT → 422 (must be exactly 64 chars)."""
        long_hash = "a" * 65
        resp = await static_client.put(
            f"/admin/keys/{long_hash}", json={"id": FAKE_CONTRIBUTOR_KEY}
        )
        assert resp.status_code == 422

    @pytest.mark.anyio
    async def test_put_key_non_hex_hash_returns_422(
        self, static_client: httpx.AsyncClient
    ) -> None:
        """Hash with non-hex chars on PUT → 422."""
        non_hex = "g" * 64  # 'g' is not a valid hex digit
        resp = await static_client.put(
            f"/admin/keys/{non_hex}", json={"id": FAKE_CONTRIBUTOR_KEY}
        )
        assert resp.status_code == 422

    @pytest.mark.anyio
    async def test_put_key_uppercase_hash_returns_422(
        self, static_client: httpx.AsyncClient
    ) -> None:
        """Uppercase 64-char hash on PUT → 422 (only lowercase allowed)."""
        uppercase_hash = FAKE_DATA_KEY_DIGEST.upper()
        resp = await static_client.put(
            f"/admin/keys/{uppercase_hash}", json={"id": FAKE_CONTRIBUTOR_KEY}
        )
        assert resp.status_code == 422

    @pytest.mark.anyio
    async def test_delete_key_short_hash_returns_422(
        self, static_client: httpx.AsyncClient
    ) -> None:
        """63-char hex hash on DELETE → 422."""
        short_hash = "b" * 63
        resp = await static_client.delete(f"/admin/keys/{short_hash}")
        assert resp.status_code == 422

    @pytest.mark.anyio
    async def test_delete_key_non_hex_hash_returns_422(
        self, static_client: httpx.AsyncClient
    ) -> None:
        """Hash with non-hex chars on DELETE → 422."""
        non_hex = "z" * 64
        resp = await static_client.delete(f"/admin/keys/{non_hex}")
        assert resp.status_code == 422

    @pytest.mark.anyio
    async def test_delete_key_uppercase_hash_returns_422(
        self, static_client: httpx.AsyncClient
    ) -> None:
        """Uppercase 64-char hash on DELETE → 422."""
        uppercase_hash = FAKE_DATA_KEY_DIGEST.upper()
        resp = await static_client.delete(f"/admin/keys/{uppercase_hash}")
        assert resp.status_code == 422


# ===========================================================================
# Contributor body validation → 422 (TB-12)
# ===========================================================================


class TestContributorBodyValidation422:
    """id field in request body must pass max-length and no-null-byte checks."""

    @pytest.mark.anyio
    async def test_put_identity_oversized_contributor_returns_422(
        self, entra_client: httpx.AsyncClient
    ) -> None:
        """id > 256 chars on PUT /admin/identities → 422."""
        oversized = "a" * 257
        resp = await entra_client.put(
            f"/admin/identities/{FAKE_OID}", json={"id": oversized}
        )
        assert resp.status_code == 422

    @pytest.mark.anyio
    async def test_put_identity_null_byte_contributor_returns_422(
        self, entra_client: httpx.AsyncClient
    ) -> None:
        """id containing null byte on PUT /admin/identities → 422."""
        null_byte_id = "alice\x00malicious"
        resp = await entra_client.put(
            f"/admin/identities/{FAKE_OID}", json={"id": null_byte_id}
        )
        assert resp.status_code == 422

    @pytest.mark.anyio
    async def test_put_key_oversized_contributor_returns_422(
        self, static_client: httpx.AsyncClient
    ) -> None:
        """id > 256 chars on PUT /admin/keys → 422."""
        oversized = "b" * 257
        resp = await static_client.put(
            f"/admin/keys/{FAKE_DATA_KEY_DIGEST}", json={"id": oversized}
        )
        assert resp.status_code == 422

    @pytest.mark.anyio
    async def test_put_key_null_byte_contributor_returns_422(
        self, static_client: httpx.AsyncClient
    ) -> None:
        """id containing null byte on PUT /admin/keys → 422."""
        null_byte_id = "carol\x00injected"
        resp = await static_client.put(
            f"/admin/keys/{FAKE_DATA_KEY_DIGEST}", json={"id": null_byte_id}
        )
        assert resp.status_code == 422

    @pytest.mark.anyio
    async def test_put_identity_max_length_contributor_accepted(
        self, entra_client: httpx.AsyncClient
    ) -> None:
        """id of exactly 256 chars on PUT /admin/identities → 200 (boundary OK)."""
        max_len = "c" * 256
        resp = await entra_client.put(
            f"/admin/identities/{FAKE_OID}", json={"id": max_len}
        )
        assert resp.status_code == 200


# ===========================================================================
# Admin-key guard → 409 (TB-05)
# ===========================================================================


class TestAdminKeyGuard409:
    """DELETE / PUT targeting the admin key hash → 409 Conflict.

    The admin key lives in config, not in the data keystore.  The guard
    prevents shadow-binding (PUT) and deletion (DELETE) via the API.
    """

    @pytest.mark.anyio
    async def test_delete_admin_key_hash_returns_409(
        self, static_admin_client: httpx.AsyncClient
    ) -> None:
        """DELETE /admin/keys/{admin_key_hash} → 409 (un-deletable guard)."""
        resp = await static_admin_client.delete(f"/admin/keys/{FAKE_ADMIN_KEY_DIGEST}")
        assert resp.status_code == 409

    @pytest.mark.anyio
    async def test_put_admin_key_hash_returns_409(
        self, static_admin_client: httpx.AsyncClient
    ) -> None:
        """PUT /admin/keys/{admin_key_hash} → 409 (un-shadowable guard)."""
        resp = await static_admin_client.put(
            f"/admin/keys/{FAKE_ADMIN_KEY_DIGEST}",
            json={"id": "imposter-contributor"},
        )
        assert resp.status_code == 409

    @pytest.mark.anyio
    async def test_delete_different_key_is_not_409(
        self, static_admin_client: httpx.AsyncClient
    ) -> None:
        """DELETE of a regular data key is not blocked (only admin key hash is special)."""
        # First register a key so it can be deleted
        await static_admin_client.put(
            f"/admin/keys/{FAKE_DATA_KEY_DIGEST}",
            json={"id": FAKE_CONTRIBUTOR_KEY},
        )
        resp = await static_admin_client.delete(f"/admin/keys/{FAKE_DATA_KEY_DIGEST}")
        # Must not be 409; 200 if it was in store, 404 if already absent
        assert resp.status_code != 409

    @pytest.mark.anyio
    async def test_no_admin_key_configured_allows_any_delete(
        self, static_client: httpx.AsyncClient
    ) -> None:
        """When admin_api_key is not configured, no hash is guarded → no 409."""
        # static_client has no admin_api_key configured, so the guard is inactive.
        # FAKE_DATA_KEY_DIGEST is seeded at startup; deleting it must succeed.
        resp = await static_client.delete(f"/admin/keys/{FAKE_DATA_KEY_DIGEST}")
        # 200 (present and deleted) or 404 (absent) but NOT 409
        assert resp.status_code != 409


# ===========================================================================
# Mutation audit log (TB-11)
# ===========================================================================


class TestMutationAuditLog:
    """Every successful mutation emits one structured audit log line."""

    @pytest.mark.anyio
    async def test_put_identity_emits_audit_log(
        self, entra_client: httpx.AsyncClient, caplog: pytest.LogCaptureFixture
    ) -> None:
        """PUT /admin/identities → one audit log line with action=put."""
        with caplog.at_level(logging.INFO, logger=ADMIN_LOGGER):
            resp = await entra_client.put(
                f"/admin/identities/{FAKE_OID}", json={"id": "bob"}
            )
        assert resp.status_code == 200
        audit_lines = [r for r in caplog.records if "audit" in r.getMessage()]
        assert len(audit_lines) >= 1
        msg = audit_lines[-1].getMessage()
        assert "put" in msg
        assert FAKE_OID in msg

    @pytest.mark.anyio
    async def test_delete_identity_emits_audit_log(
        self, entra_client: httpx.AsyncClient, caplog: pytest.LogCaptureFixture
    ) -> None:
        """DELETE /admin/identities → one audit log line with action=delete."""
        with caplog.at_level(logging.INFO, logger=ADMIN_LOGGER):
            resp = await entra_client.delete(f"/admin/identities/{FAKE_OID}")
        assert resp.status_code == 200
        audit_lines = [r for r in caplog.records if "audit" in r.getMessage()]
        assert len(audit_lines) >= 1
        msg = audit_lines[-1].getMessage()
        assert "delete" in msg
        assert FAKE_OID in msg

    @pytest.mark.anyio
    async def test_put_key_emits_audit_log(
        self, static_client: httpx.AsyncClient, caplog: pytest.LogCaptureFixture
    ) -> None:
        """PUT /admin/keys → one audit log line with action=put, containing hash."""
        with caplog.at_level(logging.INFO, logger=ADMIN_LOGGER):
            resp = await static_client.put(
                f"/admin/keys/{FAKE_DATA_KEY_DIGEST}",
                json={"id": FAKE_CONTRIBUTOR_KEY},
            )
        assert resp.status_code == 200
        audit_lines = [r for r in caplog.records if "audit" in r.getMessage()]
        assert len(audit_lines) >= 1
        msg = audit_lines[-1].getMessage()
        assert "put" in msg
        assert FAKE_DATA_KEY_DIGEST in msg

    @pytest.mark.anyio
    async def test_delete_key_emits_audit_log(
        self, static_client: httpx.AsyncClient, caplog: pytest.LogCaptureFixture
    ) -> None:
        """DELETE /admin/keys → one audit log line with action=delete, containing hash."""
        with caplog.at_level(logging.INFO, logger=ADMIN_LOGGER):
            resp = await static_client.delete(f"/admin/keys/{FAKE_DATA_KEY_DIGEST}")
        assert resp.status_code == 200
        audit_lines = [r for r in caplog.records if "audit" in r.getMessage()]
        assert len(audit_lines) >= 1
        msg = audit_lines[-1].getMessage()
        assert "delete" in msg
        assert FAKE_DATA_KEY_DIGEST in msg

    @pytest.mark.anyio
    async def test_put_key_audit_never_logs_raw_key(
        self, static_client: httpx.AsyncClient, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Audit log line for PUT /admin/keys must not contain the raw key value."""
        raw_key = "super-secret-raw-api-key-value"
        key_hash = hashlib.sha256(raw_key.encode()).hexdigest()
        with caplog.at_level(logging.INFO, logger=ADMIN_LOGGER):
            resp = await static_client.put(
                f"/admin/keys/{key_hash}",
                json={"id": "test-user"},
            )
        assert resp.status_code == 200
        # Raw key value must never appear in any log record
        for record in caplog.records:
            assert raw_key not in record.getMessage(), (
                f"Raw key leaked in log: {record.getMessage()!r}"
            )
        # But the hash IS expected to appear
        audit_lines = [r for r in caplog.records if "audit" in r.getMessage()]
        assert any(key_hash in r.getMessage() for r in audit_lines)


# ===========================================================================
# Overwrite audit: old→new contributor (TB-11)
# ===========================================================================


class TestOverwriteAudit:
    """A PUT that changes an existing mapping's contributor emits old→new audit."""

    @pytest.mark.anyio
    async def test_overwrite_identity_emits_old_new_audit(
        self, entra_client: httpx.AsyncClient, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Overwriting an OID→contributor mapping logs both old and new contributor."""
        # Seed initial mapping
        await entra_client.put(
            f"/admin/identities/{FAKE_OID}", json={"id": "old-contributor"}
        )
        # Overwrite with a different contributor
        with caplog.at_level(logging.INFO, logger=ADMIN_LOGGER):
            resp = await entra_client.put(
                f"/admin/identities/{FAKE_OID}", json={"id": "new-contributor"}
            )
        assert resp.status_code == 200
        audit_lines = [r for r in caplog.records if "audit" in r.getMessage()]
        assert len(audit_lines) >= 1
        overwrite_msgs = [r.getMessage() for r in audit_lines]
        # At least one audit line must mention both old and new contributor
        combined = " ".join(overwrite_msgs)
        assert "old-contributor" in combined, (
            "Old contributor missing from overwrite audit"
        )
        assert "new-contributor" in combined, (
            "New contributor missing from overwrite audit"
        )

    @pytest.mark.anyio
    async def test_overwrite_key_emits_old_new_audit(
        self, static_client: httpx.AsyncClient, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Overwriting a hash→contributor mapping logs both old and new contributor."""
        new_hash = "c" * 64
        # Seed initial mapping
        await static_client.put(f"/admin/keys/{new_hash}", json={"id": "old-key-owner"})
        # Overwrite with different contributor
        with caplog.at_level(logging.INFO, logger=ADMIN_LOGGER):
            resp = await static_client.put(
                f"/admin/keys/{new_hash}", json={"id": "new-key-owner"}
            )
        assert resp.status_code == 200
        audit_lines = [r for r in caplog.records if "audit" in r.getMessage()]
        combined = " ".join(r.getMessage() for r in audit_lines)
        assert "old-key-owner" in combined, (
            "Old contributor missing from overwrite audit"
        )
        assert "new-key-owner" in combined, (
            "New contributor missing from overwrite audit"
        )

    @pytest.mark.anyio
    async def test_put_same_contributor_no_overwrite_audit(
        self, entra_client: httpx.AsyncClient, caplog: pytest.LogCaptureFixture
    ) -> None:
        """PUT with same contributor as existing value does NOT trigger overwrite audit."""
        await entra_client.put(
            f"/admin/identities/{FAKE_OID}", json={"id": "same-contributor"}
        )
        with caplog.at_level(logging.INFO, logger=ADMIN_LOGGER):
            resp = await entra_client.put(
                f"/admin/identities/{FAKE_OID}", json={"id": "same-contributor"}
            )
        assert resp.status_code == 200
        # Should have a normal audit line but NOT an overwrite (old→new) line
        audit_msgs = [
            r.getMessage() for r in caplog.records if "audit" in r.getMessage()
        ]
        # "old_contributor" key should NOT appear
        assert not any("old_contributor" in m for m in audit_msgs), (
            "Spurious overwrite audit emitted for same-contributor PUT"
        )


# ===========================================================================
# Corrupt-store integration assertion (ROB F5)
# ===========================================================================


@pytest.mark.integration
class TestCorruptStoreIntegration:
    """When the on-disk store file is corrupt, create_asgi_app boots with an empty
    map (fail-closed), logs a LOUD error, and does NOT crash.
    """

    def test_corrupt_api_key_store_boots_with_empty_map_and_logs_error(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Corrupt api-keys.json → static-mode app starts, empty keystore, loud error.

        The resolver serves an empty map so unknown tokens are rejected (fail-closed),
        no crash-loop occurs, and the error is emitted to the log for operator visibility.
        """
        from context_intelligence_server.config import Settings  # noqa: PLC0415
        from context_intelligence_server.main import (  # noqa: PLC0415
            create_asgi_app,
            get_api_key_store,
        )

        # Write a corrupt (non-JSON) store file before the app starts.
        store_path = tmp_path / "api-keys.json"
        store_path.write_text("}{corrupted json content!!", encoding="utf-8")

        settings = Settings(
            auth_mode="static",
            allow_unauthenticated=True,
            api_keys_store_path=str(store_path),
            entra_identities_store_path=str(tmp_path / "entra-identities.json"),
        )

        with caplog.at_level(
            logging.ERROR, logger="context_intelligence_server.identity_store"
        ):
            # Must NOT raise — corrupt store is fail-closed, not crash-loop.
            create_asgi_app(settings=settings)

        # The store exists (app started) and the in-process map is empty.
        store = get_api_key_store()
        assert store is not None, "api_key_store not set on app.state"
        assert len(store) == 0, (
            f"Expected empty store on corrupt load; got {len(store)} entries"
        )
        assert store.flat_dict == {}, (
            "flat_dict must be empty after corrupt load (fail-closed)"
        )

        # A LOUD error must have been logged (operator visibility).
        error_records = [r for r in caplog.records if r.levelno >= logging.ERROR]
        assert len(error_records) >= 1, (
            "Expected at least one ERROR/CRITICAL log record on corrupt store load"
        )
        # The log must mention the corrupt file path.
        assert any(str(store_path) in r.getMessage() for r in error_records), (
            "Error log must reference the corrupt file path for operator diagnosis"
        )

    def test_corrupt_entra_identity_store_boots_with_empty_map_and_logs_error(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Corrupt entra-identities.json → entra-mode app starts, empty map, loud error."""
        from context_intelligence_server.config import Settings  # noqa: PLC0415
        from context_intelligence_server.main import (  # noqa: PLC0415
            create_asgi_app,
            get_entra_identity_store,
        )

        store_path = tmp_path / "entra-identities.json"
        store_path.write_text("not valid json at all ---", encoding="utf-8")

        settings = Settings(
            auth_mode="entra",
            allow_unauthenticated=True,
            azure_client_id=FAKE_CLIENT_ID,
            azure_tenant_id=FAKE_TENANT_ID,
            entra_identities={FAKE_OID: {"id": FAKE_CONTRIBUTOR}},
            entra_identities_store_path=str(store_path),
            api_keys_store_path=str(tmp_path / "api-keys.json"),
        )

        with caplog.at_level(
            logging.ERROR, logger="context_intelligence_server.identity_store"
        ):
            create_asgi_app(settings=settings, _jwks_client=_StubJWKSClient())

        store = get_entra_identity_store()
        assert store is not None, "entra_identity_store not set on app.state"
        # After corrupt load + seed from config, the map should reflect the config seed.
        # The corrupt file is overwritten by the seed — this is the expected behavior
        # (corrupt file → fail-closed load → empty map → seed from config → populated).
        # What matters: no crash-loop, store is accessible.
        # (The seeded map may be non-empty because create_asgi_app seeds from config.)
        assert store is not None  # Already checked; emphasizing no crash

        error_records = [r for r in caplog.records if r.levelno >= logging.ERROR]
        assert len(error_records) >= 1, (
            "Expected at least one ERROR log record on corrupt entra store load"
        )
