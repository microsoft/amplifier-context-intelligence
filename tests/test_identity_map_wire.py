"""T1 config + T3 resolver wiring tests for the runtime identity-map.

Tests cover:

  T1 (config additions):
    - admin_api_key: str | None — env AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_ADMIN_API_KEY
      or YAML admin_api_key; consistent with api_key/api_keys pattern.
    - api_keys_store_path: str — default "/data/identity/api-keys.json"; env/YAML override.
    - entra_identities_store_path: str — default "/data/identity/entra-identities.json";
      env/YAML override.

  T3 (IdentityStore wired to BOTH resolvers):
    Static mode:
      - First boot (no file) → seeds store from build_keystore() → StaticKeyResolver.resolve()
        returns the seeded contributor immediately.
      - Store-wins (file present) → config seeds are NOT applied; resolver uses file data.
      - After store.put(sha256, {"id": ...}) → resolve() returns the new entry immediately,
        with NO restart — the resolver's keystore IS the store's live flat_dict object.
    Entra mode (same three assertions via identity_map):
      - First boot → seeds from build_identity_map() → identity_map populated.
      - Store-wins → config ignored; identity_map uses file data.
      - After store.put(oid, {"id": ...}) → identity_map sees it immediately.
    Accessors:
      - get_api_key_store() returns the store in static mode, None in entra mode.
      - get_entra_identity_store() returns the store in entra mode, None in static mode.

Fake constants (NEVER real app-reg IDs or OIDs — §0.3):
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pytest

# ---------------------------------------------------------------------------
# Fake constants — NEVER real credentials / GUIDs
# ---------------------------------------------------------------------------
FAKE_RAW_TOKEN = "test-token-aabbcc"
FAKE_TOKEN_DIGEST = hashlib.sha256(FAKE_RAW_TOKEN.encode()).hexdigest()
FAKE_CONTRIBUTOR = "alice"

FAKE_OID = "11111111-2222-3333-4444-555566667777"
FAKE_CONTRIBUTOR_ENTRA = "bob"

FAKE_CLIENT_ID = "aaaabbbb-1111-2222-3333-ccccddddeeee"
FAKE_TENANT_ID = "ffffeeee-dddd-cccc-bbbb-aaaa99998888"

# A second token — used for the "put visible immediately" tests
FAKE_NEW_RAW_TOKEN = "new-token-ddeeff"
FAKE_NEW_DIGEST = hashlib.sha256(FAKE_NEW_RAW_TOKEN.encode()).hexdigest()
FAKE_NEW_CONTRIBUTOR = "carol"

FAKE_NEW_OID = "99999999-8888-7777-6666-555544443333"
FAKE_NEW_CONTRIBUTOR_ENTRA = "david"


# ---------------------------------------------------------------------------
# JWKS stub — no network; satisfies EntraResolver's eager prefetch guard
# ---------------------------------------------------------------------------


class _StubSigningKey:
    """Mimics PyJWKClient.get_signing_key_from_jwt(token).key."""

    def __init__(self, key: Any = "dummy-key") -> None:
        self.key = key


class _StubJWKSClient:
    """JWKS client stub: fetch_data is a no-op; returns one non-empty key set."""

    def fetch_data(self) -> None:
        pass  # satisfies eager-prefetch guard

    def get_signing_key_from_jwt(self, token: str) -> _StubSigningKey:
        raise NotImplementedError("Identity-map wire tests do not call resolve()")

    def get_jwk_set(self) -> Any:
        """Return a non-empty JWK set — satisfies the empty-JWKS startup guard."""

        class _FakeJWKSet:
            keys = [_StubSigningKey()]

        return _FakeJWKSet()


# ===========================================================================
# T1: Config field tests
# ===========================================================================


class TestT1AdminApiKey:
    """admin_api_key follows the same YAML+env pattern as api_key."""

    def test_default_is_none(self) -> None:
        """admin_api_key defaults to None when not set."""
        from context_intelligence_server.config import Settings  # noqa: PLC0415

        s = Settings()
        assert s.admin_api_key is None

    def test_empty_string_normalised_to_none(self) -> None:
        """Empty string is normalised to None (same as api_key)."""
        from context_intelligence_server.config import Settings  # noqa: PLC0415

        s = Settings(admin_api_key="")
        assert s.admin_api_key is None

    def test_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """admin_api_key is loaded from the correct env variable."""
        monkeypatch.setenv(
            "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_ADMIN_API_KEY", "secret-admin"
        )
        from context_intelligence_server.config import Settings  # noqa: PLC0415

        s = Settings()
        assert s.admin_api_key == "secret-admin"

    def test_from_yaml(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """admin_api_key is loaded from the YAML config file."""
        cfg = tmp_path / "server-config.yaml"
        cfg.write_text("admin_api_key: yaml-admin-key\n")
        monkeypatch.setenv(
            "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_CONFIG_FILE", str(cfg)
        )
        from context_intelligence_server.config import Settings  # noqa: PLC0415

        s = Settings()
        assert s.admin_api_key == "yaml-admin-key"

    def test_env_overrides_yaml(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Env var takes precedence over YAML config (standard pydantic-settings priority)."""
        cfg = tmp_path / "server-config.yaml"
        cfg.write_text("admin_api_key: yaml-admin-key\n")
        monkeypatch.setenv(
            "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_CONFIG_FILE", str(cfg)
        )
        monkeypatch.setenv(
            "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_ADMIN_API_KEY", "env-admin-key"
        )
        from context_intelligence_server.config import Settings  # noqa: PLC0415

        s = Settings()
        assert s.admin_api_key == "env-admin-key"


class TestT1ApiKeysStorePath:
    """api_keys_store_path: default + env + YAML override."""

    def test_default(self) -> None:
        from context_intelligence_server.config import Settings  # noqa: PLC0415

        assert Settings().api_keys_store_path == "/data/identity/api-keys.json"

    def test_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(
            "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_API_KEYS_STORE_PATH",
            "/custom/keys.json",
        )
        from context_intelligence_server.config import Settings  # noqa: PLC0415

        assert Settings().api_keys_store_path == "/custom/keys.json"

    def test_from_yaml(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        cfg = tmp_path / "server-config.yaml"
        cfg.write_text("api_keys_store_path: /yaml/keys.json\n")
        monkeypatch.setenv(
            "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_CONFIG_FILE", str(cfg)
        )
        from context_intelligence_server.config import Settings  # noqa: PLC0415

        assert Settings().api_keys_store_path == "/yaml/keys.json"


class TestT1EntraIdentitiesStorePath:
    """entra_identities_store_path: default + env + YAML override."""

    def test_default(self) -> None:
        from context_intelligence_server.config import Settings  # noqa: PLC0415

        assert (
            Settings().entra_identities_store_path
            == "/data/identity/entra-identities.json"
        )

    def test_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(
            "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_ENTRA_IDENTITIES_STORE_PATH",
            "/custom/entra.json",
        )
        from context_intelligence_server.config import Settings  # noqa: PLC0415

        assert Settings().entra_identities_store_path == "/custom/entra.json"

    def test_from_yaml(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        cfg = tmp_path / "server-config.yaml"
        cfg.write_text("entra_identities_store_path: /yaml/entra.json\n")
        monkeypatch.setenv(
            "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_CONFIG_FILE", str(cfg)
        )
        from context_intelligence_server.config import Settings  # noqa: PLC0415

        assert Settings().entra_identities_store_path == "/yaml/entra.json"


# ===========================================================================
# T3: Resolver wiring — helpers
# ===========================================================================


def _make_static_settings(
    tmp_path: Path,
    token: str = FAKE_RAW_TOKEN,
    contributor: str = FAKE_CONTRIBUTOR,
    *,
    store_path: Path | None = None,
) -> "Any":
    """Return a Settings instance for static mode with one api_key entry."""
    from context_intelligence_server.config import Settings  # noqa: PLC0415

    digest = hashlib.sha256(token.encode()).hexdigest()
    sp = store_path or (tmp_path / "api-keys.json")
    return Settings(
        auth_mode="static",
        allow_unauthenticated=False,
        api_keys={digest: {"id": contributor}},
        api_keys_store_path=str(sp),
        entra_identities_store_path=str(tmp_path / "entra-identities.json"),
    )


def _make_entra_settings(
    tmp_path: Path,
    oid: str = FAKE_OID,
    contributor: str = FAKE_CONTRIBUTOR_ENTRA,
    *,
    store_path: Path | None = None,
) -> "Any":
    """Return a Settings instance for entra mode with one identity entry."""
    from context_intelligence_server.config import Settings  # noqa: PLC0415

    sp = store_path or (tmp_path / "entra-identities.json")
    return Settings(
        auth_mode="entra",
        allow_unauthenticated=False,
        azure_client_id=FAKE_CLIENT_ID,
        azure_tenant_id=FAKE_TENANT_ID,
        entra_identities={oid: {"id": contributor}},
        entra_identities_store_path=str(sp),
        api_keys_store_path=str(tmp_path / "api-keys.json"),
    )


# ===========================================================================
# T3: Static mode wiring
# ===========================================================================


class TestT3StaticWiring:
    """StaticKeyResolver is wired to the live IdentityStore flat_dict."""

    def test_first_boot_no_file_seeds_from_build_keystore(self, tmp_path: Path) -> None:
        """No store file → seeds from build_keystore() → resolve() returns contributor."""
        settings = _make_static_settings(tmp_path)
        store_path = Path(settings.api_keys_store_path)
        assert not store_path.exists(), (
            "Pre-condition: file must not exist on first boot"
        )

        from context_intelligence_server.main import (  # noqa: PLC0415
            create_asgi_app,
            get_api_key_store,
        )

        create_asgi_app(settings=settings)
        store = get_api_key_store()

        assert store is not None
        assert FAKE_TOKEN_DIGEST in store.flat_dict
        assert store.flat_dict[FAKE_TOKEN_DIGEST] == FAKE_CONTRIBUTOR

        # The resolver itself can resolve the seeded token (T5: returns tuple)
        middleware = create_asgi_app(settings=settings)
        result = middleware.resolver.resolve(FAKE_RAW_TOKEN)
        assert result is not None and result[0] == FAKE_CONTRIBUTOR

    def test_store_wins_when_file_present(self, tmp_path: Path) -> None:
        """File exists with different data → config seeds are NOT applied (store wins)."""
        store_path = tmp_path / "api-keys.json"
        # Write file with a DIFFERENT token than the config
        file_digest = "f" * 64
        file_data = {file_digest: {"id": "file-contributor"}}
        store_path.write_text(json.dumps(file_data))

        settings = _make_static_settings(tmp_path, store_path=store_path)

        from context_intelligence_server.main import (  # noqa: PLC0415
            create_asgi_app,
            get_api_key_store,
        )

        create_asgi_app(settings=settings)
        store = get_api_key_store()

        assert store is not None
        # File data is present
        assert file_digest in store.flat_dict
        assert store.flat_dict[file_digest] == "file-contributor"
        # Config token is NOT present (store wins)
        assert FAKE_TOKEN_DIGEST not in store.flat_dict

    def test_put_visible_immediately_to_resolver(self, tmp_path: Path) -> None:
        """After store.put(new_sha256, {...}) → resolver.resolve(new_token) succeeds immediately.

        This is the load-bearing guarantee: no restart required.
        """
        settings = _make_static_settings(tmp_path)

        from context_intelligence_server.main import (  # noqa: PLC0415
            create_asgi_app,
            get_api_key_store,
        )

        # Capture the middleware (resolver) and the store
        middleware = create_asgi_app(settings=settings)
        store = get_api_key_store()
        assert store is not None

        # At this point the new token is NOT resolvable
        assert middleware.resolver.resolve(FAKE_NEW_RAW_TOKEN) is None

        # Mutate the store: add the new token
        store.put(FAKE_NEW_DIGEST, {"id": FAKE_NEW_CONTRIBUTOR})

        # The resolver sees it immediately — no create_asgi_app call, no restart
        # T5 protocol change: resolve() returns (contributor_id, roles) tuple.
        new_result = middleware.resolver.resolve(FAKE_NEW_RAW_TOKEN)
        assert new_result is not None and new_result[0] == FAKE_NEW_CONTRIBUTOR

    def test_resolver_keystore_is_store_flat_dict(self, tmp_path: Path) -> None:
        """The resolver's internal keystore IS the same dict object as store.flat_dict.

        This identity guarantees that any in-place mutation to flat_dict is
        instantly visible to the resolver without copying.
        """
        settings = _make_static_settings(tmp_path)

        from context_intelligence_server.main import (  # noqa: PLC0415
            create_asgi_app,
            get_api_key_store,
        )

        middleware = create_asgi_app(settings=settings)
        store = get_api_key_store()
        assert store is not None

        # The resolver's internal keystore must be the exact same object
        assert middleware.resolver._keystore is store.flat_dict  # type: ignore[union-attr]


# ===========================================================================
# T3: Entra mode wiring
# ===========================================================================


class TestT3EntraWiring:
    """EntraResolver is wired to the live IdentityStore flat_dict."""

    def test_first_boot_no_file_seeds_from_build_identity_map(
        self, tmp_path: Path
    ) -> None:
        """No store file → seeds from build_identity_map() → identity_map populated."""
        settings = _make_entra_settings(tmp_path)
        store_path = Path(settings.entra_identities_store_path)
        assert not store_path.exists(), (
            "Pre-condition: file must not exist on first boot"
        )

        from context_intelligence_server.main import (  # noqa: PLC0415
            create_asgi_app,
            get_entra_identity_store,
        )

        create_asgi_app(settings=settings, _jwks_client=_StubJWKSClient())
        store = get_entra_identity_store()

        assert store is not None
        assert FAKE_OID in store.flat_dict
        assert store.flat_dict[FAKE_OID] == FAKE_CONTRIBUTOR_ENTRA

    def test_store_wins_when_file_present(self, tmp_path: Path) -> None:
        """File exists with different OID → config seeds are NOT applied (store wins)."""
        store_path = tmp_path / "entra-identities.json"
        file_oid = "aaaabbbb-cccc-dddd-eeee-ffffaaaabbbb"
        file_data = {file_oid: {"id": "file-entra-contributor"}}
        store_path.write_text(json.dumps(file_data))

        settings = _make_entra_settings(tmp_path, store_path=store_path)

        from context_intelligence_server.main import (  # noqa: PLC0415
            create_asgi_app,
            get_entra_identity_store,
        )

        create_asgi_app(settings=settings, _jwks_client=_StubJWKSClient())
        store = get_entra_identity_store()

        assert store is not None
        # File data present
        assert file_oid in store.flat_dict
        assert store.flat_dict[file_oid] == "file-entra-contributor"
        # Config OID NOT present (store wins)
        assert FAKE_OID not in store.flat_dict

    def test_put_visible_immediately_via_identity_map(self, tmp_path: Path) -> None:
        """After store.put(new_oid, {...}) → resolver._identity_map sees it immediately.

        We verify via the resolver's internal dict rather than calling resolve() (which
        requires a valid JWT) — the wiring is the same: flat_dict IS _identity_map.
        """
        settings = _make_entra_settings(tmp_path)

        from context_intelligence_server.main import (  # noqa: PLC0415
            create_asgi_app,
            get_entra_identity_store,
        )

        middleware = create_asgi_app(settings=settings, _jwks_client=_StubJWKSClient())
        store = get_entra_identity_store()
        assert store is not None

        # New OID not yet in the map
        assert FAKE_NEW_OID not in middleware.resolver._identity_map  # type: ignore[union-attr]

        # Mutate the store
        store.put(FAKE_NEW_OID, {"id": FAKE_NEW_CONTRIBUTOR_ENTRA})

        # Visible immediately
        assert (
            middleware.resolver._identity_map[FAKE_NEW_OID]
            == FAKE_NEW_CONTRIBUTOR_ENTRA
        )  # type: ignore[union-attr]

    def test_resolver_identity_map_is_store_flat_dict(self, tmp_path: Path) -> None:
        """The resolver's _identity_map IS the same dict object as store.flat_dict."""
        settings = _make_entra_settings(tmp_path)

        from context_intelligence_server.main import (  # noqa: PLC0415
            create_asgi_app,
            get_entra_identity_store,
        )

        middleware = create_asgi_app(settings=settings, _jwks_client=_StubJWKSClient())
        store = get_entra_identity_store()
        assert store is not None

        assert middleware.resolver._identity_map is store.flat_dict  # type: ignore[union-attr]


# ===========================================================================
# T3: Mode-specific accessors
# ===========================================================================


class TestT3Accessors:
    """get_api_key_store() / get_entra_identity_store() return the right store per mode."""

    def test_get_api_key_store_returns_store_in_static_mode(
        self, tmp_path: Path
    ) -> None:
        from context_intelligence_server.main import (  # noqa: PLC0415
            create_asgi_app,
            get_api_key_store,
        )

        settings = _make_static_settings(tmp_path)
        create_asgi_app(settings=settings)

        store = get_api_key_store()
        assert store is not None
        from context_intelligence_server.identity_store import (  # noqa: PLC0415
            IdentityStore,
        )

        assert isinstance(store, IdentityStore)

    def test_get_api_key_store_returns_none_in_entra_mode(self, tmp_path: Path) -> None:
        from context_intelligence_server.main import (  # noqa: PLC0415
            create_asgi_app,
            get_api_key_store,
        )

        settings = _make_entra_settings(tmp_path)
        create_asgi_app(settings=settings, _jwks_client=_StubJWKSClient())

        assert get_api_key_store() is None

    def test_get_entra_identity_store_returns_store_in_entra_mode(
        self, tmp_path: Path
    ) -> None:
        from context_intelligence_server.main import (  # noqa: PLC0415
            create_asgi_app,
            get_entra_identity_store,
        )

        settings = _make_entra_settings(tmp_path)
        create_asgi_app(settings=settings, _jwks_client=_StubJWKSClient())

        store = get_entra_identity_store()
        assert store is not None
        from context_intelligence_server.identity_store import (  # noqa: PLC0415
            IdentityStore,
        )

        assert isinstance(store, IdentityStore)

    def test_get_entra_identity_store_returns_none_in_static_mode(
        self, tmp_path: Path
    ) -> None:
        from context_intelligence_server.main import (  # noqa: PLC0415
            create_asgi_app,
            get_entra_identity_store,
        )

        settings = _make_static_settings(tmp_path)
        create_asgi_app(settings=settings)

        assert get_entra_identity_store() is None
