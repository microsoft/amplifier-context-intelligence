"""Tests for the Neo4j two-client split (Step 1, doc 12).

Covers the structured `neo4j` config model, the admin/query resolver
fallback logic, the startup guard, and the nested env-override wiring.

All tests here are pure unit tests: no Neo4j connection, no network, no
running server. The one test that *would* require a live Neo4j (the
READ-session evidence probe, doc 12 §6.2) is authored but guarded to skip
cleanly when no reachable instance is configured -- it must never run as
part of this isolated suite.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from context_intelligence_server.config import Neo4jClientConfig, Neo4jConfig, Settings


# ---------------------------------------------------------------------------
# 6.1 -- config model + fallback resolver (no DB)
# ---------------------------------------------------------------------------


class TestNeo4jResolvers:
    def test_structured_config_wins(self) -> None:
        """When `neo4j` is set, both resolvers return the structured values."""
        s = Settings(
            neo4j=Neo4jConfig(
                admin=Neo4jClientConfig(
                    url="bolt://admin-host:7687", username="a", password="pa"
                ),
                cypher_query=Neo4jClientConfig(
                    url="bolt://query-host:7687",
                    username="q",
                    password="pq",
                    access_mode="READ",
                ),
            )
        )

        admin = s.resolve_neo4j_admin()
        query = s.resolve_neo4j_query()

        assert admin.url == "bolt://admin-host:7687"
        assert admin.access_mode == "WRITE"
        assert query.url == "bolt://query-host:7687"
        assert query.access_mode == "READ"

    def test_legacy_fallback_when_neo4j_absent(self) -> None:
        """When `neo4j` is None, both resolvers fall back to the flat fields."""
        s = Settings(
            neo4j=None,
            neo4j_url="bolt://x:7687",
            neo4j_user="u",
            neo4j_password="p",
        )

        admin = s.resolve_neo4j_admin()
        query = s.resolve_neo4j_query()

        assert admin.url == "bolt://x:7687"
        assert admin.username == "u"
        assert admin.password == "p"
        assert admin.access_mode == "WRITE"

        assert query.url == "bolt://x:7687"
        assert query.username == "u"
        assert query.password == "p"
        assert query.access_mode == "READ"

    def test_empty_password_yields_no_auth(self) -> None:
        """Empty password -> auth is None (matches registry.py's prior behavior)."""
        s = Settings(neo4j=None, neo4j_password="")
        assert s.resolve_neo4j_admin().auth is None
        assert s.resolve_neo4j_query().auth is None

    def test_partial_structured_block_rejected(self) -> None:
        """Neo4jConfig requires BOTH admin and cypher_query."""
        with pytest.raises(ValidationError):
            Neo4jConfig(admin=Neo4jClientConfig(url="bolt://x:7687"))  # type: ignore[call-arg]


# ---------------------------------------------------------------------------
# Blocker 2 (council review) -- Neo4jConfig access_mode enforcement.
#
# A `cypher_query` block that omits `access_mode` (defaulting to "WRITE") is
# exactly the silent-write "read" client hole (TB-2): it would look like a
# read-intent client but still open WRITE sessions. Reject it at construction
# time instead of accepting it silently. Symmetrically, `admin.access_mode`
# must be "WRITE" -- an admin block accidentally set to "READ" is just as
# broken (writes would silently go through a read-hinted session).
# ---------------------------------------------------------------------------


class TestNeo4jConfigAccessModeValidation:
    def test_cypher_query_missing_access_mode_defaults_write_and_is_rejected(
        self,
    ) -> None:
        """The TB-2 hole: cypher_query omitting access_mode defaults to
        "WRITE" (Neo4jClientConfig's default) -- Neo4jConfig must reject this,
        not silently accept a write-capable "read" client."""
        with pytest.raises(ValidationError, match="cypher_query.access_mode"):
            Neo4jConfig(
                admin=Neo4jClientConfig(url="bolt://a:7687"),
                cypher_query=Neo4jClientConfig(url="bolt://a:7687"),  # no access_mode!
            )

    def test_admin_access_mode_read_is_rejected(self) -> None:
        """admin.access_mode="READ" is just as broken as the TB-2 hole --
        reject it just as loudly."""
        with pytest.raises(ValidationError, match="admin.access_mode"):
            Neo4jConfig(
                admin=Neo4jClientConfig(url="bolt://a:7687", access_mode="READ"),
                cypher_query=Neo4jClientConfig(url="bolt://a:7687", access_mode="READ"),
            )

    def test_valid_admin_write_cypher_query_read_still_constructs(self) -> None:
        """The correct shape (admin=WRITE, cypher_query=READ) is unaffected --
        mirrors TestNeo4jStartupGuard.test_inert_when_required_and_neo4j_present,
        which must stay green."""
        cfg = Neo4jConfig(
            admin=Neo4jClientConfig(url="bolt://a:7687"),
            cypher_query=Neo4jClientConfig(url="bolt://a:7687", access_mode="READ"),
        )
        assert cfg.admin.access_mode == "WRITE"
        assert cfg.cypher_query.access_mode == "READ"


# ---------------------------------------------------------------------------
# 6.3 -- back-compat fallback (no structured block, guard inert)
# ---------------------------------------------------------------------------


class TestBackCompatFallback:
    def test_create_asgi_app_does_not_raise_on_legacy_only_config(self) -> None:
        from context_intelligence_server.main import create_asgi_app

        s = Settings(
            neo4j=None,
            neo4j_url="bolt://legacy:7687",
            neo4j_user="u",
            neo4j_password="p",
            neo4j_require_explicit_clients=False,
            allow_unauthenticated=True,
        )

        # Must not raise -- existing server-config.yaml deployments keep working.
        create_asgi_app(settings=s)

        assert s.resolve_neo4j_admin().url == "bolt://legacy:7687"
        assert s.resolve_neo4j_query().url == "bolt://legacy:7687"


# ---------------------------------------------------------------------------
# 6.4 -- startup guard (gap #12)
# ---------------------------------------------------------------------------


class TestNeo4jStartupGuard:
    def test_raises_when_required_but_neo4j_absent(self) -> None:
        from context_intelligence_server.main import create_asgi_app

        s = Settings(
            neo4j=None,
            neo4j_require_explicit_clients=True,
            allow_unauthenticated=True,
        )

        with pytest.raises(RuntimeError, match="neo4j"):
            create_asgi_app(settings=s)

    def test_inert_when_required_and_neo4j_present(self) -> None:
        from context_intelligence_server.main import create_asgi_app

        s = Settings(
            neo4j=Neo4jConfig(
                admin=Neo4jClientConfig(url="bolt://a:7687"),
                cypher_query=Neo4jClientConfig(url="bolt://a:7687", access_mode="READ"),
            ),
            neo4j_require_explicit_clients=True,
            allow_unauthenticated=True,
        )

        # Must not raise on the neo4j guard.
        create_asgi_app(settings=s)


# ---------------------------------------------------------------------------
# 6.5 -- nested env-override (gap #13) + YAML regression guard
# ---------------------------------------------------------------------------


class TestNestedEnvOverride:
    def test_nested_env_vars_override_structured_fields(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """With env_nested_delimiter='__' active, NEO4J__<CLIENT>__<FIELD> lands."""
        monkeypatch.setenv(
            "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_NEO4J__ADMIN__URL",
            "bolt://env-admin:7687",
        )
        monkeypatch.setenv(
            "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_NEO4J__ADMIN__USERNAME",
            "envadmin",
        )
        monkeypatch.setenv(
            "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_NEO4J__ADMIN__PASSWORD",
            "envsecret",
        )
        monkeypatch.setenv(
            "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_NEO4J__CYPHER_QUERY__URL",
            "bolt://env-query:7687",
        )
        monkeypatch.setenv(
            "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_NEO4J__CYPHER_QUERY__ACCESS_MODE",
            "READ",
        )

        # Bypass get_settings()'s lru_cache -- instantiate Settings() directly.
        s = Settings()

        assert s.resolve_neo4j_admin().url == "bolt://env-admin:7687"
        assert s.resolve_neo4j_admin().username == "envadmin"
        assert s.resolve_neo4j_admin().password == "envsecret"
        assert s.resolve_neo4j_query().url == "bolt://env-query:7687"
        assert s.resolve_neo4j_query().access_mode == "READ"

    def test_yaml_api_keys_map_unaffected_by_nested_delimiter(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Regression guard (doc 12 §5): env_nested_delimiter must not break the
        existing dict[str, dict[str, str]] `api_keys` field, which is populated
        via YAML, not env, in every current profile.
        """
        digest = "a" * 64
        config_file = tmp_path / "server-config.yaml"
        config_file.write_text(f'api_keys:\n  "{digest}":\n    id: owner\n')

        monkeypatch.setenv(
            "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_CONFIG_FILE", str(config_file)
        )

        s = Settings()

        assert s.api_keys is not None
        assert s.api_keys[digest]["id"] == "owner"
        keystore = s.build_keystore()
        assert keystore[digest] == "owner"

    def test_yaml_entra_identities_map_unaffected_by_nested_delimiter(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Concern A (council review): env_nested_delimiter='__' is GLOBAL --
        it also governs the existing dict[str, dict[str, str]] `entra_identities`
        field. Prove YAML-loaded entra_identities still parses correctly with
        the delimiter active.
        """
        oid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        config_file = tmp_path / "server-config.yaml"
        config_file.write_text(f'entra_identities:\n  "{oid}":\n    id: alice\n')

        monkeypatch.setenv(
            "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_CONFIG_FILE", str(config_file)
        )

        s = Settings()

        assert s.entra_identities is not None
        assert s.entra_identities[oid]["id"] == "alice"
        identity_map = s.build_identity_map()
        assert identity_map[oid] == "alice"

    def test_yaml_service_identities_map_unaffected_by_nested_delimiter(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Concern A (council review): same regression guard for the existing
        `service_identities` field.
        """
        oid = "bbbbbbbb-cccc-dddd-eeee-ffffffffffff"
        config_file = tmp_path / "server-config.yaml"
        config_file.write_text(
            f'service_identities:\n  "{oid}":\n    id: svc-account\n'
        )

        monkeypatch.setenv(
            "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_CONFIG_FILE", str(config_file)
        )

        s = Settings()

        assert s.service_identities is not None
        assert s.service_identities[oid]["id"] == "svc-account"
        service_map = s.build_service_identity_map()
        assert service_map[oid] == "svc-account"

    def test_flat_json_env_var_api_keys_unaffected_by_nested_delimiter(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Concern A (council review): the known pydantic-settings interaction
        point -- a dict field populated via a FLAT JSON env var (not the
        NEO4J__<client>__<field> nested form) must still parse correctly with
        env_nested_delimiter='__' active.
        """
        digest = "b" * 64
        monkeypatch.setenv(
            "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_API_KEYS",
            json.dumps({digest: {"id": "owner"}}),
        )

        s = Settings()

        assert s.api_keys is not None
        assert s.api_keys[digest]["id"] == "owner"
        keystore = s.build_keystore()
        assert keystore[digest] == "owner"


# ---------------------------------------------------------------------------
# 6.2 -- READ-session EVIDENCE test (gap #14). AUTHORED BUT DORMANT.
#
# This test requires a REACHABLE Neo4j instance and actually runs a
# destructive query through a READ session to record (not assert) whether
# the write is rejected. It must SKIP cleanly in this isolated suite --
# no live Neo4j, no network -- and only activates when a developer has
# explicitly pointed it at a real instance via NEO4J_TEST_URI.
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    not __import__("os").environ.get("NEO4J_TEST_URI"),
    reason=(
        "Requires a reachable Neo4j instance; set NEO4J_TEST_URI to opt in. "
        "Dormant by default -- this suite must never open a real DB connection."
    ),
)
async def test_read_session_destructive_query_evidence() -> None:
    """Doc 12 §6.2: run a destructive query through a READ session and RECORD
    the outcome. This is evidence, not a guarantee -- on a Community
    single-instance over bolt:// the write is expected to NOT be rejected.
    """
    import os

    from neo4j import READ_ACCESS, AsyncGraphDatabase

    uri = os.environ["NEO4J_TEST_URI"]
    user = os.environ.get("NEO4J_TEST_USER", "neo4j")
    password = os.environ.get("NEO4J_TEST_PASSWORD", "")
    auth = (user, password) if password else None

    driver = AsyncGraphDatabase.driver(uri, auth=auth)
    rejected = None
    try:
        async with driver.session(default_access_mode=READ_ACCESS) as session:
            try:
                await session.run(
                    "CREATE (n:__ReadModeProbe__ {t: timestamp()}) RETURN n"
                )
                await session.run("MATCH (n:__ReadModeProbe__) DELETE n")
                rejected = False
            except Exception as exc:
                rejected = True
                print(f"READ session rejected write: {type(exc).__name__}: {exc}")
    finally:
        await driver.close()

    print(f"READ_ACCESS destructive-write rejected == {rejected}")
    assert rejected is not None  # the probe actually ran


# ---------------------------------------------------------------------------
# Two-client RUNTIME smoke test. AUTHORED BUT DORMANT.
#
# This proves the two-client split works END-TO-END through the REAL
# lifespan() and the REAL POST /cypher route against a REACHABLE Neo4j --
# with BOTH the admin and cypher_query clients wired to the SAME url and
# credentials. That is exactly today's Azure shape: one Neo4j instance,
# two logical clients (admin=WRITE, cypher_query=READ).
#
# Guarded with the same idiom as test_read_session_destructive_query_evidence
# above: skips cleanly unless NEO4J_TEST_URI is set. No Neo4j connection, no
# network, no running server as part of this isolated suite.
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    not __import__("os").environ.get("NEO4J_TEST_URI"),
    reason=(
        "Requires a reachable Neo4j instance; set NEO4J_TEST_URI to opt in. "
        "Dormant by default -- this suite must never open a real DB connection."
    ),
)
async def test_two_client_split_runtime_smoke(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Runtime smoke test: real ``lifespan()`` + real ``POST /cypher`` against
    a REAL Neo4j instance, with BOTH admin and cypher_query pointed at the
    SAME url+credentials.

    Approach: PREFERRED (HTTP boot). ``main.py``'s module-level ``_settings``
    is only read by ``lifespan()`` for the two ``resolve_neo4j_*()`` calls
    (main.py:146-147) -- swapping it for the duration of this test, then
    driving the real ``lifespan()`` context manager plus a real HTTP request
    through ``httpx.ASGITransport``, exercises the actual dual-driver +
    access-mode code (main.py's ``lifespan`` and ``post_cypher``), not a
    reimplementation. This mirrors the existing
    ``test_lifespan_creates_and_closes_driver`` pattern in test_main.py
    (patch module state, invoke ``lifespan()`` directly) combined with the
    ``client`` fixture pattern in conftest.py (``httpx.AsyncClient`` +
    ``ASGITransport``).

    Steps:
      1. Build a structured ``Settings`` with admin+cypher_query both
         pointing at ``NEO4J_TEST_URI`` -- assert the resolvers agree
         (same instance, two clients, correct access_mode each).
      2. Swap ``main._settings`` and run the REAL ``lifespan()`` context
         manager -- this creates both real drivers and runs
         ``ensure_neo4j_schema`` on the admin driver (proves the admin/WRITE
         path is live).
      3. Write a tiny probe node via the admin driver (further exercises the
         admin/WRITE path with real data).
      4. POST /cypher (real HTTP round-trip via ASGITransport) reads it back
         -- proves the request reads ``app.state.neo4j_query_driver`` and
         opens a READ-mode session against the live instance.
      5. Clean up the probe node via the admin driver, always, even on
         failure.

    Does NOT assert that a READ session rejects a write (it won't, on
    Community -- that is the accepted, separately-recorded limitation; see
    ``test_read_session_destructive_query_evidence`` above).
    """
    import os

    import httpx

    from context_intelligence_server import main as main_module

    uri = os.environ["NEO4J_TEST_URI"]
    user = os.environ.get("NEO4J_TEST_USER", "neo4j")
    password = os.environ.get("NEO4J_TEST_PASSWORD", "")

    test_settings = Settings(
        neo4j=Neo4jConfig(
            admin=Neo4jClientConfig(
                url=uri, username=user, password=password, access_mode="WRITE"
            ),
            cypher_query=Neo4jClientConfig(
                url=uri, username=user, password=password, access_mode="READ"
            ),
        ),
        neo4j_require_explicit_clients=True,
        allow_unauthenticated=True,
    )

    # Same instance, two logical clients -- the Azure-today shape.
    admin_cfg = test_settings.resolve_neo4j_admin()
    query_cfg = test_settings.resolve_neo4j_query()
    assert admin_cfg.url == uri
    assert query_cfg.url == uri
    assert admin_cfg.url == query_cfg.url
    assert admin_cfg.access_mode == "WRITE"
    assert query_cfg.access_mode == "READ"

    # lifespan() reads the module-level `_settings` singleton for
    # resolve_neo4j_admin()/resolve_neo4j_query() only -- swap it so the REAL
    # lifespan boots REAL drivers against our test instance.
    monkeypatch.setattr(main_module, "_settings", test_settings)
    # lifespan() also calls setup_logging(), which mkdir's the production log
    # path (/data) -- irrelevant to the two-client split and not writable in a
    # local test env. No-op it; logging is not under test here.
    monkeypatch.setattr(main_module, "setup_logging", lambda: None)

    probe_value = "two-client-runtime-smoke"

    async with main_module.lifespan(main_module.app):
        try:
            # Sanity: lifespan wired the query driver in READ mode.
            assert main_module.app.state.neo4j_query_access_mode == "READ"

            # Write a tiny probe via the ADMIN (WRITE) driver -- real data,
            # real write path, same instance as the query client.
            async with main_module.app.state.neo4j_driver.session() as session:
                await session.run(
                    "CREATE (n:__TwoClientRuntimeSmokeProbe__ {value: $value})",
                    {"value": probe_value},
                )

            # Read it back through the REAL HTTP /cypher route -- this reads
            # app.state.neo4j_query_driver and opens a READ-mode session.
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=main_module.app),
                base_url="http://test",
            ) as client:
                response = await client.post(
                    "/cypher",
                    json={
                        "query": (
                            "MATCH (n:__TwoClientRuntimeSmokeProbe__ {value: $value}) "
                            "RETURN n.value AS value"
                        ),
                        "params": {"value": probe_value},
                    },
                )

            assert response.status_code == 200
            body = response.json()
            assert body["results"] == [{"value": probe_value}]
        finally:
            # Clean up via the admin (WRITE) driver regardless of outcome.
            async with main_module.app.state.neo4j_driver.session() as session:
                await session.run(
                    "MATCH (n:__TwoClientRuntimeSmokeProbe__) DETACH DELETE n"
                )
    # lifespan's own finally block has now closed both drivers.
