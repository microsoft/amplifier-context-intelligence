"""Configuration via pydantic-settings for the Context Intelligence Server.

Values are resolved in this priority order (highest first):

1. Environment variables (``AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_*``).
2. YAML configuration file — path from the
   ``AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_CONFIG_FILE`` environment variable,
   or ``server-config.yaml`` in the working directory if it exists.
3. Built-in defaults.
"""

import hashlib
import os
import re
from functools import lru_cache
from pathlib import Path
from typing import Any, Literal, Tuple, Type

import yaml
from pydantic import field_validator, model_validator
from pydantic.fields import FieldInfo
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)

# Environment variable used to locate the YAML configuration file.
# This variable is intentionally NOT covered by the AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_
# prefix — it is read directly from the environment before the Settings class is
# instantiated, so the prefix-based machinery cannot apply.
_CONFIG_FILE_ENV = "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_CONFIG_FILE"
_CONFIG_FILE_DEFAULT = "server-config.yaml"

# ---------------------------------------------------------------------------
# GUID validation helpers (Entra identities)
# ---------------------------------------------------------------------------
# Anchored pattern for lowercase hex groups of 8-4-4-4-12.
# re.fullmatch() anchors the match to the full string, so braces, urn:uuid:
# prefixes, and trailing junk are all rejected without explicit anchors in the
# pattern.
_GUID_RE = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
# The all-zeros sentinel is explicitly rejected — a placeholder accidentally
# left in config should never authorize anyone.
_ALL_ZEROS_GUID = "00000000-0000-0000-0000-000000000000"


class YamlConfigSettingsSource(PydanticBaseSettingsSource):
    """Load settings from a YAML configuration file.

    The file path is resolved in this order:

    1. The ``yaml_file`` argument passed to the constructor (for tests / explicit use).
    2. The ``AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_CONFIG_FILE`` environment variable.
    3. ``server-config.yaml`` in the current working directory (silently skipped if
       it does not exist).

    Keys in the YAML file correspond to the field names in :class:`Settings` without
    the ``AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_`` prefix.  Unknown keys are ignored.

    Example ``server-config.yaml``::

        neo4j_url: neo4j://localhost:7687
        neo4j_password: ""
        blob_path: /home/user/.local/share/ci-server/blobs
        log_path: /home/user/.local/share/ci-server/logs/server.jsonl
    Environment variables always take precedence over values in the YAML file.
    """

    def __init__(
        self,
        settings_cls: Type[BaseSettings],
        yaml_file: Path | None = None,
    ) -> None:
        super().__init__(settings_cls)
        if yaml_file is None:
            env_path = os.environ.get(_CONFIG_FILE_ENV)
            yaml_file = Path(env_path) if env_path else Path(_CONFIG_FILE_DEFAULT)
        self.yaml_file = yaml_file
        self._data: dict[str, Any] = {}
        if self.yaml_file.exists():
            with open(self.yaml_file) as fh:
                loaded = yaml.safe_load(fh)
                if isinstance(loaded, dict):
                    self._data = loaded

    def get_field_value(
        self, field: FieldInfo, field_name: str
    ) -> tuple[Any, str, bool]:
        if field_name in self._data:
            return self._data[field_name], field_name, False
        return None, field_name, False

    def __call__(self) -> dict[str, Any]:
        return {
            k: v for k, v in self._data.items() if k in self.settings_cls.model_fields
        }


class Settings(BaseSettings):
    """Application settings for the Context Intelligence Server."""

    model_config = SettingsConfigDict(
        env_prefix="AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_"
    )

    # -------------------------------------------------------------------------
    # Server
    # -------------------------------------------------------------------------
    server_host: str = "0.0.0.0"
    server_port: int = 8000

    # -------------------------------------------------------------------------
    # Authentication
    # -------------------------------------------------------------------------
    api_key: str | None = None

    @field_validator("api_key", mode="before")
    @classmethod
    def _normalize_api_key(cls, v: str | None) -> str | None:
        """Normalize empty string to None so that api_key: '' in config disables auth."""
        return None if v == "" else v

    # Per-contributor API keys (NESTED form, design D4): the keystore is keyed by
    # the SHA-256 hex digest of the raw token (64 lowercase hex chars), and each
    # value is a metadata dict carrying at least ``id`` (the contributor id). The
    # nested shape leaves room to add ``role`` / ``label`` later without a breaking
    # config change. Raw tokens NEVER appear here — only their digests.
    #
    #   api_keys:
    #     "<64-hex sha256 of token>":
    #       id: owner
    #     "<64-hex sha256 of token>":
    #       id: peer-test
    api_keys: dict[str, dict[str, str]] | None = None

    @field_validator("api_keys", mode="after")
    @classmethod
    def _validate_api_keys(
        cls, v: dict[str, dict[str, str]] | None
    ) -> dict[str, dict[str, str]] | None:
        """Fail-closed: raise unless every entry is ``<64-hex> -> {"id": <non-empty str>}``.

        Rejects (by raising ``ValueError``):
        - an explicitly empty dict (omit or null-out to disable authentication);
        - a key that is not exactly 64 lowercase-hex characters after normalization
          (whitespace characters are rejected because they are not valid hex digits);
        - a value whose ``id`` is missing, empty, or whitespace-only.

        Non-dict values are already rejected by pydantic's ``dict[str, dict[str, str]]``
        coercion before this validator runs (``mode="after"``), so no extra
        ``isinstance`` check is needed here.

        Digest keys are normalized to lowercase before validation and returned as
        lowercase so an UPPERCASE digest in a config file maps correctly to the
        lowercase hexdigest produced by ``hashlib.sha256(...).hexdigest()``.

        NOTE: Duplicate digest keys in YAML/dict collapse to last-wins at the YAML
        parse level, before this validator sees the data.  Detection is not possible
        here.
        """
        if v is None:
            return None
        # Fail-closed: an explicitly empty map is a misconfiguration, not "auth off".
        # Omit api_keys or set it to null to disable per-contributor authentication.
        if len(v) == 0:
            raise ValueError(
                "api_keys must contain at least one entry if specified; "
                "omit it or use null to disable authentication"
            )
        normalized: dict[str, dict[str, str]] = {}
        for digest, meta in v.items():
            digest_lower = digest.lower()
            if len(digest_lower) != 64 or not all(
                c in "0123456789abcdef" for c in digest_lower
            ):
                raise ValueError(
                    f"api_keys key {digest!r} must be a 64-character SHA-256 hex digest"
                )
            contributor_id = meta.get("id")
            if not isinstance(contributor_id, str) or not contributor_id.strip():
                raise ValueError(
                    f"api_keys[{digest!r}]['id'] must be a non-empty, "
                    f"non-whitespace string, got {contributor_id!r}"
                )
            normalized[digest_lower] = meta
        return normalized

    def build_keystore(self) -> dict[str, str]:
        """Return ``{sha256_hex(token) -> contributor_id}`` for all configured keys.

        Combines the legacy ``api_key`` (folded to id ``"owner"``) with every entry
        in ``api_keys``.  An empty result means authentication is disabled (no keys
        configured) — backward-compatible with ``api_key=None`` setups.

        For the nested ``api_keys`` form the dict key IS already the SHA-256 hex
        digest of the token, so it is used verbatim; only the legacy single
        ``api_key`` is hashed here (over its UTF-8 bytes) so the bearer token sent
        in the Authorization header and the digest derived here always match.
        """
        ks: dict[str, str] = {}
        # Legacy api_key folds to contributor id "owner" for back-compat.
        if self.api_key is not None:
            digest = hashlib.sha256(self.api_key.encode()).hexdigest()
            ks[digest] = "owner"
        # Explicit per-contributor keys: key is the digest, value carries id.
        # (May overwrite the legacy "owner" entry if the same digest is present.)
        # Defensive .lower(): validator normalizes digests, but belt-and-suspenders here.
        for digest, meta in (self.api_keys or {}).items():
            ks[digest.lower()] = meta["id"]
        return ks

    # -------------------------------------------------------------------------
    # Entra authentication (auth_mode=entra)
    # -------------------------------------------------------------------------
    # auth_mode selects which resolver is active: "static" = today's sha256
    # keystore; "entra" = JWT validation via Entra / JWKS.  Exactly one mode
    # is active at a time — no "both".  Choosing "entra" without the required
    # supporting fields is a hard startup error (AC7 / §8b).
    auth_mode: Literal["static", "entra"] = "static"

    # allow_unauthenticated: explicit opt-out of the fail-closed startup gate.
    #
    # Production deployments MUST have auth configured (api_key / api_keys for
    # auth_mode=static, or entra_identities for auth_mode=entra).  Setting this
    # flag to True bypasses the RuntimeError that create_asgi_app() raises when
    # no credentials are configured, allowing the server to start in
    # unauthenticated mode (every request passes through).
    #
    # This flag exists ONLY for the test harness and local dev environments
    # where auth is intentionally disabled.  Never set it in production.
    allow_unauthenticated: bool = False

    # web_ui_enabled: serve the browser dashboard, OpenAPI docs, and the
    # streaming log endpoint (/logs/stream).
    #
    # Set to False for a locked-down API-only deployment (the CI pilot profile):
    #   - FastAPI is constructed without docs_url / redoc_url / openapi_url
    #     (no OpenAPI schema served; no Swagger UI).
    #   - The index, dashboard, static assets, and /logs/stream routes are NOT
    #     registered — those paths return 404.
    #   - /logs/stream, /, /dashboard, /docs, /openapi.json are removed from the
    #     auth-exempt set so they cannot be reached unauthenticated even if a
    #     misconfiguration somehow re-adds them.
    #
    # Default True preserves the current full-web behaviour.
    # Env: AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_WEB_UI_ENABLED=false
    web_ui_enabled: bool = True

    # azure_client_id / azure_tenant_id: the App Registration coordinates.
    # Both are required when auth_mode="entra".  Empty / whitespace-only
    # strings are normalized to None so that a template placeholder in a YAML
    # file (e.g. azure_client_id: "") behaves identically to omitting the field
    # and triggers a clear startup error rather than a silent wrong-value lookup.
    azure_client_id: str | None = None
    azure_tenant_id: str | None = None

    @field_validator("azure_client_id", "azure_tenant_id", mode="before")
    @classmethod
    def _normalize_azure_field(cls, v: Any) -> str | None:
        """Normalize empty/whitespace-only strings to None (mirrors _normalize_api_key)."""
        if v is None:
            return None
        if isinstance(v, str) and not v.strip():
            return None
        return v

    # entra_identities: the oid→contributor map — exact parity with api_keys.
    #
    # Shape: { "<oid-GUID>": {"id": "<contributor>"} }  (value = {id} only)
    #
    # Key  = the user's Azure AD object ID (oid), stored verbatim (public id);
    #        not hashed — hashing buys nothing for a public id and hurts auditability.
    # Value = {"id": "<contributor>"} matching the api_keys payload — same
    #        contributor string space, same write-once provenance semantics.
    #
    # Many oids → one contributor works automatically: each oid is its own key
    # with the same "id" value (e.g. two AD identities for the same person).
    #
    # NOTE: oid is a persistent personal identifier.  Do NOT commit real oid
    # values to product repos — use env/secret injection or a git-ignored map
    # (see §3 PII note in the auth plan).
    entra_identities: dict[str, dict[str, str]] | None = None

    @field_validator("entra_identities", mode="after")
    @classmethod
    def _validate_entra_identities(
        cls, v: dict[str, dict[str, str]] | None
    ) -> dict[str, dict[str, str]] | None:
        """Fail-closed: raise unless every entry is ``<GUID> -> {"id": <non-empty str>}``.

        Mirrors ``_validate_api_keys`` exactly, with GUID validation replacing
        64-hex validation.

        Rejects (by raising ``ValueError`` or pydantic ``ValidationError``):
        - an explicitly empty dict (omit or null-out to disable Entra auth);
        - a key that is not a valid GUID in 8-4-4-4-12 lowercase hex form after
          normalization (rejects braces, urn:uuid: prefixes, trailing junk);
        - the all-zeros GUID (placeholder sentinel);
        - a value whose ``id`` is missing, empty, or whitespace-only.

        This validator runs in ``mode="after"``, so pydantic has already coerced
        the field as ``dict[str, dict[str, str]]`` before this function is called.
        Non-dict values and non-string ``id`` values are caught by pydantic before
        reaching this code.  The ``if not isinstance(contributor_id, str)`` check
        fires only when the ``id`` key is *absent* from the dict.

        GUID keys are normalized to lowercase before validation and returned as
        lowercase so an UPPERCASE oid in a config file maps correctly to the
        lowercase oid extracted from a JWT claim.
        """
        if v is None:
            return None
        # Fail-closed: an explicitly empty map is a misconfiguration.
        # Omit entra_identities or set it to null to disable Entra auth.
        if len(v) == 0:
            raise ValueError(
                "entra_identities must contain at least one entry if specified; "
                "omit it or use null to disable Entra authentication"
            )
        normalized: dict[str, dict[str, str]] = {}
        for oid, meta in v.items():
            oid_lower = oid.lower()
            if not _GUID_RE.fullmatch(oid_lower):
                raise ValueError(
                    f"entra_identities key {oid!r} must be a valid GUID "
                    f"(xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)"
                )
            if oid_lower == _ALL_ZEROS_GUID:
                raise ValueError(
                    f"entra_identities key {oid!r} must not be the all-zeros GUID; "
                    f"use the real oid from 'az ad signed-in-user show --query id -o tsv'"
                )
            contributor_id = meta.get("id")
            if not isinstance(contributor_id, str) or not contributor_id.strip():
                raise ValueError(
                    f"entra_identities[{oid!r}]['id'] must be a non-empty, "
                    f"non-whitespace string, got {contributor_id!r}"
                )
            normalized[oid_lower] = meta
        return normalized

    @model_validator(mode="after")
    def _validate_entra_config(self) -> "Settings":
        """Cross-field startup validator for auth_mode='entra' (AC7).

        When auth_mode is 'entra' ALL of the following must be present and
        non-None after normalization:
        - azure_client_id
        - azure_tenant_id
        - entra_identities (non-empty — field validator already rejects ``{}``,
          so here we only catch None / omitted)

        A single ValueError names every missing field so the operator sees one
        clear startup message rather than cryptic downstream failures.
        """
        if self.auth_mode == "entra":
            errors: list[str] = []
            if self.azure_client_id is None:
                errors.append(
                    "azure_client_id is required when auth_mode='entra'; "
                    "set AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_AZURE_CLIENT_ID "
                    "or azure_client_id in the config file"
                )
            if self.azure_tenant_id is None:
                errors.append(
                    "azure_tenant_id is required when auth_mode='entra'; "
                    "set AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_AZURE_TENANT_ID "
                    "or azure_tenant_id in the config file"
                )
            if not self.entra_identities:
                errors.append(
                    "entra_identities must be a non-empty map when auth_mode='entra'; "
                    "provide at least one oid → {id: contributor} entry"
                )
            if errors:
                raise ValueError(
                    "Entra auth misconfiguration (startup refused): "
                    + "; ".join(errors)
                )
        return self

    def build_identity_map(self) -> dict[str, str]:
        """Return ``{oid_lower -> contributor_id}`` for all configured Entra identities.

        Mirrors ``build_keystore()`` — returns a plain ``{key: contributor_id}``
        dict that the EntraResolver can use for O(1) lookup after extracting the
        ``oid`` claim from a validated JWT.

        Keys are lowercased as a belt-and-suspenders guarantee: the field
        validator already normalizes them, but the resolver also lowercases the
        JWT ``oid`` claim before lookup, so both sides use the same casing.
        """
        if not self.entra_identities:
            return {}
        return {oid.lower(): meta["id"] for oid, meta in self.entra_identities.items()}

    # -------------------------------------------------------------------------
    # Admin API key (static mode only — gates /admin/* map-mutation endpoints)
    # -------------------------------------------------------------------------
    # admin_api_key is a separate credential from the data-auth api_keys.
    # It is set via the YAML config file (same CONFIG_FILE that carries api_keys)
    # and/or the env var below (env overrides YAML — standard pydantic-settings
    # priority).  Empty string is normalised to None so that admin_api_key: ""
    # in a YAML template behaves identically to omitting the field.
    admin_api_key: str | None = None

    @field_validator("admin_api_key", mode="before")
    @classmethod
    def _normalize_admin_api_key(cls, v: object) -> str | None:
        """Normalize empty string to None (mirrors _normalize_api_key)."""
        return None if v == "" else v  # type: ignore[return-value]

    # -------------------------------------------------------------------------
    # Entra admin role (entra mode only — gates /admin/* map-mutation endpoints)
    # -------------------------------------------------------------------------
    # entra_admin_role is the Entra App Role name whose presence in a token's
    # `roles` claim grants access to /admin/* endpoints.  The role is created
    # in the App Registration (approles-patch.json).
    #
    # Empty string ("") means the admin API is DISABLED in entra mode (callers
    # receive 503).  The default "IdentityAdmin" matches the App Registration
    # role defined for the pilot.  Override via YAML or env var to rename.
    #
    # NOTE: the check ONLY reads the `roles` claim — NEVER `groups`.  A value
    # in the `groups` claim must NOT grant admin access (TB-09 / design §6).
    entra_admin_role: str = "IdentityAdmin"

    @field_validator("entra_admin_role", mode="before")
    @classmethod
    def _normalize_entra_admin_role(cls, v: object) -> str:
        """Normalize None → '' so that entra_admin_role: null disables the admin API."""
        if v is None:
            return ""
        return str(v)

    # -------------------------------------------------------------------------
    # Durable identity-map store paths
    # -------------------------------------------------------------------------
    # These paths control where the two JSON identity-map files live on the
    # Azure Files volume (/data).  Both are env/YAML overridable to allow
    # non-default layouts in development or custom deployments.
    #
    # api_keys_store_path:          SHA-256 digest → contributor map (static mode)
    # entra_identities_store_path:  OID → contributor map (entra mode)
    api_keys_store_path: str = "/data/identity/api-keys.json"
    entra_identities_store_path: str = "/data/identity/entra-identities.json"

    # -------------------------------------------------------------------------
    # Neo4j
    # -------------------------------------------------------------------------
    neo4j_url: str = "neo4j://neo4j:7687"
    neo4j_user: str = "neo4j"
    neo4j_password: str = "password"
    neo4j_browser_url: str = "http://localhost:7474"

    # -------------------------------------------------------------------------
    # Storage paths
    # -------------------------------------------------------------------------
    blob_path: str = "/data/blobs"
    queues_path: str = "/data/queues"

    # -------------------------------------------------------------------------
    # Durable ingest queue
    # -------------------------------------------------------------------------
    # Conservative working defaults pending tuning (design Open Question 4).
    write_concurrency: int = 8  # global cap on concurrent Neo4j-write flushes
    max_delivery_attempts: int = 5  # flush retries for one batch before dead-letter
    # Sub-transaction chunk bounds for _flush_body (issue #278).
    # A chunk closes when EITHER bound trips first: cardinality or payload size.
    neo4j_flush_chunk_rows: int = (
        100  # max rows per sub-transaction (cardinality bound)
    )
    neo4j_flush_chunk_bytes: int = (
        4_194_304  # max serialized bytes per sub-tx (4 MiB payload bound)
    )
    neo4j_lock_timeout: float = (
        30.0  # per-transaction server-side timeout in seconds (Layer B)
    )
    # A conservative default matching max_transaction_retry_time=30s.  Prevents
    # a blocked flush from parking indefinitely when db.lock.acquisition.timeout=0
    # (Neo4j default) holds all write_semaphore permits and stalls the pipeline.
    # Set to 0 to disable (no per-transaction timeout).

    # -------------------------------------------------------------------------
    # Logging
    # -------------------------------------------------------------------------
    log_level: str = "INFO"
    log_path: str = "/data/logs/server.jsonl"

    # -------------------------------------------------------------------------
    # Session lifecycle timeouts
    # -------------------------------------------------------------------------
    dashboard_inactive_timeout: float = 1800.0  # 30 min  — dashboard visibility
    stale_session_timeout: float = 432000.0  # 5 days  — worker reap

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: Type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> Tuple[PydanticBaseSettingsSource, ...]:
        # Priority: programmatic > env vars > YAML file > defaults
        return (
            init_settings,
            env_settings,
            YamlConfigSettingsSource(settings_cls),
            file_secret_settings,
        )


@lru_cache
def get_settings() -> Settings:
    """Return a cached :class:`Settings` instance."""
    return Settings()
