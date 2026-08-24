"""Configuration via pydantic-settings for the Context Intelligence Server.

Values are resolved in this priority order (highest first):

1. Environment variables (``AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_*``).
2. YAML configuration file — path from the
   ``AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_CONFIG_FILE`` environment variable,
   or ``server-config.yaml`` in the working directory if it exists.
3. Built-in defaults.
"""

import hashlib
import logging
import math
import os
import re
from functools import lru_cache
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, field_validator, model_validator
from pydantic.fields import FieldInfo
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)

# Read directly from the environment (not the AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_
# prefix) before Settings is constructed.
_CONFIG_FILE_ENV = "AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_CONFIG_FILE"
_CONFIG_FILE_DEFAULT = "server-config.yaml"

logger = logging.getLogger(__name__)

# Removed/renamed config keys. Pydantic silently drops unknown keys, so warn
# loudly instead of failing -- the server must still boot.
_REMOVED_CONFIG_KEYS: dict[str, str] = {
    "web_ui_enabled": (
        "removed -- the server is headless-only and has no web UI toggle; "
        "delete this key from your config"
    ),
    "dashboard_inactive_timeout": (
        "renamed to 'status_inactive_timeout' (same units/behaviour); "
        "rename this key or your override is ignored and the default is used"
    ),
}

# ---------------------------------------------------------------------------
# GUID validation helpers (Entra identities)
# ---------------------------------------------------------------------------
# 8-4-4-4-12 lowercase hex, fullmatch()'d so braces/urn:uuid: prefixes/trailing
# junk are rejected without explicit anchors.
_GUID_RE = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
# Placeholder sentinel; never a valid identity.
_ALL_ZEROS_GUID = "00000000-0000-0000-0000-000000000000"


def _validate_identity_map(
    v: dict[str, dict[str, str]] | None,
    field_name: str,
    *,
    allow_empty: bool = False,
) -> dict[str, dict[str, str]] | None:
    """Shared validator for GUID-keyed identity maps (entra_identities, service_identities).

    - ``None`` passes through (optional field).
    - Empty dict is rejected unless ``allow_empty=True`` (entra_identities only).
    - Keys must be valid lowercase GUIDs (8-4-4-4-12); all-zeros is rejected.
    - Every value must carry a non-empty, non-whitespace ``id``.
    - Keys are normalized to lowercase.
    """
    if v is None:
        return None
    if len(v) == 0:
        if allow_empty:
            return {}
        raise ValueError(
            f"{field_name} must contain at least one entry if specified; "
            "omit it or use null to disable"
        )
    normalized: dict[str, dict[str, str]] = {}
    for oid, meta in v.items():
        oid_lower = oid.lower()
        if not _GUID_RE.fullmatch(oid_lower):
            raise ValueError(
                f"{field_name} key {oid!r} must be a valid GUID "
                "(xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)"
            )
        if oid_lower == _ALL_ZEROS_GUID:
            raise ValueError(
                f"{field_name} key {oid!r} must not be the all-zeros GUID; "
                "use the real oid from 'az ad signed-in-user show --query id -o tsv'"
            )
        contributor_id = meta.get("id")
        if not isinstance(contributor_id, str) or not contributor_id.strip():
            raise ValueError(
                f"{field_name}[{oid!r}]['id'] must be a non-empty, "
                f"non-whitespace string, got {contributor_id!r}"
            )
        normalized[oid_lower] = meta
    return normalized


def _build_identity_map_from(
    identity_dict: dict[str, dict[str, str]] | None,
) -> dict[str, str]:
    """Shared helper: return ``{oid_lower -> id}`` for a GUID-keyed identity map.

    Returns an empty dict when ``identity_dict`` is ``None`` or empty.
    Keys are lowercased as a belt-and-suspenders guarantee: the field validator
    already normalizes them, but both ``build_identity_map()`` and
    ``build_service_identity_map()`` need identical casing behaviour.
    """
    if not identity_dict:
        return {}
    return {oid.lower(): meta["id"] for oid, meta in identity_dict.items()}


def _default_identity_store_path(filename: str) -> str:
    """Return a host-install-writable default path for an identity-map store file.

    Defaults under the invoking user's own data dir rather than a container-only
    /data mount. Container deployments override these paths explicitly.
    """
    return str(Path.home() / ".local" / "share" / "ci-server" / "identity" / filename)


class YamlConfigSettingsSource(PydanticBaseSettingsSource):
    """Load settings from a YAML configuration file.

    Path resolution order: constructor ``yaml_file`` arg, then
    ``AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_CONFIG_FILE``, then
    ``server-config.yaml`` in cwd (skipped if absent).

    Keys match :class:`Settings` field names (no prefix); unknown keys are
    ignored. Environment variables take precedence over YAML values.
    """

    def __init__(
        self,
        settings_cls: type[BaseSettings],
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
        # Warn (do not fail) when the YAML still carries a config key that was
        # removed/renamed: pydantic silently drops unknown keys, so without this
        # the drop would be invisible on an upgrade.
        for key in self._data:
            if (
                key in _REMOVED_CONFIG_KEYS
                and key not in self.settings_cls.model_fields
            ):
                logger.warning(
                    "Ignoring obsolete config key %r in %s: %s",
                    key,
                    self.yaml_file,
                    _REMOVED_CONFIG_KEYS[key],
                )
        return {
            k: v for k, v in self._data.items() if k in self.settings_cls.model_fields
        }


class Neo4jClientConfig(BaseModel):
    """One Neo4j logical client (admin OR cypher_query). Same shape for both."""

    url: str
    username: str = "neo4j"
    password: str = ""
    # "WRITE" for admin, "READ" for cypher_query. On a Community single
    # instance over bolt:// this is a routing hint, not server-side enforcement.
    access_mode: Literal["READ", "WRITE"] = "WRITE"

    @property
    def auth(self) -> tuple[str, str] | None:
        """Return (username, password), or None when password is empty.

        Mirrors the existing registry.py semantics: an empty password means
        "no auth" (None), so behavior is identical to today's flat path.
        """
        return (self.username, self.password) if self.password else None


class Neo4jConfig(BaseModel):
    """The structured `neo4j` block: two same-shaped clients.

    Both sub-clients are required when present, so the startup guard's only
    fallback case to detect is `Settings.neo4j is None`.
    """

    admin: Neo4jClientConfig
    cypher_query: Neo4jClientConfig

    @model_validator(mode="after")
    def _validate_access_modes(self) -> "Neo4jConfig":
        """Enforce `admin.access_mode == "WRITE"` and `cypher_query.access_mode == "READ"`.

        Fails loud (both violations reported together) rather than letting a
        copy-pasted `cypher_query` block silently behave as WRITE-capable.
        """
        errors: list[str] = []
        if self.admin.access_mode != "WRITE":
            errors.append(
                f"neo4j.admin.access_mode must be 'WRITE', got "
                f"{self.admin.access_mode!r}"
            )
        if self.cypher_query.access_mode != "READ":
            errors.append(
                f"neo4j.cypher_query.access_mode must be 'READ', got "
                f"{self.cypher_query.access_mode!r}"
            )
        if errors:
            raise ValueError(
                "Neo4j client config invariant violated: " + "; ".join(errors)
            )
        return self


class Settings(BaseSettings):
    """Application settings for the Context Intelligence Server."""

    model_config = SettingsConfigDict(
        env_prefix="AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_",
        env_nested_delimiter="__",
    )

    # -------------------------------------------------------------------------
    # Server
    # -------------------------------------------------------------------------
    server_host: str = "0.0.0.0"
    server_port: int = 8000

    # Gunicorn worker timeouts (run() in main.py). Crash-recovery boot work is
    # O(backlog size) and can take minutes; raise these for a deployment that
    # expects a slow/large-backlog boot.
    #
    # gunicorn_worker_timeout: seconds a silent worker is allowed before kill.
    # gunicorn_graceful_timeout: seconds to finish in-flight work after SIGTERM.
    gunicorn_worker_timeout: int = 30
    gunicorn_graceful_timeout: int = 10

    # -------------------------------------------------------------------------
    # Authentication
    # -------------------------------------------------------------------------
    api_key: str | None = None

    @field_validator("api_key", mode="before")
    @classmethod
    def _normalize_api_key(cls, v: str | None) -> str | None:
        """Normalize empty string to None so that api_key: '' in config disables auth."""
        return None if v == "" else v

    # Per-contributor API keys (NESTED form): the keystore is keyed by
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

        Rejects an empty dict, a key that isn't 64 lowercase-hex chars after
        normalization, or a value with a missing/empty ``id``. Digest keys are
        lowercased so an uppercase digest still maps to
        ``hashlib.sha256(...).hexdigest()``.
        """
        if v is None:
            return None
        # An explicitly empty map is a SUPPORTED bootstrap state (symmetric with
        # entra_identities): the server boots fail-CLOSED with zero keys and is
        # populated at runtime via the admin API (PUT /admin/keys/{sha256hash}).
        if len(v) == 0:
            return {}
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
    # "static" = sha256 keystore; "entra" = JWT validation via Entra/JWKS.
    # Exactly one mode is active at a time.
    auth_mode: Literal["static", "entra"] = "static"

    # The sole fail-open trigger in the server. An empty keystore/identity map
    # boots fail-closed (401/403 until populated via /admin). Setting this True
    # with no credentials configured makes every request pass unauthenticated
    # ("WIDE OPEN" warning at startup) -- test/dev only, never production. No
    # effect in auth_mode=entra (EntraResolver.auth_enabled is always True).
    allow_unauthenticated: bool = False

    # App Registration coordinates; both required when auth_mode="entra".
    # Empty/whitespace strings normalize to None so a blank YAML placeholder
    # triggers a clear startup error instead of a silent wrong-value lookup.
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

    # oid -> contributor map: { "<oid-GUID>": {"id": "<contributor>"} }.
    # oid is stored verbatim (not hashed -- it's already a public id). Many
    # oids may map to one contributor.
    # NOTE: oid is a persistent personal identifier -- do not commit real
    # values to product repos; use env/secret injection or a git-ignored map.
    entra_identities: dict[str, dict[str, str]] | None = None

    @field_validator("entra_identities", mode="after")
    @classmethod
    def _validate_entra_identities(
        cls, v: dict[str, dict[str, str]] | None
    ) -> dict[str, dict[str, str]] | None:
        """Fail-closed: raise unless every entry is ``<GUID> -> {"id": <non-empty str>}``."""
        return _validate_identity_map(v, "entra_identities", allow_empty=True)

    @model_validator(mode="after")
    def _validate_entra_config(self) -> "Settings":
        """When auth_mode='entra', require azure_client_id and azure_tenant_id.

        entra_identities is not required (empty/omitted is a supported
        bootstrap state). Names every missing field in one ValueError.
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
            if errors:
                raise ValueError(
                    "Entra auth misconfiguration (startup refused): "
                    + "; ".join(errors)
                )
        return self

    def build_identity_map(self) -> dict[str, str]:
        """Return ``{oid_lower -> contributor_id}`` for all configured Entra identities.

        Mirrors ``build_keystore()`` for O(1) lookup after extracting the
        ``oid`` claim from a validated JWT.
        """
        return _build_identity_map_from(self.entra_identities)

    # -------------------------------------------------------------------------
    # M2 non-interactive auth: service / app-token identity path
    # -------------------------------------------------------------------------
    # OID -> contributor map for service principals / managed identities.
    # Same shape and validation as entra_identities; config-only (no durable
    # store). Optional; doesn't participate in _validate_entra_config.
    service_identities: dict[str, dict[str, str]] | None = None

    @field_validator("service_identities", mode="after")
    @classmethod
    def _validate_service_identities(
        cls, v: dict[str, dict[str, str]] | None
    ) -> dict[str, dict[str, str]] | None:
        """Fail-closed: same GUID-map rules as entra_identities (shared helper)."""
        return _validate_identity_map(v, "service_identities")

    def build_service_identity_map(self) -> dict[str, str]:
        """Return ``{oid_lower -> contributor_id}`` for all configured service identities."""
        return _build_identity_map_from(self.service_identities)

    # -------------------------------------------------------------------------
    # Admin API key (static mode only — gates /admin/* map-mutation endpoints)
    # -------------------------------------------------------------------------
    # Separate credential from the data-auth api_keys. Empty string normalizes
    # to None so admin_api_key: "" behaves like omitting the field.
    admin_api_key: str | None = None

    @field_validator("admin_api_key", mode="before")
    @classmethod
    def _normalize_admin_api_key(cls, v: object) -> str | None:
        """Normalize empty string to None (mirrors _normalize_api_key)."""
        return None if v == "" else v  # type: ignore[return-value]

    # Recommended way to configure the admin key: store the SHA-256 hex digest
    # at rest, never the raw token. The legacy raw ``admin_api_key`` still
    # works (hashed at load time) but is deprecated; when both are set, this
    # wins. Empty string normalizes to None.
    admin_api_key_sha256: str | None = None

    @field_validator("admin_api_key_sha256", mode="before")
    @classmethod
    def _normalize_admin_api_key_sha256(cls, v: object) -> str | None:
        """Normalize empty string to None (mirrors _normalize_admin_api_key)."""
        return None if v == "" else v  # type: ignore[return-value]

    @field_validator("admin_api_key_sha256", mode="after")
    @classmethod
    def _validate_admin_api_key_sha256(cls, v: str | None) -> str | None:
        """Fail-closed: require a 64-char lowercase SHA-256 hex digest (or None).

        Mirrors ``_validate_api_keys``' digest check so a misconfigured admin
        digest fails loudly at startup rather than silently rejecting every
        admin request at runtime.  An UPPERCASE digest is normalized to
        lowercase so it matches ``hashlib.sha256(...).hexdigest()``.
        """
        if v is None:
            return None
        digest_lower = v.strip().lower()
        if len(digest_lower) != 64 or not all(
            c in "0123456789abcdef" for c in digest_lower
        ):
            raise ValueError(
                f"admin_api_key_sha256 must be a 64-character SHA-256 hex digest, "
                f"got {v!r}. Derive it with: python3 -c "
                f'"import hashlib,sys;print(hashlib.sha256(sys.argv[1].encode())'
                f'.hexdigest())" "<token>" (see docs/managing-api-keys.md).'
            )
        return digest_lower

    def resolve_admin_api_key_digest(self) -> str | None:
        """Return the admin key's sha256-hex digest, or None if not configured.

        Precedence:
        - ``admin_api_key_sha256`` (digest stored at rest, RECOMMENDED) is used
          verbatim (already validated/lowercased).
        - the legacy raw ``admin_api_key`` (DEPRECATED, plaintext at rest) is
          hashed here over its UTF-8 bytes so the derived digest matches the
          bearer token sent in the Authorization header.
        - ``None`` when neither is set (admin API disabled in static mode).

        Pure function (no logging/side effects) so it is safe to call from
        config, request handlers, and status endpoints.  The one-time
        deprecation/precedence warnings are emitted by create_asgi_app().
        """
        if self.admin_api_key_sha256 is not None:
            return self.admin_api_key_sha256
        if self.admin_api_key is not None:
            return hashlib.sha256(self.admin_api_key.encode()).hexdigest()
        return None

    # -------------------------------------------------------------------------
    # Entra admin role (entra mode only — gates /admin/* map-mutation endpoints)
    # -------------------------------------------------------------------------
    # Entra App Role name whose presence in a token's `roles` claim grants
    # access to /admin/* endpoints. Empty string disables the admin API in
    # entra mode (503). Checks ONLY the `roles` claim — never `groups`.
    entra_admin_role: str = "IdentityAdmin"

    @field_validator("entra_admin_role", mode="before")
    @classmethod
    def _normalize_entra_admin_role(cls, v: object) -> str:
        """Normalize None → '' so that entra_admin_role: null disables the admin API."""
        if v is None:
            return ""
        return str(v)

    # Entra App Roles gating service/app-token access: service_data_role for
    # standard Contributor-level data access, reader_role for read-only.
    # Empty string disables the respective path.
    service_data_role: str = "Contributor"
    reader_role: str = "Reader"

    @field_validator("service_data_role", "reader_role", mode="before")
    @classmethod
    def _normalize_service_role_fields(cls, v: object) -> str:
        """Normalize None → '' so that null in config disables the respective role."""
        if v is None:
            return ""
        return str(v)

    # -------------------------------------------------------------------------
    # Durable identity-map store paths
    # -------------------------------------------------------------------------
    # Where the two JSON identity-map files live; both env/YAML overridable.
    # api_keys_store_path:          SHA-256 digest → contributor map (static mode)
    # entra_identities_store_path:  OID → contributor map (entra mode)
    api_keys_store_path: str = _default_identity_store_path("api-keys.json")
    entra_identities_store_path: str = _default_identity_store_path(
        "entra-identities.json"
    )

    # -------------------------------------------------------------------------
    # Neo4j
    # -------------------------------------------------------------------------
    neo4j_url: str = "neo4j://neo4j:7687"
    neo4j_user: str = "neo4j"
    neo4j_password: str = "password"
    neo4j_browser_url: str = "http://localhost:7474"

    # Structured two-client config. Optional for backward-compat: when absent,
    # both clients fall back to the legacy flat neo4j_* fields above.
    neo4j: Neo4jConfig | None = None

    # When True, the startup guard refuses to boot on the legacy fallback
    # (neo4j is None) -- admin + cypher_query must be declared explicitly.
    neo4j_require_explicit_clients: bool = False

    def resolve_neo4j_admin(self) -> Neo4jClientConfig:
        """Admin (read/write) client config. Structured block wins; else legacy flat."""
        if self.neo4j is not None:
            return self.neo4j.admin
        return Neo4jClientConfig(
            url=self.neo4j_url,
            username=self.neo4j_user,
            password=self.neo4j_password,
            access_mode="WRITE",
        )

    def resolve_neo4j_query(self) -> Neo4jClientConfig:
        """Read-intent client config. Structured block wins; else legacy flat + READ."""
        if self.neo4j is not None:
            return self.neo4j.cypher_query
        return Neo4jClientConfig(
            url=self.neo4j_url,
            username=self.neo4j_user,
            password=self.neo4j_password,
            access_mode="READ",
        )

    # Upper bound on concurrent bolt connections for a driver shared across many
    # logical callers (the lifespan admin driver, the registry's per-session
    # driver). Well under the server's default bolt thread-pool size so a
    # driver leak can no longer starve it.
    neo4j_max_connection_pool_size: int = 50

    # Recycles a pooled connection after this many seconds, so a long-idle
    # connection cannot accumulate indefinitely on the server side.
    neo4j_max_connection_lifetime: float = 3600.0

    @field_validator("neo4j_max_connection_pool_size")
    @classmethod
    def _validate_neo4j_max_connection_pool_size(cls, v: int) -> int:
        """Fail loud on a non-positive pool size."""
        if v <= 0:
            raise ValueError(f"neo4j_max_connection_pool_size must be > 0, got {v}")
        return v

    @field_validator("neo4j_max_connection_lifetime")
    @classmethod
    def _validate_neo4j_max_connection_lifetime(cls, v: float) -> float:
        """Fail loud on a non-positive lifetime (must be finite so idle connections recycle)."""
        if v <= 0:
            raise ValueError(f"neo4j_max_connection_lifetime must be > 0, got {v}")
        return v

    # -------------------------------------------------------------------------
    # Storage paths
    # -------------------------------------------------------------------------
    # blob_backend selects the BlobStore implementation via
    # context_intelligence_server.blob_store.create_blob_store(); "filesystem"
    # is the only backend implemented today. blob_path is that filesystem
    # backend's own root -- read only by the factory (and here, at
    # declaration) and otherwise meaningless to any other backend.
    blob_backend: str = "filesystem"
    blob_path: str = "/data/blobs"
    queues_path: str = "/data/queues"

    # -------------------------------------------------------------------------
    # Durable ingest queue
    # -------------------------------------------------------------------------
    write_concurrency: int = 8  # global cap on concurrent Neo4j-write flushes
    max_delivery_attempts: int = 5  # flush retries for one batch before dead-letter
    # Sub-transaction chunk bounds for _flush_body. A chunk closes when EITHER
    # bound trips first: cardinality or payload size.
    neo4j_flush_chunk_rows: int = (
        100  # max rows per sub-transaction (cardinality bound)
    )
    neo4j_flush_chunk_bytes: int = (
        4_194_304  # max serialized bytes per sub-tx (4 MiB payload bound)
    )
    neo4j_lock_timeout: float = 30.0  # per-transaction server-side timeout in seconds
    # Prevents a blocked flush from parking indefinitely when
    # db.lock.acquisition.timeout=0 holds all write_semaphore permits and
    # stalls the pipeline. 0 disables the per-transaction timeout.

    # Hard ceiling on drainers respawned from the recovered backlog on THIS
    # boot (mirrors write_concurrency's role, but bounds the respawn loop in
    # lifespan() rather than write-flush concurrency). The remainder are
    # deferred, left untouched on disk (still durable; recoverable on a later
    # boot, or instantly via get_or_create() on a new event). Never silent:
    # lifespan() logs the respawned/deferred counts, and /status's spool
    # block makes the backlog observable continuously.
    #
    # None means unbounded; with an unbounded ceiling no sweep task starts
    # (see crash_recovery_sweep_interval_seconds below), so a recovered drainer
    # that runs dry with no terminal record
    # (the common shape of a legacy/crashed backlog) never freed its slot
    # and was never replaced: there was no real protection against the OOM
    # this field exists to prevent. 8 is a pessimistic, single-line-
    # overridable default: an operator who can safely respawn more may raise
    # it; one who cannot is now protected OUT OF THE BOX. 0 remains a valid
    # explicit opt-out (never respawn automatically at boot; the deferred
    # tail then drains only via a new event or the periodic sweep).
    crash_recovery_respawn_limit: int | None = 8

    @field_validator("crash_recovery_respawn_limit")
    @classmethod
    def _validate_crash_recovery_respawn_limit(cls, v: int | None) -> int | None:
        """Fail loud on a negative ceiling; None (unbounded) and 0 are valid."""
        if v is not None and v < 0:
            raise ValueError(
                "crash_recovery_respawn_limit must be a non-negative integer "
                f"or null (unbounded), got {v}"
            )
        return v

    # Deferred-backlog sweep interval (seconds); only relevant when
    # crash_recovery_respawn_limit is finite. Periodically re-runs recover()
    # and tops the drainer pool back up to the ceiling so the deferred tail
    # keeps advancing instead of stalling until a restart or new event.
    # 0 disables the sweep (deferred tail then drains only on restart/new event).
    crash_recovery_sweep_interval_seconds: int = 60

    @field_validator("crash_recovery_sweep_interval_seconds")
    @classmethod
    def _validate_crash_recovery_sweep_interval(cls, v: int) -> int:
        """Fail loud on a negative interval; 0 (disabled) and positive are valid."""
        if v < 0:
            raise ValueError(
                "crash_recovery_sweep_interval_seconds must be a non-negative "
                f"integer (0 disables the sweep), got {v}"
            )
        return v

    # A bad `.offset` (unparseable, negative, or past-EOF) below this many
    # bytes is reset (re-drained from byte 0, bounded and idempotent) rather
    # than deleted outright; at/above the threshold the `.log` is deleted too.
    # 0 means "always delete".
    reclaim_redrain_max_bytes: int = 64 * 1024 * 1024

    # Reclaim pass's DELETE/RESET_OFFSET actions ship disabled by default.
    # With this False, boot still classifies every key and logs the same
    # audit line (action=dry_run), but nothing is unlinked. First-deploy
    # sequence: boot dry-run, review boot_reclaim_histogram, then opt in.
    reclaim_enabled: bool = False

    # The queue is a transient buffer, not an archive -- an open,
    # actively-draining session's already-committed prefix is reclaimed
    # continuously (not just at session:end). Ships True: the decision input
    # is the committed offset, the single value the durability design already
    # trusts, and delete_drained -- already shipped, always on -- deletes the
    # entire file on exactly this same evidence. False is a
    # config-change-plus-restart kill switch, not a live toggle.
    queue_compact_enabled: bool = True

    # Bounds compaction frequency on a continuously-hot session: below this
    # many committed bytes, the rewrite is skipped (the idle path closes the
    # gap for free once the session goes idle).
    queue_compact_min_prefix_bytes: int = 8 * 1024 * 1024

    # Separate flag from reclaim_enabled: the log-less + stale-mtime predicate
    # is a structural proof (not a heuristic), so gating this on
    # reclaim_enabled would let dead-letters accumulate forever by default.
    # Ships False: a dead-letter may be the only surviving copy of an
    # un-recovered event -- never auto-delete it without an explicit opt-in.
    dead_letter_expiry_enabled: bool = False

    # How long a log-less `.dead.jsonl` survives before being expired.
    # Long enough that an operator who notices a dead-letter via
    # `GET /queues/dead-letter` or `/status`'s dead count has time to purge or
    # replay it; short enough that the file cannot accumulate indefinitely.
    # <=0 disables expiry outright (an explicit opt-out, not a silent one).
    dead_letter_retention_seconds: float = 30 * 86400.0

    @field_validator("queue_compact_min_prefix_bytes")
    @classmethod
    def _validate_queue_compact_bytes(cls, v: int) -> int:
        """Fail loud on a negative value; 0 is a valid explicit opt-out."""
        if v < 0:
            raise ValueError(
                f"queue_compact_min_prefix_bytes must be a non-negative integer, got {v}"
            )
        return v

    @field_validator("dead_letter_retention_seconds")
    @classmethod
    def _validate_dead_letter_retention_seconds(cls, v: float) -> float:
        """Fail loud on a negative retention; 0 (disabled) is valid."""
        if v < 0:
            raise ValueError(
                "dead_letter_retention_seconds must be a non-negative number "
                f"(0 disables expiry), got {v}"
            )
        return v

    # Hard ceiling per _boot_reconcile phase (heal/reclaim/expire/reconcile/
    # seed/topup). A phase that hangs (e.g. a blocking stat/read on a
    # degraded mount) would otherwise leave boot phase stuck pre-ready
    # forever, latching /status's spool/metrics at null. <=0 disables the
    # per-phase timeout (unbounded wait, pre-existing behavior).
    boot_phase_timeout_seconds: float = 300.0

    @field_validator("boot_phase_timeout_seconds")
    @classmethod
    def _validate_boot_phase_timeout_seconds(cls, v: float) -> float:
        """Fail loud on non-finite input; <=0 is the documented opt-out."""
        if not math.isfinite(v):
            raise ValueError(f"boot_phase_timeout_seconds must be finite, got {v}")
        return v

    # -------------------------------------------------------------------------
    # Writer lease
    # -------------------------------------------------------------------------
    # Refuses boot against a live foreign lease (takes over a stale one);
    # `detect` only observes+heartbeats without ever refusing; `off` disables it.
    writer_lease_mode: Literal["off", "detect", "enforce"] = "enforce"
    # Renew + re-read interval; fixed at 5s for "conflict visible within one
    # heartbeat" well inside a typical revision-overlap window.
    writer_lease_heartbeat_seconds: float = 5.0
    # Staleness window = heartbeat_seconds * this multiplier. Must survive two
    # consecutive missed ticks without a false "stale" verdict.
    writer_lease_staleness_multiplier: float = 3.0
    # Post-write settle delay before the acquire's confirming re-read, to
    # exceed write -> other-reader visibility latency on the shared mount.
    writer_lease_confirm_delay_seconds: float = 1.0
    # Hard bound on the entire acquire/renew (reads, write, confirm sleep) so
    # a hung mount can never block `lifespan` before its `yield` forever.
    writer_lease_acquire_timeout_seconds: float = 5.0
    # One-boot operator escape hatch: force-acquire over a fresh foreign
    # lease. Logs a warning every boot while set and is surfaced on /status.
    writer_lease_force_acquire: bool = False

    # Maintenance mode: the live schema-health gate + /admin/maintenance repair.
    maintenance_probe_ttl_seconds: float = 5.0  # :Node constraint probe cache TTL
    maintenance_retry_after_seconds: int = 30  # Retry-After on the maintenance 503
    # Bounded pre-repair quiesce so ordinary in-flight flushes settle before
    # run_repair (drain poll interval is 0.05s); a flush outliving it is a
    # residual risk the constraint create then fails loud on.
    maintenance_quiesce_seconds: float = 2.0

    @field_validator("writer_lease_heartbeat_seconds")
    @classmethod
    def _validate_writer_lease_heartbeat_seconds(cls, v: float) -> float:
        """Fail loud on a non-positive heartbeat interval."""
        if v <= 0:
            raise ValueError(f"writer_lease_heartbeat_seconds must be > 0, got {v}")
        return v

    @field_validator("writer_lease_staleness_multiplier")
    @classmethod
    def _validate_writer_lease_staleness_multiplier(cls, v: float) -> float:
        """Fail loud below 2.0x (one missed tick would yield a false stale verdict)."""
        if v < 2.0:
            raise ValueError(
                "writer_lease_staleness_multiplier must be >= 2.0 (below 2, "
                f"one missed heartbeat tick yields a false stale verdict), got {v}"
            )
        return v

    @field_validator("writer_lease_confirm_delay_seconds")
    @classmethod
    def _validate_writer_lease_confirm_delay_seconds(cls, v: float) -> float:
        """Fail loud on a negative confirm delay."""
        if v < 0:
            raise ValueError(
                f"writer_lease_confirm_delay_seconds must be >= 0, got {v}"
            )
        return v

    @model_validator(mode="after")
    def _validate_writer_lease_timeout_exceeds_confirm_delay(self) -> "Settings":
        """Cross-field guard: timeout <= confirm delay makes every acquire
        time out, silently disarming the detector -- must fail at config load."""
        if (
            self.writer_lease_acquire_timeout_seconds
            <= self.writer_lease_confirm_delay_seconds
        ):
            raise ValueError(
                "writer_lease_acquire_timeout_seconds must exceed "
                "writer_lease_confirm_delay_seconds (otherwise every acquire "
                f"times out): got acquire_timeout="
                f"{self.writer_lease_acquire_timeout_seconds}, confirm_delay="
                f"{self.writer_lease_confirm_delay_seconds}"
            )
        return self

    # -------------------------------------------------------------------------
    # Logging
    # -------------------------------------------------------------------------
    log_level: str = "INFO"
    log_path: str = "/data/logs/server.jsonl"

    # -------------------------------------------------------------------------
    # Session lifecycle timeouts
    # -------------------------------------------------------------------------
    status_inactive_timeout: float = 1800.0  # 30 min  — /status visibility
    stale_session_timeout: float = 432000.0  # 5 days  — worker reap

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
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
