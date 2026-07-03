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
from collections.abc import Mapping
from functools import lru_cache
from pathlib import Path
from typing import Any, Literal, Tuple, Type

import yaml
from pydantic import BaseModel, field_validator, model_validator
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
# PUBLIC names (doc 14 section 2.3 / C4): promoted so auth.py's
# EntraResolver.resolve_principal_id() (EasyAuth browser-identity path) can
# import the single source of truth for GUID shape + sentinel ban instead of
# duplicating the regex.
GUID_RE = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
# The all-zeros sentinel is explicitly rejected — a placeholder accidentally
# left in config should never authorize anyone.
ALL_ZEROS_GUID = "00000000-0000-0000-0000-000000000000"

# Back-compat aliases: routers/admin.py imports the previous private names.
_GUID_RE = GUID_RE
_ALL_ZEROS_GUID = ALL_ZEROS_GUID


def _validate_identity_map(
    v: dict[str, dict[str, str | bool]] | None,
    field_name: str,
) -> dict[str, dict[str, str | bool]] | None:
    """Shared validator for GUID-keyed identity maps (entra_identities, service_identities).

    Enforces the same rules for both fields so they stay in sync:

    - ``None`` passes through (field is optional).
    - An empty dict is rejected (fail-closed; omit or null-out to disable).
    - Every key must be a valid lowercase GUID in 8-4-4-4-12 form after
      normalization (rejects braces, urn:uuid: prefixes, trailing junk).
    - The all-zeros GUID is rejected (placeholder sentinel).
    - Every value must carry a non-empty, non-whitespace ``id`` string.
    - Keys are normalized to lowercase and returned as such.

    ``field_name`` is included verbatim in error messages so operators can tell
    which field failed at startup.

    The ``str | bool`` value type accommodates entra_identities' additive
    ``"admin"`` key (doc 14 §2.3/C5); ``service_identities`` never populates
    that key but shares this same permissive value type since only ``id`` is
    ever inspected here.
    """
    if v is None:
        return None
    if len(v) == 0:
        raise ValueError(
            f"{field_name} must contain at least one entry if specified; "
            "omit it or use null to disable"
        )
    normalized: dict[str, dict[str, str | bool]] = {}
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
    identity_dict: Mapping[str, Mapping[str, str | bool]] | None,
) -> dict[str, str]:
    """Shared helper: return ``{oid_lower -> id}`` for a GUID-keyed identity map.

    Returns an empty dict when ``identity_dict`` is ``None`` or empty.
    Keys are lowercased as a belt-and-suspenders guarantee: the field validator
    already normalizes them, but both ``build_identity_map()`` and
    ``build_service_identity_map()`` need identical casing behaviour.

    The value type is ``str | bool`` (the additive ``admin`` key, C5); ``id`` is
    guaranteed a ``str`` by ``_validate_identity_map`` (TB-4), so the
    ``isinstance`` narrow below is a static-typing formality that also drops any
    non-``id`` keys (e.g. ``admin``) from the flat contributor map.
    """
    if not identity_dict:
        return {}
    return {
        oid.lower(): cid
        for oid, meta in identity_dict.items()
        if isinstance((cid := meta["id"]), str)
    }


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


class Neo4jClientConfig(BaseModel):
    """One Neo4j logical client (admin OR cypher_query). Same shape for both.

    Future RBAC fields (rbac_role, database, etc.) land in THIS object -- no
    new top-level knobs later (doc 11 Structured config).
    """

    url: str
    username: str = "neo4j"
    password: str = ""
    # access_mode steers session routing/intent. "WRITE" for admin, "READ" for
    # cypher_query. On a Community single instance over bolt:// this is a
    # routing HINT, not server-side enforcement (doc 11 Honest caveat).
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

    When present, BOTH sub-clients are required (pydantic enforces this), so
    the only fallback case the startup guard must detect is
    `Settings.neo4j is None`.
    """

    admin: Neo4jClientConfig
    cypher_query: Neo4jClientConfig

    @model_validator(mode="after")
    def _validate_access_modes(self) -> "Neo4jConfig":
        """Enforce role/access_mode correctness -- fail loud, not silent.

        `Neo4jClientConfig.access_mode` defaults to "WRITE", so a `cypher_query`
        block that is a copy-paste of `admin` (or simply omits `access_mode`)
        would silently behave as a WRITE-capable "read" client -- defeating the
        entire point of the two-client split. Reject that at construction time:

        - `admin.access_mode` MUST be "WRITE" (the read/write client).
        - `cypher_query.access_mode` MUST be "READ" (the read-intent client).

        Both violations are reported together so the operator sees one clear
        message naming exactly which client has the wrong access_mode.
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
        # doc 14 EasyAuth browser-identity spec, C1 (council-binding, unanimous):
        # freeze Settings post-construction. Without this, trust_easyauth_principal
        # (or any other field) could be flipped after boot with no re-validation,
        # silently defeating the fail-closed _validate_entra_config gate above.
        # A repo-wide grep (see doc 14 build report) found NO code that mutates a
        # constructed Settings instance's attributes, so this tightens a guarantee
        # nothing depended on breaking.
        frozen=True,
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

    # trust_easyauth_principal (doc 14 EasyAuth browser-identity spec, section 2.1):
    # honor the EasyAuth-injected X-MS-CLIENT-PRINCIPAL-ID header as a browser
    # identity source inside auth_mode="entra". FAIL-CLOSED default (off).
    #
    # Only meaningful when auth_mode="entra" AND web_ui_enabled=True (enforced by
    # the _validate_entra_config startup gate below). The header is trusted ONLY
    # because EasyAuth is the sole ingress (proven by the deploy-time Test B
    # topology probe, doc 13 Gate #2) -- the app performs NO cryptographic
    # verification of the principal (see EntraResolver.resolve_principal_id).
    # Env: AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_TRUST_EASYAUTH_PRINCIPAL=true
    trust_easyauth_principal: bool = False

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
    # doc 14 (EasyAuth browser-identity spec) §2.3/C5 (FROZEN shape): the value
    # type is widened from dict[str, str] to dict[str, str | bool] SOLELY so an
    # entry can additionally carry "admin": <bool> (default false; consumed in
    # Step 3 -- NOT read anywhere in this spike). Without this widening,
    # pydantic's field-level coercion rejects a literal YAML `admin: true`
    # (a Python bool) before _validate_identity_map ever runs -- confirmed by
    # test_additive_admin_key_validates. service_identities does NOT need this
    # (no browser-admin concept there) and is intentionally left as
    # dict[str, str].
    entra_identities: dict[str, dict[str, str | bool]] | None = None

    @field_validator("entra_identities", mode="before")
    @classmethod
    def _validate_entra_identities_raw_types(cls, v: Any) -> Any:
        """Strict RAW-input type gate (TB-4/TB-5) — runs BEFORE pydantic coercion.

        Adversarial-review hardening. The ``str | bool`` value type widen (C5,
        for the additive ``admin`` key) opened two coercion holes that only a
        ``mode="before"`` validator can close, because by ``mode="after"``
        pydantic has already coerced the values and the original intent is lost:

        - **TB-4** — ``{"id": True}``: bool is truthy, so a naive presence check
          would let ``id`` resolve to a Python ``bool``. (The ``mode="after"``
          ``isinstance(id, str)`` check already rejects this, but we also reject
          it here, explicitly, on the raw value.)
        - **TB-5** — ``{"admin": 1}``: pydantic's ``str | bool`` union coerces
          int ``1`` -> ``True`` *before* the after-validator runs, hiding a
          malformed config. And ``{"admin": "false"}`` (a truthy STRING) would
          survive as ``"false"`` and, if Step 3 consumed it naively, GRANT admin
          — the exact opposite of intent. So ``admin``, if present, MUST be a
          real ``bool`` (reject ``"true"``/``"false"`` strings and ``1``/``0``).

        ``admin`` is a browser-path concept that only exists on
        ``entra_identities`` (doc 14 §2.3/C5), so this gate is entra-only.
        Non-dict shapes are passed through untouched for pydantic / the
        ``mode="after"`` validator to reject with their standard messages.
        """
        if not isinstance(v, dict):
            return v
        for oid, meta in v.items():
            if not isinstance(meta, dict):
                continue  # shape error — let pydantic/after-validator handle it
            if "id" in meta and not isinstance(meta["id"], str):
                raise ValueError(
                    f"entra_identities[{oid!r}]['id'] must be a string, "
                    f"got {type(meta['id']).__name__} ({meta['id']!r})"
                )
            # bool is a subclass of int, but isinstance(1, bool) is False, so
            # this correctly rejects 1/0 while accepting True/False.
            if "admin" in meta and not isinstance(meta["admin"], bool):
                raise ValueError(
                    f"entra_identities[{oid!r}]['admin'] must be a bool "
                    f"(true/false), got {type(meta['admin']).__name__} "
                    f"({meta['admin']!r}); string 'true'/'false' and 1/0 are "
                    "rejected fail-closed so a truthy non-bool cannot grant admin"
                )
        return v

    @field_validator("entra_identities", mode="after")
    @classmethod
    def _validate_entra_identities(
        cls, v: dict[str, dict[str, str | bool]] | None
    ) -> dict[str, dict[str, str | bool]] | None:
        """Fail-closed: raise unless every entry is ``<GUID> -> {"id": <non-empty str>}``.

        Delegates to the shared ``_validate_identity_map()`` helper which enforces
        GUID key validation, the all-zeros sentinel rejection, non-empty ``id``
        requirement, and key lowercasing.  See that function's docstring for the
        full rule set.

        This validator runs in ``mode="after"``, so pydantic has already coerced
        the field as ``dict[str, dict[str, str | bool]]`` before this function is
        called (the ``str | bool`` value type reserves the additive ``"admin"``
        key, doc 14 §2.3/C5).  Non-dict values and non-string ``id`` values are
        caught by pydantic before
        reaching this code.
        """
        return _validate_identity_map(v, "entra_identities")

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
        # doc 14 EasyAuth browser-identity spec, section 2.2: trust_easyauth_principal
        # is only coherent inside the entra+web-UI world. Joined into the SAME
        # aggregated check (separate from the auth_mode=="entra" block above) so
        # this fires even when auth_mode != "entra" (the block above is skipped
        # entirely in that case).
        if self.trust_easyauth_principal:
            easyauth_errors: list[str] = []
            if self.auth_mode != "entra":
                easyauth_errors.append(
                    "trust_easyauth_principal=True requires auth_mode='entra' -- "
                    "there is no identity map to resolve the header against"
                )
            if not self.web_ui_enabled:
                easyauth_errors.append(
                    "trust_easyauth_principal=True requires web_ui_enabled=True -- "
                    "the EasyAuth header is a browser identity source"
                )
            if easyauth_errors:
                raise ValueError(
                    "EasyAuth trust misconfiguration (startup refused): "
                    + "; ".join(easyauth_errors)
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
        return _build_identity_map_from(self.entra_identities)

    # -------------------------------------------------------------------------
    # M2 non-interactive auth: service / app-token identity path
    # -------------------------------------------------------------------------
    # service_identities: the OID → contributor map for service principals /
    # managed identities.  Same shape as entra_identities; lives in config
    # only (no durable store — service identities don't need runtime mutation).
    #
    # Shape: { "<oid-GUID>": {"id": "<contributor>"} }
    #
    # Validation rules are identical to entra_identities (both delegate to the
    # shared _validate_identity_map() helper) — GUID keys, non-empty id, no
    # all-zeros sentinel.
    #
    # This field is OPTIONAL.  The service identity path never participates in
    # the _validate_entra_config cross-field check, so auth_mode=entra boots
    # with only client_id / tenant_id / entra_identities.
    # Value type is ``str | bool`` purely to share the ``_validate_identity_map``
    # helper and ``_build_identity_map_from`` with entra_identities without
    # dict-invariance friction (the ``admin`` key is entra-only and is never
    # read from service identities; only ``id`` is inspected here).
    service_identities: dict[str, dict[str, str | bool]] | None = None

    @field_validator("service_identities", mode="after")
    @classmethod
    def _validate_service_identities(
        cls, v: dict[str, dict[str, str | bool]] | None
    ) -> dict[str, dict[str, str | bool]] | None:
        """Fail-closed: same GUID-map rules as entra_identities (shared helper).

        Delegates to ``_validate_identity_map()``.  See that function's docstring
        for the full rule set.
        """
        return _validate_identity_map(v, "service_identities")

    def build_service_identity_map(self) -> dict[str, str]:
        """Return ``{oid_lower -> contributor_id}`` for all configured service identities.

        Mirrors ``build_identity_map()`` — returns a plain ``{key: contributor_id}``
        dict for O(1) lookup after extracting the ``oid`` claim from an app token.

        Returns ``{}`` when ``service_identities`` is ``None`` or empty.
        """
        return _build_identity_map_from(self.service_identities)

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

    # admin_api_key_sha256 is the RECOMMENDED way to configure the admin key:
    # store the SHA-256 hex digest of the admin token at rest, never the raw
    # token — mirroring how the data-auth ``api_keys`` map stores digests, not
    # tokens (see docs/managing-api-keys.md).  A leak of the config file then
    # yields only a one-way digest, not a usable admin credential.
    #
    # The legacy raw ``admin_api_key`` above still works for back-compat (it is
    # hashed at load time, exactly like the legacy singular ``api_key``), but is
    # DEPRECATED because it stores the secret in plaintext at rest.  When both
    # are set, ``admin_api_key_sha256`` wins and the raw field is ignored
    # (surfaced as a startup warning in create_asgi_app).
    #
    # Set via YAML or the env var
    # ``AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_ADMIN_API_KEY_SHA256``.  Empty
    # string normalises to None (mirrors admin_api_key).
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

    # M2 service role names
    #
    # service_data_role: the Entra App Role name whose presence in an app token's
    # ``roles`` claim grants the standard Contributor-level data access.  This
    # mirrors what a delegated user gets via entra_identities, but for service
    # principals.  Empty string ('') disables the service data path entirely.
    #
    # reader_role: the Entra App Role name granting read-only access.  Empty string
    # disables read-only app-token gating.  Default 'Reader' matches the App
    # Registration role defined for the M2 service path.
    #
    # Both fields normalize None → '' (same pattern as entra_admin_role).
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

    # Structured two-client config (doc 11). OPTIONAL for backward-compat: when
    # absent, BOTH clients fall back to the legacy flat neo4j_* fields above.
    # The real amplifier-online.yaml MUST set this explicitly (see
    # neo4j_require_explicit_clients + the startup guard).
    neo4j: Neo4jConfig | None = None

    # Deployed-profile signal (gap #12). When True, the startup guard REFUSES to
    # boot on the legacy fallback (i.e. neo4j is None) -- the deployed system must
    # declare admin + cypher_query explicitly, even pointing at the same instance.
    # Default False so existing deployments / server-config.yaml keep booting on
    # the legacy fallback during the transition.
    # Env: AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_NEO4J_REQUIRE_EXPLICIT_CLIENTS=true
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
