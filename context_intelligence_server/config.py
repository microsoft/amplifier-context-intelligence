"""Configuration via pydantic-settings for the Context Intelligence Server.

Values are resolved in this priority order (highest first):

1. Environment variables (``AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_*``).
2. YAML configuration file — path from the
   ``AMPLIFIER_CONTEXT_INTELLIGENCE_SERVER_CONFIG_FILE`` environment variable,
   or ``server-config.yaml`` in the working directory if it exists.
3. Built-in defaults.
"""

import os
from functools import lru_cache
from pathlib import Path
from typing import Any, Tuple, Type

import yaml
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
        cursor_path: /home/user/.local/share/ci-server/cursors

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
    # Neo4j
    # -------------------------------------------------------------------------
    neo4j_url: str = "neo4j://neo4j:7687"
    neo4j_user: str = "neo4j"
    neo4j_password: str = "password"

    # -------------------------------------------------------------------------
    # Storage paths
    # -------------------------------------------------------------------------
    blob_path: str = "/data/blobs"
    cursor_path: str = "/data/cursors"

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
    cursor_persist_ttl: float = 15552000.0  # 180 days — cursor file TTL

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
