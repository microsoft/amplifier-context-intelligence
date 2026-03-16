"""Configuration module for the Intelligence Service.

Settings are loaded from environment variables with the AMPLIFIER_CONTEXT_INTELLIGENCE_SERVICE_ prefix.
Use get_settings() to obtain the singleton Settings instance.
"""

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Intelligence Service configuration."""

    model_config = SettingsConfigDict(
        env_prefix="AMPLIFIER_CONTEXT_INTELLIGENCE_SERVICE_"
    )

    server_host: str = "0.0.0.0"
    server_port: int = 8100
    # Used by the event-ingestion forwarding path (not yet wired in app.py)
    ingestion_url: str = "http://context-intelligence-server:8000"
    bundle_name: str = "context-intelligence-server"
    drain_timeout_seconds: int = 30
    max_sessions: int = 50
    blob_path: str = "/data/blobs"
    log_level: str = "INFO"
    runtime_state_path: str = "/data/intelligence-runtime"
    workspace_path: str = "/data/intelligence-runtime/workspace"
    routing_matrix: str = "balanced"


@lru_cache
def get_settings() -> Settings:
    """Return the cached Settings singleton."""
    return Settings()
