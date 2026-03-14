"""Configuration via pydantic-settings for the Context Intelligence Server."""

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(env_prefix="CI_SERVER_")

    server_host: str = "0.0.0.0"
    server_port: int = 8000
    neo4j_url: str = "neo4j://neo4j:7687"
    neo4j_user: str = "neo4j"
    neo4j_password: str = "password"
    blob_path: str = "/data/blobs"
    log_level: str = "INFO"


@lru_cache
def get_settings() -> Settings:
    """Return cached Settings instance."""
    return Settings()
