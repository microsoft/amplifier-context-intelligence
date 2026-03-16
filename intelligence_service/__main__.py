"""Entry point for running the Intelligence Service."""

import uvicorn

from intelligence_service.config import get_settings


def main() -> None:
    """Start the Intelligence Service using settings from environment."""
    settings = get_settings()
    uvicorn.run(
        "intelligence_service.app:app",
        host=settings.server_host,
        port=settings.server_port,
        log_level=settings.log_level.lower(),
    )


if __name__ == "__main__":
    main()
