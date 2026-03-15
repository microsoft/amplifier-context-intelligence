"""Intelligence Service FastAPI application."""

import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(application: FastAPI) -> AsyncGenerator[None, None]:
    """Manage application startup and shutdown lifecycle."""
    logger.info("Intelligence Service starting up")
    yield
    logger.info("Intelligence Service shutting down")


app = FastAPI(title="Intelligence Service", lifespan=lifespan)


@app.get("/health")
async def health() -> dict[str, str]:
    """Return service health status."""
    return {"status": "ok"}


@app.get("/admin/reload-bundle")
async def reload_bundle() -> dict[str, str]:
    """Stub endpoint for bundle reload (not yet implemented)."""
    return {
        "status": "reload_not_implemented",
        "message": "Bundle reload will be available when agent integration is complete.",
    }
