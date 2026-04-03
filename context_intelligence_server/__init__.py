"""Context Intelligence Server."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("context-intelligence-server")
except PackageNotFoundError:
    __version__ = "0.0.0"  # fallback for development installs
