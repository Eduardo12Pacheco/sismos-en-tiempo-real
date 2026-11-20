"""realtime-seismic-stream: local real-time earthquake streaming pipeline."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("realtime-seismic-stream")
except PackageNotFoundError:  # pragma: no cover - only when running from a source tree
    __version__ = "0.0.0"

__all__ = ["__version__"]
