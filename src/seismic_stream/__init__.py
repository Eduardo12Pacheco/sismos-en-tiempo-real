"""realtime-seismic-stream: local real-time earthquake streaming pipeline."""

import warnings
from importlib.metadata import PackageNotFoundError, version

try:  # authlib is only present through confluent-kafka's optional extras.
    from authlib.deprecate import AuthlibDeprecationWarning
except ImportError:  # pragma: no cover - base installs without Schema Registry extras
    pass
else:
    # authlib's httpx integration warns on import and even re-enables itself with
    # ``simplefilter("always")``; keep third-party log noise out of JSON output.
    warnings.filterwarnings("ignore", category=AuthlibDeprecationWarning)

try:
    __version__ = version("realtime-seismic-stream")
except PackageNotFoundError:  # pragma: no cover - only when running from a source tree
    __version__ = "0.0.0"

__all__ = ["__version__"]
