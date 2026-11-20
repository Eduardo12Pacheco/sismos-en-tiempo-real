"""Lightweight metrics, health and readiness HTTP server.

Every long-running service runs this stdlib server in a daemon thread next to
its main loop. It exposes Prometheus/OpenMetrics text on ``/metrics`` and JSON
on ``/healthz`` and ``/readyz``, so the Compose healthchecks and Prometheus
scrape work without pulling in a web framework.
"""

from __future__ import annotations

import json
import logging
import signal
import threading
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import FrameType
from typing import Any
from urllib.parse import urlsplit

from prometheus_client import CollectorRegistry
from prometheus_client.openmetrics.exposition import CONTENT_TYPE_LATEST, generate_latest

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class Readiness:
    """Result of a readiness probe: ready or not, with human-readable reasons."""

    ready: bool
    reasons: tuple[str, ...] = ()


ReadinessProbe = Callable[[], Readiness]


def _handler_factory(
    registry: CollectorRegistry, readiness: ReadinessProbe
) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self) -> None:
            path = urlsplit(self.path).path
            if path == "/metrics":
                # prometheus-client ships no type hints for this helper.
                body = generate_latest(registry)  # type: ignore[no-untyped-call]
                self._send(HTTPStatus.OK, body, CONTENT_TYPE_LATEST)
            elif path == "/healthz":
                self._send_json(HTTPStatus.OK, {"status": "ok"})
            elif path == "/readyz":
                self._ready()
            else:
                self._send_json(HTTPStatus.NOT_FOUND, {"status": "not_found"})

        def _ready(self) -> None:
            try:
                state = readiness()
            except Exception as error:
                self._send_json(
                    HTTPStatus.SERVICE_UNAVAILABLE,
                    {"status": "unready", "reasons": [f"readiness probe failed: {error}"]},
                )
                return
            if state.ready:
                self._send_json(HTTPStatus.OK, {"status": "ok", "reasons": []})
            else:
                self._send_json(
                    HTTPStatus.SERVICE_UNAVAILABLE,
                    {"status": "unready", "reasons": list(state.reasons)},
                )

        def _send_json(self, status: HTTPStatus, payload: dict[str, Any]) -> None:
            body = json.dumps(payload, sort_keys=True).encode("utf-8")
            self._send(status, body, "application/json")

        def _send(self, status: HTTPStatus, body: bytes, content_type: str) -> None:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:
            LOGGER.debug("observability request", extra={"detail": format % args})

    return Handler


class ObservabilityServer:
    """Serve ``/metrics``, ``/healthz`` and ``/readyz`` from a daemon thread."""

    def __init__(
        self,
        port: int,
        registry: CollectorRegistry,
        readiness: ReadinessProbe,
        host: str = "0.0.0.0",
    ) -> None:
        if not 0 <= port <= 65535:
            raise ValueError(f"port must be between 0 and 65535, got {port!r}")
        self._requested_port = port
        self._registry = registry
        self._readiness = readiness
        self._host = host
        self._httpd: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None

    @property
    def port(self) -> int:
        """Bound port (useful when the server was started with port ``0``)."""
        if self._httpd is None:
            return self._requested_port
        return int(self._httpd.server_address[1])

    @property
    def is_running(self) -> bool:
        """Whether the serving thread is alive."""
        return self._thread is not None and self._thread.is_alive()

    def start(self) -> None:
        """Bind the port and start serving in a daemon thread."""
        if self._httpd is not None:
            raise RuntimeError("observability server is already running")
        httpd = ThreadingHTTPServer(
            (self._host, self._requested_port),
            _handler_factory(self._registry, self._readiness),
        )
        httpd.daemon_threads = True
        self._httpd = httpd
        self._thread = threading.Thread(
            target=httpd.serve_forever,
            name="observability-server",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        """Stop serving, close the socket and join the thread."""
        httpd = self._httpd
        thread = self._thread
        if httpd is None:
            return
        httpd.shutdown()
        httpd.server_close()
        if thread is not None:
            thread.join(timeout=5)
        self._httpd = None
        self._thread = None

    def __enter__(self) -> ObservabilityServer:
        self.start()
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.stop()


class ShutdownSignal:
    """Set a :class:`threading.Event` when SIGTERM/SIGINT arrives.

    The previous handlers are restored on :meth:`restore`, which keeps the
    helper safe to use inside tests and embedding applications.
    """

    def __init__(self, signals: Iterable[int] = (signal.SIGTERM, signal.SIGINT)) -> None:
        self._event = threading.Event()
        self._signals = tuple(signals)
        self._previous: list[tuple[int, Any]] = []
        self._installed = False

    @property
    def stop_event(self) -> threading.Event:
        """Event set by the signal handler."""
        return self._event

    def install(self) -> ShutdownSignal:
        """Register the handlers (main thread only)."""
        if self._installed:
            return self
        for signum in self._signals:
            self._previous.append((signum, signal.getsignal(signum)))
            signal.signal(signum, self._handle)
        self._installed = True
        return self

    def restore(self) -> None:
        """Restore the handlers that were installed before :meth:`install`."""
        if not self._installed:
            return
        for signum, previous in reversed(self._previous):
            signal.signal(signum, previous)
        self._previous.clear()
        self._installed = False

    def _handle(self, signum: int, frame: FrameType | None) -> None:
        LOGGER.info("shutdown signal received", extra={"signal": signum})
        self._event.set()

    def wait(self, timeout: float | None = None) -> bool:
        """Block until a signal was received or ``timeout`` expires."""
        return self._event.wait(timeout)

    def __enter__(self) -> ShutdownSignal:
        return self.install()

    def __exit__(self, *exc_info: object) -> None:
        self.restore()
