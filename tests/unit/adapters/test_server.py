"""Tests for the metrics/health HTTP server and shutdown signal helper."""

from __future__ import annotations

import json
import signal
import threading
from urllib.error import HTTPError
from urllib.request import urlopen

from prometheus_client import CollectorRegistry

from seismic_stream.adapters.observability.metrics import PrometheusMetrics
from seismic_stream.adapters.observability.server import (
    ObservabilityServer,
    Readiness,
    ShutdownSignal,
)


def _get(url: str) -> tuple[int, bytes]:
    try:
        with urlopen(url, timeout=5) as response:
            return response.status, response.read()
    except HTTPError as error:
        return error.code, error.read()


def _metrics_registry() -> CollectorRegistry:
    registry = CollectorRegistry()
    PrometheusMetrics(registry).set_build_info("sink", "0.1.0")
    return registry


def test_metrics_health_and_readiness_endpoints() -> None:
    state = {"readiness": Readiness(False, ("clickhouse unavailable",))}
    server = ObservabilityServer(0, _metrics_registry(), lambda: state["readiness"])
    server.start()
    base = f"http://127.0.0.1:{server.port}"
    try:
        status, body = _get(f"{base}/metrics")
        assert status == 200
        assert b"seismic_build_info" in body

        status, body = _get(f"{base}/healthz")
        assert status == 200
        assert json.loads(body) == {"status": "ok"}

        status, body = _get(f"{base}/readyz")
        assert status == 503
        payload = json.loads(body)
        assert payload["status"] == "unready"
        assert payload["reasons"] == ["clickhouse unavailable"]

        state["readiness"] = Readiness(True)
        status, body = _get(f"{base}/readyz")
        assert status == 200
        assert json.loads(body) == {"status": "ok", "reasons": []}

        status, _ = _get(f"{base}/unknown")
        assert status == 404
    finally:
        server.stop()

    assert server.is_running is False


def test_readiness_probe_errors_are_reported_as_unready() -> None:
    def broken() -> Readiness:
        raise RuntimeError("boom")

    server = ObservabilityServer(0, _metrics_registry(), broken)
    server.start()
    try:
        status, body = _get(f"http://127.0.0.1:{server.port}/readyz")
        assert status == 503
        payload = json.loads(body)
        assert payload["status"] == "unready"
        assert "boom" in payload["reasons"][0]
    finally:
        server.stop()


def test_port_zero_binds_an_ephemeral_port() -> None:
    server = ObservabilityServer(0, _metrics_registry(), lambda: Readiness(True))
    server.start()
    try:
        assert server.port > 0
    finally:
        server.stop()


def test_shutdown_signal_sets_the_event_and_restores_handlers() -> None:
    previous = signal.getsignal(signal.SIGTERM)
    shutdown = ShutdownSignal(signals=(signal.SIGTERM,))
    try:
        shutdown.install()
        signal.raise_signal(signal.SIGTERM)
        assert shutdown.stop_event.is_set()
        assert shutdown.wait(0.1) is True
    finally:
        shutdown.restore()

    assert signal.getsignal(signal.SIGTERM) is previous


def test_server_can_be_used_as_a_context_manager() -> None:
    server = ObservabilityServer(0, _metrics_registry(), lambda: Readiness(True))
    with server:
        assert server.is_running is True
        status, _ = _get(f"http://127.0.0.1:{server.port}/healthz")
        assert status == 200
    assert server.is_running is False
    assert threading.active_count() >= 1
