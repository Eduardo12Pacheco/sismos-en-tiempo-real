"""End-to-end proof for S1: the pipeline services have no egress by construction.

Each long-running service container must fail both a TCP connect to a public IP
and DNS resolution of the USGS host, because they only join the internal
``backend`` network. The optional live mode is the only component that joins
``edge`` and is never started by this test.
"""

from __future__ import annotations

import pytest

from tests.e2e.helpers import compose

pytestmark = pytest.mark.e2e

SERVICES = ("ingest", "processor", "sink", "monitor")

CONNECT = "import socket; socket.create_connection(('1.1.1.1', 443), timeout=3)"
RESOLVE = "import socket; socket.getaddrinfo('earthquake.usgs.gov', 443)"


def _probe(service: str, code: str):
    return compose(
        "run",
        "--rm",
        "--no-deps",
        "--entrypoint",
        "python",
        service,
        "-c",
        code,
        check=False,
    )


def test_backend_only_services_cannot_reach_the_internet(stack: None) -> None:
    for service in SERVICES:
        connect = _probe(service, CONNECT)
        assert connect.returncode != 0, (
            f"{service} reached 1.1.1.1:443 but must only join the internal network\n"
            f"{connect.stdout}\n{connect.stderr}"
        )

        resolve = _probe(service, RESOLVE)
        assert resolve.returncode != 0, (
            f"{service} resolved earthquake.usgs.gov but must have no egress\n"
            f"{resolve.stdout}\n{resolve.stderr}"
        )
