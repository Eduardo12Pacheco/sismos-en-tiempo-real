"""Sockets must be disabled for the default test run."""

from __future__ import annotations

import socket

import pytest
from pytest_socket import SocketBlockedError, SocketConnectBlockedError


def test_outbound_connection_is_blocked() -> None:
    # pytest-socket versions differ: newer ones raise SocketConnectBlockedError
    # for connect(), older ones the base SocketBlockedError. Both mean blocked.
    with pytest.raises((SocketBlockedError, SocketConnectBlockedError)):
        socket.create_connection(("example.com", 80), timeout=1)
