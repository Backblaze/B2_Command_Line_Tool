######################################################################
#
# File: test/unit/test_license_fetch_timeout.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
"""
The ``license`` command downloads third-party license texts over HTTP. That outbound call
must stop waiting for an unresponsive server within ``REQUEST_TIMEOUT_S`` and raise a
``requests`` timeout instead of hanging the caller.

The server is a real local TCP listener that accepts connections and never answers, so the
actual socket deadline is exercised. No external network is involved.
"""

from __future__ import annotations

import contextlib
import socket
import threading
import time
from types import SimpleNamespace

import pytest
import requests

from b2._internal.console_tool import License

TIMEOUT_SECONDS = 0.3
MAX_ELAPSED_SECONDS = 4.0
# The server drops a silent connection after this long, so a regression that removes the
# client timeout fails the test (connection error) rather than hanging the suite.
SERVER_GIVE_UP_SECONDS = 5.0


@pytest.fixture
def unresponsive_server_url(monkeypatch):
    monkeypatch.setenv('NO_PROXY', '127.0.0.1')
    monkeypatch.setenv('no_proxy', '127.0.0.1')
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(('127.0.0.1', 0))
    listener.listen(8)
    listener.settimeout(0.1)
    stop = threading.Event()
    held = []

    def accept_and_hold():
        while not stop.is_set():
            with contextlib.suppress(OSError):
                connection = listener.accept()[0]
                held.append(connection)
                threading.Timer(SERVER_GIVE_UP_SECONDS, connection.close).start()

    thread = threading.Thread(target=accept_and_hold, daemon=True)
    thread.start()
    try:
        yield f'http://127.0.0.1:{listener.getsockname()[1]}/LICENSE'
    finally:
        stop.set()
        thread.join(timeout=5)
        for connection in held:
            connection.close()
        listener.close()


def test_license_fetch_from_unresponsive_server_times_out_within_bound(
    unresponsive_server_url, monkeypatch
):
    monkeypatch.setattr(License, 'REQUEST_TIMEOUT_S', TIMEOUT_SECONDS)
    command = License(SimpleNamespace(api=None, stdout=None, stderr=None))

    started = time.monotonic()
    with pytest.raises(requests.exceptions.Timeout):
        command._fetch_license_from_url(unresponsive_server_url)
    elapsed = time.monotonic() - started

    assert elapsed >= TIMEOUT_SECONDS * 0.5, 'call failed before the timeout could have expired'
    assert elapsed < MAX_ELAPSED_SECONDS, f'call was not bounded by its timeout: {elapsed:.1f}s'


def test_license_fetch_default_timeout_is_a_finite_bound():
    assert 0 < License.REQUEST_TIMEOUT_S <= 60
