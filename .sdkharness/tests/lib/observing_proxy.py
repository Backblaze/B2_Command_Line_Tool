######################################################################
#
# File: .sdkharness/tests/lib/observing_proxy.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
"""A loopback forwarding proxy that observes how many requests overlap.

The B2 CLI is a separate process, so a check cannot see its thread pool. What it
can see is the wire: the CLI is pointed at this proxy as its realm, the proxy
forwards everything to the simulator, and it counts requests that are in flight at
the same moment. Two ``b2_upload_part`` requests (or two ranged downloads) that are
open at once are real concurrency; a client that serialises its transfers can never
produce that, however many threads it was told to use.

Overlap is only reliably observable if each request lasts long enough, and the
simulator answers in a few milliseconds, so the proxy can hold selected requests
for a fixed time before forwarding them (a client-side throttle).

The proxy only ever forwards to the one loopback origin it was given, rewrites that
origin to its own in JSON responses (so the URLs the simulator returns keep leading
back through the proxy), and never logs a header or body.
"""

from __future__ import annotations

import http.client
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit

# Returns the labels a request is counted under (none: not observed).
Classifier = Callable[[str, str, Mapping[str, str]], Sequence[str]]

_HOP_BY_HOP = {
    'connection',
    'keep-alive',
    'proxy-connection',
    'te',
    'trailer',
    'transfer-encoding',
    'upgrade',
    'host',
    'content-length',
}


def classify_transfer(method: str, path: str, headers: Mapping[str, str]) -> Sequence[str]:
    """The labels a transfer request is counted under.

    ``upload_part`` for a part upload; ``download_stream`` for a file-body GET (by name or
    by id); ``ranged_get`` in addition when that GET carries a Range header.
    """
    pathname = urlsplit(path).path
    if method == 'POST' and pathname.endswith('/b2_upload_part'):
        return ('upload_part',)
    if method == 'GET' and (
        pathname.startswith('/file/') or pathname.endswith('/b2_download_file_by_id')
    ):
        ranged = any(name.lower() == 'range' for name in headers)
        return ('download_stream', 'ranged_get') if ranged else ('download_stream',)
    return ()


class ObservingProxy:
    def __init__(
        self,
        upstream_origin: str,
        *,
        classify: Classifier = classify_transfer,
        hold_seconds: float = 0.0,
    ) -> None:
        parsed = urlsplit(upstream_origin)
        if parsed.scheme != 'http' or parsed.hostname != '127.0.0.1' or parsed.port is None:
            raise ValueError('the proxy only forwards to a loopback HTTP origin')
        self.upstream_host = parsed.hostname
        self.upstream_port = parsed.port
        self.upstream_origin = f'http://{parsed.hostname}:{parsed.port}'
        self.classify = classify
        self.hold_seconds = hold_seconds
        self._lock = threading.Lock()
        self._in_flight: dict[str, int] = {}
        self._peak: dict[str, int] = {}
        self._total: dict[str, int] = {}
        self._server: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None

    # -- lifecycle ----------------------------------------------------------

    @property
    def origin(self) -> str:
        assert self._server is not None, 'the proxy is not running'
        return f'http://127.0.0.1:{self._server.server_address[1]}'

    def __enter__(self) -> ObservingProxy:
        proxy = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = 'HTTP/1.1'

            def log_message(self, *_args) -> None:  # never log request lines
                pass

            def _forward(self) -> None:
                proxy._forward(self)

            do_GET = do_POST = do_PUT = do_DELETE = do_HEAD = _forward

        self._server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self._server.daemon_threads = True
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *_exc) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
        if self._thread is not None:
            self._thread.join(timeout=5)

    # -- observation --------------------------------------------------------

    def peak(self, label: str) -> int:
        with self._lock:
            return self._peak.get(label, 0)

    def total(self, label: str) -> int:
        with self._lock:
            return self._total.get(label, 0)

    def reset(self) -> None:
        with self._lock:
            self._peak.clear()
            self._total.clear()

    def _enter(self, label: str) -> None:
        with self._lock:
            current = self._in_flight.get(label, 0) + 1
            self._in_flight[label] = current
            self._peak[label] = max(self._peak.get(label, 0), current)
            self._total[label] = self._total.get(label, 0) + 1

    def _leave(self, label: str) -> None:
        with self._lock:
            self._in_flight[label] -= 1

    # -- forwarding ---------------------------------------------------------

    def _forward(self, handler: BaseHTTPRequestHandler) -> None:
        headers = {name: value for name, value in handler.headers.items()}
        length = int(headers.get('Content-Length') or 0)
        body = handler.rfile.read(length) if length else None
        labels = tuple(self.classify(handler.command, handler.path, headers))
        for label in labels:
            self._enter(label)
        try:
            if labels and self.hold_seconds:
                time.sleep(self.hold_seconds)
            try:
                status, response_headers, payload = self._upstream(
                    handler.command, handler.path, headers, body
                )
            except OSError:
                handler.send_error(502)
                return
            handler.send_response(status)
            for name, value in response_headers:
                # send_response() already wrote Date and Server; a second Date is refused
                if name.lower() not in _HOP_BY_HOP | {'date', 'server'}:
                    handler.send_header(name, value)
            handler.send_header('Content-Length', str(len(payload)))
            handler.send_header('Connection', 'close')
            handler.end_headers()
            if handler.command != 'HEAD':
                # A request stays in flight until its response has been delivered (or the
                # client hung up, as a parallel download does once its share is read).
                try:
                    handler.wfile.write(payload)
                except OSError:
                    pass
            handler.close_connection = True
        finally:
            for label in labels:
                self._leave(label)

    def _upstream(self, method, path, headers, body):
        outgoing = {k: v for k, v in headers.items() if k.lower() not in _HOP_BY_HOP}
        connection = http.client.HTTPConnection(self.upstream_host, self.upstream_port, timeout=120)
        try:
            connection.request(method, path, body=body, headers=outgoing)
            response = connection.getresponse()
            payload = response.read() if method != 'HEAD' else b''
            response_headers = response.getheaders()
            if method == 'HEAD':
                # keep the upstream's own Content-Length: there is no body to measure
                payload = b''
            elif any(
                name.lower() == 'content-type' and 'json' in value.lower()
                for name, value in response_headers
            ):
                payload = payload.replace(self.upstream_origin.encode(), self.origin.encode())
            return response.status, response_headers, payload
        finally:
            connection.close()
