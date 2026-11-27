######################################################################
#
# File: test/unit/b2_simulator.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
"""
Pytest plugin: run the simulator-backed unit tests against the standalone b2-simulator.

The unit tests that build their API through :func:`raw_api_class` (``BaseConsoleToolTest``, the
``b2_cli`` fixture and ``TestReprentFileMetadata``) use b2sdk's in-process ``RawSimulator`` by
default. With the b2-simulator backend they talk over HTTP to backblaze-labs/b2-simulator
instead: one simulator per pytest process (so one per pytest-xdist worker), reset before every
test.

Choosing the backend:

- ``pytest --b2-simulator`` turns it on for one run, ``--no-b2-simulator`` turns it off.
- The ``b2_simulator`` ini option (``[tool.pytest.ini_options]`` in ``pyproject.toml``) sets the
  default for runs without either flag. It is ``false`` today, so the default run does not change;
  setting it to ``true`` makes b2-simulator the default backend.

With the backend on, ``B2_SIMULATOR_DIR`` must name a checkout of backblaze-labs/b2-simulator and
``node`` must be on ``PATH``; otherwise the run stops with a usage error. It never skips.

``@pytest.mark.b2_simulator_config(**settings)`` adds keys of the simulator's ``POST /reset``
``config`` for one test (for example ``customUploadTimestampsEnabled=True``).

A test whose expected value is one RawSimulator makes up and B2 does not (an error text, a field
B2 fills in) branches on :func:`b2_simulator_enabled`; both branches assert an exact value.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import tempfile
import threading
import typing
import urllib.request
from dataclasses import dataclass
from pathlib import Path

import pytest

# Imported with the test modules, not on first use: some unit tests (the autocomplete cache
# unpickler) drop b2sdk from sys.modules, and a later import would give classes and enums that
# the already imported b2sdk code does not recognise.
from b2sdk.v3 import B2Http, B2RawHTTPApi

ENV_VAR = 'B2_SIMULATOR_DIR'
INI_OPTION = 'b2_simulator'
SERVER_SCRIPT = Path('bin', 'simulator', 'serve.mjs')
START_TIMEOUT_SECONDS = 30
# Profile ``b2`` of b2-simulator (its docs/CONTRACT.md, "Credentials and profile"): the account id
# authorizes in place of the master key id, as on B2 and as RawSimulator's create_account() pair.
ACCOUNT_ID = 'simacct00001'
MASTER_KEY = 'sim-master-secret'
# RawSimulator.MIN_PART_SIZE, which the part-size tests are written for.
PART_SIZE = 200
DEFAULT_CONFIG = {
    'minimumPartSize': PART_SIZE,
    'recommendedPartSize': PART_SIZE,
    # A fresh RawSimulator account lists only the keys the test made; so does this.
    'listTestKey': False,
}
# Exits node when its stdin closes, which happens when the process that started it dies in any
# way (including SIGKILL), so a simulator never outlives its pytest worker.
_EXIT_WITH_PARENT = (
    'data:text/javascript,process.stdin.on("end",()=>process.exit(0));process.stdin.resume()'
)

_server: SimulatorServer | None = None
_enabled = False


@dataclass
class SimulatorServer:
    process: subprocess.Popen
    api_url: str
    control_url: str
    stderr: typing.IO[bytes]

    @classmethod
    def start(cls, simulator_dir) -> SimulatorServer:
        script = Path(simulator_dir) / SERVER_SCRIPT
        args = ['--control', '--profile', 'b2']
        stderr = tempfile.TemporaryFile()
        process = subprocess.Popen(
            [_node(), '--import', _EXIT_WITH_PARENT, str(script), *args],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=stderr,
            text=True,
        )
        lines = _read_ready_lines(process)
        urls = dict(line.split(' ', 1) for line in lines if ' ' in line)
        api_url = next(
            (line.split()[1] for line in lines if line.startswith('SIMULATOR-LISTENING http:')),
            None,
        )
        if api_url is None or 'SIMULATOR-CONTROL' not in urls:
            process.kill()
            process.wait()
            process.stdout.close()
            stderr.seek(0)
            message = stderr.read().decode(errors='replace')
            stderr.close()
            raise RuntimeError(
                f'b2-simulator at {script} did not start (exit code {process.returncode}): '
                f'stdout {lines!r}, stderr {message!r}'
            )
        return cls(process, api_url, urls['SIMULATOR-CONTROL'], stderr)

    def reset(self, config=None):
        body = json.dumps({'config': {**DEFAULT_CONFIG, **(config or {})}}).encode()
        request = urllib.request.Request(
            self.control_url + '/reset',
            data=body,
            method='POST',
            headers={'Content-Type': 'application/json'},
        )
        with urllib.request.urlopen(request, timeout=10) as response:
            response.read()

    def stop(self):
        try:
            # The simulator exits when its stdin closes; terminate and kill are fallbacks.
            self.process.stdin.close()
            for fallback in (self.process.terminate, self.process.kill):
                try:
                    self.process.wait(timeout=5)
                    break
                except subprocess.TimeoutExpired:
                    fallback()
            self.process.wait(timeout=5)
        finally:
            self.process.stdout.close()
            self.stderr.close()


def _node():
    node = shutil.which('node')
    if node is None:
        raise pytest.UsageError('the b2-simulator backend needs Node.js 20.6 or later (`node`)')
    return node


def _read_ready_lines(process, count=3):
    """Read the simulator's ready lines; give up after START_TIMEOUT_SECONDS or at end of output."""
    lines = []

    def read():
        for _ in range(count):
            line = process.stdout.readline()
            if not line:
                return
            lines.append(line.strip())

    reader = threading.Thread(target=read, daemon=True)
    reader.start()
    reader.join(START_TIMEOUT_SECONDS)
    return list(lines)


def simulator_dir_from_env() -> Path:
    value = os.environ.get(ENV_VAR)
    if not value:
        raise pytest.UsageError(
            f'the b2-simulator backend needs {ENV_VAR}: set it to a checkout of '
            'backblaze-labs/b2-simulator'
        )
    script = Path(value) / SERVER_SCRIPT
    if not script.is_file():
        raise pytest.UsageError(f'b2-simulator backend: no server at {script} (check {ENV_VAR})')
    return Path(value)


def b2_simulator_enabled() -> bool:
    """True when this run's simulator-backed tests use b2-simulator instead of RawSimulator."""
    return _enabled


def raw_api_class(default):
    """
    Return the raw API class a test should build its ``B2Api`` with.

    ``default`` is b2sdk's ``RawSimulator``. With the b2-simulator backend it is replaced by an
    HTTP client of this process's b2-simulator.
    """
    if not _enabled:
        return default
    return _http_raw_api_class()


_HTTP_CLASS = None


def _http_raw_api_class():
    global _HTTP_CLASS
    if _HTTP_CLASS is None:

        class B2SimulatorRawApi(B2RawHTTPApi):
            """``B2RawHTTPApi`` for this worker's b2-simulator, with RawSimulator's test hook."""

            def __init__(self, b2_http=None, *args, **kwargs):
                super().__init__(b2_http or B2Http())

            def create_account(self):
                """
                Return the account id and master key, as RawSimulator does.

                RawSimulator makes a new account on every call. b2-simulator has one account, so a
                second call fails instead of silently returning the first account again.
                """
                if getattr(self, '_account_created', False):
                    raise RuntimeError(
                        'b2-simulator has one account: this test needs a second account, '
                        'which only RawSimulator provides'
                    )
                self._account_created = True
                return ACCOUNT_ID, MASTER_KEY

            def authorize_account(self, realm_url, application_key_id, application_key):
                # Every realm goes to the simulator, so a test can never reach real B2.
                if _server is None:
                    raise RuntimeError('b2-simulator is not running (use it inside a test)')
                return super().authorize_account(
                    _server.api_url, application_key_id, application_key
                )

        _HTTP_CLASS = B2SimulatorRawApi
    return _HTTP_CLASS


def pytest_addoption(parser):
    group = parser.getgroup('b2-simulator')
    group.addoption(
        '--b2-simulator',
        action='store_true',
        default=None,
        dest='b2_simulator',
        help=f'run the simulator-backed unit tests against b2-simulator (checkout in ${ENV_VAR})',
    )
    group.addoption(
        '--no-b2-simulator',
        action='store_false',
        dest='b2_simulator',
        help='run the simulator-backed unit tests against b2sdk RawSimulator',
    )
    parser.addini(
        INI_OPTION,
        type='bool',
        default=False,
        help='default backend of the simulator-backed unit tests: true for b2-simulator',
    )


def _wanted(config) -> bool:
    option = config.getoption('b2_simulator')
    return config.getini(INI_OPTION) if option is None else option


def pytest_configure(config):
    global _enabled
    config.addinivalue_line(
        'markers',
        'b2_simulator_config(**settings): b2-simulator reset settings for this test '
        '(only used with the b2-simulator backend)',
    )
    if _wanted(config):
        simulator_dir_from_env()
        _node()
        _enabled = True


def pytest_unconfigure(config):
    global _enabled
    _enabled = False


def pytest_report_header(config):
    if _wanted(config):
        return f'b2-simulator: {os.environ[ENV_VAR]}'


@pytest.fixture(scope='session')
def b2_simulator_server():
    """This process's b2-simulator, started on first use and stopped with the session."""
    global _server
    if not _enabled:
        raise pytest.UsageError('the b2_simulator_server fixture needs the b2-simulator backend')
    _server = SimulatorServer.start(simulator_dir_from_env())
    yield _server
    _server.stop()
    _server = None


@pytest.fixture(autouse=True)
def _b2_simulator_reset(request):
    """Reset the simulator before each test, with the test's ``b2_simulator_config`` settings."""
    if not _enabled:
        return
    server = request.getfixturevalue('b2_simulator_server')
    config = {}
    for marker in reversed(list(request.node.iter_markers('b2_simulator_config'))):
        config.update(marker.kwargs)
    server.reset(config)
