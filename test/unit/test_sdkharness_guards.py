######################################################################
#
# File: test/unit/test_sdkharness_guards.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
"""Tests of the safety guards in the repository-owned ``.sdkharness/`` checks.

The checks must only ever talk to a loopback simulator with its fixed credential and must
never hand an ambient ``B2_*`` value or proxy setting to the CLI. These tests keep those
guards from regressing silently, and exercise the helpers that decide what a result means.
"""

from __future__ import annotations

import importlib.util
import json
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.request import ProxyHandler, Request, build_opener

import pytest

TESTS_DIR = Path(__file__).resolve().parents[2] / '.sdkharness' / 'tests'
sys.path.insert(0, str(TESTS_DIR / 'lib'))

import observing_proxy  # noqa: E402
import simulator_guard  # noqa: E402


def load(filename: str):
    spec = importlib.util.spec_from_file_location(
        'sdkharness_' + filename.replace('-', '_').removesuffix('.py'), TESTS_DIR / filename
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


lifecycle = load('conformance-file-lifecycle.py')
files_upload = load('conformance-files-upload.py')
health = load('health-golden-path.py')
resilience = load('resilience-upload.py')

REAL_LOOKING_KEY_ID = '0' * 12 + 'REALKEYID'
REAL_LOOKING_KEY = 'K' + '1' * 30
GOOD_URL = 'http://127.0.0.1:4321'

# (module, scenario, extra environment) for each executable that validates a simulator URL
# and a credential pair before it runs the CLI.
CLI_CHECKS = [
    pytest.param(lifecycle, 'files.hide', {}, id='conformance-file-lifecycle'),
    pytest.param(files_upload, 'files.upload', {}, id='conformance-files-upload'),
    pytest.param(
        health, 'golden-path', {'HEALTHCHECK_REALM_URL': GOOD_URL}, id='health-golden-path'
    ),
]


def environment_for(module, scenario, extra, **overrides):
    environment = {
        'SDKHARNESS_TEST_LEVEL': module.LEVEL,
        'SDKHARNESS_SCENARIO': scenario,
        'SDKHARNESS_SIMULATOR_URL': GOOD_URL,
        'B2_TEST_APPLICATION_KEY_ID': 'test-key-id',
        'B2_TEST_APPLICATION_KEY': 'test-key',
        'B2_BUCKET_NAME': 'sdkharness-conformance',
        **extra,
    }
    environment.update(overrides)
    if 'SDKHARNESS_SIMULATOR_URL' in overrides and 'HEALTHCHECK_REALM_URL' in extra:
        environment['HEALTHCHECK_REALM_URL'] = overrides['SDKHARNESS_SIMULATOR_URL']
    return environment


# -- simulator_guard ---------------------------------------------------------------------


def test_only_the_fixed_credential_is_accepted():
    assert simulator_guard.credential_is_fixed('test-key-id', 'test-key')
    assert not simulator_guard.credential_is_fixed(REAL_LOOKING_KEY_ID, REAL_LOOKING_KEY)
    assert not simulator_guard.credential_is_fixed('test-key-id', REAL_LOOKING_KEY)
    assert not simulator_guard.credential_is_fixed(REAL_LOOKING_KEY_ID, 'test-key')
    assert not simulator_guard.credential_is_fixed('', '')


def test_scrubbed_environment_drops_b2_and_proxy_variables():
    environment = {
        'PATH': '/usr/bin',
        'B2_APPLICATION_KEY_ID': 'ambient-id',
        'B2_APPLICATION_KEY': 'ambient-key',
        'B2_ENVIRONMENT': 'https://example.invalid',
        'HTTP_PROXY': 'http://proxy.invalid:8080',
        'https_proxy': 'http://proxy.invalid:8080',
        'ALL_PROXY': 'socks5://proxy.invalid',
        'FTP_PROXY': 'http://proxy.invalid:21',
        'grpc_proxy': 'http://proxy.invalid:50051',
        'NO_PROXY': 'example.com',
        'no_proxy': 'example.com',
    }
    original = dict(environment)

    scrubbed = simulator_guard.scrubbed_environment(environment)

    assert environment == original, 'the caller environment must not be modified'
    assert scrubbed['PATH'] == '/usr/bin'
    assert not [name for name in scrubbed if name.startswith('B2_')]
    assert not {name for name in scrubbed if name.lower().endswith('_proxy')} - {
        'NO_PROXY',
        'no_proxy',
    }
    # loopback is exempted explicitly, whatever the ambient exemptions were
    assert scrubbed['NO_PROXY'] == scrubbed['no_proxy'] == '127.0.0.1'


# -- the loopback and credential guards of every CLI-driving executable --------------------


@pytest.mark.parametrize('module, scenario, extra', CLI_CHECKS)
def test_validate_environment_accepts_the_simulator_setup(module, scenario, extra):
    result = module.validate_environment(environment_for(module, scenario, extra))
    assert GOOD_URL in result
    assert 'test-key-id' in result and 'test-key' in result


@pytest.mark.parametrize(
    'url',
    [
        'https://127.0.0.1:4321',  # TLS: not the plain loopback listener
        'http://localhost:4321',  # a name, not the IPv4 literal
        'http://[::1]:4321',
        'http://0.0.0.0:4321',
        'http://127.0.0.2:4321',
        'http://192.168.1.10:4321',
        'https://api.backblazeb2.com',
        'http://127.0.0.1',  # no port
        'http://user:pass@127.0.0.1:4321',
        'http://127.0.0.1:4321/b2api',
        'http://127.0.0.1:4321/?next=elsewhere',
        'http://127.0.0.1:4321/#fragment',
        'http://127.0.0.1:notaport',
        '',
    ],
)
@pytest.mark.parametrize('module, scenario, extra', CLI_CHECKS)
def test_validate_environment_refuses_a_non_loopback_url(module, scenario, extra, url):
    environment = environment_for(module, scenario, extra, SDKHARNESS_SIMULATOR_URL=url)
    with pytest.raises(module.CheckFailure) as refused:
        module.validate_environment(environment)
    assert refused.value.step == 'configuration'


@pytest.mark.parametrize(
    'key_id, application_key',
    [
        (REAL_LOOKING_KEY_ID, REAL_LOOKING_KEY),
        ('test-key-id', REAL_LOOKING_KEY),
        (REAL_LOOKING_KEY_ID, 'test-key'),
    ],
)
@pytest.mark.parametrize('module, scenario, extra', CLI_CHECKS)
def test_validate_environment_refuses_a_real_credential_without_echoing_it(
    module, scenario, extra, key_id, application_key
):
    environment = environment_for(
        module,
        scenario,
        extra,
        B2_TEST_APPLICATION_KEY_ID=key_id,
        B2_TEST_APPLICATION_KEY=application_key,
    )
    with pytest.raises(module.CheckFailure) as refused:
        module.validate_environment(environment)
    assert refused.value.detail == simulator_guard.CREDENTIAL_REFUSAL
    message = str(refused.value)
    assert REAL_LOOKING_KEY not in message and REAL_LOOKING_KEY_ID not in message


@pytest.mark.parametrize('module, scenario, extra', CLI_CHECKS)
@pytest.mark.parametrize(
    'missing', ['B2_TEST_APPLICATION_KEY_ID', 'B2_TEST_APPLICATION_KEY', 'B2_BUCKET_NAME']
)
def test_validate_environment_requires_every_input(module, scenario, extra, missing):
    environment = environment_for(module, scenario, extra)
    del environment[missing]
    with pytest.raises(module.CheckFailure) as refused:
        module.validate_environment(environment)
    assert refused.value.step == 'configuration'


def test_the_health_check_refuses_a_realm_that_is_not_the_simulator_url():
    environment = environment_for(
        health, 'golden-path', {'HEALTHCHECK_REALM_URL': 'https://api.backblazeb2.com'}
    )
    with pytest.raises(health.CheckFailure, match='mismatch'):
        health.validate_environment(environment)


def test_the_lifecycle_check_refuses_an_unknown_scenario_or_level():
    with pytest.raises(lifecycle.CheckFailure):
        lifecycle.validate_environment(
            environment_for(lifecycle, 'files.hide', {}, SDKHARNESS_SCENARIO='bucket.nope')
        )
    with pytest.raises(lifecycle.CheckFailure):
        lifecycle.validate_environment(
            environment_for(lifecycle, 'files.hide', {}, SDKHARNESS_TEST_LEVEL='resilience')
        )


# -- the resilience executable ---------------------------------------------------------------


def resilience_environment(**overrides):
    environment = {
        'SDKHARNESS_TEST_LEVEL': 'resilience',
        'SDKHARNESS_SCENARIO': 'upload.retry_503',
        'SDKHARNESS_SIMULATOR_URL': GOOD_URL,
        'SDKHARNESS_SIMULATOR_CONTROL_URL': 'http://127.0.0.1:4322',
    }
    environment.update(overrides)
    return environment


def test_resilience_accepts_loopback_simulator_and_control_urls():
    assert resilience.validate_environment(resilience_environment()) == 'upload.retry_503'


@pytest.mark.parametrize('name', ['SDKHARNESS_SIMULATOR_URL', 'SDKHARNESS_SIMULATOR_CONTROL_URL'])
@pytest.mark.parametrize(
    'url',
    ['https://127.0.0.1:1', 'http://localhost:1', 'http://[::1]:1', 'http://10.0.0.1:1', '', 'x'],
)
def test_resilience_refuses_a_non_loopback_url(name, url):
    with pytest.raises(resilience.Failure) as refused:
        resilience.validate_environment(resilience_environment(**{name: url}))
    assert refused.value.step == 'configuration'


def test_resilience_refuses_an_unknown_scenario():
    with pytest.raises(resilience.Failure):
        resilience.validate_environment(resilience_environment(SDKHARNESS_SCENARIO='upload.nope'))


def test_resilience_names_the_scenario_in_the_result_line_when_configuration_fails(
    monkeypatch, capsys
):
    for name, value in resilience_environment(SDKHARNESS_SIMULATOR_URL='https://x.invalid').items():
        monkeypatch.setenv(name, value)
    assert resilience.main() == 1
    fields = capsys.readouterr().out.strip().split('\t')
    assert fields[:4] == ['SDKHARNESS_RESULT', 'resilience', 'upload.retry_503', 'FAIL']
    assert fields[4].startswith('configuration')


def test_resilience_does_not_echo_an_unknown_scenario(monkeypatch, capsys):
    for name, value in resilience_environment(SDKHARNESS_SCENARIO='bad\tname').items():
        monkeypatch.setenv(name, value)
    assert resilience.main() == 1
    fields = capsys.readouterr().out.strip().split('\t')
    assert len(fields) == 5 and fields[2] == 'unknown'


def test_resilience_follows_the_latest_stable_apiver():
    from b2._internal.version_listing import LATEST_STABLE_VERSION

    assert resilience.CLI_PREFIX[-1] == f'b2._internal.{LATEST_STABLE_VERSION}'


class FakeProcess:
    returncode = 0
    stderr = ''

    def __init__(self, record):
        self.stdout = json.dumps(record)


def test_a_small_file_must_report_its_real_sha1():
    import hashlib

    payload = b'small payload'
    real = hashlib.sha1(payload).hexdigest()
    resilience.uploaded_or_fail(FakeProcess({'contentSha1': real}), payload)
    resilience.uploaded_or_fail(FakeProcess({'contentSha1': f'unverified:{real}'}), payload)
    for reported in ('none', '', '0' * 40):
        with pytest.raises(resilience.Failure, match='contentSha1'):
            resilience.uploaded_or_fail(FakeProcess({'contentSha1': reported}), payload)


def test_none_is_accepted_only_for_a_large_file_with_a_matching_digest():
    import hashlib

    payload = b'large payload'
    digest = hashlib.sha1(payload).hexdigest()
    record = {'contentSha1': 'none', 'fileInfo': {'large_file_sha1': digest}}
    resilience.uploaded_or_fail(FakeProcess(record), payload, large_file=True)
    with pytest.raises(resilience.Failure):  # not a documented large-file case
        resilience.uploaded_or_fail(FakeProcess(record), payload)
    wrong = {'contentSha1': 'none', 'fileInfo': {'large_file_sha1': '0' * 40}}
    with pytest.raises(resilience.Failure, match='large_file_sha1'):
        resilience.uploaded_or_fail(FakeProcess(wrong), payload, large_file=True)
    with pytest.raises(resilience.Failure, match='large_file_sha1'):
        resilience.uploaded_or_fail(FakeProcess({'contentSha1': 'none'}), payload, large_file=True)


def test_resilience_child_environment_has_no_ambient_b2_or_proxy_values(monkeypatch, tmp_path):
    monkeypatch.setenv('B2_APPLICATION_KEY', REAL_LOOKING_KEY)
    monkeypatch.setenv('HTTPS_PROXY', 'http://proxy.invalid:8080')
    monkeypatch.setenv('SDKHARNESS_SIMULATOR_URL', GOOD_URL)
    cli = resilience.Cli(str(tmp_path))
    assert cli.env['B2_APPLICATION_KEY'] == 'test-key'
    assert REAL_LOOKING_KEY not in cli.env.values()
    assert 'HTTPS_PROXY' not in cli.env
    assert cli.env['NO_PROXY'] == '127.0.0.1'


def test_resilience_child_uses_customer_timeouts_even_under_github_ci(monkeypatch, tmp_path):
    monkeypatch.setenv('CI', 'true')
    monkeypatch.setenv('GITHUB_REPOSITORY', 'Backblaze/B2_Command_Line_Tool')
    monkeypatch.setenv('SDKHARNESS_SIMULATOR_URL', GOOD_URL)

    cli = resilience.Cli(str(tmp_path))

    assert 'CI' not in cli.env
    assert 'GITHUB_REPOSITORY' not in cli.env


# -- the observing proxy ---------------------------------------------------------------------


class Upstream(BaseHTTPRequestHandler):
    def log_message(self, *_args):
        pass

    def do_POST(self):
        self.rfile.read(int(self.headers.get('Content-Length') or 0))
        body = json.dumps({'self': self.server.origin}).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    do_GET = do_POST


@pytest.fixture
def upstream():
    server = ThreadingHTTPServer(('127.0.0.1', 0), Upstream)
    server.origin = f'http://127.0.0.1:{server.server_address[1]}'
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield server
    server.shutdown()
    server.server_close()


@pytest.mark.parametrize(
    'origin',
    ['https://127.0.0.1:1', 'http://localhost:1', 'http://example.com:80', 'http://127.0.0.1'],
)
def test_the_proxy_forwards_only_to_a_loopback_origin(origin):
    with pytest.raises(ValueError):
        observing_proxy.ObservingProxy(origin)


def test_classify_transfer_labels_part_uploads_and_downloads():
    classify = observing_proxy.classify_transfer
    assert classify('POST', '/b2api/v4/b2_upload_part', {}) == ('upload_part',)
    assert classify('POST', '/b2api/v4/b2_upload_file', {}) == ()
    assert classify('GET', '/file/bucket/name', {}) == ('download_stream',)
    assert classify('GET', '/file/bucket/name', {'range': 'bytes=0-9'}) == (
        'download_stream',
        'ranged_get',
    )
    assert classify('GET', '/b2api/v4/b2_download_file_by_id?fileId=x', {'Range': 'bytes=0-9'}) == (
        'download_stream',
        'ranged_get',
    )
    assert classify('POST', '/b2api/v4/b2_list_buckets', {}) == ()


NO_PROXY_OPENER = build_opener(ProxyHandler({}))


def fetch_json(url):
    with NO_PROXY_OPENER.open(Request(url, data=b''), timeout=10) as response:
        return json.load(response)


def fire(url, count):
    """POST ``count`` requests that leave together, so that thread start-up skew on a busy
    machine cannot spread them wider than the proxy's hold time."""
    start_together = threading.Barrier(count)

    def post():
        start_together.wait(10)
        with NO_PROXY_OPENER.open(Request(url, data=b'x'), timeout=10) as response:
            response.read()

    threads = [threading.Thread(target=post) for _ in range(count)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()


def test_the_proxy_observes_overlapping_requests_and_rewrites_the_origin(upstream):
    with observing_proxy.ObservingProxy(upstream.origin, hold_seconds=0.3) as proxy:
        fire(f'{proxy.origin}/b2api/v4/b2_upload_part', 3)
        assert proxy.peak('upload_part') == 3
        assert proxy.total('upload_part') == 3
        reply = fetch_json(f'{proxy.origin}/b2api/v4/b2_list_buckets')
        assert reply == {'self': proxy.origin}, 'JSON URLs must lead back through the proxy'


def test_the_proxy_checks_ignore_ambient_proxy_variables(upstream, monkeypatch):
    """An ambient HTTP(S)_PROXY must not capture the loopback requests of these tests."""
    for name in (
        'HTTP_PROXY',
        'HTTPS_PROXY',
        'ALL_PROXY',
        'http_proxy',
        'https_proxy',
        'all_proxy',
    ):
        monkeypatch.setenv(name, 'http://127.0.0.1:1')  # nothing listens here
    for name in ('NO_PROXY', 'no_proxy'):
        monkeypatch.delenv(name, raising=False)
    with observing_proxy.ObservingProxy(upstream.origin, hold_seconds=0.3) as proxy:
        fire(f'{proxy.origin}/b2api/v4/b2_upload_part', 3)
        assert proxy.peak('upload_part') == 3
        reply = fetch_json(f'{proxy.origin}/b2api/v4/b2_list_buckets')
        assert reply == {'self': proxy.origin}


def test_the_proxy_sees_no_overlap_for_serial_requests(upstream):
    with observing_proxy.ObservingProxy(upstream.origin, hold_seconds=0.05) as proxy:
        for _ in range(3):
            fire(f'{proxy.origin}/b2api/v4/b2_upload_part', 1)
        assert proxy.peak('upload_part') == 1
        assert proxy.total('upload_part') == 3
        proxy.reset()
        assert proxy.peak('upload_part') == 0


@pytest.mark.parametrize('content_type', ['application/json', 'application/octet-stream'])
def test_completed_upstream_cleanup_is_not_counted_as_transfer_overlap(
    upstream, monkeypatch, content_type
):
    """A finished upstream response must not overlap the next serial transfer."""
    cleanup_started = threading.Event()
    release_cleanup = threading.Event()
    connection_class = observing_proxy.http.client.HTTPConnection
    send_header = Upstream.send_header

    def upstream_header(handler, name, value):
        if name.lower() == 'content-type':
            value = content_type
        return send_header(handler, name, value)

    class DelayedCleanupConnection(connection_class):
        consumed = False

        def getresponse(self):
            response = super().getresponse()
            read = response.read

            def observed_read(amount=None):
                data = read(amount)
                if amount is None or not data:
                    self.consumed = True
                return data

            response.read = observed_read
            return response

        def close(self):
            if (
                self.port == upstream.server_address[1]
                and self.consumed
                and not cleanup_started.is_set()
            ):
                cleanup_started.set()
                assert release_cleanup.wait(5), 'test did not release upstream cleanup'
            return super().close()

    monkeypatch.setattr(Upstream, 'send_header', upstream_header)
    monkeypatch.setattr(observing_proxy.http.client, 'HTTPConnection', DelayedCleanupConnection)
    try:
        with observing_proxy.ObservingProxy(upstream.origin, hold_seconds=0.3) as proxy:
            fire(f'{proxy.origin}/b2api/v4/b2_upload_part', 1)
            assert cleanup_started.wait(5), 'first upstream connection did not enter cleanup'
            fire(f'{proxy.origin}/b2api/v4/b2_upload_part', 1)
            assert proxy.peak('upload_part') == 1
            assert proxy.total('upload_part') == 2
            release_cleanup.set()
            proxy.reset()
            fire(f'{proxy.origin}/b2api/v4/b2_upload_part', 3)
            assert proxy.peak('upload_part') == 3
            assert proxy.total('upload_part') == 3
    finally:
        release_cleanup.set()


def raw_exchange(port, request: bytes) -> bytes:
    """Send raw bytes to a loopback port and return everything until the server closes."""
    import socket

    with socket.create_connection(('127.0.0.1', port), timeout=10) as connection:
        connection.sendall(request)
        chunks = []
        while chunk := connection.recv(65536):
            chunks.append(chunk)
    return b''.join(chunks)


@pytest.mark.parametrize(
    'request_line, extra_headers',
    [
        ('GET /a"b HTTP/1.1', ''),  # a character outside the safe set
        ('GET relative HTTP/1.1', ''),  # not an origin-form target
        ('POST /b2api/v4/b2_upload_part HTTP/1.1', 'Content-Length: abc\r\n'),
        ('POST /b2api/v4/b2_upload_part HTTP/1.1', 'Content-Length: -5\r\n'),
    ],
)
def test_the_proxy_refuses_a_bad_request_without_forwarding_it(
    upstream, request_line, extra_headers
):
    classified = []

    def record(method, path, headers):
        classified.append(path)
        return ()

    with observing_proxy.ObservingProxy(upstream.origin, classify=record) as proxy:
        port = int(proxy.origin.rsplit(':', 1)[1])
        reply = raw_exchange(
            port, f'{request_line}\r\nHost: x\r\n{extra_headers}Connection: close\r\n\r\n'.encode()
        )

    assert reply.startswith(b'HTTP/1.1 400 '), reply[:80]
    assert classified == [], 'a refused request must never be classified or forwarded'


def test_a_truncated_upstream_body_drops_the_client_connection_without_a_traceback(
    capfd, monkeypatch
):
    """http.client.IncompleteRead is an HTTPException, not an OSError: it must still be handled."""
    import socket

    listener = socket.socket()
    listener.bind(('127.0.0.1', 0))
    listener.listen(1)

    def serve_a_short_body():
        connection, _ = listener.accept()
        with connection:
            connection.recv(65536)
            connection.sendall(
                b'HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\n'
                b'Transfer-Encoding: chunked\r\n\r\n3e8\r\n' + b'x' * 10
            )

    thread = threading.Thread(target=serve_a_short_body, daemon=True)
    thread.start()
    origin = f'http://127.0.0.1:{listener.getsockname()[1]}'
    try:
        with observing_proxy.ObservingProxy(origin) as proxy:
            port = int(proxy.origin.rsplit(':', 1)[1])
            reply = raw_exchange(
                port, b'GET /file/bucket/name HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n'
            )
            assert proxy.peak('download_stream') == 1
            assert proxy._in_flight['download_stream'] == 0, 'the observation must be finished'
    finally:
        listener.close()
        thread.join(5)

    assert reply.startswith(b'HTTP/1.1 200 ')
    assert len(reply.split(b'\r\n\r\n', 1)[1]) < 1000, 'the body must not be completed'
    assert 'Traceback' not in capfd.readouterr().err
