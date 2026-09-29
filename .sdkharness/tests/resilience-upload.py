#!/usr/bin/env python3
"""Repository-owned B2 CLI upload resilience contracts.

Each scenario runs the CLI implementation from this checkout against the
harness-provided loopback simulator and its private fault-control listener.
"""

import http.client
import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import uuid
from collections.abc import Mapping

LEVEL = 'resilience'
SCENARIOS = {
    'upload.retry_503': ('b2_upload_file', 503, 'service_unavailable'),
    'upload.retry_500': ('b2_upload_file', 500, 'internal_error'),
    'upload.retry_408': ('b2_upload_file', 408, 'request_timeout'),
    'upload.expired_token_401': ('b2_upload_file', 401, 'expired_auth_token'),
    'upload.get_url_503': ('b2_get_upload_url', 503, 'service_unavailable'),
    'upload.cap_exceeded_403': ('b2_upload_file', 403, 'cap_exceeded'),
}
SCENARIO = ''
OBJECT_NAME = ''
FAULT: dict[str, object] = {}
CLI_PREFIX = [sys.executable, '-m', 'b2._internal.b2v5']
ARM_PATH = '/faults'


class Failure(Exception):
    def __init__(self, step: str, detail: str) -> None:
        self.step = step
        self.detail = detail


def result(outcome: str, detail: str = '-') -> None:
    print(f'SDKHARNESS_RESULT\t{LEVEL}\t{SCENARIO}\t{outcome}\t{detail}', flush=True)


def note(text: str) -> None:
    print(f'NOTE B2_Command_Line_Tool {SCENARIO}: {text}', flush=True)


def loopback_port(environment: Mapping[str, str], name: str) -> int:
    from urllib.parse import urlsplit

    parsed = urlsplit(environment.get(name, ''))
    if (
        parsed.scheme != 'http'
        or parsed.hostname != '127.0.0.1'
        or parsed.port is None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in ('', '/')
        or parsed.query
        or parsed.fragment
    ):
        raise Failure('configuration', f'{name} must be a bare IPv4 loopback HTTP origin')
    return parsed.port


def control(method: str, path: str, body=None):
    data = None if body is None else json.dumps(body).encode()
    connection = http.client.HTTPConnection(
        '127.0.0.1', loopback_port(os.environ, 'SDKHARNESS_SIMULATOR_CONTROL_URL'), timeout=10
    )
    try:
        connection.request(method, path, body=data, headers={'content-type': 'application/json'})
        response = connection.getresponse()
        if response.status < 200 or response.status >= 300:
            raise Failure('control request', f'{method} {path} returned HTTP {response.status}')
        return json.load(response)
    finally:
        connection.close()


def journal():
    try:
        return control('GET', '/journal')['entries']
    except Exception as error:  # noqa: BLE001 -- only the class name is reported
        raise Failure('journal', type(error).__name__) from error


def arm():
    try:
        control('POST', ARM_PATH, FAULT)
    except Exception as error:  # noqa: BLE001
        raise Failure('arm fault', type(error).__name__) from error


def after(entries, seq, endpoint, status=None):
    """Entries on `endpoint` later than `seq`, optionally with one status."""
    return [
        e
        for e in entries
        if e['seq'] > seq
        and e['endpoint'] == endpoint
        and (status is None or e['status'] == status)
    ]


def tries(endpoint):
    return len([e for e in journal() if e['endpoint'] == endpoint])


def is_faulted(entry) -> bool:
    endpoint, status, _code = SCENARIOS[SCENARIO]
    return (
        entry['endpoint'] == endpoint and entry['fault'] == 'injected' and entry['status'] == status
    )


class Cli:
    """One CLI user: the installed `b2`, with its account-info file and config
    directory inside the check's own temporary directory, pointed at the simulator.

    REALM. The CLI documents no realm option; it honours the undocumented
    B2_ENVIRONMENT, and an unrecognised value is used verbatim as the realm URL
    (docs/sdks/B2_Command_Line_Tool/card.md, "Auth realm selector"), exactly as
    bin/conformance/B2_Command_Line_Tool/files.upload reaches @simulator.
    """

    def __init__(self, scratch: str) -> None:
        self.scratch = scratch
        self.env = {k: v for k, v in os.environ.items() if not k.startswith('B2_')}
        self.env.update(
            B2_APPLICATION_KEY_ID='test-key-id',
            B2_APPLICATION_KEY='test-key',
            B2_ENVIRONMENT=f"http://127.0.0.1:{loopback_port(os.environ, 'SDKHARNESS_SIMULATOR_URL')}",
            B2_ACCOUNT_INFO=os.path.join(scratch, 'account-info'),
            XDG_CONFIG_HOME=os.path.join(scratch, 'xdg'),
        )
        os.makedirs(self.env['XDG_CONFIG_HOME'])

    def run(self, *args):
        return subprocess.run(
            [*CLI_PREFIX, *args],
            env=self.env,
            capture_output=True,
            text=True,
            stdin=subprocess.DEVNULL,
            timeout=300,
        )

    def must(self, step_name, *args):
        try:
            proc = self.run(*args)
        except Exception as error:  # noqa: BLE001 -- only the class name is reported
            raise Failure(step_name, type(error).__name__) from error
        if proc.returncode != 0:
            error_note(proc)
            raise Failure(step_name, f'b2 exited {proc.returncode}')
        return proc

    def path(self, name):
        return os.path.join(self.scratch, name)


def error_note(proc) -> None:
    """The CLI's own ERROR line, for diagnosis. The simulator's credential is its
    fixed fake and its tokens are synthetic, so nothing real can appear here."""
    lines = [ln.strip() for ln in proc.stderr.splitlines() if ln.strip().startswith('ERROR')]
    if lines:
        note(f'b2 stderr: {lines[-1][:240]}')


def json_of(text: str):
    """The JSON document in `b2` stdout, after any `URL by ...` lines."""
    starts = [i for i in (text.find('{'), text.find('[')) if i >= 0]
    if not starts:
        raise ValueError('no JSON in the output')
    return json.loads(text[min(starts) :])


def setup():
    """An authorized CLI and a fresh bucket. There is no unreachable/unauthorized
    amber: the runner supplies the server and the credential, so either failing
    is a harness defect."""
    scratch = tempfile.mkdtemp(prefix='sdkharness-res-cli.')
    CLEANUP.append(scratch)
    cli = Cli(scratch)
    cli.must('authenticate', 'account', 'authorize')
    bucket = f'sdkharness-res-{uuid.uuid4().hex[:12]}'
    cli.must('create bucket', 'bucket', 'create', bucket, 'allPrivate')
    note(
        'realm selected with the undocumented B2_ENVIRONMENT; b2 --help-all documents no realm option'
    )
    return cli, bucket


def put(cli, bucket, name, payload, *flags):
    """Write `payload` to a local file and run `b2 file upload` on it; the
    process is returned, not judged."""
    local = cli.path('payload.' + uuid.uuid4().hex[:6])
    with open(local, 'wb') as handle:
        handle.write(payload)
    try:
        return cli.run('file', 'upload', '--no-progress', *flags, bucket, local, name)
    except Exception as error:  # noqa: BLE001
        raise Failure('upload', type(error).__name__) from error


def uploaded_or_fail(proc, endpoint='b2_upload_file'):
    """An upload process that must have recovered and returned a file record.

    The caller proves byte integrity by downloading the resulting object. That
    is stronger than trusting the response's B2 protocol checksum and avoids
    treating SHA-1 as a security primitive in this test.
    """
    if proc.returncode != 0:
        error_note(proc)
        raise Failure(
            'upload',
            f'b2 exited {proc.returncode} after {tries(endpoint)} {endpoint} '
            'attempt(s); the upload was not recovered',
        )
    try:
        meta = json_of(proc.stdout)
    except Exception as error:  # noqa: BLE001
        raise Failure('upload', 'b2 file upload printed no JSON file record') from error
    return meta


def round_trip(cli, source, payload):
    """`source` is b2://bucket/name or b2id://fileId; the bytes must match."""
    out = cli.path('roundtrip.' + uuid.uuid4().hex[:6])
    cli.must('download', 'file', 'download', '--no-progress', source, out)
    with open(out, 'rb') as handle:
        if handle.read() != payload:
            raise Failure('round trip', 'downloaded bytes differ from what was uploaded')


CLEANUP = []


def validate_environment(environment: Mapping[str, str]) -> str:
    if environment.get('SDKHARNESS_TEST_LEVEL') != LEVEL:
        raise Failure('configuration', 'unexpected test level')
    scenario = environment.get('SDKHARNESS_SCENARIO', '')
    if scenario not in SCENARIOS:
        raise Failure('configuration', 'unexpected scenario')
    for name in ('SDKHARNESS_SIMULATOR_URL', 'SDKHARNESS_SIMULATOR_CONTROL_URL'):
        loopback_port(environment, name)
    return scenario


def run_retry() -> None:
    cli, bucket = setup()
    payload = (b'sdkharness resilience ' * 64)[:1024]
    arm()
    proc = put(cli, bucket, OBJECT_NAME, payload)

    endpoint, status, _code = SCENARIOS[SCENARIO]
    if SCENARIO == 'upload.cap_exceeded_403':
        if proc.returncode == 0:
            raise Failure('upload', 'b2 exited 0; cap_exceeded never reached the caller')
        error_note(proc)
        entries = journal()
        uploads = [entry for entry in entries if entry['endpoint'] == endpoint]
        if len([entry for entry in uploads if is_faulted(entry)]) != 1:
            raise Failure('journal', 'the injected 403 did not fire exactly once')
        if len(uploads) != 1:
            raise Failure(
                'recovery path', f'{len(uploads)} upload calls; cap_exceeded must not be retried'
            )
        return

    uploaded_or_fail(proc, endpoint)
    round_trip(cli, f'b2://{bucket}/{OBJECT_NAME}', payload)
    entries = journal()
    faulted_entries = [entry for entry in entries if is_faulted(entry)]
    if len(faulted_entries) != 1:
        raise Failure(
            'journal',
            f'{len(faulted_entries)} injected {status} responses on {endpoint}, expected 1',
        )

    recovered = after(entries, faulted_entries[0]['seq'], endpoint, 200)
    if not recovered:
        raise Failure('recovery path', f'no successful {endpoint} after the {status}')
    if endpoint == 'b2_get_upload_url':
        used = after(entries, recovered[0]['seq'], 'b2_upload_file', 200)
        if not used or used[0]['uploadUrlId'] != recovered[0]['uploadUrlId']:
            raise Failure('recovery path', 'upload did not use the retried URL')
        return

    if recovered[0]['uploadUrlId'] in (None, faulted_entries[0]['uploadUrlId']):
        raise Failure('recovery path', 'retry reused the failed upload URL')
    if SCENARIO == 'upload.expired_token_401' and not after(
        entries, faulted_entries[0]['seq'], 'b2_get_upload_url', 200
    ):
        raise Failure('recovery path', 'no b2_get_upload_url after the 401')


def main() -> int:
    global SCENARIO, OBJECT_NAME, FAULT
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(143))
    try:
        SCENARIO = validate_environment(os.environ)
        endpoint, status, code = SCENARIOS[SCENARIO]
        OBJECT_NAME = f"res/{SCENARIO.replace('.', '-')}.bin"
        FAULT = {'on': endpoint, 'status': status, 'code': code, 'count': 1}
        run_retry()
    except Failure as failure:
        result('FAIL', f'{failure.step}: {failure.detail}')
        return 1
    except Exception as error:  # noqa: BLE001
        result('FAIL', f'setup: {type(error).__name__}')
        return 1
    finally:
        for path in CLEANUP:
            shutil.rmtree(path, ignore_errors=True)
    result('PASS')
    return 0


if __name__ == '__main__':
    sys.exit(main())
