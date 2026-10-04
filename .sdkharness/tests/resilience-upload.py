#!/usr/bin/env python3
"""Repository-owned B2 CLI upload resilience contracts.

Each scenario runs the CLI implementation from this checkout against the
harness-provided loopback simulator and its private fault-control listener.
"""

import hashlib
import http.client
import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import uuid
from collections.abc import Mapping
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]

# Resolve the CLI implementation from this checkout, as the other executables do, and
# follow the latest stable apiver rather than a hard-coded one.
sys.path.insert(0, str(REPOSITORY_ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent / 'lib'))
from simulator_guard import scrubbed_environment  # noqa: E402

import b2  # noqa: E402
from b2._internal.version_listing import LATEST_STABLE_VERSION  # noqa: E402

LEVEL = 'resilience'
SCENARIOS = {
    'upload.retry_503': ('b2_upload_file', 503, 'service_unavailable'),
    'upload.retry_500': ('b2_upload_file', 500, 'internal_error'),
    'upload.retry_408': ('b2_upload_file', 408, 'request_timeout'),
    'upload.expired_token_401': ('b2_upload_file', 401, 'expired_auth_token'),
    'upload.get_url_503': ('b2_get_upload_url', 503, 'service_unavailable'),
    'upload.cap_exceeded_403': ('b2_upload_file', 403, 'cap_exceeded'),
}
WIRE_SCENARIOS = {
    'upload.reset_before_response': 'reset-before-response',
    'upload.reset_mid_request': 'reset-mid-request',
    'upload.stall': 'stall',
}
API_SCENARIOS = {
    'auth.expired_401': {
        'arm': '/faults',
        'fault': {
            'on': 'b2_list_file_names',
            'status': 401,
            'code': 'expired_auth_token',
            'count': 1,
        },
    },
    'auth.clock_expiry': {
        'arm': '/clock',
        'fault': {'advanceMs': 86400001},
    },
    'api.retry_after_429': {
        'arm': '/faults',
        'fault': {
            'on': 'b2_list_file_names',
            'status': 429,
            'code': 'too_many_requests',
            'count': 1,
            'retryAfter': 2,
        },
        'floor': 2.0,
    },
    'api.retry_after_503': {
        'arm': '/faults',
        'fault': {
            'on': 'b2_list_file_names',
            'status': 503,
            'code': 'service_unavailable',
            'count': 1,
            'retryAfter': 2,
        },
        'floor': 2.0,
    },
    'api.backoff_503': {
        'arm': '/faults',
        'fault': {
            'on': 'b2_list_file_names',
            'status': 503,
            'code': 'service_unavailable',
            'count': 1,
        },
        'floor': 1.0,
    },
}
TRANSFER_SCENARIOS = {
    'download.retry_503': ('b2_download_file_by_id', 503, 'service_unavailable'),
    'part.retry_503': ('b2_upload_part', 503, 'service_unavailable'),
}
ALL_SCENARIOS = {*SCENARIOS, *WIRE_SCENARIOS, *API_SCENARIOS, *TRANSFER_SCENARIOS}
SCENARIO = ''
OBJECT_NAME = ''
FAULT: dict[str, object] = {}
CLI_PREFIX = [sys.executable, '-m', f'b2._internal.{LATEST_STABLE_VERSION}']
ARM_PATH = '/faults'


class Failure(Exception):
    def __init__(self, step: str, detail: str) -> None:
        self.step = step
        self.detail = detail


class Skip(Exception):
    def __init__(self, reason: str, detail: str) -> None:
        self.reason = reason
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
        # No ambient B2_* value or proxy variable reaches the CLI (see simulator_guard).
        self.env = scrubbed_environment(os.environ)
        self.env.update(
            PYTHONPATH=os.pathsep.join(
                filter(None, (str(REPOSITORY_ROOT), os.environ.get('PYTHONPATH', '')))
            ),
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
            cwd=REPOSITORY_ROOT,
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


def authorize(cli, attempts: int = 3) -> None:
    """Authorize the CLI, retrying the setup step only.

    Authorizing is idempotent and is not what any scenario tests, so a transient failure of
    the very first CLI start (a cold interpreter and import cache, or a listener that has
    printed its URL a moment before it accepts) must not be reported as an SDK failure.
    A retry is announced, and a persistent failure still fails with the CLI's own error.
    """
    for attempt in range(1, attempts + 1):
        try:
            cli.must('authenticate', 'account', 'authorize')
            return
        except Failure:
            if attempt == attempts:
                raise
            note(f'authorize attempt {attempt} failed; retrying (setup step, not under test)')
            time.sleep(1.0)


def setup():
    """An authorized CLI and a fresh bucket. There is no unreachable/unauthorized
    amber: the runner supplies the server and the credential, so either failing
    is a harness defect."""
    scratch = tempfile.mkdtemp(prefix='sdkharness-res-cli.')
    CLEANUP.append(scratch)
    cli = Cli(scratch)
    authorize(cli)
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


def uploaded_or_fail(proc, payload, endpoint='b2_upload_file', large_file=False):
    """An upload process that must have recovered and returned the right file record.

    It checks the exit status, that a JSON file record came back, and that the
    record's protocol checksum (`contentSha1`) matches the bytes that were sent.
    The caller still proves byte integrity by downloading the object; both are
    kept because a CLI that stores the right bytes but reports a wrong checksum
    in its response must still fail. (SHA-1 is B2's wire checksum here, not a
    security primitive.)

    A large (multipart) file legitimately reports ``contentSha1: none`` -- B2 has no single
    checksum for it and carries ``large_file_sha1`` in ``fileInfo`` instead -- so ``none`` is
    accepted only when the caller says the upload is a large file, and then the digest in
    ``fileInfo`` must match. Every small-file scenario requires the real checksum.
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
    got = (meta.get('contentSha1') or '').split(':')[-1]
    expected = hashlib.sha1(payload).hexdigest()
    if large_file and got == 'none':
        info = meta.get('fileInfo') or {}
        if not isinstance(info, dict) or info.get('large_file_sha1') != expected:
            raise Failure('upload', 'the large file record has no matching large_file_sha1')
    elif got != expected:
        raise Failure('upload', 'the returned contentSha1 does not match the bytes sent')
    return meta


def round_trip(cli, source, payload):
    """`source` is b2://bucket/name or b2id://fileId; the bytes must match."""
    out = cli.path('roundtrip.' + uuid.uuid4().hex[:6])
    cli.must('download', 'file', 'download', '--no-progress', source, out)
    with open(out, 'rb') as handle:
        if handle.read() != payload:
            raise Failure('round trip', 'downloaded bytes differ from what was uploaded')


def listed(cli, bucket):
    proc = cli.must('list', 'ls', '--recursive', f'b2://{bucket}')
    return [line for line in proc.stdout.splitlines() if line.strip()]


CLEANUP = []


def check_checkout() -> None:
    """The CLI under test must be this checkout, not an installed distribution."""
    try:
        Path(b2.__file__).resolve().relative_to(REPOSITORY_ROOT)
    except ValueError as error:
        raise Failure('setup', 'b2 CLI was not imported from this checkout') from error


def validate_environment(environment: Mapping[str, str]) -> str:
    if environment.get('SDKHARNESS_TEST_LEVEL') != LEVEL:
        raise Failure('configuration', 'unexpected test level')
    scenario = environment.get('SDKHARNESS_SCENARIO', '')
    if scenario not in ALL_SCENARIOS:
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

    uploaded_or_fail(proc, payload, endpoint)
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


def run_wire_fault() -> None:
    kind = WIRE_SCENARIOS[SCENARIO]
    if kind == 'stall':
        raise Skip(
            'no-client-option',
            'the CLI exposes no supported per-request timeout shorter than the controlled stall',
        )

    cli, bucket = setup()
    payload = (b'sdkharness resilience wire fault ' * 64)[:1024]
    arm()
    uploaded_or_fail(put(cli, bucket, OBJECT_NAME, payload), payload)
    round_trip(cli, f'b2://{bucket}/{OBJECT_NAME}', payload)

    entries = journal()
    uploads = [entry for entry in entries if entry['endpoint'] == 'b2_upload_file']
    faulted = [entry for entry in uploads if entry['fault'] == kind]
    if len(faulted) != 1:
        raise Failure('journal', f'{len(faulted)} {kind} faults on b2_upload_file, expected 1')
    recovered = after(entries, faulted[0]['seq'], 'b2_upload_file', 200)
    if not recovered:
        raise Failure('journal', f'no successful b2_upload_file after the {kind}')
    if recovered[0]['uploadUrlId'] in (None, faulted[0]['uploadUrlId']):
        raise Failure('recovery path', f'the retry reused the upload URL that got the {kind}')

    if kind != 'reset-before-response':
        return

    # The server may have committed the first attempt before its response was
    # reset. One or two versions are valid, but every committed version must
    # contain the complete payload.
    proc = cli.must('list versions', 'ls', '--json', '--versions', '--recursive', f'b2://{bucket}')
    mine = [item for item in json_of(proc.stdout) if item.get('fileName') == OBJECT_NAME]
    if not 1 <= len(mine) <= 2:
        raise Failure('end state', f'{len(mine)} versions of the object, expected 1 or 2')
    for version in mine:
        file_id = version.get('fileId')
        if not file_id:
            raise Failure('end state', 'a stored version has no fileId')
        stored_sha1 = (version.get('contentSha1') or '').split(':')[-1]
        if stored_sha1 != hashlib.sha1(payload).hexdigest():
            raise Failure('end state', 'a stored version does not carry the uploaded bytes')
        round_trip(cli, f'b2id://{file_id}', payload)
    note(f'{len(mine)} complete version(s) after the reset-before-response')


def run_api_fault() -> None:
    config = API_SCENARIOS[SCENARIO]
    cli, bucket = setup()
    payload = b'sdkharness resilience listing'
    uploaded_or_fail(put(cli, bucket, OBJECT_NAME, payload), payload)
    round_trip(cli, f'b2://{bucket}/{OBJECT_NAME}', payload)

    baseline = None
    before = None
    floor = config.get('floor')
    if floor is not None:
        started = time.monotonic()
        listed(cli, bucket)
        baseline = time.monotonic() - started
    if SCENARIO == 'auth.clock_expiry':
        before = max(entry['seq'] for entry in journal())

    arm()
    started = time.monotonic()
    names = listed(cli, bucket)
    elapsed = time.monotonic() - started
    if names != [OBJECT_NAME]:
        raise Failure('listing', f'listed {len(names)} names, expected exactly the fixture')

    entries = journal()
    if before is not None:
        entries = [entry for entry in entries if entry['seq'] > before]
        if any(entry['fault'] is not None for entry in entries):
            raise Failure(
                'journal', 'a fault was injected; clock expiry must see only real answers'
            )
        expired = [entry for entry in entries if entry['status'] == 401 and entry['fault'] is None]
        if not expired:
            raise Failure('journal', 'the advanced clock produced no real expired-token 401')
        faulted = expired[0]
        note(f"the real 401 came from {faulted['endpoint']}")
    else:
        status = config['fault']['status']
        faulted_entries = [
            entry
            for entry in entries
            if entry['endpoint'] == 'b2_list_file_names'
            and entry['fault'] == 'injected'
            and entry['status'] == status
        ]
        if len(faulted_entries) != 1:
            raise Failure(
                'journal',
                f'{len(faulted_entries)} injected {status} responses on b2_list_file_names, expected 1',
            )
        faulted = faulted_entries[0]

    if SCENARIO.startswith('auth.'):
        reauthorized = after(entries, faulted['seq'], 'b2_authorize_account', 200)
        if not reauthorized:
            raise Failure('recovery path', 'the CLI did not reauthorize after token expiry')
        if not after(entries, reauthorized[0]['seq'], 'b2_list_file_names', 200):
            raise Failure('recovery path', 'no successful listing after reauthorization')
        return

    if not after(entries, faulted['seq'], 'b2_list_file_names', 200):
        raise Failure('journal', 'no successful listing after the injected response')
    note(
        f'the whole b2 ls process took {elapsed:.2f} s against a floor of {floor:.1f} s; '
        f'an unfaulted b2 ls took {baseline:.2f} s'
    )
    if elapsed < floor:
        raise Failure('recovery path', f'retried after {elapsed:.2f} s, sooner than {floor:.1f} s')


def run_transfer_fault() -> None:
    cli, bucket = setup()
    endpoint, status, _code = TRANSFER_SCENARIOS[SCENARIO]
    if SCENARIO == 'download.retry_503':
        payload = bytes(index % 251 for index in range(4096))
        meta = uploaded_or_fail(put(cli, bucket, OBJECT_NAME, payload), payload)
        file_id = meta.get('fileId')
        if not file_id:
            raise Failure('upload fixture', 'b2 file upload reported no fileId')
        arm()
        round_trip(cli, f'b2id://{file_id}', payload)
    else:
        part_size = 5000
        part_count = 3
        payload = bytes(index % 251 for index in range(part_size * part_count))
        arm()
        uploaded_or_fail(
            put(
                cli,
                bucket,
                OBJECT_NAME,
                payload,
                '--threads',
                '1',
                '--min-part-size',
                str(part_size),
            ),
            payload,
            endpoint,
            large_file=True,
        )
        round_trip(cli, f'b2://{bucket}/{OBJECT_NAME}', payload)

    entries = journal()
    faulted_entries = [
        entry
        for entry in entries
        if entry['endpoint'] == endpoint
        and entry['fault'] == 'injected'
        and entry['status'] == status
    ]
    if len(faulted_entries) != 1:
        raise Failure('journal', f'{len(faulted_entries)} injected 503s on {endpoint}, expected 1')
    faulted = faulted_entries[0]
    retries = after(entries, faulted['seq'], endpoint, 200)
    if not retries:
        raise Failure('recovery path', f'{endpoint} was not retried after the 503')
    if SCENARIO == 'download.retry_503':
        return

    if not [
        entry
        for entry in entries
        if entry['endpoint'] == 'b2_start_large_file' and entry['status'] == 200
    ]:
        raise Failure(
            'journal', 'no b2_start_large_file; the upload never took the large-file path'
        )
    if not after(entries, faulted['seq'], 'b2_get_upload_part_url', 200):
        raise Failure('recovery path', 'no b2_get_upload_part_url after the 503')
    if retries[0]['uploadUrlId'] in (None, faulted['uploadUrlId']):
        raise Failure('recovery path', 'the retried part reused the failed part URL')
    if (
        len(
            [
                entry
                for entry in entries
                if entry['endpoint'] == 'b2_upload_part' and entry['status'] == 200
            ]
        )
        < part_count
    ):
        raise Failure('journal', f'fewer than {part_count} successful b2_upload_part calls')
    if not after(entries, faulted['seq'], 'b2_finish_large_file', 200):
        raise Failure('journal', 'the large file was never finished after the retried part')


def main() -> int:
    global SCENARIO, OBJECT_NAME, FAULT, ARM_PATH
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(143))
    # Name the scenario in the result line even when the configuration is refused.
    requested = os.environ.get('SDKHARNESS_SCENARIO', '')
    SCENARIO = requested if requested in ALL_SCENARIOS else 'unknown'
    try:
        check_checkout()
        SCENARIO = validate_environment(os.environ)
        OBJECT_NAME = f"res/{SCENARIO.replace('.', '-')}.bin"
        if SCENARIO in WIRE_SCENARIOS:
            kind = WIRE_SCENARIOS[SCENARIO]
            FAULT = {'on': 'b2_upload_file', 'kind': kind, 'count': 1}
            if kind == 'stall':
                FAULT['ms'] = 60000
            ARM_PATH = '/wire-faults'
            run_wire_fault()
        elif SCENARIO in API_SCENARIOS:
            config = API_SCENARIOS[SCENARIO]
            ARM_PATH = config['arm']
            FAULT = config['fault']
            run_api_fault()
        elif SCENARIO in TRANSFER_SCENARIOS:
            endpoint, status, code = TRANSFER_SCENARIOS[SCENARIO]
            FAULT = {'on': endpoint, 'status': status, 'code': code, 'count': 1}
            run_transfer_fault()
        else:
            endpoint, status, code = SCENARIOS[SCENARIO]
            FAULT = {'on': endpoint, 'status': status, 'code': code, 'count': 1}
            run_retry()
    except Skip as skipped:
        result('SKIP', f'{skipped.reason}: {skipped.detail}')
        return 0
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
