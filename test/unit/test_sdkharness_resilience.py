######################################################################
#
# File: test/unit/test_sdkharness_resilience.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import hashlib
import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import pytest

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
CHECK = REPOSITORY_ROOT / '.sdkharness/tests/resilience-upload.py'
SCENARIOS = {
    'upload.retry_503',
    'upload.retry_500',
    'upload.retry_408',
    'upload.expired_token_401',
    'upload.get_url_503',
    'upload.cap_exceeded_403',
    'upload.reset_before_response',
    'upload.reset_mid_request',
    'upload.stall',
    'auth.expired_401',
    'auth.clock_expiry',
    'api.retry_after_429',
    'api.retry_after_503',
    'api.backoff_503',
    'download.retry_503',
    'part.retry_503',
}


def load_check():
    spec = importlib.util.spec_from_file_location('sdkharness_resilience_upload', CHECK)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def environment(scenario: str = 'upload.retry_503', **overrides: str) -> dict[str, str]:
    value = {
        'SDKHARNESS_TEST_LEVEL': 'resilience',
        'SDKHARNESS_SCENARIO': scenario,
        'SDKHARNESS_SIMULATOR_URL': 'http://127.0.0.1:8123',
        'SDKHARNESS_SIMULATOR_CONTROL_URL': 'http://127.0.0.1:8124',
    }
    value.update(overrides)
    return value


def test_contract_rows_point_to_one_tracked_executable():
    check = load_check()
    assert check.ALL_SCENARIOS == SCENARIOS
    rows = set((REPOSITORY_ROOT / '.sdkharness/tests.tsv').read_text().splitlines())
    for scenario in SCENARIOS:
        assert (
            f'resilience\t{scenario}\tsimulator\t./.sdkharness/tests/resilience-upload.py' in rows
        )
    tracked = subprocess.run(
        ['git', 'ls-files', '-s', '--', CHECK.relative_to(REPOSITORY_ROOT)],
        cwd=REPOSITORY_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    assert tracked.stdout.split(maxsplit=1)[0] == '100755'


def test_scenario_groups_partition_the_registered_contract():
    check = load_check()
    groups = [
        set(check.SCENARIOS),
        set(check.WIRE_SCENARIOS),
        set(check.API_SCENARIOS),
        set(check.TRANSFER_SCENARIOS),
    ]
    assert set().union(*groups) == SCENARIOS
    for index, group in enumerate(groups):
        assert group.isdisjoint(set().union(*groups[index + 1 :]))


@pytest.mark.parametrize('scenario', sorted(SCENARIOS))
def test_environment_accepts_each_owned_scenario(scenario):
    check = load_check()
    assert check.validate_environment(environment(scenario)) == scenario


@pytest.mark.parametrize(
    ('overrides', 'expected'),
    [
        ({'SDKHARNESS_TEST_LEVEL': 'conformance'}, 'unexpected test level'),
        ({'SDKHARNESS_SCENARIO': 'bucket.crud'}, 'unexpected scenario'),
        (
            {'SDKHARNESS_SIMULATOR_URL': 'https://api.backblazeb2.com'},
            'bare IPv4 loopback HTTP origin',
        ),
        (
            {'SDKHARNESS_SIMULATOR_CONTROL_URL': 'http://example.com'},
            'bare IPv4 loopback HTTP origin',
        ),
        (
            {'SDKHARNESS_SIMULATOR_CONTROL_URL': 'http://127.0.0.1:8124/redirect'},
            'bare IPv4 loopback HTTP origin',
        ),
        (
            {'SDKHARNESS_SIMULATOR_CONTROL_URL': 'http://user@127.0.0.1:8124'},
            'bare IPv4 loopback HTTP origin',
        ),
    ],
)
def test_environment_rejects_wrong_identity_or_unsafe_inputs(overrides, expected):
    check = load_check()
    with pytest.raises(check.Failure, match=expected):
        check.validate_environment(environment(**overrides))


class FakeProcess:
    def __init__(self, stdout: str, returncode: int = 0) -> None:
        self.stdout = stdout
        self.stderr = ''
        self.returncode = returncode


PAYLOAD = b'sdkharness resilience payload'


def file_record(sha1: str) -> str:
    return json.dumps({'fileId': '4_zabc', 'fileName': 'res/x.bin', 'contentSha1': sha1})


def test_upload_must_report_the_sha1_of_the_bytes_that_were_sent():
    check = load_check()
    right = hashlib.sha1(PAYLOAD).hexdigest()
    assert check.uploaded_or_fail(FakeProcess(file_record(right)), PAYLOAD)['fileId'] == '4_zabc'
    assert check.uploaded_or_fail(FakeProcess(file_record(f'unverified:{right}')), PAYLOAD)
    with pytest.raises(check.Failure, match='contentSha1 does not match'):
        # a small file has a real checksum; 'none' is only valid for a large file
        check.uploaded_or_fail(FakeProcess(file_record('none')), PAYLOAD)
    with pytest.raises(check.Failure) as caught:
        check.uploaded_or_fail(FakeProcess(file_record('0' * 40)), PAYLOAD)
    assert caught.value.step == 'upload'
    assert 'contentSha1 does not match' in caught.value.detail


def test_a_large_file_may_report_no_sha1_only_with_a_matching_large_file_sha1():
    check = load_check()
    digest = hashlib.sha1(PAYLOAD).hexdigest()
    record = json.dumps(
        {'fileId': '4_zabc', 'contentSha1': 'none', 'fileInfo': {'large_file_sha1': digest}}
    )
    assert (
        check.uploaded_or_fail(FakeProcess(record), PAYLOAD, large_file=True)['fileId'] == '4_zabc'
    )
    with pytest.raises(check.Failure, match='large_file_sha1'):
        check.uploaded_or_fail(FakeProcess(file_record('none')), PAYLOAD, large_file=True)


def test_upload_must_exit_zero_and_print_a_json_record():
    check = load_check()
    check.journal = lambda: []  # the control listener is not running in a unit test
    with pytest.raises(check.Failure) as nonzero:
        check.uploaded_or_fail(FakeProcess('', returncode=1), PAYLOAD)
    assert 'was not recovered' in nonzero.value.detail
    with pytest.raises(check.Failure) as no_json:
        check.uploaded_or_fail(FakeProcess('not json'), PAYLOAD)
    assert 'no JSON file record' in no_json.value.detail
