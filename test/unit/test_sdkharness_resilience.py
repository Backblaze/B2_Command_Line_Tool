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

import importlib.util
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
