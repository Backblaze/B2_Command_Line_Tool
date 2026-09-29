######################################################################
#
# File: test/unit/test_sdkharness_file_lifecycle.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import pytest

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
CHECK = REPOSITORY_ROOT / '.sdkharness/tests/conformance-file-lifecycle.py'
SCENARIOS = {
    'files.delete_version',
    'files.download_by_id',
    'files.download_content',
    'files.hide',
    'files.list',
    'files.metadata',
}


def load_check():
    spec = importlib.util.spec_from_file_location('sdkharness_conformance_file_lifecycle', CHECK)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def simulator_environment(scenario: str = 'files.list', **overrides: str) -> dict[str, str]:
    environment = {
        'SDKHARNESS_TEST_LEVEL': 'conformance',
        'SDKHARNESS_SCENARIO': scenario,
        'SDKHARNESS_SIMULATOR_URL': 'http://127.0.0.1:8123',
        'B2_TEST_APPLICATION_KEY_ID': 'test-key-id',
        'B2_TEST_APPLICATION_KEY': 'test-key',
        'B2_BUCKET_NAME': 'sdkharness-healthcheck',
    }
    environment.update(overrides)
    return environment


def test_contract_rows_point_to_one_tracked_executable():
    rows = set((REPOSITORY_ROOT / '.sdkharness/tests.tsv').read_text().splitlines())
    for scenario in SCENARIOS:
        assert (
            f'conformance\t{scenario}\tsimulator\t'
            './.sdkharness/tests/conformance-file-lifecycle.py'
        ) in rows
    tracked = subprocess.run(
        ['git', 'ls-files', '-s', '--', CHECK.relative_to(REPOSITORY_ROOT)],
        cwd=REPOSITORY_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    assert tracked.stdout.split(maxsplit=1)[0] == '100755'


@pytest.mark.parametrize('scenario', sorted(SCENARIOS))
def test_environment_accepts_each_owned_scenario(scenario):
    check = load_check()
    assert check.validate_environment(simulator_environment(scenario))[0] == scenario


@pytest.mark.parametrize(
    ('overrides', 'expected'),
    [
        ({'SDKHARNESS_TEST_LEVEL': 'health'}, 'unexpected test level'),
        ({'SDKHARNESS_SCENARIO': 'files.upload'}, 'unexpected scenario'),
        ({'SDKHARNESS_SIMULATOR_URL': 'https://api.backblazeb2.com'}, 'loopback HTTP'),
        ({'B2_TEST_APPLICATION_KEY': ''}, 'required simulator input is missing'),
    ],
)
def test_environment_rejects_wrong_identity_or_unsafe_inputs(overrides, expected):
    check = load_check()
    with pytest.raises(check.CheckFailure, match=expected):
        check.validate_environment(simulator_environment(**overrides))


def test_versions_parser_accepts_cli_list_and_envelope_shapes():
    check = load_check()
    version = {'fileId': '4_zfixture', 'fileName': 'fixture'}
    assert check.versions_from(json.dumps([version]), 'list') == [version]
    assert check.versions_from(json.dumps({'files': [version]}), 'list') == [version]
    with pytest.raises(check.CheckFailure, match='no file list'):
        check.versions_from(json.dumps('not-a-list'), 'list')


@pytest.mark.parametrize('scenario', sorted(SCENARIOS))
def test_run_check_dispatches_scenario_and_always_cleans_up(tmp_path, scenario):
    check = load_check()
    calls: list[str] = []

    def scenario_method(self):
        calls.append(self.scenario)

    setattr(check.Lifecycle, scenario.replace('.', '_'), scenario_method)

    def fake_run(step: str, _command: list[str], environment: dict[str, str]):
        calls.append(step)
        assert environment['B2_ENVIRONMENT'] == environment['SDKHARNESS_SIMULATOR_URL']
        assert environment['B2_APPLICATION_KEY_ID'] == 'test-key-id'
        assert environment['B2_APPLICATION_KEY'] == 'test-key'
        return ''

    check.run_check(
        simulator_environment(scenario),
        cli_prefix=['repository-b2'],
        run_command=fake_run,
        object_prefix='sdkharness-conformance/fixed',
        scratch_root=tmp_path,
    )
    assert calls == ['authenticate', scenario, 'cleanup']


def test_scenario_failure_still_attempts_cleanup(tmp_path):
    check = load_check()
    calls: list[str] = []

    def scenario_method(_self):
        raise check.CheckFailure('scenario', 'failed')

    check.Lifecycle.files_list = scenario_method

    def fake_run(step: str, _command: list[str], _environment: dict[str, str]):
        calls.append(step)
        return ''

    with pytest.raises(check.CheckFailure, match='scenario'):
        check.run_check(
            simulator_environment(),
            cli_prefix=['repository-b2'],
            run_command=fake_run,
            object_prefix='sdkharness-conformance/fixed',
            scratch_root=tmp_path,
        )
    assert calls == ['authenticate', 'cleanup']
