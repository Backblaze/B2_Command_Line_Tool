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
import os
import subprocess
import sys
from pathlib import Path

import pytest

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
CHECK = REPOSITORY_ROOT / '.sdkharness/tests/conformance-file-lifecycle.py'
SCENARIOS = {
    'bucket.cors',
    'bucket.crud',
    'bucket.lifecycle',
    'bucket.notification_rules',
    'bucket.replication_config',
    'bucket.replication_helper',
    'client.auth_persistence',
    'client.progress',
    'client.sync',
    'enc.sse_b2',
    'enc.sse_c',
    'lock.bucket_default',
    'lock.bypass_governance',
    'lock.legal_hold',
    'lock.per_file_retention',
    'keys.crud',
    'keys.multi_bucket',
    'files.delete_version',
    'files.download_by_id',
    'files.download_content',
    'files.hide',
    'files.list',
    'files.metadata',
    'files.server_side_copy',
    'large.concurrent_parts',
    'large.multipart',
    'large.parallel_download',
    'large.unbound_incremental',
    'urls.native_download',
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
    check = load_check()
    assert check.SCENARIOS == SCENARIOS
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


def test_file_lock_metadata_helpers_accept_cli_wrapper_shapes():
    check = load_check()
    retention = {'mode': 'governance', 'retainUntilTimestamp': 123}
    assert check.Lifecycle.retention_value({'fileRetention': retention}) == retention
    assert check.Lifecycle.retention_value({'fileRetention': {'value': retention}}) == retention
    assert check.Lifecycle.legal_hold_value({'legalHold': 'on'}) == 'on'
    assert check.Lifecycle.legal_hold_value({'legalHold': {'value': 'off'}}) == 'off'


def test_process_probe_preserves_nonzero_status_and_captured_output():
    check = load_check()
    lifecycle = check.Lifecycle(
        simulator_environment(),
        cli_prefix=[
            sys.executable,
            '-c',
            "import sys; print('probe-out'); print('probe-err', file=sys.stderr); sys.exit(7)",
        ],
        run_command=lambda *_args: '',
        object_prefix='sdkharness-conformance/fixed',
        scratch_root=None,
    )
    probe_environment = {
        key: value for key, value in os.environ.items() if not key.startswith('B2_')
    }
    completed = lifecycle.invoke_process('probe', environment=probe_environment)
    assert completed.returncode == 7
    assert completed.stdout == 'probe-out\n'
    assert completed.stderr == 'probe-err\n'


def test_process_probe_reports_timeout_as_scenario_failure():
    check = load_check()
    lifecycle = check.Lifecycle(
        simulator_environment(),
        cli_prefix=[sys.executable, '-c', 'import time; time.sleep(10)'],
        run_command=lambda *_args: '',
        object_prefix='sdkharness-conformance/fixed',
        scratch_root=None,
    )
    lifecycle.child_environment = {
        key: value for key, value in os.environ.items() if not key.startswith('B2_')
    }

    with pytest.raises(check.CheckFailure, match='TimeoutExpired'):
        lifecycle.invoke_process('probe timeout', timeout=0.2)


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


class FakeHideCli:
    """Just enough CLI to run files.hide: one upload, one hide, version/name listings."""

    def __init__(self, hide_timestamp: int, original_timestamp: int = 100) -> None:
        self.payload = b''
        self.name = ''
        self.hidden = False
        self.hide_timestamp = hide_timestamp
        self.original_timestamp = original_timestamp

    def _versions(self):
        import hashlib

        versions = [
            {
                'fileId': 'original-id',
                'fileName': self.name,
                'action': 'upload',
                'size': len(self.payload),
                'contentSha1': hashlib.sha1(self.payload).hexdigest(),
                'uploadTimestamp': self.original_timestamp,
            }
        ]
        if self.hidden:
            versions.append(
                {
                    'fileId': 'hide-id',
                    'fileName': self.name,
                    'action': 'hide',
                    'size': 0,
                    'uploadTimestamp': self.hide_timestamp,
                }
            )
        return versions

    def __call__(self, step: str, command: list[str], _environment: dict[str, str]) -> str:
        import json

        args = command[1:]
        if args[:2] == ['file', 'upload']:
            self.payload = Path(args[-2]).read_bytes()
            self.name = args[-1]
        elif args[:2] == ['file', 'hide']:
            self.hidden = True
        elif args[:2] == ['file', 'download']:
            Path(args[-1]).write_bytes(self.payload)
        elif args[0] == 'ls' and '--versions' in args:
            return json.dumps(self._versions())
        elif args[0] == 'ls':
            return json.dumps([] if self.hidden else [{'fileName': self.name, 'action': 'upload'}])
        return ''


def run_hide(check, tmp_path, cli):
    check.run_check(
        simulator_environment('files.hide'),
        cli_prefix=['repository-b2'],
        run_command=cli,
        object_prefix='sdkharness-conformance/fixed',
        scratch_root=tmp_path,
    )


def test_files_hide_passes_when_the_newest_version_is_the_hide_marker(tmp_path):
    run_hide(load_check(), tmp_path, FakeHideCli(hide_timestamp=200))


def test_files_hide_fails_when_the_hide_marker_is_not_the_newest_version(tmp_path):
    check = load_check()
    with pytest.raises(check.CheckFailure) as caught:
        run_hide(check, tmp_path, FakeHideCli(hide_timestamp=50))
    assert caught.value.step == 'hide'
    assert 'newest version reports action' in caught.value.detail
