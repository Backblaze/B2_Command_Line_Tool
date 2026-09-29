######################################################################
#
# File: test/unit/test_sdkharness_conformance.py
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
CHECK = REPOSITORY_ROOT / '.sdkharness/tests/conformance-files-upload.py'


def load_check():
    spec = importlib.util.spec_from_file_location('sdkharness_conformance_files_upload', CHECK)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def simulator_environment(**overrides: str) -> dict[str, str]:
    environment = {
        'SDKHARNESS_TEST_LEVEL': 'conformance',
        'SDKHARNESS_SCENARIO': 'files.upload',
        'SDKHARNESS_SIMULATOR_URL': 'http://127.0.0.1:8123',
        'B2_TEST_APPLICATION_KEY_ID': 'test-key-id',
        'B2_TEST_APPLICATION_KEY': 'test-key',
        'B2_BUCKET_NAME': 'sdkharness-healthcheck',
    }
    environment.update(overrides)
    return environment


def test_contract_points_to_tracked_executable():
    rows = (REPOSITORY_ROOT / '.sdkharness/tests.tsv').read_text().splitlines()
    assert (
        'conformance\tfiles.upload\tsimulator\t' './.sdkharness/tests/conformance-files-upload.py'
    ) in rows
    tracked = subprocess.run(
        ['git', 'ls-files', '-s', '--', CHECK.relative_to(REPOSITORY_ROOT)],
        cwd=REPOSITORY_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    assert tracked.stdout.split(maxsplit=1)[0] == '100755'


@pytest.mark.parametrize(
    ('overrides', 'expected'),
    [
        ({'SDKHARNESS_TEST_LEVEL': 'health'}, 'unexpected test level'),
        ({'SDKHARNESS_SCENARIO': 'files.list'}, 'unexpected scenario'),
        ({'SDKHARNESS_SIMULATOR_URL': 'https://api.backblazeb2.com'}, 'loopback HTTP'),
        ({'B2_TEST_APPLICATION_KEY': ''}, 'required simulator input is missing'),
    ],
)
def test_environment_rejects_wrong_identity_or_unsafe_inputs(overrides, expected):
    check = load_check()
    with pytest.raises(check.CheckFailure, match=expected):
        check.validate_environment(simulator_environment(**overrides))


def test_repository_cli_prefix_imports_this_checkout():
    check = load_check()
    prefix = check.repository_cli_prefix()
    assert prefix[:2] == [sys.executable, '-m']
    assert prefix[2].startswith('b2._internal.')
    assert Path(check.b2.__file__).resolve().is_relative_to(REPOSITORY_ROOT)


def test_upload_contract_checks_metadata_round_trip_and_cleanup(tmp_path):
    check = load_check()
    object_name = 'sdkharness-conformance/fixed.bin'
    calls: list[str] = []
    payload = (b'sdkharness-files-upload-' * 45)[:1024]

    def fake_run(step: str, command: list[str], environment: dict[str, str]):
        calls.append(step)
        assert environment['B2_ENVIRONMENT'] == environment['SDKHARNESS_SIMULATOR_URL']
        if step == 'metadata':
            return json.dumps(
                [
                    {
                        'fileId': '4_zfixture',
                        'fileName': object_name,
                        'size': 1024,
                        'contentSha1': check.hashlib.sha1(
                            payload, usedforsecurity=False
                        ).hexdigest(),
                    }
                ]
            )
        if step == 'download':
            Path(command[-1]).write_bytes(payload)
        return ''

    check.run_check(
        simulator_environment(),
        cli_prefix=['repository-b2'],
        run_command=fake_run,
        object_name=object_name,
        scratch_root=tmp_path,
    )
    assert calls == ['authenticate', 'upload', 'metadata', 'download', 'cleanup']


def test_failure_still_attempts_cleanup(tmp_path):
    check = load_check()
    calls: list[str] = []

    def fake_run(step: str, _command: list[str], _environment: dict[str, str]):
        calls.append(step)
        if step == 'metadata':
            raise check.CheckFailure(step, 'command failed')
        return ''

    with pytest.raises(check.CheckFailure, match='metadata'):
        check.run_check(
            simulator_environment(),
            cli_prefix=['repository-b2'],
            run_command=fake_run,
            object_name='sdkharness-conformance/fixed.bin',
            scratch_root=tmp_path,
        )
    assert calls[-1] == 'cleanup'
