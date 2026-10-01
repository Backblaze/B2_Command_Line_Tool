######################################################################
#
# File: test/unit/test_sdkharness_health.py
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
HEALTH_CHECK = REPOSITORY_ROOT / '.sdkharness/tests/health-golden-path.py'


def load_health_check():
    spec = importlib.util.spec_from_file_location('sdkharness_health_golden_path', HEALTH_CHECK)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def simulator_environment(**overrides: str) -> dict[str, str]:
    environment = {
        'SDKHARNESS_TEST_LEVEL': 'health',
        'SDKHARNESS_SCENARIO': 'golden-path',
        'SDKHARNESS_SIMULATOR_URL': 'http://127.0.0.1:8123',
        'HEALTHCHECK_REALM_URL': 'http://127.0.0.1:8123',
        'B2_TEST_APPLICATION_KEY_ID': 'test-key-id',
        'B2_TEST_APPLICATION_KEY': 'test-key',
        'B2_BUCKET_NAME': 'sdkharness-healthcheck',
    }
    environment.update(overrides)
    return environment


def test_contract_is_simulator_only_and_points_to_the_executable():
    rows = (REPOSITORY_ROOT / '.sdkharness/tests.tsv').read_text().splitlines()
    assert rows[0] == 'test_level\tscenario\ttarget\texecutable'
    assert (
        rows.count('health\tgolden-path\tsimulator\t./.sdkharness/tests/health-golden-path.py') == 1
    )
    tracked = subprocess.run(
        ['git', 'ls-files', '-s', '--', HEALTH_CHECK.relative_to(REPOSITORY_ROOT)],
        cwd=REPOSITORY_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    assert tracked.stdout.split(maxsplit=1)[0] == '100755'


@pytest.mark.parametrize(
    ('overrides', 'expected'),
    [
        ({'SDKHARNESS_SIMULATOR_URL': ''}, 'simulator URL mismatch'),
        ({'HEALTHCHECK_REALM_URL': 'http://127.0.0.1:9999'}, 'simulator URL mismatch'),
        (
            {
                'SDKHARNESS_SIMULATOR_URL': 'https://api.backblazeb2.com',
                'HEALTHCHECK_REALM_URL': 'https://api.backblazeb2.com',
            },
            'simulator URL must be loopback HTTP',
        ),
        ({'B2_TEST_APPLICATION_KEY_ID': ''}, 'required simulator input is missing'),
    ],
)
def test_environment_rejects_unsafe_or_incomplete_inputs(overrides, expected):
    health = load_health_check()

    with pytest.raises(health.CheckFailure, match=expected):
        health.validate_environment(simulator_environment(**overrides))


def test_repository_cli_prefix_imports_this_checkout():
    health = load_health_check()

    prefix = health.repository_cli_prefix()

    assert prefix[:2] == [sys.executable, '-m']
    assert prefix[2].startswith('b2._internal.')
    assert Path(health.b2.__file__).resolve().is_relative_to(REPOSITORY_ROOT)


def test_golden_path_drives_authorize_upload_download_list_and_delete(tmp_path):
    health = load_health_check()
    object_name = 'sdkharness-health-check/fixed.txt'
    calls: list[tuple[str, list[str]]] = []
    deleted = False

    def fake_run(step: str, command: list[str], environment: dict[str, str]):
        nonlocal deleted
        calls.append((step, command))
        assert environment['B2_ENVIRONMENT'] == environment['SDKHARNESS_SIMULATOR_URL']
        assert environment['B2_APPLICATION_KEY_ID'] == 'test-key-id'
        assert environment['B2_APPLICATION_KEY'] == 'test-key'
        assert environment['B2_ACCOUNT_INFO'].startswith(str(tmp_path))
        if step == 'download':
            Path(command[-1]).write_bytes(health.payload_for(object_name))
        if step == 'delete':
            deleted = True
        if step == 'list':
            return '' if deleted else object_name
        return ''

    health.run_health(
        simulator_environment(),
        cli_prefix=['repository-b2'],
        run_command=fake_run,
        object_name=object_name,
        scratch_root=tmp_path,
    )

    assert [step for step, _ in calls] == [
        'authenticate',
        'upload',
        'download',
        'list',
        'delete',
        'confirm gone',
    ]
    assert all(command[0] == 'repository-b2' for _, command in calls)


def test_golden_path_attempts_cleanup_after_a_failure(tmp_path):
    health = load_health_check()
    calls: list[str] = []

    def fake_run(step: str, _command: list[str], _environment: dict[str, str]):
        calls.append(step)
        if step == 'download':
            raise health.CheckFailure(step, 'command failed')
        return ''

    with pytest.raises(health.CheckFailure, match='download'):
        health.run_health(
            simulator_environment(),
            cli_prefix=['repository-b2'],
            run_command=fake_run,
            object_name='sdkharness-health-check/fixed.txt',
            scratch_root=tmp_path,
        )

    assert calls[-1] == 'cleanup'


def forbidden_run(step, _command, _environment):  # pragma: no cover - must not run
    raise AssertionError(f'a non-simulator credential reached the CLI at {step!r}')


@pytest.mark.parametrize(
    ('key_id', 'key'),
    [
        ('005realkeyid0000000000000', 'K005realapplicationkey00000000000'),
        ('test-key-id', 'K005realapplicationkey00000000000'),
        ('005realkeyid0000000000000', 'test-key'),
    ],
)
def test_ambient_real_looking_credentials_are_refused_before_the_cli(key_id, key):
    health = load_health_check()
    environment = simulator_environment(
        B2_TEST_APPLICATION_KEY_ID=key_id, B2_TEST_APPLICATION_KEY=key
    )

    with pytest.raises(health.CheckFailure) as raised:
        health.run_health(environment, cli_prefix=['must-not-run'], run_command=forbidden_run)
    assert raised.value.step == 'configuration'
    assert raised.value.detail == 'only the fixed simulator credential is accepted'
    assert 'K005' not in str(raised.value.detail)


def test_non_ipv4_loopback_simulator_urls_are_refused():
    health = load_health_check()
    environment = simulator_environment(
        SDKHARNESS_SIMULATOR_URL='http://[::1]:8123', HEALTHCHECK_REALM_URL='http://[::1]:8123'
    )

    with pytest.raises(health.CheckFailure, match='loopback HTTP'):
        health.validate_environment(environment)


def test_the_cli_child_inherits_no_ambient_b2_values(tmp_path):
    health = load_health_check()
    seen = []

    def recording_run(step, _command, environment):
        seen.append(dict(environment))
        return ''

    try:
        health.run_health(
            simulator_environment(B2_AMBIENT_SECRET='leak', B2_ACCOUNT_INFO='/ambient/db'),
            cli_prefix=['repository-b2'],
            run_command=recording_run,
            scratch_root=tmp_path,
            object_name='sdkharness-health-check/fixed.txt',
        )
    except health.CheckFailure:
        pass  # later steps of the fake CLI may fail; only the child environments matter
    assert seen
    for environment in seen:
        assert 'B2_AMBIENT_SECRET' not in environment
        assert 'B2_TEST_APPLICATION_KEY' not in environment
        assert 'B2_TEST_APPLICATION_KEY_ID' not in environment
        assert environment['B2_APPLICATION_KEY_ID'] == 'test-key-id'
        assert environment['B2_APPLICATION_KEY'] == 'test-key'
        assert environment['B2_ACCOUNT_INFO'].startswith(str(tmp_path))
