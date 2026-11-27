######################################################################
#
# File: test/unit/test_b2_simulator_plugin.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
"""Tests of the b2-simulator backend plugin (``test/unit/b2_simulator.py``)."""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest
from b2sdk.v3 import B2RawHTTPApi, RawSimulator

from test.unit import b2_simulator
from test.unit.b2_simulator import b2_simulator_enabled

ROOT = Path(__file__).parents[2]

PASSING_TEST = """
def test_nothing():
    pass
"""

BACKEND_TEST = """
from b2sdk.v3 import RawSimulator

def test_backend(b2_cli):
    assert type(b2_cli.raw_api) is RawSimulator
"""


def run_pytest(tmp_path, test_source, *args, env_update=None, env_remove=()):
    """Run pytest in a subprocess on one generated test file, with the unit-test plugins loaded."""
    test_file = tmp_path / 'test_generated.py'
    test_file.write_text(textwrap.dedent(test_source))
    env = {name: value for name, value in os.environ.items() if name not in env_remove}
    env.pop('PYTEST_XDIST_WORKER', None)
    env['PYTHONPATH'] = os.pathsep.join([str(ROOT), env.get('PYTHONPATH', '')])
    env.update(env_update or {})
    return subprocess.run(
        [
            sys.executable,
            '-m',
            'pytest',
            '-p',
            'test.conftest',
            '-p',
            'test.unit.conftest',
            '-p',
            'no:cacheprovider',
            '-p',
            'no:xdist',
            '-q',
            f'--rootdir={tmp_path}',
            *args,
            str(test_file),
        ],
        cwd=tmp_path,
        env=env,
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )


def test_flag_without_a_checkout_fails_and_names_the_variable(tmp_path):
    completed = run_pytest(
        tmp_path, PASSING_TEST, '--b2-simulator', env_remove=('B2_SIMULATOR_DIR',)
    )
    assert completed.returncode == pytest.ExitCode.USAGE_ERROR, completed.stdout
    assert 'the b2-simulator backend needs B2_SIMULATOR_DIR' in completed.stderr
    assert 'passed' not in completed.stdout
    assert 'skipped' not in completed.stdout


def test_flag_with_a_directory_that_is_not_a_checkout_fails(tmp_path):
    not_a_checkout = tmp_path / 'empty'
    not_a_checkout.mkdir()
    completed = run_pytest(
        tmp_path,
        PASSING_TEST,
        '--b2-simulator',
        env_update={'B2_SIMULATOR_DIR': str(not_a_checkout)},
    )
    assert completed.returncode == pytest.ExitCode.USAGE_ERROR, completed.stdout
    expected = not_a_checkout / 'bin' / 'simulator' / 'serve.mjs'
    assert f'b2-simulator backend: no server at {expected}' in completed.stderr


def test_ini_option_makes_the_simulator_the_default(tmp_path):
    # With the ini option on and no checkout, the run needs the simulator, so it stops.
    completed = run_pytest(
        tmp_path, PASSING_TEST, '-o', 'b2_simulator=true', env_remove=('B2_SIMULATOR_DIR',)
    )
    assert completed.returncode == pytest.ExitCode.USAGE_ERROR, completed.stdout
    assert 'the b2-simulator backend needs B2_SIMULATOR_DIR' in completed.stderr


def test_no_flag_overrides_the_ini_option(tmp_path):
    completed = run_pytest(
        tmp_path,
        BACKEND_TEST,
        '-o',
        'b2_simulator=true',
        '--no-b2-simulator',
        env_remove=('B2_SIMULATOR_DIR',),
    )
    assert completed.returncode == pytest.ExitCode.OK, completed.stdout + completed.stderr
    assert '1 passed' in completed.stdout


def test_default_run_uses_raw_simulator(tmp_path):
    # Even with a checkout configured, nothing changes without the flag or the ini option.
    completed = run_pytest(
        tmp_path, BACKEND_TEST, env_update={'B2_SIMULATOR_DIR': str(tmp_path / 'unused')}
    )
    assert completed.returncode == pytest.ExitCode.OK, completed.stdout + completed.stderr
    assert '1 passed' in completed.stdout


def test_b2_cli_uses_the_selected_backend(b2_cli):
    raw_api = b2_cli.raw_api
    if not b2_simulator_enabled():
        assert type(raw_api) is RawSimulator
        return
    assert isinstance(raw_api, B2RawHTTPApi)
    assert (b2_cli.account_id, b2_cli.master_key) == ('simacct00001', 'sim-master-secret')
    b2_cli.authorize()
    assert b2_cli.account_info.get_api_url() == b2_simulator._server.api_url
    assert b2_cli.account_info.get_account_id() == b2_cli.account_id
    # RawSimulator.create_account() makes a new account on every call; b2-simulator has one.
    with pytest.raises(RuntimeError, match='b2-simulator has one account'):
        raw_api.create_account()


def test_reset_lists_only_the_keys_a_test_made(b2_cli):
    b2_cli.authorize()
    assert list(b2_cli.b2_api.list_keys()) == []


@pytest.mark.b2_simulator_config(minimumPartSize=300, recommendedPartSize=400)
def test_marker_config_is_sent_with_the_reset(b2_cli):
    b2_cli.authorize()
    part_sizes = (
        b2_cli.account_info.get_absolute_minimum_part_size(),
        b2_cli.account_info.get_recommended_part_size(),
    )
    if b2_simulator_enabled():
        assert part_sizes == (300, 400)
    else:
        # The marker only shapes the simulator's reset.
        assert part_sizes == (RawSimulator.MIN_PART_SIZE, RawSimulator.MIN_PART_SIZE)
