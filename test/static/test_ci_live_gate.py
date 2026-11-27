######################################################################
#
# File: test/static/test_ci_live_gate.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
"""Keep pull requests on Simulator and real-B2 tests outside the PR path."""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).parents[2]
WORKFLOWS = ROOT / '.github/workflows'
CI_TEXT = (WORKFLOWS / 'ci.yml').read_text()
LIVE_PATH = WORKFLOWS / 'live-b2.yml'
REPORT_SCRIPT = ROOT / 'scripts/report-live-b2-failure.sh'
KEYS = ('B2_TEST_APPLICATION_KEY', 'B2_TEST_APPLICATION_KEY_ID')
LIVE_TEST_JOBS = (
    'live-b2-integration',
    'live-b2-docker',
    'live-b2-linux-bundle',
    'live-b2-windows-bundle',
)
POSIX = os.name == 'posix' and all(shutil.which(tool) for tool in ('bash', 'jq'))


def triggers(workflow):
    """Return workflow triggers despite PyYAML treating ``on`` as YAML 1.1 boolean."""
    return workflow.get('on', workflow.get(True))


def load_live():
    return yaml.safe_load(LIVE_PATH.read_text())


def run_scripts(job):
    return '\n'.join(step.get('run', '') for step in job['steps'])


def test_pull_request_ci_never_references_real_b2_credentials():
    for key in KEYS:
        assert not re.search(rf'secrets\.{key}\b', CI_TEXT)
        assert not re.search(
            rf'secrets\.{key}\b', (WORKFLOWS / 'simulator-integration.yml').read_text()
        )


def test_the_retired_live_tests_switch_is_not_read_by_any_workflow():
    # B2_LIVE_TESTS_ENABLED used to opt pull request CI into live B2. Pull requests no longer run
    # live B2, and the weekly run is paused by disabling the workflow, not by a variable that
    # would turn a scheduled run green without testing anything.
    for workflow in WORKFLOWS.glob('*.yml'):
        assert 'B2_LIVE_TESTS_ENABLED' not in workflow.read_text(), workflow.name


def test_live_b2_workflow_is_weekly_and_manual_only():
    workflow = load_live()
    event_names = set(triggers(workflow))
    assert event_names == {'schedule', 'workflow_dispatch'}
    assert triggers(workflow)['schedule']


def test_live_runs_never_overlap_on_the_shared_b2_account():
    workflow = load_live()
    assert workflow['concurrency'] == {'group': 'live-b2', 'cancel-in-progress': False}


def test_weekly_live_b2_run_fails_closed_without_credentials():
    workflow = load_live()
    preflight = workflow['jobs']['live-b2-preflight']
    assert preflight['env'] == {
        'HAS_B2_TEST_APPLICATION_KEY_ID': "${{ secrets.B2_TEST_APPLICATION_KEY_ID != '' }}",
        'HAS_B2_TEST_APPLICATION_KEY': "${{ secrets.B2_TEST_APPLICATION_KEY != '' }}",
    }
    assert 'if' not in preflight
    script = run_scripts(preflight)

    def check(key_id, key):
        return subprocess.run(
            ['bash', '-eu', '-c', script],
            env={
                **os.environ,
                'HAS_B2_TEST_APPLICATION_KEY_ID': key_id,
                'HAS_B2_TEST_APPLICATION_KEY': key,
            },
            capture_output=True,
            text=True,
            check=False,
        )

    assert check('true', 'true').returncode == 0
    for key_id, key in (('false', 'true'), ('true', 'false'), ('false', 'false')):
        result = check(key_id, key)
        assert result.returncode == 1
        assert (
            'B2_TEST_APPLICATION_KEY_ID and B2_TEST_APPLICATION_KEY are required' in result.stdout
        )

    cleanup = workflow['jobs']['live-b2-cleanup']
    assert cleanup['needs'] == 'live-b2-preflight'
    assert 'uv run nox -vs cleanup_buckets' in run_scripts(cleanup)
    for name in LIVE_TEST_JOBS:
        job = workflow['jobs'][name]
        assert job['needs'] == 'live-b2-cleanup', name
        assert 'if' not in job, name


def test_every_live_job_receives_the_live_credentials_and_runs_secret_tests():
    workflow = load_live()
    for name in ('live-b2-cleanup', *LIVE_TEST_JOBS):
        job = workflow['jobs'][name]
        for key in KEYS:
            assert job['env'][key] == f'${{{{ secrets.{key} }}}}', name
        assert job['env']['WORKFLOW_ID'].startswith('${{ github.run_id }}'), name
    for name in LIVE_TEST_JOBS:
        assert '--cleanup' in run_scripts(workflow['jobs'][name]), name


def test_live_b2_covers_the_python_versions_and_systems_that_used_to_run_live():
    job = load_live()['jobs']['live-b2-integration']
    matrix = job['strategy']['matrix']
    assert job['strategy']['fail-fast'] is False
    assert job['runs-on'] == '${{ matrix.os }}'
    combinations = {
        (os_name, python)
        for os_name in matrix['os']
        for python in matrix['python-version']
        if {'os': os_name, 'python-version': python} not in matrix.get('exclude', [])
    }
    assert combinations == {
        (os_name, python)
        for os_name in ('ubuntu-latest', 'ubuntu-24.04-arm', 'macos-latest', 'windows-latest')
        for python in ('3.10', '3.14', 'pypy3.10')
        if not (python == 'pypy3.10' and os_name in ('macos-latest', 'windows-latest'))
    }
    assert (
        'uv run nox -vs integration -p ${{ matrix.python-version }} -- -m "require_secrets" '
        '--cleanup' in run_scripts(job)
    )


def test_live_b2_tests_the_docker_image_and_the_bundles():
    jobs = load_live()['jobs']
    docker = jobs['live-b2-docker']
    assert 'uv run nox -vs docker_test -- backblazeit/b2:test --cleanup' in run_scripts(docker)
    assert 'B2_TEST_DOCKER_NETWORK' not in str(docker)

    linux = jobs['live-b2-linux-bundle']
    assert set(linux['strategy']['matrix']['os']) == {'ubuntu-22.04', 'ubuntu-22.04-arm'}
    assert jobs['live-b2-windows-bundle']['runs-on'] == 'windows-2022'
    for job in (linux, jobs['live-b2-windows-bundle']):
        assert (
            'uv run nox -vs integration -p ${{ env.PYTHON_DEFAULT_VERSION }} -- '
            '--sut=${{ steps.bundle.outputs.sut_path }} -m "require_secrets" --cleanup'
        ) in run_scripts(job)


def test_only_the_failure_report_can_write_issues():
    workflow = load_live()
    assert workflow['permissions'] == {'contents': 'read'}
    report = workflow['jobs']['report-scheduled-failure']
    assert report['permissions'] == {'contents': 'read', 'issues': 'write'}
    assert set(report['needs']) == {'live-b2-preflight', 'live-b2-cleanup', *LIVE_TEST_JOBS}
    assert report['if'] == "${{ failure() && github.event_name == 'schedule' }}"
    assert 'run: scripts/report-live-b2-failure.sh' in yaml.safe_dump(report, width=1000)
    for name, job in workflow['jobs'].items():
        if name != 'report-scheduled-failure':
            assert 'permissions' not in job, name
            assert 'secrets.GITHUB_TOKEN' not in str(job), name
            assert 'github.token' not in str(job), name


@pytest.fixture
def fake_gh(tmp_path):
    fake_bin = tmp_path / 'bin'
    fake_bin.mkdir()
    gh = fake_bin / 'gh'
    gh.write_text(
        f'#!{sys.executable}\n'
        'import json, os, pathlib, sys\n'
        'with pathlib.Path(os.environ["GH_CALLS"]).open("a") as calls:\n'
        '    calls.write(json.dumps(sys.argv[1:]) + "\\n")\n'
        'if sys.argv[1:3] == ["issue", "list"]:\n'
        '    print(os.environ["GH_ISSUES"])\n'
    )
    gh.chmod(0o755)
    calls = tmp_path / 'calls'

    def run(issues):
        result = subprocess.run(
            ['bash', str(REPORT_SCRIPT)],
            env={
                **os.environ,
                'PATH': f'{fake_bin}{os.pathsep}{os.environ["PATH"]}',
                'GH_CALLS': str(calls),
                'GH_ISSUES': json.dumps(issues),
                'GITHUB_SERVER_URL': 'https://github.com',
                'GITHUB_REPOSITORY': 'Backblaze/B2_Command_Line_Tool',
                'GITHUB_RUN_ID': '42',
            },
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        recorded = [json.loads(line) for line in calls.read_text().splitlines()]
        return result, recorded

    return run


TITLE = 'Weekly live B2 integration failed'
RUN_URL = 'https://github.com/Backblaze/B2_Command_Line_Tool/actions/runs/42'


def option(call, name):
    return call[call.index(name) + 1]


@pytest.mark.skipif(not POSIX, reason='report script uses Bash and jq')
def test_a_failed_scheduled_run_opens_an_issue(fake_gh):
    result, calls = fake_gh([{'number': 7, 'title': 'Some other issue'}])
    assert result.returncode == 0, result.stdout + result.stderr
    assert calls[0][:2] == ['issue', 'list']
    assert option(calls[0], '--state') == 'open'
    created = calls[1]
    assert created[:2] == ['issue', 'create']
    assert option(created, '--title') == TITLE
    assert RUN_URL in option(created, '--body')
    assert option(created, '--repo') == 'Backblaze/B2_Command_Line_Tool'
    assert len(calls) == 2


@pytest.mark.skipif(not POSIX, reason='report script uses Bash and jq')
def test_a_repeated_failure_updates_the_open_issue(fake_gh):
    result, calls = fake_gh([{'number': 7, 'title': 'Other'}, {'number': 12, 'title': TITLE}])
    assert result.returncode == 0, result.stdout + result.stderr
    assert len(calls) == 2
    comment = calls[1]
    assert comment[:3] == ['issue', 'comment', '12']
    assert RUN_URL in option(comment, '--body')
    assert not any(call[:2] == ['issue', 'create'] for call in calls)
