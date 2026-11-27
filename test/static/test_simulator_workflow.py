######################################################################
#
# File: test/static/test_simulator_workflow.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
"""Pin the mandatory, private-Simulator PR integration-test policy."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import textwrap
import time
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).parents[2]
WORKFLOW_PATH = ROOT / '.github/workflows/simulator-integration.yml'
HELPER_PATH = ROOT / 'scripts/with-b2-simulator.sh'
SIMULATOR_REPOSITORY = 'backblaze-labs/b2-simulator'
SIMULATOR_REVISION = 'a1e1db373b71f2d5f40082c19bc716cb6b3c3675'
KEY_CHECK_STEP = 'Require the private Simulator deploy key'
DEPENDABOT_MESSAGE = 'an admin must add SIMULATOR_DEPLOY_KEY as a Dependabot secret'
POSIX = os.name == 'posix' and shutil.which('bash') is not None


def load_workflow():
    return yaml.safe_load(WORKFLOW_PATH.read_text())


def triggers(workflow):
    # PyYAML 1.1 treats the unquoted key ``on`` as boolean True.
    return workflow.get('on', workflow.get(True))


def simulator_checkout(job):
    return next(
        step
        for step in job['steps']
        if step.get('with', {}).get('repository') == SIMULATOR_REPOSITORY
    )


def simulator_jobs(workflow):
    return {
        name: job
        for name, job in workflow['jobs'].items()
        if any(
            step.get('with', {}).get('repository') == SIMULATOR_REPOSITORY for step in job['steps']
        )
    }


def step_named(job, name):
    return next(step for step in job['steps'] if step.get('name') == name)


def test_every_pull_request_is_subject_to_the_simulator_policy():
    workflow = load_workflow()
    workflow_triggers = triggers(workflow)
    assert 'pull_request_target' not in workflow_triggers
    pull_request = workflow_triggers['pull_request']
    assert set(pull_request['types']) == {
        'opened',
        'synchronize',
        'reopened',
        'ready_for_review',
    }

    policy = workflow['jobs']['simulator-access-policy']
    assert 'if' not in policy  # a skipped job would satisfy a required check
    policy_script = policy['steps'][0]['run']
    assert 'github.event.pull_request.head.repo.full_name' in str(policy['steps'][0]['env'])

    def run_policy(*, event_name, head_repository):
        env = {
            **os.environ,
            'EVENT_NAME': event_name,
            'HEAD_REPOSITORY': head_repository,
            'GITHUB_REPOSITORY': 'Backblaze/B2_Command_Line_Tool',
        }
        return subprocess.run(['bash', '-eu', '-c', policy_script], env=env, check=False)

    assert (
        run_policy(
            event_name='pull_request',
            head_repository='Backblaze/B2_Command_Line_Tool',
        ).returncode
        == 0
    )
    assert run_policy(event_name='push', head_repository='').returncode == 0
    assert (
        run_policy(
            event_name='pull_request',
            head_repository='outside-contributor/B2_Command_Line_Tool',
        ).returncode
        != 0
    )


def test_the_simulator_revision_is_stated_once_and_every_checkout_uses_it():
    workflow = load_workflow()
    assert workflow['env']['SIMULATOR_REVISION'] == SIMULATOR_REVISION
    assert WORKFLOW_PATH.read_text().count(SIMULATOR_REVISION) == 1
    jobs = simulator_jobs(workflow)
    assert jobs
    for name, job in jobs.items():
        checkout = simulator_checkout(job)
        assert checkout['with']['ref'] == '${{ env.SIMULATOR_REVISION }}', name
        verify = step_named(job, 'Verify the Simulator revision')
        assert '"$SIMULATOR_REVISION"' in verify['run'], name


def test_private_simulator_is_exactly_pinned_and_uses_a_read_only_deploy_key():
    workflow = load_workflow()
    assert workflow['permissions'] == {'contents': 'read'}
    assert 'continue-on-error' not in str(workflow)
    job = workflow['jobs']['simulator-integration']
    assert job['needs'] == 'simulator-access-policy'
    assert 'if' not in job  # a skipped job would satisfy a required check
    assert job['timeout-minutes'] == 90

    for name, simulator_job in simulator_jobs(workflow).items():
        assert 'if' not in simulator_job, name  # a skipped job would satisfy a required check
        cli_checkout = step_named(simulator_job, 'Check out the CLI')
        assert cli_checkout['with']['persist-credentials'] is False, name

        checkout = simulator_checkout(simulator_job)
        assert checkout['with']['persist-credentials'] is False, name
        assert checkout['with']['ssh-key'] == '${{ secrets.SIMULATOR_DEPLOY_KEY }}', name
        assert 'token' not in checkout['with'], name

        node_setup = next(
            step
            for step in simulator_job['steps']
            if str(step.get('uses', '')).startswith('actions/setup-node@')
        )
        assert node_setup['with']['node-version'] == 22, name

        remaining_steps = simulator_job['steps'][simulator_job['steps'].index(checkout) + 1 :]
        assert 'SIMULATOR_DEPLOY_KEY' not in str(remaining_steps), name
        assert 'secrets.' not in str(remaining_steps), name


def test_the_deploy_key_check_never_passes_the_key_into_a_shell():
    workflow = load_workflow()
    for name, job in simulator_jobs(workflow).items():
        # Only a boolean derived from the secret reaches the job environment.
        assert job['env']['HAS_SIMULATOR_DEPLOY_KEY'] == "${{ secrets.SIMULATOR_DEPLOY_KEY != '' }}"
        key_check = step_named(job, KEY_CHECK_STEP)
        assert job['steps'].index(key_check) == 0, name
        assert key_check['if'] == "${{ env.HAS_SIMULATOR_DEPLOY_KEY != 'true' }}", name
        assert 'secrets.' not in str(key_check), name
        assert '$SIMULATOR_DEPLOY_KEY' not in key_check['run'], name
        assert 'dependabot' not in str(job.get('if', '')).lower(), name


@pytest.mark.skipif(not POSIX, reason='workflow step uses Bash')
@pytest.mark.parametrize(
    ('actor', 'expected'),
    [
        ('dependabot[bot]', DEPENDABOT_MESSAGE),
        ('maintainer', 'SIMULATOR_DEPLOY_KEY is required because Simulator testing is mandatory'),
    ],
)
def test_a_missing_deploy_key_fails_with_an_actionable_message(actor, expected):
    workflow = load_workflow()
    scripts = {step_named(job, KEY_CHECK_STEP)['run'] for job in simulator_jobs(workflow).values()}
    assert len(scripts) == 1
    envs = [step_named(job, KEY_CHECK_STEP)['env'] for job in simulator_jobs(workflow).values()]
    assert all(env == {'ACTOR': '${{ github.actor }}'} for env in envs)

    result = subprocess.run(
        ['bash', '-eu', '-c', scripts.pop()],
        env={**os.environ, 'ACTOR': actor},
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 1
    assert expected in result.stdout
    if actor != 'dependabot[bot]':
        assert 'Dependabot' not in result.stdout


def simulator_command(job, step_name='Test against Simulator'):
    step = step_named(job, step_name)
    assert step['env']['B2_TEST_APPLICATION_KEY_ID'] == 'sim-test-id'
    assert step['env']['B2_TEST_APPLICATION_KEY'] == 'sim-test-secret'
    assert step['env']['B2_TEST_SHORT_TIMEOUTS'] == '1'
    lines = [line.strip() for line in step['run'].strip().splitlines()]
    assert len(lines) == 1, step['run']
    prefix = 'bash scripts/with-b2-simulator.sh ../simulator '
    assert lines[0].startswith(prefix), lines[0]
    assert step['working-directory'] == 'cli'
    command = lines[0][len(prefix) :]
    assert '--cleanup' not in command
    assert 'cleanup_buckets' not in command
    return step, command


def test_simulator_runs_the_secret_marked_suite_with_fixture_credentials():
    workflow = load_workflow()
    _, command = simulator_command(workflow['jobs']['simulator-integration'])
    assert (
        command
        == 'uv run nox -vs integration -p "$PYTHON_DEFAULT_VERSION" -- -n 0 -m "require_secrets"'
    )


def test_simulator_covers_the_python_versions_that_used_to_run_live_in_pull_requests():
    workflow = load_workflow()
    job = workflow['jobs']['simulator-python-versions']
    assert job['needs'] == 'simulator-access-policy'
    assert job['runs-on'] == 'ubuntu-latest'
    assert job['strategy']['fail-fast'] is False
    assert set(job['strategy']['matrix']['python-version']) == {'3.10', '3.14', 'pypy3.10'}
    setup_python = next(
        step
        for step in job['steps']
        if str(step.get('uses', '')).startswith('actions/setup-python@')
    )
    assert setup_python['with']['python-version'] == '${{ matrix.python-version }}'
    step, command = simulator_command(job)
    assert step['env']['PYTHON_VERSION'] == '${{ matrix.python-version }}'
    assert command == 'uv run nox -vs integration -p "$PYTHON_VERSION" -- -n 0 -m "require_secrets"'


def test_simulator_tests_the_docker_image_over_the_host_network():
    workflow = load_workflow()
    job = workflow['jobs']['simulator-docker']
    assert job['needs'] == 'simulator-access-policy'
    assert job['runs-on'] == 'ubuntu-latest'
    build = next(
        step
        for step in job['steps']
        if str(step.get('uses', '')).startswith('docker/build-push-action@')
    )
    assert build['with']['context'] == 'cli'
    assert build['with']['load'] is True
    assert build['with']['tags'] == 'backblazeit/b2:test'
    assert 'push' not in build['with']
    step, command = simulator_command(job, 'Test the Docker image against Simulator')
    # The container reaches the loopback-only Simulator through the host network and reads the
    # trust bundle from the /tmp mount that docker_test shares with it.
    assert step['env']['B2_TEST_DOCKER_NETWORK'] == 'host'
    assert step['env']['B2_SIMULATOR_CA_BUNDLE'].startswith('/tmp/')
    # Like the live run this replaces, the image gets the whole integration suite.
    assert command == 'uv run nox -vs docker_test -- backblazeit/b2:test -n 0'


@pytest.mark.parametrize(
    ('job_name', 'runners', 'python'),
    [
        ('simulator-linux-bundle', {'ubuntu-22.04', 'ubuntu-22.04-arm'}, '3.10'),
        ('simulator-windows-bundle', {'windows-2022'}, None),
    ],
)
def test_simulator_tests_the_standalone_bundles(job_name, runners, python):
    workflow = load_workflow()
    job = workflow['jobs'][job_name]
    assert job['needs'] == 'simulator-access-policy'
    runs_on = job['runs-on']
    if runs_on == '${{ matrix.os }}':
        assert set(job['strategy']['matrix']['os']) == runners
    else:
        assert {runs_on} == runners
    if python:
        assert job['env']['PYTHON_DEFAULT_VERSION'] == python
    bundle = step_named(job, 'Bundle the distribution')
    assert bundle['id'] == 'bundle'
    assert bundle['run'] == 'uv run nox -vs bundle'
    step, command = simulator_command(job)
    assert step.get('shell') == 'bash'
    assert step['env']['SUT'] == '${{ steps.bundle.outputs.sut_path }}'
    assert command == (
        'uv run nox -vs integration -p "$PYTHON_DEFAULT_VERSION" -- --sut="$SUT" -n 0 '
        '-m "require_secrets"'
    )


def test_simulator_covers_the_macos_only_tqdm_regression():
    workflow = load_workflow()
    job = workflow['jobs']['simulator-macos-regression']

    assert job['runs-on'] == 'macos-latest'
    assert job['needs'] == 'simulator-integration'
    _, command = simulator_command(job)
    assert command == (
        'uv run nox -vs integration -p "$PYTHON_DEFAULT_VERSION" -- -n 0 -m "require_secrets" '
        '-k "tqdm_closer"'
    )


def test_the_simulator_helper_starts_the_pinned_server_and_trusts_only_its_loopback_cert():
    script = HELPER_PATH.read_text()
    assert 'bin/simulator/serve.mjs' in script
    assert '--custom-upload-timestamps' in script
    assert 'SIMULATOR-LISTENING' in script
    assert 'https://127\\.0\\.0\\.1' in script
    assert 'bin/simulator/loopback-cert.pem' in script
    assert 'certifi.where()' in script
    for name in ('REQUESTS_CA_BUNDLE', 'SSL_CERT_FILE', 'B2_ENVIRONMENT', 'B2_TEST_ENVIRONMENT'):
        assert f'export {name}' in script


@pytest.fixture
def fake_simulator(tmp_path):
    simulator = tmp_path / 'simulator'
    (simulator / 'bin/simulator').mkdir(parents=True)
    (simulator / 'bin/simulator/serve.mjs').write_text('// replaced by the fake node\n')
    (simulator / 'bin/simulator/loopback-cert.pem').write_text('LOOPBACK CERT\n')
    certifi_bundle = tmp_path / 'certifi.pem'
    certifi_bundle.write_text('PUBLIC ROOTS\n')

    fake_bin = tmp_path / 'bin'
    fake_bin.mkdir()
    node = fake_bin / 'node'
    node.write_text(
        f'#!{sys.executable}\n'
        'import os, pathlib, sys, time\n'
        'state = pathlib.Path(os.environ["FAKE_STATE"])\n'
        'state.joinpath("node.pid").write_text(str(os.getpid()))\n'
        'state.joinpath("node.args").write_text("\\n".join(sys.argv[1:]))\n'
        'if os.environ.get("FAKE_NODE_CRASH"):\n'
        '    print("simulator crashed on start", flush=True)\n'
        '    sys.exit(7)\n'
        'print("SIMULATOR-LISTENING http://127.0.0.1:41000", flush=True)\n'
        'print("SIMULATOR-LISTENING https://127.0.0.1:41001\\r", flush=True)\n'
        'time.sleep(60)\n'
    )
    uv = fake_bin / 'uv'
    uv.write_text(f'#!/bin/sh\necho "{certifi_bundle}"\n')
    for executable in (node, uv):
        executable.chmod(0o755)
    state = tmp_path / 'state'
    state.mkdir()
    temp = tmp_path / 'runner-temp'
    temp.mkdir()
    env = {
        **os.environ,
        'PATH': f'{fake_bin}{os.pathsep}{os.environ["PATH"]}',
        'FAKE_STATE': str(state),
        'RUNNER_TEMP': str(temp),
    }
    env.pop('B2_SIMULATOR_CA_BUNDLE', None)
    return simulator, state, temp, env


def run_helper(simulator, env, command):
    return subprocess.run(
        ['bash', str(HELPER_PATH), str(simulator), 'bash', '-c', command],
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )


def process_is_gone(pid):
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return True
        time.sleep(0.05)
    return False


@pytest.mark.skipif(not POSIX, reason='helper uses Bash')
def test_the_simulator_helper_runs_the_command_against_the_https_listener(fake_simulator):
    simulator, state, temp, env = fake_simulator
    command = textwrap.dedent(
        """
        printf '%s\\n' "$B2_ENVIRONMENT" "$B2_TEST_ENVIRONMENT" "$REQUESTS_CA_BUNDLE" \
            "$SSL_CERT_FILE" >"$FAKE_STATE/env"
        exit 3
        """
    )
    result = run_helper(simulator, env, command)

    assert result.returncode == 3, result.stdout + result.stderr  # the command's status wins
    realm, test_realm, ca_bundle, ssl_cert_file = (state / 'env').read_text().splitlines()
    assert realm == test_realm == 'https://127.0.0.1:41001'
    assert ca_bundle == ssl_cert_file == str(temp / 'b2-simulator-ca.pem')
    assert Path(ca_bundle).read_text() == 'PUBLIC ROOTS\nLOOPBACK CERT\n'
    assert (state / 'node.args').read_text().splitlines() == [
        f'{simulator}/bin/simulator/serve.mjs',
        '--custom-upload-timestamps',
    ]
    assert process_is_gone(int((state / 'node.pid').read_text()))


@pytest.mark.skipif(not POSIX, reason='helper uses Bash')
def test_the_simulator_helper_writes_the_trust_bundle_where_it_is_told(fake_simulator, tmp_path):
    simulator, state, _, env = fake_simulator
    shared = tmp_path / 'shared' / 'ca.pem'
    shared.parent.mkdir()
    env['B2_SIMULATOR_CA_BUNDLE'] = str(shared)
    result = run_helper(simulator, env, 'printf %s "$REQUESTS_CA_BUNDLE" >"$FAKE_STATE/env"')

    assert result.returncode == 0, result.stdout + result.stderr
    assert (state / 'env').read_text() == str(shared)
    assert shared.read_text() == 'PUBLIC ROOTS\nLOOPBACK CERT\n'


@pytest.mark.skipif(not POSIX, reason='helper uses Bash')
def test_the_simulator_helper_fails_without_running_the_command_if_the_server_dies(
    fake_simulator,
):
    simulator, state, _, env = fake_simulator
    env['FAKE_NODE_CRASH'] = '1'
    result = run_helper(simulator, env, 'touch "$FAKE_STATE/ran"')

    assert result.returncode != 0
    assert 'simulator crashed on start' in result.stdout + result.stderr
    assert not (state / 'ran').exists()


@pytest.mark.skipif(not POSIX, reason='helper uses Bash')
def test_the_simulator_helper_accepts_a_windows_line_ending_from_python(fake_simulator, tmp_path):
    # Windows Python prints "\r\n"; Git Bash may keep the "\r" in a command substitution.
    simulator, state, temp, env = fake_simulator
    uv = Path(env['PATH'].split(os.pathsep)[0]) / 'uv'
    uv.write_text(f'#!/bin/sh\nprintf "%s\\r\\n" "{tmp_path / "certifi.pem"}"\n')
    result = run_helper(simulator, env, 'printf %s "$REQUESTS_CA_BUNDLE" >"$FAKE_STATE/env"')

    assert result.returncode == 0, result.stdout + result.stderr
    assert Path((state / 'env').read_text()).read_text() == 'PUBLIC ROOTS\nLOOPBACK CERT\n'
