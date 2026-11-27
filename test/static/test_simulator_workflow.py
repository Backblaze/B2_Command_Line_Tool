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
import subprocess
from pathlib import Path

import yaml

WORKFLOW_PATH = Path(__file__).parents[2] / '.github/workflows/simulator-integration.yml'
SIMULATOR_REPOSITORY = 'backblaze-labs/b2-simulator'
SIMULATOR_REVISION = '529721037901412b7b24fdc18498631d298295c5'


def load_workflow():
    return yaml.safe_load(WORKFLOW_PATH.read_text())


def triggers(workflow):
    # PyYAML 1.1 treats the unquoted key ``on`` as boolean True.
    return workflow.get('on', workflow.get(True))


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


def test_private_simulator_is_exactly_pinned_and_uses_a_read_only_deploy_key():
    workflow = load_workflow()
    assert workflow['permissions'] == {'contents': 'read'}
    assert 'continue-on-error' not in str(workflow)
    job = workflow['jobs']['simulator-integration']
    assert job['needs'] == 'simulator-access-policy'
    assert 'if' not in job  # a skipped job would satisfy a required check
    assert job['timeout-minutes'] == 90

    cli_checkout = next(step for step in job['steps'] if step.get('name') == 'Check out the CLI')
    assert cli_checkout['with']['persist-credentials'] is False

    checkout = next(
        step
        for step in job['steps']
        if step.get('with', {}).get('repository') == SIMULATOR_REPOSITORY
    )
    assert workflow['env']['SIMULATOR_REVISION'] == SIMULATOR_REVISION
    assert checkout['with']['ref'] == workflow['env']['SIMULATOR_REVISION']
    assert checkout['with']['persist-credentials'] is False
    assert checkout['with']['ssh-key'] == '${{ secrets.SIMULATOR_DEPLOY_KEY }}'
    assert 'token' not in checkout['with']

    node_setup = next(
        step for step in job['steps'] if str(step.get('uses', '')).startswith('actions/setup-node@')
    )
    assert node_setup['with']['node-version'] == 22

    remaining_steps = job['steps'][job['steps'].index(checkout) + 1 :]
    assert 'SIMULATOR_DEPLOY_KEY' not in str(remaining_steps)
    assert 'secrets.' not in str(remaining_steps)


def test_simulator_runs_the_secret_marked_suite_with_fixture_credentials():
    workflow = load_workflow()
    job = workflow['jobs']['simulator-integration']
    test_step = next(step for step in job['steps'] if step.get('name') == 'Test against Simulator')

    assert test_step['env']['B2_TEST_APPLICATION_KEY_ID'] == 'sim-test-id'
    assert test_step['env']['B2_TEST_APPLICATION_KEY'] == 'sim-test-secret'
    script = test_step['run']
    assert 'bin/simulator/serve.mjs' in script
    assert '--custom-upload-timestamps' in script
    assert 'SIMULATOR-LISTENING' in script
    assert 'https://127\\.0\\.0\\.1' in script
    assert 'loopback-cert.pem' in script
    assert 'certifi.where()' in script
    assert 'REQUESTS_CA_BUNDLE' in script
    assert 'B2_ENVIRONMENT' in script
    assert 'B2_TEST_ENVIRONMENT' in script
    assert '-n 0' in script
    assert '-m "require_secrets"' in script
    nox_command = next(
        line.strip() for line in script.splitlines() if 'nox -vs integration' in line
    )
    assert nox_command == (
        'uv run nox -vs integration -p "$PYTHON_DEFAULT_VERSION" -- -n 0 -m "require_secrets"'
    )
    assert '--cleanup' not in script
    assert 'cleanup_buckets' not in script
