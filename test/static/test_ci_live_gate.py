######################################################################
#
# File: test/static/test_ci_live_gate.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
"""Check the live B2 opt-in and offline job dependencies in the CI workflow."""

from __future__ import annotations

import ast
import copy
import re
from pathlib import Path

import pytest
import yaml

WORKFLOW = yaml.safe_load((Path(__file__).parents[2] / '.github/workflows/ci.yml').read_text())
JOBS = WORKFLOW['jobs']
KEYS = ('B2_TEST_APPLICATION_KEY', 'B2_TEST_APPLICATION_KEY_ID')
CONSUMERS = ('test', 'test-docker', 'test-linux-bundle', 'test-windows-bundle')


def expression(value, context):
    """Evaluate the workflow expression subset used by these gates; reject unknown syntax."""
    if not isinstance(value, str) or not value.startswith('${{'):
        return value
    code = value[3:-2].strip().replace('cancelled()', repr(context['cancelled']))
    matrix_guard = 'contains(fromJSON(\'["3.10", "pypy3.10", "3.14"]\'), matrix.python-version)'
    code = code.replace(
        matrix_guard, repr(context['matrix.python-version'] in ('3.10', 'pypy3.10', '3.14'))
    )
    code = re.sub(
        r'[a-zA-Z_][\w-]*(?:\.[a-zA-Z_][\w-]*)+',
        lambda match: repr(context[match.group()]),
        code,
    )
    code = code.replace('&&', ' and ').replace('||', ' or ')
    code = re.sub(r'!(?!=)', 'not ', code)
    code = re.sub(r"(?<!['\"])\btrue\b(?!['\"])", 'True', code)
    code = re.sub(r"(?<!['\"])\bfalse\b(?!['\"])", 'False', code)
    tree = ast.parse(code.strip(), mode='eval')

    def resolve(node):
        if isinstance(node, ast.Constant) and isinstance(node.value, (str, bool)):
            return node.value
        if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.Not):
            return not resolve(node.operand)
        if isinstance(node, ast.BoolOp):
            result = resolve(node.values[0])
            for child in node.values[1:]:
                if isinstance(node.op, ast.And) and not result:
                    return result
                if isinstance(node.op, ast.Or) and result:
                    return result
                result = resolve(child)
            return result
        if isinstance(node, ast.Compare):
            left = resolve(node.left)
            for operator, child in zip(node.ops, node.comparators):
                right = resolve(child)
                assert isinstance(operator, (ast.Eq, ast.NotEq)), code
                if (left == right) != isinstance(operator, ast.Eq):
                    return False
                left = right
            return True
        raise AssertionError(f'Unsupported workflow expression: {code}')

    return resolve(tree.body)


def context(enabled='', event='push', draft=False, credentials=True):
    return {
        'vars.B2_LIVE_TESTS_ENABLED': enabled,
        'github.event_name': event,
        'github.event.pull_request.draft': draft,
        'secrets.B2_TEST_APPLICATION_KEY': 'example-key' if credentials else '',
        'secrets.B2_TEST_APPLICATION_KEY_ID': 'example-id' if credentials else '',
        'needs.lint.result': 'success',
        'needs.cleanup_buckets.result': 'skipped',
        'matrix.python-version': '3.14',
        'cancelled': False,
    }


def live_steps(job):
    return [
        step
        for step in job['steps']
        if 'require_secrets' in step.get('run', '')
        and 'not require_secrets' not in step.get('run', '')
        or 'nox -vs docker_test' in step.get('run', '')
        or 'nox -vs cleanup_buckets' in step.get('run', '')
    ]


def assert_credential_placement(workflow):
    remaining = copy.deepcopy(workflow)
    for name in ('cleanup_buckets', *CONSUMERS):
        for key in KEYS:
            value = remaining['jobs'][name]['env'].pop(key)
            assert f'secrets.{key}' in value
    assert not re.search(r'secrets\.B2_TEST_APPLICATION_KEY', str(remaining), re.IGNORECASE)


@pytest.mark.parametrize('placement', ['workflow_env', 'step_env', 'step_with', 'other_job'])
def test_credential_scan_rejects_ungated_references(placement):
    workflow = copy.deepcopy(WORKFLOW)
    reference = '${{ secrets.B2_TEST_APPLICATION_KEY }}'
    if placement == 'workflow_env':
        workflow['env']['UNEXPECTED'] = reference
    elif placement == 'step_env':
        workflow['jobs']['test']['steps'][0]['env'] = {'UNEXPECTED': reference}
    elif placement == 'step_with':
        workflow['jobs']['test']['steps'][0]['with']['UNEXPECTED'] = reference
    else:
        workflow['jobs']['doc']['steps'][0]['env'] = {'UNEXPECTED': reference}
    with pytest.raises(AssertionError):
        assert_credential_placement(workflow)


@pytest.mark.parametrize(
    'enabled,event,draft,credentials,allowed',
    [
        ('', 'push', False, True, False),
        ('false', 'push', False, True, False),
        ('true', 'pull_request', True, True, False),
        ('true', 'pull_request', False, True, True),
        ('true', 'push', False, True, True),
        ('true', 'push', False, False, False),
    ],
)
def test_live_gate_and_credentials(enabled, event, draft, credentials, allowed):
    values = context(enabled, event, draft, credentials)
    gate_enabled = enabled == 'true' and (event != 'pull_request' or not draft)
    assert (
        bool(expression(JOBS['cleanup_buckets'].get('if', '${{ true }}'), values)) == gate_enabled
    )
    assert_credential_placement(WORKFLOW)
    for name in ('cleanup_buckets', *CONSUMERS):
        job = JOBS[name]
        job_values = dict(values)
        for key in KEYS:
            actual = expression(job['env'][key], values)
            assert actual == (values[f'secrets.{key}'] if gate_enabled else '')
            job_values[f'env.{key}'] = actual
        steps = live_steps(job)
        assert len(steps) == 1, name
        assert bool(expression(steps[0].get('if', '${{ true }}'), job_values)) == allowed
        if not gate_enabled:
            # Check the step gate independently of the credential masking above.
            job_values.update({f'env.{key}': 'example' for key in KEYS})
            assert not expression(steps[0].get('if', '${{ true }}'), job_values)


@pytest.mark.parametrize('name', ['cleanup_buckets', *CONSUMERS])
@pytest.mark.parametrize('missing_key', KEYS)
def test_live_steps_require_both_credentials(name, missing_key):
    values = context(enabled='true')
    values.update({f'env.{key}': 'example' for key in KEYS})
    values[f'env.{missing_key}'] = ''
    (step,) = live_steps(JOBS[name])
    assert not expression(step.get('if', '${{ true }}'), values)


def assert_live_matrix_filter(step):
    values = context(enabled='true')
    values.update({f'env.{key}': 'example' for key in KEYS})
    for version in ('3.10', '3.11', '3.12', '3.13', '3.14', 'pypy3.10'):
        values['matrix.python-version'] = version
        assert bool(expression(step['if'], values)) == (version in ('3.10', '3.14', 'pypy3.10'))


def test_live_matrix_filter_rejects_removed_guard():
    (step,) = live_steps(JOBS['test'])
    mutant = copy.deepcopy(step)
    guard = ' && contains(fromJSON(\'["3.10", "pypy3.10", "3.14"]\'), matrix.python-version)'
    assert guard in mutant['if']
    mutant['if'] = mutant['if'].replace(guard, '')
    with pytest.raises(AssertionError):
        assert_live_matrix_filter(mutant)


def test_live_matrix_filter_keeps_edge_versions():
    (step,) = live_steps(JOBS['test'])
    assert_live_matrix_filter(step)


@pytest.mark.parametrize('name', CONSUMERS)
@pytest.mark.parametrize(
    'lint,cleanup,cancelled,allowed',
    [
        ('success', 'skipped', False, True),
        ('success', 'success', False, True),
        ('failure', 'skipped', False, False),
        ('success', 'failure', False, False),
        ('success', 'cancelled', False, False),
        ('success', 'skipped', True, False),
    ],
)
def test_offline_jobs_survive_skipped_cleanup(name, lint, cleanup, cancelled, allowed):
    job = JOBS[name]
    assert set(job['needs']) == {'lint', 'cleanup_buckets'}
    values = context()
    values.update(
        {'needs.lint.result': lint, 'needs.cleanup_buckets.result': cleanup, 'cancelled': cancelled}
    )
    assert bool(expression(job['if'], values)) == allowed


def test_offline_steps_are_unconditional_and_draft_trigger_is_present():
    triggers = WORKFLOW.get('on', WORKFLOW.get(True))
    assert set(triggers['pull_request']['types']) == {
        'opened',
        'synchronize',
        'reopened',
        'ready_for_review',
    }
    for name in ('test', 'test-linux-bundle', 'test-windows-bundle'):
        steps = JOBS[name]['steps']
        offline = [step for step in steps if 'not require_secrets' in step.get('run', '')]
        assert len(offline) == 1
        assert 'if' not in offline[0]
    assert 'if' not in next(
        step for step in JOBS['test']['steps'] if step.get('name') == 'Run unit tests'
    )
