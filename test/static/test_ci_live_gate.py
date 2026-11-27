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

import re
from pathlib import Path

import yaml

ROOT = Path(__file__).parents[2]
CI_TEXT = (ROOT / '.github/workflows/ci.yml').read_text()
LIVE_PATH = ROOT / '.github/workflows/live-b2.yml'
KEYS = ('B2_TEST_APPLICATION_KEY', 'B2_TEST_APPLICATION_KEY_ID')


def triggers(workflow):
    """Return workflow triggers despite PyYAML treating ``on`` as YAML 1.1 boolean."""
    return workflow.get('on', workflow.get(True))


def test_pull_request_ci_never_references_real_b2_credentials():
    assert 'B2_LIVE_TESTS_ENABLED' not in CI_TEXT
    for key in KEYS:
        assert not re.search(rf'secrets\.{key}\b', CI_TEXT)


def test_live_b2_workflow_is_weekly_and_manual_only():
    workflow = yaml.safe_load(LIVE_PATH.read_text())
    event_names = set(triggers(workflow))
    assert event_names == {'schedule', 'workflow_dispatch'}
    assert triggers(workflow)['schedule']


def test_weekly_live_b2_run_fails_closed_and_runs_secret_tests():
    text = LIVE_PATH.read_text()
    workflow = yaml.safe_load(text)
    assert all(f'secrets.{key}' in text for key in KEYS)
    job = workflow['jobs']['live-b2-integration']
    scripts = '\n'.join(step.get('run', '') for step in job['steps'])
    assert 'B2_TEST_APPLICATION_KEY' in scripts
    assert 'B2_TEST_APPLICATION_KEY_ID' in scripts
    assert '-m "require_secrets"' in scripts
    assert '--cleanup' in scripts
    assert 'exit 1' in scripts
