######################################################################
#
# File: test/unit/test_release_policy.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
"""Check the release workflow's publication and review policy."""

import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]


def test_pypi_uses_pinned_trusted_publishing_with_attestations():
    jobs = yaml.safe_load((ROOT / '.github/workflows/cd.yml').read_text())['jobs']
    publisher = jobs['deploy-pypi']
    assert publisher['permissions']['id-token'] == 'write'
    assert publisher['environment']['name'] == 'pypi'
    assert jobs['deploy']['needs'] == 'release-gate'
    assert publisher['needs'] == 'deploy'
    step = next(step for step in publisher['steps'] if 'pypi-publish@' in step.get('uses', ''))
    assert re.fullmatch(r'pypa/gh-action-pypi-publish@[0-9a-f]{40}', step['uses'])
    assert step['with']['attestations'] is True
    assert 'password' not in step['with']


def test_ci_covers_master_and_the_explicit_release_stack_only():
    workflow = yaml.safe_load((ROOT / '.github/workflows/ci.yml').read_text())
    events = workflow.get('on', workflow.get(True))
    assert events['pull_request']['branches'] == ['master', 'release-standard-changie-oidc']


def test_release_signing_principal_is_not_a_personal_identity():
    lines = (ROOT / 'release-keys/maintainers.allowed_signers').read_text().splitlines()
    principals = [line.split()[0] for line in lines if line.strip() and not line.startswith('#')]
    assert principals == ['backblaze-release']
