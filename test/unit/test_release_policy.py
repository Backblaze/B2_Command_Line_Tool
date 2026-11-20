######################################################################
#
# File: test/unit/test_release_policy.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
"""Check release workflow contracts without adding a test-only YAML dependency."""

import os
import re
import subprocess
import textwrap
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


def job(name, workflow='cd.yml'):
    """Select a top-level job from the repository's workflow source."""
    source = (ROOT / '.github/workflows' / workflow).read_text()
    match = re.search(rf'^  {re.escape(name)}:\n.*?(?=^  [\w-]+:|\Z)', source, re.M | re.S)
    assert match is not None, f'missing job: {name}'
    return match.group()


def test_pypi_uses_pinned_trusted_publishing_with_attestations():
    publisher = job('deploy-pypi')
    assert re.search(r'^      id-token: write\s*$', publisher, re.M)
    assert re.search(r'^      name: pypi\s*$', publisher, re.M)
    assert re.search(r'^    needs: release-gate\s*$', job('deploy'), re.M)
    assert re.search(r'^    needs: deploy\s*$', publisher, re.M)
    assert "if: ${{ needs.deploy.outputs.prerelease == 'false' }}" in publisher
    assert re.search(
        r'^        uses: pypa/gh-action-pypi-publish@[0-9a-f]{40}(?:\s+#.*)?$', publisher, re.M
    )
    assert re.search(r'^          attestations: true\s*$', publisher, re.M)
    assert not re.search(r'^          password:', publisher, re.M)


def test_advisory_token_is_forwarded_by_the_release_gate_job():
    gate = job('release-gate')
    assert re.search(
        r'^          RELEASE_SECURITY_TOKEN: \$\{\{ secrets\.RELEASE_SECURITY_TOKEN }}\s*$',
        gate,
        re.M,
    )
    assert re.search(r'^          GH_TOKEN: \$\{\{ github\.token }}\s*$', gate, re.M)
    assert re.search(
        r'^          RELEASE_ALERTS_TOKEN: \$\{\{ secrets\.RELEASE_ALERTS_TOKEN }}\s*$', gate, re.M
    )
    assert 'run: scripts/release-security-gate.sh' in gate


def test_all_publication_jobs_wait_for_the_release_gate():
    assert 'run: scripts/verify-release-tag.sh "$GITHUB_REF_NAME"' in job('release-gate')
    assert re.search(r'^    needs: release-gate\s*$', job('deploy'), re.M)
    for name in ('deploy-pypi', 'deploy-linux-bundle', 'deploy-windows-bundle', 'deploy-docker'):
        assert re.search(r'^    needs: deploy\s*$', job(name), re.M), name


def test_bundle_signing_and_docker_publication_settings_are_preserved():
    windows = job('deploy-windows-bundle')
    assert re.search(r'uses: digicert/ssm-code-signing@[0-9a-f]{40}', windows)
    assert (
        "uv run nox -vs sign -- '${{ secrets.SM_KEYPAIR_ALIAS }}' '${{ secrets.SM_CERT_FINGERPRINT }}'"
        in windows
    )
    docker = job('deploy-docker')
    assert "if: ${{ needs.deploy.outputs.publish_docker == 'true' }}" in docker
    for setting in (
        'context: .',
        'pull: true',
        'no-cache: true',
        'push: true',
        'platforms: linux/amd64,linux/arm64',
    ):
        assert f'          {setting}\n' in docker
    assert 'backblazeit/b2:' in docker
    assert 'run: uv run nox -vs sign' in job('deploy-linux-bundle')


def test_workflow_actions_are_pinned():
    for name in ('ci.yml', 'cd.yml'):
        source = (ROOT / '.github/workflows' / name).read_text()
        actions = re.findall(r'^\s+(?:- )?uses: (\S+)', source, re.M)
        assert actions
        assert all(re.fullmatch(r'[^@]+@[0-9a-f]{40}', action) for action in actions)


def test_release_signing_principal_is_not_a_personal_identity():
    lines = (ROOT / 'release-keys/maintainers.allowed_signers').read_text().splitlines()
    principals = [line.split()[0] for line in lines if line.strip() and not line.startswith('#')]
    assert principals == ['backblaze-release']


def test_pull_request_ci_covers_stacked_bases():
    source = (ROOT / '.github/workflows/ci.yml').read_text()
    trigger = source.split('on:\n', 1)[1].split('\nenv:', 1)[0]
    assert re.search(r'^  pull_request:\s*(?=^\S|\Z)', trigger, re.M)
    assert re.search(r'^  push:\n    branches: \[master\]', trigger, re.M)


@pytest.mark.skipif(os.name != 'posix', reason='workflow uses Bash')
@pytest.mark.parametrize('username', ['test-placeholder', ''])
@pytest.mark.parametrize(
    ('tag', 'prerelease'),
    [
        ('v5.1.0', 'false'),
        ('v5.1.0rc1', 'true'),
        ('v5.1.0a1', 'true'),
        ('v5.1.0b1', 'true'),
        ('v5.1.0dev1', 'true'),
    ],
)
def test_prerelease_step_classifies_tags_before_publication(tmp_path, tag, prerelease, username):
    source = job('deploy').split('      - name: Determine if pre-release', 1)[1]
    script = textwrap.dedent(source.split('        run: |\n', 1)[1].split('      - uses:', 1)[0])
    output = tmp_path / 'outputs'
    result = subprocess.run(
        ['bash', '-c', script],
        env=dict(
            os.environ,
            GITHUB_REF_NAME=tag,
            GITHUB_OUTPUT=str(output),
            DOCKERHUB_USERNAME=username,
        ),
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
    actual = dict(line.split('=', 1) for line in output.read_text().splitlines())
    assert actual['prerelease'] == prerelease
    assert actual['publish_docker'] == ('true' if prerelease == 'false' and username else 'false')


@pytest.mark.skipif(os.name != 'posix', reason='workflow uses Bash')
@pytest.mark.parametrize(
    ('prerelease', 'version', 'tags'),
    [
        ('false', '5.1.0', 'backblazeit/b2:5.1.0,backblazeit/b2:latest'),
        ('true', '5.1.0rc1', 'backblazeit/b2:5.1.0rc1'),
    ],
)
def test_docker_tags_do_not_mark_prereleases_as_latest(tmp_path, prerelease, version, tags):
    source = job('deploy-docker').split('      - name: Prepare Docker tags', 1)[1]
    script = textwrap.dedent(source.split('        run: |\n', 1)[1].split('      - name:', 1)[0])
    output = tmp_path / 'outputs'
    result = subprocess.run(
        ['bash', '-c', script],
        env=dict(os.environ, GITHUB_OUTPUT=str(output), VERSION=version, PRERELEASE=prerelease),
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
    assert output.read_text() == f'docker_tags={tags}\n'


def test_replaced_actions_stay_replaced_and_docker_build_pins_match():
    pins = []
    for workflow in ('ci.yml', 'cd.yml'):
        source = (ROOT / '.github/workflows' / workflow).read_text()
        actions = re.findall(r'^\s+(?:- )?uses: (\S+)', source, re.M)
        for name in (
            'softprops/action-gh-release',
            'mindsers/changelog-reader-action',
            'codespell-project/actions-codespell',
            'peter-evans/dockerhub-description',
            'deadsnakes/action',
        ):
            assert not any(action.startswith(name + '@') for action in actions)
        docker = [action for action in actions if action.startswith('docker/build-push-action@')]
        assert len(docker) == 1
        pins.append(docker[0])
    assert pins[0] == pins[1]
