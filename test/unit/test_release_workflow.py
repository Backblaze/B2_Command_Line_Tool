######################################################################
#
# File: test/unit/test_release_workflow.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
"""Release gate and signed-tag checks run without publishing artifacts."""

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
POSIX_TOOLS = os.name == 'posix' and all(
    shutil.which(tool) for tool in ('bash', 'git', 'ssh-keygen')
)


@pytest.mark.skipif(not POSIX_TOOLS, reason='release scripts require POSIX tools')
@pytest.mark.parametrize(
    ('alerts', 'fixes', 'expected_status'),
    [
        ({'dependabot': [{'security_advisory': {'severity': 'low'}}]}, '', 0),
        ({'dependabot': [{'security_advisory': {'severity': 'medium'}}]}, '', 1),
        ({'code-scanning': [{'rule': {'security_severity_level': 'high'}}]}, '', 1),
        ({'secret-scanning': [{'state': 'open'}]}, '', 1),
        ({'security-advisories': [{'state': 'draft', 'severity': 'critical'}]}, '', 1),
        (
            {
                'security-advisories': [
                    {'state': 'draft', 'severity': 'critical', 'ghsa_id': 'GHSA-xxxx-xxxx-xxxx'}
                ]
            },
            'GHSA-xxxx-xxxx-xxxx',
            0,
        ),
        (
            {
                'security-advisories': [
                    {'state': 'triage', 'severity': 'critical', 'ghsa_id': 'GHSA-xxxx-xxxx-xxxx'}
                ]
            },
            'GHSA-xxxx-xxxx-xxxx',
            1,
        ),
    ],
)
def test_release_security_gate_filters_open_alerts(tmp_path, alerts, fixes, expected_status):
    fixtures = tmp_path / 'alerts'
    fixtures.mkdir()
    for source in ('dependabot', 'code-scanning', 'secret-scanning', 'security-advisories'):
        (fixtures / f'{source}.json').write_text(json.dumps(alerts.get(source, [])))

    fake_bin = tmp_path / 'bin'
    fake_bin.mkdir()
    fake_gh = fake_bin / 'gh'
    fake_gh.write_text(
        f'#!{sys.executable}\n'
        'import json, os, pathlib, sys\n'
        'url = next(arg for arg in sys.argv if arg.startswith("repos/"))\n'
        'source = url.split("/")[3].split("?")[0]\n'
        'if source == os.environ.get("FAKE_GH_UNREADABLE"): sys.exit(1)\n'
        'path = pathlib.Path(os.environ["RELEASE_GATE_FIXTURES"]) / (source + ".json")\n'
        'for alert in json.loads(path.read_text()): print(json.dumps(alert))\n'
    )
    fake_gh.chmod(0o755)
    env = dict(os.environ)
    env.update(
        GITHUB_REPOSITORY='Backblaze/B2_Command_Line_Tool',
        RELEASE_GATE_FIXTURES=str(fixtures),
        GITHUB_ACTIONS='true',
        RELEASE_SECURITY_TOKEN='test-placeholder',
        SECURITY_GATE_STRICT='false',
        RELEASE_FIXES_ADVISORIES=fixes,
        PATH=f'{fake_bin}{os.pathsep}{env["PATH"]}',
    )
    result = subprocess.run(
        ['bash', str(ROOT / 'scripts/release-security-gate.sh')],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == expected_status, result.stdout + result.stderr


@pytest.mark.skipif(not POSIX_TOOLS, reason='release scripts require POSIX tools')
@pytest.mark.parametrize(('strict', 'expected_status'), [('false', 0), ('true', 1)])
def test_release_security_gate_reports_unverified_sources(tmp_path, strict, expected_status):
    fake_bin = tmp_path / 'bin'
    fake_bin.mkdir()
    fake_gh = fake_bin / 'gh'
    fake_gh.write_text(
        f'#!{sys.executable}\n'
        'import os, sys\n'
        'url = next(arg for arg in sys.argv if arg.startswith("repos/"))\n'
        'if "/code-scanning/" in url: sys.exit(1)\n'
    )
    fake_gh.chmod(0o755)
    env = dict(os.environ)
    env.update(
        GITHUB_REPOSITORY='Backblaze/B2_Command_Line_Tool',
        SECURITY_GATE_STRICT=strict,
        GITHUB_ACTIONS='true',
        RELEASE_SECURITY_TOKEN='test-placeholder',
        PATH=f'{fake_bin}{os.pathsep}{env["PATH"]}',
    )
    result = subprocess.run(
        ['bash', str(ROOT / 'scripts/release-security-gate.sh')],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == expected_status, result.stdout + result.stderr
    assert 'UNVERIFIED' in result.stdout + result.stderr


@pytest.fixture
def signed_release_repo(tmp_path):
    if not POSIX_TOOLS:
        pytest.skip('release scripts require POSIX tools')
    repo = tmp_path / 'repo'
    repo.mkdir()

    def git(*args):
        subprocess.run(['git', *args], cwd=repo, check=True, capture_output=True)

    git('init', '-b', 'master')
    git('config', 'user.name', 'Release Test')
    git('config', 'user.email', 'release@example.test')
    (repo / 'README').write_text('release test\n')
    key = tmp_path / 'signing-key'
    subprocess.run(
        ['ssh-keygen', '-q', '-t', 'ed25519', '-N', '', '-f', str(key)],
        check=True,
        capture_output=True,
    )
    (repo / 'release-keys').mkdir()
    principal = (ROOT / 'release-keys/maintainers.allowed_signers').read_text().split()[0]
    (repo / 'release-keys/maintainers.allowed_signers').write_text(
        f'{principal} {key.with_suffix(".pub").read_text()}'
    )
    git('add', 'README', 'release-keys/maintainers.allowed_signers')
    git('commit', '-m', 'baseline')
    git(
        '-c',
        'gpg.format=ssh',
        '-c',
        f'user.signingkey={key}',
        'tag',
        '-s',
        'v1.0.0',
        '-m',
        'v1.0.0',
    )
    git('tag', 'v1.0.1')
    remote = tmp_path / 'remote.git'
    subprocess.run(['git', 'init', '--bare', str(remote)], check=True, capture_output=True)
    git('remote', 'add', 'origin', str(remote))
    git('push', '-u', 'origin', 'master')
    return repo, git, key


def run_tag_gate(repo, tag, triggering_sha=None):
    env = {key: value for key, value in os.environ.items() if key != 'GITHUB_SHA'}
    if triggering_sha is not None:
        env['GITHUB_SHA'] = triggering_sha
    return subprocess.run(
        ['bash', str(ROOT / 'scripts/verify-release-tag.sh'), tag],
        cwd=repo,
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )


def test_release_tag_gate_accepts_only_trusted_signed_tags(signed_release_repo):
    repo, git, _ = signed_release_repo
    git('update-ref', '-d', 'refs/remotes/origin/master')
    no_master = run_tag_gate(repo, 'v1.0.0')
    git('update-ref', 'refs/remotes/origin/master', 'HEAD')
    signed = run_tag_gate(repo, 'v1.0.0')
    unsigned = run_tag_gate(repo, 'v1.0.1')
    assert no_master.returncode != 0
    assert 'master ref is unavailable' in no_master.stderr
    assert signed.returncode == 0, signed.stderr
    assert 'Verified release tag v1.0.0.' in signed.stdout
    sha = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=repo, text=True).strip()
    triggered = run_tag_gate(repo, 'v1.0.0', triggering_sha=sha)
    assert triggered.returncode == 0, triggered.stderr
    assert unsigned.returncode != 0


def test_release_tag_gate_rejects_a_renamed_signed_tag(signed_release_repo):
    repo, git, _ = signed_release_repo
    git('update-ref', 'refs/tags/v9.9.9', 'refs/tags/v1.0.0')
    result = run_tag_gate(repo, 'v9.9.9')
    assert result.returncode != 0
    assert 'does not match its signed tag header' in result.stderr


def test_release_tag_gate_rejects_a_commit_outside_master(signed_release_repo):
    repo, git, key = signed_release_repo
    git('checkout', '-b', 'outside-master')
    (repo / 'README').write_text('unmerged commit\n')
    git('commit', '-am', 'Unmerged change')
    git(
        '-c',
        'gpg.format=ssh',
        '-c',
        f'user.signingkey={key}',
        'tag',
        '-s',
        'v1.0.2',
        '-m',
        'v1.0.2',
    )
    result = run_tag_gate(repo, 'v1.0.2')
    assert result.returncode != 0
    assert 'must point to a commit on master' in result.stderr


def test_release_tag_gate_rejects_a_different_triggering_commit(signed_release_repo):
    repo, _, _ = signed_release_repo
    result = run_tag_gate(repo, 'v1.0.0', triggering_sha='0' * 40)
    assert result.returncode != 0
    assert 'does not point to the triggering commit' in result.stderr


def test_release_tag_gate_rejects_an_untrusted_signer(signed_release_repo, tmp_path):
    repo, git, _ = signed_release_repo
    untrusted = tmp_path / 'untrusted-key'
    subprocess.run(
        ['ssh-keygen', '-q', '-t', 'ed25519', '-N', '', '-f', str(untrusted)], check=True
    )
    git(
        '-c',
        'gpg.format=ssh',
        '-c',
        f'user.signingkey={untrusted}',
        'tag',
        '-s',
        'v1.0.2',
        '-m',
        'v1.0.2',
    )
    result = run_tag_gate(repo, 'v1.0.2')
    assert result.returncode != 0
    assert 'No principal matched' in result.stderr


def test_release_tag_gate_uses_the_current_master_signing_keys(signed_release_repo, tmp_path):
    repo, git, _ = signed_release_repo
    replacement = tmp_path / 'replacement-key'
    subprocess.run(
        ['ssh-keygen', '-q', '-t', 'ed25519', '-N', '', '-f', str(replacement)], check=True
    )
    principal = (ROOT / 'release-keys/maintainers.allowed_signers').read_text().split()[0]
    (repo / 'release-keys/maintainers.allowed_signers').write_text(
        f'{principal} {replacement.with_suffix(".pub").read_text()}'
    )
    git('commit', '-am', 'Update release signing keys')
    git('push', 'origin', 'master')
    git('reset', '--hard', 'v1.0.0')
    result = run_tag_gate(repo, 'v1.0.0')
    assert result.returncode != 0
    assert 'No principal matched' in result.stderr


@pytest.mark.skipif(not POSIX_TOOLS, reason='release scripts require POSIX tools')
@pytest.mark.parametrize('ci', [True, False])
def test_advisories_use_the_configured_token_at_the_gh_boundary(tmp_path, ci):
    fake_bin = tmp_path / 'bin'
    fake_bin.mkdir()
    calls = tmp_path / 'calls'
    gh = fake_bin / 'gh'
    gh.write_text(
        f'#!{sys.executable}\n'
        'import json, os, pathlib, sys\n'
        'url = next(arg for arg in sys.argv if arg.startswith("repos/"))\n'
        'with pathlib.Path(os.environ["GATE_CALLS"]).open("a") as calls:\n'
        '    calls.write(json.dumps([url, os.environ.get("GH_TOKEN")]) + "\\n")\n'
    )
    gh.chmod(0o755)
    env = dict(
        os.environ,
        GH_TOKEN='local-test-placeholder',
        GATE_CALLS=str(calls),
        SECURITY_GATE_STRICT='false',
        PATH=f'{fake_bin}{os.pathsep}{os.environ["PATH"]}',
    )
    env.pop('GITHUB_ACTIONS', None)
    env.pop('RELEASE_SECURITY_TOKEN', None)
    if ci:
        env.update(GITHUB_ACTIONS='true', RELEASE_SECURITY_TOKEN='dedicated-test-placeholder')
    result = subprocess.run(
        ['bash', str(ROOT / 'scripts/release-security-gate.sh')],
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    recorded = [json.loads(line) for line in calls.read_text().splitlines()]
    expected = 'dedicated-test-placeholder' if ci else 'local-test-placeholder'
    assert [token for url, token in recorded if '/security-advisories?' in url] == [expected]
    assert 'CLEAR: security-advisories' in result.stdout
    assert 'UNVERIFIED: security-advisories' not in result.stdout


@pytest.mark.skipif(not POSIX_TOOLS, reason='release scripts require POSIX tools')
@pytest.mark.parametrize(('strict', 'status'), [('false', 0), ('true', 1)])
def test_ci_advisories_are_unverified_without_a_dedicated_token(tmp_path, strict, status):
    fake_bin = tmp_path / 'bin'
    fake_bin.mkdir()
    calls = tmp_path / 'calls'
    gh = fake_bin / 'gh'
    gh.write_text(
        f'#!{sys.executable}\n'
        'import os, pathlib, sys\n'
        'with pathlib.Path(os.environ["GATE_CALLS"]).open("a") as calls:\n'
        '    calls.write(" ".join(sys.argv[1:]) + "\\n")\n'
    )
    gh.chmod(0o755)
    env = dict(
        os.environ,
        GITHUB_ACTIONS='true',
        RELEASE_SECURITY_TOKEN='',
        SECURITY_GATE_STRICT=strict,
        GATE_CALLS=str(calls),
        PATH=f'{fake_bin}{os.pathsep}{os.environ["PATH"]}',
    )
    result = subprocess.run(
        ['bash', str(ROOT / 'scripts/release-security-gate.sh')],
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == status, result.stdout + result.stderr
    assert 'UNVERIFIED: security-advisories' in result.stdout
    assert 'CLEAR: security-advisories' not in result.stdout
    assert 'security-advisories' not in calls.read_text()


@pytest.mark.skipif(not POSIX_TOOLS, reason='release scripts require POSIX tools')
def test_release_notes_select_only_requested_version():
    script = ROOT / 'scripts/release-notes.sh'
    current = subprocess.run(
        ['bash', str(script), '5.0.3'], cwd=ROOT, capture_output=True, text=True, check=False
    )
    missing = subprocess.run(
        ['bash', str(script), '99.0.0'], cwd=ROOT, capture_output=True, text=True, check=False
    )
    assert current.returncode == 0, current.stderr
    assert '### Fixed' in current.stdout
    assert '## [5.0.2]' not in current.stdout
    assert missing.returncode != 0
