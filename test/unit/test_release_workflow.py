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
    ('alerts', 'expected_status'),
    [
        ({'dependabot': [{'security_advisory': {'severity': 'low'}}]}, 0),
        ({'dependabot': [{'security_advisory': {'severity': 'medium'}}]}, 1),
        ({'code-scanning': [{'rule': {'security_severity_level': 'high'}}]}, 1),
        ({'security-advisories': [{'state': 'draft', 'severity': 'critical'}]}, 1),
    ],
)
def test_release_security_gate_filters_open_alerts(tmp_path, alerts, expected_status):
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


@pytest.mark.skipif(not POSIX_TOOLS, reason='release scripts require POSIX tools')
def test_release_tag_gate_accepts_only_trusted_signed_tags(tmp_path):
    repo = tmp_path / 'repo'
    repo.mkdir()

    def git(*args):
        subprocess.run(['git', *args], cwd=repo, check=True, capture_output=True)

    git('init', '-b', 'master')
    git('config', 'user.name', 'Release Test')
    git('config', 'user.email', 'release@example.test')
    (repo / 'README').write_text('release test\n')
    git('add', 'README')
    git('commit', '-m', 'baseline')
    key = tmp_path / 'signing-key'
    subprocess.run(
        ['ssh-keygen', '-q', '-t', 'ed25519', '-N', '', '-f', str(key)],
        check=True,
        capture_output=True,
    )
    (repo / 'release-keys').mkdir()
    (repo / 'release-keys/maintainers.allowed_signers').write_text(
        f'release@example.test {key.with_suffix(".pub").read_text()}'
    )
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
    script = ROOT / 'scripts/verify-release-tag.sh'
    no_master = subprocess.run(['bash', str(script), 'v1.0.0'], cwd=repo, capture_output=True)
    remote = tmp_path / 'remote.git'
    subprocess.run(['git', 'init', '--bare', str(remote)], check=True, capture_output=True)
    git('remote', 'add', 'origin', str(remote))
    git('push', '-u', 'origin', 'master')
    signed = subprocess.run(['bash', str(script), 'v1.0.0'], cwd=repo, capture_output=True)
    unsigned = subprocess.run(['bash', str(script), 'v1.0.1'], cwd=repo, capture_output=True)
    assert no_master.returncode != 0
    assert signed.returncode == 0, signed.stderr.decode()
    assert unsigned.returncode != 0


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
