######################################################################
#
# File: test/release/test_changelog.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
"""Exercise release preparation in disposable worktrees without publishing."""

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
RELEASE_VERSION = '999.0.0'


@pytest.fixture
def release_worktree(tmp_path):
    worktree = tmp_path / 'release'
    subprocess.run(
        ['git', 'worktree', 'add', '--detach', str(worktree), 'HEAD'], cwd=ROOT, check=True
    )
    try:
        # Include uncommitted release-helper edits when running this test locally.
        shutil.copyfile(ROOT / 'noxfile.py', worktree / 'noxfile.py')
        fragment = worktree / '.changes/unreleased/Fixed-release-test.yaml'
        fragment.parent.mkdir(exist_ok=True)
        fragment.write_text(
            'kind: Fixed\nbody: Include the release test entry.\ntime: 2026-01-01T00:00:00Z\n'
        )
        subprocess.run(['git', 'add', 'noxfile.py', str(fragment)], cwd=worktree, check=True)
        subprocess.run(
            [
                'git',
                '-c',
                'user.name=Release Test',
                '-c',
                'user.email=release@example.test',
                '-c',
                'commit.gpgsign=false',
                'commit',
                '--allow-empty',
                '-m',
                'Prepare release test worktree',
            ],
            cwd=worktree,
            check=True,
        )
        yield worktree
    finally:
        subprocess.run(
            ['git', 'worktree', 'remove', '--force', str(worktree)], cwd=ROOT, check=True
        )


def run_session(worktree, session, *args, requirements='b2sdk==2.13.2\n'):
    # The export command is the dependency boundary for the released-SDK guard.
    # Real Changie commands and Git operations run against the disposable worktree.
    tools = worktree.parent / 'tools'
    tools.mkdir(exist_ok=True)
    uv = tools / 'uv'
    real_uv = shutil.which('uv')
    assert real_uv is not None
    uv.write_text(
        f'#!{sys.executable}\n'
        'import os, sys\n'
        'if sys.argv[1:2] == ["export"]:\n'
        f'    print({requirements!r}, end="")\n'
        'else:\n'
        f'    os.execv({real_uv!r}, [{real_uv!r}, *sys.argv[1:]])\n'
    )
    uv.chmod(0o755)
    real_go = shutil.which('go')
    assert real_go is not None
    go = tools / 'go'
    go.write_text(
        f'#!{sys.executable}\n'
        'import json, os, sys\n'
        f'with open({str(worktree.parent / "go-calls.jsonl")!r}, "a") as calls:\n'
        '    calls.write(json.dumps(sys.argv[1:]) + "\\n")\n'
        f'os.execv({real_go!r}, [{real_go!r}, *sys.argv[1:]])\n'
    )
    go.chmod(0o755)
    env = dict(os.environ, PATH=f'{tools}{os.pathsep}{os.environ["PATH"]}')
    return subprocess.run(
        [sys.executable, '-m', 'nox', '--no-venv', '-s', session, '--', *args],
        cwd=worktree,
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
    )


def test_release_preparation_batches_fragments_and_preserves_history(release_worktree):
    changes = release_worktree / '.changes'
    history = {path.name: path.read_bytes() for path in changes.glob('v*.md')}
    fragments = list((changes / 'unreleased').glob('*.yaml'))
    assert fragments, 'release test requires pending fragments'

    result = run_session(release_worktree, 'make_release_commit', RELEASE_VERSION)

    assert result.returncode == 0, result.stdout + result.stderr
    assert not list((changes / 'unreleased').glob('*.yaml'))
    release_notes = (changes / f'v{RELEASE_VERSION}.md').read_text()
    assert 'Include the release test entry.' in release_notes
    assert all((changes / name).read_bytes() == content for name, content in history.items())
    changelog = (release_worktree / 'CHANGELOG.md').read_text()
    assert release_notes.strip() in changelog
    for content in history.values():
        assert content.decode().strip() in changelog


@pytest.mark.parametrize(
    'requirements',
    [
        'b2sdk @ git+https://example.invalid/b2sdk\n',
        'b2sdk==2.13.2\nother @ git+https://example.invalid/x\n',
        'other==1.0\n',
    ],
)
def test_release_preparation_rejects_unreleased_sdk(release_worktree, requirements):
    before = (release_worktree / 'CHANGELOG.md').read_bytes()

    result = run_session(
        release_worktree,
        'make_release_commit',
        RELEASE_VERSION,
        requirements=requirements,
    )

    assert result.returncode != 0
    assert 'release version must depend on released b2sdk version' in result.stderr
    assert (release_worktree / 'CHANGELOG.md').read_bytes() == before
    assert not (release_worktree / f'.changes/v{RELEASE_VERSION}.md').exists()


def test_changelog_check_after_release_still_checks_rendered_history(release_worktree):
    result = run_session(release_worktree, 'make_release_commit', RELEASE_VERSION)
    assert result.returncode == 0, result.stdout + result.stderr
    assert not list((release_worktree / '.changes/unreleased').glob('*.yaml'))
    calls_file = release_worktree.parent / 'go-calls.jsonl'
    calls_file.write_text('')

    result = run_session(release_worktree, 'changie_check')
    assert result.returncode == 0, result.stdout + result.stderr
    calls = [json.loads(line) for line in calls_file.read_text().splitlines()]
    assert any('merge' in call for call in calls)
    assert all('batch' not in call for call in calls)

    with (release_worktree / 'CHANGELOG.md').open('a') as changelog:
        changelog.write('\nUnexpected rendered content\n')
    result = run_session(release_worktree, 'changie_check')
    assert result.returncode != 0
    assert 'CHANGELOG.md differs' in result.stderr


def test_changelog_check_validates_pending_fragments(release_worktree):
    result = run_session(release_worktree, 'changie_check')
    assert result.returncode == 0, result.stdout + result.stderr
    calls = [
        json.loads(line)
        for line in (release_worktree.parent / 'go-calls.jsonl').read_text().splitlines()
    ]
    assert any(call[-3:] == ['batch', 'patch', '--dry-run'] for call in calls)
    assert any(call[-2:] == ['merge', '--dry-run'] for call in calls)


def test_changelog_check_rejects_a_malformed_fragment(release_worktree):
    before = (release_worktree / 'CHANGELOG.md').read_bytes()
    (release_worktree / '.changes/unreleased/Fixed-release-test.yaml').write_text('kind: [\n')
    result = run_session(release_worktree, 'changie_check')
    assert result.returncode != 0
    calls = [
        json.loads(line)
        for line in (release_worktree.parent / 'go-calls.jsonl').read_text().splitlines()
    ]
    assert any(call[-3:] == ['batch', 'patch', '--dry-run'] for call in calls)
    assert (release_worktree / 'CHANGELOG.md').read_bytes() == before
