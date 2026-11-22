######################################################################
#
# File: test/unit/_cli/test_autocomplete_cache_per_version.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import os
import subprocess
import sys

import pytest

from b2._internal._cli import autocomplete_cache
from b2._internal._cli.autocomplete_cache import HomeCachePickleStore, VersionTracker


def complete(version, cache_home):
    command = 'b2 bucket create --default-server-side-encryption n'
    output = cache_home / 'completions.txt'
    env = {key: value for key, value in os.environ.items() if not key.startswith('B2_')}
    env.update(
        HOME=str(cache_home),
        USERPROFILE=str(cache_home),
        LOCALAPPDATA=str(cache_home),
        APPDATA=str(cache_home),
        XDG_CACHE_HOME=str(cache_home),
        TEST_AUTOCOMPLETE_CACHE_ROOT=str(cache_home),
        _ARGCOMPLETE='1',
        _ARGCOMPLETE_IFS=' ',
        _ARGCOMPLETE_STDOUT_FILENAME=str(output),
        COMP_LINE=command,
        COMP_POINT=str(len(command)),
    )
    # Windows may resolve the cache path via the OS instead of environment variables.
    # Override only that platform boundary before executing the real CLI module.
    bootstrap = (
        'import os, platformdirs, runpy, sys\n'
        'platformdirs.user_cache_dir = lambda *args, **kwargs: '
        'os.environ["TEST_AUTOCOMPLETE_CACHE_ROOT"]\n'
        'runpy.run_module(f"b2._internal.{sys.argv[1]}", run_name="__main__", alter_sys=True)\n'
    )
    subprocess.run(
        [sys.executable, '-c', bootstrap, version],
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
        check=True,
    )
    return output.read_text()


@pytest.mark.parametrize('older_version', ['b2v3', 'b2v4'])
@pytest.mark.parametrize('newer_first', [False, True])
def test_cli_versions_use_separate_completion_caches(tmp_path, older_version, newer_first):
    expected = {older_version: 'none', 'b2v5': ''}
    versions = ['b2v5', older_version] if newer_first else [older_version, 'b2v5']
    completions = {version: complete(version, tmp_path).strip() for version in versions}

    cache_files = list(tmp_path.rglob('b2-autocomplete-cache-*.pickle'))
    assert len(cache_files) == 2
    assert completions == expected

    for path in cache_files:
        os.utime(path, (1, 1))
    cached = {path: (path.read_bytes(), path.stat().st_mtime_ns) for path in cache_files}
    assert {version: complete(version, tmp_path).strip() for version in versions} == expected
    assert {path: (path.read_bytes(), path.stat().st_mtime_ns) for path in cache_files} == cached


def test_all_three_cli_versions_keep_their_cache(tmp_path):
    assert complete('b2v3', tmp_path).strip() == 'none'
    assert complete('b2v4', tmp_path).strip() == 'none'
    assert complete('b2v5', tmp_path).strip() == ''
    assert len(list(tmp_path.rglob('b2-autocomplete-cache-*.pickle'))) == 3


def test_completion_cache_is_isolated_when_platform_directory_ignores_environment(
    tmp_path, monkeypatch
):
    fake_site = tmp_path / 'site'
    fake_site.mkdir()
    (fake_site / 'sitecustomize.py').write_text(
        'import os, platformdirs\n'
        'platformdirs.user_cache_dir = lambda *args, **kwargs: os.environ["TEST_OTHER_CACHE"]\n'
    )
    other_cache = tmp_path / 'other-cache'
    isolated_cache = tmp_path / 'isolated-cache'
    isolated_cache.mkdir()
    monkeypatch.setenv('PYTHONPATH', str(fake_site))
    monkeypatch.setenv('TEST_OTHER_CACHE', str(other_cache))

    assert complete('b2v5', isolated_cache).strip() == ''
    assert len(list(isolated_cache.rglob('b2-autocomplete-cache-*.pickle'))) == 1
    assert not list(other_cache.rglob('b2-autocomplete-cache-*.pickle'))


def test_updating_one_cli_version_keeps_other_versions_cache(tmp_path):
    older_store = HomeCachePickleStore(tmp_path, cli_version='b2v3')
    newer_store = HomeCachePickleStore(tmp_path, cli_version='b2v5')
    older_store.set_pickle('previous', b'old package version')
    newer_store.set_pickle('current', b'other CLI version')

    older_store.set_pickle('current', b'new package version')
    assert older_store.get_pickle('previous') is None
    assert older_store.get_pickle('current') == b'new package version'
    assert newer_store.get_pickle('current') == b'other CLI version'


def test_version_tracker_includes_package_and_cli_version(monkeypatch):
    monkeypatch.setattr(autocomplete_cache, 'VERSION', 'package-one')
    old_key = VersionTracker('b2v3').current_state_identifier()
    assert old_key != VersionTracker('b2v5').current_state_identifier()
    monkeypatch.setattr(autocomplete_cache, 'VERSION', 'package-two')
    assert old_key != VersionTracker('b2v3').current_state_identifier()
