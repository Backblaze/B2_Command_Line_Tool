######################################################################
#
# File: test/unit/_cli/test_autocomplete_cache.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

# Most of the tests in this module are running in a forked process
# because argcomplete and autocomplete_cache mess with global state,
# making the argument parser unusable for other tests.

from __future__ import annotations

import contextlib
import io
import os
import pathlib
import subprocess
import sys

import argcomplete
import pytest

import b2._internal._cli.argcompleters
import b2._internal.arg_parser
import b2._internal.console_tool
from b2._internal._cli import autocomplete_cache
from test.known_defect import known_defect

from .unpickle import unpickle

# We can't use pytest.mark.skipif to skip forked tests because with pytest-forked,
# there is an attempt to fork even if the test is marked as skipped.
# See https://github.com/pytest-dev/pytest-forked/issues/44
if sys.platform == 'win32':
    forked = pytest.mark.skip(reason="Tests can't be run forked on windows")
else:
    forked = pytest.mark.forked


class Exit:
    """A mocked exit method callable. Instead of actually exiting,
    it just stores the exit code and returns."""

    code: int | None

    @property
    def success(self):
        return self.code == 0

    @property
    def empty(self):
        return self.code is None

    def __init__(self):
        self.code = None

    def __call__(self, n: int):
        self.code = n


@pytest.fixture
def autocomplete_runner(monkeypatch, b2_cli):
    def fdopen(fd, *args, **kwargs):
        # argcomplete package tries to open fd 9 for debugging which causes
        # pytest to later raise errors about bad file descriptors.
        if fd == 9:
            return sys.stderr
        return os.fdopen(fd, *args, **kwargs)

    @contextlib.contextmanager
    def runner(command: str):
        with monkeypatch.context() as m:
            m.setenv('COMP_LINE', command)
            m.setenv('COMP_POINT', str(len(command)))
            m.setenv('_ARGCOMPLETE_IFS', ' ')
            m.setenv('_ARGCOMPLETE', '1')
            m.setattr('os.fdopen', fdopen)

            def _get_b2api_for_profile(profile: str):
                return b2_cli.b2_api

            m.setattr('b2._internal._cli.b2api._get_b2api_for_profile', _get_b2api_for_profile)
            yield

    return runner


def argcomplete_result():
    parser = b2._internal.console_tool.B2.create_parser()
    exit, output = Exit(), io.StringIO()
    argcomplete.autocomplete(parser, exit_method=exit, output_stream=output)
    return exit.code, output.getvalue()


def cached_complete_result(cache: autocomplete_cache.AutocompleteCache, raise_exc: bool = True):
    exit, output = Exit(), io.StringIO()
    cache.autocomplete_from_cache(
        uncached_args={'exit_method': exit, 'output_stream': output}, raise_exc=raise_exc
    )
    return exit.code, output.getvalue()


def uncached_complete_result(cache: autocomplete_cache.AutocompleteCache):
    exit, output = Exit(), io.StringIO()
    parser = b2._internal.console_tool.B2.create_parser()
    cache.cache_and_autocomplete(
        parser, uncached_args={'exit_method': exit, 'output_stream': output}
    )
    return exit.code, output.getvalue()


@forked
def test_complete_main_command(autocomplete_runner, tmp_path):
    cache = autocomplete_cache.AutocompleteCache(
        tracker=autocomplete_cache.VersionTracker(),
        store=autocomplete_cache.HomeCachePickleStore(tmp_path),
    )
    with autocomplete_runner('b2 '):
        exit, argcomplete_output = argcomplete_result()
        assert exit == 0
        assert 'get-bucket' in argcomplete_output

    with autocomplete_runner('b2 '):
        exit, output = cached_complete_result(cache)
        # Nothing has been cached yet, we expect simple return, not an exit
        assert exit is None
        assert not output

    with autocomplete_runner('b2 '):
        exit, output = uncached_complete_result(cache)
        assert exit == 0
        assert output == argcomplete_output

    with autocomplete_runner('b2 '):
        exit, output = cached_complete_result(cache)
        assert exit == 0
        assert output == argcomplete_output


@forked
def test_complete_with_bucket_suggestions(autocomplete_runner, tmp_path, bucket, authorized_b2_cli):
    cache = autocomplete_cache.AutocompleteCache(
        tracker=autocomplete_cache.VersionTracker(),
        store=autocomplete_cache.HomeCachePickleStore(tmp_path),
    )
    with autocomplete_runner('b2 get-bucket '):
        exit, argcomplete_output = argcomplete_result()
        assert exit == 0
        assert bucket in argcomplete_output

        exit, output = uncached_complete_result(cache)
        assert exit == 0
        assert output == argcomplete_output

        exit, output = cached_complete_result(cache)
        assert exit == 0
        assert output == argcomplete_output


@forked
def test_complete_with_escaped_control_characters(
    autocomplete_runner, tmp_path, bucket, uploaded_file_with_control_chars, authorized_b2_cli
):
    cc_file_name = uploaded_file_with_control_chars['fileName']
    escaped_cc_file_name = uploaded_file_with_control_chars['escapedFileName']
    cache = autocomplete_cache.AutocompleteCache(
        tracker=autocomplete_cache.VersionTracker(),
        store=autocomplete_cache.HomeCachePickleStore(tmp_path),
    )

    with autocomplete_runner(f'b2 hide-file {bucket} '):
        exit, argcomplete_output = argcomplete_result()
        assert exit == 0
        assert escaped_cc_file_name in argcomplete_output
        assert cc_file_name not in argcomplete_output

        exit, output = uncached_complete_result(cache)
        assert exit == 0
        assert output == argcomplete_output

        exit, output = cached_complete_result(cache)
        assert exit == 0
        assert output == argcomplete_output


@forked
def test_complete_with_file_suggestions(
    autocomplete_runner, tmp_path, bucket, uploaded_file, authorized_b2_cli
):
    file_name = uploaded_file['fileName']
    cache = autocomplete_cache.AutocompleteCache(
        tracker=autocomplete_cache.VersionTracker(),
        store=autocomplete_cache.HomeCachePickleStore(tmp_path),
    )
    with autocomplete_runner(f'b2 hide-file {bucket} '):
        exit, argcomplete_output = argcomplete_result()
        assert exit == 0
        assert file_name in argcomplete_output

        exit, output = cached_complete_result(cache)
        assert exit is None
        assert output == ''

        exit, output = uncached_complete_result(cache)
        assert exit == 0
        assert output == argcomplete_output

        exit, output = cached_complete_result(cache)
        assert exit == 0
        assert output == argcomplete_output


@forked
def test_complete_with_file_uri_suggestions(
    autocomplete_runner, tmp_path, bucket, uploaded_file, authorized_b2_cli
):
    file_name = uploaded_file['fileName']
    cache = autocomplete_cache.AutocompleteCache(
        tracker=autocomplete_cache.VersionTracker(),
        store=autocomplete_cache.HomeCachePickleStore(tmp_path),
    )
    with autocomplete_runner(f'b2 file download b2://{bucket}/'):
        exit, argcomplete_output = argcomplete_result()
        assert exit == 0
        assert file_name in argcomplete_output

        exit, output = uncached_complete_result(cache)
        assert exit == 0
        assert output == argcomplete_output

        exit, output = cached_complete_result(cache)
        assert exit == 0
        assert output == argcomplete_output


@forked
def test_that_autocomplete_cache_loading_does_not_load_b2sdk(autocomplete_runner, tmp_path):
    cache = autocomplete_cache.AutocompleteCache(
        tracker=autocomplete_cache.VersionTracker(),
        store=autocomplete_cache.HomeCachePickleStore(tmp_path),
        unpickle=unpickle,  # using our unpickling function that fails if b2sdk is loaded
    )
    with autocomplete_runner('b2 '):
        exit, uncached_output = uncached_complete_result(cache)
        assert exit == 0
        assert 'get-bucket' in uncached_output

        exit, output = cached_complete_result(cache, raise_exc=True)
        assert (exit, output) == (0, uncached_output)


def _complete_in_subprocess(apiver: str, home: pathlib.Path) -> list[str]:
    """Run `<apiver> <TAB>` completion in a fresh interpreter; return the completions it printed."""
    out_path = home / f'completions-{apiver}.txt'
    script = (
        'import os, sys\n'
        'out = os.open(sys.argv[2], os.O_WRONLY | os.O_CREAT | os.O_TRUNC)\n'
        'os.dup2(out, 8)\n'  # argcomplete writes the completions to fd 8
        'import importlib\n'
        'sys.argv = [sys.argv[1]]\n'
        'importlib.import_module(f"b2._internal.{sys.argv[0]}.registry").main()\n'
    )
    env = {
        **os.environ,
        'HOME': str(home),
        'XDG_CACHE_HOME': str(home / 'xdg'),
        'COMP_LINE': f'{apiver} ',
        'COMP_POINT': str(len(apiver) + 1),
        '_ARGCOMPLETE': '1',
        '_ARGCOMPLETE_IFS': '\n',
    }
    result = subprocess.run(
        [sys.executable, '-c', script, apiver, str(out_path)],
        env=env,
        cwd=home,
        timeout=120,
        capture_output=True,
        text=True,
    )
    # argcomplete exits through sys.exit(0) after printing; anything else is a setup problem, not the defect
    if result.returncode != 0:
        pytest.fail(f'{apiver} completion subprocess failed: {result.stderr}', pytrace=False)
    return out_path.read_text().split()


def _cache_files(home: pathlib.Path) -> dict[pathlib.Path, bytes]:
    return {p: p.read_bytes() for p in home.rglob('b2-autocomplete-cache-*.pickle')}


@known_defect('b2v3, b2v4 and b2v5 share one autocomplete cache file')
def test_autocomplete_cache_is_separated_per_cli_api_version(tmp_path):
    """The b2v3 and b2v5 parsers must each keep, and keep using, their own cache file."""
    if sys.platform == 'win32':
        pytest.skip('argcomplete uses file descriptor 8')

    # reference caches, each written in a pristine home so that nothing is shared (setup, not the defect)
    reference = {}
    for apiver in ('b2v3', 'b2v5'):
        fresh_home = tmp_path / f'reference-{apiver}'
        fresh_home.mkdir()
        reference[apiver] = (_complete_in_subprocess(apiver, fresh_home), _cache_files(fresh_home))
        if not reference[apiver][1]:
            pytest.fail(f'{apiver} wrote no cache file in a pristine home', pytrace=False)
    if sorted(reference['b2v3'][1].values()) == sorted(reference['b2v5'][1].values()):
        pytest.fail('b2v3 and b2v5 write identical caches, the test cannot tell them apart')

    # now both versions run against one shared home
    shared = tmp_path / 'shared'
    shared.mkdir()
    _complete_in_subprocess('b2v3', shared)
    cache_after_v3 = _cache_files(shared)
    assert sorted(cache_after_v3.values()) == sorted(reference['b2v3'][1].values())

    _complete_in_subprocess('b2v5', shared)
    cache_after_v5 = _cache_files(shared)
    assert set(cache_after_v5) > set(cache_after_v3), 'b2v5 wrote no cache file of its own'
    for path, content in cache_after_v3.items():
        assert cache_after_v5[path] == content, f'b2v5 overwrote the cache written by b2v3: {path}'

    # with both caches in place, each version must keep being served its own completions
    for apiver in ('b2v3', 'b2v5'):
        assert _complete_in_subprocess(apiver, shared) == reference[apiver][0]
    assert _cache_files(shared) == cache_after_v5, 'a cached run modified the cache files'


def _walk_parsers(parser):
    yield parser
    for action in parser._actions:
        for sub in getattr(action, 'choices', None) or ():
            if isinstance(action.choices, dict):
                yield from _walk_parsers(action.choices[sub])


@known_defect('argument completers are set through a misspelled attribute and are never used')
def test_no_argument_uses_a_misspelled_completer_attribute(b2_cli):
    """Argcomplete reads `.completer`; an argument carrying `.completion` is never completed."""
    parser = b2._internal.console_tool.B2.create_parser()
    misspelled = [
        f'{p.prog}: {action.dest}'
        for p in _walk_parsers(parser)
        for action in p._actions
        if hasattr(action, 'completion')
    ]
    assert misspelled == []


@known_defect('the os.fdopen test shim recurses into itself', raises=RecursionError)
def test_os_fdopen_shim_does_not_recurse(autocomplete_runner):
    """The test shim for os.fdopen must delegate to the real function, not to itself."""
    if sys.platform == 'win32':
        pytest.skip('needs fcntl to pick a descriptor number')
    import fcntl

    pipe_read, pipe_write = os.pipe()
    # the shim special-cases fd 9 (and returns sys.stderr for it); make sure the descriptor under test is above it
    read_fd = fcntl.fcntl(pipe_read, fcntl.F_DUPFD, 10)
    os.close(pipe_read)
    assert read_fd != 9
    try:
        with autocomplete_runner('b2 '):
            handle = os.fdopen(read_fd)  # any fd except 9 goes through the shim
            assert handle is not sys.stderr
            read_fd = None  # the handle owns the descriptor now
            try:
                assert handle.readable()
            finally:
                handle.close()
    finally:
        if read_fd is not None:
            os.close(read_fd)
        os.close(pipe_write)
