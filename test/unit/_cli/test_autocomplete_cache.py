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


@known_defect('b2v3, b2v4 and b2v5 share one autocomplete cache file')
def test_autocomplete_cache_is_separated_per_cli_api_version(tmp_path):
    """The b2v3, b2v4 and b2v5 parsers must not share one cache file."""
    if sys.platform == 'win32':
        pytest.skip('argcomplete uses file descriptor 8')
    script = (
        'import os, sys\n'
        'os.dup2(os.open(os.devnull, os.O_WRONLY), 8)\n'
        'sys.argv = [sys.argv[1]]\n'
        'import importlib\n'
        'importlib.import_module(f"b2._internal.{sys.argv[0]}.registry").main()\n'
    )
    cache_names = {}
    for apiver in ('b2v3', 'b2v5'):
        env = {
            **os.environ,
            'HOME': str(tmp_path),
            'XDG_CACHE_HOME': str(tmp_path / 'xdg'),
            'COMP_LINE': f'{apiver} ',
            'COMP_POINT': str(len(apiver) + 1),
            '_ARGCOMPLETE': '1',
            '_ARGCOMPLETE_IFS': ' ',
        }
        subprocess.run([sys.executable, '-c', script, apiver], env=env, cwd=tmp_path, timeout=120)
        cache_names[apiver] = sorted(
            p.name for p in tmp_path.rglob('b2-autocomplete-cache-*.pickle')
        )
    assert cache_names['b2v3'], 'no cache file was written'
    assert (
        cache_names['b2v3'] != cache_names['b2v5']
    ), f'both api versions use the same cache file: {cache_names}'


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


@known_defect('the os.fdopen test shim recurses into itself')
def test_os_fdopen_shim_does_not_recurse(autocomplete_runner):
    """The test shim for os.fdopen must delegate to the real function, not to itself."""
    read_fd, write_fd = os.pipe()
    try:
        with autocomplete_runner('b2 '):
            with os.fdopen(read_fd) as handle:  # any fd except 9 goes through the shim
                read_fd = None
                assert handle.readable()
    finally:
        if read_fd is not None:
            os.close(read_fd)
        os.close(write_fd)
