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
import sys

import argcomplete
import pytest

import b2._internal._cli.argcompleters
import b2._internal.arg_parser
import b2._internal.console_tool
from b2._internal._cli import autocomplete_cache

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
