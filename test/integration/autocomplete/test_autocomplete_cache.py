######################################################################
#
# File: test/integration/autocomplete/test_autocomplete_cache.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import contextlib
import importlib
import io
import os
import pathlib
import pickle
import sys
from typing import Any

import argcomplete
import pytest

import b2._cli.argcompleters
import b2.arg_parser
import b2.console_tool
from b2 import autocomplete_cache


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
def autocomplete_runner(monkeypatch):
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
            yield

    return runner


def argcomplete_result():
    parser = b2.console_tool.B2.get_parser()
    exit, output = Exit(), io.StringIO()
    argcomplete.autocomplete(parser, exit_method=exit, output_stream=output)
    return exit.code, output.getvalue()


def cached_complete_result(cache: autocomplete_cache.AutocompleteCache):
    exit, output = Exit(), io.StringIO()
    cache.autocomplete_from_cache(uncached_args={'exit_method': exit, 'output_stream': output})
    return exit.code, output.getvalue()


def uncached_complete_result(cache: autocomplete_cache.AutocompleteCache):
    exit, output = Exit(), io.StringIO()
    parser = b2.console_tool.B2.get_parser()
    cache.cache_and_autocomplete(
        parser, uncached_args={
            'exit_method': exit,
            'output_stream': output
        }
    )
    return exit.code, output.getvalue()


def test_complete_main_command(autocomplete_runner, tmpdir):
    cache = autocomplete_cache.AutocompleteCache(
        tracker=autocomplete_cache.FileSetStateTrakcer([pathlib.Path(__file__)]),
        store=autocomplete_cache.HomeCachePickleStore(pathlib.Path(tmpdir)),
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


def test_complete_with_bucket_suggestions(autocomplete_runner, tmpdir, bucket_name, b2_tool):
    cache = autocomplete_cache.AutocompleteCache(
        tracker=autocomplete_cache.FileSetStateTrakcer([pathlib.Path(__file__)]),
        store=autocomplete_cache.HomeCachePickleStore(pathlib.Path(tmpdir)),
    )
    with autocomplete_runner('b2 get-bucket '):
        exit, argcomplete_output = argcomplete_result()
        assert exit == 0
        assert bucket_name in argcomplete_output

        exit, output = cached_complete_result(cache)
        assert exit is None
        assert output == ''

        exit, output = uncached_complete_result(cache)
        assert exit == 0
        assert output == argcomplete_output

        exit, output = cached_complete_result(cache)
        assert exit == 0
        assert output == argcomplete_output


def test_complete_with_file_suggestions(
    autocomplete_runner, tmpdir, bucket_name, file_name, b2_tool
):
    cache = autocomplete_cache.AutocompleteCache(
        tracker=autocomplete_cache.FileSetStateTrakcer([pathlib.Path(__file__)]),
        store=autocomplete_cache.HomeCachePickleStore(pathlib.Path(tmpdir)),
    )
    with autocomplete_runner(f'b2 download-file-by-name {bucket_name} '):
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


def test_hasher(tmpdir):
    path_1 = pathlib.Path(tmpdir) / 'test_1'
    path_1.write_text('test_1', 'ascii')
    path_2 = pathlib.Path(tmpdir) / 'test_2'
    path_2.write_text('test_2', 'ascii')
    path_3 = pathlib.Path(tmpdir) / 'test_3'
    path_3.write_text('test_3', 'ascii')
    tracker_1 = autocomplete_cache.FileSetStateTrakcer([path_1, path_2])
    tracker_2 = autocomplete_cache.FileSetStateTrakcer([path_2, path_3])
    assert tracker_1.current_state_identifier() != tracker_2.current_state_identifier()


def test_pickle_store(tmpdir):
    dir = pathlib.Path(tmpdir)
    store = autocomplete_cache.HomeCachePickleStore(dir)

    store.set_pickle('test_1', b'test_data_1')
    assert store.get_pickle('test_1') == b'test_data_1'
    assert store.get_pickle('test_2') is None
    assert len(list(dir.glob('**'))) == 1

    store.set_pickle('test_2', b'test_data_2')
    assert store.get_pickle('test_2') == b'test_data_2'
    assert store.get_pickle('test_1') is None
    assert len(list(dir.glob('**'))) == 1


class Unpickler(pickle.Unpickler):
    """This Unpickler will raise an exception if loading the pickled object
    imports any b2sdk module."""

    _modules_to_load: set[str]

    def load(self):
        self._modules_to_load = set()

        b2_modules = [module for module in sys.modules if 'b2sdk' in module]
        for key in b2_modules:
            del sys.modules[key]

        result = super().load()

        for module in self._modules_to_load:
            importlib.import_module(module)
            importlib.reload(sys.modules[module])

        if any('b2sdk' in module for module in sys.modules):
            raise RuntimeError("Loading the pickled object imported b2sdk module")
        return result

    def find_class(self, module: str, name: str) -> Any:
        self._modules_to_load.add(module)
        return super().find_class(module, name)


def unpickle(data: bytes) -> Any:
    """Unpickling function that raises RunTimError if unpickled
    object depends on b2sdk."""
    return Unpickler(io.BytesIO(data)).load()


def test_unpickle():
    """This tests ensures that Unpickler works as expected:
    prevents successful unpickling of objects that depend on loading
    modules from b2sdk."""
    from .module_loading_b2sdk import function
    pickled = pickle.dumps(function)
    with pytest.raises(RuntimeError):
        unpickle(pickled)


def test_that_autocomplete_cache_loading_does_not_load_b2sdk(autocomplete_runner, tmpdir):
    cache = autocomplete_cache.AutocompleteCache(
        tracker=autocomplete_cache.FileSetStateTrakcer([pathlib.Path(__file__)]),
        store=autocomplete_cache.HomeCachePickleStore(pathlib.Path(tmpdir)),
        unpickle=unpickle,  # using our unpickling function that fails if b2sdk is loaded
    )
    with autocomplete_runner('b2 '):
        exit, uncached_output = uncached_complete_result(cache)
        assert exit == 0
        assert 'get-bucket' in uncached_output

        exit, output = cached_complete_result(cache)
        assert exit == 0
        assert output == uncached_output
