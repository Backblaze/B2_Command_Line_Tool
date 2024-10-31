######################################################################
#
# File: test/unit/_cli/test_pickle.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
import pickle

import pytest

from b2._internal._cli import autocomplete_cache

from .unpickle import unpickle


def test_pickle_store(tmp_path):
    dir = tmp_path
    store = autocomplete_cache.HomeCachePickleStore(dir)

    store.set_pickle('test_1', b'test_data_1')
    assert store.get_pickle('test_1') == b'test_data_1'
    assert store.get_pickle('test_2') is None
    assert len(list(dir.rglob('*.pickle'))) == 1

    store.set_pickle('test_2', b'test_data_2')
    assert store.get_pickle('test_2') == b'test_data_2'
    assert store.get_pickle('test_1') is None
    assert len(list(dir.rglob('*.pickle'))) == 1


def test_unpickle():
    """This tests ensures that Unpickler works as expected:
    prevents successful unpickling of objects that depend on loading
    modules from b2sdk."""
    from .fixtures.module_loading_b2sdk import function
    pickled = pickle.dumps(function)
    with pytest.raises(RuntimeError):
        unpickle(pickled)
