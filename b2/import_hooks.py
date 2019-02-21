######################################################################
#
# File: b2/import_hooks.py
#
# Copyright 2018 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import sys
import importlib


class ProxyImporter(object):
    def __init__(self, source_name, target_name):
        self._source_name = source_name
        self._target_name = target_name
        self._callback = lambda s, t: None
        self._excl_pred = lambda s, n: False
        self._dotted_source = '{}.'.format(source_name)
        self._skip = set()

    def exclude_predicate(self, func):
        self._excl_pred = func

    def callback(self, func):
        self._callback = func

    def find_module(self, name, path=None):
        if (
            not name.startswith(self._dotted_source) or self._excl_pred(self._source_name, name) or
            name in self._skip
        ):
            return None
        self._skip.add(name)
        return self

    def load_module(self, fullname):
        target_name = fullname
        if fullname in sys.modules:
            return sys.modules[fullname]
        _, submodule = fullname.split('.', 1)
        target_name = '{}.{}'.format(self._target_name, submodule)
        target_mod = importlib.import_module(target_name)
        sys.modules[fullname] = target_mod
        self._callback(fullname, target_name)
        self._skip.remove(fullname)
        return target_mod


class ModuleWrapper(object):
    def __init__(self, wrapped, target, callback=None):
        self._wrapped = wrapped
        self._target = target
        if callback is None:
            callback = lambda s, t, n: None
        self._callback = callback

    def __call__(self):
        sys.modules[self._wrapped.__name__] = self

    def __getattr__(self, name):
        try:
            return getattr(self._wrapped, name)
        except AttributeError:
            attr = getattr(self._target, name)
            self._callback(self._wrapped.__name__, self._target.__name__, name)
            return attr
