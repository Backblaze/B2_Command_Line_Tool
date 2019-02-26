######################################################################
#
# File: b2/import_hooks.py
#
# Copyright 2018 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
"""
This module contains the logic for handling the import of absent modules.
The ProxyImporter class acts as an import hook and proxies imports to
some target package. ModuleWrapper acts as a wrapper for module objects
to be able to handle missing attributes and get them from some target
module.
"""

import sys
import importlib


class ProxyImporter(object):
    """
    This import hook works as an import proxy between two packages.
    When it's added to sys.meta_path and one wants to import from the
    package set as a source, then the hook actually imports from 
    the target package.
    """

    def __init__(self, source_name, target_name):
        """
        :param source_name: str, source package name
        :param target_name: str, target package name
        """
        self._source_name = source_name
        self._target_name = target_name
        self._callback = lambda s, t: None
        self._excl_pred = lambda s, n: False
        self._dotted_source = '{0}.'.format(source_name)
        self._skip = set()

    def exclude_predicate(self, func):
        """
        Set exclude predicate

        :param func: callable, must be a callable with the following signature:
        (source_name: str, name: str) -> bool, where 'source_name' is the name of
        a source package, and 'name' is a name of an arbitrary module being checked for exclusion.
        The callable must determine whether the module must be excluded and not handled 
        by this import hook and return the appropriate boolean value. 
        True if it must be excluded, False otherwise.
        """
        self._excl_pred = func

    def callback(self, func):
        """
        Set a callback to be executed when a module from source package
        was successfully proxied to a target package

        :param func: callable, must be a callable with the following signature:
        (source_name: str, target_name: str) -> None, where 'source_name' is the name
        of a source module, and 'target_name' is the name of a target module, which
        was imported instead of the source.
        """
        self._callback = func

    def find_module(self, name, path=None):
        """
        Find a loader for a module

        :param name: str, module name
        :param path: contains parent package's __path__ attribute's value
        :return: loader instance
        """
        if (
            not name.startswith(self._dotted_source) or self._excl_pred(self._source_name, name) or
            name in self._skip
        ):
            return None
        self._skip.add(name)
        return self

    def load_module(self, fullname):
        """
        Load module

        :param fullname: str, a module name to load
        :return: loaded module object
        """
        target_name = fullname
        if fullname in sys.modules:
            return sys.modules[fullname]
        _, submodule = fullname.split('.', 1)
        target_name = '{0}.{1}'.format(self._target_name, submodule)
        target_mod = importlib.import_module(target_name)
        sys.modules[fullname] = target_mod
        self._callback(fullname, target_name)
        self._skip.remove(fullname)
        return target_mod


class ModuleWrapper(object):
    """
    Wrapper for module objects
    """

    def __init__(self, wrapped, target, callback=None):
        """
        :param wrapped: object, a module object to wrap
        :param target: object, a target module object
        to get attributes from, if wrapped module doesn't
        have requested attributes
        :param callback: callable, a callback to be executed
        when a missing attribute was succesfully gotten from
        the target module. It must have the following signature:
        (wrapped_name: str, target_name: str, name: str) -> None,
        where 'wrapped_name' is a name of the wrapped module,
        'target_name' is a name of target module, 'name' is a name
        of an atribute being accessed.
        """
        self._wrapped = wrapped
        self._target = target
        if callback is None:
            callback = lambda s, t, n: None
        self._callback = callback

    def __call__(self):
        """
        Insert wrapped module to the module cache
        """
        sys.modules[self._wrapped.__name__] = self

    def __getattr__(self, name):
        try:
            return getattr(self._wrapped, name)
        except AttributeError:
            attr = getattr(self._target, name)
            self._callback(self._wrapped.__name__, self._target.__name__, name)
            return attr
