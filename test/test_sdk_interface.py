######################################################################
#
# File: test/test_sdk_interface.py
#
# Copyright 2018 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import imp
import sys
import six
import types
import pkgutil
import unittest
import warnings
warnings.simplefilter('always', DeprecationWarning)

import importlib
import b2sdk


def list_modules(package, exclude=None):
    """
    Get a list of modules in a package

    :param package: object, a module object
    :param exclude: set, a set of modules to
    exclude from the resulting list
    :return: list, a list of modules of a package
    """
    if exclude is None:
        exclude = set()
    res = []
    if not hasattr(package, '__path__'):
        return res
    base_name = package.__name__
    for _, m, _ in pkgutil.iter_modules(package.__path__):
        if m.startswith('__'):
            continue
        fullname = '{}.{}'.format(base_name, m)
        mod = importlib.import_module(fullname)
        del sys.modules[fullname]
        res.append(fullname)
        res += list_modules(mod)
    return [m for m in res if m not in exclude]


def _dir(m):
    """
    Get a list of attributes of an object, excluding
    the ones starting with underscore

    :param m: object, an object to get attributes from
    :return: list, a list of attributes as strings
    """
    return [a for a in dir(m) if not a.startswith('_')]


if six.PY2:
    """
    Shim for Python 2.x
    """
    import operator

    def accumulate(iterable, func=operator.__add__):
        it = iter(iterable)
        res = next(it)
        yield res
        for val in it:
            res = func(res, val)
            yield res

else:
    from itertools import accumulate


class TestSdkImports(unittest.TestCase):
    def _del_mod(self, name):
        """
        Remove a module with the given name from module cache
        """
        mods = accumulate(name.split('.'), func=lambda a, v: '{}.{}'.format(a, v))
        next(mods)
        for mod_name in mods:
            try:
                del sys.modules[mod_name]
            except KeyError:
                pass

    @classmethod
    def setUpClass(cls):
        cls.sdk_modules = list_modules(b2sdk, exclude={'b2sdk.version', 'b2sdk.utils'})
        cls.cli_modules = [m.replace('b2sdk.', 'b2.') for m in cls.sdk_modules]
        # create a list of all attributes of all sdk modules
        cls.attributes = {}
        for mod_name in cls.sdk_modules:
            cls.attributes[mod_name] = []
            mod = importlib.import_module(mod_name)
            for attr_name in _dir(mod):
                if not isinstance(getattr(mod, attr_name), types.ModuleType):
                    cls.attributes[mod_name].append(attr_name)
        # add the importer manually, as nosetest does not respect sys.meta_path content for some reason
        from b2.import_hooks import ProxyImporter
        importer_found = False
        for importer in sys.meta_path:
            if isinstance(importer, ProxyImporter):
                importer_found = True
                break
        if not importer_found:
            from b2 import importer
            sys.meta_path.insert(0, importer)

    def tearDown(self):
        for mod in self.sdk_modules + self.cli_modules:
            try:
                del sys.modules[mod]
            except KeyError:
                pass

    def test_import_existing_modules(self):
        for mod_name in ['b2.console_tool', 'b2.utils', 'b2.version']:
            with warnings.catch_warnings(record=True) as w:
                importlib.import_module(mod_name)
                self.assertEqual(len(w), 0)

    def test_import_modules_from_sdk(self):
        for cli_mod_name, sdk_mod_name in zip(self.cli_modules, self.sdk_modules):
            with warnings.catch_warnings(record=True) as w:
                cli_mod = importlib.import_module(cli_mod_name)
                # import second time to make sure there were no more warnings displayed
                importlib.import_module(cli_mod_name)
                sdk_mod = importlib.import_module(sdk_mod_name)
                self.assertEqual(len(w), 1)
                self.assertIn('deprecated', str(w[0].message))
                self.assertTrue(issubclass(w[0].category, DeprecationWarning))
                self.assertEqual(dir(cli_mod), dir(sdk_mod))

    def test_import_all_from_modules(self):
        for cli_mod_name, sdk_mod_name in zip(self.cli_modules, self.sdk_modules):
            with warnings.catch_warnings(record=True) as w:
                code = 'from {0} import *'.format(cli_mod_name)
                cli_mod = imp.new_module('tets1')
                exec(code, cli_mod.__dict__)
                sdk_mod = importlib.import_module(sdk_mod_name)
                self.assertEqual(len(w), 1)
                self.assertIn('deprecated', str(w[0].message))
                self.assertTrue(issubclass(w[0].category, DeprecationWarning))
                self.assertEqual(_dir(cli_mod), _dir(sdk_mod))

    def test_import_attributes_one_by_one(self):
        for sdk_mod_name, attrs in self.attributes.items():
            cli_mod_name = sdk_mod_name.replace('b2sdk.', 'b2.')
            for attr in attrs:
                with warnings.catch_warnings(record=True) as w:
                    code = 'from {0} import {1}'.format(cli_mod_name, attr)
                    cli_mod = imp.new_module('tets1')
                    exec(code, cli_mod.__dict__)
                    sdk_mod = importlib.import_module(sdk_mod_name)
                    self.assertGreaterEqual(len(w), 1)
                    self.assertIn('deprecated', str(w[0].message))
                    self.assertTrue(issubclass(w[0].category, DeprecationWarning))
                    self.assertEqual(getattr(cli_mod, attr), getattr(sdk_mod, attr))
                self._del_mod(cli_mod_name)
                self._del_mod(sdk_mod_name)

    def test_import_existing_utils_functions(self):
        with warnings.catch_warnings(record=True) as w:
            from b2.utils import current_time_millis
            self.assertEqual(len(w), 0)
        self.assertEqual(type(current_time_millis).__name__, 'function')

    def test_import_utils_functions_from_sdk(self):
        sdk_utils = importlib.import_module('b2sdk.utils')
        cli_utils = importlib.import_module('b2.utils')
        attrs = [a for a in dir(sdk_utils) if not a.startswith('_')]

        for attr in attrs:
            # skip attributes which are defined in b2.utils
            if attr in cli_utils._wrapped.__dict__:
                continue
            with warnings.catch_warnings(record=True) as w:
                self.assertTrue(getattr(cli_utils, attr), getattr(sdk_utils, attr))
                self.assertEqual(len(w), 1)
                self.assertIn(attr, str(w[0].message))


if __name__ == '__main__':
    unittest.main()
