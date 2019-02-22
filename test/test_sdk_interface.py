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
import pkgutil
import unittest
import warnings
warnings.simplefilter('always', DeprecationWarning)

import importlib
import b2_sdk


def list_modules(package, exclude=None):
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


if six.PY2:
    import operator

    def accumulate(iterable, func=operator.__add__):
        it = iter(iterable)
        res = next(it)
        yield res
        for val in it:
            res =  func(res, val)
            yield res

else:
    from itertools import accumulate


class TestSdkImports(unittest.TestCase):
    def _del_mod(self, name):
        mods = accumulate(name.split('.'), func=lambda a, v: '{}.{}'.format(a, v))
        next(mods)
        for mod_name in mods:
            try:
                del sys.modules[mod_name]
            except KeyError:
                pass

    @classmethod
    def setUpClass(cls):
        cls.sdk_modules = list_modules(b2_sdk, exclude={'b2_sdk.version', 'b2_sdk.utils'})
        cls.cli_modules = [m.replace('b2_sdk.', 'b2.') for m in cls.sdk_modules]

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

    def test_import_existing_utils_functions(self):
        with warnings.catch_warnings(record=True) as w:
            from b2.utils import current_time_millis
            self.assertEqual(len(w), 0)
        with warnings.catch_warnings(record=True) as w:
            from b2.utils import set_shutting_down
            self.assertEqual(len(w), 0)
        self.assertEquals(type(current_time_millis).__name__, 'function')
        self.assertEquals(type(set_shutting_down).__name__, 'function')

    def test_import_utils_functions_from_sdk(self):
        sdk_utils = importlib.import_module('b2_sdk.utils')
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
