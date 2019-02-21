######################################################################
#
# File: test/test_proxy_importer.py
#
# Copyright 2018 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import sys
import os.path
fixtures_dir = os.path.join(os.path.dirname(__file__), 'fixtures')
sys.path.append(fixtures_dir)

import six
import unittest
from b2.proxy_importer import ProxyImporter

if six.PY2:
    ModuleNotFoundError = ImportError

class TestProxyImporter(unittest.TestCase):
    def _del_mod(self, mod_name):
        for key in list(globals().keys()):
            if key == mod_name or key.startswith(mod_name + '.'):
                try:
                    del globals()[key]
                except KeyError:
                    pass
        for key in list(sys.modules):
            if key == mod_name or key.startswith(mod_name + '.'):
                try:
                    del sys.modules[key]
                except KeyError:
                    pass

    @classmethod
    def setUpClass(cls):
        if six.PY2:
            cls.assertRaisesRegex = cls.assertRaisesRegexp

    def setUp(self):
        self._del_mod('test_source_mod')
        self._del_mod('test_target_mod')

    def tearDown(self):
        for i, p in enumerate(sys.meta_path):
            if isinstance(sys.meta_path[0], ProxyImporter):
                del sys.meta_path[i]

    def test_find_module_skip_modules_other_than_source_submodules(self):
        importer = ProxyImporter('test_source_mod', 'test_target_mod')
        self.assertIsNone(importer.find_module(''))
        importer._skip = set()
        self.assertIsNone(importer.find_module('test_target_mod'))
        importer._skip = set()
        self.assertIsNone(importer.find_module('logging'))
        importer._skip = set()
        self.assertIsNone(importer.find_module('test_source_mod'))
        importer._skip = set()
        self.assertIsNotNone(importer.find_module('test_source_mod.c'))
        importer._skip = set()
        self.assertIsNone(importer.find_module('test_source_mod1'))
        importer._skip = set()
        self.assertIsNone(importer.find_module('atest_source_mod1'))
        importer._skip = set()
        self.assertIsNone(importer.find_module('atest_source_mod'))
        importer._skip = set()

    def test_find_module_exclude_predicate(self):
        importer = ProxyImporter('test_source_mod', 'test_target_mod')
        self.assertIsNotNone(importer.find_module('test_source_mod.c'))

        @importer.exclude_predicate
        def excl_pred(source_name, fullname):
            return fullname == 'test_source_mod.c'

        self.assertIsNone(importer.find_module('test_source_mod.c'))

    def test_load_module_raise_import_error_is_source_does_not_exist(self):
        sys.meta_path.insert(0, ProxyImporter('some_mod', 'test_target_mod'))
        self.assertNotIn('some_mod', sys.modules)
        self.assertNotIn('test_target_mod', sys.modules)
        with self.assertRaises(ModuleNotFoundError):
            import some_mod
            # to prevent pyflakes from complaining
            some_mod
        self.assertNotIn('some_mod', sys.modules)
        self.assertNotIn('test_target_mod', sys.modules)

    def test_load_module_target_module_imported_successfully(self):
        sys.meta_path.insert(0, ProxyImporter('test_source_mod', 'test_target_mod'))
        import test_target_mod.a

        self.assertIn('test_target_mod', sys.modules)
        self.assertIn('test_target_mod.a', sys.modules)
        self.assertEqual(test_target_mod.__name__, 'test_target_mod')
        self.assertEqual(test_target_mod.a.__name__, 'test_target_mod.a')
        self.assertEqual(test_target_mod.a.f(3), 3)

    def test_load_module_source_name_is_empty_string(self):
        sys.meta_path.insert(0, ProxyImporter('', 'test_target_mod'))
        import test_target_mod.a

        self.assertIn('test_target_mod', sys.modules)
        self.assertIn('test_target_mod.a', sys.modules)
        self.assertEqual(test_target_mod.__name__, 'test_target_mod')
        self.assertEqual(test_target_mod.a.__name__, 'test_target_mod.a')
        self.assertEqual(test_target_mod.a.f(3), 3)

    def test_load_module_source_module_does_not_exist(self):
        sys.meta_path.insert(0, ProxyImporter('non_existent_module', 'test_target_mod'))
        with self.assertRaisesRegex(ModuleNotFoundError, 'non_existent_module'):
            import non_existent_module.a
            # to prevent pyflakes from complaining
            non_existent_module.a

        self.assertNotIn('test_target_mod', sys.modules)
        self.assertNotIn('test_target_mod.a', sys.modules)
        self.assertNotIn('non_existent_module', sys.modules)
        self.assertNotIn('non_existent_module.a', sys.modules)

    def test_load_module_target_module_does_not_exist(self):
        sys.meta_path.insert(0, ProxyImporter('test_source_mod', 'non_existent_module'))

        with self.assertRaisesRegex(ModuleNotFoundError, 'non_existent_module'):
            import test_source_mod.a
            # to prevent pyflakes from complaining
            test_source_mod.a

        self.assertIn('test_source_mod', sys.modules)
        self.assertNotIn('test_source_mod.a', sys.modules)
        self.assertNotIn('non_existent_module', sys.modules)
        self.assertNotIn('non_existent_module.a', sys.modules)

    def test_load_module_replace_all_submodules(self):
        sys.meta_path.insert(0, ProxyImporter('test_source_mod', 'test_target_mod'))
        import test_source_mod.a.b

        self.assertIn('test_source_mod', sys.modules)
        self.assertIn('test_source_mod.a', sys.modules)
        self.assertIn('test_source_mod.a.b', sys.modules)
        self.assertIn('test_target_mod', sys.modules)
        self.assertIn('test_target_mod.a', sys.modules)
        self.assertIn('test_target_mod.a.b', sys.modules)
        self.assertEqual(test_source_mod.__name__, 'test_source_mod')
        self.assertEqual(test_source_mod.a.__name__, 'test_target_mod.a')
        self.assertEqual(test_source_mod.a.b.__name__, 'test_target_mod.a.b')
        self.assertEqual(test_source_mod.a.b.g(13), 14)

        with self.assertRaises(ModuleNotFoundError):
            import test_source_mod.c.d

        self.assertIn('test_source_mod', sys.modules)
        self.assertIn('test_source_mod.c', sys.modules)
        self.assertNotIn('test_source_mod.c.d', sys.modules)
        self.assertEqual(test_source_mod.c.__name__, 'test_target_mod.c')

        import test_source_mod.z
        self.assertIn('test_source_mod.z', sys.modules)
        self.assertIn('test_target_mod.z', sys.modules)
        self.assertEqual(test_source_mod.z.f(), 'from target')

    def test_load_module_raise_import_error_if_target_submodule_does_not_exist(self):
        sys.meta_path.insert(0, ProxyImporter('test_source_mod', 'test_target_mod'))
        with self.assertRaises(ModuleNotFoundError):
            import test_source_mod.e
            # to prevent pyflakes from complaining
            test_source_mod.e
        self.assertIn('test_source_mod', sys.modules)
        self.assertIn('test_target_mod', sys.modules)
        self.assertNotIn('test_source_mod.e', sys.modules)
        self.assertNotIn('test_target_mod.e', sys.modules)

    def test_load_module_nonexistent_with_the_same_prefix_as_source(self):
        sys.meta_path.insert(0, ProxyImporter('test_source_mod1', 'test_target_mod'))
        with self.assertRaisesRegex(ModuleNotFoundError, 'test_source_mod1'):
            import test_source_mod1.a
            # to prevent pyflakes from complaining
            test_source_mod1.a

        self.assertNotIn('test_source_mod1', sys.modules)
        self.assertNotIn('test_source_mod1.a', sys.modules)
        self.assertNotIn('test_target_mod', sys.modules)
        self.assertNotIn('test_target_mod.a', sys.modules)

    def test_load_module_importer_callback(self):
        res = {}
        importer = ProxyImporter('test_source_mod', 'test_target_mod')

        @importer.callback
        def cb(orig_name, target_name):
            res['orig_name'] = orig_name
            res['target_name'] = target_name

        sys.meta_path.insert(0, importer)

        import test_source_mod.a.b
        # to prevent pyflake from complaining
        test_source_mod.a.b

        self.assertEqual(
            res, {
                'orig_name': 'test_source_mod.a.b',
                'target_name': 'test_target_mod.a.b'
            }
        )

    def test_load_module_importer_callback_not_called(self):
        res = {}
        importer = ProxyImporter('test_source_mod', 'test_target_mod')

        @importer.callback
        def cb(orig_name, target_name):
            res['orig_name'] = orig_name
            res['target_name'] = target_name

        sys.meta_path.insert(0, importer)

        import test_target_mod.a.b
        # to prevent pyflake from complaining
        test_target_mod.a.b

        self.assertEqual(res, {})


if __name__ == '__main__':
    unittest.main()
