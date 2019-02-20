import os
import re
import sys
import six
import importlib
import warnings


if six.PY2:
    import operator
    from contextlib import contextmanager

    ModuleNotFoundError = ImportError

    def accumulate(iterable, func=operator.__add__):
        it = iter(iterable)
        res = next(it)
        yield res
        for element in it:
            res = func(res, element)
            yield res
    
    @contextmanager
    def suppress(*exceptions):
        try:
            yield
        except exceptions:
            pass
else:
    from itertools import accumulate
    from contextlib import suppress


class ProxyImporter(object):
    def __init__(self, source_name, target_name):
        self._source_name = source_name
        self._target_name = target_name
        self._callback = lambda s, t: None
        self._dotted_source = '{}.'.format(source_name)
        self._skip = set()

    def _module_exists(self, fullname):
        all_modules = list(accumulate(
            fullname.split('.'), 
            func=lambda a, v: '{}.{}'.format(a, v)
        ))
        imported_modules = {mod for mod in all_modules if mod in sys.modules}
        try:
            mod = importlib.import_module(fullname)
            # slow?
            re_str = '(^|{sep}){module}{sep}'.format(
                module=all_modules[0], 
                sep=re.escape(os.sep)
            )
            result = bool(re.search(re_str, mod.__file__))
        except ModuleNotFoundError:
            result = False
        finally:
            # cleanup
            for module in all_modules:
                if module in imported_modules:
                    continue
                with suppress(KeyError):
                    del sys.modules[module]
        return result

    def callback(self, func):
        self._callback = func

    def find_module(self, name, path=None):
        is_not_source = (
            not name.startswith(self._dotted_source) and
            name != self._source_name
        )
        if name in self._skip or is_not_source:
            return None
        self._skip.add(name)
        return self

    def load_module(self, fullname):
        orig_name = fullname
        target_imported = False
        if not self._module_exists(fullname) and fullname.startswith(self._dotted_source):
            _, submodule = fullname.split('.', 1)
            fullname = '{}.{}'.format(self._target_name, submodule)
            target_imported = True
        module = importlib.import_module(fullname)
        if target_imported:
            self._callback(orig_name, fullname)
            sys.modules[orig_name] = module
        self._skip.remove(orig_name)
        return module
