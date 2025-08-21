######################################################################
#
# File: test/unit/_cli/unpickle.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
import importlib
import io
import pickle
import sys
from typing import Any


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
            raise RuntimeError('Loading the pickled object imported b2sdk module')
        return result

    def find_class(self, module: str, name: str) -> Any:
        self._modules_to_load.add(module)
        return super().find_class(module, name)


def unpickle(data: bytes) -> Any:
    """Unpickling function that raises RuntimeError if unpickled
    object depends on b2sdk."""
    return Unpickler(io.BytesIO(data)).load()
