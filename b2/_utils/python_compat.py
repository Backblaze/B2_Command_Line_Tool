######################################################################
#
# File: b2/_utils/python_compat.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
"""
Utilities for compatibility with older Python versions.
"""
import functools
import sys

if sys.version_info < (3, 9):

    def removeprefix(s: str, prefix: str) -> str:
        return s[len(prefix):] if s.startswith(prefix) else s

else:
    removeprefix = str.removeprefix

if sys.version_info < (3, 8):

    class singledispatchmethod:
        """
        singledispatchmethod backport for Python 3.7.

        There are no guarantees for its completeness.
        """

        def __init__(self, method):
            self.dispatcher = functools.singledispatch(method)
            self.method = method

        def register(self, cls, method=None):
            return self.dispatcher.register(cls, func=method)

        def __get__(self, obj, cls):
            @functools.wraps(self.method)
            def method_wrapper(arg, *args, **kwargs):
                method_desc = self.dispatcher.dispatch(arg.__class__)
                return method_desc.__get__(obj, cls)(arg, *args, **kwargs)

            method_wrapper.register = self.register
            return method_wrapper
else:
    singledispatchmethod = functools.singledispatchmethod
