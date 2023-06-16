######################################################################
#
# File: test/unit/test_base.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import re
import unittest
from contextlib import contextmanager


class TestBase(unittest.TestCase):
    @contextmanager
    def assertRaises(self, exc, msg=None):
        try:
            yield
        except exc as e:
            if msg is not None:
                if msg != str(e):
                    assert False, f"expected message '{msg}', but got '{str(e)}'"
        else:
            assert False, f'should have thrown {exc}'

    @contextmanager
    def assertRaisesRegexp(self, expected_exception, expected_regexp):
        try:
            yield
        except expected_exception as e:
            if not re.search(expected_regexp, str(e)):
                assert False, f"expected message '{expected_regexp}', but got '{str(e)}'"
        else:
            assert False, f'should have thrown {expected_exception}'
