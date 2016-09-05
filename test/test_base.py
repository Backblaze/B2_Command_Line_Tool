from contextlib import contextmanager
import unittest


class TestBase(unittest.TestCase):
    @contextmanager
    def assertRaises(self, exc):
        try:
            yield
        except exc:
            pass
        else:
            assert False, 'should have thrown %s' % (exc,)
