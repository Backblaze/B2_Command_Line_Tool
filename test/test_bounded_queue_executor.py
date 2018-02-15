######################################################################
#
# File: test_bounded_queue_executor.py
#
# Copyright 2018 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from __future__ import print_function

import six

import time

from .test_base import TestBase
from b2.bounded_queue_executor import BoundedQueueExecutor

try:
    import concurrent.futures as futures
except ImportError:
    import futures


class TestBoundedQueueExecutor(TestBase):
    def setUp(self):
        unbounded_executor = futures.ThreadPoolExecutor(max_workers=1)
        self.executor = BoundedQueueExecutor(unbounded_executor, 1)

    def tearDown(self):
        self.executor.shutdown()

    def test_return_future(self):
        future_1 = self.executor.submit(lambda: 1)
        print(future_1)
        self.assertEqual(1, future_1.result())

    def test_blocking(self):
        # This doesn't actually test that it waits, but it does exercise the code.

        # Make some futures using a function that takes a little time.
        def sleep_and_return_fcn(n):
            def fcn():
                time.sleep(0.01)
                return n

            return fcn

        futures = [self.executor.submit(sleep_and_return_fcn(i)) for i in six.moves.range(10)]

        # Check the answers
        answers = list(six.moves.map(lambda f: f.result(), futures))
        self.assertEqual(list(six.moves.range(10)), answers)

    def test_no_exceptions(self):
        f = self.executor.submit(lambda: 1)
        self.executor.shutdown()
        self.assertEqual(0, self.executor.get_num_exceptions())
        self.assertTrue(f.exception() is None)

    def test_two_exceptions(self):
        def thrower():
            raise Exception('test_exception')

        f1 = self.executor.submit(thrower)
        f2 = self.executor.submit(thrower)
        self.executor.shutdown()
        self.assertEqual(2, self.executor.get_num_exceptions())
        self.assertFalse(f1.exception() is None)
        self.assertEqual('test_exception', str(f2.exception()))
