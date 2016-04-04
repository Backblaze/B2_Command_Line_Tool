######################################################################
#
# File: b2/thread_pool.py
#
# Copyright 2016 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import random
import threading
import time
import unittest

from b2.thread_pool import ThreadPool
from six.moves import range


class Counter(object):
    def __init__(self):
        self.lock = threading.Lock()
        self.count = 0

    def increment(self):
        with self.lock:
            self.count += 1
            return self.count

    def get(self):
        with self.lock:
            return self.count


class TestTask(object):
    """
    A simple task that increments a counter.

    Also, adds another task to the queue if this
    task has an even-numbered index.
    """

    def __init__(self, pool, index, counter):
        self.pool = pool
        self.index = index
        self.counter = counter

    def run(self):
        if random.randint(1, 5) == 3:
            time.sleep(0.05)
        if self.index % 2 == 0:
            self.pool.add_task(TestTask(None, self.index + 1, self.counter))
        new_count = self.counter.increment()
        if new_count == 5:
            raise Exception('five')

    def __repr__(self):
        return 'TestTask(%d)' % (self.index,)


class TestThreadPool(unittest.TestCase):
    def test_it(self):
        pool = ThreadPool(3, 4)
        counter = Counter()
        for i in range(0, 10, 2):
            pool.add_task(TestTask(pool, i, counter))
        pool.join()

        for i in range(10, 20, 2):
            pool.add_task(TestTask(pool, i, counter))
        pool.join()

        pool.shut_down()
        exceptions = pool.get_exceptions()
        self.assertEqual(20, counter.get())
        self.assertEqual(1, len(exceptions))
        self.assertEqual(('five',), exceptions[0][1].args)
