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
    def __init__(self, index, counter):
        self.index = index
        self.counter = counter

    def run(self):
        if random.randint(1, 5) == 3:
            time.sleep(0.05)
        new_count = self.counter.increment()
        if new_count == 5:
            raise Exception('five')

    def __repr__(self):
        return 'TestTask(%d)' % (self.index,)


class TestThreadPool(unittest.TestCase):
    def test_it(self):
        pool = ThreadPool(3, 4)
        counter = Counter()
        for i in range(20):
            pool.add_task(TestTask(i, counter))
        pool.join_all()
        exceptions = pool.get_exceptions()
        self.assertEqual(20, counter.get())
        self.assertEqual(1, len(exceptions))
        self.assertEqual(('five',), exceptions[0][1].args)
