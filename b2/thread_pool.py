######################################################################
#
# File: b2/thread_pool.py
#
# Copyright 2016 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from __future__ import print_function

import threading
from six.moves import range
from six.moves.queue import Queue

# this is added to the task queue to tell the threads to stop
SHUT_DOWN_TOKEN = object()


class ThreadInThreadPool(threading.Thread):
    """
    Runs tasks from the queue until getting a SHUT_DOWN_TOKEN.
    """

    def __init__(self, pool, queue):
        super(ThreadInThreadPool, self).__init__()
        self.pool = pool
        self.queue = queue

    def run(self):
        while True:
            task = self.queue.get()
            if task is SHUT_DOWN_TOKEN:
                return
            try:
                task.run()
            except Exception as e:
                self.pool._add_exception((task, e))


class ThreadPool(object):
    """
    Simple fixed-size thread pool.  Runs task objects that have a run() method.

    Catches exceptions thrown by the tasks, and keeps a list of them for the
    master to check.

    The queue for tasks is unbounded, which means that if you queue a lot of
    tasks, it will use a lot of memory.

    Usage:
        pool = ThreadPool(thread_count=10, queue_capacity=1000)
        pool.add_task(new MyTask())
        pool.add_task(new MyTask())
        pool.join_all()
        errors = pool.get_exceptions()

    Once you call join_all() you may not add any more tasks.
    """

    def __init__(self, thread_count=10, queue_capacity=1000):
        """
        Initializes a new thread pool, starts the threads running,
        and gets ready to accept tasks.
        """
        self._queue = Queue(maxsize=queue_capacity)
        self._threads = [self._start_thread() for i in range(thread_count)]
        self._lock = threading.Lock()  # controls self.exceptions
        self._exceptions = []

    def add_task(self, task):
        """
        Adds a task to the queue, blocking until there is room.
        """
        self._queue.put(task)

    def join_all(self):
        """
        Waits until all tasks are completed and then shuts down the threads.
        """
        # Tell each of the threads to stop.  Each thread will consume
        # one of the tokens and then exit.
        for _ in self._threads:
            self._queue.put(SHUT_DOWN_TOKEN)

        # Nobody can add any tasks any more.
        self._queue = None

        # Wait for the threads to be done.
        for thread in self._threads:
            thread.join()
        self._threads = None

    def get_exceptions(self):
        """
        Returns a list of all of the exceptions that happened.
        The list contains two-tuples: (task, exception)
        """
        with self._lock:
            return self._exceptions

    def _add_exception(self, exception):
        """
        Called by a thread to add an exception to the list.
        """
        with self._lock:
            self._exceptions.append(exception)

    def _start_thread(self):
        """
        Starts one thread that runs until it gets a SHUT_DOWN_TOKEN.
        """
        thread = ThreadInThreadPool(self, self._queue)
        thread.start()
        return thread
