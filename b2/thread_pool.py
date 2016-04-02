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

from six.moves.queue import Queue
from six.moves import range

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
                self.queue.task_done()
                return
            try:
                task.run()
            except Exception as e:
                self.pool._add_exception((task, e))
            self.queue.task_done()


class ThreadPool(object):
    """
    Simple fixed-size thread pool.  Runs task objects that have a run() method.

    Catches exceptions thrown by the tasks, and keeps a list of them for the
    master to check.

    The queue for tasks is unbounded, which means that if you queue a lot of
    tasks, it will use a lot of memory.

    Usage:
        pool = ThreadPool(thread_count=10, queue_capacity=1000)
        pool.add_task(MyTask(1))
        pool.add_task(MyTask(2))
        pool.join()

        pool.add_task(MyTask(3))
        pool.join()

        pool.shut_down()

        errors = pool.get_exceptions()

    This class is THREAD SAFE, so that tasks can add more tasks as they are running.
    """

    def __init__(self, thread_count=10, queue_capacity=1000):
        """
        Initializes a new thread pool, starts the threads running,
        and gets ready to accept tasks.
        """
        self._lock = threading.Lock()  # controls all state
        self._queue = Queue(maxsize=queue_capacity)
        self._threads = [self._start_thread() for i in range(thread_count)]
        self._exceptions = []

    def add_task(self, task):
        """
        Adds a task to the queue, blocking until there is room.
        """
        with self._lock:
            self._queue.put(task)

    def join(self):
        """
        Waits until all tasks are completed and then shuts down the threads.
        """
        # grab the queue in a thread-safe way
        with self._lock:
            queue = self._queue

        # wait while not holding the lock, so tasks can add more tasks.
        queue.join()

    def get_exceptions(self):
        """
        Returns a list of all of the exceptions that happened.
        The list contains two-tuples: (task, exception)
        """
        with self._lock:
            return self._exceptions

    def shut_down(self):
        # Nobody can add any tasks any more.
        with self._lock:
            queue = self._queue
            self._queue = None

        # Tell each of the threads to stop.  Each thread will consume
        # one of the tokens and then exit.
        for _ in self._threads:
            queue.put(SHUT_DOWN_TOKEN)

        # Wait for the threads to finish
        for t in self._threads:
            t.join()

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
