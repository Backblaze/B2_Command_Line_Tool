######################################################################
#
# File: b2/bounded_queue_executor.py
#
# Copyright 2018 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import threading


class BoundedQueueExecutor(object):
    """
    Wraps a futures.Executor and limits the number of requests that
    can be queued at once.  Requests to submit() tasks block until
    there is room in the queue.

    The number of available slots in the queue is tracked with a
    semaphore that is acquired before queueing an action, and
    released when an action finishes.

    Counts the number of exceptions thrown by tasks, and makes them
    available from get_num_exceptions() after shutting down.
    """

    def __init__(self, executor, queue_limit):
        self.executor = executor
        self.semaphore = threading.Semaphore(queue_limit)
        self.num_exceptions = 0

    def submit(self, fcn, *args, **kwargs):
        # Wait until there is room in the queue.
        self.semaphore.acquire()

        # Wrap the action in a function that will release
        # the semaphore after it runs.
        def run_it():
            try:
                return fcn(*args, **kwargs)
            except Exception:
                self.num_exceptions += 1
                raise
            finally:
                self.semaphore.release()

        # Submit the wrapped action.
        return self.executor.submit(run_it)

    def shutdown(self):
        self.executor.shutdown()

    def get_num_exceptions(self):
        return self.num_exceptions
