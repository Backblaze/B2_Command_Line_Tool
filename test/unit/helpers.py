######################################################################
#
# File: test/unit/helpers.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
import concurrent.futures
import sys


class RunOrDieExecutor(concurrent.futures.ThreadPoolExecutor):
    """
    Deadly ThreadPoolExecutor, which ensures all task are quickly closed before exiting.

    Only really usable in tests.
    """

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.shutdown(wait=False, cancel_futures=True)
        return super().__exit__(exc_type, exc_val, exc_tb)

    if sys.version_info < (3, 9):  # shutdown(cancel_futures=True) is Python 3.9+

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self._futures = []

        def shutdown(self, wait=True, cancel_futures=False):
            if cancel_futures:
                for future in self._futures:
                    future.cancel()
            super().shutdown(wait=wait)

        def submit(self, *args, **kwargs):
            future = super().submit(*args, **kwargs)
            self._futures.append(future)
            return future
