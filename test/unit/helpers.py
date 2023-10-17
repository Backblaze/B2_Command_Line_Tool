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


class RunOrDieExecutor(concurrent.futures.ThreadPoolExecutor):
    """
    Deadly ThreadPoolExecutor, which ensures all task are quickly closed before exiting.

    Only really usable in tests.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._futures = []

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self.shutdown(wait=False, cancel_futures=True)
        except TypeError:  # Python <3.9
            self.shutdown(wait=False)
            for future in self._futures:
                future.cancel()
        return super().__exit__(exc_type, exc_val, exc_tb)

    def submit(self, *args, **kwargs):  # to be removed when Python 3.9 is minimum
        future = super().submit(*args, **kwargs)
        self._futures.append(future)
        return future
