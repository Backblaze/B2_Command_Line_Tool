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

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.shutdown(wait=False, cancel_futures=True)
        return super().__exit__(exc_type, exc_val, exc_tb)
