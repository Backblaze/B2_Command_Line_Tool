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


def auto_content_type(resolved: str) -> str:
    """
    The content type reported for a file uploaded with ``b2/x-auto``.

    B2 picks the type from the file name and stores that ("automatically set the stored
    Content-Type", b2_upload_file; b2-simulator ledger F-78, F-137). RawSimulator stores
    ``b2/x-auto`` itself.
    """
    from .b2_simulator import b2_simulator_enabled

    return resolved if b2_simulator_enabled() else 'b2/x-auto'
