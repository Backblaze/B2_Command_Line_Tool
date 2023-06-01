######################################################################
#
# File: test/unit/helpers.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
import threading


def run_in_background(func, *args, **kwargs) -> threading.Thread:
    thread = threading.Thread(target=func, args=args, kwargs=kwargs)
    thread.start()
    return thread
