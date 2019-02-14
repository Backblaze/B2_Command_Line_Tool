######################################################################
#
# File: utils.py
#
# Copyright 2018 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import time

# Global variable that says whether the app is shutting down
_shutting_down = False


def set_shutting_down():
    global _shutting_down
    _shutting_down = True


def current_time_millis():
    """
    File times are in integer milliseconds, to avoid roundoff errors.
    """
    return int(round(time.time() * 1000))
