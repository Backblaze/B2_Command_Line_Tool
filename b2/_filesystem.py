#!/usr/bin/env python3
######################################################################
#
# File: b2/_filesystem.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
import os
import stat


def points_to_fifo(path: str) -> bool:
    path = os.path.realpath(path)
    try:
        return stat.S_ISFIFO(os.stat(path).st_mode)
    except OSError:
        return False
