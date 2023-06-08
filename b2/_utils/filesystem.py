######################################################################
#
# File: b2/_utils/filesystem.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
import stat
from pathlib import Path

# to be used in open(..., buffering=RECOMMENDED_READ_BUF_SIZE)
RECOMMENDED_READ_BUF_SIZE = 1 * 1024 * 1024


def points_to_fifo(path: Path) -> bool:
    path = path.resolve()
    try:

        return stat.S_ISFIFO(path.stat().st_mode)
    except OSError:
        return False
