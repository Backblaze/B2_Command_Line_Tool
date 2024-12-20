######################################################################
#
# File: b2/_internal/_utils/python_compat.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
"""
Utilities for compatibility with older Python versions.
"""

import sys

if sys.version_info < (3, 9):

    def removeprefix(s: str, prefix: str) -> str:
        return s[len(prefix) :] if s.startswith(prefix) else s

else:
    removeprefix = str.removeprefix
