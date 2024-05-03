######################################################################
#
# File: b2/_internal/b2v3/sync.py
#
# Copyright 2024 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from b2._internal.b2v4.registry import Sync as SyncV4


class Sync(SyncV4):
    __doc__ = SyncV4.__doc__
    FAIL_ON_REPORTER_ERRORS_OR_WARNINGS = False
