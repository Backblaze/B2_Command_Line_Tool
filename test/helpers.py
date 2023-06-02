######################################################################
#
# File: test/helpers.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
import platform

import pytest


def skip_on_windows(*args, reason='Not supported on Windows', **kwargs):
    return pytest.mark.skipif(
        platform.system() == 'Windows',
        reason=reason,
    )(*args, **kwargs)
