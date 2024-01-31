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

_MISSING = object()


def skip_on_windows(*args, reason='Not supported on Windows', **kwargs):
    return pytest.mark.skipif(
        platform.system() == 'Windows',
        reason=reason,
    )(*args, **kwargs)


def b2_uri_args_v3(bucket_name, path=_MISSING):
    if path is _MISSING:
        return [bucket_name]
    else:
        return [bucket_name, path]


def b2_uri_args_v4(bucket_name, path=_MISSING):
    if path is _MISSING:
        path = ''
    return [f'b2://{bucket_name}/{path}']
