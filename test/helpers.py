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


def deep_cast_dict(actual, expected):
    """
    For composite objects `actual` and `expected`, return a copy of `actual` (with all dicts and lists deeply copied)
    with all keys of dicts not appearing in `expected` (comparing dicts on any level) removed. Useful for assertions
    in tests ignoring extra keys.
    """
    if isinstance(expected, dict) and isinstance(actual, dict):
        return {k: deep_cast_dict(actual[k], expected[k]) for k in expected if k in actual}

    elif isinstance(expected, list) and isinstance(actual, list):
        return [deep_cast_dict(a, e) for a, e in zip(actual, expected)]

    return actual


def assert_dict_equal_ignore_extra(actual, expected):
    assert deep_cast_dict(actual, expected) == expected
