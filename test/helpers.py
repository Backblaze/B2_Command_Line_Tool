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
import sys

import pexpect
import pytest

_MISSING = object()


def skip_on_windows(*args, reason='Not supported on Windows', **kwargs):
    return pytest.mark.skipif(
        platform.system() == 'Windows',
        reason=reason,
    )(*args, **kwargs)


def patched_spawn(*args, **kwargs):
    """
    Wrapper around pexpect.spawn with improved error messages.

    pexpect's errors are confusing to interpret when things go wrong,
    because it doesn't output the actual stdout by default. This wrapper
    addresses that inconvenience.
    """
    instance = pexpect.spawn(*args, **kwargs)

    def _patch_expect(func):
        def _wrapper(pattern_list, **kwargs):
            try:
                return func(pattern_list, **kwargs)
            except pexpect.exceptions.TIMEOUT as exc:
                raise pexpect.exceptions.TIMEOUT(
                    f'Timeout reached waiting for `{pattern_list}`'
                ) from exc
            except pexpect.exceptions.EOF as exc:
                raise pexpect.exceptions.EOF(f'Received EOF waiting for `{pattern_list}`') from exc
            except Exception as exc:
                raise RuntimeError(f'Unexpected error waiting for `{pattern_list}`') from exc

        return _wrapper

    instance.expect = _patch_expect(instance.expect)
    instance.expect_exact = _patch_expect(instance.expect_exact)

    # capture child shell's output for debugging
    instance.logfile = sys.stdout.buffer

    return instance


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
