######################################################################
#
# File: test/known_defect.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
"""
Marker for tests that describe the CORRECT behaviour of something that is still broken.

A test decorated with `known_defect(reason, match=...)` is a strict expected failure:

* in the default run (`nox -s unit`) it is reported as `xfailed` and does not fail the build;
* if the defect gets fixed the test starts passing, which pytest reports as a failure (strict xfail),
  so the marker has to be removed in the same change that fixes the defect - the test then
  joins the normal suite and protects the fix;
* to see the tests fail with their real assertion messages and tracebacks, run only the
  marked tests and tell pytest to ignore the expected-failure outcome::

      pytest test/unit test/static --cli b2v5 -m known_defect --runxfail

The expected failure is constrained by exception type, a per-test message pattern, and the test's
call frame. Setup/teardown errors and unrelated assertion failures remain real failures, even when
an exception has the same type as the known defect. Passing tests still produce strict XPASS.

For parametrized cases use `known_defect_marks(reason, match=..., test_name="test_function")`.

"""

from __future__ import annotations

import pytest


def _raised_during_test(error: BaseException, test_name: str) -> bool:
    traceback = error.__traceback__
    while traceback is not None:
        if traceback.tb_frame.f_code.co_name == test_name:
            return True
        traceback = traceback.tb_next
    return False


def known_defect_marks(
    reason: str,
    raises: type[BaseException] | tuple[type[BaseException], ...] = AssertionError,
    *,
    match: str,
    test_name: str,
) -> list[pytest.MarkDecorator]:
    """Constrain an expected failure to its test call and exception signature."""
    return [
        pytest.mark.known_defect,
        pytest.mark.xfail(
            strict=True,
            raises=pytest.RaisesExc(
                raises, match=match, check=lambda error: _raised_during_test(error, test_name)
            ),
            reason=f'known defect: {reason}',
        ),
    ]


def known_defect(
    reason: str,
    raises: type[BaseException] | tuple[type[BaseException], ...] = AssertionError,
    *,
    match: str,
):
    """Mark a regression test function with its specific expected failure."""

    def decorator(obj):
        for mark in known_defect_marks(reason, raises, match=match, test_name=obj.__name__):
            obj = mark(obj)
        return obj

    return decorator
