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

A test decorated with `known_defect(reason)` is a strict expected failure:

* in the default run (`nox -s unit`, `nox -s lint`) it is reported as `xfailed` and does not fail the build;
* if the defect gets fixed the test starts passing, which pytest reports as a failure (strict xfail),
  so the marker has to be removed in the same change that fixes the defect - the test then
  joins the normal suite and protects the fix;
* to see the tests fail with their real assertion messages and tracebacks, run only the
  marked tests and tell pytest to ignore the expected-failure outcome::

      pytest test/unit test/static --cli b2v5 -m known_defect --runxfail

For a parametrized test, mark only the cases that are broken: `pytest.param(..., marks=known_defect_marks(reason))`.
"""

from __future__ import annotations

import pytest


def known_defect_marks(reason: str) -> list[pytest.MarkDecorator]:
    """Marks for `pytest.param(..., marks=...)`."""
    return [
        pytest.mark.known_defect,
        pytest.mark.xfail(strict=True, reason=f'known defect: {reason}'),
    ]


def known_defect(reason: str):
    """Decorator for a test (or test class) that currently fails because of a known defect."""

    def decorator(obj):
        for mark in known_defect_marks(reason):
            obj = mark(obj)
        return obj

    return decorator
