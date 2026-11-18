######################################################################
#
# File: test/unit/test_test_suite_hygiene.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
"""
Checks on the test suite itself.

These do not test the CLI; they pin the properties the suite's own helpers must have, so that a
broken helper (swallowed exceptions, unused parametrization, leaked patches) is caught instead of
silently weakening other tests. Tests decorated with `known_defect` describe a defect that is still
present (see `test/known_defect.py`).
"""

from __future__ import annotations

import ast
import inspect
import pathlib

import pytest

from b2._internal.console_tool import _TqdmCloser
from test.known_defect import known_defect

from .test_console_tool import BaseConsoleToolTest

UNIT_DIR = pathlib.Path(__file__).parent


@known_defect(
    'the autouse tqdm-closer fixture swallows exceptions raised inside commands',
    raises=pytest.fail.Exception,  # pytest.raises: DID NOT RAISE
)
def test_tqdm_closer_fixture_does_not_swallow_exceptions():
    """The autouse fixture mocks `_TqdmCloser.__exit__`; the mock must not suppress exceptions."""
    with pytest.raises(RuntimeError, match='boom'):
        with _TqdmCloser(progress_listener=None):
            raise RuntimeError('boom')


def _unused_parametrized_arguments(path: pathlib.Path) -> list[str]:
    problems = []
    tree = ast.parse(path.read_text())
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        used = {n.id for n in ast.walk(node) if isinstance(n, ast.Name)}
        for decorator in node.decorator_list:
            is_parametrize = (
                isinstance(decorator, ast.Call)
                and isinstance(decorator.func, ast.Attribute)
                and decorator.func.attr == 'parametrize'
            )
            if not is_parametrize or not decorator.args:
                continue
            names_arg = decorator.args[0]
            if isinstance(names_arg, ast.Constant) and isinstance(names_arg.value, str):
                names = [n.strip() for n in names_arg.value.split(',')]
                problems.extend(
                    f'{path.name}::{node.name}: parametrized argument {name!r} is never used'
                    for name in names
                    if name not in used
                )
    return problems


@known_defect('a parametrized argument of the download-file tests is never used')
def test_download_file_tests_use_all_their_parametrized_arguments():
    """A parametrized argument that the body ignores makes every case run the same command."""
    source = UNIT_DIR / 'console_tool' / 'test_download_file.py'
    if not source.is_file():
        pytest.skip('test sources are not available (not a full checkout)')
    problems = _unused_parametrized_arguments(source)
    assert problems == []


@pytest.mark.apiver(from_ver=4)
def test_camel_case_option_is_rejected_for_the_right_reason(b2_cli, capsys):
    """
    The existing test passes `--bucketInfo allPrivate` (not valid JSON), so exit status 2 is
    produced whether or not camelCase options are supported. With valid input the rejection must be
    specifically about the unknown `--bucketInfo` option, and nothing may be created.
    """
    b2_cli.authorize()
    capsys.readouterr()
    status, _, _ = b2_cli.run(
        ['bucket', 'create', 'my-bucket', 'allPrivate', '--bucketInfo', '{"xxx": "123"}'],
        expected_stdout='',
        expected_stderr='',
        expected_status=2,
    )
    assert 'unrecognized arguments: --bucketInfo' in capsys.readouterr().err
    b2_cli.run(['bucket', 'list'], expected_stdout='')


@known_defect('BaseConsoleToolTest.tearDown does not restore the patched class attribute')
def test_base_console_tool_test_restores_patched_class_attribute(console_tool_class):
    """`BaseConsoleToolTest.setUp` replaces a method on the shared class; tearDown must restore it."""
    name = '_initialize_b2_api'
    before = inspect.getattr_static(console_tool_class, name)  # the raw classmethod object
    owned_by_class = name in vars(console_tool_class)

    try:
        test_case = BaseConsoleToolTest()
        test_case.console_tool_class = console_tool_class
        test_case.setUp()
        test_case.tearDown()

        assert inspect.getattr_static(console_tool_class, name) is before
    finally:
        # the defect leaves the shared class patched; never let that leak into other tests,
        # whether the assertion above passes or fails
        if owned_by_class:
            setattr(console_tool_class, name, before)
        elif name in vars(console_tool_class):
            delattr(console_tool_class, name)
