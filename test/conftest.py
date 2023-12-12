######################################################################
#
# File: test/conftest.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import sys

import pytest


@pytest.hookimpl
def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "cli_version(from_version, to_version): run tests only on certain versions",
    )


@pytest.fixture(scope='session')
def cli_int_version() -> int:
    """
    This should never be called, only provides a placeholder for tests
    not belonging to neither units nor integrations.
    """
    return -1


@pytest.fixture(autouse=True)
def run_on_cli_version_handler(request, cli_int_version):
    """
    Auto-fixture that allows skipping tests based on the CLI version.

    Usage:
        @pytest.mark.cli_version(1, 3)
        def test_foo():
            # Test is run only for versions 1 and 3
            ...

        @pytest.mark.cli_version(from_version=2, to_version=5)
        def test_bar():
            # Test is run only for versions 2, 3, 4 and 5
            ...

    Note that it requires the `cli_int_version` fixture to be defined.
    Both unit tests and integration tests handle it a little bit different, thus
    two different fixtures are provided.
    """
    node = request.node.get_closest_marker('cli_version')
    if not node:
        return

    if not node.args and not node.kwargs:
        return

    assert cli_int_version >= 0, 'cli_int_version fixture is not defined'

    if node.args:
        if cli_int_version in node.args:
            # Run the test.
            return

    if node.kwargs:
        from_version = node.kwargs.get('from_version', 0)
        to_version = node.kwargs.get('to_version', sys.maxsize)

        if from_version <= cli_int_version <= to_version:
            # Run the test.
            return

    pytest.skip('Not supported on this CLI version')
