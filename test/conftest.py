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
        'markers',
        'apiver(from_ver, to_ver): run tests only on certain apiver versions',
    )


@pytest.fixture(scope='session')
def apiver(request):
    """Get apiver as a v-prefixed string, e.g. "v2"."""
    return request.config.getoption('--cli', '').lstrip('_').removeprefix('b2') or None


@pytest.fixture(scope='session')
def apiver_int(apiver) -> int:
    return int(apiver[1:]) if apiver else -1


@pytest.fixture(autouse=True)
def run_on_apiver_handler(request, apiver_int):
    """
    Auto-fixture that allows skipping tests based on the CLI apiver versions.

    Usage:
        @pytest.mark.apiver(1, 3)
        def test_foo():
            # Test is run only for versions 1 and 3
            ...

        @pytest.mark.apiver(from_ver=2, to_ver=5)
        def test_bar():
            # Test is run only for versions 2, 3, 4 and 5
            ...

    Note that it requires the `cli_int_version` fixture to be defined.
    Both unit tests and integration tests handle it a little bit different, thus
    two different fixtures are provided.
    """
    node = request.node.get_closest_marker('apiver')
    if not node:
        return

    if not node.args and not node.kwargs:
        return

    assert apiver_int >= 0, 'apiver_int fixture is not defined'

    if node.args:
        if apiver_int in node.args:
            # Run the test.
            return

    if node.kwargs:
        from_ver = node.kwargs.get('from_ver', 0)
        to_ver = node.kwargs.get('to_ver', sys.maxsize)

        if from_ver <= apiver_int <= to_ver:
            # Run the test.
            return

    pytest.skip('Not supported on this apiver version')
