######################################################################
#
# File: test/integration/conftest.py
#
# Copyright 2020 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import contextlib
import sys

from os import environ, path
from tempfile import TemporaryDirectory

import pytest

from b2sdk.v2 import B2_ACCOUNT_INFO_ENV_VAR, XDG_CONFIG_HOME_ENV_VAR
from b2sdk.exception import BucketIdNotFound, NonExistentBucket

from .helpers import Api, CommandLine


@pytest.hookimpl
def pytest_addoption(parser):
    parser.addoption(
        '--sut', default='%s -m b2' % sys.executable, help='Path to the System Under Test'
    )
    parser.addoption('--cleanup', action='store_true', help='Perform full cleanup at exit')


@pytest.fixture(scope='session')
def application_key() -> str:
    key = environ.get('B2_TEST_APPLICATION_KEY')
    assert application_key, 'B2_TEST_APPLICATION_KEY is not set'
    yield key


@pytest.fixture(scope='session')
def application_key_id() -> str:
    key_id = environ.get('B2_TEST_APPLICATION_KEY_ID')
    assert key_id, 'B2_TEST_APPLICATION_KEY_ID is not set'
    yield key_id


@pytest.fixture(scope='session')
def realm() -> str:
    yield environ.get('B2_TEST_ENVIRONMENT', 'production')


@pytest.fixture(scope='function')
def create_test_bucket(b2_api):
    def factory(*args, **kwargs):
        return b2_api.create_test_bucket(*args, **kwargs).name

    return factory


@pytest.fixture(scope='function')
def bucket_name(create_test_bucket) -> str:
    return create_test_bucket()


@pytest.fixture(scope='module')
def monkey_patch():
    """ Module-scope monkeypatching (original `monkeypatch` is function-scope) """
    from _pytest.monkeypatch import MonkeyPatch
    monkey = MonkeyPatch()
    yield monkey
    monkey.undo()


@pytest.fixture(scope='module', autouse=True)
def auto_change_account_info_dir(monkey_patch) -> dir:
    """
    Automatically for the whole module testing:
    1) temporary remove B2_APPLICATION_KEY and B2_APPLICATION_KEY_ID from environment
    2) create a temporary directory for storing account info database
    """

    monkey_patch.delenv('B2_APPLICATION_KEY_ID', raising=False)
    monkey_patch.delenv('B2_APPLICATION_KEY', raising=False)

    # make b2sdk use temp dir for storing default & per-profile account information
    with TemporaryDirectory() as temp_dir:
        monkey_patch.setenv(B2_ACCOUNT_INFO_ENV_VAR, path.join(temp_dir, '.b2_account_info'))
        monkey_patch.setenv(XDG_CONFIG_HOME_ENV_VAR, temp_dir)
        yield temp_dir


@pytest.fixture(scope='module')
def b2_api(application_key_id, application_key, realm) -> Api:
    return Api(application_key_id, application_key, realm)


@pytest.fixture(scope='module')
def b2_tool(request, application_key_id, application_key, realm) -> CommandLine:
    tool = CommandLine(
        request.config.getoption('--sut'), application_key_id, application_key, realm
    )
    tool.reauthorize(check_key_capabilities=True)  # reauthorize for the first time (with check)
    return tool


@pytest.fixture(scope='function', autouse=True)
def auto_reauthorize(request, b2_tool):
    """ Automatically reauthorize for each test (without check) """
    b2_tool.reauthorize(check_key_capabilities=False)


@pytest.fixture(scope='function', autouse=True)
def auto_clean_buckets(b2_api, b2_tool):
    """Automatically delete created buckets after each test case"""
    yield

    # remove buckets created using the CLI
    while b2_tool.buckets:
        with contextlib.suppress(BucketIdNotFound, NonExistentBucket):
            # The buckets were created with the CLI tool, but we still delete them using the API as it will handle
            # corner cases properly (like retries or deleting non-empty buckets).
            b2_api.clean_bucket(b2_tool.buckets.pop())

    # remove buckets created using the API
    b2_api.clean_buckets()
