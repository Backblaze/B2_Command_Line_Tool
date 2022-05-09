######################################################################
#
# File: test/integration/conftest.py
#
# Copyright 2020 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
import sys

from os import environ, path
from tempfile import TemporaryDirectory
from typing import Tuple

import pytest

from b2sdk.v2 import B2_ACCOUNT_INFO_ENV_VAR, XDG_CONFIG_HOME_ENV_VAR

from .helpers import Api, CommandLine, bucket_name_part


@pytest.hookimpl
def pytest_addoption(parser):
    parser.addoption(
        '--sut', default='%s -m b2' % sys.executable, help='Path to the System Under Test'
    )
    parser.addoption('--cleanup', action='store_true', help='Perform full cleanup at exit')


@pytest.fixture(scope='session')
def sut(request):
    return request.config.getoption('--sut')


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
def bucket_name(b2_api):
    yield b2_api.create_bucket()


@pytest.fixture(scope='session')
def general_bucket_name_prefix() -> str:
    yield 'clitst'


@pytest.fixture(scope='session')
def this_run_bucket_name_prefix(general_bucket_name_prefix) -> str:
    yield general_bucket_name_prefix + bucket_name_part(8)


@pytest.fixture(scope='module')
def monkey_patch():
    """ Module-scope monkeypatching """
    from _pytest.monkeypatch import MonkeyPatch
    monkey = MonkeyPatch()
    yield monkey
    monkey.undo()


@pytest.fixture(scope='module', autouse=True)
def auto_change_account_info_dir(monkey_patch) -> dir:

    monkey_patch.delenv('B2_APPLICATION_KEY_ID')
    monkey_patch.delenv('B2_APPLICATION_KEY')

    # make b2sdk use temp dir for storing default & per-profile account information
    with TemporaryDirectory() as temp_dir:
        monkey_patch.setenv(B2_ACCOUNT_INFO_ENV_VAR, path.join(temp_dir, '.b2_account_info'))
        monkey_patch.setenv(XDG_CONFIG_HOME_ENV_VAR, temp_dir)
        yield temp_dir


@pytest.fixture(scope='module')
def b2_api(application_key_id, application_key, realm, general_bucket_name_prefix, this_run_bucket_name_prefix) -> Api:
    yield Api(
        application_key_id, application_key, realm, general_bucket_name_prefix, this_run_bucket_name_prefix
    )


@pytest.fixture(scope='module', autouse=True)
def auto_clean_buckets(b2_api):
    b2_api.clean_buckets()
    yield
    b2_api.clean_buckets()


@pytest.fixture(scope='module')
def b2_tool(application_key_id, application_key, realm, this_run_bucket_name_prefix) -> CommandLine:
    tool = CommandLine(
        f'{sys.executable} -m b2',  # TODO: args.command
        application_key_id,
        application_key,
        realm,
        this_run_bucket_name_prefix,
    )
    tool.reauthorize(check=True)
    return tool


@pytest.fixture(scope='function', autouse=True)
def auto_reauthorize(request, b2_tool):
    b2_tool.reauthorize(check=False)
