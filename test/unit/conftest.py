######################################################################
#
# File: test/unit/conftest.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
import importlib
import os
from unittest import mock

import pytest
from b2sdk.raw_api import REALM_URLS

from b2._internal.console_tool import _TqdmCloser
from b2._internal.version_listing import CLI_VERSIONS, UNSTABLE_CLI_VERSION, get_int_version

from ..helpers import b2_uri_args_v3, b2_uri_args_v4
from .helpers import RunOrDieExecutor
from .test_console_tool import BaseConsoleToolTest


@pytest.hookimpl
def pytest_addoption(parser):
    parser.addoption(
        '--cli',
        default=UNSTABLE_CLI_VERSION,
        choices=CLI_VERSIONS,
        help='version of the CLI',
    )


@pytest.hookimpl
def pytest_report_header(config):
    int_version = get_int_version(config.getoption('--cli'))
    return f'b2cli version: {int_version}'


@pytest.fixture(scope='session')
def cli_version(request) -> str:
    return request.config.getoption('--cli')


@pytest.fixture(scope='session')
def cli_int_version(cli_version) -> int:
    return get_int_version(cli_version)


@pytest.fixture(scope='session')
def console_tool_class(cli_version):
    # Ensures import of the correct library to handle all the tests.
    module = importlib.import_module(f'b2._internal.{cli_version}.registry')
    return module.ConsoleTool


@pytest.fixture(scope='class')
def unit_test_console_tool_class(request, console_tool_class):
    # Ensures that the unittest class uses the correct console tool version.
    request.cls.console_tool_class = console_tool_class


@pytest.fixture(autouse=True, scope='session')
def mock_realm_urls():
    with mock.patch.dict(REALM_URLS, {'production': 'http://production.example.com'}):
        yield


@pytest.fixture
def bg_executor():
    """Executor for running background tasks in tests"""
    with RunOrDieExecutor() as executor:
        yield executor


@pytest.fixture(autouse=True)
def disable_tqdm_closer_cleanup():
    with mock.patch.object(_TqdmCloser, '__exit__'):
        yield


class ConsoleToolTester(BaseConsoleToolTest):
    def authorize(self):
        self._authorize_account()

    def run(self, *args, **kwargs):
        return self._run_command(*args, **kwargs)


@pytest.fixture
def b2_cli(console_tool_class):
    cli_tester = ConsoleToolTester()
    # Because of the magic the pytest does on importing and collecting fixtures,
    # ConsoleToolTester is not injected with the `unit_test_console_tool_class`
    # despite having it as a parent.
    # Thus, we inject it manually here.
    cli_tester.console_tool_class = console_tool_class
    cli_tester.setUp()
    yield cli_tester
    cli_tester.tearDown()


@pytest.fixture
def authorized_b2_cli(b2_cli):
    b2_cli.authorize()
    yield b2_cli


@pytest.fixture
def bucket_info(b2_cli, authorized_b2_cli):
    bucket_name = "my-bucket"
    bucket_id = "bucket_0"
    b2_cli.run(['create-bucket', bucket_name, 'allPublic'], expected_stdout=f'{bucket_id}\n')
    return {
        'bucketName': bucket_name,
        'bucketId': bucket_id,
    }


@pytest.fixture
def bucket(bucket_info):
    return bucket_info['bucketName']


@pytest.fixture
def local_file(tmp_path):
    """Set up a test file and return its path."""
    filename = 'file1.txt'
    content = 'hello world'
    local_file = tmp_path / filename
    local_file.write_text(content)

    mod_time = 1500111222
    os.utime(local_file, (mod_time, mod_time))

    return local_file


@pytest.fixture
def uploaded_file_with_control_chars(b2_cli, bucket_info, local_file):
    filename = '\u009bC\u009bC\u009bIfile.txt'
    b2_cli.run(['upload-file', bucket_info["bucketName"], str(local_file), filename])
    return {
        'bucket': bucket_info["bucketName"],
        'bucketId': bucket_info["bucketId"],
        'fileName': filename,
        'escapedFileName': '\\\\x9bC\\\\x9bC\\\\x9bIfile.txt',
        'fileId': '1111',
        'content': local_file.read_text(),
    }


@pytest.fixture
def uploaded_file(b2_cli, bucket_info, local_file):
    filename = 'file1.txt'
    b2_cli.run(['upload-file', '--quiet', bucket_info["bucketName"], str(local_file), filename])
    return {
        'bucket': bucket_info["bucketName"],
        'bucketId': bucket_info["bucketId"],
        'fileName': filename,
        'fileId': '9999',
        'content': local_file.read_text(),
    }


@pytest.fixture(scope='class')
def b2_uri_args(cli_int_version, request):
    if cli_int_version >= 4:
        fn = b2_uri_args_v4
    else:
        fn = b2_uri_args_v3

    request.cls.b2_uri_args = staticmethod(fn)
