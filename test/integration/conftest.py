######################################################################
#
# File: test/integration/conftest.py
#
# Copyright 2020 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import logging
import os
import pathlib
import re
import subprocess
import sys
import tempfile
import uuid
from collections.abc import Generator
from os import environ, path
from tempfile import TemporaryDirectory

import pytest
from b2sdk.v3 import B2_ACCOUNT_INFO_ENV_VAR, XDG_CONFIG_HOME_ENV_VAR
from b2sdk.v3.testing import NODE_DESCRIPTION, RNG_SEED, random_token

from b2._internal.version_listing import (
    CLI_VERSIONS,
    LATEST_STABLE_VERSION,
    UNSTABLE_CLI_VERSION,
    get_int_version,
)

from ..helpers import b2_uri_args_v3, b2_uri_args_v4
from .helpers import CommandLine
from .persistent_bucket import (
    PersistentBucketAggregate,
    get_or_create_persistent_bucket,
    prune_used_files,
)

logger = logging.getLogger(__name__)

TEMPDIR = tempfile.gettempdir()
ROOT_PATH = pathlib.Path(__file__).parent.parent.parent
GENERAL_BUCKET_NAME_PREFIX = 'clitst'


pytest_plugins = ['b2sdk.v3.testing']


@pytest.fixture(scope='session', autouse=True)
def summary_notes(request, worker_id):
    capmanager = request.config.pluginmanager.getplugin('capturemanager')
    with capmanager.global_and_fixture_disabled():
        log_handler = logging.StreamHandler(sys.stderr)
    log_fmt = logging.Formatter(f'{worker_id} %(asctime)s %(levelname).1s %(message)s')
    log_handler.setFormatter(log_fmt)
    logger.addHandler(log_handler)

    class Notes:
        def append(self, note):
            logger.info(note)

    return Notes()


@pytest.fixture(scope='session', autouse=True)
def node_stats(summary_notes):
    summary_notes.append(f'NODE={NODE_DESCRIPTION} seed={RNG_SEED}')


@pytest.hookimpl
def pytest_addoption(parser):
    parser.addoption(
        '--sut',
        default=f'{sys.executable} -m b2._internal.{UNSTABLE_CLI_VERSION}',
        help='Path to the System Under Test',
    )
    parser.addoption(
        '--env-file-cmd-placeholder',
        default=None,
        help=(
            'If specified, all occurrences of this string in `--sut` will be substituted with a '
            'path to a tmp file containing env vars to be used when running commands in tests. Useful '
            'for docker.'
        ),
    )
    parser.addoption(
        '--as_version',
        default=None,
        help='Force running tests as a particular version of the CLI, '
        'useful if version cannot be determined easily from the executable',
    )
    parser.addoption('--cleanup', action='store_true', help='Perform full cleanup at exit')


def get_raw_cli_int_version(config) -> int | None:
    forced_version = config.getoption('--as_version')
    if forced_version:
        return int(forced_version)

    executable = config.getoption('--sut')
    # If the executable contains anything that looks like a proper version, we can try to pick it up.
    versions_list = '|'.join(CLI_VERSIONS)
    versions_match = re.search(rf'({versions_list})', executable)
    if versions_match:
        return get_int_version(versions_match.group(1))

    return None


def get_cli_int_version(config) -> int:
    return get_raw_cli_int_version(config) or get_int_version(LATEST_STABLE_VERSION)


@pytest.fixture(scope='session')
def apiver_int(request):
    return get_cli_int_version(request.config)


@pytest.fixture(scope='session')
def apiver(apiver_int):
    return f'v{apiver_int}'


@pytest.hookimpl
def pytest_report_header(config):
    cli_version = get_cli_int_version(config)
    return f'b2 apiver: {cli_version}'


@pytest.fixture(scope='session')
def cli_version(request) -> str:
    """
    Get CLI version name, i.e. b2v3, _b2v4, etc.
    """
    # The default stable version could be provided directly as e.g.: b2v4, but also indirectly as b2.
    # In case there is no direct version, we return the default binary name instead.
    raw_cli_version = get_raw_cli_int_version(request.config)
    if raw_cli_version is None:
        return 'b2'

    for version in CLI_VERSIONS:
        if get_int_version(version) == raw_cli_version:
            return version
    raise pytest.UsageError(f'Unknown CLI version: {raw_cli_version}')


@pytest.fixture(scope='session')
def application_key(b2_auth_data) -> str:
    yield b2_auth_data[1]


@pytest.fixture(scope='session')
def application_key_id(b2_auth_data) -> str:
    yield b2_auth_data[0]


@pytest.fixture(scope='function')
def bucket_name(bucket) -> str:
    yield bucket.name


@pytest.fixture(scope='function')
def file_name(bucket) -> str:
    file_ = bucket.upload_bytes(b'test_file', f'{random_token(8)}.txt')
    yield file_.file_name


@pytest.fixture(scope='function')  # , autouse=True)
def debug_print_buckets(bucket_manager):
    print('-' * 30)
    print('Buckets before test ' + environ['PYTEST_CURRENT_TEST'])
    num_buckets = bucket_manager.count_and_print_buckets()
    print('-' * 30)
    try:
        yield
    finally:
        print('-' * 30)
        print('Buckets after test ' + environ['PYTEST_CURRENT_TEST'])
        delta = bucket_manager.count_and_print_buckets() - num_buckets
        print(f'DELTA: {delta}')
        print('-' * 30)


@pytest.fixture(scope='session')
def monkeysession():
    with pytest.MonkeyPatch.context() as mp:
        yield mp


@pytest.fixture(scope='session', autouse=True)
def auto_change_account_info_dir(monkeysession) -> str:
    """
    Automatically for the whole testing:
    1) temporary remove B2_APPLICATION_KEY and B2_APPLICATION_KEY_ID from environment
    2) create a temporary directory for storing account info database
    3) set B2_ACCOUNT_INFO_ENV_VAR to point to the temporary account info file
    """

    monkeysession.delenv('B2_APPLICATION_KEY_ID', raising=False)
    monkeysession.delenv('B2_APPLICATION_KEY', raising=False)

    # Ignore occasional PermissionError on Windows
    if sys.platform == 'win32' and (sys.version_info.major, sys.version_info.minor) > (3, 9):
        kwargs = dict(ignore_cleanup_errors=True)
    else:
        kwargs = {}

    # make b2sdk use temp dir for storing default & per-profile account information
    with TemporaryDirectory(**kwargs) as temp_dir:
        monkeysession.setenv(B2_ACCOUNT_INFO_ENV_VAR, path.join(temp_dir, '.b2_account_info'))
        monkeysession.setenv(XDG_CONFIG_HOME_ENV_VAR, temp_dir)
        yield temp_dir


@pytest.fixture(scope='session')
def general_bucket_name_prefix():
    return GENERAL_BUCKET_NAME_PREFIX


@pytest.fixture(scope='session')
def bucket_manager(
    bucket_manager,
    application_key_id,
    application_key,
    realm,
    auto_change_account_info_dir,
    summary_notes,
):
    yield bucket_manager
    # showing account_id in the logs is safe; so we explicitly prevent it from being redacted
    summary_notes.append(f'B2 Account ID: {application_key_id[:1]!r}{application_key_id[1:]!r}')
    summary_notes.append(
        f'Buckets names used during this tests: {bucket_manager.bucket_name_log!r}'
    )


@pytest.fixture(scope='module')
def global_b2_tool(
    request,
    application_key_id,
    application_key,
    realm,
    bucket_name_prefix,
    bucket_manager,
    auto_change_account_info_dir,
    b2_uri_args,
) -> CommandLine:
    tool = CommandLine(
        request.config.getoption('--sut'),
        application_key_id,
        application_key,
        realm,
        bucket_name_prefix,
        request.config.getoption('--env-file-cmd-placeholder'),
        bucket_manager=bucket_manager,
        b2_uri_args=b2_uri_args,
    )
    tool.reauthorize(check_key_capabilities=True)  # reauthorize for the first time (with check)
    yield tool


@pytest.fixture(scope='function')
def b2_tool(global_b2_tool):
    """Automatically reauthorized b2_tool for each test (without check)"""
    global_b2_tool.reauthorize(check_key_capabilities=False)
    return global_b2_tool


@pytest.fixture
def account_info_file() -> pathlib.Path:
    return pathlib.Path(os.environ[B2_ACCOUNT_INFO_ENV_VAR]).expanduser()


@pytest.fixture
def schedule_bucket_cleanup(global_b2_tool):
    """
    Explicitly ask for buckets cleanup after the test

    This should be only used when testing `bucket create` command;
    otherwise use `b2sdk.v3.testing.IntegrationTestBase.create_bucket()`.
    """
    buckets_to_clean = {}

    def add_bucket_to_cleanup(bucket_name, bucket_dict: dict | None = None):
        buckets_to_clean[bucket_name] = bucket_dict

    yield add_bucket_to_cleanup
    if buckets_to_clean:
        global_b2_tool.reauthorize(
            check_key_capabilities=False
        )  # test may have mangled authorization
        global_b2_tool.cleanup_buckets(buckets_to_clean)


@pytest.fixture(autouse=True, scope='session')
def sample_filepath():
    """Copy the README.md file to /tmp so that docker tests can access it"""
    tmp_readme = pathlib.Path(TEMPDIR) / 'README.md'
    if not tmp_readme.exists():
        tmp_readme.write_text((ROOT_PATH / 'README.md').read_text())
    return tmp_readme


@pytest.fixture(autouse=True, scope='session')
def sample_file(sample_filepath):
    return str(sample_filepath)


@pytest.fixture(scope='session')
def is_running_on_docker(pytestconfig):
    return pytestconfig.getoption('--sut').startswith('docker')


SECRET_FIXTURES = {'application_key', 'application_key_id'}


@pytest.fixture(scope='session')
def homedir(tmp_path_factory):
    yield tmp_path_factory.mktemp('test_homedir')


@pytest.fixture(scope='session')
def b2_in_path(tmp_path_factory):
    """
    Create a dummy b2 executable in a temporary directory and add it to PATH.

    This allows us to test the b2 command from shell level even if tested `b2` package was not installed.
    """

    tempdir = tmp_path_factory.mktemp('temp_bin')
    temp_executable = tempdir / 'b2'
    with open(temp_executable, 'w') as f:
        f.write(
            f'#!{sys.executable}\n'
            'import sys\n'
            f'sys.path.insert(0, {os.getcwd()!r})\n'  # ensure relative imports work even if command is run in different directory
            'from b2.console_tool import main\n'
            'main()\n'
        )

    temp_executable.chmod(0o700)

    original_path = os.environ['PATH']
    new_path = f'{tempdir}:{original_path}'
    yield new_path


@pytest.fixture(scope='module')
def env(b2_in_path, homedir, monkeysession, is_running_on_docker):
    """Get ENV for running b2 command from shell level."""
    if not is_running_on_docker:
        monkeysession.setenv('PATH', b2_in_path)
    monkeysession.setenv('HOME', str(homedir))
    monkeysession.setenv('SHELL', '/bin/bash')  # fix for running under github actions
    yield os.environ


@pytest.fixture
def bash_runner(env):
    """Run command in bash shell."""

    def run_command(command: str):
        try:
            return subprocess.run(
                ['/bin/bash', '-c', command],
                capture_output=True,
                check=True,
                env=env,
                text=True,
            )
        except subprocess.CalledProcessError as e:
            print(f'Command {command!r} failed with exit code {e.returncode}')
            print(e.stdout)
            print(e.stderr, file=sys.stderr)
            raise

    return run_command


def pytest_collection_modifyitems(items):
    """
    Add 'require_secrets' marker to all tests that use secrets.
    """
    for item in items:
        if SECRET_FIXTURES & set(getattr(item, 'fixturenames', ())):
            item.add_marker('require_secrets')


@pytest.fixture(scope='module')
def b2_uri_args(apiver_int):
    if apiver_int >= 4:
        return b2_uri_args_v4
    else:
        return b2_uri_args_v3


# -- Persistent bucket code ---

subfolder_list: list[str] = []


@pytest.fixture(scope='session')
def base_persistent_bucket(bucket_manager):
    bucket = get_or_create_persistent_bucket(bucket_manager)
    yield bucket
    prune_used_files(bucket_manager=bucket_manager, bucket=bucket, folders=subfolder_list)


@pytest.fixture
def unique_subfolder():
    subfolder = f'test-{uuid.uuid4().hex[:8]}'
    subfolder_list.append(subfolder)
    yield subfolder


@pytest.fixture
def persistent_bucket(
    unique_subfolder, base_persistent_bucket
) -> Generator[PersistentBucketAggregate]:
    """
    Since all consumers of the `bucket_name` fixture expect a new bucket to be created,
    we need to mirror this behavior by appending a unique subfolder to the persistent bucket name.
    """
    yield PersistentBucketAggregate(base_persistent_bucket.name, unique_subfolder)

    logger.info('Persistent bucket aggregate finished completion.')
