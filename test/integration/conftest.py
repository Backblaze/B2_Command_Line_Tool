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
import os
import pathlib
import subprocess
import sys
import tempfile
from os import environ, path
from tempfile import TemporaryDirectory

import pytest
from b2sdk.exception import BadRequest, BucketIdNotFound
from b2sdk.v2 import B2_ACCOUNT_INFO_ENV_VAR, XDG_CONFIG_HOME_ENV_VAR

from .helpers import Api, CommandLine, bucket_name_part

GENERAL_BUCKET_NAME_PREFIX = 'clitst'
TEMPDIR = tempfile.gettempdir()
ROOT_PATH = pathlib.Path(__file__).parent.parent.parent


@pytest.hookimpl
def pytest_addoption(parser):
    parser.addoption(
        '--sut', default='%s -m b2' % sys.executable, help='Path to the System Under Test'
    )
    parser.addoption(
        '--env-file-cmd-placeholder',
        default=None,
        help=(
            'If specified, all occurrences of this string in `--sut` will be substituted with a'
            'path to a tmp file containing env vars to be used when running commands in tests. Useful'
            'for docker.'
        )
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
def bucket(b2_api) -> str:
    try:
        bucket = b2_api.create_bucket()
    except BadRequest as e:
        if e.code != 'too_many_buckets':
            raise
        num_buckets = b2_api.count_and_print_buckets()
        print('current number of buckets:', num_buckets)
        raise
    yield bucket
    with contextlib.suppress(BucketIdNotFound):
        b2_api.clean_bucket(bucket)


@pytest.fixture(scope='function')
def bucket_name(bucket) -> str:
    yield bucket.name


@pytest.fixture(scope='function')
def file_name(bucket) -> str:
    file_ = bucket.upload_bytes(b'test_file', f'{bucket_name_part(8)}.txt')
    yield file_.file_name


@pytest.fixture(scope='function')  # , autouse=True)
def debug_print_buckets(b2_api):
    print('-' * 30)
    print('Buckets before test ' + environ['PYTEST_CURRENT_TEST'])
    num_buckets = b2_api.count_and_print_buckets()
    print('-' * 30)
    try:
        yield
    finally:
        print('-' * 30)
        print('Buckets after test ' + environ['PYTEST_CURRENT_TEST'])
        delta = b2_api.count_and_print_buckets() - num_buckets
        print(f'DELTA: {delta}')
        print('-' * 30)


@pytest.fixture(scope='session')
def this_run_bucket_name_prefix() -> str:
    yield GENERAL_BUCKET_NAME_PREFIX + bucket_name_part(8)


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
def b2_api(application_key_id, application_key, realm, this_run_bucket_name_prefix) -> Api:
    yield Api(
        application_key_id, application_key, realm, GENERAL_BUCKET_NAME_PREFIX,
        this_run_bucket_name_prefix
    )


@pytest.fixture(scope='module')
def global_b2_tool(
    request, application_key_id, application_key, realm, this_run_bucket_name_prefix
) -> CommandLine:
    tool = CommandLine(
        request.config.getoption('--sut'),
        application_key_id,
        application_key,
        realm,
        this_run_bucket_name_prefix,
        request.config.getoption('--env-file-cmd-placeholder'),
    )
    tool.reauthorize(check_key_capabilities=True)  # reauthorize for the first time (with check)
    return tool


@pytest.fixture(scope='function')
def b2_tool(global_b2_tool):
    """Automatically reauthorized b2_tool for each test (without check)"""
    global_b2_tool.reauthorize(check_key_capabilities=False)
    return global_b2_tool


@pytest.fixture(autouse=True, scope='session')
def sample_file():
    """Copy the README.md file to /tmp so that docker tests can access it"""
    tmp_readme = pathlib.Path(f'{TEMPDIR}/README.md')
    if not tmp_readme.exists():
        tmp_readme.write_text((ROOT_PATH / 'README.md').read_text())
    return str(tmp_readme)


@pytest.fixture(scope='session')
def is_running_on_docker(pytestconfig):
    return pytestconfig.getoption('--sut').startswith('docker')


SECRET_FIXTURES = {'application_key', 'application_key_id'}


@pytest.fixture(scope="session")
def homedir(tmp_path_factory):
    yield tmp_path_factory.mktemp("test_homedir")


@pytest.fixture(scope="session")
def b2_in_path(tmp_path_factory):
    """
    Create a dummy b2 executable in a temporary directory and add it to PATH.

    This allows us to test the b2 command from shell level even if tested `b2` package was not installed.
    """

    tempdir = tmp_path_factory.mktemp("temp_bin")
    temp_executable = tempdir / "b2"
    with open(temp_executable, "w") as f:
        f.write(
            f"#!{sys.executable}\n"
            "import sys\n"
            f"sys.path.insert(0, {os.getcwd()!r})\n"  # ensure relative imports work even if command is run in different directory
            "from b2.console_tool import main\n"
            "main()\n"
        )

    temp_executable.chmod(0o700)

    original_path = os.environ["PATH"]
    new_path = f"{tempdir}:{original_path}"
    yield new_path


@pytest.fixture(scope="module")
def env(b2_in_path, homedir, monkey_patch, is_running_on_docker):
    """Get ENV for running b2 command from shell level."""
    if not is_running_on_docker:
        monkey_patch.setenv('PATH', b2_in_path)
    monkey_patch.setenv('HOME', str(homedir))
    monkey_patch.setenv('SHELL', "/bin/bash")  # fix for running under github actions
    yield os.environ


@pytest.fixture
def bash_runner(env):
    """Run command in bash shell."""

    def run_command(command: str):
        try:
            return subprocess.run(
                ["/bin/bash", "-c", command],
                capture_output=True,
                check=True,
                env=env,
                text=True,
            )
        except subprocess.CalledProcessError as e:
            print(f"Command {command!r} failed with exit code {e.returncode}")
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
