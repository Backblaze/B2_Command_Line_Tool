######################################################################
#
# File: test/integration/test_autocomplete.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import sys

import pexpect
import pytest

from test.helpers import skip_on_windows

TIMEOUT = 120  # CI can be slow at times when parallelization is extreme

BASHRC_CONTENT = """\
# ~/.bashrc dummy file

echo "Just testing if we don't replace existing script" > /dev/null
# >>> just a test section >>>
# regardless what is in there already
# <<< just a test section <<<
"""


def patched_spawn(*args, **kwargs):
    """
    Patch pexpect.spawn to improve error messages
    """

    instance = pexpect.spawn(*args, **kwargs)

    def _patch_expect(func):
        def _wrapper(pattern_list, **kwargs):
            try:
                return func(pattern_list, **kwargs)
            except pexpect.exceptions.TIMEOUT as exc:
                raise pexpect.exceptions.TIMEOUT(
                    f'Timeout reached waiting for `{pattern_list}` to be autocompleted'
                ) from exc
            except pexpect.exceptions.EOF as exc:
                raise pexpect.exceptions.EOF(
                    f'Received EOF waiting for `{pattern_list}` to be autocompleted'
                ) from exc
            except Exception as exc:
                raise RuntimeError(
                    f'Unexpected error waiting for `{pattern_list}` to be autocompleted'
                ) from exc

        return _wrapper

    instance.expect = _patch_expect(instance.expect)
    instance.expect_exact = _patch_expect(instance.expect_exact)

    # capture child shell's output for debugging
    instance.logfile = sys.stdout.buffer

    return instance


@pytest.fixture(scope='session')
def bashrc(homedir):
    bashrc_path = homedir / '.bashrc'
    bashrc_path.write_text(BASHRC_CONTENT)
    yield bashrc_path


@pytest.fixture(scope='module')
def cli_command(request) -> str:
    return request.config.getoption('--sut')


@pytest.fixture(scope='module')
def autocomplete_installed(env, homedir, bashrc, cli_version, cli_command, is_running_on_docker):
    if is_running_on_docker:
        pytest.skip('Not supported on Docker')

    shell = patched_spawn(
        f'bash -i -c "{cli_command} install-autocomplete"', env=env, logfile=sys.stderr.buffer
    )
    try:
        shell.expect_exact('Autocomplete successfully installed for bash', timeout=TIMEOUT)
    finally:
        shell.close()
    shell.wait()
    assert (homedir / '.bash_completion.d' / cli_version).is_file()
    assert bashrc.read_text().startswith(BASHRC_CONTENT)


@pytest.fixture
def shell(env):
    shell = patched_spawn('bash -i', env=env, maxread=1000)
    shell.setwinsize(100, 1000)  # required to see all suggestions in tests
    yield shell
    shell.close()


@skip_on_windows
def test_autocomplete_b2_commands(autocomplete_installed, is_running_on_docker, shell, cli_version):
    if is_running_on_docker:
        pytest.skip('Not supported on Docker')
    shell.send(f'{cli_version} \t\t')
    shell.expect_exact(['authorize-account', 'download-file', 'get-bucket'], timeout=TIMEOUT)


@skip_on_windows
def test_autocomplete_b2_only_matching_commands(
    autocomplete_installed, is_running_on_docker, shell, cli_version
):
    if is_running_on_docker:
        pytest.skip('Not supported on Docker')
    shell.send(f'{cli_version} delete-\t\t')

    shell.expect_exact('file', timeout=TIMEOUT)  # common part of remaining cmds is autocompleted
    with pytest.raises(pexpect.exceptions.TIMEOUT):  # no other commands are suggested
        shell.expect_exact('get-bucket', timeout=0.5)


@skip_on_windows
def test_autocomplete_b2__download_file__b2uri(
    autocomplete_installed,
    shell,
    b2_tool,
    bucket_name,
    file_name,
    is_running_on_docker,
    cli_version,
):
    """Test that autocomplete suggests bucket names and file names."""
    if is_running_on_docker:
        pytest.skip('Not supported on Docker')
    shell.send(f'{cli_version} file download \t\t')
    shell.expect_exact('b2://', timeout=TIMEOUT)
    shell.send('b2://\t\t')
    shell.expect_exact(bucket_name, timeout=TIMEOUT)
    shell.send(f'{bucket_name}/\t\t')
    shell.expect_exact(file_name, timeout=TIMEOUT)
