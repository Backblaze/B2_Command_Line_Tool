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
from test.helpers import skip_on_windows

import pexpect
import pytest

TIMEOUT = 120  # CI can be slow at times when parallelization is extreme

BASHRC_CONTENT = """\
# ~/.bashrc dummy file

echo "Just testing if we don't replace existing script" > /dev/null
# >>> just a test section >>>
# regardless what is in there already
# <<< just a test section <<<
"""


@pytest.fixture(scope="session")
def bashrc(homedir):
    bashrc_path = (homedir / '.bashrc')
    bashrc_path.write_text(BASHRC_CONTENT)
    yield bashrc_path


@pytest.fixture(scope="module")
def autocomplete_installed(env, homedir, bashrc):
    shell = pexpect.spawn(
        'bash -i -c "b2 install-autocomplete"', env=env, logfile=sys.stderr.buffer
    )
    try:
        shell.expect_exact('Autocomplete successfully installed for bash', timeout=TIMEOUT)
    finally:
        shell.close()
    shell.wait()
    assert (homedir / '.bash_completion.d' / 'b2').is_file()
    assert bashrc.read_text().startswith(BASHRC_CONTENT)


@pytest.fixture
def shell(env):
    shell = pexpect.spawn('bash -i', env=env, maxread=1000)
    shell.setwinsize(100, 100)  # required to see all suggestions in tests
    yield shell
    shell.close()


@skip_on_windows
def test_autocomplete_b2_commands(autocomplete_installed, is_running_on_docker, shell):
    if is_running_on_docker:
        pytest.skip('Not supported on Docker')
    shell.send('b2 \t\t')
    shell.expect_exact(["authorize-account", "download-file-by-id", "get-bucket"], timeout=TIMEOUT)


@skip_on_windows
def test_autocomplete_b2_only_matching_commands(
    autocomplete_installed, is_running_on_docker, shell
):
    if is_running_on_docker:
        pytest.skip('Not supported on Docker')
    shell.send('b2 download-\t\t')

    shell.expect_exact(
        "file-by-", timeout=TIMEOUT
    )  # common part of remaining cmds is autocompleted
    with pytest.raises(pexpect.exceptions.TIMEOUT):  # no other commands are suggested
        shell.expect_exact("get-bucket", timeout=0.5)


@skip_on_windows
def test_autocomplete_b2_bucket_n_file_name(
    autocomplete_installed, shell, b2_tool, bucket_name, file_name, is_running_on_docker
):
    """Test that autocomplete suggests bucket names and file names."""
    if is_running_on_docker:
        pytest.skip('Not supported on Docker')
    shell.send('b2 download_file_by_name \t\t')
    shell.expect_exact(bucket_name, timeout=TIMEOUT)
    shell.send(f'{bucket_name} \t\t')
    shell.expect_exact(file_name, timeout=TIMEOUT)
