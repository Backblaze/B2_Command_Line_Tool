######################################################################
#
# File: test/integration/test_help.py
#
# Copyright 2024 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
import os
import platform
import re
import subprocess

import pytest

skip_on_windows = pytest.mark.skipif(
    platform.system() == 'Windows',
    reason='PTY tests require Unix-like system',
)


def test_help(cli_version):
    p = subprocess.run(
        [cli_version, '--help'],
        check=True,
        capture_output=True,
        text=True,
    )

    # verify help contains apiver binary name
    expected_name = cli_version
    if platform.system() == 'Windows':
        expected_name += '.exe'
    assert re.match(r'^_?b2(v\d+)?(\.exe)?$', expected_name)  # test sanity check
    assert f'{expected_name} <command> --help' in p.stdout


@skip_on_windows
def test_help_with_tty(cli_version):
    """
    Test that B2 CLI works correctly with a real TTY (pseudo-terminal).

    This test specifically verifies that the rst2ansi buffer overflow bug
    on Python 3.14+ is properly handled. The bug occurs when rst2ansi's
    get_terminal_size() function passes a 4-byte buffer to TIOCGWINSZ ioctl
    which expects 8 bytes.

    See: https://github.com/Backblaze/B2_Command_Line_Tool/issues/1119

    NOTE: This test uses pexpect to spawn a subprocess with a real PTY.
    It works correctly in CI even with pytest-xdist parallelization.
    However, when run locally with nox, the test environment may not properly
    trigger the buffer overflow, causing the test to pass even without the fix.
    This is due to differences in how PTY state is managed in local vs CI environments.
    """
    pexpect = pytest.importorskip('pexpect')

    # Set up environment - remove LINES/COLUMNS to ensure ioctl is called
    env = os.environ.copy()
    env.pop('LINES', None)
    env.pop('COLUMNS', None)

    # Spawn b2 --help with pexpect to create a real PTY
    # This is where the bug would trigger on Python 3.14 without our fix
    child = pexpect.spawn(
        cli_version,
        ['--help'],
        env=env,
        timeout=10,
    )

    # Wait for process to complete
    child.expect(pexpect.EOF)

    # Get the output
    output = child.before.decode('utf-8', errors='replace')

    # Check exit status
    child.close()
    exit_code = child.exitstatus

    # Verify the command succeeded and produced help output
    assert exit_code == 0, (
        f'b2 --help failed with exit code {exit_code}.\n'
        f'This may indicate the buffer overflow bug is not properly handled.\n'
        f'Output: {output}\n'
        f'See: https://github.com/Backblaze/B2_Command_Line_Tool/issues/1119'
    )

    # Verify help output contains expected content
    assert 'b2 <command>' in output or cli_version in output, (
        f'Help output does not contain expected content.\n'
        f'Output: {output}'
    )
