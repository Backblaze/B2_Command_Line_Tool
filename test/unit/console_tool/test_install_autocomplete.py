######################################################################
#
# File: test/unit/console_tool/test_install_autocomplete.py
#
# Copyright 2024 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import contextlib
import shutil
from test.helpers import skip_on_windows

import pexpect
import pytest


@contextlib.contextmanager
def pexpect_shell(shell_bin, env):
    p = pexpect.spawn(f"{shell_bin} -i", env=env, maxread=1000)
    p.setwinsize(100, 100)  # required to see all suggestions in tests
    yield p
    p.close()


@pytest.mark.parametrize("shell", ["bash", "zsh", "fish"])
@skip_on_windows
def test_install_autocomplete(b2_cli, env, shell, monkeypatch):
    shell_bin = shutil.which(shell)
    if shell_bin is None:
        pytest.skip(f"{shell} is not installed")

    monkeypatch.setenv("SHELL", shell_bin)
    b2_cli.run(
        ["install-autocomplete"],
        expected_part_of_stdout=f"Autocomplete successfully installed for {shell}",
    )

    with pexpect_shell(shell_bin, env=env) as pshell:
        pshell.send("b2 \t\t")
        pshell.expect_exact(["authorize-account", "download-file", "get-bucket"], timeout=30)
