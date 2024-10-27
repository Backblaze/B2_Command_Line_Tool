######################################################################
#
# File: test/integration/test_help.py
#
# Copyright 2024 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
import platform
import re
import subprocess


def test_help(cli_version):
    p = subprocess.run(
        [cli_version, "--help"],
        check=True,
        capture_output=True,
        text=True,
    )

    # verify help contains apiver binary name
    expected_name = cli_version
    if platform.system() == 'Windows':
        expected_name += '.exe'
    assert re.match(r"^_?b2(v\d+)?(\.exe)?$", expected_name)  # test sanity check
    assert f"{expected_name} <command> --help" in p.stdout
