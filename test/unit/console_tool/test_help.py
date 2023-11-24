######################################################################
#
# File: test/unit/console_tool/test_help.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
import pytest


@pytest.mark.parametrize(
    "flag, included, excluded",
    [
        # --help shouldn't show deprecated commands
        (
            "--help",
            [" b2 download-file ", "-h", "--help-all"],
            [" download-file-by-name ", "(DEPRECATED)"],
        ),
        # --help-all should show deprecated commands, but marked as deprecated
        (
            "--help-all",
            ["(DEPRECATED) b2 download-file-by-name ", "-h", "--help-all"],
            [],
        ),
    ],
)
def test_help(b2_cli, flag, included, excluded, capsys):
    b2_cli.run([flag], expected_stdout=None)

    out = capsys.readouterr().out

    found = set()
    for i in included:
        if i in out:
            found.add(i)
    for e in excluded:
        if e in out:
            found.add(e)
    assert found.issuperset(included), f"expected {included!r} in {out!r}"
    assert found.isdisjoint(excluded), f"expected {excluded!r} not in {out!r}"
