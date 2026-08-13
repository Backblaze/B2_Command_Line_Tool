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
    'flag, included, excluded',
    [
        # --help shouldn't show deprecated commands
        (
            '--help',
            [' b2 file ', '-h', '--help-all'],
            [' b2 download-file-by-name ', '(DEPRECATED)'],
        ),
        # --help-all should show deprecated commands, but marked as deprecated
        (
            '--help-all',
            ['(DEPRECATED) b2 download-file-by-name ', '-h', '--help-all'],
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
    assert found.issuperset(included), f'expected {included!r} in {out!r}'
    assert found.isdisjoint(excluded), f'expected {excluded!r} not in {out!r}'


@pytest.mark.parametrize(
    'argv',
    [
        pytest.param(['--nonexistent-flag'], id='unrecognized-argument'),
        pytest.param(['sync'], id='missing-required-arguments'),
        pytest.param(['sync', '--nonexistent-flag'], id='unrecognized-subcommand-argument'),
    ],
)
def test_help_on_command_line_error_goes_to_stderr(b2_cli, argv, capsys):
    """Help printed because of a command line error belongs on stderr, not stdout."""
    b2_cli.run(argv, expected_status=2, expected_stdout=None)

    captured = capsys.readouterr()
    assert captured.out == ''
    assert 'error:' in captured.err
    assert '-h, --help' in captured.err


def test_help_on_request_goes_to_stdout(b2_cli, capsys):
    b2_cli.run(['--help'], expected_stdout=None)

    captured = capsys.readouterr()
    assert '-h, --help' in captured.out
    assert captured.err == ''
