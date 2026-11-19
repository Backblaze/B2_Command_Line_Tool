######################################################################
#
# File: test/unit/console_tool/test_profile_before_subcommand.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import os
import subprocess
import sys
from pathlib import Path

import pytest
from b2sdk.v3 import SqliteAccountInfo
from b2sdk.v3.exception import MissingAccountData

from b2._internal.console_tool import B2

ROOT = Path(__file__).resolve().parents[3]


@pytest.fixture
def profile_accounts(tmp_path, monkeypatch):
    for name in list(os.environ):
        if name.startswith('B2_'):
            monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv('HOME', str(tmp_path))
    monkeypatch.setenv('USERPROFILE', str(tmp_path))
    monkeypatch.setenv('XDG_CONFIG_HOME', str(tmp_path))

    default_path = Path(SqliteAccountInfo.get_user_account_info_path()).resolve()
    selected_path = Path(
        SqliteAccountInfo.get_user_account_info_path(profile='myprofile')
    ).resolve()
    assert default_path.is_relative_to(tmp_path.resolve())
    assert selected_path.is_relative_to(tmp_path.resolve())
    assert default_path != selected_path

    default_account = SqliteAccountInfo()
    selected_account = SqliteAccountInfo(profile='myprofile')
    for account, account_id in (
        (default_account, 'default-account'),
        (selected_account, 'selected-account'),
    ):
        account.set_auth_data(
            account_id=account_id,
            auth_token='test-token',
            api_url='https://api.example.invalid',
            download_url='https://download.example.invalid',
            recommended_part_size=100,
            absolute_minimum_part_size=10,
            application_key='test-key',
            realm='production',
            s3_api_url='https://s3.example.invalid',
            allowed=None,
            application_key_id=account_id,
        )
        assert account.get_account_id() == account_id

    return default_account, selected_account


def _run_cli(cli_version, argv):
    return subprocess.run(
        [sys.executable, '-m', f'b2._internal.{cli_version}', *argv],
        cwd=ROOT,
        env=os.environ.copy(),
        capture_output=True,
        text=True,
        timeout=30,
    )


@pytest.mark.parametrize(
    'argv',
    [
        ['--profile', 'myprofile', 'account', 'clear'],
        ['account', '--profile', 'myprofile', 'clear'],
        ['account', 'clear', '--profile', 'myprofile'],
        ['--profile=myprofile', 'account', 'clear'],
        ['account', '--profile=myprofile', 'clear'],
        [
            '--profile',
            'myprofile',
            'account',
            '--profile',
            'myprofile',
            'clear',
            '--profile',
            'myprofile',
        ],
    ],
    ids=['before-command', 'before-leaf', 'after-leaf', 'equals-root', 'equals-group', 'repeated'],
)
def test_account_clear_uses_the_selected_profile(argv, cli_version, profile_accounts):
    default_account, selected_account = profile_accounts
    result = _run_cli(cli_version, argv)

    assert result.returncode == 0, result.stderr
    assert default_account.get_account_id() == 'default-account'
    with pytest.raises(MissingAccountData):
        selected_account.get_account_id()


@pytest.mark.parametrize(
    ('argv', 'expected'),
    [
        (['bucket', 'notification-rule', 'delete', 'b2://bucket', 'rule'], None),
        (
            ['--profile', 'named', 'bucket', 'notification-rule', 'delete', 'b2://bucket', 'rule'],
            'named',
        ),
        (
            ['bucket', '--profile', 'named', 'notification-rule', 'delete', 'b2://bucket', 'rule'],
            'named',
        ),
        (
            ['bucket', 'notification-rule', '--profile', 'named', 'delete', 'b2://bucket', 'rule'],
            'named',
        ),
        (
            ['bucket', 'notification-rule', 'delete', 'b2://bucket', 'rule', '--profile', 'named'],
            'named',
        ),
        (
            [
                '--profile',
                'first',
                'bucket',
                '--profile',
                'second',
                'notification-rule',
                '--profile',
                'third',
                'delete',
                'b2://bucket',
                'rule',
            ],
            'third',
        ),
    ],
)
def test_profile_at_each_nested_parser_depth(console_tool_class, argv, expected):
    args = B2.create_parser(name='b2').parse_args(argv)
    assert args.profile == expected


@pytest.mark.parametrize(
    'argv',
    [
        ['--profile=', 'account', 'clear'],
        ['--profile', '', 'account', 'clear'],
        ['account', '--profile=', 'clear'],
        ['account', '--profile', '', 'clear'],
        ['account', 'clear', '--profile='],
        ['account', 'clear', '--profile', ''],
        ['--profile', 'myprofile', 'account', 'clear', '--profile='],
        ['account', '--profile', 'myprofile', 'clear', '--profile', ''],
    ],
)
def test_empty_profile_is_rejected_without_clearing_accounts(argv, cli_version, profile_accounts):
    default_account, selected_account = profile_accounts
    result = _run_cli(cli_version, argv)
    assert result.returncode == 2, result.stderr
    assert 'profile' in result.stderr.lower() and 'empty' in result.stderr.lower()
    assert default_account.get_account_id() == 'default-account'
    assert selected_account.get_account_id() == 'selected-account'


@pytest.mark.parametrize('position', [1, 2])
def test_empty_profile_is_rejected_at_deeper_parser_levels(console_tool_class, position):
    argv = ['bucket', 'notification-rule', 'delete', 'b2://bucket', 'rule']
    argv[position:position] = ['--profile=']
    with pytest.raises(SystemExit) as error:
        B2.create_parser(name='b2').parse_args(argv)
    assert error.value.code == 2


def test_account_clear_without_profile_uses_default(cli_version, profile_accounts):
    default_account, selected_account = profile_accounts
    result = _run_cli(cli_version, ['account', 'clear'])
    assert result.returncode == 0, result.stderr
    with pytest.raises(MissingAccountData):
        default_account.get_account_id()
    assert selected_account.get_account_id() == 'selected-account'


@pytest.mark.parametrize('reuse_tool', [True, False])
def test_profile_does_not_carry_over_to_later_commands(cli_version, profile_accounts, reuse_tool):
    default_account, selected_account = profile_accounts
    script = """
import importlib
import sys
from io import StringIO

ConsoleTool = importlib.import_module(f'b2._internal.{sys.argv[1]}.registry').ConsoleTool
tool = ConsoleTool(StringIO(), StringIO())
assert tool.run_command(['b2', '--profile', 'myprofile', 'account', 'get']) == 0
assert tool.api.account_info.get_account_id() == 'selected-account'
if sys.argv[2] == 'False':
    tool = ConsoleTool(StringIO(), StringIO())
assert tool.run_command(['b2', 'account', 'clear']) == 0
"""
    result = subprocess.run(
        [sys.executable, '-c', script, cli_version, str(reuse_tool)],
        cwd=ROOT,
        env=os.environ.copy(),
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
    with pytest.raises(MissingAccountData):
        default_account.get_account_id()
    assert selected_account.get_account_id() == 'selected-account'


def test_reusing_parser_without_profile_uses_default(console_tool_class):
    parser = B2.create_parser(name='b2')
    assert parser.parse_args(['--profile', 'named', 'account', 'clear']).profile == 'named'
    assert parser.parse_args(['account', 'clear']).profile is None
