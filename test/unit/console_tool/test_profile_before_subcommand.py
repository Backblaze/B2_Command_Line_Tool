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


@pytest.mark.parametrize(
    'argv',
    [
        ['--profile', 'myprofile', 'account', 'clear'],
        ['account', '--profile', 'myprofile', 'clear'],
        ['account', 'clear', '--profile', 'myprofile'],
    ],
    ids=['before-command', 'before-leaf', 'after-leaf'],
)
def test_account_clear_uses_the_selected_profile(argv, cli_version, tmp_path, monkeypatch):
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

    repo = Path(__file__).resolve().parents[3]
    result = subprocess.run(
        [sys.executable, '-m', f'b2._internal.{cli_version}', *argv],
        cwd=repo,
        env=os.environ.copy(),
        capture_output=True,
        text=True,
        timeout=30,
    )

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
