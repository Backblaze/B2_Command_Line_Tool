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
from uuid import uuid4

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
            auth_token=f'fixture-{uuid4()}',
            api_url='https://api.example.invalid',
            download_url='https://download.example.invalid',
            recommended_part_size=100,
            absolute_minimum_part_size=10,
            application_key=f'fixture-{uuid4()}',
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


@pytest.mark.parametrize('profile', ['../x', '.', 'has space', 'a/b', 'a\\b', 'é', 'a' * 65])
@pytest.mark.parametrize('position', [0, 1])
def test_invalid_profile_is_a_usage_error(profile, position, cli_version, profile_accounts):
    argv = ['version']
    argv[position:position] = ['--profile', profile]
    result = _run_cli(cli_version, argv)
    assert result.returncode == 2, result.stderr
    assert 'invalid profile name' in result.stderr.lower()
    assert 'Traceback' not in result.stderr
    assert profile_accounts[0].get_account_id() == 'default-account'
    assert profile_accounts[1].get_account_id() == 'selected-account'


@pytest.mark.parametrize('profile', ['a', 'A_09-', 'a' * 64])
def test_profile_grammar_boundaries(console_tool_class, profile):
    assert (
        B2.create_parser(name='b2').parse_args(['version', '--profile', profile]).profile == profile
    )


def test_profile_account_file_conflict_reports_command_error(
    cli_version, profile_accounts, tmp_path, monkeypatch
):
    monkeypatch.setenv('B2_ACCOUNT_INFO', str(tmp_path / 'account.sqlite'))
    result = _run_cli(cli_version, ['version', '--profile', 'myprofile'])
    assert result.returncode == 1
    assert 'ERROR:' in result.stderr and 'B2_ACCOUNT_INFO' in result.stderr
    assert 'Traceback' not in result.stderr


@pytest.mark.parametrize('profile_option', [[], ['--profile', 'myprofile']])
def test_cached_file_uri_completion_preserves_profile(
    cli_version, profile_accounts, profile_option, tmp_path
):
    for account, name in zip(profile_accounts, ('default-bucket', 'selected-bucket')):
        account.refresh_entire_bucket_name_cache([(name, name)])
    script = """
import importlib
import io
import os
import sys
from pathlib import Path

importlib.import_module(f'b2._internal.{sys.argv[1]}.registry')
from b2._internal.console_tool import B2
from b2._internal._cli.autocomplete_cache import AutocompleteCache, HomeCachePickleStore, VersionTracker

command = ' '.join(['b2', *sys.argv[3:], 'file', 'download', 'b2://'])
os.environ.update(COMP_LINE=command, COMP_POINT=str(len(command)), _ARGCOMPLETE='1', _ARGCOMPLETE_IFS='\\n')
expected = 'selected-bucket' if sys.argv[3:] else 'default-bucket'
cache = AutocompleteCache(VersionTracker(), HomeCachePickleStore(Path(sys.argv[2])))
for cache_state in ('cold', 'warm'):
    output = io.StringIO()
    exits = []
    kwargs = {'exit_method': exits.append, 'output_stream': output}
    if cache_state == 'cold':
        cache.cache_and_autocomplete(B2.create_parser(name='b2'), uncached_args=kwargs)
    else:
        cache.autocomplete_from_cache(uncached_args=kwargs, raise_exc=True)
    assert exits == [0], cache_state
    assert output.getvalue().splitlines() == [f'b2://{expected}/'], cache_state
"""
    result = subprocess.run(
        [sys.executable, '-c', script, cli_version, str(tmp_path / 'cache'), *profile_option],
        cwd=ROOT,
        env=os.environ.copy(),
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize(
    'command',
    [
        ['replication', 'setup', 'src-bucket', 'dst-bucket'],
        ['replication', 'status', 'src-bucket'],
    ],
)
@pytest.mark.parametrize('option', [['--destination-profile='], ['--destination-profile', '']])
def test_empty_destination_profile_is_rejected(console_tool_class, command, option, capsys):
    with pytest.raises(SystemExit) as error:
        B2.create_parser(name='b2').parse_args(['--profile', 'named', *command, *option])
    assert error.value.code == 2
    stderr = capsys.readouterr().err
    assert 'destination-profile' in stderr and 'empty' in stderr


@pytest.mark.parametrize(
    'command',
    [
        ['replication', 'setup', 'src-bucket', 'dst-bucket'],
        ['replication', 'status', 'src-bucket'],
    ],
)
@pytest.mark.parametrize('destination', [None, 'destination'])
def test_destination_profile_selection_is_preserved(console_tool_class, command, destination):
    option = [] if destination is None else ['--destination-profile', destination]
    args = B2.create_parser(name='b2').parse_args(['--profile', 'named', *command, *option])
    assert args.profile == 'named'
    assert args.destination_profile == destination
