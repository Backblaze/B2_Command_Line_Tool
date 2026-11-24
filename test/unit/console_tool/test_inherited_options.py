######################################################################
#
# File: test/unit/console_tool/test_inherited_options.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
import pytest

from b2._internal.console_tool import B2


def test_inherited_escape_option_sanitizes_download_metadata(b2_cli, api_bucket, tmp_path):
    uploaded = api_bucket.upload_bytes(
        b'payload', 'fixture.txt', file_info={'note': 'before\x1b[31mafter'}
    )
    target = tmp_path / 'download.txt'
    _, stdout, _ = b2_cli.run(
        [
            'file',
            '--escape-control-characters',
            'download',
            '--no-progress',
            f'b2id://{uploaded.id_}',
            str(target),
        ]
    )
    assert '\x1b' not in stdout
    assert 'before\\x1b[31mafter' in stdout
    assert target.read_bytes() == b'payload'


def test_inherited_quiet_option_suppresses_download_status(b2_cli, uploaded_file, tmp_path):
    target = tmp_path / 'download.txt'
    b2_cli.run(
        ['file', '--quiet', 'download', '--no-progress', 'b2id://9999', str(target)],
        expected_stdout='',
    )
    assert target.read_text() == uploaded_file['content']


@pytest.mark.parametrize(
    'option,attribute,value',
    [
        (['--quiet'], 'quiet', True),
        (['-q'], 'quiet', True),
        (['--verbose'], 'verbose', True),
        (['--debug-logs'], 'debug_logs', True),
        (['--log-config', 'fixture.ini'], 'log_config', 'fixture.ini'),
        (['--escape-control-characters'], 'escape_control_characters', True),
        (['--no-escape-control-characters'], 'escape_control_characters', False),
    ],
)
@pytest.mark.parametrize('depth', [1, 2, 3, 4])
def test_common_options_survive_nested_parsers(console_tool_class, option, attribute, value, depth):
    parser = B2.create_parser(name='b2', b2_binary_name='b2')
    argv = ['file', 'large', 'unfinished', 'list', 'b2://bucket']
    argv[depth:depth] = option
    assert getattr(parser.parse_args(argv), attribute) == value


def test_common_defaults_are_present_without_options(console_tool_class):
    parser = B2.create_parser(name='b2', b2_binary_name='b2')
    for argv in (['version'], ['file', 'large', 'unfinished', 'list', 'b2://bucket']):
        args = parser.parse_args(argv)
        assert args.quiet is False
        assert args.verbose is False
        assert args.debug_logs is False
        assert args.log_config is None
        assert args.escape_control_characters is None
        assert args.profile is None


def test_leaf_escape_option_overrides_inherited_value(console_tool_class):
    parser = B2.create_parser(name='b2', b2_binary_name='b2')
    args = parser.parse_args(
        [
            'file',
            '--escape-control-characters',
            'info',
            '--no-escape-control-characters',
            'b2id://abc',
        ]
    )
    assert args.escape_control_characters is False
