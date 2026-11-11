######################################################################
#
# File: test/unit/console_tool/test_notification_rules.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
import json
import logging
import os
import stat

import pytest

from b2._internal.console_tool import CommandError


@pytest.fixture()
def bucket_notification_rule(b2_cli, bucket):
    rule = {
        'eventTypes': ['b2:ObjectCreated:*'],
        'isEnabled': True,
        'isSuspended': False,
        'name': 'test-rule',
        'objectNamePrefix': '',
        'suspensionReason': '',
        'targetConfiguration': {
            'targetType': 'webhook',
            'url': 'https://example.com/webhook',
        },
    }
    _, stdout, _ = b2_cli.run(
        [
            'bucket',
            'notification-rule',
            'create',
            '--json',
            f'b2://{bucket}',
            'test-rule',
            '--webhook-url',
            'https://example.com/webhook',
            '--event-type',
            'b2:ObjectCreated:*',
        ],
    )
    actual_rule = json.loads(stdout)
    assert actual_rule == rule
    return actual_rule


@pytest.mark.parametrize('command', [['bucket', 'notification-rule'], ['notification-rules']])
def test_notification_rules__list_all(b2_cli, bucket, bucket_notification_rule, command):
    _, stdout, _ = b2_cli.run(
        [
            *command,
            'list',
            f'b2://{bucket}',
        ]
    )
    assert (
        stdout
        == f"""\
Notification rules for b2://{bucket}/ :
- name: test-rule
  eventTypes:
    - b2:ObjectCreated:*
  isEnabled: true
  isSuspended: false
  objectNamePrefix: ''
  suspensionReason: ''
  targetConfiguration:
    targetType: webhook
    url: https://example.com/webhook
"""
    )


@pytest.mark.parametrize('command', [['bucket', 'notification-rule'], ['notification-rules']])
def test_notification_rules__list_all_json(b2_cli, bucket, bucket_notification_rule, command):
    _, stdout, _ = b2_cli.run(
        [
            *command,
            'list',
            '--json',
            f'b2://{bucket}',
        ]
    )
    assert json.loads(stdout) == [bucket_notification_rule]


@pytest.mark.parametrize('command', [['bucket', 'notification-rule'], ['notification-rules']])
def test_notification_rules__update(b2_cli, bucket, bucket_notification_rule, command):
    bucket_notification_rule['isEnabled'] = False
    _, stdout, _ = b2_cli.run(
        [
            *command,
            'update',
            '--json',
            f'b2://{bucket}',
            bucket_notification_rule['name'],
            '--disable',
            '--custom-header',
            'X-Custom-Header=value=1',
        ],
    )
    bucket_notification_rule['targetConfiguration']['customHeaders'] = {
        'X-Custom-Header': 'value=1'
    }
    assert json.loads(stdout) == bucket_notification_rule


@pytest.mark.parametrize('command', [['bucket', 'notification-rule'], ['notification-rules']])
def test_notification_rules__update__no_such_rule(
    b2_cli, bucket, bucket_notification_rule, command
):
    b2_cli.run(
        [
            *command,
            'update',
            f'b2://{bucket}',
            f'{bucket_notification_rule["name"]}-unexisting',
            '--disable',
        ],
        expected_stderr=(
            "ERROR: rule with name 'test-rule-unexisting' does not exist on bucket "
            "'my-bucket', available rules: ['test-rule']\n"
        ),
        expected_status=1,
    )


@pytest.mark.parametrize('command', [['bucket', 'notification-rule'], ['notification-rules']])
def test_notification_rules__update__custom_header_malformed(
    b2_cli, bucket, bucket_notification_rule, command
):
    bucket_notification_rule['isEnabled'] = False
    _, stdout, _ = b2_cli.run(
        [
            *command,
            'update',
            '--json',
            f'b2://{bucket}',
            bucket_notification_rule['name'],
            '--disable',
            '--custom-header',
            'X-Custom-Header: value',
        ],
    )
    bucket_notification_rule['targetConfiguration']['customHeaders'] = {
        'X-Custom-Header: value': ''
    }
    assert json.loads(stdout) == bucket_notification_rule


def test_notification_rules__delete(b2_cli, bucket, bucket_notification_rule):
    _, stdout, _ = b2_cli.run(
        [
            'bucket',
            'notification-rule',
            'delete',
            f'b2://{bucket}',
            bucket_notification_rule['name'],
        ],
    )
    assert stdout == "Rule 'test-rule' has been deleted from b2://my-bucket/\n"


@pytest.mark.parametrize('command', [['bucket', 'notification-rule'], ['notification-rules']])
def test_notification_rules__delete_no_such_rule(b2_cli, bucket, bucket_notification_rule, command):
    b2_cli.run(
        [
            *command,
            'delete',
            f'b2://{bucket}',
            f'{bucket_notification_rule["name"]}-unexisting',
        ],
        expected_stderr=(
            "ERROR: no such rule to delete: 'test-rule-unexisting', available rules: ['test-rule'];"
            ' No rules have been deleted.\n'
        ),
        expected_status=1,
    )


@pytest.mark.parametrize(
    'args,expected_stdout',
    [
        (['-q'], ''),
        ([], 'No notification rules for b2://my-bucket/\n'),
        (['--json'], '[]\n'),
    ],
)
def test_notification_rules__no_rules(b2_cli, bucket, args, expected_stdout):
    b2_cli.run(
        ['bucket', 'notification-rule', 'list', f'b2://{bucket}', *args],
        expected_stdout=expected_stdout,
    )


@pytest.mark.parametrize('command', [['bucket', 'notification-rule'], ['notification-rules']])
def test_notification_rules__disable_enable(b2_cli, bucket, bucket_notification_rule, command):
    _, stdout, _ = b2_cli.run(
        [
            *command,
            'disable',
            '--json',
            f'b2://{bucket}',
            bucket_notification_rule['name'],
        ],
    )
    assert json.loads(stdout) == {**bucket_notification_rule, 'isEnabled': False}

    _, stdout, _ = b2_cli.run(
        [
            *command,
            'enable',
            '--json',
            f'b2://{bucket}',
            bucket_notification_rule['name'],
        ],
    )
    assert json.loads(stdout) == {**bucket_notification_rule, 'isEnabled': True}


@pytest.mark.parametrize(
    'subcommand',
    ['disable', 'enable'],
)
def test_notification_rules__disable_enable__no_such_rule(
    b2_cli,
    bucket,
    bucket_notification_rule,
    subcommand,
):
    b2_cli.run(
        [
            'bucket',
            'notification-rule',
            subcommand,
            f'b2://{bucket}',
            f'{bucket_notification_rule["name"]}-unexisting',
        ],
        expected_stderr=(
            "ERROR: rule with name 'test-rule-unexisting' does not exist on bucket "
            "'my-bucket', available rules: ['test-rule']\n"
        ),
        expected_status=1,
    )


@pytest.mark.parametrize('command', [['bucket', 'notification-rule'], ['notification-rules']])
def test_notification_rules__sign_secret(b2_cli, bucket, bucket_notification_rule, command):
    b2_cli.run(
        [
            *command,
            'update',
            '--json',
            f'b2://{bucket}',
            bucket_notification_rule['name'],
            '--sign-secret',
            'new-secret',
        ],
        expected_status=2,
    )

    _, stdout, _ = b2_cli.run(
        [
            *command,
            'update',
            '--json',
            f'b2://{bucket}',
            bucket_notification_rule['name'],
            '--sign-secret',
            '7' * 32,
        ],
    )
    bucket_notification_rule['targetConfiguration']['hmacSha256SigningSecret'] = '7' * 32
    assert json.loads(stdout) == bucket_notification_rule

    assert json.loads(
        b2_cli.run(
            [*command, 'list', '--json', f'b2://{bucket}'],
        )[1]
    ) == [bucket_notification_rule]


@pytest.mark.parametrize('command', [['bucket', 'notification-rule'], ['notification-rules']])
@pytest.mark.parametrize('subcommand', ['create', 'update'])
@pytest.mark.parametrize(
    'logging_flags', [['--verbose'], ['--debug-logs'], ['--verbose', '--debug-logs']]
)
def test_notification_rules__sensitive_arguments_are_not_logged(
    b2_cli,
    bucket,
    bucket_notification_rule,
    b2_cli_log_fix,
    cwd_path,
    caplog,
    capsys,
    command,
    subcommand,
    logging_flags,
):
    # Deliberately synthetic values, not credentials.
    sign_secret = 'a' * 32
    header_value = 'test-only-header-value'
    caplog.set_level(logging.DEBUG)
    caplog.set_level(logging.DEBUG, logger='b2sdk._internal.bucket')
    caplog.clear()
    capsys.readouterr()
    original_handlers = logging.root.handlers[:]

    try:
        b2_cli.run(
            [
                *command,
                subcommand,
                *logging_flags,
                f'b2://{bucket}',
                'new-rule' if subcommand == 'create' else bucket_notification_rule['name'],
                '--event-type',
                'b2:ObjectCreated:*',
                '--webhook-url',
                'https://example.com/webhook',
                '--sign-secret',
                sign_secret,
                '--custom-header',
                f'X-Test-Header={header_value}',
            ]
        )

        outputs = [caplog.text, capsys.readouterr().err]
        if '--debug-logs' in logging_flags:
            outputs.append((cwd_path / 'b2_cli.log').read_text())
        for output in outputs:
            assert sign_secret not in output
            assert header_value not in output
        assert 'arguments hidden' in caplog.text

        caplog.clear()
        control_args = ['bucket', 'create', *logging_flags, 'control-log-bucket', 'allPrivate']
        b2_cli.run(control_args)
        assert f"with arguments: {['b2', *control_args]}" in caplog.text
        assert 'calling Bucket(' in caplog.text
    finally:
        for handler in logging.root.handlers[:]:
            if handler not in original_handlers:
                logging.root.removeHandler(handler)
                handler.close()


@pytest.mark.parametrize('subcommand', ['create', 'update'])
@pytest.mark.parametrize('existing_log', [False, True], ids=['new', 'existing'])
def test_notification_rules__debug_log_permissions(
    b2_cli, bucket, bucket_notification_rule, b2_cli_log_fix, cwd_path, subcommand, existing_log
):
    marker = 'q' * 32
    log_path = cwd_path / 'b2_cli.log'
    if existing_log:
        log_path.write_text('previous entry\n')
        if os.name == 'posix':
            log_path.chmod(0o644)

    original_handlers = logging.root.handlers[:]
    previous_umask = os.umask(0o022) if os.name == 'posix' else None
    try:
        b2_cli.run(
            [
                'bucket',
                'notification-rule',
                subcommand,
                '--debug-logs',
                f'b2://{bucket}',
                'new-rule' if subcommand == 'create' else bucket_notification_rule['name'],
                '--event-type',
                'b2:ObjectCreated:*',
                '--webhook-url',
                'https://example.com/webhook',
                '--sign-secret',
                marker,
            ]
        )
    finally:
        if previous_umask is not None:
            os.umask(previous_umask)
        for handler in logging.root.handlers[:]:
            if handler not in original_handlers:
                logging.root.removeHandler(handler)
                handler.close()

    assert marker not in log_path.read_text()
    if os.name == 'posix':
        assert stat.S_IMODE(log_path.stat().st_mode) == 0o600


@pytest.mark.parametrize('sdk_log_level', [logging.DEBUG, logging.WARNING])
def test_notification_rules__sdk_logging_restored_after_error(
    b2_cli, bucket, bucket_notification_rule, caplog, monkeypatch, sdk_log_level
):
    sdk_logger = logging.getLogger('b2sdk._internal.bucket')
    caplog.set_level(logging.DEBUG)
    caplog.set_level(sdk_log_level, logger=sdk_logger.name)

    def reject_rules(*args, **kwargs):
        sdk_logger.debug('test-only SDK request trace')
        sdk_logger.warning('test-only SDK warning')
        raise CommandError('test-only request failure')

    monkeypatch.setattr(b2_cli.raw_api, 'set_bucket_notification_rules', reject_rules)
    b2_cli.run(
        [
            'bucket',
            'notification-rule',
            'update',
            f'b2://{bucket}',
            bucket_notification_rule['name'],
            '--disable',
        ],
        expected_status=1,
        expected_stderr='ERROR: test-only request failure\n',
    )

    assert 'test-only SDK request trace' not in caplog.text
    assert 'test-only SDK warning' in caplog.text
    assert sdk_logger.level == sdk_log_level
