######################################################################
#
# File: test/unit/console_tool/test_notification_rule_empty_prefix.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
import json

import pytest


def create_rule(b2_cli, bucket, command, prefix):
    _, created, _ = b2_cli.run(
        [
            *command,
            'create',
            '--json',
            f'b2://{bucket}/{prefix}',
            'rule',
            '--webhook-url',
            'https://example.com/webhook',
            '--event-type',
            'b2:ObjectCreated:*',
        ]
    )
    assert json.loads(created)['objectNamePrefix'] == prefix


@pytest.mark.parametrize('command', [['bucket', 'notification-rule'], ['notification-rules']])
@pytest.mark.parametrize('trailing_slash', ['', '/'])
def test_update_without_path_preserves_existing_prefix(b2_cli, bucket, command, trailing_slash):
    prefix = 'restricted/prefix/'
    create_rule(b2_cli, bucket, command, prefix)

    _, updated, _ = b2_cli.run(
        [*command, 'update', '--json', f'b2://{bucket}{trailing_slash}', 'rule', '--disable']
    )
    _, listed, _ = b2_cli.run([*command, 'list', '--json', f'b2://{bucket}'])

    assert json.loads(updated)['objectNamePrefix'] == prefix
    assert json.loads(updated)['isEnabled'] is False
    assert json.loads(listed)[0]['objectNamePrefix'] == prefix


@pytest.mark.parametrize('command', [['bucket', 'notification-rule'], ['notification-rules']])
def test_update_with_path_replaces_existing_prefix(b2_cli, bucket, command):
    create_rule(b2_cli, bucket, command, 'restricted/prefix/')
    _, updated, _ = b2_cli.run(
        [*command, 'update', '--json', f'b2://{bucket}/new/prefix/', 'rule', '--disable']
    )
    _, listed, _ = b2_cli.run([*command, 'list', '--json', f'b2://{bucket}'])
    assert json.loads(updated)['objectNamePrefix'] == 'new/prefix/'
    assert json.loads(listed)[0]['objectNamePrefix'] == 'new/prefix/'


def test_create_without_path_uses_empty_prefix(b2_cli, bucket):
    create_rule(b2_cli, bucket, ['bucket', 'notification-rule'], '')
