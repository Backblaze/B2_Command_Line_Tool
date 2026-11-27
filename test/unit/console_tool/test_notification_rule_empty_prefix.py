######################################################################
#
# File: test/unit/console_tool/test_notification_rule_empty_prefix.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
import copy
import json

import pytest
from b2sdk.v3 import Bucket

# B2 rule names are 6 to 63 letters, digits and hyphens (Event Notifications reference;
# b2-simulator ledger F-68).
RULE_NAME = 'prefix-rule'


def create_rule(b2_cli, bucket, command, prefix):
    _, created, _ = b2_cli.run(
        [
            *command,
            'create',
            '--json',
            f'b2://{bucket}/{prefix}',
            RULE_NAME,
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
        [*command, 'update', '--json', f'b2://{bucket}{trailing_slash}', RULE_NAME, '--disable']
    )
    _, listed, _ = b2_cli.run([*command, 'list', '--json', f'b2://{bucket}'])

    assert json.loads(updated)['objectNamePrefix'] == prefix
    assert json.loads(updated)['isEnabled'] is False
    assert json.loads(listed)[0]['objectNamePrefix'] == prefix


@pytest.mark.parametrize('command', [['bucket', 'notification-rule'], ['notification-rules']])
def test_update_with_path_replaces_existing_prefix(b2_cli, bucket, command):
    create_rule(b2_cli, bucket, command, 'restricted/prefix/')
    _, updated, _ = b2_cli.run(
        [*command, 'update', '--json', f'b2://{bucket}/new/prefix/', RULE_NAME, '--disable']
    )
    _, listed, _ = b2_cli.run([*command, 'list', '--json', f'b2://{bucket}'])
    assert json.loads(updated)['objectNamePrefix'] == 'new/prefix/'
    assert json.loads(listed)[0]['objectNamePrefix'] == 'new/prefix/'


def test_create_without_path_uses_empty_prefix(b2_cli, bucket, monkeypatch):
    requests = []
    original_set_rules = Bucket.set_notification_rules

    def record_set_rules(bucket, rules):
        requests.append(copy.deepcopy(rules))
        return original_set_rules(bucket, rules)

    monkeypatch.setattr(Bucket, 'set_notification_rules', record_set_rules)
    create_rule(b2_cli, bucket, ['bucket', 'notification-rule'], '')
    assert len(requests) == 1
    assert requests[0][0]['objectNamePrefix'] == ''
