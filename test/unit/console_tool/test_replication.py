######################################################################
#
# File: test/unit/console_tool/test_replication.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import pytest
from b2sdk.v3 import ReplicationConfiguration, ReplicationRule


@pytest.fixture
def replicated_bucket(b2_cli, api_bucket):
    b2_cli.run(
        ['bucket', 'create', 'my-destination-bucket', 'allPrivate'], expected_stdout='bucket_1\n'
    )
    api_bucket.update(
        replication=ReplicationConfiguration(
            rules=[
                ReplicationRule(
                    destination_bucket_id='bucket_1',
                    name='my-rule',
                    priority=1,
                )
            ],
            source_key_id='key-id',
        )
    )
    return api_bucket


def _rule_enabled(bucket) -> bool:
    (rule,) = bucket.get_fresh_state().replication.rules
    return rule.is_enabled


@pytest.mark.parametrize('command, enabled', [('pause', False), ('unpause', True)])
def test_replication_pause_unpause__is_idempotent(b2_cli, replicated_bucket, command, enabled):
    for _ in range(2):
        b2_cli.run(['replication', command, replicated_bucket.name, 'my-rule'], expected_stdout='')
        assert _rule_enabled(replicated_bucket) is enabled


def test_replication_pause__unknown_rule(b2_cli, replicated_bucket):
    b2_cli.run(
        ['replication', 'pause', replicated_bucket.name, 'no-such-rule'],
        expected_stderr='ERROR: replication rule could not be found!\n',
        expected_status=1,
    )
