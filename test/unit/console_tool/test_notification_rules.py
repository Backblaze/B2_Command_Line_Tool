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

import pytest


@pytest.fixture()
def bucket_notification_rule(b2_cli, bucket):
    rule = {
        "eventTypes": ["b2:ObjectCreated:*"],
        "isEnabled": True,
        "isSuspended": False,
        "name": "test-rule",
        "objectNamePrefix": "",
        "suspensionReason": "",
        "targetConfiguration": {
            "targetType": "webhook",
            "url": "https://example.com/webhook",
        },
    }
    _, stdout, _ = b2_cli.run(
        [
            "bucket", "notification-rule",
            "create",
            "--json",
            f"b2://{bucket}",
            "test-rule",
            "--webhook-url",
            "https://example.com/webhook",
            "--event-type",
            "b2:ObjectCreated:*",
        ],
    )
    actual_rule = json.loads(stdout)
    assert actual_rule == rule
    return actual_rule


def test_notification_rules__list_all(b2_cli, bucket, bucket_notification_rule):
    _, stdout, _ = b2_cli.run([
        "bucket", "notification-rule",
        "list",
        f"b2://{bucket}",
    ])
    assert (
        stdout == f"""\
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


def test_notification_rules__list_all_json(b2_cli, bucket, bucket_notification_rule):
    _, stdout, _ = b2_cli.run([
        "bucket", "notification-rule",
        "list",
        "--json",
        f"b2://{bucket}",
    ])
    assert json.loads(stdout) == [bucket_notification_rule]


def test_notification_rules__update(b2_cli, bucket, bucket_notification_rule):
    bucket_notification_rule["isEnabled"] = False
    _, stdout, _ = b2_cli.run(
        [
            "bucket", "notification-rule",
            "update",
            "--json",
            f"b2://{bucket}",
            bucket_notification_rule["name"],
            "--disable",
            "--custom-header",
            "X-Custom-Header=value=1",
        ],
    )
    bucket_notification_rule["targetConfiguration"]["customHeaders"] = {
        "X-Custom-Header": "value=1"
    }
    assert json.loads(stdout) == bucket_notification_rule


def test_notification_rules__update__no_such_rule(b2_cli, bucket, bucket_notification_rule):
    b2_cli.run(
        [
            "bucket", "notification-rule",
            "update",
            f"b2://{bucket}",
            f'{bucket_notification_rule["name"]}-unexisting',
            "--disable",
        ],
        expected_stderr=(
            "ERROR: rule with name 'test-rule-unexisting' does not exist on bucket "
            "'my-bucket', available rules: ['test-rule']\n"
        ),
        expected_status=1,
    )


def test_notification_rules__update__custom_header_malformed(
    b2_cli, bucket, bucket_notification_rule
):
    bucket_notification_rule["isEnabled"] = False
    _, stdout, _ = b2_cli.run(
        [
            "bucket", "notification-rule",
            "update",
            "--json",
            f"b2://{bucket}",
            bucket_notification_rule["name"],
            "--disable",
            "--custom-header",
            "X-Custom-Header: value",
        ],
    )
    bucket_notification_rule["targetConfiguration"]["customHeaders"] = {
        "X-Custom-Header: value": ""
    }
    assert json.loads(stdout) == bucket_notification_rule


def test_notification_rules__delete(b2_cli, bucket, bucket_notification_rule):
    _, stdout, _ = b2_cli.run(
        [
            "bucket", "notification-rule",
            "delete",
            f"b2://{bucket}",
            bucket_notification_rule["name"],
        ],
    )
    assert stdout == "Rule 'test-rule' has been deleted from b2://my-bucket/\n"


def test_notification_rules__delete_no_such_rule(b2_cli, bucket, bucket_notification_rule):
    b2_cli.run(
        [
            "bucket", "notification-rule",
            "delete",
            f"b2://{bucket}",
            f'{bucket_notification_rule["name"]}-unexisting',
        ],
        expected_stderr=(
            "ERROR: no such rule to delete: 'test-rule-unexisting', available rules: ['test-rule'];"
            " No rules have been deleted.\n"
        ),
        expected_status=1,
    )


@pytest.mark.parametrize(
    "args,expected_stdout",
    [
        (["-q"], ""),
        ([], "No notification rules for b2://my-bucket/\n"),
        (["--json"], "[]\n"),
    ],
)
def test_notification_rules__no_rules(b2_cli, bucket, args, expected_stdout):
    b2_cli.run(
        ["bucket", "notification-rule", "list", f"b2://{bucket}", *args],
        expected_stdout=expected_stdout,
    )


def test_notification_rules__disable_enable(b2_cli, bucket, bucket_notification_rule):
    _, stdout, _ = b2_cli.run(
        [
            "bucket", "notification-rule",
            "disable",
            "--json",
            f"b2://{bucket}",
            bucket_notification_rule["name"],
        ],
    )
    assert json.loads(stdout) == {**bucket_notification_rule, "isEnabled": False}

    _, stdout, _ = b2_cli.run(
        [
            "bucket", "notification-rule",
            "enable",
            "--json",
            f"b2://{bucket}",
            bucket_notification_rule["name"],
        ],
    )
    assert json.loads(stdout) == {**bucket_notification_rule, "isEnabled": True}


@pytest.mark.parametrize(
    "command",
    ["disable", "enable"],
)
def test_notification_rules__disable_enable__no_such_rule(
    b2_cli, bucket, bucket_notification_rule, command
):
    b2_cli.run(
        [
            "bucket", "notification-rule",
            command,
            f"b2://{bucket}",
            f'{bucket_notification_rule["name"]}-unexisting',
        ],
        expected_stderr=(
            "ERROR: rule with name 'test-rule-unexisting' does not exist on bucket "
            "'my-bucket', available rules: ['test-rule']\n"
        ),
        expected_status=1,
    )


def test_notification_rules__sign_secret(b2_cli, bucket, bucket_notification_rule):
    b2_cli.run(
        [
            "bucket", "notification-rule",
            "update",
            "--json",
            f"b2://{bucket}",
            bucket_notification_rule["name"],
            "--sign-secret",
            "new-secret",
        ],
        expected_status=2,
    )

    _, stdout, _ = b2_cli.run(
        [
            "bucket", "notification-rule",
            "update",
            "--json",
            f"b2://{bucket}",
            bucket_notification_rule["name"],
            "--sign-secret",
            "7" * 32,
        ],
    )
    bucket_notification_rule["targetConfiguration"]["hmacSha256SigningSecret"] = "7" * 32
    assert json.loads(stdout) == bucket_notification_rule

    assert json.loads(b2_cli.run(["bucket", "notification-rule", "list", "--json", f"b2://{bucket}"],)[1]
                     ) == [bucket_notification_rule]
