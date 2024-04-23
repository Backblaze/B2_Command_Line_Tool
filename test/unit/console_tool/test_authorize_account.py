######################################################################
#
# File: test/unit/console_tool/test_authorize_account.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from unittest import mock

import pytest
from b2sdk.v2 import ALL_CAPABILITIES

from b2._internal._cli.const import (
    B2_APPLICATION_KEY_ENV_VAR,
    B2_APPLICATION_KEY_ID_ENV_VAR,
    B2_ENVIRONMENT_ENV_VAR,
)


@pytest.fixture
def b2_cli_is_authorized_afterwards(b2_cli):
    assert b2_cli.account_info.get_account_auth_token() is None
    yield b2_cli
    assert b2_cli.account_info.get_account_auth_token() is not None


def test_authorize_with_bad_key(b2_cli):
    expected_stdout = ""
    expected_stderr = """
    ERROR: unable to authorize account: Invalid authorization token. Server said: secret key is wrong (unauthorized)
    """

    b2_cli._run_command(
        ["account", "authorize", b2_cli.account_id, "bad-app-key"],
        expected_stdout,
        expected_stderr,
        1,
    )
    assert b2_cli.account_info.get_account_auth_token() is None


@pytest.mark.parametrize(
    "command",
    [
        ["authorize-account"],
        ["authorize_account"],
        ["account", "authorize"],
    ],
)
def test_authorize_with_good_key(b2_cli, b2_cli_is_authorized_afterwards, command):
    assert b2_cli.account_info.get_account_auth_token() is None

    expected_stderr = "" if len(
        command
    ) == 2 else "WARNING: authorize-account command is deprecated. Use account instead.\n"

    b2_cli._run_command([*command, b2_cli.account_id, b2_cli.master_key], None, expected_stderr, 0)

    assert b2_cli.account_info.get_account_auth_token() is not None


def test_authorize_using_env_variables(b2_cli):
    assert b2_cli.account_info.get_account_auth_token() is None

    expected_stderr = """
    """

    with mock.patch.dict(
        "os.environ",
        {
            B2_APPLICATION_KEY_ID_ENV_VAR: b2_cli.account_id,
            B2_APPLICATION_KEY_ENV_VAR: b2_cli.master_key,
        },
    ):
        b2_cli._run_command(["account", "authorize"], None, expected_stderr, 0)

    assert b2_cli.account_info.get_account_auth_token() is not None


@pytest.mark.parametrize(
    "flags,realm_url",
    [
        ([], "http://production.example.com"),
        (["--debug-logs"], "http://production.example.com"),
        (["--environment", "http://custom.example.com"], "http://custom.example.com"),
        (["--environment", "production"], "http://production.example.com"),
        (["--dev"], "http://api.backblazeb2.xyz:8180"),
        (["--staging"], "https://api.backblaze.net"),
    ],
)
def test_authorize_towards_realm(
    b2_cli, b2_cli_is_authorized_afterwards, flags, realm_url, cwd_path, b2_cli_log_fix
):
    expected_stderr = f"Using {realm_url}\n" if any(f != "--debug-logs" for f in flags) else ""

    b2_cli._run_command(
        ["account", "authorize", *flags, b2_cli.account_id, b2_cli.master_key],
        None,
        expected_stderr,
        0,
    )
    log_path = cwd_path / "b2_cli.log"
    if "--debug-logs" in flags:
        assert f"Using {realm_url}\n" in log_path.read_text()
    else:
        assert not log_path.exists()


def test_authorize_towards_custom_realm_using_env(b2_cli, b2_cli_is_authorized_afterwards):
    expected_stderr = """
    Using http://custom2.example.com
    """

    with mock.patch.dict(
        "os.environ",
        {
            B2_ENVIRONMENT_ENV_VAR: "http://custom2.example.com",
        },
    ):
        b2_cli._run_command(
            ["account", "authorize", b2_cli.account_id, b2_cli.master_key],
            None,
            expected_stderr,
            0,
        )


def test_authorize_account_prints_account_info(b2_cli):
    expected_json = {
        'accountAuthToken': 'auth_token_0',
        'accountFilePath': None,
        'accountId': 'account-0',
        'allowed':
            {
                'bucketId': None,
                'bucketName': None,
                'capabilities': sorted(ALL_CAPABILITIES),
                'namePrefix': None,
            },
        'apiUrl': 'http://api.example.com',
        'applicationKey': 'masterKey-0',
        'applicationKeyId': 'account-0',
        'downloadUrl': 'http://download.example.com',
        'isMasterKey': True,
        's3endpoint': 'http://s3.api.example.com'
    }

    b2_cli._run_command(
        ['account', 'authorize', b2_cli.account_id, b2_cli.master_key],
        expected_stderr='',
        expected_status=0,
        expected_json_in_stdout=expected_json,
    )
