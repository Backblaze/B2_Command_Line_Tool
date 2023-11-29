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

from b2._cli.const import (
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
        ["authorize-account", b2_cli.account_id, "bad-app-key"],
        expected_stdout,
        expected_stderr,
        1,
    )
    assert b2_cli.account_info.get_account_auth_token() is None


@pytest.mark.parametrize(
    "command",
    [
        "authorize-account",
        "authorize_account",
    ],
)
def test_authorize_with_good_key(b2_cli, b2_cli_is_authorized_afterwards, command):
    assert b2_cli.account_info.get_account_auth_token() is None

    expected_stderr = """
    """

    b2_cli._run_command([command, b2_cli.account_id, b2_cli.master_key], "", expected_stderr, 0)

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
        b2_cli._run_command(["authorize-account"], "", expected_stderr, 0)

    assert b2_cli.account_info.get_account_auth_token() is not None


@pytest.mark.parametrize(
    "flags,realm_url",
    [
        ([], "http://production.example.com"),
        (["--debugLogs"], "http://production.example.com"),
        (["--environment", "http://custom.example.com"], "http://custom.example.com"),
        (["--environment", "production"], "http://production.example.com"),
        (["--dev"], "http://api.backblazeb2.xyz:8180"),
        (["--staging"], "https://api.backblaze.net"),
    ],
)
def test_authorize_towards_realm(
    b2_cli, b2_cli_is_authorized_afterwards, flags, realm_url, cwd_path, b2_cli_log_fix
):
    expected_stderr = f"Using {realm_url}\n" if any(f != "--debugLogs" for f in flags) else ""

    b2_cli._run_command(
        ["authorize-account", *flags, b2_cli.account_id, b2_cli.master_key],
        "",
        expected_stderr,
        0,
    )
    log_path = cwd_path / "b2_cli.log"
    if "--debugLogs" in flags:
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
            ["authorize-account", b2_cli.account_id, b2_cli.master_key],
            "",
            expected_stderr,
            0,
        )
