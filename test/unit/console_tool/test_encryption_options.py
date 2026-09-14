######################################################################
#
# File: test/unit/console_tool/test_encryption_options.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import pytest

BUCKET_PLAINTEXT_COMMANDS = [
    [
        'bucket',
        'create',
        '--default-server-side-encryption',
        'none',
        'my-bucket',
        'allPrivate',
    ],
    [
        'bucket',
        'update',
        '--default-server-side-encryption',
        'none',
        'my-bucket',
    ],
    [
        'create-bucket',
        '--default-server-side-encryption',
        'none',
        'my-bucket',
        'allPrivate',
    ],
    [
        'update-bucket',
        '--default-server-side-encryption',
        'none',
        'my-bucket',
    ],
]


@pytest.mark.apiver(to_ver=4)
@pytest.mark.parametrize('command', BUCKET_PLAINTEXT_COMMANDS)
def test_legacy_bucket_plaintext_option_has_explanatory_error(authorized_b2_cli, command):
    _, _, stderr = authorized_b2_cli.run(
        command,
        expected_status=1,
        expected_stderr=None,
    )

    assert "'none' is no longer supported for --default-server-side-encryption" in stderr
    if command[0] != 'bucket':
        assert f'`{command[0]}` command is deprecated' in stderr


@pytest.mark.apiver(from_ver=5)
@pytest.mark.parametrize('command', BUCKET_PLAINTEXT_COMMANDS)
def test_bucket_plaintext_option_is_not_offered(b2_cli, command, capfd):
    b2_cli.run(command, expected_status=2, expected_stderr=None)

    stderr = capfd.readouterr().err
    assert "invalid choice: 'none'" in stderr
    assert "'none' is no longer supported for --default-server-side-encryption" not in stderr
