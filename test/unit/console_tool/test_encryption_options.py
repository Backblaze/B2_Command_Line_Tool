######################################################################
#
# File: test/unit/console_tool/test_encryption_options.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import base64
from io import BytesIO

import pytest
from b2sdk.v3 import EncryptionAlgorithm, EncryptionKey, EncryptionMode, EncryptionSetting

from test.unit.console_tool.test_command_regressions import run


@pytest.mark.parametrize(
    'local_side, encryption_mode',
    [('destination', 'SSE-B2'), ('destination', 'SSE-C'), ('source', 'SSE-C')],
)
def test_sync_encryption_on_local_endpoint_reports_error_without_changes(
    b2_cli, api_bucket, uploaded_file, tmp_path, monkeypatch, local_side, encryption_mode
):
    monkeypatch.setenv(
        f'B2_{local_side.upper()}_SSE_C_KEY_B64', base64.b64encode(b'a' * 32).decode()
    )
    monkeypatch.setenv('B2_DESTINATION_SSE_C_KEY_ID', 'test-key')
    local_dir = tmp_path / 'local'
    local_dir.mkdir()
    sentinel = local_dir / 'sentinel.txt'
    sentinel.write_bytes(b'keep local data')
    remote_before = [(file.id_, file.file_name) for file, _ in api_bucket.ls(latest_only=False)]
    source, destination = str(local_dir), f'b2://{api_bucket.name}'
    if local_side == 'destination':
        source, destination = destination, source

    status, stdout, stderr = run(
        b2_cli,
        [
            'sync',
            '--no-progress',
            '--delete',
            f'--{local_side}-server-side-encryption',
            encryption_mode,
            source,
            destination,
        ],
    )

    assert status == 1
    assert stdout == ''
    assert stderr == (
        f'ERROR: server-side encryption cannot be set for a non-b2 sync {local_side}\n'
    )
    assert list(local_dir.iterdir()) == [sentinel]
    assert sentinel.read_bytes() == b'keep local data'
    assert [
        (file.id_, file.file_name) for file, _ in api_bucket.ls(latest_only=False)
    ] == remote_before


@pytest.mark.parametrize(
    'remote_side, encryption_mode',
    [('destination', 'SSE-B2'), ('destination', 'SSE-C'), ('source', 'SSE-C')],
)
def test_sync_encryption_on_b2_endpoint_preserves_content(
    b2_cli, api_bucket, tmp_path, monkeypatch, remote_side, encryption_mode
):
    key = b'a' * 32
    monkeypatch.setenv(f'B2_{remote_side.upper()}_SSE_C_KEY_B64', base64.b64encode(key).decode())
    monkeypatch.setenv('B2_DESTINATION_SSE_C_KEY_ID', 'test-key')
    encryption = EncryptionSetting(
        EncryptionMode(encryption_mode),
        EncryptionAlgorithm.AES256,
        EncryptionKey(key, 'test-key') if encryption_mode == 'SSE-C' else None,
    )
    content = b'encrypted sync content'
    local_file = tmp_path / 'file.txt'
    source, destination = str(tmp_path), f'b2://{api_bucket.name}'
    if remote_side == 'source':
        api_bucket.upload_bytes(content, 'file.txt', encryption=encryption)
        source, destination = destination, source
    else:
        local_file.write_bytes(content)

    status, _, stderr = run(
        b2_cli,
        [
            'sync',
            '--no-progress',
            f'--{remote_side}-server-side-encryption',
            encryption_mode,
            source,
            destination,
        ],
    )

    assert status == 0
    assert stderr == ''
    if remote_side == 'source':
        assert local_file.read_bytes() == content
    else:
        assert (
            api_bucket.get_file_info_by_name('file.txt').server_side_encryption.mode
            is encryption.mode
        )
        downloaded = BytesIO()
        api_bucket.download_file_by_name('file.txt', encryption=encryption).save(downloaded)
        assert downloaded.getvalue() == content


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
