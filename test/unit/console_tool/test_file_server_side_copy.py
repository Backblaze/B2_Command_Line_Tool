######################################################################
#
# File: test/unit/console_tool/test_file_server_side_copy.py
#
# Copyright 2024 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import base64
import json

import pytest
from b2sdk.v3 import EncryptionAlgorithm, EncryptionKey, EncryptionMode, EncryptionSetting

from test.unit.console_tool.test_command_regressions import run
from test.unit.helpers import auto_content_type


@pytest.mark.parametrize('encryption_side', ['source', 'destination'])
@pytest.mark.parametrize(
    'metadata_options', [[], ['--fetch-metadata'], ['--content-type', 'text/plain', '--no-info']]
)
def test_sse_c_copy_metadata_requirement(
    b2_cli, api_bucket, uploaded_file, monkeypatch, encryption_side, metadata_options
):
    key = b'a' * 32
    encoded_key = base64.b64encode(key).decode()
    monkeypatch.setenv(f'B2_{encryption_side.upper()}_SSE_C_KEY_B64', encoded_key)
    if encryption_side == 'source':
        source_id = api_bucket.upload_bytes(
            b'hello world',
            'encrypted-source.txt',
            encryption=EncryptionSetting(
                EncryptionMode.SSE_C,
                EncryptionAlgorithm.AES256,
                EncryptionKey(key, 'source-key'),
            ),
        ).id_
    else:
        source_id = uploaded_file['fileId']
        monkeypatch.setenv('B2_DESTINATION_SSE_C_KEY_ID', 'destination-key')

    status, stdout, stderr = run(
        b2_cli,
        [
            'file',
            'server-side-copy',
            f'--{encryption_side}-server-side-encryption',
            'SSE-C',
            *metadata_options,
            f'b2id://{source_id}',
            f'b2://{api_bucket.name}/copy.txt',
        ],
    )
    if metadata_options:
        assert status == 0
        assert stderr == ''
        assert json.loads(stdout)['fileName'] == 'copy.txt'
    else:
        assert status == 1
        assert stdout == ''
        assert stderr == (
            'ERROR: Attempting to copy file with metadata while either source or destination uses '
            'SSE-C. Use --fetch-metadata to fetch source file metadata before copying.\n'
        )
        assert all(file.file_name != 'copy.txt' for file, _ in api_bucket.ls())


def assert_copied_file(b2_cli, stdout, expected_json, bucket_name, file_name):
    """Check the printed copy against expected_json plus the id and timestamp the server gave it."""
    copied = b2_cli.b2_api.get_bucket_by_name(bucket_name).get_file_info_by_name(file_name)
    expected = {
        **expected_json,
        'fileId': copied.id_,
        'uploadTimestamp': copied.upload_timestamp,
    }
    b2_cli.assertDictIsContained(expected, json.loads(stdout))


@pytest.mark.apiver
def test_copy_file_by_id(b2_cli, api_bucket, uploaded_file):
    expected_json = {
        'accountId': b2_cli.account_id,
        'action': 'copy',
        'bucketId': api_bucket.id_,
        'size': 11,
        'contentSha1': '2aae6c35c94fcfb415dbe95f408b9ce91ee846ed',
        'contentType': auto_content_type('text/plain'),
        'fileInfo': {'src_last_modified_millis': '1500111222000'},
        'fileName': 'file1_copy.txt',
        'serverSideEncryption': {'algorithm': 'AES256', 'mode': 'SSE-B2'},
    }
    _, stdout, _ = b2_cli.run(
        ['file', 'copy-by-id', uploaded_file['fileId'], 'my-bucket', 'file1_copy.txt'],
        expected_stderr='WARNING: `copy-by-id` command is deprecated. Use `file server-side-copy` instead.\n',
    )
    assert_copied_file(b2_cli, stdout, expected_json, 'my-bucket', 'file1_copy.txt')


@pytest.mark.apiver
def test_file_server_side_copy__with_range(b2_cli, api_bucket, uploaded_file):
    expected_json = {
        'accountId': b2_cli.account_id,
        'action': 'copy',
        'bucketId': api_bucket.id_,
        'size': 5,
        'contentSha1': '4f664540ff30b8d34e037298a84e4736be39d731',
        'contentType': auto_content_type('text/plain'),
        'fileInfo': {'src_last_modified_millis': '1500111222000'},
        'fileName': 'file1_copy.txt',
        'serverSideEncryption': {'algorithm': 'AES256', 'mode': 'SSE-B2'},
    }
    _, stdout, _ = b2_cli.run(
        [
            'file',
            'server-side-copy',
            '--range',
            '3,7',
            f'b2id://{uploaded_file["fileId"]}',
            'b2://my-bucket/file1_copy.txt',
        ],
    )
    assert_copied_file(b2_cli, stdout, expected_json, 'my-bucket', 'file1_copy.txt')


@pytest.mark.apiver
def test_file_server_side_copy__invalid_metadata_copy_with_file_info(
    b2_cli, api_bucket, uploaded_file
):
    b2_cli.run(
        [
            'file',
            'server-side-copy',
            '--info',
            'a=b',
            f'b2id://{uploaded_file["fileId"]}',
            'b2://my-bucket/file1_copy.txt',
        ],
        '',
        expected_stderr='ERROR: File info can be set only when content type is set\n',
        expected_status=1,
    )


@pytest.mark.apiver
def test_file_server_side_copy__invalid_metadata_replace_file_info(
    b2_cli, api_bucket, uploaded_file
):
    b2_cli.run(
        [
            'file',
            'server-side-copy',
            '--content-type',
            'text/plain',
            f'b2id://{uploaded_file["fileId"]}',
            'b2://my-bucket/file1_copy.txt',
        ],
        '',
        expected_stderr='ERROR: File info can be not set only when content type is not set\n',
        expected_status=1,
    )

    # replace with content type and file info
    expected_json = {
        'accountId': b2_cli.account_id,
        'action': 'copy',
        'bucketId': api_bucket.id_,
        'size': 11,
        'contentSha1': '2aae6c35c94fcfb415dbe95f408b9ce91ee846ed',
        'contentType': 'text/plain',
        'fileInfo': {'a': 'b'},
        'fileName': 'file1_copy.txt',
        'serverSideEncryption': {'algorithm': 'AES256', 'mode': 'SSE-B2'},
    }
    _, stdout, _ = b2_cli.run(
        [
            'file',
            'server-side-copy',
            '--content-type',
            'text/plain',
            '--info',
            'a=b',
            f'b2id://{uploaded_file["fileId"]}',
            'b2://my-bucket/file1_copy.txt',
        ],
    )
    assert_copied_file(b2_cli, stdout, expected_json, 'my-bucket', 'file1_copy.txt')


@pytest.mark.apiver
def test_file_server_side_copy__unsatisfied_range(b2_cli, api_bucket, uploaded_file):
    expected_stderr = 'ERROR: The range in the request is outside the size of the file\n'
    b2_cli.run(
        [
            'file',
            'server-side-copy',
            '--range',
            '12,20',
            f'b2id://{uploaded_file["fileId"]}',
            'b2://my-bucket/file1_copy.txt',
        ],
        '',
        expected_stderr,
        1,
    )

    # Copy in different bucket
    bucket_1_id = b2_cli._create_bucket('my-bucket1', 'allPublic')
    expected_json = {
        'accountId': b2_cli.account_id,
        'action': 'copy',
        'bucketId': bucket_1_id,
        'size': 11,
        'contentSha1': '2aae6c35c94fcfb415dbe95f408b9ce91ee846ed',
        'contentType': auto_content_type('text/plain'),
        'fileInfo': {'src_last_modified_millis': '1500111222000'},
        'fileName': 'file1_copy.txt',
        'serverSideEncryption': {'algorithm': 'AES256', 'mode': 'SSE-B2'},
    }
    _, stdout, _ = b2_cli.run(
        [
            'file',
            'server-side-copy',
            f'b2id://{uploaded_file["fileId"]}',
            'b2://my-bucket1/file1_copy.txt',
        ],
    )
    assert_copied_file(b2_cli, stdout, expected_json, 'my-bucket1', 'file1_copy.txt')


@pytest.mark.apiver
def test_copy_file_by_id__deprecated(b2_cli, api_bucket, uploaded_file):
    expected_json = {
        'accountId': b2_cli.account_id,
        'action': 'copy',
        'bucketId': api_bucket.id_,
        'size': 11,
        'contentSha1': '2aae6c35c94fcfb415dbe95f408b9ce91ee846ed',
        'contentType': auto_content_type('text/plain'),
        'fileInfo': {'src_last_modified_millis': '1500111222000'},
        'fileName': 'file1_copy_2.txt',
        'serverSideEncryption': {'algorithm': 'AES256', 'mode': 'SSE-B2'},
    }
    _, stdout, _ = b2_cli.run(
        ['copy-file-by-id', uploaded_file['fileId'], api_bucket.name, 'file1_copy_2.txt'],
        expected_stderr='WARNING: `copy-file-by-id` command is deprecated. Use `file server-side-copy` instead.\n',
    )
    assert_copied_file(b2_cli, stdout, expected_json, api_bucket.name, 'file1_copy_2.txt')


@pytest.mark.apiver
def test_file_server_side_copy__by_b2_uri(b2_cli, api_bucket, uploaded_file):
    b2_cli.run(
        [
            'file',
            'server-side-copy',
            f"b2://{uploaded_file['bucket']}/{uploaded_file['fileName']}",
            f"b2://{uploaded_file['bucket']}/copy.bin",
        ],
    )
    assert [fv.file_name for fv, _ in api_bucket.ls()] == ['copy.bin', uploaded_file['fileName']]


@pytest.mark.apiver
def test_file_hide__by_b2id_uri(b2_cli, api_bucket, uploaded_file):
    b2_cli.run(
        [
            'file',
            'server-side-copy',
            f"b2id://{uploaded_file['fileId']}",
            f"b2://{uploaded_file['bucket']}/copy.bin",
        ],
    )
    assert [fv.file_name for fv, _ in api_bucket.ls()] == ['copy.bin', uploaded_file['fileName']]
