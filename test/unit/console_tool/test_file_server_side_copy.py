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

import pytest


@pytest.mark.apiver
def test_copy_file_by_id(b2_cli, api_bucket, uploaded_file):
    expected_json = {
        "accountId": b2_cli.account_id,
        "action": "copy",
        "bucketId": api_bucket.id_,
        "size": 11,
        "contentSha1": "2aae6c35c94fcfb415dbe95f408b9ce91ee846ed",
        "contentType": "b2/x-auto",
        "fileId": "9998",
        "fileInfo": {
            "src_last_modified_millis": "1500111222000"
        },
        "fileName": "file1_copy.txt",
        "serverSideEncryption": {
            "mode": "none"
        },
        "uploadTimestamp": 5001
    }
    b2_cli.run(
        ['file', 'copy-by-id', '9999', 'my-bucket', 'file1_copy.txt'],
        expected_json_in_stdout=expected_json,
        expected_stderr=
        'WARNING: `copy-by-id` command is deprecated. Use `file server-side-copy` instead.\n',
    )


@pytest.mark.apiver
def test_file_server_side_copy__with_range(b2_cli, api_bucket, uploaded_file):
    expected_json = {
        "accountId": b2_cli.account_id,
        "action": "copy",
        "bucketId": api_bucket.id_,
        "size": 5,
        "contentSha1": "4f664540ff30b8d34e037298a84e4736be39d731",
        "contentType": "b2/x-auto",
        "fileId": "9998",
        "fileInfo": {
            "src_last_modified_millis": "1500111222000"
        },
        "fileName": "file1_copy.txt",
        "serverSideEncryption": {
            "mode": "none"
        },
        "uploadTimestamp": 5001
    }
    b2_cli.run(
        [
            'file', 'server-side-copy', '--range', '3,7', f'b2id://{uploaded_file["fileId"]}',
            'b2://my-bucket/file1_copy.txt'
        ],
        expected_json_in_stdout=expected_json,
    )


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
            'b2id://9999',
            'b2://my-bucket/file1_copy.txt',
        ],
        '',
        expected_stderr="ERROR: File info can be set only when content type is set\n",
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
            'b2id://9999',
            'b2://my-bucket/file1_copy.txt',
        ],
        '',
        expected_stderr="ERROR: File info can be not set only when content type is not set\n",
        expected_status=1,
    )

    # replace with content type and file info
    expected_json = {
        "accountId": b2_cli.account_id,
        "action": "copy",
        "bucketId": api_bucket.id_,
        "size": 11,
        "contentSha1": "2aae6c35c94fcfb415dbe95f408b9ce91ee846ed",
        "contentType": "text/plain",
        "fileId": "9998",
        "fileInfo": {
            "a": "b"
        },
        "fileName": "file1_copy.txt",
        "serverSideEncryption": {
            "mode": "none"
        },
        "uploadTimestamp": 5001
    }
    b2_cli.run(
        [
            'file',
            'server-side-copy',
            '--content-type',
            'text/plain',
            '--info',
            'a=b',
            'b2id://9999',
            'b2://my-bucket/file1_copy.txt',
        ],
        expected_json_in_stdout=expected_json,
    )


@pytest.mark.apiver
def test_file_server_side_copy__unsatisfied_range(b2_cli, api_bucket, uploaded_file):
    expected_stderr = "ERROR: The range in the request is outside the size of the file\n"
    b2_cli.run(
        [
            'file', 'server-side-copy', '--range', '12,20', 'b2id://9999',
            'b2://my-bucket/file1_copy.txt'
        ],
        '',
        expected_stderr,
        1,
    )

    # Copy in different bucket
    b2_cli.run(['bucket', 'create', 'my-bucket1', 'allPublic'], 'bucket_1\n', '', 0)
    expected_json = {
        "accountId": b2_cli.account_id,
        "action": "copy",
        "bucketId": "bucket_1",
        "size": 11,
        "contentSha1": "2aae6c35c94fcfb415dbe95f408b9ce91ee846ed",
        "contentType": "b2/x-auto",
        "fileId": "9997",
        "fileInfo": {
            "src_last_modified_millis": "1500111222000"
        },
        "fileName": "file1_copy.txt",
        "serverSideEncryption": {
            "mode": "none"
        },
        "uploadTimestamp": 5001
    }
    b2_cli.run(
        ['file', 'server-side-copy', 'b2id://9999', 'b2://my-bucket1/file1_copy.txt'],
        expected_json_in_stdout=expected_json,
    )


@pytest.mark.apiver
def test_copy_file_by_id__deprecated(b2_cli, api_bucket, uploaded_file):
    expected_json = {
        "accountId": b2_cli.account_id,
        "action": "copy",
        "bucketId": api_bucket.id_,
        "size": 11,
        "contentSha1": "2aae6c35c94fcfb415dbe95f408b9ce91ee846ed",
        "contentType": "b2/x-auto",
        "fileId": "9998",
        "fileInfo": {
            "src_last_modified_millis": "1500111222000"
        },
        "fileName": "file1_copy_2.txt",
        "serverSideEncryption": {
            "mode": "none"
        },
        "uploadTimestamp": 5001
    }
    b2_cli.run(
        ['copy-file-by-id', '9999', api_bucket.name, 'file1_copy_2.txt'],
        expected_stderr=
        'WARNING: `copy-file-by-id` command is deprecated. Use `file server-side-copy` instead.\n',
        expected_json_in_stdout=expected_json,
    )


@pytest.mark.apiver
def test_file_server_side_copy__by_b2_uri(b2_cli, api_bucket, uploaded_file):
    b2_cli.run(
        [
            "file", "server-side-copy",
            f"b2://{uploaded_file['bucket']}/{uploaded_file['fileName']}",
            f"b2://{uploaded_file['bucket']}/copy.bin"
        ],
    )
    assert [fv.file_name for fv, _ in api_bucket.ls()] == ['copy.bin', uploaded_file['fileName']]


@pytest.mark.apiver
def test_file_hide__by_b2id_uri(b2_cli, api_bucket, uploaded_file):
    b2_cli.run(
        [
            "file", "server-side-copy", f"b2id://{uploaded_file['fileId']}",
            f"b2://{uploaded_file['bucket']}/copy.bin"
        ],
    )
    assert [fv.file_name for fv, _ in api_bucket.ls()] == ['copy.bin', uploaded_file['fileName']]
