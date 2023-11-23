######################################################################
#
# File: test/unit/console_tool/test_file_info.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
import pytest


@pytest.fixture
def uploaded_download_version(b2_cli, bucket_info, uploaded_file):
    return {
        "contentSha1": "2aae6c35c94fcfb415dbe95f408b9ce91ee846ed",
        "contentType": "b2/x-auto",
        "fileId": uploaded_file["fileId"],
        "fileInfo": {
            "src_last_modified_millis": "1500111222000"
        },
        "fileName": "file1.txt",
        "serverSideEncryption": {
            "mode": "none"
        },
        "size": 11,
        "uploadTimestamp": 5000,
    }


@pytest.fixture
def uploaded_file_version(b2_cli, bucket_info, uploaded_file, uploaded_download_version):
    return {
        **uploaded_download_version,
        "accountId": b2_cli.account_id,
        "action": "upload",
        "bucketId": uploaded_file["bucketId"],
    }


def test_get_file_info(b2_cli, uploaded_file_version):
    b2_cli.run(
        ["get-file-info", uploaded_file_version["fileId"]],
        expected_json_in_stdout=uploaded_file_version,
        expected_stderr='WARNING: get-file-info command is deprecated. Use file-info instead.\n',
    )


def test_file_info__b2_uri(b2_cli, bucket, uploaded_download_version):
    b2_cli.run(
        [
            "file-info",
            f'b2://{bucket}/{uploaded_download_version["fileName"]}',
        ],
        expected_json_in_stdout=uploaded_download_version,
    )


def test_file_info__b2id_uri(b2_cli, uploaded_file_version):
    b2_cli.run(
        ["file-info", f'b2id://{uploaded_file_version["fileId"]}'],
        expected_json_in_stdout=uploaded_file_version,
    )
