######################################################################
#
# File: test/unit/console_tool/test_get_url.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
import pytest


@pytest.fixture
def uploaded_file_url(bucket_info, uploaded_file):
    return f"http://download.example.com/file/{bucket_info['bucketName']}/{uploaded_file['fileName']}"


@pytest.fixture
def uploaded_file_url_by_id(uploaded_file):
    return f"http://download.example.com/b2api/v2/b2_download_file_by_id?fileId={uploaded_file['fileId']}"


def test_make_url(b2_cli, uploaded_file, uploaded_file_url_by_id):
    b2_cli.run(
        ["make-url", uploaded_file["fileId"]],
        expected_stdout=f"{uploaded_file_url_by_id}\n",
        expected_stderr='WARNING: make-url command is deprecated. Use get-url instead.\n',
    )


def test_make_friendly_url(b2_cli, bucket, uploaded_file, uploaded_file_url):
    b2_cli.run(
        ["make-friendly-url", bucket, uploaded_file["fileName"]],
        expected_stdout=f"{uploaded_file_url}\n",
        expected_stderr='WARNING: make-friendly-url command is deprecated. Use get-url instead.\n',
    )


def test_get_url__b2_uri(b2_cli, bucket, uploaded_file, uploaded_file_url):
    b2_cli.run(
        [
            "get-url",
            f'b2://{bucket}/{uploaded_file["fileName"]}',
        ],
        expected_stdout=f"{uploaded_file_url}\n",
    )


def test_get_url__b2id_uri(b2_cli, uploaded_file, uploaded_file_url_by_id):
    b2_cli.run(
        ["get-url", f'b2id://{uploaded_file["fileId"]}'],
        expected_stdout=f"{uploaded_file_url_by_id}\n",
    )
