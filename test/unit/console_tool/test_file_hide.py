######################################################################
#
# File: test/unit/console_tool/test_file_hide.py
#
# Copyright 2024 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import pytest


@pytest.mark.apiver(to_ver=3)
def test_legacy_hide_file(b2_cli, api_bucket, uploaded_file):
    b2_cli.run(
        ['hide-file', uploaded_file['bucket'], uploaded_file['fileName']],
        expected_stderr='WARNING: `hide-file` command is deprecated. Use `file hide` instead.\n',
    )
    assert not list(api_bucket.ls())


@pytest.mark.apiver(to_ver=4)
def test_file_hide__by_bucket_and_file_name(b2_cli, api_bucket, uploaded_file):
    b2_cli.run(
        ['file', 'hide', uploaded_file['bucket'], uploaded_file['fileName']],
        expected_stderr=(
            'WARNING: "bucketName fileName" arguments syntax is deprecated, use "b2://bucketName/fileName" instead\n'
        ),
    )
    assert not list(api_bucket.ls())


@pytest.mark.apiver
def test_file_hide__by_b2_uri(b2_cli, api_bucket, uploaded_file):
    b2_cli.run(['file', 'hide', f"b2://{uploaded_file['bucket']}/{uploaded_file['fileName']}"])
    assert not list(api_bucket.ls())


@pytest.mark.apiver
def test_file_hide__cannot_hide_by_b2id(b2_cli, api_bucket, uploaded_file):
    b2_cli.run(['file', 'hide', f"b2id://{uploaded_file['fileId']}"], expected_status=2)
    assert list(api_bucket.ls())


@pytest.mark.apiver
@pytest.mark.parametrize(
    'make_args, expected_stderr',
    [
        (
            lambda f: [f"b2://{f['bucket']}/{f['fileName']}", f['fileName']],
            'ERROR: argument B2_URI: Both B2 URI and file name were provided, but only one is expected\n',
        ),
        (
            lambda f: ['notauri'],
            "ERROR: argument B2_URI: Incorrect B2 URI was provided, expected `b2://bucketName/fileName`, but got 'notauri'\n",
        ),
    ],
)
def test_file_hide__invalid_b2_uri_argument(
    b2_cli, api_bucket, uploaded_file, make_args, expected_stderr
):
    b2_cli.run(
        ['file', 'hide', *make_args(uploaded_file)],
        expected_stderr=expected_stderr,
        expected_status=2,
    )
    assert list(api_bucket.ls())
