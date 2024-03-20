######################################################################
#
# File: test/unit/console_tool/test_ls.py
#
# Copyright 2024 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
######################################################################
#
# File: test/unit/console_tool/test_download_file.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import pytest


def test_ls__without_bucket_name(b2_cli, bucket_info):
    expected_output = "bucket_0  allPublic   my-bucket\n"

    b2_cli.run(["ls"], expected_stdout=expected_output)
    b2_cli.run(["ls", "b2://"], expected_stdout=expected_output)


def test_ls__without_bucket_name__json(b2_cli, bucket_info):
    expected_output = [
        {
            "accountId": "account-0",
            "bucketId": "bucket_0",
            "bucketInfo": {},
            "bucketName": "my-bucket",
            "bucketType": "allPublic",
            "corsRules": [],
            "defaultRetention": {
                "mode": None
            },
            "defaultServerSideEncryption": {
                "mode": "none"
            },
            "isFileLockEnabled": False,
            "lifecycleRules": [],
            "options": [],
            "replication": {
                "asReplicationDestination": None,
                "asReplicationSource": None,
            },
            "revision": 1,
        }
    ]

    b2_cli.run(["ls", "--json"], expected_json_in_stdout=expected_output)
    b2_cli.run(["ls", "--json", "b2://"], expected_json_in_stdout=expected_output)


@pytest.mark.parametrize("flag", ["--long", "--recursive", "--replication"])
def test_ls__without_bucket_name__option_not_supported(b2_cli, bucket_info, flag):
    b2_cli.run(
        ["ls", flag],
        expected_stderr=f"ERROR: Cannot use {flag} option without specifying a bucket name\n",
        expected_status=1,
    )
