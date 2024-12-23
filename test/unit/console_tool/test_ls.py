######################################################################
#
# File: test/unit/console_tool/test_ls.py
#
# Copyright 2024 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import pytest


def test_ls__without_bucket_name(b2_cli, bucket_info):
    expected_output = 'bucket_0  allPublic   my-bucket\n'

    b2_cli.run(['ls'], expected_stdout=expected_output)
    b2_cli.run(['ls', 'b2://'], expected_stdout=expected_output)


def test_ls__without_bucket_name__json(b2_cli, bucket_info):
    expected_output = [
        {
            'accountId': 'account-0',
            'bucketId': 'bucket_0',
            'bucketInfo': {},
            'bucketName': 'my-bucket',
            'bucketType': 'allPublic',
            'corsRules': [],
            'defaultRetention': {'mode': None},
            'defaultServerSideEncryption': {'mode': 'none'},
            'isFileLockEnabled': False,
            'lifecycleRules': [],
            'options': [],
            'replication': {
                'asReplicationDestination': None,
                'asReplicationSource': None,
            },
            'revision': 1,
        }
    ]

    b2_cli.run(['ls', '--json'], expected_json_in_stdout=expected_output)
    b2_cli.run(['ls', '--json', 'b2://'], expected_json_in_stdout=expected_output)


@pytest.mark.parametrize('flag', ['--long', '--recursive', '--replication'])
def test_ls__without_bucket_name__option_not_supported(b2_cli, bucket_info, flag):
    b2_cli.run(
        ['ls', flag],
        expected_stderr=f'ERROR: Cannot use {flag} option without specifying a bucket name\n',
        expected_status=1,
    )


@pytest.mark.apiver(to_ver=3)
def test_ls__pre_v4__should_not_return_exact_match_filename(b2_cli, uploaded_file):
    """`b2v3 ls bucketName folderName` should not return files named `folderName` even if such exist"""
    b2_cli.run(['ls', uploaded_file['bucket']], expected_stdout='file1.txt\n')  # sanity check
    b2_cli.run(
        ['ls', uploaded_file['bucket'], uploaded_file['fileName']],
        expected_stdout='',
    )


@pytest.mark.apiver(from_ver=4)
def test_ls__b2_uri__pointing_to_bucket(b2_cli, uploaded_file):
    b2_cli.run(
        ['ls', f"b2://{uploaded_file['bucket']}/"],
        expected_stdout='file1.txt\n',
    )


@pytest.mark.apiver(from_ver=4)
def test_ls__b2_uri__pointing_to_a_file(b2_cli, uploaded_file):
    b2_cli.run(
        ['ls', f"b2://{uploaded_file['bucket']}/{uploaded_file['fileName']}"],
        expected_stdout='file1.txt\n',
    )

    b2_cli.run(
        ['ls', f"b2://{uploaded_file['bucket']}/nonExistingFile"],
        expected_stdout='',
    )
