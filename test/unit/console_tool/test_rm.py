######################################################################
#
# File: test/unit/console_tool/test_rm.py
#
# Copyright 2024 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import pytest


@pytest.mark.apiver(to_ver=3)
def test_rm__pre_v4__should_not_rm_exact_match_filename(b2_cli, api_bucket, uploaded_file):
    """`b2v3 rm bucketName folderName` should not remove file named `folderName` even if such exist"""
    b2_cli.run(['rm', uploaded_file['bucket'], uploaded_file['fileName']])
    assert list(api_bucket.ls())  # nothing was removed


@pytest.mark.apiver(from_ver=4)
def test_rm__b2_uri__pointing_to_a_file(b2_cli, api_bucket, uploaded_file):
    b2_cli.run(['rm', f"b2://{uploaded_file['bucket']}/noSuchFile"])
    assert list(api_bucket.ls())  # sanity check: bucket is not empty
    b2_cli.run(['rm', f"b2://{uploaded_file['bucket']}/{uploaded_file['fileName']}"])
    assert not list(api_bucket.ls())
