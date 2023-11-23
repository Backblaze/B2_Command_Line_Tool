######################################################################
#
# File: test/integration/test_tqdm_closer.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
import re
import sys

import pytest


@pytest.mark.skipif(
    (sys.platform != 'darwin') or ((sys.version_info.major, sys.version_info.minor) < (3, 11)),
    reason='Tqdm closing error only occurs on OSX and python 3.11 or newer',
)
def test_tqdm_closer(b2_tool, bucket, file_name):
    # test that stderr doesn't contain any warning, in particular warnings about multiprocessing resource tracker
    # leaking semaphores
    b2_tool.should_succeed([
        'cat',
        f'b2://{bucket.name}/{file_name}',
    ])

    # test that disabling _TqdmCloser does produce a resource tracker warning. Should the following check ever fail,
    # that would mean that either Tqdm or python fixed the issue and _TqdmCloser can be disabled for fixed versions
    b2_tool.should_succeed(
        [
            'cat',
            f'b2://{bucket.name}/{file_name}',
        ],
        additional_env={'B2_TEST_DISABLE_TQDM_CLOSER': '1'},
        expected_stderr_pattern=re.compile(
            r'UserWarning: resource_tracker: There appear to be \d+ leaked semaphore'
            r' objects to clean up at shutdown'
        ),
    )
