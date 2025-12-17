######################################################################
#
# File: test/integration/test_tqdm_closer.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
import sys

import pytest


@pytest.mark.skipif(
    (sys.platform != 'darwin'),
    reason='Tqdm closing error only occurs on OSX',
)
def test_tqdm_closer(b2_tool, bucket, file_name):
    # test that stderr doesn't contain any warning, in particular warnings about multiprocessing resource tracker
    # leaking semaphores
    b2_tool.should_succeed(
        [
            'file',
            'cat',
            f'b2://{bucket.name}/{file_name}',
        ]
    )
