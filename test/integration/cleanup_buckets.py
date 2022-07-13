######################################################################
#
# File: test/integration/cleanup_buckets.py
#
# Copyright 2022 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################


def test_cleanup_buckets(b2_api):
    # this is not a test, but it is intended to be called
    # via pytest because it reuses fixtures which have everything
    # set up
    b2_api.clean_buckets()
