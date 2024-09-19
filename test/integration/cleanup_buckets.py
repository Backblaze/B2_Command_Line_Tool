######################################################################
#
# File: test/integration/cleanup_buckets.py
#
# Copyright 2022 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from .persistent_bucket import get_or_create_persistent_bucket


def test_cleanup_buckets(b2_api):
    # this is not a test, but it is intended to be called
    # via pytest because it reuses fixtures which have everything
    # set up.
    pass
    # The persistent bucket is cleared manually now and not
    # when tests tear down, as otherwise we'd lose the main benefit
    # of a persistent bucket, whose identity is shared across tests.
    persistent_bucket = get_or_create_persistent_bucket(b2_api)
    b2_api.clean_bucket(persistent_bucket)
