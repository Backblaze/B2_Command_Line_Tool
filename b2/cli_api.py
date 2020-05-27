######################################################################
#
# File: b2/cli_api.py
#
# Copyright 2020 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from b2sdk import v1
from .cli_bucket import CliBucket, CliBucketFactory


class CliB2Api(v1.B2Api):
    BUCKET_FACTORY_CLASS = staticmethod(CliBucketFactory)
    BUCKET_CLASS = staticmethod(CliBucket)
