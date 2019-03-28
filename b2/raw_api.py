######################################################################
#
# File: b2/raw_api.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from b2sdk.raw_api import *  # noqa

import b2._sdk_deprecation
b2._sdk_deprecation.deprecate_module('b2.raw_api')
