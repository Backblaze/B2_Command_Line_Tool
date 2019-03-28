######################################################################
#
# File: b2/sync/exception.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from b2sdk.sync.exception import *  # noqa

import b2._sdk_deprecation
b2._sdk_deprecation.deprecate_module('b2.sync.exception')
