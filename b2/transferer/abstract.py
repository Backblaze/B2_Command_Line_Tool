######################################################################
#
# File: b2/transferer/abstract.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from b2sdk.transferer.abstract import *  # noqa

import b2._sdk_deprecation
b2._sdk_deprecation.deprecate_module('b2.transferer.abstract')
