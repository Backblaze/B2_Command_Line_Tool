######################################################################
#
# File: b2/utils.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from b2sdk.utils import *  # noqa
from b2.console_tool import current_time_millis
from b2.parse_args import repr_dict_deterministically

assert current_time_millis
assert repr_dict_deterministically

import b2._sdk_deprecation
b2._sdk_deprecation.deprecate_module('b2.utils')
