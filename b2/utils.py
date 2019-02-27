######################################################################
#
# File: utils.py
#
# Copyright 2018 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import sys
import time
import warnings
import b2sdk.utils
from .import_hooks import ModuleWrapper

# Global variable that says whether the app is shutting down
_shutting_down = False


def set_shutting_down():
    global _shutting_down
    _shutting_down = True


def current_time_millis():
    """
    File times are in integer milliseconds, to avoid roundoff errors.
    """
    return int(round(time.time() * 1000))


def _show_warning(source_name, target_name, attr_name):
    name_fmt = '{0}.{1}'
    src_attr = name_fmt.format(source_name, attr_name)
    dst_attr = name_fmt.format(target_name, attr_name)
    message = '{0} is deprecated, use {1} instead'.format(src_attr, dst_attr)
    warnings.warn(message, DeprecationWarning)


wrapper = ModuleWrapper(sys.modules[__name__], b2sdk.utils, callback=_show_warning)
wrapper()
