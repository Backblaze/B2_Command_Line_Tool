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
import six
import time
import warnings
import b2sdk.utils
from .import_hooks import ModuleWrapper


def current_time_millis():
    """
    File times are in integer milliseconds, to avoid roundoff errors.
    """
    return int(round(time.time() * 1000))


def repr_dict_deterministically(dict_):
    """
    Represent a dictionary in a deterministic way, i.e. with
    the same order of keys

    :param dict_: a dictionary
    :type dict_: dict
    :return: a string representation of a dictionary
    :rtype: str
    """
    # a simple version had a disadvantage of outputting dictionary keys in random order.
    # It was hard to read. Therefore we sort items by key.
    fields = ', '.join('%s: %s' % (repr(k), repr(v)) for k, v in sorted(six.iteritems(dict_)))
    return '{%s}' % (fields,)


def _show_warning(source_name, target_name, attr_name):
    name_fmt = '{0}.{1}'
    src_attr = name_fmt.format(source_name, attr_name)
    dst_attr = name_fmt.format(target_name, attr_name)
    message = '{0} is deprecated, use {1} instead'.format(src_attr, dst_attr)
    warnings.warn(message, DeprecationWarning)


wrapper = ModuleWrapper(sys.modules[__name__], b2sdk.utils, callback=_show_warning)
wrapper()
