######################################################################
#
# File: b2/_internal/_cli/arg_parser_types.py
#
# Copyright 2020 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import argparse
import functools
import re

import arrow
from b2sdk.v2 import RetentionPeriod

_arrow_version = tuple(int(p) for p in arrow.__version__.split("."))


def parse_comma_separated_list(s):
    """
    Parse comma-separated list.
    """
    return [word.strip() for word in s.split(",")]


def parse_millis_from_float_timestamp(s):
    """
    Parse timestamp, e.g. 1367900664 or 1367900664.152
    """
    parsed = arrow.get(float(s))
    if _arrow_version < (1, 0, 0):
        return int(parsed.format("XSSS"))
    else:
        return int(parsed.format("x")[:13])


def parse_range(s):
    """
    Parse optional integer range
    """
    bytes_range = None
    if s is not None:
        bytes_range = s.split(',')
        if len(bytes_range) != 2:
            raise argparse.ArgumentTypeError('the range must have 2 values: start,end')
        bytes_range = (
            int(bytes_range[0]),
            int(bytes_range[1]),
        )

    return bytes_range


def parse_default_retention_period(s):
    unit_part = '(' + ')|('.join(RetentionPeriod.KNOWN_UNITS) + ')'
    m = re.match(r'^(?P<duration>\d+) (?P<unit>%s)$' % (unit_part), s)
    if not m:
        raise argparse.ArgumentTypeError(
            'default retention period must be in the form of "X days|years "'
        )
    return RetentionPeriod(**{m.group('unit'): int(m.group('duration'))})


def wrap_with_argument_type_error(func, translator=str, exc_type=ValueError):
    """
    Wrap function that may raise an exception into a function that raises ArgumentTypeError error.
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except exc_type as e:
            raise argparse.ArgumentTypeError(translator(e))

    return wrapper
