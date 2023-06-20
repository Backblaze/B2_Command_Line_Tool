######################################################################
#
# File: b2/arg_parser.py
#
# Copyright 2020 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import argparse
import locale
import re
import sys
import textwrap

import arrow
from b2sdk.v2 import RetentionPeriod
from rst2ansi import rst2ansi

_arrow_version = tuple(int(p) for p in arrow.__version__.split("."))


class RawTextHelpFormatter(argparse.RawTextHelpFormatter):
    """
    CLI custom formatter.

    It removes default "usage: " text and prints usage for all subcommands.
    """

    def add_usage(self, usage, actions, groups, prefix=None):
        if prefix is None:
            prefix = ''
        super().add_usage(usage, actions, groups, prefix)

    def add_argument(self, action):
        if isinstance(action, argparse._SubParsersAction) and action.help is not argparse.SUPPRESS:
            usages = []
            for choice in self._unique_choice_values(action):
                usages.append(choice.format_usage())
            self.add_text(''.join(usages))
        else:
            super().add_argument(action)

    @classmethod
    def _unique_choice_values(cls, action):
        seen = set()
        seen_add = seen.add
        for value in action.choices.values():
            if not (value in seen or seen_add(value)):
                yield value


class ArgumentParser(argparse.ArgumentParser):
    """
    CLI custom parser.

    It fixes indentation of the description, set the custom formatter as a default
    and use help message in case of error.
    """

    def __init__(self, *args, for_docs=False, **kwargs):
        self._raw_description = None
        self._description = None
        self._for_docs = for_docs
        kwargs.setdefault('formatter_class', RawTextHelpFormatter)
        super().__init__(*args, **kwargs)

    @property
    def description(self):
        if self._description is None and self._raw_description is not None:
            if self._for_docs:
                self._description = textwrap.dedent(self._raw_description)
            else:
                encoding = self._get_encoding()
                self._description = rst2ansi(
                    self._raw_description.encode(encoding), output_encoding=encoding
                )

        return self._description

    @description.setter
    def description(self, value):
        self._raw_description = value

    def error(self, message):
        self.print_help()

        self.exit(2, f'\n{self.prog}: error: {message}\n')

    @classmethod
    def _get_encoding(cls):
        _, locale_encoding = locale.getdefaultlocale()

        # Check if the stdout is properly set
        if sys.stdout.encoding is not None:
            # Use the stdout encoding
            return sys.stdout.encoding

        # Fall back to the locale_encoding if stdout encoding is not set
        elif locale_encoding is not None:
            return locale_encoding

        # locales are improperly configured
        return 'ascii'


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
