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
import textwrap

import arrow
from rst2ansi import rst2ansi


class RawTextHelpFormatter(argparse.RawTextHelpFormatter):
    """
    CLI custom formatter.

    It removes default "usage: " text and prints usage for all subcommands.
    """

    def add_usage(self, usage, actions, groups, prefix=None):
        if prefix is None:
            prefix = ''
        super(RawTextHelpFormatter, self).add_usage(usage, actions, groups, prefix)

    def add_argument(self, action):
        if isinstance(action, argparse._SubParsersAction) and action.help is not argparse.SUPPRESS:
            usages = []
            for choice in self._unique_choice_values(action):
                usages.append(choice.format_usage())
            self.add_text(''.join(usages))
        else:
            super(RawTextHelpFormatter, self).add_argument(action)

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
        self._for_docs = for_docs

        kwargs.setdefault('formatter_class', RawTextHelpFormatter)
        description = kwargs.get('description', None)
        if description is not None:
            kwargs['description'] = self._format_description(description)
        super(ArgumentParser, self).__init__(*args, **kwargs)

    def error(self, message):
        self.print_help()
        args = {'prog': self.prog, 'message': message}
        self.exit(2, '\n%(prog)s: error: %(message)s\n' % args)

    def _format_description(self, text):
        if self._for_docs:
            return textwrap.dedent(text)
        return rst2ansi(text.encode('utf-8'))


def parse_comma_separated_list(s):
    """
    Parse comma-separated list.
    """
    return [word.strip() for word in s.split(',')]


def parse_millis_from_float_timestamp(s):
    """
    Parse timestamp, e.g. 1367900664 or 1367900664.152
    """
    return int(arrow.get(float(s)).format('XSSS'))


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
