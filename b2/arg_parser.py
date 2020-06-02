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

try:
    from textwrap import indent
except ImportError:

    def indent(text, prefix):
        def prefixed_lines():
            for line in text.splitlines(True):
                yield prefix + line if line.strip() else line

        return ''.join(prefixed_lines())


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
            for choice in action.choices.values():
                usages.append(choice.format_usage())
            self.add_text(''.join(usages))
        else:
            super(RawTextHelpFormatter, self).add_argument(action)


class ArgumentParser(argparse.ArgumentParser):
    """
    CLI custom parser.

    It fixes indentation of the description and set the custom formatter as a default.
    """
    def __init__(self, *args, **kwargs):
        kwargs.setdefault('formatter_class', RawTextHelpFormatter)
        description = kwargs.get('description', None)
        if description is not None:
            kwargs['description'] = self._format_description(description)
        super(ArgumentParser, self).__init__(*args, **kwargs)

    @classmethod
    def _format_description(cls, text):
        return indent(textwrap.dedent(text), '  ')
