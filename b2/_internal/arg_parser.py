######################################################################
#
# File: b2/_internal/arg_parser.py
#
# Copyright 2020 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import argparse
import functools
import locale
import sys
import textwrap
import unittest.mock

from rst2ansi import rst2ansi


class B2RawTextHelpFormatter(argparse.RawTextHelpFormatter):
    """
    CLI custom formatter.

    It removes default "usage: " text and prints usage for all (non-hidden) subcommands.
    """

    def __init__(self, *args, show_all: bool = False, **kwargs):
        super().__init__(*args, **kwargs)
        self.show_all = show_all

    def add_usage(self, usage, actions, groups, prefix=None):
        if prefix is None:
            prefix = ''
        super().add_usage(usage, actions, groups, prefix)

    def add_argument(self, action):
        if isinstance(action, argparse._SubParsersAction) and action.help is not argparse.SUPPRESS:
            usages = []
            for choice in self._unique_choice_values(action):
                deprecated = getattr(choice, 'deprecated', False)
                if deprecated:
                    if self.show_all:
                        usages.append(f'(DEPRECATED) {choice.format_usage()}')
                else:
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


class _HelpAllAction(argparse._HelpAction):
    """Like argparse._HelpAction but prints help for all subcommands (even deprecated ones)."""

    def __call__(self, parser, namespace, values, option_string=None):
        parser.print_help(show_all=True)
        parser.exit()


class B2ArgumentParser(argparse.ArgumentParser):
    """
    CLI custom parser.

    It fixes indentation of the description, set the custom formatter as a default
    and use help message in case of error.
    """

    def __init__(
        self,
        *args,
        add_help_all: bool = True,
        for_docs: bool = False,
        deprecated: bool = False,
        **kwargs
    ):
        """

        :param for_docs: is this parser used for generating docs
        :param deprecated: is this option deprecated
        """
        self._raw_description = None
        self._description = None
        self._for_docs = for_docs
        self.deprecated = deprecated
        kwargs.setdefault('formatter_class', B2RawTextHelpFormatter)
        super().__init__(*args, **kwargs)
        if add_help_all:
            self.register('action', 'help_all', _HelpAllAction)
            self.add_argument(
                '--help-all',
                help='show help for all options, including deprecated ones',
                action='help_all',
            )

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

    def print_help(self, *args, show_all: bool = False, **kwargs):
        """
        Print help message.
        """
        with unittest.mock.patch.object(
            self, 'formatter_class', functools.partial(B2RawTextHelpFormatter, show_all=show_all)
        ):
            super().print_help(*args, **kwargs)
