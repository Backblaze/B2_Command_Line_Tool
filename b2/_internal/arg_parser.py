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
import contextlib
import functools
import locale
import re
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
            col_length = max(len(choice.prog) for choice in action.choices.values())

            for choice in action.choices.values():
                deprecated = getattr(choice, 'deprecated', False)
                if deprecated:
                    if self.show_all:
                        usages.append(f'(DEPRECATED) {choice.format_usage()}')
                else:
                    usages.append(
                        choice.format_usage(
                            use_short_description=not self.show_all, col_length=col_length
                        )
                    )
            self.add_text(''.join(usages))
        else:
            super().add_argument(action)


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
        self._short_description = self._make_short_description(
            kwargs.get('usage', ''), kwargs.get('description', '')
        )
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
            self._description = self._encode_description(self._raw_description)
        return self._description

    @description.setter
    def description(self, value):
        self._raw_description = value

    def _encode_description(self, value: str):
        if self._for_docs:
            return textwrap.dedent(value)
        else:
            encoding = self._get_encoding()
            return rst2ansi(value.encode(encoding), output_encoding=encoding)

    def _make_short_description(self, usage: str, raw_description: str) -> str:
        if usage:
            return usage

        if not raw_description:
            return ""

        for line in raw_description.splitlines():
            if line.strip():
                return self._encode_description(line.strip())

        return ""

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

        All subcommands are sorted alphabetically.
        Unless show_all is True, it hides deprecated options and subcommands.
        In additional all subcommand aliases (such as `sub_command` alias for `sub-command) are hidden.

        Due how deep changes are in argparse stack this is done through temporary patches.
        """
        patches = [
            unittest.mock.patch.object(
                self, 'formatter_class',
                functools.partial(B2RawTextHelpFormatter, show_all=show_all)
            )
        ]
        if self._subparsers is not None and not show_all:
            patches.extend(
                [
                    self._hide_duplicated_action_choices(action)
                    for action in self._subparsers._actions
                    if isinstance(action, argparse._SubParsersAction)
                ]
            )
        with contextlib.ExitStack() as stack:
            for patch in patches:
                stack.enter_context(patch)
            super().print_help(*args, **kwargs)

    @contextlib.contextmanager
    def _hide_duplicated_action_choices(self, action):
        original_choices = action.choices
        seen_choices = set()
        filtered_choices = {}
        for name, choice in sorted(original_choices.items()):
            if (not getattr(choice, 'deprecated', False)) and choice not in seen_choices:
                filtered_choices[name] = choice
                seen_choices.add(choice)
        action.choices = filtered_choices
        yield
        action.choices = original_choices

    def format_usage(self, use_short_description: bool = False, col_length: int = 16):
        if not use_short_description or not self._short_description:
            return super().format_usage()

        formatter = self._get_formatter()
        formatter.add_text(f"{self.prog:{col_length + 2}} {self._short_description}")
        return formatter.format_help()


SUPPORT_CAMEL_CASE_ARGUMENTS = False


def enable_camel_case_arguments():
    global SUPPORT_CAMEL_CASE_ARGUMENTS
    SUPPORT_CAMEL_CASE_ARGUMENTS = True


def make_deprecated_action_call(action):
    def deprecated_action_call(self, parser, namespace, values, option_string=None, **kwargs):
        action.__call__(self, parser, namespace, values, option_string, **kwargs)
        if option_string:
            kebab_option_string = _camel_to_kebab(option_string)
            print(
                f"The '{option_string}' argument is deprecated. Use '{kebab_option_string}' instead.",
                file=sys.stderr
            )

    return deprecated_action_call


_kebab_to_snake_pattern = re.compile(r'-')
_camel_to_kebab_pattern = re.compile(r'(?<=[a-z])([A-Z])')
_kebab_to_camel_pattern = re.compile(r'-(\w)')


def _camel_to_kebab(s: str):
    return _camel_to_kebab_pattern.sub(r'-\1', s).lower()


def _kebab_to_camel(s: str):
    return "--" + _kebab_to_camel_pattern.sub(lambda m: m.group(1).upper(), s[2:])


def _kebab_to_snake(s: str):
    return _kebab_to_snake_pattern.sub('_', s)


class DeprecatedActionMarker:
    pass


def add_normalized_argument(parser, param_name, *args, **kwargs):
    param_name_kebab = _camel_to_kebab(param_name)
    param_name_camel = _kebab_to_camel(param_name_kebab)
    dest_name_snake = _kebab_to_snake(param_name_kebab)[2:]
    kwargs_kebab = dict(kwargs)
    kwargs_camel = kwargs
    kwargs_camel['help'] = argparse.SUPPRESS

    if 'dest' not in kwargs_kebab:
        kwargs_kebab['dest'] = dest_name_snake
        kwargs_camel['dest'] = dest_name_snake

    if 'action' in kwargs:
        if isinstance(kwargs['action'], str):
            action = parser._registry_get('action', kwargs['action'])
        else:
            action = kwargs['action']
    else:
        action = argparse._StoreAction

    kwargs_camel['action'] = type(
        'DeprecatedAction', (action, DeprecatedActionMarker),
        {'__call__': make_deprecated_action_call(action)}
    )

    parser.add_argument(f'{param_name_kebab}', *args, **kwargs_kebab)

    if SUPPORT_CAMEL_CASE_ARGUMENTS and param_name_kebab != param_name_camel:
        parser.add_argument(f'{param_name_camel}', *args, **kwargs_camel)
