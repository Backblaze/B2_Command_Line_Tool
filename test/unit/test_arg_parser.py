######################################################################
#
# File: test/unit/test_arg_parser.py
#
# Copyright 2020 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import argparse
import sys

import pytest

from b2._internal._cli.arg_parser_types import (
    parse_comma_separated_list,
    parse_millis_from_float_timestamp,
    parse_range,
)
from b2._internal.arg_parser import B2ArgumentParser
from b2._internal.console_tool import B2

from .test_base import TestBase


class TestCustomArgTypes(TestBase):
    def test_parse_comma_separated_list(self):
        self.assertEqual([''], parse_comma_separated_list(''))
        self.assertEqual(['1', '2', '3'], parse_comma_separated_list('1,2,3'))

    def test_parse_millis_from_float_timestamp(self):
        self.assertEqual(1367900664000, parse_millis_from_float_timestamp('1367900664'))
        self.assertEqual(1367900664152, parse_millis_from_float_timestamp('1367900664.152'))
        with self.assertRaises(ValueError):
            parse_millis_from_float_timestamp('!$@$%@!@$')

    def test_parse_range(self):
        self.assertEqual((1, 2), parse_range('1,2'))
        with self.assertRaises(argparse.ArgumentTypeError):
            parse_range('1')
        with self.assertRaises(argparse.ArgumentTypeError):
            parse_range('1,2,3')
        with self.assertRaises(ValueError):
            parse_range('!@#,%^&')


class _ASCIIEncodedStream:
    def __init__(self, original_stream):
        self.original_stream = original_stream
        self.encoding = 'ascii'

    def write(self, data):
        if isinstance(data, str):
            data = data.encode(self.encoding, 'strict')
        self.original_stream.buffer.write(data)

    def flush(self):
        self.original_stream.flush()


def _build_command_names_classes_mapping() -> dict[str, type]:
    """
    Recursively build a dictionary of all command names and corresponding classes, including all level subcommands
    """

    command_classes = {}

    def _walk_command_classes(command_name: str, command_class: type) -> None:
        assert command_name not in command_classes
        command_classes[command_name] = command_class

        registry = getattr(command_class, 'subcommands_registry', None)
        if registry:
            for subcommand_name, subcommand_class in registry.items():
                _walk_command_classes(f'{command_name} {subcommand_name}', subcommand_class)

    _walk_command_classes('b2', B2)
    return command_classes


COMMAND_NAMES_CLASSES_MAPPING = _build_command_names_classes_mapping()


@pytest.mark.parametrize('command_name', COMMAND_NAMES_CLASSES_MAPPING)
def test_help_in_non_utf8_terminal(command_name: str, monkeypatch):
    command_class = COMMAND_NAMES_CLASSES_MAPPING[command_name]
    parser = B2ArgumentParser(description=command_class.__doc__)

    monkeypatch.setattr(sys, 'stdout', _ASCIIEncodedStream(sys.stdout))
    monkeypatch.setattr(sys, 'stderr', _ASCIIEncodedStream(sys.stderr))

    try:
        parser.print_help()
    except UnicodeEncodeError as e:
        pytest.fail(
            f'Failed to encode help message for command "{command_name}" on a non-UTF-8 terminal: {e}'
        )
