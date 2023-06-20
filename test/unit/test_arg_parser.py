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

from b2.arg_parser import (
    ArgumentParser,
    parse_comma_separated_list,
    parse_millis_from_float_timestamp,
    parse_range,
)
from b2.console_tool import B2

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


class TestNonUTF8TerminalSupport(TestBase):
    class ASCIIEncodedStream:
        def __init__(self, original_stream):
            self.original_stream = original_stream
            self.encoding = 'ascii'

        def write(self, data):
            if isinstance(data, str):
                data = data.encode(self.encoding, 'strict')
            self.original_stream.buffer.write(data)

        def flush(self):
            self.original_stream.flush()

    def check_help_string(self, command_class, command_name):
        help_string = command_class.__doc__

        # create a parser with a help message that is based on the command_class.__doc__ string
        parser = ArgumentParser(description=help_string)

        try:
            old_stdout = sys.stdout
            old_stderr = sys.stderr
            sys.stdout = TestNonUTF8TerminalSupport.ASCIIEncodedStream(sys.stdout)
            sys.stderr = TestNonUTF8TerminalSupport.ASCIIEncodedStream(sys.stderr)

            parser.print_help()

        except UnicodeEncodeError as e:
            self.fail(
                f'Failed to encode help message for command "{command_name}" on a non-UTF-8 terminal: {e}'
            )

        finally:
            # Restore original stdout and stderr
            sys.stdout = old_stdout
            sys.stderr = old_stderr

    def test_help_in_non_utf8_terminal(self):
        command_classes = dict(B2.subcommands_registry.items())
        command_classes['b2'] = B2

        for command_name, command_class in command_classes.items():
            with self.subTest(command_class=command_class, command_name=command_name):
                self.check_help_string(command_class, command_name)
