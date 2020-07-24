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

from b2.arg_parser import parse_comma_separated_list, parse_millis_from_float_timestamp, \
    parse_range

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
