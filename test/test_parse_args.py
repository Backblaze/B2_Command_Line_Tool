######################################################################
#
# File: test/test_parse_args.py
#
# Copyright 2016 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import unittest

from b2.parse_args import parse_arg_list


class TestParseArgs(unittest.TestCase):

    NO_ARGS = {
        'option_flags': [],
        'option_args': [],
        'list_args': [],
        'required': [],
        'optional': [],
        'arg_parser': {}
    }

    EVERYTHING = {
        'option_flags': ['optionFlag'],
        'option_args': ['optionArg'],
        'list_args': ['list'],
        'required': ['required'],
        'optional': ['optional'],
        'arg_parser': {'optionArg': int}
    }

    def test_no_args(self):
        args = parse_arg_list([], **self.NO_ARGS)
        self.assertTrue(args is not None)

    def test_unexpected_flag(self):
        args = parse_arg_list(['--badFlag'], **self.NO_ARGS)
        self.assertTrue(args is None)

    def test_unexpected_arg(self):
        args = parse_arg_list(['badArg'], **self.NO_ARGS)
        self.assertTrue(args is None)

    def test_option_defaults(self):
        args = parse_arg_list(['req-value'], **self.EVERYTHING)
        self.assertFalse(args.optionFlag)
        self.assertTrue(args.optionArg is None)
        self.assertEqual([], args.list)
        self.assertEqual('req-value', args.required)
        self.assertTrue(args.optional is None)

    def test_all_there(self):
        args = parse_arg_list(
            ['--optionFlag', '--optionArg', '99', '--list', '1', '--list', '2', 'b', 'c'],
            **self.EVERYTHING
        )  # yapf disable
        self.assertTrue(args.optionFlag)
        self.assertEqual(99, args.optionArg)
        self.assertEqual('b', args.required)
        self.assertEqual(['1', '2'], args.list)
        self.assertEqual('c', args.optional)

    def test_optional_arg_missing_value(self):
        args = parse_arg_list(['--optionArg'], **self.EVERYTHING)
        self.assertTrue(args is None)
