######################################################################
#
# File: test/test_console_tool.py
#
# Copyright 2016 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import unittest

import six

from b2.b2 import B2Api, InMemoryCache
from b2.console_tool import ConsoleTool
from b2.raw_simulator import RawSimulator
from b2.stub_account_info import StubAccountInfo


class TestConsoleTool(unittest.TestCase):
    def setUp(self):
        self.account_info = StubAccountInfo()
        self.cache = InMemoryCache()
        self.raw_api = RawSimulator()
        self.b2_api = B2Api(self.account_info, self.cache, self.raw_api)

    def test_authorize_and_clear_account(self):
        # Initial condition
        assert (self.account_info.get_account_auth_token() is None)

        # Authorize an account with a bad api key.
        expected_stdout = '''
        Using http://production.example.com
        '''

        expected_stderr = '''
        ERROR: unable to authorize account: invalid application key: bad-app-key
        '''

        self._run_command(
            ['b2', 'authorize_account', 'my-account', 'bad-app-key'], expected_stdout,
            expected_stderr, 1
        )

        # Authorize an account with a good api key.
        expected_stdout = """
        Using http://production.example.com
        """

        self._run_command(
            ['b2', 'authorize_account', 'my-account', 'good-app-key'], expected_stdout, '', 0
        )
        assert (self.account_info.get_account_auth_token() is not None)

        # Clearing the account should remove the auth token
        # from the account info.
        self._run_command(['b2', 'clear_account'], '', '', 0)
        assert (self.account_info.get_account_auth_token() is None)

    def _run_command(self, argv, expected_stdout='', expected_stderr='', expected_status=0):
        """
        Runs one command using the ConsoleTool, checking stdout, stderr, and
        the returned status code.

        The ConsoleTool is stateless, so we can make a new one for each
        call, with a fresh stdout and stderr
        """
        stdout = six.StringIO()
        stderr = six.StringIO()
        console_tool = ConsoleTool(self.b2_api, stdout, stderr)
        actual_status = console_tool.run_command(argv)
        self.assertEqual(expected_status, actual_status, 'exit status code')
        self.assertEqual(self._trim_leading_spaces(expected_stdout), stdout.getvalue(), 'stdout')
        self.assertEqual(self._trim_leading_spaces(expected_stderr), stderr.getvalue(), 'stderr')

    def _trim_leading_spaces(self, s):
        """
        Takes the contents of a triple-quoted string, and removes the leading
        newline and leading spaces that come from it being indented with code.
        """
        # The first line starts on the line following the triple
        # quote, so the first line after splitting can be discarded.
        lines = s.split('\n')
        if lines[0] == '':
            lines = lines[1:]

        # Count the leading spaces
        space_count = 0
        while 0 < len(lines) and space_count < len(lines[0]) and lines[0][space_count] == ' ':
            space_count += 1

        # Remove the leading spaces from each line
        leading_spaces = ' ' * space_count
        assert all(
            line.startswith(leading_spaces) for line in lines
        ), 'all lines have leading spaces'
        return '\n'.join(line[space_count:] for line in lines)
