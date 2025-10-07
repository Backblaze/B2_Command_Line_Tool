######################################################################
#
# File: test/unit/test_console_tool.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import json
import os
import pathlib
import re
from functools import cache
from io import StringIO
from itertools import chain, product
from tempfile import TemporaryDirectory
from typing import Optional
from unittest import mock

import pytest
from b2sdk.v2 import TempDir
from b2sdk.v3 import (
    ALL_CAPABILITIES,
    B2Api,
    B2HttpApiConfig,
    ProgressReport,
    RawSimulator,
    StubAccountInfo,
    UploadSourceBytes,
    fix_windows_path_limit,
)
from b2sdk.v3.exception import Conflict  # Any error for testing fast-fail of the rm command.
from more_itertools import one

from b2._internal._cli.const import (
    B2_APPLICATION_KEY_ENV_VAR,
    B2_APPLICATION_KEY_ID_ENV_VAR,
    B2_ENVIRONMENT_ENV_VAR,
)
from b2._internal.b2v3.rm import Rm as v3Rm
from b2._internal.b2v4.registry import Rm as v4Rm
from b2._internal.version import VERSION
from test.helpers import skip_on_windows

from .test_base import TestBase


def file_mod_time_millis(path):
    return int(os.path.getmtime(path) * 1000)


class BaseConsoleToolTest(TestBase):
    RE_API_VERSION = re.compile(r'\/v\d\/')
    json_pattern = re.compile(r'[^{,^\[]*(?P<dict_json>{.*})|(?P<list_json>\[.*]).*', re.DOTALL)

    def setUp(self):
        self.account_info = StubAccountInfo()

        # this is a hack - B2HttpApiConfig expects a class, but we want to use an instance
        # which will be reused during the test, thus instead of class we pass a lambda which
        # returns already predefined instance
        self.raw_simulator = RawSimulator()
        self.api_config = B2HttpApiConfig(_raw_api_class=lambda *args, **kwargs: self.raw_simulator)

        @cache
        def _get_b2api(**kwargs) -> B2Api:
            kwargs.pop('profile', None)
            return B2Api(
                account_info=self.account_info,
                cache=None,
                api_config=self.api_config,
                **kwargs,
            )

        self.console_tool_class._initialize_b2_api = lambda cls, args, kwargs: _get_b2api(**kwargs)

        self.b2_api = _get_b2api()
        self.raw_api = self.b2_api.session.raw_api
        self.account_id, self.master_key = self.raw_api.create_account()

        for env_var_name in [
            B2_APPLICATION_KEY_ID_ENV_VAR,
            B2_APPLICATION_KEY_ENV_VAR,
            B2_ENVIRONMENT_ENV_VAR,
        ]:
            os.environ.pop(env_var_name, None)

    def _get_stdouterr(self):
        stdout = StringIO()
        stderr = StringIO()
        return stdout, stderr

    def _run_command_ignore_output(self, argv):
        """
        Runs the given command in the console tool, checking that it
        success, but ignoring the stdout.
        """
        stdout, stderr = self._get_stdouterr()
        actual_status = self.console_tool_class(stdout, stderr).run_command(['b2'] + argv)
        actual_stderr = self._trim_trailing_spaces(stderr.getvalue())

        if actual_stderr != '':
            print('ACTUAL STDERR:  ', repr(actual_stderr))
            print(actual_stderr)

        assert re.match(r'^(|Using https?://[\w.]+)$', actual_stderr), f'stderr: {actual_stderr!r}'
        self.assertEqual(0, actual_status, 'exit status code')

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
        if len(lines) == 0:
            return ''

        # Count the leading spaces
        space_count = min((self._leading_spaces(line) for line in lines if line != ''), default=0)

        # Remove the leading spaces from each line, based on the line
        # with the fewest leading spaces
        leading_spaces = ' ' * space_count
        assert all(
            line.startswith(leading_spaces) or line == '' for line in lines
        ), 'all lines have leading spaces'
        return '\n'.join('' if line == '' else line[space_count:] for line in lines)

    def _leading_spaces(self, s):
        space_count = 0
        while space_count < len(s) and s[space_count] == ' ':
            space_count += 1
        return space_count

    def _trim_trailing_spaces(self, s):
        return '\n'.join(line.rstrip() for line in s.split('\n'))

    def _make_local_file(self, temp_dir, file_name):
        local_path = os.path.join(temp_dir, file_name)
        with open(local_path, 'wb') as f:
            f.write(b'hello world')
        return local_path

    def _read_file(self, local_path):
        with open(local_path, 'rb') as f:
            return f.read()

    def _remove_api_version_number(self, s):
        return re.sub(self.RE_API_VERSION, '/vx/', s)

    def _normalize_expected_output(self, text, format_vars=None):
        if text is None:
            return None
        format_vars = format_vars or {}
        return self._trim_leading_spaces(text).format(
            account_id=self.account_id, master_key=self.master_key, **format_vars
        )

    def assertDictIsContained(self, subset, superset):
        """Asserts that all keys in `subset` are present is `superset` and their corresponding values are the same"""
        truncated_superset = {k: v for k, v in superset.items() if k in subset}
        self.maxDiff = None
        self.assertEqual(subset, truncated_superset)

    def assertListOfDictsIsContained(self, list_of_subsets, list_of_supersets):
        """Performs the same assertion as assertDictIsContained, but for dicts in two lists itertively"""
        self.assertEqual(len(list_of_subsets), len(list_of_supersets))
        truncated_list_of_supersets = []
        for subset, superset in zip(list_of_subsets, list_of_supersets):
            truncated_list_of_supersets.append({k: v for k, v in superset.items() if k in subset})
        self.assertEqual(list_of_subsets, truncated_list_of_supersets)

    def _authorize_account(self):
        """
        Prepare for a test by authorizing an account and getting an account auth token
        """
        self._run_command_ignore_output(['account', 'authorize', self.account_id, self.master_key])

    def _clear_account(self):
        """
        Clear account auth data
        """
        self._run_command_ignore_output(['account', 'clear'])

    def _create_my_bucket(self):
        self._run_command(['bucket', 'create', 'my-bucket', 'allPublic'], 'bucket_0\n', '', 0)

    def _run_command(
        self,
        argv,
        expected_stdout=None,
        expected_stderr='',
        expected_status=0,
        format_vars=None,
        remove_version=False,
        expected_json_in_stdout: Optional[dict] = None,
        expected_part_of_stdout=None,
        unexpected_part_of_stdout=None,
    ):
        """
        Runs one command using the ConsoleTool, checking stdout, stderr, and
        the returned status code.

        The expected output strings are format strings (as used by str.format),
        so braces need to be escaped by doubling them.  The variables 'account_id'
        and 'master_key' are set by default, plus any variables passed in the dict
        format_vars.

        The ConsoleTool is stateless, so we can make a new one for each
        call, with a fresh stdout and stderr. However, last instance of ConsoleTool
        is stored in `self.console_tool`, may be handy for testing internals
        of the tool after last command invocation.
        """
        expected_stderr = self._normalize_expected_output(expected_stderr, format_vars)
        stdout, stderr = self._get_stdouterr()
        self.console_tool = self.console_tool_class(stdout, stderr)
        try:
            actual_status = self.console_tool.run_command(['b2'] + argv)
        except SystemExit as e:
            actual_status = e.code

        actual_stdout = self._trim_trailing_spaces(stdout.getvalue())
        actual_stderr = self._trim_trailing_spaces(stderr.getvalue())

        # ignore any references to specific api version
        if remove_version:
            actual_stdout = self._remove_api_version_number(actual_stdout)
            actual_stderr = self._remove_api_version_number(actual_stderr)

        if expected_stdout is not None and expected_stdout != actual_stdout:
            expected_stdout = self._normalize_expected_output(expected_stdout, format_vars)
            print('EXPECTED STDOUT:', repr(expected_stdout))
            print('ACTUAL STDOUT:  ', repr(actual_stdout))
            print(actual_stdout)
        if expected_part_of_stdout is not None and expected_part_of_stdout not in actual_stdout:
            expected_part_of_stdout = self._normalize_expected_output(
                expected_part_of_stdout, format_vars
            )
            print('EXPECTED TO FIND IN STDOUT:', repr(expected_part_of_stdout))
            print('ACTUAL STDOUT:             ', repr(actual_stdout))
        if expected_stderr is not None and expected_stderr != actual_stderr:
            print('EXPECTED STDERR:', repr(expected_stderr))
            print('ACTUAL STDERR:  ', repr(actual_stderr))
            print(actual_stderr)

        if expected_json_in_stdout is not None:
            json_match = self.json_pattern.match(actual_stdout)
            if not json_match:
                self.fail('EXPECTED TO FIND A JSON IN: ' + repr(actual_stdout))

            found_json = json.loads(json_match.group('dict_json') or json_match.group('list_json'))
            if json_match.group('dict_json'):
                self.assertDictIsContained(expected_json_in_stdout, found_json)
            else:
                self.assertListOfDictsIsContained(expected_json_in_stdout, found_json)

        if expected_stdout is not None:
            self.assertEqual(expected_stdout, actual_stdout, 'stdout')
        if expected_part_of_stdout is not None:
            self.assertIn(expected_part_of_stdout, actual_stdout)
        if unexpected_part_of_stdout is not None:
            self.assertNotIn(unexpected_part_of_stdout, actual_stdout)
        if expected_stderr is not None:
            self.assertEqual(expected_stderr, actual_stderr, 'stderr')
        assert expected_status == actual_status
        return actual_status, actual_stdout, actual_stderr

    @classmethod
    def _upload_multiple_files(cls, bucket):
        data = UploadSourceBytes(b'test-data')
        bucket.upload(data, 'a/test.csv')
        bucket.upload(data, 'a/test.tsv')
        bucket.upload(data, 'b/b/test.csv')
        bucket.upload(data, 'b/b1/test.csv')
        bucket.upload(data, 'b/b2/test.tsv')
        bucket.upload(data, 'b/test.txt')
        bucket.upload(data, 'c/test.csv')
        bucket.upload(data, 'c/test.tsv')


class TestTTYConsoleTool(BaseConsoleToolTest):
    def _get_stdouterr(self):
        class FakeStringIO(StringIO):
            def isatty(self):
                return True

        stdout = FakeStringIO()
        stderr = FakeStringIO()
        return stdout, stderr

    def test_e_c1_char_ls_default_escape_control_chars_setting(self):
        self._authorize_account()
        self._run_command(['bucket', 'create', 'my-bucket-cc', 'allPrivate'], 'bucket_0\n', '', 0)

        with TempDir() as temp_dir:
            local_file = self._make_local_file(temp_dir, 'x')
            bad_str = '\u009b2K\u009b7Gb\u009b24Gx\u009b4GH'
            escaped_bad_str = '\\x9b2K\\x9b7Gb\\x9b24Gx\\x9b4GH'

            self._run_command(
                ['file', 'upload', '--no-progress', 'my-bucket-cc', local_file, bad_str]
            )
            self._run_command(
                ['file', 'upload', '--no-progress', 'my-bucket-cc', local_file, 'some_normal_text']
            )

            self._run_command(
                ['ls', *self.b2_uri_args('my-bucket-cc')], expected_part_of_stdout=escaped_bad_str
            )


class TestConsoleTool(BaseConsoleToolTest):
    @pytest.mark.apiver(to_ver=3)
    def test_camel_case_supported_in_v3(self):
        self._authorize_account()
        self._run_command(
            ['bucket', 'create', 'my-bucket', '--bucketInfo', '{"xxx": "123"}', 'allPrivate'],
            'bucket_0\n',
            '',
            0,
        )
        self._run_command(
            [
                'bucket',
                'create',
                'my-bucket-kebab',
                '--bucket-info',
                '{"xxx": "123"}',
                'allPrivate',
            ],
            'bucket_1\n',
            '',
            0,
        )

    @pytest.mark.apiver(from_ver=4)
    def test_camel_case_not_supported_in_v4(self):
        self._authorize_account()
        self._run_command(
            ['bucket', 'create', 'my-bucket', '--bucketInfo', 'allPrivate'], '', '', 2
        )
        self._run_command(
            [
                'bucket',
                'create',
                'my-bucket-kebab',
                '--bucket-info',
                '{"xxx": "123"}',
                'allPrivate',
            ],
            'bucket_0\n',
            '',
            0,
        )

    def test_create_key_and_authorize_with_it(self):
        # Start with authorizing with the master key
        self._authorize_account()

        # Create a key
        self._run_command(
            ['key', 'create', 'key1', 'listBuckets,listKeys'],
            'appKeyId0 appKey0\n',
            '',
            0,
        )

        # test deprecated command
        self._run_command(
            ['create-key', 'key2', 'listBuckets,listKeys'],
            'appKeyId1 appKey1\n',
            'WARNING: `create-key` command is deprecated. Use `key create` instead.\n',
            0,
        )

        # Authorize with the key
        self._run_command(
            ['account', 'authorize', 'appKeyId0', 'appKey0'],
            None,
            '',
            0,
        )

        self._run_command(
            ['account', 'authorize', 'appKeyId1', 'appKey1'],
            None,
            '',
            0,
        )

        # test deprecated command
        self._run_command(
            ['authorize-account', 'appKeyId0', 'appKey0'],
            None,
            'WARNING: `authorize-account` command is deprecated. Use `account authorize` instead.\n',
            0,
        )

        # test deprecated command
        self._run_command(
            ['authorize-account', 'appKeyId1', 'appKey1'],
            None,
            'WARNING: `authorize-account` command is deprecated. Use `account authorize` instead.\n',
            0,
        )

    def test_create_key_with_authorization_from_env_vars(self):
        # Initial condition
        assert self.account_info.get_account_auth_token() is None

        # Authorize an account with a good api key.

        # Setting up environment variables
        with mock.patch.dict(
            'os.environ',
            {
                B2_APPLICATION_KEY_ID_ENV_VAR: self.account_id,
                B2_APPLICATION_KEY_ENV_VAR: self.master_key,
            },
        ):
            assert B2_APPLICATION_KEY_ID_ENV_VAR in os.environ
            assert B2_APPLICATION_KEY_ENV_VAR in os.environ

            # The first time we're running on this cache there will be output from the implicit "account authorize" call
            self._run_command(
                ['key', 'create', 'key1', 'listBuckets,listKeys'],
                'appKeyId0 appKey0\n',
                '',
                0,
            )

            # The second time "account authorize" is not called
            self._run_command(
                ['key', 'create', 'key1', 'listBuckets,listKeys,writeKeys'],
                'appKeyId1 appKey1\n',
                '',
                0,
            )

            with mock.patch.dict(
                'os.environ',
                {
                    B2_APPLICATION_KEY_ID_ENV_VAR: 'appKeyId1',
                    B2_APPLICATION_KEY_ENV_VAR: 'appKey1',
                },
            ):
                # "account authorize" is called when the key changes
                self._run_command(
                    ['key', 'create', 'key1', 'listBuckets,listKeys'],
                    'appKeyId2 appKey2\n',
                    '',
                    0,
                )

                # "account authorize" is also called when the realm changes
                with mock.patch.dict(
                    'os.environ',
                    {
                        B2_ENVIRONMENT_ENV_VAR: 'http://custom.example.com',
                    },
                ):
                    self._run_command(
                        ['key', 'create', 'key1', 'listBuckets,listKeys'],
                        'appKeyId3 appKey3\n',
                        'Using http://custom.example.com\n',
                        0,
                    )

    def test_authorize_key_without_list_buckets(self):
        self._authorize_account()

        # Create a key without listBuckets
        self._run_command(['key', 'create', 'key1', 'listKeys'], 'appKeyId0 appKey0\n', '', 0)

        # Authorize with the key
        self._run_command(
            ['account', 'authorize', 'appKeyId0', 'appKey0'],
            '',
            'ERROR: application key has no listBuckets capability, which is required for the b2 command-line tool\n',
            1,
        )

    def test_create_bucket__with_lifecycle_rule(self):
        self._authorize_account()

        rule = json.dumps(
            {'daysFromHidingToDeleting': 1, 'daysFromUploadingToHiding': None, 'fileNamePrefix': ''}
        )

        self._run_command(
            ['bucket', 'create', 'my-bucket', 'allPrivate', '--lifecycle-rule', rule],
            'bucket_0\n',
            '',
            0,
        )

    def test_create_bucket__with_lifecycle_rules(self):
        self._authorize_account()

        rules = json.dumps(
            [
                {
                    'daysFromHidingToDeleting': 1,
                    'daysFromUploadingToHiding': None,
                    'fileNamePrefix': '',
                }
            ]
        )

        self._run_command(
            ['bucket', 'create', 'my-bucket', 'allPrivate', '--lifecycle-rules', rules],
            'bucket_0\n',
            '',
            0,
        )

    def test_create_bucket__mutually_exclusive_lifecycle_rules_options(self):
        self._authorize_account()

        rule = json.dumps(
            {'daysFromHidingToDeleting': 1, 'daysFromUploadingToHiding': None, 'fileNamePrefix': ''}
        )

        self._run_command(
            [
                'bucket',
                'create',
                'my-bucket',
                'allPrivate',
                '--lifecycle-rule',
                rule,
                '--lifecycle-rules',
                f'[{rule}]',
            ],
            '',
            '',
            2,
        )

    def test_create_bucket_key_and_authorize_with_it(self):
        # Start with authorizing with the master key
        self._authorize_account()

        # Make a bucket
        self._run_command(['bucket', 'create', 'my-bucket', 'allPrivate'], 'bucket_0\n', '', 0)

        # Create a key restricted to that bucket
        self._run_command(
            ['key', 'create', '--bucket', 'my-bucket', 'key1', 'listKeys,listBuckets'],
            'appKeyId0 appKey0\n',
            '',
            0,
        )

        # test deprecated command
        self._run_command(
            ['create-key', '--bucket', 'my-bucket', 'key2', 'listKeys,listBuckets'],
            'appKeyId1 appKey1\n',
            'WARNING: `create-key` command is deprecated. Use `key create` instead.\n',
            0,
        )

        # Authorize with the key
        self._run_command(
            ['account', 'authorize', 'appKeyId0', 'appKey0'],
            None,
            '',
            0,
        )

        self._run_command(
            ['account', 'authorize', 'appKeyId1', 'appKey1'],
            None,
            '',
            0,
        )

    def test_create_multi_bucket_key_and_authorize_with_it(self):
        # Start with authorizing with the master key
        self._authorize_account()

        # Make two buckets
        self._run_command(['bucket', 'create', 'my-bucket-0', 'allPrivate'], 'bucket_0\n', '', 0)
        self._run_command(['bucket', 'create', 'my-bucket-1', 'allPrivate'], 'bucket_1\n', '', 0)

        # Create a key restricted to those buckets
        self._run_command(
            [
                'key',
                'create',
                '--bucket',
                'my-bucket-0',
                '--bucket',
                'my-bucket-1',
                'key1',
                'listKeys,listBuckets',
            ],
            'appKeyId0 appKey0\n',
            '',
            0,
        )

        # test deprecated command
        self._run_command(
            [
                'create-key',
                '--bucket',
                'my-bucket-0',
                '--bucket',
                'my-bucket-1',
                'key2',
                'listKeys,listBuckets',
            ],
            'appKeyId1 appKey1\n',
            'WARNING: `create-key` command is deprecated. Use `key create` instead.\n',
            0,
        )

        # Authorize with the key
        self._run_command(
            ['account', 'authorize', 'appKeyId0', 'appKey0'],
            None,
            '',
            0,
        )

        self._run_command(
            ['account', 'authorize', 'appKeyId1', 'appKey1'],
            None,
            '',
            0,
        )

    def test_update_bucket_without_lifecycle(self):
        # Start with authorizing with the master key
        self._authorize_account()

        bucket_name = 'my-bucket-liferules'
        # Create a bucket with lifecycleRule
        self._run_command(
            [
                'bucket',
                'create',
                '--lifecycle-rule',
                '{"daysFromHidingToDeleting": 2, "fileNamePrefix": "foo"}',
                bucket_name,
                'allPrivate',
            ],
            'bucket_0\n',
            '',
            0,
        )

        expected_stdout_dict = {
            'accountId': self.account_id,
            'bucketId': 'bucket_0',
            'bucketInfo': {'xxx': '123'},
            'bucketName': 'my-bucket-liferules',
            'bucketType': 'allPrivate',
            'lifecycleRules': [{'daysFromHidingToDeleting': 2, 'fileNamePrefix': 'foo'}],
        }

        # Update some other attribute than lifecycleRule, which should remain intact
        self._run_command(
            ['bucket', 'update', bucket_name, '--bucket-info', '{"xxx": "123"}'],
            expected_json_in_stdout=expected_stdout_dict,
        )

    def test_clear_account(self):
        # Initial condition
        self._authorize_account()
        assert self.account_info.get_account_auth_token() is not None

        # Clearing the account should remove the auth token
        # from the account info.
        self._run_command(['account', 'clear'], '', '', 0)
        assert self.account_info.get_account_auth_token() is None

    def test_deprecated_clear_account(self):
        # Initial condition
        self._authorize_account()
        assert self.account_info.get_account_auth_token() is not None

        # Clearing the account should remove the auth token
        # from the account info.
        self._run_command(
            ['clear-account'],
            '',
            'WARNING: `clear-account` command is deprecated. Use `account clear` instead.\n',
            0,
        )
        assert self.account_info.get_account_auth_token() is None

    def test_buckets(self):
        self._authorize_account()

        # Make a bucket with an illegal name
        expected_stderr = 'ERROR: Bad request: illegal bucket name: bad/bucket/name\n'
        self._run_command(
            ['bucket', 'create', 'bad/bucket/name', 'allPublic'], '', expected_stderr, 1
        )

        # Make two buckets
        self._run_command(['bucket', 'create', 'my-bucket', 'allPrivate'], 'bucket_0\n', '', 0)
        self._run_command(['bucket', 'create', 'your-bucket', 'allPrivate'], 'bucket_1\n', '', 0)

        # Update one of them
        expected_json = {
            'accountId': self.account_id,
            'bucketId': 'bucket_0',
            'bucketInfo': {},
            'bucketName': 'my-bucket',
            'bucketType': 'allPublic',
            'corsRules': [],
            'defaultServerSideEncryption': {'mode': 'none'},
            'lifecycleRules': [],
            'options': [],
            'revision': 2,
        }

        self._run_command(
            ['bucket', 'update', 'my-bucket', 'allPublic'], expected_json_in_stdout=expected_json
        )

        # Make sure they are there
        expected_stdout = """
        bucket_0  allPublic   my-bucket
        bucket_1  allPrivate  your-bucket
        """

        self._run_command(['bucket', 'list'], expected_stdout, '', 0)

        # Delete one
        expected_stdout = ''

        self._run_command(['bucket', 'delete', 'your-bucket'], expected_stdout, '', 0)

    def test_deprecated_bucket_commands(self):
        self._authorize_account()

        # Make a bucket with an illegal name
        expected_stderr = (
            'WARNING: `create-bucket` command is deprecated. Use `bucket create` instead.\n'
            'ERROR: Bad request: illegal bucket name: bad/bucket/name\n'
        )
        self._run_command(['create-bucket', 'bad/bucket/name', 'allPublic'], '', expected_stderr, 1)

        # Make two buckets
        self._run_command(
            ['create-bucket', 'my-bucket', 'allPrivate'],
            'bucket_0\n',
            'WARNING: `create-bucket` command is deprecated. Use `bucket create` instead.\n',
            0,
        )
        self._run_command(
            ['create-bucket', 'your-bucket', 'allPrivate'],
            'bucket_1\n',
            'WARNING: `create-bucket` command is deprecated. Use `bucket create` instead.\n',
            0,
        )

        # Update one of them
        expected_json = {
            'accountId': self.account_id,
            'bucketId': 'bucket_0',
            'bucketInfo': {},
            'bucketName': 'my-bucket',
            'bucketType': 'allPublic',
            'corsRules': [],
            'defaultServerSideEncryption': {'mode': 'none'},
            'lifecycleRules': [],
            'options': [],
            'revision': 2,
        }

        self._run_command(
            ['update-bucket', 'my-bucket', 'allPublic'],
            expected_stderr='WARNING: `update-bucket` command is deprecated. Use `bucket update` instead.\n',
            expected_json_in_stdout=expected_json,
        )

        # Make sure they are there
        expected_stdout = """
        bucket_0  allPublic   my-bucket
        bucket_1  allPrivate  your-bucket
        """

        self._run_command(
            ['list-buckets'],
            expected_stdout,
            'WARNING: `list-buckets` command is deprecated. Use `bucket list` instead.\n',
            0,
        )

        # Delete one
        expected_stdout = ''

        self._run_command(
            ['delete-bucket', 'your-bucket'],
            expected_stdout,
            'WARNING: `delete-bucket` command is deprecated. Use `bucket delete` instead.\n',
            0,
        )

    def test_encrypted_buckets(self):
        self._authorize_account()

        # Make two encrypted buckets
        self._run_command(['bucket', 'create', 'my-bucket', 'allPrivate'], 'bucket_0\n', '', 0)
        self._run_command(
            [
                'bucket',
                'create',
                '--default-server-side-encryption=SSE-B2',
                'your-bucket',
                'allPrivate',
            ],
            'bucket_1\n',
            '',
            0,
        )

        # Update the one without encryption
        expected_json = {
            'accountId': self.account_id,
            'bucketId': 'bucket_0',
            'bucketInfo': {},
            'bucketName': 'my-bucket',
            'bucketType': 'allPublic',
            'corsRules': [],
            'defaultServerSideEncryption': {'algorithm': 'AES256', 'mode': 'SSE-B2'},
            'lifecycleRules': [],
            'options': [],
            'revision': 2,
        }

        self._run_command(
            [
                'bucket',
                'update',
                '--default-server-side-encryption=SSE-B2',
                'my-bucket',
                'allPublic',
            ],
            expected_json_in_stdout=expected_json,
        )

        # Update the one with encryption
        expected_json = {
            'accountId': self.account_id,
            'bucketId': 'bucket_1',
            'bucketInfo': {},
            'bucketName': 'your-bucket',
            'bucketType': 'allPrivate',
            'corsRules': [],
            'defaultServerSideEncryption': {'algorithm': 'AES256', 'mode': 'SSE-B2'},
            'lifecycleRules': [],
            'options': [],
            'revision': 2,
        }

        self._run_command(
            ['bucket', 'update', 'your-bucket', 'allPrivate'], expected_json_in_stdout=expected_json
        )

        # Make sure they are there
        expected_stdout = """
        bucket_0  allPublic   my-bucket
        bucket_1  allPrivate  your-bucket
        """

        self._run_command(['bucket', 'list'], expected_stdout, '', 0)

    def test_keys(self):
        self._authorize_account()

        self._run_command(['bucket', 'create', 'my-bucket-a', 'allPublic'], 'bucket_0\n', '', 0)
        self._run_command(['bucket', 'create', 'my-bucket-b', 'allPublic'], 'bucket_1\n', '', 0)
        self._run_command(['bucket', 'create', 'my-bucket-c', 'allPublic'], 'bucket_2\n', '', 0)

        capabilities = ['readFiles', 'listBuckets']
        capabilities_with_commas = ','.join(capabilities)

        # Make a key with an illegal name
        expected_stderr = 'ERROR: Bad request: illegal key name: bad_key_name\n'
        self._run_command(
            ['key', 'create', 'bad_key_name', capabilities_with_commas], '', expected_stderr, 1
        )

        # Make a key with negative validDurationInSeconds
        expected_stderr = 'ERROR: Bad request: valid duration must be greater than 0, and less than 1000 days in seconds\n'
        self._run_command(
            ['key', 'create', '--duration', '-456', 'goodKeyName', capabilities_with_commas],
            '',
            expected_stderr,
            1,
        )

        # Make a key with validDurationInSeconds outside of range
        expected_stderr = (
            'ERROR: Bad request: valid duration must be greater than 0, '
            'and less than 1000 days in seconds\n'
        )
        self._run_command(
            ['key', 'create', '--duration', '0', 'goodKeyName', capabilities_with_commas],
            '',
            expected_stderr,
            1,
        )
        self._run_command(
            ['key', 'create', '--duration', '86400001', 'goodKeyName', capabilities_with_commas],
            '',
            expected_stderr,
            1,
        )

        # Create three keys
        self._run_command(
            ['key', 'create', 'goodKeyName-One', capabilities_with_commas],
            'appKeyId0 appKey0\n',
            '',
            0,
        )
        self._run_command(
            [
                'key',
                'create',
                '--bucket',
                'my-bucket-a',
                'goodKeyName-Two',
                capabilities_with_commas + ',readBucketEncryption',
            ],
            'appKeyId1 appKey1\n',
            '',
            0,
        )
        self._run_command(
            [
                'key',
                'create',
                '--bucket',
                'my-bucket-b',
                'goodKeyName-Three',
                capabilities_with_commas,
            ],
            'appKeyId2 appKey2\n',
            '',
            0,
        )
        self._run_command(
            ['key', 'create', '--all-capabilities', 'goodKeyName-Four'],
            'appKeyId3 appKey3\n',
            '',
            0,
        )
        self._run_command(
            [
                'key',
                'create',
                '--bucket',
                'my-bucket-b',
                'goodKeyName-Five',
                capabilities_with_commas,
            ],
            'appKeyId4 appKey4\n',
            '',
            0,
        )
        self._run_command(
            ['create-key', '--bucket', 'my-bucket-b', 'goodKeyName-Six', capabilities_with_commas],
            'appKeyId5 appKey5\n',
            'WARNING: `create-key` command is deprecated. Use `key create` instead.\n',
            0,
        )

        # Delete one key
        self._run_command(['key', 'delete', 'appKeyId2'], 'appKeyId2\n', '', 0)

        # test deprecated command
        self._run_command(
            ['delete-key', 'appKeyId5'],
            'appKeyId5\n',
            'WARNING: `delete-key` command is deprecated. Use `key delete` instead.\n',
            0,
        )

        # Delete one bucket, to test listing when a bucket is gone.
        self._run_command_ignore_output(['bucket', 'delete', 'my-bucket-b'])

        # List keys
        expected_list_keys_out = """
            appKeyId0   goodKeyName-One
            appKeyId1   goodKeyName-Two
            appKeyId3   goodKeyName-Four
            appKeyId4   goodKeyName-Five
            """

        expected_list_keys_out_long = """
            appKeyId0   goodKeyName-One        -                      -            -          ''   readFiles,listBuckets
            appKeyId1   goodKeyName-Two        my-bucket-a            -            -          ''   readFiles,listBuckets,readBucketEncryption
            appKeyId3   goodKeyName-Four       -                      -            -          ''   {}
            appKeyId4   goodKeyName-Five       id=bucket_1            -            -          ''   readFiles,listBuckets
            """.format(','.join(sorted(ALL_CAPABILITIES)))

        self._run_command(['key', 'list'], expected_list_keys_out, '', 0)
        self._run_command(['key', 'list', '--long'], expected_list_keys_out_long, '', 0)

        self._run_command(
            ['list-keys'],
            expected_list_keys_out,
            'WARNING: `list-keys` command is deprecated. Use `key list` instead.\n',
            0,
        )
        self._run_command(
            ['list-keys', '--long'],
            expected_list_keys_out_long,
            'WARNING: `list-keys` command is deprecated. Use `key list` instead.\n',
            0,
        )

        # authorize and make calls using application key with no restrictions
        self._run_command(['account', 'authorize', 'appKeyId0', 'appKey0'], None, '', 0)
        self._run_command(
            ['bucket', 'list'],
            'bucket_0  allPublic   my-bucket-a\nbucket_2  allPublic   my-bucket-c\n',
            '',
            0,
        )

        expected_json = {
            'accountId': self.account_id,
            'bucketId': 'bucket_0',
            'bucketInfo': {},
            'bucketName': 'my-bucket-a',
            'bucketType': 'allPublic',
            'corsRules': [],
            'defaultServerSideEncryption': {'mode': None},
            'lifecycleRules': [],
            'options': [],
            'revision': 1,
        }
        self._run_command(['bucket', 'get', 'my-bucket-a'], expected_json_in_stdout=expected_json)

        # authorize and make calls using an application key with bucket restrictions
        self._run_command(['account', 'authorize', 'appKeyId1', 'appKey1'], None, '', 0)

        self._run_command(
            ['bucket', 'list'],
            '',
            "ERROR: Application key is restricted to buckets: ['my-bucket-a']\n",
            1,
        )
        self._run_command(
            ['bucket', 'get', 'my-bucket-c'],
            '',
            "ERROR: Application key is restricted to buckets: ['my-bucket-a']\n",
            1,
        )

        expected_json = {
            'accountId': self.account_id,
            'bucketId': 'bucket_0',
            'bucketInfo': {},
            'bucketName': 'my-bucket-a',
            'bucketType': 'allPublic',
            'corsRules': [],
            'defaultServerSideEncryption': {'mode': 'none'},
            'lifecycleRules': [],
            'options': [],
            'revision': 1,
        }

        self._run_command(['bucket', 'get', 'my-bucket-a'], expected_json_in_stdout=expected_json)
        self._run_command(
            ['ls', '--json', *self.b2_uri_args('my-bucket-c')],
            '',
            "ERROR: Application key is restricted to buckets: ['my-bucket-a']\n",
            1,
        )

    def test_multi_bucket_keys(self):
        self._authorize_account()

        self._run_command(['bucket', 'create', 'my-bucket-a', 'allPublic'], 'bucket_0\n', '', 0)
        self._run_command(['bucket', 'create', 'my-bucket-b', 'allPublic'], 'bucket_1\n', '', 0)
        self._run_command(['bucket', 'create', 'my-bucket-c', 'allPublic'], 'bucket_2\n', '', 0)

        capabilities = ['readFiles', 'listBuckets']
        capabilities_with_commas = ','.join(capabilities)

        # Create a multi-bucket key with one of the buckets having invalid name
        expected_stderr = 'Bucket not found: invalid. If you believe it exists, run `b2 bucket list` to reset cache, then try again.\n'

        self._run_command(
            [
                'key',
                'create',
                '--bucket',
                'my-bucket-a',
                '--bucket',
                'invalid',
                'goodKeyName',
                capabilities_with_commas,
            ],
            '',
            expected_stderr,
            1,
        )

        # Create a multi-bucket key
        self._run_command(
            [
                'key',
                'create',
                '--bucket',
                'my-bucket-a',
                '--bucket',
                'my-bucket-b',
                'goodKeyName',
                capabilities_with_commas,
            ],
            'appKeyId0 appKey0\n',
            '',
            0,
        )

        # List keys
        expected_list_keys_out = 'appKeyId0   goodKeyName\n'

        expected_list_keys_out_long = """
            appKeyId0   goodKeyName            my-bucket-a, my-bucket-b   -            -          ''   readFiles,listBuckets
            """

        self._run_command(['key', 'list'], expected_list_keys_out, '', 0)
        self._run_command(['key', 'list', '--long'], expected_list_keys_out_long, '', 0)

        # authorize and make calls using an application key with bucket restrictions
        self._run_command(['account', 'authorize', 'appKeyId0', 'appKey0'], None, '', 0)

        self._run_command(
            ['bucket', 'list'],
            '',
            "ERROR: Application key is restricted to buckets: ['my-bucket-a', 'my-bucket-b']\n",
            1,
        )
        self._run_command(
            ['bucket', 'get', 'my-bucket-c'],
            '',
            "ERROR: Application key is restricted to buckets: ['my-bucket-a', 'my-bucket-b']\n",
            1,
        )

        def _get_expected_json(bucket_id: str, bucket_name: str):
            return {
                'accountId': self.account_id,
                'bucketId': bucket_id,
                'bucketInfo': {},
                'bucketName': bucket_name,
                'bucketType': 'allPublic',
                'corsRules': [],
                'defaultServerSideEncryption': {'mode': None},
                'lifecycleRules': [],
                'options': [],
                'revision': 1,
            }

        self._run_command(
            ['bucket', 'get', 'my-bucket-a'],
            expected_json_in_stdout=_get_expected_json('bucket_0', 'my-bucket-a'),
        )

        self._run_command(
            ['bucket', 'get', 'my-bucket-b'],
            expected_json_in_stdout=_get_expected_json('bucket_1', 'my-bucket-b'),
        )

        self._run_command(
            ['ls', '--json', *self.b2_uri_args('my-bucket-c')],
            '',
            "ERROR: Application key is restricted to buckets: ['my-bucket-a', 'my-bucket-b']\n",
            1,
        )

    def test_bucket_info_from_json(self):
        self._authorize_account()
        self._run_command(['bucket', 'create', 'my-bucket', 'allPublic'], 'bucket_0\n', '', 0)

        bucket_info = {'color': 'blue'}

        expected_json = {
            'accountId': self.account_id,
            'bucketId': 'bucket_0',
            'bucketInfo': {'color': 'blue'},
            'bucketName': 'my-bucket',
            'bucketType': 'allPrivate',
            'corsRules': [],
            'defaultServerSideEncryption': {'mode': 'none'},
            'lifecycleRules': [],
            'options': [],
            'revision': 2,
        }
        self._run_command(
            [
                'bucket',
                'update',
                '--bucket-info',
                json.dumps(bucket_info),
                'my-bucket',
                'allPrivate',
            ],
            expected_json_in_stdout=expected_json,
        )

    @pytest.mark.apiver(from_ver=4)
    def test_rm_fileid_v4(self):
        self._authorize_account()
        self._run_command(['bucket', 'create', 'my-bucket', 'allPublic'], 'bucket_0\n', '', 0)

        with TempDir() as temp_dir:
            local_file1 = self._make_local_file(temp_dir, 'file1.txt')
            # For this test, use a mod time without millis.  My mac truncates
            # millis and just leaves seconds.
            mod_time = 1500111222
            os.utime(local_file1, (mod_time, mod_time))
            self.assertEqual(1500111222, os.path.getmtime(local_file1))

            # Upload a file
            self._run_command(
                [
                    'file',
                    'upload',
                    '--no-progress',
                    'my-bucket',
                    local_file1,
                    'file1.txt',
                    '--cache-control=private, max-age=3600',
                ],
                remove_version=True,
            )

            # Hide file
            self._run_command(
                ['file', 'hide', 'b2://my-bucket/file1.txt'],
            )

            # Delete one file version
            self._run_command(['rm', 'b2id://9998'])
            # Delete one file version
            self._run_command(['rm', 'b2id://9999'])

    def test_hide_file_legacy_syntax(self):
        self._authorize_account()
        self._run_command(['bucket', 'create', 'my-bucket', 'allPublic'], 'bucket_0\n', '', 0)

        with TempDir() as temp_dir:
            local_file1 = self._make_local_file(temp_dir, 'file1.txt')
            # For this test, use a mod time without millis.  My mac truncates
            # millis and just leaves seconds.
            mod_time = 1500111222
            os.utime(local_file1, (mod_time, mod_time))
            self.assertEqual(1500111222, os.path.getmtime(local_file1))

            # Upload a file
            self._run_command(
                [
                    'file',
                    'upload',
                    '--no-progress',
                    'my-bucket',
                    local_file1,
                    'file1.txt',
                    '--cache-control=private, max-age=3600',
                ],
                remove_version=True,
            )

            # Get file info
            expected_json = {
                'accountId': self.account_id,
                'action': 'upload',
                'bucketId': 'bucket_0',
                'size': 11,
                'contentSha1': '2aae6c35c94fcfb415dbe95f408b9ce91ee846ed',
                'contentType': 'b2/x-auto',
                'fileId': '9999',
                'fileInfo': {
                    'src_last_modified_millis': '1500111222000',
                    'b2-cache-control': 'private, max-age=3600',
                },
                'fileName': 'file1.txt',
                'serverSideEncryption': {'mode': 'none'},
                'uploadTimestamp': 5000,
            }

            self._run_command(
                ['file', 'info', 'b2id://9999'],
                expected_json_in_stdout=expected_json,
            )

            # Hide the file
            expected_json = {
                'action': 'hide',
                'contentSha1': 'none',
                'fileId': '9998',
                'fileInfo': {},
                'fileName': 'file1.txt',
                'serverSideEncryption': {'mode': 'none'},
                'size': 0,
                'uploadTimestamp': 5001,
            }

            self._run_command(
                ['file', 'hide', 'b2://my-bucket/file1.txt'],
                expected_json_in_stdout=expected_json,
            )

    def test_files(self):
        self._authorize_account()
        self._run_command(['bucket', 'create', 'my-bucket', 'allPublic'], 'bucket_0\n', '', 0)

        with TempDir() as temp_dir:
            local_file1 = self._make_local_file(temp_dir, 'file1.txt')
            # For this test, use a mod time without millis.  My mac truncates
            # millis and just leaves seconds.
            mod_time = 1500111222
            os.utime(local_file1, (mod_time, mod_time))
            self.assertEqual(1500111222, os.path.getmtime(local_file1))

            # Upload a file
            expected_stdout = """
            URL by file name: http://download.example.com/file/my-bucket/file1.txt
            URL by fileId: http://download.example.com/b2api/vx/b2_download_file_by_id?fileId=9999"""
            expected_json = {
                'action': 'upload',
                'contentSha1': '2aae6c35c94fcfb415dbe95f408b9ce91ee846ed',
                'contentType': 'b2/x-auto',
                'fileId': '9999',
                'fileInfo': {
                    'src_last_modified_millis': '1500111222000',
                    'b2-cache-control': 'private, max-age=3600',
                },
                'fileName': 'file1.txt',
                'serverSideEncryption': {'mode': 'none'},
                'size': 11,
                'uploadTimestamp': 5000,
            }

            self._run_command(
                [
                    'file',
                    'upload',
                    '--no-progress',
                    'my-bucket',
                    local_file1,
                    'file1.txt',
                    '--cache-control=private, max-age=3600',
                ],
                expected_json_in_stdout=expected_json,
                remove_version=True,
                expected_part_of_stdout=expected_stdout,
            )

            # Get file info
            mod_time_str = str(file_mod_time_millis(local_file1))
            expected_json = {
                'accountId': self.account_id,
                'action': 'upload',
                'bucketId': 'bucket_0',
                'size': 11,
                'contentSha1': '2aae6c35c94fcfb415dbe95f408b9ce91ee846ed',
                'contentType': 'b2/x-auto',
                'fileId': '9999',
                'fileInfo': {
                    'src_last_modified_millis': '1500111222000',
                    'b2-cache-control': 'private, max-age=3600',
                },
                'fileName': 'file1.txt',
                'serverSideEncryption': {'mode': 'none'},
                'uploadTimestamp': 5000,
            }

            self._run_command(
                ['file', 'info', 'b2id://9999'],
                expected_json_in_stdout=expected_json,
            )

            # Hide the file
            expected_json = {
                'action': 'hide',
                'contentSha1': 'none',
                'fileId': '9998',
                'fileInfo': {},
                'fileName': 'file1.txt',
                'serverSideEncryption': {'mode': 'none'},
                'size': 0,
                'uploadTimestamp': 5001,
            }

            self._run_command(
                ['file', 'hide', 'b2://my-bucket/file1.txt'],
                expected_json_in_stdout=expected_json,
            )

            # List the file versions
            expected_json = [
                {
                    'action': 'hide',
                    'contentSha1': 'none',
                    'fileId': '9998',
                    'fileInfo': {},
                    'fileName': 'file1.txt',
                    'serverSideEncryption': {'mode': 'none'},
                    'size': 0,
                    'uploadTimestamp': 5001,
                },
                {
                    'action': 'upload',
                    'contentSha1': '2aae6c35c94fcfb415dbe95f408b9ce91ee846ed',
                    'contentType': 'b2/x-auto',
                    'fileId': '9999',
                    'fileInfo': {
                        'src_last_modified_millis': str(mod_time_str),
                        'b2-cache-control': 'private, max-age=3600',
                    },
                    'fileName': 'file1.txt',
                    'serverSideEncryption': {'mode': 'none'},
                    'size': 11,
                    'uploadTimestamp': 5000,
                },
            ]

            self._run_command(
                ['ls', '--json', '--versions', *self.b2_uri_args('my-bucket')],
                expected_json_in_stdout=expected_json,
            )

            # List the file names
            expected_stdout = """
            []
            """

            self._run_command(
                ['ls', '--json', *self.b2_uri_args('my-bucket')], expected_stdout, '', 0
            )

            # Delete one file version, passing the name in
            expected_json = {'action': 'delete', 'fileId': '9998', 'fileName': 'file1.txt'}

            self._run_command(
                ['delete-file-version', 'file1.txt', '9998'],
                expected_stderr='WARNING: `delete-file-version` command is deprecated. Use `rm` instead.\n',
                expected_json_in_stdout=expected_json,
            )

            # Delete one file version, not passing the name in
            expected_json = {'action': 'delete', 'fileId': '9999', 'fileName': 'file1.txt'}

            self._run_command(
                ['delete-file-version', '9999'],
                expected_stderr='WARNING: `delete-file-version` command is deprecated. Use `rm` instead.\n',
                expected_json_in_stdout=expected_json,
            )

    def test_files_encrypted(self):
        self._authorize_account()
        self._run_command(['bucket', 'create', 'my-bucket', 'allPublic'], 'bucket_0\n', '', 0)

        with TempDir() as temp_dir:
            local_file1 = self._make_local_file(temp_dir, 'file1.txt')
            # For this test, use a mod time without millis.  My mac truncates
            # millis and just leaves seconds.
            mod_time = 1500111222
            os.utime(local_file1, (mod_time, mod_time))
            self.assertEqual(1500111222, os.path.getmtime(local_file1))

            # Upload a file
            expected_stdout = """
            URL by file name: http://download.example.com/file/my-bucket/file1.txt
            URL by fileId: http://download.example.com/b2api/vx/b2_download_file_by_id?fileId=9999"""
            expected_json = {
                'action': 'upload',
                'contentSha1': '2aae6c35c94fcfb415dbe95f408b9ce91ee846ed',
                'contentType': 'b2/x-auto',
                'fileId': '9999',
                'fileInfo': {'src_last_modified_millis': '1500111222000'},
                'fileName': 'file1.txt',
                'serverSideEncryption': {'algorithm': 'AES256', 'mode': 'SSE-B2'},
                'size': 11,
                'uploadTimestamp': 5000,
            }

            self._run_command(
                [
                    'file',
                    'upload',
                    '--no-progress',
                    '--destination-server-side-encryption=SSE-B2',
                    'my-bucket',
                    local_file1,
                    'file1.txt',
                ],
                expected_json_in_stdout=expected_json,
                remove_version=True,
                expected_part_of_stdout=expected_stdout,
            )

            # Get file info
            mod_time_str = str(file_mod_time_millis(local_file1))
            expected_json = {
                'accountId': self.account_id,
                'action': 'upload',
                'bucketId': 'bucket_0',
                'size': 11,
                'contentSha1': '2aae6c35c94fcfb415dbe95f408b9ce91ee846ed',
                'contentType': 'b2/x-auto',
                'fileId': '9999',
                'fileInfo': {'src_last_modified_millis': '1500111222000'},
                'fileName': 'file1.txt',
                'serverSideEncryption': {'algorithm': 'AES256', 'mode': 'SSE-B2'},
                'uploadTimestamp': 5000,
            }

            self._run_command(
                ['file', 'info', 'b2id://9999'],
                expected_json_in_stdout=expected_json,
            )

            self._run_command(
                ['file-info', 'b2id://9999'],
                expected_stderr='WARNING: `file-info` command is deprecated. Use `file info` instead.\n',
                expected_json_in_stdout=expected_json,
            )

            self._run_command(
                ['get-file-info', '9999'],
                expected_stderr='WARNING: `get-file-info` command is deprecated. Use `file info` instead.\n',
                expected_json_in_stdout=expected_json,
            )

            # Download by name
            local_download1 = os.path.join(temp_dir, 'download1.txt')
            expected_stdout_template = """
            File name:           file1.txt
            File id:             9999
            Output file path:    {output_path}
            File size:           11
            Content type:        b2/x-auto
            Content sha1:        2aae6c35c94fcfb415dbe95f408b9ce91ee846ed
            Encryption:          mode=SSE-B2, algorithm=AES256
            Retention:           none
            Legal hold:          <unset>
            INFO src_last_modified_millis: 1500111222000
            Checksum matches
            Download finished
            """
            expected_stdout = expected_stdout_template.format(
                output_path=pathlib.Path(local_download1).resolve()
            )

            self._run_command(
                ['file', 'download', '--no-progress', 'b2://my-bucket/file1.txt', local_download1],
                expected_stdout,
                '',
                0,
            )
            self.assertEqual(b'hello world', self._read_file(local_download1))
            self.assertEqual(mod_time, int(round(os.path.getmtime(local_download1))))

            # Download file by ID.  (Same expected output as downloading by name)
            local_download2 = os.path.join(temp_dir, 'download2.txt')
            expected_stdout = expected_stdout_template.format(
                output_path=pathlib.Path(local_download2).resolve()
            )
            self._run_command(
                ['file', 'download', '--no-progress', 'b2id://9999', local_download2],
                expected_stdout,
                '',
                0,
            )
            self.assertEqual(b'hello world', self._read_file(local_download2))

            # Hide the file
            expected_json = {
                'action': 'hide',
                'contentSha1': 'none',
                'fileId': '9998',
                'fileInfo': {},
                'fileName': 'file1.txt',
                'serverSideEncryption': {'mode': 'none'},
                'size': 0,
                'uploadTimestamp': 5001,
            }

            self._run_command(
                ['file', 'hide', 'b2://my-bucket/file1.txt'],
                expected_json_in_stdout=expected_json,
            )

            # List the file versions
            expected_json = [
                {
                    'action': 'hide',
                    'contentSha1': 'none',
                    'fileId': '9998',
                    'fileInfo': {},
                    'fileName': 'file1.txt',
                    'serverSideEncryption': {'mode': 'none'},
                    'size': 0,
                    'uploadTimestamp': 5001,
                },
                {
                    'action': 'upload',
                    'contentSha1': '2aae6c35c94fcfb415dbe95f408b9ce91ee846ed',
                    'contentType': 'b2/x-auto',
                    'fileId': '9999',
                    'fileInfo': {'src_last_modified_millis': str(mod_time_str)},
                    'fileName': 'file1.txt',
                    'serverSideEncryption': {'algorithm': 'AES256', 'mode': 'SSE-B2'},
                    'size': 11,
                    'uploadTimestamp': 5000,
                },
            ]

            self._run_command(
                ['ls', '--json', '--versions', *self.b2_uri_args('my-bucket')],
                expected_json_in_stdout=expected_json,
            )

            # List the file names
            expected_stdout = """
            []
            """

            self._run_command(
                ['ls', '--json', *self.b2_uri_args('my-bucket')], expected_stdout, '', 0
            )

            # Delete one file version, passing the name in
            expected_json = {'action': 'delete', 'fileId': '9998', 'fileName': 'file1.txt'}

            self._run_command(
                ['delete-file-version', 'file1.txt', '9998'],
                expected_stderr='WARNING: `delete-file-version` command is deprecated. Use `rm` instead.\n',
                expected_json_in_stdout=expected_json,
            )

            # Delete one file version, not passing the name in
            expected_json = {'action': 'delete', 'fileId': '9999', 'fileName': 'file1.txt'}

            self._run_command(
                ['delete-file-version', '9999'],
                expected_stderr='WARNING: `delete-file-version` command is deprecated. Use `rm` instead.\n',
                expected_json_in_stdout=expected_json,
            )

    def _test_download_to_directory(self, download_by: str):
        self._authorize_account()
        self._create_my_bucket()

        base_filename = 'file'
        extension = '.txt'
        source_filename = f'{base_filename}{extension}'

        with TempDir() as temp_dir:
            local_file = self._make_local_file(temp_dir, source_filename)
            local_file_content = self._read_file(local_file)

            self._run_command(
                ['file', 'upload', '--no-progress', 'my-bucket', local_file, source_filename],
                remove_version=True,
            )

            b2uri = f'b2://my-bucket/{source_filename}' if download_by == 'name' else 'b2id://9999'
            command = [
                'file',
                'download',
                '--no-progress',
                b2uri,
            ]

            target_directory = os.path.join(temp_dir, 'target')
            os.mkdir(target_directory)
            command += [target_directory]
            self._run_command(command)
            self.assertEqual(
                local_file_content, self._read_file(os.path.join(target_directory, source_filename))
            )

            # Download the file second time, to check the override behavior.
            self._run_command(command)
            # We should get another file.
            target_directory_files = [
                elem
                for elem in pathlib.Path(target_directory).glob(f'{base_filename}*{extension}')
                if elem.name != source_filename
            ]
            assert len(target_directory_files) == 1, f'{target_directory_files}'
            self.assertEqual(local_file_content, self._read_file(target_directory_files[0]))

    def test_download_by_id_to_directory(self):
        self._test_download_to_directory(download_by='id')

    def test_download_by_name_to_directory(self):
        self._test_download_to_directory(download_by='name')

    def test_get_download_auth_defaults(self):
        self._authorize_account()
        self._create_my_bucket()
        self._run_command(
            ['bucket', 'get-download-auth', 'my-bucket'],
            'fake_download_auth_token_bucket_0__86400\n',
            '',
            0,
        )

    def test_get_download_auth_explicit(self):
        self._authorize_account()
        self._create_my_bucket()
        self._run_command(
            [
                'bucket',
                'get-download-auth',
                '--prefix',
                'prefix',
                '--duration',
                '12345',
                'my-bucket',
            ],
            'fake_download_auth_token_bucket_0_prefix_12345\n',
            '',
            0,
        )

    def test_get_download_auth_url(self):
        self._authorize_account()
        self._create_my_bucket()
        self._run_command(
            ['get-download-url-with-auth', '--duration', '12345', 'my-bucket', 'my-file'],
            'http://download.example.com/file/my-bucket/my-file?Authorization=fake_download_auth_token_bucket_0_my-file_12345\n',
            'WARNING: `get-download-url-with-auth` command is deprecated. Use `file url` instead.\n',
            0,
        )
        self._run_command(
            ['file', 'url', '--with-auth', '--duration', '12345', 'b2://my-bucket/my-file'],
            'http://download.example.com/file/my-bucket/my-file?Authorization=fake_download_auth_token_bucket_0_my-file_12345\n',
            '',
            0,
        )

    def test_get_download_auth_url_with_encoding(self):
        self._authorize_account()
        self._create_my_bucket()
        self._run_command(
            ['get-download-url-with-auth', '--duration', '12345', 'my-bucket', '\u81ea'],
            'http://download.example.com/file/my-bucket/%E8%87%AA?Authorization=fake_download_auth_token_bucket_0_%E8%87%AA_12345\n',
            'WARNING: `get-download-url-with-auth` command is deprecated. Use `file url` instead.\n',
            0,
        )
        self._run_command(
            ['file', 'url', '--with-auth', '--duration', '12345', 'b2://my-bucket/\u81ea'],
            'http://download.example.com/file/my-bucket/%E8%87%AA?Authorization=fake_download_auth_token_bucket_0_%E8%87%AA_12345\n',
            '',
            0,
        )

    def test_list_unfinished_large_files_with_none(self):
        self._authorize_account()
        self._create_my_bucket()
        self._run_command(
            ['list-unfinished-large-files', 'my-bucket'],
            '',
            'WARNING: `list-unfinished-large-files` command is deprecated. Use `file large unfinished list` instead.\n',
            0,
        )

    def test_upload_large_file(self):
        self._authorize_account()
        self._create_my_bucket()
        min_part_size = self.account_info.get_recommended_part_size()
        file_size = min_part_size * 3

        with TempDir() as temp_dir:
            file_path = os.path.join(temp_dir, 'test.txt')
            text = '*' * file_size
            with open(file_path, 'wb') as f:
                f.write(text.encode('utf-8'))
            mod_time_str = str(file_mod_time_millis(file_path))
            expected_stdout = """
            URL by file name: http://download.example.com/file/my-bucket/test.txt
            URL by fileId: http://download.example.com/b2api/vx/b2_download_file_by_id?fileId=9999"""
            expected_json = {
                'action': 'upload',
                'contentSha1': 'none',
                'contentType': 'b2/x-auto',
                'fileId': '9999',
                'fileInfo': {
                    'large_file_sha1': 'cc8954ec25e0c564b6a693fb22200e4f832c18e8',
                    'src_last_modified_millis': str(mod_time_str),
                },
                'fileName': 'test.txt',
                'serverSideEncryption': {'mode': 'none'},
                'size': 600,
                'uploadTimestamp': 5000,
            }

            self._run_command(
                [
                    'file',
                    'upload',
                    '--no-progress',
                    '--threads',
                    '5',
                    'my-bucket',
                    file_path,
                    'test.txt',
                ],
                expected_json_in_stdout=expected_json,
                remove_version=True,
                expected_part_of_stdout=expected_stdout,
            )

    def test_upload_large_file_encrypted(self):
        self._authorize_account()
        self._run_command(['bucket', 'create', 'my-bucket', 'allPublic'], 'bucket_0\n', '', 0)
        min_part_size = self.account_info.get_recommended_part_size()
        file_size = min_part_size * 3

        with TempDir() as temp_dir:
            file_path = os.path.join(temp_dir, 'test.txt')
            text = '*' * file_size
            with open(file_path, 'wb') as f:
                f.write(text.encode('utf-8'))
            mod_time_str = str(file_mod_time_millis(file_path))
            expected_stdout = """
            URL by file name: http://download.example.com/file/my-bucket/test.txt
            URL by fileId: http://download.example.com/b2api/vx/b2_download_file_by_id?fileId=9999"""
            expected_json = {
                'action': 'upload',
                'contentSha1': 'none',
                'contentType': 'b2/x-auto',
                'fileId': '9999',
                'fileInfo': {
                    'large_file_sha1': 'cc8954ec25e0c564b6a693fb22200e4f832c18e8',
                    'src_last_modified_millis': str(mod_time_str),
                },
                'fileName': 'test.txt',
                'serverSideEncryption': {'algorithm': 'AES256', 'mode': 'SSE-B2'},
                'size': 600,
                'uploadTimestamp': 5000,
            }

            self._run_command(
                [
                    'file',
                    'upload',
                    '--no-progress',
                    '--destination-server-side-encryption=SSE-B2',
                    '--threads',
                    '5',
                    'my-bucket',
                    file_path,
                    'test.txt',
                ],
                expected_json_in_stdout=expected_json,
                remove_version=True,
                expected_part_of_stdout=expected_stdout,
            )

    def test_upload_incremental(self):
        self._authorize_account()
        self._run_command(['bucket', 'create', 'my-bucket', 'allPublic'], 'bucket_0\n', '', 0)
        min_part_size = self.account_info.get_recommended_part_size()
        file_size = min_part_size * 2

        with TempDir() as temp_dir:
            file_path = pathlib.Path(temp_dir) / 'test.txt'

            incremental_upload_params = [
                'file',
                'upload',
                '--no-progress',
                '--threads',
                '5',
                '--incremental-mode',
                'my-bucket',
                str(file_path),
                'test.txt',
            ]

            file_path.write_bytes(b'*' * file_size)
            self._run_command(incremental_upload_params)

            with open(file_path, 'ab') as f:
                f.write(b'*' * min_part_size)
            self._run_command(incremental_upload_params)

            downloaded_path = pathlib.Path(temp_dir) / 'out.txt'
            self._run_command(
                [
                    'file',
                    'download',
                    '-q',
                    'b2://my-bucket/test.txt',
                    str(downloaded_path),
                ]
            )
            assert downloaded_path.read_bytes() == file_path.read_bytes()

    def test_get_account_info(self):
        self._authorize_account()
        expected_json = {
            'accountAuthToken': 'auth_token_0',
            'accountFilePath': getattr(
                self.account_info, 'filename', None
            ),  # missing in StubAccountInfo in tests
            'accountId': self.account_id,
            'allowed': {
                'buckets': None,
                'capabilities': sorted(ALL_CAPABILITIES),
                'namePrefix': None,
            },
            'apiUrl': 'http://api.example.com',
            'applicationKey': self.master_key,
            'downloadUrl': 'http://download.example.com',
            's3endpoint': 'http://s3.api.example.com',
        }
        self._run_command(
            ['account', 'get'],
            expected_json_in_stdout=expected_json,
        )
        # test deprecated command
        self._run_command(
            ['get-account-info'],
            expected_json_in_stdout=expected_json,
            expected_stderr='WARNING: `get-account-info` command is deprecated. Use `account get` instead.\n',
        )

    def test_get_bucket(self):
        self._authorize_account()
        self._create_my_bucket()
        expected_json = {
            'accountId': self.account_id,
            'bucketId': 'bucket_0',
            'bucketInfo': {},
            'bucketName': 'my-bucket',
            'bucketType': 'allPublic',
            'corsRules': [],
            'defaultServerSideEncryption': {'mode': 'none'},
            'lifecycleRules': [],
            'options': [],
            'revision': 1,
        }
        self._run_command(
            ['bucket', 'get', 'my-bucket'],
            expected_json_in_stdout=expected_json,
        )

    def test_get_bucket_empty_show_size(self):
        self._authorize_account()
        self._create_my_bucket()
        expected_json = {
            'accountId': self.account_id,
            'bucketId': 'bucket_0',
            'bucketInfo': {},
            'bucketName': 'my-bucket',
            'bucketType': 'allPublic',
            'corsRules': [],
            'defaultServerSideEncryption': {'mode': 'none'},
            'fileCount': 0,
            'lifecycleRules': [],
            'options': [],
            'revision': 1,
            'totalSize': 0,
        }
        self._run_command(
            ['bucket', 'get', '--show-size', 'my-bucket'],
            expected_json_in_stdout=expected_json,
        )

    def test_get_bucket_one_item_show_size(self):
        self._authorize_account()
        self._create_my_bucket()
        with TempDir() as temp_dir:
            # Upload a standard test file.
            local_file1 = self._make_local_file(temp_dir, 'file1.txt')
            mod_time_str = str(file_mod_time_millis(local_file1))
            expected_stdout = """
            URL by file name: http://download.example.com/file/my-bucket/file1.txt
            URL by fileId: http://download.example.com/b2api/vx/b2_download_file_by_id?fileId=9999"""
            expected_json = {
                'action': 'upload',
                'contentSha1': '2aae6c35c94fcfb415dbe95f408b9ce91ee846ed',
                'contentType': 'b2/x-auto',
                'fileId': '9999',
                'fileInfo': {'src_last_modified_millis': str(mod_time_str)},
                'fileName': 'file1.txt',
                'serverSideEncryption': {'mode': 'none'},
                'size': 11,
                'uploadTimestamp': 5000,
            }
            self._run_command(
                ['file', 'upload', '--no-progress', 'my-bucket', local_file1, 'file1.txt'],
                expected_json_in_stdout=expected_json,
                remove_version=True,
                expected_part_of_stdout=expected_stdout,
            )

            # Now check the output of `bucket get` against the canon.
            expected_json = {
                'accountId': self.account_id,
                'bucketId': 'bucket_0',
                'bucketInfo': {},
                'bucketName': 'my-bucket',
                'bucketType': 'allPublic',
                'corsRules': [],
                'defaultServerSideEncryption': {'mode': 'none'},
                'fileCount': 1,
                'lifecycleRules': [],
                'options': [],
                'revision': 1,
                'totalSize': 11,
            }
            self._run_command(
                ['bucket', 'get', '--show-size', 'my-bucket'],
                expected_json_in_stdout=expected_json,
            )

    def test_get_bucket_with_versions(self):
        self._authorize_account()
        self._create_my_bucket()

        # Put many versions of a file into the test bucket. Unroll the loop here for convenience.
        bucket = self.b2_api.get_bucket_by_name('my-bucket')
        bucket.upload(UploadSourceBytes(b'test'), 'test')
        bucket.upload(UploadSourceBytes(b'test'), 'test')
        bucket.upload(UploadSourceBytes(b'test'), 'test')
        bucket.upload(UploadSourceBytes(b'test'), 'test')
        bucket.upload(UploadSourceBytes(b'test'), 'test')
        bucket.upload(UploadSourceBytes(b'test'), 'test')
        bucket.upload(UploadSourceBytes(b'test'), 'test')
        bucket.upload(UploadSourceBytes(b'test'), 'test')
        bucket.upload(UploadSourceBytes(b'test'), 'test')
        bucket.upload(UploadSourceBytes(b'test'), 'test')

        # Now check the output of `bucket get` against the canon.
        expected_json = {
            'accountId': self.account_id,
            'bucketId': 'bucket_0',
            'bucketInfo': {},
            'bucketName': 'my-bucket',
            'bucketType': 'allPublic',
            'corsRules': [],
            'defaultServerSideEncryption': {'mode': 'none'},
            'fileCount': 10,
            'lifecycleRules': [],
            'options': [],
            'revision': 1,
            'totalSize': 40,
        }
        self._run_command(
            ['bucket', 'get', '--show-size', 'my-bucket'],
            expected_json_in_stdout=expected_json,
        )

    def test_get_bucket_with_folders(self):
        self._authorize_account()
        self._create_my_bucket()

        # Create a hierarchical structure within the test bucket. Unroll the loop here for
        # convenience.
        bucket = self.b2_api.get_bucket_by_name('my-bucket')
        bucket.upload(UploadSourceBytes(b'test'), 'test')
        bucket.upload(UploadSourceBytes(b'test'), '1/test')
        bucket.upload(UploadSourceBytes(b'test'), '1/2/test')
        bucket.upload(UploadSourceBytes(b'test'), '1/2/3/test')
        bucket.upload(UploadSourceBytes(b'test'), '1/2/3/4/test')
        bucket.upload(UploadSourceBytes(b'test'), '1/2/3/4/5/test')
        bucket.upload(UploadSourceBytes(b'test'), '1/2/3/4/5/6/test')
        bucket.upload(UploadSourceBytes(b'test'), '1/2/3/4/5/6/7/test')
        bucket.upload(UploadSourceBytes(b'test'), '1/2/3/4/5/6/7/8/test')
        bucket.upload(UploadSourceBytes(b'test'), '1/2/3/4/5/6/7/8/9/test')
        bucket.upload(UploadSourceBytes(b'check'), 'check')
        bucket.upload(UploadSourceBytes(b'check'), '1/check')
        bucket.upload(UploadSourceBytes(b'check'), '1/2/check')
        bucket.upload(UploadSourceBytes(b'check'), '1/2/3/check')
        bucket.upload(UploadSourceBytes(b'check'), '1/2/3/4/check')
        bucket.upload(UploadSourceBytes(b'check'), '1/2/3/4/5/check')
        bucket.upload(UploadSourceBytes(b'check'), '1/2/3/4/5/6/check')
        bucket.upload(UploadSourceBytes(b'check'), '1/2/3/4/5/6/7/check')
        bucket.upload(UploadSourceBytes(b'check'), '1/2/3/4/5/6/7/8/check')
        bucket.upload(UploadSourceBytes(b'check'), '1/2/3/4/5/6/7/8/9/check')

        # Now check the output of `bucket get` against the canon.
        expected_json = {
            'accountId': self.account_id,
            'bucketId': 'bucket_0',
            'bucketInfo': {},
            'bucketName': 'my-bucket',
            'bucketType': 'allPublic',
            'corsRules': [],
            'defaultServerSideEncryption': {'mode': 'none'},
            'fileCount': 20,
            'lifecycleRules': [],
            'options': [],
            'revision': 1,
            'totalSize': 90,
        }
        self._run_command(
            ['bucket', 'get', '--show-size', 'my-bucket'],
            expected_json_in_stdout=expected_json,
        )

    def test_get_bucket_with_hidden(self):
        self._authorize_account()
        self._create_my_bucket()

        # Put some files into the test bucket. Unroll the loop for convenience.
        bucket = self.b2_api.get_bucket_by_name('my-bucket')
        bucket.upload(UploadSourceBytes(b'test'), 'upload1')
        bucket.upload(UploadSourceBytes(b'test'), 'upload2')
        bucket.upload(UploadSourceBytes(b'test'), 'upload3')
        bucket.upload(UploadSourceBytes(b'test'), 'upload4')
        bucket.upload(UploadSourceBytes(b'test'), 'upload5')
        bucket.upload(UploadSourceBytes(b'test'), 'upload6')

        # Hide some new files. Don't check the results here; it will be clear enough that
        # something has failed if the output of 'bucket get' does not match the canon.
        stdout, stderr = self._get_stdouterr()
        console_tool = self.console_tool_class(stdout, stderr)
        console_tool.run_command(['b2', 'file', 'hide', 'b2://my-bucket/hidden1'])
        console_tool.run_command(['b2', 'file', 'hide', 'b2://my-bucket/hidden2'])
        console_tool.run_command(['b2', 'file', 'hide', 'b2://my-bucket/hidden3'])
        console_tool.run_command(['b2', 'file', 'hide', 'b2://my-bucket/hidden4'])

        # unhide one file
        console_tool.run_command(['b2', 'file', 'unhide', 'b2://my-bucket/hidden2'])

        # Now check the output of `bucket get` against the canon.
        expected_json = {
            'accountId': self.account_id,
            'bucketId': 'bucket_0',
            'bucketInfo': {},
            'bucketName': 'my-bucket',
            'bucketType': 'allPublic',
            'corsRules': [],
            'defaultServerSideEncryption': {'mode': 'none'},
            'fileCount': 9,
            'lifecycleRules': [],
            'options': [],
            'revision': 1,
            'totalSize': 24,
        }
        self._run_command(
            ['bucket', 'get', '--show-size', 'my-bucket'],
            expected_json_in_stdout=expected_json,
        )

    @pytest.mark.apiver(from_ver=4)
    def test_unhide_b2id(self):
        self._authorize_account()
        self._create_my_bucket()

        bucket = self.b2_api.get_bucket_by_name('my-bucket')
        stdout, stderr = self._get_stdouterr()
        console_tool = self.console_tool_class(stdout, stderr)

        file_version = bucket.upload(UploadSourceBytes(b'test'), 'test.txt')
        bucket.hide_file('test.txt')

        console_tool.run_command(['b2', 'file', 'unhide', f'b2id://{file_version.id_}'])

        expected_json = {
            'accountId': self.account_id,
            'bucketId': 'bucket_0',
            'bucketInfo': {},
            'bucketName': 'my-bucket',
            'bucketType': 'allPublic',
            'corsRules': [],
            'defaultServerSideEncryption': {'mode': 'none'},
            'fileCount': 1,
            'lifecycleRules': [],
            'options': [],
            'revision': 1,
            'totalSize': 4,
        }
        self._run_command(
            ['bucket', 'get', '--show-size', 'my-bucket'],
            expected_json_in_stdout=expected_json,
        )

    def test_get_bucket_complex(self):
        self._authorize_account()
        self._create_my_bucket()

        # Create a hierarchical structure within the test bucket. Unroll the loop here for
        # convenience.
        bucket = self.b2_api.get_bucket_by_name('my-bucket')
        bucket.upload(UploadSourceBytes(b'test'), 'test')
        bucket.upload(UploadSourceBytes(b'test'), 'test')
        bucket.upload(UploadSourceBytes(b'test'), '1/test')
        bucket.upload(UploadSourceBytes(b'test'), '1/test')
        bucket.upload(UploadSourceBytes(b'test'), '1/2/test')
        bucket.upload(UploadSourceBytes(b'test'), '1/2/test')
        bucket.upload(UploadSourceBytes(b'test'), '1/2/3/test')
        bucket.upload(UploadSourceBytes(b'test'), '1/2/3/test')
        bucket.upload(UploadSourceBytes(b'test'), '1/2/3/test')
        bucket.upload(UploadSourceBytes(b'test'), '1/2/3/test')
        bucket.upload(UploadSourceBytes(b'test'), '1/2/3/test')
        bucket.upload(UploadSourceBytes(b'check'), 'check')
        bucket.upload(UploadSourceBytes(b'check'), 'check')
        bucket.upload(UploadSourceBytes(b'check'), '1/check')
        bucket.upload(UploadSourceBytes(b'check'), '1/check')
        bucket.upload(UploadSourceBytes(b'check'), '1/2/check')
        bucket.upload(UploadSourceBytes(b'check'), '1/2/check')
        bucket.upload(UploadSourceBytes(b'check'), '1/2/check')
        bucket.upload(UploadSourceBytes(b'check'), '1/2/3/check')
        bucket.upload(UploadSourceBytes(b'check'), '1/2/3/4/check')
        bucket.upload(UploadSourceBytes(b'check'), '1/2/3/4/check')
        bucket.upload(UploadSourceBytes(b'check'), '1/2/3/4/check')

        # Hide some new files. Don't check the results here; it will be clear enough that
        # something has failed if the output of 'bucket get' does not match the canon.
        stdout, stderr = self._get_stdouterr()
        console_tool = self.console_tool_class(stdout, stderr)
        for _ in range(2):
            console_tool.run_command(['b2', 'file', 'hide', 'b2://my-bucket/hidden1'])
        console_tool.run_command(['b2', 'file', 'hide', 'b2://my-bucket/1/hidden2'])
        for _ in range(4):
            console_tool.run_command(['b2', 'file', 'hide', 'b2://my-bucket/1/2/hidden3'])

        # Unhide a file
        console_tool.run_command(['b2', 'file', 'unhide', 'b2://my-bucket/1/hidden2'])
        console_tool.run_command(['b2', 'file', 'unhide', 'b2://my-bucket/1/hidden2'])

        # Now check the output of `bucket get` against the canon.
        expected_json = {
            'accountId': self.account_id,
            'bucketId': 'bucket_0',
            'bucketInfo': {},
            'bucketName': 'my-bucket',
            'bucketType': 'allPublic',
            'corsRules': [],
            'defaultServerSideEncryption': {'mode': 'none'},
            'fileCount': 28,
            'lifecycleRules': [],
            'options': [],
            'revision': 1,
            'totalSize': 99,
        }
        self._run_command(
            ['bucket', 'get', '--show-size', 'my-bucket'],
            expected_json_in_stdout=expected_json,
        )

    def test_get_bucket_encrypted(self):
        self._authorize_account()
        self._run_command(
            [
                'bucket',
                'create',
                '--default-server-side-encryption=SSE-B2',
                '--default-server-side-encryption-algorithm=AES256',
                'my-bucket',
                'allPublic',
            ],
            'bucket_0\n',
            '',
            0,
        )
        expected_json = {
            'accountId': self.account_id,
            'bucketId': 'bucket_0',
            'bucketInfo': {},
            'bucketName': 'my-bucket',
            'bucketType': 'allPublic',
            'corsRules': [],
            'defaultServerSideEncryption': {'algorithm': 'AES256', 'mode': 'SSE-B2'},
            'fileCount': 0,
            'lifecycleRules': [],
            'options': [],
            'revision': 1,
            'totalSize': 0,
        }
        self._run_command(
            ['bucket', 'get', '--show-size', 'my-bucket'],
            expected_json_in_stdout=expected_json,
        )

    def test_non_existent_bucket(self):
        self._authorize_account()

        bucket_name = 'nonexistent'
        expected_stderr = f'Bucket not found: {bucket_name}. If you believe it exists, run `b2 bucket list` to reset cache, then try again.\n'

        self._run_command(
            ['bucket', 'get', bucket_name],
            expected_stderr=expected_stderr,
            expected_status=1,
        )

    def test_sync(self):
        self._authorize_account()
        self._create_my_bucket()

        with TempDir() as temp_dir:
            file_path = os.path.join(temp_dir, 'test.txt')
            with open(file_path, 'wb') as f:
                f.write(b'hello world')
            expected_stdout = """
            upload test.txt
            """

            command = ['sync', '--no-progress', temp_dir, 'b2://my-bucket']
            self._run_command(command, expected_stdout, '', 0)

    def test_sync_empty_folder_when_not_enabled(self):
        self._authorize_account()
        self._create_my_bucket()
        with TempDir() as temp_dir:
            command = ['sync', '--no-progress', temp_dir, 'b2://my-bucket']
            expected_stderr = (
                'ERROR: Directory %s is empty.  Use --allow-empty-source to sync anyway.\n'
                % fix_windows_path_limit(temp_dir.replace('\\\\', '\\'))
            )
            self._run_command(command, '', expected_stderr, 1)

    def test_sync_empty_folder_when_enabled(self):
        self._authorize_account()
        self._create_my_bucket()
        with TempDir() as temp_dir:
            command = ['sync', '--no-progress', '--allow-empty-source', temp_dir, 'b2://my-bucket']
            self._run_command(command, '', '', 0)

    def test_sync_dry_run(self):
        self._authorize_account()
        self._create_my_bucket()

        with TempDir() as temp_dir:
            temp_file = self._make_local_file(temp_dir, 'test-dry-run.txt')

            # dry-run
            expected_stdout = """
            upload test-dry-run.txt
            """
            command = ['sync', '--no-progress', '--dry-run', temp_dir, 'b2://my-bucket']
            self._run_command(command, expected_stdout, '', 0)

            # file should not have been uploaded
            expected_stdout = """
            []
            """
            self._run_command(
                ['ls', '--json', *self.b2_uri_args('my-bucket')], expected_stdout, '', 0
            )

            # upload file
            expected_stdout = """
            upload test-dry-run.txt
            """
            command = ['sync', '--no-progress', temp_dir, 'b2://my-bucket']
            self._run_command(command, expected_stdout, '', 0)

            # file should have been uploaded
            mtime = file_mod_time_millis(temp_file)
            expected_json = [
                {
                    'action': 'upload',
                    'contentSha1': '2aae6c35c94fcfb415dbe95f408b9ce91ee846ed',
                    'contentType': 'b2/x-auto',
                    'fileId': '9999',
                    'fileInfo': {'src_last_modified_millis': str(mtime)},
                    'fileName': 'test-dry-run.txt',
                    'serverSideEncryption': {'mode': 'none'},
                    'size': 11,
                    'uploadTimestamp': 5000,
                }
            ]
            self._run_command(
                ['ls', '--json', *self.b2_uri_args('my-bucket')],
                expected_json_in_stdout=expected_json,
            )

    def test_sync_exclude_all_symlinks(self):
        self._authorize_account()
        self._create_my_bucket()

        with TempDir() as temp_dir:
            self._make_local_file(temp_dir, 'test.txt')
            os.symlink('test.txt', os.path.join(temp_dir, 'alink'))
            expected_stdout = """
            upload test.txt
            """

            command = [
                'sync',
                '--no-progress',
                '--exclude-all-symlinks',
                temp_dir,
                'b2://my-bucket',
            ]
            self._run_command(command, expected_stdout, '', 0)

    def test_sync_dont_exclude_all_symlinks(self):
        self._authorize_account()
        self._create_my_bucket()

        with TempDir() as temp_dir:
            self._make_local_file(temp_dir, 'test.txt')
            os.symlink('test.txt', os.path.join(temp_dir, 'alink'))
            # Exact stdout cannot be asserted because line order is non-deterministic
            expected_part_of_stdout = """
            upload alink
            """

            command = ['sync', '--no-progress', temp_dir, 'b2://my-bucket']
            self._run_command(command, expected_part_of_stdout=expected_part_of_stdout)

    def test_sync_exclude_if_modified_after_in_range(self):
        self._authorize_account()
        self._create_my_bucket()

        with TempDir() as temp_dir:
            for file, mtime in (('test.txt', 1367900664.152), ('test2.txt', 1367600664.152)):
                self._make_local_file(temp_dir, file)
                path = os.path.join(temp_dir, file)
                os.utime(path, (mtime, mtime))

            expected_stdout = """
            upload test2.txt
            """

            command = [
                'sync',
                '--no-progress',
                '--exclude-if-modified-after',
                '1367700664.152',
                temp_dir,
                'b2://my-bucket',
            ]
            self._run_command(command, expected_stdout, '', 0)

    def test_sync_exclude_if_modified_after_exact(self):
        self._authorize_account()
        self._create_my_bucket()

        with TempDir() as temp_dir:
            for file, mtime in (('test.txt', 1367900664.152), ('test2.txt', 1367600664.152)):
                self._make_local_file(temp_dir, file)
                path = os.path.join(temp_dir, file)
                os.utime(path, (mtime, mtime))

            expected_stdout = """
            upload test2.txt
            """

            command = [
                'sync',
                '--no-progress',
                '--exclude-if-modified-after',
                '1367600664.152',
                temp_dir,
                'b2://my-bucket',
            ]
            self._run_command(command, expected_stdout, '', 0)

    def test_sync_exclude_if_uploaded_after_in_range(self):
        self._authorize_account()
        self._create_my_bucket()

        with TemporaryDirectory() as temp_dir:
            for file, utime in (('test.txt', 1367900664152), ('test2.txt', 1367600664152)):
                file_path = self._make_local_file(temp_dir, file)
                command = [
                    'file',
                    'upload',
                    '--no-progress',
                    '--custom-upload-timestamp',
                    str(utime),
                    'my-bucket',
                    file_path,
                    file,
                ]
                self._run_command(command, expected_status=0)

        with TemporaryDirectory() as temp_dir:
            command = [
                'sync',
                '--no-progress',
                '--exclude-if-uploaded-after',
                '1367700664.152',
                'b2://my-bucket',
                temp_dir,
            ]
            expected_stdout = """
            dnload test2.txt
            """
            self._run_command(command, expected_stdout, '', 0)

    def test_sync_exclude_if_uploaded_after_exact(self):
        self._authorize_account()
        self._create_my_bucket()

        with TemporaryDirectory() as temp_dir:
            for file, utime in (('test.txt', 1367900664152), ('test2.txt', 1367600664152)):
                file_path = self._make_local_file(temp_dir, file)
                command = [
                    'file',
                    'upload',
                    '--no-progress',
                    '--custom-upload-timestamp',
                    str(utime),
                    'my-bucket',
                    file_path,
                    file,
                ]
                self._run_command(command, expected_status=0)

        with TemporaryDirectory() as temp_dir:
            command = [
                'sync',
                '--no-progress',
                '--exclude-if-uploaded-after',
                '1367600664.152',
                'b2://my-bucket',
                temp_dir,
            ]
            expected_stdout = """
            dnload test2.txt
            """
            self._run_command(command, expected_stdout, '', 0)

    def _test_sync_threads(
        self,
        threads=None,
        sync_threads=None,
        download_threads=None,
        upload_threads=None,
    ):
        self._authorize_account()
        self._create_my_bucket()

        with TempDir() as temp_dir:
            self._make_local_file(temp_dir, 'file.txt')
            command = ['sync', '--no-progress']
            if threads is not None:
                command += ['--threads', str(threads)]
            if sync_threads is not None:
                command += ['--sync-threads', str(sync_threads)]
            if download_threads is not None:
                command += ['--download-threads', str(download_threads)]
            if upload_threads is not None:
                command += ['--upload-threads', str(upload_threads)]
            command += [temp_dir, 'b2://my-bucket']
            expected_stdout = """
            upload file.txt
            """
            self._run_command(command, expected_stdout)

    def test_sync_threads(self):
        self._test_sync_threads(threads=1)

    def test_sync_sync_threads(self):
        self._test_sync_threads(sync_threads=1)

    def test_sync_download_threads(self):
        self._test_sync_threads(download_threads=1)

    def test_sync_upload_threads(self):
        self._test_sync_threads(upload_threads=1)

    def test_sync_many_thread_options(self):
        self._test_sync_threads(sync_threads=1, download_threads=1, upload_threads=1)

    def test_sync_threads_and_upload_threads(self):
        # Using --threads is exclusive with other options
        with self.assertRaises(ValueError):
            self._test_sync_threads(threads=1, upload_threads=1)

    def test_sync_threads_and_sync_threads(self):
        # Using --threads is exclusive with other options
        with self.assertRaises(ValueError):
            self._test_sync_threads(threads=1, sync_threads=1)

    def test_sync_threads_and_download_threads(self):
        # Using --threads is exclusive with other options
        with self.assertRaises(ValueError):
            self._test_sync_threads(threads=1, download_threads=1)

    def test_sync_all_thread_options(self):
        # Using --threads is exclusive with other options
        with self.assertRaises(ValueError):
            self._test_sync_threads(threads=1, sync_threads=1, download_threads=1, upload_threads=1)

    def test_ls(self):
        self._authorize_account()
        self._create_my_bucket()

        # Check with no files
        self._run_command(['ls', *self.b2_uri_args('my-bucket')], '', '', 0)

        # Create some files, including files in a folder
        bucket = self.b2_api.get_bucket_by_name('my-bucket')
        bucket.upload(UploadSourceBytes(b''), 'a')
        bucket.upload(UploadSourceBytes(b' '), 'b/b1')
        bucket.upload(UploadSourceBytes(b'   '), 'b/b2')
        bucket.upload(UploadSourceBytes(b'     '), 'c')
        bucket.upload(UploadSourceBytes(b'      '), 'c')

        # Condensed output
        expected_stdout = """
        a
        b/
        c
        """
        self._run_command(['ls', *self.b2_uri_args('my-bucket')], expected_stdout, '', 0)

        # Recursive output
        expected_stdout = """
        a
        b/b1
        b/b2
        c
        """
        self._run_command(
            ['ls', '--recursive', *self.b2_uri_args('my-bucket')], expected_stdout, '', 0
        )
        self._run_command(['ls', '-r', *self.b2_uri_args('my-bucket')], expected_stdout, '', 0)

        # Check long output.   (The format expects full-length file ids, so it causes whitespace here)
        expected_stdout = """
                                                                                       9999  upload  1970-01-01  00:00:05          0  a
                                                                                          -       -           -         -          0  b/
                                                                                       9995  upload  1970-01-01  00:00:05          6  c
        """
        self._run_command(['ls', '--long', *self.b2_uri_args('my-bucket')], expected_stdout, '', 0)

        # Check long versions output   (The format expects full-length file ids, so it causes whitespace here)
        expected_stdout = """
                                                                                       9999  upload  1970-01-01  00:00:05          0  a
                                                                                          -       -           -         -          0  b/
                                                                                       9995  upload  1970-01-01  00:00:05          6  c
                                                                                       9996  upload  1970-01-01  00:00:05          5  c
        """
        self._run_command(
            ['ls', '--long', '--versions', *self.b2_uri_args('my-bucket')], expected_stdout, '', 0
        )

    def test_ls_wildcard(self):
        self._authorize_account()
        self._create_my_bucket()

        # Check with no files
        self._run_command(
            ['ls', '--recursive', '--with-wildcard', *self.b2_uri_args('my-bucket', '*.txt')],
            '',
            '',
            0,
        )

        # Create some files, including files in a folder
        bucket = self.b2_api.get_bucket_by_name('my-bucket')
        self._upload_multiple_files(bucket)

        expected_stdout = """
        a/test.csv
        a/test.tsv
        b/b/test.csv
        b/b1/test.csv
        b/b2/test.tsv
        c/test.csv
        c/test.tsv
        """
        self._run_command(
            ['ls', '--recursive', '--with-wildcard', *self.b2_uri_args('my-bucket', '*.[tc]sv')],
            expected_stdout,
        )

        expected_stdout = """
        a/test.tsv
        b/b2/test.tsv
        c/test.tsv
        """
        self._run_command(
            ['ls', '--recursive', '--with-wildcard', *self.b2_uri_args('my-bucket', '*.tsv')],
            expected_stdout,
        )

        expected_stdout = """
        b/b1/test.csv
        """
        self._run_command(
            [
                'ls',
                '--recursive',
                '--with-wildcard',
                *self.b2_uri_args('my-bucket', 'b/b?/test.csv'),
            ],
            expected_stdout,
        )

        expected_stdout = """
        a/test.csv
        a/test.tsv
        c/test.csv
        c/test.tsv
        """
        self._run_command(
            ['ls', '--recursive', '--with-wildcard', *self.b2_uri_args('my-bucket', '?/test.?sv')],
            expected_stdout,
        )

        expected_stdout = """
        b/b/test.csv
        b/b1/test.csv
        """
        self._run_command(
            [
                'ls',
                '--recursive',
                '--with-wildcard',
                *self.b2_uri_args('my-bucket', '?/*/*.[!t]sv'),
            ],
            expected_stdout,
        )

    def test_ls_with_wildcard_no_recursive(self):
        self._authorize_account()
        self._create_my_bucket()

        # Check with no files
        self._run_command(
            ['ls', '--with-wildcard', *self.b2_uri_args('my-bucket')],
            expected_stderr='ERROR: with_wildcard requires recursive to be turned on as well\n',
            expected_status=1,
        )

    def test_restrictions(self):
        # Initial condition
        self.assertEqual(None, self.account_info.get_account_auth_token())

        # Authorize an account with the master key.
        account_id = self.account_id
        self._run_command_ignore_output(['account', 'authorize', account_id, self.master_key])

        # Create a bucket to use
        bucket_name = 'restrictedBucket'
        bucket_id = 'bucket_0'
        self._run_command(['bucket', 'create', bucket_name, 'allPrivate'], bucket_id + '\n', '', 0)

        # Create another bucket
        other_bucket_name = 'otherBucket'
        self._run_command_ignore_output(['bucket', 'create', other_bucket_name, 'allPrivate'])

        # Create a key restricted to a bucket
        app_key_id = 'appKeyId0'
        app_key = 'appKey0'
        capabilities = 'listBuckets,readFiles'
        file_prefix = 'some/file/prefix/'
        self._run_command(
            [
                'key',
                'create',
                '--bucket',
                bucket_name,
                '--name-prefix',
                file_prefix,
                'my-key',
                capabilities,
            ],
            app_key_id + ' ' + app_key + '\n',
            '',
            0,
        )

        self._run_command_ignore_output(['account', 'authorize', app_key_id, app_key])

        # Auth token should be in account info now
        self.assertEqual('auth_token_1', self.account_info.get_account_auth_token())

        # Assertions that the restrictions not only are saved but what they are supposed to be
        self.assertEqual(
            dict(
                buckets=[{'id': bucket_id, 'name': bucket_name}],
                capabilities=[
                    'listBuckets',
                    'readFiles',
                ],
                namePrefix=file_prefix,
            ),
            self.account_info.get_allowed(),
        )

        # Test that the application key info gets added to the unauthorized error message.
        expected_create_key_stderr = (
            'ERROR: unauthorized for application key '
            "with capabilities 'listBuckets,readFiles', "
            "restricted to buckets ['restrictedBucket'], "
            "restricted to files that start with 'some/file/prefix/' (unauthorized)\n"
        )
        self._run_command(
            ['key', 'create', 'goodKeyName-One', 'readFiles,listBuckets'],
            '',
            expected_create_key_stderr,
            1,
        )

    def test_list_buckets_not_allowed_for_app_key(self):
        # Create a bucket and a key restricted to that bucket.
        self._authorize_account()
        self._run_command(
            ['bucket', 'create', 'my-bucket', 'allPrivate'],
            'bucket_0\n',
            '',
            0,
        )

        # Authorizing with the key will fail because the ConsoleTool needs
        # to be able to look up the name of the bucket.
        self._run_command(
            ['key', 'create', 'my-key', 'listFiles'],
            'appKeyId0 appKey0\n',
            '',
            0,
        )

        # Authorize with the key, which should result in an error.
        self._run_command(
            ['account', 'authorize', 'appKeyId0', 'appKey0'],
            '',
            'ERROR: application key has no listBuckets capability, which is required for the b2 command-line tool\n',
            1,
        )

    def test_bucket_missing_for_bucket_key(self):
        # Create a bucket and a key restricted to that bucket.
        self._authorize_account()
        self._run_command(
            ['bucket', 'create', 'my-bucket', 'allPrivate'],
            'bucket_0\n',
            '',
            0,
        )
        self._run_command(
            ['key', 'create', '--bucket', 'my-bucket', 'my-key', 'listBuckets,listFiles'],
            'appKeyId0 appKey0\n',
            '',
            0,
        )

        # Get rid of the bucket, leaving the key with a dangling pointer to it.
        self._run_command_ignore_output(['bucket', 'delete', 'my-bucket'])

        # Authorizing with the key will fail because the ConsoleTool needs
        # to be able to look up the name of the bucket.
        self._run_command(
            ['account', 'authorize', 'appKeyId0', 'appKey0'],
            '',
            "ERROR: unable to authorize account: Application key is restricted to a bucket that doesn't exist\n",
            1,
        )

    def test_ls_for_restricted_bucket(self):
        # Create a bucket and a key restricted to that bucket.
        self._authorize_account()
        self._run_command(
            ['bucket', 'create', 'my-bucket', 'allPrivate'],
            'bucket_0\n',
            '',
            0,
        )
        self._run_command(
            ['key', 'create', '--bucket', 'my-bucket', 'my-key', 'listBuckets,listFiles'],
            'appKeyId0 appKey0\n',
            '',
            0,
        )

        # Authorize with the key and list the files
        self._run_command_ignore_output(
            ['account', 'authorize', 'appKeyId0', 'appKey0'],
        )
        self._run_command(
            ['ls', *self.b2_uri_args('my-bucket')],
            '',
            '',
            0,
        )

    def test_bad_terminal(self):
        stdout = mock.MagicMock()
        stdout.write = mock.MagicMock(
            side_effect=[
                UnicodeEncodeError('codec', 'foo', 100, 105, 'artificial UnicodeEncodeError')
            ]
            + list(range(25))
        )
        stderr = mock.MagicMock()
        console_tool = self.console_tool_class(stdout, stderr)
        console_tool.run_command(['b2', 'account', 'authorize', self.account_id, self.master_key])

    def test_passing_api_parameters(self):
        self._authorize_account()
        commands = [
            [
                'b2',
                'download-file-by-name',
                '--profile',
                'nonexistent',
                'dummy-name',
                'dummy-file-name',
                'dummy-local-file-name',
            ],
            [
                'b2',
                'download-file-by-id',
                '--profile',
                'nonexistent',
                'dummy-id',
                'dummy-local-file-name',
            ],
            ['b2', 'sync', '--profile', 'nonexistent', 'b2:dummy-source', 'dummy-destination'],
        ]
        parameters = [
            {
                '--write-buffer-size': 123,
                '--skip-hash-verification': None,
                '--max-download-streams-per-file': 8,
            },
            {
                '--write-buffer-size': 321,
                '--max-download-streams-per-file': 7,
            },
        ]
        for command, params in product(commands, parameters):
            console_tool = self.console_tool_class(
                mock.MagicMock(),
                mock.MagicMock(),
            )

            args = [str(val) for val in chain.from_iterable(params.items()) if val]
            console_tool.run_command(command + args)

            download_manager = console_tool.api.services.download_manager
            assert download_manager.write_buffer_size == params['--write-buffer-size']
            assert download_manager.check_hash is ('--skip-hash-verification' not in params)

            parallel_strategy = one(
                strategy
                for strategy in download_manager.strategies
                if isinstance(strategy, download_manager.PARALLEL_DOWNLOADER_CLASS)
            )
            assert parallel_strategy.max_streams == params['--max-download-streams-per-file']

    def test_passing_api_parameters_with_auth_env_vars(self):
        os.environ[B2_APPLICATION_KEY_ID_ENV_VAR] = self.account_id
        os.environ[B2_APPLICATION_KEY_ENV_VAR] = self.master_key

        command = [
            'b2',
            'download-file-by-id',
            'dummy-id',
            'dummy-local-file-name',
            '--write-buffer-size',
            '123',
            '--max-download-streams-per-file',
            '5',
            '--skip-hash-verification',
        ]

        console_tool = self.console_tool_class(
            mock.MagicMock(),
            mock.MagicMock(),
        )
        console_tool.run_command(command)

        download_manager = console_tool.api.services.download_manager
        assert download_manager.write_buffer_size == 123
        assert download_manager.check_hash is False

        parallel_strategy = one(
            strategy
            for strategy in download_manager.strategies
            if isinstance(strategy, download_manager.PARALLEL_DOWNLOADER_CLASS)
        )
        assert parallel_strategy.max_streams == 5

    @pytest.mark.apiver(from_ver=4)
    def test_ls_b2id(self):
        self._authorize_account()
        self._create_my_bucket()

        # Create a file
        bucket = self.b2_api.get_bucket_by_name('my-bucket')
        file_version = bucket.upload(UploadSourceBytes(b''), 'test.txt')

        # Condensed output
        expected_stdout = """
                test.txt
                """
        self._run_command(['ls', f'b2id://{file_version.id_}'], expected_stdout, '', 0)

    def test_ls_filters(self):
        self._authorize_account()
        self._create_my_bucket()

        # Create some files, including files in a folder
        bucket = self.b2_api.get_bucket_by_name('my-bucket')
        data = UploadSourceBytes(b'test-data')
        bucket.upload(data, 'a/test.csv')
        bucket.upload(data, 'a/test.tsv')
        bucket.upload(data, 'b/b/test.csv')
        bucket.upload(data, 'c/test.csv')
        bucket.upload(data, 'c/test.tsv')
        bucket.upload(data, 'test.csv')
        bucket.upload(data, 'test.tsv')

        expected_stdout = """
            a/
            b/
            c/
            test.csv
            """
        self._run_command(
            ['ls', *self.b2_uri_args('my-bucket'), '--include', '*.csv'],
            expected_stdout,
        )
        self._run_command(
            ['ls', *self.b2_uri_args('my-bucket'), '--exclude', '*.tsv'],
            expected_stdout,
        )

        expected_stdout = """
            a/test.csv
            b/b/test.csv
            c/test.csv
            test.csv
            """
        self._run_command(
            ['ls', *self.b2_uri_args('my-bucket'), '--recursive', '--include', '*.csv'],
            expected_stdout,
        )
        self._run_command(
            ['ls', *self.b2_uri_args('my-bucket'), '--recursive', '--exclude', '*.tsv'],
            expected_stdout,
        )

        expected_stdout = """
            b/b/test.csv
            c/test.csv
            test.csv
            """
        self._run_command(
            [
                'ls',
                *self.b2_uri_args('my-bucket'),
                '--recursive',
                '--exclude',
                '*',
                '--include',
                '*.csv',
                '--exclude',
                'a/*',
            ],
            expected_stdout,
        )

    @pytest.mark.skip('temporarily disabled')
    @skip_on_windows
    def test_escape_c0_char_on_sync_stack_trace(self):
        self._authorize_account()
        self._run_command(['bucket', 'create', 'my-bucket-0', 'allPrivate'], 'bucket_0\n', '', 0)
        self._run_command(['bucket', 'create', 'my-bucket-1', 'allPrivate'], 'bucket_1\n', '', 0)

        with TempDir() as temp_dir:
            _ = self._make_local_file(temp_dir, '\x1b[32mC\x1b[33mC\x1b[34mI\x1b[0m')
            self._run_command(
                [
                    'sync',
                    '--no-progress',
                    '--no-escape-control-characters',
                    temp_dir,
                    'b2://my-bucket-0',
                ],
                expected_part_of_stdout='\\x1b[32m',
                expected_status=0,
            )
            self._run_command(
                [
                    'sync',
                    '--no-progress',
                    '--escape-control-characters',
                    temp_dir,
                    'b2://my-bucket-1',
                ],
                expected_part_of_stdout="upload '\\x1b[32mC\\x1b[33mC\\x1b[34mI\\x1b[0m'\n",
                expected_status=0,
                unexpected_part_of_stdout='\x1b[32m',
            )

    def test_escape_c0_char_on_key_restricted_path(self):
        self._authorize_account()
        self._run_command(['bucket', 'create', 'my-bucket-0', 'allPublic'], 'bucket_0\n', '', 0)
        cc_name = "$'\x1b[31mC\x1b[32mC\x1b[33mI\x1b[0m'"
        escaped_error = "ERROR: unauthorized for application key with capabilities 'listBuckets,listKeys', restricted to buckets ['my-bucket-0'], restricted to files that start with '$'\\x1b[31mC\\x1b[32mC\\x1b[33mI\\x1b[0m'' (unauthorized)\n"

        # Create a key
        self._run_command(
            [
                'key',
                'create',
                '--bucket',
                'my-bucket-0',
                '--name-prefix',
                cc_name,
                'key1',
                'listBuckets,listKeys',
            ],
            'appKeyId0 appKey0\n',
            expected_status=0,
        )

        # Authorize with the key
        self._run_command(['account', 'authorize', 'appKeyId0', 'appKey0'], expected_status=0)

        self._run_command(
            ['ls', *self.b2_uri_args('my-bucket-0'), '--no-escape-control-characters'],
            expected_status=1,
            expected_stderr=escaped_error,
        )

        self._run_command(
            ['ls', *self.b2_uri_args('my-bucket-0'), '--escape-control-characters'],
            expected_status=1,
            expected_stderr=escaped_error,
        )

        self._run_command(
            ['ls', *self.b2_uri_args('my-bucket-0')],
            expected_status=1,
            expected_stderr=escaped_error,
        )

    def test_escape_c1_char_on_ls_long(self):
        self._authorize_account()
        self._run_command(['bucket', 'create', 'my-bucket-0', 'allPrivate'], 'bucket_0\n', '', 0)

        with TempDir() as temp_dir:
            local_file = self._make_local_file(temp_dir, 'file1.txt')
            cc_filename = '\u009bT\u009bE\u009bS\u009bTtest.txt'
            escaped_cc_filename = '\\x9bT\\x9bE\\x9bS\\x9bTtest.txt'

            self._run_command(
                ['file', 'upload', '--no-progress', 'my-bucket-0', local_file, cc_filename]
            )

        self._run_command(
            ['ls', '--long', '--no-escape-control-characters', *self.b2_uri_args('my-bucket-0')],
            expected_part_of_stdout=cc_filename,
        )

        self._run_command(
            ['ls', '--long', *self.b2_uri_args('my-bucket-0')], expected_part_of_stdout=cc_filename
        )

        self._run_command(
            ['ls', '--long', '--escape-control-characters', *self.b2_uri_args('my-bucket-0')],
            expected_part_of_stdout=escaped_cc_filename,
            unexpected_part_of_stdout=cc_filename,
        )

    def test_escape_c1_char_ls(self):
        self._authorize_account()
        self._run_command(['bucket', 'create', 'my-bucket-cc', 'allPrivate'], 'bucket_0\n', '', 0)

        with TempDir() as temp_dir:
            local_file = self._make_local_file(temp_dir, 'x')
            bad_str = '\u009b2K\u009b7Gb\u009b24Gx\u009b4GH'
            escaped_bad_str = '\\x9b2K\\x9b7Gb\\x9b24Gx\\x9b4GH'

            self._run_command(
                ['file', 'upload', '--no-progress', 'my-bucket-cc', local_file, bad_str]
            )

            self._run_command(
                ['file', 'upload', '--no-progress', 'my-bucket-cc', local_file, 'some_normal_text']
            )

            self._run_command(
                ['ls', *self.b2_uri_args('my-bucket-cc'), '--no-escape-control-characters'],
                expected_part_of_stdout=bad_str,
            )

            self._run_command(
                ['ls', *self.b2_uri_args('my-bucket-cc')], expected_part_of_stdout=bad_str
            )

            self._run_command(
                ['ls', *self.b2_uri_args('my-bucket-cc'), '--escape-control-characters'],
                expected_part_of_stdout=escaped_bad_str,
            )


class TestConsoleToolWithBucket(BaseConsoleToolTest):
    """These tests create a bucket during setUp before running CLI commands"""

    def setUp(self):
        super().setUp()
        self._authorize_account()
        self._create_my_bucket()

    def test_cancel_large_file(self):
        file = self.b2_api.services.large_file.start_large_file(
            'bucket_0', 'file1', 'text/plain', {}
        )

        self._run_command(
            ['file', 'large', 'unfinished', 'cancel', f'b2id://{file.file_id}'],
            '9999 canceled\n',
            '',
            0,
        )

    def test_cancel_large_file_deprecated(self):
        file = self.b2_api.services.large_file.start_large_file(
            'bucket_0', 'file1', 'text/plain', {}
        )

        self._run_command(
            ['cancel-large-file', file.file_id],
            '9999 canceled\n',
            'WARNING: `cancel-large-file` command is deprecated. Use `file large unfinished cancel` instead.\n',
            0,
        )

    def test_cancel_all_large_file(self):
        self.b2_api.services.large_file.start_large_file('bucket_0', 'file1', 'text/plain', {})
        self.b2_api.services.large_file.start_large_file('bucket_0', 'file2', 'text/plain', {})

        expected_stdout = """
        9999 canceled
        9998 canceled
        """

        self._run_command(
            ['file', 'large', 'unfinished', 'cancel', 'b2://my-bucket'], expected_stdout, '', 0
        )

    def test_cancel_all_large_file_deprecated(self):
        self.b2_api.services.large_file.start_large_file('bucket_0', 'file1', 'text/plain', {})
        self.b2_api.services.large_file.start_large_file('bucket_0', 'file2', 'text/plain', {})
        expected_stdout = """
        9999 canceled
        9998 canceled
        """

        self._run_command(
            ['cancel-all-unfinished-large-files', 'my-bucket'],
            expected_stdout,
            'WARNING: `cancel-all-unfinished-large-files` command is deprecated. Use `file large unfinished cancel` instead.\n',
            0,
        )

    def test_list_parts_with_none(self):
        file = self.b2_api.services.large_file.start_large_file(
            'bucket_0', 'file1', 'text/plain', {}
        )
        self._run_command(['file', 'large', 'parts', f'b2id://{file.file_id}'], '', '', 0)

    def test_list_parts_with_none_deprecated(self):
        file = self.b2_api.services.large_file.start_large_file(
            'bucket_0', 'file1', 'text/plain', {}
        )
        self._run_command(
            ['list-parts', file.file_id],
            '',
            'WARNING: `list-parts` command is deprecated. Use `file large parts` instead.\n',
            0,
        )

    def test_list_parts_with_parts(self):
        bucket = self.b2_api.get_bucket_by_name('my-bucket')
        file = self.b2_api.services.large_file.start_large_file(
            'bucket_0', 'file', 'text/plain', {}
        )
        content = b'hello world'
        large_file_upload_state = mock.MagicMock()
        large_file_upload_state.has_error.return_value = False
        bucket.api.services.upload_manager._upload_part(
            bucket.id_,
            file.file_id,
            UploadSourceBytes(content),
            1,
            large_file_upload_state,
            None,
            None,
        )
        bucket.api.services.upload_manager._upload_part(
            bucket.id_,
            file.file_id,
            UploadSourceBytes(content),
            3,
            large_file_upload_state,
            None,
            None,
        )
        expected_stdout = """
            1         11  2aae6c35c94fcfb415dbe95f408b9ce91ee846ed
            3         11  2aae6c35c94fcfb415dbe95f408b9ce91ee846ed
        """

        self._run_command(
            ['file', 'large', 'parts', f'b2id://{file.file_id}'], expected_stdout, '', 0
        )

    def test_list_parts_with_parts_deprecated(self):
        bucket = self.b2_api.get_bucket_by_name('my-bucket')
        file = self.b2_api.services.large_file.start_large_file(
            'bucket_0', 'file', 'text/plain', {}
        )
        content = b'hello world'
        large_file_upload_state = mock.MagicMock()
        large_file_upload_state.has_error.return_value = False
        bucket.api.services.upload_manager._upload_part(
            bucket.id_,
            file.file_id,
            UploadSourceBytes(content),
            1,
            large_file_upload_state,
            None,
            None,
        )
        bucket.api.services.upload_manager._upload_part(
            bucket.id_,
            file.file_id,
            UploadSourceBytes(content),
            3,
            large_file_upload_state,
            None,
            None,
        )
        expected_stdout = """
            1         11  2aae6c35c94fcfb415dbe95f408b9ce91ee846ed
            3         11  2aae6c35c94fcfb415dbe95f408b9ce91ee846ed
        """

        self._run_command(
            ['list-parts', file.file_id],
            expected_stdout,
            'WARNING: `list-parts` command is deprecated. Use `file large parts` instead.\n',
            0,
        )

    def test_list_unfinished_large_files_with_some(self):
        api_url = self.account_info.get_api_url()
        auth_token = self.account_info.get_account_auth_token()
        self.raw_api.start_large_file(api_url, auth_token, 'bucket_0', 'file1', 'text/plain', {})
        self.raw_api.start_large_file(
            api_url, auth_token, 'bucket_0', 'file2', 'text/plain', {'color': 'blue'}
        )
        self.raw_api.start_large_file(
            api_url, auth_token, 'bucket_0', 'file3', 'application/json', {}
        )
        expected_stdout = """
        9999 file1 text/plain
        9998 file2 text/plain color=blue
        9997 file3 application/json
        """

        self._run_command(
            ['file', 'large', 'unfinished', 'list', 'b2://my-bucket'], expected_stdout, '', 0
        )

    def test_list_unfinished_large_files_with_some_deprecated(self):
        api_url = self.account_info.get_api_url()
        auth_token = self.account_info.get_account_auth_token()
        self.raw_api.start_large_file(api_url, auth_token, 'bucket_0', 'file1', 'text/plain', {})
        self.raw_api.start_large_file(
            api_url, auth_token, 'bucket_0', 'file2', 'text/plain', {'color': 'blue'}
        )
        self.raw_api.start_large_file(
            api_url, auth_token, 'bucket_0', 'file3', 'application/json', {}
        )
        expected_stdout = """
        9999 file1 text/plain
        9998 file2 text/plain color=blue
        9997 file3 application/json
        """

        self._run_command(
            ['list-unfinished-large-files', 'my-bucket'],
            expected_stdout,
            'WARNING: `list-unfinished-large-files` command is deprecated. Use `file large unfinished list` instead.\n',
            0,
        )


class TestRmConsoleTool(BaseConsoleToolTest):
    """
    These tests replace default progress reporter of Rm class
    to ensure that it reports everything as fast as possible.
    """

    class InstantReporter(ProgressReport):
        UPDATE_INTERVAL = 0.0

    @classmethod
    def setUpClass(cls) -> None:
        cls.original_v3_progress_class = v3Rm.PROGRESS_REPORT_CLASS
        cls.original_v4_progress_class = v4Rm.PROGRESS_REPORT_CLASS

        v3Rm.PROGRESS_REPORT_CLASS = cls.InstantReporter
        v4Rm.PROGRESS_REPORT_CLASS = cls.InstantReporter

    def setUp(self):
        super().setUp()

        self._authorize_account()
        self._create_my_bucket()

        self.bucket = self.b2_api.get_bucket_by_name('my-bucket')
        self._upload_multiple_files(self.bucket)

    @classmethod
    def tearDownClass(cls) -> None:
        v3Rm.PROGRESS_REPORT_CLASS = cls.original_v3_progress_class
        v4Rm.PROGRESS_REPORT_CLASS = cls.original_v4_progress_class

    def test_rm_wildcard(self):
        self._run_command(
            [
                'rm',
                '--recursive',
                '--with-wildcard',
                '--no-progress',
                *self.b2_uri_args('my-bucket', '*.csv'),
            ],
        )

        expected_stdout = """
        a/test.tsv
        b/b2/test.tsv
        b/test.txt
        c/test.tsv
        """
        self._run_command(['ls', '--recursive', *self.b2_uri_args('my-bucket')], expected_stdout)

    def test_rm_versions(self):
        # Uploading content of the bucket again to create second version of each file.
        self._upload_multiple_files(self.bucket)

        self._run_command(
            [
                'rm',
                '--versions',
                '--recursive',
                '--with-wildcard',
                *self.b2_uri_args('my-bucket', '*.csv'),
            ],
        )

        expected_stdout = """
        a/test.tsv
        a/test.tsv
        b/b2/test.tsv
        b/b2/test.tsv
        b/test.txt
        b/test.txt
        c/test.tsv
        c/test.tsv
        """
        self._run_command(
            ['ls', '--versions', '--recursive', *self.b2_uri_args('my-bucket')], expected_stdout
        )

    def test_rm_no_recursive(self):
        self._run_command(['rm', '--no-progress', *self.b2_uri_args('my-bucket', 'b/')])

        expected_stdout = """
        a/test.csv
        a/test.tsv
        b/b/test.csv
        b/b1/test.csv
        b/b2/test.tsv
        c/test.csv
        c/test.tsv
        """
        self._run_command(['ls', '--recursive', *self.b2_uri_args('my-bucket')], expected_stdout)

    def test_rm_dry_run(self):
        expected_stdout = """
        a/test.csv
        b/b/test.csv
        b/b1/test.csv
        c/test.csv
        """
        self._run_command(
            [
                'rm',
                '--recursive',
                '--with-wildcard',
                '--dry-run',
                *self.b2_uri_args('my-bucket', '*.csv'),
            ],
            expected_stdout,
        )

        expected_stdout = """
        a/test.csv
        a/test.tsv
        b/b/test.csv
        b/b1/test.csv
        b/b2/test.tsv
        b/test.txt
        c/test.csv
        c/test.tsv
        """
        self._run_command(['ls', '--recursive', *self.b2_uri_args('my-bucket')], expected_stdout)

    def test_rm_exact_filename(self):
        self._run_command(
            [
                'rm',
                '--recursive',
                '--with-wildcard',
                '--no-progress',
                *self.b2_uri_args('my-bucket', 'b/b/test.csv'),
            ],
        )

        expected_stdout = """
        a/test.csv
        a/test.tsv
        b/b1/test.csv
        b/b2/test.tsv
        b/test.txt
        c/test.csv
        c/test.tsv
        """
        self._run_command(['ls', '--recursive', *self.b2_uri_args('my-bucket')], expected_stdout)

    def test_rm_no_name_removes_everything(self):
        self._run_command(['rm', '--recursive', '--no-progress', *self.b2_uri_args('my-bucket')])
        self._run_command(['ls', '--recursive', *self.b2_uri_args('my-bucket')], '')

    def test_rm_with_wildcard_without_recursive(self):
        self._run_command(
            ['rm', '--with-wildcard', *self.b2_uri_args('my-bucket')],
            expected_stderr='ERROR: with_wildcard requires recursive to be turned on as well\n',
            expected_status=1,
        )

    def test_rm_queue_size_and_number_of_threads(self):
        self._run_command(
            [
                'rm',
                '--recursive',
                '--threads',
                '2',
                '--queue-size',
                '4',
                *self.b2_uri_args('my-bucket'),
            ]
        )
        self._run_command(['ls', '--recursive', *self.b2_uri_args('my-bucket')], '')

    def test_rm_progress(self):
        expected_in_stdout = ' count: 4/4 '
        self._run_command(
            ['rm', '--recursive', '--with-wildcard', *self.b2_uri_args('my-bucket', '*.csv')],
            expected_part_of_stdout=expected_in_stdout,
        )

        expected_stdout = """
        a/test.tsv
        b/b2/test.tsv
        b/test.txt
        c/test.tsv
        """
        self._run_command(['ls', '--recursive', *self.b2_uri_args('my-bucket')], expected_stdout)

    def _run_problematic_removal(
        self,
        additional_parameters: Optional[list[str]] = None,
        expected_in_stdout: Optional[str] = None,
        unexpected_in_stdout: Optional[str] = None,
    ):
        additional_parameters = additional_parameters or []

        original_delete_file_version = self.b2_api.raw_api.delete_file_version

        def mocked_delete_file_version(
            this, account_auth_token, file_id, file_name, bypass_governance=False, *args, **kwargs
        ):
            if file_name == 'b/b1/test.csv':
                raise Conflict()
            return original_delete_file_version(
                this, account_auth_token, file_id, file_name, bypass_governance, *args, **kwargs
            )

        with mock.patch.object(
            self.b2_api.raw_api,
            'delete_file_version',
            side_effect=mocked_delete_file_version,
        ):
            self._run_command(
                [
                    'rm',
                    '--recursive',
                    '--with-wildcard',
                    '--threads',
                    '1',
                    '--queue-size',
                    '1',
                    *additional_parameters,
                    *self.b2_uri_args('my-bucket', '*'),
                ],
                expected_status=1,
                expected_part_of_stdout=expected_in_stdout,
                unexpected_part_of_stdout=unexpected_in_stdout,
            )

    def test_rm_fail_fast(self):
        # Since we already have all the jobs submitted to another thread,
        # we can only rely on the log to tell when it stopped.
        expected_in_stdout = """
        Deletion of file "b/b1/test.csv" (9996) failed: Conflict:
         count: 3/4"""
        unexpected_in_stdout = ' count: 5/5 '
        self._run_problematic_removal(['--fail-fast'], expected_in_stdout, unexpected_in_stdout)

    def test_rm_skipping_over_errors(self):
        self._run_problematic_removal()

        expected_stdout = """
        b/b1/test.csv
        """
        self._run_command(['ls', '--recursive', *self.b2_uri_args('my-bucket')], expected_stdout)

    @pytest.mark.apiver(from_ver=4)
    def test_rm_b2id(self):
        # Create a file
        file_version = self.bucket.upload(UploadSourceBytes(b''), 'new-file.txt')

        # Before deleting
        expected_stdout = """
        a/test.csv
        a/test.tsv
        b/b/test.csv
        b/b1/test.csv
        b/b2/test.tsv
        b/test.txt
        c/test.csv
        c/test.tsv
        new-file.txt
        """
        self._run_command(['ls', '--recursive', 'b2://my-bucket'], expected_stdout)

        # Delete file
        self._run_command(['rm', '--no-progress', f'b2id://{file_version.id_}'], '', '', 0)

        # After deleting
        expected_stdout = """
        a/test.csv
        a/test.tsv
        b/b/test.csv
        b/b1/test.csv
        b/b2/test.tsv
        b/test.txt
        c/test.csv
        c/test.tsv
        """
        self._run_command(['ls', '--recursive', 'b2://my-bucket'], expected_stdout)

    def rm_filters_helper(self, rm_args: list[str], expected_ls_stdout: str):
        self._authorize_account()
        self._run_command(['bucket', 'create', 'my-rm-bucket', 'allPublic'], 'bucket_1\n', '', 0)
        bucket = self.b2_api.get_bucket_by_name('my-rm-bucket')

        # Create some files, including files in a folder
        data = UploadSourceBytes(b'test-data')
        bucket.upload(data, 'a/test.csv')
        bucket.upload(data, 'a/test.tsv')
        bucket.upload(data, 'b/b/test.csv')
        bucket.upload(data, 'c/test.csv')
        bucket.upload(data, 'c/test.tsv')
        bucket.upload(data, 'test.csv')
        bucket.upload(data, 'test.tsv')
        bucket.upload(data, 'test.txt')

        self._run_command(
            ['rm', '--no-progress', *self.b2_uri_args('my-rm-bucket'), *rm_args], '', '', 0
        )
        self._run_command(
            ['ls', *self.b2_uri_args('my-rm-bucket'), '--recursive'],
            expected_ls_stdout,
        )

    def test_rm_filters_include(self):
        expected_ls_stdout = """
            a/test.csv
            a/test.tsv
            b/b/test.csv
            c/test.csv
            c/test.tsv
            test.tsv
            test.txt
            """
        self.rm_filters_helper(['--include', '*.csv'], expected_ls_stdout)

    def test_rm_filters_exclude(self):
        expected_ls_stdout = """
            a/test.csv
            a/test.tsv
            b/b/test.csv
            c/test.csv
            c/test.tsv
            test.csv
            """
        self.rm_filters_helper(['--exclude', '*.csv'], expected_ls_stdout)

    def test_rm_filters_include_recursive(self):
        expected_ls_stdout = """
            a/test.tsv
            c/test.tsv
            test.tsv
            test.txt
            """
        self.rm_filters_helper(['--recursive', '--include', '*.csv'], expected_ls_stdout)

    def test_rm_filters_exclude_recursive(self):
        expected_ls_stdout = """
            a/test.csv
            b/b/test.csv
            c/test.csv
            test.csv
            """
        self.rm_filters_helper(['--recursive', '--exclude', '*.csv'], expected_ls_stdout)

    def test_rm_filters_mixed(self):
        expected_ls_stdout = """
            a/test.csv
            a/test.tsv
            c/test.tsv
            test.tsv
            test.txt
            """
        self.rm_filters_helper(
            ['--recursive', '--exclude', '*', '--include', '*.csv', '--exclude', 'a/*'],
            expected_ls_stdout,
        )


class TestVersionConsoleTool(BaseConsoleToolTest):
    def test_version(self):
        self._run_command(['version', '--short'], expected_stdout=f'{VERSION}\n')
        self._run_command(['version'], expected_stdout=f'b2 command line tool, version {VERSION}\n')
