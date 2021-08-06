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
import pytest
import re
import unittest.mock as mock
from io import StringIO
from typing import Optional

from b2sdk import v1

from b2sdk.v2 import REALM_URLS
from b2sdk.v2 import StubAccountInfo
from b2sdk.v2 import B2Api
from b2sdk.v2 import B2HttpApiConfig
from b2.console_tool import ConsoleTool, B2_APPLICATION_KEY_ID_ENV_VAR, B2_APPLICATION_KEY_ENV_VAR, \
    B2_ENVIRONMENT_ENV_VAR
from b2sdk.v2 import RawSimulator
from b2sdk.v2 import UploadSourceBytes
from b2sdk.v2 import TempDir, fix_windows_path_limit

from .test_base import TestBase


def file_mod_time_millis(path):
    return int(os.path.getmtime(path) * 1000)


class BaseConsoleToolTest(TestBase):
    RE_API_VERSION = re.compile(r"\/v\d\/")
    json_pattern = re.compile(r'[^{,^\[]*(?P<dict_json>{.*})|(?P<list_json>\[.*]).*', re.DOTALL)

    def setUp(self):
        self.account_info = StubAccountInfo()

        self.b2_api = B2Api(
            self.account_info, None, api_config=B2HttpApiConfig(_raw_api_class=RawSimulator)
        )
        self.raw_api = self.b2_api.session.raw_api
        (self.account_id, self.master_key) = self.raw_api.create_account()

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
        actual_status = ConsoleTool(self.b2_api, stdout, stderr).run_command(['b2'] + argv)
        actual_stderr = self._trim_trailing_spaces(stderr.getvalue())

        if actual_stderr != '':
            print('ACTUAL STDERR:  ', repr(actual_stderr))
            print(actual_stderr)

        self.assertEqual('', actual_stderr, 'stderr')
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
        space_count = min(self._leading_spaces(line) for line in lines if line != '')

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
        format_vars = format_vars or {}
        return self._trim_leading_spaces(text).format(
            account_id=self.account_id, master_key=self.master_key, **format_vars
        )

    def assertDictIsContained(self, subset, superset):
        """Asserts that all keys in `subset` are present is `superset` and their corresponding values are the same"""
        truncated_superset = {k: v for k, v in superset.items() if k in subset}
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
        self._run_command_ignore_output(['authorize-account', self.account_id, self.master_key])

    def _create_my_bucket(self):
        self._run_command(['create-bucket', 'my-bucket', 'allPublic'], 'bucket_0\n', '', 0)

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
    ):
        """
        Runs one command using the ConsoleTool, checking stdout, stderr, and
        the returned status code.

        The expected output strings are format strings (as used by str.format),
        so braces need to be escaped by doubling them.  The variables 'account_id'
        and 'master_key' are set by default, plus any variables passed in the dict
        format_vars.

        The ConsoleTool is stateless, so we can make a new one for each
        call, with a fresh stdout and stderr
        """
        expected_stderr = self._normalize_expected_output(expected_stderr, format_vars)
        stdout, stderr = self._get_stdouterr()
        console_tool = ConsoleTool(self.b2_api, stdout, stderr)
        actual_status = console_tool.run_command(['b2'] + argv)

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
        if expected_stderr != actual_stderr:
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
        self.assertEqual(expected_stderr, actual_stderr, 'stderr')
        self.assertEqual(expected_status, actual_status, 'exit status code')


@mock.patch.dict(REALM_URLS, {'production': 'http://production.example.com'})
class TestConsoleTool(BaseConsoleToolTest):
    def test_authorize_with_bad_key(self):
        expected_stdout = '''
        Using http://production.example.com
        '''

        expected_stderr = '''
        ERROR: unable to authorize account: Invalid authorization token. Server said: secret key is wrong (unauthorized)
        '''

        self._run_command(
            ['authorize-account', self.account_id, 'bad-app-key'], expected_stdout, expected_stderr,
            1
        )

    def test_authorize_with_good_key_using_hyphen(self):
        # Initial condition
        assert self.account_info.get_account_auth_token() is None

        # Authorize an account with a good api key.
        expected_stdout = """
        Using http://production.example.com
        """

        self._run_command(
            ['authorize-account', self.account_id, self.master_key], expected_stdout, '', 0
        )

        # Auth token should be in account info now
        assert self.account_info.get_account_auth_token() is not None

    def test_authorize_with_good_key_using_underscore(self):
        # Initial condition
        assert self.account_info.get_account_auth_token() is None

        # Authorize an account with a good api key.
        expected_stdout = """
        Using http://production.example.com
        """

        self._run_command(
            ['authorize_account', self.account_id, self.master_key], expected_stdout, '', 0
        )

        # Auth token should be in account info now
        assert self.account_info.get_account_auth_token() is not None

    def test_authorize_using_env_variables(self):
        # Initial condition
        assert self.account_info.get_account_auth_token() is None

        # Authorize an account with a good api key.
        expected_stdout = """
        Using http://production.example.com
        """

        # Setting up environment variables
        with mock.patch.dict(
            'os.environ', {
                B2_APPLICATION_KEY_ID_ENV_VAR: self.account_id,
                B2_APPLICATION_KEY_ENV_VAR: self.master_key,
            }
        ):
            assert B2_APPLICATION_KEY_ID_ENV_VAR in os.environ
            assert B2_APPLICATION_KEY_ENV_VAR in os.environ

            self._run_command(['authorize-account'], expected_stdout, '', 0)

        # Auth token should be in account info now
        assert self.account_info.get_account_auth_token() is not None

    def test_authorize_towards_custom_realm(self):
        # Initial condition
        assert self.account_info.get_account_auth_token() is None

        # Authorize an account with a good api key.
        expected_stdout = """
        Using http://custom.example.com
        """

        # realm provided with args
        self._run_command(
            [
                'authorize-account', '--environment', 'http://custom.example.com', self.account_id,
                self.master_key
            ], expected_stdout, '', 0
        )

        # Auth token should be in account info now
        assert self.account_info.get_account_auth_token() is not None

        expected_stdout = """
        Using http://custom2.example.com
        """
        # realm provided with env var
        with mock.patch.dict(
            'os.environ', {
                B2_ENVIRONMENT_ENV_VAR: 'http://custom2.example.com',
            }
        ):
            self._run_command(
                ['authorize-account', self.account_id, self.master_key], expected_stdout, '', 0
            )

    def test_create_key_and_authorize_with_it(self):
        # Start with authorizing with the master key
        self._authorize_account()

        # Create a key
        self._run_command(
            ['create-key', 'key1', 'listBuckets,listKeys'],
            'appKeyId0 appKey0\n',
            '',
            0,
        )

        # Authorize with the key
        self._run_command(
            ['authorize-account', 'appKeyId0', 'appKey0'],
            'Using http://production.example.com\n',
            '',
            0,
        )

    def test_create_key_with_authorization_from_env_vars(self):
        # Initial condition
        assert self.account_info.get_account_auth_token() is None

        # Authorize an account with a good api key.

        # Setting up environment variables
        with mock.patch.dict(
            'os.environ', {
                B2_APPLICATION_KEY_ID_ENV_VAR: self.account_id,
                B2_APPLICATION_KEY_ENV_VAR: self.master_key,
            }
        ):
            assert B2_APPLICATION_KEY_ID_ENV_VAR in os.environ
            assert B2_APPLICATION_KEY_ENV_VAR in os.environ

            # The first time we're running on this cache there will be output from the implicit "authorize-account" call
            self._run_command(
                ['create-key', 'key1', 'listBuckets,listKeys'],
                'Using http://production.example.com\n'
                'appKeyId0 appKey0\n',
                '',
                0,
            )

            # The second time "authorize-account" is not called
            self._run_command(
                ['create-key', 'key1', 'listBuckets,listKeys,writeKeys'],
                'appKeyId1 appKey1\n',
                '',
                0,
            )

            with mock.patch.dict(
                'os.environ', {
                    B2_APPLICATION_KEY_ID_ENV_VAR: 'appKeyId1',
                    B2_APPLICATION_KEY_ENV_VAR: 'appKey1',
                }
            ):
                # "authorize-account" is called when the key changes
                self._run_command(
                    ['create-key', 'key1', 'listBuckets,listKeys'],
                    'Using http://production.example.com\n'
                    'appKeyId2 appKey2\n',
                    '',
                    0,
                )

                # "authorize-account" is also called when the realm changes
                with mock.patch.dict(
                    'os.environ', {
                        B2_ENVIRONMENT_ENV_VAR: 'http://custom.example.com',
                    }
                ):
                    self._run_command(
                        ['create-key', 'key1', 'listBuckets,listKeys'],
                        'Using http://custom.example.com\n'
                        'appKeyId3 appKey3\n',
                        '',
                        0,
                    )

    def test_authorize_key_without_list_buckets(self):
        self._authorize_account()

        # Create a key without listBuckets
        self._run_command(['create-key', 'key1', 'listKeys'], 'appKeyId0 appKey0\n', '', 0)

        # Authorize with the key
        self._run_command(
            ['authorize-account', 'appKeyId0', 'appKey0'],
            'Using http://production.example.com\n',
            'ERROR: application key has no listBuckets capability, which is required for the b2 command-line tool\n',
            1,
        )

    def test_create_bucket_key_and_authorize_with_it(self):
        # Start with authorizing with the master key
        self._authorize_account()

        # Make a bucket
        self._run_command(['create-bucket', 'my-bucket', 'allPrivate'], 'bucket_0\n', '', 0)

        # Create a key restricted to that bucket
        self._run_command(
            ['create-key', '--bucket', 'my-bucket', 'key1', 'listKeys,listBuckets'],
            'appKeyId0 appKey0\n', '', 0
        )

        # Authorize with the key
        self._run_command(
            ['authorize-account', 'appKeyId0', 'appKey0'],
            'Using http://production.example.com\n',
            '',
            0,
        )

    def test_clear_account(self):
        # Initial condition
        self._authorize_account()
        assert self.account_info.get_account_auth_token() is not None

        # Clearing the account should remove the auth token
        # from the account info.
        self._run_command(['clear-account'], '', '', 0)
        assert self.account_info.get_account_auth_token() is None

    def test_buckets(self):
        self._authorize_account()

        # Make a bucket with an illegal name
        expected_stdout = 'ERROR: Bad request: illegal bucket name: bad/bucket/name\n'
        self._run_command(['create-bucket', 'bad/bucket/name', 'allPublic'], '', expected_stdout, 1)

        # Make two buckets
        self._run_command(['create-bucket', 'my-bucket', 'allPrivate'], 'bucket_0\n', '', 0)
        self._run_command(['create-bucket', 'your-bucket', 'allPrivate'], 'bucket_1\n', '', 0)

        # Update one of them
        expected_json = {
            "accountId": self.account_id,
            "bucketId": "bucket_0",
            "bucketInfo": {},
            "bucketName": "my-bucket",
            "bucketType": "allPublic",
            "corsRules": [],
            "defaultServerSideEncryption": {
                "mode": "none"
            },
            "lifecycleRules": [],
            "options": [],
            "revision": 2
        }

        self._run_command(
            ['update-bucket', 'my-bucket', 'allPublic'], expected_json_in_stdout=expected_json
        )

        # Make sure they are there
        expected_stdout = '''
        bucket_0  allPublic   my-bucket
        bucket_1  allPrivate  your-bucket
        '''

        self._run_command(['list-buckets'], expected_stdout, '', 0)

        # Delete one
        expected_stdout = ''

        self._run_command(['delete-bucket', 'your-bucket'], expected_stdout, '', 0)

    def test_encrypted_buckets(self):
        self._authorize_account()

        # Make two encrypted buckets
        self._run_command(['create-bucket', 'my-bucket', 'allPrivate'], 'bucket_0\n', '', 0)
        self._run_command(
            ['create-bucket', '--defaultServerSideEncryption=SSE-B2', 'your-bucket', 'allPrivate'],
            'bucket_1\n', '', 0
        )

        # Update the one without encryption
        expected_json = {
            "accountId": self.account_id,
            "bucketId": "bucket_0",
            "bucketInfo": {},
            "bucketName": "my-bucket",
            "bucketType": "allPublic",
            "corsRules": [],
            "defaultServerSideEncryption": {
                "algorithm": "AES256",
                "mode": "SSE-B2"
            },
            "lifecycleRules": [],
            "options": [],
            "revision": 2
        }

        self._run_command(
            ['update-bucket', '--defaultServerSideEncryption=SSE-B2', 'my-bucket', 'allPublic'],
            expected_json_in_stdout=expected_json,
        )

        # Update the one with encryption
        expected_json = {
            "accountId": self.account_id,
            "bucketId": "bucket_1",
            "bucketInfo": {},
            "bucketName": "your-bucket",
            "bucketType": "allPrivate",
            "corsRules": [],
            "defaultServerSideEncryption": {
                "algorithm": "AES256",
                "mode": "SSE-B2"
            },
            "lifecycleRules": [],
            "options": [],
            "revision": 2
        }

        self._run_command(
            ['update-bucket', 'your-bucket', 'allPrivate'], expected_json_in_stdout=expected_json
        )

        # Make sure they are there
        expected_stdout = '''
        bucket_0  allPublic   my-bucket
        bucket_1  allPrivate  your-bucket
        '''

        self._run_command(['list-buckets'], expected_stdout, '', 0)

    def test_keys(self):
        self._authorize_account()

        self._run_command(['create-bucket', 'my-bucket-a', 'allPublic'], 'bucket_0\n', '', 0)
        self._run_command(['create-bucket', 'my-bucket-b', 'allPublic'], 'bucket_1\n', '', 0)
        self._run_command(['create-bucket', 'my-bucket-c', 'allPublic'], 'bucket_2\n', '', 0)

        capabilities = ['readFiles', 'listBuckets']
        capabilities_with_commas = ','.join(capabilities)

        # Make a key with an illegal name
        expected_stderr = 'ERROR: Bad request: illegal key name: bad_key_name\n'
        self._run_command(
            ['create-key', 'bad_key_name', capabilities_with_commas], '', expected_stderr, 1
        )

        # Make a key with negative validDurationInSeconds
        expected_stderr = 'ERROR: Bad request: valid duration must be greater than 0, and less than 1000 days in seconds\n'
        self._run_command(
            ['create-key', '--duration', '-456', 'goodKeyName', capabilities_with_commas], '',
            expected_stderr, 1
        )

        # Make a key with validDurationInSeconds outside of range
        expected_stderr = 'ERROR: Bad request: valid duration must be greater than 0, ' \
                          'and less than 1000 days in seconds\n'
        self._run_command(
            ['create-key', '--duration', '0', 'goodKeyName', capabilities_with_commas], '',
            expected_stderr, 1
        )
        self._run_command(
            ['create-key', '--duration', '86400001', 'goodKeyName', capabilities_with_commas], '',
            expected_stderr, 1
        )

        # Create three keys
        self._run_command(
            ['create-key', 'goodKeyName-One', capabilities_with_commas],
            'appKeyId0 appKey0\n',
            '',
            0,
        )
        self._run_command(
            [
                'create-key', '--bucket', 'my-bucket-a', 'goodKeyName-Two',
                capabilities_with_commas + ',readBucketEncryption'
            ],
            'appKeyId1 appKey1\n',
            '',
            0,
        )
        self._run_command(
            [
                'create-key', '--bucket', 'my-bucket-b', 'goodKeyName-Three',
                capabilities_with_commas
            ],
            'appKeyId2 appKey2\n',
            '',
            0,
        )

        # Delete one key
        self._run_command(['delete-key', 'appKeyId2'], 'appKeyId2\n', '', 0)

        # Delete one bucket, to test listing when a bucket is gone.
        self._run_command_ignore_output(['delete-bucket', 'my-bucket-b'])

        # List keys
        expected_list_keys_out = """
            appKeyId0   goodKeyName-One
            appKeyId1   goodKeyName-Two
            appKeyId2   goodKeyName-Three
            """

        expected_list_keys_out_long = """
            appKeyId0   goodKeyName-One        -                      -            -          ''   readFiles,listBuckets
            appKeyId1   goodKeyName-Two        my-bucket-a            -            -          ''   readFiles,listBuckets,readBucketEncryption
            appKeyId2   goodKeyName-Three      id=bucket_1            -            -          ''   readFiles,listBuckets
            """

        self._run_command(['list-keys'], expected_list_keys_out, '', 0)
        self._run_command(['list-keys', '--long'], expected_list_keys_out_long, '', 0)

        # authorize and make calls using application key with no restrictions
        self._run_command(
            ['authorize-account', 'appKeyId0', 'appKey0'], 'Using http://production.example.com\n',
            '', 0
        )
        self._run_command(
            ['list-buckets'],
            'bucket_0  allPublic   my-bucket-a\nbucket_2  allPublic   my-bucket-c\n', '', 0
        )

        expected_json = {
            "accountId": self.account_id,
            "bucketId": "bucket_0",
            "bucketInfo": {},
            "bucketName": "my-bucket-a",
            "bucketType": "allPublic",
            "corsRules": [],
            "defaultServerSideEncryption": {
                "mode": None
            },
            "lifecycleRules": [],
            "options": [],
            "revision": 1
        }
        self._run_command(['get-bucket', 'my-bucket-a'], expected_json_in_stdout=expected_json)

        # authorize and make calls using an application key with bucket restrictions
        self._run_command(
            ['authorize-account', 'appKeyId1', 'appKey1'], 'Using http://production.example.com\n',
            '', 0
        )

        self._run_command(
            ['list-buckets'], '', 'ERROR: Application key is restricted to bucket: my-bucket-a\n', 1
        )
        self._run_command(
            ['get-bucket', 'my-bucket-c'], '',
            'ERROR: Application key is restricted to bucket: my-bucket-a\n', 1
        )

        expected_json = {
            "accountId": self.account_id,
            "bucketId": "bucket_0",
            "bucketInfo": {},
            "bucketName": "my-bucket-a",
            "bucketType": "allPublic",
            "corsRules": [],
            "defaultServerSideEncryption": {
                "mode": "none"
            },
            "lifecycleRules": [],
            "options": [],
            "revision": 1
        }

        self._run_command(['get-bucket', 'my-bucket-a'], expected_json_in_stdout=expected_json)
        self._run_command(
            ['ls', '--json', 'my-bucket-c'], '',
            'ERROR: Application key is restricted to bucket: my-bucket-a\n', 1
        )

    def test_bucket_info_from_json(self):

        self._authorize_account()
        self._run_command(['create-bucket', 'my-bucket', 'allPublic'], 'bucket_0\n', '', 0)

        bucket_info = {'color': 'blue'}

        expected_json = {
            "accountId": self.account_id,
            "bucketId": "bucket_0",
            "bucketInfo": {
                "color": "blue"
            },
            "bucketName": "my-bucket",
            "bucketType": "allPrivate",
            "corsRules": [],
            "defaultServerSideEncryption": {
                "mode": "none"
            },
            "lifecycleRules": [],
            "options": [],
            "revision": 2
        }
        self._run_command(
            ['update-bucket', '--bucketInfo',
             json.dumps(bucket_info), 'my-bucket', 'allPrivate'],
            expected_json_in_stdout=expected_json,
        )

    def test_files(self):

        self._authorize_account()
        self._run_command(['create-bucket', 'my-bucket', 'allPublic'], 'bucket_0\n', '', 0)

        with TempDir() as temp_dir:
            local_file1 = self._make_local_file(temp_dir, 'file1.txt')
            # For this test, use a mod time without millis.  My mac truncates
            # millis and just leaves seconds.
            mod_time = 1500111222
            os.utime(local_file1, (mod_time, mod_time))
            self.assertEqual(1500111222, os.path.getmtime(local_file1))

            # Upload a file
            expected_stdout = '''
            URL by file name: http://download.example.com/file/my-bucket/file1.txt
            URL by fileId: http://download.example.com/b2api/vx/b2_download_file_by_id?fileId=9999'''
            expected_json = {
                "action": "upload",
                "contentSha1": "2aae6c35c94fcfb415dbe95f408b9ce91ee846ed",
                "contentType": "b2/x-auto",
                "fileId": "9999",
                "fileInfo": {
                    "src_last_modified_millis": "1500111222000"
                },
                "fileName": "file1.txt",
                "serverSideEncryption": {
                    "mode": "none"
                },
                "size": 11,
                "uploadTimestamp": 5000
            }

            self._run_command(
                ['upload-file', '--noProgress', 'my-bucket', local_file1, 'file1.txt'],
                expected_json_in_stdout=expected_json,
                remove_version=True,
                expected_part_of_stdout=expected_stdout,
            )

            # Get file info
            mod_time_str = str(file_mod_time_millis(local_file1))
            expected_json = {
                "accountId": self.account_id,
                "action": "upload",
                "bucketId": "bucket_0",
                "size": 11,
                "contentSha1": "2aae6c35c94fcfb415dbe95f408b9ce91ee846ed",
                "contentType": "b2/x-auto",
                "fileId": "9999",
                "fileInfo": {
                    "src_last_modified_millis": "1500111222000"
                },
                "fileName": "file1.txt",
                "serverSideEncryption": {
                    "mode": "none"
                },
                "uploadTimestamp": 5000
            }

            self._run_command(
                ['get-file-info', '9999'],
                expected_json_in_stdout=expected_json,
            )

            # Download by name
            local_download1 = os.path.join(temp_dir, 'download1.txt')
            expected_stdout = '''
            File name:           file1.txt
            File id:             9999
            File size:           11
            Content type:        b2/x-auto
            Content sha1:        2aae6c35c94fcfb415dbe95f408b9ce91ee846ed
            Encryption:          none
            Retention:           none
            Legal hold:          <unset>
            INFO src_last_modified_millis: 1500111222000
            Checksum matches
            Download finished
            '''

            self._run_command(
                [
                    'download-file-by-name', '--noProgress', 'my-bucket', 'file1.txt',
                    local_download1
                ], expected_stdout, '', 0
            )
            self.assertEqual(b'hello world', self._read_file(local_download1))
            self.assertEqual(mod_time, int(round(os.path.getmtime(local_download1))))

            # Download file by ID.  (Same expected output as downloading by name)
            local_download2 = os.path.join(temp_dir, 'download2.txt')
            self._run_command(
                ['download-file-by-id', '--noProgress', '9999', local_download2], expected_stdout,
                '', 0
            )
            self.assertEqual(b'hello world', self._read_file(local_download2))

            # Hide the file
            expected_json = {
                "action": "hide",
                "contentSha1": "none",
                "fileId": "9998",
                "fileInfo": {},
                "fileName": "file1.txt",
                "serverSideEncryption": {
                    "mode": "none"
                },
                "size": 0,
                "uploadTimestamp": 5001
            }

            self._run_command(
                ['hide-file', 'my-bucket', 'file1.txt'],
                expected_json_in_stdout=expected_json,
            )

            # List the file versions
            expected_json = [
                {
                    "action": "hide",
                    "contentSha1": "none",
                    "fileId": "9998",
                    "fileInfo": {},
                    "fileName": "file1.txt",
                    "serverSideEncryption": {
                        "mode": "none"
                    },
                    "size": 0,
                    "uploadTimestamp": 5001
                }, {
                    "action": "upload",
                    "contentSha1": "2aae6c35c94fcfb415dbe95f408b9ce91ee846ed",
                    "contentType": "b2/x-auto",
                    "fileId": "9999",
                    "fileInfo": {
                        "src_last_modified_millis": str(mod_time_str)
                    },
                    "fileName": "file1.txt",
                    "serverSideEncryption": {
                        "mode": "none"
                    },
                    "size": 11,
                    "uploadTimestamp": 5000
                }
            ]

            self._run_command(
                ['ls', '--json', '--versions', 'my-bucket'],
                expected_json_in_stdout=expected_json,
            )

            # List the file names
            expected_stdout = '''
            []
            '''

            self._run_command(['ls', '--json', 'my-bucket'], expected_stdout, '', 0)

            # Delete one file version, passing the name in
            expected_json = {"action": "delete", "fileId": "9998", "fileName": "file1.txt"}

            self._run_command(
                ['delete-file-version', 'file1.txt', '9998'], expected_json_in_stdout=expected_json
            )

            # Delete one file version, not passing the name in
            expected_json = {"action": "delete", "fileId": "9999", "fileName": "file1.txt"}

            self._run_command(
                ['delete-file-version', '9999'], expected_json_in_stdout=expected_json
            )

    def test_files_encrypted(self):

        self._authorize_account()
        self._run_command(['create-bucket', 'my-bucket', 'allPublic'], 'bucket_0\n', '', 0)

        with TempDir() as temp_dir:
            local_file1 = self._make_local_file(temp_dir, 'file1.txt')
            # For this test, use a mod time without millis.  My mac truncates
            # millis and just leaves seconds.
            mod_time = 1500111222
            os.utime(local_file1, (mod_time, mod_time))
            self.assertEqual(1500111222, os.path.getmtime(local_file1))

            # Upload a file
            expected_stdout = '''
            URL by file name: http://download.example.com/file/my-bucket/file1.txt
            URL by fileId: http://download.example.com/b2api/vx/b2_download_file_by_id?fileId=9999'''
            expected_json = {
                "action": "upload",
                "contentSha1": "2aae6c35c94fcfb415dbe95f408b9ce91ee846ed",
                "contentType": "b2/x-auto",
                "fileId": "9999",
                "fileInfo": {
                    "src_last_modified_millis": "1500111222000"
                },
                "fileName": "file1.txt",
                "serverSideEncryption": {
                    "algorithm": "AES256",
                    "mode": "SSE-B2"
                },
                "size": 11,
                "uploadTimestamp": 5000
            }

            self._run_command(
                [
                    'upload-file', '--noProgress', '--destinationServerSideEncryption=SSE-B2',
                    'my-bucket', local_file1, 'file1.txt'
                ],
                expected_json_in_stdout=expected_json,
                remove_version=True,
                expected_part_of_stdout=expected_stdout,
            )

            # Get file info
            mod_time_str = str(file_mod_time_millis(local_file1))
            expected_json = {
                "accountId": self.account_id,
                "action": "upload",
                "bucketId": "bucket_0",
                "size": 11,
                "contentSha1": "2aae6c35c94fcfb415dbe95f408b9ce91ee846ed",
                "contentType": "b2/x-auto",
                "fileId": "9999",
                "fileInfo": {
                    "src_last_modified_millis": "1500111222000"
                },
                "fileName": "file1.txt",
                "serverSideEncryption": {
                    "algorithm": "AES256",
                    "mode": "SSE-B2"
                },
                "uploadTimestamp": 5000
            }

            self._run_command(
                ['get-file-info', '9999'],
                expected_json_in_stdout=expected_json,
            )

            # Download by name
            local_download1 = os.path.join(temp_dir, 'download1.txt')
            expected_stdout = '''
            File name:           file1.txt
            File id:             9999
            File size:           11
            Content type:        b2/x-auto
            Content sha1:        2aae6c35c94fcfb415dbe95f408b9ce91ee846ed
            Encryption:          mode=SSE-B2, algorithm=AES256
            Retention:           none
            Legal hold:          <unset>
            INFO src_last_modified_millis: 1500111222000
            Checksum matches
            Download finished
            '''

            self._run_command(
                [
                    'download-file-by-name', '--noProgress', 'my-bucket', 'file1.txt',
                    local_download1
                ], expected_stdout, '', 0
            )
            self.assertEqual(b'hello world', self._read_file(local_download1))
            self.assertEqual(mod_time, int(round(os.path.getmtime(local_download1))))

            # Download file by ID.  (Same expected output as downloading by name)
            local_download2 = os.path.join(temp_dir, 'download2.txt')
            self._run_command(
                ['download-file-by-id', '--noProgress', '9999', local_download2], expected_stdout,
                '', 0
            )
            self.assertEqual(b'hello world', self._read_file(local_download2))

            # Hide the file
            expected_json = {
                "action": "hide",
                "contentSha1": "none",
                "fileId": "9998",
                "fileInfo": {},
                "fileName": "file1.txt",
                "serverSideEncryption": {
                    "mode": "none"
                },
                "size": 0,
                "uploadTimestamp": 5001
            }

            self._run_command(
                ['hide-file', 'my-bucket', 'file1.txt'],
                expected_json_in_stdout=expected_json,
            )

            # List the file versions
            expected_json = [
                {
                    "action": "hide",
                    "contentSha1": "none",
                    "fileId": "9998",
                    "fileInfo": {},
                    "fileName": "file1.txt",
                    "serverSideEncryption": {
                        "mode": "none"
                    },
                    "size": 0,
                    "uploadTimestamp": 5001
                }, {
                    "action": "upload",
                    "contentSha1": "2aae6c35c94fcfb415dbe95f408b9ce91ee846ed",
                    "contentType": "b2/x-auto",
                    "fileId": "9999",
                    "fileInfo": {
                        "src_last_modified_millis": str(mod_time_str)
                    },
                    "fileName": "file1.txt",
                    "serverSideEncryption": {
                        "algorithm": "AES256",
                        "mode": "SSE-B2"
                    },
                    "size": 11,
                    "uploadTimestamp": 5000
                }
            ]

            self._run_command(
                ['ls', '--json', '--versions', 'my-bucket'],
                expected_json_in_stdout=expected_json,
            )

            # List the file names
            expected_stdout = '''
            []
            '''

            self._run_command(['ls', '--json', 'my-bucket'], expected_stdout, '', 0)

            # Delete one file version, passing the name in
            expected_json = {"action": "delete", "fileId": "9998", "fileName": "file1.txt"}

            self._run_command(
                ['delete-file-version', 'file1.txt', '9998'],
                expected_json_in_stdout=expected_json,
            )

            # Delete one file version, not passing the name in
            expected_json = {"action": "delete", "fileId": "9999", "fileName": "file1.txt"}

            self._run_command(
                ['delete-file-version', '9999'],
                expected_json_in_stdout=expected_json,
            )

    def test_copy_file_by_id(self):
        self._authorize_account()
        self._create_my_bucket()

        with TempDir() as temp_dir:
            local_file1 = self._make_local_file(temp_dir, 'file1.txt')
            # For this test, use a mod time without millis.  My mac truncates
            # millis and just leaves seconds.
            mod_time = 1500111222
            os.utime(local_file1, (mod_time, mod_time))
            self.assertEqual(1500111222, os.path.getmtime(local_file1))

            # Upload a file
            expected_stdout = '''
            URL by file name: http://download.example.com/file/my-bucket/file1.txt
            URL by fileId: http://download.example.com/b2api/vx/b2_download_file_by_id?fileId=9999'''
            expected_json = {
                "action": "upload",
                "contentSha1": "2aae6c35c94fcfb415dbe95f408b9ce91ee846ed",
                "contentType": "b2/x-auto",
                "fileId": "9999",
                "fileInfo": {
                    "src_last_modified_millis": "1500111222000"
                },
                "fileName": "file1.txt",
                "serverSideEncryption": {
                    "mode": "none"
                },
                "size": 11,
                "uploadTimestamp": 5000
            }

            self._run_command(
                ['upload-file', '--noProgress', 'my-bucket', local_file1, 'file1.txt'],
                expected_json_in_stdout=expected_json,
                remove_version=True,
                expected_part_of_stdout=expected_stdout,
            )

            # Copy File
            expected_json = {
                "accountId": self.account_id,
                "action": "copy",
                "bucketId": "bucket_0",
                "size": 11,
                "contentSha1": "2aae6c35c94fcfb415dbe95f408b9ce91ee846ed",
                "contentType": "b2/x-auto",
                "fileId": "9998",
                "fileInfo": {
                    "src_last_modified_millis": "1500111222000"
                },
                "fileName": "file1_copy.txt",
                "serverSideEncryption": {
                    "mode": "none"
                },
                "uploadTimestamp": 5001
            }
            self._run_command(
                ['copy-file-by-id', '9999', 'my-bucket', 'file1_copy.txt'],
                expected_json_in_stdout=expected_json,
            )

            # Copy File with range parameter
            expected_json = {
                "accountId": self.account_id,
                "action": "copy",
                "bucketId": "bucket_0",
                "size": 6,
                "contentSha1": "2aae6c35c94fcfb415dbe95f408b9ce91ee846ed",
                "contentType": "b2/x-auto",
                "fileId": "9997",
                "fileInfo": {
                    "src_last_modified_millis": "1500111222000"
                },
                "fileName": "file1_copy.txt",
                "serverSideEncryption": {
                    "mode": "none"
                },
                "uploadTimestamp": 5002
            }
            self._run_command(
                ['copy-file-by-id', '--range', '3,9', '9999', 'my-bucket', 'file1_copy.txt'],
                expected_json_in_stdout=expected_json,
            )

            # Invalid metadata copy with file info
            expected_stderr = "ERROR: File info can be set only when content type is set\n"
            self._run_command(
                [
                    'copy-file-by-id',
                    '--info',
                    'a=b',
                    '9999',
                    'my-bucket',
                    'file1_copy.txt',
                ],
                '',
                expected_stderr,
                1,
            )

            # Invalid metadata replace without file info
            expected_stderr = "ERROR: File info can be not set only when content type is not set\n"
            self._run_command(
                [
                    'copy-file-by-id',
                    '--contentType',
                    'text/plain',
                    '9999',
                    'my-bucket',
                    'file1_copy.txt',
                ],
                '',
                expected_stderr,
                1,
            )

            # replace with content type and file info
            expected_json = {
                "accountId": self.account_id,
                "action": "copy",
                "bucketId": "bucket_0",
                "size": 11,
                "contentSha1": "2aae6c35c94fcfb415dbe95f408b9ce91ee846ed",
                "contentType": "text/plain",
                "fileId": "9996",
                "fileInfo": {
                    "a": "b"
                },
                "fileName": "file1_copy.txt",
                "serverSideEncryption": {
                    "mode": "none"
                },
                "uploadTimestamp": 5003
            }
            self._run_command(
                [
                    'copy-file-by-id',
                    '--contentType',
                    'text/plain',
                    '--info',
                    'a=b',
                    '9999',
                    'my-bucket',
                    'file1_copy.txt',
                ],
                expected_json_in_stdout=expected_json,
            )

            # UnsatisfiableRange
            expected_stderr = "ERROR: The range in the request is outside the size of the file\n"
            self._run_command(
                ['copy-file-by-id', '--range', '12,20', '9999', 'my-bucket', 'file1_copy.txt'],
                '',
                expected_stderr,
                1,
            )

            # Copy in different bucket
            self._run_command(['create-bucket', 'my-bucket1', 'allPublic'], 'bucket_1\n', '', 0)
            expected_json = {
                "accountId": self.account_id,
                "action": "copy",
                "bucketId": "bucket_1",
                "size": 11,
                "contentSha1": "2aae6c35c94fcfb415dbe95f408b9ce91ee846ed",
                "contentType": "b2/x-auto",
                "fileId": "9994",
                "fileInfo": {
                    "src_last_modified_millis": "1500111222000"
                },
                "fileName": "file1_copy.txt",
                "serverSideEncryption": {
                    "mode": "none"
                },
                "uploadTimestamp": 5004
            }
            self._run_command(
                ['copy-file-by-id', '9999', 'my-bucket1', 'file1_copy.txt'],
                expected_json_in_stdout=expected_json,
            )

    def test_get_download_auth_defaults(self):
        self._authorize_account()
        self._create_my_bucket()
        self._run_command(
            ['get-download-auth', 'my-bucket'], 'fake_download_auth_token_bucket_0__86400\n', '', 0
        )

    def test_get_download_auth_explicit(self):
        self._authorize_account()
        self._create_my_bucket()
        self._run_command(
            ['get-download-auth', '--prefix', 'prefix', '--duration', '12345', 'my-bucket'],
            'fake_download_auth_token_bucket_0_prefix_12345\n', '', 0
        )

    def test_get_download_auth_url(self):
        self._authorize_account()
        self._create_my_bucket()
        self._run_command(
            ['get-download-url-with-auth', '--duration', '12345', 'my-bucket', 'my-file'],
            'http://download.example.com/file/my-bucket/my-file?Authorization=fake_download_auth_token_bucket_0_my-file_12345\n',
            '', 0
        )

    def test_get_download_auth_url_with_encoding(self):
        self._authorize_account()
        self._create_my_bucket()
        self._run_command(
            ['get-download-url-with-auth', '--duration', '12345', 'my-bucket', u'\u81ea'],
            u'http://download.example.com/file/my-bucket/%E8%87%AA?Authorization=fake_download_auth_token_bucket_0_%E8%87%AA_12345\n',
            '', 0
        )

    def test_list_unfinished_large_files_with_none(self):
        self._authorize_account()
        self._create_my_bucket()
        self._run_command(['list-unfinished-large-files', 'my-bucket'], '', '', 0)

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
            expected_stdout = '''
            URL by file name: http://download.example.com/file/my-bucket/test.txt
            URL by fileId: http://download.example.com/b2api/vx/b2_download_file_by_id?fileId=9999'''
            expected_json = {
                "action": "upload",
                "contentSha1": "none",
                "contentType": "b2/x-auto",
                "fileId": "9999",
                "fileInfo": {
                    "src_last_modified_millis": str(mod_time_str)
                },
                "fileName": "test.txt",
                "serverSideEncryption": {
                    "mode": "none"
                },
                "size": 600,
                "uploadTimestamp": 5000
            }

            self._run_command(
                [
                    'upload-file', '--noProgress', '--threads', '5', 'my-bucket', file_path,
                    'test.txt'
                ],
                expected_json_in_stdout=expected_json,
                remove_version=True,
                expected_part_of_stdout=expected_stdout,
            )

    def test_upload_large_file_encrypted(self):
        self._authorize_account()
        self._run_command(['create-bucket', 'my-bucket', 'allPublic'], 'bucket_0\n', '', 0)
        min_part_size = self.account_info.get_recommended_part_size()
        file_size = min_part_size * 3

        with TempDir() as temp_dir:
            file_path = os.path.join(temp_dir, 'test.txt')
            text = '*' * file_size
            with open(file_path, 'wb') as f:
                f.write(text.encode('utf-8'))
            mod_time_str = str(file_mod_time_millis(file_path))
            expected_stdout = '''
            URL by file name: http://download.example.com/file/my-bucket/test.txt
            URL by fileId: http://download.example.com/b2api/vx/b2_download_file_by_id?fileId=9999'''
            expected_json = {
                "action": "upload",
                "contentSha1": "none",
                "contentType": "b2/x-auto",
                "fileId": "9999",
                "fileInfo": {
                    "src_last_modified_millis": str(mod_time_str)
                },
                "fileName": "test.txt",
                "serverSideEncryption": {
                    "algorithm": "AES256",
                    "mode": "SSE-B2"
                },
                "size": 600,
                "uploadTimestamp": 5000
            }

            self._run_command(
                [
                    'upload-file', '--noProgress', '--destinationServerSideEncryption=SSE-B2',
                    '--threads', '5', 'my-bucket', file_path, 'test.txt'
                ],
                expected_json_in_stdout=expected_json,
                remove_version=True,
                expected_part_of_stdout=expected_stdout,
            )

    def test_get_account_info(self):
        self._authorize_account()
        expected_json = {
            "accountAuthToken": "auth_token_0",
            "accountId": self.account_id,
            "allowed":
                {
                    "bucketId": None,
                    "bucketName": None,
                    "capabilities":
                        [
                            "listKeys", "writeKeys", "deleteKeys", "listBuckets", "writeBuckets",
                            "deleteBuckets", "readBucketEncryption", "writeBucketEncryption",
                            "readBucketRetentions", "writeBucketRetentions", "writeFileRetentions",
                            "writeFileLegalHolds", "readFileRetentions", "readFileLegalHolds",
                            "listFiles", "readFiles", "shareFiles", "writeFiles", "deleteFiles"
                        ],
                    "namePrefix": None
                },
            "apiUrl": "http://api.example.com",
            "applicationKey": self.master_key,
            "downloadUrl": "http://download.example.com"
        }
        self._run_command(
            ['get-account-info'],
            expected_json_in_stdout=expected_json,
        )

    def test_get_bucket(self):
        self._authorize_account()
        self._create_my_bucket()
        expected_json = {
            "accountId": self.account_id,
            "bucketId": "bucket_0",
            "bucketInfo": {},
            "bucketName": "my-bucket",
            "bucketType": "allPublic",
            "corsRules": [],
            "defaultServerSideEncryption": {
                "mode": "none"
            },
            "lifecycleRules": [],
            "options": [],
            "revision": 1
        }
        self._run_command(
            ['get-bucket', 'my-bucket'],
            expected_json_in_stdout=expected_json,
        )

    def test_get_bucket_empty_show_size(self):
        self._authorize_account()
        self._create_my_bucket()
        expected_json = {
            "accountId": self.account_id,
            "bucketId": "bucket_0",
            "bucketInfo": {},
            "bucketName": "my-bucket",
            "bucketType": "allPublic",
            "corsRules": [],
            "defaultServerSideEncryption": {
                "mode": "none"
            },
            "fileCount": 0,
            "lifecycleRules": [],
            "options": [],
            "revision": 1,
            "totalSize": 0
        }
        self._run_command(
            ['get-bucket', '--showSize', 'my-bucket'],
            expected_json_in_stdout=expected_json,
        )

    def test_get_bucket_one_item_show_size(self):
        self._authorize_account()
        self._create_my_bucket()
        with TempDir() as temp_dir:
            # Upload a standard test file.
            local_file1 = self._make_local_file(temp_dir, 'file1.txt')
            mod_time_str = str(file_mod_time_millis(local_file1))
            expected_stdout = '''
            URL by file name: http://download.example.com/file/my-bucket/file1.txt
            URL by fileId: http://download.example.com/b2api/vx/b2_download_file_by_id?fileId=9999'''
            expected_json = {
                "action": "upload",
                "contentSha1": "2aae6c35c94fcfb415dbe95f408b9ce91ee846ed",
                "contentType": "b2/x-auto",
                "fileId": "9999",
                "fileInfo": {
                    "src_last_modified_millis": str(mod_time_str)
                },
                "fileName": "file1.txt",
                "serverSideEncryption": {
                    "mode": "none"
                },
                "size": 11,
                "uploadTimestamp": 5000
            }
            self._run_command(
                ['upload-file', '--noProgress', 'my-bucket', local_file1, 'file1.txt'],
                expected_json_in_stdout=expected_json,
                remove_version=True,
                expected_part_of_stdout=expected_stdout,
            )

            # Now check the output of get-bucket against the canon.
            expected_json = {
                "accountId": self.account_id,
                "bucketId": "bucket_0",
                "bucketInfo": {},
                "bucketName": "my-bucket",
                "bucketType": "allPublic",
                "corsRules": [],
                "defaultServerSideEncryption": {
                    "mode": "none"
                },
                "fileCount": 1,
                "lifecycleRules": [],
                "options": [],
                "revision": 1,
                "totalSize": 11
            }
            self._run_command(
                ['get-bucket', '--showSize', 'my-bucket'],
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

        # Now check the output of get-bucket against the canon.
        expected_json = {
            "accountId": self.account_id,
            "bucketId": "bucket_0",
            "bucketInfo": {},
            "bucketName": "my-bucket",
            "bucketType": "allPublic",
            "corsRules": [],
            "defaultServerSideEncryption": {
                "mode": "none"
            },
            "fileCount": 10,
            "lifecycleRules": [],
            "options": [],
            "revision": 1,
            "totalSize": 40
        }
        self._run_command(
            ['get-bucket', '--showSize', 'my-bucket'],
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

        # Now check the output of get-bucket against the canon.
        expected_json = {
            "accountId": self.account_id,
            "bucketId": "bucket_0",
            "bucketInfo": {},
            "bucketName": "my-bucket",
            "bucketType": "allPublic",
            "corsRules": [],
            "defaultServerSideEncryption": {
                "mode": "none"
            },
            "fileCount": 20,
            "lifecycleRules": [],
            "options": [],
            "revision": 1,
            "totalSize": 90
        }
        self._run_command(
            ['get-bucket', '--showSize', 'my-bucket'],
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
        # something has failed if the output of 'get-bucket' does not match the canon.
        stdout, stderr = self._get_stdouterr()
        console_tool = ConsoleTool(self.b2_api, stdout, stderr)
        console_tool.run_command(['b2', 'hide-file', 'my-bucket', 'hidden1'])
        console_tool.run_command(['b2', 'hide-file', 'my-bucket', 'hidden2'])
        console_tool.run_command(['b2', 'hide-file', 'my-bucket', 'hidden3'])
        console_tool.run_command(['b2', 'hide-file', 'my-bucket', 'hidden4'])

        # Now check the output of get-bucket against the canon.
        expected_json = {
            "accountId": self.account_id,
            "bucketId": "bucket_0",
            "bucketInfo": {},
            "bucketName": "my-bucket",
            "bucketType": "allPublic",
            "corsRules": [],
            "defaultServerSideEncryption": {
                "mode": "none"
            },
            "fileCount": 10,
            "lifecycleRules": [],
            "options": [],
            "revision": 1,
            "totalSize": 24
        }
        self._run_command(
            ['get-bucket', '--showSize', 'my-bucket'],
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
        # something has failed if the output of 'get-bucket' does not match the canon.
        stdout, stderr = self._get_stdouterr()
        console_tool = ConsoleTool(self.b2_api, stdout, stderr)
        console_tool.run_command(['b2', 'hide-file', 'my-bucket', '1/hidden1'])
        console_tool.run_command(['b2', 'hide-file', 'my-bucket', '1/hidden1'])
        console_tool.run_command(['b2', 'hide-file', 'my-bucket', '1/hidden2'])
        console_tool.run_command(['b2', 'hide-file', 'my-bucket', '1/2/hidden3'])
        console_tool.run_command(['b2', 'hide-file', 'my-bucket', '1/2/hidden3'])
        console_tool.run_command(['b2', 'hide-file', 'my-bucket', '1/2/hidden3'])
        console_tool.run_command(['b2', 'hide-file', 'my-bucket', '1/2/hidden3'])

        # Now check the output of get-bucket against the canon.
        expected_json = {
            "accountId": self.account_id,
            "bucketId": "bucket_0",
            "bucketInfo": {},
            "bucketName": "my-bucket",
            "bucketType": "allPublic",
            "corsRules": [],
            "defaultServerSideEncryption": {
                "mode": "none"
            },
            "fileCount": 29,
            "lifecycleRules": [],
            "options": [],
            "revision": 1,
            "totalSize": 99
        }
        self._run_command(
            ['get-bucket', '--showSize', 'my-bucket'],
            expected_json_in_stdout=expected_json,
        )

    def test_get_bucket_encrypted(self):
        self._authorize_account()
        self._run_command(
            [
                'create-bucket', '--defaultServerSideEncryption=SSE-B2',
                '--defaultServerSideEncryptionAlgorithm=AES256', 'my-bucket', 'allPublic'
            ], 'bucket_0\n', '', 0
        )
        expected_json = {
            "accountId": self.account_id,
            "bucketId": "bucket_0",
            "bucketInfo": {},
            "bucketName": "my-bucket",
            "bucketType": "allPublic",
            "corsRules": [],
            "defaultServerSideEncryption": {
                "algorithm": "AES256",
                "mode": "SSE-B2"
            },
            "fileCount": 0,
            "lifecycleRules": [],
            "options": [],
            "revision": 1,
            "totalSize": 0
        }
        self._run_command(
            ['get-bucket', '--showSize', 'my-bucket'],
            expected_json_in_stdout=expected_json,
        )

    def test_sync(self):
        self._authorize_account()
        self._create_my_bucket()

        with TempDir() as temp_dir:
            file_path = os.path.join(temp_dir, 'test.txt')
            with open(file_path, 'wb') as f:
                f.write('hello world'.encode('utf-8'))
            expected_stdout = '''
            upload test.txt
            '''

            command = ['sync', '--threads', '5', '--noProgress', temp_dir, 'b2://my-bucket']
            self._run_command(command, expected_stdout, '', 0)

    def test_sync_empty_folder_when_not_enabled(self):
        self._authorize_account()
        self._create_my_bucket()
        with TempDir() as temp_dir:
            command = ['sync', '--threads', '1', '--noProgress', temp_dir, 'b2://my-bucket']
            expected_stderr = 'ERROR: Directory %s is empty.  Use --allowEmptySource to sync anyway.\n' % fix_windows_path_limit(
                temp_dir.replace('\\\\', '\\')
            )
            self._run_command(command, '', expected_stderr, 1)

    def test_sync_empty_folder_when_enabled(self):
        self._authorize_account()
        self._create_my_bucket()
        with TempDir() as temp_dir:
            command = [
                'sync', '--threads', '1', '--noProgress', '--allowEmptySource', temp_dir,
                'b2://my-bucket'
            ]
            self._run_command(command, '', '', 0)

    def test_sync_dry_run(self):
        self._authorize_account()
        self._create_my_bucket()

        with TempDir() as temp_dir:
            temp_file = self._make_local_file(temp_dir, 'test-dry-run.txt')

            # dry-run
            expected_stdout = '''
            upload test-dry-run.txt
            '''
            command = ['sync', '--noProgress', '--dryRun', temp_dir, 'b2://my-bucket']
            self._run_command(command, expected_stdout, '', 0)

            # file should not have been uploaded
            expected_stdout = '''
            []
            '''
            self._run_command(['ls', '--json', 'my-bucket'], expected_stdout, '', 0)

            # upload file
            expected_stdout = '''
            upload test-dry-run.txt
            '''
            command = ['sync', '--noProgress', temp_dir, 'b2://my-bucket']
            self._run_command(command, expected_stdout, '', 0)

            # file should have been uploaded
            mtime = file_mod_time_millis(temp_file)
            expected_json = [
                {
                    "action": "upload",
                    "contentSha1": "2aae6c35c94fcfb415dbe95f408b9ce91ee846ed",
                    "contentType": "b2/x-auto",
                    "fileId": "9999",
                    "fileInfo": {
                        "src_last_modified_millis": str(mtime)
                    },
                    "fileName": "test-dry-run.txt",
                    "serverSideEncryption": {
                        "mode": "none"
                    },
                    "size": 11,
                    "uploadTimestamp": 5000
                }
            ]
            self._run_command(
                ['ls', '--json', 'my-bucket'],
                expected_json_in_stdout=expected_json,
            )

    def test_sync_exclude_all_symlinks(self):
        self._authorize_account()
        self._create_my_bucket()

        with TempDir() as temp_dir:
            self._make_local_file(temp_dir, 'test.txt')
            os.symlink('test.txt', os.path.join(temp_dir, 'alink'))
            expected_stdout = '''
            upload test.txt
            '''

            command = [
                'sync', '--threads', '1', '--noProgress', '--excludeAllSymlinks', temp_dir,
                'b2://my-bucket'
            ]
            self._run_command(command, expected_stdout, '', 0)

    def test_sync_dont_exclude_all_symlinks(self):
        self._authorize_account()
        self._create_my_bucket()

        with TempDir() as temp_dir:
            self._make_local_file(temp_dir, 'test.txt')
            os.symlink('test.txt', os.path.join(temp_dir, 'alink'))
            expected_stdout = '''
            upload alink
            upload test.txt
            '''

            command = ['sync', '--threads', '1', '--noProgress', temp_dir, 'b2://my-bucket']
            self._run_command(command, expected_stdout, '', 0)

    def test_sync_exclude_if_modified_after_in_range(self):
        self._authorize_account()
        self._create_my_bucket()

        with TempDir() as temp_dir:
            for file, mtime in (('test.txt', 1367900664.152), ('test2.txt', 1367600664.152)):
                self._make_local_file(temp_dir, file)
                path = os.path.join(temp_dir, file)
                os.utime(path, (mtime, mtime))

            expected_stdout = '''
            upload test2.txt
            '''

            command = [
                'sync', '--threads', '1', '--noProgress', '--excludeIfModifiedAfter',
                '1367700664.152', temp_dir, 'b2://my-bucket'
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

            expected_stdout = '''
            upload test2.txt
            '''

            command = [
                'sync', '--threads', '1', '--noProgress', '--excludeIfModifiedAfter',
                '1367600664.152', temp_dir, 'b2://my-bucket'
            ]
            self._run_command(command, expected_stdout, '', 0)

    def test_ls(self):
        self._authorize_account()
        self._create_my_bucket()

        # Check with no files
        self._run_command(['ls', 'my-bucket'], '', '', 0)

        # Create some files, including files in a folder
        bucket = self.b2_api.get_bucket_by_name('my-bucket')
        bucket.upload(UploadSourceBytes(b''), 'a')
        bucket.upload(UploadSourceBytes(b' '), 'b/b1')
        bucket.upload(UploadSourceBytes(b'   '), 'b/b2')
        bucket.upload(UploadSourceBytes(b'     '), 'c')
        bucket.upload(UploadSourceBytes(b'      '), 'c')

        # Condensed output
        expected_stdout = '''
        a
        b/
        c
        '''
        self._run_command(['ls', 'my-bucket'], expected_stdout, '', 0)

        # Recursive output
        expected_stdout = '''
        a
        b/b1
        b/b2
        c
        '''
        self._run_command(['ls', '--recursive', 'my-bucket'], expected_stdout, '', 0)

        # Check long output.   (The format expects full-length file ids, so it causes whitespace here)
        expected_stdout = '''
                                                                                       9999  upload  1970-01-01  00:00:05          0  a
                                                                                          -       -           -         -          0  b/
                                                                                       9995  upload  1970-01-01  00:00:05          6  c
        '''
        self._run_command(['ls', '--long', 'my-bucket'], expected_stdout, '', 0)

        # Check long versions output   (The format expects full-length file ids, so it causes whitespace here)
        expected_stdout = '''
                                                                                       9999  upload  1970-01-01  00:00:05          0  a
                                                                                          -       -           -         -          0  b/
                                                                                       9995  upload  1970-01-01  00:00:05          6  c
                                                                                       9996  upload  1970-01-01  00:00:05          5  c
        '''
        self._run_command(['ls', '--long', '--versions', 'my-bucket'], expected_stdout, '', 0)

    def test_restrictions(self):
        # Initial condition
        self.assertEqual(None, self.account_info.get_account_auth_token())

        # Authorize an account with the master key.
        account_id = self.account_id
        self._run_command_ignore_output(['authorize-account', account_id, self.master_key])

        # Create a bucket to use
        bucket_name = 'restrictedBucket'
        bucket_id = 'bucket_0'
        self._run_command(['create-bucket', bucket_name, 'allPrivate'], bucket_id + '\n', '', 0)

        # Create another bucket
        other_bucket_name = 'otherBucket'
        self._run_command_ignore_output(['create-bucket', other_bucket_name, 'allPrivate'])

        # Create a key restricted to a bucket
        app_key_id = 'appKeyId0'
        app_key = 'appKey0'
        capabilities = "listBuckets,readFiles"
        file_prefix = 'some/file/prefix/'
        self._run_command(
            [
                'create-key', '--bucket', bucket_name, '--namePrefix', file_prefix, 'my-key',
                capabilities
            ],
            app_key_id + ' ' + app_key + '\n',
            '',
            0,
        )

        self._run_command_ignore_output(['authorize-account', app_key_id, app_key])

        # Auth token should be in account info now
        self.assertEqual('auth_token_1', self.account_info.get_account_auth_token())

        # Assertions that the restrictions not only are saved but what they are supposed to be
        self.assertEqual(
            dict(
                bucketId=bucket_id,
                bucketName=bucket_name,
                capabilities=[
                    'listBuckets',
                    'readFiles',
                ],
                namePrefix=file_prefix,
            ),
            self.account_info.get_allowed(),
        )

        # Test that the application key info gets added to the unauthorized error message.
        expected_create_key_stderr = "ERROR: unauthorized for application key " \
                                     "with capabilities 'listBuckets,readFiles', " \
                                     "restricted to bucket 'restrictedBucket', " \
                                     "restricted to files that start with 'some/file/prefix/' (unauthorized)\n"
        self._run_command(
            ['create-key', 'goodKeyName-One', 'readFiles,listBuckets'],
            '',
            expected_create_key_stderr,
            1,
        )

    def test_list_buckets_not_allowed_for_app_key(self):
        # Create a bucket and a key restricted to that bucket.
        self._authorize_account()
        self._run_command(
            ['create-bucket', 'my-bucket', 'allPrivate'],
            'bucket_0\n',
            '',
            0,
        )

        # Authorizing with the key will fail because the ConsoleTool needs
        # to be able to look up the name of the bucket.
        self._run_command(
            ['create-key', 'my-key', 'listFiles'],
            'appKeyId0 appKey0\n',
            '',
            0,
        )

        # Authorize with the key, which should result in an error.
        self._run_command(
            ['authorize-account', 'appKeyId0', 'appKey0'],
            'Using http://production.example.com\n',
            'ERROR: application key has no listBuckets capability, which is required for the b2 command-line tool\n',
            1,
        )

    def test_bucket_missing_for_bucket_key(self):
        # Create a bucket and a key restricted to that bucket.
        self._authorize_account()
        self._run_command(
            ['create-bucket', 'my-bucket', 'allPrivate'],
            'bucket_0\n',
            '',
            0,
        )
        self._run_command(
            ['create-key', '--bucket', 'my-bucket', 'my-key', 'listBuckets,listFiles'],
            'appKeyId0 appKey0\n',
            '',
            0,
        )

        # Get rid of the bucket, leaving the key with a dangling pointer to it.
        self._run_command_ignore_output(['delete-bucket', 'my-bucket'])

        # Authorizing with the key will fail because the ConsoleTool needs
        # to be able to look up the name of the bucket.
        self._run_command(
            ['authorize-account', 'appKeyId0', 'appKey0'],
            'Using http://production.example.com\n',
            "ERROR: application key is restricted to bucket id 'bucket_0', which no longer exists\n",
            1,
        )

    def test_ls_for_restricted_bucket(self):
        # Create a bucket and a key restricted to that bucket.
        self._authorize_account()
        self._run_command(
            ['create-bucket', 'my-bucket', 'allPrivate'],
            'bucket_0\n',
            '',
            0,
        )
        self._run_command(
            ['create-key', '--bucket', 'my-bucket', 'my-key', 'listBuckets,listFiles'],
            'appKeyId0 appKey0\n',
            '',
            0,
        )

        # Authorize with the key and list the files
        self._run_command_ignore_output(['authorize-account', 'appKeyId0', 'appKey0'],)
        self._run_command(
            ['ls', 'my-bucket'],
            '',
            '',
            0,
        )

    def test_bad_terminal(self):
        stdout = mock.MagicMock()
        stdout.write = mock.MagicMock(
            side_effect=[
                UnicodeEncodeError('codec', u'foo', 100, 105, 'artificial UnicodeEncodeError')
            ] + list(range(25))
        )
        stderr = mock.MagicMock()
        console_tool = ConsoleTool(self.b2_api, stdout, stderr)
        console_tool.run_command(['b2', 'authorize-account', self.account_id, self.master_key])


@mock.patch.dict(REALM_URLS, {'production': 'http://production.example.com'})
class TestConsoleToolWithV1(BaseConsoleToolTest):
    """These tests use v1 interface to perform various setups before running CLI commands"""

    def setUp(self):
        super().setUp()
        self.v1_account_info = v1.StubAccountInfo()

        self.v1_b2_api = v1.B2Api(self.v1_account_info, None)
        self.v1_b2_api.session.raw_api = self.raw_api
        self.v1_b2_api.authorize_account('production', self.account_id, self.master_key)
        self._authorize_account()
        self._create_my_bucket()
        self.v1_bucket = self.v1_b2_api.create_bucket('my-v1-bucket', 'allPrivate')

    def test_cancel_large_file(self):
        file = self.v1_bucket.start_large_file('file1', 'text/plain', {})
        self._run_command(['cancel-large-file', file.file_id], '9999 canceled\n', '', 0)

    def test_cancel_all_large_file(self):
        self.v1_bucket.start_large_file('file1', 'text/plain', {})
        self.v1_bucket.start_large_file('file2', 'text/plain', {})
        expected_stdout = '''
        9999 canceled
        9998 canceled
        '''

        self._run_command(
            ['cancel-all-unfinished-large-files', 'my-v1-bucket'], expected_stdout, '', 0
        )

    def test_list_parts_with_none(self):
        file = self.v1_bucket.start_large_file('file', 'text/plain', {})
        self._run_command(['list-parts', file.file_id], '', '', 0)

    def test_list_parts_with_parts(self):

        bucket = self.b2_api.get_bucket_by_name('my-bucket')
        file = self.v1_bucket.start_large_file('file', 'text/plain', {})
        content = b'hello world'
        large_file_upload_state = mock.MagicMock()
        large_file_upload_state.has_error.return_value = False
        bucket.api.services.upload_manager._upload_part(
            bucket.id_, file.file_id, UploadSourceBytes(content), 1, large_file_upload_state, None,
            None
        )
        bucket.api.services.upload_manager._upload_part(
            bucket.id_, file.file_id, UploadSourceBytes(content), 3, large_file_upload_state, None,
            None
        )
        expected_stdout = '''
            1         11  2aae6c35c94fcfb415dbe95f408b9ce91ee846ed
            3         11  2aae6c35c94fcfb415dbe95f408b9ce91ee846ed
        '''

        self._run_command(['list-parts', file.file_id], expected_stdout, '', 0)

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
        expected_stdout = '''
        9999 file1 text/plain
        9998 file2 text/plain color=blue
        9997 file3 application/json
        '''

        self._run_command(['list-unfinished-large-files', 'my-bucket'], expected_stdout, '', 0)
