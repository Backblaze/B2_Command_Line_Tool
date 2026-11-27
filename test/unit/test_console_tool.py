######################################################################
#
# File: test/unit/test_console_tool.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import datetime
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
from b2sdk.version import VERSION as b2sdk_version
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

from .b2_simulator import b2_simulator_enabled, raw_api_class
from .test_base import TestBase


class _ArgsTail(list):
    """A view of recorded calls that keeps only the last `n` positional arguments of each."""

    def __init__(self, calls, n):
        super().__init__()
        self._calls = calls
        self._n = n

    def __iter__(self):
        return iter([(args[-self._n :], answer) for args, answer in self._calls])

    def __len__(self):
        return len(self._calls)


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
        self.raw_simulator = raw_api_class(RawSimulator)()
        self.on_b2_simulator = b2_simulator_enabled()
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

        patcher = mock.patch.object(
            self.console_tool_class,
            '_initialize_b2_api',
            lambda cls, args, kwargs: _get_b2api(**kwargs),
        )
        patcher.start()
        self.addCleanup(patcher.stop)

        self.b2_api = _get_b2api()
        self.raw_api = self.b2_api.session.raw_api
        self.account_id, self.master_key = self.raw_api.create_account()

        for env_var_name in [
            B2_APPLICATION_KEY_ID_ENV_VAR,
            B2_APPLICATION_KEY_ENV_VAR,
            B2_ENVIRONMENT_ENV_VAR,
        ]:
            os.environ.pop(env_var_name, None)

    def tearDown(self):
        self.doCleanups()
        super().tearDown()

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

    def _create_bucket(self, bucket_name, *args):
        """Run `bucket create`, check that it prints exactly the new bucket's id, return the id."""
        _, stdout, _ = self._run_command(['bucket', 'create', bucket_name, *args])
        bucket_id = self.b2_api.get_bucket_by_name(bucket_name).id_
        self.assertEqual(f'{bucket_id}\n', stdout)
        return bucket_id

    def _create_my_bucket(self):
        return self._create_bucket('my-bucket', 'allPublic')

    def _file_id(self, bucket_name, file_name):
        """The id of the latest version of a file, as the server reports it."""
        return self.b2_api.get_bucket_by_name(bucket_name).get_file_info_by_name(file_name).id_

    def _b2(self, raw_simulator_value, b2_value):
        """
        Expected value where b2sdk's RawSimulator differs from B2.

        RawSimulator's own value in the default run, B2's value on b2-simulator.
        """
        return b2_value if self.on_b2_simulator else raw_simulator_value

    @property
    def _bucket_options(self):
        # B2 reports `options: ["s3"]` on buckets (b2-simulator ledger F-79); RawSimulator `[]`.
        return self._b2([], ['s3'])

    @property
    def _hide_marker_sha1(self):
        # B2 gives a hide marker the SHA-1 of empty content (real B2, 2026-11-27, b2-simulator
        # parity report item 9); RawSimulator 'none'.
        return self._b2('none', 'da39a3ee5e6b4b0d3255bfef95601890afd80709')

    def _bucket_revision(self, updates):
        # B2 reports revision 2 for a new bucket and +1 per update (b2-simulator ledger F-75);
        # RawSimulator starts at 1.
        return updates + self._b2(1, 2)

    def _master_b2_api(self):
        """A B2Api of its own, authorized with the master key, on the same server as the test."""
        api = getattr(self, '_master_api', None)
        if api is None:
            api = B2Api(StubAccountInfo(), cache=None, api_config=self.api_config)
            api.authorize_account(self.account_id, self.master_key, realm='production')
            self._master_api = api
        return api

    def _create_key(self, argv, expected_stderr=''):
        """
        Run a key-creating command and return the new key's id and secret.

        Checks that the command prints exactly `<keyId> <secret>` and that the server lists a key
        with that id and the key name given in `argv`.
        """
        _, stdout, _ = self._run_command(argv, None, expected_stderr, 0)
        key_id, secret = stdout.split()
        self.assertEqual(f'{key_id} {secret}\n', stdout)
        keys = {key.id_: key for key in self._master_b2_api().list_keys()}
        self.assertIn(key_id, keys)
        self.assertIn(keys[key_id].key_name, argv)
        return key_id, secret

    def _versions(self, bucket_name, file_name):
        """Every version of one file name, newest first, as the server lists them."""
        bucket = self._master_b2_api().get_bucket_by_name(bucket_name)
        return [
            version
            for version in bucket.list_file_versions(file_name)
            if version.file_name == file_name
        ]

    def _newest_version(self, bucket_name, file_name):
        """Id and upload timestamp of the newest version of one file name, from the server."""
        newest = self._versions(bucket_name, file_name)[0]
        return newest.id_, newest.upload_timestamp

    def _record_answers(self, raw_api_method):
        """
        Record each call of one raw API method and the server's answer to it.

        Returns the list the calls are appended to, as `(positional arguments, answer)`.
        """
        calls = []
        original = getattr(self.raw_api, raw_api_method)

        def recording(*args):
            answer = original(*args)
            calls.append((args, answer))
            return answer

        patcher = mock.patch.object(self.raw_api, raw_api_method, recording)
        patcher.start()
        self.addCleanup(patcher.stop)
        return calls

    def _record_download_authorizations(self):
        """`_record_answers` of `get_download_authorization`, as `((bucket, prefix, duration), answer)`."""
        calls = self._record_answers('get_download_authorization')
        return _ArgsTail(calls, 3)

    def _assert_json_in_stdout(self, expected_json, stdout):
        """The same check as `_run_command(expected_json_in_stdout=...)`, on captured stdout."""
        json_match = self.json_pattern.match(stdout)
        if not json_match:
            self.fail('EXPECTED TO FIND A JSON IN: ' + repr(stdout))
        found_json = json.loads(json_match.group('dict_json') or json_match.group('list_json'))
        if json_match.group('dict_json'):
            self.assertDictIsContained(expected_json, found_json)
        else:
            self.assertListOfDictsIsContained(expected_json, found_json)

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
        self._create_bucket('my-bucket-cc', 'allPrivate')

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
        self._create_bucket('my-bucket', '--bucketInfo', '{"xxx": "123"}', 'allPrivate')
        self._create_bucket('my-bucket-kebab', '--bucket-info', '{"xxx": "123"}', 'allPrivate')

    @pytest.mark.apiver(from_ver=4)
    def test_camel_case_not_supported_in_v4(self):
        self._authorize_account()
        self._run_command(
            ['bucket', 'create', 'my-bucket', '--bucketInfo', 'allPrivate'], '', '', 2
        )
        self._create_bucket('my-bucket-kebab', '--bucket-info', '{"xxx": "123"}', 'allPrivate')

    def test_create_key_and_authorize_with_it(self):
        # Start with authorizing with the master key
        self._authorize_account()

        # Create a key
        key_id_0, key_0 = self._create_key(['key', 'create', 'key1', 'listBuckets,listKeys'])

        # test deprecated command
        key_id_1, key_1 = self._create_key(
            ['create-key', 'key2', 'listBuckets,listKeys'],
            'WARNING: `create-key` command is deprecated. Use `key create` instead.\n',
        )

        # Authorize with the key
        self._run_command(
            ['account', 'authorize', key_id_0, key_0],
            None,
            '',
            0,
        )

        self._run_command(
            ['account', 'authorize', key_id_1, key_1],
            None,
            '',
            0,
        )

        # test deprecated command
        self._run_command(
            ['authorize-account', key_id_0, key_0],
            None,
            'WARNING: `authorize-account` command is deprecated. Use `account authorize` instead.\n',
            0,
        )

        # test deprecated command
        self._run_command(
            ['authorize-account', key_id_1, key_1],
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
            self._create_key(['key', 'create', 'key1', 'listBuckets,listKeys'])

            # The second time "account authorize" is not called
            key_id_1, key_1 = self._create_key(
                ['key', 'create', 'key1', 'listBuckets,listKeys,writeKeys']
            )

            with mock.patch.dict(
                'os.environ',
                {
                    B2_APPLICATION_KEY_ID_ENV_VAR: key_id_1,
                    B2_APPLICATION_KEY_ENV_VAR: key_1,
                },
            ):
                # "account authorize" is called when the key changes
                self._create_key(['key', 'create', 'key1', 'listBuckets,listKeys'])

                # "account authorize" is also called when the realm changes
                with mock.patch.dict(
                    'os.environ',
                    {
                        B2_ENVIRONMENT_ENV_VAR: 'http://custom.example.com',
                    },
                ):
                    self._create_key(
                        ['key', 'create', 'key1', 'listBuckets,listKeys'],
                        'Using http://custom.example.com\n',
                    )

    def test_authorize_key_without_list_buckets(self):
        self._authorize_account()

        # Create a key without listBuckets
        key_id, key = self._create_key(['key', 'create', 'key1', 'listKeys'])

        # Authorize with the key
        self._run_command(
            ['account', 'authorize', key_id, key],
            '',
            'ERROR: application key has no listBuckets capability, which is required for the b2 command-line tool\n',
            1,
        )

    def test_create_bucket__with_lifecycle_rule(self):
        self._authorize_account()

        rule = json.dumps(
            {'daysFromHidingToDeleting': 1, 'daysFromUploadingToHiding': None, 'fileNamePrefix': ''}
        )

        self._create_bucket('my-bucket', 'allPrivate', '--lifecycle-rule', rule)

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

        self._create_bucket('my-bucket', 'allPrivate', '--lifecycle-rules', rules)

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
        self._create_bucket('my-bucket', 'allPrivate')

        # Create a key restricted to that bucket. B2 refuses listKeys, writeKeys and deleteKeys on
        # a bucket-restricted key (https://www.backblaze.com/docs/cloud-storage-application-keys).
        key_id_0, key_0 = self._create_key(
            ['key', 'create', '--bucket', 'my-bucket', 'key1', 'listFiles,listBuckets']
        )

        # test deprecated command
        key_id_1, key_1 = self._create_key(
            ['create-key', '--bucket', 'my-bucket', 'key2', 'listFiles,listBuckets'],
            'WARNING: `create-key` command is deprecated. Use `key create` instead.\n',
        )

        # Authorize with the key
        self._run_command(
            ['account', 'authorize', key_id_0, key_0],
            None,
            '',
            0,
        )

        self._run_command(
            ['account', 'authorize', key_id_1, key_1],
            None,
            '',
            0,
        )

    def test_create_multi_bucket_key_and_authorize_with_it(self):
        # Start with authorizing with the master key
        self._authorize_account()

        # Make two buckets
        self._create_bucket('my-bucket-0', 'allPrivate')
        self._create_bucket('my-bucket-1', 'allPrivate')

        # Create a key restricted to those buckets (no listKeys on a bucket-restricted key, as
        # above)
        key_id_0, key_0 = self._create_key(
            [
                'key',
                'create',
                '--bucket',
                'my-bucket-0',
                '--bucket',
                'my-bucket-1',
                'key1',
                'listFiles,listBuckets',
            ]
        )

        # test deprecated command
        key_id_1, key_1 = self._create_key(
            [
                'create-key',
                '--bucket',
                'my-bucket-0',
                '--bucket',
                'my-bucket-1',
                'key2',
                'listFiles,listBuckets',
            ],
            'WARNING: `create-key` command is deprecated. Use `key create` instead.\n',
        )

        # Authorize with the key
        self._run_command(
            ['account', 'authorize', key_id_0, key_0],
            None,
            '',
            0,
        )

        self._run_command(
            ['account', 'authorize', key_id_1, key_1],
            None,
            '',
            0,
        )

    def test_update_bucket_without_lifecycle(self):
        # Start with authorizing with the master key
        self._authorize_account()

        bucket_name = 'my-bucket-liferules'
        # Create a bucket with lifecycleRule
        bucket_id = self._create_bucket(
            bucket_name,
            '--lifecycle-rule',
            '{"daysFromHidingToDeleting": 2, "fileNamePrefix": "foo"}',
            'allPrivate',
        )

        expected_stdout_dict = {
            'accountId': self.account_id,
            'bucketId': bucket_id,
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
        bucket_0 = self._create_bucket('my-bucket', 'allPrivate')
        bucket_1 = self._create_bucket('your-bucket', 'allPrivate')

        # Update one of them
        expected_json = {
            'accountId': self.account_id,
            'bucketId': bucket_0,
            'bucketInfo': {},
            'bucketName': 'my-bucket',
            'bucketType': 'allPublic',
            'corsRules': [],
            'defaultServerSideEncryption': {'algorithm': 'AES256', 'mode': 'SSE-B2'},
            'lifecycleRules': [],
            'options': self._bucket_options,
            'revision': self._bucket_revision(1),
        }

        self._run_command(
            ['bucket', 'update', 'my-bucket', 'allPublic'], expected_json_in_stdout=expected_json
        )

        # Make sure they are there
        expected_stdout = """
        {bucket_0}  allPublic   my-bucket
        {bucket_1}  allPrivate  your-bucket
        """
        bucket_ids = {'bucket_0': bucket_0, 'bucket_1': bucket_1}

        self._run_command(['bucket', 'list'], expected_stdout, '', 0, format_vars=bucket_ids)

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
        deprecation = (
            'WARNING: `create-bucket` command is deprecated. Use `bucket create` instead.\n'
        )
        _, stdout, _ = self._run_command(
            ['create-bucket', 'my-bucket', 'allPrivate'], None, deprecation, 0
        )
        bucket_0 = self.b2_api.get_bucket_by_name('my-bucket').id_
        self.assertEqual(f'{bucket_0}\n', stdout)
        _, stdout, _ = self._run_command(
            ['create-bucket', 'your-bucket', 'allPrivate'], None, deprecation, 0
        )
        bucket_1 = self.b2_api.get_bucket_by_name('your-bucket').id_
        self.assertEqual(f'{bucket_1}\n', stdout)

        # Update one of them
        expected_json = {
            'accountId': self.account_id,
            'bucketId': bucket_0,
            'bucketInfo': {},
            'bucketName': 'my-bucket',
            'bucketType': 'allPublic',
            'corsRules': [],
            'defaultServerSideEncryption': {'algorithm': 'AES256', 'mode': 'SSE-B2'},
            'lifecycleRules': [],
            'options': self._bucket_options,
            'revision': self._bucket_revision(1),
        }

        self._run_command(
            ['update-bucket', 'my-bucket', 'allPublic'],
            expected_stderr='WARNING: `update-bucket` command is deprecated. Use `bucket update` instead.\n',
            expected_json_in_stdout=expected_json,
        )

        # Make sure they are there
        expected_stdout = """
        {bucket_0}  allPublic   my-bucket
        {bucket_1}  allPrivate  your-bucket
        """
        bucket_ids = {'bucket_0': bucket_0, 'bucket_1': bucket_1}

        self._run_command(
            ['list-buckets'],
            expected_stdout,
            'WARNING: `list-buckets` command is deprecated. Use `bucket list` instead.\n',
            0,
            format_vars=bucket_ids,
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
        bucket_0 = self._create_bucket('my-bucket', 'allPrivate')
        bucket_1 = self._create_bucket(
            'your-bucket', '--default-server-side-encryption=SSE-B2', 'allPrivate'
        )

        # Update the one without encryption
        expected_json = {
            'accountId': self.account_id,
            'bucketId': bucket_0,
            'bucketInfo': {},
            'bucketName': 'my-bucket',
            'bucketType': 'allPublic',
            'corsRules': [],
            'defaultServerSideEncryption': {'algorithm': 'AES256', 'mode': 'SSE-B2'},
            'lifecycleRules': [],
            'options': self._bucket_options,
            'revision': self._bucket_revision(1),
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
            'bucketId': bucket_1,
            'bucketInfo': {},
            'bucketName': 'your-bucket',
            'bucketType': 'allPrivate',
            'corsRules': [],
            'defaultServerSideEncryption': {'algorithm': 'AES256', 'mode': 'SSE-B2'},
            'lifecycleRules': [],
            'options': self._bucket_options,
            'revision': self._bucket_revision(1),
        }

        self._run_command(
            ['bucket', 'update', 'your-bucket', 'allPrivate'], expected_json_in_stdout=expected_json
        )

        # Make sure they are there
        expected_stdout = """
        {bucket_0}  allPublic   my-bucket
        {bucket_1}  allPrivate  your-bucket
        """
        bucket_ids = {'bucket_0': bucket_0, 'bucket_1': bucket_1}

        self._run_command(['bucket', 'list'], expected_stdout, '', 0, format_vars=bucket_ids)

    def test_keys(self):
        self._authorize_account()

        bucket_0 = self._create_bucket('my-bucket-a', 'allPublic')
        bucket_1 = self._create_bucket('my-bucket-b', 'allPublic')
        bucket_2 = self._create_bucket('my-bucket-c', 'allPublic')

        capabilities = ['readFiles', 'listBuckets']
        capabilities_with_commas = ','.join(capabilities)

        # Make a key with an illegal name. Real B2's text (measured 2026-11-27, b2-simulator ledger
        # F-148); RawSimulator has its own.
        expected_stderr = self._b2(
            'ERROR: Bad request: illegal key name: bad_key_name\n',
            "ERROR: Invalid characters in keyName: must be alphanumeric or '-' (bad_request)\n",
        )
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
        key_id_0, key_0 = self._create_key(
            ['key', 'create', 'goodKeyName-One', capabilities_with_commas]
        )
        key_id_1, key_1 = self._create_key(
            [
                'key',
                'create',
                '--bucket',
                'my-bucket-a',
                'goodKeyName-Two',
                capabilities_with_commas + ',readBucketEncryption',
            ]
        )
        key_id_2, _ = self._create_key(
            [
                'key',
                'create',
                '--bucket',
                'my-bucket-b',
                'goodKeyName-Three',
                capabilities_with_commas,
            ]
        )
        key_id_3, _ = self._create_key(['key', 'create', '--all-capabilities', 'goodKeyName-Four'])
        key_id_4, _ = self._create_key(
            [
                'key',
                'create',
                '--bucket',
                'my-bucket-b',
                'goodKeyName-Five',
                capabilities_with_commas,
            ]
        )
        key_id_5, _ = self._create_key(
            ['create-key', '--bucket', 'my-bucket-b', 'goodKeyName-Six', capabilities_with_commas],
            'WARNING: `create-key` command is deprecated. Use `key create` instead.\n',
        )

        # Delete one key
        self._run_command(['key', 'delete', key_id_2], f'{key_id_2}\n', '', 0)

        # test deprecated command
        self._run_command(
            ['delete-key', key_id_5],
            f'{key_id_5}\n',
            'WARNING: `delete-key` command is deprecated. Use `key delete` instead.\n',
            0,
        )

        # Delete one bucket, to test listing when a bucket is gone.
        self._run_command_ignore_output(['bucket', 'delete', 'my-bucket-b'])

        # List keys
        key_ids = {
            'key_id_0': key_id_0,
            'key_id_1': key_id_1,
            'key_id_3': key_id_3,
            'key_id_4': key_id_4,
            # A deleted bucket is shown by its id
            'deleted_bucket': f'id={bucket_1}',
            # `--all-capabilities` asks for b2sdk's ALL_CAPABILITIES plus every capability of the
            # key in use (here the master key, which B2 may give capabilities b2sdk does not name)
            'all_capabilities': ','.join(
                sorted(
                    set(ALL_CAPABILITIES)
                    | set(self._master_b2_api().account_info.get_allowed()['capabilities'])
                )
            ),
        }
        expected_list_keys_out = """
            {key_id_0}   goodKeyName-One
            {key_id_1}   goodKeyName-Two
            {key_id_3}   goodKeyName-Four
            {key_id_4}   goodKeyName-Five
            """

        expected_list_keys_out_long = """
            {key_id_0}   goodKeyName-One        -                      -            -          ''   readFiles,listBuckets
            {key_id_1}   goodKeyName-Two        my-bucket-a            -            -          ''   readFiles,listBuckets,readBucketEncryption
            {key_id_3}   goodKeyName-Four       -                      -            -          ''   {all_capabilities}
            {key_id_4}   goodKeyName-Five       {deleted_bucket:20s}   -            -          ''   readFiles,listBuckets
            """

        self._run_command(['key', 'list'], expected_list_keys_out, '', 0, format_vars=key_ids)
        self._run_command(
            ['key', 'list', '--long'], expected_list_keys_out_long, '', 0, format_vars=key_ids
        )

        self._run_command(
            ['list-keys'],
            expected_list_keys_out,
            'WARNING: `list-keys` command is deprecated. Use `key list` instead.\n',
            0,
            format_vars=key_ids,
        )
        self._run_command(
            ['list-keys', '--long'],
            expected_list_keys_out_long,
            'WARNING: `list-keys` command is deprecated. Use `key list` instead.\n',
            0,
            format_vars=key_ids,
        )

        # authorize and make calls using application key with no restrictions
        self._run_command(['account', 'authorize', key_id_0, key_0], None, '', 0)
        self._run_command(
            ['bucket', 'list'],
            f'{bucket_0}  allPublic   my-bucket-a\n{bucket_2}  allPublic   my-bucket-c\n',
            '',
            0,
        )

        expected_json = {
            'accountId': self.account_id,
            'bucketId': bucket_0,
            'bucketInfo': {},
            'bucketName': 'my-bucket-a',
            'bucketType': 'allPublic',
            'corsRules': [],
            'defaultServerSideEncryption': {'mode': None},
            'lifecycleRules': [],
            'options': self._bucket_options,
            'revision': self._bucket_revision(0),
        }
        self._run_command(['bucket', 'get', 'my-bucket-a'], expected_json_in_stdout=expected_json)

        # authorize and make calls using an application key with bucket restrictions
        self._run_command(['account', 'authorize', key_id_1, key_1], None, '', 0)

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
            'bucketId': bucket_0,
            'bucketInfo': {},
            'bucketName': 'my-bucket-a',
            'bucketType': 'allPublic',
            'corsRules': [],
            'defaultServerSideEncryption': {'algorithm': 'AES256', 'mode': 'SSE-B2'},
            'lifecycleRules': [],
            'options': self._bucket_options,
            'revision': self._bucket_revision(0),
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

        bucket_0 = self._create_bucket('my-bucket-a', 'allPublic')
        bucket_1 = self._create_bucket('my-bucket-b', 'allPublic')
        self._create_bucket('my-bucket-c', 'allPublic')

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
        key_id, key = self._create_key(
            [
                'key',
                'create',
                '--bucket',
                'my-bucket-a',
                '--bucket',
                'my-bucket-b',
                'goodKeyName',
                capabilities_with_commas,
            ]
        )

        # List keys
        expected_list_keys_out = f'{key_id}   goodKeyName\n'

        expected_list_keys_out_long = """
            {key_id}   goodKeyName            my-bucket-a, my-bucket-b   -            -          ''   readFiles,listBuckets
            """

        self._run_command(['key', 'list'], expected_list_keys_out, '', 0)
        self._run_command(
            ['key', 'list', '--long'],
            expected_list_keys_out_long,
            '',
            0,
            format_vars={'key_id': key_id},
        )

        # authorize and make calls using an application key with bucket restrictions
        self._run_command(['account', 'authorize', key_id, key], None, '', 0)

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
                'options': self._bucket_options,
                'revision': self._bucket_revision(0),
            }

        self._run_command(
            ['bucket', 'get', 'my-bucket-a'],
            expected_json_in_stdout=_get_expected_json(bucket_0, 'my-bucket-a'),
        )

        self._run_command(
            ['bucket', 'get', 'my-bucket-b'],
            expected_json_in_stdout=_get_expected_json(bucket_1, 'my-bucket-b'),
        )

        self._run_command(
            ['ls', '--json', *self.b2_uri_args('my-bucket-c')],
            '',
            "ERROR: Application key is restricted to buckets: ['my-bucket-a', 'my-bucket-b']\n",
            1,
        )

    def test_bucket_info_from_json(self):
        self._authorize_account()
        bucket_0 = self._create_bucket('my-bucket', 'allPublic')

        bucket_info = {'color': 'blue'}

        expected_json = {
            'accountId': self.account_id,
            'bucketId': bucket_0,
            'bucketInfo': {'color': 'blue'},
            'bucketName': 'my-bucket',
            'bucketType': 'allPrivate',
            'corsRules': [],
            'defaultServerSideEncryption': {'algorithm': 'AES256', 'mode': 'SSE-B2'},
            'lifecycleRules': [],
            'options': self._bucket_options,
            'revision': self._bucket_revision(1),
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
        self._create_bucket('my-bucket', 'allPublic')

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
            upload_id, upload_ts = self._newest_version('my-bucket', 'file1.txt')

            # Hide file
            self._run_command(
                ['file', 'hide', 'b2://my-bucket/file1.txt'],
            )

            hide_id, upload_id = (v.id_ for v in self._versions('my-bucket', 'file1.txt'))
            # Delete one file version
            self._run_command(['rm', f'b2id://{hide_id}'])
            # Delete one file version
            self._run_command(['rm', f'b2id://{upload_id}'])

    def test_hide_file_legacy_syntax(self):
        self._authorize_account()
        bucket_0 = self._create_bucket('my-bucket', 'allPublic')

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
            upload_id, upload_ts = self._newest_version('my-bucket', 'file1.txt')

            # Get file info
            expected_json = {
                'accountId': self.account_id,
                'action': 'upload',
                'bucketId': bucket_0,
                'size': 11,
                'contentSha1': '2aae6c35c94fcfb415dbe95f408b9ce91ee846ed',
                'contentType': self._b2('b2/x-auto', 'text/plain'),
                'fileId': upload_id,
                'fileInfo': {
                    'src_last_modified_millis': '1500111222000',
                    'b2-cache-control': 'private, max-age=3600',
                },
                'fileName': 'file1.txt',
                'serverSideEncryption': {'algorithm': 'AES256', 'mode': 'SSE-B2'},
                'uploadTimestamp': upload_ts,
            }

            self._run_command(
                ['file', 'info', f'b2id://{upload_id}'],
                expected_json_in_stdout=expected_json,
            )

            # Hide the file
            _, stdout, _ = self._run_command(['file', 'hide', 'b2://my-bucket/file1.txt'])
            hide_id, hide_ts = self._newest_version('my-bucket', 'file1.txt')
            expected_json = {
                'action': 'hide',
                'contentSha1': self._hide_marker_sha1,
                'fileId': hide_id,
                'fileInfo': {},
                'fileName': 'file1.txt',
                'serverSideEncryption': {'mode': 'none'},
                'size': 0,
                'uploadTimestamp': hide_ts,
            }

            self._assert_json_in_stdout(expected_json, stdout)

    def test_files(self):
        self._authorize_account()
        bucket_0 = self._create_bucket('my-bucket', 'allPublic')

        with TempDir() as temp_dir:
            local_file1 = self._make_local_file(temp_dir, 'file1.txt')
            # For this test, use a mod time without millis.  My mac truncates
            # millis and just leaves seconds.
            mod_time = 1500111222
            os.utime(local_file1, (mod_time, mod_time))
            self.assertEqual(1500111222, os.path.getmtime(local_file1))

            # Upload a file
            _, stdout, _ = self._run_command(
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
            upload_id, upload_ts = self._newest_version('my-bucket', 'file1.txt')
            upload_id, upload_ts = self._newest_version('my-bucket', 'file1.txt')
            expected_stdout = """
            URL by file name: {download_url}/file/my-bucket/file1.txt
            URL by fileId: {download_url}/b2api/vx/b2_download_file_by_id?fileId={upload_id}"""
            expected_json = {
                'action': 'upload',
                'contentSha1': '2aae6c35c94fcfb415dbe95f408b9ce91ee846ed',
                'contentType': self._b2('b2/x-auto', 'text/plain'),
                'fileId': upload_id,
                'fileInfo': {
                    'src_last_modified_millis': '1500111222000',
                    'b2-cache-control': 'private, max-age=3600',
                },
                'fileName': 'file1.txt',
                'serverSideEncryption': {'algorithm': 'AES256', 'mode': 'SSE-B2'},
                'size': 11,
                'uploadTimestamp': upload_ts,
            }

            self._assert_json_in_stdout(expected_json, stdout)
            self.assertIn(
                self._normalize_expected_output(
                    expected_stdout,
                    {'download_url': self.account_info.get_download_url(), 'upload_id': upload_id},
                ),
                stdout,
            )

            # Get file info
            mod_time_str = str(file_mod_time_millis(local_file1))
            expected_json = {
                'accountId': self.account_id,
                'action': 'upload',
                'bucketId': bucket_0,
                'size': 11,
                'contentSha1': '2aae6c35c94fcfb415dbe95f408b9ce91ee846ed',
                'contentType': self._b2('b2/x-auto', 'text/plain'),
                'fileId': upload_id,
                'fileInfo': {
                    'src_last_modified_millis': '1500111222000',
                    'b2-cache-control': 'private, max-age=3600',
                },
                'fileName': 'file1.txt',
                'serverSideEncryption': {'algorithm': 'AES256', 'mode': 'SSE-B2'},
                'uploadTimestamp': upload_ts,
            }

            self._run_command(
                ['file', 'info', f'b2id://{upload_id}'],
                expected_json_in_stdout=expected_json,
            )

            # Hide the file
            _, stdout, _ = self._run_command(['file', 'hide', 'b2://my-bucket/file1.txt'])
            hide_id, hide_ts = self._newest_version('my-bucket', 'file1.txt')
            expected_json = {
                'action': 'hide',
                'contentSha1': self._hide_marker_sha1,
                'fileId': hide_id,
                'fileInfo': {},
                'fileName': 'file1.txt',
                'serverSideEncryption': {'mode': 'none'},
                'size': 0,
                'uploadTimestamp': hide_ts,
            }

            self._assert_json_in_stdout(expected_json, stdout)

            # List the file versions
            expected_json = [
                {
                    'action': 'hide',
                    'contentSha1': self._hide_marker_sha1,
                    'fileId': hide_id,
                    'fileInfo': {},
                    'fileName': 'file1.txt',
                    'serverSideEncryption': {'mode': 'none'},
                    'size': 0,
                    'uploadTimestamp': hide_ts,
                },
                {
                    'action': 'upload',
                    'contentSha1': '2aae6c35c94fcfb415dbe95f408b9ce91ee846ed',
                    'contentType': self._b2('b2/x-auto', 'text/plain'),
                    'fileId': upload_id,
                    'fileInfo': {
                        'src_last_modified_millis': str(mod_time_str),
                        'b2-cache-control': 'private, max-age=3600',
                    },
                    'fileName': 'file1.txt',
                    'serverSideEncryption': {'algorithm': 'AES256', 'mode': 'SSE-B2'},
                    'size': 11,
                    'uploadTimestamp': upload_ts,
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
            expected_json = {'action': 'delete', 'fileId': hide_id, 'fileName': 'file1.txt'}

            self._run_command(
                ['delete-file-version', 'file1.txt', hide_id],
                expected_stderr='WARNING: `delete-file-version` command is deprecated. Use `rm` instead.\n',
                expected_json_in_stdout=expected_json,
            )

            # Delete one file version, not passing the name in
            expected_json = {'action': 'delete', 'fileId': upload_id, 'fileName': 'file1.txt'}

            self._run_command(
                ['delete-file-version', upload_id],
                expected_stderr='WARNING: `delete-file-version` command is deprecated. Use `rm` instead.\n',
                expected_json_in_stdout=expected_json,
            )

    def test_files_encrypted(self):
        self._authorize_account()
        bucket_0 = self._create_bucket('my-bucket', 'allPublic')

        with TempDir() as temp_dir:
            local_file1 = self._make_local_file(temp_dir, 'file1.txt')
            # For this test, use a mod time without millis.  My mac truncates
            # millis and just leaves seconds.
            mod_time = 1500111222
            os.utime(local_file1, (mod_time, mod_time))
            self.assertEqual(1500111222, os.path.getmtime(local_file1))

            # Upload a file
            _, stdout, _ = self._run_command(
                [
                    'file',
                    'upload',
                    '--no-progress',
                    '--destination-server-side-encryption=SSE-B2',
                    'my-bucket',
                    local_file1,
                    'file1.txt',
                ],
                remove_version=True,
            )
            upload_id, upload_ts = self._newest_version('my-bucket', 'file1.txt')
            expected_stdout = """
            URL by file name: {download_url}/file/my-bucket/file1.txt
            URL by fileId: {download_url}/b2api/vx/b2_download_file_by_id?fileId={upload_id}"""
            expected_json = {
                'action': 'upload',
                'contentSha1': '2aae6c35c94fcfb415dbe95f408b9ce91ee846ed',
                'contentType': self._b2('b2/x-auto', 'text/plain'),
                'fileId': upload_id,
                'fileInfo': {'src_last_modified_millis': '1500111222000'},
                'fileName': 'file1.txt',
                'serverSideEncryption': {'algorithm': 'AES256', 'mode': 'SSE-B2'},
                'size': 11,
                'uploadTimestamp': upload_ts,
            }

            self._assert_json_in_stdout(expected_json, stdout)
            self.assertIn(
                self._normalize_expected_output(
                    expected_stdout,
                    {'download_url': self.account_info.get_download_url(), 'upload_id': upload_id},
                ),
                stdout,
            )

            # Get file info
            mod_time_str = str(file_mod_time_millis(local_file1))
            expected_json = {
                'accountId': self.account_id,
                'action': 'upload',
                'bucketId': bucket_0,
                'size': 11,
                'contentSha1': '2aae6c35c94fcfb415dbe95f408b9ce91ee846ed',
                'contentType': self._b2('b2/x-auto', 'text/plain'),
                'fileId': upload_id,
                'fileInfo': {'src_last_modified_millis': '1500111222000'},
                'fileName': 'file1.txt',
                'serverSideEncryption': {'algorithm': 'AES256', 'mode': 'SSE-B2'},
                'uploadTimestamp': upload_ts,
            }

            self._run_command(
                ['file', 'info', f'b2id://{upload_id}'],
                expected_json_in_stdout=expected_json,
            )

            self._run_command(
                ['file-info', f'b2id://{upload_id}'],
                expected_stderr='WARNING: `file-info` command is deprecated. Use `file info` instead.\n',
                expected_json_in_stdout=expected_json,
            )

            self._run_command(
                ['get-file-info', upload_id],
                expected_stderr='WARNING: `get-file-info` command is deprecated. Use `file info` instead.\n',
                expected_json_in_stdout=expected_json,
            )

            # Download by name
            local_download1 = os.path.join(temp_dir, 'download1.txt')
            expected_stdout_template = """
            File name:           file1.txt
            File id:             {file_id}
            Output file path:    {output_path}
            File size:           11
            Content type:        {content_type}
            Content sha1:        2aae6c35c94fcfb415dbe95f408b9ce91ee846ed
            Encryption:          mode=SSE-B2, algorithm=AES256
            Retention:           none
            Legal hold:          <unset>
            INFO src_last_modified_millis: 1500111222000
            Checksum matches
            Download finished
            """
            expected_stdout = expected_stdout_template.format(
                output_path=pathlib.Path(local_download1).resolve(),
                file_id=upload_id,
                content_type=self._b2('b2/x-auto', 'text/plain'),
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
                output_path=pathlib.Path(local_download2).resolve(),
                file_id=upload_id,
                content_type=self._b2('b2/x-auto', 'text/plain'),
            )
            self._run_command(
                ['file', 'download', '--no-progress', f'b2id://{upload_id}', local_download2],
                expected_stdout,
                '',
                0,
            )
            self.assertEqual(b'hello world', self._read_file(local_download2))

            # Hide the file
            _, stdout, _ = self._run_command(['file', 'hide', 'b2://my-bucket/file1.txt'])
            hide_id, hide_ts = self._newest_version('my-bucket', 'file1.txt')
            expected_json = {
                'action': 'hide',
                'contentSha1': self._hide_marker_sha1,
                'fileId': hide_id,
                'fileInfo': {},
                'fileName': 'file1.txt',
                'serverSideEncryption': {'mode': 'none'},
                'size': 0,
                'uploadTimestamp': hide_ts,
            }

            self._assert_json_in_stdout(expected_json, stdout)

            # List the file versions
            expected_json = [
                {
                    'action': 'hide',
                    'contentSha1': self._hide_marker_sha1,
                    'fileId': hide_id,
                    'fileInfo': {},
                    'fileName': 'file1.txt',
                    'serverSideEncryption': {'mode': 'none'},
                    'size': 0,
                    'uploadTimestamp': hide_ts,
                },
                {
                    'action': 'upload',
                    'contentSha1': '2aae6c35c94fcfb415dbe95f408b9ce91ee846ed',
                    'contentType': self._b2('b2/x-auto', 'text/plain'),
                    'fileId': upload_id,
                    'fileInfo': {'src_last_modified_millis': str(mod_time_str)},
                    'fileName': 'file1.txt',
                    'serverSideEncryption': {'algorithm': 'AES256', 'mode': 'SSE-B2'},
                    'size': 11,
                    'uploadTimestamp': upload_ts,
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
            expected_json = {'action': 'delete', 'fileId': hide_id, 'fileName': 'file1.txt'}

            self._run_command(
                ['delete-file-version', 'file1.txt', hide_id],
                expected_stderr='WARNING: `delete-file-version` command is deprecated. Use `rm` instead.\n',
                expected_json_in_stdout=expected_json,
            )

            # Delete one file version, not passing the name in
            expected_json = {'action': 'delete', 'fileId': upload_id, 'fileName': 'file1.txt'}

            self._run_command(
                ['delete-file-version', upload_id],
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
            file_id, _ = self._newest_version('my-bucket', source_filename)

            b2uri = (
                f'b2://my-bucket/{source_filename}'
                if download_by == 'name'
                else f'b2id://{file_id}'
            )
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
        bucket_id = self._create_my_bucket()
        authorizations = self._record_download_authorizations()
        _, stdout, _ = self._run_command(['bucket', 'get-download-auth', 'my-bucket'], None, '', 0)
        [(request, answer)] = authorizations
        self.assertEqual((bucket_id, '', 86400), request)
        self.assertEqual(f"{answer['authorizationToken']}\n", stdout)

    def test_get_download_auth_explicit(self):
        self._authorize_account()
        bucket_id = self._create_my_bucket()
        authorizations = self._record_download_authorizations()
        _, stdout, _ = self._run_command(
            [
                'bucket',
                'get-download-auth',
                '--prefix',
                'prefix',
                '--duration',
                '12345',
                'my-bucket',
            ],
            None,
            '',
            0,
        )
        [(request, answer)] = authorizations
        self.assertEqual((bucket_id, 'prefix', 12345), request)
        self.assertEqual(f"{answer['authorizationToken']}\n", stdout)

    def test_get_download_auth_url(self):
        self._authorize_account()
        bucket_id = self._create_my_bucket()
        authorizations = self._record_download_authorizations()
        _, stdout, _ = self._run_command(
            ['get-download-url-with-auth', '--duration', '12345', 'my-bucket', 'my-file'],
            None,
            'WARNING: `get-download-url-with-auth` command is deprecated. Use `file url` instead.\n',
            0,
        )
        _, stdout_2, _ = self._run_command(
            ['file', 'url', '--with-auth', '--duration', '12345', 'b2://my-bucket/my-file'],
            None,
            '',
            0,
        )
        download_url = self.account_info.get_download_url()
        self.assertEqual(2, len(authorizations))
        for (request, answer), output in zip(authorizations, (stdout, stdout_2)):
            self.assertEqual((bucket_id, 'my-file', 12345), request)
            self.assertEqual(
                f"{download_url}/file/my-bucket/my-file?Authorization={answer['authorizationToken']}\n",
                output,
            )

    def test_get_download_auth_url_with_encoding(self):
        self._authorize_account()
        bucket_id = self._create_my_bucket()
        authorizations = self._record_download_authorizations()
        _, stdout, _ = self._run_command(
            ['get-download-url-with-auth', '--duration', '12345', 'my-bucket', '\u81ea'],
            None,
            'WARNING: `get-download-url-with-auth` command is deprecated. Use `file url` instead.\n',
            0,
        )
        _, stdout_2, _ = self._run_command(
            ['file', 'url', '--with-auth', '--duration', '12345', 'b2://my-bucket/\u81ea'],
            None,
            '',
            0,
        )
        download_url = self.account_info.get_download_url()
        self.assertEqual(2, len(authorizations))
        for (request, answer), output in zip(authorizations, (stdout, stdout_2)):
            self.assertEqual((bucket_id, '\u81ea', 12345), request)
            self.assertEqual(
                f"{download_url}/file/my-bucket/%E8%87%AA?Authorization={answer['authorizationToken']}\n",
                output,
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
            _, stdout, _ = self._run_command(
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
                remove_version=True,
            )
            upload_id, upload_ts = self._newest_version('my-bucket', 'test.txt')
            mod_time_str = str(file_mod_time_millis(file_path))
            expected_stdout = """
            URL by file name: {download_url}/file/my-bucket/test.txt
            URL by fileId: {download_url}/b2api/vx/b2_download_file_by_id?fileId={upload_id}"""
            expected_json = {
                'action': 'upload',
                'contentSha1': 'none',
                'contentType': self._b2('b2/x-auto', 'text/plain'),
                'fileId': upload_id,
                'fileInfo': {
                    'large_file_sha1': 'cc8954ec25e0c564b6a693fb22200e4f832c18e8',
                    'src_last_modified_millis': str(mod_time_str),
                },
                'fileName': 'test.txt',
                'serverSideEncryption': {'algorithm': 'AES256', 'mode': 'SSE-B2'},
                'size': 600,
                'uploadTimestamp': upload_ts,
            }

            self._assert_json_in_stdout(expected_json, stdout)
            self.assertIn(
                self._normalize_expected_output(
                    expected_stdout,
                    {'download_url': self.account_info.get_download_url(), 'upload_id': upload_id},
                ),
                stdout,
            )

    def test_upload_large_file_encrypted(self):
        self._authorize_account()
        self._create_bucket('my-bucket', 'allPublic')
        min_part_size = self.account_info.get_recommended_part_size()
        file_size = min_part_size * 3

        with TempDir() as temp_dir:
            file_path = os.path.join(temp_dir, 'test.txt')
            text = '*' * file_size
            with open(file_path, 'wb') as f:
                f.write(text.encode('utf-8'))
            _, stdout, _ = self._run_command(
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
                remove_version=True,
            )
            upload_id, upload_ts = self._newest_version('my-bucket', 'test.txt')
            mod_time_str = str(file_mod_time_millis(file_path))
            expected_stdout = """
            URL by file name: {download_url}/file/my-bucket/test.txt
            URL by fileId: {download_url}/b2api/vx/b2_download_file_by_id?fileId={upload_id}"""
            expected_json = {
                'action': 'upload',
                'contentSha1': 'none',
                'contentType': self._b2('b2/x-auto', 'text/plain'),
                'fileId': upload_id,
                'fileInfo': {
                    'large_file_sha1': 'cc8954ec25e0c564b6a693fb22200e4f832c18e8',
                    'src_last_modified_millis': str(mod_time_str),
                },
                'fileName': 'test.txt',
                'serverSideEncryption': {'algorithm': 'AES256', 'mode': 'SSE-B2'},
                'size': 600,
                'uploadTimestamp': upload_ts,
            }

            self._assert_json_in_stdout(expected_json, stdout)
            self.assertIn(
                self._normalize_expected_output(
                    expected_stdout,
                    {'download_url': self.account_info.get_download_url(), 'upload_id': upload_id},
                ),
                stdout,
            )

    def test_upload_incremental(self):
        self._authorize_account()
        self._create_bucket('my-bucket', 'allPublic')
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
            'accountAuthToken': self.account_info.get_account_auth_token(),
            'accountFilePath': getattr(
                self.account_info, 'filename', None
            ),  # missing in StubAccountInfo in tests
            'accountId': self.account_id,
            'allowed': {
                'buckets': None,
                # The master key's capabilities: RawSimulator grants b2sdk's ALL_CAPABILITIES, B2
                # every capability it has, which b2sdk's list may not name yet.
                'capabilities': self._b2(
                    sorted(ALL_CAPABILITIES),
                    sorted(self._master_b2_api().account_info.get_allowed()['capabilities']),
                ),
                'namePrefix': None,
            },
            'apiUrl': self.account_info.get_api_url(),
            'applicationKey': self.master_key,
            'downloadUrl': self.account_info.get_download_url(),
            's3endpoint': self.account_info.get_s3_api_url(),
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
        bucket_0 = self._create_my_bucket()
        expected_json = {
            'accountId': self.account_id,
            'bucketId': bucket_0,
            'bucketInfo': {},
            'bucketName': 'my-bucket',
            'bucketType': 'allPublic',
            'corsRules': [],
            'defaultServerSideEncryption': {'algorithm': 'AES256', 'mode': 'SSE-B2'},
            'lifecycleRules': [],
            'options': self._bucket_options,
            'revision': self._bucket_revision(0),
        }
        self._run_command(
            ['bucket', 'get', 'my-bucket'],
            expected_json_in_stdout=expected_json,
        )

    def test_get_bucket_empty_show_size(self):
        self._authorize_account()
        bucket_0 = self._create_my_bucket()
        expected_json = {
            'accountId': self.account_id,
            'bucketId': bucket_0,
            'bucketInfo': {},
            'bucketName': 'my-bucket',
            'bucketType': 'allPublic',
            'corsRules': [],
            'defaultServerSideEncryption': {'algorithm': 'AES256', 'mode': 'SSE-B2'},
            'fileCount': 0,
            'lifecycleRules': [],
            'options': self._bucket_options,
            'revision': self._bucket_revision(0),
            'totalSize': 0,
        }
        self._run_command(
            ['bucket', 'get', '--show-size', 'my-bucket'],
            expected_json_in_stdout=expected_json,
        )

    def test_get_bucket_one_item_show_size(self):
        self._authorize_account()
        bucket_0 = self._create_my_bucket()
        with TempDir() as temp_dir:
            # Upload a standard test file.
            local_file1 = self._make_local_file(temp_dir, 'file1.txt')
            mod_time_str = str(file_mod_time_millis(local_file1))
            _, stdout, _ = self._run_command(
                ['file', 'upload', '--no-progress', 'my-bucket', local_file1, 'file1.txt'],
                remove_version=True,
            )
            upload_id, upload_ts = self._newest_version('my-bucket', 'file1.txt')
            expected_stdout = (
                f'URL by file name: {self.account_info.get_download_url()}/file/my-bucket/file1.txt\n'
                f'URL by fileId: {self.account_info.get_download_url()}'
                f'/b2api/vx/b2_download_file_by_id?fileId={upload_id}'
            )
            expected_json = {
                'action': 'upload',
                'contentSha1': '2aae6c35c94fcfb415dbe95f408b9ce91ee846ed',
                'contentType': self._b2('b2/x-auto', 'text/plain'),
                'fileId': upload_id,
                'fileInfo': {'src_last_modified_millis': str(mod_time_str)},
                'fileName': 'file1.txt',
                'serverSideEncryption': {'algorithm': 'AES256', 'mode': 'SSE-B2'},
                'size': 11,
                'uploadTimestamp': upload_ts,
            }
            self._assert_json_in_stdout(expected_json, stdout)
            self.assertIn(expected_stdout, stdout)

            # Now check the output of `bucket get` against the canon.
            expected_json = {
                'accountId': self.account_id,
                'bucketId': bucket_0,
                'bucketInfo': {},
                'bucketName': 'my-bucket',
                'bucketType': 'allPublic',
                'corsRules': [],
                'defaultServerSideEncryption': {'algorithm': 'AES256', 'mode': 'SSE-B2'},
                'fileCount': 1,
                'lifecycleRules': [],
                'options': self._bucket_options,
                'revision': self._bucket_revision(0),
                'totalSize': 11,
            }
            self._run_command(
                ['bucket', 'get', '--show-size', 'my-bucket'],
                expected_json_in_stdout=expected_json,
            )

    def test_get_bucket_with_versions(self):
        self._authorize_account()
        bucket_0 = self._create_my_bucket()

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
            'bucketId': bucket_0,
            'bucketInfo': {},
            'bucketName': 'my-bucket',
            'bucketType': 'allPublic',
            'corsRules': [],
            'defaultServerSideEncryption': {'algorithm': 'AES256', 'mode': 'SSE-B2'},
            'fileCount': 10,
            'lifecycleRules': [],
            'options': self._bucket_options,
            'revision': self._bucket_revision(0),
            'totalSize': 40,
        }
        self._run_command(
            ['bucket', 'get', '--show-size', 'my-bucket'],
            expected_json_in_stdout=expected_json,
        )

    def test_get_bucket_with_folders(self):
        self._authorize_account()
        bucket_0 = self._create_my_bucket()

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
            'bucketId': bucket_0,
            'bucketInfo': {},
            'bucketName': 'my-bucket',
            'bucketType': 'allPublic',
            'corsRules': [],
            'defaultServerSideEncryption': {'algorithm': 'AES256', 'mode': 'SSE-B2'},
            'fileCount': 20,
            'lifecycleRules': [],
            'options': self._bucket_options,
            'revision': self._bucket_revision(0),
            'totalSize': 90,
        }
        self._run_command(
            ['bucket', 'get', '--show-size', 'my-bucket'],
            expected_json_in_stdout=expected_json,
        )

    def test_get_bucket_with_hidden(self):
        self._authorize_account()
        bucket_0 = self._create_my_bucket()

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
            'bucketId': bucket_0,
            'bucketInfo': {},
            'bucketName': 'my-bucket',
            'bucketType': 'allPublic',
            'corsRules': [],
            'defaultServerSideEncryption': {'algorithm': 'AES256', 'mode': 'SSE-B2'},
            'fileCount': 9,
            'lifecycleRules': [],
            'options': self._bucket_options,
            'revision': self._bucket_revision(0),
            'totalSize': 24,
        }
        self._run_command(
            ['bucket', 'get', '--show-size', 'my-bucket'],
            expected_json_in_stdout=expected_json,
        )

    @pytest.mark.apiver(from_ver=4)
    def test_unhide_b2id(self):
        self._authorize_account()
        bucket_0 = self._create_my_bucket()

        bucket = self.b2_api.get_bucket_by_name('my-bucket')
        stdout, stderr = self._get_stdouterr()
        console_tool = self.console_tool_class(stdout, stderr)

        file_version = bucket.upload(UploadSourceBytes(b'test'), 'test.txt')
        bucket.hide_file('test.txt')

        console_tool.run_command(['b2', 'file', 'unhide', f'b2id://{file_version.id_}'])

        expected_json = {
            'accountId': self.account_id,
            'bucketId': bucket_0,
            'bucketInfo': {},
            'bucketName': 'my-bucket',
            'bucketType': 'allPublic',
            'corsRules': [],
            'defaultServerSideEncryption': {'algorithm': 'AES256', 'mode': 'SSE-B2'},
            'fileCount': 1,
            'lifecycleRules': [],
            'options': self._bucket_options,
            'revision': self._bucket_revision(0),
            'totalSize': 4,
        }
        self._run_command(
            ['bucket', 'get', '--show-size', 'my-bucket'],
            expected_json_in_stdout=expected_json,
        )

    def test_get_bucket_complex(self):
        self._authorize_account()
        bucket_0 = self._create_my_bucket()

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
            'bucketId': bucket_0,
            'bucketInfo': {},
            'bucketName': 'my-bucket',
            'bucketType': 'allPublic',
            'corsRules': [],
            'defaultServerSideEncryption': {'algorithm': 'AES256', 'mode': 'SSE-B2'},
            'fileCount': 28,
            'lifecycleRules': [],
            'options': self._bucket_options,
            'revision': self._bucket_revision(0),
            'totalSize': 99,
        }
        self._run_command(
            ['bucket', 'get', '--show-size', 'my-bucket'],
            expected_json_in_stdout=expected_json,
        )

    def test_get_bucket_encrypted(self):
        self._authorize_account()
        bucket_0 = self._create_bucket(
            'my-bucket',
            '--default-server-side-encryption=SSE-B2',
            '--default-server-side-encryption-algorithm=AES256',
            'allPublic',
        )
        expected_json = {
            'accountId': self.account_id,
            'bucketId': bucket_0,
            'bucketInfo': {},
            'bucketName': 'my-bucket',
            'bucketType': 'allPublic',
            'corsRules': [],
            'defaultServerSideEncryption': {'algorithm': 'AES256', 'mode': 'SSE-B2'},
            'fileCount': 0,
            'lifecycleRules': [],
            'options': self._bucket_options,
            'revision': self._bucket_revision(0),
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
            upload_id, upload_ts = self._newest_version('my-bucket', 'test-dry-run.txt')
            expected_json = [
                {
                    'action': 'upload',
                    'contentSha1': '2aae6c35c94fcfb415dbe95f408b9ce91ee846ed',
                    'contentType': self._b2('b2/x-auto', 'text/plain'),
                    'fileId': upload_id,
                    'fileInfo': {'src_last_modified_millis': str(mtime)},
                    'fileName': 'test-dry-run.txt',
                    'serverSideEncryption': {'algorithm': 'AES256', 'mode': 'SSE-B2'},
                    'size': 11,
                    'uploadTimestamp': upload_ts,
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

    # Custom upload timestamps are a restricted B2 feature that must be enabled for the account.
    @pytest.mark.b2_simulator_config(customUploadTimestampsEnabled=True)
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

    # Custom upload timestamps are a restricted B2 feature that must be enabled for the account.
    @pytest.mark.b2_simulator_config(customUploadTimestampsEnabled=True)
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
        expect_error=False,
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
            if expect_error:
                self._run_command(
                    command,
                    expected_stdout='',
                    expected_stderr='ERROR: --threads cannot be used with other thread options\n',
                    expected_status=1,
                )
                self.assertEqual([], list(self.b2_api.get_bucket_by_name('my-bucket').ls()))
                return
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
        self._test_sync_threads(threads=1, upload_threads=1, expect_error=True)

    def test_sync_threads_and_sync_threads(self):
        # Using --threads is exclusive with other options
        self._test_sync_threads(threads=1, sync_threads=1, expect_error=True)

    def test_sync_threads_and_download_threads(self):
        # Using --threads is exclusive with other options
        self._test_sync_threads(threads=1, download_threads=1, expect_error=True)

    def test_sync_all_thread_options(self):
        # Using --threads is exclusive with other options
        self._test_sync_threads(
            threads=1, sync_threads=1, download_threads=1, upload_threads=1, expect_error=True
        )

    def test_ls(self):
        self._authorize_account()
        self._create_my_bucket()

        # Check with no files
        self._run_command(['ls', *self.b2_uri_args('my-bucket')], '', '', 0)

        # Create some files, including files in a folder
        bucket = self.b2_api.get_bucket_by_name('my-bucket')
        file_a = bucket.upload(UploadSourceBytes(b''), 'a')
        bucket.upload(UploadSourceBytes(b' '), 'b/b1')
        bucket.upload(UploadSourceBytes(b'   '), 'b/b2')
        file_c_old = bucket.upload(UploadSourceBytes(b'     '), 'c')
        file_c = bucket.upload(UploadSourceBytes(b'      '), 'c')

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
        {a}  upload  {a_time}          0  a
                                                                                          -       -           -         -          0  b/
        {c}  upload  {c_time}          6  c
        """
        # The upload times in the listing are UTC.
        times = {
            name: datetime.datetime.fromtimestamp(
                version.upload_timestamp / 1000, datetime.timezone.utc
            ).strftime('%Y-%m-%d  %H:%M:%S')
            for name, version in (('a', file_a), ('c', file_c), ('c_old', file_c_old))
        }
        listed = {
            'a': f'{file_a.id_:>83}',
            'a_time': times['a'],
            'c': f'{file_c.id_:>83}',
            'c_time': times['c'],
            'c_old': f'{file_c_old.id_:>83}',
            'c_old_time': times['c_old'],
        }
        self._run_command(
            ['ls', '--long', *self.b2_uri_args('my-bucket')],
            expected_stdout,
            '',
            0,
            format_vars=listed,
        )

        # Check long versions output   (The format expects full-length file ids, so it causes whitespace here)
        expected_stdout = """
        {a}  upload  {a_time}          0  a
                                                                                          -       -           -         -          0  b/
        {c}  upload  {c_time}          6  c
        {c_old}  upload  {c_old_time}          5  c
        """
        self._run_command(
            ['ls', '--long', '--versions', *self.b2_uri_args('my-bucket')],
            expected_stdout,
            '',
            0,
            format_vars=listed,
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
        bucket_id = self._create_bucket(bucket_name, 'allPrivate')

        # Create another bucket
        other_bucket_name = 'otherBucket'
        self._run_command_ignore_output(['bucket', 'create', other_bucket_name, 'allPrivate'])

        # Create a key restricted to a bucket
        capabilities = 'listBuckets,readFiles'
        file_prefix = 'some/file/prefix/'
        app_key_id, app_key = self._create_key(
            [
                'key',
                'create',
                '--bucket',
                bucket_name,
                '--name-prefix',
                file_prefix,
                'my-key',
                capabilities,
            ]
        )

        authorizations = self._record_answers('authorize_account')
        self._run_command_ignore_output(['account', 'authorize', app_key_id, app_key])

        # Auth token should be in account info now: the one the server issued for the new key
        [(_, answer)] = authorizations
        self.assertEqual(answer['authorizationToken'], self.account_info.get_account_auth_token())

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
        self._create_bucket('my-bucket', 'allPrivate')

        # Authorizing with the key will fail because the ConsoleTool needs
        # to be able to look up the name of the bucket.
        key_id, key = self._create_key(['key', 'create', 'my-key', 'listFiles'])

        # Authorize with the key, which should result in an error.
        self._run_command(
            ['account', 'authorize', key_id, key],
            '',
            'ERROR: application key has no listBuckets capability, which is required for the b2 command-line tool\n',
            1,
        )

    def test_bucket_missing_for_bucket_key(self):
        # Create a bucket and a key restricted to that bucket.
        self._authorize_account()
        self._create_bucket('my-bucket', 'allPrivate')
        key_id, key = self._create_key(
            ['key', 'create', '--bucket', 'my-bucket', 'my-key', 'listBuckets,listFiles']
        )

        # Get rid of the bucket, leaving the key with a dangling pointer to it.
        self._run_command_ignore_output(['bucket', 'delete', 'my-bucket'])

        # Authorizing with the key will fail because the ConsoleTool needs
        # to be able to look up the name of the bucket.
        self._run_command(
            ['account', 'authorize', key_id, key],
            '',
            "ERROR: unable to authorize account: Application key is restricted to a bucket that doesn't exist\n",
            1,
        )

    def test_ls_for_restricted_bucket(self):
        # Create a bucket and a key restricted to that bucket.
        self._authorize_account()
        self._create_bucket('my-bucket', 'allPrivate')
        key_id, key = self._create_key(
            ['key', 'create', '--bucket', 'my-bucket', 'my-key', 'listBuckets,listFiles']
        )

        # Authorize with the key and list the files
        self._run_command_ignore_output(
            ['account', 'authorize', key_id, key],
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
        self._create_my_bucket()
        bucket = self.b2_api.get_bucket_by_name('my-bucket')
        uploaded = bucket.upload(UploadSourceBytes(b'test'), 'test.txt')
        temporary_dir = TemporaryDirectory()
        self.addCleanup(temporary_dir.cleanup)
        output_path = str(pathlib.Path(temporary_dir.name) / 'download.txt')
        sync_path = str(pathlib.Path(temporary_dir.name) / 'sync')
        commands = [
            [
                'b2',
                'download-file-by-name',
                '--profile',
                'nonexistent',
                'my-bucket',
                'test.txt',
                output_path,
            ],
            [
                'b2',
                'download-file-by-id',
                '--profile',
                'nonexistent',
                uploaded.id_,
                output_path,
            ],
            ['b2', 'sync', '--profile', 'nonexistent', 'b2:my-bucket', sync_path],
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
        self._authorize_account()
        self._create_my_bucket()
        bucket = self.b2_api.get_bucket_by_name('my-bucket')
        uploaded = bucket.upload(UploadSourceBytes(b'test'), 'test.txt')
        temporary_dir = TemporaryDirectory()
        self.addCleanup(temporary_dir.cleanup)
        output_path = str(pathlib.Path(temporary_dir.name) / 'download.txt')
        os.environ[B2_APPLICATION_KEY_ID_ENV_VAR] = self.account_id
        os.environ[B2_APPLICATION_KEY_ENV_VAR] = self.master_key

        command = [
            'b2',
            'download-file-by-id',
            uploaded.id_,
            output_path,
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
        self._create_bucket('my-bucket-0', 'allPrivate')
        self._create_bucket('my-bucket-1', 'allPrivate')

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
        self._create_bucket('my-bucket-0', 'allPublic')
        cc_name = "$'\x1b[31mC\x1b[32mC\x1b[33mI\x1b[0m'"
        escaped_error = "ERROR: unauthorized for application key with capabilities 'listBuckets,readFiles', restricted to buckets ['my-bucket-0'], restricted to files that start with '$'\\x1b[31mC\\x1b[32mC\\x1b[33mI\\x1b[0m'' (unauthorized)\n"

        # Create a key (without listFiles, so that `ls` is refused). B2 refuses listKeys on a
        # bucket-restricted key (https://www.backblaze.com/docs/cloud-storage-application-keys).
        key_id, key = self._create_key(
            [
                'key',
                'create',
                '--bucket',
                'my-bucket-0',
                '--name-prefix',
                cc_name,
                'key1',
                'listBuckets,readFiles',
            ]
        )

        # Authorize with the key
        self._run_command(['account', 'authorize', key_id, key], expected_status=0)

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
        self._create_bucket('my-bucket-0', 'allPrivate')

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
        self._create_bucket('my-bucket-cc', 'allPrivate')

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
        self.bucket_id = self._create_my_bucket()

    def test_cancel_large_file(self):
        file = self.b2_api.services.large_file.start_large_file(
            self.bucket_id, 'file1', 'text/plain', {}
        )

        self._run_command(
            ['file', 'large', 'unfinished', 'cancel', f'b2id://{file.file_id}'],
            f'{file.file_id} canceled\n',
            '',
            0,
        )

    def test_cancel_large_file_deprecated(self):
        file = self.b2_api.services.large_file.start_large_file(
            self.bucket_id, 'file1', 'text/plain', {}
        )

        self._run_command(
            ['cancel-large-file', file.file_id],
            f'{file.file_id} canceled\n',
            'WARNING: `cancel-large-file` command is deprecated. Use `file large unfinished cancel` instead.\n',
            0,
        )

    def test_cancel_all_large_file(self):
        large_files = self.b2_api.services.large_file
        file1 = large_files.start_large_file(self.bucket_id, 'file1', 'text/plain', {})
        file2 = large_files.start_large_file(self.bucket_id, 'file2', 'text/plain', {})

        expected_stdout = """
        {file1} canceled
        {file2} canceled
        """
        file_ids = {'file1': file1.file_id, 'file2': file2.file_id}

        self._run_command(
            ['file', 'large', 'unfinished', 'cancel', 'b2://my-bucket'],
            expected_stdout,
            '',
            0,
            format_vars=file_ids,
        )

    def test_cancel_all_large_file_deprecated(self):
        large_files = self.b2_api.services.large_file
        file1 = large_files.start_large_file(self.bucket_id, 'file1', 'text/plain', {})
        file2 = large_files.start_large_file(self.bucket_id, 'file2', 'text/plain', {})
        expected_stdout = """
        {file1} canceled
        {file2} canceled
        """
        file_ids = {'file1': file1.file_id, 'file2': file2.file_id}

        self._run_command(
            ['cancel-all-unfinished-large-files', 'my-bucket'],
            expected_stdout,
            'WARNING: `cancel-all-unfinished-large-files` command is deprecated. Use `file large unfinished cancel` instead.\n',
            0,
            format_vars=file_ids,
        )

    def test_list_parts_with_none(self):
        file = self.b2_api.services.large_file.start_large_file(
            self.bucket_id, 'file1', 'text/plain', {}
        )
        self._run_command(['file', 'large', 'parts', f'b2id://{file.file_id}'], '', '', 0)

    def test_list_parts_with_none_deprecated(self):
        file = self.b2_api.services.large_file.start_large_file(
            self.bucket_id, 'file1', 'text/plain', {}
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
            self.bucket_id, 'file', 'text/plain', {}
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
            self.bucket_id, 'file', 'text/plain', {}
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
        file1 = self.raw_api.start_large_file(
            api_url, auth_token, self.bucket_id, 'file1', 'text/plain', {}
        )
        file2 = self.raw_api.start_large_file(
            api_url, auth_token, self.bucket_id, 'file2', 'text/plain', {'color': 'blue'}
        )
        file3 = self.raw_api.start_large_file(
            api_url, auth_token, self.bucket_id, 'file3', 'application/json', {}
        )
        expected_stdout = """
        {file1} file1 text/plain
        {file2} file2 text/plain color=blue
        {file3} file3 application/json
        """
        file_ids = {'file1': file1['fileId'], 'file2': file2['fileId'], 'file3': file3['fileId']}

        self._run_command(
            ['file', 'large', 'unfinished', 'list', 'b2://my-bucket'],
            expected_stdout,
            '',
            0,
            format_vars=file_ids,
        )

    def test_list_unfinished_large_files_with_some_deprecated(self):
        api_url = self.account_info.get_api_url()
        auth_token = self.account_info.get_account_auth_token()
        file1 = self.raw_api.start_large_file(
            api_url, auth_token, self.bucket_id, 'file1', 'text/plain', {}
        )
        file2 = self.raw_api.start_large_file(
            api_url, auth_token, self.bucket_id, 'file2', 'text/plain', {'color': 'blue'}
        )
        file3 = self.raw_api.start_large_file(
            api_url, auth_token, self.bucket_id, 'file3', 'application/json', {}
        )
        expected_stdout = """
        {file1} file1 text/plain
        {file2} file2 text/plain color=blue
        {file3} file3 application/json
        """
        file_ids = {'file1': file1['fileId'], 'file2': file2['fileId'], 'file3': file3['fileId']}

        self._run_command(
            ['list-unfinished-large-files', 'my-bucket'],
            expected_stdout,
            'WARNING: `list-unfinished-large-files` command is deprecated. Use `file large unfinished list` instead.\n',
            0,
            format_vars=file_ids,
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
        file_id = self._file_id('my-bucket', 'b/b1/test.csv')
        expected_in_stdout = f"""
        Deletion of file "b/b1/test.csv" ({file_id}) failed: Conflict:
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
        self._create_bucket('my-rm-bucket', 'allPublic')
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
        self._run_command(
            ['version'],
            expected_stdout=f'b2 command line tool, version {VERSION} (b2sdk version {b2sdk_version})\n',
        )
