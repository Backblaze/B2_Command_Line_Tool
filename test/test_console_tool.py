######################################################################
#
# File: test/test_console_tool.py
#
# Copyright 2016 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import os

import six

from .stub_account_info import StubAccountInfo
from .test_base import TestBase
from b2.api import B2Api
from b2.cache import InMemoryCache
from b2.console_tool import ConsoleTool
from b2.raw_simulator import RawSimulator
from b2.upload_source import UploadSourceBytes
from b2.utils import TempDir

try:
    import unittest.mock as mock
except ImportError:
    import mock


class TestConsoleTool(TestBase):
    def setUp(self):
        self.account_info = StubAccountInfo()
        self.cache = InMemoryCache()
        self.raw_api = RawSimulator()
        self.b2_api = B2Api(self.account_info, self.cache, self.raw_api)

    def test_authorize_with_bad_key(self):
        expected_stdout = '''
        Using http://production.example.com
        '''

        expected_stderr = '''
        ERROR: unable to authorize account: Invalid authorization token. Server said: invalid application key: bad-app-key (bad_auth_token)
        '''

        self._run_command(
            ['authorize_account', 'my-account', 'bad-app-key'], expected_stdout, expected_stderr, 1
        )

    def test_authorize_with_good_key(self):
        # Initial condition
        assert self.account_info.get_account_auth_token() is None

        # Authorize an account with a good api key.
        expected_stdout = """
        Using http://production.example.com
        """

        self._run_command(
            ['authorize_account', 'my-account', 'good-app-key'], expected_stdout, '', 0
        )

        # Auth token should be in account info now
        assert self.account_info.get_account_auth_token() is not None

    def test_help_with_bad_args(self):
        expected_stderr = '''

        b2 create_bucket <bucketName> [allPublic | allPrivate]

            Creates a new bucket.  Prints the ID of the bucket created.

        '''

        self._run_command(['create_bucket'], '', expected_stderr, 1)

    def test_clear_account(self):
        # Initial condition
        self._authorize_account()
        assert self.account_info.get_account_auth_token() is not None

        # Clearing the account should remove the auth token
        # from the account info.
        self._run_command(['clear_account'], '', '', 0)
        assert self.account_info.get_account_auth_token() is None

    def test_buckets(self):
        self._authorize_account()

        # Make a bucket with an illegal name
        expected_stdout = 'ERROR: Bad request: illegal bucket name: bad/bucket/name\n'
        self._run_command(['create_bucket', 'bad/bucket/name', 'allPublic'], '', expected_stdout, 1)

        # Make two buckets
        self._run_command(['create_bucket', 'my-bucket', 'allPrivate'], 'bucket_0\n', '', 0)
        self._run_command(['create_bucket', 'your-bucket', 'allPrivate'], 'bucket_1\n', '', 0)

        # Update one of them
        expected_stdout = '''
        {
            "accountId": "my-account",
            "bucketId": "bucket_0",
            "bucketName": "my-bucket",
            "bucketType": "allPublic"
        }
        '''

        self._run_command(['update_bucket', 'my-bucket', 'allPublic'], expected_stdout, '', 0)

        # Make sure they are there
        expected_stdout = '''
        bucket_0  allPublic   my-bucket
        bucket_1  allPrivate  your-bucket
        '''

        self._run_command(['list_buckets'], expected_stdout, '', 0)

        # Delete one
        expected_stdout = '''
        {
            "accountId": "my-account",
            "bucketId": "bucket_1",
            "bucketName": "your-bucket",
            "bucketType": "allPrivate"
        }
        '''

        self._run_command(['delete_bucket', 'your-bucket'], expected_stdout, '', 0)

    def test_cancel_large_file(self):
        self._authorize_account()
        self._create_my_bucket()
        bucket = self.b2_api.get_bucket_by_name('my-bucket')
        file = bucket.start_large_file('file1', 'text/plain', {})
        self._run_command(['cancel_large_file', file.file_id], '9999 canceled\n', '', 0)

    def test_cancel_all_large_file(self):
        self._authorize_account()
        self._create_my_bucket()
        bucket = self.b2_api.get_bucket_by_name('my-bucket')
        bucket.start_large_file('file1', 'text/plain', {})
        bucket.start_large_file('file2', 'text/plain', {})
        expected_stdout = '''
        9999 canceled
        9998 canceled
        '''

        self._run_command(
            ['cancel_all_unfinished_large_files', 'my-bucket'], expected_stdout, '', 0
        )

    def test_files(self):

        self._authorize_account()
        self._run_command(['create_bucket', 'my-bucket', 'allPublic'], 'bucket_0\n', '', 0)

        with TempDir() as temp_dir:
            local_file1 = self._make_local_file(temp_dir, 'file1.txt')

            # Upload a file
            expected_stdout = '''
            URL by file name: http://download.example.com/file/my-bucket/file1.txt
            URL by fileId: http://download.example.com/b2api/v1/b2_download_file_by_id?fileId=9999
            {
              "action": "upload",
              "fileId": "9999",
              "fileName": "file1.txt",
              "size": 11,
              "uploadTimestamp": 5000
            }
            '''

            self._run_command(
                [
                    'upload_file', '--noProgress', 'my-bucket', local_file1, 'file1.txt'
                ], expected_stdout, '', 0
            )

            # Download by name
            local_download1 = os.path.join(temp_dir, 'download1.txt')
            expected_stdout = '''
            File name:    file1.txt
            File id:      9999
            File size:    11
            Content type: b2/x-auto
            Content sha1: 2aae6c35c94fcfb415dbe95f408b9ce91ee846ed
            checksum matches
            '''

            self._run_command(
                [
                    'download_file_by_name', '--noProgress', 'my-bucket', 'file1.txt',
                    local_download1
                ], expected_stdout, '', 0
            )
            self.assertEquals(six.b('hello world'), self._read_file(local_download1))

            # Download file by ID.  (Same expected output as downloading by name)
            local_download2 = os.path.join(temp_dir, 'download2.txt')
            self._run_command(
                [
                    'download_file_by_id', '--noProgress', '9999', local_download2
                ], expected_stdout, '', 0
            )
            self.assertEquals(six.b('hello world'), self._read_file(local_download2))

            # Hide the file
            expected_stdout = '''
            {
              "action": "hide",
              "fileId": "9998",
              "fileName": "file1.txt",
              "size": 0,
              "uploadTimestamp": 5001
            }
            '''

            self._run_command(['hide_file', 'my-bucket', 'file1.txt'], expected_stdout, '', 0)

            # List the file versions
            expected_stdout = '''
            {
              "files": [
                {
                  "action": "hide",
                  "contentSha1": "none",
                  "contentType": null,
                  "fileId": "9998",
                  "fileInfo": {},
                  "fileName": "file1.txt",
                  "size": 0,
                  "uploadTimestamp": 5001
                },
                {
                  "action": "upload",
                  "contentSha1": "2aae6c35c94fcfb415dbe95f408b9ce91ee846ed",
                  "contentType": "b2/x-auto",
                  "fileId": "9999",
                  "fileInfo": {},
                  "fileName": "file1.txt",
                  "size": 11,
                  "uploadTimestamp": 5000
                }
              ],
              "nextFileId": null,
              "nextFileName": null
            }
            '''

            self._run_command(['list_file_versions', 'my-bucket'], expected_stdout, '', 0)

            # List the file names
            expected_stdout = '''
            {
              "files": [],
              "nextFileName": null
            }
            '''

            self._run_command(['list_file_names', 'my-bucket'], expected_stdout, '', 0)

            # Delete one file version, passing the name in
            expected_stdout = '''
            {
              "action": "delete",
              "fileId": "9998",
              "fileName": "file1.txt"
            }
            '''

            self._run_command(['delete_file_version', 'file1.txt', '9998'], expected_stdout, '', 0)

            # Delete one file version, not passing the name in
            expected_stdout = '''
            {
              "action": "delete",
              "fileId": "9999",
              "fileName": "file1.txt"
            }
            '''

            self._run_command(['delete_file_version', '9999'], expected_stdout, '', 0)

    def test_list_parts_with_none(self):
        self._authorize_account()
        self._create_my_bucket()
        bucket = self.b2_api.get_bucket_by_name('my-bucket')
        file = bucket.start_large_file('file', 'text/plain', {})
        self._run_command(['list_parts', file.file_id], '', '', 0)

    def test_list_parts_with_parts(self):
        self._authorize_account()
        self._create_my_bucket()
        bucket = self.b2_api.get_bucket_by_name('my-bucket')
        file = bucket.start_large_file('file', 'text/plain', {})
        content = six.b('hello world')
        large_file_upload_state = mock.MagicMock()
        large_file_upload_state.has_error.return_value = False
        bucket._upload_part(
            file.file_id, 1, (0, 11), UploadSourceBytes(content), large_file_upload_state
        )
        bucket._upload_part(
            file.file_id, 3, (0, 11), UploadSourceBytes(content), large_file_upload_state
        )
        expected_stdout = '''
            1         11  2aae6c35c94fcfb415dbe95f408b9ce91ee846ed
            3         11  2aae6c35c94fcfb415dbe95f408b9ce91ee846ed
        '''

        self._run_command(['list_parts', file.file_id], expected_stdout, '', 0)

    def test_list_unfinished_large_files_with_none(self):
        self._authorize_account()
        self._create_my_bucket()
        self._run_command(['list_unfinished_large_files', 'my-bucket'], '', '', 0)

    def test_list_unfinished_large_files_with_some(self):
        self._authorize_account()
        self._create_my_bucket()
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

        self._run_command(['list_unfinished_large_files', 'my-bucket'], expected_stdout, '', 0)

    def test_upload_large_file(self):
        self._authorize_account()
        self._create_my_bucket()
        min_part_size = self.account_info.get_minimum_part_size()
        file_size = min_part_size * 3

        with TempDir() as temp_dir:
            file_path = os.path.join(temp_dir, 'test.txt')
            text = six.u('*') * file_size
            with open(file_path, 'wb') as f:
                f.write(text.encode('utf-8'))
            expected_stdout = '''
            URL by file name: http://download.example.com/file/my-bucket/test.txt
            URL by fileId: http://download.example.com/b2api/v1/b2_download_file_by_id?fileId=9999
            {
              "action": "upload",
              "fileId": "9999",
              "fileName": "test.txt",
              "size": 600,
              "uploadTimestamp": 5000
            }
            '''

            self._run_command(
                [
                    'upload_file', '--noProgress', '--threads', '5', 'my-bucket', file_path,
                    'test.txt'
                ], expected_stdout, '', 0
            )

    def test_sync(self):
        self._authorize_account()
        self._create_my_bucket()

        with TempDir() as temp_dir:
            file_path = os.path.join(temp_dir, 'test.txt')
            with open(file_path, 'wb') as f:
                f.write(six.u('hello world').encode('utf-8'))
            expected_stdout = '''
            upload test.txt
            '''

            command = ['sync', '--threads', '5', '--noProgress', temp_dir, 'b2://my-bucket']
            self._run_command(command, expected_stdout, '', 0)

    def test_sync_syntax_error(self):
        self._authorize_account()
        self._create_my_bucket()
        expected_stderr = 'ERROR: --includeRegex cannot be used without --excludeRegex at the same time\n'
        self._run_command(['sync', '--includeRegex', '.incl', 'non-existent-local-folder', 'b2://my-bucket'], expected_stderr=expected_stderr, expected_status=1)

    def _authorize_account(self):
        """
        Prepare for a test by authorizing an account and getting an
        account auth token
        """
        self._run_command_no_checks(['authorize_account', 'my-account', 'good-app-key'])

    def _create_my_bucket(self):
        self._run_command(['create_bucket', 'my-bucket', 'allPublic'], 'bucket_0\n', '', 0)

    def _run_command(self, argv, expected_stdout='', expected_stderr='', expected_status=0):
        """
        Runs one command using the ConsoleTool, checking stdout, stderr, and
        the returned status code.

        The ConsoleTool is stateless, so we can make a new one for each
        call, with a fresh stdout and stderr
        """
        expected_stdout = self._trim_leading_spaces(expected_stdout)
        expected_stderr = self._trim_leading_spaces(expected_stderr)
        stdout = six.StringIO()
        stderr = six.StringIO()
        console_tool = ConsoleTool(self.b2_api, stdout, stderr)
        actual_status = console_tool.run_command(['b2'] + argv)

        # The json module in Python 2.6 includes trailing spaces.  Later version of Python don't.
        actual_stdout = self._trim_trailing_spaces(stdout.getvalue())
        actual_stderr = self._trim_trailing_spaces(stderr.getvalue())

        if expected_stdout != actual_stdout:
            print(repr(expected_stdout))
            print(repr(actual_stdout))
        if expected_stderr != actual_stderr:
            print(repr(expected_stderr))
            print(repr(actual_stderr))

        self.assertEqual(expected_stdout, actual_stdout, 'stdout')
        self.assertEqual(expected_stderr, actual_stderr, 'stderr')
        self.assertEqual(expected_status, actual_status, 'exit status code')

    def _run_command_no_checks(self, argv):
        ConsoleTool(self.b2_api, six.StringIO(), six.StringIO()).run_command(['b2'] + argv)

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
        assert all(line.startswith(leading_spaces) or line == ''
                   for line in lines), 'all lines have leading spaces'
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
            f.write(six.b('hello world'))
        return local_path

    def _read_file(self, local_path):
        with open(local_path, 'rb') as f:
            return f.read()
