######################################################################
#
# File: test/test_console_tool.py
#
# Copyright 2018 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import json
import os
import six

from .stub_account_info import StubAccountInfo
from .test_base import TestBase
from b2.api import B2Api
from b2.cache import InMemoryCache
from b2.console_tool import ConsoleTool
from b2.raw_api import API_VERSION
from b2.raw_simulator import RawSimulator
from b2.upload_source import UploadSourceBytes
from b2.utils import TempDir
from test_b2_command_line import file_mod_time_millis

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
        (self.account_id, self.master_key) = self.raw_api.create_account()

    def test_authorize_with_bad_key(self):
        expected_stdout = '''
        Using http://production.example.com
        '''

        expected_stderr = '''
        ERROR: unable to authorize account: Invalid authorization token. Server said: secret key is wrong (unauthorized)
        '''

        self._run_command(
            ['authorize_account', self.account_id, 'bad-app-key'], expected_stdout, expected_stderr,
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
            ['authorize-account', self.account_id, self.master_key], expected_stdout, '', 0
        )

        # Auth token should be in account info now
        assert self.account_info.get_account_auth_token() is not None

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

    def test_help_with_bad_args(self):
        expected_stderr = '''

        b2 list-parts <largeFileId>

            Lists all of the parts that have been uploaded for the given
            large file, which must be a file that was started but not
            finished or canceled.

            Requires capability: writeFiles

        '''

        self._run_command(['list_parts'], '', expected_stderr, 1)

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
        self._run_command(['create_bucket', 'bad/bucket/name', 'allPublic'], '', expected_stdout, 1)

        # Make two buckets
        self._run_command(['create_bucket', 'my-bucket', 'allPrivate'], 'bucket_0\n', '', 0)
        self._run_command(['create_bucket', 'your-bucket', 'allPrivate'], 'bucket_1\n', '', 0)

        # Update one of them
        expected_stdout = '''
        {{
            "accountId": "{account_id}",
            "bucketId": "bucket_0",
            "bucketInfo": {{}},
            "bucketName": "my-bucket",
            "bucketType": "allPublic",
            "corsRules": [],
            "lifecycleRules": [],
            "revision": 2
        }}
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
        {{
            "accountId": "{account_id}",
            "bucketId": "bucket_1",
            "bucketInfo": {{}},
            "bucketName": "your-bucket",
            "bucketType": "allPrivate",
            "corsRules": [],
            "lifecycleRules": [],
            "revision": 1
        }}
        '''

        self._run_command(['delete_bucket', 'your-bucket'], expected_stdout, '', 0)

    def test_keys(self):
        self._authorize_account()

        self._run_command(['create_bucket', 'my-bucket-a', 'allPublic'], 'bucket_0\n', '', 0)
        self._run_command(['create_bucket', 'my-bucket-b', 'allPublic'], 'bucket_1\n', '', 0)
        self._run_command(['create_bucket', 'my-bucket-c', 'allPublic'], 'bucket_2\n', '', 0)

        capabilities = ['readFiles', 'listBuckets']
        capabilities_with_commas = ','.join(capabilities)

        # Make a key with an illegal name
        expected_stderr = 'ERROR: Bad request: illegal key name: bad_key_name\n'
        self._run_command(
            ['create_key', 'bad_key_name', capabilities_with_commas], '', expected_stderr, 1
        )

        # Make a key with negative validDurationInSeconds
        expected_stderr = 'ERROR: Bad request: valid duration must be greater than 0, and less than 1000 days in seconds\n'
        self._run_command(
            ['create_key', '--duration', '-456', 'goodKeyName', capabilities_with_commas], '',
            expected_stderr, 1
        )

        # Make a key with validDurationInSeconds outside of range
        expected_stderr = 'ERROR: Bad request: valid duration must be greater than 0, ' \
                          'and less than 1000 days in seconds\n'
        self._run_command(
            ['create_key', '--duration', '0', 'goodKeyName', capabilities_with_commas], '',
            expected_stderr, 1
        )
        self._run_command(
            ['create_key', '--duration', '86400001', 'goodKeyName', capabilities_with_commas], '',
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
            ['create-key', '--bucket', 'my-bucket-a', 'goodKeyName-Two', capabilities_with_commas],
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
        self._run_command(['delete_key', 'abc123'], 'abc123\n', '', 0)

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
            appKeyId1   goodKeyName-Two        my-bucket-a            -            -          ''   readFiles,listBuckets
            appKeyId2   goodKeyName-Three      id=bucket_1            -            -          ''   readFiles,listBuckets
            """

        self._run_command(['list_keys'], expected_list_keys_out, '', 0)
        self._run_command(['list_keys', '--long'], expected_list_keys_out_long, '', 0)

        # make sure calling list_buckets with one bucket doesn't clear the cache
        cache_map_before = self.cache.name_id_map
        self.b2_api.list_buckets('my-bucket-a')
        cache_map_after = self.cache.name_id_map
        assert cache_map_before == cache_map_after

        # authorize and make calls using application key with no restrictions
        self._run_command(
            ['authorize_account', 'appKeyId0', 'appKey0'], 'Using http://production.example.com\n',
            '', 0
        )
        self._run_command(
            ['list-buckets'],
            'bucket_0  allPublic   my-bucket-a\nbucket_2  allPublic   my-bucket-c\n', '', 0
        )

        get_bucket_stdout = '''
        {{
            "accountId": "{account_id}",
            "bucketId": "bucket_0",
            "bucketInfo": {{}},
            "bucketName": "my-bucket-a",
            "bucketType": "allPublic",
            "corsRules": [],
            "lifecycleRules": [],
            "revision": 1
        }}
        '''
        self._run_command(['get-bucket', 'my-bucket-a'], get_bucket_stdout, '', 0)

        # authorize and make calls using an application key with bucket restrictions
        self._run_command(
            ['authorize_account', 'appKeyId1', 'appKey1'], 'Using http://production.example.com\n',
            '', 0
        )

        self._run_command(
            ['list-buckets'], '', 'ERROR: Application key is restricted to bucket: my-bucket-a\n', 1
        )
        self._run_command(
            ['get-bucket', 'my-bucket-c'], '',
            'ERROR: Application key is restricted to bucket: my-bucket-a\n', 1
        )

        expected_get_bucket_stdout = '''
        {{
            "accountId": "{account_id}",
            "bucketId": "bucket_0",
            "bucketInfo": {{}},
            "bucketName": "my-bucket-a",
            "bucketType": "allPublic",
            "corsRules": [],
            "lifecycleRules": [],
            "revision": 1
        }}
        '''

        self._run_command(['get-bucket', 'my-bucket-a'], expected_get_bucket_stdout, '', 0)
        self._run_command(
            ['list-file-names', 'my-bucket-c'], '',
            'ERROR: Application key is restricted to bucket: my-bucket-a\n', 1
        )

    def test_bucket_info_from_json(self):

        self._authorize_account()
        self._run_command(['create_bucket', 'my-bucket', 'allPublic'], 'bucket_0\n', '', 0)

        bucket_info = {'color': 'blue'}

        expected_stdout = '''
            {{
                "accountId": "{account_id}",
                "bucketId": "bucket_0",
                "bucketInfo": {{
                    "color": "blue"
                }},
                "bucketName": "my-bucket",
                "bucketType": "allPrivate",
                "corsRules": [],
                "lifecycleRules": [],
                "revision": 2
            }}
            '''
        self._run_command(
            ['update_bucket', '--bucketInfo',
             json.dumps(bucket_info), 'my-bucket', 'allPrivate'], expected_stdout, '', 0
        )

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
            # For this test, use a mod time without millis.  My mac truncates
            # millis and just leaves seconds.
            mod_time = 1500111222
            os.utime(local_file1, (mod_time, mod_time))
            self.assertEqual(1500111222, os.path.getmtime(local_file1))

            # Upload a file
            expected_stdout = '''
            URL by file name: http://download.example.com/file/my-bucket/file1.txt
            URL by fileId: http://download.example.com/b2api/{api_version}/b2_download_file_by_id?fileId=9999
            {{
              "action": "upload",
              "fileId": "9999",
              "fileName": "file1.txt",
              "size": 11,
              "uploadTimestamp": 5000
            }}
            '''

            self._run_command(
                ['upload_file', '--noProgress', 'my-bucket', local_file1, 'file1.txt'],
                expected_stdout, '', 0
            )

            # Get file info
            mod_time_str = str(int(os.path.getmtime(local_file1) * 1000))
            expected_stdout = '''
            {{
              "accountId": "{account_id}",
              "action": "upload",
              "bucketId": "bucket_0",
              "contentLength": 11,
              "contentSha1": "2aae6c35c94fcfb415dbe95f408b9ce91ee846ed",
              "contentType": "b2/x-auto",
              "fileId": "9999",
              "fileInfo": {{
                "src_last_modified_millis": "1500111222000"
              }},
              "fileName": "file1.txt",
              "uploadTimestamp": 5000
            }}
            '''

            self._run_command(['get_file_info', '9999'], expected_stdout, '', 0)

            # Download by name
            local_download1 = os.path.join(temp_dir, 'download1.txt')
            expected_stdout = '''
            File name:    file1.txt
            File id:      9999
            File size:    11
            Content type: b2/x-auto
            Content sha1: 2aae6c35c94fcfb415dbe95f408b9ce91ee846ed
            INFO src_last_modified_millis: 1500111222000
            checksum matches
            '''

            self._run_command(
                [
                    'download_file_by_name', '--noProgress', 'my-bucket', 'file1.txt',
                    local_download1
                ], expected_stdout, '', 0
            )
            self.assertEqual(six.b('hello world'), self._read_file(local_download1))
            self.assertEqual(mod_time, os.path.getmtime(local_download1))

            # Download file by ID.  (Same expected output as downloading by name)
            local_download2 = os.path.join(temp_dir, 'download2.txt')
            self._run_command(
                ['download_file_by_id', '--noProgress', '9999', local_download2], expected_stdout,
                '', 0
            )
            self.assertEqual(six.b('hello world'), self._read_file(local_download2))

            # Hide the file
            expected_stdout = '''
            {{
              "action": "hide",
              "fileId": "9998",
              "fileName": "file1.txt",
              "size": 0,
              "uploadTimestamp": 5001
            }}
            '''

            self._run_command(['hide_file', 'my-bucket', 'file1.txt'], expected_stdout, '', 0)

            # List the file versions
            expected_stdout = '''
            {{
              "files": [
                {{
                  "action": "hide",
                  "contentSha1": "none",
                  "contentType": null,
                  "fileId": "9998",
                  "fileInfo": {{}},
                  "fileName": "file1.txt",
                  "size": 0,
                  "uploadTimestamp": 5001
                }},
                {{
                  "action": "upload",
                  "contentSha1": "2aae6c35c94fcfb415dbe95f408b9ce91ee846ed",
                  "contentType": "b2/x-auto",
                  "fileId": "9999",
                  "fileInfo": {{
                    "src_last_modified_millis": "%s"
                  }},
                  "fileName": "file1.txt",
                  "size": 11,
                  "uploadTimestamp": 5000
                }}
              ],
              "nextFileId": null,
              "nextFileName": null
            }}
            ''' % (mod_time_str,)

            self._run_command(['list_file_versions', 'my-bucket'], expected_stdout, '', 0)

            # List the file names
            expected_stdout = '''
            {{
              "files": [],
              "nextFileName": null
            }}
            '''

            self._run_command(['list_file_names', 'my-bucket'], expected_stdout, '', 0)

            # Delete one file version, passing the name in
            expected_stdout = '''
            {{
              "action": "delete",
              "fileId": "9998",
              "fileName": "file1.txt"
            }}
            '''

            self._run_command(['delete_file_version', 'file1.txt', '9998'], expected_stdout, '', 0)

            # Delete one file version, not passing the name in
            expected_stdout = '''
            {{
              "action": "delete",
              "fileId": "9999",
              "fileName": "file1.txt"
            }}
            '''

            self._run_command(['delete_file_version', '9999'], expected_stdout, '', 0)

    def test_get_download_auth_defaults(self):
        self._authorize_account()
        self._create_my_bucket()
        self._run_command(
            ['get_download_auth', 'my-bucket'], 'fake_download_auth_token_bucket_0__86400\n', '', 0
        )

    def test_get_download_auth_explicit(self):
        self._authorize_account()
        self._create_my_bucket()
        self._run_command(
            ['get_download_auth', '--prefix', 'prefix', '--duration', '12345', 'my-bucket'],
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
            URL by fileId: http://download.example.com/b2api/{api_version}/b2_download_file_by_id?fileId=9999
            {{
              "action": "upload",
              "fileId": "9999",
              "fileName": "test.txt",
              "size": 600,
              "uploadTimestamp": 5000
            }}
            '''

            self._run_command(
                [
                    'upload_file', '--noProgress', '--threads', '5', 'my-bucket', file_path,
                    'test.txt'
                ], expected_stdout, '', 0
            )

    def test_get_account_info(self):
        self._authorize_account()
        expected_stdout = '''
        {{
            "accountAuthToken": "auth_token_0",
            "accountId": "{account_id}",
            "allowed": {{
                "bucketId": null,
                "bucketName": null,
                "capabilities": [
                    "listKeys",
                    "writeKeys",
                    "deleteKeys",
                    "listBuckets",
                    "writeBuckets",
                    "deleteBuckets",
                    "listFiles",
                    "readFiles",
                    "shareFiles",
                    "writeFiles",
                    "deleteFiles"
                ],
                "namePrefix": null
            }},
            "apiUrl": "http://api.example.com",
            "applicationKey": "{master_key}",
            "downloadUrl": "http://download.example.com"
        }}
        '''
        self._run_command(['get-account-info'], expected_stdout, '', 0)

    def test_get_bucket(self):
        self._authorize_account()
        self._create_my_bucket()
        expected_stdout = '''
        {{
            "accountId": "{account_id}",
            "bucketId": "bucket_0",
            "bucketInfo": {{}},
            "bucketName": "my-bucket",
            "bucketType": "allPublic",
            "corsRules": [],
            "lifecycleRules": [],
            "revision": 1
        }}
        '''
        self._run_command(['get-bucket', 'my-bucket'], expected_stdout, '', 0)

    def test_get_bucket_empty_show_size(self):
        self._authorize_account()
        self._create_my_bucket()
        expected_stdout = '''
        {{
            "accountId": "{account_id}",
            "bucketId": "bucket_0",
            "bucketInfo": {{}},
            "bucketName": "my-bucket",
            "bucketType": "allPublic",
            "corsRules": [],
            "fileCount": 0,
            "lifecycleRules": [],
            "revision": 1,
            "totalSize": 0
        }}
        '''
        self._run_command(['get-bucket', '--showSize', 'my-bucket'], expected_stdout, '', 0)

    def test_get_bucket_one_item_show_size(self):
        self._authorize_account()
        self._create_my_bucket()
        with TempDir() as temp_dir:
            # Upload a standard test file.
            local_file1 = self._make_local_file(temp_dir, 'file1.txt')
            expected_stdout = '''
            URL by file name: http://download.example.com/file/my-bucket/file1.txt
            URL by fileId: http://download.example.com/b2api/{api_version}/b2_download_file_by_id?fileId=9999
            {{
              "action": "upload",
              "fileId": "9999",
              "fileName": "file1.txt",
              "size": 11,
              "uploadTimestamp": 5000
            }}
            '''
            self._run_command(
                ['upload_file', '--noProgress', 'my-bucket', local_file1, 'file1.txt'],
                expected_stdout, '', 0
            )

            # Now check the output of get-bucket against the canon.
            expected_stdout = '''
            {{
                "accountId": "{account_id}",
                "bucketId": "bucket_0",
                "bucketInfo": {{}},
                "bucketName": "my-bucket",
                "bucketType": "allPublic",
                "corsRules": [],
                "fileCount": 1,
                "lifecycleRules": [],
                "revision": 1,
                "totalSize": 11
            }}
            '''
            self._run_command(['get-bucket', '--showSize', 'my-bucket'], expected_stdout, '', 0)

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
        expected_stdout = '''
        {{
            "accountId": "{account_id}",
            "bucketId": "bucket_0",
            "bucketInfo": {{}},
            "bucketName": "my-bucket",
            "bucketType": "allPublic",
            "corsRules": [],
            "fileCount": 10,
            "lifecycleRules": [],
            "revision": 1,
            "totalSize": 40
        }}
        '''
        self._run_command(['get-bucket', '--showSize', 'my-bucket'], expected_stdout, '', 0)

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
        expected_stdout = '''
        {{
            "accountId": "{account_id}",
            "bucketId": "bucket_0",
            "bucketInfo": {{}},
            "bucketName": "my-bucket",
            "bucketType": "allPublic",
            "corsRules": [],
            "fileCount": 20,
            "lifecycleRules": [],
            "revision": 1,
            "totalSize": 90
        }}
        '''
        self._run_command(['get-bucket', '--showSize', 'my-bucket'], expected_stdout, '', 0)

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
        console_tool.run_command(['b2', 'hide_file', 'my-bucket', 'hidden1'])
        console_tool.run_command(['b2', 'hide_file', 'my-bucket', 'hidden2'])
        console_tool.run_command(['b2', 'hide_file', 'my-bucket', 'hidden3'])
        console_tool.run_command(['b2', 'hide_file', 'my-bucket', 'hidden4'])

        # Now check the output of get-bucket against the canon.
        expected_stdout = '''
        {{
            "accountId": "{account_id}",
            "bucketId": "bucket_0",
            "bucketInfo": {{}},
            "bucketName": "my-bucket",
            "bucketType": "allPublic",
            "corsRules": [],
            "fileCount": 10,
            "lifecycleRules": [],
            "revision": 1,
            "totalSize": 24
        }}
        '''
        self._run_command(['get-bucket', '--showSize', 'my-bucket'], expected_stdout, '', 0)

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
        console_tool.run_command(['b2', 'hide_file', 'my-bucket', '1/hidden1'])
        console_tool.run_command(['b2', 'hide_file', 'my-bucket', '1/hidden1'])
        console_tool.run_command(['b2', 'hide_file', 'my-bucket', '1/hidden2'])
        console_tool.run_command(['b2', 'hide_file', 'my-bucket', '1/2/hidden3'])
        console_tool.run_command(['b2', 'hide_file', 'my-bucket', '1/2/hidden3'])
        console_tool.run_command(['b2', 'hide_file', 'my-bucket', '1/2/hidden3'])
        console_tool.run_command(['b2', 'hide_file', 'my-bucket', '1/2/hidden3'])

        # Now check the output of get-bucket against the canon.
        expected_stdout = '''
        {{
            "accountId": "{account_id}",
            "bucketId": "bucket_0",
            "bucketInfo": {{}},
            "bucketName": "my-bucket",
            "bucketType": "allPublic",
            "corsRules": [],
            "fileCount": 29,
            "lifecycleRules": [],
            "revision": 1,
            "totalSize": 99
        }}
        '''
        self._run_command(['get-bucket', '--showSize', 'my-bucket'], expected_stdout, '', 0)

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

    def test_sync_empty_folder_when_not_enabled(self):
        self._authorize_account()
        self._create_my_bucket()
        with TempDir() as temp_dir:
            command = ['sync', '--threads', '1', '--noProgress', temp_dir, 'b2://my-bucket']
            expected_stderr = 'ERROR: Directory %s is empty.  Use --allowEmptySource to sync anyway.\n' % temp_dir
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

    def test_sync_syntax_error(self):
        self._authorize_account()
        self._create_my_bucket()
        expected_stderr = 'ERROR: --includeRegex cannot be used without --excludeRegex at the same time\n'
        self._run_command(
            ['sync', '--includeRegex', '.incl', 'non-existent-local-folder', 'b2://my-bucket'],
            expected_stderr=expected_stderr,
            expected_status=1
        )

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
            {{
              "files": [],
              "nextFileName": null
            }}
            '''
            self._run_command(['list_file_names', 'my-bucket'], expected_stdout, '', 0)

            # upload file
            expected_stdout = '''
            upload test-dry-run.txt
            '''
            command = ['sync', '--noProgress', temp_dir, 'b2://my-bucket']
            self._run_command(command, expected_stdout, '', 0)

            # file should have been uploaded
            mtime = file_mod_time_millis(temp_file)
            expected_stdout = '''
            {{
              "files": [
                {{
                  "action": "upload",
                  "contentSha1": "2aae6c35c94fcfb415dbe95f408b9ce91ee846ed",
                  "contentType": "b2/x-auto",
                  "fileId": "9999",
                  "fileInfo": {{
                    "src_last_modified_millis": "%d"
                  }},
                  "fileName": "test-dry-run.txt",
                  "size": 11,
                  "uploadTimestamp": 5000
                }}
              ],
              "nextFileName": null
            }}
            ''' % (mtime)
            self._run_command(['list_file_names', 'my-bucket'], expected_stdout, '', 0)

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
            ['create_key', 'goodKeyName-One', 'readFiles,listBuckets'],
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

    def _authorize_account(self):
        """
        Prepare for a test by authorizing an account and getting an
        account auth token
        """
        self._run_command_ignore_output(['authorize_account', self.account_id, self.master_key])

    def _create_my_bucket(self):
        self._run_command(['create_bucket', 'my-bucket', 'allPublic'], 'bucket_0\n', '', 0)

    def _run_command(
        self, argv, expected_stdout='', expected_stderr='', expected_status=0, format_vars=None
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
        expected_stdout = self._normalize_expected_output(expected_stdout, format_vars)
        expected_stderr = self._normalize_expected_output(expected_stderr, format_vars)
        stdout, stderr = self._get_stdouterr()
        console_tool = ConsoleTool(self.b2_api, stdout, stderr)
        actual_status = console_tool.run_command(['b2'] + argv)

        # The json module in Python 2.6 includes trailing spaces.  Later version of Python don't.
        actual_stdout = self._trim_trailing_spaces(stdout.getvalue())
        actual_stderr = self._trim_trailing_spaces(stderr.getvalue())

        if expected_stdout != actual_stdout:
            print('EXPECTED STDOUT:', repr(expected_stdout))
            print('ACTUAL STDOUT:  ', repr(actual_stdout))
            print(actual_stdout)
        if expected_stderr != actual_stderr:
            print('EXPECTED STDERR:', repr(expected_stderr))
            print('ACTUAL STDERR:  ', repr(actual_stderr))
            print(actual_stderr)

        self.assertEqual(expected_stdout, actual_stdout, 'stdout')
        self.assertEqual(expected_stderr, actual_stderr, 'stderr')
        self.assertEqual(expected_status, actual_status, 'exit status code')

    def _normalize_expected_output(self, text, format_vars=None):
        format_vars = format_vars or {}
        return self._trim_leading_spaces(text).format(
            account_id=self.account_id,
            master_key=self.master_key,
            api_version=API_VERSION,
            **format_vars
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
        console_tool.run_command(['b2', 'authorize_account', self.account_id, self.master_key])

    def _get_stdouterr(self):
        class MyStringIO(six.StringIO):
            if six.PY2:  # python3 already has this attribute
                encoding = 'fake_encoding'

        stdout = MyStringIO()
        stderr = MyStringIO()
        return stdout, stderr

    def _run_command_ignore_output(self, argv):
        """
        Runs the given command in the console tool, checking that it
        success, but ignoring the stdout.
        """
        stdout, stderr = self._get_stdouterr()
        actual_status = ConsoleTool(self.b2_api, stdout, stderr).run_command(['b2'] + argv)
        actual_stderr = self._trim_trailing_spaces(stderr.getvalue())

        if '' != actual_stderr:
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
