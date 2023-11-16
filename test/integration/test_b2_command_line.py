#!/usr/bin/env python3
######################################################################
#
# File: test/integration/test_b2_command_line.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import base64
import contextlib
import hashlib
import itertools
import json
import os
import os.path
import re
import sys
import time
from pathlib import Path

import pytest
from b2sdk.v2 import (
    B2_ACCOUNT_INFO_ENV_VAR,
    SSE_C_KEY_ID_FILE_INFO_KEY_NAME,
    UNKNOWN_FILE_RETENTION_SETTING,
    EncryptionMode,
    EncryptionSetting,
    FileRetentionSetting,
    LegalHold,
    RetentionMode,
    fix_windows_path_limit,
)

from b2.console_tool import current_time_millis

from ..helpers import skip_on_windows
from .helpers import (
    ONE_DAY_MILLIS,
    ONE_HOUR_MILLIS,
    SSE_B2_AES,
    SSE_C_AES,
    SSE_C_AES_2,
    SSE_NONE,
    TempDir,
    file_mod_time_millis,
    random_hex,
    read_file,
    set_file_mod_time_millis,
    should_equal,
    write_file,
)


@pytest.fixture
def uploaded_sample_file(b2_tool, bucket_name, sample_filepath):
    return b2_tool.should_succeed_json(
        ['upload-file', '--quiet', bucket_name,
         str(sample_filepath), 'sample_file']
    )


def test_download(b2_tool, bucket_name, sample_filepath, uploaded_sample_file, tmp_path):
    output_a = tmp_path / 'a'
    b2_tool.should_succeed(
        [
            'download-file-by-name', '--quiet', bucket_name, uploaded_sample_file['fileName'],
            str(output_a)
        ]
    )
    assert output_a.read_text() == sample_filepath.read_text()

    output_b = tmp_path / 'b'
    b2_tool.should_succeed(
        ['download-file-by-id', '--quiet', uploaded_sample_file['fileId'],
         str(output_b)]
    )
    assert output_b.read_text() == sample_filepath.read_text()


def test_basic(b2_tool, bucket_name, sample_file, tmp_path):

    file_mod_time_str = str(file_mod_time_millis(sample_file))

    file_data = read_file(sample_file)
    hex_sha1 = hashlib.sha1(file_data).hexdigest()

    list_of_buckets = b2_tool.should_succeed_json(['list-buckets', '--json'])
    should_equal(
        [bucket_name], [b['bucketName'] for b in list_of_buckets if b['bucketName'] == bucket_name]
    )

    b2_tool.should_succeed(['upload-file', '--quiet', bucket_name, sample_file, 'a'])
    b2_tool.should_succeed(['ls', '--long', '--replication', bucket_name])
    b2_tool.should_succeed(['upload-file', '--noProgress', bucket_name, sample_file, 'a'])
    b2_tool.should_succeed(['upload-file', '--noProgress', bucket_name, sample_file, 'b/1'])
    b2_tool.should_succeed(['upload-file', '--noProgress', bucket_name, sample_file, 'b/2'])
    b2_tool.should_succeed(
        [
            'upload-file', '--noProgress', '--sha1', hex_sha1, '--info', 'foo=bar=baz', '--info',
            'color=blue', bucket_name, sample_file, 'c'
        ]
    )
    b2_tool.should_fail(
        [
            'upload-file', '--noProgress', '--sha1', hex_sha1, '--info', 'foo-bar', '--info',
            'color=blue', bucket_name, sample_file, 'c'
        ], r'ERROR: Bad file info: foo-bar'
    )
    b2_tool.should_succeed(
        [
            'upload-file', '--noProgress', '--contentType', 'text/plain', bucket_name, sample_file,
            'd'
        ]
    )

    b2_tool.should_succeed(['upload-file', '--noProgress', bucket_name, sample_file, 'rm'])
    b2_tool.should_succeed(['upload-file', '--noProgress', bucket_name, sample_file, 'rm1'])
    # with_wildcard allows us to target a single file. rm will be removed, rm1 will be left alone
    b2_tool.should_succeed(['rm', '--recursive', '--withWildcard', bucket_name, 'rm'])
    list_of_files = b2_tool.should_succeed_json(
        ['ls', '--json', '--recursive', '--withWildcard', bucket_name, 'rm*']
    )
    should_equal(['rm1'], [f['fileName'] for f in list_of_files])
    b2_tool.should_succeed(['rm', '--recursive', '--withWildcard', bucket_name, 'rm1'])

    b2_tool.should_succeed(
        ['download-file-by-name', '--noProgress', '--quiet', bucket_name, 'b/1', tmp_path / 'a']
    )

    b2_tool.should_succeed(['hide-file', bucket_name, 'c'])

    list_of_files = b2_tool.should_succeed_json(['ls', '--json', '--recursive', bucket_name])
    should_equal(['a', 'b/1', 'b/2', 'd'], [f['fileName'] for f in list_of_files])

    list_of_files = b2_tool.should_succeed_json(
        ['ls', '--json', '--recursive', '--versions', bucket_name]
    )
    should_equal(['a', 'a', 'b/1', 'b/2', 'c', 'c', 'd'], [f['fileName'] for f in list_of_files])
    should_equal(
        ['upload', 'upload', 'upload', 'upload', 'hide', 'upload', 'upload'],
        [f['action'] for f in list_of_files]
    )

    first_a_version = list_of_files[0]

    first_c_version = list_of_files[4]
    second_c_version = list_of_files[5]
    list_of_files = b2_tool.should_succeed_json(
        ['ls', '--json', '--recursive', '--versions', bucket_name, 'c']
    )
    should_equal([], [f['fileName'] for f in list_of_files])

    b2_tool.should_succeed(['copy-file-by-id', first_a_version['fileId'], bucket_name, 'x'])

    b2_tool.should_succeed(['ls', bucket_name], '^a{0}b/{0}d{0}'.format(os.linesep))
    # file_id, action, date, time, size(, replication), name
    b2_tool.should_succeed(
        ['ls', '--long', bucket_name],
        '^4_z.* upload .* {1}  a{0}.* - .* b/{0}4_z.* upload .* {1}  d{0}'.format(
            os.linesep, len(file_data)
        )
    )
    b2_tool.should_succeed(
        ['ls', '--long', '--replication', bucket_name],
        '^4_z.* upload .* {1}  -  a{0}.* - .*  -  b/{0}4_z.* upload .* {1}  -  d{0}'.format(
            os.linesep, len(file_data)
        )
    )
    b2_tool.should_succeed(
        ['ls', '--versions', bucket_name], '^a{0}a{0}b/{0}c{0}c{0}d{0}'.format(os.linesep)
    )
    b2_tool.should_succeed(['ls', bucket_name, 'b'], '^b/1{0}b/2{0}'.format(os.linesep))
    b2_tool.should_succeed(['ls', bucket_name, 'b/'], '^b/1{0}b/2{0}'.format(os.linesep))

    file_info = b2_tool.should_succeed_json(['get-file-info', second_c_version['fileId']])
    expected_info = {
        'color': 'blue',
        'foo': 'bar=baz',
        'src_last_modified_millis': file_mod_time_str
    }
    should_equal(expected_info, file_info['fileInfo'])

    b2_tool.should_succeed(['delete-file-version', 'c', first_c_version['fileId']])
    b2_tool.should_succeed(['ls', bucket_name], '^a{0}b/{0}c{0}d{0}'.format(os.linesep))

    b2_tool.should_succeed(['make-url', second_c_version['fileId']])

    b2_tool.should_succeed(
        ['make-friendly-url', bucket_name, 'any-file-name'],
        '^https://.*/file/{}/{}\r?$'.format(
            bucket_name,
            'any-file-name',
        ),
    )  # \r? is for Windows, as $ doesn't match \r\n


def test_debug_logs(b2_tool, is_running_on_docker, tmp_path):
    to_be_removed_bucket_name = b2_tool.generate_bucket_name()
    b2_tool.should_succeed(
        [
            'create-bucket',
            to_be_removed_bucket_name,
            'allPublic',
            *b2_tool.get_bucket_info_args(),
        ],
    )
    b2_tool.should_succeed(['delete-bucket', to_be_removed_bucket_name],)
    b2_tool.should_fail(
        ['delete-bucket', to_be_removed_bucket_name],
        re.compile(r'^ERROR: Bucket with id=\w* not found\s*$')
    )
    # Check logging settings
    if not is_running_on_docker:  # It's difficult to read the log in docker in CI
        b2_tool.should_fail(
            ['delete-bucket', to_be_removed_bucket_name, '--debugLogs'],
            re.compile(r'^ERROR: Bucket with id=\w* not found\s*$')
        )
        stack_trace_in_log = r'Traceback \(most recent call last\):.*Bucket with id=\w* not found'

        # the two regexes below depend on log message from urllib3, which is not perfect, but this test needs to
        # check global logging settings
        stderr_regex = re.compile(
            r'DEBUG:urllib3.connectionpool:.* "POST /b2api/v2/b2_delete_bucket HTTP'
            r'.*' + stack_trace_in_log,
            re.DOTALL,
        )
        log_file_regex = re.compile(
            r'urllib3.connectionpool\tDEBUG\t.* "POST /b2api/v2/b2_delete_bucket HTTP'
            r'.*' + stack_trace_in_log,
            re.DOTALL,
        )
        with open('b2_cli.log') as logfile:
            log = logfile.read()
            assert re.search(log_file_regex, log), log
        os.remove('b2_cli.log')

        b2_tool.should_fail(['delete-bucket', to_be_removed_bucket_name, '--verbose'], stderr_regex)
        assert not os.path.exists('b2_cli.log')

        b2_tool.should_fail(
            ['delete-bucket', to_be_removed_bucket_name, '--verbose', '--debugLogs'], stderr_regex
        )
        with open('b2_cli.log') as logfile:
            log = logfile.read()
            assert re.search(log_file_regex, log), log


def test_bucket(b2_tool, bucket_name):
    rule = """{
        "daysFromHidingToDeleting": 1,
        "daysFromUploadingToHiding": null,
        "fileNamePrefix": ""
    }"""
    output = b2_tool.should_succeed_json(
        [
            'update-bucket', '--lifecycleRule', rule, bucket_name, 'allPublic',
            *b2_tool.get_bucket_info_args()
        ],
    )

    ########## // doesn't happen on production, but messes up some tests \\ ##########
    for key in output['lifecycleRules'][0]:
        if key[8] == 'S' and len(key) == 47:
            del output['lifecycleRules'][0][key]
            break
    ########## \\ doesn't happen on production, but messes up some tests // ##########

    assert output["lifecycleRules"] == [
        {
            "daysFromHidingToDeleting": 1,
            "daysFromUploadingToHiding": None,
            "fileNamePrefix": ""
        }
    ]


def test_key_restrictions(b2_tool, bucket_name, sample_file, bucket_factory):
    # A single file for rm to fail on.
    b2_tool.should_succeed(['upload-file', '--noProgress', bucket_name, sample_file, 'test'])

    key_one_name = 'clt-testKey-01' + random_hex(6)
    created_key_stdout = b2_tool.should_succeed(
        [
            'create-key',
            key_one_name,
            'listFiles,listBuckets,readFiles,writeKeys',
        ]
    )
    key_one_id, key_one = created_key_stdout.split()

    b2_tool.should_succeed(
        ['authorize-account', '--environment', b2_tool.realm, key_one_id, key_one],
    )

    b2_tool.should_succeed(['get-bucket', bucket_name],)
    second_bucket_name = bucket_factory().name
    b2_tool.should_succeed(['get-bucket', second_bucket_name],)

    key_two_name = 'clt-testKey-02' + random_hex(6)
    created_key_two_stdout = b2_tool.should_succeed(
        [
            'create-key',
            '--bucket',
            bucket_name,
            key_two_name,
            'listFiles,listBuckets,readFiles',
        ]
    )
    key_two_id, key_two = created_key_two_stdout.split()

    b2_tool.should_succeed(
        ['authorize-account', '--environment', b2_tool.realm, key_two_id, key_two],
    )
    b2_tool.should_succeed(['get-bucket', bucket_name],)
    b2_tool.should_succeed(['ls', bucket_name],)

    # Capabilities can be listed in any order. While this regex doesn't confirm that all three are present,
    # in ensures that there are three in total.
    failed_bucket_err = r'Deletion of file "test" \([^\)]+\) failed: unauthorized for ' \
                        r'application key with capabilities ' \
                        r"'(.*listFiles.*|.*listBuckets.*|.*readFiles.*){3}', " \
                        r"restricted to bucket '%s' \(unauthorized\)" % bucket_name
    b2_tool.should_fail(['rm', '--recursive', '--noProgress', bucket_name], failed_bucket_err)

    failed_bucket_err = r'ERROR: Application key is restricted to bucket: ' + bucket_name
    b2_tool.should_fail(['get-bucket', second_bucket_name], failed_bucket_err)

    failed_list_files_err = r'ERROR: Application key is restricted to bucket: ' + bucket_name
    b2_tool.should_fail(['ls', second_bucket_name], failed_list_files_err)

    failed_list_files_err = r'ERROR: Application key is restricted to bucket: ' + bucket_name
    b2_tool.should_fail(['rm', second_bucket_name], failed_list_files_err)

    # reauthorize with more capabilities for clean up
    b2_tool.should_succeed(
        [
            'authorize-account', '--environment', b2_tool.realm, b2_tool.account_id,
            b2_tool.application_key
        ]
    )
    b2_tool.should_succeed(['delete-key', key_one_id])
    b2_tool.should_succeed(['delete-key', key_two_id])


def test_delete_bucket(b2_tool, bucket_name):
    b2_tool.should_succeed(['delete-bucket', bucket_name])
    b2_tool.should_fail(
        ['delete-bucket', bucket_name], re.compile(r'^ERROR: Bucket with id=\w* not found\s*$')
    )


def test_rapid_bucket_operations(b2_tool):
    new_bucket_name = b2_tool.generate_bucket_name()
    bucket_info_args = b2_tool.get_bucket_info_args()
    # apparently server behaves erratically when we delete a bucket and recreate it right away
    b2_tool.should_succeed(['create-bucket', new_bucket_name, 'allPrivate', *bucket_info_args])
    b2_tool.should_succeed(['update-bucket', new_bucket_name, 'allPublic'])
    b2_tool.should_succeed(['delete-bucket', new_bucket_name])


def test_account(b2_tool):
    with b2_tool.env_var_test_context:
        b2_tool.should_succeed(['clear-account'])
        bad_application_key = random_hex(len(b2_tool.application_key))
        b2_tool.should_fail(
            ['authorize-account', b2_tool.account_id, bad_application_key], r'unauthorized'
        )  # this call doesn't use --environment on purpose, so that we check that it is non-mandatory
        b2_tool.should_succeed(
            [
                'authorize-account',
                '--environment',
                b2_tool.realm,
                b2_tool.account_id,
                b2_tool.application_key,
            ]
        )

    # Testing (B2_APPLICATION_KEY, B2_APPLICATION_KEY_ID) for commands other than authorize-account
    with b2_tool.env_var_test_context as new_creds:
        os.remove(new_creds)

        # first, let's make sure "create-bucket" doesn't work without auth data - i.e. that the sqlite file hs been
        # successfully removed
        bucket_name = b2_tool.generate_bucket_name()
        b2_tool.should_fail(
            ['create-bucket', bucket_name, 'allPrivate'],
            r'ERROR: Missing account data: \'NoneType\' object is not subscriptable (\(key 0\) )? '
            r'Use: b2(\.exe)? authorize-account or provide auth data with "B2_APPLICATION_KEY_ID" and '
            r'"B2_APPLICATION_KEY" environment variables'
        )
        os.remove(new_creds)

        # then, let's see that auth data from env vars works
        os.environ['B2_APPLICATION_KEY'] = os.environ['B2_TEST_APPLICATION_KEY']
        os.environ['B2_APPLICATION_KEY_ID'] = os.environ['B2_TEST_APPLICATION_KEY_ID']
        os.environ['B2_ENVIRONMENT'] = b2_tool.realm

        bucket_name = b2_tool.generate_bucket_name()
        b2_tool.should_succeed(
            ['create-bucket', bucket_name, 'allPrivate', *b2_tool.get_bucket_info_args()]
        )
        b2_tool.should_succeed(['delete-bucket', bucket_name])
        assert os.path.exists(new_creds), 'sqlite file not created'

        os.environ.pop('B2_APPLICATION_KEY')
        os.environ.pop('B2_APPLICATION_KEY_ID')

        # last, let's see that providing only one of the env vars results in a failure
        os.environ['B2_APPLICATION_KEY'] = os.environ['B2_TEST_APPLICATION_KEY']
        b2_tool.should_fail(
            ['create-bucket', bucket_name, 'allPrivate'],
            r'Please provide both "B2_APPLICATION_KEY" and "B2_APPLICATION_KEY_ID" environment variables or none of them'
        )
        os.environ.pop('B2_APPLICATION_KEY')

        os.environ['B2_APPLICATION_KEY_ID'] = os.environ['B2_TEST_APPLICATION_KEY_ID']
        b2_tool.should_fail(
            ['create-bucket', bucket_name, 'allPrivate'],
            r'Please provide both "B2_APPLICATION_KEY" and "B2_APPLICATION_KEY_ID" environment variables or none of them'
        )
        os.environ.pop('B2_APPLICATION_KEY_ID')


def file_version_summary(list_of_files):
    """
    Given the result of list-file-versions, returns a list
    of all file versions, with "+" for upload and "-" for
    hide, looking like this:

       ['+ photos/a.jpg', '- photos/b.jpg', '+ photos/c.jpg']

    """
    return [filename_summary(f) for f in list_of_files]


def filename_summary(file_):
    return ('+ ' if (file_['action'] == 'upload') else '- ') + file_['fileName']


def file_version_summary_with_encryption(list_of_files):
    """
    Given the result of list-file-versions, returns a list
    of all file versions, with "+" for upload and "-" for
    hide, with information about encryption, looking like this:

       [
           ('+ photos/a.jpg', 'SSE-C:AES256?sse_c_key_id=user-generated-key-id'),
           ('+ photos/a.jpg', 'SSE-B2:AES256'),
           ('- photos/b.jpg', None),
           ('+ photos/c.jpg', 'none'),
       ]
    """
    result = []
    for f in list_of_files:
        entry = filename_summary(f)
        encryption = encryption_summary(f['serverSideEncryption'], f['fileInfo'])
        result.append((entry, encryption))
    return result


def find_file_id(list_of_files, file_name):
    for file in list_of_files:
        if file['fileName'] == file_name:
            return file['fileId']
    assert False, f'file not found: {file_name}'


def encryption_summary(sse_dict, file_info):
    if isinstance(sse_dict, EncryptionSetting):
        sse_dict = sse_dict.as_dict()
    encryption = sse_dict['mode']
    assert encryption in (
        EncryptionMode.NONE.value, EncryptionMode.SSE_B2.value, EncryptionMode.SSE_C.value
    )
    algorithm = sse_dict.get('algorithm')
    if algorithm is not None:
        encryption += ':' + algorithm
    if sse_dict['mode'] == 'SSE-C':
        sse_c_key_id = file_info.get(SSE_C_KEY_ID_FILE_INFO_KEY_NAME)
        encryption += f'?{SSE_C_KEY_ID_FILE_INFO_KEY_NAME}={sse_c_key_id}'

    return encryption


def test_sync_up(b2_tool, bucket_name):
    sync_up_helper(b2_tool, bucket_name, 'sync')


def test_sync_up_sse_b2(b2_tool, bucket_name):
    sync_up_helper(b2_tool, bucket_name, 'sync', encryption=SSE_B2_AES)


def test_sync_up_sse_c(b2_tool, bucket_name):
    sync_up_helper(b2_tool, bucket_name, 'sync', encryption=SSE_C_AES)


def test_sync_up_no_prefix(b2_tool, bucket_name):
    sync_up_helper(b2_tool, bucket_name, '')


def sync_up_helper(b2_tool, bucket_name, dir_, encryption=None):
    sync_point_parts = [bucket_name]
    if dir_:
        sync_point_parts.append(dir_)
        prefix = dir_ + '/'
    else:
        prefix = ''
    b2_sync_point = 'b2:' + '/'.join(sync_point_parts)

    with TempDir() as dir_path:
        file_versions = b2_tool.list_file_versions(bucket_name)
        should_equal([], file_version_summary(file_versions))

        write_file(dir_path / 'a', b'hello')
        write_file(dir_path / 'b', b'hello')
        write_file(dir_path / 'c', b'hello')

        # simulate action (nothing should be uploaded)
        b2_tool.should_succeed(['sync', '--noProgress', '--dryRun', dir_path, b2_sync_point])
        file_versions = b2_tool.list_file_versions(bucket_name)
        should_equal([], file_version_summary(file_versions))

        #
        # A note about OSError: [WinError 1314]
        #
        # If you are seeing this, then probably you ran the integration test suite from
        # a non-admin account which on Windows doesn't by default get to create symlinks.
        # A special permission is needed. Now maybe there is a way to give that permission,
        # but it didn't work for me, so I just ran it as admin. A guide that I've found
        # recommended to go to Control Panel, Administrative Tools, Local Security Policy,
        # Local Policies, User Rights Assignment and there you can find a permission to
        # create symbilic links. Add your user to it (or a group that the user is in).
        #
        # Finally in order to apply the new policy, run `cmd` and execute
        # ``gpupdate /force``.
        #
        # Again, if it still doesn't work, consider just running the shell you are
        # launching ``nox`` as admin.

        os.symlink('broken', dir_path / 'd')  # OSError: [WinError 1314] ? See the comment above

        additional_env = None

        # now upload
        if encryption is None:
            command = ['sync', '--noProgress', dir_path, b2_sync_point]
            expected_encryption = SSE_NONE
            expected_encryption_str = encryption_summary(expected_encryption.as_dict(), {})
        elif encryption == SSE_B2_AES:
            command = [
                'sync', '--noProgress', '--destinationServerSideEncryption', 'SSE-B2', dir_path,
                b2_sync_point
            ]
            expected_encryption = encryption
            expected_encryption_str = encryption_summary(expected_encryption.as_dict(), {})
        elif encryption == SSE_C_AES:
            command = [
                'sync', '--noProgress', '--destinationServerSideEncryption', 'SSE-C', dir_path,
                b2_sync_point
            ]
            expected_encryption = encryption
            additional_env = {
                'B2_DESTINATION_SSE_C_KEY_B64': base64.b64encode(SSE_C_AES.key.secret).decode(),
                'B2_DESTINATION_SSE_C_KEY_ID': SSE_C_AES.key.key_id,
            }
            expected_encryption_str = encryption_summary(
                expected_encryption.as_dict(),
                {SSE_C_KEY_ID_FILE_INFO_KEY_NAME: SSE_C_AES.key.key_id}
            )
        else:
            raise NotImplementedError('unsupported encryption mode: %s' % encryption)

        b2_tool.should_succeed(
            command, expected_pattern="d could not be accessed", additional_env=additional_env
        )
        file_versions = b2_tool.list_file_versions(bucket_name)

        should_equal(
            [
                ('+ ' + prefix + 'a', expected_encryption_str),
                ('+ ' + prefix + 'b', expected_encryption_str),
                ('+ ' + prefix + 'c', expected_encryption_str),
            ],
            file_version_summary_with_encryption(file_versions),
        )
        if encryption and encryption.mode == EncryptionMode.SSE_C:
            b2_tool.should_fail(
                command,
                expected_pattern="ValueError: Using SSE-C requires providing an encryption key via "
                "B2_DESTINATION_SSE_C_KEY_B64 env var"
            )
        if encryption is not None:
            return  # that's enough, we've checked that encryption works, no need to repeat the whole sync suite

        c_id = find_file_id(file_versions, prefix + 'c')
        file_info = b2_tool.should_succeed_json(['get-file-info', c_id])['fileInfo']
        should_equal(
            file_mod_time_millis(dir_path / 'c'), int(file_info['src_last_modified_millis'])
        )

        os.unlink(dir_path / 'b')
        write_file(dir_path / 'c', b'hello world')

        b2_tool.should_succeed(
            ['sync', '--noProgress', '--keepDays', '10', dir_path, b2_sync_point]
        )
        file_versions = b2_tool.list_file_versions(bucket_name)
        should_equal(
            [
                '+ ' + prefix + 'a',
                '- ' + prefix + 'b',
                '+ ' + prefix + 'b',
                '+ ' + prefix + 'c',
                '+ ' + prefix + 'c',
            ], file_version_summary(file_versions)
        )

        os.unlink(dir_path / 'a')

        b2_tool.should_succeed(['sync', '--noProgress', '--delete', dir_path, b2_sync_point])
        file_versions = b2_tool.list_file_versions(bucket_name)
        should_equal([
            '+ ' + prefix + 'c',
        ], file_version_summary(file_versions))

        # test --compareThreshold with file size
        write_file(dir_path / 'c', b'hello world!')

        # should not upload new version of c
        b2_tool.should_succeed(
            [
                'sync', '--noProgress', '--keepDays', '10', '--compareVersions', 'size',
                '--compareThreshold', '1', dir_path, b2_sync_point
            ]
        )
        file_versions = b2_tool.list_file_versions(bucket_name)
        should_equal([
            '+ ' + prefix + 'c',
        ], file_version_summary(file_versions))

        # should upload new version of c
        b2_tool.should_succeed(
            [
                'sync', '--noProgress', '--keepDays', '10', '--compareVersions', 'size', dir_path,
                b2_sync_point
            ]
        )
        file_versions = b2_tool.list_file_versions(bucket_name)
        should_equal(
            [
                '+ ' + prefix + 'c',
                '+ ' + prefix + 'c',
            ], file_version_summary(file_versions)
        )

        set_file_mod_time_millis(dir_path / 'c', file_mod_time_millis(dir_path / 'c') + 2000)

        # test --compareThreshold with modTime
        # should not upload new version of c
        b2_tool.should_succeed(
            [
                'sync', '--noProgress', '--keepDays', '10', '--compareVersions', 'modTime',
                '--compareThreshold', '2000', dir_path, b2_sync_point
            ]
        )
        file_versions = b2_tool.list_file_versions(bucket_name)
        should_equal(
            [
                '+ ' + prefix + 'c',
                '+ ' + prefix + 'c',
            ], file_version_summary(file_versions)
        )

        # should upload new version of c
        b2_tool.should_succeed(
            [
                'sync', '--noProgress', '--keepDays', '10', '--compareVersions', 'modTime',
                dir_path, b2_sync_point
            ]
        )
        file_versions = b2_tool.list_file_versions(bucket_name)
        should_equal(
            [
                '+ ' + prefix + 'c',
                '+ ' + prefix + 'c',
                '+ ' + prefix + 'c',
            ], file_version_summary(file_versions)
        )

        # create one more file
        write_file(dir_path / 'linktarget', b'hello')
        mod_time = str((file_mod_time_millis(dir_path / 'linktarget') - 10) / 1000)

        # exclude last created file because of mtime
        b2_tool.should_succeed(
            ['sync', '--noProgress', '--excludeIfModifiedAfter', mod_time, dir_path, b2_sync_point]
        )
        file_versions = b2_tool.list_file_versions(bucket_name)
        should_equal(
            [
                '+ ' + prefix + 'c',
                '+ ' + prefix + 'c',
                '+ ' + prefix + 'c',
            ],
            file_version_summary(file_versions),
        )

        # confirm symlink is skipped
        os.symlink('linktarget', dir_path / 'alink')

        b2_tool.should_succeed(
            ['sync', '--noProgress', '--excludeAllSymlinks', dir_path, b2_sync_point],
        )
        file_versions = b2_tool.list_file_versions(bucket_name)
        should_equal(
            [
                '+ ' + prefix + 'c',
                '+ ' + prefix + 'c',
                '+ ' + prefix + 'c',
                '+ ' + prefix + 'linktarget',
            ],
            file_version_summary(file_versions),
        )

        # confirm symlink target is uploaded (with symlink's name)
        b2_tool.should_succeed(['sync', '--noProgress', dir_path, b2_sync_point])
        file_versions = b2_tool.list_file_versions(bucket_name)
        should_equal(
            [
                '+ ' + prefix + 'alink',
                '+ ' + prefix + 'c',
                '+ ' + prefix + 'c',
                '+ ' + prefix + 'c',
                '+ ' + prefix + 'linktarget',
            ],
            file_version_summary(file_versions),
        )


def test_sync_down(b2_tool, bucket_name, sample_file):
    sync_down_helper(b2_tool, bucket_name, 'sync', sample_file)


def test_sync_down_no_prefix(b2_tool, bucket_name, sample_file):
    sync_down_helper(b2_tool, bucket_name, '', sample_file)


def test_sync_down_sse_c_no_prefix(b2_tool, bucket_name, sample_file):
    sync_down_helper(b2_tool, bucket_name, '', sample_file, SSE_C_AES)


def sync_down_helper(b2_tool, bucket_name, folder_in_bucket, sample_file, encryption=None):

    b2_sync_point = 'b2:%s' % bucket_name
    if folder_in_bucket:
        b2_sync_point += '/' + folder_in_bucket
        b2_file_prefix = folder_in_bucket + '/'
    else:
        b2_file_prefix = ''

    if encryption is None or encryption.mode in (EncryptionMode.NONE, EncryptionMode.SSE_B2):
        upload_encryption_args = []
        upload_additional_env = {}
        sync_encryption_args = []
        sync_additional_env = {}
    elif encryption.mode == EncryptionMode.SSE_C:
        upload_encryption_args = ['--destinationServerSideEncryption', 'SSE-C']
        upload_additional_env = {
            'B2_DESTINATION_SSE_C_KEY_B64': base64.b64encode(encryption.key.secret).decode(),
            'B2_DESTINATION_SSE_C_KEY_ID': encryption.key.key_id,
        }
        sync_encryption_args = ['--sourceServerSideEncryption', 'SSE-C']
        sync_additional_env = {
            'B2_SOURCE_SSE_C_KEY_B64': base64.b64encode(encryption.key.secret).decode(),
            'B2_SOURCE_SSE_C_KEY_ID': encryption.key.key_id,
        }
    else:
        raise NotImplementedError(encryption)

    with TempDir() as local_path:
        # Sync from an empty "folder" as a source.
        b2_tool.should_succeed(['sync', b2_sync_point, local_path])
        should_equal([], sorted(local_path.iterdir()))

        # Put a couple files in B2
        b2_tool.should_succeed(
            ['upload-file', '--noProgress', bucket_name, sample_file, b2_file_prefix + 'a'] +
            upload_encryption_args,
            additional_env=upload_additional_env,
        )
        b2_tool.should_succeed(
            ['upload-file', '--noProgress', bucket_name, sample_file, b2_file_prefix + 'b'] +
            upload_encryption_args,
            additional_env=upload_additional_env,
        )
        b2_tool.should_succeed(
            ['sync', b2_sync_point, local_path] + sync_encryption_args,
            additional_env=sync_additional_env,
        )
        should_equal(['a', 'b'], sorted(os.listdir(local_path)))

        b2_tool.should_succeed(
            ['upload-file', '--noProgress', bucket_name, sample_file, b2_file_prefix + 'c'] +
            upload_encryption_args,
            additional_env=upload_additional_env,
        )

        # Sync the files with one file being excluded because of mtime
        mod_time = str((file_mod_time_millis(sample_file) - 10) / 1000)
        b2_tool.should_succeed(
            [
                'sync', '--noProgress', '--excludeIfModifiedAfter', mod_time, b2_sync_point,
                local_path
            ] + sync_encryption_args,
            additional_env=sync_additional_env,
        )
        should_equal(['a', 'b'], sorted(os.listdir(local_path)))
        # Sync all the files
        b2_tool.should_succeed(
            ['sync', '--noProgress', b2_sync_point, local_path] + sync_encryption_args,
            additional_env=sync_additional_env,
        )
        should_equal(['a', 'b', 'c'], sorted(os.listdir(local_path)))
    with TempDir() as new_local_path:
        if encryption and encryption.mode == EncryptionMode.SSE_C:
            b2_tool.should_fail(
                ['sync', '--noProgress', b2_sync_point, new_local_path] + sync_encryption_args,
                expected_pattern='ValueError: Using SSE-C requires providing an encryption key via '
                'B2_SOURCE_SSE_C_KEY_B64 env var',
            )
            b2_tool.should_fail(
                ['sync', '--noProgress', b2_sync_point, new_local_path],
                expected_pattern=
                'b2sdk.exception.BadRequest: The object was stored using a form of Server Side '
                'Encryption. The correct parameters must be provided to retrieve the object. '
                r'\(bad_request\)',
            )


def test_sync_copy(bucket_factory, b2_tool, bucket_name, sample_file):
    prepare_and_run_sync_copy_tests(
        bucket_factory, b2_tool, bucket_name, 'sync', sample_file=sample_file
    )


def test_sync_copy_no_prefix_default_encryption(bucket_factory, b2_tool, bucket_name, sample_file):
    prepare_and_run_sync_copy_tests(
        bucket_factory,
        b2_tool,
        bucket_name,
        '',
        sample_file=sample_file,
        destination_encryption=None,
        expected_encryption=SSE_NONE
    )


def test_sync_copy_no_prefix_no_encryption(bucket_factory, b2_tool, bucket_name, sample_file):
    prepare_and_run_sync_copy_tests(
        bucket_factory,
        b2_tool,
        bucket_name,
        '',
        sample_file=sample_file,
        destination_encryption=SSE_NONE,
        expected_encryption=SSE_NONE
    )


def test_sync_copy_no_prefix_sse_b2(bucket_factory, b2_tool, bucket_name, sample_file):
    prepare_and_run_sync_copy_tests(
        bucket_factory,
        b2_tool,
        bucket_name,
        '',
        sample_file=sample_file,
        destination_encryption=SSE_B2_AES,
        expected_encryption=SSE_B2_AES,
    )


def test_sync_copy_no_prefix_sse_c(bucket_factory, b2_tool, bucket_name, sample_file):
    prepare_and_run_sync_copy_tests(
        bucket_factory,
        b2_tool,
        bucket_name,
        '',
        sample_file=sample_file,
        destination_encryption=SSE_C_AES,
        expected_encryption=SSE_C_AES,
        source_encryption=SSE_C_AES_2,
    )


def test_sync_copy_sse_c_single_bucket(b2_tool, bucket_name, sample_file):
    run_sync_copy_with_basic_checks(
        b2_tool=b2_tool,
        b2_file_prefix='first_folder/',
        b2_sync_point=f'b2:{bucket_name}/first_folder',
        bucket_name=bucket_name,
        other_b2_sync_point=f'b2:{bucket_name}/second_folder',
        destination_encryption=SSE_C_AES_2,
        source_encryption=SSE_C_AES,
        sample_file=sample_file,
    )
    expected_encryption_first = encryption_summary(
        SSE_C_AES.as_dict(),
        {SSE_C_KEY_ID_FILE_INFO_KEY_NAME: SSE_C_AES.key.key_id},
    )
    expected_encryption_second = encryption_summary(
        SSE_C_AES_2.as_dict(),
        {SSE_C_KEY_ID_FILE_INFO_KEY_NAME: SSE_C_AES_2.key.key_id},
    )

    file_versions = b2_tool.list_file_versions(bucket_name)
    should_equal(
        [
            ('+ first_folder/a', expected_encryption_first),
            ('+ first_folder/b', expected_encryption_first),
            ('+ second_folder/a', expected_encryption_second),
            ('+ second_folder/b', expected_encryption_second),
        ],
        file_version_summary_with_encryption(file_versions),
    )


def prepare_and_run_sync_copy_tests(
    bucket_factory,
    b2_tool,
    bucket_name,
    folder_in_bucket,
    sample_file,
    destination_encryption=None,
    expected_encryption=SSE_NONE,
    source_encryption=None,
):
    b2_sync_point = 'b2:%s' % bucket_name
    if folder_in_bucket:
        b2_sync_point += '/' + folder_in_bucket
        b2_file_prefix = folder_in_bucket + '/'
    else:
        b2_file_prefix = ''

    other_bucket_name = bucket_factory().name

    other_b2_sync_point = 'b2:%s' % other_bucket_name
    if folder_in_bucket:
        other_b2_sync_point += '/' + folder_in_bucket

    run_sync_copy_with_basic_checks(
        b2_tool=b2_tool,
        b2_file_prefix=b2_file_prefix,
        b2_sync_point=b2_sync_point,
        bucket_name=bucket_name,
        other_b2_sync_point=other_b2_sync_point,
        destination_encryption=destination_encryption,
        source_encryption=source_encryption,
        sample_file=sample_file,
    )

    if destination_encryption is None or destination_encryption in (SSE_NONE, SSE_B2_AES):
        encryption_file_info = {}
    elif destination_encryption.mode == EncryptionMode.SSE_C:
        encryption_file_info = {SSE_C_KEY_ID_FILE_INFO_KEY_NAME: destination_encryption.key.key_id}
    else:
        raise NotImplementedError(destination_encryption)

    file_versions = b2_tool.list_file_versions(other_bucket_name)
    expected_encryption_str = encryption_summary(
        expected_encryption.as_dict(), encryption_file_info
    )
    should_equal(
        [
            ('+ ' + b2_file_prefix + 'a', expected_encryption_str),
            ('+ ' + b2_file_prefix + 'b', expected_encryption_str),
        ],
        file_version_summary_with_encryption(file_versions),
    )


def run_sync_copy_with_basic_checks(
    b2_tool,
    b2_file_prefix,
    b2_sync_point,
    bucket_name,
    other_b2_sync_point,
    destination_encryption,
    source_encryption,
    sample_file,
):
    # Put a couple files in B2
    if source_encryption is None or source_encryption.mode in (
        EncryptionMode.NONE, EncryptionMode.SSE_B2
    ):
        b2_tool.should_succeed(
            [
                'upload-file', '--noProgress', '--destinationServerSideEncryption', 'SSE-B2',
                bucket_name, sample_file, b2_file_prefix + 'a'
            ]
        )
        b2_tool.should_succeed(
            ['upload-file', '--noProgress', bucket_name, sample_file, b2_file_prefix + 'b']
        )
    elif source_encryption.mode == EncryptionMode.SSE_C:
        for suffix in ['a', 'b']:
            b2_tool.should_succeed(
                [
                    'upload-file', '--noProgress', '--destinationServerSideEncryption', 'SSE-C',
                    bucket_name, sample_file, b2_file_prefix + suffix
                ],
                additional_env={
                    'B2_DESTINATION_SSE_C_KEY_B64':
                        base64.b64encode(source_encryption.key.secret).decode(),
                    'B2_DESTINATION_SSE_C_KEY_ID':
                        source_encryption.key.key_id,
                },
            )
    else:
        raise NotImplementedError(source_encryption)

    # Sync all the files
    if destination_encryption is None or destination_encryption == SSE_NONE:
        b2_tool.should_succeed(['sync', '--noProgress', b2_sync_point, other_b2_sync_point])
    elif destination_encryption == SSE_B2_AES:
        b2_tool.should_succeed(
            [
                'sync', '--noProgress', '--destinationServerSideEncryption',
                destination_encryption.mode.value, b2_sync_point, other_b2_sync_point
            ]
        )
    elif destination_encryption.mode == EncryptionMode.SSE_C:
        b2_tool.should_fail(
            [
                'sync', '--noProgress', '--destinationServerSideEncryption',
                destination_encryption.mode.value, b2_sync_point, other_b2_sync_point
            ],
            additional_env={
                'B2_DESTINATION_SSE_C_KEY_B64':
                    base64.b64encode(destination_encryption.key.secret).decode(),
                'B2_DESTINATION_SSE_C_KEY_ID':
                    destination_encryption.key.key_id,
            },
            expected_pattern=
            'b2sdk.exception.BadRequest: The object was stored using a form of Server Side '
            'Encryption. The correct parameters must be provided to retrieve the object. '
            r'\(bad_request\)'
        )
        b2_tool.should_succeed(
            [
                'sync', '--noProgress', '--destinationServerSideEncryption',
                destination_encryption.mode.value, '--sourceServerSideEncryption',
                source_encryption.mode.value, b2_sync_point, other_b2_sync_point
            ],
            additional_env={
                'B2_DESTINATION_SSE_C_KEY_B64':
                    base64.b64encode(destination_encryption.key.secret).decode(),
                'B2_DESTINATION_SSE_C_KEY_ID':
                    destination_encryption.key.key_id,
                'B2_SOURCE_SSE_C_KEY_B64':
                    base64.b64encode(source_encryption.key.secret).decode(),
                'B2_SOURCE_SSE_C_KEY_ID':
                    source_encryption.key.key_id,
            }
        )

    else:
        raise NotImplementedError(destination_encryption)


def test_sync_long_path(b2_tool, bucket_name):
    """
    test sync with very long path (overcome windows 260 character limit)
    """
    b2_sync_point = 'b2://' + bucket_name

    long_path = '/'.join(
        (
            'extremely_long_path_which_exceeds_windows_unfortunate_260_character_path_limit',
            'and_needs_special_prefixes_containing_backslashes_added_to_overcome_this_limitation',
            'when_doing_so_beware_leaning_toothpick_syndrome_as_it_can_cause_frustration',
            'see_also_xkcd_1638'
        )
    )

    with TempDir() as dir_path:
        local_long_path = (dir_path / long_path).resolve()
        fixed_local_long_path = Path(fix_windows_path_limit(str(local_long_path)))
        os.makedirs(fixed_local_long_path.parent)
        write_file(fixed_local_long_path, b'asdf')

        b2_tool.should_succeed(['sync', '--noProgress', '--delete', dir_path, b2_sync_point])
        file_versions = b2_tool.list_file_versions(bucket_name)
        should_equal(['+ ' + long_path], file_version_summary(file_versions))


def test_default_sse_b2__update_bucket(b2_tool, bucket_name, schedule_bucket_cleanup):
    # Set default encryption via update-bucket
    bucket_info = b2_tool.should_succeed_json(['get-bucket', bucket_name])
    bucket_default_sse = {'mode': 'none'}
    should_equal(bucket_default_sse, bucket_info['defaultServerSideEncryption'])

    bucket_info = b2_tool.should_succeed_json(
        ['update-bucket', '--defaultServerSideEncryption=SSE-B2', bucket_name]
    )
    bucket_default_sse = {
        'algorithm': 'AES256',
        'mode': 'SSE-B2',
    }
    should_equal(bucket_default_sse, bucket_info['defaultServerSideEncryption'])

    bucket_info = b2_tool.should_succeed_json(['get-bucket', bucket_name])
    bucket_default_sse = {
        'algorithm': 'AES256',
        'mode': 'SSE-B2',
    }
    should_equal(bucket_default_sse, bucket_info['defaultServerSideEncryption'])


def test_default_sse_b2__create_bucket(b2_tool, schedule_bucket_cleanup):
    # Set default encryption via create-bucket
    second_bucket_name = b2_tool.generate_bucket_name()
    schedule_bucket_cleanup(second_bucket_name)
    b2_tool.should_succeed(
        [
            'create-bucket',
            '--defaultServerSideEncryption=SSE-B2',
            second_bucket_name,
            'allPublic',
            *b2_tool.get_bucket_info_args(),
        ]
    )
    second_bucket_info = b2_tool.should_succeed_json(['get-bucket', second_bucket_name])
    second_bucket_default_sse = {
        'algorithm': 'AES256',
        'mode': 'SSE-B2',
    }
    should_equal(second_bucket_default_sse, second_bucket_info['defaultServerSideEncryption'])


def test_sse_b2(b2_tool, bucket_name, sample_file, tmp_path):
    b2_tool.should_succeed(
        [
            'upload-file', '--destinationServerSideEncryption=SSE-B2', '--quiet', bucket_name,
            sample_file, 'encrypted'
        ]
    )
    b2_tool.should_succeed(['upload-file', '--quiet', bucket_name, sample_file, 'not_encrypted'])

    b2_tool.should_succeed(
        ['download-file-by-name', '--quiet', bucket_name, 'encrypted', tmp_path / 'encrypted']
    )
    b2_tool.should_succeed(
        [
            'download-file-by-name', '--quiet', bucket_name, 'not_encrypted',
            tmp_path / 'not_encypted'
        ]
    )

    list_of_files = b2_tool.should_succeed_json(['ls', '--json', '--recursive', bucket_name])
    should_equal(
        [{
            'algorithm': 'AES256',
            'mode': 'SSE-B2'
        }, {
            'mode': 'none'
        }], [f['serverSideEncryption'] for f in list_of_files]
    )

    encrypted_version = list_of_files[0]
    file_info = b2_tool.should_succeed_json(['get-file-info', encrypted_version['fileId']])
    should_equal({'algorithm': 'AES256', 'mode': 'SSE-B2'}, file_info['serverSideEncryption'])
    not_encrypted_version = list_of_files[1]
    file_info = b2_tool.should_succeed_json(['get-file-info', not_encrypted_version['fileId']])
    should_equal({'mode': 'none'}, file_info['serverSideEncryption'])

    b2_tool.should_succeed(
        [
            'copy-file-by-id', '--destinationServerSideEncryption=SSE-B2',
            encrypted_version['fileId'], bucket_name, 'copied_encrypted'
        ]
    )
    b2_tool.should_succeed(
        ['copy-file-by-id', not_encrypted_version['fileId'], bucket_name, 'copied_not_encrypted']
    )

    list_of_files = b2_tool.should_succeed_json(['ls', '--json', '--recursive', bucket_name])
    should_equal(
        [{
            'algorithm': 'AES256',
            'mode': 'SSE-B2'
        }, {
            'mode': 'none'
        }] * 2, [f['serverSideEncryption'] for f in list_of_files]
    )

    copied_encrypted_version = list_of_files[2]
    file_info = b2_tool.should_succeed_json(['get-file-info', copied_encrypted_version['fileId']])
    should_equal({'algorithm': 'AES256', 'mode': 'SSE-B2'}, file_info['serverSideEncryption'])

    copied_not_encrypted_version = list_of_files[3]
    file_info = b2_tool.should_succeed_json(
        ['get-file-info', copied_not_encrypted_version['fileId']]
    )
    should_equal({'mode': 'none'}, file_info['serverSideEncryption'])


def test_sse_c(b2_tool, bucket_name, is_running_on_docker, sample_file, tmp_path):

    sse_c_key_id = 'user-generated-key-id \nąóźćż\nœøΩ≈ç\nßäöü'
    if is_running_on_docker:
        # TODO: fix this once we figure out how to pass env vars with \n in them to docker, docker-compose should work
        sse_c_key_id = sse_c_key_id.replace('\n', '')

    secret = os.urandom(32)

    b2_tool.should_fail(
        [
            'upload-file', '--noProgress', '--quiet', '--destinationServerSideEncryption', 'SSE-C',
            bucket_name, sample_file, 'gonna-fail-anyway'
        ],
        'Using SSE-C requires providing an encryption key via B2_DESTINATION_SSE_C_KEY_B64 env var'
    )
    file_version_info = b2_tool.should_succeed_json(
        [
            'upload-file', '--noProgress', '--quiet', '--destinationServerSideEncryption', 'SSE-C',
            bucket_name, sample_file, 'uploaded_encrypted'
        ],
        additional_env={
            'B2_DESTINATION_SSE_C_KEY_B64': base64.b64encode(secret).decode(),
            'B2_DESTINATION_SSE_C_KEY_ID': sse_c_key_id,
        }
    )
    should_equal(
        {
            "algorithm": "AES256",
            "customerKey": "******",
            "customerKeyMd5": "******",
            "mode": "SSE-C"
        }, file_version_info['serverSideEncryption']
    )
    should_equal(sse_c_key_id, file_version_info['fileInfo'][SSE_C_KEY_ID_FILE_INFO_KEY_NAME])

    b2_tool.should_fail(
        [
            'download-file-by-name', '--quiet', bucket_name, 'uploaded_encrypted',
            'gonna_fail_anyway'
        ],
        expected_pattern='ERROR: The object was stored using a form of Server Side Encryption. The '
        r'correct parameters must be provided to retrieve the object. \(bad_request\)'
    )
    b2_tool.should_fail(
        [
            'download-file-by-name', '--quiet', '--sourceServerSideEncryption', 'SSE-C',
            bucket_name, 'uploaded_encrypted', 'gonna_fail_anyway'
        ],
        expected_pattern='ValueError: Using SSE-C requires providing an encryption key via '
        'B2_SOURCE_SSE_C_KEY_B64 env var'
    )
    b2_tool.should_fail(
        [
            'download-file-by-name', '--quiet', '--sourceServerSideEncryption', 'SSE-C',
            bucket_name, 'uploaded_encrypted', 'gonna_fail_anyway'
        ],
        expected_pattern='ERROR: Wrong or no SSE-C key provided when reading a file.',
        additional_env={'B2_SOURCE_SSE_C_KEY_B64': base64.b64encode(os.urandom(32)).decode()}
    )
    with contextlib.nullcontext(tmp_path) as dir_path:
        b2_tool.should_succeed(
            [
                'download-file-by-name',
                '--noProgress',
                '--quiet',
                '--sourceServerSideEncryption',
                'SSE-C',
                bucket_name,
                'uploaded_encrypted',
                dir_path / 'a',
            ],
            additional_env={'B2_SOURCE_SSE_C_KEY_B64': base64.b64encode(secret).decode()}
        )
        assert read_file(dir_path / 'a') == read_file(sample_file)
        b2_tool.should_succeed(
            [
                'download-file-by-id',
                '--noProgress',
                '--quiet',
                '--sourceServerSideEncryption',
                'SSE-C',
                file_version_info['fileId'],
                dir_path / 'b',
            ],
            additional_env={'B2_SOURCE_SSE_C_KEY_B64': base64.b64encode(secret).decode()}
        )
        assert read_file(dir_path / 'b') == read_file(sample_file)

    b2_tool.should_fail(
        ['copy-file-by-id', file_version_info['fileId'], bucket_name, 'gonna-fail-anyway'],
        expected_pattern=
        'ERROR: The object was stored using a form of Server Side Encryption. The correct '
        r'parameters must be provided to retrieve the object. \(bad_request\)'
    )
    b2_tool.should_fail(
        [
            'copy-file-by-id', '--sourceServerSideEncryption=SSE-C', file_version_info['fileId'],
            bucket_name, 'gonna-fail-anyway'
        ],
        expected_pattern='ValueError: Using SSE-C requires providing an encryption key via '
        'B2_SOURCE_SSE_C_KEY_B64 env var'
    )
    b2_tool.should_fail(
        [
            'copy-file-by-id', '--sourceServerSideEncryption=SSE-C',
            '--destinationServerSideEncryption=SSE-C', file_version_info['fileId'], bucket_name,
            'gonna-fail-anyway'
        ],
        expected_pattern='ValueError: Using SSE-C requires providing an encryption key via '
        'B2_DESTINATION_SSE_C_KEY_B64 env var',
        additional_env={'B2_SOURCE_SSE_C_KEY_B64': base64.b64encode(secret).decode()}
    )
    b2_tool.should_fail(
        [
            'copy-file-by-id', '--sourceServerSideEncryption=SSE-C', file_version_info['fileId'],
            bucket_name, 'gonna-fail-anyway'
        ],
        additional_env={'B2_SOURCE_SSE_C_KEY_B64': base64.b64encode(secret).decode()},
        expected_pattern=
        'Attempting to copy file with metadata while either source or destination uses '
        'SSE-C. Use --fetchMetadata to fetch source file metadata before copying.',
    )
    b2_tool.should_succeed(
        [
            'copy-file-by-id',
            '--sourceServerSideEncryption=SSE-C',
            file_version_info['fileId'],
            bucket_name,
            'not_encrypted_copied_from_encrypted_metadata_replace',
            '--info',
            'a=b',
            '--contentType',
            'text/plain',
        ],
        additional_env={'B2_SOURCE_SSE_C_KEY_B64': base64.b64encode(secret).decode()}
    )
    b2_tool.should_succeed(
        [
            'copy-file-by-id',
            '--sourceServerSideEncryption=SSE-C',
            file_version_info['fileId'],
            bucket_name,
            'not_encrypted_copied_from_encrypted_metadata_replace_empty',
            '--noInfo',
            '--contentType',
            'text/plain',
        ],
        additional_env={'B2_SOURCE_SSE_C_KEY_B64': base64.b64encode(secret).decode()}
    )
    b2_tool.should_succeed(
        [
            'copy-file-by-id',
            '--sourceServerSideEncryption=SSE-C',
            file_version_info['fileId'],
            bucket_name,
            'not_encrypted_copied_from_encrypted_metadata_pseudo_copy',
            '--fetchMetadata',
        ],
        additional_env={'B2_SOURCE_SSE_C_KEY_B64': base64.b64encode(secret).decode()}
    )
    b2_tool.should_succeed(
        [
            'copy-file-by-id',
            '--sourceServerSideEncryption=SSE-C',
            '--destinationServerSideEncryption=SSE-C',
            file_version_info['fileId'],
            bucket_name,
            'encrypted_no_id_copied_from_encrypted',
            '--fetchMetadata',
        ],
        additional_env={
            'B2_SOURCE_SSE_C_KEY_B64': base64.b64encode(secret).decode(),
            'B2_DESTINATION_SSE_C_KEY_B64': base64.b64encode(os.urandom(32)).decode(),
        }
    )
    b2_tool.should_succeed(
        [
            'copy-file-by-id',
            '--sourceServerSideEncryption=SSE-C',
            '--destinationServerSideEncryption=SSE-C',
            file_version_info['fileId'],
            bucket_name,
            'encrypted_with_id_copied_from_encrypted_metadata_replace',
            '--noInfo',
            '--contentType',
            'text/plain',
        ],
        additional_env={
            'B2_SOURCE_SSE_C_KEY_B64': base64.b64encode(secret).decode(),
            'B2_DESTINATION_SSE_C_KEY_B64': base64.b64encode(os.urandom(32)).decode(),
            'B2_DESTINATION_SSE_C_KEY_ID': 'another-user-generated-key-id',
        }
    )
    b2_tool.should_succeed(
        [
            'copy-file-by-id',
            '--sourceServerSideEncryption=SSE-C',
            '--destinationServerSideEncryption=SSE-C',
            file_version_info['fileId'],
            bucket_name,
            'encrypted_with_id_copied_from_encrypted_metadata_pseudo_copy',
            '--fetchMetadata',
        ],
        additional_env={
            'B2_SOURCE_SSE_C_KEY_B64': base64.b64encode(secret).decode(),
            'B2_DESTINATION_SSE_C_KEY_B64': base64.b64encode(os.urandom(32)).decode(),
            'B2_DESTINATION_SSE_C_KEY_ID': 'another-user-generated-key-id',
        }
    )
    list_of_files = b2_tool.should_succeed_json(['ls', '--json', '--recursive', bucket_name])
    should_equal(
        [
            {
                'file_name': 'encrypted_no_id_copied_from_encrypted',
                'sse_c_key_id': 'missing_key',
                'serverSideEncryption':
                    {
                        "algorithm": "AES256",
                        "customerKey": "******",
                        "customerKeyMd5": "******",
                        "mode": "SSE-C"
                    },
            },
            {
                'file_name': 'encrypted_with_id_copied_from_encrypted_metadata_pseudo_copy',
                'sse_c_key_id': 'another-user-generated-key-id',
                'serverSideEncryption':
                    {
                        'algorithm': 'AES256',
                        "customerKey": "******",
                        "customerKeyMd5": "******",
                        'mode': 'SSE-C',
                    },
            },
            {
                'file_name': 'encrypted_with_id_copied_from_encrypted_metadata_replace',
                'sse_c_key_id': 'another-user-generated-key-id',
                'serverSideEncryption':
                    {
                        'algorithm': 'AES256',
                        "customerKey": "******",
                        "customerKeyMd5": "******",
                        'mode': 'SSE-C',
                    },
            },
            {
                'file_name': 'not_encrypted_copied_from_encrypted_metadata_pseudo_copy',
                'sse_c_key_id': 'missing_key',
                'serverSideEncryption': {
                    'mode': 'none',
                },
            },
            {
                'file_name': 'not_encrypted_copied_from_encrypted_metadata_replace',
                'sse_c_key_id': 'missing_key',
                'serverSideEncryption': {
                    'mode': 'none',
                },
            },
            {
                'file_name': 'not_encrypted_copied_from_encrypted_metadata_replace_empty',
                'sse_c_key_id': 'missing_key',
                'serverSideEncryption': {
                    'mode': 'none',
                },
            },
            {
                'file_name': 'uploaded_encrypted',
                'sse_c_key_id': sse_c_key_id,
                'serverSideEncryption':
                    {
                        "algorithm": "AES256",
                        "customerKey": "******",
                        "customerKeyMd5": "******",
                        "mode": "SSE-C"
                    },
            },
        ],
        sorted(
            [
                {
                    'sse_c_key_id':
                        f['fileInfo'].get(SSE_C_KEY_ID_FILE_INFO_KEY_NAME, 'missing_key'),
                    'serverSideEncryption':
                        f['serverSideEncryption'],
                    'file_name':
                        f['fileName']
                } for f in list_of_files
            ],
            key=lambda r: r['file_name']
        )
    )


@pytest.mark.skipif(
    (sys.version_info.major, sys.version_info.minor) < (3, 8),
    reason="License extraction doesn't work on older versions, and we're only "
    "obliged to provide this "
    "data in bundled and built packages."
)
@pytest.mark.parametrize('with_packages', [True, False])
def test_license(b2_tool, with_packages):
    license_text = b2_tool.should_succeed(
        ['license'] + (['--with-packages'] if with_packages else [])
    )

    if with_packages:
        full_license_re = re.compile(
            r'Licenses of all modules used by b2(\.EXE)?, shipped with it in binary form:\r?\n'
            r'\+-*\+-*\+\r?\n'
            r'\|\s*Module name\s*\|\s*License text\s*\|\r?\n'
            r'.*'
            r'\+-*\+-*\+\r?\n', re.MULTILINE + re.DOTALL
        )
        full_license_text = next(full_license_re.finditer(license_text), None)
        assert full_license_text, license_text
        assert len(
            full_license_text.group(0)
        ) > 140_000  # we should know if the length of this block changes dramatically
        # Note that GitHub CI adds additional packages:
        # 'colorlog', 'virtualenv', 'nox', 'packaging', 'argcomplete', 'filelock'
        # that sum up to around 50k characters. Tests ran from docker image are unaffected.

        license_summary_re = re.compile(
            r'Summary of all modules used by b2(\.EXE)?, shipped with it in binary form:\r?\n'
            r'\+-*\+-*\+-*\+-*\+-*\+\r?\n'
            r'\|\s*Module name\s*\|\s*Version\s*\|\s*License\s*\|\s*Author\s*\|\s*URL\s*\|\r?\n'
            r'.*'
            r'\+-*\+-*\+-*\+-*\+-*\+\r?\n', re.MULTILINE + re.DOTALL
        )
        license_summary_text = next(license_summary_re.finditer(license_text), None)
        assert license_summary_text, license_text
        assert len(
            license_summary_text.group(0)
        ) > 6_300  # we should know if the length of this block changes dramatically

    assert """ license:
Backblaze wants developers and organization to copy and re-use our
code examples, so we make the samples available by several different
licenses.  One option is the MIT license (below).  Other options are
available here:

    https://www.backblaze.com/using_b2_code.html


The MIT License (MIT)

Copyright (c) 2015 Backblaze

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.""" in license_text.replace(os.linesep, '\n'), repr(license_text[-2000:])


def test_file_lock(
    b2_tool, application_key_id, application_key, sample_file, bucket_factory,
    schedule_bucket_cleanup
):
    lock_disabled_bucket_name = bucket_factory(bucket_type='allPrivate').name

    now_millis = current_time_millis()

    not_lockable_file = b2_tool.should_succeed_json(  # file in a lock disabled bucket
        ['upload-file', '--quiet', lock_disabled_bucket_name, sample_file, 'a']
    )

    _assert_file_lock_configuration(
        b2_tool,
        not_lockable_file['fileId'],
        retention_mode=RetentionMode.NONE,
        legal_hold=LegalHold.UNSET
    )

    b2_tool.should_fail(
        [
            'upload-file',
            '--quiet',
            lock_disabled_bucket_name,
            sample_file,
            'a',
            '--fileRetentionMode',
            'governance',
            '--retainUntil',
            str(now_millis + 1.5 * ONE_HOUR_MILLIS),
            '--legalHold',
            'on',
        ], r'ERROR: The bucket is not file lock enabled \(bucket_missing_file_lock\)'
    )

    b2_tool.should_fail(
        [
            'update-bucket', lock_disabled_bucket_name, 'allPrivate', '--defaultRetentionMode',
            'compliance'
        ], 'ValueError: must specify period for retention mode RetentionMode.COMPLIANCE'
    )
    b2_tool.should_fail(
        [
            'update-bucket', lock_disabled_bucket_name, 'allPrivate', '--defaultRetentionMode',
            'compliance', '--defaultRetentionPeriod', '7 days'
        ], r'ERROR: The bucket is not file lock enabled \(bucket_missing_file_lock\)'
    )
    lock_enabled_bucket_name = b2_tool.generate_bucket_name()
    schedule_bucket_cleanup(lock_enabled_bucket_name)
    b2_tool.should_succeed(
        [
            'create-bucket',
            lock_enabled_bucket_name,
            'allPrivate',
            '--fileLockEnabled',
            *b2_tool.get_bucket_info_args(),
        ],
    )
    updated_bucket = b2_tool.should_succeed_json(
        [
            'update-bucket',
            lock_enabled_bucket_name,
            'allPrivate',
            '--defaultRetentionMode',
            'governance',
            '--defaultRetentionPeriod',
            '1 days',
        ],
    )
    assert updated_bucket['defaultRetention'] == {
        'mode': 'governance',
        'period': {
            'duration': 1,
            'unit': 'days',
        },
    }

    lockable_file = b2_tool.should_succeed_json(  # file in a lock enabled bucket
        ['upload-file', '--noProgress', '--quiet', lock_enabled_bucket_name, sample_file, 'a']
    )

    b2_tool.should_fail(
        [
            'update-file-retention', not_lockable_file['fileName'], not_lockable_file['fileId'],
            'governance', '--retainUntil',
            str(now_millis + ONE_DAY_MILLIS + ONE_HOUR_MILLIS)
        ], r'ERROR: The bucket is not file lock enabled \(bucket_missing_file_lock\)'
    )

    b2_tool.should_succeed(  # first let's try with a file name
        ['update-file-retention', lockable_file['fileName'], lockable_file['fileId'], 'governance',
         '--retainUntil', str(now_millis + ONE_DAY_MILLIS + ONE_HOUR_MILLIS)]
    )

    _assert_file_lock_configuration(
        b2_tool,
        lockable_file['fileId'],
        retention_mode=RetentionMode.GOVERNANCE,
        retain_until=now_millis + ONE_DAY_MILLIS + ONE_HOUR_MILLIS
    )

    b2_tool.should_succeed(  # and now without a file name
        ['update-file-retention', lockable_file['fileId'], 'governance',
         '--retainUntil', str(now_millis + ONE_DAY_MILLIS + 2 * ONE_HOUR_MILLIS)]
    )

    _assert_file_lock_configuration(
        b2_tool,
        lockable_file['fileId'],
        retention_mode=RetentionMode.GOVERNANCE,
        retain_until=now_millis + ONE_DAY_MILLIS + 2 * ONE_HOUR_MILLIS
    )

    b2_tool.should_fail(
        [
            'update-file-retention', lockable_file['fileName'], lockable_file['fileId'],
            'governance', '--retainUntil',
            str(now_millis + ONE_HOUR_MILLIS)
        ],
        "ERROR: Auth token not authorized to write retention or file already in 'compliance' mode or "
        "bypassGovernance=true parameter missing",
    )
    b2_tool.should_succeed(
        [
            'update-file-retention', lockable_file['fileName'], lockable_file['fileId'],
            'governance', '--retainUntil',
            str(now_millis + ONE_HOUR_MILLIS), '--bypassGovernance'
        ],
    )

    _assert_file_lock_configuration(
        b2_tool,
        lockable_file['fileId'],
        retention_mode=RetentionMode.GOVERNANCE,
        retain_until=now_millis + ONE_HOUR_MILLIS
    )

    b2_tool.should_fail(
        ['update-file-retention', lockable_file['fileName'], lockable_file['fileId'], 'none'],
        "ERROR: Auth token not authorized to write retention or file already in 'compliance' mode or "
        "bypassGovernance=true parameter missing",
    )
    b2_tool.should_succeed(
        [
            'update-file-retention', lockable_file['fileName'], lockable_file['fileId'], 'none',
            '--bypassGovernance'
        ],
    )

    _assert_file_lock_configuration(
        b2_tool, lockable_file['fileId'], retention_mode=RetentionMode.NONE
    )

    b2_tool.should_fail(
        ['update-file-legal-hold', not_lockable_file['fileId'], 'on'],
        r'ERROR: The bucket is not file lock enabled \(bucket_missing_file_lock\)'
    )

    b2_tool.should_succeed(  # first let's try with a file name
        ['update-file-legal-hold', lockable_file['fileName'], lockable_file['fileId'], 'on'],
    )

    _assert_file_lock_configuration(b2_tool, lockable_file['fileId'], legal_hold=LegalHold.ON)

    b2_tool.should_succeed(  # and now without a file name
        ['update-file-legal-hold', lockable_file['fileId'], 'off'],
    )

    _assert_file_lock_configuration(b2_tool, lockable_file['fileId'], legal_hold=LegalHold.OFF)

    updated_bucket = b2_tool.should_succeed_json(
        [
            'update-bucket',
            lock_enabled_bucket_name,
            'allPrivate',
            '--defaultRetentionMode',
            'none',
        ],
    )
    assert updated_bucket['defaultRetention'] == {'mode': None}

    b2_tool.should_fail(
        [
            'upload-file',
            '--noProgress',
            '--quiet',
            lock_enabled_bucket_name,
            sample_file,
            'a',
            '--fileRetentionMode',
            'governance',
            '--retainUntil',
            str(now_millis - 1.5 * ONE_HOUR_MILLIS),
        ],
        r'ERROR: The retainUntilTimestamp must be in future \(retain_until_timestamp_must_be_in_future\)',
    )

    uploaded_file = b2_tool.should_succeed_json(
        [
            'upload-file',
            '--noProgress',
            '--quiet',
            lock_enabled_bucket_name,
            sample_file,
            'a',
            '--fileRetentionMode',
            'governance',
            '--retainUntil',
            str(now_millis + 1.5 * ONE_HOUR_MILLIS),
            '--legalHold',
            'on',
        ]
    )

    _assert_file_lock_configuration(
        b2_tool,
        uploaded_file['fileId'],
        retention_mode=RetentionMode.GOVERNANCE,
        retain_until=now_millis + 1.5 * ONE_HOUR_MILLIS,
        legal_hold=LegalHold.ON
    )

    b2_tool.should_fail(
        [
            'copy-file-by-id',
            lockable_file['fileId'],
            lock_disabled_bucket_name,
            'copied',
            '--fileRetentionMode',
            'governance',
            '--retainUntil',
            str(now_millis + 1.25 * ONE_HOUR_MILLIS),
            '--legalHold',
            'off',
        ], r'ERROR: The bucket is not file lock enabled \(bucket_missing_file_lock\)'
    )

    copied_file = b2_tool.should_succeed_json(
        [
            'copy-file-by-id',
            lockable_file['fileId'],
            lock_enabled_bucket_name,
            'copied',
            '--fileRetentionMode',
            'governance',
            '--retainUntil',
            str(now_millis + 1.25 * ONE_HOUR_MILLIS),
            '--legalHold',
            'off',
        ]
    )

    _assert_file_lock_configuration(
        b2_tool,
        copied_file['fileId'],
        retention_mode=RetentionMode.GOVERNANCE,
        retain_until=now_millis + 1.25 * ONE_HOUR_MILLIS,
        legal_hold=LegalHold.OFF
    )
    lock_disabled_key_id, lock_disabled_key = make_lock_disabled_key(b2_tool)

    b2_tool.should_succeed(
        [
            'authorize-account', '--environment', b2_tool.realm, lock_disabled_key_id,
            lock_disabled_key
        ],
    )

    file_lock_without_perms_test(
        b2_tool,
        lock_enabled_bucket_name,
        lock_disabled_bucket_name,
        lockable_file['fileId'],
        not_lockable_file['fileId'],
        sample_file=sample_file
    )

    b2_tool.should_succeed(
        ['authorize-account', '--environment', b2_tool.realm, application_key_id, application_key],
    )

    deleting_locked_files(
        b2_tool, lock_enabled_bucket_name, lock_disabled_key_id, lock_disabled_key, sample_file
    )


def make_lock_disabled_key(b2_tool):
    key_name = 'no-perms-for-file-lock' + random_hex(6)
    created_key_stdout = b2_tool.should_succeed(
        [
            'create-key',
            key_name,
            'listFiles,listBuckets,readFiles,writeKeys,deleteFiles',
        ]
    )
    key_id, key = created_key_stdout.split()
    return key_id, key


def file_lock_without_perms_test(
    b2_tool, lock_enabled_bucket_name, lock_disabled_bucket_name, lockable_file_id,
    not_lockable_file_id, sample_file
):

    b2_tool.should_fail(
        [
            'update-bucket', lock_enabled_bucket_name, 'allPrivate', '--defaultRetentionMode',
            'governance', '--defaultRetentionPeriod', '1 days'
        ],
        'ERROR: unauthorized for application key with capabilities',
    )

    _assert_file_lock_configuration(
        b2_tool,
        lockable_file_id,
        retention_mode=RetentionMode.UNKNOWN,
        legal_hold=LegalHold.UNKNOWN
    )

    b2_tool.should_fail(
        [
            'update-file-retention', lockable_file_id, 'governance', '--retainUntil',
            str(current_time_millis() + 7 * ONE_DAY_MILLIS)
        ],
        "ERROR: Auth token not authorized to write retention or file already in 'compliance' mode or "
        "bypassGovernance=true parameter missing",
    )

    b2_tool.should_fail(
        [
            'update-file-retention', not_lockable_file_id, 'governance', '--retainUntil',
            str(current_time_millis() + 7 * ONE_DAY_MILLIS)
        ],
        "ERROR: Auth token not authorized to write retention or file already in 'compliance' mode or "
        "bypassGovernance=true parameter missing",
    )

    b2_tool.should_fail(
        ['update-file-legal-hold', lockable_file_id, 'on'],
        "ERROR: Auth token not authorized to write retention or file already in 'compliance' mode or "
        "bypassGovernance=true parameter missing",
    )

    b2_tool.should_fail(
        ['update-file-legal-hold', not_lockable_file_id, 'on'],
        "ERROR: Auth token not authorized to write retention or file already in 'compliance' mode or "
        "bypassGovernance=true parameter missing",
    )

    b2_tool.should_fail(
        [
            'upload-file',
            '--noProgress',
            '--quiet',
            lock_enabled_bucket_name,
            sample_file,
            'bound_to_fail_anyway',
            '--fileRetentionMode',
            'governance',
            '--retainUntil',
            str(current_time_millis() + ONE_HOUR_MILLIS),
            '--legalHold',
            'on',
        ],
        "unauthorized for application key with capabilities",
    )

    b2_tool.should_fail(
        [
            'upload-file',
            '--noProgress',
            '--quiet',
            lock_disabled_bucket_name,
            sample_file,
            'bound_to_fail_anyway',
            '--fileRetentionMode',
            'governance',
            '--retainUntil',
            str(current_time_millis() + ONE_HOUR_MILLIS),
            '--legalHold',
            'on',
        ],
        "unauthorized for application key with capabilities",
    )

    b2_tool.should_fail(
        [
            'copy-file-by-id',
            lockable_file_id,
            lock_enabled_bucket_name,
            'copied',
            '--fileRetentionMode',
            'governance',
            '--retainUntil',
            str(current_time_millis() + ONE_HOUR_MILLIS),
            '--legalHold',
            'off',
        ],
        'ERROR: unauthorized for application key with capabilities',
    )

    b2_tool.should_fail(
        [
            'copy-file-by-id',
            lockable_file_id,
            lock_disabled_bucket_name,
            'copied',
            '--fileRetentionMode',
            'governance',
            '--retainUntil',
            str(current_time_millis() + ONE_HOUR_MILLIS),
            '--legalHold',
            'off',
        ],
        'ERROR: unauthorized for application key with capabilities',
    )


def upload_locked_file(b2_tool, bucket_name, sample_file):
    return b2_tool.should_succeed_json(
        [
            'upload-file',
            '--noProgress',
            '--quiet',
            '--fileRetentionMode',
            'governance',
            '--retainUntil',
            str(int(time.time()) + 1000),
            bucket_name,
            sample_file,
            'a-locked',
        ]
    )


def deleting_locked_files(
    b2_tool, lock_enabled_bucket_name, lock_disabled_key_id, lock_disabled_key, sample_file
):
    locked_file = upload_locked_file(b2_tool, lock_enabled_bucket_name, sample_file)
    b2_tool.should_fail(
        [  # master key
            'delete-file-version',
            locked_file['fileName'],
            locked_file['fileId'],
        ],
        "ERROR: Access Denied for application key "
    )
    b2_tool.should_succeed([  # master key
        'delete-file-version',
        locked_file['fileName'],
        locked_file['fileId'],
        '--bypassGovernance'
    ])

    locked_file = upload_locked_file(b2_tool, lock_enabled_bucket_name, sample_file)

    b2_tool.should_succeed(
        [
            'authorize-account', '--environment', b2_tool.realm, lock_disabled_key_id,
            lock_disabled_key
        ],
    )
    b2_tool.should_fail([  # lock disabled key
        'delete-file-version',
        locked_file['fileName'],
        locked_file['fileId'],
        '--bypassGovernance',
    ], "ERROR: unauthorized for application key with capabilities '")


def test_profile_switch(b2_tool):
    # this test could be unit, but it adds a lot of complexity because of
    # necessity to pass mocked B2Api to ConsoleTool; it's much easier to
    # just have an integration test instead

    MISSING_ACCOUNT_PATTERN = 'Missing account data'

    b2_tool.should_succeed(
        [
            'authorize-account',
            '--environment',
            b2_tool.realm,
            b2_tool.account_id,
            b2_tool.application_key,
        ]
    )
    b2_tool.should_succeed(['get-account-info'])
    b2_tool.should_succeed(['clear-account'])
    b2_tool.should_fail(['get-account-info'], expected_pattern=MISSING_ACCOUNT_PATTERN)

    # in order to use --profile flag, we need to temporary
    # delete B2_ACCOUNT_INFO_ENV_VAR
    B2_ACCOUNT_INFO = os.environ.pop(B2_ACCOUNT_INFO_ENV_VAR, None)

    # now authorize a different account
    profile = 'profile-for-test-' + random_hex(6)
    b2_tool.should_fail(
        ['get-account-info', '--profile', profile],
        expected_pattern=MISSING_ACCOUNT_PATTERN,
    )
    b2_tool.should_succeed(
        [
            'authorize-account',
            '--environment',
            b2_tool.realm,
            '--profile',
            profile,
            b2_tool.account_id,
            b2_tool.application_key,
        ]
    )

    account_info = b2_tool.should_succeed_json(['get-account-info', '--profile', profile])
    account_file_path = account_info['accountFilePath']
    assert profile in account_file_path, \
        'accountFilePath "{}" should contain profile name "{}"'.format(
            account_file_path, profile,
        )

    b2_tool.should_succeed(['clear-account', '--profile', profile])
    b2_tool.should_fail(
        ['get-account-info', '--profile', profile],
        expected_pattern=MISSING_ACCOUNT_PATTERN,
    )
    os.remove(account_file_path)

    # restore B2_ACCOUNT_INFO_ENV_VAR, if existed
    if B2_ACCOUNT_INFO:
        os.environ[B2_ACCOUNT_INFO_ENV_VAR] = B2_ACCOUNT_INFO


def test_replication_basic(b2_tool, bucket_name, schedule_bucket_cleanup):
    key_one_name = 'clt-testKey-01' + random_hex(6)
    created_key_stdout = b2_tool.should_succeed(
        [
            'create-key',
            key_one_name,
            'listBuckets,readFiles',
        ]
    )
    key_one_id, _ = created_key_stdout.split()

    key_two_name = 'clt-testKey-02' + random_hex(6)
    created_key_stdout = b2_tool.should_succeed(
        [
            'create-key',
            key_two_name,
            'listBuckets,writeFiles',
        ]
    )
    key_two_id, _ = created_key_stdout.split()

    destination_bucket_name = bucket_name
    destination_bucket = b2_tool.should_succeed_json(['get-bucket', destination_bucket_name])

    # test that by default there's no `replicationConfiguration` key
    assert 'replicationConfiguration' not in destination_bucket

    # ---------------- set up replication destination ----------------

    # update destination bucket info
    destination_replication_configuration = {
        'asReplicationSource': None,
        'asReplicationDestination': {
            'sourceToDestinationKeyMapping': {
                key_one_id: key_two_id,
            },
        },
    }
    destination_replication_configuration_json = json.dumps(destination_replication_configuration)
    destination_bucket = b2_tool.should_succeed_json(
        [
            'update-bucket',
            destination_bucket_name,
            'allPublic',
            '--replication',
            destination_replication_configuration_json,
        ]
    )

    # test that destination bucket is registered as replication destination
    assert destination_bucket['replication'].get('asReplicationSource') is None
    assert destination_bucket['replication'
                             ]['asReplicationDestination'
                              ] == destination_replication_configuration['asReplicationDestination']

    # ---------------- set up replication source ----------------
    source_replication_configuration = {
        "asReplicationSource":
            {
                "replicationRules":
                    [
                        {
                            "destinationBucketId": destination_bucket['bucketId'],
                            "fileNamePrefix": "one/",
                            "includeExistingFiles": False,
                            "isEnabled": True,
                            "priority": 1,
                            "replicationRuleName": "replication-one"
                        }, {
                            "destinationBucketId": destination_bucket['bucketId'],
                            "fileNamePrefix": "two/",
                            "includeExistingFiles": False,
                            "isEnabled": True,
                            "priority": 2,
                            "replicationRuleName": "replication-two"
                        }
                    ],
                "sourceApplicationKeyId": key_one_id,
            },
    }
    source_replication_configuration_json = json.dumps(source_replication_configuration)

    # create a source bucket and set up replication to destination bucket
    source_bucket_name = b2_tool.generate_bucket_name()
    schedule_bucket_cleanup(source_bucket_name)
    b2_tool.should_succeed(
        [
            'create-bucket',
            source_bucket_name,
            'allPublic',
            '--replication',
            source_replication_configuration_json,
            *b2_tool.get_bucket_info_args(),
        ]
    )
    source_bucket = b2_tool.should_succeed_json(['get-bucket', source_bucket_name])

    # test that all replication rules are present in source bucket
    assert source_bucket['replication']['asReplicationSource'
                                       ] == source_replication_configuration['asReplicationSource']

    # test that source bucket is not mentioned as replication destination
    assert source_bucket['replication'].get('asReplicationDestination') is None

    # ---------------- attempt enabling object lock  ----------------
    b2_tool.should_fail(
        ['update-bucket', source_bucket_name, '--fileLockEnabled'],
        'ERROR: Operation not supported for buckets with source replication'
    )

    # ---------------- remove replication source ----------------

    no_replication_configuration = {
        'asReplicationSource': None,
        'asReplicationDestination': None,
    }
    no_replication_configuration_json = json.dumps(no_replication_configuration)
    source_bucket = b2_tool.should_succeed_json(
        [
            'update-bucket', source_bucket_name, 'allPublic', '--replication',
            no_replication_configuration_json
        ]
    )

    # test that source bucket replication is removed
    assert source_bucket['replication'] == {
        'asReplicationDestination': None,
        'asReplicationSource': None
    }

    # ---------------- remove replication destination ----------------

    destination_bucket = b2_tool.should_succeed_json(
        [
            'update-bucket',
            destination_bucket_name,
            'allPublic',
            '--replication',
            '{}',
        ]
    )

    # test that destination bucket replication is removed
    assert destination_bucket['replication'] == {
        'asReplicationDestination': None,
        'asReplicationSource': None
    }

    b2_tool.should_succeed(['delete-key', key_one_id])
    b2_tool.should_succeed(['delete-key', key_two_id])


def test_replication_setup(b2_tool, bucket_name, schedule_bucket_cleanup):
    source_bucket_name = b2_tool.generate_bucket_name()
    schedule_bucket_cleanup(source_bucket_name)
    b2_tool.should_succeed(
        [
            'create-bucket',
            source_bucket_name,
            'allPublic',
            '--fileLockEnabled',
            *b2_tool.get_bucket_info_args(),
        ]
    )
    destination_bucket_name = bucket_name
    b2_tool.should_succeed(['replication-setup', source_bucket_name, destination_bucket_name])
    destination_bucket_old = b2_tool.should_succeed_json(['get-bucket', destination_bucket_name])

    b2_tool.should_succeed(
        [
            'replication-setup',
            '--priority',
            '132',
            '--file-name-prefix',
            'foo',
            '--name',
            'my-replication-rule',
            source_bucket_name,
            destination_bucket_name,
        ]
    )
    source_bucket = b2_tool.should_succeed_json(['get-bucket', source_bucket_name])
    destination_bucket = b2_tool.should_succeed_json(['get-bucket', destination_bucket_name])
    assert source_bucket['replication']['asReplicationSource']['replicationRules'] == [
        {
            "destinationBucketId": destination_bucket['bucketId'],
            "fileNamePrefix": "",
            "includeExistingFiles": False,
            "isEnabled": True,
            "priority": 128,
            "replicationRuleName": destination_bucket['bucketName'],
        },
        {
            "destinationBucketId": destination_bucket['bucketId'],
            "fileNamePrefix": "foo",
            "includeExistingFiles": False,
            "isEnabled": True,
            "priority": 132,
            "replicationRuleName": "my-replication-rule",
        },
    ]

    for key_one_id, key_two_id in destination_bucket['replication']['asReplicationDestination'][
        'sourceToDestinationKeyMapping'].items():
        b2_tool.should_succeed(['delete-key', key_one_id])
        b2_tool.should_succeed(['delete-key', key_two_id])
    assert destination_bucket_old['replication']['asReplicationDestination'][
        'sourceToDestinationKeyMapping'] == destination_bucket['replication'][
            'asReplicationDestination']['sourceToDestinationKeyMapping']


def test_replication_monitoring(b2_tool, bucket_name, sample_file, schedule_bucket_cleanup):

    # ---------------- set up keys ----------------
    key_one_name = 'clt-testKey-01' + random_hex(6)
    created_key_stdout = b2_tool.should_succeed(
        [
            'create-key',
            key_one_name,
            'listBuckets,readFiles',
        ]
    )
    key_one_id, _ = created_key_stdout.split()

    key_two_name = 'clt-testKey-02' + random_hex(6)
    created_key_stdout = b2_tool.should_succeed(
        [
            'create-key',
            key_two_name,
            'listBuckets,writeFiles',
        ]
    )
    key_two_id, _ = created_key_stdout.split()

    # ---------------- add test data ----------------
    destination_bucket_name = bucket_name
    uploaded_a = b2_tool.should_succeed_json(
        ['upload-file', '--quiet', destination_bucket_name, sample_file, 'one/a']
    )

    # ---------------- set up replication destination ----------------

    # update destination bucket info
    destination_replication_configuration = {
        'asReplicationSource': None,
        'asReplicationDestination': {
            'sourceToDestinationKeyMapping': {
                key_one_id: key_two_id,
            },
        },
    }
    destination_replication_configuration_json = json.dumps(destination_replication_configuration)
    destination_bucket = b2_tool.should_succeed_json(
        [
            'update-bucket',
            destination_bucket_name,
            'allPublic',
            '--replication',
            destination_replication_configuration_json,
        ]
    )

    # ---------------- set up replication source ----------------
    source_replication_configuration = {
        "asReplicationSource":
            {
                "replicationRules":
                    [
                        {
                            "destinationBucketId": destination_bucket['bucketId'],
                            "fileNamePrefix": "one/",
                            "includeExistingFiles": False,
                            "isEnabled": True,
                            "priority": 1,
                            "replicationRuleName": "replication-one"
                        }, {
                            "destinationBucketId": destination_bucket['bucketId'],
                            "fileNamePrefix": "two/",
                            "includeExistingFiles": False,
                            "isEnabled": True,
                            "priority": 2,
                            "replicationRuleName": "replication-two"
                        }
                    ],
                "sourceApplicationKeyId": key_one_id,
            },
    }
    source_replication_configuration_json = json.dumps(source_replication_configuration)

    # create a source bucket and set up replication to destination bucket
    source_bucket_name = b2_tool.generate_bucket_name()
    schedule_bucket_cleanup(source_bucket_name)
    b2_tool.should_succeed(
        [
            'create-bucket',
            source_bucket_name,
            'allPublic',
            '--fileLockEnabled',
            '--replication',
            source_replication_configuration_json,
            *b2_tool.get_bucket_info_args(),
        ]
    )

    # make test data
    uploaded_a = b2_tool.should_succeed_json(
        ['upload-file', '--quiet', source_bucket_name, sample_file, 'one/a']
    )
    b2_tool.should_succeed_json(
        [
            'upload-file',
            '--quiet',
            source_bucket_name,
            '--legalHold',
            'on',
            sample_file,
            'two/b',
        ]
    )

    # encryption
    # SSE-B2
    upload_encryption_args = ['--destinationServerSideEncryption', 'SSE-B2']
    upload_additional_env = {}
    b2_tool.should_succeed_json(
        ['upload-file', '--quiet', source_bucket_name, sample_file, 'two/c'] +
        upload_encryption_args,
        additional_env=upload_additional_env,
    )

    # SSE-C
    upload_encryption_args = ['--destinationServerSideEncryption', 'SSE-C']
    upload_additional_env = {
        'B2_DESTINATION_SSE_C_KEY_B64': base64.b64encode(SSE_C_AES.key.secret).decode(),
        'B2_DESTINATION_SSE_C_KEY_ID': SSE_C_AES.key.key_id,
    }
    b2_tool.should_succeed_json(
        ['upload-file', '--quiet', source_bucket_name, sample_file, 'two/d'] +
        upload_encryption_args,
        additional_env=upload_additional_env,
    )

    # encryption + legal hold
    b2_tool.should_succeed_json(
        [
            'upload-file',
            '--quiet',
            source_bucket_name,
            sample_file,
            'two/e',
            '--legalHold',
            'on',
        ] + upload_encryption_args,
        additional_env=upload_additional_env,
    )

    # there is just one file, so clean after itself for faster execution
    b2_tool.should_succeed(['delete-file-version', uploaded_a['fileName'], uploaded_a['fileId']])

    # run stats command
    replication_status_json = b2_tool.should_succeed_json(
        [
            'replication-status',
            # '--destination-profile',
            # profile,
            '--noProgress',
            # '--columns=count, hash differs',
            '--output-format',
            'json',
            source_bucket_name,
        ]
    )

    assert replication_status_json in [
        {
            "replication-one":
                [
                    {
                        "count": 1,
                        "destination_replication_status": None,
                        "hash_differs": None,
                        "metadata_differs": None,
                        "source_has_file_retention": None,
                        "source_has_hide_marker": None,
                        "source_has_large_metadata": None,
                        "source_has_legal_hold": None,
                        "source_encryption_mode": None,
                        "source_replication_status": None,
                    }
                ],
            "replication-two":
                [
                    {
                        "count": 1,
                        "destination_replication_status": None,
                        "hash_differs": None,
                        "metadata_differs": None,
                        "source_has_file_retention": False,
                        "source_has_hide_marker": False,
                        "source_has_large_metadata": False,
                        "source_has_legal_hold": True,
                        "source_encryption_mode": 'none',
                        "source_replication_status": first,
                    }, {
                        "count": 1,
                        "destination_replication_status": None,
                        "hash_differs": None,
                        "metadata_differs": None,
                        "source_has_file_retention": False,
                        "source_has_hide_marker": False,
                        "source_has_large_metadata": False,
                        "source_has_legal_hold": False,
                        "source_encryption_mode": 'SSE-B2',
                        "source_replication_status": second,
                    }, {
                        "count": 1,
                        "destination_replication_status": None,
                        "hash_differs": None,
                        "metadata_differs": None,
                        "source_has_file_retention": False,
                        "source_has_hide_marker": False,
                        "source_has_large_metadata": False,
                        "source_has_legal_hold": False,
                        "source_encryption_mode": 'SSE-C',
                        "source_replication_status": None,
                    }, {
                        "count": 1,
                        "destination_replication_status": None,
                        "hash_differs": None,
                        "metadata_differs": None,
                        "source_has_file_retention": False,
                        "source_has_hide_marker": False,
                        "source_has_large_metadata": False,
                        "source_has_legal_hold": True,
                        "source_encryption_mode": 'SSE-C',
                        "source_replication_status": None,
                    }
                ]
        } for first, second in itertools.product(['FAILED', 'PENDING'], ['FAILED', 'PENDING'])
    ]


def test_enable_file_lock_first_retention_second(b2_tool, bucket_name):
    # enable file lock only
    b2_tool.should_succeed(['update-bucket', bucket_name, '--fileLockEnabled'])

    # set retention with file lock already enabled
    b2_tool.should_succeed(
        [
            'update-bucket', bucket_name, '--defaultRetentionMode', 'compliance',
            '--defaultRetentionPeriod', '7 days'
        ]
    )

    # attempt to re-enable should be a noop
    b2_tool.should_succeed(['update-bucket', bucket_name, '--fileLockEnabled'])


def test_enable_file_lock_and_set_retention_at_once(b2_tool, bucket_name):
    # attempt setting retention without file lock enabled
    b2_tool.should_fail(
        [
            'update-bucket', bucket_name, '--defaultRetentionMode', 'compliance',
            '--defaultRetentionPeriod', '7 days'
        ], r'ERROR: The bucket is not file lock enabled \(bucket_missing_file_lock\)'
    )

    # enable file lock and set retention at once
    b2_tool.should_succeed(
        [
            'update-bucket', bucket_name, '--defaultRetentionMode', 'compliance',
            '--defaultRetentionPeriod', '7 days', '--fileLockEnabled'
        ]
    )

    # attempt to re-enable should be a noop
    b2_tool.should_succeed(['update-bucket', bucket_name, '--fileLockEnabled'])


def _assert_file_lock_configuration(
    b2_tool,
    file_id,
    retention_mode: RetentionMode | None = None,
    retain_until: int | None = None,
    legal_hold: LegalHold | None = None
):

    file_version = b2_tool.should_succeed_json(['get-file-info', file_id])
    if retention_mode is not None:
        if file_version['fileRetention']['mode'] == 'unknown':
            actual_file_retention = UNKNOWN_FILE_RETENTION_SETTING
        else:
            actual_file_retention = FileRetentionSetting.from_file_retention_value_dict(
                file_version['fileRetention']
            )
        expected_file_retention = FileRetentionSetting(retention_mode, retain_until)
        assert expected_file_retention == actual_file_retention
    if legal_hold is not None:
        if file_version['legalHold'] == 'unknown':
            actual_legal_hold = LegalHold.UNKNOWN
        else:
            actual_legal_hold = LegalHold.from_string_or_none(file_version['legalHold'])
        assert legal_hold == actual_legal_hold


def test_upload_file__custom_upload_time(b2_tool, bucket_name, sample_file):
    file_data = read_file(sample_file)
    cut = 12345
    cut_printable = '1970-01-01  00:00:12'
    args = [
        'upload-file',
        '--noProgress',
        '--custom-upload-time',
        str(cut),
        '--quiet',
        bucket_name,
        sample_file,
        'a',
    ]
    succeeded, stdout = b2_tool.run_command(args)
    if not succeeded:
        b2_tool.should_fail(args, 'custom_timestamp_not_allowed')
    else:
        # file_id, action, date, time, size(, replication), name
        b2_tool.should_succeed(
            ['ls', '--long', bucket_name], '^4_z.*  upload  {} +{}  a'.format(
                cut_printable,
                len(file_data),
            )
        )
        # file_id, action, date, time, size(, replication), name
        b2_tool.should_succeed(
            ['ls', '--long', '--replication', bucket_name], '^4_z.*  upload  {} +{}  -  a'.format(
                cut_printable,
                len(file_data),
            )
        )


@skip_on_windows
def test_upload_file__stdin_pipe_operator(request, bash_runner, b2_tool, bucket_name):
    """Test upload-file from stdin using pipe operator."""
    content = request.node.name
    run = bash_runner(
        f'echo -n {content!r} '
        f'| '
        f'{" ".join(b2_tool.parse_command(b2_tool.prepare_env()))} upload-file {bucket_name} - {request.node.name}.txt'
    )
    assert hashlib.sha1(content.encode()).hexdigest() in run.stdout


@skip_on_windows
def test_upload_unbound_stream__redirect_operator(
    request, bash_runner, b2_tool, bucket_name, is_running_on_docker
):
    """Test upload-unbound-stream from stdin using redirect operator."""
    if is_running_on_docker:
        pytest.skip('Not supported on Docker')
    content = request.node.name
    run = bash_runner(
        f'b2 upload-unbound-stream {bucket_name} <(echo -n {content}) {request.node.name}.txt'
    )
    assert hashlib.sha1(content.encode()).hexdigest() in run.stdout


def test_download_file_stdout(
    b2_tool, bucket_name, sample_filepath, tmp_path, uploaded_sample_file
):
    assert b2_tool.should_succeed(
        ['download-file-by-name', '--quiet', bucket_name, uploaded_sample_file['fileName'], '-'],
    ).replace("\r", "") == sample_filepath.read_text()
    assert b2_tool.should_succeed(
        ['download-file-by-id', '--quiet', uploaded_sample_file['fileId'], '-'],
    ).replace("\r", "") == sample_filepath.read_text()


def test_cat(b2_tool, bucket_name, sample_filepath, tmp_path, uploaded_sample_file):
    assert b2_tool.should_succeed(
        ['cat', f"b2://{bucket_name}/{uploaded_sample_file['fileName']}"],
    ).replace("\r", "") == sample_filepath.read_text()
    assert b2_tool.should_succeed(['cat', f"b2id://{uploaded_sample_file['fileId']}"
                                  ],).replace("\r", "") == sample_filepath.read_text()
