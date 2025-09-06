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
import pathlib
import re
import sys
import time
from pathlib import Path
from tempfile import mkdtemp

import pytest
from b2sdk.v3 import (
    B2_ACCOUNT_INFO_ENV_VAR,
    SSE_C_KEY_ID_FILE_INFO_KEY_NAME,
    UNKNOWN_FILE_RETENTION_SETTING,
    EncryptionMode,
    EncryptionSetting,
    FileRetentionSetting,
    LegalHold,
    RetentionMode,
    SqliteAccountInfo,
    fix_windows_path_limit,
)
from b2sdk.v3.exception import MissingAccountData
from b2sdk.v3.testing import IntegrationTestBase

from b2._internal._cli.const import (
    B2_APPLICATION_KEY_ENV_VAR,
    B2_APPLICATION_KEY_ID_ENV_VAR,
    B2_ENVIRONMENT_ENV_VAR,
)
from b2._internal.console_tool import current_time_millis

from ..helpers import assert_dict_equal_ignore_extra, skip_on_windows
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


def test_authorize_account_via_params_saving_credentials(
    b2_tool,
    realm,
    application_key,
    application_key_id,
    account_info_file,
):
    """
    When calling `account authorize` and passing credentials as params,
    we want the credentials to be saved.
    """

    b2_tool.should_succeed(['account', 'clear'])

    assert B2_APPLICATION_KEY_ID_ENV_VAR not in os.environ
    assert B2_APPLICATION_KEY_ENV_VAR not in os.environ

    b2_tool.should_succeed(
        ['account', 'authorize', '--environment', realm, application_key_id, application_key]
    )

    assert account_info_file.exists()
    account_info = SqliteAccountInfo()
    assert account_info.get_application_key() == application_key
    assert account_info.get_application_key_id() == application_key_id


def test_authorize_account_via_env_vars_saving_credentials(
    b2_tool,
    realm,
    application_key,
    application_key_id,
    account_info_file,
):
    """
    When calling `account authorize` and passing credentials
    via env vars, we still want the credentials to be saved.
    """

    b2_tool.should_succeed(['account', 'clear'])

    assert B2_APPLICATION_KEY_ID_ENV_VAR not in os.environ
    assert B2_APPLICATION_KEY_ENV_VAR not in os.environ

    b2_tool.should_succeed(
        ['account', 'authorize'],
        additional_env={
            B2_ENVIRONMENT_ENV_VAR: realm,
            B2_APPLICATION_KEY_ID_ENV_VAR: application_key_id,
            B2_APPLICATION_KEY_ENV_VAR: application_key,
        },
    )

    assert account_info_file.exists()
    account_info = SqliteAccountInfo()
    assert account_info.get_application_key() == application_key
    assert account_info.get_application_key_id() == application_key_id


def test_clear_account_with_env_vars(
    b2_tool,
    realm,
    application_key,
    application_key_id,
    account_info_file,
):
    """
    When calling `account clear` and passing credentials via env vars,
    we want the credentials to be removed from the file.
    """

    assert account_info_file.exists()
    account_info = SqliteAccountInfo()
    assert account_info.get_application_key() == application_key
    assert account_info.get_application_key_id() == application_key_id

    b2_tool.should_succeed(
        ['account', 'clear'],
        additional_env={
            B2_ENVIRONMENT_ENV_VAR: realm,
            B2_APPLICATION_KEY_ID_ENV_VAR: application_key_id,
            B2_APPLICATION_KEY_ENV_VAR: application_key,
        },
    )

    assert account_info_file.exists()
    account_info = SqliteAccountInfo()
    with pytest.raises(MissingAccountData):
        account_info.get_application_key()
    with pytest.raises(MissingAccountData):
        account_info.get_application_key_id()


@pytest.mark.apiver(to_ver=3)
def test_command_with_env_vars_saving_credentials(
    b2_tool,
    realm,
    application_key,
    application_key_id,
    account_info_file,
    bucket_name,
    b2_uri_args,
):
    """
    When calling any command other then `account authorize` and passing credentials
    via env vars, we don't want them to be saved.
    """

    b2_tool.should_succeed(['account', 'clear'])

    assert B2_APPLICATION_KEY_ID_ENV_VAR not in os.environ
    assert B2_APPLICATION_KEY_ENV_VAR not in os.environ

    b2_tool.should_succeed(
        ['ls', '--long', *b2_uri_args(bucket_name)],
        additional_env={
            B2_ENVIRONMENT_ENV_VAR: realm,
            B2_APPLICATION_KEY_ID_ENV_VAR: application_key_id,
            B2_APPLICATION_KEY_ENV_VAR: application_key,
        },
    )

    assert account_info_file.exists()
    account_info = SqliteAccountInfo()
    assert account_info.get_application_key() == application_key
    assert account_info.get_application_key_id() == application_key_id


@pytest.mark.apiver(from_ver=4)
def test_command_with_env_vars_not_saving_credentials(
    b2_tool,
    realm,
    application_key,
    application_key_id,
    account_info_file,
    bucket_name,
    b2_uri_args,
):
    """
    When calling any command other then `account authorize` and passing credentials
    via env vars, we don't want them to be saved.
    """

    b2_tool.should_succeed(['account', 'clear'])

    assert B2_APPLICATION_KEY_ID_ENV_VAR not in os.environ
    assert B2_APPLICATION_KEY_ENV_VAR not in os.environ

    b2_tool.should_succeed(
        ['ls', '--long', *b2_uri_args(bucket_name)],
        additional_env={
            B2_ENVIRONMENT_ENV_VAR: realm,
            B2_APPLICATION_KEY_ID_ENV_VAR: application_key_id,
            B2_APPLICATION_KEY_ENV_VAR: application_key,
        },
    )

    assert account_info_file.exists()
    account_info = SqliteAccountInfo()
    with pytest.raises(MissingAccountData):
        account_info.get_application_key()
    with pytest.raises(MissingAccountData):
        account_info.get_application_key_id()


@pytest.mark.apiver(from_ver=4)
def test_command_with_env_vars_reusing_existing_account_info(
    b2_tool,
    realm,
    application_key,
    application_key_id,
    account_info_file,
    bucket_name,
    b2_uri_args,
):
    """
    When calling any command with credentials passed via env vars, and the account
    info file already contains the same credentials, we want to use filesystem for
    storing cache, not the in-memory cache.
    """

    assert B2_APPLICATION_KEY_ID_ENV_VAR not in os.environ
    assert B2_APPLICATION_KEY_ENV_VAR not in os.environ

    assert account_info_file.exists()
    account_info = SqliteAccountInfo()
    assert account_info.get_application_key() == application_key
    assert account_info.get_application_key_id() == application_key_id

    account_info.remove_bucket_name(bucket_name)
    assert account_info.get_bucket_id_or_none_from_bucket_name(bucket_name) is None

    b2_tool.should_succeed(
        ['ls', '--long', *b2_uri_args(bucket_name)],
        additional_env={
            B2_ENVIRONMENT_ENV_VAR: realm,
            B2_APPLICATION_KEY_ID_ENV_VAR: application_key_id,
            B2_APPLICATION_KEY_ENV_VAR: application_key,
        },
    )

    assert account_info_file.exists()
    account_info = SqliteAccountInfo()
    assert account_info.get_bucket_id_or_none_from_bucket_name(bucket_name) is not None


@pytest.fixture
def uploaded_sample_file(b2_tool, persistent_bucket, sample_filepath):
    return b2_tool.should_succeed_json(
        [
            'file',
            'upload',
            '--quiet',
            persistent_bucket.bucket_name,
            str(sample_filepath),
            f'{persistent_bucket.subfolder}/sample_file',
        ]
    )


def test_download(b2_tool, persistent_bucket, sample_filepath, uploaded_sample_file, tmp_path):
    output_a = tmp_path / 'a'
    b2_tool.should_succeed(
        [
            'file',
            'download',
            '--quiet',
            f"b2://{persistent_bucket.bucket_name}/{uploaded_sample_file['fileName']}",
            str(output_a),
        ]
    )
    assert output_a.read_text() == sample_filepath.read_text()

    output_b = tmp_path / 'b'
    b2_tool.should_succeed(
        ['file', 'download', '--quiet', f"b2id://{uploaded_sample_file['fileId']}", str(output_b)]
    )
    assert output_b.read_text() == sample_filepath.read_text()


def test_basic(b2_tool, persistent_bucket, sample_file, tmp_path, b2_uri_args, apiver_int):
    bucket_name = persistent_bucket.bucket_name
    subfolder = f'{persistent_bucket.subfolder}/'
    file_mod_time_str = str(file_mod_time_millis(sample_file))

    file_data = read_file(sample_file)
    hex_sha1 = hashlib.sha1(file_data).hexdigest()

    list_of_buckets = b2_tool.should_succeed_json(['bucket', 'list', '--json'])
    should_equal(
        [bucket_name], [b['bucketName'] for b in list_of_buckets if b['bucketName'] == bucket_name]
    )

    b2_tool.should_succeed(['file', 'upload', '--quiet', bucket_name, sample_file, f'{subfolder}a'])
    b2_tool.should_succeed(['ls', '--long', '--replication', *b2_uri_args(bucket_name)])
    b2_tool.should_succeed(
        ['file', 'upload', '--no-progress', bucket_name, sample_file, f'{subfolder}a']
    )
    b2_tool.should_succeed(
        ['file', 'upload', '--no-progress', bucket_name, sample_file, f'{subfolder}b/1']
    )
    b2_tool.should_succeed(
        ['file', 'upload', '--no-progress', bucket_name, sample_file, f'{subfolder}b/2']
    )
    b2_tool.should_succeed(
        [
            'file',
            'upload',
            '--no-progress',
            '--sha1',
            hex_sha1,
            '--info',
            'foo=bar=baz',
            '--info',
            'color=blue',
            bucket_name,
            sample_file,
            f'{subfolder}c',
        ]
    )
    b2_tool.should_fail(
        [
            'file',
            'upload',
            '--no-progress',
            '--sha1',
            hex_sha1,
            '--info',
            'foo-bar',
            '--info',
            'color=blue',
            bucket_name,
            sample_file,
            f'{subfolder}c',
        ],
        r'ERROR: Bad file info: foo-bar',
    )
    b2_tool.should_succeed(
        [
            'file',
            'upload',
            '--no-progress',
            '--content-type',
            'text/plain',
            bucket_name,
            sample_file,
            f'{subfolder}d',
        ]
    )

    b2_tool.should_succeed(
        ['file', 'upload', '--no-progress', bucket_name, sample_file, f'{subfolder}rm']
    )
    b2_tool.should_succeed(
        ['file', 'upload', '--no-progress', bucket_name, sample_file, f'{subfolder}rm1']
    )
    # with_wildcard allows us to target a single file. rm will be removed, rm1 will be left alone
    b2_tool.should_succeed(
        ['rm', '--recursive', '--with-wildcard', *b2_uri_args(bucket_name, f'{subfolder}rm')]
    )
    list_of_files = b2_tool.should_succeed_json(
        [
            'ls',
            '--json',
            '--recursive',
            '--with-wildcard',
            *b2_uri_args(bucket_name, f'{subfolder}rm*'),
        ]
    )
    should_equal([f'{subfolder}rm1'], [f['fileName'] for f in list_of_files])
    b2_tool.should_succeed(
        ['rm', '--recursive', '--with-wildcard', *b2_uri_args(bucket_name, f'{subfolder}rm1')]
    )

    b2_tool.should_succeed(
        ['file', 'download', '--quiet', f'b2://{bucket_name}/{subfolder}b/1', tmp_path / 'a']
    )

    b2_tool.should_succeed(['file', 'hide', f'b2://{bucket_name}/{subfolder}c'])

    list_of_files = b2_tool.should_succeed_json(
        ['ls', '--json', '--recursive', *b2_uri_args(bucket_name, f'{subfolder}')]
    )
    should_equal(
        [
            f'{subfolder}a',
            f'{subfolder}b/1',
            f'{subfolder}b/2',
            f'{subfolder}d',
        ],
        [f['fileName'] for f in list_of_files],
    )

    b2_tool.should_succeed(['file', 'unhide', f'b2://{persistent_bucket.virtual_bucket_name}/c'])

    list_of_files = b2_tool.should_succeed_json(
        ['ls', '--json', '--recursive', *b2_uri_args(bucket_name, f'{subfolder}')]
    )
    should_equal(
        [
            f'{subfolder}a',
            f'{subfolder}b/1',
            f'{subfolder}b/2',
            f'{subfolder}c',
            f'{subfolder}d',
        ],
        [f['fileName'] for f in list_of_files],
    )

    b2_tool.should_succeed(['file', 'hide', f'b2://{bucket_name}/{subfolder}c'])

    list_of_files = b2_tool.should_succeed_json(
        ['ls', '--json', '--recursive', *b2_uri_args(bucket_name, f'{subfolder}')]
    )
    should_equal(
        [
            f'{subfolder}a',
            f'{subfolder}b/1',
            f'{subfolder}b/2',
            f'{subfolder}d',
        ],
        [f['fileName'] for f in list_of_files],
    )

    list_of_files = b2_tool.should_succeed_json(
        ['ls', '--json', '--recursive', '--versions', *b2_uri_args(bucket_name, f'{subfolder}')]
    )
    should_equal(
        [
            f'{subfolder}a',
            f'{subfolder}a',
            f'{subfolder}b/1',
            f'{subfolder}b/2',
            f'{subfolder}c',
            f'{subfolder}c',
            f'{subfolder}d',
        ],
        [f['fileName'] for f in list_of_files],
    )
    should_equal(
        ['upload', 'upload', 'upload', 'upload', 'hide', 'upload', 'upload'],
        [f['action'] for f in list_of_files],
    )

    first_a_version = list_of_files[0]

    first_c_version = list_of_files[4]
    second_c_version = list_of_files[5]
    list_of_files = b2_tool.should_succeed_json(
        ['ls', '--json', '--recursive', '--versions', *b2_uri_args(bucket_name, f'{subfolder}c')]
    )
    if apiver_int >= 4:  # b2://bucketName/c should list all c versions on v4
        should_equal(
            [
                f'{subfolder}c',
                f'{subfolder}c',
            ],
            [f['fileName'] for f in list_of_files],
        )
    else:
        should_equal([], [f['fileName'] for f in list_of_files])

    b2_tool.should_succeed(
        [
            'file',
            'server-side-copy',
            f'b2id://{first_a_version["fileId"]}',
            f'b2://{bucket_name}/{subfolder}x',
        ]
    )

    b2_tool.should_succeed(
        ['ls', *b2_uri_args(bucket_name, f'{subfolder}')],
        f'^{subfolder}a{os.linesep}{subfolder}b/{os.linesep}{subfolder}d{os.linesep}',
    )
    # file_id, action, date, time, size(, replication), name

    b2_tool.should_succeed(
        ['ls', '--long', *b2_uri_args(bucket_name, f'{subfolder}')],
        '^4_z.* upload .* {1}  {2}a{0}.* - .* {2}b/{0}4_z.* upload .* {1}  {2}d{0}'.format(
            os.linesep, len(file_data), subfolder
        ),
    )
    b2_tool.should_succeed(
        ['ls', '--long', '--replication', *b2_uri_args(bucket_name, f'{subfolder}')],
        '^4_z.* upload .* {1}  -  {2}a{0}.* - .*  -  {2}b/{0}4_z.* upload .* {1}  -  {2}d{0}'.format(
            os.linesep, len(file_data), subfolder
        ),
    )

    b2_tool.should_succeed(
        ['ls', '--versions', *b2_uri_args(bucket_name, f'{subfolder}')],
        f'^{subfolder}a{os.linesep}{subfolder}a{os.linesep}{subfolder}b/{os.linesep}{subfolder}c{os.linesep}{subfolder}c{os.linesep}{subfolder}d{os.linesep}',
    )
    b2_tool.should_succeed(
        ['ls', *b2_uri_args(bucket_name, f'{subfolder}b')],
        f'^{subfolder}b/1{os.linesep}{subfolder}b/2{os.linesep}',
    )
    b2_tool.should_succeed(
        ['ls', *b2_uri_args(bucket_name, f'{subfolder}b/')],
        f'^{subfolder}b/1{os.linesep}{subfolder}b/2{os.linesep}',
    )

    file_info = b2_tool.should_succeed_json(
        ['file', 'info', f"b2id://{second_c_version['fileId']}"]
    )
    expected_info = {
        'color': 'blue',
        'foo': 'bar=baz',
        'src_last_modified_millis': file_mod_time_str,
    }
    should_equal(expected_info, file_info['fileInfo'])

    b2_tool.should_succeed(
        ['delete-file-version', f'{subfolder}c', first_c_version['fileId']],
        expected_stderr_pattern=re.compile(
            re.escape('WARNING: `delete-file-version` command is deprecated. Use `rm` instead.')
        ),
    )
    b2_tool.should_succeed(
        ['ls', *b2_uri_args(bucket_name, f'{subfolder}')],
        f'^{subfolder}a{os.linesep}{subfolder}b/{os.linesep}{subfolder}c{os.linesep}{subfolder}d{os.linesep}',
    )

    b2_tool.should_succeed(['file', 'url', f"b2id://{second_c_version['fileId']}"])

    b2_tool.should_succeed(
        ['file', 'url', f'b2://{persistent_bucket.virtual_bucket_name}/any-file-name'],
        '^https://.*/file/{}/{}\r?$'.format(
            persistent_bucket.virtual_bucket_name,
            'any-file-name',
        ),
    )  # \r? is for Windows, as $ doesn't match \r\n


@pytest.mark.apiver(from_ver=4)
def test_ls_b2id(b2_tool, uploaded_sample_file):
    b2_tool.should_succeed(
        ['ls', f"b2id://{uploaded_sample_file['fileId']}"],
        expected_pattern=f"^{uploaded_sample_file['fileName']}",
    )


@pytest.mark.apiver(from_ver=4)
def test_rm_b2id(b2_tool, persistent_bucket, uploaded_sample_file):
    # remove the file by id
    b2_tool.should_succeed(['rm', f"b2id://{uploaded_sample_file['fileId']}"])

    # check that the file is gone
    b2_tool.should_succeed(
        ['ls', f'b2://{persistent_bucket.bucket_name}/{persistent_bucket.subfolder}/'],
        expected_pattern='^$',
    )


def test_debug_logs(b2_tool, is_running_on_docker, tmp_path):
    to_be_removed_bucket_name = b2_tool.generate_bucket_name()
    b2_tool.should_succeed(
        [
            'bucket',
            'create',
            to_be_removed_bucket_name,
            'allPublic',
            *b2_tool.get_bucket_info_args(),
        ],
    )
    b2_tool.should_succeed(
        ['bucket', 'delete', to_be_removed_bucket_name],
    )
    b2_tool.should_fail(
        ['bucket', 'delete', to_be_removed_bucket_name],
        re.compile(r'^ERROR: Bucket with id=\w* not found[^$]*$'),
    )
    # Check logging settings
    if not is_running_on_docker:  # It's difficult to read the log in docker in CI
        b2_tool.should_fail(
            ['bucket', 'delete', to_be_removed_bucket_name, '--debug-logs'],
            re.compile(r'^ERROR: Bucket with id=\w* not found[^$]*$'),
        )
        stack_trace_in_log = (
            r'Traceback \(most recent call last\):.*Bucket with id=\w* not found[^$]*'
        )

        # the two regexes below depend on log message from urllib3, which is not perfect, but this test needs to
        # check global logging settings
        stderr_regex = re.compile(
            r'DEBUG:urllib3.connectionpool:.* "POST /b2api/v4/b2_delete_bucket HTTP'
            r'.*' + stack_trace_in_log,
            re.DOTALL,
        )
        log_file_regex = re.compile(
            r'urllib3.connectionpool\tDEBUG\t.* "POST /b2api/v4/b2_delete_bucket HTTP'
            r'.*' + stack_trace_in_log,
            re.DOTALL,
        )
        with open('b2_cli.log') as logfile:
            log = logfile.read()
            assert re.search(log_file_regex, log), log
        os.remove('b2_cli.log')

        b2_tool.should_fail(
            ['bucket', 'delete', to_be_removed_bucket_name, '--verbose'], stderr_regex
        )
        assert not os.path.exists('b2_cli.log')

        b2_tool.should_fail(
            ['bucket', 'delete', to_be_removed_bucket_name, '--verbose', '--debug-logs'],
            stderr_regex,
        )
        with open('b2_cli.log') as logfile:
            log = logfile.read()
            assert re.search(log_file_regex, log), log


def test_bucket(b2_tool, persistent_bucket):
    rule = """{
        "daysFromHidingToDeleting": 1,
        "daysFromUploadingToHiding": null,
        "fileNamePrefix": ""
    }"""
    output = b2_tool.should_succeed_json(
        [
            'bucket',
            'update',
            '--lifecycle-rule',
            rule,
            persistent_bucket.bucket_name,
            'allPublic',
            *b2_tool.get_bucket_info_args(),
        ],
    )

    ########## // doesn't happen on production, but messes up some tests \\ ##########
    for key in output['lifecycleRules'][0]:
        if key[8] == 'S' and len(key) == 47:
            del output['lifecycleRules'][0][key]
            break
    ########## \\ doesn't happen on production, but messes up some tests // ##########

    assert output['lifecycleRules'] == [
        {'daysFromHidingToDeleting': 1, 'daysFromUploadingToHiding': None, 'fileNamePrefix': ''}
    ]


class TestKeyRestrictions(IntegrationTestBase):
    def test_key_restrictions(self, b2_tool, bucket_name, sample_file, b2_uri_args):
        # A single file for rm to fail on.
        b2_tool.should_succeed(
            ['file', 'upload', '--no-progress', bucket_name, sample_file, 'test']
        )

        key_one_name = 'clt-testKey-01' + random_hex(6)
        created_key_stdout = b2_tool.should_succeed(
            [
                'key',
                'create',
                key_one_name,
                'listFiles,listBuckets,readFiles,writeKeys',
            ]
        )
        key_one_id, key_one = created_key_stdout.split()

        b2_tool.should_succeed(
            ['account', 'authorize', '--environment', b2_tool.realm, key_one_id, key_one],
        )

        b2_tool.should_succeed(
            ['bucket', 'get', bucket_name],
        )
        second_bucket_name = self.create_bucket().name
        b2_tool.should_succeed(
            ['bucket', 'get', second_bucket_name],
        )

        key_two_name = 'clt-testKey-02' + random_hex(6)
        created_key_two_stdout = b2_tool.should_succeed(
            [
                'key',
                'create',
                '--bucket',
                bucket_name,
                key_two_name,
                'listFiles,listBuckets,readFiles',
            ]
        )
        key_two_id, key_two = created_key_two_stdout.split()

        create_key_deprecated_pattern = re.compile(
            re.escape('WARNING: `create-key` command is deprecated. Use `key create` instead.')
        )
        key_three_name = 'clt-testKey-03' + random_hex(6)
        created_key_three_stdout = b2_tool.should_succeed(
            [
                'create-key',
                '--bucket',
                bucket_name,
                key_three_name,
                'listFiles,listBuckets,readFiles',
            ],
            expected_stderr_pattern=create_key_deprecated_pattern,
        )
        key_three_id, key_three = created_key_three_stdout.split()

        b2_tool.should_succeed(
            ['account', 'authorize', '--environment', b2_tool.realm, key_two_id, key_two],
        )
        b2_tool.should_succeed(
            ['bucket', 'get', bucket_name],
        )
        b2_tool.should_succeed(
            ['ls', *b2_uri_args(bucket_name)],
        )

        b2_tool.should_succeed(
            ['account', 'authorize', '--environment', b2_tool.realm, key_three_id, key_three],
        )

        # Capabilities can be listed in any order. While this regex doesn't confirm that all three are present,
        # in ensures that there are three in total.
        failed_bucket_err = (
            r'Deletion of file "test" \([^\)]+\) failed: unauthorized for '
            r'application key with capabilities '
            r"'(.*listFiles.*|.*listBuckets.*|.*readFiles.*){3}', "
            r"restricted to buckets \['%s'\] \(unauthorized\)" % bucket_name
        )
        b2_tool.should_fail(
            ['rm', '--recursive', '--no-progress', *b2_uri_args(bucket_name)], failed_bucket_err
        )

        failed_bucket_err = rf"ERROR: Application key is restricted to buckets: \['{bucket_name}'\]"
        b2_tool.should_fail(['bucket', 'get', second_bucket_name], failed_bucket_err)

        failed_list_files_err = (
            rf"ERROR: Application key is restricted to buckets: \['{bucket_name}'\]"
        )
        b2_tool.should_fail(['ls', *b2_uri_args(second_bucket_name)], failed_list_files_err)

        failed_list_files_err = (
            rf"ERROR: Application key is restricted to buckets: \['{bucket_name}'\]"
        )
        b2_tool.should_fail(['rm', *b2_uri_args(second_bucket_name)], failed_list_files_err)

        # reauthorize with more capabilities for clean up
        b2_tool.should_succeed(
            [
                'account',
                'authorize',
                '--environment',
                b2_tool.realm,
                b2_tool.account_id,
                b2_tool.application_key,
            ]
        )
        b2_tool.should_succeed(['key', 'delete', key_one_id])
        b2_tool.should_succeed(['key', 'delete', key_two_id])

        delete_key_deprecated_pattern = re.compile(
            re.escape('WARNING: `delete-key` command is deprecated. Use `key delete` instead.')
        )
        b2_tool.should_succeed(
            ['delete-key', key_three_id],
            expected_stderr_pattern=delete_key_deprecated_pattern,
        )

    def test_multi_bucket_key_restrictions(self, b2_tool):
        bucket_a = self.create_bucket()
        bucket_b = self.create_bucket()
        bucket_c = self.create_bucket()

        key_name = 'clt-testKey-01' + random_hex(6)

        created_key_stdout = b2_tool.should_succeed(
            [
                'key',
                'create',
                '--bucket',
                bucket_a.name,
                '--bucket',
                bucket_b.name,
                key_name,
                'listFiles,listBuckets,readFiles',
            ]
        )

        mb_key_id, mb_key = created_key_stdout.split()

        b2_tool.should_succeed(
            ['account', 'authorize', '--environment', b2_tool.realm, mb_key_id, mb_key],
        )

        b2_tool.should_succeed(
            ['bucket', 'get', bucket_a.name],
        )
        b2_tool.should_succeed(
            ['bucket', 'get', bucket_b.name],
        )

        failed_bucket_err = rf"ERROR: Application key is restricted to buckets: \['{bucket_a.name}', '{bucket_b.name}'|'{bucket_b.name}', '{bucket_a.name}'\]"

        b2_tool.should_fail(['bucket', 'get', bucket_c.name], failed_bucket_err)

        # reauthorize with more capabilities for clean up
        b2_tool.should_succeed(
            [
                'account',
                'authorize',
                '--environment',
                b2_tool.realm,
                b2_tool.account_id,
                b2_tool.application_key,
            ]
        )

        b2_tool.should_succeed(['key', 'delete', mb_key_id])


def test_delete_bucket(b2_tool, bucket_name):
    b2_tool.should_succeed(['bucket', 'delete', bucket_name])
    b2_tool.should_fail(
        ['bucket', 'delete', bucket_name], re.compile(r'^ERROR: Bucket with id=\w* not found[^$]*$')
    )


def test_rapid_bucket_operations(b2_tool):
    new_bucket_name = b2_tool.generate_bucket_name()
    bucket_info_args = b2_tool.get_bucket_info_args()
    # apparently server behaves erratically when we delete a bucket and recreate it right away
    b2_tool.should_succeed(['bucket', 'create', new_bucket_name, 'allPrivate', *bucket_info_args])
    b2_tool.should_succeed(['bucket', 'update', new_bucket_name, 'allPublic'])
    b2_tool.should_succeed(['bucket', 'delete', new_bucket_name])


def test_account(b2_tool, cli_version, apiver_int, monkeypatch):
    with monkeypatch.context() as mp:
        account_info_file_path = os.path.join(mkdtemp(), 'b2_account_info')
        mp.setenv(B2_ACCOUNT_INFO_ENV_VAR, account_info_file_path)

        b2_tool.should_succeed(['account', 'clear'])
        bad_application_key = random_hex(len(b2_tool.application_key))
        b2_tool.should_fail(
            ['account', 'authorize', b2_tool.account_id, bad_application_key], r'unauthorized'
        )  # this call doesn't use --environment on purpose, so that we check that it is non-mandatory
        b2_tool.should_succeed(
            [
                'account',
                'authorize',
                '--environment',
                b2_tool.realm,
                b2_tool.account_id,
                b2_tool.application_key,
            ]
        )

    # Testing (B2_APPLICATION_KEY, B2_APPLICATION_KEY_ID) for commands other than `account authorize`
    with monkeypatch.context() as mp:
        account_info_file_path = os.path.join(mkdtemp(), 'b2_account_info')
        mp.setenv(B2_ACCOUNT_INFO_ENV_VAR, account_info_file_path)

        # first, let's make sure "bucket create" doesn't work without auth data - i.e. that the sqlite file has been
        # successfully removed
        bucket_name = b2_tool.generate_bucket_name()
        b2_tool.should_fail(
            ['bucket', 'create', bucket_name, 'allPrivate'],
            r'ERROR: Missing account data: \'NoneType\' object is not subscriptable (\(key 0\) )? '
            rf'Use: \'{cli_version}(\.(exe|EXE))? account authorize\' or provide auth data with \'B2_APPLICATION_KEY_ID\' and '
            r'\'B2_APPLICATION_KEY\' environment variables',
        )

    with monkeypatch.context() as mp:
        account_info_file_path = os.path.join(mkdtemp(), 'b2_account_info')
        mp.setenv(B2_ACCOUNT_INFO_ENV_VAR, account_info_file_path)

        # then, let's see that auth data from env vars works
        os.environ['B2_APPLICATION_KEY'] = os.environ['B2_TEST_APPLICATION_KEY']
        os.environ['B2_APPLICATION_KEY_ID'] = os.environ['B2_TEST_APPLICATION_KEY_ID']
        os.environ['B2_ENVIRONMENT'] = b2_tool.realm

        bucket_name = b2_tool.generate_bucket_name()
        b2_tool.should_succeed(
            ['bucket', 'create', bucket_name, 'allPrivate', *b2_tool.get_bucket_info_args()]
        )
        b2_tool.should_succeed(['bucket', 'delete', bucket_name])

        if apiver_int >= 4:
            assert not os.path.exists(
                account_info_file_path
            ), "sqlite file was created while it shouldn't"
        else:
            assert os.path.exists(account_info_file_path), 'sqlite file was not created'
            account_info = SqliteAccountInfo(account_info_file_path)
            assert account_info.get_application_key_id() == os.environ['B2_TEST_APPLICATION_KEY_ID']
            assert account_info.get_application_key() == os.environ['B2_TEST_APPLICATION_KEY']

        os.environ.pop('B2_APPLICATION_KEY')
        os.environ.pop('B2_APPLICATION_KEY_ID')

        # last, let's see that providing only one of the env vars results in a failure
        os.environ['B2_APPLICATION_KEY'] = os.environ['B2_TEST_APPLICATION_KEY']
        b2_tool.should_fail(
            ['bucket', 'create', bucket_name, 'allPrivate'],
            r'Please provide both "B2_APPLICATION_KEY" and "B2_APPLICATION_KEY_ID" environment variables or none of them',
        )
        os.environ.pop('B2_APPLICATION_KEY')

        os.environ['B2_APPLICATION_KEY_ID'] = os.environ['B2_TEST_APPLICATION_KEY_ID']
        b2_tool.should_fail(
            ['bucket', 'create', bucket_name, 'allPrivate'],
            r'Please provide both "B2_APPLICATION_KEY" and "B2_APPLICATION_KEY_ID" environment variables or none of them',
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
        EncryptionMode.NONE.value,
        EncryptionMode.SSE_B2.value,
        EncryptionMode.SSE_C.value,
    )
    algorithm = sse_dict.get('algorithm')
    if algorithm is not None:
        encryption += ':' + algorithm
    if sse_dict['mode'] == 'SSE-C':
        sse_c_key_id = file_info.get(SSE_C_KEY_ID_FILE_INFO_KEY_NAME)
        encryption += f'?{SSE_C_KEY_ID_FILE_INFO_KEY_NAME}={sse_c_key_id}'

    return encryption


@pytest.mark.parametrize(
    'dir_, encryption',
    [('sync', None), ('sync', SSE_B2_AES), ('sync', SSE_C_AES), ('', None)],
)
def test_sync_up(tmp_path, b2_tool, persistent_bucket, apiver_int, dir_, encryption):
    # persistent_bucket.subfolder = persistent_bucket.subfolder + random_hex(6)

    sync_point_parts = [persistent_bucket.bucket_name, persistent_bucket.subfolder]
    if dir_:
        sync_point_parts.append(dir_)
        prefix = f'{persistent_bucket.subfolder}/{dir_}/'
    else:
        prefix = persistent_bucket.subfolder + '/'
    b2_sync_point = 'b2:' + '/'.join(sync_point_parts)

    file_versions = b2_tool.list_file_versions(
        persistent_bucket.bucket_name, persistent_bucket.subfolder
    )
    should_equal([], file_version_summary(file_versions))

    write_file(tmp_path / 'a', b'hello')
    write_file(tmp_path / 'b', b'hello')
    write_file(tmp_path / 'c', b'hello')

    # simulate action (nothing should be uploaded)
    b2_tool.should_succeed(['sync', '--no-progress', '--dry-run', tmp_path, b2_sync_point])
    file_versions = b2_tool.list_file_versions(
        persistent_bucket.bucket_name, persistent_bucket.subfolder
    )
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
    # create symbolic links. Add your user to it (or a group that the user is in).
    #
    # Finally in order to apply the new policy, run `cmd` and execute
    # ``gpupdate /force``.
    #
    # Again, if it still doesn't work, consider just running the shell you are
    # launching ``nox`` as admin.

    os.symlink('broken', tmp_path / 'd')  # OSError: [WinError 1314] ? See the comment above

    additional_env = None

    # now upload
    if encryption is None:
        command = ['sync', '--no-progress', tmp_path, b2_sync_point]
        expected_encryption = SSE_NONE
        expected_encryption_str = encryption_summary(expected_encryption.as_dict(), {})
    elif encryption == SSE_B2_AES:
        command = [
            'sync',
            '--no-progress',
            '--destination-server-side-encryption',
            'SSE-B2',
            tmp_path,
            b2_sync_point,
        ]
        expected_encryption = encryption
        expected_encryption_str = encryption_summary(expected_encryption.as_dict(), {})
    elif encryption == SSE_C_AES:
        command = [
            'sync',
            '--no-progress',
            '--destination-server-side-encryption',
            'SSE-C',
            tmp_path,
            b2_sync_point,
        ]
        expected_encryption = encryption
        additional_env = {
            'B2_DESTINATION_SSE_C_KEY_B64': base64.b64encode(SSE_C_AES.key.secret).decode(),
            'B2_DESTINATION_SSE_C_KEY_ID': SSE_C_AES.key.key_id,
        }
        expected_encryption_str = encryption_summary(
            expected_encryption.as_dict(), {SSE_C_KEY_ID_FILE_INFO_KEY_NAME: SSE_C_AES.key.key_id}
        )
    else:
        raise NotImplementedError('unsupported encryption mode: %s' % encryption)

    status, stdout, stderr = b2_tool.execute(command, additional_env=additional_env)
    assert re.search(r'd[\'"]? could not be accessed', stdout)
    assert status == (1 if apiver_int >= 4 else 0)
    file_versions = b2_tool.list_file_versions(
        persistent_bucket.bucket_name, persistent_bucket.subfolder
    )

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
            expected_pattern='ValueError: Using SSE-C requires providing an encryption key via '
            'B2_DESTINATION_SSE_C_KEY_B64 env var',
        )
    if encryption is not None:
        return  # that's enough, we've checked that encryption works, no need to repeat the whole sync suite

    c_id = find_file_id(file_versions, prefix + 'c')
    file_info = b2_tool.should_succeed_json(['file', 'info', f'b2id://{c_id}'])['fileInfo']
    should_equal(file_mod_time_millis(tmp_path / 'c'), int(file_info['src_last_modified_millis']))

    os.unlink(tmp_path / 'b')
    write_file(tmp_path / 'c', b'hello world')

    status, stdout, stderr = b2_tool.execute(
        ['sync', '--no-progress', '--keep-days', '10', tmp_path, b2_sync_point]
    )
    assert re.search(r'd[\'"]? could not be accessed', stdout)
    assert status == (1 if apiver_int >= 4 else 0)
    file_versions = b2_tool.list_file_versions(
        persistent_bucket.bucket_name, persistent_bucket.subfolder
    )
    should_equal(
        [
            '+ ' + prefix + 'a',
            '- ' + prefix + 'b',
            '+ ' + prefix + 'b',
            '+ ' + prefix + 'c',
            '+ ' + prefix + 'c',
        ],
        file_version_summary(file_versions),
    )

    os.unlink(tmp_path / 'a')
    os.unlink(tmp_path / 'd')  # remove broken symlink to get status 0 on >=b2v4

    b2_tool.should_succeed(['sync', '--no-progress', '--delete', tmp_path, b2_sync_point])
    file_versions = b2_tool.list_file_versions(
        persistent_bucket.bucket_name, persistent_bucket.subfolder
    )
    should_equal(
        [
            '+ ' + prefix + 'c',
        ],
        file_version_summary(file_versions),
    )

    # test --compare-threshold with file size
    write_file(tmp_path / 'c', b'hello world!')

    # should not upload new version of c
    b2_tool.should_succeed(
        [
            'sync',
            '--no-progress',
            '--keep-days',
            '10',
            '--compare-versions',
            'size',
            '--compare-threshold',
            '1',
            tmp_path,
            b2_sync_point,
        ]
    )
    file_versions = b2_tool.list_file_versions(
        persistent_bucket.bucket_name, persistent_bucket.subfolder
    )
    should_equal(
        [
            '+ ' + prefix + 'c',
        ],
        file_version_summary(file_versions),
    )

    # should upload new version of c
    b2_tool.should_succeed(
        [
            'sync',
            '--no-progress',
            '--keep-days',
            '10',
            '--compare-versions',
            'size',
            tmp_path,
            b2_sync_point,
        ]
    )
    file_versions = b2_tool.list_file_versions(
        persistent_bucket.bucket_name, persistent_bucket.subfolder
    )
    should_equal(
        [
            '+ ' + prefix + 'c',
            '+ ' + prefix + 'c',
        ],
        file_version_summary(file_versions),
    )

    set_file_mod_time_millis(tmp_path / 'c', file_mod_time_millis(tmp_path / 'c') + 2000)

    # test --compare-threshold with modTime
    # should not upload new version of c
    b2_tool.should_succeed(
        [
            'sync',
            '--no-progress',
            '--keep-days',
            '10',
            '--compare-versions',
            'modTime',
            '--compare-threshold',
            '2000',
            tmp_path,
            b2_sync_point,
        ]
    )
    file_versions = b2_tool.list_file_versions(
        persistent_bucket.bucket_name, persistent_bucket.subfolder
    )
    should_equal(
        [
            '+ ' + prefix + 'c',
            '+ ' + prefix + 'c',
        ],
        file_version_summary(file_versions),
    )

    # should upload new version of c
    b2_tool.should_succeed(
        [
            'sync',
            '--no-progress',
            '--keep-days',
            '10',
            '--compare-versions',
            'modTime',
            tmp_path,
            b2_sync_point,
        ]
    )
    file_versions = b2_tool.list_file_versions(
        persistent_bucket.bucket_name, persistent_bucket.subfolder
    )
    should_equal(
        [
            '+ ' + prefix + 'c',
            '+ ' + prefix + 'c',
            '+ ' + prefix + 'c',
        ],
        file_version_summary(file_versions),
    )

    # create one more file
    write_file(tmp_path / 'linktarget', b'hello')
    mod_time = str((file_mod_time_millis(tmp_path / 'linktarget') - 10) / 1000)

    # exclude last created file because of mtime
    b2_tool.should_succeed(
        ['sync', '--no-progress', '--exclude-if-modified-after', mod_time, tmp_path, b2_sync_point]
    )
    file_versions = b2_tool.list_file_versions(
        persistent_bucket.bucket_name, persistent_bucket.subfolder
    )
    should_equal(
        [
            '+ ' + prefix + 'c',
            '+ ' + prefix + 'c',
            '+ ' + prefix + 'c',
        ],
        file_version_summary(file_versions),
    )

    # confirm symlink is skipped
    os.symlink('linktarget', tmp_path / 'alink')

    b2_tool.should_succeed(
        ['sync', '--no-progress', '--exclude-all-symlinks', tmp_path, b2_sync_point],
    )
    file_versions = b2_tool.list_file_versions(
        persistent_bucket.bucket_name, persistent_bucket.subfolder
    )
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
    b2_tool.should_succeed(['sync', '--no-progress', tmp_path, b2_sync_point])
    file_versions = b2_tool.list_file_versions(
        persistent_bucket.bucket_name, persistent_bucket.subfolder
    )
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
    b2_sync_point = f'b2:{bucket_name}'
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
        upload_encryption_args = ['--destination-server-side-encryption', 'SSE-C']
        upload_additional_env = {
            'B2_DESTINATION_SSE_C_KEY_B64': base64.b64encode(encryption.key.secret).decode(),
            'B2_DESTINATION_SSE_C_KEY_ID': encryption.key.key_id,
        }
        sync_encryption_args = ['--source-server-side-encryption', 'SSE-C']
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
            ['file', 'upload', '--no-progress', bucket_name, sample_file, b2_file_prefix + 'a']
            + upload_encryption_args,
            additional_env=upload_additional_env,
        )
        b2_tool.should_succeed(
            ['file', 'upload', '--no-progress', bucket_name, sample_file, b2_file_prefix + 'b']
            + upload_encryption_args,
            additional_env=upload_additional_env,
        )

        # Sync all the files
        b2_tool.should_succeed(
            ['sync', b2_sync_point, local_path] + sync_encryption_args,
            additional_env=sync_additional_env,
        )
        should_equal(['a', 'b'], sorted(os.listdir(local_path)))

        # Put another file in B2
        b2_tool.should_succeed(
            ['file', 'upload', '--no-progress', bucket_name, sample_file, b2_file_prefix + 'c']
            + upload_encryption_args,
            additional_env=upload_additional_env,
        )

        # Sync the files with one file being excluded because of mtime
        mod_time = str((file_mod_time_millis(sample_file) - 10) / 1000)
        b2_tool.should_succeed(
            [
                'sync',
                '--no-progress',
                '--exclude-if-modified-after',
                mod_time,
                b2_sync_point,
                local_path,
            ]
            + sync_encryption_args,
            additional_env=sync_additional_env,
        )
        should_equal(['a', 'b'], sorted(os.listdir(local_path)))

        # Put another file in B2 with a custom upload timestamp
        b2_tool.should_succeed(
            [
                'file',
                'upload',
                '--no-progress',
                '--custom-upload-timestamp',
                '1367900664152',
                bucket_name,
                sample_file,
                b2_file_prefix + 'd',
            ]
            + upload_encryption_args,
            additional_env=upload_additional_env,
        )

        # Sync the files with one file being excluded because of upload timestamp
        b2_tool.should_succeed(
            [
                'sync',
                '--no-progress',
                '--exclude-if-uploaded-after',
                '1367900664142',
                b2_sync_point,
                local_path,
            ]
            + sync_encryption_args,
            additional_env=sync_additional_env,
        )
        should_equal(['a', 'b'], sorted(os.listdir(local_path)))

        # Sync all the files
        b2_tool.should_succeed(
            ['sync', '--no-progress', b2_sync_point, local_path] + sync_encryption_args,
            additional_env=sync_additional_env,
        )
        should_equal(['a', 'b', 'c', 'd'], sorted(os.listdir(local_path)))

    if encryption and encryption.mode == EncryptionMode.SSE_C:
        with TempDir() as new_local_path:
            b2_tool.should_fail(
                ['sync', '--no-progress', b2_sync_point, new_local_path] + sync_encryption_args,
                expected_pattern='ValueError: Using SSE-C requires providing an encryption key via '
                'B2_SOURCE_SSE_C_KEY_B64 env var',
            )
            b2_tool.should_fail(
                ['sync', '--no-progress', b2_sync_point, new_local_path],
                expected_pattern='b2sdk._internal.exception.BadRequest: The object was stored using a form of Server Side '
                'Encryption. The correct parameters must be provided to retrieve the object. '
                r'\(bad_request\)',
            )


class TestSyncCopy(IntegrationTestBase):
    def test_sync_copy(self, b2_tool, bucket_name, sample_file):
        self.prepare_and_run_sync_copy_tests(b2_tool, bucket_name, 'sync', sample_file=sample_file)

    def test_sync_copy_no_prefix_default_encryption(self, b2_tool, bucket_name, sample_file):
        self.prepare_and_run_sync_copy_tests(
            b2_tool,
            bucket_name,
            '',
            sample_file=sample_file,
            destination_encryption=None,
            expected_encryption=SSE_NONE,
        )

    def test_sync_copy_no_prefix_no_encryption(self, b2_tool, bucket_name, sample_file):
        self.prepare_and_run_sync_copy_tests(
            b2_tool,
            bucket_name,
            '',
            sample_file=sample_file,
            destination_encryption=SSE_NONE,
            expected_encryption=SSE_NONE,
        )

    def test_sync_copy_no_prefix_sse_b2(self, b2_tool, bucket_name, sample_file):
        self.prepare_and_run_sync_copy_tests(
            b2_tool,
            bucket_name,
            '',
            sample_file=sample_file,
            destination_encryption=SSE_B2_AES,
            expected_encryption=SSE_B2_AES,
        )

    def test_sync_copy_no_prefix_sse_c(self, b2_tool, bucket_name, sample_file):
        self.prepare_and_run_sync_copy_tests(
            b2_tool,
            bucket_name,
            '',
            sample_file=sample_file,
            destination_encryption=SSE_C_AES,
            expected_encryption=SSE_C_AES,
            source_encryption=SSE_C_AES_2,
        )

    def test_sync_copy_sse_c_single_bucket(self, b2_tool, bucket_name, sample_file):
        self.run_sync_copy_with_basic_checks(
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
        self,
        b2_tool,
        bucket_name,
        folder_in_bucket,
        sample_file,
        destination_encryption=None,
        expected_encryption=SSE_NONE,
        source_encryption=None,
    ):
        b2_sync_point = f'b2:{bucket_name}'
        if folder_in_bucket:
            b2_sync_point += '/' + folder_in_bucket
            b2_file_prefix = folder_in_bucket + '/'
        else:
            b2_file_prefix = ''

        other_bucket_name = self.create_bucket().name

        other_b2_sync_point = f'b2:{other_bucket_name}'
        if folder_in_bucket:
            other_b2_sync_point += '/' + folder_in_bucket

        self.run_sync_copy_with_basic_checks(
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
            encryption_file_info = {
                SSE_C_KEY_ID_FILE_INFO_KEY_NAME: destination_encryption.key.key_id
            }
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
        self,
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
            EncryptionMode.NONE,
            EncryptionMode.SSE_B2,
        ):
            b2_tool.should_succeed(
                [
                    'file',
                    'upload',
                    '--no-progress',
                    '--destination-server-side-encryption',
                    'SSE-B2',
                    bucket_name,
                    sample_file,
                    b2_file_prefix + 'a',
                ]
            )
            b2_tool.should_succeed(
                ['file', 'upload', '--no-progress', bucket_name, sample_file, b2_file_prefix + 'b']
            )
        elif source_encryption.mode == EncryptionMode.SSE_C:
            for suffix in ['a', 'b']:
                b2_tool.should_succeed(
                    [
                        'file',
                        'upload',
                        '--no-progress',
                        '--destination-server-side-encryption',
                        'SSE-C',
                        bucket_name,
                        sample_file,
                        b2_file_prefix + suffix,
                    ],
                    additional_env={
                        'B2_DESTINATION_SSE_C_KEY_B64': base64.b64encode(
                            source_encryption.key.secret
                        ).decode(),
                        'B2_DESTINATION_SSE_C_KEY_ID': source_encryption.key.key_id,
                    },
                )
        else:
            raise NotImplementedError(source_encryption)

        # Sync all the files
        if destination_encryption is None or destination_encryption == SSE_NONE:
            b2_tool.should_succeed(['sync', '--no-progress', b2_sync_point, other_b2_sync_point])
        elif destination_encryption == SSE_B2_AES:
            b2_tool.should_succeed(
                [
                    'sync',
                    '--no-progress',
                    '--destination-server-side-encryption',
                    destination_encryption.mode.value,
                    b2_sync_point,
                    other_b2_sync_point,
                ]
            )
        elif destination_encryption.mode == EncryptionMode.SSE_C:
            b2_tool.should_fail(
                [
                    'sync',
                    '--no-progress',
                    '--destination-server-side-encryption',
                    destination_encryption.mode.value,
                    b2_sync_point,
                    other_b2_sync_point,
                ],
                additional_env={
                    'B2_DESTINATION_SSE_C_KEY_B64': base64.b64encode(
                        destination_encryption.key.secret
                    ).decode(),
                    'B2_DESTINATION_SSE_C_KEY_ID': destination_encryption.key.key_id,
                },
                expected_pattern='b2sdk._internal.exception.BadRequest: The object was stored using a form of Server Side '
                'Encryption. The correct parameters must be provided to retrieve the object. '
                r'\(bad_request\)',
            )
            b2_tool.should_succeed(
                [
                    'sync',
                    '--no-progress',
                    '--destination-server-side-encryption',
                    destination_encryption.mode.value,
                    '--source-server-side-encryption',
                    source_encryption.mode.value,
                    b2_sync_point,
                    other_b2_sync_point,
                ],
                additional_env={
                    'B2_DESTINATION_SSE_C_KEY_B64': base64.b64encode(
                        destination_encryption.key.secret
                    ).decode(),
                    'B2_DESTINATION_SSE_C_KEY_ID': destination_encryption.key.key_id,
                    'B2_SOURCE_SSE_C_KEY_B64': base64.b64encode(
                        source_encryption.key.secret
                    ).decode(),
                    'B2_SOURCE_SSE_C_KEY_ID': source_encryption.key.key_id,
                },
            )

        else:
            raise NotImplementedError(destination_encryption)

    def test_sync_long_path(self, tmp_path, b2_tool, persistent_bucket):
        """
        test sync with very long path (overcome windows 260 character limit)
        """
        b2_sync_point = f'b2://{persistent_bucket.virtual_bucket_name}'

        long_path = '/'.join(
            (
                'extremely_long_path_which_exceeds_windows_unfortunate_260_character_path_limit',
                'and_needs_special_prefixes_containing_backslashes_added_to_overcome_this_limitation',
                'when_doing_so_beware_leaning_toothpick_syndrome_as_it_can_cause_frustration',
                'see_also_xkcd_1638',
            )
        )

        local_long_path = (tmp_path / long_path).resolve()
        fixed_local_long_path = Path(fix_windows_path_limit(str(local_long_path)))
        os.makedirs(fixed_local_long_path.parent)
        write_file(fixed_local_long_path, b'asdf')

        b2_tool.should_succeed(['sync', '--no-progress', '--delete', str(tmp_path), b2_sync_point])
        file_versions = b2_tool.list_file_versions(
            persistent_bucket.bucket_name, persistent_bucket.subfolder
        )
        should_equal(
            [f'+ {persistent_bucket.subfolder}/{long_path}'], file_version_summary(file_versions)
        )


def test_default_sse_b2__update_bucket(b2_tool, bucket_name, schedule_bucket_cleanup):
    # Set default encryption via `bucket update`
    bucket_info = b2_tool.should_succeed_json(['bucket', 'get', bucket_name])
    bucket_default_sse = {'mode': 'none'}
    should_equal(bucket_default_sse, bucket_info['defaultServerSideEncryption'])

    bucket_info = b2_tool.should_succeed_json(
        ['bucket', 'update', '--default-server-side-encryption=SSE-B2', bucket_name]
    )
    bucket_default_sse = {
        'algorithm': 'AES256',
        'mode': 'SSE-B2',
    }
    should_equal(bucket_default_sse, bucket_info['defaultServerSideEncryption'])

    bucket_info = b2_tool.should_succeed_json(['bucket', 'get', bucket_name])
    bucket_default_sse = {
        'algorithm': 'AES256',
        'mode': 'SSE-B2',
    }
    should_equal(bucket_default_sse, bucket_info['defaultServerSideEncryption'])


def test_default_sse_b2__create_bucket(b2_tool, schedule_bucket_cleanup):
    # Set default encryption via `bucket create`
    second_bucket_name = b2_tool.generate_bucket_name()
    schedule_bucket_cleanup(second_bucket_name)
    b2_tool.should_succeed(
        [
            'bucket',
            'create',
            '--default-server-side-encryption=SSE-B2',
            second_bucket_name,
            'allPublic',
            *b2_tool.get_bucket_info_args(),
        ]
    )
    second_bucket_info = b2_tool.should_succeed_json(['bucket', 'get', second_bucket_name])
    second_bucket_default_sse = {
        'algorithm': 'AES256',
        'mode': 'SSE-B2',
    }
    should_equal(second_bucket_default_sse, second_bucket_info['defaultServerSideEncryption'])


def test_sse_b2(b2_tool, persistent_bucket, sample_file, tmp_path, b2_uri_args):
    bucket_name = persistent_bucket.bucket_name
    subfolder = persistent_bucket.subfolder
    b2_tool.should_succeed(
        [
            'file',
            'upload',
            '--destination-server-side-encryption=SSE-B2',
            '--quiet',
            bucket_name,
            sample_file,
            f'{subfolder}/encrypted',
        ]
    )
    b2_tool.should_succeed(
        ['file', 'upload', '--quiet', bucket_name, sample_file, f'{subfolder}/not_encrypted']
    )

    b2_tool.should_succeed(
        [
            'file',
            'download',
            '--quiet',
            f'b2://{bucket_name}/{subfolder}/encrypted',
            tmp_path / 'encrypted',
        ]
    )
    b2_tool.should_succeed(
        [
            'file',
            'download',
            '--quiet',
            f'b2://{bucket_name}/{subfolder}/not_encrypted',
            tmp_path / 'not_encrypted',
        ]
    )

    list_of_files = b2_tool.should_succeed_json(
        ['ls', '--json', '--recursive', *b2_uri_args(bucket_name, subfolder)]
    )
    should_equal(
        [{'algorithm': 'AES256', 'mode': 'SSE-B2'}, {'mode': 'none'}],
        [f['serverSideEncryption'] for f in list_of_files],
    )

    encrypted_version = list_of_files[0]
    file_info = b2_tool.should_succeed_json(
        ['file', 'info', f"b2id://{encrypted_version['fileId']}"]
    )
    should_equal({'algorithm': 'AES256', 'mode': 'SSE-B2'}, file_info['serverSideEncryption'])
    not_encrypted_version = list_of_files[1]
    file_info = b2_tool.should_succeed_json(
        ['file', 'info', f"b2id://{not_encrypted_version['fileId']}"]
    )
    should_equal({'mode': 'none'}, file_info['serverSideEncryption'])

    b2_tool.should_succeed(
        [
            'file',
            'server-side-copy',
            '--destination-server-side-encryption=SSE-B2',
            f"b2id://{encrypted_version['fileId']}",
            f'b2://{bucket_name}/{subfolder}/copied_encrypted',
        ]
    )
    b2_tool.should_succeed(
        [
            'file',
            'server-side-copy',
            f"b2id://{not_encrypted_version['fileId']}",
            f'b2://{bucket_name}/{subfolder}/copied_not_encrypted',
        ]
    )

    list_of_files = b2_tool.should_succeed_json(
        ['ls', '--json', '--recursive', *b2_uri_args(bucket_name, subfolder)]
    )
    should_equal(
        [{'algorithm': 'AES256', 'mode': 'SSE-B2'}, {'mode': 'none'}] * 2,
        [f['serverSideEncryption'] for f in list_of_files],
    )

    copied_encrypted_version = list_of_files[2]
    file_info = b2_tool.should_succeed_json(
        ['file', 'info', f"b2id://{copied_encrypted_version['fileId']}"]
    )
    should_equal({'algorithm': 'AES256', 'mode': 'SSE-B2'}, file_info['serverSideEncryption'])

    copied_not_encrypted_version = list_of_files[3]
    file_info = b2_tool.should_succeed_json(
        ['file', 'info', f"b2id://{copied_not_encrypted_version['fileId']}"]
    )
    should_equal({'mode': 'none'}, file_info['serverSideEncryption'])


def test_sse_c(
    b2_tool, persistent_bucket, is_running_on_docker, sample_file, tmp_path, b2_uri_args
):
    bucket_name = persistent_bucket.bucket_name
    subfolder = persistent_bucket.subfolder
    sse_c_key_id = 'user-generated-key-id \nąóźćż\nœøΩ≈ç\nßäöü'
    if is_running_on_docker:
        # TODO: fix this once we figure out how to pass env vars with \n in them to docker, docker-compose should work
        sse_c_key_id = sse_c_key_id.replace('\n', '')

    secret = os.urandom(32)

    b2_tool.should_fail(
        [
            'file',
            'upload',
            '--no-progress',
            '--quiet',
            '--destination-server-side-encryption',
            'SSE-C',
            bucket_name,
            sample_file,
            'gonna-fail-anyway',
        ],
        'Using SSE-C requires providing an encryption key via B2_DESTINATION_SSE_C_KEY_B64 env var',
    )
    file_version_info = b2_tool.should_succeed_json(
        [
            'file',
            'upload',
            '--no-progress',
            '--quiet',
            '--destination-server-side-encryption',
            'SSE-C',
            bucket_name,
            sample_file,
            f'{subfolder}/uploaded_encrypted',
        ],
        additional_env={
            'B2_DESTINATION_SSE_C_KEY_B64': base64.b64encode(secret).decode(),
            'B2_DESTINATION_SSE_C_KEY_ID': sse_c_key_id,
        },
    )
    should_equal(
        {
            'algorithm': 'AES256',
            'customerKey': '******',
            'customerKeyMd5': '******',
            'mode': 'SSE-C',
        },
        file_version_info['serverSideEncryption'],
    )
    should_equal(sse_c_key_id, file_version_info['fileInfo'][SSE_C_KEY_ID_FILE_INFO_KEY_NAME])

    b2_tool.should_fail(
        [
            'file',
            'download',
            '--quiet',
            f'b2://{bucket_name}/{subfolder}/uploaded_encrypted',
            'gonna_fail_anyway',
        ],
        expected_pattern='ERROR: The object was stored using a form of Server Side Encryption. The '
        r'correct parameters must be provided to retrieve the object. \(bad_request\)',
    )
    b2_tool.should_fail(
        [
            'file',
            'download',
            '--quiet',
            '--source-server-side-encryption',
            'SSE-C',
            f'b2://{bucket_name}/{subfolder}/uploaded_encrypted',
            'gonna_fail_anyway',
        ],
        expected_pattern='ValueError: Using SSE-C requires providing an encryption key via '
        'B2_SOURCE_SSE_C_KEY_B64 env var',
    )
    b2_tool.should_fail(
        [
            'file',
            'download',
            '--quiet',
            '--source-server-side-encryption',
            'SSE-C',
            f'b2://{bucket_name}/{subfolder}/uploaded_encrypted',
            'gonna_fail_anyway',
        ],
        expected_pattern='ERROR: Wrong or no SSE-C key provided when reading a file.',
        additional_env={'B2_SOURCE_SSE_C_KEY_B64': base64.b64encode(os.urandom(32)).decode()},
    )
    with contextlib.nullcontext(tmp_path) as dir_path:
        b2_tool.should_succeed(
            [
                'file',
                'download',
                '--no-progress',
                '--quiet',
                '--source-server-side-encryption',
                'SSE-C',
                f'b2://{bucket_name}/{subfolder}/uploaded_encrypted',
                dir_path / 'a',
            ],
            additional_env={'B2_SOURCE_SSE_C_KEY_B64': base64.b64encode(secret).decode()},
        )
        assert read_file(dir_path / 'a') == read_file(sample_file)
        b2_tool.should_succeed(
            [
                'file',
                'download',
                '--no-progress',
                '--quiet',
                '--source-server-side-encryption',
                'SSE-C',
                f"b2id://{file_version_info['fileId']}",
                dir_path / 'b',
            ],
            additional_env={'B2_SOURCE_SSE_C_KEY_B64': base64.b64encode(secret).decode()},
        )
        assert read_file(dir_path / 'b') == read_file(sample_file)

    b2_tool.should_fail(
        [
            'file',
            'server-side-copy',
            f'b2id://{file_version_info["fileId"]}',
            f'b2://{bucket_name}/gonna-fail-anyway',
        ],
        expected_pattern='ERROR: The object was stored using a form of Server Side Encryption. The correct '
        r'parameters must be provided to retrieve the object. \(bad_request\)',
    )
    b2_tool.should_fail(
        [
            'file',
            'server-side-copy',
            '--source-server-side-encryption=SSE-C',
            f'b2id://{file_version_info["fileId"]}',
            f'b2://{bucket_name}/gonna-fail-anyway',
        ],
        expected_pattern='ValueError: Using SSE-C requires providing an encryption key via '
        'B2_SOURCE_SSE_C_KEY_B64 env var',
    )
    b2_tool.should_fail(
        [
            'file',
            'server-side-copy',
            '--source-server-side-encryption=SSE-C',
            '--destination-server-side-encryption=SSE-C',
            f'b2id://{file_version_info["fileId"]}',
            f'b2://{bucket_name}/gonna-fail-anyway',
        ],
        expected_pattern='ValueError: Using SSE-C requires providing an encryption key via '
        'B2_DESTINATION_SSE_C_KEY_B64 env var',
        additional_env={'B2_SOURCE_SSE_C_KEY_B64': base64.b64encode(secret).decode()},
    )
    b2_tool.should_fail(
        [
            'file',
            'server-side-copy',
            '--source-server-side-encryption=SSE-C',
            f'b2id://{file_version_info["fileId"]}',
            f'b2://{bucket_name}/gonna-fail-anyway',
        ],
        additional_env={'B2_SOURCE_SSE_C_KEY_B64': base64.b64encode(secret).decode()},
        expected_pattern='Attempting to copy file with metadata while either source or destination uses '
        'SSE-C. Use --fetch-metadata to fetch source file metadata before copying.',
    )
    b2_tool.should_succeed(
        [
            'file',
            'server-side-copy',
            '--source-server-side-encryption=SSE-C',
            f'b2id://{file_version_info["fileId"]}',
            f'b2://{bucket_name}/{subfolder}/not_encrypted_copied_from_encrypted_metadata_replace',
            '--info',
            'a=b',
            '--content-type',
            'text/plain',
        ],
        additional_env={'B2_SOURCE_SSE_C_KEY_B64': base64.b64encode(secret).decode()},
    )
    b2_tool.should_succeed(
        [
            'file',
            'server-side-copy',
            '--source-server-side-encryption=SSE-C',
            f'b2id://{file_version_info["fileId"]}',
            f'b2://{bucket_name}/{subfolder}/not_encrypted_copied_from_encrypted_metadata_replace_empty',
            '--no-info',
            '--content-type',
            'text/plain',
        ],
        additional_env={'B2_SOURCE_SSE_C_KEY_B64': base64.b64encode(secret).decode()},
    )
    b2_tool.should_succeed(
        [
            'file',
            'server-side-copy',
            '--source-server-side-encryption=SSE-C',
            f'b2id://{file_version_info["fileId"]}',
            f'b2://{bucket_name}/{subfolder}/not_encrypted_copied_from_encrypted_metadata_pseudo_copy',
            '--fetch-metadata',
        ],
        additional_env={'B2_SOURCE_SSE_C_KEY_B64': base64.b64encode(secret).decode()},
    )
    b2_tool.should_succeed(
        [
            'file',
            'server-side-copy',
            '--source-server-side-encryption=SSE-C',
            '--destination-server-side-encryption=SSE-C',
            f'b2id://{file_version_info["fileId"]}',
            f'b2://{bucket_name}/{subfolder}/encrypted_no_id_copied_from_encrypted',
            '--fetch-metadata',
        ],
        additional_env={
            'B2_SOURCE_SSE_C_KEY_B64': base64.b64encode(secret).decode(),
            'B2_DESTINATION_SSE_C_KEY_B64': base64.b64encode(os.urandom(32)).decode(),
        },
    )
    b2_tool.should_succeed(
        [
            'file',
            'server-side-copy',
            '--source-server-side-encryption=SSE-C',
            '--destination-server-side-encryption=SSE-C',
            f'b2id://{file_version_info["fileId"]}',
            f'b2://{bucket_name}/{subfolder}/encrypted_with_id_copied_from_encrypted_metadata_replace',
            '--no-info',
            '--content-type',
            'text/plain',
        ],
        additional_env={
            'B2_SOURCE_SSE_C_KEY_B64': base64.b64encode(secret).decode(),
            'B2_DESTINATION_SSE_C_KEY_B64': base64.b64encode(os.urandom(32)).decode(),
            'B2_DESTINATION_SSE_C_KEY_ID': 'another-user-generated-key-id',
        },
    )
    b2_tool.should_succeed(
        [
            'file',
            'server-side-copy',
            '--source-server-side-encryption=SSE-C',
            '--destination-server-side-encryption=SSE-C',
            f'b2id://{file_version_info["fileId"]}',
            f'b2://{bucket_name}/{subfolder}/encrypted_with_id_copied_from_encrypted_metadata_pseudo_copy',
            '--fetch-metadata',
        ],
        additional_env={
            'B2_SOURCE_SSE_C_KEY_B64': base64.b64encode(secret).decode(),
            'B2_DESTINATION_SSE_C_KEY_B64': base64.b64encode(os.urandom(32)).decode(),
            'B2_DESTINATION_SSE_C_KEY_ID': 'another-user-generated-key-id',
        },
    )
    list_of_files = b2_tool.should_succeed_json(
        ['ls', '--json', '--recursive', *b2_uri_args(bucket_name, subfolder)]
    )

    should_equal(
        [
            {
                'file_name': f'{subfolder}/encrypted_no_id_copied_from_encrypted',
                'sse_c_key_id': 'missing_key',
                'serverSideEncryption': {
                    'algorithm': 'AES256',
                    'customerKey': '******',
                    'customerKeyMd5': '******',
                    'mode': 'SSE-C',
                },
            },
            {
                'file_name': f'{subfolder}/encrypted_with_id_copied_from_encrypted_metadata_pseudo_copy',
                'sse_c_key_id': 'another-user-generated-key-id',
                'serverSideEncryption': {
                    'algorithm': 'AES256',
                    'customerKey': '******',
                    'customerKeyMd5': '******',
                    'mode': 'SSE-C',
                },
            },
            {
                'file_name': f'{subfolder}/encrypted_with_id_copied_from_encrypted_metadata_replace',
                'sse_c_key_id': 'another-user-generated-key-id',
                'serverSideEncryption': {
                    'algorithm': 'AES256',
                    'customerKey': '******',
                    'customerKeyMd5': '******',
                    'mode': 'SSE-C',
                },
            },
            {
                'file_name': f'{subfolder}/not_encrypted_copied_from_encrypted_metadata_pseudo_copy',
                'sse_c_key_id': 'missing_key',
                'serverSideEncryption': {
                    'mode': 'none',
                },
            },
            {
                'file_name': f'{subfolder}/not_encrypted_copied_from_encrypted_metadata_replace',
                'sse_c_key_id': 'missing_key',
                'serverSideEncryption': {
                    'mode': 'none',
                },
            },
            {
                'file_name': f'{subfolder}/not_encrypted_copied_from_encrypted_metadata_replace_empty',
                'sse_c_key_id': 'missing_key',
                'serverSideEncryption': {
                    'mode': 'none',
                },
            },
            {
                'file_name': f'{subfolder}/uploaded_encrypted',
                'sse_c_key_id': sse_c_key_id,
                'serverSideEncryption': {
                    'algorithm': 'AES256',
                    'customerKey': '******',
                    'customerKeyMd5': '******',
                    'mode': 'SSE-C',
                },
            },
        ],
        sorted(
            [
                {
                    'sse_c_key_id': f['fileInfo'].get(
                        SSE_C_KEY_ID_FILE_INFO_KEY_NAME, 'missing_key'
                    ),
                    'serverSideEncryption': f['serverSideEncryption'],
                    'file_name': f['fileName'],
                }
                for f in list_of_files
            ],
            key=lambda r: r['file_name'],
        ),
    )


@pytest.mark.skipif(
    (sys.version_info.major, sys.version_info.minor) < (3, 9),
    reason="License extraction doesn't work on older versions, and we're only "
    'obliged to provide this '
    'data in bundled and built packages.',
)
@pytest.mark.parametrize('with_packages', [True, False])
def test_license(b2_tool, with_packages, cli_version):
    license_text = b2_tool.should_succeed(
        ['license'] + (['--with-packages'] if with_packages else [])
    )

    if with_packages:
        # In the case of e.g.: docker image, it has a license built-in with a `b2`.
        # It also is unable to generate this license because it lacks required packages.
        # Thus, I'm allowing here for the test of licenses to pass whenever
        # the binary is named `b2` or with the proper cli version string (e.g. `_b2v4` or `b2v3`).
        full_license_re = re.compile(
            rf'Licenses of all modules used by ({cli_version}|b2)(\.EXE)?, shipped with it in binary form:\r?\n'
            r'\+-*\+-*\+\r?\n'
            r'\|\s*Module name\s*\|\s*License text\s*\|\r?\n'
            r'.*'
            r'\+-*\+-*\+\r?\n',
            re.MULTILINE + re.DOTALL,
        )
        full_license_text = next(full_license_re.finditer(license_text), None)
        assert full_license_text, license_text
        assert (
            len(full_license_text.group(0)) > 140_000
        )  # we should know if the length of this block changes dramatically
        # Note that GitHub CI adds additional packages:
        # 'colorlog', 'virtualenv', 'nox', 'packaging', 'argcomplete', 'filelock'
        # that sum up to around 50k characters. Tests ran from docker image are unaffected.

        # See the explanation above for why both `b2` and `cli_version` are allowed here.
        license_summary_re = re.compile(
            rf'Summary of all modules used by ({cli_version}|b2)(\.EXE)?, shipped with it in binary form:\r?\n'
            r'\+-*\+-*\+-*\+-*\+-*\+\r?\n'
            r'\|\s*Module name\s*\|\s*Version\s*\|\s*License\s*\|\s*Author\s*\|\s*URL\s*\|\r?\n'
            r'.*'
            r'\+-*\+-*\+-*\+-*\+-*\+\r?\n',
            re.MULTILINE + re.DOTALL,
        )
        license_summary_text = next(license_summary_re.finditer(license_text), None)
        assert license_summary_text, license_text
        assert (
            len(license_summary_text.group(0)) > 6_300
        )  # we should know if the length of this block changes dramatically

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


class TestFileLock(IntegrationTestBase):
    def test_file_lock(
        self,
        b2_tool,
        application_key_id,
        application_key,
        sample_file,
        schedule_bucket_cleanup,
    ):
        lock_disabled_bucket_name = self.create_bucket().name

        now_millis = current_time_millis()

        not_lockable_file = b2_tool.should_succeed_json(  # file in a lock disabled bucket
            ['file', 'upload', '--quiet', lock_disabled_bucket_name, sample_file, 'a']
        )

        _assert_file_lock_configuration(
            b2_tool,
            not_lockable_file['fileId'],
            retention_mode=RetentionMode.NONE,
            legal_hold=LegalHold.UNSET,
        )

        b2_tool.should_fail(
            [
                'file',
                'upload',
                '--quiet',
                lock_disabled_bucket_name,
                sample_file,
                'a',
                '--file-retention-mode',
                'governance',
                '--retain-until',
                str(now_millis + 1.5 * ONE_HOUR_MILLIS),
                '--legal-hold',
                'on',
            ],
            r'ERROR: The bucket is not file lock enabled \(bucket_missing_file_lock\)',
        )

        b2_tool.should_fail(
            [
                'bucket',
                'update',
                lock_disabled_bucket_name,
                'allPrivate',
                '--default-retention-mode',
                'compliance',
            ],
            'ValueError: must specify period for retention mode RetentionMode.COMPLIANCE',
        )
        b2_tool.should_fail(
            [
                'bucket',
                'update',
                lock_disabled_bucket_name,
                'allPrivate',
                '--default-retention-mode',
                'compliance',
                '--default-retention-period',
                '7 days',
            ],
            r'ERROR: The bucket is not file lock enabled \(bucket_missing_file_lock\)',
        )
        lock_enabled_bucket_name = b2_tool.generate_bucket_name()
        schedule_bucket_cleanup(lock_enabled_bucket_name)
        b2_tool.should_succeed(
            [
                'bucket',
                'create',
                lock_enabled_bucket_name,
                'allPrivate',
                '--file-lock-enabled',
                *b2_tool.get_bucket_info_args(),
            ],
        )
        updated_bucket = b2_tool.should_succeed_json(
            [
                'bucket',
                'update',
                lock_enabled_bucket_name,
                'allPrivate',
                '--default-retention-mode',
                'governance',
                '--default-retention-period',
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
            [
                'file',
                'upload',
                '--no-progress',
                '--quiet',
                lock_enabled_bucket_name,
                sample_file,
                'a',
            ]
        )

        # deprecated command
        b2_tool.should_fail(
            [
                'update-file-retention',
                not_lockable_file['fileName'],
                not_lockable_file['fileId'],
                'governance',
                '--retain-until',
                str(now_millis + ONE_DAY_MILLIS + ONE_HOUR_MILLIS),
            ],
            r'ERROR: The bucket is not file lock enabled \(bucket_missing_file_lock\)',
        )

        # deprecated command
        update_file_retention_deprecated_pattern = re.compile(
            re.escape(
                'WARNING: `update-file-retention` command is deprecated. Use `file update` instead.'
            )
        )
        b2_tool.should_succeed(  # first let's try with a file name
            [
                'update-file-retention',
                lockable_file['fileName'],
                lockable_file['fileId'],
                'governance',
                '--retain-until',
                str(now_millis + ONE_DAY_MILLIS + ONE_HOUR_MILLIS),
            ],
            expected_stderr_pattern=update_file_retention_deprecated_pattern,
        )

        lockable_b2uri = f"b2://{lock_enabled_bucket_name}/{lockable_file['fileName']}"
        not_lockable_b2uri = f"b2://{lock_disabled_bucket_name}/{not_lockable_file['fileName']}"

        _assert_file_lock_configuration(
            b2_tool,
            lockable_file['fileId'],
            retention_mode=RetentionMode.GOVERNANCE,
            retain_until=now_millis + ONE_DAY_MILLIS + ONE_HOUR_MILLIS,
        )

        b2_tool.should_succeed(  # and now without a file name
            [
                'file',
                'update',
                '--file-retention-mode',
                'governance',
                '--retain-until',
                str(now_millis + ONE_DAY_MILLIS + 2 * ONE_HOUR_MILLIS),
                lockable_b2uri,
            ],
        )

        _assert_file_lock_configuration(
            b2_tool,
            lockable_file['fileId'],
            retention_mode=RetentionMode.GOVERNANCE,
            retain_until=now_millis + ONE_DAY_MILLIS + 2 * ONE_HOUR_MILLIS,
        )

        b2_tool.should_fail(
            [
                'file',
                'update',
                '--file-retention-mode',
                'governance',
                '--retain-until',
                str(now_millis + ONE_HOUR_MILLIS),
                lockable_b2uri,
            ],
            "ERROR: Auth token not authorized to write retention or file already in 'compliance' mode or "
            'bypassGovernance=true parameter missing',
        )
        b2_tool.should_succeed(
            [
                'file',
                'update',
                '--file-retention-mode',
                'governance',
                '--retain-until',
                str(now_millis + ONE_HOUR_MILLIS),
                '--bypass-governance',
                lockable_b2uri,
            ],
        )

        _assert_file_lock_configuration(
            b2_tool,
            lockable_file['fileId'],
            retention_mode=RetentionMode.GOVERNANCE,
            retain_until=now_millis + ONE_HOUR_MILLIS,
        )

        b2_tool.should_fail(
            ['file', 'update', '--file-retention-mode', 'none', lockable_b2uri],
            "ERROR: Auth token not authorized to write retention or file already in 'compliance' mode or "
            'bypassGovernance=true parameter missing',
        )
        b2_tool.should_succeed(
            [
                'file',
                'update',
                '--file-retention-mode',
                'none',
                '--bypass-governance',
                lockable_b2uri,
            ],
        )

        _assert_file_lock_configuration(
            b2_tool, lockable_file['fileId'], retention_mode=RetentionMode.NONE
        )

        b2_tool.should_fail(
            ['file', 'update', '--legal-hold', 'on', not_lockable_b2uri],
            r'ERROR: The bucket is not file lock enabled \(bucket_missing_file_lock\)',
        )

        # deprecated command
        update_file_legal_hold_deprecated_pattern = re.compile(
            re.escape(
                'WARNING: `update-file-legal-hold` command is deprecated. Use `file update` instead.'
            )
        )
        b2_tool.should_succeed(  # first let's try with a file name
            ['update-file-legal-hold', lockable_file['fileName'], lockable_file['fileId'], 'on'],
            expected_stderr_pattern=update_file_legal_hold_deprecated_pattern,
        )

        _assert_file_lock_configuration(b2_tool, lockable_file['fileId'], legal_hold=LegalHold.ON)

        b2_tool.should_succeed(  # and now without a file name
            ['file', 'update', '--legal-hold', 'off', lockable_b2uri],
        )

        _assert_file_lock_configuration(b2_tool, lockable_file['fileId'], legal_hold=LegalHold.OFF)

        updated_bucket = b2_tool.should_succeed_json(
            [
                'bucket',
                'update',
                lock_enabled_bucket_name,
                'allPrivate',
                '--default-retention-mode',
                'none',
            ],
        )
        assert updated_bucket['defaultRetention'] == {'mode': None}

        b2_tool.should_fail(
            [
                'file',
                'upload',
                '--no-progress',
                '--quiet',
                lock_enabled_bucket_name,
                sample_file,
                'a',
                '--file-retention-mode',
                'governance',
                '--retain-until',
                str(now_millis - 1.5 * ONE_HOUR_MILLIS),
            ],
            r'ERROR: The retainUntilTimestamp must be in future \(retain_until_timestamp_must_be_in_future\)',
        )

        uploaded_file = b2_tool.should_succeed_json(
            [
                'file',
                'upload',
                '--no-progress',
                '--quiet',
                lock_enabled_bucket_name,
                sample_file,
                'a',
                '--file-retention-mode',
                'governance',
                '--retain-until',
                str(now_millis + 1.5 * ONE_HOUR_MILLIS),
                '--legal-hold',
                'on',
            ]
        )

        _assert_file_lock_configuration(
            b2_tool,
            uploaded_file['fileId'],
            retention_mode=RetentionMode.GOVERNANCE,
            retain_until=now_millis + 1.5 * ONE_HOUR_MILLIS,
            legal_hold=LegalHold.ON,
        )

        b2_tool.should_fail(
            [
                'file',
                'server-side-copy',
                f'b2id://{lockable_file["fileId"]}',
                f'b2://{lock_disabled_bucket_name}/copied',
                '--file-retention-mode',
                'governance',
                '--retain-until',
                str(now_millis + 1.25 * ONE_HOUR_MILLIS),
                '--legal-hold',
                'off',
            ],
            r'ERROR: The bucket is not file lock enabled \(bucket_missing_file_lock\)',
        )

        copied_file = b2_tool.should_succeed_json(
            [
                'file',
                'server-side-copy',
                f"b2id://{lockable_file['fileId']}",
                f'b2://{lock_enabled_bucket_name}/copied',
                '--file-retention-mode',
                'governance',
                '--retain-until',
                str(now_millis + 1.25 * ONE_HOUR_MILLIS),
                '--legal-hold',
                'off',
            ]
        )

        _assert_file_lock_configuration(
            b2_tool,
            copied_file['fileId'],
            retention_mode=RetentionMode.GOVERNANCE,
            retain_until=now_millis + 1.25 * ONE_HOUR_MILLIS,
            legal_hold=LegalHold.OFF,
        )
        lock_disabled_key_id, lock_disabled_key = make_lock_disabled_key(b2_tool)

        b2_tool.should_succeed(
            [
                'account',
                'authorize',
                '--environment',
                b2_tool.realm,
                lock_disabled_key_id,
                lock_disabled_key,
            ],
        )

        file_lock_without_perms_test(
            b2_tool,
            lock_enabled_bucket_name,
            lock_disabled_bucket_name,
            lockable_file['fileId'],
            not_lockable_file['fileId'],
            lockable_b2uri,
            not_lockable_b2uri,
            sample_file=sample_file,
        )

        b2_tool.should_succeed(
            [
                'account',
                'authorize',
                '--environment',
                b2_tool.realm,
                application_key_id,
                application_key,
            ],
        )

        deleting_locked_files(
            b2_tool, lock_enabled_bucket_name, lock_disabled_key_id, lock_disabled_key, sample_file
        )


def make_lock_disabled_key(b2_tool):
    key_name = 'no-perms-for-file-lock' + random_hex(6)
    created_key_stdout = b2_tool.should_succeed(
        [
            'key',
            'create',
            key_name,
            'listFiles,listBuckets,readFiles,writeKeys,deleteFiles',
        ]
    )
    key_id, key = created_key_stdout.split()
    return key_id, key


def file_lock_without_perms_test(
    b2_tool,
    lock_enabled_bucket_name,
    lock_disabled_bucket_name,
    lockable_file_id,
    not_lockable_file_id,
    lockable_b2uri,
    not_lockable_b2uri,
    sample_file,
):
    b2_tool.should_fail(
        [
            'bucket',
            'update',
            lock_enabled_bucket_name,
            'allPrivate',
            '--default-retention-mode',
            'governance',
            '--default-retention-period',
            '1 days',
        ],
        'ERROR: unauthorized for application key with capabilities',
    )

    _assert_file_lock_configuration(
        b2_tool,
        lockable_file_id,
        retention_mode=RetentionMode.UNKNOWN,
        legal_hold=LegalHold.UNKNOWN,
    )

    b2_tool.should_fail(
        [
            'file',
            'update',
            '--file-retention-mode',
            'governance',
            '--retain-until',
            str(current_time_millis() + 7 * ONE_DAY_MILLIS),
            lockable_b2uri,
        ],
        "ERROR: Auth token not authorized to write retention or file already in 'compliance' mode or "
        'bypassGovernance=true parameter missing',
    )

    b2_tool.should_fail(
        [
            'file',
            'update',
            '--file-retention-mode',
            'governance',
            '--retain-until',
            str(current_time_millis() + 7 * ONE_DAY_MILLIS),
            not_lockable_b2uri,
        ],
        "ERROR: Auth token not authorized to write retention or file already in 'compliance' mode or "
        'bypassGovernance=true parameter missing',
    )

    b2_tool.should_fail(
        ['file', 'update', '--legal-hold', 'on', lockable_b2uri],
        "ERROR: Auth token not authorized to write retention or file already in 'compliance' mode or "
        'bypassGovernance=true parameter missing',
    )

    b2_tool.should_fail(
        ['file', 'update', '--legal-hold', 'on', not_lockable_b2uri],
        "ERROR: Auth token not authorized to write retention or file already in 'compliance' mode or "
        'bypassGovernance=true parameter missing',
    )

    b2_tool.should_fail(
        [
            'file',
            'upload',
            '--no-progress',
            '--quiet',
            lock_enabled_bucket_name,
            sample_file,
            'bound_to_fail_anyway',
            '--file-retention-mode',
            'governance',
            '--retain-until',
            str(current_time_millis() + ONE_HOUR_MILLIS),
            '--legal-hold',
            'on',
        ],
        'unauthorized for application key with capabilities',
    )

    b2_tool.should_fail(
        [
            'file',
            'upload',
            '--no-progress',
            '--quiet',
            lock_disabled_bucket_name,
            sample_file,
            'bound_to_fail_anyway',
            '--file-retention-mode',
            'governance',
            '--retain-until',
            str(current_time_millis() + ONE_HOUR_MILLIS),
            '--legal-hold',
            'on',
        ],
        'unauthorized for application key with capabilities',
    )

    b2_tool.should_fail(
        [
            'file',
            'server-side-copy',
            f'b2id://{lockable_file_id}',
            f'b2://{lock_enabled_bucket_name}/copied',
            '--file-retention-mode',
            'governance',
            '--retain-until',
            str(current_time_millis() + ONE_HOUR_MILLIS),
            '--legal-hold',
            'off',
        ],
        'ERROR: unauthorized for application key with capabilities',
    )

    b2_tool.should_fail(
        [
            'file',
            'server-side-copy',
            f'b2id://{lockable_file_id}',
            f'b2://{lock_disabled_bucket_name}/copied',
            '--file-retention-mode',
            'governance',
            '--retain-until',
            str(current_time_millis() + ONE_HOUR_MILLIS),
            '--legal-hold',
            'off',
        ],
        'ERROR: unauthorized for application key with capabilities',
    )


def upload_locked_file(b2_tool, bucket_name, sample_file):
    return b2_tool.should_succeed_json(
        [
            'file',
            'upload',
            '--no-progress',
            '--quiet',
            '--file-retention-mode',
            'governance',
            '--retain-until',
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
        'ERROR: Access Denied for application key ',
    )
    b2_tool.should_succeed(
        [  # master key
            'delete-file-version',
            locked_file['fileName'],
            locked_file['fileId'],
            '--bypass-governance',
        ],
        expected_stderr_pattern=re.compile(
            re.escape('WARNING: `delete-file-version` command is deprecated. Use `rm` instead.')
        ),
    )

    locked_file = upload_locked_file(b2_tool, lock_enabled_bucket_name, sample_file)

    b2_tool.should_succeed(
        [
            'account',
            'authorize',
            '--environment',
            b2_tool.realm,
            lock_disabled_key_id,
            lock_disabled_key,
        ],
    )
    b2_tool.should_fail(
        [  # lock disabled key
            'delete-file-version',
            locked_file['fileName'],
            locked_file['fileId'],
            '--bypass-governance',
        ],
        "ERROR: unauthorized for application key with capabilities '",
    )


@pytest.mark.apiver(from_ver=4)
def test_deleting_locked_files_v4(b2_tool, sample_file, schedule_bucket_cleanup):
    lock_enabled_bucket_name = b2_tool.generate_bucket_name()
    schedule_bucket_cleanup(lock_enabled_bucket_name)
    b2_tool.should_succeed(
        [
            'bucket',
            'create',
            lock_enabled_bucket_name,
            'allPrivate',
            '--file-lock-enabled',
            *b2_tool.get_bucket_info_args(),
        ],
    )
    updated_bucket = b2_tool.should_succeed_json(
        [
            'bucket',
            'update',
            lock_enabled_bucket_name,
            'allPrivate',
            '--default-retention-mode',
            'governance',
            '--default-retention-period',
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

    locked_file = upload_locked_file(b2_tool, lock_enabled_bucket_name, sample_file)
    b2_tool.should_fail(
        [  # master key
            'rm',
            f"b2id://{locked_file['fileId']}",
        ],
        ' failed: Access Denied for application key ',
    )
    b2_tool.should_succeed(
        [  # master key
            'rm',
            '--bypass-governance',
            f"b2id://{locked_file['fileId']}",
        ]
    )

    locked_file = upload_locked_file(b2_tool, lock_enabled_bucket_name, sample_file)

    lock_disabled_key_id, lock_disabled_key = make_lock_disabled_key(b2_tool)
    b2_tool.should_succeed(
        [
            'account',
            'authorize',
            '--environment',
            b2_tool.realm,
            lock_disabled_key_id,
            lock_disabled_key,
        ],
    )

    b2_tool.should_fail(
        [  # lock disabled key
            'rm',
            '--bypass-governance',
            f"b2id://{locked_file['fileId']}",
        ],
        " failed: unauthorized for application key with capabilities '",
    )


def test_profile_switch(b2_tool):
    # this test could be unit, but it adds a lot of complexity because of
    # necessity to pass mocked B2Api to ConsoleTool; it's much easier to
    # just have an integration test instead

    MISSING_ACCOUNT_PATTERN = 'Missing account data'

    b2_tool.should_succeed(
        [
            'account',
            'authorize',
            '--environment',
            b2_tool.realm,
            b2_tool.account_id,
            b2_tool.application_key,
        ]
    )
    b2_tool.should_succeed(['account', 'get'])
    b2_tool.should_succeed(['account', 'clear'])
    b2_tool.should_fail(['account', 'get'], expected_pattern=MISSING_ACCOUNT_PATTERN)

    # in order to use --profile flag, we need to temporary
    # delete B2_ACCOUNT_INFO_ENV_VAR
    B2_ACCOUNT_INFO = os.environ.pop(B2_ACCOUNT_INFO_ENV_VAR, None)

    # now authorize a different account
    profile = 'profile-for-test-' + random_hex(6)
    b2_tool.should_fail(
        ['account', 'get', '--profile', profile],
        expected_pattern=MISSING_ACCOUNT_PATTERN,
    )
    b2_tool.should_succeed(
        [
            'account',
            'authorize',
            '--environment',
            b2_tool.realm,
            '--profile',
            profile,
            b2_tool.account_id,
            b2_tool.application_key,
        ]
    )

    account_info = b2_tool.should_succeed_json(['account', 'get', '--profile', profile])
    account_file_path = account_info['accountFilePath']
    assert (
        profile in account_file_path
    ), f'accountFilePath "{account_file_path}" should contain profile name "{profile}"'

    b2_tool.should_succeed(['account', 'clear', '--profile', profile])
    b2_tool.should_fail(
        ['account', 'get', '--profile', profile],
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
            'key',
            'create',
            key_one_name,
            'listBuckets,readFiles',
        ]
    )
    key_one_id, _ = created_key_stdout.split()

    key_two_name = 'clt-testKey-02' + random_hex(6)
    created_key_stdout = b2_tool.should_succeed(
        [
            'key',
            'create',
            key_two_name,
            'listBuckets,writeFiles',
        ]
    )
    key_two_id, _ = created_key_stdout.split()

    destination_bucket_name = bucket_name
    destination_bucket = b2_tool.should_succeed_json(['bucket', 'get', destination_bucket_name])

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
            'bucket',
            'update',
            destination_bucket_name,
            'allPublic',
            '--replication',
            destination_replication_configuration_json,
        ]
    )

    # test that destination bucket is registered as replication destination
    assert destination_bucket['replication'].get('asReplicationSource') is None
    assert (
        destination_bucket['replication']['asReplicationDestination']
        == destination_replication_configuration['asReplicationDestination']
    )

    # ---------------- set up replication source ----------------
    source_replication_configuration = {
        'asReplicationSource': {
            'replicationRules': [
                {
                    'destinationBucketId': destination_bucket['bucketId'],
                    'fileNamePrefix': 'one/',
                    'includeExistingFiles': False,
                    'isEnabled': True,
                    'priority': 1,
                    'replicationRuleName': 'replication-one',
                },
                {
                    'destinationBucketId': destination_bucket['bucketId'],
                    'fileNamePrefix': 'two/',
                    'includeExistingFiles': False,
                    'isEnabled': True,
                    'priority': 2,
                    'replicationRuleName': 'replication-two',
                },
            ],
            'sourceApplicationKeyId': key_one_id,
        },
    }
    source_replication_configuration_json = json.dumps(source_replication_configuration)

    # create a source bucket and set up replication to destination bucket
    source_bucket_name = b2_tool.generate_bucket_name()
    schedule_bucket_cleanup(source_bucket_name)
    b2_tool.should_succeed(
        [
            'bucket',
            'create',
            source_bucket_name,
            'allPublic',
            '--replication',
            source_replication_configuration_json,
            *b2_tool.get_bucket_info_args(),
        ]
    )
    source_bucket = b2_tool.should_succeed_json(['bucket', 'get', source_bucket_name])

    # test that all replication rules are present in source bucket
    assert (
        source_bucket['replication']['asReplicationSource']
        == source_replication_configuration['asReplicationSource']
    )

    # test that source bucket is not mentioned as replication destination
    assert source_bucket['replication'].get('asReplicationDestination') is None

    # ---------------- attempt enabling object lock  ----------------
    b2_tool.should_fail(
        ['bucket', 'update', source_bucket_name, '--file-lock-enabled'],
        'ERROR: Operation not supported for buckets with source replication',
    )

    # ---------------- remove replication source ----------------

    no_replication_configuration = {
        'asReplicationSource': None,
        'asReplicationDestination': None,
    }
    no_replication_configuration_json = json.dumps(no_replication_configuration)
    source_bucket = b2_tool.should_succeed_json(
        [
            'bucket',
            'update',
            source_bucket_name,
            'allPublic',
            '--replication',
            no_replication_configuration_json,
        ]
    )

    # test that source bucket replication is removed
    assert source_bucket['replication'] == {
        'asReplicationDestination': None,
        'asReplicationSource': None,
    }

    # ---------------- remove replication destination ----------------

    destination_bucket = b2_tool.should_succeed_json(
        [
            'bucket',
            'update',
            destination_bucket_name,
            'allPublic',
            '--replication',
            '{}',
        ]
    )

    # test that destination bucket replication is removed
    assert destination_bucket['replication'] == {
        'asReplicationDestination': None,
        'asReplicationSource': None,
    }

    b2_tool.should_succeed(['key', 'delete', key_one_id])
    b2_tool.should_succeed(['key', 'delete', key_two_id])


def test_replication_setup(b2_tool, bucket_name, schedule_bucket_cleanup):
    base_test_replication_setup(b2_tool, bucket_name, schedule_bucket_cleanup, True)


def test_replication_setup_deprecated(b2_tool, bucket_name, schedule_bucket_cleanup):
    base_test_replication_setup(b2_tool, bucket_name, schedule_bucket_cleanup, False)


def base_test_replication_setup(b2_tool, bucket_name, schedule_bucket_cleanup, use_subcommands):
    setup_cmd = ['replication', 'setup'] if use_subcommands else ['replication-setup']
    replication_setup_deprecated_pattern = re.compile(
        re.escape(
            'WARNING: `replication-setup` command is deprecated. Use `replication setup` instead.'
        )
    )
    replication_setup_expected_stderr_pattern = (
        None if use_subcommands else replication_setup_deprecated_pattern
    )

    source_bucket_name = b2_tool.generate_bucket_name()
    schedule_bucket_cleanup(source_bucket_name)
    b2_tool.should_succeed(
        [
            'bucket',
            'create',
            source_bucket_name,
            'allPublic',
            '--file-lock-enabled',
            *b2_tool.get_bucket_info_args(),
        ]
    )
    destination_bucket_name = bucket_name
    b2_tool.should_succeed(
        [*setup_cmd, source_bucket_name, destination_bucket_name],
        expected_stderr_pattern=replication_setup_expected_stderr_pattern,
    )
    destination_bucket_old = b2_tool.should_succeed_json(['bucket', 'get', destination_bucket_name])

    b2_tool.should_succeed(
        [
            *setup_cmd,
            '--priority',
            '132',
            '--file-name-prefix',
            'foo',
            '--name',
            'my-replication-rule',
            source_bucket_name,
            destination_bucket_name,
        ],
        expected_stderr_pattern=replication_setup_expected_stderr_pattern,
    )
    source_bucket = b2_tool.should_succeed_json(['bucket', 'get', source_bucket_name])
    destination_bucket = b2_tool.should_succeed_json(['bucket', 'get', destination_bucket_name])
    assert source_bucket['replication']['asReplicationSource']['replicationRules'] == [
        {
            'destinationBucketId': destination_bucket['bucketId'],
            'fileNamePrefix': '',
            'includeExistingFiles': False,
            'isEnabled': True,
            'priority': 128,
            'replicationRuleName': destination_bucket['bucketName'],
        },
        {
            'destinationBucketId': destination_bucket['bucketId'],
            'fileNamePrefix': 'foo',
            'includeExistingFiles': False,
            'isEnabled': True,
            'priority': 132,
            'replicationRuleName': 'my-replication-rule',
        },
    ]

    for key_one_id, key_two_id in destination_bucket['replication']['asReplicationDestination'][
        'sourceToDestinationKeyMapping'
    ].items():
        b2_tool.should_succeed(['key', 'delete', key_one_id])
        b2_tool.should_succeed(['key', 'delete', key_two_id])
    assert (
        destination_bucket_old['replication']['asReplicationDestination'][
            'sourceToDestinationKeyMapping'
        ]
        == destination_bucket['replication']['asReplicationDestination'][
            'sourceToDestinationKeyMapping'
        ]
    )


def test_replication_monitoring(b2_tool, bucket_name, sample_file, schedule_bucket_cleanup):
    # ---------------- set up keys ----------------
    key_one_name = 'clt-testKey-01' + random_hex(6)
    created_key_stdout = b2_tool.should_succeed(
        [
            'key',
            'create',
            key_one_name,
            'listBuckets,readFiles',
        ]
    )
    key_one_id, _ = created_key_stdout.split()

    key_two_name = 'clt-testKey-02' + random_hex(6)
    created_key_stdout = b2_tool.should_succeed(
        [
            'key',
            'create',
            key_two_name,
            'listBuckets,writeFiles',
        ]
    )
    key_two_id, _ = created_key_stdout.split()

    # ---------------- add test data ----------------
    destination_bucket_name = bucket_name
    uploaded_a = b2_tool.should_succeed_json(
        ['file', 'upload', '--quiet', destination_bucket_name, sample_file, 'one/a']
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
            'bucket',
            'update',
            destination_bucket_name,
            'allPublic',
            '--replication',
            destination_replication_configuration_json,
        ]
    )

    # ---------------- set up replication source ----------------
    source_replication_configuration = {
        'asReplicationSource': {
            'replicationRules': [
                {
                    'destinationBucketId': destination_bucket['bucketId'],
                    'fileNamePrefix': 'one/',
                    'includeExistingFiles': False,
                    'isEnabled': True,
                    'priority': 1,
                    'replicationRuleName': 'replication-one',
                },
                {
                    'destinationBucketId': destination_bucket['bucketId'],
                    'fileNamePrefix': 'two/',
                    'includeExistingFiles': False,
                    'isEnabled': True,
                    'priority': 2,
                    'replicationRuleName': 'replication-two',
                },
            ],
            'sourceApplicationKeyId': key_one_id,
        },
    }
    source_replication_configuration_json = json.dumps(source_replication_configuration)

    # create a source bucket and set up replication to destination bucket
    source_bucket_name = b2_tool.generate_bucket_name()
    schedule_bucket_cleanup(source_bucket_name)
    b2_tool.should_succeed(
        [
            'bucket',
            'create',
            source_bucket_name,
            'allPublic',
            '--file-lock-enabled',
            '--replication',
            source_replication_configuration_json,
            *b2_tool.get_bucket_info_args(),
        ]
    )

    # make test data
    uploaded_a = b2_tool.should_succeed_json(
        ['file', 'upload', '--quiet', source_bucket_name, sample_file, 'one/a']
    )
    b2_tool.should_succeed_json(
        [
            'file',
            'upload',
            '--quiet',
            source_bucket_name,
            '--legal-hold',
            'on',
            sample_file,
            'two/b',
        ]
    )

    # encryption
    # SSE-B2
    upload_encryption_args = ['--destination-server-side-encryption', 'SSE-B2']
    upload_additional_env = {}
    b2_tool.should_succeed_json(
        ['file', 'upload', '--quiet', source_bucket_name, sample_file, 'two/c']
        + upload_encryption_args,
        additional_env=upload_additional_env,
    )

    # SSE-C
    upload_encryption_args = ['--destination-server-side-encryption', 'SSE-C']
    upload_additional_env = {
        'B2_DESTINATION_SSE_C_KEY_B64': base64.b64encode(SSE_C_AES.key.secret).decode(),
        'B2_DESTINATION_SSE_C_KEY_ID': SSE_C_AES.key.key_id,
    }
    b2_tool.should_succeed_json(
        ['file', 'upload', '--quiet', source_bucket_name, sample_file, 'two/d']
        + upload_encryption_args,
        additional_env=upload_additional_env,
    )

    # encryption + legal hold
    b2_tool.should_succeed_json(
        [
            'file',
            'upload',
            '--quiet',
            source_bucket_name,
            sample_file,
            'two/e',
            '--legal-hold',
            'on',
        ]
        + upload_encryption_args,
        additional_env=upload_additional_env,
    )

    # there is just one file, so clean after itself for faster execution
    b2_tool.should_succeed(
        ['delete-file-version', uploaded_a['fileName'], uploaded_a['fileId']],
        expected_stderr_pattern=re.compile(
            re.escape('WARNING: `delete-file-version` command is deprecated. Use `rm` instead.')
        ),
    )

    # run stats command
    replication_status_deprecated_pattern = re.compile(
        re.escape(
            'WARNING: `replication-status` command is deprecated. Use `replication status` instead.'
        )
    )
    replication_status_json = b2_tool.should_succeed_json(
        [
            'replication-status',
            # '--destination-profile',
            # profile,
            '--no-progress',
            # '--columns=count, hash differs',
            '--output-format',
            'json',
            source_bucket_name,
        ],
        expected_stderr_pattern=replication_status_deprecated_pattern,
    )

    replication_status_json = b2_tool.should_succeed_json(
        [
            'replication',
            'status',
            # '--destination-profile',
            # profile,
            '--no-progress',
            # '--columns=count, hash differs',
            '--output-format',
            'json',
            source_bucket_name,
        ]
    )

    assert replication_status_json in [
        {
            'replication-one': [
                {
                    'count': 1,
                    'destination_replication_status': None,
                    'hash_differs': None,
                    'metadata_differs': None,
                    'source_has_file_retention': None,
                    'source_has_hide_marker': None,
                    'source_has_large_metadata': None,
                    'source_has_legal_hold': None,
                    'source_encryption_mode': None,
                    'source_replication_status': None,
                }
            ],
            'replication-two': [
                {
                    'count': 1,
                    'destination_replication_status': None,
                    'hash_differs': None,
                    'metadata_differs': None,
                    'source_has_file_retention': False,
                    'source_has_hide_marker': False,
                    'source_has_large_metadata': False,
                    'source_has_legal_hold': True,
                    'source_encryption_mode': 'none',
                    'source_replication_status': first,
                },
                {
                    'count': 1,
                    'destination_replication_status': None,
                    'hash_differs': None,
                    'metadata_differs': None,
                    'source_has_file_retention': False,
                    'source_has_hide_marker': False,
                    'source_has_large_metadata': False,
                    'source_has_legal_hold': False,
                    'source_encryption_mode': 'SSE-B2',
                    'source_replication_status': second,
                },
                {
                    'count': 1,
                    'destination_replication_status': None,
                    'hash_differs': None,
                    'metadata_differs': None,
                    'source_has_file_retention': False,
                    'source_has_hide_marker': False,
                    'source_has_large_metadata': False,
                    'source_has_legal_hold': False,
                    'source_encryption_mode': 'SSE-C',
                    'source_replication_status': None,
                },
                {
                    'count': 1,
                    'destination_replication_status': None,
                    'hash_differs': None,
                    'metadata_differs': None,
                    'source_has_file_retention': False,
                    'source_has_hide_marker': False,
                    'source_has_large_metadata': False,
                    'source_has_legal_hold': True,
                    'source_encryption_mode': 'SSE-C',
                    'source_replication_status': None,
                },
            ],
        }
        for first, second in itertools.product(['FAILED', 'PENDING'], ['FAILED', 'PENDING'])
    ]


def test_enable_file_lock_first_retention_second(b2_tool, bucket_name):
    # enable file lock only
    b2_tool.should_succeed(['bucket', 'update', bucket_name, '--file-lock-enabled'])

    # set retention with file lock already enabled
    b2_tool.should_succeed(
        [
            'bucket',
            'update',
            bucket_name,
            '--default-retention-mode',
            'compliance',
            '--default-retention-period',
            '7 days',
        ]
    )

    # attempt to re-enable should be a noop
    b2_tool.should_succeed(['bucket', 'update', bucket_name, '--file-lock-enabled'])


def test_enable_file_lock_and_set_retention_at_once(b2_tool, bucket_name):
    # attempt setting retention without file lock enabled
    b2_tool.should_fail(
        [
            'bucket',
            'update',
            bucket_name,
            '--default-retention-mode',
            'compliance',
            '--default-retention-period',
            '7 days',
        ],
        r'ERROR: The bucket is not file lock enabled \(bucket_missing_file_lock\)',
    )

    # enable file lock and set retention at once
    b2_tool.should_succeed(
        [
            'bucket',
            'update',
            bucket_name,
            '--default-retention-mode',
            'compliance',
            '--default-retention-period',
            '7 days',
            '--file-lock-enabled',
        ]
    )

    # attempt to re-enable should be a noop
    b2_tool.should_succeed(['bucket', 'update', bucket_name, '--file-lock-enabled'])


def _assert_file_lock_configuration(
    b2_tool,
    file_id,
    retention_mode: RetentionMode | None = None,
    retain_until: int | None = None,
    legal_hold: LegalHold | None = None,
):
    file_version = b2_tool.should_succeed_json(['file', 'info', f'b2id://{file_id}'])
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


def test_upload_file__custom_upload_time(b2_tool, persistent_bucket, sample_file, b2_uri_args):
    bucket_name = persistent_bucket.bucket_name
    subfolder = persistent_bucket.subfolder
    file_data = read_file(sample_file)
    cut = 12345
    cut_printable = '1970-01-01  00:00:12'
    args = [
        'file',
        'upload',
        '--no-progress',
        '--custom-upload-time',
        str(cut),
        '--quiet',
        bucket_name,
        sample_file,
        f'{subfolder}/a',
    ]
    succeeded, stdout = b2_tool.run_command(args)
    if not succeeded:
        b2_tool.should_fail(args, 'custom_timestamp_not_allowed')
    else:
        # file_id, action, date, time, size(, replication), name
        b2_tool.should_succeed(
            ['ls', '--long', *b2_uri_args(bucket_name, subfolder)],
            f'^4_z.*  upload  {cut_printable} +{len(file_data)}  {subfolder}/a',
        )
        # file_id, action, date, time, size(, replication), name
        b2_tool.should_succeed(
            ['ls', '--long', '--replication', *b2_uri_args(bucket_name, subfolder)],
            f'^4_z.*  upload  {cut_printable} +{len(file_data)}  -  {subfolder}/a',
        )


@skip_on_windows
def test_upload_file__stdin_pipe_operator(request, bash_runner, b2_tool, persistent_bucket):
    """Test `file upload` from stdin using pipe operator."""
    bucket_name = persistent_bucket.bucket_name
    subfolder = persistent_bucket.subfolder
    content = request.node.name
    run = bash_runner(
        f'echo -n {content!r} '
        f'| '
        f'{" ".join(b2_tool.parse_command(b2_tool.prepare_env()))} file upload {bucket_name} - {subfolder}/{request.node.name}.txt'
    )
    assert hashlib.sha1(content.encode()).hexdigest() in run.stdout


@skip_on_windows
def test_upload_unbound_stream__redirect_operator(
    request, bash_runner, b2_tool, persistent_bucket, is_running_on_docker
):
    """Test upload-unbound-stream from stdin using redirect operator."""
    bucket_name = persistent_bucket.bucket_name
    subfolder = persistent_bucket.subfolder
    if is_running_on_docker:
        pytest.skip('Not supported on Docker')
    content = request.node.name
    command = request.config.getoption('--sut')
    run = bash_runner(
        f'{command} upload-unbound-stream {bucket_name} <(echo -n {content}) {subfolder}/{request.node.name}.txt'
    )
    assert hashlib.sha1(content.encode()).hexdigest() in run.stdout


def test_download_file_stdout(
    b2_tool, persistent_bucket, sample_filepath, tmp_path, uploaded_sample_file
):
    assert (
        b2_tool.should_succeed(
            [
                'file',
                'download',
                '--quiet',
                f"b2://{persistent_bucket.bucket_name}/{uploaded_sample_file['fileName']}",
                '-',
            ],
        )
        == sample_filepath.read_text()
    )
    assert (
        b2_tool.should_succeed(
            ['file', 'download', '--quiet', f"b2id://{uploaded_sample_file['fileId']}", '-'],
        )
        == sample_filepath.read_text()
    )


def test_download_file_to_directory(
    b2_tool, persistent_bucket, sample_filepath, tmp_path, uploaded_sample_file
):
    downloads_directory = 'downloads/'
    target_directory = tmp_path / downloads_directory
    target_directory.mkdir()
    (target_directory / persistent_bucket.subfolder).mkdir()
    filename_as_path = pathlib.Path(uploaded_sample_file['fileName'])

    sample_file_content = sample_filepath.read_text()
    b2_tool.should_succeed(
        [
            'file',
            'download',
            '--quiet',
            f"b2://{persistent_bucket.bucket_name}/{uploaded_sample_file['fileName']}",
            str(target_directory),
        ],
    )
    downloaded_file = target_directory / filename_as_path
    assert (
        downloaded_file.read_text() == sample_file_content
    ), f'{downloaded_file}, {downloaded_file.read_text()}, {sample_file_content}'

    b2_tool.should_succeed(
        [
            'file',
            'download',
            '--quiet',
            f"b2id://{uploaded_sample_file['fileId']}",
            str(target_directory),
        ],
    )
    # A second file should be created.
    new_files = [
        filepath
        for filepath in target_directory.glob(f'{filename_as_path.stem}*{filename_as_path.suffix}')
        if filepath.name != filename_as_path.name
    ]
    assert len(new_files) == 1, f'{new_files}'
    assert (
        new_files[0].read_text() == sample_file_content
    ), f'{new_files}, {new_files[0].read_text()}, {sample_file_content}'


def test_cat(b2_tool, persistent_bucket, sample_filepath, tmp_path, uploaded_sample_file):
    assert (
        b2_tool.should_succeed(
            [
                'file',
                'cat',
                f"b2://{persistent_bucket.bucket_name}/{uploaded_sample_file['fileName']}",
            ],
        )
        == sample_filepath.read_text()
    )
    assert (
        b2_tool.should_succeed(['file', 'cat', f"b2id://{uploaded_sample_file['fileId']}"])
        == sample_filepath.read_text()
    )


def test_header_arguments(b2_tool, persistent_bucket, sample_filepath, tmp_path):
    bucket_name = persistent_bucket.bucket_name
    args = [
        '--cache-control',
        'max-age=3600',
        '--content-disposition',
        'attachment',
        '--content-encoding',
        'gzip',
        '--content-language',
        'en',
        '--expires',
        'Thu, 01 Dec 2050 16:00:00 GMT',
    ]
    expected_file_info = {
        'b2-cache-control': 'max-age=3600',
        'b2-content-disposition': 'attachment',
        'b2-content-encoding': 'gzip',
        'b2-content-language': 'en',
        'b2-expires': 'Thu, 01 Dec 2050 16:00:00 GMT',
    }

    def assert_expected(file_info, expected=expected_file_info):
        for key, val in expected.items():
            assert file_info[key] == val

    status, stdout, stderr = b2_tool.execute(
        [
            'file',
            'upload',
            '--quiet',
            '--no-progress',
            bucket_name,
            str(sample_filepath),
            f'{persistent_bucket.subfolder}/sample_file',
            *args,
            '--info',
            'b2-content-disposition=will-be-overwritten',
        ]
    )
    assert status == 0
    file_version = json.loads(stdout)
    assert_expected(file_version['fileInfo'])

    # Since we used both --info and --content-disposition to set b2-content-disposition,
    # a warning should be emitted
    assert 'will be overwritten' in stderr and 'b2-content-disposition = attachment' in stderr

    copied_version = b2_tool.should_succeed_json(
        [
            'file',
            'server-side-copy',
            '--quiet',
            *args,
            '--content-type',
            'text/plain',
            f"b2id://{file_version['fileId']}",
            f'b2://{bucket_name}/{persistent_bucket.subfolder}/copied_file',
        ]
    )
    assert_expected(copied_version['fileInfo'])

    download_output = b2_tool.should_succeed(
        ['file', 'download', f"b2id://{file_version['fileId']}", tmp_path / 'downloaded_file']
    )
    assert re.search(r'CacheControl: *max-age=3600', download_output)
    assert re.search(r'ContentDisposition: *attachment', download_output)
    assert re.search(r'ContentEncoding: *gzip', download_output)
    assert re.search(r'ContentLanguage: *en', download_output)
    assert re.search(r'Expires: *Thu, 01 Dec 2050 16:00:00 GMT', download_output)


def test_notification_rules(b2_tool, bucket_name):
    auth_dict = b2_tool.should_succeed_json(['account', 'get'])
    if 'writeBucketNotifications' not in auth_dict['allowed']['capabilities']:
        pytest.skip('Test account does not have writeBucketNotifications capability')

    assert (
        b2_tool.should_succeed_json(
            ['bucket', 'notification-rule', 'list', f'b2://{bucket_name}', '--json']
        )
        == []
    )

    notification_rule = {
        'eventTypes': ['b2:ObjectCreated:*'],
        'isEnabled': True,
        'name': 'test-rule',
        'objectNamePrefix': '',
        'targetConfiguration': {
            'customHeaders': None,
            'hmacSha256SigningSecret': None,
            'targetType': 'webhook',
            'url': 'https://example.com/webhook',
        },
    }
    # add rule
    created_rule = b2_tool.should_succeed_json(
        [
            'bucket',
            'notification-rule',
            'create',
            '--json',
            f'b2://{bucket_name}',
            'test-rule',
            '--webhook-url',
            'https://example.com/webhook',
            '--event-type',
            'b2:ObjectCreated:*',
        ]
    )
    expected_rules = [{**notification_rule, 'isSuspended': False, 'suspensionReason': ''}]
    assert_dict_equal_ignore_extra(created_rule, expected_rules[0])

    # modify rule
    secret = '0testSecret000000000000000000032'
    modified_rule = b2_tool.should_succeed_json(
        [
            'bucket',
            'notification-rule',
            'update',
            '--json',
            f'b2://{bucket_name}/prefix',
            'test-rule',
            '--disable',
            '--sign-secret',
            secret,
        ]
    )
    expected_rules[0].update({'objectNamePrefix': 'prefix', 'isEnabled': False})
    expected_rules[0]['targetConfiguration']['hmacSha256SigningSecret'] = secret
    assert_dict_equal_ignore_extra(modified_rule, expected_rules[0])

    # read updated rules
    assert_dict_equal_ignore_extra(
        b2_tool.should_succeed_json(
            ['bucket', 'notification-rule', 'list', f'b2://{bucket_name}', '--json']
        ),
        expected_rules,
    )

    # delete rule by name
    assert (
        b2_tool.should_succeed(
            ['bucket', 'notification-rule', 'delete', f'b2://{bucket_name}', 'test-rule']
        )
        == f"Rule 'test-rule' has been deleted from b2://{bucket_name}/\n"
    )
    assert (
        b2_tool.should_succeed_json(
            ['bucket', 'notification-rule', 'list', f'b2://{bucket_name}', '--json']
        )
        == []
    )
