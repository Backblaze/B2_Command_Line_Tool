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

import argparse
import base64
import hashlib
import json
import logging
import os
import os.path
import platform
import random
import re
import shutil
import string
import subprocess
import sys
import tempfile
import threading

import pytest
from typing import Optional

from b2.console_tool import current_time_millis
from b2sdk.v2 import (
    B2Api,
    Bucket,
    EncryptionAlgorithm,
    EncryptionMode,
    EncryptionSetting,
    EncryptionKey,
    FileRetentionSetting,
    fix_windows_path_limit,
    InMemoryAccountInfo,
    InMemoryCache,
    LegalHold,
    NO_RETENTION_FILE_SETTING,
    RetentionMode,
    SSE_C_KEY_ID_FILE_INFO_KEY_NAME,
    SqliteAccountInfo,
    UNKNOWN_FILE_RETENTION_SETTING,
)

from b2sdk.v2.exception import BucketIdNotFound, FileNotPresent

SSE_NONE = EncryptionSetting(mode=EncryptionMode.NONE,)
SSE_B2_AES = EncryptionSetting(
    mode=EncryptionMode.SSE_B2,
    algorithm=EncryptionAlgorithm.AES256,
)
SSE_C_AES = EncryptionSetting(
    mode=EncryptionMode.SSE_C,
    algorithm=EncryptionAlgorithm.AES256,
    key=EncryptionKey(secret=os.urandom(32), key_id='user-generated-key-id')
)
SSE_C_AES_2 = EncryptionSetting(
    mode=EncryptionMode.SSE_C,
    algorithm=EncryptionAlgorithm.AES256,
    key=EncryptionKey(secret=os.urandom(32), key_id='another-user-generated-key-id')
)

ONE_HOUR_MILLIS = 60 * 60 * 1000
ONE_DAY_MILLIS = ONE_HOUR_MILLIS * 24
BUCKET_CREATED_AT_MILLIS = 'created_at_millis'

BUCKET_NAME_CHARS = string.ascii_letters + string.digits + '-'
BUCKET_NAME_LENGTH = 50


def bucket_name_part(length):
    return ''.join(random.choice(BUCKET_NAME_CHARS) for _ in range(length))


def parse_args(tests):
    parser = argparse.ArgumentParser(
        prog='test_b2_comand_line.py',
        description='This program tests the B2 command-line client.',
    )
    parser.add_argument(
        'tests',
        help='Specifie which of the tests to run. If not specified, all test will run',
        default='all',
        nargs='*',
        choices=['all'] + tests
    )
    parser.add_argument(
        '--command',
        help='Specifie a command tu run. If not specified, the tests will run from the source',
        default='%s -m b2' % sys.executable
    )

    args = parser.parse_args()
    if 'all' in args.tests:
        args.tests = tests

    return args


def error_and_exit(message):
    print('ERROR:', message)
    _exit(1)


def read_file(path):
    with open(path, 'rb') as f:
        return f.read()


def write_file(path, contents):
    with open(path, 'wb') as f:
        f.write(contents)


def file_mod_time_millis(path):
    return int(os.path.getmtime(path) * 1000)


def set_file_mod_time_millis(path, time):
    os.utime(path, (os.path.getatime(path), time / 1000))


def random_hex(length):
    return ''.join(random.choice('0123456789abcdef') for _ in range(length))


class TempDir(object):
    def __init__(self):
        self.dirpath = None

    def get_dir(self):
        return self.dirpath

    def __enter__(self):
        self.dirpath = tempfile.mkdtemp()
        return self.dirpath

    def __exit__(self, exc_type, exc_val, exc_tb):
        shutil.rmtree(fix_windows_path_limit(self.dirpath))


class StringReader(object):
    def __init__(self):
        self.string = None

    def get_string(self):
        return self.string

    def read_from(self, f):
        try:
            self.string = f.read()
        except Exception as e:
            print(e)
            self.string = str(e)


def remove_warnings(text):
    return os.linesep.join(
        line for line in text.split(os.linesep) if 'DeprecationWarning' not in line
    )


def run_command(cmd, args, additional_env: Optional[dict] = None):
    """
    :param cmd: a command to run
    :param args: command's arguments
    :param additional_env: environment variables to pass to the command, overwriting parent process ones
    :return: (status, stdout, stderr)
    """
    # We'll run the b2 command-line by running the b2 module from
    # the current directory or provided as parameter
    os.environ['PYTHONPATH'] = '.'
    os.environ['PYTHONIOENCODING'] = 'utf-8'
    command = cmd.split(' ')
    command.extend(args)

    print('Running:', ' '.join(command))

    stdout = StringReader()
    stderr = StringReader()

    env = os.environ.copy()
    env.update(additional_env or {})

    p = subprocess.Popen(
        command,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        close_fds=platform.system() != 'Windows',
        env=env,
    )
    p.stdin.close()
    reader1 = threading.Thread(target=stdout.read_from, args=[p.stdout])
    reader1.start()
    reader2 = threading.Thread(target=stderr.read_from, args=[p.stderr])
    reader2.start()
    p.wait()
    reader1.join()
    reader2.join()

    stdout_decoded = remove_warnings(stdout.get_string().decode('utf-8'))
    stderr_decoded = remove_warnings(stderr.get_string().decode('utf-8'))

    print_output(p.returncode, stdout_decoded, stderr_decoded)
    return p.returncode, stdout_decoded, stderr_decoded


def print_text_indented(text):
    """
    Prints text that may include weird characters, indented four spaces.
    """
    for line in text.split(os.linesep):
        print('   ', repr(line)[1:-1])


def print_json_indented(value):
    """
    Converts the value to JSON, then prints it.
    """
    print_text_indented(json.dumps(value, indent=4, sort_keys=True, default=serialize_enc_settings))


def serialize_enc_settings(value):
    if not isinstance(value, EncryptionSetting):
        raise TypeError
    return value.as_dict()


def print_output(status, stdout, stderr):
    print('  status:', status)
    if stdout != '':
        print('  stdout:')
        print_text_indented(stdout)
    if stderr != '':
        print('  stderr:')
        print_text_indented(stderr)
    print()


class Api:
    def __init__(
        self, account_id, application_key, realm, general_bucket_name_prefix,
        this_run_bucket_name_prefix
    ):
        self.account_id = account_id
        self.application_key = application_key
        self.realm = realm
        self.general_bucket_name_prefix = general_bucket_name_prefix
        self.this_run_bucket_name_prefix = this_run_bucket_name_prefix

        info = InMemoryAccountInfo()
        cache = InMemoryCache()
        self.api = B2Api(info, cache=cache)
        self.api.authorize_account(self.realm, self.account_id, self.application_key)

    def create_bucket(self):
        bucket_name = self.this_run_bucket_name_prefix + bucket_name_part(
            BUCKET_NAME_LENGTH - len(self.this_run_bucket_name_prefix)
        )
        print('Creating bucket:', bucket_name)
        self.api.create_bucket(
            bucket_name, 'allPublic', bucket_info={'created_at_millis': str(current_time_millis())}
        )
        print()
        return bucket_name

    def _should_remove_bucket(self, bucket: Bucket):
        if bucket.name.startswith(self.this_run_bucket_name_prefix):
            return True
        if bucket.name.startswith(self.general_bucket_name_prefix):
            if BUCKET_CREATED_AT_MILLIS in bucket.bucket_info:
                if int(bucket.bucket_info[BUCKET_CREATED_AT_MILLIS]
                      ) < current_time_millis() - ONE_HOUR_MILLIS:
                    return True
        return False

    def clean_buckets(self):
        buckets = self.api.list_buckets()
        for bucket in buckets:
            if not self._should_remove_bucket(bucket):
                print('Skipping bucket removal:', bucket.name)
            else:
                print('Trying to remove bucket:', bucket.name)
                files_leftover = False
                file_versions = bucket.ls(latest_only=False, recursive=True)
                for file_version_info, _ in file_versions:
                    if file_version_info.file_retention:
                        if file_version_info.file_retention.mode == RetentionMode.GOVERNANCE:
                            print('Removing retention from file version:', file_version_info.id_)
                            self.api.update_file_retention(
                                file_version_info.id_, file_version_info.file_name,
                                NO_RETENTION_FILE_SETTING, True
                            )
                        elif file_version_info.file_retention.mode == RetentionMode.COMPLIANCE:
                            if file_version_info.file_retention.retain_until > current_time_millis():  # yapf: disable
                                print(
                                    'File version: %s cannot be removed due to compliance mode retention'
                                    % (file_version_info.id_,)
                                )
                                files_leftover = True
                                continue
                        elif file_version_info.file_retention.mode == RetentionMode.NONE:
                            pass
                        else:
                            raise ValueError(
                                'Unknown retention mode: %s' %
                                (file_version_info.file_retention.mode,)
                            )
                    if file_version_info.legal_hold.is_on():
                        print('Removing legal hold from file version:', file_version_info.id_)
                        self.api.update_file_legal_hold(
                            file_version_info.id_, file_version_info.file_name, LegalHold.OFF
                        )
                    print('Removing file version:', file_version_info.id_)
                    try:
                        self.api.delete_file_version(
                            file_version_info.id_, file_version_info.file_name
                        )
                    except FileNotPresent:
                        print(
                            'It seems that file version %s has already been removed' %
                            (file_version_info.id_,)
                        )

                if files_leftover:
                    print('Unable to remove bucket because some retained files remain')
                else:
                    print('Removing bucket:', bucket.name)
                    try:
                        self.api.delete_bucket(bucket)
                    except BucketIdNotFound:
                        print('It seems that bucket %s has already been removed' % (bucket.name,))
                print()


class EnvVarTestContext:
    """
    Establish config for environment variable test.
    Copy the B2 credential file and rename the existing copy
    """
    ENV_VAR = 'B2_ACCOUNT_INFO'

    def __init__(self, account_info_file_name: str):
        self.account_info_file_name = account_info_file_name

    def __enter__(self):
        src = self.account_info_file_name
        dst = os.path.join(tempfile.gettempdir(), 'b2_account_info')
        shutil.copyfile(src, dst)
        shutil.move(src, src + '.bkup')
        os.environ[self.ENV_VAR] = dst
        return dst

    def __exit__(self, exc_type, exc_val, exc_tb):
        os.remove(os.environ.get(self.ENV_VAR))
        fname = self.account_info_file_name
        shutil.move(fname + '.bkup', fname)
        if os.environ.get(self.ENV_VAR) is not None:
            del os.environ[self.ENV_VAR]


class CommandLine:

    EXPECTED_STDERR_PATTERNS = [
        re.compile(r'.*B/s]$', re.DOTALL),  # progress bar
        re.compile(r'^$'),  # empty line
        re.compile(
            r'Encrypting file\(s\) with SSE-C without providing key id. '
            r'Set B2_DESTINATION_SSE_C_KEY_ID to allow key identification'
        ),
    ]

    def __init__(self, command, account_id, application_key, realm, bucket_name_prefix):
        self.command = command
        self.account_id = account_id
        self.application_key = application_key
        self.realm = realm
        self.bucket_name_prefix = bucket_name_prefix
        self.env_var_test_context = EnvVarTestContext(SqliteAccountInfo().filename)
        self.account_info_file_name = SqliteAccountInfo().filename

    def generate_bucket_name(self):
        return self.bucket_name_prefix + bucket_name_part(
            BUCKET_NAME_LENGTH - len(self.bucket_name_prefix)
        )

    def run_command(self, args, additional_env: Optional[dict] = None):
        """
        Runs the command with the given arguments, returns a tuple in form of
        (succeeded, stdout)
        """
        status, stdout, stderr = run_command(self.command, args, additional_env)
        return status == 0 and stderr == '', stdout

    def should_succeed(self, args, expected_pattern=None, additional_env: Optional[dict] = None):
        """
        Runs the command-line with the given arguments.  Raises an exception
        if there was an error; otherwise, returns the stdout of the command
        as as string.
        """
        status, stdout, stderr = run_command(self.command, args, additional_env)
        if status != 0:
            print('FAILED with status', status)
            _exit(1)
        if stderr != '':
            failed = False
            for line in (s.strip() for s in stderr.split(os.linesep)):
                if not any(p.match(line) for p in self.EXPECTED_STDERR_PATTERNS):
                    print('Unexpected stderr line:', repr(line))
                    failed = True
            if failed:
                print('FAILED because of stderr')
                print(stderr)
                _exit(1)
        if expected_pattern is not None:
            if re.search(expected_pattern, stdout) is None:
                print('STDOUT:')
                print(stdout)
                error_and_exit('did not match pattern: ' + expected_pattern)
        return stdout

    def should_succeed_json(self, args, additional_env: Optional[dict] = None):
        """
        Runs the command-line with the given arguments.  Raises an exception
        if there was an error; otherwise, treats the stdout as JSON and returns
        the data in it.
        """
        return json.loads(self.should_succeed(args, additional_env=additional_env))

    def should_fail(self, args, expected_pattern, additional_env: Optional[dict] = None):
        """
        Runs the command-line with the given args, expecting the given pattern
        to appear in stderr.
        """
        status, stdout, stderr = run_command(self.command, args, additional_env)
        if status == 0:
            print('ERROR: should have failed')
            _exit(1)
        if re.search(expected_pattern, stdout + stderr) is None:
            print(expected_pattern)
            # quotes are helpful when reading fail logs, they help find trailing white spaces etc.
            print("'%s'" % (stdout + stderr,))
            error_and_exit('did not match pattern: ' + str(expected_pattern))

    def reauthorize(self):
        """Clear and authorize again to the account."""
        self.should_succeed(['clear-account'])
        self.should_succeed(
            [
                'authorize-account', '--environment', self.realm, self.account_id,
                self.application_key
            ]
        )

    def list_file_versions(self, bucket_name):
        return self.should_succeed_json(['ls', '--json', '--recursive', '--versions', bucket_name])


def should_equal(expected, actual):
    print('  expected:')
    print_json_indented(expected)
    print('  actual:')
    print_json_indented(actual)
    if expected != actual:
        print('  ERROR')
        _exit(1)
    print()


def _exit(error_code):
    logging.shutdown()
    sys.stdout.flush()
    sys.stderr.flush()
    sys.exit(error_code)


def download_test(b2_tool, bucket_name):

    file_to_upload = 'README.md'

    uploaded_a = b2_tool.should_succeed_json(
        ['upload-file', '--noProgress', '--quiet', bucket_name, file_to_upload, 'a']
    )
    with TempDir() as dir_path:
        p = lambda fname: os.path.join(dir_path, fname)
        b2_tool.should_succeed(['download-file-by-name', '--noProgress', bucket_name, 'a', p('a')])
        assert read_file(p('a')) == read_file(file_to_upload)
        b2_tool.should_succeed(
            ['download-file-by-id', '--noProgress', uploaded_a['fileId'],
             p('b')]
        )
        assert read_file(p('b')) == read_file(file_to_upload)

    # there is just one file, so clean after itself for faster execution
    b2_tool.should_succeed(['delete-file-version', uploaded_a['fileName'], uploaded_a['fileId']])
    b2_tool.should_succeed(['delete-bucket', bucket_name])


def basic_test(b2_tool, bucket_name):

    file_to_upload = 'README.md'
    file_mod_time_str = str(file_mod_time_millis(file_to_upload))

    hex_sha1 = hashlib.sha1(read_file(file_to_upload)).hexdigest()

    list_of_buckets = b2_tool.should_succeed_json(['list-buckets', '--json'])
    should_equal(
        [bucket_name], [b['bucketName'] for b in list_of_buckets if b['bucketName'] == bucket_name]
    )

    b2_tool.should_succeed(
        ['upload-file', '--noProgress', '--quiet', bucket_name, file_to_upload, 'a']
    )
    b2_tool.should_succeed(['upload-file', '--noProgress', bucket_name, file_to_upload, 'a'])
    b2_tool.should_succeed(['upload-file', '--noProgress', bucket_name, file_to_upload, 'b/1'])
    b2_tool.should_succeed(['upload-file', '--noProgress', bucket_name, file_to_upload, 'b/2'])
    b2_tool.should_succeed(
        [
            'upload-file', '--noProgress', '--sha1', hex_sha1, '--info', 'foo=bar=baz', '--info',
            'color=blue', bucket_name, file_to_upload, 'c'
        ]
    )
    b2_tool.should_fail(
        [
            'upload-file', '--noProgress', '--sha1', hex_sha1, '--info', 'foo-bar', '--info',
            'color=blue', bucket_name, file_to_upload, 'c'
        ], r'ERROR: Bad file info: foo-bar'
    )
    b2_tool.should_succeed(
        [
            'upload-file', '--noProgress', '--contentType', 'text/plain', bucket_name,
            file_to_upload, 'd'
        ]
    )

    with TempDir() as dir_path:
        b2_tool.should_succeed(
            [
                'download-file-by-name', '--noProgress', bucket_name, 'b/1',
                os.path.join(dir_path, 'a')
            ]
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
    b2_tool.should_succeed(
        ['ls', '--long', bucket_name],
        '^4_z.*upload.*a{0}.*-.*b/{0}4_z.*upload.*d{0}'.format(os.linesep)
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
        ['make-friendly-url', bucket_name, file_to_upload],
        '^https://.*/file/%s/%s\r?$' % (
            bucket_name,
            file_to_upload,
        ),
    )  # \r? is for Windows, as $ doesn't match \r\n
    to_be_removed_bucket_name = b2_tool.generate_bucket_name()
    b2_tool.should_succeed(['create-bucket', to_be_removed_bucket_name, 'allPublic'],)
    b2_tool.should_succeed(['delete-bucket', to_be_removed_bucket_name],)
    b2_tool.should_fail(
        ['delete-bucket', to_be_removed_bucket_name],
        re.compile(r'^ERROR: Bucket with id=\w* not found\s*$')
    )
    # Check logging settings
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
    with open('b2_cli.log', 'r') as logfile:
        log = logfile.read()
        assert re.search(log_file_regex, log), log
    os.remove('b2_cli.log')

    b2_tool.should_fail(['delete-bucket', to_be_removed_bucket_name, '--verbose'], stderr_regex)
    assert not os.path.exists('b2_cli.log')

    b2_tool.should_fail(
        ['delete-bucket', to_be_removed_bucket_name, '--verbose', '--debugLogs'], stderr_regex
    )
    with open('b2_cli.log', 'r') as logfile:
        log = logfile.read()
        assert re.search(log_file_regex, log), log


def key_restrictions_test(b2_tool, bucket_name):

    second_bucket_name = b2_tool.generate_bucket_name()
    b2_tool.should_succeed(['create-bucket', second_bucket_name, 'allPublic'],)

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

    failed_bucket_err = r'ERROR: Application key is restricted to bucket: ' + bucket_name
    b2_tool.should_fail(['get-bucket', second_bucket_name], failed_bucket_err)

    failed_list_files_err = r'ERROR: Application key is restricted to bucket: ' + bucket_name
    b2_tool.should_fail(['ls', second_bucket_name], failed_list_files_err)

    # reauthorize with more capabilities for clean up
    b2_tool.should_succeed(
        [
            'authorize-account', '--environment', b2_tool.realm, b2_tool.account_id,
            b2_tool.application_key
        ]
    )
    b2_tool.should_succeed(['delete-bucket', second_bucket_name])
    b2_tool.should_succeed(['delete-key', key_one_id])
    b2_tool.should_succeed(['delete-key', key_two_id])


def account_test(b2_tool, bucket_name):
    # actually a high level operations test - we run bucket tests here since this test doesn't use it
    b2_tool.should_succeed(['delete-bucket', bucket_name])
    new_bucket_name = b2_tool.generate_bucket_name()
    # apparently server behaves erratically when we delete a bucket and recreate it right away
    b2_tool.should_succeed(['create-bucket', new_bucket_name, 'allPrivate'])
    b2_tool.should_succeed(['update-bucket', new_bucket_name, 'allPublic'])

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
        b2_tool.should_succeed(['create-bucket', bucket_name, 'allPrivate'])
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
    assert False, 'file not found: %s' % (file_name,)


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
        encryption += '?%s=%s' % (SSE_C_KEY_ID_FILE_INFO_KEY_NAME, sse_c_key_id)

    return encryption


def sync_up_test(b2_tool, bucket_name):
    sync_up_helper(b2_tool, bucket_name, 'sync')


def sync_up_sse_b2_test(b2_tool, bucket_name):
    sync_up_helper(b2_tool, bucket_name, 'sync', encryption=SSE_B2_AES)


def sync_up_sse_c_test(b2_tool, bucket_name):
    sync_up_helper(b2_tool, bucket_name, 'sync', encryption=SSE_C_AES)


def sync_up_test_no_prefix(b2_tool, bucket_name):
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

        p = lambda fname: os.path.join(dir_path, fname)

        file_versions = b2_tool.list_file_versions(bucket_name)
        should_equal([], file_version_summary(file_versions))

        write_file(p('a'), b'hello')
        write_file(p('b'), b'hello')
        write_file(p('c'), b'hello')

        # simulate action (nothing should be uploaded)
        b2_tool.should_succeed(['sync', '--noProgress', '--dryRun', dir_path, b2_sync_point])
        file_versions = b2_tool.list_file_versions(bucket_name)
        should_equal([], file_version_summary(file_versions))

        os.symlink('broken', p('d'))

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
        should_equal(file_mod_time_millis(p('c')), int(file_info['src_last_modified_millis']))

        os.unlink(p('b'))
        write_file(p('c'), b'hello world')

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

        os.unlink(p('a'))

        b2_tool.should_succeed(['sync', '--noProgress', '--delete', dir_path, b2_sync_point])
        file_versions = b2_tool.list_file_versions(bucket_name)
        should_equal([
            '+ ' + prefix + 'c',
        ], file_version_summary(file_versions))

        #test --compareThreshold with file size
        write_file(p('c'), b'hello world!')

        #should not upload new version of c
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

        #should upload new version of c
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

        set_file_mod_time_millis(p('c'), file_mod_time_millis(p('c')) + 2000)

        #test --compareThreshold with modTime
        #should not upload new version of c
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

        #should upload new version of c
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
        write_file(p('linktarget'), b'hello')
        mod_time = str((file_mod_time_millis(p('linktarget')) - 10) / 1000)

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
        os.symlink('linktarget', p('alink'))

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


def sync_down_test(b2_tool, bucket_name):
    sync_down_helper(b2_tool, bucket_name, 'sync')


def sync_down_test_no_prefix(b2_tool, bucket_name):
    sync_down_helper(b2_tool, bucket_name, '')


def sync_down_sse_c_test_no_prefix(b2_tool, bucket_name):
    sync_down_helper(b2_tool, bucket_name, '', SSE_C_AES)


def sync_down_helper(b2_tool, bucket_name, folder_in_bucket, encryption=None):

    file_to_upload = 'README.md'

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
        should_equal([], sorted(os.listdir(local_path)))

        # Put a couple files in B2
        b2_tool.should_succeed(
            ['upload-file', '--noProgress', bucket_name, file_to_upload, b2_file_prefix + 'a'] +
            upload_encryption_args,
            additional_env=upload_additional_env,
        )
        b2_tool.should_succeed(
            ['upload-file', '--noProgress', bucket_name, file_to_upload, b2_file_prefix + 'b'] +
            upload_encryption_args,
            additional_env=upload_additional_env,
        )
        b2_tool.should_succeed(
            ['sync', b2_sync_point, local_path] + sync_encryption_args,
            additional_env=sync_additional_env,
        )
        should_equal(['a', 'b'], sorted(os.listdir(local_path)))

        b2_tool.should_succeed(
            ['upload-file', '--noProgress', bucket_name, file_to_upload, b2_file_prefix + 'c'] +
            upload_encryption_args,
            additional_env=upload_additional_env,
        )

        # Sync the files with one file being excluded because of mtime
        mod_time = str((file_mod_time_millis(file_to_upload) - 10) / 1000)
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


def sync_copy_test(b2_tool, bucket_name):
    prepare_and_run_sync_copy_tests(b2_tool, bucket_name, 'sync')


def sync_copy_test_no_prefix_default_encryption(b2_tool, bucket_name):
    prepare_and_run_sync_copy_tests(
        b2_tool, bucket_name, '', destination_encryption=None, expected_encryption=SSE_NONE
    )


def sync_copy_test_no_prefix_no_encryption(b2_tool, bucket_name):
    prepare_and_run_sync_copy_tests(
        b2_tool, bucket_name, '', destination_encryption=SSE_NONE, expected_encryption=SSE_NONE
    )


def sync_copy_test_no_prefix_sse_b2(b2_tool, bucket_name):
    prepare_and_run_sync_copy_tests(
        b2_tool,
        bucket_name,
        '',
        destination_encryption=SSE_B2_AES,
        expected_encryption=SSE_B2_AES,
    )


def sync_copy_test_no_prefix_sse_c(b2_tool, bucket_name):
    prepare_and_run_sync_copy_tests(
        b2_tool,
        bucket_name,
        '',
        destination_encryption=SSE_C_AES,
        expected_encryption=SSE_C_AES,
        source_encryption=SSE_C_AES_2,
    )


def sync_copy_test_sse_c_single_bucket(b2_tool, bucket_name):
    run_sync_copy_with_basic_checks(
        b2_tool=b2_tool,
        b2_file_prefix='first_folder/',
        b2_sync_point='b2:%s/%s' % (bucket_name, 'first_folder'),
        bucket_name=bucket_name,
        other_b2_sync_point='b2:%s/%s' % (bucket_name, 'second_folder'),
        destination_encryption=SSE_C_AES_2,
        source_encryption=SSE_C_AES,
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
    b2_tool,
    bucket_name,
    folder_in_bucket,
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

    other_bucket_name = b2_tool.generate_bucket_name()
    success, _ = b2_tool.run_command(['create-bucket', other_bucket_name, 'allPublic'])

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
):
    file_to_upload = 'README.md'

    # Put a couple files in B2
    if source_encryption is None or source_encryption.mode in (
        EncryptionMode.NONE, EncryptionMode.SSE_B2
    ):
        b2_tool.should_succeed(
            [
                'upload-file', '--noProgress', '--destinationServerSideEncryption', 'SSE-B2',
                bucket_name, file_to_upload, b2_file_prefix + 'a'
            ]
        )
        b2_tool.should_succeed(
            ['upload-file', '--noProgress', bucket_name, file_to_upload, b2_file_prefix + 'b']
        )
    elif source_encryption.mode == EncryptionMode.SSE_C:
        for suffix in ['a', 'b']:
            b2_tool.should_succeed(
                [
                    'upload-file', '--noProgress', '--destinationServerSideEncryption', 'SSE-C',
                    bucket_name, file_to_upload, b2_file_prefix + suffix
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


def sync_long_path_test(b2_tool, bucket_name):
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
        local_long_path = os.path.normpath(os.path.join(dir_path, long_path))
        fixed_local_long_path = fix_windows_path_limit(local_long_path)
        os.makedirs(os.path.dirname(fixed_local_long_path))
        write_file(fixed_local_long_path, b'asdf')

        b2_tool.should_succeed(['sync', '--noProgress', '--delete', dir_path, b2_sync_point])
        file_versions = b2_tool.list_file_versions(bucket_name)
        should_equal(['+ ' + long_path], file_version_summary(file_versions))


def default_sse_b2_test(b2_tool, bucket_name):
    # Set default encryption via update-bucket
    bucket_info = b2_tool.should_succeed_json(['get-bucket', bucket_name])
    bucket_default_sse = {'mode': 'none'}
    should_equal(bucket_default_sse, bucket_info['defaultServerSideEncryption'])

    bucket_info = b2_tool.should_succeed_json(
        ['update-bucket', '--defaultServerSideEncryption=SSE-B2', bucket_name, 'allPublic']
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

    # Set default encryption via create-bucket
    second_bucket_name = b2_tool.generate_bucket_name()
    b2_tool.should_succeed(
        ['create-bucket', '--defaultServerSideEncryption=SSE-B2', second_bucket_name, 'allPublic']
    )
    second_bucket_info = b2_tool.should_succeed_json(['get-bucket', second_bucket_name])
    second_bucket_default_sse = {
        'algorithm': 'AES256',
        'mode': 'SSE-B2',
    }
    should_equal(second_bucket_default_sse, second_bucket_info['defaultServerSideEncryption'])


def sse_b2_test(b2_tool, bucket_name):
    file_to_upload = 'README.md'

    b2_tool.should_succeed(
        [
            'upload-file', '--destinationServerSideEncryption=SSE-B2', '--noProgress', '--quiet',
            bucket_name, file_to_upload, 'encrypted'
        ]
    )
    b2_tool.should_succeed(
        ['upload-file', '--noProgress', '--quiet', bucket_name, file_to_upload, 'not_encrypted']
    )
    with TempDir() as dir_path:
        p = lambda fname: os.path.join(dir_path, fname)
        b2_tool.should_succeed(
            ['download-file-by-name', '--noProgress', bucket_name, 'encrypted',
             p('encrypted')]
        )
        b2_tool.should_succeed(
            [
                'download-file-by-name', '--noProgress', bucket_name, 'not_encrypted',
                p('not_encypted')
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


def sse_c_test(b2_tool, bucket_name):

    file_to_upload = 'README.md'
    secret = os.urandom(32)

    b2_tool.should_fail(
        [
            'upload-file', '--noProgress', '--quiet', '--destinationServerSideEncryption', 'SSE-C',
            bucket_name, file_to_upload, 'gonna-fail-anyway'
        ],
        'Using SSE-C requires providing an encryption key via B2_DESTINATION_SSE_C_KEY_B64 env var'
    )
    file_version_info = b2_tool.should_succeed_json(
        [
            'upload-file', '--noProgress', '--quiet', '--destinationServerSideEncryption', 'SSE-C',
            bucket_name, file_to_upload, 'uploaded_encrypted'
        ],
        additional_env={
            'B2_DESTINATION_SSE_C_KEY_B64': base64.b64encode(secret).decode(),
            'B2_DESTINATION_SSE_C_KEY_ID': 'user-generated-key-id \nąóźćż\nœøΩ≈ç\nßäöü',
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
    should_equal(
        'user-generated-key-id \nąóźćż\nœøΩ≈ç\nßäöü',
        file_version_info['fileInfo'][SSE_C_KEY_ID_FILE_INFO_KEY_NAME]
    )

    b2_tool.should_fail(
        [
            'download-file-by-name', '--noProgress', bucket_name, 'uploaded_encrypted',
            'gonna_fail_anyway'
        ],
        expected_pattern='ERROR: The object was stored using a form of Server Side Encryption. The '
        r'correct parameters must be provided to retrieve the object. \(bad_request\)'
    )
    b2_tool.should_fail(
        [
            'download-file-by-name', '--noProgress', '--sourceServerSideEncryption', 'SSE-C',
            bucket_name, 'uploaded_encrypted', 'gonna_fail_anyway'
        ],
        expected_pattern='ValueError: Using SSE-C requires providing an encryption key via '
        'B2_SOURCE_SSE_C_KEY_B64 env var'
    )
    b2_tool.should_fail(
        [
            'download-file-by-name', '--noProgress', '--sourceServerSideEncryption', 'SSE-C',
            bucket_name, 'uploaded_encrypted', 'gonna_fail_anyway'
        ],
        expected_pattern='ERROR: Wrong or no SSE-C key provided when reading a file.',
        additional_env={'B2_SOURCE_SSE_C_KEY_B64': base64.b64encode(os.urandom(32)).decode()}
    )
    with TempDir() as dir_path:
        p = lambda fname: os.path.join(dir_path, fname)
        b2_tool.should_succeed(
            [
                'download-file-by-name', '--noProgress', '--sourceServerSideEncryption', 'SSE-C',
                bucket_name, 'uploaded_encrypted',
                p('a')
            ],
            additional_env={'B2_SOURCE_SSE_C_KEY_B64': base64.b64encode(secret).decode()}
        )
        assert read_file(p('a')) == read_file(file_to_upload)
        b2_tool.should_succeed(
            [
                'download-file-by-id', '--noProgress', '--sourceServerSideEncryption', 'SSE-C',
                file_version_info['fileId'],
                p('b')
            ],
            additional_env={'B2_SOURCE_SSE_C_KEY_B64': base64.b64encode(secret).decode()}
        )
        assert read_file(p('b')) == read_file(file_to_upload)

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
                'sse_c_key_id': 'user-generated-key-id \nąóźćż\nœøΩ≈ç\nßäöü',
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


def file_lock_test(b2_tool, bucket_name):
    lock_disabled_bucket_name = b2_tool.generate_bucket_name()
    b2_tool.should_succeed([
        'create-bucket',
        lock_disabled_bucket_name,
        'allPrivate',
    ],)

    file_to_upload = 'README.md'
    now_millis = current_time_millis()

    not_lockable_file = b2_tool.should_succeed_json(  # file in a lock disabled bucket
        ['upload-file', '--noProgress', '--quiet', lock_disabled_bucket_name, file_to_upload, 'a']
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
            '--noProgress',
            '--quiet',
            lock_disabled_bucket_name,
            file_to_upload,
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
    b2_tool.should_succeed(
        [
            'create-bucket',
            lock_enabled_bucket_name,
            'allPrivate',
            '--fileLockEnabled',
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
        ['upload-file', '--noProgress', '--quiet', lock_enabled_bucket_name, file_to_upload, 'a']
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
            file_to_upload,
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
            file_to_upload,
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

    file_lock_without_perms_test(
        b2_tool, lock_enabled_bucket_name, lock_disabled_bucket_name, lockable_file['fileId'],
        not_lockable_file['fileId']
    )


def file_lock_without_perms_test(
    b2_tool, lock_enabled_bucket_name, lock_disabled_bucket_name, lockable_file_id,
    not_lockable_file_id
):
    key_name = 'no-perms-for-file-lock' + random_hex(6)
    created_key_stdout = b2_tool.should_succeed(
        [
            'create-key',
            key_name,
            'listFiles,listBuckets,readFiles,writeKeys',
        ]
    )
    key_one_id, key_one = created_key_stdout.split()

    b2_tool.should_succeed(
        ['authorize-account', '--environment', b2_tool.realm, key_one_id, key_one],
    )

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
            'README.md',
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
            'README.md',
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


def _assert_file_lock_configuration(
    b2_tool,
    file_id,
    retention_mode: Optional['RetentionMode'] = None,
    retain_until: Optional[int] = None,
    legal_hold: Optional[LegalHold] = None
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


def main(realm, general_bucket_name_prefix, this_run_bucket_name_prefix):
    test_map = {  # yapf: disable
        'account': account_test,
        'basic': basic_test,
        'file_lock': file_lock_test,
        'keys': key_restrictions_test,
        'sync_down': sync_down_test,
        'sync_down_sse_c': sync_down_sse_c_test_no_prefix,
        'sync_down_no_prefix': sync_down_test_no_prefix,
        'sync_up': sync_up_test,
        'sync_up_sse_b2': sync_up_sse_b2_test,
        'sync_up_sse_c': sync_up_sse_c_test,
        'sync_up_no_prefix': sync_up_test_no_prefix,
        'sync_long_path': sync_long_path_test,
        'sync_copy': sync_copy_test,
        'sync_copy_test_no_prefix_default_encryption': sync_copy_test_no_prefix_default_encryption,
        # 'sync_copy_test_no_prefix_no_encryption': sync_copy_test_no_prefix_no_encryption, # not supported by the server
        'sync_copy_test_no_prefix_sse_b2': sync_copy_test_no_prefix_sse_b2,
        'sync_copy_test_no_prefix_sse_c': sync_copy_test_no_prefix_sse_c,
        'sync_copy_test_sse_c_single_bucket': sync_copy_test_sse_c_single_bucket,
        'download': download_test,
        'default_sse_b2': default_sse_b2_test,
        'sse_b2': sse_b2_test,
        'sse_c': sse_c_test,
    }

    args = parse_args(tests=sorted(test_map))
    print(args)
    account_id = os.environ.get('B2_TEST_APPLICATION_KEY_ID', '')
    application_key = os.environ.get('B2_TEST_APPLICATION_KEY', '')

    if os.environ.get('B2_ACCOUNT_INFO') is not None:
        del os.environ['B2_ACCOUNT_INFO']

    b2_tool = CommandLine(
        args.command, account_id, application_key, realm, this_run_bucket_name_prefix
    )
    b2_api = Api(
        account_id, application_key, realm, general_bucket_name_prefix, this_run_bucket_name_prefix
    )

    # Run each of the tests in its own empty bucket
    for test_name in args.tests:

        print('#')
        print('# Setup for test:', test_name)
        print('#')
        print()

        b2_api.clean_buckets()
        bucket_name = b2_api.create_bucket()

        print('#')
        print('# Running test:', test_name)
        print('#')
        print()

        b2_tool.reauthorize()  # authorization is common for all tests
        test_fcn = test_map[test_name]
        test_fcn(b2_tool, bucket_name)

        print('#')
        print('# Teardown for test:', test_name)
        print('#')
        print()

        b2_api.clean_buckets()

    print()
    print("ALL OK")


def cleanup_hook(
    application_key_id, application_key, realm, general_bucket_name_prefix,
    this_run_bucket_name_prefix
):
    print()
    print('#')
    print('# Clean up:')
    print('#')
    print()
    b2_api = Api(
        application_key_id, application_key, realm, general_bucket_name_prefix,
        this_run_bucket_name_prefix
    )
    b2_api.clean_buckets()


# TODO: rewrite to multiple tests
def test_integration(sut, cleanup):
    application_key_id = os.environ.get('B2_TEST_APPLICATION_KEY_ID')
    if application_key_id is None:
        pytest.fail('B2_TEST_APPLICATION_KEY_ID is not set.')

    application_key = os.environ.get('B2_TEST_APPLICATION_KEY')
    if application_key is None:
        pytest.fail('B2_TEST_APPLICATION_KEY is not set.')

    print()

    realm = os.environ.get('B2_TEST_ENVIRONMENT', 'production')

    sys.argv = ['test_b2_command_line.py', '--command', sut]
    general_bucket_name_prefix = 'clitst'
    this_run_bucket_name_prefix = general_bucket_name_prefix + bucket_name_part(8)

    try:
        main(realm, general_bucket_name_prefix, this_run_bucket_name_prefix)
    finally:
        if cleanup:
            cleanup_hook(
                application_key_id, application_key, realm, general_bucket_name_prefix,
                this_run_bucket_name_prefix
            )


if __name__ == '__main__':
    general_bucket_name_prefix = 'clitst'
    realm = os.environ.get('B2_TEST_ENVIRONMENT', 'production')
    this_run_bucket_name_prefix = general_bucket_name_prefix + bucket_name_part(8)
    main(realm, general_bucket_name_prefix, this_run_bucket_name_prefix)
