######################################################################
#
# File: test/integration/helpers.py
#
# Copyright 2022 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import json
import os
import platform
import random
import re
import shutil
import string
import subprocess
import sys
import threading

from dataclasses import dataclass
from datetime import datetime
from os import environ, linesep, path
from pathlib import Path
from tempfile import gettempdir, mkdtemp
from typing import List, Optional, Union

import backoff

from b2sdk._v3.exception import BucketIdNotFound as v3BucketIdNotFound
from b2sdk.v2 import ALL_CAPABILITIES, NO_RETENTION_FILE_SETTING, B2Api, Bucket, EncryptionAlgorithm, EncryptionKey, EncryptionMode, EncryptionSetting, InMemoryAccountInfo, InMemoryCache, LegalHold, RetentionMode, SqliteAccountInfo, fix_windows_path_limit
from b2sdk.v2.exception import BucketIdNotFound, DuplicateBucketName, FileNotPresent, TooManyRequests

from b2.console_tool import Command, current_time_millis

BUCKET_CLEANUP_PERIOD_MILLIS = 0
ONE_HOUR_MILLIS = 60 * 60 * 1000
ONE_DAY_MILLIS = ONE_HOUR_MILLIS * 24

BUCKET_NAME_LENGTH = 50
BUCKET_NAME_CHARS = string.ascii_letters + string.digits + '-'
BUCKET_CREATED_AT_MILLIS = 'created_at_millis'

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


def bucket_name_part(length: int) -> str:
    return ''.join(random.choice(BUCKET_NAME_CHARS) for _ in range(length))


@dataclass
class Api:
    account_id: str
    application_key: str
    realm: str
    general_bucket_name_prefix: str
    this_run_bucket_name_prefix: str

    api: B2Api = None

    def __post_init__(self):
        info = InMemoryAccountInfo()
        cache = InMemoryCache()
        self.api = B2Api(info, cache=cache)
        self.api.authorize_account(self.realm, self.account_id, self.application_key)

    def create_bucket(self) -> Bucket:
        for _ in range(10):
            bucket_name = self.this_run_bucket_name_prefix + bucket_name_part(
                BUCKET_NAME_LENGTH - len(self.this_run_bucket_name_prefix)
            )
            print('Creating bucket:', bucket_name)
            try:
                return self.api.create_bucket(
                    bucket_name,
                    'allPublic',
                    bucket_info={BUCKET_CREATED_AT_MILLIS: str(current_time_millis())},
                )
            except DuplicateBucketName:
                pass
            print()

        raise ValueError('Failed to create bucket due to name collision')

    def _should_remove_bucket(self, bucket: Bucket):
        if bucket.name.startswith(self.this_run_bucket_name_prefix):
            return True, 'it is a bucket for this very run'
        OLD_PATTERN = 'test-b2-cli-'
        if bucket.name.startswith(self.general_bucket_name_prefix) or bucket.name.startswith(OLD_PATTERN):  # yapf: disable
            if BUCKET_CREATED_AT_MILLIS in bucket.bucket_info:
                delete_older_than = current_time_millis() - BUCKET_CLEANUP_PERIOD_MILLIS
                this_bucket_creation_time = bucket.bucket_info[BUCKET_CREATED_AT_MILLIS]
                if int(this_bucket_creation_time) < delete_older_than:
                    return True, f"this_bucket_creation_time={this_bucket_creation_time} < delete_older_than={delete_older_than}"
            else:
                return True, 'undefined ' + BUCKET_CREATED_AT_MILLIS
        return False, ''

    def clean_buckets(self):
        buckets = self.api.list_buckets()
        print('Total bucket count:', len(buckets))
        for bucket in buckets:
            should_remove, why = self._should_remove_bucket(bucket)
            if not should_remove:
                print(f'Skipping bucket removal: "{bucket.name}"')
                continue

            print('Trying to remove bucket:', bucket.name, 'because', why)
            try:
                self.clean_bucket(bucket)
            except (BucketIdNotFound, v3BucketIdNotFound):
                print('It seems that bucket %s has already been removed' % (bucket.name,))
        buckets = self.api.list_buckets()
        print('Total bucket count after cleanup:', len(buckets))
        for bucket in buckets:
            print(bucket)

    @backoff.on_exception(
        backoff.expo,
        TooManyRequests,
        max_tries=8,
    )
    def clean_bucket(self, bucket: Union[Bucket, str]):
        if isinstance(bucket, str):
            bucket = self.api.get_bucket_by_name(bucket)

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
                            'File version: %s cannot be removed due to compliance mode retention' %
                            (file_version_info.id_,)
                        )
                        files_leftover = True
                        continue
                elif file_version_info.file_retention.mode == RetentionMode.NONE:
                    pass
                else:
                    raise ValueError(
                        'Unknown retention mode: %s' % (file_version_info.file_retention.mode,)
                    )
            if file_version_info.legal_hold.is_on():
                print('Removing legal hold from file version:', file_version_info.id_)
                self.api.update_file_legal_hold(
                    file_version_info.id_, file_version_info.file_name, LegalHold.OFF
                )
            print('Removing file version:', file_version_info.id_)
            try:
                self.api.delete_file_version(file_version_info.id_, file_version_info.file_name)
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

    def count_and_print_buckets(self) -> int:
        buckets = self.api.list_buckets()
        count = len(buckets)
        print(f'Total bucket count at {datetime.now()}: {count}')
        for i, bucket in enumerate(buckets, start=1):
            print(f'- {i}\t{bucket.name} [{bucket.id_}]')
        return count


def print_text_indented(text):
    """
    Prints text that may include weird characters, indented four spaces.
    """
    for line in text.split(linesep):
        Command._print_standard_descriptor(sys.stdout, '   ', repr(line)[1:-1])


def print_output(status, stdout, stderr):
    print('  status:', status)
    if stdout != '':
        print('  stdout:')
        print_text_indented(stdout)
    if stderr != '':
        print('  stderr:')
        print_text_indented(stderr)
    print()


def serialize_enc_settings(value):
    if not isinstance(value, EncryptionSetting):
        raise TypeError
    return value.as_dict()


def print_json_indented(value):
    """
    Converts the value to JSON, then prints it.
    """
    print_text_indented(json.dumps(value, indent=4, sort_keys=True, default=serialize_enc_settings))


def remove_warnings(text):
    return linesep.join(line for line in text.split(linesep) if 'DeprecationWarning' not in line)


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


def run_command(
    cmd: str,
    args: Optional[List[Union[str, Path, int]]] = None,
    additional_env: Optional[dict] = None,
):
    """
    :param cmd: a command to run
    :param args: command's arguments
    :param additional_env: environment variables to pass to the command, overwriting parent process ones
    :return: (status, stdout, stderr)
    """
    # We'll run the b2 command-line by running the b2 module from
    # the current directory or provided as parameter
    environ['PYTHONPATH'] = '.'
    environ['PYTHONIOENCODING'] = 'utf-8'
    command = cmd.split(' ')
    args = [str(arg) for arg in args]
    command.extend(args or [])

    print('Running:', ' '.join(command))

    stdout = StringReader()
    stderr = StringReader()

    env = environ.copy()
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

    stdout_decoded = remove_warnings(stdout.get_string().decode('utf-8', errors='replace'))
    stderr_decoded = remove_warnings(stderr.get_string().decode('utf-8', errors='replace'))

    print_output(p.returncode, stdout_decoded, stderr_decoded)
    return p.returncode, stdout_decoded, stderr_decoded


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
        dst = path.join(gettempdir(), 'b2_account_info')
        shutil.copyfile(src, dst)
        shutil.move(src, src + '.bkup')
        environ[self.ENV_VAR] = dst
        return dst

    def __exit__(self, exc_type, exc_val, exc_tb):
        os.remove(environ.get(self.ENV_VAR))
        fname = self.account_info_file_name
        shutil.move(fname + '.bkup', fname)
        if environ.get(self.ENV_VAR) is not None:
            del environ[self.ENV_VAR]


def should_equal(expected, actual):
    print('  expected:')
    print_json_indented(expected)
    print('  actual:')
    print_json_indented(actual)
    assert expected == actual
    print()


class CommandLine:

    EXPECTED_STDERR_PATTERNS = [
        re.compile(r'.*B/s]$', re.DOTALL),  # progress bar
        re.compile(r'^\r?$'),  # empty line
        re.compile(
            r'Encrypting file\(s\) with SSE-C without providing key id. '
            r'Set B2_DESTINATION_SSE_C_KEY_ID to allow key identification'
        ),
        re.compile(
            r'WARNING: Unable to print unicode.  Encoding for stdout is: '
            r'\'[a-zA-Z0-9]+\''
        ),  # windows-bundle tests on CI use cp1252
        re.compile(r'Trying to print: .*'),
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

    def should_succeed(
        self,
        args: Optional[List[str]],
        expected_pattern: Optional[str] = None,
        additional_env: Optional[dict] = None,
    ) -> str:
        """
        Runs the command-line with the given arguments.  Raises an exception
        if there was an error; otherwise, returns the stdout of the command
        as as string.
        """
        status, stdout, stderr = run_command(self.command, args, additional_env)
        assert status == 0, f'FAILED with status {status}, stderr={stderr}'

        if stderr != '':
            for line in (s.strip() for s in stderr.split(os.linesep)):
                assert any(p.match(line) for p in self.EXPECTED_STDERR_PATTERNS), \
                    f'Unexpected stderr line: {repr(line)}'

        if expected_pattern is not None:
            assert re.search(expected_pattern, stdout), \
            f'did not match pattern="{expected_pattern}", stdout="{stdout}"'

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
        assert status != 0, 'ERROR: should have failed'

        assert re.search(expected_pattern, stdout + stderr), \
            f'did not match pattern="{expected_pattern}", stdout="{stdout}", stderr="{stderr}"'

    def reauthorize(self, check_key_capabilities=False):
        """Clear and authorize again to the account."""
        self.should_succeed(['clear-account'])
        self.should_succeed(
            [
                'authorize-account', '--environment', self.realm, self.account_id,
                self.application_key
            ]
        )
        if check_key_capabilities:
            auth_dict = self.should_succeed_json(['get-account-info'])
            missing_capabilities = set(ALL_CAPABILITIES) - {
                'readBuckets', 'listAllBucketNames'
            } - set(auth_dict['allowed']['capabilities'])
            assert not missing_capabilities, 'it appears that the raw_api integration test is being run with a non-full key. Missing capabilities: %s' % (
                missing_capabilities,
            )

    def list_file_versions(self, bucket_name):
        return self.should_succeed_json(['ls', '--json', '--recursive', '--versions', bucket_name])


class TempDir(object):
    def __init__(self):
        self.dirpath = None

    def get_dir(self):
        assert self.dirpath is not None, \
            "can't call get_dir() before entering the context manager"
        return self.dirpath

    def __enter__(self):
        self.dirpath = mkdtemp()
        return Path(self.dirpath)

    def __exit__(self, exc_type, exc_val, exc_tb):
        shutil.rmtree(fix_windows_path_limit(self.dirpath))


def read_file(path: Union[str, Path]):
    if isinstance(path, Path):
        path = str(path)
    with open(path, 'rb') as f:
        return f.read()


def write_file(path: Union[str, Path], contents: bytes):
    if isinstance(path, Path):
        path = str(path)
    with open(path, 'wb') as f:
        f.write(contents)


def file_mod_time_millis(path: Union[str, Path]):
    if isinstance(path, Path):
        path = str(path)
    return int(os.path.getmtime(path) * 1000)


def set_file_mod_time_millis(path: Union[str, Path], time):
    if isinstance(path, Path):
        path = str(path)
    os.utime(path, (os.path.getatime(path), time / 1000))


def random_hex(length):
    return ''.join(random.choice('0123456789abcdef') for _ in range(length))
