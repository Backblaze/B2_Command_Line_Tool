######################################################################
#
# File: test/integration/helpers.py
#
# Copyright 2022 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import argparse
import contextlib
import dataclasses
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

from datetime import datetime
from os import environ, linesep, path
from pathlib import Path
from tempfile import gettempdir, mkdtemp
from typing import List, Optional, Union

import backoff

from b2sdk._v3.exception import BucketIdNotFound as v3BucketIdNotFound
from b2sdk.v2 import ALL_CAPABILITIES, NO_RETENTION_FILE_SETTING, B2Api, Bucket, EncryptionAlgorithm, EncryptionKey, EncryptionMode, EncryptionSetting, InMemoryAccountInfo, InMemoryCache, LegalHold, RetentionMode, BucketTrackingMixin, SqliteAccountInfo, fix_windows_path_limit
from b2sdk.v2.exception import BucketIdNotFound, DuplicateBucketName, FileNotPresent, TooManyRequests, NonExistentBucket

from b2.console_tool import Command, current_time_millis

ONE_HOUR_MILLIS = 60 * 60 * 1000
ONE_DAY_MILLIS = ONE_HOUR_MILLIS * 24

BUCKET_NAME_LENGTH = 50
BUCKET_NAME_CHARS = string.ascii_letters + string.digits + '-'

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

BUCKET_NAME_PREFIX = 'clitst'
BUCKET_NAME_PREFIX_OLD = 'test-b2-cli-'  # TODO: remove this when sure that there are no more old buckets


def generate_bucket_name():
    suffix_length = BUCKET_NAME_LENGTH - len(BUCKET_NAME_PREFIX)
    suffix = ''.join(random.choice(BUCKET_NAME_CHARS) for _ in range(suffix_length))
    return BUCKET_NAME_PREFIX + suffix


class Api(BucketTrackingMixin, B2Api):
    def __init__(self, account_id, application_key, realm):
        info = InMemoryAccountInfo()
        cache = InMemoryCache()
        super().__init__(info, cache=cache)
        self.authorize_account(realm, account_id, application_key)

    def create_test_bucket(self, bucket_type="allPublic", **kwargs) -> Bucket:
        for _ in range(10):
            bucket_name = generate_bucket_name()
            print('Creating bucket:', bucket_name)
            try:
                return self.create_bucket(bucket_name, bucket_type, **kwargs)
            except DuplicateBucketName:
                pass
            print()

        raise ValueError('Failed to create bucket due to name collision')

    @backoff.on_exception(
        backoff.expo,
        TooManyRequests,
        max_tries=8,
    )
    def clean_bucket(self, bucket: Union[Bucket, str]):
        if isinstance(bucket, str):
            bucket = self.get_bucket_by_name(bucket)

        files_leftover = False
        file_versions = bucket.ls(latest_only=False, recursive=True)

        for file_version_info, _ in file_versions:
            if file_version_info.file_retention:
                if file_version_info.file_retention.mode == RetentionMode.GOVERNANCE:
                    print('Removing retention from file version:', file_version_info.id_)
                    self.update_file_retention(
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
                self.update_file_legal_hold(
                    file_version_info.id_, file_version_info.file_name, LegalHold.OFF
                )
            print('Removing file version:', file_version_info.id_)
            try:
                self.delete_file_version(file_version_info.id_, file_version_info.file_name)
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
                self.delete_bucket(bucket)
            except BucketIdNotFound:
                print('It seems that bucket %s has already been removed' % (bucket.name,))
        print()

    def clean_buckets(self):
        for bucket in self.buckets:
            with contextlib.suppress(BucketIdNotFound, v3BucketIdNotFound, NonExistentBucket):
                self.clean_bucket(bucket)
        self.buckets = []

    def clean_all_buckets(self):
        buckets = self.list_buckets()
        print(f'Total bucket count: {len(buckets)}')

        for bucket in buckets:
            if not bucket.name.startswith((BUCKET_NAME_PREFIX, BUCKET_NAME_PREFIX_OLD)):
                print(f'Skipping bucket removal: "{bucket.name}"')
                continue

            print(f'Removing bucket: "{bucket.name}"')
            try:
                self.clean_bucket(bucket)
            except (BucketIdNotFound, v3BucketIdNotFound):
                print(f'It seems that bucket "{bucket.name}" has already been removed')

        buckets = self.list_buckets()
        print(f'Total bucket count after cleanup: {len(buckets)}')
        for bucket in buckets:
            print(bucket)

    def count_and_print_buckets(self) -> int:
        buckets = self.list_buckets()
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


@dataclasses.dataclass
class CommandResult:
    status: int
    stdout: str
    stderr: str

    @property
    def success(self):
        return self.status == 0

    @property
    def failure(self):
        return not self.success

    @property
    def output(self):
        return self.stdout + self.stderr


def run_command(
    cmd: str,
    args: Optional[List[Union[str, Path, int]]] = None,
    additional_env: Optional[dict] = None,
) -> CommandResult:
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

    return CommandResult(p.returncode, stdout_decoded, stderr_decoded)


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

    def __init__(self, command, account_id, application_key, realm):
        self.command = command
        self.account_id = account_id
        self.application_key = application_key
        self.realm = realm
        self.env_var_test_context = EnvVarTestContext(SqliteAccountInfo().filename)
        self.account_info_file_name = SqliteAccountInfo().filename
        self.buckets = set()

    def run(self, args, additional_env: Optional[dict] = None) -> CommandResult:
        if args:
            if args[0] == 'create-bucket':
                raise ValueError(f'use {type(self).__name__}.create_bucket instead')
            elif args[0] == 'delete-bucket':
                raise ValueError(f'use {type(self).__name__}.delete_bucket instead')

        return run_command(self.command, args, additional_env)

    def create_bucket(
        self, bucket_name, *args, additional_env: Optional[dict] = None
    ) -> CommandResult:
        args = ['create-bucket', bucket_name] + [str(arg) for arg in args]
        result = run_command(self.command, args, additional_env)
        if result.success:
            self.buckets.add(bucket_name)
        return result

    def delete_bucket(
        self, bucket_name, *args, additional_env: Optional[dict] = None
    ) -> CommandResult:
        args = ['delete-bucket', bucket_name] + [str(arg) for arg in args]
        result = run_command(self.command, args, additional_env)
        if result.success:
            self.buckets.discard(bucket_name)
        return result

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
        result = self.run(args, additional_env)
        assert result.success, f'FAILED with status {result.status}, stderr={result.stderr}'

        if result.stderr:
            for line in (s.strip() for s in result.stderr.split(os.linesep)):
                assert any(p.match(line) for p in self.EXPECTED_STDERR_PATTERNS), \
                    f'Unexpected stderr line: {repr(line)}'

        if expected_pattern:
            assert re.search(expected_pattern, result.stdout), \
                f'did not match pattern="{expected_pattern}", stdout="{result.stdout}"'

        return result.stdout

    def should_succeed_json(self, args, additional_env: Optional[dict] = None) -> dict:
        """
        Runs the command-line with the given arguments.  Raises an exception
        if there was an error; otherwise, treats the stdout as JSON and returns
        the data in it.
        """
        return json.loads(self.should_succeed(args, additional_env=additional_env))

    def should_fail(self, args, expected_pattern, additional_env: Optional[dict] = None) -> None:
        """
        Runs the command-line with the given args, expecting the given pattern
        to appear in stderr.
        """
        result = self.run(args, additional_env)
        assert result.failure, 'ERROR: should have failed'

        assert re.search(expected_pattern, result.output), \
            f'did not match pattern="{expected_pattern}", stdout="{result.stdout}", stderr="{result.stderr}"'

    def reauthorize(self, check_key_capabilities=False) -> None:
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
