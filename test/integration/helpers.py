######################################################################
#
# File: test/integration/helpers.py
#
# Copyright 2022 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import json
import logging
import os
import pathlib
import platform
import re
import shutil
import subprocess
import sys
import threading
import warnings
from collections.abc import Iterable
from os import environ, linesep
from pathlib import Path
from tempfile import mkdtemp, mktemp
from typing import TypeVar

from b2sdk.v3 import (
    ALL_CAPABILITIES,
    EncryptionAlgorithm,
    EncryptionKey,
    EncryptionMode,
    EncryptionSetting,
    fix_windows_path_limit,
)
from b2sdk.v3.testing import ONE_HOUR_MILLIS, RNG, BucketManager

from b2._internal.console_tool import Command

logger = logging.getLogger(__name__)

ONE_DAY_MILLIS = ONE_HOUR_MILLIS * 24

SSE_NONE = EncryptionSetting(
    mode=EncryptionMode.NONE,
)
SSE_B2_AES = EncryptionSetting(
    mode=EncryptionMode.SSE_B2,
    algorithm=EncryptionAlgorithm.AES256,
)
_SSE_KEY = RNG.randbytes(32)
SSE_C_AES = EncryptionSetting(
    mode=EncryptionMode.SSE_C,
    algorithm=EncryptionAlgorithm.AES256,
    key=EncryptionKey(secret=_SSE_KEY, key_id='user-generated-key-id'),
)
SSE_C_AES_2 = EncryptionSetting(
    mode=EncryptionMode.SSE_C,
    algorithm=EncryptionAlgorithm.AES256,
    key=EncryptionKey(secret=_SSE_KEY, key_id='another-user-generated-key-id'),
)


T = TypeVar('T')


def wrap_iterables(generators: list[Iterable[T]]):
    for g in generators:
        yield from g


def print_text_indented(text):
    """
    Prints text that may include weird characters, indented four spaces.
    """
    for line in text.split(linesep):
        Command._print_helper(sys.stdout, sys.stdout.encoding, '   ', repr(line)[1:-1])


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
    print_text_indented(
        json.dumps(
            value, indent=4, sort_keys=True, ensure_ascii=True, default=serialize_enc_settings
        )
    )


def remove_warnings(text):
    """Filter out Python warnings from command output."""
    return linesep.join(
        line
        for line in text.split(linesep)
        if 'DeprecationWarning' not in line
        and 'resource_tracker' not in line  # Python 3.14+ multiprocessing warnings
        and 'UserWarning' not in line  # Python 3.14+ shows more warning details
        and 'warnings.warn(' not in line  # Python 3.14+ shows source line in warnings
    )


class StringReader:
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


def should_equal(expected, actual):
    print('  expected:')
    print_json_indented(expected)
    print('  actual:')
    print_json_indented(actual)
    assert expected == actual
    print()


class CommandLine:
    EXPECTED_STDERR_PATTERNS = [
        re.compile(r'^Using https?://[\w.]+$'),  # account auth
        re.compile(r'.*B/s]$', re.DOTALL),  # progress bar
        re.compile(r'^\r?$'),  # empty line
        re.compile(
            r'Encrypting file\(s\) with SSE-C without providing key id. '
            r'Set B2_DESTINATION_SSE_C_KEY_ID to allow key identification'
        ),
        re.compile(
            r'WARNING: Unable to print unicode.  Encoding for stdout is: ' r'\'[a-zA-Z0-9]+\''
        ),  # windows-bundle tests on CI use cp1252
        re.compile(r'Trying to print: .*'),
    ]

    def __init__(
        self,
        command,
        account_id,
        application_key,
        realm,
        bucket_name_prefix,
        env_file_cmd_placeholder,
        bucket_manager: BucketManager,
        b2_uri_args,
    ):
        self.command = command
        self.account_id = account_id
        self.application_key = application_key
        self.realm = realm
        self.bucket_name_prefix = bucket_name_prefix
        self.env_file_cmd_placeholder = env_file_cmd_placeholder
        self.bucket_manager = bucket_manager
        self.b2_uri_args = b2_uri_args

    def generate_bucket_name(self):
        return self.bucket_manager.new_bucket_name()

    def get_bucket_info_args(self) -> tuple[str, str]:
        return '--bucket-info', json.dumps(self.bucket_manager.new_bucket_info(), ensure_ascii=True)

    def run_command(self, args, additional_env: dict | None = None):
        """
        Runs the command with the given arguments, returns a tuple in form of
        (succeeded, stdout)
        """
        status, stdout, stderr = self.execute(args, additional_env)
        return status == 0 and stderr == '', stdout

    def should_succeed(
        self,
        args: list[str] | None,
        expected_pattern: str | None = None,
        additional_env: dict | None = None,
        expected_stderr_pattern: str | re.Pattern = None,
    ) -> str:
        """
        Runs the command-line with the given arguments.  Raises an exception
        if there was an error; otherwise, returns the stdout of the command
        as string.
        """
        status, stdout, stderr = self.execute(args, additional_env)
        assert status == 0, f'FAILED with status {status}, stderr={stderr}'

        if expected_stderr_pattern:
            assert expected_stderr_pattern.search(
                stderr
            ), f'stderr did not match pattern="{expected_stderr_pattern}", stderr="{stderr}"'
        elif stderr != '':
            for line in (s.strip() for s in stderr.split(os.linesep)):
                assert any(
                    p.match(line) for p in self.EXPECTED_STDERR_PATTERNS
                ), f'Unexpected stderr line: {repr(line)}'

        if expected_pattern is not None:
            assert re.search(
                expected_pattern, stdout
            ), f'did not match pattern="{expected_pattern}", stdout="{stdout}"'

        return stdout.replace(os.linesep, '\n')

    @classmethod
    def prepare_env(self, additional_env: dict | None = None):
        environ['PYTHONIOENCODING'] = 'utf-8'
        env = environ.copy()
        env.update(additional_env or {})
        return env

    def parse_command(self, env):
        """
        Split `self.command` into a list of strings. If necessary, dump the env vars to a tmp file and substitute
        one the command's argument with that file's path.
        """
        command = self.command.split(' ')
        if self.env_file_cmd_placeholder:
            if any('\n' in var_value for var_value in env.values()):
                raise ValueError(
                    'Env vars containing new line characters will break env file format'
                )
            env_file_path = mktemp()
            pathlib.Path(env_file_path).write_text('\n'.join(f'{k}={v}' for k, v in env.items()))
            command = [
                (c if c != self.env_file_cmd_placeholder else env_file_path) for c in command
            ]
        return command

    def execute(
        self,
        args: list[str | Path | int] | None = None,
        additional_env: dict | None = None,
    ):
        """
        :param cmd: a command to run
        :param args: command's arguments
        :param additional_env: environment variables to pass to the command, overwriting parent process ones
        :return: (status, stdout, stderr)
        """
        # We'll run the b2 command-line by running the b2 module from
        # the current directory or provided as parameter
        env = self.prepare_env(additional_env)
        command = self.parse_command(env)

        args: list[str] = [str(arg) for arg in args] if args else []
        command.extend(args)

        print('Running:', ' '.join(command))

        stdout = StringReader()
        stderr = StringReader()

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

    def should_succeed_json(self, args, additional_env: dict | None = None, **kwargs):
        """
        Runs the command-line with the given arguments.  Raises an exception
        if there was an error; otherwise, treats the stdout as JSON and returns
        the data in it.
        """
        result = self.should_succeed(args, additional_env=additional_env, **kwargs)
        try:
            loaded_result = json.loads(result)
        except json.JSONDecodeError:
            raise ValueError(f'{result} is not a valid json')
        return loaded_result

    def should_fail(self, args, expected_pattern, additional_env: dict | None = None):
        """
        Runs the command-line with the given args, expecting the given pattern
        to appear in stderr.
        """
        status, stdout, stderr = self.execute(args, additional_env)
        assert status != 0, 'ERROR: should have failed'

        assert re.search(
            expected_pattern, stdout + stderr
        ), f'did not match pattern="{expected_pattern}", stdout="{stdout}", stderr="{stderr}"'

    def reauthorize(self, check_key_capabilities=False):
        """Clear and authorize again to the account."""
        self.should_succeed(['account', 'clear'])
        self.should_succeed(
            [
                'account',
                'authorize',
                '--environment',
                self.realm,
                self.account_id,
                self.application_key,
            ]
        )
        if check_key_capabilities:
            auth_dict = self.should_succeed_json(['account', 'get'])
            private_preview_caps = {
                'readBucketNotifications',
                'writeBucketNotifications',
            }
            missing_capabilities = (
                set(ALL_CAPABILITIES)
                - {'readBuckets', 'listAllBucketNames'}
                - private_preview_caps
                - set(auth_dict['allowed']['capabilities'])
            )
            assert not missing_capabilities, f'it appears that the raw_api integration test is being run with a non-full key. Missing capabilities: {missing_capabilities}'

    def list_file_versions(self, bucket_name, path=''):
        return self.should_succeed_json(
            ['ls', '--json', '--recursive', '--versions', *self.b2_uri_args(bucket_name, path)]
        )

    def cleanup_buckets(self, buckets: dict[str, dict | None]) -> None:
        for bucket_name, bucket_dict in buckets.items():
            self.cleanup_bucket(bucket_name, bucket_dict)

    def cleanup_bucket(self, bucket_name: str, bucket_dict: dict | None = None) -> None:
        """
        Cleanup bucket

        Since bucket was being handled by the tool, it is safe to assume it is cached in its cache and we don't
        need to call C class API list_buckets endpoint to get it.
        """
        if not bucket_dict:
            try:
                bucket_dict = self.should_succeed_json(['bucket', 'get', bucket_name])
            except (ValueError, AssertionError):  # bucket doesn't exist
                return

        bucket = self.bucket_manager.b2_api.BUCKET_CLASS(
            api=self.bucket_manager.b2_api,
            id_=bucket_dict['bucketId'],
            name=bucket_name,
        )
        self.bucket_manager.clean_bucket(bucket)


class TempDir:
    def __init__(self):
        warnings.warn(
            'TempDir is deprecated; use pytest tmp_path fixture instead',
            DeprecationWarning,
            stacklevel=2,
        )
        self.dirpath = None

    def __enter__(self):
        self.dirpath = mkdtemp()
        return Path(self.dirpath)

    def __exit__(self, exc_type, exc_val, exc_tb):
        shutil.rmtree(fix_windows_path_limit(self.dirpath))


def read_file(path: str | Path):
    with open(path, 'rb') as f:
        return f.read()


def write_file(path: str | Path, contents: bytes):
    with open(path, 'wb') as f:
        f.write(contents)


def file_mod_time_millis(path: str | Path) -> int:
    return int(os.path.getmtime(path) * 1000)


def set_file_mod_time_millis(path: str | Path, time):
    os.utime(path, (os.path.getatime(path), time / 1000))


def random_hex(length):
    return ''.join(RNG.choice('0123456789abcdef') for _ in range(length))
