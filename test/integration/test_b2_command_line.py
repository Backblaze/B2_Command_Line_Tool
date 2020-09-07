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
import hashlib
import json
import os.path
import platform
import random
import re
import shutil
import subprocess
import sys
import tempfile
import threading

import pytest

from b2sdk.utils import fix_windows_path_limit


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
    sys.exit(1)


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


def run_command(cmd, args):
    """
    :param cmd: a command to run
    :param args: command's arguments
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
    p = subprocess.Popen(
        command,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        close_fds=platform.system() != 'Windows'
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
    print_text_indented(json.dumps(value, indent=4, sort_keys=True))


def print_output(status, stdout, stderr):
    print('  status:', status)
    if stdout != '':
        print('  stdout:')
        print_text_indented(stdout)
    if stderr != '':
        print('  stderr:')
        print_text_indented(stderr)
    print()


class CommandLine(object):

    EXPECTED_STDERR_PATTERNS = [
        re.compile(r'.*B/s]$', re.DOTALL),  # progress bar
        re.compile(r'^$')  # empty line
    ]

    def __init__(self, command, account_id, application_key):
        self.command = command
        self.account_id = account_id
        self.application_key = application_key

    def run_command(self, args):
        """
        Runs the command with the given arguments, returns a tuple in form of
        (succeeded, stdout)
        """
        status, stdout, stderr = run_command(self.command, args)
        return status == 0 and stderr == '', stdout

    def should_succeed(self, args, expected_pattern=None):
        """
        Runs the command-line with the given arguments.  Raises an exception
        if there was an error; otherwise, returns the stdout of the command
        as as string.
        """
        status, stdout, stderr = run_command(self.command, args)
        if status != 0:
            print('FAILED with status', status)
            sys.exit(1)
        if stderr != '':
            failed = False
            for line in (s.strip() for s in stderr.split(os.linesep)):
                if not any(p.match(line) for p in self.EXPECTED_STDERR_PATTERNS):
                    print('Unexpected stderr line:', repr(line))
                    failed = True
            if failed:
                print('FAILED because of stderr')
                print(stderr)
                sys.exit(1)
        if expected_pattern is not None:
            if re.search(expected_pattern, stdout) is None:
                print('STDOUT:')
                print(stdout)
                error_and_exit('did not match pattern: ' + expected_pattern)
        return stdout

    def should_succeed_json(self, args):
        """
        Runs the command-line with the given arguments.  Raises an exception
        if there was an error; otherwise, treats the stdout as JSON and returns
        the data in it.
        """
        return json.loads(self.should_succeed(args))

    def should_fail(self, args, expected_pattern):
        """
        Runs the command-line with the given args, expecting the given pattern
        to appear in stderr.
        """
        status, stdout, stderr = run_command(self.command, args)
        if status == 0:
            print('ERROR: should have failed')
            sys.exit(1)
        if re.search(expected_pattern, stdout + stderr) is None:
            print(expected_pattern)
            print(stdout + stderr)
            error_and_exit('did not match pattern: ' + expected_pattern)

    def list_file_versions(self, bucket_name):
        return self.should_succeed_json(['ls', '--json', '--recursive', '--versions', bucket_name])


def should_equal(expected, actual):
    print('  expected:')
    print_json_indented(expected)
    print('  actual:')
    print_json_indented(actual)
    if expected != actual:
        print('  ERROR')
        sys.exit(1)
    print()


def delete_files_in_bucket(b2_tool, bucket_name):
    while True:
        files = b2_tool.should_succeed_json(['ls', '--json', '--versions', bucket_name])
        if len(files) == 0:
            return
        for file_info in files:
            b2_tool.should_succeed(
                ['delete-file-version', file_info['fileName'], file_info['fileId']]
            )


def clean_buckets(b2_tool, bucket_name_prefix):
    """
    Removes the named bucket, if it's there.

    In doing so, exercises list_buckets.
    """
    text = b2_tool.should_succeed(['list-buckets'])

    buckets = {}
    for line in text.split(os.linesep)[:-1]:
        words = line.split()
        if len(words) != 3:
            error_and_exit('bad list_buckets line: ' + line)
        (b_id, b_type, b_name) = words
        buckets[b_name] = b_id

    for bucket_name in buckets:
        if bucket_name.startswith(bucket_name_prefix):
            delete_files_in_bucket(b2_tool, bucket_name)
            b2_tool.should_succeed(['delete-bucket', bucket_name])


def setup_envvar_test(envvar_name, envvar_value):
    """
    Establish config for environment variable test.
    The envvar_value names the new credential file
    Create an environment variable with the given value
    Copy the B2 credential file (~/.b2_account_info) and rename the existing copy
    Extract and return the account_id and application_key from the credential file
    """

    src = os.path.expanduser('~/.b2_account_info')
    dst = os.path.expanduser(envvar_value)
    shutil.copyfile(src, dst)
    shutil.move(src, src + '.bkup')
    os.environ[envvar_name] = envvar_value


def tearDown_envvar_test(envvar_name):
    """
    Clean up after running the environment variable test.
    Delete the new B2 credential file (file contained in the
    envvar_name environment variable.
    Rename the backup of the original credential file back to
    the standard name (~/.b2_account_info)
    Delete the environment variable
    """

    os.remove(os.environ.get(envvar_name))
    fname = os.path.expanduser('~/.b2_account_info')
    shutil.move(fname + '.bkup', fname)
    if os.environ.get(envvar_name) is not None:
        del os.environ[envvar_name]


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
    return True


def basic_test(b2_tool, bucket_name):

    file_to_upload = 'README.md'
    file_mod_time_str = str(file_mod_time_millis(file_to_upload))

    hex_sha1 = hashlib.sha1(read_file(file_to_upload)).hexdigest()

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

    b2_tool.should_succeed(
        ['download-file-by-name', '--noProgress', bucket_name, 'b/1', os.devnull]
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


def key_restrictions_test(b2_tool, bucket_name):

    second_bucket_name = 'test-b2-cli-' + random_hex(8)
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

    b2_tool.should_succeed(['authorize-account', key_one_id, key_one],)

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

    b2_tool.should_succeed(['authorize-account', key_two_id, key_two],)
    b2_tool.should_succeed(['get-bucket', bucket_name],)
    b2_tool.should_succeed(['ls', bucket_name],)

    failed_bucket_err = r'ERROR: Application key is restricted to bucket: ' + bucket_name
    b2_tool.should_fail(['get-bucket', second_bucket_name], failed_bucket_err)

    failed_list_files_err = r'ERROR: Application key is restricted to bucket: ' + bucket_name
    b2_tool.should_fail(['ls', second_bucket_name], failed_list_files_err)

    # reauthorize with more capabilities for clean up
    b2_tool.should_succeed(['authorize-account', b2_tool.account_id, b2_tool.application_key])
    b2_tool.should_succeed(['delete-bucket', second_bucket_name])
    b2_tool.should_succeed(['delete-key', key_one_id])
    b2_tool.should_succeed(['delete-key', key_two_id])


def account_test(b2_tool, bucket_name):
    # actually a high level operations test - we run bucket tests here since this test doesn't use it
    b2_tool.should_succeed(['delete-bucket', bucket_name])
    new_bucket_name = bucket_name[:-8] + random_hex(
        8
    )  # apparently server behaves erratically when we delete a bucket and recreate it right away
    b2_tool.should_succeed(['create-bucket', new_bucket_name, 'allPrivate'])
    b2_tool.should_succeed(['update-bucket', new_bucket_name, 'allPublic'])

    new_creds = os.path.join(tempfile.gettempdir(), 'b2_account_info')
    setup_envvar_test('B2_ACCOUNT_INFO', new_creds)
    b2_tool.should_succeed(['clear-account'])
    bad_application_key = random_hex(len(b2_tool.application_key))
    b2_tool.should_fail(
        ['authorize-account', b2_tool.account_id, bad_application_key], r'unauthorized'
    )
    b2_tool.should_succeed(['authorize-account', b2_tool.account_id, b2_tool.application_key])
    tearDown_envvar_test('B2_ACCOUNT_INFO')


def file_version_summary(list_of_files):
    """
    Given the result of list-file-versions, returns a list
    of all file versions, with "+" for upload and "-" for
    hide, looking like this:

       ['+ photos/a.jpg', '- photos/b.jpg', '+ photos/c.jpg']
    """
    return [('+ ' if (f['action'] == 'upload') else '- ') + f['fileName'] for f in list_of_files]


def find_file_id(list_of_files, file_name):
    for file in list_of_files:
        if file['fileName'] == file_name:
            return file['fileId']
    assert False, 'file not found: %s' % (file_name,)


def sync_up_test(b2_tool, bucket_name):
    _sync_test_using_dir(b2_tool, bucket_name, 'sync')


def sync_test_no_prefix(b2_tool, bucket_name):
    _sync_test_using_dir(b2_tool, bucket_name, '')


def _sync_test_using_dir(b2_tool, bucket_name, dir_):
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

        # now upload
        b2_tool.should_succeed(
            ['sync', '--noProgress', dir_path, b2_sync_point],
            expected_pattern="d could not be accessed"
        )
        file_versions = b2_tool.list_file_versions(bucket_name)
        should_equal(
            [
                '+ ' + prefix + 'a',
                '+ ' + prefix + 'b',
                '+ ' + prefix + 'c',
            ], file_version_summary(file_versions)
        )

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


def sync_down_helper(b2_tool, bucket_name, folder_in_bucket):

    file_to_upload = 'README.md'

    b2_sync_point = 'b2:%s' % bucket_name
    if folder_in_bucket:
        b2_sync_point += '/' + folder_in_bucket
        b2_file_prefix = folder_in_bucket + '/'
    else:
        b2_file_prefix = ''

    with TempDir() as local_path:
        # Sync from an empty "folder" as a source.
        b2_tool.should_succeed(['sync', b2_sync_point, local_path])
        should_equal([], sorted(os.listdir(local_path)))

        # Put a couple files in B2
        b2_tool.should_succeed(
            ['upload-file', '--noProgress', bucket_name, file_to_upload, b2_file_prefix + 'a']
        )
        b2_tool.should_succeed(
            ['upload-file', '--noProgress', bucket_name, file_to_upload, b2_file_prefix + 'b']
        )
        b2_tool.should_succeed(['sync', b2_sync_point, local_path])
        should_equal(['a', 'b'], sorted(os.listdir(local_path)))

        b2_tool.should_succeed(
            ['upload-file', '--noProgress', bucket_name, file_to_upload, b2_file_prefix + 'c']
        )

        # Sync the files with one file being excluded because of mtime
        mod_time = str((file_mod_time_millis(file_to_upload) - 10) / 1000)
        b2_tool.should_succeed(
            [
                'sync', '--noProgress', '--excludeIfModifiedAfter', mod_time, b2_sync_point,
                local_path
            ]
        )
        should_equal(['a', 'b'], sorted(os.listdir(local_path)))
        # Sync all the files
        b2_tool.should_succeed(['sync', '--noProgress', b2_sync_point, local_path])
        should_equal(['a', 'b', 'c'], sorted(os.listdir(local_path)))


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


def main():
    test_map = {
        'account': account_test,
        'basic': basic_test,
        'keys': key_restrictions_test,
        'sync_down': sync_down_test,
        'sync_up': sync_up_test,
        'sync_up_no_prefix': sync_test_no_prefix,
        'sync_long_path': sync_long_path_test,
        'download': download_test,
    }

    args = parse_args(tests=sorted(test_map))
    print(args)
    account_id = os.environ.get('B2_TEST_APPLICATION_KEY_ID', '')
    application_key = os.environ.get('B2_TEST_APPLICATION_KEY', '')

    defer_cleanup = True
    bucket_name_prefix = 'test-b2-cli-' + random_hex(8)

    if os.environ.get('B2_ACCOUNT_INFO') is not None:
        del os.environ['B2_ACCOUNT_INFO']

    b2_tool = CommandLine(args.command, account_id, application_key)

    global_dirty = False
    # Run each of the tests in its own empty bucket
    for test_name in args.tests:

        print('#')
        print('# Cleaning and making bucket for:', test_name)
        print('#')
        print()

        b2_tool.should_succeed(['clear-account'])

        b2_tool.should_succeed(['authorize-account', account_id, application_key])

        if not defer_cleanup:
            clean_buckets(b2_tool, bucket_name_prefix)
        bucket_name = bucket_name_prefix + '-' + random_hex(8)

        success, _ = b2_tool.run_command(['create-bucket', bucket_name, 'allPublic'])
        if not success:
            clean_buckets(b2_tool, bucket_name_prefix)
            b2_tool.should_succeed(['create-bucket', bucket_name, 'allPublic'])

        print('#')
        print('# Running test:', test_name)
        print('#')
        print()

        test_fcn = test_map[test_name]
        dirty = not test_fcn(b2_tool, bucket_name)
        global_dirty = global_dirty or dirty

    if global_dirty:
        print('#' * 70)
        print('#')
        print('# The last test was run, cleaning up')
        print('#')
        print('#' * 70)
        print()
        clean_buckets(b2_tool, bucket_name_prefix)
    print()
    print("ALL OK")


# TODO: rewrite to multiple tests
def test_integration():
    application_key_id = os.environ.get('B2_TEST_APPLICATION_KEY_ID')
    if application_key_id is None:
        pytest.fail('B2_TEST_APPLICATION_KEY_ID is not set.')

    application_key = os.environ.get('B2_TEST_APPLICATION_KEY')
    if application_key is None:
        pytest.fail('B2_TEST_APPLICATION_KEY is not set.')

    print()

    sys.argv = ['test_b2_command_line.py']
    main()


if __name__ == '__main__':
    main()
