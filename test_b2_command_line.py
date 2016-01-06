#!/usr/bin/env python2
######################################################################
#
# File: test_b2_command_line.py
#
# Copyright 2015 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import hashlib
import json
import os.path
import random
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import unittest


USAGE = """
This program tests the B2 command-line client.

Usages:

    {command} <path_to_b2_script> <accountId> <applicationKey> [basic | sync]

        The optional last argument specifies which of the tests to run.  If not
        specified, all test will run.

    {command} test

        Runs internal unit tests.
"""


def usage_and_exit():
    print >>sys.stderr, USAGE.format(command=sys.argv[0])
    sys.exit(1)


def error_and_exit(message):
    print 'ERROR:', message
    sys.exit(1)


def read_file(path):
    with open(path, 'rb') as f:
        return f.read()


def write_file(path, contents):
    with open(path, 'wb') as f:
        f.write(contents)


def file_mod_time_millis(path):
    return int(1000 * os.path.getmtime(path))


def random_hex(length):
    return ''.join(random.choice('0123456789abcdef') for i in xrange(length))


class TempDir(object):

    def get_dir(self):
        return self.dirpath

    def __enter__(self):
        self.dirpath = tempfile.mkdtemp()
        return self.dirpath

    def __exit__(self, exc_type, exc_val, exc_tb):
        shutil.rmtree(self.dirpath)



class StringReader(object):

    def get_string(self):
        return self.string

    def read_from(self, f):
        try:
            self.string = f.read()
        except Exception as e:
            print e
            self.string = str(e)


def run_command(command):
    """
    :param command: A list of strings like ['ls', '-l', '/dev']
    :return: (status, stdout, stderr)
    """
    stdout = StringReader()
    stderr = StringReader()
    p = subprocess.Popen(
        command,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        close_fds=True
    )
    p.stdin.close()
    reader1 = threading.Thread(target=stdout.read_from, args=[p.stdout])
    reader1.start()
    reader2 = threading.Thread(target=stderr.read_from, args=[p.stderr])
    reader2.start()
    p.wait()
    reader1.join()
    reader2.join()
    return (p.returncode, stdout.get_string(), stderr.get_string())


def print_text_indented(text):
    """
    Prints text that may include weird characters, indented four spaces.
    """
    for line in text.split('\n'):
        print '   ', repr(line)[1:-1]


def print_json_indented(value):
    """
    Converts the value to JSON, then prints it.
    """
    print_text_indented(json.dumps(value, indent=4, sort_keys=True))


def print_output(status, stdout, stderr):
    print '  status:', status
    if stdout != '':
        print '  stdout:'
        print_text_indented(stdout)
    if stderr != '':
        print '  stderr:'
        print_text_indented(stderr)
    print


class CommandLine(object):

    PROGRESS_BAR_PATTERN = re.compile(r'.*KB/s]$', re.DOTALL)

    EXPECTED_STDERR_PATTERNS = [
        PROGRESS_BAR_PATTERN,
        re.compile(r'^$')  # empty line
    ]

    def __init__(self, path_to_script):
        self.path_to_script = path_to_script

    def should_succeed(self, args, expected_pattern=None):
        """
        Runs the command-line with the given arguments.  Raises an exception
        if there was an error; otherwise, returns the stdout of the command
        as as string.
        """
        command = [self.path_to_script] + args
        print 'Running:', ' '.join(command)
        (status, stdout, stderr) = run_command(command)
        print_output(status, stdout, stderr)
        if status != 0:
            print 'FAILED with status', status
            sys.exit(1)
        if stderr != '':
            failed = False
            for line in map(lambda s: s.strip(), stderr.split('\n')):
                if not any(p.match(line) for p in self.EXPECTED_STDERR_PATTERNS):
                    print 'Unexpected stderr line:', repr(line)
                    failed = True
            if failed:
                print 'FAILED because of stderr'
                print stderr
                sys.exit(1)
        if expected_pattern is not None:
            if re.search(expected_pattern, stdout) is None:
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
        command = [self.path_to_script] + args
        print 'Running:', ' '.join(command)
        (status, stdout, stderr) = run_command(command)
        print_output(status, stdout, stderr)
        if status == 0:
            print 'ERROR: should have failed'
            sys.exit(1)
        if re.search(expected_pattern, stdout + stderr) is None:
            error_and_exit('did not match pattern: ' + expected_pattern)

    def list_file_versions(self, bucket_name):
        return self.should_succeed_json(['list_file_versions', bucket_name])['files']


class TestCommandLine(unittest.TestCase):

    def test_stderr_patterns(self):
        progress_bar_line = './b2:   0%|          | 0.00/33.3K [00:00<?, ?B/s]\r./b2:  25%|\xe2\x96\x88\xe2\x96\x88\xe2\x96\x8d       | 8.19K/33.3K [00:00<00:01, 21.7KB/s]\r./b2: 33.3KB [00:02, 12.1KB/s]'
        self.assertIsNotNone(CommandLine.PROGRESS_BAR_PATTERN.match(progress_bar_line))
        progress_bar_line = '\r./b2:   0%|          | 0.00/33.3K [00:00<?, ?B/s]\r./b2:  25%|\xe2\x96\x88\xe2\x96\x88\xe2\x96\x8d       | 8.19K/33.3K [00:00<00:01, 19.6KB/s]\r./b2: 33.3KB [00:02, 14.0KB/s]'
        self.assertIsNotNone(CommandLine.PROGRESS_BAR_PATTERN.match(progress_bar_line))


def should_equal(expected, actual):
    print '  expected:'
    print_json_indented(expected)
    print '  actual:'
    print_json_indented(actual)
    if expected != actual:
        print '  ERROR'
        sys.exit(1)
    print


def delete_files_in_bucket(b2_tool, bucket_name):
    while True:
        data = b2_tool.should_succeed_json(['list_file_versions', bucket_name])
        files = data['files']
        if len(files) == 0:
            return
        for file_info in files:
            b2_tool.should_succeed(['delete_file_version', file_info['fileName'], file_info['fileId']])


def clean_buckets(b2_tool, bucket_name_prefix):
    """
    Removes the named bucket, if it's there.

    In doing so, exercises list_buckets.
    """
    text = b2_tool.should_succeed(['list_buckets'])

    buckets = {}
    for line in text.split('\n')[:-1]:
        words = line.split()
        if len(words) != 3:
            error_and_exit('bad list_buckets line: ' + line)
        (b_id, b_type, b_name) = words
        buckets[b_name] = b_id

    for bucket_name in buckets:
        if bucket_name.startswith(bucket_name_prefix):
            delete_files_in_bucket(b2_tool, bucket_name)
            b2_tool.should_succeed(['delete_bucket', bucket_name])


def basic_test(b2_tool, bucket_name):

    path_to_script = b2_tool.path_to_script

    with open(path_to_script, 'rb') as f:
        hex_sha1 = hashlib.sha1(f.read()).hexdigest()
    uploaded_a = b2_tool.should_succeed_json(['upload_file', '--quiet', bucket_name, path_to_script, 'a'])
    b2_tool.should_succeed(['upload_file', bucket_name, path_to_script, 'a'])
    b2_tool.should_succeed(['upload_file', bucket_name, path_to_script, 'b/1'])
    b2_tool.should_succeed(['upload_file', bucket_name, path_to_script, 'b/2'])
    b2_tool.should_succeed(['upload_file', '--sha1', hex_sha1, '--info', 'foo=bar', '--info', 'color=blue', bucket_name, path_to_script, 'c'])
    b2_tool.should_succeed(['upload_file', '--contentType', 'text/plain', bucket_name, path_to_script, 'd'])

    b2_tool.should_succeed(['download_file_by_name', bucket_name, 'b/1', '/dev/null'])
    b2_tool.should_succeed(['download_file_by_id', uploaded_a['fileId'], '/dev/null'])

    b2_tool.should_succeed(['hide_file', bucket_name, 'c'])

    list_of_files = b2_tool.should_succeed_json(['list_file_names', bucket_name])
    should_equal(['a', 'b/1', 'b/2', 'd'], [f['fileName'] for f in list_of_files['files']])
    list_of_files = b2_tool.should_succeed_json(['list_file_names', bucket_name, 'b/2'])
    should_equal(['b/2', 'd'], [f['fileName'] for f in list_of_files['files']])
    list_of_files = b2_tool.should_succeed_json(['list_file_names', bucket_name, 'b', '2'])
    should_equal(['b/1', 'b/2'], [f['fileName'] for f in list_of_files['files']])

    list_of_files = b2_tool.should_succeed_json(['list_file_versions', bucket_name])
    should_equal(['a', 'a', 'b/1', 'b/2', 'c', 'c', 'd'], [f['fileName'] for f in list_of_files['files']])
    should_equal(['upload', 'upload', 'upload', 'upload', 'hide', 'upload', 'upload'], [f['action'] for f in list_of_files['files']])
    first_c_version = list_of_files['files'][4]
    second_c_version = list_of_files['files'][5]
    list_of_files = b2_tool.should_succeed_json(['list_file_versions', bucket_name, 'c'])
    should_equal(['c', 'c', 'd'], [f['fileName'] for f in list_of_files['files']])
    list_of_files = b2_tool.should_succeed_json(['list_file_versions', bucket_name, 'c', second_c_version['fileId']])
    should_equal(['c', 'd'], [f['fileName'] for f in list_of_files['files']])
    list_of_files = b2_tool.should_succeed_json(['list_file_versions', bucket_name, 'c', second_c_version['fileId'], '1'])
    should_equal(['c'], [f['fileName'] for f in list_of_files['files']])

    b2_tool.should_succeed(['ls', bucket_name], r'^a\nb/\nd\n')
    b2_tool.should_succeed(['ls', '--long', bucket_name], r'^4_z.*upload.*a\n.*-.*b/\n4_z.*upload.*d\n')
    b2_tool.should_succeed(['ls', '--versions', bucket_name], r'^a\na\nb/\nc\nc\nd\n')
    b2_tool.should_succeed(['ls', bucket_name, 'b'], r'^b/1\nb/2\n')
    b2_tool.should_succeed(['ls', bucket_name, 'b/'], r'^b/1\nb/2\n')

    file_info = b2_tool.should_succeed_json(['get_file_info', second_c_version['fileId']])
    should_equal({'color': 'blue', 'foo': 'bar'}, file_info['fileInfo'])

    b2_tool.should_succeed(['delete_file_version', 'c', first_c_version['fileId']])
    b2_tool.should_succeed(['ls', bucket_name], r'^a\nb/\nc\nd\n')

    b2_tool.should_succeed(['make_url', second_c_version['fileId']])


def file_version_summary(list_of_files):
    """
    Given the result of list_file_versions, returns a list
    of all file versions, with "+" for upload and "-" for
    hide, looking like this:

       ['+ photos/a.jpg', '- photos/b.jpg', '+ photos/c.jpg']
    """
    return [
        ('+ ' if (f['action'] == 'upload') else '- ') + f['fileName']
        for f in list_of_files
        ]


def find_file_id(list_of_files, file_name):
    for file in list_of_files:
        if file['fileName'] == file_name:
            return file['fileId']
    raise Exception('file not found: ', file_name)


def sync_test(b2_tool, bucket_name):

    with TempDir() as dir_path:

        p = lambda fname: os.path.join(dir_path, fname)

        b2_sync_point = 'b2:%s/sync' % bucket_name

        b2_tool.should_succeed(['sync', dir_path, b2_sync_point])
        file_versions = b2_tool.list_file_versions(bucket_name)
        should_equal([], file_version_summary(file_versions))

        write_file(p('a'), 'hello')
        write_file(p('b'), 'hello')
        write_file(p('c'), 'hello')

        b2_tool.should_succeed(['sync', dir_path, b2_sync_point])
        file_versions = b2_tool.list_file_versions(bucket_name)
        should_equal(
            [
                '+ sync/a',
                '+ sync/b',
                '+ sync/c'
            ],
            file_version_summary(file_versions)
        )

        c_id = find_file_id(file_versions, 'sync/c')
        file_info = b2_tool.should_succeed_json(['get_file_info', c_id])['fileInfo']
        should_equal(
            file_mod_time_millis(p('a')),
            int(file_info['src_last_modified_millis'])
        )

        os.unlink(p('b'))
        write_file(p('c'), 'hello world')

        b2_tool.should_succeed(['sync', '--hide', dir_path, b2_sync_point])
        file_versions = b2_tool.list_file_versions(bucket_name)
        should_equal(
            [
                '+ sync/a',
                '- sync/b',
                '+ sync/b',
                '+ sync/c',
                '+ sync/c'
            ],
            file_version_summary(file_versions)
        )

        os.unlink(p('a'))

        b2_tool.should_succeed(['sync', '--delete', dir_path, b2_sync_point])
        file_versions = b2_tool.list_file_versions(bucket_name)
        should_equal(
            [
                '- sync/b',
                '+ sync/b',
                '+ sync/c',
                '+ sync/c'
            ],
            file_version_summary(file_versions)
        )


def main():

    if len(sys.argv) < 4:
        usage_and_exit()
    path_to_script = sys.argv[1]
    account_id = sys.argv[2]
    application_key = sys.argv[3]

    all_tests = ['basic', 'sync']
    if len(sys.argv) == 4:
        tests_to_run = all_tests
    else:
        tests_to_run = sys.argv[4:]
    for test_name in tests_to_run:
        if test_name not in all_tests:
            error_and_exit('unknown test: ' + test_name)

    test_map = {
        'basic': basic_test,
        'sync': sync_test
    }

    b2_tool = CommandLine(path_to_script)

    # Run each of the tests in its own empty bucket
    for test_name in tests_to_run:

        print '#'
        print '# Cleaning and making bucket for:', test_name
        print '#'
        print

        b2_tool.should_succeed(['clear_account'])
        if '{}' != read_file(os.path.expanduser('~/.b2_account_info')):
            error_and_exit('should have cleared ~/.b2_account_info')

        bad_application_key = application_key[:-8] + ''.join(reversed(application_key[-8:]))
        b2_tool.should_fail(['authorize_account', account_id, bad_application_key], r'invalid authorization')
        b2_tool.should_succeed(['authorize_account', account_id, application_key])

        bucket_name_prefix = 'test-b2-command-line-' + account_id
        clean_buckets(b2_tool, bucket_name_prefix)
        bucket_name = bucket_name_prefix + '-' + random_hex(8)

        b2_tool.should_succeed(['create_bucket', bucket_name, 'allPrivate'])
        b2_tool.should_succeed(['update_bucket', bucket_name, 'allPublic'])

        print '#'
        print '# Running test:', test_name
        print '#'
        print

        test_fcn = test_map[test_name]
        test_fcn(b2_tool, bucket_name)

    print
    print "ALL OK"


if __name__ == '__main__':
    if sys.argv[1:] == ['test']:
        del sys.argv[1]
        unittest.main()
    else:
        main()
