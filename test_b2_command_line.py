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

import json
import os.path
import re
import subprocess
import sys
import threading
import unittest


USAGE = """
This program tests the B2 command-line client.

Usage:
    {command} <path_to_b2_script> <accountId> <applicationKey>
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


class TestCommandLine(unittest.TestCase):

    def test_stderr_patterns(self):
        progress_bar_line = './b2:   0%|          | 0.00/33.3K [00:00<?, ?B/s]\r./b2:  25%|\xe2\x96\x88\xe2\x96\x88\xe2\x96\x8d       | 8.19K/33.3K [00:00<00:01, 21.7KB/s]\r./b2: 33.3KB [00:02, 12.1KB/s]'
        self.assertIsNotNone(CommandLine.PROGRESS_BAR_PATTERN.match(progress_bar_line))


def delete_files_in_bucket(b2_tool, bucket_name):
    while True:
        data = b2_tool.should_succeed_json(['list_file_versions', bucket_name])
        files = data['files']
        if len(files) == 0:
            return
        for file_info in files:
            b2_tool.should_succeed(['delete_file_version', file_info['fileName'], file_info['fileId']])


def clean_bucket(b2_tool, bucket_name):
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

    if bucket_name in buckets:
        delete_files_in_bucket(b2_tool, bucket_name)
        b2_tool.should_succeed(['delete_bucket', bucket_name])


def main():

    if len(sys.argv) != 4:
        usage_and_exit()
    path_to_script = sys.argv[1]
    account_id = sys.argv[2]
    application_key = sys.argv[3]

    b2_tool = CommandLine(path_to_script)

    b2_tool.should_succeed(['clear_account'])
    if '{}' != read_file(os.path.expanduser('~/.b2_account_info')):
        error_and_exit('should have cleared ~/.b2_account_info')

    bad_application_key = application_key[:-8] + ''.join(reversed(application_key[-8:]))
    b2_tool.should_fail(['authorize_account', account_id, bad_application_key], r'invalid authorization')
    b2_tool.should_succeed(['authorize_account', account_id, application_key])

    bucket_name = 'test-b2-command-line-' + account_id
    clean_bucket(b2_tool, bucket_name)

    b2_tool.should_succeed(['create_bucket', bucket_name, 'allPublic'])

    b2_tool.should_succeed(['upload_file', bucket_name, path_to_script, 'a'])
    b2_tool.should_succeed(['upload_file', bucket_name, path_to_script, 'a'])
    b2_tool.should_succeed(['upload_file', bucket_name, path_to_script, 'b/1'])
    b2_tool.should_succeed(['upload_file', bucket_name, path_to_script, 'b/2'])
    b2_tool.should_succeed(['upload_file', bucket_name, path_to_script, 'c'])
    b2_tool.should_succeed(['upload_file', bucket_name, path_to_script, 'd'])

    b2_tool.should_succeed(['hide_file', bucket_name, 'c'])

    b2_tool.should_succeed(['ls', bucket_name], r'^a\nb/\nd\n')
    b2_tool.should_succeed(['ls', '--long', bucket_name], r'^4_z.*upload.*a\n.*-.*b/\n4_z.*upload.*d\n')
    b2_tool.should_succeed(['ls', '--versions', bucket_name], r'^a\na\nb/\nc\nc\nd\n')
    b2_tool.should_succeed(['ls', bucket_name, 'b'], r'^b/1\nb/2\n')

    print
    print "ALL OK"


if __name__ == '__main__':
    if sys.argv[1:] == ['test']:
        del sys.argv[1]
        unittest.main()
    else:
        main()
