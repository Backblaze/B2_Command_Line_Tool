######################################################################
#
# File: test_bucket.py
#
# Copyright 2015 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from __future__ import absolute_import, division, print_function

import os
import shutil
import sys
import tempfile
import unittest

import six

from b2.b2 import (
    AbstractWrappedError, B2Api, DownloadDestBytes, FileVersionInfo, MaxRetriesExceeded
)
from b2.progress import ProgressListener
from b2.raw_simulator import RawSimulator
from b2.stub_account_info import StubAccountInfo

IS_27_OR_LATER = sys.version_info[0] >= 3 or (sys.version_info[0] == 2 and sys.version_info[1] >= 7)


class TempDir(object):
    def __enter__(self):
        self.dirpath = tempfile.mkdtemp()
        return self.dirpath

    def __exit__(self, exc_type, exc_val, exc_tb):
        shutil.rmtree(self.dirpath)
        return None  # do not hide exception


def write_file(path, data):
    with open(path, 'wb') as f:
        f.write(data)


class StubProgressListener(ProgressListener):
    """
    Implementation of a progress listener that remembers what calls were made,
    and returns them as a short string to use in unit tests.

    For a total byte count of 100, and updates at 33 and 66, the returned
    string looks like: "100: 33 66"
    """

    def __init__(self):
        self.history = []

    def get_history(self):
        return ' '.join(self.history)

    def set_total_bytes(self, total_byte_count):
        assert len(self.history) == 0
        self.history.append('%d:' % (total_byte_count,))

    def bytes_completed(self, byte_count):
        self.history.append(str(byte_count))

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class CanRetry(AbstractWrappedError):
    """
    An exception that can be retryable, or not.
    """

    def __init__(self, can_retry):
        super(CanRetry, self).__init__(None, None, None, None, None)
        self.can_retry = can_retry

    def should_retry(self):
        return self.can_retry


class TestCaseWithBucket(unittest.TestCase):
    def setUp(self):
        self.bucket_name = 'my-bucket'
        self.simulator = RawSimulator()
        self.account_info = StubAccountInfo()
        self.api = B2Api(self.account_info, raw_api=self.simulator)
        self.api.authorize_account('production', 'my-account', 'good-app-key')
        self.bucket = self.api.create_bucket('my-bucket', 'allPublic')


class TestListUnfinished(TestCaseWithBucket):
    def test_empty(self):
        self.assertEqual([], list(self.bucket.list_unfinished_large_files()))

    def test_one(self):
        file1 = self.bucket.start_large_file('file1.txt', 'text/plain', {})
        self.assertEqual([file1], list(self.bucket.list_unfinished_large_files()))

    def test_three(self):
        file1 = self.bucket.start_large_file('file1.txt', 'text/plain', {})
        file2 = self.bucket.start_large_file('file2.txt', 'text/plain', {})
        file3 = self.bucket.start_large_file('file3.txt', 'text/plain', {})
        self.assertEqual(
            [file1, file2, file3],
            list(self.bucket.list_unfinished_large_files(batch_size=1))
        )

    def _make_file(self, file_id, file_name):
        return self.bucket.start_large_file(file_name, 'text/plain', {})


class TestLs(TestCaseWithBucket):
    def test_empty(self):
        self.assertEqual([], list(self.bucket.ls('foo')))

    def test_one_file_at_root(self):
        data = six.b('hello world')
        self.bucket.upload_bytes(data, 'hello.txt')
        expected = [('hello.txt', 11, 'upload', None)]
        actual = [
            (info.file_name, info.size, info.action, folder)
            for (info, folder) in self.bucket.ls('')
        ]
        self.assertEqual(expected, actual)

    def test_three_files_at_root(self):
        data = six.b('hello world')
        self.bucket.upload_bytes(data, 'a')
        self.bucket.upload_bytes(data, 'bb')
        self.bucket.upload_bytes(data, 'ccc')
        expected = [
            ('a', 11, 'upload', None), ('bb', 11, 'upload', None), ('ccc', 11, 'upload', None)
        ]
        actual = [
            (info.file_name, info.size, info.action, folder)
            for (info, folder) in self.bucket.ls('')
        ]
        self.assertEqual(expected, actual)

    def test_three_files_in_dir(self):
        data = six.b('hello world')
        self.bucket.upload_bytes(data, 'a')
        self.bucket.upload_bytes(data, 'bb/1')
        self.bucket.upload_bytes(data, 'bb/2/sub1')
        self.bucket.upload_bytes(data, 'bb/2/sub2')
        self.bucket.upload_bytes(data, 'bb/3')
        self.bucket.upload_bytes(data, 'ccc')
        expected = [
            ('bb/1', 11, 'upload', None), ('bb/2/sub1', 11, 'upload', 'bb/2/'),
            ('bb/3', 11, 'upload', None)
        ]
        actual = [
            (info.file_name, info.size, info.action, folder)
            for (info, folder) in self.bucket.ls(
                'bb',
                fetch_count=1
            )
        ]
        self.assertEqual(expected, actual)

    def test_three_files_multiple_versions(self):
        data = six.b('hello world')
        self.bucket.upload_bytes(data, 'a')
        self.bucket.upload_bytes(data, 'bb/1')
        self.bucket.upload_bytes(data, 'bb/2')
        self.bucket.upload_bytes(data, 'bb/2')
        self.bucket.upload_bytes(data, 'bb/2')
        self.bucket.upload_bytes(data, 'bb/3')
        self.bucket.upload_bytes(data, 'ccc')
        expected = [
            ('9998', 'bb/1', 11, 'upload', None), ('9995', 'bb/2', 11, 'upload', None),
            ('9996', 'bb/2', 11, 'upload', None), ('9997', 'bb/2', 11, 'upload', None),
            ('9994', 'bb/3', 11, 'upload', None)
        ]
        actual = [
            (info.id_, info.file_name, info.size, info.action, folder)
            for (info, folder) in self.bucket.ls(
                'bb',
                show_versions=True,
                fetch_count=1
            )
        ]
        self.assertEqual(expected, actual)


class TestUpload(TestCaseWithBucket):
    def test_upload_bytes(self):
        data = six.b('hello world')
        file_info = self.bucket.upload_bytes(data, 'file1')
        self.assertTrue(isinstance(file_info, FileVersionInfo))

    def test_upload_local_file(self):
        with TempDir() as d:
            path = os.path.join(d, 'file1')
            data = six.b('hello world')
            write_file(path, data)
            self.bucket.upload_local_file(path, 'file1')
            self._check_file_contents('file1', data)

    def test_upload_one_retryable_error(self):
        self.simulator.set_upload_errors([CanRetry(True)])
        data = six.b('hello world')
        self.bucket.upload_bytes(data, 'file1')

    def test_upload_file_one_fatal_error(self):
        if IS_27_OR_LATER:
            self.simulator.set_upload_errors([CanRetry(False)])
            data = six.b('hello world')
            with self.assertRaises(CanRetry):
                self.bucket.upload_bytes(data, 'file1')

    def test_upload_file_too_many_retryable_errors(self):
        if IS_27_OR_LATER:
            self.simulator.set_upload_errors([CanRetry(True)] * 6)
            data = six.b('hello world')
            with self.assertRaises(MaxRetriesExceeded):
                self.bucket.upload_bytes(data, 'file1')

    def test_upload_large(self):
        data = six.b('hello world') * (self.simulator.MIN_PART_SIZE * 3 // 10)
        progress_listener = StubProgressListener()
        self.bucket.upload_bytes(data, 'file1', progress_listener=progress_listener)
        self._check_file_contents('file1', data)
        self.assertEqual("660: 220 440 660", progress_listener.get_history())

    def _check_file_contents(self, file_name, expected_contents):
        download = DownloadDestBytes()
        self.bucket.download_file_by_name(file_name, download)
        self.assertEqual(expected_contents, download.bytes_io.getvalue())
