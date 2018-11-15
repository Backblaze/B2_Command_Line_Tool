######################################################################
#
# File: test/test_download_dest.py
#
# Copyright 2018 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import os

import six

from b2.download_dest import DownloadDestLocalFile, DownloadDestProgressWrapper, PreSeekedDownloadDest
from b2.progress import ProgressListenerForTest
from b2.utils import TempDir
from .test_base import TestBase


class TestDownloadDestLocalFile(TestBase):
    expected_result = 'hello world'

    def _make_dest(self, temp_dir):
        file_path = os.path.join(temp_dir, "test.txt")
        return DownloadDestLocalFile(file_path), file_path

    def test_write_and_set_mod_time(self):
        """
        Check that the file gets written and that its mod time gets set.
        """
        with TempDir() as temp_dir:
            download_dest, file_path = self._make_dest(temp_dir)
            with download_dest.make_file_context(
                "file_id", "file_name", 100, "content_type", "sha1", {}, 1500222333000
            ) as f:
                f.write(six.b('hello world'))
            with open(file_path, 'rb') as f:
                self.assertEqual(
                    six.b(self.expected_result),
                    f.read(),
                )
            self.assertEqual(1500222333, os.path.getmtime(file_path))

    def test_failed_write_deletes_partial_file(self):
        with TempDir() as temp_dir:
            download_dest, file_path = self._make_dest(temp_dir)
            try:
                with download_dest.make_file_context(
                    "file_id", "file_name", 100, "content_type", "sha1", {}, 1500222333000
                ) as f:
                    f.write(six.b('hello world'))
                    raise Exception('test error')
            except Exception as e:
                self.assertEqual('test error', str(e))
            self.assertFalse(os.path.exists(file_path), msg='failed download should be deleted')


class TestPreSeekedDownloadDest(TestDownloadDestLocalFile):
    expected_result = '123hello world567890'

    def _make_dest(self, temp_dir):
        file_path = os.path.join(temp_dir, "test.txt")
        with open(file_path, 'wb') as f:
            f.write(six.b('12345678901234567890'))
        return PreSeekedDownloadDest(local_file_path=file_path, seek_target=3), file_path


class TestDownloadDestProgressWrapper(TestBase):
    def test_write_and_set_mod_time_and_progress(self):
        """
        Check that the file gets written and that its mod time gets set.
        """
        with TempDir() as temp_dir:
            file_path = os.path.join(temp_dir, "test.txt")
            download_local_file = DownloadDestLocalFile(file_path)
            progress_listener = ProgressListenerForTest()
            download_dest = DownloadDestProgressWrapper(download_local_file, progress_listener)
            with download_dest.make_file_context(
                "file_id", "file_name", 100, "content_type", "sha1", {}, 1500222333000
            ) as f:
                f.write(b'hello world\n')
            with open(file_path, 'rb') as f:
                self.assertEqual(b'hello world\n', f.read())
            self.assertEqual(1500222333, os.path.getmtime(file_path))
            self.assertEqual(
                [
                    'set_total_bytes(100)',
                    'bytes_completed(12)',
                    'close()',
                ],
                progress_listener.get_calls(),
            )
