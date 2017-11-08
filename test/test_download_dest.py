#
# File: test_download_dest
#
# Copyright 2017, Backblaze Inc.  All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#

import os

from b2.download_dest import DownloadDestLocalFile, DownloadDestProgressWrapper
from b2.progress import ProgressListenerForTest
from b2.utils import TempDir
from .test_base import TestBase


class TestDownloadDestLocalFile(TestBase):
    def test_write_and_set_mod_time(self):
        """
        Check that the file gets written and that its mod time gets set.
        """
        with TempDir() as temp_dir:
            file_path = os.path.join(temp_dir, "test.txt")
            download_dest = DownloadDestLocalFile(file_path)
            with download_dest.make_file_context(
                "file_id", "file_name", 100, "content_type", "sha1", {}, 1500222333000
            ) as f:
                f.write(b'hello world\n')
            with open(file_path, 'rb') as f:
                self.assertEqual(b'hello world\n', f.read())
            self.assertEqual(1500222333, os.path.getmtime(file_path))


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
                ['set_total_bytes(100)', 'bytes_completed(12)'], progress_listener.get_calls()
            )
