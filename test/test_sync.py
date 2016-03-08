######################################################################
#
# File: b2/test_sync.py
#
# Copyright 2016 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from __future__ import print_function

from b2 import LocalFolder
import os
import tempfile
import shutil
import unittest


def write_file(path, contents):
    with open(path, 'wb') as f:
        f.write(contents)


class TempDir(object):

    def __enter__(self):
        self.dirpath = tempfile.mkdtemp()
        return self.dirpath

    def __exit__(self, exc_type, exc_val, exc_tb):
        shutil.rmtree(self.dirpath)
        return None # do not hide exception


class TestLocalFolder(unittest.TestCase):

    def test_dir(self):
        with TempDir() as dir:
            write_file(os.path.join(dir, 'hello'), '')
            folder = LocalFolder(dir)
            self.assertEqual('hello', folder.next_or_none().name)
            self.assertIsNone(folder.next_or_none())


class TestSync(unittest.TestCase):

    def test_sync_down(self):
        self.assertEqual(2, 2)
