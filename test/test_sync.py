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
    parent = os.path.dirname(path)
    if not os.path.isdir(parent):
        os.makedirs(parent)
    with open(path, 'wb') as f:
        f.write(contents)


def create_files(root_dir, relative_paths):
    for relative_path in relative_paths:
        full_path = os.path.join(root_dir, relative_path)
        write_file(full_path, b'')


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
            create_files(dir, ['hello.', 'hello/a', 'hello/b', 'hello0'])
            folder = LocalFolder(dir)
            self.assertEqual('hello.', folder.next_or_none().name)
            self.assertEqual('hello/a', folder.next_or_none().name)
            self.assertEqual('hello/b', folder.next_or_none().name)
            self.assertEqual('hello0', folder.next_or_none().name)
            self.assertEqual(None, folder.next_or_none())


class TestSync(unittest.TestCase):

    def test_sync_down(self):
        self.assertEqual(2, 2)


if __name__ == '__main__':
    unittest.main()
