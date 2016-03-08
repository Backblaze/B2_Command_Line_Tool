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

from b2 import File, FileVersion, Folder, LocalFolder, zip_folders
import os
import tempfile
import shutil
import sys
import unittest

IS_27_OR_LATER = sys.version_info[0] >= 3 or (sys.version_info[0] == 2 and sys.version_info[1] >= 7)


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
        return None  # do not hide exception


class TestLocalFolder(unittest.TestCase):
    def test_slash_sorting(self):
        # '/' should sort between '.' and '0'
        with TempDir() as dir:
            create_files(dir, ['hello.', 'hello/a', 'hello/b', 'hello0'])
            folder = LocalFolder(dir)
            files = list(folder.all_files())
            names = map(lambda f: f.name, files)
            self.assertEqual(['hello.', 'hello/a', 'hello/b', 'hello0'], names)


class FakeFolder(Folder):
    def __init__(self, files):
        self.files = files

    def all_files(self):
        return iter(self.files)


class TestZipFolders(unittest.TestCase):
    def test_empty(self):
        folder_a = FakeFolder([])
        folder_b = FakeFolder([])
        self.assertEqual([], list(zip_folders(folder_a, folder_b)))

    def test_one_empty(self):
        file_a1 = File("a.txt", [FileVersion("a", 100, "upload")])
        folder_a = FakeFolder([file_a1])
        folder_b = FakeFolder([])
        self.assertEqual([(file_a1, None)], list(zip_folders(folder_a, folder_b)))

    def test_two(self):
        file_a1 = File("a.txt", [FileVersion("a", 100, "upload")])
        file_a2 = File("b.txt", [FileVersion("b", 100, "upload")])
        file_a3 = File("d.txt", [FileVersion("c", 100, "upload")])
        file_a4 = File("f.txt", [FileVersion("f", 100, "upload")])
        file_b1 = File("b.txt", [FileVersion("b", 200, "upload")])
        file_b2 = File("e.txt", [FileVersion("e", 200, "upload")])
        folder_a = FakeFolder([file_a1, file_a2, file_a3, file_a4])
        folder_b = FakeFolder([file_b1, file_b2])
        self.assertEqual(
            [
                (file_a1, None), (file_a2, file_b1), (file_a3, None), (None, file_b2),
                (file_a4, None)
            ], list(zip_folders(folder_a, folder_b))
        )


class TestSync(unittest.TestCase):
    def test_sync_down(self):
        self.assertEqual(2, 2)


if __name__ == '__main__':
    unittest.main()
