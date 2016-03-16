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

import os
import sys
import unittest

from six.moves import map

from b2.sync import File, FileVersion, AbstractFolder, LocalFolder, make_folder_sync_actions, zip_folders
from b2.utils import TempDir

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


class TestLocalFolder(unittest.TestCase):
    def test_slash_sorting(self):
        # '/' should sort between '.' and '0'
        with TempDir() as tmpdir:
            create_files(tmpdir, ['hello.', 'hello/a', 'hello/b', 'hello0'])
            folder = LocalFolder(tmpdir)
            files = list(folder.all_files())
            names = [f.name for f in files]
            self.assertEqual(['hello.', 'hello/a', 'hello/b', 'hello0'], names)


class FakeFolder(AbstractFolder):
    def __init__(self, f_type, files):
        self.f_type = f_type
        self.files = files

    def all_files(self):
        return iter(self.files)

    def folder_type(self):
        return self.f_type

    def make_full_path(self, name):
        return "/dir/" + name


class TestZipFolders(unittest.TestCase):
    def test_empty(self):
        folder_a = FakeFolder('b2', [])
        folder_b = FakeFolder('b2', [])
        self.assertEqual([], list(zip_folders(folder_a, folder_b)))

    def test_one_empty(self):
        file_a1 = File("a.txt", [FileVersion("a", 100, "upload")])
        folder_a = FakeFolder('b2', [file_a1])
        folder_b = FakeFolder('b2', [])
        self.assertEqual([(file_a1, None)], list(zip_folders(folder_a, folder_b)))

    def test_two(self):
        file_a1 = File("a.txt", [FileVersion("a", 100, "upload")])
        file_a2 = File("b.txt", [FileVersion("b", 100, "upload")])
        file_a3 = File("d.txt", [FileVersion("c", 100, "upload")])
        file_a4 = File("f.txt", [FileVersion("f", 100, "upload")])
        file_b1 = File("b.txt", [FileVersion("b", 200, "upload")])
        file_b2 = File("e.txt", [FileVersion("e", 200, "upload")])
        folder_a = FakeFolder('b2', [file_a1, file_a2, file_a3, file_a4])
        folder_b = FakeFolder('b2', [file_b1, file_b2])
        self.assertEqual(
            [
                (file_a1, None), (file_a2, file_b1), (file_a3, None), (None, file_b2),
                (file_a4, None)
            ], list(zip_folders(folder_a, folder_b))
        )


class TestMakeSyncActions(unittest.TestCase):
    def test_illegal_cases(self):
        if IS_27_OR_LATER:
            with self.assertRaises(NotImplementedError):
                b2_folder = FakeFolder('b2', [])
                list(make_folder_sync_actions(b2_folder, b2_folder, 1))
            with self.assertRaises(NotImplementedError):
                local_folder = FakeFolder('local', [])
                list(make_folder_sync_actions(local_folder, local_folder, 1))

    def test_empty(self):
        folder_a = FakeFolder('b2', [])
        folder_b = FakeFolder('local', [])
        self.assertEqual([], list(make_folder_sync_actions(folder_a, folder_b, 1)))

    def test_local_to_b2(self):
        file_a1 = File("a.txt", [FileVersion("/dir/a.txt", 100, "upload")])  # only in source
        file_a2 = File("c.txt", [FileVersion("/dir/c.txt", 200, "upload")])  # mod time matches
        file_a3 = File("d.txt", [FileVersion("/dir/d.txt", 100, "upload")])  # newer in dest
        file_a4 = File("e.txt", [FileVersion("/dir/e.txt", 300, "upload")])  # newer in source

        file_b1 = File("b.txt", [FileVersion("id_b_200", 200, "upload")])  # only in dest
        file_b2 = File("c.txt", [FileVersion("id_c_200", 200, "upload")])  # mod time matches
        file_b3 = File("d.txt", [FileVersion("id_d_200", 200, "upload")])  # newer in dest
        file_b4 = File("e.txt", [FileVersion("id_e_200", 200, "upload")])  # newer in source

        folder_a = FakeFolder('local', [file_a1, file_a2, file_a3, file_a4])
        folder_b = FakeFolder('b2', [file_b1, file_b2, file_b3, file_b4])

        actions = list(make_folder_sync_actions(folder_a, folder_b, 1))
        self.assertEqual(
            [
                "b2_upload(/dir/a.txt, a.txt, 100)", "b2_delete(b.txt, id_b_200)",
                "b2_upload(/dir/e.txt, e.txt, 300)"
            ], list(map(str, actions))
        )

    def test_b2_to_local(self):
        file_a1 = File("a.txt", [FileVersion("id_a_100", 100, "upload")])  # only in source
        file_a2 = File("c.txt", [FileVersion("id_c_200", 200, "upload")])  # mod time matches
        file_a3 = File("d.txt", [FileVersion("id_d_100", 100, "upload")])  # newer in dest
        file_a4 = File("e.txt", [FileVersion("id_e_300", 300, "upload")])  # newer in source

        file_b1 = File("b.txt", [FileVersion("/dir/b.txt", 200, "upload")])  # only in dest
        file_b2 = File("c.txt", [FileVersion("/dir/c.txt", 200, "upload")])  # mod time matches
        file_b3 = File("d.txt", [FileVersion("/dir/d.txt", 200, "upload")])  # newer in dest
        file_b4 = File("e.txt", [FileVersion("/dir/e.txt", 200, "upload")])  # newer in source

        folder_a = FakeFolder('b2', [file_a1, file_a2, file_a3, file_a4])
        folder_b = FakeFolder('local', [file_b1, file_b2, file_b3, file_b4])

        actions = list(make_folder_sync_actions(folder_a, folder_b, 1))
        self.assertEqual(
            [
                "b2_download(a.txt, id_a_100)", "local_delete(/dir/b.txt)",
                "b2_download(e.txt, id_e_300)"
            ], list(map(str, actions))
        )


if __name__ == '__main__':
    unittest.main()
