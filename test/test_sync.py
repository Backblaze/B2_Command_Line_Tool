######################################################################
#
# File: b2/test_sync.py
#
# Copyright 2018 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from __future__ import print_function

import os
import platform
import threading
import time
import unittest

import six

from .test_base import TestBase
from b2.exception import CommandError, DestFileNewer
from b2.file_version import FileVersionInfo
from b2.sync.folder import AbstractFolder, B2Folder, LocalFolder
from b2.sync.file import File, FileVersion
from b2.sync.scan_policies import ScanPoliciesManager, DEFAULT_SCAN_MANAGER
from b2.sync.sync import BoundedQueueExecutor, make_folder_sync_actions, zip_folders
from b2.sync.folder_parser import parse_sync_folder
from b2.utils import TempDir

try:
    from unittest.mock import MagicMock
except ImportError:
    from mock import MagicMock

try:
    import concurrent.futures as futures
except ImportError:
    import futures

DAY = 86400000  # milliseconds
TODAY = DAY * 100  # an arbitrary reference time for testing


def write_file(path, contents):
    parent = os.path.dirname(path)
    if not os.path.isdir(parent):
        os.makedirs(parent)
    with open(path, 'wb') as f:
        f.write(contents)


class TestSync(TestBase):
    def setUp(self):
        self.reporter = MagicMock()


class TestLocalFolder(TestSync):
    NAMES = [
        six.u('.dot_file'),
        six.u('hello.'),
        six.u('hello/a/1'),
        six.u('hello/a/2'),
        six.u('hello/b'),
        six.u('hello0'),
        six.u('inner/a.bin'),
        six.u('inner/a.txt'),
        six.u('inner/b.bin'),
        six.u('inner/b.txt'),
        six.u('inner/more/a.bin'),
        six.u('inner/more/a.txt'),
        six.u('\u81ea\u7531'),
    ]

    @classmethod
    def _create_files(cls, root_dir, relative_paths):
        for relative_path in relative_paths:
            full_path = os.path.join(root_dir, relative_path)
            write_file(full_path, b'')

    def _prepare_folder(self, root_dir, broken_symlink=False, invalid_permissions=False):
        assert not (broken_symlink and invalid_permissions)
        self._create_files(root_dir, self.NAMES)
        if broken_symlink:
            os.symlink(
                os.path.join(root_dir, 'non_existant_file'), os.path.join(root_dir, 'bad_symlink')
            )
        elif invalid_permissions:
            os.chmod(os.path.join(root_dir, self.NAMES[0]), 0)
        return LocalFolder(root_dir)

    def test_slash_sorting(self):
        # '/' should sort between '.' and '0'
        with TempDir() as tmpdir:
            folder = self._prepare_folder(tmpdir)
            self.assertEqual(self.NAMES, list(f.name for f in folder.all_files(self.reporter)))
            self.reporter.local_access_error.assert_not_called()

    def test_broken_symlink(self):
        with TempDir() as tmpdir:
            folder = self._prepare_folder(tmpdir, broken_symlink=True)
            self.assertEqual(self.NAMES, list(f.name for f in folder.all_files(self.reporter)))
            self.reporter.local_access_error.assert_called_once_with(
                os.path.join(tmpdir, 'bad_symlink')
            )

    def test_invalid_permissions(self):
        with TempDir() as tmpdir:
            folder = self._prepare_folder(tmpdir, invalid_permissions=True)
            # tests differ depending on the user running them. "root" will
            # succeed in os.access(path, os.R_OK) even if the permissions of
            # the file are 0 as implemented on self._prepare_folder().
            # use-case: running test suite inside a docker container
            if not os.access(os.path.join(tmpdir, self.NAMES[0]), os.R_OK):
                self.assertEqual(
                    self.NAMES[1:], list(f.name for f in folder.all_files(self.reporter))
                )
                self.reporter.local_permission_error.assert_called_once_with(
                    os.path.join(tmpdir, self.NAMES[0])
                )
            else:
                self.assertEqual(self.NAMES, list(f.name for f in folder.all_files(self.reporter)))

    def _check_file_filters_results(self, policies_manager, expected_scan_results):
        with TempDir() as tmpdir:
            folder = self._prepare_folder(tmpdir)
            self.assertEqual(
                expected_scan_results,
                list(f.name for f in folder.all_files(self.reporter, policies_manager))
            )
            self.reporter.local_access_error.assert_not_called()

    def test_exclusions(self):
        expected_list = [
            six.u('.dot_file'),
            six.u('hello.'),
            six.u('hello/a/1'),
            six.u('hello/a/2'),
            six.u('hello/b'),
            six.u('hello0'),
            six.u('inner/a.txt'),
            six.u('inner/b.txt'),
            six.u('inner/more/a.txt'),
            six.u('\u81ea\u7531'),
        ]
        polices_manager = ScanPoliciesManager(exclude_file_regexes=['.*\\.bin'])
        self._check_file_filters_results(polices_manager, expected_list)

    def test_exclude_all(self):
        expected_list = []
        polices_manager = ScanPoliciesManager(exclude_file_regexes=['.*'])
        self._check_file_filters_results(polices_manager, expected_list)

    def test_exclusions_inclusions(self):
        expected_list = [
            six.u('.dot_file'),
            six.u('hello.'),
            six.u('hello/a/1'),
            six.u('hello/a/2'),
            six.u('hello/b'),
            six.u('hello0'),
            six.u('inner/a.bin'),
            six.u('inner/a.txt'),
            six.u('inner/b.txt'),
            six.u('inner/more/a.bin'),
            six.u('inner/more/a.txt'),
            six.u('\u81ea\u7531'),
        ]
        polices_manager = ScanPoliciesManager(
            exclude_file_regexes=['.*\\.bin'],
            include_file_regexes=['.*a\\.bin'],
        )
        self._check_file_filters_results(polices_manager, expected_list)

    def test_exclude_matches_prefix(self):
        expected_list = [
            six.u('.dot_file'),
            six.u('hello.'),
            six.u('hello/b'),
            six.u('hello0'),
            six.u('inner/b.bin'),
            six.u('inner/b.txt'),
            six.u('\u81ea\u7531'),
        ]
        polices_manager = ScanPoliciesManager(exclude_file_regexes=['.*a'])
        self._check_file_filters_results(polices_manager, expected_list)

    def test_exclude_directory(self):
        expected_list = [
            six.u('.dot_file'),
            six.u('hello.'),
            six.u('hello0'),
            six.u('\u81ea\u7531'),
        ]
        polices_manager = ScanPoliciesManager(
            exclude_dir_regexes=['hello', 'more', 'hello0'], exclude_file_regexes=['inner']
        )
        self._check_file_filters_results(polices_manager, expected_list)

    def test_exclude_directory2(self):
        expected_list = [
            six.u('.dot_file'),
            six.u('hello.'),
            six.u('hello0'),
            six.u('\u81ea\u7531'),
        ]
        polices_manager = ScanPoliciesManager(exclude_dir_regexes=['hello$', 'inner'],)
        self._check_file_filters_results(polices_manager, expected_list)

    def test_exclude_directory_trailing_slash_does_not_match(self):
        expected_list = [
            six.u('.dot_file'),
            six.u('hello.'),
            six.u('hello0'),
            six.u('inner/a.bin'),
            six.u('inner/a.txt'),
            six.u('inner/b.bin'),
            six.u('inner/b.txt'),
            six.u('\u81ea\u7531'),
        ]
        polices_manager = ScanPoliciesManager(exclude_dir_regexes=['hello$', 'inner/'],)
        self._check_file_filters_results(polices_manager, expected_list)

    def test_exclusion_with_exact_match(self):
        expected_list = [
            six.u('.dot_file'),
            six.u('hello.'),
            six.u('hello/a/1'),
            six.u('hello/a/2'),
            six.u('hello/b'),
            six.u('inner/a.bin'),
            six.u('inner/a.txt'),
            six.u('inner/b.bin'),
            six.u('inner/b.txt'),
            six.u('inner/more/a.bin'),
            six.u('inner/more/a.txt'),
            six.u('\u81ea\u7531'),
        ]
        polices_manager = ScanPoliciesManager(exclude_file_regexes=['hello0'],)
        self._check_file_filters_results(polices_manager, expected_list)


class TestB2Folder(TestSync):
    def setUp(self):
        super(TestB2Folder, self).setUp()
        self.bucket = MagicMock()
        self.api = MagicMock()
        self.api.get_bucket_by_name.return_value = self.bucket
        self.b2_folder = B2Folder('bucket-name', 'folder', self.api)

    def test_empty(self):
        self.bucket.ls.return_value = []
        self.assertEqual([], list(self.b2_folder.all_files(self.reporter)))

    def test_multiple_versions(self):
        # Test two files, to cover the yield within the loop, and
        # the yield without.
        self.bucket.ls.return_value = [
            (
                FileVersionInfo(
                    'a2', 'folder/a.txt', 200, 'text/plain', 'sha1', {}, 2000, 'upload'
                ), 'folder'
            ),
            (
                FileVersionInfo(
                    'a1', 'folder/a.txt', 100, 'text/plain', 'sha1', {}, 1000, 'upload'
                ), 'folder'
            ),
            (
                FileVersionInfo(
                    'b2', 'folder/b.txt', 200, 'text/plain', 'sha1', {}, 2000, 'upload'
                ), 'folder'
            ),
            (
                FileVersionInfo('bs', 'folder/b.txt', 150, 'text/plain', 'sha1', {}, 1500, 'start'),
                'folder'
            ),
            (
                FileVersionInfo(
                    'b1', 'folder/b.txt', 100, 'text/plain', 'sha1',
                    {'src_last_modified_millis': 1000}, 6666, 'upload'
                ), 'folder'
            ),
        ]
        self.assertEqual(
            [
                "File(a.txt, [FileVersion('a2', 'folder/a.txt', 2000, 'upload'), FileVersion('a1', 'folder/a.txt', 1000, 'upload')])",
                "File(b.txt, [FileVersion('b2', 'folder/b.txt', 2000, 'upload'), FileVersion('b1', 'folder/b.txt', 1000, 'upload')])",
            ], [str(f) for f in self.b2_folder.all_files(self.reporter)]
        )


class FakeFolder(AbstractFolder):
    def __init__(self, f_type, files):
        self.f_type = f_type
        self.files = files

    def all_files(self, reporter, policies_manager=DEFAULT_SCAN_MANAGER):
        for single_file in self.files:
            if single_file.name.endswith('/'):
                if policies_manager.should_exclude_directory(single_file.name):
                    continue
            else:
                if policies_manager.should_exclude_file(single_file.name):
                    continue
            yield single_file

    def folder_type(self):
        return self.f_type

    def make_full_path(self, name):
        if self.f_type == 'local':
            return '/dir/' + name
        else:
            return 'folder/' + name

    def __str__(self):
        return '%s(%s, %s)' % (self.__class__.__name__, self.f_type, self.make_full_path(''))


class TestParseSyncFolder(TestBase):
    def test_b2_double_slash(self):
        self._check_one('B2Folder(my-bucket, folder/path)', 'b2://my-bucket/folder/path')

    def test_b2_no_double_slash(self):
        self._check_one('B2Folder(my-bucket, folder/path)', 'b2:my-bucket/folder/path')

    def test_b2_trailing_slash(self):
        self._check_one('B2Folder(my-bucket, a)', 'b2://my-bucket/a/')

    def test_b2_no_folder(self):
        self._check_one('B2Folder(my-bucket, )', 'b2://my-bucket')
        self._check_one('B2Folder(my-bucket, )', 'b2://my-bucket/')

    def test_local(self):
        if platform.system() == 'Windows':
            expected = 'LocalFolder(C:\\foo)'
        else:
            expected = 'LocalFolder(/foo)'
        self._check_one(expected, '/foo')

    def test_local_trailing_slash(self):
        if platform.system() == 'Windows':
            expected = 'LocalFolder(C:\\foo)'
        else:
            expected = 'LocalFolder(/foo)'
        self._check_one(expected, '/foo/')

    def _check_one(self, expected, to_parse):
        api = MagicMock()
        self.assertEqual(expected, str(parse_sync_folder(six.u(to_parse), api)))


class TestZipFolders(TestSync):
    def test_empty(self):
        folder_a = FakeFolder('b2', [])
        folder_b = FakeFolder('b2', [])
        self.assertEqual([], list(zip_folders(folder_a, folder_b, self.reporter)))

    def test_one_empty(self):
        file_a1 = File("a.txt", [FileVersion("a", "a", 100, "upload", 10)])
        folder_a = FakeFolder('b2', [file_a1])
        folder_b = FakeFolder('b2', [])
        self.assertEqual([(file_a1, None)], list(zip_folders(folder_a, folder_b, self.reporter)))

    def test_two(self):
        file_a1 = File("a.txt", [FileVersion("a", "a", 100, "upload", 10)])
        file_a2 = File("b.txt", [FileVersion("b", "b", 100, "upload", 10)])
        file_a3 = File("d.txt", [FileVersion("c", "c", 100, "upload", 10)])
        file_a4 = File("f.txt", [FileVersion("f", "f", 100, "upload", 10)])
        file_b1 = File("b.txt", [FileVersion("b", "b", 200, "upload", 10)])
        file_b2 = File("e.txt", [FileVersion("e", "e", 200, "upload", 10)])
        folder_a = FakeFolder('b2', [file_a1, file_a2, file_a3, file_a4])
        folder_b = FakeFolder('b2', [file_b1, file_b2])
        self.assertEqual(
            [
                (file_a1, None), (file_a2, file_b1), (file_a3, None), (None, file_b2),
                (file_a4, None)
            ], list(zip_folders(folder_a, folder_b, self.reporter))
        )

    def test_pass_reporter_to_folder(self):
        """
        Check that the zip_folders() function passes the reporter through
        to both folders.
        """
        folder_a = MagicMock()
        folder_b = MagicMock()
        folder_a.all_files = MagicMock(return_value=iter([]))
        folder_b.all_files = MagicMock(return_value=iter([]))
        self.assertEqual([], list(zip_folders(folder_a, folder_b, self.reporter)))
        folder_a.all_files.assert_called_once_with(self.reporter, DEFAULT_SCAN_MANAGER)
        folder_b.all_files.assert_called_once_with(self.reporter)


class FakeArgs(object):
    """
    Can be passed to sync code to simulate command-line options.
    """

    def __init__(
        self,
        delete=False,
        keepDays=None,
        skipNewer=False,
        replaceNewer=False,
        compareVersions=None,
        compareThreshold=None,
        excludeRegex=None,
        excludeDirRegex=None,
        includeRegex=None,
        debugLogs=True,
        dryRun=False,
        allowEmptySource=False,
        excludeAllSymlinks=False,
    ):
        self.delete = delete
        self.keepDays = keepDays
        self.skipNewer = skipNewer
        self.replaceNewer = replaceNewer
        self.compareVersions = compareVersions
        self.compareThreshold = compareThreshold
        if excludeRegex is None:
            excludeRegex = []
        self.excludeRegex = excludeRegex
        if includeRegex is None:
            includeRegex = []
        self.includeRegex = includeRegex
        if excludeDirRegex is None:
            excludeDirRegex = []
        self.excludeDirRegex = excludeDirRegex
        self.debugLogs = debugLogs
        self.dryRun = dryRun
        self.allowEmptySource = allowEmptySource
        self.excludeAllSymlinks = excludeAllSymlinks


def b2_file(name, mod_times, size=10):
    """
    Makes a File object for a b2 file, with one FileVersion for
    each modification time given in mod_times.

    Positive modification times are uploads, and negative modification
    times are hides.  It's a hack, but it works.

        b2_file('a.txt', [300, -200, 100])

    Is the same as:

        File(
            'a.txt',
            [
               FileVersion('id_a_300', 'a.txt', 300, 'upload'),
               FileVersion('id_a_200', 'a.txt', 200, 'hide'),
               FileVersion('id_a_100', 'a.txt', 100, 'upload')
            ]
        )
    """
    versions = [
        FileVersion(
            'id_%s_%d' % (name[0], abs(mod_time)),
            'folder/' + name,
            abs(mod_time),
            'upload' if 0 < mod_time else 'hide',
            size,
        ) for mod_time in mod_times
    ]  # yapf disable
    return File(name, versions)


def local_file(name, mod_times, size=10):
    """
    Makes a File object for a b2 file, with one FileVersion for
    each modification time given in mod_times.
    """
    versions = [
        FileVersion('/dir/%s' % (name,), name, mod_time, 'upload', size) for mod_time in mod_times
    ]
    return File(name, versions)


class TestExclusions(TestSync):
    def _check_folder_sync(self, expected_actions, fakeargs):
        # only local
        file_a = local_file('a.txt', [100])
        file_b = local_file('b.txt', [100])
        file_d = local_file('d/d.txt', [100])
        file_e = local_file('e/e.incl', [100])

        # both local and remote
        file_bi = local_file('b.txt.incl', [100])
        file_z = local_file('z.incl', [100])

        # only remote
        file_c = local_file('c.txt', [100])

        local_folder = FakeFolder('local', [file_a, file_b, file_d, file_e, file_bi, file_z])
        b2_folder = FakeFolder('b2', [file_bi, file_c, file_z])

        policies_manager = ScanPoliciesManager(
            exclude_dir_regexes=fakeargs.excludeDirRegex,
            exclude_file_regexes=fakeargs.excludeRegex,
            include_file_regexes=fakeargs.includeRegex,
            exclude_all_symlinks=fakeargs.excludeAllSymlinks
        )
        actions = list(
            make_folder_sync_actions(
                local_folder, b2_folder, fakeargs, TODAY, self.reporter, policies_manager
            )
        )
        self.assertEqual(expected_actions, [str(a) for a in actions])

    def test_file_exclusions_with_delete(self):
        expected_actions = [
            'b2_upload(/dir/a.txt, folder/a.txt, 100)',
            'b2_delete(folder/b.txt.incl, /dir/b.txt.incl, )',
            'b2_delete(folder/c.txt, /dir/c.txt, )',
            'b2_upload(/dir/d/d.txt, folder/d/d.txt, 100)',
            'b2_upload(/dir/e/e.incl, folder/e/e.incl, 100)',
        ]
        self._check_folder_sync(expected_actions, FakeArgs(delete=True, excludeRegex=["b\\.txt"]))

    def test_file_exclusions_inclusions_with_delete(self):
        expected_actions = [
            'b2_upload(/dir/a.txt, folder/a.txt, 100)',
            'b2_delete(folder/b.txt.incl, /dir/b.txt.incl, )',
            'b2_delete(folder/c.txt, /dir/c.txt, )',
            'b2_upload(/dir/d/d.txt, folder/d/d.txt, 100)',
            'b2_upload(/dir/e/e.incl, folder/e/e.incl, 100)',
            'b2_upload(/dir/b.txt.incl, folder/b.txt.incl, 100)',
        ]
        fakeargs = FakeArgs(delete=True, excludeRegex=["b\\.txt"], includeRegex=[".*\\.incl"])
        self._check_folder_sync(expected_actions, fakeargs)


class TestMakeSyncActions(TestSync):
    def test_illegal_b2_to_b2(self):
        b2_folder = FakeFolder('b2', [])
        with self.assertRaises(NotImplementedError):
            list(make_folder_sync_actions(b2_folder, b2_folder, FakeArgs(), 0, self.reporter))

    def test_illegal_local_to_local(self):
        local_folder = FakeFolder('local', [])
        with self.assertRaises(NotImplementedError):
            list(make_folder_sync_actions(local_folder, local_folder, FakeArgs(), 0, self.reporter))

    def test_illegal_skip_and_replace(self):
        with self.assertRaises(CommandError):
            self._check_local_to_b2(None, None, FakeArgs(skipNewer=True, replaceNewer=True), [])

    def test_illegal_delete_and_keep_days(self):
        with self.assertRaises(CommandError):
            self._check_local_to_b2(None, None, FakeArgs(delete=True, keepDays=1), [])

    # src: absent, dst: absent

    def test_empty_b2(self):
        self._check_local_to_b2(None, None, FakeArgs(), [])

    def test_empty_local(self):
        self._check_b2_to_local(None, None, FakeArgs(), [])

    # src: present, dst: absent

    def test_not_there_b2(self):
        src_file = local_file('a.txt', [100])
        self._check_local_to_b2(
            src_file, None, FakeArgs(), ['b2_upload(/dir/a.txt, folder/a.txt, 100)']
        )

    def test_dir_not_there_b2_keepdays(self):  # reproduces issue 220
        src_file = b2_file('directory/a.txt', [100])
        actions = ['b2_upload(/dir/directory/a.txt, folder/directory/a.txt, 100)']
        self._check_local_to_b2(src_file, None, FakeArgs(keepDays=1), actions)

    def test_dir_not_there_b2_delete(self):  # reproduces issue 218
        src_file = b2_file('directory/a.txt', [100])
        actions = ['b2_upload(/dir/directory/a.txt, folder/directory/a.txt, 100)']
        self._check_local_to_b2(src_file, None, FakeArgs(delete=True), actions)

    def test_not_there_local(self):
        src_file = b2_file('a.txt', [100])
        actions = ['b2_download(folder/a.txt, id_a_100, /dir/a.txt, 100)']
        self._check_b2_to_local(src_file, None, FakeArgs(), actions)

    # src: absent, dst: present

    def test_no_delete_b2(self):
        dst_file = b2_file('a.txt', [100])
        self._check_local_to_b2(None, dst_file, FakeArgs(), [])

    def test_no_delete_local(self):
        dst_file = local_file('a.txt', [100])
        self._check_b2_to_local(None, dst_file, FakeArgs(), [])

    def test_delete_b2(self):
        dst_file = b2_file('a.txt', [100])
        actions = ['b2_delete(folder/a.txt, id_a_100, )']
        self._check_local_to_b2(None, dst_file, FakeArgs(delete=True), actions)

    def test_delete_large_b2(self):
        dst_file = b2_file('a.txt', [100])
        actions = ['b2_delete(folder/a.txt, id_a_100, )']
        self._check_local_to_b2(None, dst_file, FakeArgs(delete=True), actions)

    def test_delete_b2_multiple_versions(self):
        dst_file = b2_file('a.txt', [100, 200])
        actions = [
            'b2_delete(folder/a.txt, id_a_100, )',
            'b2_delete(folder/a.txt, id_a_200, (old version))'
        ]
        self._check_local_to_b2(None, dst_file, FakeArgs(delete=True), actions)

    def test_delete_hide_b2_multiple_versions(self):
        dst_file = b2_file('a.txt', [TODAY, TODAY - 2 * DAY, TODAY - 4 * DAY])
        actions = [
            'b2_hide(folder/a.txt)', 'b2_delete(folder/a.txt, id_a_8294400000, (old version))'
        ]
        self._check_local_to_b2(None, dst_file, FakeArgs(keepDays=1), actions)

    def test_delete_hide_b2_multiple_versions_old(self):
        dst_file = b2_file('a.txt', [TODAY - 1 * DAY, TODAY - 3 * DAY, TODAY - 5 * DAY])
        actions = [
            'b2_hide(folder/a.txt)', 'b2_delete(folder/a.txt, id_a_8208000000, (old version))'
        ]
        self._check_local_to_b2(None, dst_file, FakeArgs(keepDays=2), actions)

    def test_already_hidden_multiple_versions_keep(self):
        dst_file = b2_file('a.txt', [-TODAY, TODAY - 2 * DAY, TODAY - 4 * DAY])
        self._check_local_to_b2(None, dst_file, FakeArgs(), [])

    def test_already_hidden_multiple_versions_keep_days(self):
        dst_file = b2_file('a.txt', [-TODAY, TODAY - 2 * DAY, TODAY - 4 * DAY])
        actions = ['b2_delete(folder/a.txt, id_a_8294400000, (old version))']
        self._check_local_to_b2(None, dst_file, FakeArgs(keepDays=1), actions)

    def test_already_hidden_multiple_versions_keep_days_one_old(self):
        # The 6-day-old file should be preserved, because it was visible
        # 5 days ago.
        dst_file = b2_file('a.txt', [-(TODAY - 2 * DAY), TODAY - 4 * DAY, TODAY - 6 * DAY])
        actions = []
        self._check_local_to_b2(None, dst_file, FakeArgs(keepDays=5), actions)

    def test_already_hidden_multiple_versions_keep_days_two_old(self):
        dst_file = b2_file('a.txt', [-(TODAY - 2 * DAY), TODAY - 4 * DAY, TODAY - 6 * DAY])
        actions = ['b2_delete(folder/a.txt, id_a_8121600000, (old version))']
        self._check_local_to_b2(None, dst_file, FakeArgs(keepDays=2), actions)

    def test_already_hidden_multiple_versions_keep_days_delete_hide_marker(self):
        dst_file = b2_file('a.txt', [-(TODAY - 2 * DAY), TODAY - 4 * DAY, TODAY - 6 * DAY])
        actions = [
            'b2_delete(folder/a.txt, id_a_8467200000, (hide marker))',
            'b2_delete(folder/a.txt, id_a_8294400000, (old version))',
            'b2_delete(folder/a.txt, id_a_8121600000, (old version))'
        ]
        self._check_local_to_b2(None, dst_file, FakeArgs(keepDays=1), actions)

    def test_already_hidden_multiple_versions_keep_days_old_delete(self):
        dst_file = b2_file('a.txt', [-TODAY + 2 * DAY, TODAY - 4 * DAY])
        actions = [
            'b2_delete(folder/a.txt, id_a_8467200000, (hide marker))',
            'b2_delete(folder/a.txt, id_a_8294400000, (old version))'
        ]
        self._check_local_to_b2(None, dst_file, FakeArgs(keepDays=1), actions)

    def test_already_hidden_multiple_versions_delete(self):
        dst_file = b2_file('a.txt', [-TODAY, TODAY - 2 * DAY, TODAY - 4 * DAY])
        actions = [
            'b2_delete(folder/a.txt, id_a_8640000000, (hide marker))',
            'b2_delete(folder/a.txt, id_a_8467200000, (old version))',
            'b2_delete(folder/a.txt, id_a_8294400000, (old version))'
        ]
        self._check_local_to_b2(None, dst_file, FakeArgs(delete=True), actions)

    def test_delete_local(self):
        dst_file = local_file('a.txt', [100])
        self._check_b2_to_local(None, dst_file, FakeArgs(delete=True), ['local_delete(/dir/a.txt)'])

    # src same as dst

    def test_same_b2(self):
        src_file = local_file('a.txt', [100])
        dst_file = b2_file('a.txt', [100])
        self._check_local_to_b2(src_file, dst_file, FakeArgs(), [])

    def test_same_local(self):
        src_file = b2_file('a.txt', [100])
        dst_file = local_file('a.txt', [100])
        self._check_b2_to_local(src_file, dst_file, FakeArgs(), [])

    def test_same_leave_old_versions(self):
        src_file = local_file('a.txt', [TODAY])
        dst_file = b2_file('a.txt', [TODAY, TODAY - 3 * DAY])
        self._check_local_to_b2(src_file, dst_file, FakeArgs(), [])

    def test_same_clean_old_versions(self):
        src_file = local_file('a.txt', [TODAY - 3 * DAY])
        dst_file = b2_file('a.txt', [TODAY - 3 * DAY, TODAY - 4 * DAY])
        actions = ['b2_delete(folder/a.txt, id_a_8294400000, (old version))']
        self._check_local_to_b2(src_file, dst_file, FakeArgs(keepDays=1), actions)

    def test_keep_days_no_change_with_old_file(self):
        src_file = local_file('a.txt', [TODAY - 3 * DAY])
        dst_file = b2_file('a.txt', [TODAY - 3 * DAY])
        self._check_local_to_b2(src_file, dst_file, FakeArgs(keepDays=1), [])

    def test_same_delete_old_versions(self):
        src_file = local_file('a.txt', [TODAY])
        dst_file = b2_file('a.txt', [TODAY, TODAY - 3 * DAY])
        actions = ['b2_delete(folder/a.txt, id_a_8380800000, (old version))']
        self._check_local_to_b2(src_file, dst_file, FakeArgs(delete=True), actions)

    # src newer than dst

    def test_newer_b2(self):
        src_file = local_file('a.txt', [200])
        dst_file = b2_file('a.txt', [100])
        actions = ['b2_upload(/dir/a.txt, folder/a.txt, 200)']
        self._check_local_to_b2(src_file, dst_file, FakeArgs(), actions)

    def test_newer_b2_clean_old_versions(self):
        src_file = local_file('a.txt', [TODAY])
        dst_file = b2_file('a.txt', [TODAY - 1 * DAY, TODAY - 3 * DAY, TODAY - 5 * DAY])
        actions = [
            'b2_upload(/dir/a.txt, folder/a.txt, 8640000000)',
            'b2_delete(folder/a.txt, id_a_8208000000, (old version))'
        ]
        self._check_local_to_b2(src_file, dst_file, FakeArgs(keepDays=2), actions)

    def test_newer_b2_delete_old_versions(self):
        src_file = local_file('a.txt', [TODAY])
        dst_file = b2_file('a.txt', [TODAY - 1 * DAY, TODAY - 3 * DAY])
        actions = [
            'b2_upload(/dir/a.txt, folder/a.txt, 8640000000)',
            'b2_delete(folder/a.txt, id_a_8553600000, (old version))',
            'b2_delete(folder/a.txt, id_a_8380800000, (old version))'
        ]  # yapf disable
        self._check_local_to_b2(src_file, dst_file, FakeArgs(delete=True), actions)

    def test_newer_local(self):
        src_file = b2_file('a.txt', [200])
        dst_file = local_file('a.txt', [100])
        actions = ['b2_download(folder/a.txt, id_a_200, /dir/a.txt, 200)']
        self._check_b2_to_local(src_file, dst_file, FakeArgs(delete=True), actions)

    # src older than dst

    def test_older_b2(self):
        src_file = local_file('a.txt', [100])
        dst_file = b2_file('a.txt', [200])
        try:
            self._check_local_to_b2(src_file, dst_file, FakeArgs(), [])
            self.fail('should have raised DestFileNewer')
        except DestFileNewer as e:
            self.assertEqual(
                'source file is older than destination: local://a.txt with a time of 100 cannot be synced to b2://a.txt with a time of 200, unless --skipNewer or --replaceNewer is provided',
                str(e)
            )

    def test_older_b2_skip(self):
        src_file = local_file('a.txt', [100])
        dst_file = b2_file('a.txt', [200])
        self._check_local_to_b2(src_file, dst_file, FakeArgs(skipNewer=True), [])

    def test_older_b2_replace(self):
        src_file = local_file('a.txt', [100])
        dst_file = b2_file('a.txt', [200])
        actions = ['b2_upload(/dir/a.txt, folder/a.txt, 100)']
        self._check_local_to_b2(src_file, dst_file, FakeArgs(replaceNewer=True), actions)

    def test_older_b2_replace_delete(self):
        src_file = local_file('a.txt', [100])
        dst_file = b2_file('a.txt', [200])
        args = FakeArgs(replaceNewer=True, delete=True)
        actions = [
            'b2_upload(/dir/a.txt, folder/a.txt, 100)',
            'b2_delete(folder/a.txt, id_a_200, (old version))'
        ]
        self._check_local_to_b2(src_file, dst_file, args, actions)

    def test_older_local(self):
        src_file = b2_file('directory/a.txt', [100])
        dst_file = local_file('directory/a.txt', [200])
        try:
            self._check_b2_to_local(src_file, dst_file, FakeArgs(), [])
            self.fail('should have raised DestFileNewer')
        except DestFileNewer as e:
            self.assertEqual(
                'source file is older than destination: b2://directory/a.txt with a time of 100 cannot be synced to local://directory/a.txt with a time of 200, unless --skipNewer or --replaceNewer is provided',
                str(e)
            )

    def test_older_local_skip(self):
        src_file = b2_file('a.txt', [100])
        dst_file = local_file('a.txt', [200])
        self._check_b2_to_local(src_file, dst_file, FakeArgs(skipNewer=True), [])

    def test_older_local_replace(self):
        src_file = b2_file('a.txt', [100])
        dst_file = local_file('a.txt', [200])
        actions = ['b2_download(folder/a.txt, id_a_100, /dir/a.txt, 100)']
        self._check_b2_to_local(src_file, dst_file, FakeArgs(replaceNewer=True), actions)

    # compareVersions option

    def test_compare_b2_none_newer(self):
        src_file = local_file('a.txt', [200])
        dst_file = b2_file('a.txt', [100])
        self._check_local_to_b2(src_file, dst_file, FakeArgs(compareVersions='none'), [])

    def test_compare_b2_none_older(self):
        src_file = local_file('a.txt', [100])
        dst_file = b2_file('a.txt', [200])
        self._check_local_to_b2(src_file, dst_file, FakeArgs(compareVersions='none'), [])

    def test_compare_b2_size_equal(self):
        src_file = local_file('a.txt', [200], size=10)
        dst_file = b2_file('a.txt', [100], size=10)
        self._check_local_to_b2(src_file, dst_file, FakeArgs(compareVersions='size'), [])

    def test_compare_b2_size_not_equal(self):
        src_file = local_file('a.txt', [200], size=11)
        dst_file = b2_file('a.txt', [100], size=10)
        actions = ['b2_upload(/dir/a.txt, folder/a.txt, 200)']
        self._check_local_to_b2(src_file, dst_file, FakeArgs(compareVersions='size'), actions)

    def test_compare_b2_size_not_equal_delete(self):
        src_file = local_file('a.txt', [200], size=11)
        dst_file = b2_file('a.txt', [100], size=10)
        args = FakeArgs(compareVersions='size', delete=True)
        actions = [
            'b2_upload(/dir/a.txt, folder/a.txt, 200)',
            'b2_delete(folder/a.txt, id_a_100, (old version))'
        ]
        self._check_local_to_b2(src_file, dst_file, args, actions)

    # helper methods

    def _check_local_to_b2(self, src_file, dst_file, args, expected_actions):
        self._check_one_file('local', src_file, 'b2', dst_file, args, expected_actions)

    def _check_b2_to_local(self, src_file, dst_file, args, expected_actions):
        self._check_one_file('b2', src_file, 'local', dst_file, args, expected_actions)

    def _check_one_file(self, src_type, src_file, dst_type, dst_file, args, expected_actions):
        """
        Checks the actions generated for one file.  The file may or may not
        exist at the source, and may or may not exist at the destination.
        Passing in None means that the file does not exist.

        The source and destination files may have multiple versions.
        """
        src_folder = FakeFolder(src_type, [src_file] if src_file else [])
        dst_folder = FakeFolder(dst_type, [dst_file] if dst_file else [])
        actions = list(make_folder_sync_actions(src_folder, dst_folder, args, TODAY, self.reporter))
        action_strs = [str(a) for a in actions]
        if expected_actions != action_strs:
            print('Expected:')
            for a in expected_actions:
                print('   ', a)
            print('Actual:')
            for a in action_strs:
                print('   ', a)
        self.assertEqual(expected_actions, [str(a) for a in actions])


class TestBoundedQueueExecutor(TestBase):
    def test_run_more_than_queue_size(self):
        """
        Makes sure that the executor will run more jobs that the
        queue size, which ensures that the semaphore gets released,
        even if an exception is thrown.
        """
        raw_executor = futures.ThreadPoolExecutor(1)
        bounded_executor = BoundedQueueExecutor(raw_executor, 5)

        class Counter(object):
            """
            Counts how many times run() is called.
            """

            def __init__(self):
                self.counter = 0

            def run(self):
                """
                Always increments the counter.  Sometimes raises an exception.
                """
                self.counter += 1
                if self.counter % 2 == 0:
                    raise Exception('test')

        counter = Counter()
        for _ in six.moves.range(10):
            bounded_executor.submit(counter.run)
        bounded_executor.shutdown()
        self.assertEqual(10, counter.counter)

    def test_wait_for_running_jobs(self):
        """
        Makes sure that no more than queue_limit workers are
        running at once, which checks that the semaphore is
        acquired before submitting an action.
        """
        raw_executor = futures.ThreadPoolExecutor(2)
        bounded_executor = BoundedQueueExecutor(raw_executor, 1)
        assert_equal = self.assertEqual

        class CountAtOnce(object):
            """
            Counts how many threads are running at once.
            There should never be more than 1 because that's
            the limit on the bounded executor.
            """

            def __init__(self):
                self.running_at_once = 0
                self.lock = threading.Lock()

            def run(self):
                with self.lock:
                    self.running_at_once += 1
                    assert_equal(1, self.running_at_once)
                # While we are sleeping here, no other actions should start
                # running.  If they do, they will increment the counter and
                # fail the above assertion.
                time.sleep(0.05)
                with self.lock:
                    self.running_at_once -= 1
                self.counter += 1
                if self.counter % 2 == 0:
                    raise Exception('test')

        count_at_once = CountAtOnce()
        for _ in six.moves.range(5):
            bounded_executor.submit(count_at_once.run)
        bounded_executor.shutdown()


if __name__ == '__main__':
    unittest.main()
