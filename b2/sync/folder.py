######################################################################
#
# File: b2/sync/folder.py
#
# Copyright 2016 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import logging
import os
import six
import sys

from abc import ABCMeta, abstractmethod
from b2.exception import CommandError
from .exception import EnvironmentEncodingError
from .file import File, FileVersion
from .scan_policies import DEFAULT_SCAN_MANAGER
from ..raw_api import SRC_LAST_MODIFIED_MILLIS
from ..utils import fix_windows_path_limit, is_file_readable

logger = logging.getLogger(__name__)


@six.add_metaclass(ABCMeta)
class AbstractFolder(object):
    """
    Interface to a folder full of files, which might be a B2 bucket,
    a virtual folder in a B2 bucket, or a directory on a local file
    system.

    Files in B2 may have multiple versions, while files in local
    folders have just one.
    """

    @abstractmethod
    def all_files(self, reporter, policies_manager):
        """
        Returns an iterator over all of the files in the folder, in
        the order that B2 uses.

        It also performs filtering using policies manager.

        No matter what the folder separator on the local file system
        is, "/" is used in the returned file names.

        If a file is found, but does not exist (for example due to
        a broken symlink or a race), reporter will be informed about
        each such problem.
        """

    @abstractmethod
    def folder_type(self):
        """
        Returns one of:  'b2', 'local'
        """

    @abstractmethod
    def make_full_path(self, file_name):
        """
        Only for local folders, returns the full path to the file.
        """


def join_b2_path(b2_dir, b2_name):
    """
    Like os.path.join, but for B2 file names where the root directory is called ''.
    """
    if b2_dir == '':
        return b2_name
    else:
        return b2_dir + '/' + b2_name


class LocalFolder(AbstractFolder):
    """
    Folder interface to a directory on the local machine.
    """

    def __init__(self, root):
        """
        Initializes a new folder.

        :param root: Path to the root of the local folder.  Must be unicode.
        """
        if not isinstance(root, six.text_type):
            raise ValueError('folder path should be unicode: %s' % repr(root))
        self.root = fix_windows_path_limit(os.path.abspath(root))

    def folder_type(self):
        return 'local'

    def all_files(self, reporter, policies_manager=DEFAULT_SCAN_MANAGER):
        for file_object in self._walk_relative_paths(self.root, '', reporter, policies_manager):
            yield file_object

    def make_full_path(self, file_name):
        return os.path.join(self.root, file_name.replace('/', os.path.sep))

    def ensure_present(self):
        """
        Makes sure that the directory exists.
        """
        if not os.path.exists(self.root):
            try:
                os.mkdir(self.root)
            except:
                raise Exception('unable to create directory %s' % (self.root,))
        elif not os.path.isdir(self.root):
            raise Exception('%s is not a directory' % (self.root,))

    def ensure_non_empty(self):
        """
        Makes sure that the directory exists and is non-empty.
        """
        self.ensure_present()

        if not os.listdir(self.root):
            raise CommandError(
                'Directory %s is empty.  Use --allowEmptySource to sync anyway.' % (self.root,)
            )

    def _walk_relative_paths(self, local_dir, b2_dir, reporter, policies_manager):
        """
        Yields a File object for each of the files anywhere under this folder, in the
        order they would appear in B2, unless the path is excluded by policies manager.
        :param local_dir: The local directory to list files in
        :param b2_dir: The B2 path of this directory, or '' if at the root.
        :param reporter: A place to report errors
        :param policies_manager: A manager for polices scan results
        :return:
        """
        if not isinstance(local_dir, six.text_type):
            raise ValueError('folder path should be unicode: %s' % repr(local_dir))

        # Collect the names.  We do this before returning any results, because
        # directories need to sort as if their names end in '/'.
        #
        # With a directory containing 'a', 'a.txt', and 'a0.txt', with 'a' being
        # a directory containing 'b.txt', and 'c.txt', the results returned
        # should be:
        #
        #    a.txt
        #    a/b.txt
        #    a/c.txt
        #    a0.txt
        #
        # This is because in Unicode '.' comes before '/', which comes before '0'.
        names = []  # list of (name, local_path, b2_path)
        for name in os.listdir(local_dir):
            # We expect listdir() to return unicode if dir_path is unicode.
            # If the file name is not valid, based on the file system
            # encoding, then listdir() will return un-decoded str/bytes.
            if not isinstance(name, six.text_type):
                name = self._handle_non_unicode_file_name(name)

            if '/' in name:
                raise Exception(
                    "sync does not support file names that include '/': %s in dir %s" %
                    (name, local_dir)
                )

            local_path = os.path.join(local_dir, name)
            b2_path = join_b2_path(b2_dir, name)

            # Skip broken symlinks or other inaccessible files
            if not is_file_readable(local_path, reporter):
                continue

            if os.path.isdir(local_path):
                name += six.u('/')
                if policies_manager.should_exclude_directory(b2_path):
                    continue
            else:
                if policies_manager.should_exclude_file(b2_path):
                    continue

            names.append((name, local_path, b2_path))

        # Yield all of the answers.
        #
        # Sorting the list of triples puts them in the right order because 'name',
        # the sort key, is the first thing in the triple.
        for (name, local_path, b2_path) in sorted(names):
            if name.endswith('/'):
                for subdir_file in self._walk_relative_paths(
                    local_path, b2_path, reporter, policies_manager
                ):
                    yield subdir_file
            else:
                # Check that the file still exists and is accessible, since it can take a long time
                # to iterate through large folders
                if is_file_readable(local_path, reporter):
                    file_mod_time = int(os.path.getmtime(local_path) * 1000)
                    file_size = os.path.getsize(local_path)
                    version = FileVersion(local_path, b2_path, file_mod_time, 'upload', file_size)
                    yield File(b2_path, [version])

    def _handle_non_unicode_file_name(self, name):
        """
        Decide what to do with a name returned from os.listdir()
        that isn't unicode.  We think that this only happens when
        the file name can't be decoded using the file system
        encoding.  Just in case that's not true, we'll allow all-ascii
        names.
        """
        # if it's all ascii, allow it
        if six.PY2:
            if all(ord(c) <= 127 for c in name):
                return name
        else:
            if all(b <= 127 for b in name):
                return name
        raise EnvironmentEncodingError(repr(name), sys.getfilesystemencoding())

    def __repr__(self):
        return 'LocalFolder(%s)' % (self.root,)


class B2Folder(AbstractFolder):
    """
    Folder interface to B2.
    """

    def __init__(self, bucket_name, folder_name, api):
        self.bucket_name = bucket_name
        self.folder_name = folder_name
        self.bucket = api.get_bucket_by_name(bucket_name)
        self.prefix = '' if self.folder_name == '' else self.folder_name + '/'

    def all_files(self, reporter, policies_manager=DEFAULT_SCAN_MANAGER):
        current_name = None
        current_versions = []
        for (file_version_info, folder_name) in self.bucket.ls(
            self.folder_name, show_versions=True, recursive=True, fetch_count=1000
        ):
            assert file_version_info.file_name.startswith(self.prefix)
            if file_version_info.action == 'start':
                continue
            file_name = file_version_info.file_name[len(self.prefix):]

            if policies_manager.should_exclude_file(file_name):
                continue

            if current_name != file_name and current_name is not None:
                yield File(current_name, current_versions)
                current_versions = []
            file_info = file_version_info.file_info
            if SRC_LAST_MODIFIED_MILLIS in file_info:
                mod_time_millis = int(file_info[SRC_LAST_MODIFIED_MILLIS])
            else:
                mod_time_millis = file_version_info.upload_timestamp
            assert file_version_info.size is not None
            file_version = FileVersion(
                file_version_info.id_, file_version_info.file_name, mod_time_millis,
                file_version_info.action, file_version_info.size
            )
            current_versions.append(file_version)
            current_name = file_name
        if current_name is not None:
            yield File(current_name, current_versions)

    def folder_type(self):
        return 'b2'

    def make_full_path(self, file_name):
        if self.folder_name == '':
            return file_name
        else:
            return self.folder_name + '/' + file_name

    def __str__(self):
        return 'B2Folder(%s, %s)' % (self.bucket_name, self.folder_name)
