######################################################################
#
# File: b2/sync/folder.py
#
# Copyright 2016 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from abc import ABCMeta, abstractmethod
import os
import platform
import sys

import errno
import six

from .exception import EnvironmentEncodingError
from .file import File, FileVersion
from ..raw_api import SRC_LAST_MODIFIED_MILLIS


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
    def all_files(self, reporter):
        """
        Returns an iterator over all of the files in the folder, in
        the order that B2 uses.

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

        self.root = os.path.abspath(root)

    @property
    def prefix_len(self):
        """

            Prefix length calculation:
                https://github.com/Backblaze/B2_Command_Line_Tool/issues/334

            Using os.path.abspath, there are three cases to handle here:
                  * root paths with a drive specification (windows) -> length=3,
                  * root paths without a drive specification (not windows) -> length=1,
                  * non root paths -> length=(len(root) + 1)

            Furthermore, there is a trailing separator only if the path denotes a root filesystem,
                hence following a suggestion from @svonohr and the following SO link:
                    https://stackoverflow.com/q/2736144/1946984
                the conjunction of os.path.join(..., '') and os.path.abspath will guarantee
                that the prefix length includes the trailing separator for both the root path
                cases and arbitrary paths.

        """

        return len(os.path.join(self.root, ''))

    def folder_type(self):
        return 'local'

    def all_files(self, reporter):
        for relative_path in self._walk_relative_paths(self.prefix_len, self.root, reporter):
            yield self._make_file(relative_path)

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
            raise Exception('Directory %s is empty' % (self.root,))

    def _walk_relative_paths(self, prefix_len, dir_path, reporter):
        """
        Yields all of the file names anywhere under this folder, in the
        order they would appear in B2.
        """
        if not isinstance(dir_path, six.text_type):
            raise ValueError('folder path should be unicode: %s' % repr(dir_path))

        # Collect the names
        # We know the dir_path is unicode, which will cause os.listdir() to
        # return unicode paths.
        names = {}  # name to (full_path, relative path)
        dirs = set()  # subset of names that are directories
        for name in os.listdir(dir_path):
            # We expect listdir() to return unicode if dir_path is unicode.
            # If the file name is not valid, based on the file system
            # encoding, then listdir() will return un-decoded str/bytes.
            if not isinstance(name, six.text_type):
                name = self._handle_non_unicode_file_name(name)

            if '/' in name:
                raise Exception(
                    "sync does not support file names that include '/': %s in dir %s" %
                    (name, dir_path)
                )
            full_path = os.path.join(dir_path, name)
            relative_path = full_path[prefix_len:]
            # Skip broken symlinks or other inaccessible files
            if not os.path.exists(full_path):
                if reporter is not None:
                    reporter.local_access_error(full_path)
            elif not os.access(full_path, os.R_OK):
                if reporter is not None:
                    reporter.local_permission_error(full_path)
            else:
                if os.path.isdir(full_path):
                    name += six.u('/')
                    dirs.add(name)
                names[name] = (full_path, relative_path)

        #
        # Check whether a path is a relative symlink.
        #
        def validate(x):
            if not platform.system() == 'Linux':
                return True  # TODO: Not sure about how this should work in Windows.
            try:
                str(os.readlink(x)) not in ['.', '..']
            except OSError as e:
                return e.errno == errno.EINVAL

        # Yield all of the answers
        for name in sorted(names):
            (full_path, relative_path) = names[name]
            if name in dirs:
                if validate(full_path):  # Don't iterate over relative symlinks.
                    for rp in self._walk_relative_paths(prefix_len, full_path, reporter):
                        yield rp
            else:
                yield relative_path

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

    def _make_file(self, relative_path):
        full_path = os.path.join(self.root, relative_path)
        mod_time = int(round(os.path.getmtime(full_path) * 1000))
        slashes_path = six.u('/').join(relative_path.split(os.path.sep))
        version = FileVersion(
            full_path, slashes_path, mod_time, "upload", os.path.getsize(full_path)
        )
        return File(slashes_path, [version])

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

    def all_files(self, reporter):
        current_name = None
        current_versions = []
        for (file_version_info, folder_name) in self.bucket.ls(
            self.folder_name, show_versions=True, recursive=True, fetch_count=1000
        ):
            assert file_version_info.file_name.startswith(self.prefix)
            if file_version_info.action == 'start':
                continue
            file_name = file_version_info.file_name[len(self.prefix):]
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
