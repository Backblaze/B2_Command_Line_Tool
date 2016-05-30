######################################################################
#
# File: b2/sync.py
#
# Copyright 2016 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from __future__ import division

import os
import threading
import time
import re
from abc import (ABCMeta, abstractmethod)

import six

from .download_dest import DownloadDestLocalFile
from .exception import CommandError, DestFileNewer
from .progress import AbstractProgressListener
from .upload_source import UploadSourceLocalFile
from .utils import format_and_scale_number, format_and_scale_fraction, raise_if_shutting_down

try:
    import concurrent.futures as futures
except:
    import futures

ONE_DAY_IN_MS = 24 * 60 * 60 * 1000


class SyncReport(object):
    """
    Handles reporting progress for syncing.

    Prints out each file as it is processed, and puts up a sequence
    of progress bars.

    The progress bars are:
       - Step 1/1: count local files
       - Step 2/2: compare file lists
       - Step 3/3: transfer files

    This class is THREAD SAFE so that it can be used from parallel sync threads.
    """

    # Minimum time between displayed updates
    UPDATE_INTERVAL = 0.1

    def __init__(self, stdout, no_progress):
        self.stdout = stdout
        self.no_progress = no_progress
        self.start_time = time.time()
        self.local_file_count = 0
        self.local_done = False
        self.compare_done = False
        self.compare_count = 0
        self.total_transfer_files = 0  # set in end_compare()
        self.total_transfer_bytes = 0  # set in end_compare()
        self.transfer_files = 0
        self.transfer_bytes = 0
        self.current_line = ''
        self._last_update_time = 0
        self.closed = False
        self.lock = threading.Lock()
        self._update_progress()

    def close(self):
        with self.lock:
            if not self.no_progress:
                self._print_line('', False)
            self.closed = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def error(self, message):
        self.print_completion(message)

    def print_completion(self, message):
        """
        Removes the progress bar, prints a message, and puts the progress
        bar back.
        """
        with self.lock:
            if not self.closed:
                self._print_line(message, True)
                self._last_update_time = 0
                self._update_progress()

    def _update_progress(self):
        raise_if_shutting_down()
        if not self.closed and not self.no_progress:
            now = time.time()
            interval = now - self._last_update_time
            if self.UPDATE_INTERVAL <= interval:
                self._last_update_time = now
                time_delta = time.time() - self.start_time
                rate = 0 if time_delta == 0 else int(self.transfer_bytes / time_delta)
                if not self.local_done:
                    message = ' count: %d files   compare: %d files   updated: %d files   %s   %s' % (
                        self.local_file_count,
                        self.compare_count,
                        self.transfer_files,
                        format_and_scale_number(self.transfer_bytes, 'B'),
                        format_and_scale_number(rate, 'B/s')
                    )  # yapf: disable
                elif not self.compare_done:
                    message = ' compare: %d/%d files   updated: %d files   %s   %s' % (
                        self.compare_count,
                        self.local_file_count,
                        self.transfer_files,
                        format_and_scale_number(self.transfer_bytes, 'B'),
                        format_and_scale_number(rate, 'B/s')
                    )  # yapf: disable
                else:
                    message = ' compare: %d/%d files   updated: %d/%d files   %s   %s' % (
                        self.compare_count,
                        self.local_file_count,
                        self.transfer_files,
                        self.total_transfer_files,
                        format_and_scale_fraction(self.transfer_bytes, self.total_transfer_bytes, 'B'),
                        format_and_scale_number(rate, 'B/s')
                    )  # yapf: disable
                self._print_line(message, False)

    def _print_line(self, line, newline):
        """
        Prints a line to stdout.

        :param line: A string without a \r or \n in it.
        :param newline: True if the output should move to a new line after this one.
        """
        if len(line) < len(self.current_line):
            line += ' ' * (len(self.current_line) - len(line))
        self.stdout.write(line)
        if newline:
            self.stdout.write('\n')
            self.current_line = ''
        else:
            self.stdout.write('\r')
            self.current_line = line
        self.stdout.flush()

    def update_local(self, delta):
        """
        Reports that more local files have been found.
        """
        with self.lock:
            self.local_file_count += delta
            self._update_progress()

    def end_local(self):
        """
        Local file count is done.  Can proceed to step 2.
        """
        with self.lock:
            self.local_done = True
            self._update_progress()

    def update_compare(self, delta):
        """
        Reports that more files have been compared.
        """
        with self.lock:
            self.compare_count += delta
            self._update_progress()

    def end_compare(self, total_transfer_files, total_transfer_bytes):
        with self.lock:
            self.compare_done = True
            self.total_transfer_files = total_transfer_files
            self.total_transfer_bytes = total_transfer_bytes
            self._update_progress()

    def update_transfer(self, file_delta, byte_delta):
        with self.lock:
            self.transfer_files += file_delta
            self.transfer_bytes += byte_delta
            self._update_progress()


class SyncFileReporter(AbstractProgressListener):
    """
    Listens to the progress for a single file and passes info on to a SyncReporter.
    """

    def __init__(self, reporter):
        self.bytes_so_far = 0
        self.reporter = reporter

    def close(self):
        # no more bytes are done, but the file is done
        self.reporter.update_transfer(1, 0)

    def set_total_bytes(self, total_byte_count):
        pass

    def bytes_completed(self, byte_count):
        self.reporter.update_transfer(0, byte_count - self.bytes_so_far)
        self.bytes_so_far = byte_count


def sample_sync_report_run():
    sync_report = SyncReport()

    for i in six.moves.range(20):
        sync_report.update_local(1)
        time.sleep(0.2)
        if i == 10:
            sync_report.print_completion('transferred: a.txt')
        if i % 2 == 0:
            sync_report.update_compare(1)
    sync_report.end_local()

    for i in six.moves.range(10):
        sync_report.update_compare(1)
        time.sleep(0.2)
        if i == 3:
            sync_report.print_completion('transferred: b.txt')
        if i == 4:
            sync_report.update_transfer(25, 25000)
    sync_report.end_compare(50, 50000)

    for i in six.moves.range(25):
        if i % 2 == 0:
            sync_report.print_completion('transferred: %d.txt' % i)
        sync_report.update_transfer(1, 1000)
        time.sleep(0.2)

    sync_report.close()


@six.add_metaclass(ABCMeta)
class AbstractAction(object):
    """
    An action to take, such as uploading, downloading, or deleting
    a file.  Multi-threaded tasks create a sequence of Actions, which
    are then run by a pool of threads.

    An action can depend on other actions completing.  An example of
    this is making sure a CreateBucketAction happens before an
    UploadFileAction.
    """

    def run(self, bucket, reporter):
        raise_if_shutting_down()
        try:
            self.do_action(bucket, reporter)
        except Exception as e:
            reporter.error(str(self) + ": " + repr(e) + ' ' + str(e))

    @abstractmethod
    def get_bytes(self):
        """
        Returns the number of bytes to transfer for this action.
        """

    @abstractmethod
    def do_action(self, bucket, reporter):
        """
        Performs the action, returning only after the action is completed.
        """


class B2UploadAction(AbstractAction):
    def __init__(self, local_full_path, relative_name, b2_file_name, mod_time_millis, size):
        self.local_full_path = local_full_path
        self.relative_name = relative_name
        self.b2_file_name = b2_file_name
        self.mod_time_millis = mod_time_millis
        self.size = size

    def get_bytes(self):
        return self.size

    def do_action(self, bucket, reporter):
        bucket.upload(
            UploadSourceLocalFile(self.local_full_path),
            self.b2_file_name,
            file_info={'src_last_modified_millis': str(self.mod_time_millis)},
            progress_listener=SyncFileReporter(reporter)
        )
        reporter.update_transfer(1, 0)  # bytes reported during transfer
        reporter.print_completion('upload ' + self.relative_name)

    def __str__(self):
        return 'b2_upload(%s, %s, %s)' % (
            self.local_full_path, self.b2_file_name, self.mod_time_millis
        )


class B2HideAction(AbstractAction):
    def __init__(self, relative_name, b2_file_name):
        self.relative_name = relative_name
        self.b2_file_name = b2_file_name

    def get_bytes(self):
        return 0

    def do_action(self, bucket, reporter):
        bucket.hide_file(self.b2_file_name)
        reporter.update_transfer(1, 0)
        reporter.print_completion('hide   ' + self.relative_name)

    def __str__(self):
        return 'b2_hide(%s)' % (self.b2_file_name,)


class B2DownloadAction(AbstractAction):
    def __init__(
        self, relative_name, b2_file_name, file_id, local_full_path, mod_time_millis, file_size
    ):
        self.relative_name = relative_name
        self.b2_file_name = b2_file_name
        self.file_id = file_id
        self.local_full_path = local_full_path
        self.mod_time_millis = mod_time_millis
        self.file_size = file_size

    def get_bytes(self):
        return self.file_size

    def do_action(self, bucket, reporter):
        # Make sure the directory exists
        parent_dir = os.path.dirname(self.local_full_path)
        if not os.path.isdir(parent_dir):
            try:
                os.makedirs(parent_dir)
            except:
                pass
        if not os.path.isdir(parent_dir):
            raise Exception('could not create directory %s' % (parent_dir,))

        # Download the file to a .tmp file
        download_path = self.local_full_path + '.b2.sync.tmp'
        download_dest = DownloadDestLocalFile(download_path, SyncFileReporter(reporter))
        bucket.download_file_by_name(self.b2_file_name, download_dest)

        # Move the file into place
        try:
            os.unlink(self.local_full_path)
        except:
            pass
        os.rename(download_path, self.local_full_path)

        # Report progress
        reporter.print_completion('dnload ' + self.relative_name)

    def __str__(self):
        return (
            'b2_download(%s, %s, %s, %d)' %
            (self.b2_file_name, self.file_id, self.local_full_path, self.mod_time_millis)
        )


class B2DeleteAction(AbstractAction):
    def __init__(self, relative_name, b2_file_name, file_id, note):
        self.relative_name = relative_name
        self.b2_file_name = b2_file_name
        self.file_id = file_id
        self.note = note

    def get_bytes(self):
        return 0

    def do_action(self, bucket, reporter):
        bucket.api.delete_file_version(self.file_id, self.b2_file_name)
        reporter.update_transfer(1, 0)
        reporter.print_completion('delete ' + self.relative_name + ' ' + self.note)

    def __str__(self):
        return 'b2_delete(%s, %s, %s)' % (self.b2_file_name, self.file_id, self.note)


class LocalDeleteAction(AbstractAction):
    def __init__(self, relative_name, full_path):
        self.relative_name = relative_name
        self.full_path = full_path

    def get_bytes(self):
        return 0

    def do_action(self, bucket, reporter):
        os.unlink(self.full_path)
        reporter.update_transfer(1, 0)
        reporter.print_completion('delete ' + self.relative_name)

    def __str__(self):
        return 'local_delete(%s)' % (self.full_path)


class FileVersion(object):
    """
    Holds information about one version of a file:

       id - The B2 file id, or the local full path name
       mod_time - modification time, in milliseconds, to avoid rounding issues
                  with millisecond times from B2
       action - "hide" or "upload" (never "start")
    """

    def __init__(self, id_, file_name, mod_time, action, size):
        self.id_ = id_
        self.name = file_name
        self.mod_time = mod_time
        self.action = action
        self.size = size

    def __repr__(self):
        return 'FileVersion(%s, %s, %s, %s)' % (
            repr(self.id_), repr(self.name), repr(self.mod_time), repr(self.action)
        )


class File(object):
    """
    Holds information about one file in a folder.

    The name is relative to the folder in all cases.

    Files that have multiple versions (which only happens
    in B2, not in local folders) include information about
    all of the versions, most recent first.
    """

    def __init__(self, name, versions):
        self.name = name
        self.versions = versions

    def latest_version(self):
        return self.versions[0]

    def __repr__(self):
        return 'File(%s, [%s])' % (self.name, ', '.join(map(repr, self.versions)))


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
    def all_files(self):
        """
        Returns an iterator over all of the files in the folder, in
        the order that B2 uses.

        No matter what the folder separator on the local file system
        is, "/" is used in the returned file names.
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
        assert isinstance(root, six.text_type)
        self.root = os.path.abspath(root)

    def folder_type(self):
        return 'local'

    def all_files(self):
        prefix_len = len(self.root) + 1  # include trailing '/' in prefix length
        for relative_path in self._walk_relative_paths(prefix_len, self.root):
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

    def _walk_relative_paths(self, prefix_len, dir_path):
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
            if '/' in name:
                raise Exception(
                    "sync does not support file names that include '/': %s in dir %s" %
                    (name, dir_path)
                )
            full_path = os.path.join(dir_path, name)
            relative_path = full_path[prefix_len:]
            if os.path.isdir(full_path):
                name += six.u('/')
                dirs.add(name)
            names[name] = (full_path, relative_path)

        # Yield all of the answers
        for name in sorted(names):
            (full_path, relative_path) = names[name]
            if name in dirs:
                for rp in self._walk_relative_paths(prefix_len, full_path):
                    yield rp
            else:
                yield relative_path

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

    def all_files(self):
        current_name = None
        current_versions = []
        for (file_version_info, folder_name) in self.bucket.ls(
            self.folder_name, show_versions=True,
            recursive=True, fetch_count=1000
        ):
            assert file_version_info.file_name.startswith(self.prefix)
            file_name = file_version_info.file_name[len(self.prefix):]
            if current_name != file_name and current_name is not None:
                yield File(current_name, current_versions)
                current_versions = []
            file_info = file_version_info.file_info
            if 'src_last_modified_millis' in file_info:
                mod_time_millis = int(file_info['src_last_modified_millis'])
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


def next_or_none(iterator):
    """
    Returns the next item from the iterator, or None if there are no more.
    """
    try:
        return six.advance_iterator(iterator)
    except StopIteration:
        return None


def zip_folders(folder_a, folder_b, exclusions=[]):
    """
    An iterator over all of the files in the union of two folders,
    matching file names.

    Each item is a pair (file_a, file_b) with the corresponding file
    in both folders.  Either file (but not both) will be None if the
    file is in only one folder.
    :param folder_a: A Folder object.
    :param folder_b: A Folder object.
    """

    iter_a = (f for f in folder_a.all_files() if not any(ex.match(f.name) for ex in exclusions))
    iter_b = folder_b.all_files()

    current_a = next_or_none(iter_a)
    current_b = next_or_none(iter_b)
    while current_a is not None or current_b is not None:
        if current_a is None:
            yield (None, current_b)
            current_b = next_or_none(iter_b)
        elif current_b is None:
            yield (current_a, None)
            current_a = next_or_none(iter_a)
        elif current_a.name < current_b.name:
            yield (current_a, None)
            current_a = next_or_none(iter_a)
        elif current_b.name < current_a.name:
            yield (None, current_b)
            current_b = next_or_none(iter_b)
        else:
            assert current_a.name == current_b.name
            yield (current_a, current_b)
            current_a = next_or_none(iter_a)
            current_b = next_or_none(iter_b)


def make_transfer_action(sync_type, source_file, source_folder, dest_folder):
    source_mod_time = source_file.latest_version().mod_time
    if sync_type == 'local-to-b2':
        return B2UploadAction(
            source_folder.make_full_path(source_file.name),
            source_file.name,
            dest_folder.make_full_path(source_file.name),
            source_mod_time,
            source_file.latest_version().size
        )  # yapf: disable
    else:
        return B2DownloadAction(
            source_file.name,
            source_folder.make_full_path(source_file.name),
            source_file.latest_version().id_,
            dest_folder.make_full_path(source_file.name),
            source_mod_time,
            source_file.latest_version().size
        )  # yapf: disable

def check_file_replacement(source_file, dest_file, args):
    """
    Compare two files and determine if the the destination file
    should be replaced by the source file.
    """

    # Compare using modification time by default
    compareVersions = args.compareVersions or 'modTime'

    # Compare using file name only
    if compareVersions == 'none':
        return False

    # Compare using modification time
    elif compareVersions == 'modTime':
        # Get the modification time of the latest versions
        source_mod_time = source_file.latest_version().mod_time
        dest_mod_time = dest_file.latest_version().mod_time

        # Source is newer
        if dest_mod_time < source_mod_time:
            return True

        # Source is older
        elif source_mod_time < dest_mod_time:
            if args.replaceNewer:
                return True
            elif args.skipNewer:
                return False
            else:
                raise DestFileNewer(dest_file.name,)

    # Compare using file size
    elif compareVersions == 'size':
        # Get file size of the latest versions
        source_size = source_file.latest_version().size
        dest_size = dest_file.latest_version().size

        # Replace if sizes are different
        return source_size != dest_size

    else:
        raise CommandError('Invalid option for --compareVersions')


def make_file_sync_actions(
    sync_type, source_file, dest_file, source_folder, dest_folder, args, now_millis
):
    """
    Yields the sequence of actions needed to sync the two files
    """

    # By default, all but the current version at the destination are
    # candidates for cleaning.  This will be overridden in the case
    # where there is no source file or a new version is uploaded.
    dest_versions_to_clean = []
    if dest_file is not None:
        dest_versions_to_clean = dest_file.versions[1:]

    # Case 1: Both files exist
    transferred = False
    if source_file is not None and dest_file is not None:
        if check_file_replacement(source_file, dest_file, args):
            yield make_transfer_action(sync_type, source_file, source_folder, dest_folder)
            transferred = True
        # All destination files are candidates for cleaning, if a new version is beeing uploaded
        if transferred and sync_type == 'local-to-b2':
            dest_versions_to_clean = dest_file.versions

    # Case 2: No destination file, but source file exists
    elif source_file is not None and dest_file is None:
        yield make_transfer_action(sync_type, source_file, source_folder, dest_folder)
        transferred = True

    # Case 3: No source file, but destination file exists
    elif source_file is None and dest_file is not None:
        if args.keepDays is not None and sync_type == 'local-to-b2':
            if dest_file.latest_version().action == 'upload':
                yield B2HideAction(dest_file.name, dest_folder.make_full_path(dest_file.name))
        # All versions of the destination file are candidates for cleaning
        dest_versions_to_clean = dest_file.versions

    # Clean up old versions
    if sync_type == 'local-to-b2':
        for version in dest_versions_to_clean:
            note = ''
            if transferred or (version is not dest_file.versions[0]):
                note = '(old version)'
            if args.delete:
                yield B2DeleteAction(
                    dest_file.name, dest_folder.make_full_path(dest_file.name), version.id_, note
                )
            elif args.keepDays is not None:
                age_days = (now_millis - version.mod_time) / ONE_DAY_IN_MS
                if args.keepDays < age_days:
                    yield B2DeleteAction(
                        dest_file.name, dest_folder.make_full_path(dest_file.name), version.id_,
                        note
                    )
    elif sync_type == 'b2-to-local':
        for version in dest_versions_to_clean:
            if args.delete:
                yield LocalDeleteAction(dest_file.name, version.id_)


def make_folder_sync_actions(source_folder, dest_folder, args, now_millis, reporter):
    """
    Yields a sequence of actions that will sync the destination
    folder to the source folder.
    """
    if args.skipNewer and args.replaceNewer:
        raise CommandError('--skipNewer and --replaceNewer are incompatible')

    if args.delete and (args.keepDays is not None):
        raise CommandError('--delete and --keepDays are incompatible')

    if (args.keepDays is not None) and (dest_folder.folder_type() == 'local'):
        raise CommandError('--keepDays cannot be used for local files')

    exclusions = [re.compile(ex) for ex in args.excludeRegex]

    source_type = source_folder.folder_type()
    dest_type = dest_folder.folder_type()
    sync_type = '%s-to-%s' % (source_type, dest_type)
    if (source_folder.folder_type(), dest_folder.folder_type()) not in [
        ('b2', 'local'), ('local', 'b2')
    ]:
        raise NotImplementedError("Sync support only local-to-b2 and b2-to-local")
    for (source_file, dest_file) in zip_folders(source_folder, dest_folder, exclusions):
        if source_folder.folder_type() == 'local':
            if source_file is not None:
                reporter.update_compare(1)
        else:
            if dest_file is not None:
                reporter.update_compare(1)
        for action in make_file_sync_actions(
            sync_type, source_file, dest_file, source_folder, dest_folder, args, now_millis
        ):
            yield action


def _parse_bucket_and_folder(bucket_and_path, api):
    """
    Turns 'my-bucket/foo' into B2Folder(my-bucket, foo)
    """
    if '//' in bucket_and_path:
        raise CommandError("'//' not allowed in path names")
    if '/' not in bucket_and_path:
        bucket_name = bucket_and_path
        folder_name = ''
    else:
        (bucket_name, folder_name) = bucket_and_path.split('/', 1)
    if folder_name.endswith('/'):
        folder_name = folder_name[:-1]
    return B2Folder(bucket_name, folder_name, api)


def parse_sync_folder(folder_name, api):
    """
    Takes either a local path, or a B2 path, and returns a Folder
    object for it.

    B2 paths look like: b2://bucketName/path/name.  The '//' is optional,
    because the previous sync command didn't use it.

    Anything else is treated like a local folder.
    """
    if folder_name.startswith('b2://'):
        return _parse_bucket_and_folder(folder_name[5:], api)
    elif folder_name.startswith('b2:') and folder_name[3].isalnum():
        return _parse_bucket_and_folder(folder_name[3:], api)
    else:
        if folder_name.endswith('/'):
            folder_name = folder_name[:-1]
        return LocalFolder(folder_name)


def count_files(local_folder, reporter):
    """
    Counts all of the files in a local folder.
    """
    for _ in local_folder.all_files():
        reporter.update_local(1)
    reporter.end_local()


def sync_folders(source_folder, dest_folder, args, now_millis, stdout, no_progress, max_workers):
    """
    Syncs two folders.  Always ensures that every file in the
    source is also in the destination.  Deletes any file versions
    in the destination older than history_days.
    """

    # For downloads, make sure that the target directory is there.
    if dest_folder.folder_type() == 'local':
        dest_folder.ensure_present()

    # Make a reporter to report progress.
    with SyncReport(stdout, no_progress) as reporter:

        # Make an executor to count files and run all of the actions.  This is
        # not the same as the executor in the API object, which is used for
        # uploads.  The tasks in this executor wait for uploads.  Putting them
        # in the same thread pool could lead to deadlock.
        sync_executor = futures.ThreadPoolExecutor(max_workers=max_workers)

        # First, start the thread that counts the local files.  That's the operation
        # that should be fastest, and it provides scale for the progress reporting.
        local_folder = None
        if source_folder.folder_type() == 'local':
            local_folder = source_folder
        if dest_folder.folder_type() == 'local':
            local_folder = dest_folder
        if local_folder is None:
            raise ValueError('neither folder is a local folder')
        sync_executor.submit(count_files, local_folder, reporter)

        # Schedule each of the actions
        bucket = None
        if source_folder.folder_type() == 'b2':
            bucket = source_folder.bucket
        if dest_folder.folder_type() == 'b2':
            bucket = dest_folder.bucket
        if bucket is None:
            raise ValueError('neither folder is a b2 folder')
        total_files = 0
        total_bytes = 0
        for action in make_folder_sync_actions(
            source_folder, dest_folder, args, now_millis, reporter
        ):
            sync_executor.submit(action.run, bucket, reporter)
            total_files += 1
            total_bytes += action.get_bytes()
        reporter.end_compare(total_files, total_bytes)

        # Wait for everything to finish
        sync_executor.shutdown()
