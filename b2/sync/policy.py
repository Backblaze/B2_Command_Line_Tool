######################################################################
#
# File: b2/sync/policy.py
#
# Copyright 2016 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from abc import ABCMeta, abstractmethod

import six

from ..exception import CommandError, DestFileNewer
from .action import LocalDeleteAction, B2DeleteAction, B2DownloadAction, B2HideAction, B2UploadAction

ONE_DAY_IN_MS = 24 * 60 * 60 * 1000


@six.add_metaclass(ABCMeta)
class AbstractFileSyncPolicy(object):
    def __init__(self, source_file, source_folder, dest_file, dest_folder, now_millis, args):
        self._source_file = source_file
        self._source_folder = source_folder
        self._dest_file = dest_file
        self._delete = args.delete
        self._keepDays = args.keepDays
        self._args = args
        self._dest_folder = dest_folder
        self._now_millis = now_millis
        self._transferred = False

    def _should_transfer(self):
        """
        Decides whether to transfer the file from the source to the destination.
        """
        if self._source_file is None:
            # No source file.  Nothing to transfer.
            return False
        elif self._dest_file is None:
            # Source file exists, but no destination file.  Always transfer.
            return True
        else:
            # Both exist.  Transfer only if the two are different.
            return self.files_are_different(self._source_file, self._dest_file, self._args)

    @classmethod
    def files_are_different(cls, source_file, dest_file, args):
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

    def get_all_actions(self):
        if self._should_transfer():
            yield self._make_transfer_action()
            self._transferred = True

        assert self._dest_file is not None or self._source_file is not None

        for action in self._get_hide_delete_actions():
            yield action

    def _get_hide_delete_actions(self):
        """
        subclass policy can override this to hide or delete files
        """
        return []

    def _get_source_mod_time(self):
        return self._source_file.latest_version().mod_time

    @abstractmethod
    def _make_transfer_action(self):
        """ return an action representing transfer of file according to the selected policy """


class DownPolicy(AbstractFileSyncPolicy):
    """
    file is synced down (from the cloud to disk)
    """

    def _make_transfer_action(self):
        return B2DownloadAction(
            self._source_file.name, self._source_folder.make_full_path(self._source_file.name),
            self._source_file.latest_version().id_,
            self._dest_folder.make_full_path(self._source_file.name), self._get_source_mod_time(),
            self._source_file.latest_version().size
        )


class UpPolicy(AbstractFileSyncPolicy):
    """
    file is synced up (from disk the cloud)
    """

    def _make_transfer_action(self):
        return B2UploadAction(
            self._source_folder.make_full_path(self._source_file.name), self._source_file.name,
            self._dest_folder.make_full_path(self._source_file.name), self._get_source_mod_time(),
            self._source_file.latest_version().size
        )


class UpAndDeletePolicy(UpPolicy):
    """
    file is synced up (from disk to the cloud) and the delete flag is SET
    """

    def _get_hide_delete_actions(self):
        for action in super(UpAndDeletePolicy, self)._get_hide_delete_actions():
            yield action
        for action in make_b2_delete_actions(
            self._source_file, self._dest_file, self._dest_folder, self._transferred
        ):
            yield action


class UpAndKeepDaysPolicy(UpPolicy):
    """
    file is synced up (from disk to the cloud) and the keepDays flag is SET
    """

    def _get_hide_delete_actions(self):
        for action in super(UpAndKeepDaysPolicy, self)._get_hide_delete_actions():
            yield action
        for action in make_b2_keep_days_actions(
            self._source_file, self._dest_file, self._dest_folder, self._transferred,
            self._keepDays, self._now_millis
        ):
            yield action


class DownAndDeletePolicy(DownPolicy):
    """
    file is synced down (from the cloud to disk) and the delete flag is SET
    """

    def _get_hide_delete_actions(self):
        for action in super(DownAndDeletePolicy, self)._get_hide_delete_actions():
            yield action
        if self._dest_file is not None and self._source_file is None:
            # Local files have either 0 or 1 versions.  If the file is there,
            # it must have exactly 1 version.
            yield LocalDeleteAction(self._dest_file.name, self._dest_file.versions[0].id_)


class DownAndKeepDaysPolicy(DownPolicy):
    """
    file is synced down (from the cloud to disk) and the keepDays flag is SET
    """
    pass


def make_b2_delete_note(version, index, transferred):
    note = ''
    if version.action == 'hide':
        note = '(hide marker)'
    elif transferred or 0 < index:
        note = '(old version)'
    return note


def make_b2_delete_actions(source_file, dest_file, dest_folder, transferred):
    """
    Creates the actions to delete files stored on B2, which are not present locally.
    """
    if dest_file is None:
        # B2 does not really store folders, so there is no need to hide
        # them or delete them
        raise StopIteration()
    for version_index, version in enumerate(dest_file.versions):
        keep = (version_index == 0) and (source_file is not None) and not transferred
        if not keep:
            yield B2DeleteAction(
                dest_file.name, dest_folder.make_full_path(dest_file.name), version.id_,
                make_b2_delete_note(version, version_index, transferred)
            )


def make_b2_keep_days_actions(
    source_file, dest_file, dest_folder, transferred, keep_days, now_millis
):
    """
    Creates the actions to hide or delete existing versions of a file
    stored in B2.

    When keepDays is set, all files that were visible any time from
    keepDays ago until now must be kept.  If versions were uploaded 5
    days ago, 15 days ago, and 25 days ago, and the keepDays is 10,
    only the 25-day old version can be deleted.  The 15 day-old version
    was visible 10 days ago.
    """
    prev_age_days = None
    deleting = False
    if dest_file is None:
        # B2 does not really store folders, so there is no need to hide
        # them or delete them
        raise StopIteration()
    for version_index, version in enumerate(dest_file.versions):
        # How old is this version?
        age_days = (now_millis - version.mod_time) / ONE_DAY_IN_MS

        # We assume that the versions are ordered by time, newest first.
        assert prev_age_days is None or prev_age_days <= age_days

        # Do we need to hide this version?
        if version_index == 0 and source_file is None and version.action == 'upload':
            yield B2HideAction(dest_file.name, dest_folder.make_full_path(dest_file.name))

        # Can we start deleting?  Once we start deleting, all older
        # versions will also be deleted.
        if version.action == 'hide':
            if keep_days < age_days:
                deleting = True

        # Delete this version
        if deleting:
            yield B2DeleteAction(
                dest_file.name, dest_folder.make_full_path(dest_file.name), version.id_,
                make_b2_delete_note(version, version_index, transferred)
            )

        # Can we start deleting with the next version, based on the
        # age of this one?
        if keep_days < age_days:
            deleting = True

        # Remember this age for next time around the loop.
        prev_age_days = age_days
