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

from .utils import files_are_different
from .action import LocalDeleteAction, B2DeleteAction, B2DownloadAction, B2HideAction, B2UploadAction


ONE_DAY_IN_MS = 24 * 60 * 60 * 1000


@six.add_metaclass(ABCMeta)
class AbstractFileSyncPolicy(object):
    def __init__(self, source_file, source_folder, dest_file, dest_folder, now_millis, args):
        self.source_file = source_file
        self.source_folder = source_folder
        self.dest_file = dest_file
        self.delete = args.delete
        self.keepDays = args.keepDays
        self.args = args
        self.dest_folder = dest_folder
        self.now_millis = now_millis
        self.transferred = False
    def should_transfer(self):
        """
        Decides whether to transfer the file from the source to the destination.
        """
        if self.source_file is None:
            # No source file.  Nothing to transfer.
            return False
        elif self.dest_file is None:
            # Source file exists, but no destination file.  Always transfer.
            return True
        else:
            # Both exist.  Transfer only if the two are different.
            return files_are_different(self.source_file, self.dest_file, self.args)  # TODO: don't pass args here?
    def get_all_actions(self):
        if self.should_transfer():
            yield self.make_transfer_action()
            self.transferred = True

        assert self.dest_file is not None or self.source_file is not None
        for action in self.get_upload_delete_actions():
            yield action
    def get_upload_delete_actions(self):
        return []  # subclass can override this
    def get_source_mod_time(self):
        return self.source_file.latest_version().mod_time
    @abstractmethod
    def make_transfer_action(self):
        pass


class DownPolicy(AbstractFileSyncPolicy):
    def make_transfer_action(self):
        return B2DownloadAction(
            self.source_file.name,
            self.source_folder.make_full_path(self.source_file.name),
            self.source_file.latest_version().id_,
            self.dest_folder.make_full_path(self.source_file.name),
            self.get_source_mod_time(),
            self.source_file.latest_version().size
        )

class UpPolicy(AbstractFileSyncPolicy):
    def make_transfer_action(self):
        return B2UploadAction(
            self.source_folder.make_full_path(self.source_file.name),
            self.source_file.name,
            self.dest_folder.make_full_path(self.source_file.name),
            self.get_source_mod_time(),
            self.source_file.latest_version().size
        )


class UpAndDeletePolicy(UpPolicy):
    """
    file is synced up (from disk to the cloud) and the deleta flag is SET
    """
    def get_upload_delete_actions(self):
        for action in super(UpAndDeletePolicy, self).get_upload_delete_actions():
            yield action
        for action in make_b2_delete_actions(
                self.source_file, self.dest_file, self.dest_folder, self.transferred
            ):
            yield action


class UpAndKeepDaysPolicy(UpPolicy):
    """
    file is synced up (from disk to the cloud) and the keepDays flag is SET
    """
    def get_upload_delete_actions(self):
        for action in super(UpAndKeepDaysPolicy, self).get_upload_delete_actions():
            yield action
        for action in make_b2_keep_days_actions(
                self.source_file, self.dest_file, self.dest_folder,
                self.transferred, self.keepDays, self.now_millis
            ):
            yield action


class DownAndDeletePolicy(DownPolicy):
    """
    file is synced down (from the cloud to disk) and the delete flag is SET
    """
    def get_upload_delete_actions(self):
        for action in super(DownAndDeletePolicy, self).get_upload_delete_actions():
            yield action
        if self.dest_file is not None and self.source_file is None:
            # Local files have either 0 or 1 versions.  If the file is there,
            # it must have exactly 1 version.
            yield LocalDeleteAction(self.dest_file.name, self.dest_file.versions[0].id_)


class DownAndKeepDaysPolicy(DownPolicy):
    pass


def make_b2_delete_actions(source_file, dest_file, dest_folder, transferred):
    for (version_index, version) in enumerate(dest_file.versions):
        keep = (version_index == 0) and (source_file is not None) and not transferred
        if not keep:
            note = ''
            if version.action == 'hide':
                note = '(hide marker)'
            elif transferred or 0 < version_index:
                note = '(old version)'
            yield B2DeleteAction(
                dest_file.name, dest_folder.make_full_path(dest_file.name), version.id_, note
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
    for (version_index, version) in enumerate(dest_file.versions):
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
            note = ''
            if version.action == 'hide':
                note = '(hide marker)'
            elif transferred or 0 < version_index:
                note = '(old version)'
            yield B2DeleteAction(
                dest_file.name, dest_folder.make_full_path(dest_file.name), version.id_, note
            )

        # Can we start deleting with the next version, based on the
        # age of this one?
        if keep_days < age_days:
            deleting = True

        # Remember this age for next time around the loop.
        prev_age_days = age_days


