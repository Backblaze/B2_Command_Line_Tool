######################################################################
#
# File: test_policy
#
# Copyright 2017, Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from b2.sync.file import File, FileVersion
from b2.sync.folder import B2Folder
from b2.sync.policy import make_b2_keep_days_actions
from .test_base import TestBase

try:
    from unittest.mock import MagicMock
except ImportError:
    from mock import MagicMock


class TestMakeB2KeepDaysActions(TestBase):
    def setUp(self):
        self.keep_days = 7
        self.today = 100 * 86400
        self.one_day_millis = 86400 * 1000

    def test_no_versions(self):
        self.check_one_answer(True, [], [])

    def test_new_version_no_action(self):
        self.check_one_answer(True, [(1, -5, 'upload')], [])

    def test_no_source_one_old_version_hides(self):
        # An upload that is old gets deleted if there is no source file.
        self.check_one_answer(False, [(1, -10, 'upload')], ['b2_hide(folder/a)'])

    def test_old_hide_causes_delete(self):
        # A hide marker that is old gets deleted, as do the things after it.
        self.check_one_answer(
            True, [(1, -5, 'upload'), (2, -10, 'hide'), (3, -20, 'upload')],
            ['b2_delete(folder/a, 2, (hide marker))', 'b2_delete(folder/a, 3, (old version))']
        )

    def test_old_upload_causes_delete(self):
        # An upload that is old stays if there is a source file, but things
        # behind it go away.
        self.check_one_answer(
            True, [(1, -5, 'upload'), (2, -10, 'upload'), (3, -20, 'upload')],
            ['b2_delete(folder/a, 3, (old version))']
        )

    def test_out_of_order_dates(self):
        # The one at date -3 will get deleted because the one before it is old.
        self.check_one_answer(
            True, [(1, -5, 'upload'), (2, -10, 'upload'), (3, -3, 'upload')],
            ['b2_delete(folder/a, 3, (old version))']
        )

    def check_one_answer(self, has_source, id_relative_date_action_list, expected_actions):
        source_file = File('a', []) if has_source else None
        dest_file_versions = [
            FileVersion(id_, 'a', self.today + relative_date * self.one_day_millis, action, 100)
            for (id_, relative_date, action) in id_relative_date_action_list
        ]
        dest_file = File('a', dest_file_versions)
        bucket = MagicMock()
        api = MagicMock()
        api.get_bucket_by_name.return_value = bucket
        dest_folder = B2Folder('bucket-1', 'folder', api)
        actual_actions = list(
            make_b2_keep_days_actions(
                source_file, dest_file, dest_folder, dest_folder, self.keep_days, self.today
            )
        )
        actual_action_strs = [str(a) for a in actual_actions]
        self.assertEqual(expected_actions, actual_action_strs)
