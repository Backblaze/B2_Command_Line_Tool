######################################################################
#
# File: test_allowed_policy.py
#
# Copyright 2018 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from .test_base import TestBase
from b2.account_info.allowed_policy import check_command_allowed
from b2.exception import BucketNotAllowed, CapabilityNotAllowed, FileNameNotAllowed

try:
    from unittest.mock import MagicMock
except ImportError:
    from mock import MagicMock


class TestAllowedPolicy(TestBase):
    def setUp(self):
        self.account_info = MagicMock()

    def test_no_allowed(self):
        self.account_info.get_allowed = MagicMock(return_value=None)
        check_command_allowed('whatever', None, None, self.account_info)

    def test_unrestricted_allowed(self):
        self._allow_list_files_unrestricted()
        check_command_allowed('listFiles', 'bucket-a', 'prefix/', self.account_info)

    def test_restricted_allowed(self):
        self._allow_list_files_restricted()
        check_command_allowed('listFiles', 'bucket-a', 'prefix/', self.account_info)

    def test_capability_not_allowed(self):
        self._allow_list_files_restricted()
        with self.assertRaises(CapabilityNotAllowed):
            check_command_allowed('deleteFiles', 'bucket-a', 'prefix/', self.account_info)

    def test_bucket_allowed_when_not_restricted(self):
        self._allow_list_files_unrestricted()
        check_command_allowed('listFiles', 'bucket-b', 'prefix/', self.account_info)

    def test_bucket_allowed(self):
        self._allow_list_files_restricted()
        check_command_allowed('listFiles', 'bucket-a', 'prefix/', self.account_info)

    def test_bucket_not_allowed(self):
        self._allow_list_files_restricted()
        with self.assertRaises(BucketNotAllowed):
            check_command_allowed('listFiles', 'bucket-b', 'prefix/', self.account_info)

    def test_bucket_not_allowed_when_not_named(self):
        self._allow_list_files_restricted()
        with self.assertRaises(BucketNotAllowed):
            check_command_allowed('listFiles', None, None, self.account_info)

    def test_name_allowed_when_not_restricted(self):
        self._allow_list_files_unrestricted()
        check_command_allowed('listFiles', 'bucket-a', 'other', self.account_info)

    def test_name_allowed_when_restricted(self):
        self._allow_list_files_unrestricted()
        check_command_allowed('listFiles', 'bucket-a', 'prefix/', self.account_info)

    def test_name_not_allowed(self):
        self._allow_list_files_restricted()
        with self.assertRaises(FileNameNotAllowed):
            check_command_allowed('listFiles', 'bucket-a', 'other', self.account_info)

    def test_name_not_allowed_when_not_named(self):
        self._allow_list_files_restricted()
        with self.assertRaises(FileNameNotAllowed):
            check_command_allowed('listFiles', 'bucket-a', None, self.account_info)

    def _allow_list_files_unrestricted(self):
        allowed = dict(capabilities=['listFiles'], buckedId=None, bucketName=None)
        self.account_info.get_allowed = MagicMock(return_value=allowed)

    def _allow_list_files_restricted(self):
        allowed = dict(
            capabilities=['listFiles'],
            buckedId='bucketId',
            bucketName='bucket-a',
            namePrefix='prefix/'
        )
        self.account_info.get_allowed = MagicMock(return_value=allowed)
