######################################################################
#
# File: test/test_account_info.py
#
# Copyright 2016 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import os
import unittest

import six

from b2.account_info import StoredAccountInfo
from b2.exception import MissingAccountData

try:
    import unittest.mock as mock
except:
    import mock


class TestStoredAccountInfo(unittest.TestCase):

    FILE_NAME = 'test_b2_account_info'

    def setUp(self):
        try:
            os.unlink(self.FILE_NAME)
        except:
            pass
        self.account_info = StoredAccountInfo(file_name=self.FILE_NAME)

    def tearDown(self):
        try:
            os.unlink(self.FILE_NAME)
        except BaseException:
            pass
        try:
            os.unlink(self.FILE_NAME + '.lock')
        except BaseException:
            pass

    def test_account_info(self):
        self.account_info.set_auth_data(
            'account_id', 'account_auth', 'api_url', 'download_url', 100, 'app_key', 'realm'
        )
        info2 = StoredAccountInfo(file_name=self.FILE_NAME)
        self.assertEqual('account_id', info2.get_account_id())
        self.assertEqual('account_auth', info2.get_account_auth_token())
        self.assertEqual('api_url', info2.get_api_url())
        self.assertEqual('app_key', info2.get_application_key())
        self.assertEqual('realm', info2.get_realm())
        self.assertEqual(100, info2.get_minimum_part_size())

    def test_clear(self):
        self.account_info.set_auth_data(
            'account_id', 'account_auth', 'api_url', 'download_url', 100, 'app_key', 'realm'
        )
        self.assertEqual('account_id', self._fresh_info().get_account_id())
        self.account_info.clear()

        try:
            self._fresh_info().get_account_id()
            self.fail('should have raised MissingAccountData')
        except MissingAccountData:
            pass

    def test_bucket_upload_data(self):
        self.account_info.put_bucket_upload_url('bucket-0', 'http://bucket-0', 'bucket-0_auth')
        self.assertEqual(
            ('http://bucket-0', 'bucket-0_auth'),
            self.account_info.take_bucket_upload_url('bucket-0')
        )
        self.assertEqual((None, None), self._fresh_info().take_bucket_upload_url('bucket-0'))
        self.account_info.put_bucket_upload_url('bucket-0', 'http://bucket-0', 'bucket-0_auth')
        self.assertEqual(
            ('http://bucket-0', 'bucket-0_auth'),
            self._fresh_info().take_bucket_upload_url('bucket-0')
        )
        self.assertEqual((None, None), self.account_info.take_bucket_upload_url('bucket-0'))

    def test_clear_bucket_upload_data(self):
        self.account_info.put_bucket_upload_url('bucket-0', 'http://bucket-0', 'bucket-0_auth')
        self.account_info.clear_bucket_upload_data('bucket-0')
        self.assertEqual((None, None), self.account_info.take_bucket_upload_url('bucket-0'))

    def test_large_file_upload_urls(self):
        self.account_info.put_large_file_upload_url('file_0', 'http://file_0', 'auth_0')
        self.assertEqual(
            ('http://file_0', 'auth_0'), self.account_info.take_large_file_upload_url('file_0')
        )
        self.assertEqual((None, None), self.account_info.take_large_file_upload_url('file_0'))

    def test_clear_large_file_upload_urls(self):
        self.account_info.put_large_file_upload_url('file_0', 'http://file_0', 'auth_0')
        self.account_info.clear_large_file_upload_urls('file_0')
        self.assertEqual((None, None), self.account_info.take_large_file_upload_url('file_0'))

    def test_bucket(self):
        bucket = mock.MagicMock()
        bucket.name = 'my-bucket'
        bucket.id_ = 'bucket-0'
        self.assertEqual(
            None, self.account_info.get_bucket_id_or_none_from_bucket_name('my-bucket')
        )
        self.account_info.save_bucket(bucket)
        self.assertEqual(
            'bucket-0', self.account_info.get_bucket_id_or_none_from_bucket_name('my-bucket')
        )
        self.assertEqual(
            'bucket-0', self._fresh_info().get_bucket_id_or_none_from_bucket_name('my-bucket')
        )
        self.account_info.remove_bucket_name('my-bucket')
        self.assertEqual(
            None, self.account_info.get_bucket_id_or_none_from_bucket_name('my-bucket')
        )
        self.assertEqual(
            None, self._fresh_info().get_bucket_id_or_none_from_bucket_name('my-bucket')
        )

    def test_refresh_bucket(self):
        self.assertEqual(
            None, self.account_info.get_bucket_id_or_none_from_bucket_name('my-bucket')
        )
        bucket_names = {'a': 'bucket-0', 'b': 'bucket-1'}
        self.account_info.refresh_entire_bucket_name_cache(six.iteritems(bucket_names))
        self.assertEqual('bucket-0', self.account_info.get_bucket_id_or_none_from_bucket_name('a'))
        self.assertEqual('bucket-0', self._fresh_info().get_bucket_id_or_none_from_bucket_name('a'))

    def _fresh_info(self):
        """
        Returns a new StoredAccountInfo that has just read the data from the file.
        """
        return StoredAccountInfo(file_name=self.FILE_NAME)
