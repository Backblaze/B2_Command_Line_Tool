######################################################################
#
# File: test/test_account_info.py
#
# Copyright 2016 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import json
import os
import unittest

from b2.account_info import StoredAccountInfo


class TestStoredAccountInfo(unittest.TestCase):

    FILE_NAME = 'test_b2_account_info'

    def setUp(self):
        self.account_info = StoredAccountInfo(file_name=self.FILE_NAME)

    def tearDown(self):
        try:
            os.unlink(self.FILE_NAME)
        except BaseException:
            pass

    def test_clear(self):
        self.account_info.set_auth_data(
            'account_id', 'account_auth', 'api_url', 'download_url', 100, 'app_key', 'realm'
        )
        self.assertEqual('account_id', self._read_data()['account_id'])
        self.account_info.clear()
        self.assertEqual(None, self._read_data().get('account_id'))

    def test_bucket_upload_data(self):
        self.account_info.set_bucket_upload_data('bucket_0', 'http://bucket_0', 'bucket_0_auth')
        expected = {
            'bucket_0': {
                'bucket_upload_url': 'http://bucket_0',
                'bucket_upload_auth_token': 'bucket_0_auth'
            }
        }
        self.assertEqual(expected, self._read_data()['bucket_upload_data'])
        self.account_info.clear_bucket_upload_data('bucket_0')
        self.assertEqual({}, self._read_data()['bucket_upload_data'])

    def _read_data(self):
        with open(self.FILE_NAME, 'rb') as f:
            return json.loads(f.read().decode('utf-8'))
