######################################################################
#
# File: test_session.py
#
# Copyright 2018 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from b2.exception import InvalidAuthToken, Unauthorized
from b2.raw_api import ALL_CAPABILITIES
from b2.session import B2Session
from .test_base import TestBase

try:
    import unittest.mock as mock
except ImportError:
    import mock


class TestB2Session(TestBase):
    def setUp(self):
        self.account_info = mock.MagicMock()
        self.account_info.get_account_auth_token.return_value = 'auth_token'

        self.api = mock.MagicMock()
        self.api.account_info = self.account_info

        self.raw_api = mock.MagicMock()
        self.raw_api.do_it.__name__ = 'do_it'
        self.raw_api.do_it.side_effect = ['ok']

        self.session = B2Session(self.api, self.raw_api)

    def test_works_first_time(self):
        self.assertEqual('ok', self.session.do_it())

    def test_works_second_time(self):
        self.raw_api.do_it.side_effect = [
            InvalidAuthToken('message', 'code'),
            'ok',
        ]
        self.assertEqual('ok', self.session.do_it())

    def test_fails_second_time(self):
        self.raw_api.do_it.side_effect = [
            InvalidAuthToken('message', 'code'),
            InvalidAuthToken('message', 'code'),
        ]
        with self.assertRaises(InvalidAuthToken):
            self.session.do_it()

    def test_app_key_info_no_info(self):
        self.account_info.get_allowed.return_value = dict(
            bucketId=None,
            bucketName=None,
            capabilities=ALL_CAPABILITIES,
            namePrefix=None,
        )
        self.raw_api.do_it.side_effect = Unauthorized('no_go', 'code')
        with self.assertRaisesRegexp(
            Unauthorized, r'no_go for application key with no restrictions \(code\)'
        ):
            self.session.do_it()

    def test_app_key_info_no_info_no_message(self):
        self.account_info.get_allowed.return_value = dict(
            bucketId=None,
            bucketName=None,
            capabilities=ALL_CAPABILITIES,
            namePrefix=None,
        )
        self.raw_api.do_it.side_effect = Unauthorized('', 'code')
        with self.assertRaisesRegexp(
            Unauthorized, r'unauthorized for application key with no restrictions \(code\)'
        ):
            self.session.do_it()

    def test_app_key_info_all_info(self):
        self.account_info.get_allowed.return_value = dict(
            bucketId='123456',
            bucketName='my-bucket',
            capabilities=['readFiles'],
            namePrefix='prefix/',
        )
        self.raw_api.do_it.side_effect = Unauthorized('no_go', 'code')
        with self.assertRaisesRegexp(
            Unauthorized,
            r"no_go for application key with capabilities 'readFiles', restricted to bucket 'my-bucket', restricted to files that start with 'prefix/' \(code\)"
        ):
            self.session.do_it()
