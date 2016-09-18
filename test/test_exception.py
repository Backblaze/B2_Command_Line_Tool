######################################################################
#
# File: test/test_exception.py
#
# Copyright 2016 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import six

from .test_base import TestBase
from b2.exception import AlreadyFailed, B2Error, BadUploadUrl, CommandError


class TestB2Error(TestBase):
    def test_plain_ascii(self):
        self.assertEqual('message', str(B2Error('message')))

    def test_unicode(self):
        if six.PY2:
            self.assertEqual('\\u81ea\\u7531', str(B2Error(u'\u81ea\u7531')))
        else:
            self.assertEqual(u'\u81ea\u7531', str(B2Error(u'\u81ea\u7531')))


class TestExceptions(TestBase):
    def test_bad_upload_url_exception(self):
        try:
            raise BadUploadUrl('foo')
        except BadUploadUrl as e:
            assert e.should_retry_http()
            assert e.should_retry_upload()
            assert str(e) == 'Bad upload url: foo', str(e)

    def test_already_failed_exception(self):
        try:
            raise AlreadyFailed('foo')
        except AlreadyFailed as e:
            assert str(e) == 'Already failed: foo', str(e)

    def test_command_error(self):
        try:
            raise CommandError('foo')
        except CommandError as e:
            assert str(e) == 'foo', str(e)
