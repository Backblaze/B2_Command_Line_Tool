######################################################################
#
# File: test/test_exception.py
#
# Copyright 2016 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import unittest

from b2.exception import AlreadyFailed, BadUploadUrl, CommandError


class TestExceptions(unittest.TestCase):
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
