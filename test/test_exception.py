######################################################################
#
# File: test/test_exception.py
#
# Copyright 2018 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from b2.exception import (
    AlreadyFailed,
    B2Error,
    BadJson,
    BadUploadUrl,
    CommandError,
    Conflict,
    DuplicateBucketName,
    FileAlreadyHidden,
    FileNotPresent,
    interpret_b2_error,
    InvalidAuthToken,
    MissingPart,
    PartSha1Mismatch,
    ServiceError,
    StorageCapExceeded,
    TooManyRequests,
    Unauthorized,
    UnknownError,
)
from .test_base import TestBase

import six


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


class TestInterpretError(TestBase):
    def test_file_already_hidden(self):
        self._check_one(FileAlreadyHidden, 400, 'already_hidden', '')
        self.assertEqual(
            'File already hidden: file.txt',
            str(interpret_b2_error(400, 'already_hidden', '', {'fileName': 'file.txt'})),
        )

    def test_bad_json(self):
        self._check_one(BadJson, 400, 'bad_json', '')

    def test_file_not_present(self):
        self._check_one(FileNotPresent, 400, 'no_such_file', '')
        self._check_one(FileNotPresent, 400, 'file_not_present', '')
        self._check_one(FileNotPresent, 404, 'not_found', '')
        self.assertEqual(
            'File not present: file.txt',
            str(interpret_b2_error(404, 'not_found', '', {'fileName': 'file.txt'})),
        )

    def test_duplicate_bucket_name(self):
        self._check_one(DuplicateBucketName, 400, 'duplicate_bucket_name', '')
        self.assertEqual(
            'Bucket name is already in use: my-bucket',
            str(interpret_b2_error(400, 'duplicate_bucket_name', '', {'bucketName': 'my-bucket'})),
        )

    def test_missing_part(self):
        self._check_one(MissingPart, 400, 'missing_part', '')
        self.assertEqual(
            'Part number has not been uploaded: my-file-id',
            str(interpret_b2_error(400, 'missing_part', '', {'fileId': 'my-file-id'})),
        )

    def test_part_sha1_mismatch(self):
        self._check_one(PartSha1Mismatch, 400, 'part_sha1_mismatch', '')
        self.assertEqual(
            'Part number my-file-id has wrong SHA1',
            str(interpret_b2_error(400, 'part_sha1_mismatch', '', {'fileId': 'my-file-id'})),
        )

    def test_unauthorized(self):
        self._check_one(Unauthorized, 401, '', '')

    def test_invalid_auth_token(self):
        self._check_one(InvalidAuthToken, 401, 'bad_auth_token', '')
        self._check_one(InvalidAuthToken, 401, 'expired_auth_token', '')

    def test_storage_cap_exceeded(self):
        self._check_one(StorageCapExceeded, 403, 'storage_cap_exceeded', '')

    def test_conflict(self):
        self._check_one(Conflict, 409, '', '')

    def test_too_many_requests(self):
        self._check_one(TooManyRequests, 429, '', '')

    def test_service_error(self):
        error = interpret_b2_error(500, 'code', 'message')
        self.assertTrue(isinstance(error, ServiceError))
        self.assertEqual('500 code message', str(error))

    def test_unknown_error(self):
        error = interpret_b2_error(499, 'code', 'message')
        self.assertTrue(isinstance(error, UnknownError))
        self.assertEqual('Unknown error: 499 code message', str(error))

    def _check_one(self, expected_class, status, code, message, post_params=None):
        actual_exception = interpret_b2_error(status, code, message, post_params)
        self.assertTrue(isinstance(actual_exception, expected_class))
