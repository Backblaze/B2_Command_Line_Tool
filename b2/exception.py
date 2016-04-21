######################################################################
#
# File: b2/exception.py
#
# Copyright 2016 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################


class B2Error(Exception):
    def should_retry_http(self):
        """
        Returns true if this is an error that can cause an HTTP
        call to be retried.
        """
        return False

    def should_retry_upload(self):
        """
        Returns true if this is an error that should tell the upload
        code to get a new upload URL and try the upload again.
        """
        return False


class AlreadyFailed(B2Error):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return 'Already failed: %s' % (self.message,)


class BadJson(B2Error):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return 'Bad request: %s' % (self.message,)


class BadFileInfo(B2Error):
    def __init__(self, data):
        self.data = data

    def __str__(self):
        return 'Bad file info: %s' % (self.data,)


class BadUploadUrl(B2Error):
    def __str__(self):
        return 'Bad upload URL: %s' % (self.message,)


class BrokenPipe(B2Error):
    def __str__(self):
        return 'Broken pipe: unable to send entire request'

    def should_retry_upload(self):
        return True


class ChecksumMismatch(B2Error):
    def __init__(self, checksum_type, expected, actual):
        self.checksum_type = checksum_type
        self.expected = expected
        self.actual = actual

    def __str__(self):
        return '%s checksum mismatch -- bad data' % (self.checksum_type,)


class CommandError(B2Error):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message


class CorruptAccountInfo(B2Error):
    def __init__(self, file_name):
        self.file_name = file_name

    def __str__(self):
        return 'Account info file (%s) appears corrupted.  Try removing and then re-authorizing the account.' % (
            self.file_name,
        )


class ConnectionError(B2Error):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return 'Connection error: %s' % (self.message,)

    def should_retry_http(self):
        return True

    def should_retry_upload(self):
        return True


class DestFileNewer(B2Error):
    def __init__(self, file_name):
        self.file_name = file_name

    def __str__(self):
        return 'destination file is newer: %s' % (self.file_name,)

    def should_retry_http(self):
        return True


class DuplicateBucketName(B2Error):
    def __init__(self, bucket_name):
        self.bucket_name = bucket_name

    def __str__(self):
        return 'Bucket name is already in use: %s' % (self.bucket_name,)


class FileAlreadyHidden(B2Error):
    def __init__(self, file_name):
        self.file_name = file_name

    def __str__(self):
        return 'File already hidden: %s' % (self.file_name,)


class FileNotPresent(B2Error):
    def __init__(self, file_name):
        self.file_name = file_name

    def __str__(self):
        return 'File not present: %s' % (self.file_name,)


class InvalidAuthToken(B2Error):
    def __init__(self, message, _type):
        self.message = message
        self._type = _type

    def __str__(self):
        return 'Invalid authorization token. Server said: %s (%s)' % (self.message, self._type)

    def should_retry_upload(self):
        return True


class MaxFileSizeExceeded(B2Error):
    def __init__(self, size, max_allowed_size):
        self.size = size
        self.max_allowed_size = max_allowed_size

    def __str__(self):
        return 'Allowed file size of exceeded: %s > %s' % (self.size, self.max_allowed_size,)


class MaxRetriesExceeded(B2Error):
    def __init__(self, limit, exception_info_list):
        self.limit = limit
        self.exception_info_list = exception_info_list

    def __str__(self):
        exceptions = '\n'.join(str(wrapped_error) for wrapped_error in self.exception_info_list)
        return 'FAILED to upload after %s tries. Encountered exceptions: %s' % (
            self.limit,
            exceptions,
        )


class MissingAccountData(B2Error):
    def __init__(self, key):
        self.key = key

    def __str__(self):
        return 'Missing account data: %s' % (self.key,)


class MissingPart(B2Error):
    def __init__(self, key):
        self.key = key

    def __str__(self):
        return 'Part number has not been uploaded: %s' % (self.key,)


class NonExistentBucket(B2Error):
    def __init__(self, bucket_name_or_id):
        self.bucket_name_or_id = bucket_name_or_id

    def __str__(self):
        return 'No such bucket: %s' % (self.bucket_name_or_id,)


class PartSha1Mismatch(B2Error):
    def __init__(self, key):
        self.key = key

    def __str__(self):
        return 'Part number %s has wrong SHA1' % (self.key,)


class ServiceError(B2Error):
    """
    Used for HTTP status codes 500 through 599.
    """

    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message

    def should_retry_http(self):
        return True

    def should_retry_upload(self):
        return True


class StorageCapExceeded(B2Error):
    def __str__(self):
        return 'Cannot upload files, storage cap exceeded.'


class TooManyRequests(B2Error):
    def __str__(self):
        return 'Too many requests'

    def should_retry_http(self):
        return True


class TruncatedOutput(B2Error):
    def __init__(self, bytes_read, file_size):
        self.bytes_read = bytes_read
        self.file_size = file_size

    def __str__(self):
        return 'only %d of %d bytes read' % (self.bytes_read, self.file_size,)


class UnknownError(B2Error):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return 'unknown error: %s' % (self.message,)


class UnknownHost(B2Error):
    def __str__(self):
        return 'unknown host'


class UnrecognizedBucketType(B2Error):
    def __init__(self, type_):
        self.type_ = type_

    def __str__(self):
        return 'Unrecognized bucket type: %s' % (self.type_,)


def interpret_b2_error(status, code, message, post_params=None):
    post_params = post_params or {}
    if status == 400 and code == "already_hidden":
        return FileAlreadyHidden(post_params['fileName'])
    elif status == 400 and code == 'bad_json':
        return BadJson(message)
    elif status == 400 and code in ("no_such_file", "file_not_present"):
        # hide_file returns "no_such_file"
        # delete_file_version returns "file_not_present"
        return FileNotPresent(post_params['fileName'])
    elif status == 400 and code == "duplicate_bucket_name":
        return DuplicateBucketName(post_params['bucketName'])
    elif status == 400 and code == "missing_part":
        return MissingPart(post_params['fileId'])
    elif status == 400 and code == "part_sha1_mismatch":
        return PartSha1Mismatch(post_params['fileId'])
    elif status == 401 and code in ("bad_auth_token", "expired_auth_token"):
        return InvalidAuthToken(message, code)
    elif status == 403 and code == "storage_cap_exceeded":
        return StorageCapExceeded()
    elif status == 429:
        return TooManyRequests()
    elif 500 <= status and status < 600:
        return ServiceError('%d %s %s' % (status, code, message))
    else:
        return UnknownError('%d %s %s' % (status, code, message))
