######################################################################
#
# File: b2/exception.py
#
# Copyright 2016 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from abc import ABCMeta

import six

from .utils import camelcase_to_underscore
a = 0

@six.add_metaclass(ABCMeta)
class B2Error(Exception):
    def __init__(self, *args, **kwargs):
        super(B2Error, self).__init__(*args, **kwargs)
        global a
        a += 1
    @property
    def prefix(self):
        """
        nice auto-generated error message prefix
        >>> B2SimpleError().prefix
        'Simple error'
        >>> AlreadyFailed().prefix
        'Already failed'
        """
        prefix = self.__class__.__name__
        if prefix.startswith('B2'):
            prefix = prefix[2:]
        prefix = camelcase_to_underscore(prefix).replace('_', ' ')
        return prefix[0].upper() + prefix[1:]

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


@six.add_metaclass(ABCMeta)
class B2SimpleError(B2Error):
    """
    a B2Error with a message prefix
    """

    def __str__(self):
        return '%s: %s' % (self.prefix, super(B2SimpleError, self).__str__())


@six.add_metaclass(ABCMeta)
class TransientErrorMixin(object):
    def should_retry_http(self):
        return True

    def should_retry_upload(self):
        return True


class AlreadyFailed(B2SimpleError):
    pass


class BadJson(B2SimpleError):
    prefix = 'Bad request'


class BadFileInfo(B2SimpleError):
    pass


class BadUploadUrl(TransientErrorMixin, B2SimpleError):
    pass


class BrokenPipe(B2Error):
    def __str__(self):
        return 'Broken pipe: unable to send entire request'

    def should_retry_upload(self):
        return True


class ChecksumMismatch(TransientErrorMixin, B2Error):
    def __init__(self, checksum_type, expected, actual):
        super(ChecksumMismatch, self).__init__()
        self.checksum_type = checksum_type
        self.expected = expected
        self.actual = actual

    def __str__(self):
        return '%s checksum mismatch -- bad data' % (self.checksum_type,)


class CommandError(B2Error):
    """
    b2 command error (user caused). Accepts exactly one argument.
    We expect users of shell scripts will parse our __str__ output.
    """

    def __init__(self, message):
        super(CommandError, self).__init__()
        self.message = message

    def __str__(self):
        return self.message


class B2ConnectionError(TransientErrorMixin, B2SimpleError):
    pass


class B2RequestTimeout(TransientErrorMixin, B2SimpleError):
    pass


class DestFileNewer(B2SimpleError):
    prefix = 'destination file is newer'

    def should_retry_http(self):
        return True


class DuplicateBucketName(B2SimpleError):
    prefix = 'Bucket name is already in use'


class FileAlreadyHidden(B2SimpleError):
    pass


class FileNotPresent(B2SimpleError):
    pass


class InvalidAuthToken(B2Error):
    def __init__(self, message, _type):
        super(InvalidAuthToken, self).__init__()
        self.message = message
        self._type = _type

    def __str__(self):
        return 'Invalid authorization token. Server said: %s (%s)' % (self.message, self._type)

    def should_retry_upload(self):
        return True


class MaxFileSizeExceeded(B2Error):
    def __init__(self, size, max_allowed_size):
        super(MaxFileSizeExceeded, self).__init__()
        self.size = size
        self.max_allowed_size = max_allowed_size

    def __str__(self):
        return 'Allowed file size of exceeded: %s > %s' % (self.size,
                                                           self.max_allowed_size,)


class MaxRetriesExceeded(B2Error):
    def __init__(self, limit, exception_info_list):
        super(MaxRetriesExceeded, self).__init__()
        self.limit = limit
        self.exception_info_list = exception_info_list

    def __str__(self):
        exceptions = '\n'.join(str(wrapped_error) for wrapped_error in self.exception_info_list)
        return 'FAILED to upload after %s tries. Encountered exceptions: %s' % (
            self.limit,
            exceptions,
        )


class MissingPart(B2SimpleError):
    prefix = 'Part number has not been uploaded'


class NonExistentBucket(B2SimpleError):
    prefix = 'No such bucket'


class PartSha1Mismatch(B2Error):
    def __init__(self, key):
        super(PartSha1Mismatch, self).__init__()
        self.key = key

    def __str__(self):
        return 'Part number %s has wrong SHA1' % (self.key,)


class ServiceError(TransientErrorMixin, B2Error):
    """
    Used for HTTP status codes 500 through 599.
    """


class StorageCapExceeded(B2Error):
    def __str__(self):
        return 'Cannot upload files, storage cap exceeded.'


class TooManyRequests(B2Error):
    def __str__(self):
        return 'Too many requests'

    def should_retry_http(self):
        return True


class TruncatedOutput(TransientErrorMixin, B2Error):
    def __init__(self, bytes_read, file_size):
        super(TruncatedOutput, self).__init__()
        self.bytes_read = bytes_read
        self.file_size = file_size

    def __str__(self):
        return 'only %d of %d bytes read' % (self.bytes_read,
                                             self.file_size,)


class UnknownError(B2SimpleError):
    pass


class UnknownHost(B2Error):
    def __str__(self):
        return 'unknown host'


class UnrecognizedBucketType(B2Error):
    pass


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
