######################################################################
#
# File: b2/exception.py
#
# Copyright 2016 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import six
import traceback
from abc import ABCMeta, abstractmethod


class B2Error(Exception):
    pass


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
        return 'Bad uplod URL: %s' % (self.message,)


class ChecksumMismatch(B2Error):
    def __init__(self, checksum_type, expected, actual):
        self.checksum_type = checksum_type
        self.expected = expected
        self.actual = actual

    def __str__(self):
        return '%s checksum mismatch -- bad data' % (self.checksum_type,)


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


class FatalError(B2Error):
    def __init__(self, message, exception_tuples):
        self.message = message
        self.exception_tuples = exception_tuples

    def __str__(self):
        return 'FATAL ERROR: %s\nstacktraces:\n%s' % (
            self.message,
            "\n\n".join(
                "".join(traceback.format_exception(type_, value, tb))
                for type_, value, tb in self.exception_tuples
            ),
        )


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
        exceptions = '\n'.join(
            wrapped_error.format_exception() for wrapped_error in self.exception_info_list
        )
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


class StorageCapExceeded(B2Error):
    def __str__(self):
        return 'Cannot upload files, storage cap exceeded.'


class TruncatedOutput(B2Error):
    def __init__(self, bytes_read, file_size):
        self.bytes_read = bytes_read
        self.file_size = file_size

    def __str__(self):
        return 'only %d of %d bytes read' % (self.bytes_read, self.file_size,)


class UnrecognizedBucketType(B2Error):
    def __init__(self, type_):
        self.type_ = type_

    def __str__(self):
        return 'Unrecognized bucket type: %s' % (self.type_,)


@six.add_metaclass(ABCMeta)
class AbstractWrappedError(B2Error):
    def __init__(self, data, url, params, headers, exc_info):
        self.data = data
        self.url = url
        self.params = params
        self.headers = headers
        self.exc_info = exc_info

    def format_exception(self):
        """
        example output:

        Error returned from server:

        URL: https://pod-000-1004-00.backblaze.com/b2api/v1/b2_upload_file/424242424242424242424242/c001_v0001004_t0028
        Params: None
        Headers: {'X-Bz-Content-Sha1': '753ca1c2d0f3e8748320b38f5da057767029a036', 'X-Bz-File-Name': 'LICENSE', 'Content-Type': 'b2/x-auto', 'Content-Length': '1350'}

        {
          "code": "internal_error",
          "message": "Internal server error",
          "status": 500
        }

        Traceback (most recent call last):
          File "./b2", line 873, in __enter__
            self.file = urllib2.urlopen(request)
          File "/usr/lib/python2.7/urllib2.py", line 127, in urlopen
            return _opener.open(url, data, timeout)
          File "/usr/lib/python2.7/urllib2.py", line 410, in open
            response = meth(req, response)
          File "/usr/lib/python2.7/urllib2.py", line 523, in http_response
            'http', request, response, code, msg, hdrs)
          File "/usr/lib/python2.7/urllib2.py", line 448, in error
            return self._call_chain(*args)
          File "/usr/lib/python2.7/urllib2.py", line 382, in _call_chain
            result = func(*args)
          File "/usr/lib/python2.7/urllib2.py", line 531, in http_error_default
            raise HTTPError(req.get_full_url(), code, msg, hdrs, fp)
        HTTPError: HTTP Error 500: Internal server error
        """
        exc_type, exc_value, exc_traceback = self.exc_info
        return '%s\n\n%s\n' % (
            self, "".join(
                traceback.format_exception(
                    exc_type,
                    exc_value,
                    exc_traceback,
                )
            )
        )

    @abstractmethod
    def should_retry(self):
        pass

    def __str__(self):
        return """Error returned from server:

URL: %s
Params: %s
Headers: %s

%s""" % (self.url, self.params, self.headers, self.data.decode('utf-8'))


class WrappedHttpError(AbstractWrappedError):
    @property
    def code(self):
        return self.exc_info[1].code

    def should_retry(self):
        return 500 <= self.code < 600


class WrappedHttplibError(AbstractWrappedError):
    def should_retry(self):
        return not isinstance(
            self.exc_info[0], six.moves.http_client.InvalidURL
        )  # raised if a port is given and is either non-numeric or empty


class WrappedUrlError(AbstractWrappedError):
    def should_retry(self):
        """
        common case is that self.data == (104, 'Connection reset by peer')
        but there are others like timeout etc
        """
        return True


class WrappedSocketError(AbstractWrappedError):
    def should_retry(self):
        return True


def map_error_dict_to_exception(wrapped_exception, error_dict, post_params):
    status = error_dict.get('status')
    code = error_dict.get('code')
    if status == 400 and code == "already_hidden":
        return FileAlreadyHidden(post_params['fileName'])
    elif status == 400 and code == 'bad_json':
        return BadJson(error_dict.get('message'))
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
        return InvalidAuthToken(error_dict.get('message'), code)
    elif status == 403 and code == "storage_cap_exceeded":
        return StorageCapExceeded()
    else:
        return wrapped_exception
