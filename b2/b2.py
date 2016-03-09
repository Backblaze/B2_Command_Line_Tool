#!/usr/bin/env python
######################################################################
#
# File: b2
#
# Copyright 2015 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

# Note on #! line: There doesn't seem to be one that works for
# everybody.  Most users of this program are Mac users who use the
# default Python installed in OSX, which is called "python" or
# "python2.7", but not "python2".  So we don't use "python2".
"""
This is a B2 command-line tool.  See the USAGE message for details.
"""

from __future__ import print_function
from abc import ABCMeta, abstractmethod
import base64
import datetime
import functools
import getpass
import hashlib
import json
import os
import six
from six.moves import urllib
import socket
import stat
import sys
import time
import traceback

try:
    from tqdm import tqdm  # displays a nice progress bar
except ImportError:
    tqdm = None  # noqa

# To avoid confusion between official Backblaze releases of this tool and
# the versions on Github, we use the convention that the third number is
# odd for Github, and even for Backblaze releases.
VERSION = '0.4.5'

PYTHON_VERSION = '.'.join(map(str, sys.version_info[:3]))  # something like: 2.7.11

USER_AGENT = 'backblaze-b2/%s python/%s' % (VERSION, PYTHON_VERSION)

USAGE = """This program provides command-line access to the B2 service.

Usages:

    b2 authorize_account [--dev | --staging | --production] [accountId] [applicationKey]

        Prompts for Backblaze accountID and applicationKey (unless they are given
        on the command line).

        The account ID is a 12-digit hex number that you can get from
        your account page on backblaze.com.

        The application key is a 40-digit hex number that you can get from
        your account page on backblaze.com.

        Stores an account auth token in ~/.b2_account_info.  This can be overridden using the
        B2_ACCOUNT_INFO environment variable.

    b2 clear_account

        Erases everything in ~/.b2_account_info

    b2 create_bucket <bucketName> <bucketType>

        Creates a new bucket.  Prints the ID of the bucket created.

    b2 delete_bucket <bucketName>

        Deletes the bucket with the given name.

    b2 delete_file_version <fileName> <fileId>

        Permanently and irrevocably deletes one version of a file.

    b2 download_file_by_id <fileId> <localFileName>

        Downloads the given file, and stores it in the given local file.

    b2 download_file_by_name <bucketName> <fileName> <localFileName>

        Downloads the given file, and stores it in the given local file.

    b2 get_file_info <fileId>

        Prints all of the information about the file, but not its contents.

    b2 hide_file <bucketName> <fileName>

        Uploads a new, hidden, version of the given file.

    b2 list_buckets

        Lists all of the buckets in the current account.

    b2 list_file_names <bucketName> [<startingName>] [<numberToShow>]

        Lists the names of the files in a bucket, starting at the
        given point.

    b2 list_file_versions <bucketName> [<startingName>] [<startingFileId>] [<numberToShow>]

        Lists the names of the files in a bucket, starting at the
        given point.

    b2 ls [--long] [--versions] <bucketName> [<folderName>]

        Using the file naming convention that "/" separates folder
        names from their contents, returns a list of the files
        and folders in a given folder.  If no folder name is given,
        lists all files at the top level.

        The --long option produces very wide multi-column output
        showing the upload date/time, file size, file id, whether it
        is an uploaded file or the hiding of a file, and the file
        name.  Folders don't really exist in B2, so folders are
        shown with "-" in each of the fields other than the name.

        The --version option shows all of versions of each file, not
        just the most recent.

    b2 make_url <fileId>

        Prints an URL that can be used to download the given file, if
        it is public.

    b2 sync [--delete] [--hide] <source> <destination>

        UNDER DEVELOPMENT -- there may be changes coming to this command

        Uploads or downloads multiple files from source to destination.
        One of the paths must be a local file path, and the other must be
        a B2 bucket path. Use "b2:<bucketName>/<prefix>" for B2 paths, e.g.
        "b2:my-bucket-name/a/path/prefix/".

        If the --delete or --hide flags are specified, destination files
        are deleted or hidden if not present in the source path. Note that
        files are matched only by name and size.

    b2 update_bucket <bucketName> <bucketType>

        Updates the bucketType of an existing bucket.  Prints the ID
        of the bucket updated.

    b2 upload_file [--sha1 <sha1sum>] [--contentType <contentType>] [--info <key>=<value>]* <bucketName> <localFilePath> <b2FileName>

        Uploads one file to the given bucket.  Uploads the contents
        of the local file, and assigns the given name to the B2 file.

        By default, upload_file will compute the sha1 checksum of the file
        to be uploaded.  But, if you already have it, you can provide it
        on the command line to save a little time.

        Content type is optional.  If not set, it will be set based on the
        file extension.

        If `tqdm` library is installed, progress bar is displayed on stderr.
        (use pip install tqdm to install it)

        Each fileInfo is of the form "a=b".

    b2 version

        Echos the version number of this program.
"""

## Exceptions


class B2Error(Exception):
    pass


class BadJson(B2Error):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return 'Generic api error ("bad_json"): %s' % (self.message,)


class BadFileInfo(B2Error):
    def __init__(self, data):
        self.data = data

    def __str__(self):
        return 'Bad file info: %s' % (self.data,)


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


class MaxFileSizeExceeded(B2Error):
    def __init__(self, file_description, size, max_allowed_size):
        self.file_description = file_description
        self.size = size
        self.max_allowed_size = max_allowed_size

    def __str__(self):
        return 'Allowed file size of exceeded for %s: %s > %s' % (
            self.file_description,
            self.size,
            self.max_allowed_size,
        )


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


class NonExistentBucket(B2Error):
    def __init__(self, bucket_name):
        self.bucket_name = bucket_name

    def __str__(self):
        return 'No such bucket: %s' % (self.bucket_name,)


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

## Bucket


@six.add_metaclass(ABCMeta)
class Bucket(object):
    DEFAULT_CONTENT_TYPE = 'b2/x-auto'
    MAX_UPLOAD_ATTEMPTS = 5
    MAX_UPLOADED_FILE_SIZE = 5 * 1000 * 1000 * 1000

    def __init__(self, api, id_, name=None, type_=None):
        self.api = api
        self.id_ = id_
        self.name = name
        self.type_ = type_

    def get_id(self):
        return self.id_

    def set_type(self, type_):
        account_info = self.api.account_info
        auth_token = account_info.get_account_auth_token()
        account_id = account_info.get_account_id()
        return self.api.raw_api.update_bucket(
            account_info.get_api_url(), auth_token, account_id, self.id_, type_
        )

    def ls(self, folder_to_list='', show_versions=False, max_entries=None, recursive=False):
        """Pretends that folders exist, and yields the information about the files in a folder.

        B2 has a flat namespace for the files in a bucket, but there is a convention
        of using "/" as if there were folders.  This method searches through the
        flat namespace to find the files and "folders" that live within a given
        folder.

        When the `recursive` flag is set, lists all of the files in the given
        folder, and all of its sub-folders.

        :param folder: The name of the folder to list.  Must not start with "/".
                       Empty string means top-level folder.
        :param show_versions: When true returns info about all versions of a file,
                              when false, just returns info about the most recent
                              versions.
        :param max_entries: How many entries to return.  1 - 1000
        :param recursive:
        :return:
        """
        auth_token = self.api.account_info.get_account_auth_token()

        # Every file returned must have a name that starts with the
        # folder name and a "/".
        prefix = folder_to_list
        if prefix != '' and not prefix.endswith('/'):
            prefix += '/'

        # Loop until all files in the named directory have been listed.
        # The starting point of the first list_file_names request is the
        # prefix we're looking for.  The prefix ends with '/', which is
        # now allowed for file names, so no file name will match exactly,
        # but the first one after that point is the first file in that
        # "folder".   If the first search doesn't produce enough results,
        # then we keep calling list_file_names until we get all of the
        # names in this "folder".
        current_dir = None
        start_file_name = prefix
        start_file_id = None
        raw_api = self.api.raw_api
        api_url = self.api.account_info.get_api_url()
        auth_token = self.api.account_info.get_account_auth_token()
        while True:
            params = {'bucketId': self.id_, 'startFileName': start_file_name}
            if start_file_id is not None:
                params['startFileId'] = start_file_id
            if show_versions:
                response = raw_api.list_file_versions(
                    api_url, auth_token, self.id_, start_file_name, start_file_id
                )
            else:
                response = raw_api.list_file_names(api_url, auth_token, self.id_, start_file_name)
            for entry in response['files']:
                file_version_info = FileVersionInfoFactory.from_api_response(entry)
                if not file_version_info.file_name.startswith(prefix):
                    # We're past the files we care about
                    return
                after_prefix = file_version_info.file_name[len(prefix):]
                if '/' not in after_prefix or recursive:
                    # This is not a folder, so we'll print it out and
                    # continue on.
                    yield file_version_info, None
                    current_dir = None
                else:
                    # This is a folder.  If it's different than the folder
                    # we're already in, then we can print it.  This check
                    # is needed, because all of the files in the folder
                    # will be in the list.
                    folder_with_slash = after_prefix.split('/')[0] + '/'
                    if folder_with_slash != current_dir:
                        folder_name = prefix + folder_with_slash
                        yield file_version_info, folder_name
                        current_dir = folder_with_slash
            if response['nextFileName'] is None:
                # The response says there are no more files in the bucket,
                # so we can stop.
                return

            # Now we need to set up the next search.  The response from
            # B2 has the starting point to continue with the next file,
            # but if we're in the middle of a "folder", we can skip ahead
            # to the end of the folder.  The character after '/' is '0',
            # so we'll replace the '/' with a '0' and start there.
            #
            # When recursive is True, current_dir is always None.
            if current_dir is None:
                start_file_name = response.get('nextFileName')
                start_file_id = response.get('nextFileId')
            else:
                start_file_name = max(response['nextFileName'], prefix + current_dir[:-1] + '0',)

    def list_file_names(self, start_filename=None, max_entries=None):
        """ legacy interface which just returns whatever remote API returns """
        auth_token = self.api.account_info.get_account_auth_token()
        url = url_for_api(self.api.account_info, 'b2_list_file_names')
        params = {
            'bucketId': self.id_,
            'startFileName': start_filename,
            'maxFileCount': max_entries,
        }
        return post_json(url, params, auth_token)

    def list_file_versions(self, start_filename=None, start_file_id=None, max_entries=None):
        """ legacy interface which just returns whatever remote API returns """
        auth_token = self.api.account_info.get_account_auth_token()
        url = url_for_api(self.api.account_info, 'b2_list_file_versions')
        params = {
            'bucketId': self.id_,
            'startFileName': start_filename,
            'startFileId': start_file_id,
            'maxFileCount': max_entries,
        }
        return post_json(url, params, auth_token)

    def upload_bytes(
        self,
        data_bytes,
        remote_filename,
        content_type=None,
        file_infos=None,
        quiet=False
    ):
        """
        Upload bytes in memory to a B2 file
        """

    def upload_file(
        self,
        local_file,
        remote_filename,
        content_type=None,
        file_infos=None,
        sha1_sum=None,
        quiet=False
    ):
        """
        Uploads a file on local disk to a B2 file.
        """
        # Get the number of bytes in the local file
        size = os.path.getsize(local_file)
        if size > self.MAX_UPLOADED_FILE_SIZE:
            raise MaxFileSizeExceeded(local_file, size, self.MAX_UPLOADED_FILE_SIZE)

        # Compute the SHA1 sum if the data, if the caller didn't provide it.
        if sha1_sum is None:
            sha1_sum = hex_sha1_of_file(local_file)

        # Make a function for opening the file, and wrapping it with
        # a progress bar.
        def open_file():
            stream = open(local_file, 'rb')
            if not quiet:
                stream = StreamWithProgress(stream, desc=local_file, total=size)
            return stream

        # Upload the file
        return self._upload(open_file, remote_filename, size, content_type, file_infos, sha1_sum)

    def _upload(self, opener, remote_filename, size, content_type, file_infos, sha1_sum):
        """
        Uploads a file, retrying as needed.

        The function `opener` should return a file-like object, and it
        must be possible to call it more than once in case the upload
        is retried.
        """
        assert sha1_sum is not None
        if file_infos is None:
            file_infos = {}
        if content_type is None:
            content_type = self.DEFAULT_CONTENT_TYPE

        account_info = self.api.account_info

        # Use forward slashes for remote
        # TODO: move this up a layer, to the ConsoleTool
        if os.sep != '/':
            remote_filename = remote_filename.replace(os.sep, '/')

        exception_info_list = []
        for i in six.moves.xrange(self.MAX_UPLOAD_ATTEMPTS):
            # refresh upload data in every attempt to work around a "busy storage pod"
            upload_url, upload_auth_token = self._get_upload_data()

            try:
                with opener() as data_stream:
                    upload_response = self.api.raw_api.upload_file(
                        upload_url, upload_auth_token, remote_filename, size, content_type,
                        sha1_sum, file_infos, data_stream
                    )
                    return FileVersionInfoFactory.from_api_response(upload_response)

            except AbstractWrappedError as e:
                if not e.should_retry():
                    raise
                exception_info_list.append(e)
                account_info.clear_bucket_upload_data(self.id_)

        raise MaxRetriesExceeded(self.MAX_UPLOAD_ATTEMPTS, exception_info_list)

    def _get_upload_data(self):
        """
        Makes sure that we have an upload URL and auth token for the given bucket and
        returns it.
        """
        account_info = self.api.account_info
        upload_url, upload_auth_token = account_info.get_bucket_upload_data(self.id_)
        if None not in (upload_url, upload_auth_token):
            return upload_url, upload_auth_token

        response = self.api.raw_api.get_upload_url(
            account_info.get_api_url(), account_info.get_account_auth_token(), self.id_
        )
        account_info.set_bucket_upload_data(
            self.id_,
            response['uploadUrl'],
            response['authorizationToken'],
        )
        return account_info.get_bucket_upload_data(self.id_)

    def get_download_url(self, filename):
        return "%s/file/%s/%s" % (
            self.api.account_info.get_download_url(),
            b2_url_encode(self.name),
            b2_url_encode(filename),
        )

    def hide_file(self, file_name):
        account_info = self.api.account_info
        auth_token = account_info.get_account_auth_token()
        response = self.api.raw_api.hide_file(
            self.api.account_info.get_api_url(), auth_token, self.id_, file_name
        )
        return FileVersionInfoFactory.from_api_response(response)

    def as_dict(self):  # TODO: refactor with other as_dict()
        result = {'accountId': self.api.account_info.get_account_id(), 'bucketId': self.id_,}
        if self.name is not None:
            result['bucketName'] = self.name
        if self.type_ is not None:
            result['bucketType'] = self.type_
        return result

    def __repr__(self):
        return 'Bucket<%s,%s,%s>' % (self.id_, self.name, self.type_)


class BucketFactory(object):
    @classmethod
    def from_api_response(cls, api, response):
        return [cls.from_api_bucket_dict(api, bucket_dict) for bucket_dict in response['buckets']]

    @classmethod
    def from_api_bucket_dict(cls, api, bucket_dict):
        """
            turns this:
            {
                "bucketType": "allPrivate",
                "bucketId": "a4ba6a39d8b6b5fd561f0010",
                "bucketName": "zsdfrtsazsdfafr",
                "accountId": "4aa9865d6f00"
            }
            into a Bucket object
        """
        bucket_name = bucket_dict['bucketName']
        bucket_id = bucket_dict['bucketId']
        type_ = bucket_dict['bucketType']
        if type_ is None:
            raise UnrecognizedBucketType(bucket_dict['bucketType'])
        return Bucket(api, bucket_id, bucket_name, type_)

## DAO


class FileVersionInfo(object):
    LS_ENTRY_TEMPLATE = '%83s  %6s  %10s  %8s  %9d  %s'  # order is file_id, action, date, time, size, name

    def __init__(self, id_, file_name, size, upload_timestamp, action):
        self.id_ = id_
        self.file_name = file_name
        self.size = size  # can be None (unknown)
        self.upload_timestamp = upload_timestamp  # can be None (unknown)
        self.action = action  # "upload" or "hide" or "delete"

    def as_dict(self):
        result = {'fileId': self.id_, 'fileName': self.file_name,}

        if self.size is not None:
            result['size'] = self.size
        if self.upload_timestamp is not None:
            result['uploadTimestamp'] = self.upload_timestamp
        if self.action is not None:
            result['action'] = self.action
        return result

    def format_ls_entry(self):
        dt = datetime.datetime.utcfromtimestamp(self.upload_timestamp / 1000)
        date_str = dt.strftime('%Y-%m-%d')
        time_str = dt.strftime('%H:%M:%S')
        size = self.size or 0  # required if self.action == 'hide'
        return self.LS_ENTRY_TEMPLATE % (
            self.id_,
            self.action,
            date_str,
            time_str,
            size,
            self.file_name,
        )

    @classmethod
    def format_folder_ls_entry(cls, name):
        return cls.LS_ENTRY_TEMPLATE % ('-', '-', '-', '-', 0, name)


class FileVersionInfoFactory(object):
    @classmethod
    def from_api_response(cls, file_info_dict, force_action=None):
        """
            turns this:
            {
              "action": "hide",
              "fileId": "4_zBucketName_f103b7ca31313c69c_d20151230_m030117_c001_v0001015_t0000",
              "fileName": "randomdata",
              "size": 0,
              "uploadTimestamp": 1451444477000
            }
            or this:
            {
              "accountId": "4aa9865d6f00",
              "bucketId": "547a2a395826655d561f0010",
              "contentLength": 1350,
              "contentSha1": "753ca1c2d0f3e8748320b38f5da057767029a036",
              "contentType": "application/octet-stream",
              "fileId": "4_z547a2a395826655d561f0010_f106d4ca95f8b5b78_d20160104_m003906_c001_v0001013_t0005",
              "fileInfo": {},
              "fileName": "randomdata"
            }
            into a FileVersionInfo object
        """
        assert file_info_dict.get('action') is None or force_action is None, \
            'action was provided by both info_dict and function argument'
        action = file_info_dict.get('action') or force_action
        file_name = file_info_dict['fileName']
        id_ = file_info_dict['fileId']
        size = file_info_dict.get('size') or file_info_dict.get('contentLength')
        upload_timestamp = file_info_dict.get('uploadTimestamp')

        return FileVersionInfo(id_, file_name, size, upload_timestamp, action)

## Cache


@six.add_metaclass(ABCMeta)
class AbstractCache(object):
    @abstractmethod
    def get_bucket_id_or_none_from_bucket_name(self, name):
        pass

    @abstractmethod
    def save_bucket(self, bucket):
        pass

    @abstractmethod
    def set_bucket_name_cache(self, buckets):
        pass

    def _name_id_iterator(self, buckets):
        return ((bucket.name, bucket.id_) for bucket in buckets)


class DummyCache(AbstractCache):
    """ Cache that does nothing """

    def get_bucket_id_or_none_from_bucket_name(self, name):
        return None

    def save_bucket(self, bucket):
        pass

    def set_bucket_name_cache(self, buckets):
        pass


class InMemoryCache(AbstractCache):
    """ Cache that stores the information in memory """

    def __init__(self):
        self.name_id_map = {}

    def get_bucket_id_or_none_from_bucket_name(self, name):
        return self.name_id_map.get(name)

    def save_bucket(self, bucket):
        self.name_id_map[bucket.name] = bucket.id_

    def set_bucket_name_cache(self, buckets):
        self.name_id_map = dict(self._name_id_iterator(buckets))


class AuthInfoCache(AbstractCache):
    """ Cache that stores data persistently in StoredAccountInfo """

    def __init__(self, info):
        self.info = info

    def get_bucket_id_or_none_from_bucket_name(self, name):
        return self.info.get_bucket_id_or_none_from_bucket_name(name)

    def save_bucket(self, bucket):
        self.info.save_bucket(bucket)

    def set_bucket_name_cache(self, buckets):
        self.info.refresh_entire_bucket_name_cache(self._name_id_iterator(buckets))

## B2RawApi

# TODO: make an ABC for B2RawApi and RawSimulator


class B2RawApi(object):
    """
    Provides access to the B2 web APIs, exactly as they are provided by B2.

    Requires that you provide all necessary URLs and auth tokens for each call.

    Each API call decodes the returned JSON and returns a dict.

    For details on what each method does, see the B2 docs:
        https://www.backblaze.com/b2/docs/

    This class is intended to be a super-simple, very thin layer on top
    of the HTTP calls.  It can be mocked-out for testing higher layers.
    And this class can be tested by exercising each call just once,
    which is relatively quick.
    """

    def _post_json(self, base_url, api_name, auth, **params):
        """
        Helper method for calling an API with the given auth and params.
        :param base_url: Something like "https://api001.backblaze.com/"
        :param auth: Passed in Authorization header.
        :param api_name: Example: "b2_create_bucket"
        :param args: The rest of the parameters are passed to B2.
        :return:
        """
        url = base_url + '/b2api/v1/' + api_name
        return post_json(url, params, auth)

    def authorize_account(self, realm_url, account_id, application_key):
        auth = b'Basic ' + base64.b64encode(six.b('%s:%s' % (account_id, application_key)))
        return self._post_json(realm_url, 'b2_authorize_account', auth)

    def create_bucket(self, api_url, account_auth_token, account_id, bucket_name, bucket_type):
        return self._post_json(
            api_url,
            'b2_create_bucket',
            account_auth_token,
            accountId=account_id,
            bucketName=bucket_name,
            bucketType=bucket_type
        )

    def delete_bucket(self, api_url, account_auth_token, account_id, bucket_id):
        return self._post_json(
            api_url,
            'b2_delete_bucket',
            account_auth_token,
            accountId=account_id,
            bucketId=bucket_id
        )

    def delete_file_version(self, api_url, account_auth_token, file_id, file_name):
        return self._post_json(
            api_url,
            'b2_delete_file_version',
            account_auth_token,
            fileId=file_id,
            fileName=file_name
        )

    def get_file_info(self, api_url, account_auth_token, file_id):
        return self._post_json(api_url, 'b2_get_file_info', account_auth_token, fileId=file_id)

    def get_upload_url(self, api_url, account_auth_token, bucket_id):
        return self._post_json(api_url, 'b2_get_upload_url', account_auth_token, bucketId=bucket_id)

    def hide_file(self, api_url, account_auth_token, bucket_id, file_name):
        return self._post_json(
            api_url,
            'b2_hide_file',
            account_auth_token,
            bucketId=bucket_id,
            fileName=file_name
        )

    def list_buckets(self, api_url, account_auth_token, account_id):
        return self._post_json(api_url, 'b2_list_buckets', account_auth_token, accountId=account_id)

    def list_file_names(
        self,
        api_url,
        account_auth_token,
        bucket_id,
        start_file_name=None,
        max_file_count=None
    ):
        return self._post_json(
            api_url,
            'b2_list_file_names',
            account_auth_token,
            bucketId=bucket_id,
            startFileName=start_file_name,
            maxFileCount=max_file_count
        )

    def list_file_versions(
        self,
        api_url,
        account_auth_token,
        bucket_id,
        start_file_name=None,
        start_file_id=None,
        max_file_count=None
    ):
        return self._post_json(
            api_url,
            'b2_list_file_versions',
            account_auth_token,
            bucketId=bucket_id,
            startFileName=start_file_name,
            startFileId=start_file_id,
            maxFileCount=max_file_count
        )

    def update_bucket(self, api_url, account_auth_token, account_id, bucket_id, bucket_type):
        return self._post_json(
            api_url,
            'b2_update_bucket',
            account_auth_token,
            accountId=account_id,
            bucketId=bucket_id,
            bucketType=bucket_type
        )

    def upload_file(
        self, upload_url, upload_auth_token, file_name, content_length, content_type, content_sha1,
        file_infos, data_stream
    ):
        """
        Uploads one small file to B2.

        :param upload_url: The upload_url from b2_authorize_account
        :param upload_auth_token: The auth token from b2_authorize_account
        :param file_name: The name of the B2 file
        :param content_length: Number of bytes in the file.
        :param content_type: MIME type.
        :param content_sha1: Hex SHA1 of the contents of the file
        :param file_infos: Extra file info to upload
        :param data_stream: A file like object from which the contents of the file can be read.
        :return:
        """
        headers = {
            'Authorization': upload_auth_token,
            'Content-Length': str(content_length),
            'X-Bz-File-Name': b2_url_encode(file_name),
            'Content-Type': content_type,
            'X-Bz-Content-Sha1': content_sha1
        }
        for k, v in six.iteritems(file_infos):
            headers['X-Bz-Info-' + k] = b2_url_encode(v)

        with OpenUrl(upload_url, data_stream, headers) as response_file:
            json_text = read_str_from_http_response(response_file)
            return json.loads(json_text)

## B2Api


class B2Api(object):
    """
    Provides high-level access to the B2 API.

    Adds an object-oriented layer on top of the raw API, so that
    buckets and files returned are Python objects with accessor
    methods.

    Also,  keeps a cache of information needed to access the service,
    such as auth tokens and upload URLs.
    """

    # TODO: move HTTP code out to B2RawApi
    # TODO: ConsoleTool passes the account info cache into the constructor
    # TODO: provide method to get the account info cache (so ConsoleTool can save it)

    def __init__(self, account_info=None, cache=None, raw_api=None):
        """
        Initializes the API using the given account info.
        :param account_info:
        :param cache:
        :return:
        """
        # TODO: merge account_info and cache into a single object

        self.raw_api = raw_api or B2RawApi()
        if account_info is None:
            account_info = StoredAccountInfo()
            if cache is None:
                cache = AuthInfoCache(account_info)
        self.account_info = account_info
        if cache is None:
            cache = DummyCache()
        self.cache = cache

    def authorize_account(self, realm_url, account_id, application_key):
        response = self.raw_api.authorize_account(realm_url, account_id, application_key)
        self.account_info.set_account_id_and_auth_token(
            response['accountId'], response['authorizationToken'], response['apiUrl'],
            response['downloadUrl']
        )

    # buckets

    def create_bucket(self, name, type_):
        account_id = self.account_info.get_account_id()
        auth_token = self.account_info.get_account_auth_token()

        response = self.raw_api.create_bucket(
            self.account_info.get_api_url(), auth_token, account_id, name, type_
        )
        bucket = BucketFactory.from_api_bucket_dict(self, response)
        assert name == bucket.name, 'API created a bucket with different name\
                                     than requested: %s != %s' % (name, bucket.name)
        assert type_ == bucket.type_, 'API created a bucket with different type\
                                     than requested: %s != %s' % (type_, bucket.type_)
        self.cache.save_bucket(bucket)
        return bucket

    def get_bucket_by_id(self, bucket_id):
        return Bucket(self, bucket_id)

    def get_bucket_by_name(self, bucket_name):
        """
        Returns the bucket_id for the given bucket_name.

        If we don't already know it from the cache, try fetching it from
        the B2 service.
        """
        # If we can get it from the stored info, do that.
        id_ = self.cache.get_bucket_id_or_none_from_bucket_name(bucket_name)
        if id_ is not None:
            return Bucket(self, id_, name=bucket_name)

        for bucket in self.list_buckets():
            if bucket.name == bucket_name:
                return bucket
        raise NonExistentBucket(bucket_name)

    def delete_bucket(self, bucket):
        """
        Deletes the bucket remotely.
        For legacy reasons it returns whatever server sends in response,
        but API user should not rely on the response: if it doesn't raise
        an exception, it means that the operation was a success
        """
        account_id = self.account_info.get_account_id()
        auth_token = self.account_info.get_account_auth_token()
        return self.raw_api.delete_bucket(
            self.account_info.get_api_url(), auth_token, account_id, bucket.id_
        )

    def list_buckets(self):
        """
        Calls b2_list_buckets and returns the JSON for *all* buckets.
        """
        account_id = self.account_info.get_account_id()
        auth_token = self.account_info.get_account_auth_token()

        response = self.raw_api.list_buckets(
            self.account_info.get_api_url(), auth_token, account_id
        )

        buckets = BucketFactory.from_api_response(self, response)

        self.cache.set_bucket_name_cache(buckets)
        return buckets

    # delete
    def delete_file_version(self, file_id, file_name):
        # filename argument is not first, because one day it may become optional
        auth_token = self.account_info.get_account_auth_token()
        response = self.raw_api.delete_file_version(
            self.account_info.get_api_url(), auth_token, file_id, file_name
        )
        file_info = FileVersionInfoFactory.from_api_response(response, force_action='delete',)
        assert file_info.id_ == file_id
        assert file_info.file_name == file_name
        assert file_info.action == 'delete'
        return file_info

    # download
    def get_download_url_for_fileid(self, file_id):
        url = url_for_api(self.account_info, 'b2_download_file_by_id')
        return '%s?fileId=%s' % (url, file_id)

    def download_file_from_url(
        self,
        url,
        output_stream,
        authorization=True,
        headers_received_cb=None,
    ):
        """
        Downloads a file from given url and saves into output_stream.
        if headers_received_cb is not None, it is assumed to be a funciton that accepts one argument
        (dictionary of response headers) and is called after receiving headers, but before receiving
        the payload.
        """
        request_headers = {}
        if authorization:
            request_headers['Authorization'] = self.account_info.get_account_auth_token()
        with OpenUrl(url, None, request_headers) as response:
            info = response.info()
            if headers_received_cb is not None:
                headers_received_cb(info)  # may raise an exception to abort

            file_size = int(info['content-length'])
            file_sha1 = info['x-bz-content-sha1']
            block_size = 4096
            digest = hashlib.sha1()
            bytes_read = 0

            with output_stream as f:
                while 1:
                    data = response.read(block_size)
                    if len(data) == 0:
                        break
                    f.write(data)
                    digest.update(data)
                    bytes_read += len(data)
            if bytes_read != int(info['content-length']):
                raise TruncatedOutput(bytes_read, file_size)
            if digest.hexdigest() != file_sha1:
                raise ChecksumMismatch(
                    checksum_type='sha1',
                    expected=file_sha1,
                    actual=digest.hexdigest()
                )
        return info

    # other
    def get_file_info(self, file_id):
        """ legacy interface which just returns whatever remote API returns """
        auth_token = self.account_info.get_account_auth_token()
        return self.raw_api.get_file_info(self.account_info.get_api_url(), auth_token, file_id)

## v0.3.x functions


def message_and_exit(message):
    """Prints a message, and exits with error status.
    """
    print(message, file=sys.stderr)
    sys.exit(1)


def usage_and_exit():
    """Prints a usage message, and exits with an error status.
    """
    message_and_exit(USAGE)


def decode_sys_argv():
    """
    Returns the command-line arguments as unicode strings, decoding
    whatever format they are in.

    https://stackoverflow.com/questions/846850/read-unicode-characters-from-command-line-arguments-in-python-2-x-on-windows
    """
    encoding = sys.getfilesystemencoding()
    if six.PY2:
        return [arg.decode(encoding) for arg in sys.argv]
    return sys.argv


@six.add_metaclass(ABCMeta)
class AbstractAccountInfo(object):
    """
    Holder for auth token, API URL, and download URL.
    """
    REALM_URLS = {
        'production': 'https://api.backblaze.com',
        'dev': 'http://api.test.blaze:8180',
        'staging': 'https://api.backblaze.net',
    }

    @abstractmethod
    def clear(self):
        """ Removes all stored information """
        pass

    @abstractmethod
    def get_api_url(self):
        pass

    @abstractmethod
    def get_download_url(self):
        pass

    @abstractmethod
    def set_account_id_and_auth_token(self, account_id, auth_token, api_url, download_url):
        pass


class StoredAccountInfo(AbstractAccountInfo):
    """Manages the file that holds the account ID and stored auth tokens.

    When an instance of this class is created, it reads the account
    info file in the home directory of the user, and remembers the info.

    When any changes are made, they are written out to the file.
    """

    ACCOUNT_AUTH_TOKEN = 'account_auth_token'
    ACCOUNT_ID = 'account_id'
    API_URL = 'api_url'
    BUCKET_NAMES_TO_IDS = 'bucket_names_to_ids'
    BUCKET_UPLOAD_DATA = 'bucket_upload_data'
    BUCKET_UPLOAD_URL = 'bucket_upload_url'
    BUCKET_UPLOAD_AUTH_TOKEN = 'bucket_upload_auth_token'
    DOWNLOAD_URL = 'download_url'

    def __init__(self):
        user_account_info_path = os.environ.get('B2_ACCOUNT_INFO', '~/.b2_account_info')
        self.filename = os.path.expanduser(user_account_info_path)
        self.data = self._try_to_read_file()
        if self.BUCKET_UPLOAD_DATA not in self.data:
            self.data[self.BUCKET_UPLOAD_DATA] = {}
        if self.BUCKET_NAMES_TO_IDS not in self.data:
            self.data[self.BUCKET_NAMES_TO_IDS] = {}

    def _try_to_read_file(self):
        try:
            with open(self.filename, 'rb') as f:
                # is there a cleaner way to do this that works in both Python 2 and 3?
                json_str = f.read().decode('utf-8')
                return json.loads(json_str)
        except Exception:
            return {}

    def clear(self):
        self.data = {}
        self._write_file()

    def get_account_id(self):
        return self._get_account_info_or_exit(self.ACCOUNT_ID)

    def get_account_auth_token(self):
        return self._get_account_info_or_exit(self.ACCOUNT_AUTH_TOKEN)

    def get_api_url(self):
        return self._get_account_info_or_exit(self.API_URL)

    def get_download_url(self):
        return self._get_account_info_or_exit(self.DOWNLOAD_URL)

    def _get_account_info_or_exit(self, key):
        """Returns the named field from the account data, or errors and exits.
        """
        result = self.data.get(key)
        if result is None:
            raise MissingAccountData(key)
        return result

    def set_account_id_and_auth_token(self, account_id, auth_token, api_url, download_url):
        self.data[self.ACCOUNT_ID] = account_id
        self.data[self.ACCOUNT_AUTH_TOKEN] = auth_token
        self.data[self.API_URL] = api_url
        self.data[self.DOWNLOAD_URL] = download_url
        self._write_file()

    def set_bucket_upload_data(self, bucket_id, upload_url, upload_auth_token):
        self.data[self.BUCKET_UPLOAD_DATA][bucket_id] = {
            self.BUCKET_UPLOAD_URL: upload_url,
            self.BUCKET_UPLOAD_AUTH_TOKEN: upload_auth_token,
        }
        self._write_file()

    def get_bucket_upload_data(self, bucket_id):
        bucket_upload_data = self.data[self.BUCKET_UPLOAD_DATA].get(bucket_id)
        if bucket_upload_data is None:
            return None, None
        url = bucket_upload_data[self.BUCKET_UPLOAD_URL]
        upload_auth_token = bucket_upload_data[self.BUCKET_UPLOAD_AUTH_TOKEN]
        return url, upload_auth_token

    def clear_bucket_upload_data(self, bucket_id):
        self.data[self.BUCKET_UPLOAD_DATA].pop(bucket_id, None)

    def save_bucket(self, bucket):
        names_to_ids = self.data[self.BUCKET_NAMES_TO_IDS]
        if names_to_ids.get(bucket.name) != bucket.id_:
            names_to_ids[bucket.name] = bucket.id_
            self._write_file()

    def refresh_entire_bucket_name_cache(self, name_id_iterable):
        names_to_ids = self.data[self.BUCKET_NAMES_TO_IDS]
        new_cache = dict(name_id_iterable)
        if names_to_ids != new_cache:
            self.data[self.BUCKET_NAMES_TO_IDS] = new_cache
            self._write_file()

    def remove_bucket_name(self, bucket_name):
        names_to_ids = self.data[self.BUCKET_NAMES_TO_IDS]
        if bucket_name in names_to_ids:
            del names_to_ids[bucket_name]
        self._write_file()

    def get_bucket_id_or_none_from_bucket_name(self, bucket_name):
        names_to_ids = self.data[self.BUCKET_NAMES_TO_IDS]
        return names_to_ids.get(bucket_name)

    def _write_file(self):
        flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
        if os.name == 'nt':
            flags |= os.O_BINARY
        with os.fdopen(os.open(self.filename, flags, stat.S_IRUSR | stat.S_IWUSR), 'wb') as f:
            # is there a cleaner way to do this that works in both Python 2 and 3?
            json_bytes = json.dumps(self.data, indent=4, sort_keys=True).encode('utf-8')
            f.write(json_bytes)


class OpenUrl(object):
    """
    Context manager that handles an open urllib2.Request, and provides
    the file-like object that is the response.
    """

    def __init__(self, url, data, headers, params=None):
        self.url = url
        self.data = data
        self.headers = self._add_user_agent(headers)
        self.file = None
        self.params = None  # for debugging

    def _add_user_agent(self, headers):
        """
        Adds a User-Agent header if there isn't one already.

        Reminder: HTTP header names are case-insensitive.
        """
        for k in headers:
            if k.lower() == 'user-agent':
                return headers
        else:
            result = dict(headers)
            result['User-Agent'] = USER_AGENT
            return result

    def __enter__(self):
        try:
            request = urllib.request.Request(self.url, self.data, self.headers)
            self.file = urllib.request.urlopen(request)
            return self.file
        except urllib.error.HTTPError as e:
            data = e.read()
            raise WrappedHttpError(data, self.url, self.params, self.headers, sys.exc_info())
        except urllib.error.URLError as e:
            raise WrappedUrlError(e.reason, self.url, self.params, self.headers, sys.exc_info())
        except socket.error as e:  # includes socket.timeout
            # reportedly socket errors are not wrapped in urllib2.URLError since Python 2.7
            raise WrappedSocketError(
                'errno=%s' % (e.errno,),
                self.url,
                self.params,
                self.headers,
                sys.exc_info(),
            )
        except six.moves.http_client.HTTPException as e:  # includes httplib.BadStatusLine
            raise WrappedHttplibError(str(e), self.url, self.params, self.headers, sys.exc_info())

    def __exit__(self, exception_type, exception, traceback):
        if self.file is not None:
            self.file.close()


def read_str_from_http_response(response):
    """
    This is an ugly hack.  I probably don't understand Python 2/3
    compatibility well enough.

    The read() method on urllib responses returns a str in Python 2,
    and bytes in Python 3, so json.load() won't work in both. This
    function converts the result into a str that json.loads() will
    take.
    """
    if six.PY2:
        return response.read()
    else:
        return response.read().decode('utf-8')


def post_json(url, params, auth_token=None):
    """Coverts params to JSON and posts them to the given URL.

    Returns the resulting JSON, decoded into a dict or an
    exception, custom if possible, WrappedHttpError otherwise
    """
    data = six.b(json.dumps(params))
    headers = {}
    if auth_token is not None:
        headers['Authorization'] = auth_token
    try:
        with OpenUrl(url, data, headers, params) as f:
            json_text = read_str_from_http_response(f)
            return json.loads(json_text)
    except WrappedHttpError as e:
        # this wrapper for api errors
        # requires access to 'params'
        e_backup = sys.exc_info()
        try:
            error_dict = json.loads(e.data.decode('utf-8'))
        except ValueError:
            v = sys.exc_info()
            error_dict = {'error_decoding_json': v}
            raise FatalError('error decoding JSON when handling an exception', [e_backup, v],)
        status = error_dict.get('status')
        code = error_dict.get('code')
        if status == 400 and code == "already_hidden":
            raise FileAlreadyHidden(params['fileName'])
        elif status == 400 and code == 'bad_json':
            raise BadJson(error_dict.get('message'))
        elif status == 400 and code in ("no_such_file", "file_not_present"):
            # hide_file returns "no_such_file"
            # delete_file_version returns "file_not_present"
            raise FileNotPresent(params['fileName'])
        elif status == 400 and code == "duplicate_bucket_name":
            raise DuplicateBucketName(params['bucketName'])
        elif status == 403 and code == "storage_cap_exceeded":
            raise StorageCapExceeded()
        raise


class SimpleProgress(object):
    def __init__(self, **kwargs):
        self.desc = kwargs['desc']
        self.total = kwargs.get('total', 1)
        self.complete = 0
        self.last_time = time.time()
        self.any_printed = False

    def update(self, byte_count):
        self.complete += byte_count
        now = time.time()
        elapsed = now - self.last_time
        if 3 <= elapsed and self.total != 0:
            if not self.any_printed:
                print(self.desc)
            print('     %d%%' % int(100.0 * self.complete / self.total))
            self.last_time = now
            self.any_printed = True

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_value, traceback):
        if self.any_printed:
            print('    DONE.')


class StreamWithProgress(tqdm or SimpleProgress):
    def __init__(self, stream, *args, **kwargs):
        self.stream = stream
        kwargs.update({'unit': 'B', 'unit_scale': True, 'leave': True, 'miniters': 1,})
        super(StreamWithProgress, self).__init__(*args, **kwargs)

    def __enter__(self):
        super(StreamWithProgress, self).__enter__()
        self.stream.__enter__()
        return self

    def __exit__(self, exc_type, exc_value, traceback_):
        return any(
            (
                super(StreamWithProgress, self).__exit__(exc_type, exc_value, traceback_),
                self.stream.__exit__(exc_type, exc_value, traceback_),
            )
        )

    def update(self, n):
        if n > 0:
            # tqdm started raising an exception if n==0 in 3.8.0
            super(StreamWithProgress, self).update(n)

    def read(self, size):
        data = self.stream.read(size)
        self.update(len(data))
        return data

    def write(self, data):
        self.stream.write(data)
        self.update(len(data))


def url_for_api(info, api_name):
    if api_name in ['b2_download_file_by_id']:
        base = info.get_download_url()
    else:
        base = info.get_api_url()
    return base + '/b2api/v1/' + api_name


def b2_url_encode(s):
    """URL-encodes a unicode string to be sent to B2 in an HTTP header.
    """
    return urllib.parse.quote(s.encode('utf-8'))


def b2_url_decode(s):
    """Decodes a Unicode string returned from B2 in an HTTP header.

    Returns a Python unicode string.
    """
    # Use str() to make sure that the input to unquote is a str, not
    # unicode, which ensures that the result is a str, which allows
    # the decoding to work properly.
    return urllib.parse.unquote_plus(str(s)).decode('utf-8')


def hex_sha1_of_file(path):
    with open(path, 'rb') as f:
        block_size = 1024 * 1024
        digest = hashlib.sha1()
        while True:
            data = f.read(block_size)
            if len(data) == 0:
                break
            digest.update(data)
        return digest.hexdigest()


def _download_file_progress_callback(output_stream, print_info, headers_dict):
    file_size = int(headers_dict['content-length'])
    if print_info:
        print('File name:   ', headers_dict['x-bz-file-name'])
        print('File size:   ', file_size)
        print('Content type:', headers_dict['content-type'])
        print('Content sha1:', headers_dict['x-bz-content-sha1'])
        for name in headers_dict:
            if name.startswith('x-bz-info-'):
                print('INFO', name[10:] + ':', headers_dict[name])
    output_stream.total = file_size


def download_file_by_id_helper(
    api,
    url,
    local_file_name,
    authorization=True,
    print_progress=False,
    print_info=False,
    set_last_modified=False,
):
    output_stream = open(local_file_name, 'wb')
    headers_received_cb = None
    if print_progress:
        output_stream = StreamWithProgress(output_stream, desc=local_file_name)
        headers_received_cb = functools.partial(
            _download_file_progress_callback,
            output_stream,
            print_info,
        )
    info = api.download_file_from_url(
        url,
        output_stream,
        authorization=authorization,
        headers_received_cb=headers_received_cb,
    )
    if set_last_modified:
        last_modified_millis = info.get('x-bz-info-src_last_modified_millis')
        if last_modified_millis is not None:
            mtime = int(last_modified_millis) / 1000
            os.utime(local_file_name, (mtime, mtime))
    if print_progress:
        print('checksum matches')


@six.add_metaclass(ABCMeta)
class Action(object):
    """
    An action to take, such as uploading, downloading, or deleting
    a file.  Multi-threaded tasks create a sequence of Actions, which
    are then run by a pool of threads.

    An action can depend on other actions completing.  An example of
    this is making sure a CreateBucketAction happens before an
    UploadFileAction.
    """

    def __init__(self, prerequisites):
        """
        :param prerequisites: A list of tasks that must be completed
         before this one can be run.
        """
        self.prerequisites = prerequisites
        self.done = False

    def run(self):
        for prereq in self.prerequisites:
            prereq.wait_until_done()
        self.do_action()
        self.done = True

    def wait_until_done(self):
        # TODO: better implementation
        while not self.done:
            time.sleep(1)

    @abstractmethod
    def do_action(self):
        """
        Performs the action, returning only after the action is completed.

        Will not be called until all prerequisites are satisfied.
        """


class B2UploadAction(Action):
    def __init__(self, full_path, file_name, mod_time):
        self.full_path = full_path
        self.file_name = file_name
        self.mod_time = mod_time

    def do_action(self):
        raise NotImplementedError()

    def __str__(self):
        return 'b2_upload(%s, %s, %s)' % (self.full_path, self.file_name, self.mod_time)


class B2DownloadAction(Action):
    def __init__(self, file_name, file_id):
        self.file_name = file_name
        self.file_id = file_id

    def do_action(self):
        raise NotImplementedError()

    def __str__(self):
        return 'b2_download(%s, %s)' % (self.file_name, self.file_id)


class B2DeleteAction(Action):
    def __init__(self, file_name, file_id):
        self.file_name = file_name
        self.file_id = file_id

    def do_action(self):
        raise NotImplementedError()

    def __str__(self):
        return 'b2_delete(%s, %s)' % (self.file_name, self.file_id)


class LocalDeleteAction(Action):
    def __init__(self, full_path):
        self.full_path = full_path

    def do_action(self):
        raise NotImplementedError()

    def __str__(self):
        return 'local_delete(%s)' % (self.full_path)


class FileVersion(object):
    """
    Holds information about one version of a file:

       id - The B2 file id, or the local full path name
       mod_time - modification time, in seconds
       action - "hide" or "upload" (never "start")
    """

    def __init__(self, id_, mod_time, action):
        self.id_ = id_
        self.mod_time = mod_time
        self.action = action

    def __repr__(self):
        return 'FileVersion(%s, %s, %s)' % (repr(self.id_), repr(self.mod_time), repr(self.action))


class File(object):
    """
    Holds information about one file in a folder.

    The name is relative to the folder in all cases.

    Files that have multiple versions (which only happens
    in B2, not in local folders) include information about
    all of the versions, most recent first.
    """

    def __init__(self, name, versions):
        self.name = name
        self.versions = versions

    def latest_version(self):
        return self.versions[0]

    def __repr__(self):
        return 'File(%s, [%s])' % (self.name, ', '.join(map(repr, self.versions)))


@six.add_metaclass(ABCMeta)
class Folder(object):
    """
    Interface to a folder full of files, which might be a B2 bucket,
    a virtual folder in a B2 bucket, or a directory on a local file
    system.

    Files in B2 may have multiple versions, while files in local
    folders have just one.
    """

    @abstractmethod
    def all_files(self):
        """
        Returns an iterator over all of the files in the folder, in
        the order that B2 uses.

        No matter what the folder separator on the local file system
        is, "/" is used in the returned file names.
        """

    @abstractmethod
    def folder_type(self):
        """
        Returns one of:  'b2', 'local'
        """

    def make_full_path(self, file_name):
        """
        Only for local folders, returns the full path to the file.
        """
        raise NotImplementedError()


class LocalFolder(Folder):
    """
    Folder interface to a directory on the local machine.
    """

    def __init__(self, root):
        self.root = os.path.abspath(root)
        self.relative_paths = self._get_all_relative_paths(self.root)

    def folder_type(self):
        return 'local'

    def all_files(self):
        for relative_path in self.relative_paths:
            yield self._make_file(relative_path)

    def make_full_path(self, file_name):
        return os.path.join(self.root, file_name.replace('/', os.path.sep))

    def _get_all_relative_paths(self, root_path):
        """
        Returns a sorted list of all of the files under the given root,
        relative to that root
        """
        result = []
        for dirpath, dirnames, filenames in os.walk(root_path):
            for filename in filenames:
                full_path = os.path.join(dirpath, filename)
                relative_path = full_path[len(root_path) + 1:]
                result.append(relative_path)
        return sorted(result)

    def _make_file(self, relative_path):
        full_path = os.path.join(self.root, relative_path)
        mod_time = os.path.getmtime(full_path)
        slashes_path = '/'.join(relative_path.split(os.path.sep))
        version = FileVersion(full_path, mod_time, "upload")
        return File(slashes_path, [version])


def next_or_none(iterator):
    """
    Returns the next item from the iterator, or None if there are no more.
    """
    try:
        return six.advance_iterator(iterator)
    except StopIteration:
        return None


def zip_folders(folder_a, folder_b):
    """
    An iterator over all of the files in the union of two folders,
    matching file names.

    Each item is a pair (file_a, file_b) with the corresponding file
    in both folders.  Either file (but not both) will be None if the
    file is in only one folder.
    :param folder_a: A Folder object.
    :param folder_b: A Folder object.
    """
    iter_a = folder_a.all_files()
    iter_b = folder_b.all_files()
    current_a = next_or_none(iter_a)
    current_b = next_or_none(iter_b)
    while current_a is not None or current_b is not None:
        if current_a is None:
            yield (None, current_b)
            current_b = next_or_none(iter_b)
        elif current_b is None:
            yield (current_a, None)
            current_a = next_or_none(iter_a)
        elif current_a.name < current_b.name:
            yield (current_a, None)
            current_a = next_or_none(iter_a)
        elif current_b.name < current_a.name:
            yield (None, current_b)
            current_b = next_or_none(iter_b)
        else:
            assert current_a.name == current_b.name
            yield (current_a, current_b)
            current_a = next_or_none(iter_a)
            current_b = next_or_none(iter_b)


def make_file_sync_actions(
    sync_type, source_file, dest_file, source_folder, dest_folder, history_days
):
    """
    Yields the sequence of actions needed to sync the two files
    """
    source_mod_time = 0
    if source_file is not None:
        source_mod_time = source_file.latest_version().mod_time
    dest_mod_time = 0
    if dest_file is not None:
        dest_mod_time = dest_file.latest_version().mod_time
    if dest_mod_time < source_mod_time:
        if sync_type == 'local-to-b2':
            yield B2UploadAction(
                dest_folder.make_full_path(source_file.name), source_file.name, source_mod_time
            )
        else:
            yield B2DownloadAction(source_file.name, source_file.latest_version().id_)
    if source_mod_time == 0 and dest_mod_time != 0:
        if sync_type == 'local-to-b2':
            yield B2DeleteAction(dest_file.name, dest_file.latest_version().id_)
        else:
            yield LocalDeleteAction(dest_file.latest_version().id_)
    # TODO: clean up file history in B2
    # TODO: do not delete local files for history_days days


def make_folder_sync_actions(source_folder, dest_folder, history_days):
    """
    Yields a sequence of actions that will sync the destination
    folder to the source folder.
    """
    source_type = source_folder.folder_type()
    dest_type = dest_folder.folder_type()
    sync_type = '%s-to-%s' % (source_type, dest_type)
    if (source_folder.folder_type(), dest_folder.folder_type()) not in [
        ('b2', 'local'), ('local', 'b2')
    ]:
        raise NotImplementedError("Sync support only local-to-b2 and b2-to-local")
    for (source_file, dest_file) in zip_folders(source_folder, dest_folder):
        for action in make_file_sync_actions(
            sync_type, source_file, dest_file, source_folder, dest_folder, history_days
        ):
            yield action


def sync_folders(source, dest, history_days):
    """
    Syncs two folders.  Always ensures that every file in the
    source is also in the destination.  Deletes any file versions
    in the destination older than history_days.
    """


class ConsoleTool(object):
    """
    Implements the commands available in the B2 command-line tool
    using the B2Api library.

    Uses the StoredAccountInfo object to keep account data in
    ~/.b2_account_info between runs.
    """

    # TODO: move all creation of StoredAccountInfo into this class.

    def __init__(self):
        info = StoredAccountInfo()
        self.api = B2Api(info, AuthInfoCache(info))

    # bucket

    def create_bucket(self, args):
        if len(args) != 2:
            usage_and_exit()
        bucket_name = args[0]
        bucket_type = args[1]

        print(self.api.create_bucket(bucket_name, bucket_type).id_)

    def delete_bucket(self, args):
        if len(args) != 1:
            usage_and_exit()
        bucket_name = args[0]

        bucket = self.api.get_bucket_by_name(bucket_name)
        response = self.api.delete_bucket(bucket)

        print(json.dumps(response, indent=4, sort_keys=True))

    def update_bucket(self, args):
        if len(args) != 2:
            usage_and_exit()
        bucket_name = args[0]
        bucket_type = args[1]

        bucket = self.api.get_bucket_by_name(bucket_name)
        response = bucket.set_type(bucket_type)

        print(json.dumps(response, indent=4, sort_keys=True))

    def list_buckets(self, args):
        if len(args) != 0:
            usage_and_exit()

        for b in self.api.list_buckets():
            print('%s  %-10s  %s' % (b.id_, b.type_, b.name))

    # file

    def delete_file_version(self, args):
        if len(args) != 2:
            usage_and_exit()
        file_name = args[0]
        file_id = args[1]

        file_info = self.api.delete_file_version(file_id, file_name)

        response = file_info.as_dict()

        print(json.dumps(response, indent=2, sort_keys=True))

    def download_file_by_id(self, args):
        if len(args) != 2:
            usage_and_exit()
        file_id = args[0]
        local_file_name = args[1]

        url = self.api.get_download_url_for_fileid(file_id)

        download_file_by_id_helper(
            self.api,
            url,
            local_file_name,
            authorization=True,
            print_progress=True,
            print_info=True,
        )

    def download_file_by_name(self, args):
        if len(args) != 3:
            usage_and_exit()
        bucket_name = args[0]
        file_name = args[1]
        local_file_name = args[2]

        bucket = self.api.get_bucket_by_name(bucket_name)
        url = bucket.get_download_url(file_name)

        download_file_by_id_helper(
            self.api,
            url,
            local_file_name,
            authorization=True,
            print_progress=True,
            print_info=True,
        )

    def get_file_info(self, args):
        if len(args) != 1:
            usage_and_exit()
        file_id = args[0]

        response = self.api.get_file_info(file_id)

        print(json.dumps(response, indent=2, sort_keys=True))

    def hide_file(self, args):
        if len(args) != 2:
            usage_and_exit()
        bucket_name = args[0]
        file_name = args[1]

        bucket = self.api.get_bucket_by_name(bucket_name)
        file_info = bucket.hide_file(file_name)

        response = file_info.as_dict()

        print(json.dumps(response, indent=2, sort_keys=True))

    def upload_file(self, args):
        content_type = None
        file_infos = {}
        sha1_sum = None
        quiet = False

        while 0 < len(args) and args[0][0] == '-':
            option = args[0]
            if option == '--sha1':
                if len(args) < 2:
                    usage_and_exit()
                sha1_sum = args[1]
                args = args[2:]
            elif option == '--contentType':
                if len(args) < 2:
                    usage_and_exit()
                content_type = args[1]
                args = args[2:]
            elif option == '--info':
                if len(args) < 2:
                    usage_and_exit()
                parts = args[1].split('=', 1)
                if len(parts) == 1:
                    raise BadFileInfo(args[1])
                file_infos[parts[0]] = parts[1]
                args = args[2:]
            elif option == '--quiet':
                quiet = True
                args = args[1:]
            else:
                usage_and_exit()

        if len(args) != 3:
            usage_and_exit()
        bucket_name = args[0]
        local_file = args[1]
        remote_file = args[2]

        bucket = self.api.get_bucket_by_name(bucket_name)
        file_info = bucket.upload_file(
            local_file=local_file,
            remote_filename=remote_file,
            content_type=content_type,
            file_infos=file_infos,
            sha1_sum=sha1_sum,
            quiet=quiet,
        )
        response = file_info.as_dict()
        if not quiet:
            print("URL by file name: " + bucket.get_download_url(remote_file))
            print("URL by fileId: " + self.api.get_download_url_for_fileid(response['fileId']))
        print(json.dumps(response, indent=2, sort_keys=True))

    # account

    def authorize_account(self, args):
        option = 'production'
        while 0 < len(args) and args[0][0] == '-':
            option = args[0][2:]
            args = args[1:]
            if option in self.api.account_info.REALM_URLS:
                break
            else:
                print('ERROR: unknown option', option)
                usage_and_exit()

        url = self.api.account_info.REALM_URLS[option]
        print('Using %s' % url)

        if 2 < len(args):
            usage_and_exit()
        if 0 < len(args):
            account_id = args[0]
        else:
            account_id = six.moves.input('Backblaze account ID: ')

        if 1 < len(args):
            application_key = args[1]
        else:
            application_key = getpass.getpass('Backblaze application key: ')

        self.api.authorize_account(url, account_id, application_key)

    def clear_account(self, args):
        if len(args) != 0:
            usage_and_exit()
        self.api.account_info.clear()

    # listing

    def list_file_names(self, args):
        if len(args) < 1 or 3 < len(args):
            usage_and_exit()

        bucket_name = args[0]
        if 2 <= len(args):
            first_file_name = args[1]
        else:
            first_file_name = None
        if 3 <= len(args):
            count = int(args[2])
        else:
            count = 100

        bucket = self.api.get_bucket_by_name(bucket_name)

        response = bucket.list_file_names(first_file_name, count)
        print(json.dumps(response, indent=2, sort_keys=True))

    def list_file_versions(self, args):
        if len(args) < 1 or 4 < len(args):
            usage_and_exit()

        bucket_name = args[0]
        if 2 <= len(args):
            first_file_name = args[1]
        else:
            first_file_name = None
        if 3 <= len(args):
            first_file_id = args[2]
        else:
            first_file_id = None
        if 4 <= len(args):
            count = int(args[3])
        else:
            count = 100

        bucket = self.api.get_bucket_by_name(bucket_name)

        response = bucket.list_file_versions(first_file_name, first_file_id, count)
        print(json.dumps(response, indent=2, sort_keys=True))

    def ls(self, args):
        long_format = False
        show_versions = False
        while len(args) != 0 and args[0][0] == '-':
            option = args[0]
            args = args[1:]
            if option == '--long':
                long_format = True
            elif option == '--versions':
                show_versions = True
            else:
                print('Unknown option:', option)
                usage_and_exit()
        if len(args) < 1 or len(args) > 2:
            usage_and_exit()
        bucket_name = args[0]
        if len(args) == 1:
            prefix = ""
        else:
            prefix = args[1]
            if not prefix.endswith('/'):
                prefix += '/'

        bucket = self.api.get_bucket_by_name(bucket_name)
        for file_version_info, folder_name in bucket.ls(prefix, show_versions):
            if not long_format:
                print(folder_name or file_version_info.file_name)
            elif folder_name is not None:
                print(FileVersionInfo.format_folder_ls_entry(folder_name))
            else:
                print(file_version_info.format_ls_entry())

    # other

    def make_url(self, args):
        if len(args) != 1:
            usage_and_exit()

        file_id = args[0]

        print(self.api.get_download_url_for_fileid(file_id))

    def sync(self, args):
        # TODO: break up this method.  it's too long
        # maybe move into its own class?
        options = {'delete': False, 'hide': False}
        while args and args[0][0] == '-':
            option = args[0]
            args = args[1:]
            if option == '--delete':
                options['delete'] = True
            elif option == '--hide':
                options['hide'] = True
            else:
                message_and_exit('ERROR: unknown option: ' + option)
        if len(args) != 2:
            usage_and_exit()
        src = args[0]
        dst = args[1]
        local_path = src if dst.startswith('b2:') else dst
        b2_path = dst if dst.startswith('b2:') else src
        is_b2_src = b2_path == src
        if local_path.startswith('b2:') or not b2_path.startswith('b2:'):
            message_and_exit('ERROR: one of the paths must be a "b2:<bucket>" URI')
        elif not os.path.exists(local_path):
            message_and_exit('ERROR: local path doesn\'t exist: ' + local_path)
        bucket_name = b2_path[3:].split('/')[0]
        bucket_prefix = '/'.join(b2_path[3:].split('/')[1:])
        if bucket_prefix and not bucket_prefix.endswith('/'):
            bucket_prefix += '/'

        bucket = self.api.get_bucket_by_name(bucket_name)

        # Find all matching files in B2
        remote_files = {}
        for file_info, __ in bucket.ls(
            folder_to_list=bucket_prefix,
            max_entries=1000,
            recursive=True
        ):
            name = file_info.file_name
            after_prefix = name[len(bucket_prefix):]
            remote_files[after_prefix] = {
                'fileName': after_prefix,
                'fileId': file_info.id_,
                'size': file_info.size,
            }

        # Find all matching local files
        local_files = {}
        for dirpath, dirnames, filenames in os.walk(local_path):
            for filename in filenames:
                abspath = os.path.join(dirpath, filename)
                relpath = os.path.relpath(abspath, local_path)
                local_files[relpath] = {'fileName': relpath, 'size': os.path.getsize(abspath)}

        # Process differences
        local_fileset = set(local_files.keys())
        remote_fileset = set(remote_files.keys())
        for filename in local_fileset | remote_fileset:
            filepath = os.path.join(local_path, filename)
            dirpath = os.path.dirname(filepath)
            b2_path = os.path.join(bucket_prefix, filename)
            local_file = local_files.get(filename)
            remote_file = remote_files.get(filename)
            is_match = local_file and remote_file and local_file['size'] == remote_file['size']
            if is_b2_src and remote_file and not is_match:
                print("+ %s" % filename)
                if not os.path.exists(dirpath):
                    os.makedirs(dirpath)
                url = self.api.get_download_url_for_fileid(remote_file['fileId'])
                download_file_by_id_helper(self.api, url, filepath, authorization=True,)
            elif is_b2_src and not remote_file and options['delete']:
                print("- %s" % filename)
                os.remove(filepath)
            elif not is_b2_src and local_file and not is_match:
                print("+ %s" % filename)
                file_infos = {
                    'src_last_modified_millis': str(int(os.path.getmtime(filepath) * 1000))
                }
                bucket.upload_file(filepath, b2_path, file_infos=file_infos)
            elif not is_b2_src and not local_file and options['delete']:
                print("- %s" % filename)
                self.api.delete_file_version(remote_file['fileId'], b2_path)
            elif not is_b2_src and not local_file and options['hide']:
                print(". %s" % filename)
                bucket.hide_file(b2_path)

        # Remove empty local directories
        if is_b2_src and options['delete']:
            for dirpath, dirnames, filenames in os.walk(local_path, topdown=False):
                for name in dirnames:
                    try:
                        os.rmdir(os.path.join(dirpath, name))
                    except:
                        pass


def main():
    if len(sys.argv) < 2:
        usage_and_exit()

    decoded_argv = decode_sys_argv()

    action = decoded_argv[1]
    args = decoded_argv[2:]

    ct = ConsoleTool()
    try:
        if action == 'authorize_account':
            ct.authorize_account(args)
        elif action == 'clear_account':
            ct.clear_account(args)
        elif action == 'create_bucket':
            ct.create_bucket(args)
        elif action == 'delete_bucket':
            ct.delete_bucket(args)
        elif action == 'delete_file_version':
            ct.delete_file_version(args)
        elif action == 'download_file_by_id':
            ct.download_file_by_id(args)
        elif action == 'download_file_by_name':
            ct.download_file_by_name(args)
        elif action == 'get_file_info':
            ct.get_file_info(args)
        elif action == 'hide_file':
            ct.hide_file(args)
        elif action == 'list_buckets':
            ct.list_buckets(args)
        elif action == 'list_file_names':
            ct.list_file_names(args)
        elif action == 'list_file_versions':
            ct.list_file_versions(args)
        elif action == 'ls':
            ct.ls(args)
        elif action == 'make_url':
            ct.make_url(args)
        elif action == 'sync':
            ct.sync(args)
        elif action == 'update_bucket':
            ct.update_bucket(args)
        elif action == 'upload_file':
            ct.upload_file(args)
        elif action == 'version':
            print('b2 command line tool, version', VERSION)
        else:
            usage_and_exit()
    except MissingAccountData:
        print('ERROR: Missing account.  Use: b2 authorize_account')
        sys.exit(1)
    except B2Error as e:
        print('ERROR: %s' % (e,))
        sys.exit(1)


if __name__ == '__main__':
    main()
