######################################################################
#
# File: raw_simulator.py
#
# Copyright 2015 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import re

import six
from six.moves import range

from .exception import (
    BadJson, BadUploadUrl, ChecksumMismatch, DuplicateBucketName, FileNotPresent, InvalidAuthToken,
    MissingPart, NonExistentBucket
)
from .raw_api import AbstractRawApi


class PartSimulator(object):
    def __init__(self, file_id, part_number, content_length, content_sha1, part_data):
        self.file_id = file_id
        self.part_number = part_number
        self.content_length = content_length
        self.content_sha1 = content_sha1
        self.part_data = part_data

    def as_list_parts_dict(self):
        return dict(
            fileId=self.file_id,
            partNumber=self.part_number,
            contentLength=self.content_length,
            contentSha1=self.content_sha1
        )  # yapf: disable


class FileSimulator(object):
    """
    One of: an unfinished large file, a finished file, or a deletion marker
    """

    def __init__(
        self, account_id, bucket_id, file_id, action, name, content_type, content_sha1, file_info,
        data_bytes, upload_timestamp
    ):
        self.account_id = account_id
        self.bucket_id = bucket_id
        self.file_id = file_id
        self.action = action
        self.name = name
        if data_bytes is not None:
            self.content_length = len(data_bytes)
        self.content_type = content_type
        self.content_sha1 = content_sha1
        self.file_info = file_info
        self.data_bytes = data_bytes
        self.upload_timestamp = upload_timestamp

        if action == 'start':
            self.parts = []

    def sort_key(self):
        """
        Returns a key that can be used to sort the files in a
        bucket in the order that b2_list_file_versions returns them.
        """
        return (self.name, self.file_id)

    def as_upload_result(self):
        return dict(
            fileId=self.file_id,
            fileName=self.name,
            accountId=self.account_id,
            bucketId=self.bucket_id,
            contentLength=len(self.data_bytes) if self.data_bytes is not None else 0,
            contentType=self.content_type,
            contentSha1=self.content_sha1,
            fileInfo=self.file_info,
            action=self.action,
            uploadTimestamp=self.upload_timestamp
        )  # yapf: disable

    def as_list_files_dict(self):
        return dict(
            fileId=self.file_id,
            fileName=self.name,
            size=len(self.data_bytes) if self.data_bytes is not None else 0,
            contentType=self.content_type,
            contentSha1=self.content_sha1,
            fileInfo=self.file_info,
            action=self.action,
            uploadTimestamp=self.upload_timestamp
        )  # yapf: disable

    def as_start_large_file_result(self):
        return dict(
            fileId=self.file_id,
            fileName=self.name,
            accountId=self.account_id,
            bucketId=self.bucket_id,
            contentType=self.content_type,
            fileInfo=self.file_info,
            uploadTimestamp=self.upload_timestamp
        )  # yapf: disable

    def add_part(self, part_number, part):
        while len(self.parts) < part_number + 1:
            self.parts.append(None)
        self.parts[part_number] = part

    def finish(self, part_sha1_array):
        last_part_number = max(part.part_number for part in self.parts if part is not None)
        for part_number in six.moves.range(1, last_part_number + 1):
            if self.parts[part_number] is None:
                raise MissingPart(part_number)
        my_part_sha1_array = [
            self.parts[part_number].content_sha1
            for part_number in six.moves.range(1, last_part_number + 1)
        ]
        if part_sha1_array != my_part_sha1_array:
            raise ChecksumMismatch(
                'sha1', expected=str(part_sha1_array), actual=str(my_part_sha1_array)
            )
        self.data_bytes = six.b('').join(
            self.parts[part_number].part_data
            for part_number in six.moves.range(1, last_part_number + 1)
        )
        self.content_length = len(self.data_bytes)
        self.action = 'upload'

    def is_visible(self):
        """
        Does this file show up in b2_list_file_names?
        """
        return self.action == 'upload'

    def list_parts(self, start_part_number, max_part_count):
        start_part_number = start_part_number or 1
        max_part_count = max_part_count or 100
        parts = [
            part.as_list_parts_dict()
            for part in self.parts if part is not None and start_part_number <= part.part_number
        ]
        if len(parts) <= max_part_count:
            next_part_number = None
        else:
            next_part_number = parts[max_part_count]['partNumber']
            parts = parts[:max_part_count]
        return dict(parts=parts, nextPartNumber=next_part_number)

    def mod_time_millis(self):
        return 0


class BucketSimulator(object):

    # File IDs start at 9999 and count down, so they sort in the order
    # returned by list_file_versions.  The IDs are strings.
    FIRST_FILE_NUMBER = 9999

    FIRST_FILE_ID = str(FIRST_FILE_NUMBER)

    def __init__(self, account_id, bucket_id, bucket_name, bucket_type):
        assert bucket_type in ['allPrivate', 'allPublic']
        self.account_id = account_id
        self.bucket_name = bucket_name
        self.bucket_id = bucket_id
        self.bucket_type = bucket_type
        self.upload_url_counter = iter(range(200))
        # File IDs count down, so that the most recent will come first when they are sorted.
        self.file_id_counter = iter(range(self.FIRST_FILE_NUMBER, 0, -1))
        self.upload_timestamp_counter = iter(range(5000, 9999))
        self.file_id_to_file = dict()
        # It would be nice to use an OrderedDict for this, but 2.6 doesn't have it.
        self.file_name_and_id_to_file = dict()

    def bucket_dict(self):
        return dict(
            accountId=self.account_id,
            bucketName=self.bucket_name,
            bucketId=self.bucket_id,
            bucketType=self.bucket_type
        )

    def cancel_large_file(self, file_id):
        file_sim = self.file_id_to_file[file_id]
        key = (file_sim.name, file_id)
        del self.file_name_and_id_to_file[key]
        del self.file_id_to_file[file_id]
        return dict(
            accountId=self.account_id,
            bucketId=self.bucket_id,
            fileId=file_id,
            fileName=file_sim.name
        )  # yapf: disable

    def delete_file_version(self, file_id, file_name):
        key = (file_name, file_id)
        file_sim = self.file_name_and_id_to_file[key]
        del self.file_name_and_id_to_file[key]
        del self.file_id_to_file[file_id]
        return dict(fileId=file_id, fileName=file_name, uploadTimestamp=file_sim.upload_timestamp)

    def download_file_by_id(self, file_id, download_dest):
        file_sim = self.file_id_to_file[file_id]
        self._download_file_sim(download_dest, file_sim)

    def download_file_by_name(self, file_name, download_dest):
        files = self.list_file_names(file_name, 1)['files']
        if len(files) == 0:
            raise FileNotPresent(file_name)
        file_dict = files[0]
        if file_dict['fileName'] != file_name or file_dict['action'] != 'upload':
            raise FileNotPresent(file_name)
        file_sim = self.file_name_and_id_to_file[(file_name, file_dict['fileId'])]
        self._download_file_sim(download_dest, file_sim)

    def _download_file_sim(self, download_dest, file_sim):
        with download_dest.open(
            file_sim.file_id, file_sim.name, file_sim.content_length, file_sim.content_type,
            file_sim.content_sha1, file_sim.file_info, file_sim.mod_time_millis()
        ) as f:
            f.write(file_sim.data_bytes)

    def finish_large_file(self, file_id, part_sha1_array):
        file_sim = self.file_id_to_file[file_id]
        file_sim.finish(part_sha1_array)
        return file_sim.as_upload_result()

    def get_file_info(self, file_id):
        return self.file_id_to_file[file_id].as_upload_result()

    def get_upload_url(self):
        upload_id = six.next(self.upload_url_counter)
        upload_url = 'https://upload.example.com/%s/%s' % (self.bucket_id, upload_id)
        return dict(bucketId=self.bucket_id, uploadUrl=upload_url, authorizationToken=upload_url)

    def get_upload_part_url(self, file_id):
        upload_url = 'https://upload.example.com/part/%s' % (file_id,)
        return dict(bucketId=self.bucket_id, uploadUrl=upload_url, authorizationToken=upload_url)

    def hide_file(self, file_name):
        file_id = self._next_file_id()
        file_sim = FileSimulator(
            self.account_id, self.bucket_id, file_id, 'hide', file_name, None, "none", {},
            six.b(''), six.next(self.upload_timestamp_counter)
        )
        self.file_id_to_file[file_id] = file_sim
        self.file_name_and_id_to_file[file_sim.sort_key()] = file_sim
        return file_sim.as_list_files_dict()

    def list_file_names(self, start_file_name=None, max_file_count=None):
        start_file_name = start_file_name or ''
        max_file_count = max_file_count or 100
        result_files = []
        next_file_name = None
        prev_file_name = None
        for key in sorted(six.iterkeys(self.file_name_and_id_to_file)):
            (file_name, file_id) = key
            if start_file_name <= file_name and file_name != prev_file_name:
                prev_file_name = file_name
                file_sim = self.file_name_and_id_to_file[key]
                if file_sim.is_visible():
                    result_files.append(file_sim.as_list_files_dict())
                    if len(result_files) == max_file_count:
                        next_file_name = file_sim.name + ' '
                        break
        return dict(files=result_files, nextFileName=next_file_name)

    def list_file_versions(self, start_file_name=None, start_file_id=None, max_file_count=None):
        start_file_name = start_file_name or ''
        start_file_id = start_file_id or ''
        max_file_count = max_file_count or 100
        result_files = []
        next_file_name = None
        next_file_id = None
        for key in sorted(six.iterkeys(self.file_name_and_id_to_file)):
            (file_name, file_id) = key
            if (start_file_name < file_name) or (
                start_file_name == file_name and start_file_id <= file_id
            ):
                file_sim = self.file_name_and_id_to_file[key]
                result_files.append(file_sim.as_list_files_dict())
                if len(result_files) == max_file_count:
                    next_file_name = file_sim.name
                    next_file_id = str(int(file_id) + 1)
                    break
        return dict(files=result_files, nextFileName=next_file_name, nextFileId=next_file_id)

    def list_parts(self, file_id, start_part_number, max_part_count):
        file_sim = self.file_id_to_file[file_id]
        return file_sim.list_parts(start_part_number, max_part_count)

    def list_unfinished_large_files(self, start_file_id=None, max_file_count=None):
        start_file_id = start_file_id or self.FIRST_FILE_ID
        max_file_count = max_file_count or 100
        all_unfinished_ids = set(
            k for (k, v) in six.iteritems(self.file_id_to_file)
            if v.action == 'start' and k <= start_file_id
        )
        ids_in_order = sorted(all_unfinished_ids, reverse=True)
        file_dict_list = [
            dict(
                fileId=file_sim.file_id,
                fileName=file_sim.name,
                accountId=file_sim.account_id,
                bucketId=file_sim.bucket_id,
                contentType=file_sim.content_type,
                fileInfo=file_sim.file_info
            )
            for file_sim in (
                self.file_id_to_file[file_id] for file_id in ids_in_order[:max_file_count]
            )
        ]  # yapf: disable
        next_file_id = None
        if len(file_dict_list) == max_file_count:
            next_file_id = str(int(file_dict_list[-1]['fileId']) - 1)
        return dict(files=file_dict_list, nextFileId=next_file_id)

    def start_large_file(self, file_name, content_type, file_info):
        file_id = self._next_file_id()
        file_sim = FileSimulator(
            self.account_id, self.bucket_id, file_id, 'start', file_name, content_type, 'none',
            file_info, None, six.next(self.upload_timestamp_counter)
        )  # yapf: disable
        self.file_id_to_file[file_id] = file_sim
        self.file_name_and_id_to_file[file_sim.sort_key()] = file_sim
        return file_sim.as_start_large_file_result()

    def update_bucket(self, bucket_type):
        self.bucket_type = bucket_type
        return self.bucket_dict()

    def upload_file(
        self, upload_id, upload_auth_token, file_name, content_length, content_type, content_sha1,
        file_infos, data_stream
    ):
        data_bytes = data_stream.read()
        assert len(data_bytes) == content_length
        file_id = self._next_file_id()
        file_sim = FileSimulator(
            self.account_id, self.bucket_id, file_id, 'upload', file_name, content_type,
            content_sha1, file_infos, data_bytes, six.next(self.upload_timestamp_counter)
        )
        self.file_id_to_file[file_id] = file_sim
        self.file_name_and_id_to_file[file_sim.sort_key()] = file_sim
        return file_sim.as_upload_result()

    def upload_part(self, file_id, part_number, content_length, sha1_sum, input_stream):
        file_sim = self.file_id_to_file[file_id]
        part_data = input_stream.read(content_length)
        assert len(part_data) == content_length
        part = PartSimulator(file_sim.file_id, part_number, content_length, sha1_sum, part_data)
        file_sim.add_part(part_number, part)
        return dict(
            fileId=file_id,
            partNumber=part_number,
            contentLength=content_length,
            contentSha1=sha1_sum
        )  # yapf: disable

    def _next_file_id(self):
        return str(six.next(self.file_id_counter))


class RawSimulator(AbstractRawApi):
    """
    Implements the same interface as B2RawApi by simulating all of the
    calls and keeping state in memory.

    The intended use for this class is for unit tests that test things
    built on top of B2RawApi.
    """

    API_URL = 'http://api.example.com'
    DOWNLOAD_URL = 'http://download.example.com'

    MIN_PART_SIZE = 200

    def __init__(self):
        self.authorized_accounts = set()
        self.bucket_name_to_bucket = dict()
        self.bucket_id_to_bucket = dict()
        self.bucket_id_counter = iter(range(100))
        self.file_id_to_bucket_id = {}
        self.upload_errors = []

    def set_upload_errors(self, errors):
        """
        Stores a sequence of exceptions to raise on upload.  Each one will
        be raised in turn, until they are all gone.  Then the next upload
        will succeed.
        """
        assert len(self.upload_errors) == 0
        self.upload_errors = errors

    def authorize_account(self, realm_url, account_id, application_key):
        assert realm_url == 'http://production.example.com'
        if application_key != 'good-app-key':
            raise InvalidAuthToken(
                'invalid application key: %s' % (application_key,), 'bad_auth_token'
            )
        self.authorized_accounts.add(account_id)
        return dict(
            accountId=account_id,
            authorizationToken='AUTH:' + account_id,
            apiUrl=self.API_URL,
            downloadUrl=self.DOWNLOAD_URL,
            minimumPartSize=self.MIN_PART_SIZE
        )  # yapf: disable

    def cancel_large_file(self, api_url, account_auth_token, file_id):
        bucket_id = self.file_id_to_bucket_id[file_id]
        bucket = self._get_bucket_by_id(bucket_id)
        self._assert_account_auth(api_url, account_auth_token, bucket.account_id)
        return bucket.cancel_large_file(file_id)

    def create_bucket(self, api_url, account_auth_token, account_id, bucket_name, bucket_type):
        if not re.match(r'^[-a-zA-Z]*$', bucket_name):
            raise BadJson('illegal bucket name: ' + bucket_name)
        self._assert_account_auth(api_url, account_auth_token, account_id)
        if bucket_name in self.bucket_name_to_bucket:
            raise DuplicateBucketName(bucket_name)
        bucket_id = 'bucket_' + str(six.next(self.bucket_id_counter))
        bucket = BucketSimulator(account_id, bucket_id, bucket_name, bucket_type)
        self.bucket_name_to_bucket[bucket_name] = bucket
        self.bucket_id_to_bucket[bucket_id] = bucket
        return bucket.bucket_dict()

    def delete_file_version(self, api_url, account_auth_token, file_id, file_name):
        bucket_id = self.file_id_to_bucket_id[file_id]
        bucket = self._get_bucket_by_id(bucket_id)
        self._assert_account_auth(api_url, account_auth_token, bucket.account_id)
        return bucket.delete_file_version(file_id, file_name)

    def delete_bucket(self, api_url, account_auth_token, account_id, bucket_id):
        self._assert_account_auth(api_url, account_auth_token, account_id)
        bucket = self._get_bucket_by_id(bucket_id)
        del self.bucket_name_to_bucket[bucket.bucket_name]
        del self.bucket_id_to_bucket[bucket_id]
        return bucket.bucket_dict()

    def download_file_by_id(self, download_url, account_auth_token_or_none, file_id, download_dest):
        # TODO: check auth token if bucket is not public
        bucket_id = self.file_id_to_bucket_id[file_id]
        bucket = self._get_bucket_by_id(bucket_id)
        bucket.download_file_by_id(file_id, download_dest)

    def download_file_by_name(
        self, download_url, account_auth_token_or_none, bucket_name, file_name, download_dest
    ):
        assert download_url == self.DOWNLOAD_URL
        # TODO: check auth token if bucket is not public
        bucket = self._get_bucket_by_name(bucket_name)
        bucket.download_file_by_name(file_name, download_dest)

    def finish_large_file(self, api_url, account_auth_token, file_id, part_sha1_array):
        bucket_id = self.file_id_to_bucket_id[file_id]
        bucket = self._get_bucket_by_id(bucket_id)
        self._assert_account_auth(api_url, account_auth_token, bucket.account_id)
        return bucket.finish_large_file(file_id, part_sha1_array)

    def get_file_info(self, api_url, account_auth_token, file_id):
        bucket_id = self.file_id_to_bucket_id[file_id]
        bucket = self._get_bucket_by_id(bucket_id)
        return bucket.get_file_info(file_id)

    def get_upload_url(self, api_url, account_auth_token, bucket_id):
        bucket = self._get_bucket_by_id(bucket_id)
        self._assert_account_auth(api_url, account_auth_token, bucket.account_id)
        return self._get_bucket_by_id(bucket_id).get_upload_url()

    def get_upload_part_url(self, api_url, account_auth_token, file_id):
        bucket_id = self.file_id_to_bucket_id[file_id]
        bucket = self._get_bucket_by_id(bucket_id)
        self._assert_account_auth(api_url, account_auth_token, bucket.account_id)
        return self._get_bucket_by_id(bucket_id).get_upload_part_url(file_id)

    def hide_file(self, api_url, account_auth_token, bucket_id, file_name):
        bucket = self._get_bucket_by_id(bucket_id)
        self._assert_account_auth(api_url, account_auth_token, bucket.account_id)
        response = bucket.hide_file(file_name)
        self.file_id_to_bucket_id[response['fileId']] = bucket_id
        return response

    def list_buckets(self, api_url, account_auth_token, account_id):
        self._assert_account_auth(api_url, account_auth_token, account_id)
        sorted_buckets = [
            self.bucket_name_to_bucket[bucket_name]
            for bucket_name in sorted(six.iterkeys(self.bucket_name_to_bucket))
        ]
        bucket_list = [bucket.bucket_dict() for bucket in sorted_buckets]
        return dict(buckets=bucket_list)

    def list_file_names(
        self, api_url, account_auth, bucket_id, start_file_name=None, max_file_count=None
    ):
        bucket = self._get_bucket_by_id(bucket_id)
        self._assert_account_auth(api_url, account_auth, bucket.account_id)
        return bucket.list_file_names(start_file_name, max_file_count)

    def list_file_versions(
        self,
        api_url,
        account_auth,
        bucket_id,
        start_file_name=None,
        start_file_id=None,
        max_file_count=None
    ):
        bucket = self._get_bucket_by_id(bucket_id)
        self._assert_account_auth(api_url, account_auth, bucket.account_id)
        return bucket.list_file_versions(start_file_name, start_file_id, max_file_count)

    def list_parts(self, api_url, account_auth_token, file_id, start_part_number, max_part_count):
        bucket_id = self.file_id_to_bucket_id[file_id]
        bucket = self._get_bucket_by_id(bucket_id)
        self._assert_account_auth(api_url, account_auth_token, bucket.account_id)
        return bucket.list_parts(file_id, start_part_number, max_part_count)

    def list_unfinished_large_files(
        self, api_url, account_auth, bucket_id, start_file_id=None, max_file_count=None
    ):
        bucket = self._get_bucket_by_id(bucket_id)
        self._assert_account_auth(api_url, account_auth, bucket.account_id)
        start_file_id = start_file_id or ''
        max_file_count = max_file_count or 100
        return bucket.list_unfinished_large_files(start_file_id, max_file_count)

    def start_large_file(
        self, api_url, account_auth_token, bucket_id, file_name, content_type, file_info
    ):
        bucket = self._get_bucket_by_id(bucket_id)
        self._assert_account_auth(api_url, account_auth_token, bucket.account_id)
        result = bucket.start_large_file(file_name, content_type, file_info)
        self.file_id_to_bucket_id[result['fileId']] = bucket_id
        return result

    def update_bucket(self, api_url, account_auth_token, account_id, bucket_id, bucket_type):
        bucket = self._get_bucket_by_id(bucket_id)
        self._assert_account_auth(api_url, account_auth_token, bucket.account_id)
        return bucket.update_bucket(bucket_type)

    def upload_file(
        self, upload_url, upload_auth_token, file_name, content_length, content_type, content_sha1,
        file_infos, data_stream
    ):
        assert upload_url == upload_auth_token
        url_match = re.match(r'https://upload.example.com/([^/]*)/([^/]*)', upload_url)
        if url_match is None:
            raise BadUploadUrl(upload_url)
        if len(self.upload_errors) != 0:
            raise self.upload_errors.pop(0)
        bucket_id, upload_id = url_match.groups()
        bucket = self._get_bucket_by_id(bucket_id)
        response = bucket.upload_file(
            upload_id, upload_auth_token, file_name, content_length, content_type, content_sha1,
            file_infos, data_stream
        )  # yapf: disable
        file_id = response['fileId']
        self.file_id_to_bucket_id[file_id] = bucket_id
        return response

    def upload_part(
        self, upload_url, upload_auth_token, part_number, content_length, sha1_sum, input_stream
    ):
        re.compile('https://upload.example.com/part/([^/]*)')
        url_match = re.match('https://upload.example.com/part/([^/]*)', upload_url)
        if url_match is None:
            raise BadUploadUrl(upload_url)
        file_id = url_match.group(1)
        bucket_id = self.file_id_to_bucket_id[file_id]
        bucket = self._get_bucket_by_id(bucket_id)
        return bucket.upload_part(file_id, part_number, content_length, sha1_sum, input_stream)

    def _assert_account_auth(self, api_url, account_auth_token, account_id):
        assert api_url == self.API_URL
        assert account_auth_token == 'AUTH:' + account_id
        assert account_id in self.authorized_accounts

    def _get_bucket_by_id(self, bucket_id):
        if bucket_id not in self.bucket_id_to_bucket:
            raise NonExistentBucket(bucket_id)
        return self.bucket_id_to_bucket[bucket_id]

    def _get_bucket_by_name(self, bucket_name):
        if bucket_name not in self.bucket_name_to_bucket:
            raise NonExistentBucket(bucket_name)
        return self.bucket_name_to_bucket[bucket_name]
